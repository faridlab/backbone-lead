//! The dedup/merge surface of the lead write path (hand-authored, user-owned).
//!
//! Sibling of [`super::lead_write_service`] (the hub): same `impl LeadWriteService`, chunked
//! here per the family's split pattern. Two verbs and one read:
//!
//! - `duplicate_candidate_groups` — live leads of one company grouped by a shared normalized
//!   contact key (digits-canonicalized phone/WhatsApp, trimmed+lowered email, whitespace-
//!   collapsed organization). Groups are PER-KEY: a pair matching on phone AND email yields two
//!   groups sharing members; connected-component closure is deliberately not attempted (merging
//!   either group dissolves the overlap on the next scan).
//! - `merge_leads` — soft-absorb dupes into a master. One transaction: fetch row-locked →
//!   classify → fill master fields → absorb → commit; the `LeadMerged` event publishes AFTER
//!   commit, and only when a lead was newly absorbed (an idempotent re-merge is silent).
//!
//! Assignment (`owner_user_id` / `sales_team_id`) is STORED only. Autofill, round-robin, and
//! leader-fallback defaults are the composing service's job — do not look for them here.
//!
//! Module boundary: cross-module re-points (deal opportunities, mail activities referencing an
//! absorbed lead id) stay HOST-side, exactly as qualify/convert orchestration already does. The
//! merge outcome carries `master_id` + `absorbed_ids` precisely so the host can re-point; this
//! module takes no dependency on deal/activity.

use std::collections::HashMap;

use backbone_orm::company_scope;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::event::{LeadConversionEvent, LeadMerged};
use crate::infrastructure::persistence::LeadMatchRow;

use super::lead_write_service::{LeadError, LeadWriteService};

/// A pinned merge (caller names the master) carries at most this many absorb targets.
pub const MAX_PINNED_ABSORBS: usize = 5;
/// An auto merge (master picked by confidence) carries 2..=this many leads.
pub const MAX_AUTO_BATCH: usize = 6;
/// Guard against a corrupt absorb cycle. Chains are acyclic by construction (an absorbed lead
/// can never become a master), so this cap is unreachable defense, not an expected path.
const CHAIN_FOLLOW_LIMIT: usize = 16;

// ── vocabulary ────────────────────────────────────────────────────────────────

/// One member of a duplicate-candidate group, as decoded from the scan's aggregated members
/// array (and re-serialized for the HTTP response — camelCase on both sides).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMember {
    pub id: Uuid,
    pub lead_name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
    pub status: String,
    pub party_id: Option<Uuid>,
    pub created_at: Option<DateTime<Utc>>,
}

/// A duplicate-candidate group: every live lead of the company sharing one normalized key.
/// `members` are in confidence order (most credible master first) and `suggested_master_id`
/// is that first member.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DuplicateGroup {
    pub key_kind: String,
    pub key_value: String,
    pub member_count: i64,
    pub suggested_master_id: Uuid,
    pub members: Vec<GroupMember>,
}

/// A requested absorb id that already belongs to a DIFFERENT master: no data changed, the id
/// and its actual master are reported so the caller can redirect without an error.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AbsorbedElsewhere {
    pub id: Uuid,
    pub master_id: Uuid,
}

/// The merge result — also the host's re-point contract: re-point everything referencing an
/// id in `absorbed_ids` to `master_id`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeOutcome {
    pub master_id: Uuid,
    /// Set when the caller pinned a lead that was already absorbed: the request silently
    /// redirected to that lead's ultimate master.
    pub redirected_from: Option<Uuid>,
    /// Every absorb id now pointing at the master, including ones that already did
    /// (idempotent re-merge) — excludes ids reported in `already_absorbed_elsewhere`.
    pub absorbed_ids: Vec<Uuid>,
    pub already_absorbed_elsewhere: Vec<AbsorbedElsewhere>,
    /// Names of the master fields filled from the dupes (master's own non-null values won).
    pub fields_filled: Vec<&'static str>,
}

// ── confidence order (pure, unit-golden-tested) ───────────────────────────────

/// Status precedence for the master pick. Junk/lost rank last (the brief's rule); converted
/// ranks first — its identity is already resolved. Unknown values rank below everything
/// (defensive: the scan casts status to free text).
fn status_rank(status: &str) -> i32 {
    match status {
        "converted" => 5,
        "qualified" => 4,
        "contacted" => 3,
        "new" => 2,
        "junk" | "lost" => 1,
        _ => 0,
    }
}

/// The comparison key: (status, party anchor, created_at, id).
type ConfidenceKey<'a> = (&'a str, Option<Uuid>, Option<DateTime<Utc>>, Uuid);

/// The TOTAL confidence order used everywhere a master is chosen or dupes are walked —
/// suggested master in the scan, the auto-master pick, and the field-fill order. One rule,
/// one implementation, so repeated merges pick identically.
///
/// `Less` means the left lead ranks ABOVE (is a more credible master than) the right one:
/// status precedence first (junk/lost last, converted first), then the party anchor, then the
/// NEWEST created_at (absent created_at ranks last — never compared as raw text), then the
/// smallest id as the deterministic tiebreak.
fn confidence_cmp(a: ConfidenceKey<'_>, b: ConfidenceKey<'_>) -> std::cmp::Ordering {
    status_rank(b.0).cmp(&status_rank(a.0))
        .then_with(|| b.1.is_some().cmp(&a.1.is_some()))
        .then_with(|| b.2.cmp(&a.2))
        .then_with(|| a.3.cmp(&b.3))
}

fn row_key(r: &LeadMatchRow) -> ConfidenceKey<'_> {
    (r.status.as_str(), r.party_id, r.created_at, r.id)
}

fn member_key(m: &GroupMember) -> ConfidenceKey<'_> {
    (m.status.as_str(), m.party_id, m.created_at, m.id)
}

/// Master's non-null value wins; a null field fills from the first non-null value walking the
/// dupes in confidence order (the caller sorts `dupes` first). Returns whether a fill happened.
fn fill_or_keep<T: Clone>(
    master_value: &Option<T>,
    dupes: &[LeadMatchRow],
    extract: impl Fn(&LeadMatchRow) -> Option<T>,
) -> (Option<T>, bool) {
    if let Some(v) = master_value {
        return (Some(v.clone()), false);
    }
    match dupes.iter().filter_map(|r| extract(r)).next() {
        Some(v) => (Some(v), true),
        None => (None, false),
    }
}

// ── verbs ─────────────────────────────────────────────────────────────────────

impl LeadWriteService {
    /// Read the duplicate-candidate groups of one company.
    ///
    /// Members come back in confidence order with the suggested master first. Absorbed and
    /// soft-deleted leads never appear (the scan filters both in SQL).
    pub async fn duplicate_candidate_groups(
        &self,
        company_id: Uuid,
        min_group_size: i64,
        limit: i64,
    ) -> Result<Vec<DuplicateGroup>, LeadError> {
        if min_group_size < 2 {
            return Err(LeadError::Invalid("min_group_size must be at least 2".into()));
        }
        if !(1..=500).contains(&limit) {
            return Err(LeadError::Invalid("limit must be between 1 and 500".into()));
        }
        let scanned = company_scope::with_company_scope(Some(company_id), async {
            self.leads
                .find_duplicate_key_groups(&self.pool, company_id, min_group_size, limit)
                .await
        })
        .await?;
        let mut groups = Vec::with_capacity(scanned.len());
        for g in scanned {
            let mut members: Vec<GroupMember> = serde_json::from_value(g.members).map_err(|e| {
                LeadError::Db(sqlx::Error::ColumnDecode {
                    index: "members".into(),
                    source: Box::new(e),
                })
            })?;
            if members.len() < 2 {
                continue; // defensive: HAVING guarantees >= min_group_size, never regress on decode
            }
            members.sort_by(|a, b| confidence_cmp(member_key(a), member_key(b)));
            let suggested_master_id = members[0].id;
            groups.push(DuplicateGroup {
                key_kind: g.key_kind,
                key_value: g.key_value,
                member_count: g.member_count,
                suggested_master_id,
                members,
            });
        }
        Ok(groups)
    }

    /// Merge duplicate leads: soft-absorb every live dupe into a master.
    ///
    /// `master = Some(id)` pins the master (the caller's path id; if that lead was itself
    /// absorbed, the request redirects to its ultimate master — `redirected_from` reports it).
    /// `master = None` picks the master by the confidence order over `ids`. A converted lead
    /// may be a master but NEVER a dupe (the `party_id` anchor is one-shot) — naming one as an
    /// absorb target refuses the WHOLE request atomically, before any write. Absorb ids
    /// already pointing at this master are idempotent no-ops; ids belonging to a different
    /// master change nothing and come back in `already_absorbed_elsewhere`. Cross-tenant ids
    /// simply do not resolve (RLS fences the fetch) — the fence-shaped answer is 404.
    pub async fn merge_leads(
        &self,
        company_id: Uuid,
        master: Option<Uuid>,
        ids: Vec<Uuid>,
    ) -> Result<MergeOutcome, LeadError> {
        // Batch shape first, pre-DB: fail fast on nonsense without touching the table.
        match master {
            Some(m) => {
                if ids.is_empty() || ids.len() > MAX_PINNED_ABSORBS {
                    return Err(LeadError::InvalidBatch(format!(
                        "a pinned merge absorbs 1..={MAX_PINNED_ABSORBS} leads"
                    )));
                }
                if ids.contains(&m) {
                    return Err(LeadError::AbsorbSelf);
                }
            }
            None => {
                if ids.len() < 2 || ids.len() > MAX_AUTO_BATCH {
                    return Err(LeadError::InvalidBatch(format!(
                        "an auto merge picks a master from 2..={MAX_AUTO_BATCH} leads"
                    )));
                }
            }
        }
        // A repeated id must not double-count: deduplicate, preserving caller order.
        let mut seen = Vec::with_capacity(ids.len());
        for id in ids {
            if !seen.contains(&id) {
                seen.push(id);
            }
        }
        let ids = seen;

        // One transaction, company-bound, row-locked in deterministic id order. The fetch set
        // is the absorb ids PLUS the pinned master (the auto pick is always inside the set).
        let mut fetch_ids = ids.clone();
        if let Some(m) = master {
            if !fetch_ids.contains(&m) {
                fetch_ids.push(m);
            }
        }
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company_id).await?;
        let mut rows: HashMap<Uuid, LeadMatchRow> = self
            .leads
            .fetch_for_merge(&mut tx, &fetch_ids)
            .await?
            .into_iter()
            .map(|r| (r.id, r))
            .collect();
        // Every named id must resolve inside this company (RLS-shaped miss for a cross-tenant
        // or unknown id — the fence's 404, never a leak that it exists elsewhere). The pinned
        // master is held to the same fence: a cross-tenant or unknown master id refuses too.
        if rows.len() != fetch_ids.len() {
            return Err(LeadError::NotFound("lead"));
        }

        // Master resolution.
        let (master_id, redirected_from) = match master {
            Some(path_id) => {
                let start_hop = rows
                    .get(&path_id)
                    .ok_or(LeadError::NotFound("lead"))?
                    .merged_into_lead_id;
                match start_hop {
                    None => (path_id, None),
                    Some(first_hop) => {
                        let ultimate = self.follow_chain(&mut tx, &mut rows, first_hop).await?;
                        (ultimate, Some(path_id))
                    }
                }
            }
            None => {
                let picked = rows
                    .values()
                    .filter(|r| r.merged_into_lead_id.is_none())
                    .min_by(|a, b| confidence_cmp(row_key(a), row_key(b)))
                    .ok_or(LeadError::NotFound("lead"))?;
                let id = picked.id;
                (id, None)
            }
        };
        let master_row = rows.get(&master_id).cloned().ok_or(LeadError::NotFound("lead"))?;

        // Classification — ALL of it before ANY mutation, so every refusal is atomic.
        let mut absorb_live: Vec<LeadMatchRow> = Vec::new();
        let mut absorbed_ids: Vec<Uuid> = Vec::new();
        let mut already_absorbed_elsewhere: Vec<AbsorbedElsewhere> = Vec::new();
        for id in &ids {
            if *id == master_id {
                if master.is_some() {
                    // Only reachable via a redirect: the caller's absorb list names the master
                    // the path lead resolved to.
                    return Err(LeadError::AbsorbSelf);
                }
                continue; // auto pick: the master itself is not an absorb target
            }
            let (status, hop) = {
                let row = rows.get(id).ok_or(LeadError::NotFound("lead"))?;
                (row.status.clone(), row.merged_into_lead_id)
            };
            if status == "converted" {
                return Err(LeadError::AbsorbConverted(*id));
            }
            match hop {
                Some(first_hop) => {
                    let ultimate = self.follow_chain(&mut tx, &mut rows, first_hop).await?;
                    if ultimate == master_id {
                        absorbed_ids.push(*id); // idempotent: already absorbed into this master
                    } else {
                        already_absorbed_elsewhere.push(AbsorbedElsewhere {
                            id: *id,
                            master_id: ultimate,
                        });
                    }
                }
                None => {
                    let row = rows.get(id).ok_or(LeadError::NotFound("lead"))?;
                    absorb_live.push(row.clone());
                }
            }
        }

        // Field fill: master's non-null values win; each nullable lead-owned field fills from
        // the first non-null dupe in confidence order (most credible first). party_id /
        // converted_at are NEVER filled — conversion is once per lead, and a converted lead
        // cannot be a dupe anyway.
        absorb_live.sort_by(|a, b| confidence_cmp(row_key(a), row_key(b)));

        let mut fields_filled: Vec<&'static str> = Vec::new();
        let (organization_name, f) =
            fill_or_keep(&master_row.organization_name, &absorb_live, |r| r.organization_name.clone());
        if f { fields_filled.push("organizationName"); }
        let (phone, f) = fill_or_keep(&master_row.phone, &absorb_live, |r| r.phone.clone());
        if f { fields_filled.push("phone"); }
        let (whatsapp_no, f) = fill_or_keep(&master_row.whatsapp_no, &absorb_live, |r| r.whatsapp_no.clone());
        if f { fields_filled.push("whatsappNo"); }
        let (email, f) = fill_or_keep(&master_row.email, &absorb_live, |r| r.email.clone());
        if f { fields_filled.push("email"); }
        let (notes, f) = fill_or_keep(&master_row.notes, &absorb_live, |r| r.notes.clone());
        if f { fields_filled.push("notes"); }
        let (campaign_id, f) = fill_or_keep(&master_row.campaign_id, &absorb_live, |r| r.campaign_id);
        if f { fields_filled.push("campaignId"); }
        let (owner_user_id, f) = fill_or_keep(&master_row.owner_user_id, &absorb_live, |r| r.owner_user_id);
        if f { fields_filled.push("ownerUserId"); }
        let (sales_team_id, f) = fill_or_keep(&master_row.sales_team_id, &absorb_live, |r| r.sales_team_id);
        if f { fields_filled.push("salesTeamId"); }

        // Mutation: one fill for the master, one CAS absorb per live dupe — all-or-nothing.
        if !absorb_live.is_empty() {
            self.leads
                .fill_master_fields(
                    &mut tx,
                    master_id,
                    organization_name.as_deref(),
                    phone.as_deref(),
                    whatsapp_no.as_deref(),
                    email.as_deref(),
                    notes.as_deref(),
                    campaign_id,
                    owner_user_id,
                    sales_team_id,
                )
                .await?;
            for dupe in &absorb_live {
                self.leads.absorb_lead(&mut tx, dupe.id, master_id).await?;
                absorbed_ids.push(dupe.id);
            }
        }
        tx.commit().await?;

        // Post-commit, and only when a lead was newly absorbed: an idempotent re-merge emits
        // nothing, so replaying consumers never re-point what never moved.
        if !absorb_live.is_empty() {
            self.sink.publish(&LeadConversionEvent::LeadMerged(LeadMerged {
                lead_id: master_id,
                absorbed_ids: absorbed_ids.clone(),
                company_id,
            }));
        }

        Ok(MergeOutcome {
            master_id,
            redirected_from,
            absorbed_ids,
            already_absorbed_elsewhere,
            fields_filled,
        })
    }

    /// Follow `merged_into_lead_id` pointers to the ultimate live master, fetching rows the
    /// batch did not carry (each fetch is row-locked like the batch, on the same transaction).
    async fn follow_chain(
        &self,
        tx: &mut sqlx::PgConnection,
        cache: &mut HashMap<Uuid, LeadMatchRow>,
        from: Uuid,
    ) -> Result<Uuid, LeadError> {
        let mut current = from;
        for _ in 0..CHAIN_FOLLOW_LIMIT {
            let row = match cache.get(&current) {
                Some(r) => r.clone(),
                None => {
                    let fetched = self.leads.fetch_for_merge(tx, &[current]).await?;
                    let r = fetched.into_iter().next().ok_or(LeadError::NotFound("lead"))?;
                    cache.insert(current, r.clone());
                    r
                }
            };
            match row.merged_into_lead_id {
                Some(next) => current = next,
                None => return Ok(current),
            }
        }
        Err(LeadError::ChainTooDeep)
    }
}

#[cfg(test)]
mod tests {
    //! Pure-logic goldens for the confidence order and the fill rule — no DB, no clock.

    use super::*;

    fn key(status: &str, party: Option<Uuid>, created: Option<DateTime<Utc>>, id: Uuid) -> ConfidenceKey<'_> {
        (status, party, created, id)
    }
    fn at(days: u32) -> Option<DateTime<Utc>> {
        Some(DateTime::from_timestamp(1_800_000_000 + i64::from(days) * 86_400, 0).unwrap())
    }

    /// Status precedence: converted ranks above qualified above new; junk and lost rank last.
    #[test]
    fn status_precedence_orders_the_master_pick() {
        let id = Uuid::new_v4();
        assert_eq!(
            confidence_cmp(key("converted", None, None, id), key("qualified", None, None, id)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            confidence_cmp(key("qualified", None, None, id), key("new", None, None, id)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            confidence_cmp(key("junk", None, None, id), key("new", None, None, id)),
            std::cmp::Ordering::Greater
        );
        // junk and lost share the bottom rank: recency decides between them, not status.
        assert_eq!(
            confidence_cmp(key("junk", None, None, id), key("lost", None, None, id)),
            std::cmp::Ordering::Equal
        );
    }

    /// Same status: the party anchor wins, then the NEWEST created_at, absent created_at last.
    #[test]
    fn anchor_then_recency_then_uuid_tiebreak() {
        let small = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let large = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        assert_eq!(
            confidence_cmp(key("new", Some(small), None, large), key("new", None, at(1), small)),
            std::cmp::Ordering::Less,
            "a party anchor outranks recency"
        );
        assert_eq!(
            confidence_cmp(key("new", None, at(2), large), key("new", None, at(1), small)),
            std::cmp::Ordering::Less,
            "newer created_at ranks higher"
        );
        assert_eq!(
            confidence_cmp(key("new", None, None, large), key("new", None, at(1), small)),
            std::cmp::Ordering::Greater,
            "absent created_at ranks last"
        );
        assert_eq!(
            confidence_cmp(key("new", None, at(1), small), key("new", None, at(1), large)),
            std::cmp::Ordering::Less,
            "full tie: smaller uuid wins"
        );
    }

    /// Master's non-null value wins; a null fills from the first non-null dupe in order.
    #[test]
    fn fill_or_keep_prefers_master_then_first_dupe() {
        let mk = |id: Uuid, phone: Option<&str>| LeadMatchRow {
            id,
            lead_name: "n".into(),
            organization_name: None,
            phone: phone.map(str::to_string),
            whatsapp_no: None,
            email: None,
            notes: None,
            status: "new".into(),
            party_id: None,
            campaign_id: None,
            owner_user_id: None,
            sales_team_id: None,
            created_at: None,
            merged_into_lead_id: None,
        };
        let dupes = vec![mk(Uuid::new_v4(), None), mk(Uuid::new_v4(), Some("0812"))];

        let (kept, filled) = fill_or_keep(&Some("+62".into()), &dupes, |r| r.phone.clone());
        assert_eq!(kept.as_deref(), Some("+62"));
        assert!(!filled, "master's own value wins, no fill recorded");

        let (filled_value, filled_flag) = fill_or_keep(&None, &dupes, |r| r.phone.clone());
        assert_eq!(filled_value.as_deref(), Some("0812"));
        assert!(filled_flag, "first non-null dupe fills a null master field");

        let (none, none_filled) = fill_or_keep(&None, &[], |r| r.phone.clone());
        assert!(none.is_none() && !none_filled);
    }
}
