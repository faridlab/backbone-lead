//! Repository for Lead entities
//!
//! Generated skeleton, now **user-owned** — this exact path is declared under `user_owned` in
//! `metaphor.codegen.yaml`, so the generator skips it wholesale. The custom methods below hold the
//! hand-written Lead SQL — the capture + the once-only conversion claim (4-layer rule: services
//! orchestrate and own the unit of work, repositories hold the SQL). Ported from backbone-crm with
//! two adaptations for the split: table `crm.leads` → `lead.leads`, and the convert-once fix
//! `converted_party_id` → `party_id` (the resolved-customer anchor; many leads may share one party
//! over time, conversion stays once-per-lead via the `party_id IS NULL` CAS).
//!
//! Thin newtype over `backbone_orm::GenericCrudRepository<Lead, backbone_orm::SoftDelete>`.
//! All standard CRUD methods are available via `Deref`.

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use backbone_orm::company_scope;

use crate::domain::entity::Lead;

/// Table name for Lead entities
pub const TABLE_NAME: &str = "lead.leads";

/// Repository for Lead entities.
///
/// All standard CRUD, soft-delete, pagination, and bulk methods are
/// provided automatically via `Deref` to `backbone_orm::GenericCrudRepository`.
pub struct LeadRepository(
    backbone_orm::GenericCrudRepository<Lead, backbone_orm::SoftDelete>,
);

impl std::ops::Deref for LeadRepository {
    type Target = backbone_orm::GenericCrudRepository<Lead, backbone_orm::SoftDelete>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl LeadRepository {
    /// Create a new repository instance.
    pub fn new(pool: PgPool) -> Self {
        Self(backbone_orm::GenericCrudRepository::new(pool, TABLE_NAME))
    }
}

/// The exact row a lead capture writes.
///
/// Mirrors the raw column shape rather than the `Lead` entity: `source` is carried as a free string and
/// cast at the DB (`$8::lead_source`), so a bad source fails as a DB error rather than a deserialize
/// panic.
pub struct NewLeadRow<'a> {
    pub id: Uuid,
    pub company_id: Uuid,
    pub lead_name: &'a str,
    pub organization_name: Option<&'a str>,
    pub phone: Option<&'a str>,
    pub whatsapp_no: Option<&'a str>,
    pub email: Option<&'a str>,
    pub source: &'a str,
    pub campaign_id: Option<Uuid>,
    pub notes: Option<&'a str>,
    pub owner_user_id: Option<Uuid>,
    pub sales_team_id: Option<Uuid>,
    pub utm_source: Option<&'a str>,
    pub utm_medium: Option<&'a str>,
    pub utm_campaign: Option<&'a str>,
}

/// The qualification pre-flight projection: the company to bind, the live status, the party the
/// opportunity inherits, and the campaign it snapshots for attribution.
pub struct LeadForQualifyRow {
    pub company_id: Uuid,
    pub status: String,
    pub party_id: Option<Uuid>,
    pub campaign_id: Option<Uuid>,
}

/// The conversion pre-flight projection: the identity fields the party ACL mints a Customer from, plus
/// the once-only gate (`party_id`).
pub struct LeadForConvertRow {
    pub company_id: Uuid,
    pub lead_name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
    pub status: String,
    pub party_id: Option<Uuid>,
}

/// One duplicate-candidate group: a normalized key shared by several live leads of one company.
///
/// `members` is the JSON array the scan aggregates (one object per lead, camelCase keys) — the
/// service decodes it and applies the confidence order in Rust.
pub struct DuplicateKeyGroupRow {
    pub key_kind: String,
    pub key_value: String,
    pub member_count: i64,
    pub members: serde_json::Value,
}

/// The merge-decision projection for one lead: every field the confidence order, the field-fill rule,
/// and the absorb classification read. `status` is free text (cast at the DB) so an unexpected value
/// fails classification loudly instead of deserializing into a panic.
#[derive(Debug, Clone)]
pub struct LeadMatchRow {
    pub id: Uuid,
    pub lead_name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
    pub notes: Option<String>,
    pub status: String,
    pub party_id: Option<Uuid>,
    pub campaign_id: Option<Uuid>,
    pub owner_user_id: Option<Uuid>,
    pub sales_team_id: Option<Uuid>,
    pub utm_source: Option<String>,
    pub utm_medium: Option<String>,
    pub utm_campaign: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub merged_into_lead_id: Option<Uuid>,
}

/// Hand-written Lead SQL. Lives here (not in the write service) per the module's 4-layer rule.
impl LeadRepository {
    /// Capture a lead.
    ///
    /// A write outside any transaction: takes the pool and runs `execute_scoped` so the RLS fence
    /// (ADR-0008) applies. The caller wraps this in `with_company_scope(Some(company))` — the company is
    /// on the DTO, and that scope is what satisfies the INSERT's WITH CHECK fence. The explicit
    /// `company_id` bind stays as defense-in-depth.
    pub async fn insert_lead(&self, pool: &PgPool, l: &NewLeadRow<'_>) -> Result<(), sqlx::Error> {
        company_scope::execute_scoped(
            pool,
            sqlx::query(
                r#"INSERT INTO lead.leads
                     (id, company_id, lead_name, organization_name, phone, whatsapp_no, email,
                      source, campaign_id, status, notes, owner_user_id, sales_team_id,
                      utm_source, utm_medium, utm_campaign)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8::lead_source,$9,'new'::lead_status,$10,$11,$12,$13,$14,$15)"#,
            )
            .bind(l.id).bind(l.company_id).bind(l.lead_name).bind(l.organization_name).bind(l.phone)
            .bind(l.whatsapp_no).bind(l.email).bind(l.source).bind(l.campaign_id).bind(l.notes)
            .bind(l.owner_user_id).bind(l.sales_team_id)
            .bind(l.utm_source).bind(l.utm_medium).bind(l.utm_campaign),
        )
        .await?;
        Ok(())
    }

    /// Read what the qualification decision needs.
    ///
    /// ID-only read: no company argument to scope from up front. `fetch_optional_row_scoped` rides the
    /// REQUEST-dedicated connection (which carries the caller's `app.company_id`), so another company's
    /// lead simply isn't found. The company on the returned row is what the caller binds onto its own
    /// transaction.
    pub async fn find_for_qualify(&self, pool: &PgPool, lead_id: Uuid) -> Result<Option<LeadForQualifyRow>, sqlx::Error> {
        let row = company_scope::fetch_optional_row_scoped(
            pool,
            sqlx::query(
                r#"SELECT company_id, status::text AS status, party_id, campaign_id
                   FROM lead.leads WHERE id=$1 AND (metadata->>'deleted_at') IS NULL"#,
            )
            .bind(lead_id),
        )
        .await?;
        Ok(row.map(|r| LeadForQualifyRow {
            company_id: r.get("company_id"),
            status: r.get("status"),
            party_id: r.get("party_id"),
            campaign_id: r.get("campaign_id"),
        }))
    }

    /// Read the identity + gate the conversion decision needs. ID-only read — same fencing as
    /// `find_for_qualify`.
    pub async fn find_for_convert(&self, pool: &PgPool, lead_id: Uuid) -> Result<Option<LeadForConvertRow>, sqlx::Error> {
        let row = company_scope::fetch_optional_row_scoped(
            pool,
            sqlx::query(
                r#"SELECT company_id, lead_name, organization_name, phone, whatsapp_no, email,
                          status::text AS status, party_id
                   FROM lead.leads WHERE id=$1 AND (metadata->>'deleted_at') IS NULL"#,
            )
            .bind(lead_id),
        )
        .await?;
        Ok(row.map(|r| LeadForConvertRow {
            company_id: r.get("company_id"),
            lead_name: r.get("lead_name"),
            organization_name: r.get("organization_name"),
            phone: r.get("phone"),
            whatsapp_no: r.get("whatsapp_no"),
            email: r.get("email"),
            status: r.get("status"),
            party_id: r.get("party_id"),
        }))
    }

    /// Advance a lead to qualified — but keep 'converted' if it already was (the status IN filter is
    /// what preserves that).
    ///
    /// Takes the CALLER'S connection so it commits with the opportunity it was qualified into. The
    /// caller has already bound the company on that connection — don't re-bind here.
    pub async fn mark_qualified(&self, conn: &mut sqlx::PgConnection, lead_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE lead.leads SET status='qualified'::lead_status
               WHERE id=$1 AND status IN ('new','contacted','qualified')"#,
        )
        .bind(lead_id)
        .execute(conn)
        .await?;
        Ok(())
    }

    /// Claim the conversion exactly once (CAS on `party_id IS NULL`). Returns rows affected
    /// (0 = a concurrent conversion won; the caller re-reads the winner's party).
    ///
    /// convert-once fix: `party_id` is the resolved-customer anchor. The CAS is per-lead (a lead
    /// resolves at most once), but the same party may be claimed by other leads over time — a
    /// converted Customer can re-enter the pipeline as a new Lead.
    ///
    /// Takes the CALLER'S connection so the claim and the opportunity back-fill commit as one unit. The
    /// caller has already bound the company on that connection — don't re-bind here.
    pub async fn claim_conversion(
        &self,
        conn: &mut sqlx::PgConnection,
        lead_id: Uuid,
        party_id: Uuid,
    ) -> Result<u64, sqlx::Error> {
        let moved = sqlx::query(
            r#"UPDATE lead.leads
               SET party_id=$2, status='converted'::lead_status, converted_at=now()
               WHERE id=$1 AND party_id IS NULL"#,
        )
        .bind(lead_id)
        .bind(party_id)
        .execute(conn)
        .await?;
        Ok(moved.rows_affected())
    }

    /// Re-read the winner's party id after a losing conversion CAS.
    ///
    /// A read outside the (rolled-back) transaction: `fetch_one_scalar_scoped` applies the RLS fence.
    /// The caller wraps this in `with_company_scope(Some(company_id))`.
    pub async fn fetch_party_id(&self, pool: &PgPool, lead_id: Uuid) -> Result<Uuid, sqlx::Error> {
        company_scope::fetch_one_scalar_scoped(
            pool,
            sqlx::query_scalar("SELECT party_id FROM lead.leads WHERE id=$1").bind(lead_id),
        )
        .await
    }

    // ── duplicate-candidate scan + merge (dedup/merge surface) ────────────────

    /// Scan for duplicate candidates: live leads of one company grouped by a shared normalized key.
    ///
    /// One round trip: a UNION-ALL over the four key kinds (phone / whatsapp / email / org), grouped
    /// per key with HAVING count >= `min_group_size`, members aggregated as a JSON array riding the
    /// group row. Groups are per-key by design — a pair matching on phone AND email yields two groups
    /// sharing members (merging either dissolves the overlap on the next scan); connected-component
    /// closure is deliberately not attempted.
    ///
    /// Live = not soft-deleted and not absorbed (`merged_into_lead_id IS NULL`), so an absorbed lead
    /// never re-enters a candidate scan. The caller wraps this in `with_company_scope(Some(company))`;
    /// the `company_id=$1` filter stays as defense-in-depth.
    pub async fn find_duplicate_key_groups(
        &self,
        pool: &PgPool,
        company_id: Uuid,
        min_group_size: i64,
        limit: i64,
    ) -> Result<Vec<DuplicateKeyGroupRow>, sqlx::Error> {
        const LIVE: &str = "(metadata->>'deleted_at') IS NULL AND merged_into_lead_id IS NULL";
        let sql = format!(
            r#"WITH k AS (
                   SELECT 'phone'::text AS key_kind, phone_key AS key_value, id, lead_name,
                          organization_name, phone, whatsapp_no, email, status::text AS status,
                          party_id, utm_source, utm_medium, utm_campaign,
                          (metadata->>'created_at')::timestamptz AS created_at
                     FROM lead.leads
                    WHERE company_id = $1 AND {live} AND phone_key IS NOT NULL
                   UNION ALL
                   SELECT 'whatsapp', whatsapp_key, id, lead_name, organization_name, phone,
                          whatsapp_no, email, status::text, party_id, utm_source, utm_medium,
                          utm_campaign, (metadata->>'created_at')::timestamptz
                     FROM lead.leads
                    WHERE company_id = $1 AND {live} AND whatsapp_key IS NOT NULL
                   UNION ALL
                   SELECT 'email', email_key, id, lead_name, organization_name, phone,
                          whatsapp_no, email, status::text, party_id, utm_source, utm_medium,
                          utm_campaign, (metadata->>'created_at')::timestamptz
                     FROM lead.leads
                    WHERE company_id = $1 AND {live} AND email_key IS NOT NULL
                   UNION ALL
                   SELECT 'org', org_key, id, lead_name, organization_name, phone,
                          whatsapp_no, email, status::text, party_id, utm_source, utm_medium,
                          utm_campaign, (metadata->>'created_at')::timestamptz
                     FROM lead.leads
                    WHERE company_id = $1 AND {live} AND org_key IS NOT NULL
               ),
               g AS (
                   SELECT key_kind, key_value, count(*)::bigint AS member_count
                     FROM k GROUP BY key_kind, key_value HAVING count(*) >= $2
               )
               SELECT g.key_kind   AS key_kind,
                      g.key_value  AS key_value,
                      g.member_count AS member_count,
                      jsonb_agg(jsonb_build_object(
                          'id', k.id, 'leadName', k.lead_name, 'organizationName', k.organization_name,
                          'phone', k.phone, 'whatsappNo', k.whatsapp_no, 'email', k.email,
                          'status', k.status, 'partyId', k.party_id, 'createdAt', k.created_at,
                          'utmSource', k.utm_source, 'utmMedium', k.utm_medium,
                          'utmCampaign', k.utm_campaign
                      ) ORDER BY k.id) AS members
                 FROM g JOIN k USING (key_kind, key_value)
                GROUP BY g.key_kind, g.key_value, g.member_count
                ORDER BY g.member_count DESC, g.key_kind, g.key_value
                LIMIT $3"#,
            live = LIVE
        );
        let rows = company_scope::fetch_all_rows_scoped(
            pool,
            sqlx::query(&sql)
                .bind(company_id)
                .bind(min_group_size)
                .bind(limit),
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| DuplicateKeyGroupRow {
                key_kind: r.get("key_kind"),
                key_value: r.get("key_value"),
                member_count: r.get("member_count"),
                members: r.get("members"),
            })
            .collect())
    }

    /// Fetch the leads a merge decision needs, row-locked.
    ///
    /// Takes the CALLER'S transaction (already company-bound) so the lock, the classification, and
    /// the mutations are one unit. `ORDER BY id` gives every concurrent merge the same lock order, so
    /// overlapping batches serialize instead of deadlocking. RLS fences cross-tenant ids out of the
    /// result (zero rows → the caller answers a fence-shaped 404). Soft-deleted rows are excluded.
    pub async fn fetch_for_merge(
        &self,
        conn: &mut sqlx::PgConnection,
        ids: &[Uuid],
    ) -> Result<Vec<LeadMatchRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"SELECT id, lead_name, organization_name, phone, whatsapp_no, email, notes,
                      status::text AS status, party_id, campaign_id, owner_user_id, sales_team_id,
                      utm_source, utm_medium, utm_campaign,
                      (metadata->>'created_at')::timestamptz AS created_at, merged_into_lead_id
                 FROM lead.leads
                WHERE id = ANY($1) AND (metadata->>'deleted_at') IS NULL
                ORDER BY id
                FOR UPDATE"#,
        )
        .bind(ids)
        .fetch_all(conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| LeadMatchRow {
                id: r.get("id"),
                lead_name: r.get("lead_name"),
                organization_name: r.get("organization_name"),
                phone: r.get("phone"),
                whatsapp_no: r.get("whatsapp_no"),
                email: r.get("email"),
                notes: r.get("notes"),
                status: r.get("status"),
                party_id: r.get("party_id"),
                campaign_id: r.get("campaign_id"),
                owner_user_id: r.get("owner_user_id"),
                sales_team_id: r.get("sales_team_id"),
                utm_source: r.get("utm_source"),
                utm_medium: r.get("utm_medium"),
                utm_campaign: r.get("utm_campaign"),
                created_at: r.get("created_at"),
                merged_into_lead_id: r.get("merged_into_lead_id"),
            })
            .collect())
    }

    /// Fill the master's nullable lead-owned fields after a merge (master's own non-null values
    /// already won in the service — only the still-null fields are written here, with the values the
    /// service picked from the dupes in confidence order).
    ///
    /// Takes the CALLER'S transaction; the row is already locked by `fetch_for_merge`.
    #[allow(clippy::too_many_arguments)]
    pub async fn fill_master_fields(
        &self,
        conn: &mut sqlx::PgConnection,
        master_id: Uuid,
        organization_name: Option<&str>,
        phone: Option<&str>,
        whatsapp_no: Option<&str>,
        email: Option<&str>,
        notes: Option<&str>,
        campaign_id: Option<Uuid>,
        owner_user_id: Option<Uuid>,
        sales_team_id: Option<Uuid>,
        utm_source: Option<&str>,
        utm_medium: Option<&str>,
        utm_campaign: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE lead.leads
                  SET organization_name = $2, phone = $3, whatsapp_no = $4, email = $5,
                      notes = $6, campaign_id = $7, owner_user_id = $8, sales_team_id = $9,
                      utm_source = $10, utm_medium = $11, utm_campaign = $12
                WHERE id = $1"#,
        )
        .bind(master_id)
        .bind(organization_name)
        .bind(phone)
        .bind(whatsapp_no)
        .bind(email)
        .bind(notes)
        .bind(campaign_id)
        .bind(owner_user_id)
        .bind(sales_team_id)
        .bind(utm_source)
        .bind(utm_medium)
        .bind(utm_campaign)
        .execute(conn)
        .await?;
        Ok(())
    }

    /// Soft-absorb a dupe into its master (CAS on `merged_into_lead_id IS NULL`).
    ///
    /// Absorbed leads are never deleted: the pointer + `merged_at` mark the absorb, exclude the row
    /// from future candidate scans, and make it permanently ineligible as a master or a dupe. Returns
    /// rows affected (0 = a concurrent merge absorbed it first; the caller re-reads the winner).
    ///
    /// Takes the CALLER'S transaction so the absorb commits with the master's field fill as one unit.
    pub async fn absorb_lead(
        &self,
        conn: &mut sqlx::PgConnection,
        dupe_id: Uuid,
        master_id: Uuid,
    ) -> Result<u64, sqlx::Error> {
        let moved = sqlx::query(
            r#"UPDATE lead.leads
                  SET merged_into_lead_id = $2, merged_at = now()
                WHERE id = $1 AND merged_into_lead_id IS NULL"#,
        )
        .bind(dupe_id)
        .bind(master_id)
        .execute(conn)
        .await?;
        Ok(moved.rows_affected())
    }
}

backbone_core::impl_crud_repository!(LeadRepository, Lead, soft_delete);
