//! Behavior tests for the lead dedup/merge surface (mirrors docs/business-flows/golden-cases.md).
//!
//! DB-backed, against real Postgres (`lead` schema): normalization goldens for the generated
//! match-key columns, duplicate-candidate grouping, the confidence-ordered master pick, the
//! merge field-fill rule, idempotence, and refusals.
//! Requires DATABASE_URL; the family convention is a scratch database created inside the
//! running dev postgres container and dropped afterwards.

use std::sync::{Arc, Mutex};

use sqlx::{PgPool, Row};
use uuid::Uuid;

use backbone_lead::application::service::lead_write_service::{LeadError, LeadWriteService, NewLead};
use backbone_lead::domain::event::{LeadConversionEvent, LeadEventSink};

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@127.0.0.1:5433/lead_merge_test".into())
}

async fn pool() -> PgPool {
    PgPool::connect(&db_url()).await.expect("connect DB")
}

/// Capture a lead through the real write path.
async fn capture(svc: &LeadWriteService, name: &str, phone: Option<&str>, wa: Option<&str>, email: Option<&str>) -> Uuid {
    svc.create_lead(NewLead {
        lead_name: name.into(),
        organization_name: None,
        phone: phone.map(str::to_string),
        whatsapp_no: wa.map(str::to_string),
        email: email.map(str::to_string),
        source: "whatsapp".into(),
        campaign_id: None,
        notes: None,
        owner_user_id: None,
        sales_team_id: None,
        utm_source: None,
        utm_medium: None,
        utm_campaign: None,
    })
    .await
    .expect("capture lead")
}

/// Capture a lead carrying UTM attribution, through the real write path.
async fn capture_with_utm(
    svc: &LeadWriteService,
    name: &str,
    phone: &str,
    utm: Option<(&str, &str, &str)>,
) -> Uuid {
    svc.create_lead(NewLead {
        lead_name: name.into(),
        organization_name: None,
        phone: Some(phone.into()),
        whatsapp_no: None,
        email: None,
        source: "website".into(),
        campaign_id: None,
        notes: None,
        owner_user_id: None,
        sales_team_id: None,
        utm_source: utm.map(|u| u.0.into()),
        utm_medium: utm.map(|u| u.1.into()),
        utm_campaign: utm.map(|u| u.2.into()),
    })
    .await
    .expect("capture attributed lead")
}

async fn set_status(pool: &PgPool, id: Uuid, status: &str) {
    sqlx::query("UPDATE lead.leads SET status=$2::lead_status WHERE id=$1")
        .bind(id).bind(status).execute(pool).await.unwrap();
}

async fn set_party_anchor(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE lead.leads SET party_id=$2, status='converted'::lead_status, converted_at=now() WHERE id=$1",
    )
    .bind(id).bind(Uuid::new_v4()).execute(pool).await.unwrap();
}

/// Overwrite the audit-metadata created_at (the confidence order's recency input) with a fixed
/// timestamp, so ordering goldens do not race the insert clock.
async fn set_created_at(pool: &PgPool, id: Uuid, iso: &str) {
    sqlx::query(
        "UPDATE lead.leads SET metadata = jsonb_set(metadata, '{created_at}', to_jsonb($2::text)) WHERE id=$1",
    )
    .bind(id).bind(iso).execute(pool).await.unwrap();
}

async fn merged_into(pool: &PgPool, id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT merged_into_lead_id FROM lead.leads WHERE id=$1")
        .bind(id).fetch_one(pool).await.unwrap()
}

/// Soft-delete a lead the way generic CRUD does (audit metadata `deleted_at`).
async fn soft_delete(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE lead.leads SET metadata = jsonb_set(metadata, '{deleted_at}', to_jsonb(now()::text)) WHERE id=$1")
        .bind(id).execute(pool).await.unwrap();
}

/// A run-unique 8-digit string. The duplicate scan is database-wide (the tenant fence is the
/// composing service's decorator, absent here), so the scan tests derive their contact
/// values from this instead of fixed numbers — fixed values would collide with rows left by
/// a previous run on the same scratch database, and with sibling tests running in parallel.
/// A per-process counter guarantees distinct values even when consecutive calls land on the
/// same clock tick; the clock keeps values distinct across runs and processes.
fn run_digits() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let step = SEQ.fetch_add(1, Ordering::Relaxed) as u64;
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let tick = (d.as_secs() << 20) ^ (d.subsec_nanos() as u64);
    format!("{:08}", tick.wrapping_add(step.wrapping_mul(0x9E3779B1)) % 100_000_000)
}

/// Two formatted phone variants that normalize onto the same run-unique phone key, plus that
/// key: '+62 9dd-ddd-ddd' and '09dddddddd' both canonicalize to '629dddddddd'.
fn unique_phone_pair() -> (String, String, String) {
    let body = format!("9{}", run_digits());
    let key = format!("62{body}");
    (
        format!("+62 {}-{}-{}", &body[0..3], &body[3..6], &body[6..]),
        format!("0{body}"),
        key,
    )
}

/// A run-unique email (the key normalizes to its lowercase form).
fn unique_email() -> String {
    format!("dupe-{}@scan.example", run_digits())
}

/// The candidate groups whose member set is exactly `ids` — this test's own cluster, picked
/// out of the database-wide scan (other rows on the shared scratch database group too).
fn groups_over<'g>(
    groups: &'g [backbone_lead::application::service::lead_merge::DuplicateGroup],
    ids: &[Uuid],
) -> Vec<&'g backbone_lead::application::service::lead_merge::DuplicateGroup> {
    groups
        .iter()
        .filter(|g| {
            let member_ids: Vec<Uuid> = g.members.iter().map(|m| m.id).collect();
            member_ids.len() == ids.len() && ids.iter().all(|id| member_ids.contains(id))
        })
        .collect()
}

async fn key_of(pool: &PgPool, id: Uuid, col: &str) -> Option<String> {
    let q = format!("SELECT {col} FROM lead.leads WHERE id = $1");
    sqlx::query_scalar(&q).bind(id).fetch_one(pool).await.unwrap()
}

// ── NG: normalization goldens (the generated columns, not the verb) ───────────

/// NG-1/2/3: '+62 812-3456-789' and '0812-3456-789' canonicalize to the SAME phone key (the
/// motivating join); a bare '812…' gains the 62 prefix; a non-Indonesian number passes through
/// digit-only; NULL/empty keys stay NULL so keyless leads never cluster. Email trims + lowers;
/// organization trims, lowers, and collapses internal whitespace.
#[tokio::test]
async fn normalization_goldens() {
    let pool = pool().await;
    let id_plus62 = Uuid::new_v4();
    let id_domestic = Uuid::new_v4();
    let id_bare8 = Uuid::new_v4();
    let id_us = Uuid::new_v4();
    let id_email_org = Uuid::new_v4();
    let id_empty = Uuid::new_v4();

    for (id, phone, email, org) in [
        (id_plus62, Some("+62 812-3456-789"), None as Option<&str>, None as Option<&str>),
        (id_domestic, Some("0812-3456-789"), None, None),
        (id_bare8, Some("812-3456-7890"), None, None),
        (id_us, Some("+1 555 0100"), None, None),
        (id_email_org, None, Some("  Foo@BAR.id "), Some("  PT   Cipta  ")),
        (id_empty, Some(""), Some(""), Some("")),
    ] {
        sqlx::query(
            "INSERT INTO lead.leads (id, lead_name, phone, email, organization_name) VALUES ($1,'NG',$2,$3,$4)",
        )
        .bind(id).bind(phone).bind(email).bind(org)
        .execute(&pool)
        .await
        .unwrap();
    }

    assert_eq!(key_of(&pool, id_plus62, "phone_key").await.as_deref(), Some("628123456789"));
    assert_eq!(key_of(&pool, id_domestic, "phone_key").await.as_deref(), Some("628123456789"), "domestic 0-prefix joins the +62 form");
    assert_eq!(key_of(&pool, id_bare8, "phone_key").await.as_deref(), Some("6281234567890"), "bare 8-prefix gains the 62 country code");
    assert_eq!(key_of(&pool, id_us, "phone_key").await.as_deref(), Some("15550100"), "non-Indonesian passes through digit-only");
    assert_eq!(key_of(&pool, id_email_org, "email_key").await.as_deref(), Some("foo@bar.id"));
    assert_eq!(key_of(&pool, id_email_org, "org_key").await.as_deref(), Some("pt cipta"), "internal whitespace collapses");
    // Empty inputs are NULL keys, never empty strings — keyless leads never cluster.
    assert_eq!(key_of(&pool, id_empty, "phone_key").await, None);
    assert_eq!(key_of(&pool, id_empty, "email_key").await, None);
    assert_eq!(key_of(&pool, id_empty, "org_key").await, None);
    // Absent input likewise.
    assert_eq!(key_of(&pool, id_plus62, "whatsapp_key").await, None);
}

// ── DG: duplicate-candidate grouping ──────────────────────────────────────────

/// DG-1: a formatted-variant pair yields ONE phone group with the match reason and the
/// suggested master; DG-2: matching on two keys yields two groups sharing the same members
/// (per-key groups, documented non-collapse).
#[tokio::test]
async fn grouping_by_normalized_key() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());

    // DG-1: phone variants only, on this run's own key namespace.
    let (phone_a, phone_b, phone_key) = unique_phone_pair();
    let a = capture(&svc, "Andi", Some(&phone_a), None, None).await;
    let b = capture(&svc, "Andi dupe", Some(&phone_b), None, None).await;
    // Same status / recency on both → the members' confidence order falls to the uuid tiebreak.
    for id in [a, b] {
        set_created_at(&pool, id, "2026-02-01T00:00:00+00:00").await;
    }
    let groups = svc.duplicate_candidate_groups(2, 50).await.unwrap();
    let own = groups_over(&groups, &[a, b]);
    assert_eq!(own.len(), 1, "formatted variants join into exactly one group");
    let g = own[0];
    assert_eq!(g.key_kind, "phone");
    assert_eq!(g.key_value, phone_key);
    assert_eq!(g.member_count, 2);
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(g.members.iter().map(|m| m.id).collect::<Vec<_>>(), expected, "members in confidence order (uuid tiebreak)");
    assert_eq!(g.suggested_master_id, expected[0]);

    // DG-2: add an email match on the same pair → a second group with the same members.
    let email = unique_email();
    sqlx::query("UPDATE lead.leads SET email=$2 WHERE id = ANY($1)")
        .bind(vec![a, b]).bind(&email).execute(&pool).await.unwrap();
    let groups = svc.duplicate_candidate_groups(2, 50).await.unwrap();
    let own = groups_over(&groups, &[a, b]);
    assert_eq!(own.len(), 2, "phone AND email matches are two per-key groups, not one merged cluster");
    let email_group = own.iter().find(|g| g.key_kind == "email").expect("email group present");
    assert_eq!(email_group.key_value, email.to_lowercase());
    assert_eq!(email_group.member_count, 2);
}

/// DG-3: an absorbed lead never re-enters a scan; DG-4: a soft-deleted lead does not either.
#[tokio::test]
async fn absorbed_and_deleted_leave_the_scan() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());
    let (phone_a, phone_b, _) = unique_phone_pair();
    let a = capture(&svc, "Budi", Some(&phone_a), None, None).await;
    let b = capture(&svc, "Budi dupe", Some(&phone_b), None, None).await;
    use backbone_lead::application::service::lead_merge::DuplicateGroup;
    let in_scan = |groups: &[DuplicateGroup], id: Uuid| {
        groups.iter().any(|g| g.members.iter().any(|m| m.id == id))
    };

    let groups = svc.duplicate_candidate_groups(2, 50).await.unwrap();
    assert!(
        in_scan(&groups, a) && in_scan(&groups, b),
        "the fresh pair is a duplicate candidate"
    );

    svc.merge_leads(Some(a), vec![b]).await.unwrap();
    let groups = svc.duplicate_candidate_groups(2, 50).await.unwrap();
    assert!(
        !in_scan(&groups, a) && !in_scan(&groups, b),
        "DG-3: the absorbed pair leaves the scan"
    );

    // DG-4: a fresh pair where one side goes soft-deleted.
    let (phone_c, phone_d, _) = unique_phone_pair();
    let c = capture(&svc, "Citra", Some(&phone_c), None, None).await;
    let d = capture(&svc, "Citra dupe", Some(&phone_d), None, None).await;
    soft_delete(&pool, d).await;
    let groups = svc.duplicate_candidate_groups(2, 50).await.unwrap();
    assert!(
        !in_scan(&groups, c) && !in_scan(&groups, d),
        "DG-4: the soft-deleted side leaves the scan"
    );
}

// ── MG: confidence-ordered master pick ────────────────────────────────────────

/// MG-1: converted beats qualified beats contacted beats new beats junk/lost — a converted
/// lead CAN be the master, and its party anchor is preserved untouched.
#[tokio::test]
async fn status_precedence_picks_the_master() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());
    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    for (i, id) in ids.iter().enumerate() {
        sqlx::query("INSERT INTO lead.leads (id, lead_name, phone) VALUES ($1,'Eka',$2)")
            .bind(id).bind(format!("seed-{i}"))
            .execute(&pool).await.unwrap();
    }
    let [l_new, l_contacted, l_qualified, l_converted, l_junk] = [ids[0], ids[1], ids[2], ids[3], ids[4]];
    sqlx::query("UPDATE lead.leads SET phone='+62 815-999-8888' WHERE id = ANY($1)")
        .bind(&ids).execute(&pool).await.unwrap();
    set_status(&pool, l_contacted, "contacted").await;
    set_status(&pool, l_qualified, "qualified").await;
    set_party_anchor(&pool, l_converted).await;
    set_status(&pool, l_junk, "junk").await;

    let outcome = svc.merge_leads(None, ids.clone()).await.unwrap();
    assert_eq!(outcome.master_id, l_converted, "converted outranks qualified/contacted/new/junk");

    // The master's party anchor is untouched; every dupe is absorbed into the converted master.
    let (party, status): (Option<Uuid>, String) =
        sqlx::query_as("SELECT party_id, status::text FROM lead.leads WHERE id=$1")
            .bind(l_converted).fetch_one(&pool).await.unwrap();
    assert!(party.is_some());
    assert_eq!(status, "converted");
    assert_eq!(outcome.absorbed_ids.len(), 4);
    for id in [l_new, l_contacted, l_qualified, l_junk] {
        assert_eq!(merged_into(&pool, id).await, Some(l_converted));
    }
}

/// MG-2: junk vs lost share the bottom rank — the NEWEST created_at wins; MG-3: a full tie
/// falls to the smallest uuid, and repeated scans keep picking it (determinism).
#[tokio::test]
async fn recency_then_uuid_tiebreak_is_deterministic() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());

    // MG-2: junk (older) vs lost (newer) → the newer lost lead masters.
    let j = capture(&svc, "Fajar", Some("+62 816-1"), None, None).await;
    let l = capture(&svc, "Fajar", Some("0816-1"), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 816-777-8888' WHERE id = ANY($1)")
        .bind(vec![j, l]).execute(&pool).await.unwrap();
    set_status(&pool, j, "junk").await;
    set_status(&pool, l, "lost").await;
    set_created_at(&pool, j, "2026-01-01T00:00:00+00:00").await;
    set_created_at(&pool, l, "2026-06-01T00:00:00+00:00").await;
    let outcome = svc.merge_leads(None, vec![j, l]).await.unwrap();
    assert_eq!(outcome.master_id, l, "MG-2: same bottom status rank — newest created_at wins");

    // MG-3: identical status, party, created_at → smallest uuid wins, repeatedly.
    let (phone_x, phone_y, _) = unique_phone_pair();
    let x = capture(&svc, "Gita", Some(&phone_x), None, None).await;
    let y = capture(&svc, "Gita", Some(&phone_y), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no=$2 WHERE id = $1")
        .bind(x).bind(&phone_x).execute(&pool).await.unwrap();
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no=$2 WHERE id = $1")
        .bind(y).bind(&phone_y).execute(&pool).await.unwrap();
    for id in [x, y] {
        set_created_at(&pool, id, "2026-03-01T12:00:00+00:00").await;
    }
    let expected = x.min(y);
    for _ in 0..3 {
        let groups = svc.duplicate_candidate_groups(2, 50).await.unwrap();
        let own: Vec<_> = groups
            .iter()
            .filter(|g| g.members.iter().any(|m| m.id == x))
            .collect();
        assert_eq!(own.len(), 1, "exactly one candidate group carries this run's tie pair");
        assert_eq!(own[0].suggested_master_id, expected, "MG-3: full tie — smallest uuid wins, every scan");
    }
}

/// MG-4: the master's non-null values win; each null lead-owned field fills from the best
/// dupe; `party_id` / `converted_at` are never filled.
#[tokio::test]
async fn field_fill_prefers_master_then_dupes() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());
    let master = capture(&svc, "Master", Some("+62 818-1"), None, None).await;
    let dupe = capture(&svc, "Dupe", Some("0818-1"), None, None).await;
    let (campaign, owner, team) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    // Both share the whatsapp identity so they are true duplicates.
    sqlx::query(
        r#"UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 818-222-3333',
                  notes='master notes stay', campaign_id=$2, owner_user_id=$3, sales_team_id=$4
            WHERE id = ANY($1)"#,
    )
    .bind(vec![master, dupe]).bind(campaign).bind(owner).bind(team)
    .execute(&pool).await.unwrap();
    // The dupe carries the values the master lacks.
    set_status(&pool, dupe, "qualified").await;
    sqlx::query(
        r#"UPDATE lead.leads SET organization_name='PT Dupes Org', email='dupe@mail.id',
                  phone='+62 818-999-1111' WHERE id=$1"#,
    )
    .bind(dupe).execute(&pool).await.unwrap();

    let outcome = svc.merge_leads(Some(master), vec![dupe]).await.unwrap();
    assert_eq!(outcome.master_id, master, "a pinned master overrides the confidence pick");

    let row = sqlx::query(
        r#"SELECT organization_name, phone, email, notes, campaign_id, owner_user_id,
                  sales_team_id, party_id, converted_at
             FROM lead.leads WHERE id=$1"#,
    )
    .bind(master)
    .fetch_one(&pool)
    .await
    .unwrap();
    let s = |c: &str| row.try_get::<Option<String>, _>(c).unwrap();
    let u = |c: &str| row.try_get::<Option<Uuid>, _>(c).unwrap();
    assert_eq!(s("organization_name").as_deref(), Some("PT Dupes Org"), "null org filled from the dupe");
    assert_eq!(s("phone").as_deref(), Some("+62 818-999-1111"), "null phone filled from the dupe");
    assert_eq!(s("email").as_deref(), Some("dupe@mail.id"), "null email filled from the dupe");
    assert_eq!(s("notes").as_deref(), Some("master notes stay"), "master's non-null value kept");
    assert_eq!(u("campaign_id"), Some(campaign), "master's own assignment kept");
    assert_eq!(u("owner_user_id"), Some(owner));
    assert_eq!(u("sales_team_id"), Some(team));
    assert_eq!(u("party_id"), None, "party_id never filled");
    assert!(row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("converted_at").unwrap().is_none());
    for name in ["organizationName", "phone", "email"] {
        assert!(outcome.fields_filled.contains(&name), "fields_filled names {name}: {:?}", outcome.fields_filled);
    }
    assert!(!outcome.fields_filled.contains(&"notes"));
}

// ── IDM: idempotence ──────────────────────────────────────────────────────────

/// IDM-1: re-merging the same batch is a silent no-op — same absorbed ids, no re-stamp of
/// merged_at; IDM-2: an id belonging to ANOTHER master changes nothing and is reported;
/// IDM-3: pinning an already-absorbed lead redirects to its ultimate master.
#[tokio::test]
async fn merge_is_idempotent_and_redirects() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());

    // IDM-1: A masters B; re-running the identical pinned merge is a no-op.
    let a = capture(&svc, "Hadi", Some("+62 819-1"), None, None).await;
    let b = capture(&svc, "Hadi", Some("0819-1"), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 819-111-2222' WHERE id = ANY($1)")
        .bind(vec![a, b]).execute(&pool).await.unwrap();
    let first = svc.merge_leads(Some(a), vec![b]).await.unwrap();
    assert_eq!(first.absorbed_ids, vec![b]);
    let stamp: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT merged_at FROM lead.leads WHERE id=$1").bind(b).fetch_one(&pool).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let again = svc.merge_leads(Some(a), vec![b]).await.unwrap();
    assert_eq!(again.absorbed_ids, vec![b], "idempotent ids still listed");
    assert!(again.already_absorbed_elsewhere.is_empty());
    let stamp2: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT merged_at FROM lead.leads WHERE id=$1").bind(b).fetch_one(&pool).await.unwrap();
    assert_eq!(stamp, stamp2, "no re-absorb, no re-stamp");

    // IDM-2: C masters D; pinning C again while absorbing B (already A's dupe) reports B → A
    // WITHOUT changing anything.
    let c = capture(&svc, "Indra", Some("+62 820-1"), None, None).await;
    let d = capture(&svc, "Indra", Some("0820-1"), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 820-111-2222' WHERE id = ANY($1)")
        .bind(vec![c, d]).execute(&pool).await.unwrap();
    svc.merge_leads(Some(c), vec![d]).await.unwrap();
    let outcome = svc.merge_leads(Some(c), vec![b]).await.unwrap();
    assert_eq!(outcome.already_absorbed_elsewhere.len(), 1);
    assert_eq!(outcome.already_absorbed_elsewhere[0].id, b);
    assert_eq!(outcome.already_absorbed_elsewhere[0].master_id, a, "reported under its REAL master");
    assert!(outcome.absorbed_ids.is_empty(), "nothing new absorbed");
    assert_eq!(merged_into(&pool, b).await, Some(a), "B still points at A — no writes");

    // IDM-3: E was absorbed into F; pinning E afterwards redirects the merge to F.
    let e = capture(&svc, "Joko", Some("+62 821-1"), None, None).await;
    let f = capture(&svc, "Joko", Some("0821-1"), None, None).await;
    let g = capture(&svc, "Joko", Some("+62 821-2"), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 821-333-4444' WHERE id = ANY($1)")
        .bind(vec![e, f, g]).execute(&pool).await.unwrap();
    svc.merge_leads(Some(f), vec![e]).await.expect("F masters E");
    let redirected = svc.merge_leads(Some(e), vec![g]).await.unwrap();
    assert_eq!(redirected.master_id, f, "pinned an absorbed lead → redirected to its master");
    assert_eq!(redirected.redirected_from, Some(e));
    assert_eq!(redirected.absorbed_ids, vec![g]);
    assert_eq!(merged_into(&pool, g).await, Some(f));
}

// ── RF: refusals ──────────────────────────────────────────────────────────────

/// RF-1: naming a converted lead as an absorb target refuses the WHOLE request atomically —
/// no field fill, no absorb, not even for innocent members of the same batch.
#[tokio::test]
async fn converted_dupe_refusal_is_atomic() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());
    let a = capture(&svc, "Kiki", Some("+62 822-1"), None, None).await;
    let b = capture(&svc, "Kiki", Some("0822-1"), None, None).await;
    let c = capture(&svc, "Kiki", Some("+62 822-2"), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 822-555-6666' WHERE id = ANY($1)")
        .bind(vec![a, b, c]).execute(&pool).await.unwrap();
    set_party_anchor(&pool, b).await;

    let err = svc.merge_leads(Some(a), vec![b, c]).await.unwrap_err();
    assert!(matches!(err, LeadError::AbsorbConverted(id) if id == b));
    assert_eq!(err.http_status(), 422);
    assert_eq!(err.code(), "absorb_converted");
    // Atomic: NOTHING moved — c (innocent) and b (converted) untouched, master not filled.
    assert_eq!(merged_into(&pool, c).await, None);
    assert_eq!(merged_into(&pool, b).await, None);
    let notes: Option<String> = sqlx::query_scalar("SELECT notes FROM lead.leads WHERE id=$1")
        .bind(a).fetch_one(&pool).await.unwrap();
    assert!(notes.is_none(), "master field fill rolled back with the refusal");
}

/// RF-2: batch-shape refusals — self-absorb, empty absorb batch, oversized batches (pinned
/// absorbs cap at 5; an auto batch caps at 6 = master + 5 absorbs); an unknown id is a
/// not-found 404.
#[tokio::test]
async fn batch_shape_refusals() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());
    let a = capture(&svc, "Lia", Some("+62 823-1"), None, None).await;

    assert!(matches!(svc.merge_leads(Some(a), vec![a]).await.unwrap_err(), LeadError::AbsorbSelf));
    assert!(matches!(
        svc.merge_leads(Some(a), vec![]).await.unwrap_err(),
        LeadError::InvalidBatch(_)
    ));
    let six_absorbs: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();
    assert!(matches!(
        svc.merge_leads(Some(a), six_absorbs).await.unwrap_err(),
        LeadError::InvalidBatch(_)
    ));
    let seven_auto: Vec<Uuid> = (0..7).map(|_| Uuid::new_v4()).collect();
    assert!(matches!(
        svc.merge_leads(None, seven_auto).await.unwrap_err(),
        LeadError::InvalidBatch(_)
    ));
    // An unknown id must not resolve (the 404 shape; out-of-scope ids answer the same way
    // under a composing decorator's fence — probed host-side).
    let ghost = Uuid::new_v4();
    assert!(matches!(
        svc.merge_leads(Some(a), vec![ghost]).await.unwrap_err(),
        LeadError::NotFound(_)
    ));
    // Bad scan parameters are a typed 422.
    assert!(matches!(
        svc.duplicate_candidate_groups(1, 50).await.unwrap_err(),
        LeadError::Invalid(_)
    ));
}

/// A sink for RF-3: records every event, and PROVES post-commit ordering by reading the
/// absorbed rows from a brand-new connection at publish time — if the event fired before the
/// commit, the fresh connection would still see `merged_into_lead_id` NULL.
struct PostCommitCheckingSink {
    url: String,
    events: Mutex<Vec<LeadConversionEvent>>,
}
impl LeadEventSink for PostCommitCheckingSink {
    fn publish(&self, event: &LeadConversionEvent) {
        self.events.lock().unwrap().push(event.clone());
        let LeadConversionEvent::LeadMerged(m) = event else { return };
        let url = self.url.clone();
        let m = m.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                let pool = PgPool::connect(&url).await.expect("fresh connection in sink");
                for id in &m.absorbed_ids {
                    let into: Option<Uuid> =
                        sqlx::query_scalar("SELECT merged_into_lead_id FROM lead.leads WHERE id=$1")
                            .bind(id).fetch_one(&pool).await.unwrap();
                    assert_eq!(into, Some(m.lead_id), "event must observe the absorb already committed");
                }
            });
        })
        .join()
        .expect("sink check thread");
    }
}

/// RF-3: `LeadMerged` carries master + absorbed ids, and publishes only for merges
/// that actually absorbed something (an idempotent re-merge is silent).
#[tokio::test]
async fn merged_event_publishes_post_commit_only_on_real_absorbs() {
    let pool = pool().await;
    let sink = Arc::new(PostCommitCheckingSink {
        url: db_url(),
        events: Mutex::new(vec![]),
    });
    let svc = LeadWriteService::with_sink(pool.clone(), sink.clone());
    let a = capture(&svc, "Mira", Some("+62 824-1"), None, None).await;
    let b = capture(&svc, "Mira", Some("0824-1"), None, None).await;
    sqlx::query("UPDATE lead.leads SET phone=NULL, whatsapp_no='+62 824-111-2222' WHERE id = ANY($1)")
        .bind(vec![a, b]).execute(&pool).await.unwrap();

    svc.merge_leads(Some(a), vec![b]).await.unwrap();
    {
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1, "exactly one event for one real absorb");
        match &events[0] {
            LeadConversionEvent::LeadMerged(m) => {
                assert_eq!(m.lead_id, a);
                assert_eq!(m.absorbed_ids, vec![b]);
            }
            other => panic!("expected LeadMerged, got {other:?}"),
        }
    }
    // Idempotent re-merge: silent — no replayed event for consumers to re-point.
    svc.merge_leads(Some(a), vec![b]).await.unwrap();
    assert_eq!(sink.events.lock().unwrap().len(), 1, "no-op re-merge publishes nothing");
}

// ── MA: merge carries attribution ─────────────────────────────────────────────

/// MA-1: an un-attributed master inherits the dupe's UTM trio (attribution must survive the
/// merge, so the won roll-up still knows where the lead came from); MA-2: a master with its
/// own attribution keeps it — the master's non-null values win for the utm trio exactly as
/// for every other nullable lead-owned field.
#[tokio::test]
async fn merge_carries_attribution() {
    let pool = pool().await;
    let svc = LeadWriteService::new(pool.clone());

    // MA-1: bare master + attributed dupe.
    let master = capture(&svc, "Lina", Some("+62 823-1"), None, None).await;
    let dupe = capture_with_utm(&svc, "Lina dupe", "0823-1", Some(("google", "cpc", "spring_sale"))).await;
    let outcome = svc.merge_leads(Some(master), vec![dupe]).await.unwrap();
    let row = sqlx::query(
        r#"SELECT utm_source, utm_medium, utm_campaign FROM lead.leads WHERE id=$1"#,
    )
    .bind(master)
    .fetch_one(&pool)
    .await
    .unwrap();
    let s = |c: &str| row.try_get::<Option<String>, _>(c).unwrap();
    assert_eq!(s("utm_source").as_deref(), Some("google"), "master inherits the dupe's utm_source");
    assert_eq!(s("utm_medium").as_deref(), Some("cpc"), "master inherits the dupe's utm_medium");
    assert_eq!(s("utm_campaign").as_deref(), Some("spring_sale"), "master inherits the dupe's utm_campaign");
    for name in ["utmSource", "utmMedium", "utmCampaign"] {
        assert!(outcome.fields_filled.contains(&name), "fields_filled names {name}: {:?}", outcome.fields_filled);
    }

    // MA-2: attributed master + differently-attributed dupe — the master's own values win.
    let m2 = capture_with_utm(&svc, "Mira", "+62 824-1", Some(("newsletter", "email", "july_launch"))).await;
    let d2 = capture_with_utm(&svc, "Mira dupe", "0824-1", Some(("google", "cpc", "spring_sale"))).await;
    let outcome2 = svc.merge_leads(Some(m2), vec![d2]).await.unwrap();
    let row2 = sqlx::query(
        r#"SELECT utm_source, utm_medium, utm_campaign FROM lead.leads WHERE id=$1"#,
    )
    .bind(m2)
    .fetch_one(&pool)
    .await
    .unwrap();
    let s2 = |c: &str| row2.try_get::<Option<String>, _>(c).unwrap();
    assert_eq!(s2("utm_source").as_deref(), Some("newsletter"), "master's own utm_source kept");
    assert_eq!(s2("utm_medium").as_deref(), Some("email"), "master's own utm_medium kept");
    assert_eq!(s2("utm_campaign").as_deref(), Some("july_launch"), "master's own utm_campaign kept");
    assert!(!outcome2.fields_filled.iter().any(|f| f.starts_with("utm")), "no utm fill reported: {:?}", outcome2.fields_filled);
}
