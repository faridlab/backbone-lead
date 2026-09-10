//! Behavior tests for the website-capture attribution spine (the lead ↔
//! website-visitor M2M and its capture leg).
//!
//! DB-backed, against real Postgres (`lead` schema), same family
//! convention as `lead_merge_cases.rs`: a scratch database
//! pre-migrated with the module's migrations, named via DATABASE_URL.
//! Covers: the connection-taking capture insert, the strict normalized
//! identity match (email/phone precedence, divergence detection, the
//! live-only filter), the idempotent visitor link, and the reverse read.

use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use backbone_lead::infrastructure::persistence::{
    find_capture_match_on_conn, insert_lead_on_conn, leads_for_visitor_on_conn,
    link_website_visitor_on_conn, NewLeadRow,
};

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@127.0.0.1:5433/lead_website_capture_test".into())
}

/// A run-unique 9-digit phone body. The capture match reads the whole `lead` schema (the
/// tenant fence is the composing service's decorator, absent here), so the match test
/// derives its contact values from this instead of fixed numbers — fixed values would
/// collide with rows left by a previous run on the same scratch database. A per-process
/// counter guarantees distinct values even when consecutive calls land on the same clock
/// tick; the clock keeps values distinct across runs and processes.
fn unique_phone_body() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let step = SEQ.fetch_add(1, Ordering::Relaxed) as u64;
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let tick = (d.as_secs() << 20) ^ (d.subsec_nanos() as u64);
    format!("9{:08}", tick.wrapping_add(step.wrapping_mul(0x9E3779B1)) % 100_000_000)
}

async fn pool() -> PgPool {
    PgPool::connect(&db_url()).await.expect("connect DB")
}

/// Begin a transaction (the intake engine's persist contract: one
/// transaction owned by the caller — its savepoint wraps all three
/// statements). Under a composing service's decorator the caller also
/// relays the org scope onto it; undecorated, this is a plain transaction.
async fn tx(pool: &PgPool) -> sqlx::Transaction<'_, sqlx::Postgres> {
    pool.begin().await.expect("begin tx")
}

/// The website-capture insert leg: the same INSERT the guarded capture
/// verb runs, on the caller's connection.
async fn capture_on_conn(
    conn: &mut PgConnection,
    name: &str,
    email: Option<&str>,
    phone: Option<&str>,
) -> Uuid {
    let id = Uuid::new_v4();
    insert_lead_on_conn(
        conn,
        &NewLeadRow {
            id,
            lead_name: name,
            organization_name: None,
            phone,
            whatsapp_no: None,
            email,
            source: "website",
            campaign_id: None,
            notes: None,
            owner_user_id: None,
            sales_team_id: None,
            utm_source: Some("newsletter"),
            utm_medium: Some("email"),
            utm_campaign: None,
        },
    )
    .await
    .expect("website capture insert");
    id
}

// ── 1: the link is idempotent and genuinely many-to-many ─────────────────────

/// One lead links two visitors; a repeated link is a no-op; the reverse
/// read answers the visitor's leads newest-first.
#[tokio::test]
async fn link_idempotence_and_reverse_read() {
    let pool = pool().await;
    let website = Uuid::new_v4();
    let visitor_a = Uuid::new_v4();
    let visitor_b = Uuid::new_v4();

    let mut txc = tx(&pool).await;
    let lead = capture_on_conn(&mut *txc, "Spine One", Some("spine1@example.com"), None).await;
    link_website_visitor_on_conn(&mut *txc, lead, website, visitor_a)
        .await
        .expect("first link");
    // Re-link the SAME pair: the unique (lead, visitor) pair swallows it.
    link_website_visitor_on_conn(&mut *txc, lead, website, visitor_a)
        .await
        .expect("re-link");
    // A second visitor on the same lead: the M2M arm.
    link_website_visitor_on_conn(&mut *txc, lead, website, visitor_b)
        .await
        .expect("second visitor link");
    txc.commit().await.expect("commit");

    let links: i64 = sqlx::query_scalar("SELECT count(*) FROM lead.lead_website_visitors WHERE lead_id = $1")
        .bind(lead)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(links, 2, "re-link is idempotent; two distinct visitors stay two rows");

    // The reverse read: visitor A's leads include the captured lead.
    let mut txc = tx(&pool).await;
    let leads = leads_for_visitor_on_conn(&mut *txc, website, visitor_a, 10)
        .await
        .expect("reverse read");
    assert_eq!(leads.len(), 1, "one lead for visitor A");
    assert_eq!(leads[0].0, lead);
    // The attribution survives a soft-deleted lead being excluded.
    sqlx::query("UPDATE lead.leads SET metadata = jsonb_set(metadata, '{deleted_at}', to_jsonb(now()::text)) WHERE id = $1")
        .bind(lead)
        .execute(&pool)
        .await
        .unwrap();
    let mut txc = tx(&pool).await;
    let leads = leads_for_visitor_on_conn(&mut *txc, website, visitor_a, 10)
        .await
        .expect("reverse read after soft delete");
    assert!(leads.is_empty(), "the reverse read filters soft-deleted leads");
}

// ── 2: the strict normalized identity match ──────────────────────────────────

/// Email matches after trim+lower; phone matches after digit-fold with
/// the Indonesia normalization; a phone-key match outranks email; the
/// divergence arm is DETECTED (email matches, phones differ) instead of
/// silently dropped; absorbed leads never match.
#[tokio::test]
async fn strict_capture_match_semantics() {
    let pool = pool().await;

    // Run-unique identities: the match scan is database-wide, so fixed values would
    // collide with rows left by a previous run of this suite on the same database.
    let email = format!("match-{}@Example.COM", unique_phone_body());
    let email_folded = email.to_lowercase();
    let stored_body = unique_phone_body();
    let stored_phone = format!("+62 {} {} {}", &stored_body[0..3], &stored_body[3..6], &stored_body[6..]);
    let diverging_body = unique_phone_body();
    let diverging_phone = format!("+62 {} {} {}", &diverging_body[0..3], &diverging_body[3..6], &diverging_body[6..]);
    let phone_body = unique_phone_body();
    let phone_stored = format!("0{}", phone_body);
    let phone_query = format!("+62 {} {} {}", &phone_body[0..3], &phone_body[3..6], &phone_body[6..]);
    let absorbed_email = format!("absorbed-{}@example.com", unique_phone_body());

    let mut txc = tx(&pool).await;
    let email_lead = capture_on_conn(&mut *txc, "Match Email", Some(&format!("  {email} ")), Some(&stored_phone)).await;
    let phone_lead = capture_on_conn(&mut *txc, "Match Phone", Some("other@example.com"), Some(&phone_stored)).await;
    let absorbed = capture_on_conn(&mut *txc, "Absorbed", Some(&absorbed_email), None).await;
    txc.commit().await.expect("commit");

    // Absorb one lead the way the merge verbs do.
    sqlx::query("UPDATE lead.leads SET merged_into_lead_id = $2 WHERE id = $1")
        .bind(absorbed)
        .bind(email_lead)
        .execute(&pool)
        .await
        .unwrap();

    // Exact email (differently cased/space-padded): matches, and the
    // phone diverges from the stored one — detected, not dropped.
    let mut txc = tx(&pool).await;
    let m = find_capture_match_on_conn(&mut *txc, Some(&email_folded), Some(&diverging_phone))
        .await
        .expect("email match query")
        .expect("email match found");
    assert_eq!(m.lead_id, email_lead, "the email-keyed match wins when the phone differs");
    assert_eq!(m.matched_on, "email");
    assert!(m.phone_divergence, "the divergent phone is DETECTED, never silently dropped");

    // Phone-only match: the '+62 ddd ddd ddd' query digit-folds onto the stored
    // domestic '0dd ddd ddd' key (both 62-fold to the same digits) and outranks
    // the email non-match.
    let m = find_capture_match_on_conn(&mut *txc, Some("nobody@example.com"), Some(&phone_query))
        .await
        .expect("phone match query")
        .expect("phone match found");
    assert_eq!(m.lead_id, phone_lead);
    assert_eq!(m.matched_on, "phone");
    assert!(!m.phone_divergence, "no email match, no divergence arm");

    // The absorbed lead never matches — even by its own live email.
    let m = find_capture_match_on_conn(&mut *txc, Some(&absorbed_email), None)
        .await
        .expect("absorbed query");
    assert!(m.is_none(), "absorbed leads are excluded from the capture match");

    // No keys at all: no match.
    let m = find_capture_match_on_conn(&mut *txc, None, None)
        .await
        .expect("keyless query");
    assert!(m.is_none());
    txc.commit().await.expect("commit");
}

// ── 3: dangling visitor references are the accepted posture ──────────────────

/// The website visitor id is a plain uuid with no cross-schema FK: a
/// link to a visitor id that has no website row (or after website's GC
/// reclaimed the row) still records attribution, and the reverse read
/// still answers. The lead row is the record of truth, never the
/// visitor row.
#[tokio::test]
async fn dangling_visitor_reference_is_tolerated() {
    let pool = pool().await;
    let website = Uuid::new_v4();
    let ghost_visitor = Uuid::new_v4();

    let mut txc = tx(&pool).await;
    let lead = capture_on_conn(&mut *txc, "Ghost Visitor", Some("ghost@example.com"), None).await;
    link_website_visitor_on_conn(&mut *txc, lead, website, ghost_visitor)
        .await
        .expect("link to a visitor id with no website row");
    txc.commit().await.expect("commit");

    let mut txc = tx(&pool).await;
    let leads = leads_for_visitor_on_conn(&mut *txc, website, ghost_visitor, 10)
        .await
        .expect("reverse read for the dangling visitor");
    assert_eq!(leads.len(), 1, "attribution survives the visitor row (no FK, by the promotion contract)");

    // The lead-side columns carry everything the attribution needs.
    let row = sqlx::query("SELECT source::text, utm_source FROM lead.leads WHERE id = $1")
        .bind(lead)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("source"), "website");
    assert_eq!(row.get::<Option<String>, _>("utm_source").as_deref(), Some("newsletter"));
}
