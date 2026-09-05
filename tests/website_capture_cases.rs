//! Behavior tests for the website-capture attribution spine (the lead ↔
//! website-visitor M2M and its capture leg).
//!
//! DB-backed, against real Postgres (`lead` schema), same family
//! convention as `lead_merge_cases.rs`: a scratch database
//! pre-migrated with the module's migrations, named via DATABASE_URL.
//! Covers: the connection-taking capture insert, the strict normalized
//! identity match (email/phone precedence, divergence detection, the
//! live-only filter, the company fence), the idempotent visitor link,
//! the reverse read, and the RLS fence over the junction table.

use sqlx::{Acquire, PgConnection, PgPool, Row};
use uuid::Uuid;

use backbone_lead::infrastructure::persistence::{
    find_capture_match_on_conn, insert_lead_on_conn, leads_for_visitor_on_conn,
    link_website_visitor_on_conn, NewLeadRow,
};

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://serpa:serpa_dev_password@127.0.0.1:5432/lead_website_capture_test".into())
}

async fn pool() -> PgPool {
    PgPool::connect(&db_url()).await.expect("connect DB")
}

/// Begin a transaction with the company scope bound (the intake
/// engine's persist contract: one tx, scope bound by the caller).
async fn scoped_tx(pool: &PgPool, company: Uuid) -> sqlx::Transaction<'_, sqlx::Postgres> {
    let mut tx = pool.begin().await.expect("begin tx");
    sqlx::query("SELECT set_config('app.company_id', $1, true)")
        .bind(company.to_string())
        .execute(&mut *tx)
        .await
        .expect("bind company scope");
    tx
}

/// The website-capture insert leg: the same INSERT the guarded capture
/// verb runs, on the caller's connection.
async fn capture_on_conn(
    tx: &mut PgConnection,
    company: Uuid,
    name: &str,
    email: Option<&str>,
    phone: Option<&str>,
) -> Uuid {
    let id = Uuid::new_v4();
    insert_lead_on_conn(
        tx,
        &NewLeadRow {
            id,
            company_id: company,
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
    let company = Uuid::new_v4();
    let website = Uuid::new_v4();
    let visitor_a = Uuid::new_v4();
    let visitor_b = Uuid::new_v4();

    let mut tx = scoped_tx(&pool, company).await;
    let lead = capture_on_conn(&mut *tx, company, "Spine One", Some("spine1@example.com"), None).await;
    link_website_visitor_on_conn(&mut *tx, company, lead, website, visitor_a)
        .await
        .expect("first link");
    // Re-link the SAME pair: the unique (lead, visitor) pair swallows it.
    link_website_visitor_on_conn(&mut *tx, company, lead, website, visitor_a)
        .await
        .expect("re-link");
    // A second visitor on the same lead: the M2M arm.
    link_website_visitor_on_conn(&mut *tx, company, lead, website, visitor_b)
        .await
        .expect("second visitor link");
    tx.commit().await.expect("commit");

    let links: i64 = sqlx::query_scalar("SELECT count(*) FROM lead.lead_website_visitors WHERE lead_id = $1")
        .bind(lead)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(links, 2, "re-link is idempotent; two distinct visitors stay two rows");

    // The reverse read: visitor A's leads include the captured lead.
    let mut tx = scoped_tx(&pool, company).await;
    let leads = leads_for_visitor_on_conn(&mut *tx, company, website, visitor_a, 10)
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
    let mut tx = scoped_tx(&pool, company).await;
    let leads = leads_for_visitor_on_conn(&mut *tx, company, website, visitor_a, 10)
        .await
        .expect("reverse read after soft delete");
    assert!(leads.is_empty(), "the reverse read filters soft-deleted leads");
}

// ── 2: the strict normalized identity match ──────────────────────────────────

/// Email matches after trim+lower; phone matches after digit-fold with
/// the Indonesia normalization; a phone-key match outranks email; the
/// divergence arm is DETECTED (email matches, phones differ) instead of
/// silently dropped; absorbed and soft-deleted leads never match;
/// another company's lead never matches.
#[tokio::test]
async fn strict_capture_match_semantics() {
    let pool = pool().await;
    let company = Uuid::new_v4();
    let other = Uuid::new_v4();

    let mut tx = scoped_tx(&pool, company).await;
    let email_lead = capture_on_conn(&mut *tx, company, "Match Email", Some("  Match@Example.COM "), Some("+62 811-1111-1111")).await;
    let phone_lead = capture_on_conn(&mut *tx, company, "Match Phone", Some("other@example.com"), Some("0812-2222-2222")).await;
    let absorbed = capture_on_conn(&mut *tx, company, "Absorbed", Some("absorbed@example.com"), None).await;
    tx.commit().await.expect("commit");

    // Absorb one lead the way the merge verbs do.
    sqlx::query("UPDATE lead.leads SET merged_into_lead_id = $2 WHERE id = $1")
        .bind(absorbed)
        .bind(email_lead)
        .execute(&pool)
        .await
        .unwrap();

    // Another company's lead with the SAME email: the fence keeps it out.
    let mut tx = scoped_tx(&pool, other).await;
    capture_on_conn(&mut *tx, other, "Other Tenant", Some("match@example.com"), None).await;
    tx.commit().await.expect("commit");

    // Exact email (differently cased/space-padded): matches, and the
    // phone diverges (62811... vs 62822...) — detected, not dropped.
    let mut tx = scoped_tx(&pool, company).await;
    let m = find_capture_match_on_conn(&mut *tx, company, Some("match@example.com"), Some("+62 822-3333-4444"))
        .await
        .expect("email match query")
        .expect("email match found");
    assert_eq!(m.lead_id, email_lead, "the email-keyed match wins when the phone differs");
    assert_eq!(m.matched_on, "email");
    assert!(m.phone_divergence, "the divergent phone is DETECTED, never silently dropped");

    // Phone-only match: '+62 812 2222 2222' digit-folds onto the stored
    // '0812-2222-2222' key (both 628122222222 after the 62-prefix fold)
    // and outranks the email non-match.
    let m = find_capture_match_on_conn(&mut *tx, company, Some("nobody@example.com"), Some("+62 812 2222 2222"))
        .await
        .expect("phone match query")
        .expect("phone match found");
    assert_eq!(m.lead_id, phone_lead);
    assert_eq!(m.matched_on, "phone");
    assert!(!m.phone_divergence, "no email match, no divergence arm");

    // The absorbed lead never matches.
    let m = find_capture_match_on_conn(&mut *tx, company, Some("absorbed@example.com"), None)
        .await
        .expect("absorbed query");
    assert!(m.is_none(), "absorbed leads are excluded from the capture match");

    // No keys at all: no match.
    let m = find_capture_match_on_conn(&mut *tx, company, None, None)
        .await
        .expect("keyless query");
    assert!(m.is_none());
    tx.commit().await.expect("commit");
}

// ── 3: the RLS fence over the junction ────────────────────────────────────────

/// Walks the fence as a dedicated non-superuser role (superusers bypass
/// RLS even under FORCE): unbound sees zero link rows; a cross-company
/// link insert is refused by WITH CHECK; bound to A, only A's links are
/// visible and the reverse read stays inside the fence.
#[tokio::test]
async fn rls_fence_over_the_spine() {
    let pool = pool().await;
    let a_co = Uuid::new_v4();
    let b_co = Uuid::new_v4();
    let website = Uuid::new_v4();
    let visitor = Uuid::new_v4();

    let mut tx = scoped_tx(&pool, a_co).await;
    let lead = capture_on_conn(&mut *tx, a_co, "Fence A", Some("fence-a@example.com"), None).await;
    link_website_visitor_on_conn(&mut *tx, a_co, lead, website, visitor)
        .await
        .expect("A link");
    tx.commit().await.expect("commit");

    sqlx::query(
        r#"DO $$ BEGIN
               IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lead_probe_rls') THEN
                   CREATE ROLE lead_probe_rls NOLOGIN;
               END IF;
           END $$"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("GRANT USAGE ON SCHEMA lead TO lead_probe_rls").execute(&pool).await.unwrap();
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA lead TO lead_probe_rls")
        .execute(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("SET ROLE lead_probe_rls").execute(&mut *conn).await.unwrap();

    // Unbound: zero rows on the junction.
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM lead.lead_website_visitors")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(total, 0, "unbound role sees no attribution rows");

    // Bound to B, a link for A's lead is refused by WITH CHECK (the
    // company on the row must equal the bound company).
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.company_id', $1, true)").bind(b_co.to_string()).execute(&mut *tx).await.unwrap();
    let refused = sqlx::query(
        r#"INSERT INTO lead.lead_website_visitors
               (company_id, lead_id, website_id, website_visitor_id)
           VALUES ($1, $2, $3, $4)"#,
    )
    .bind(b_co)
    .bind(lead)
    .bind(website)
    .bind(visitor)
    .execute(&mut *tx)
    .await;
    assert!(refused.is_err(), "WITH CHECK rejects the cross-company attribution write");
    drop(tx);

    // Bound to A: exactly A's one link is visible.
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.company_id', $1, true)").bind(a_co.to_string()).execute(&mut *tx).await.unwrap();
    let seen: i64 = sqlx::query_scalar("SELECT count(*) FROM lead.lead_website_visitors")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(seen, 1, "bound to A: exactly A's attribution rows");
    let leads = leads_for_visitor_on_conn(&mut *tx, a_co, website, visitor, 10)
        .await
        .expect("reverse read under role");
    assert_eq!(leads.len(), 1, "the reverse read stays inside the fence");
    tx.commit().await.expect("commit");
}

// ── 4: dangling visitor references are the accepted posture ──────────────────

/// The website visitor id is a plain uuid with no cross-schema FK: a
/// link to a visitor id that has no website row (or after website's GC
/// reclaimed the row) still records attribution, and the reverse read
/// still answers. The lead row is the record of truth, never the
/// visitor row.
#[tokio::test]
async fn dangling_visitor_reference_is_tolerated() {
    let pool = pool().await;
    let company = Uuid::new_v4();
    let website = Uuid::new_v4();
    let ghost_visitor = Uuid::new_v4();

    let mut tx = scoped_tx(&pool, company).await;
    let lead = capture_on_conn(&mut *tx, company, "Ghost Visitor", Some("ghost@example.com"), None).await;
    link_website_visitor_on_conn(&mut *tx, company, lead, website, ghost_visitor)
        .await
        .expect("link to a visitor id with no website row");
    tx.commit().await.expect("commit");

    let mut tx = scoped_tx(&pool, company).await;
    let leads = leads_for_visitor_on_conn(&mut *tx, company, website, ghost_visitor, 10)
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
