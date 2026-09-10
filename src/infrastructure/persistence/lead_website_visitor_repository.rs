//! The lead ↔ website-visitor attribution spine (hand-written;
//! user-owned; see `metaphor.codegen.yaml`).
//!
//! The website-capture persistence leg: the insert that lands a
//! website-sourced lead, the strict normalized match against existing
//! leads, and the attribution link onto the website visitor. Every
//! function here takes the CALLER'S connection so the whole website
//! intake — match, lead insert, link — commits as ONE transaction
//! (the caller is the website module's intake engine; its savepoint
//! wraps all three statements). The caller has already bound the
//! composing service's org scope on the connection (RLS LAW): an
//! unscoped connection sees zero rows and writes nothing under a
//! decorator-installed fence.
//!
//! The website visitor id and website id are PLAIN UUIDS with no DB
//! foreign key — the cross-module promotion contract. backbone-website
//! keeps no lead-shaped edge and stays free to reclaim visitor rows
//! (the 60-day partnerless GC sweep, the erasure verb); this table
//! keeps the attribution, and reads over the visitor side tolerate
//! dangling ids by design.

use uuid::Uuid;

use super::lead_repository::{NewLeadRow, LEAD_INSERT_SQL};

/// What the strict capture match found: the closest existing live lead
/// for the submitted identity. The website intake does NOT auto-merge —
/// merging is an officer verb — it records the match loudly in its audit
/// detail so a repeat contact is visible at triage.
#[derive(Debug, Clone)]
pub struct WebsiteCaptureMatch {
    pub lead_id: Uuid,
    pub lead_name: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    /// Which normalized key matched ('email' or 'phone').
    pub matched_on: &'static str,
    /// True when the match keyed on email but the submitted phone
    /// normalizes differently than the stored lead's phone (both
    /// non-null). Upstream DROPPED the link silently in this case; the
    /// port records the divergence instead of discarding it.
    pub phone_divergence: bool,
}

/// Insert a website-captured lead on the caller's connection.
///
/// Same INSERT text as [`super::lead_repository::LeadRepository::insert_lead`]
/// (the shared `LEAD_INSERT_SQL` const keeps the two in lockstep — a
/// column added to one without the other fails to compile). The caller
/// has bound the composing service's org scope. The id is the caller's
/// (minted before the insert so the attribution link can reference it).
pub async fn insert_lead_on_conn(
    conn: &mut sqlx::PgConnection,
    l: &NewLeadRow<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(LEAD_INSERT_SQL)
        .bind(l.id)
        .bind(l.lead_name)
        .bind(l.organization_name)
        .bind(l.phone)
        .bind(l.whatsapp_no)
        .bind(l.email)
        .bind(l.source)
        .bind(l.campaign_id)
        .bind(l.notes)
        .bind(l.owner_user_id)
        .bind(l.sales_team_id)
        .bind(l.utm_source)
        .bind(l.utm_medium)
        .bind(l.utm_campaign)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The strict normalized identity match for a website capture.
///
/// Mirrors the stored generated keys (`email_key`, `phone_key`): the
/// submitted email compares trimmed+lowered; the submitted phone folds
/// to digits with the same Indonesia-country normalization (leading 0/8
/// → 62-prefixed). Only LIVE leads match (not soft-deleted, not
/// absorbed into a master). A phone-key match outranks an email-key
/// match (the stronger channel); ties break newest-first.
///
/// Divergence is DETECTED, never dropped: when the match keyed on email
/// and both phones are present but normalize differently, the outcome
/// carries `phone_divergence = true` — the caller records it in the
/// intake audit detail (upstream silently dropped the partner link in
/// exactly this arm; the port refuses the silence).
pub async fn find_capture_match_on_conn(
    conn: &mut sqlx::PgConnection,
    email: Option<&str>,
    phone: Option<&str>,
) -> Result<Option<WebsiteCaptureMatch>, sqlx::Error> {
    // The submitted phone, normalized through the SAME expression family
    // as the stored `phone_key` generated column. The digit extraction
    // repeats per branch because a generated-column-equivalent
    // expression cannot reference aliases.
    let phone_digits = |raw: &str| -> String {
        raw.chars().filter(|c| c.is_ascii_digit()).collect()
    };
    let phone_key = phone.map(|p| {
        let d = phone_digits(p.trim());
        if d.is_empty() {
            String::new()
        } else if d.starts_with("62") {
            d
        } else if let Some(stripped) = d.strip_prefix('0') {
            format!("62{stripped}")
        } else if d.starts_with('8') {
            format!("62{d}")
        } else {
            d
        }
    });
    let email_key = email.map(|e| e.trim().to_ascii_lowercase());

    let row: Option<(Uuid, String, Option<String>, Option<String>, String, bool)> =
        sqlx::query_as(
            r#"
            WITH q AS (
                SELECT
                    NULLIF(btrim(lower($1::text)), '') AS email_key,
                    NULLIF($2::text, '')              AS phone_key
            )
            SELECT l.id, l.lead_name, l.email, l.phone,
                   CASE WHEN q.phone_key IS NOT NULL
                             AND l.phone_key = q.phone_key THEN 'phone' ELSE 'email' END,
                   q.phone_key IS NOT NULL
                       AND l.phone_key IS NOT NULL
                       AND l.phone_key <> q.phone_key
              FROM lead.leads l, q
             WHERE (l.metadata->>'deleted_at') IS NULL
               AND l.merged_into_lead_id IS NULL
               AND (
                     (q.email_key  IS NOT NULL AND l.email_key  = q.email_key)
                  OR (q.phone_key  IS NOT NULL AND l.phone_key  = q.phone_key)
               )
             ORDER BY CASE WHEN q.phone_key IS NOT NULL
                                AND l.phone_key = q.phone_key THEN 0 ELSE 1 END,
                      l.metadata->>'created_at' DESC, l.id
             LIMIT 1
            "#,
        )
        .bind(email_key.unwrap_or_default())
        .bind(phone_key.filter(|k| !k.is_empty()).unwrap_or_default())
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(lead_id, lead_name, email, phone, matched_on, phone_divergence)| {
        WebsiteCaptureMatch {
            lead_id,
            lead_name,
            email,
            phone,
            matched_on: if matched_on == "phone" { "phone" } else { "email" },
            phone_divergence,
        }
    }))
}

/// Link a captured lead to the website visitor that submitted it.
///
/// Idempotent: the unique (lead_id, website_visitor_id) pair swallows a
/// re-link (`ON CONFLICT DO NOTHING`). The link never fences the
/// visitor's lifecycle — website's GC and erasure stay free to reclaim
/// the visitor row; this row is the durable attribution record.
pub async fn link_website_visitor_on_conn(
    conn: &mut sqlx::PgConnection,
    lead_id: Uuid,
    website_id: Uuid,
    website_visitor_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO lead.lead_website_visitors
            (id, lead_id, website_id, website_visitor_id, linked_at)
        VALUES (gen_random_uuid(), $1, $2, $3, now())
        ON CONFLICT (lead_id, website_visitor_id) DO NOTHING
        "#,
    )
    .bind(lead_id)
    .bind(website_id)
    .bind(website_visitor_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The reverse read: every live lead a website visitor produced,
/// newest-first. Dangling-safe by construction — it starts from the
/// link rows, so a reclaimed visitor id still answers its attributed
/// leads (the lead rows are the record of truth).
pub async fn leads_for_visitor_on_conn(
    conn: &mut sqlx::PgConnection,
    website_id: Uuid,
    website_visitor_id: Uuid,
    limit: i64,
) -> Result<Vec<(Uuid, String)>, sqlx::Error> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        r#"
        SELECT l.id, l.lead_name
          FROM lead.lead_website_visitors v
          JOIN lead.leads l ON l.id = v.lead_id
         WHERE v.website_id = $1
           AND v.website_visitor_id = $2
           AND (l.metadata->>'deleted_at') IS NULL
         ORDER BY v.linked_at DESC, l.id
         LIMIT $3
        "#,
    )
    .bind(website_id)
    .bind(website_visitor_id)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows)
}
