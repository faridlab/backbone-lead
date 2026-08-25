//! HTTP-surface tests for the guarded lead capture verb and its read surfaces
//! (complements `lead_merge_routes_test.rs`, which owns the merge/dedup verbs).
//!
//! Runs the real router in-process via `tower::ServiceExt::oneshot` against live Postgres, with
//! HS256 company tokens forged the way the family's guard tests do.
//!
//! C-1  capture source vocabulary: an invalid `source` value answers the module's typed 422
//!      (error shape + message naming the LeadSource variants), never a 500. This is the
//!      regression probe for the class where a free-string source reached the DB cast and the
//!      enum bind failure surfaced as an internal error.
//! C-2  a valid explicit source is stored as sent; an omitted source defaults to whatsapp.
//! C-3  UTM attribution rides capture and is surfaced on the generated read surface
//!      (GET /leads/:id, camelCase in the response envelope).
//! C-4  UTM attribution is surfaced in the funnel read (duplicate-candidate members), so an
//!      operator reviewing dupes sees where each lead came from before merging.
//! C-5  sweep: the OTHER source-accepting surface — the generated generic write routes
//!      (create/patch, typed DTOs) — also refuses an unknown source at the extractor
//!      (typed rejection naming the variants), never a 500.
//!
//! Requires DATABASE_URL (no silent default — a default host could point at a live dev
//! database, and these tests write).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use backbone_auth::company::CompanyVerifier;
use backbone_lead::presentation::http::create_guarded_lead_routes;
use backbone_lead::LeadModule;

const SECRET: &[u8] = b"lead-capture-routes-test-secret";

#[derive(Serialize)]
struct TestClaims {
    sub: String,
    exp: usize,
    company_id: Option<Uuid>,
}

/// Mint an HS256 token carrying the tenant the write must be scoped to.
fn token(company_id: Uuid) -> String {
    let claims = TestClaims {
        sub: "agent-1".into(),
        exp: 9_999_999_999,
        company_id: Some(company_id),
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must point at a scratch database with the lead migrations applied");
    PgPool::connect(&url).await.expect("connect DB")
}

async fn app() -> axum::Router {
    let pool = pool().await;
    let module = LeadModule::builder()
        .with_database(pool.clone())
        .build()
        .unwrap();
    create_guarded_lead_routes(&module, pool, CompanyVerifier::hs256(SECRET))
}

fn req(method: &str, uri: &str, body: Option<Value>, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = bearer {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    b.body(Body::from(body.map(|v| v.to_string()).unwrap_or_default()))
        .unwrap()
}

async fn send(router: axum::Router, r: Request<Body>) -> (StatusCode, Value) {
    let resp = router.oneshot(r).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// The full LeadSource vocabulary, spelled once — the probe asserts the 422 message against it.
const LEAD_SOURCE_VARIANTS: [&str; 6] = [
    "whatsapp",
    "instagram",
    "referral",
    "website",
    "walk_in",
    "other",
];

// ── C-1: invalid source answers the typed 422, never a 500 ────────────────────

#[tokio::test]
async fn c1_invalid_source_answers_typed_422_not_500() {
    let router = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(company);

    // Every non-variant value refuses with the module's typed 422: casing garbage, a value
    // from a neighboring vocabulary (lead_status), an empty string.
    for bad in ["twitter", "new", ""] {
        let (status, body) = send(
            router.clone(),
            req(
                "POST",
                "/leads",
                Some(json!({ "leadName": "C1 bad source", "phone": "+62 811-000-0001", "source": bad })),
                Some(bearer),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "source '{bad}' must be a typed 422, got {status}: {body}"
        );
        assert_eq!(
            body["error"],
            json!("invalid_input"),
            "module error shape for source '{bad}': {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("source"),
            "message must name the field: {message}"
        );
        for variant in LEAD_SOURCE_VARIANTS {
            assert!(
                message.contains(variant),
                "message must name variant '{variant}': {message}"
            );
        }
        assert_ne!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Nothing was written by the refused requests.
    let pool = pool().await;
    let stored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lead.leads WHERE company_id=$1 AND lead_name='C1 bad source'",
    )
    .bind(company)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored, 0, "a refused capture must not write");
}

// ── C-2: valid sources stored as sent; omitted source defaults ────────────────

#[tokio::test]
async fn c2_source_stored_as_sent_and_defaults_when_omitted() {
    let router = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(company);

    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({ "leadName": "C2 referral", "phone": "+62 811-000-0002", "source": "referral" })),
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let explicit: Uuid = serde_json::from_value(body["id"].clone()).unwrap();

    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({ "leadName": "C2 default", "phone": "+62 811-000-0003" })),
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let defaulted: Uuid = serde_json::from_value(body["id"].clone()).unwrap();

    let pool = pool().await;
    let explicit_stored: String =
        sqlx::query_scalar("SELECT source::text FROM lead.leads WHERE id=$1")
            .bind(explicit)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(explicit_stored, "referral");
    let default_stored: String =
        sqlx::query_scalar("SELECT source::text FROM lead.leads WHERE id=$1")
            .bind(defaulted)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(default_stored, "whatsapp");
}

// ── C-3: UTM rides capture and surfaces on the read surface ───────────────────

#[tokio::test]
async fn c3_utm_rides_capture_and_the_read_surface() {
    let router = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(company);

    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({
                "leadName": "C3 attributed",
                "phone": "+62 811-000-0004",
                "source": "website",
                "utmSource": "google",
                "utmMedium": "cpc",
                "utmCampaign": "spring_sale"
            })),
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let attributed: Uuid = serde_json::from_value(body["id"].clone()).unwrap();

    // The generated read surface (GET /leads/:id) surfaces the stored attribution.
    let (status, body) = send(
        router.clone(),
        req("GET", &format!("/leads/{attributed}"), None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        data["utmSource"],
        json!("google"),
        "utmSource must ride the read: {data}"
    );
    assert_eq!(
        data["utmMedium"],
        json!("cpc"),
        "utmMedium must ride the read: {data}"
    );
    assert_eq!(
        data["utmCampaign"],
        json!("spring_sale"),
        "utmCampaign must ride the read: {data}"
    );
    assert_eq!(data["source"], json!("website"));

    // A capture without attribution reads back null (the envelope serializes nullable fields
    // as explicit nulls, exactly like campaignId/notes above).
    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({ "leadName": "C3 bare", "phone": "+62 811-000-0005" })),
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let bare: Uuid = serde_json::from_value(body["id"].clone()).unwrap();
    let (status, body) = send(
        router.clone(),
        req("GET", &format!("/leads/{bare}"), None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"]["utmSource"],
        Value::Null,
        "un-attributed lead reads null utm: {body}"
    );
    assert_eq!(body["data"]["utmMedium"], Value::Null);
    assert_eq!(body["data"]["utmCampaign"], Value::Null);
}

// ── C-4: UTM surfaces in the funnel read (duplicate-candidate members) ────────

#[tokio::test]
async fn c4_utm_surfaces_in_the_funnel_read() {
    let router = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(company);

    // Two captures sharing a phone key; only one carries attribution.
    let mut attributed_body = json!({ "leadName": "C4 Andi", "phone": "+62 812-700-0001" });
    attributed_body["utmSource"] = json!("newsletter");
    attributed_body["utmMedium"] = json!("email");
    attributed_body["utmCampaign"] = json!("july_launch");
    let (status, body) = send(
        router.clone(),
        req("POST", "/leads", Some(attributed_body), Some(bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({ "leadName": "C4 Andi dupe", "phone": "0812-700-0001" })),
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = send(
        router.clone(),
        req(
            "GET",
            "/leads/duplicates-candidates?min_group_size=2&limit=50",
            None,
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let groups = body["groups"].as_array().expect("groups array");
    let group = groups
        .iter()
        .find(|g| {
            g["matchReason"]["keyKind"] == "phone" && g["matchReason"]["keyValue"] == "628127000001"
        })
        .expect("phone group for the captured pair");
    let members = group["members"].as_array().unwrap();
    assert_eq!(members.len(), 2);
    let attributed_member = members
        .iter()
        .find(|m| m["leadName"] == "C4 Andi")
        .expect("attributed member");
    assert_eq!(
        attributed_member["utmSource"],
        json!("newsletter"),
        "funnel read carries utmSource: {attributed_member}"
    );
    assert_eq!(attributed_member["utmMedium"], json!("email"));
    assert_eq!(attributed_member["utmCampaign"], json!("july_launch"));
    let bare_member = members
        .iter()
        .find(|m| m["leadName"] == "C4 Andi dupe")
        .expect("bare member");
    assert!(
        bare_member
            .get("utmSource")
            .map(Value::is_null)
            .unwrap_or(false),
        "un-attributed member carries null utm: {bare_member}"
    );
}

// ── C-5: the generated generic write surface refuses unknown sources too ─────

/// Send and return (status, raw text) — the extractor rejections are plain text, not JSON.
async fn send_raw(router: axum::Router, r: Request<Body>) -> (StatusCode, String) {
    let resp = router.oneshot(r).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn c5_generic_write_surface_refuses_unknown_source() {
    // The unguarded full-CRUD composition (what a host mounts for trusted/admin contexts).
    // Its DTOs type `source` as the enum, so serde rejects an unknown value at the extractor.
    let db = pool().await;
    let module = LeadModule::builder()
        .with_database(db.clone())
        .build()
        .unwrap();
    let router = module.all_crud_routes();

    let (status, text) = send_raw(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({
                "companyId": Uuid::new_v4(),
                "leadName": "C5 generic",
                "phone": "+62 811-000-0006",
                "source": "tiktok",
                "status": "new"
            })),
            None,
        ),
    )
    .await;
    assert!(
        status.is_client_error(),
        "generic create with unknown source must 4xx, got {status}: {text}"
    );
    assert_ne!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        text.contains("tiktok"),
        "rejection names the offending value: {text}"
    );
    assert!(
        LEAD_SOURCE_VARIANTS.iter().any(|v| text.contains(v)),
        "rejection names the vocabulary: {text}"
    );

    let (status, text) = send_raw(
        router.clone(),
        req(
            "PATCH",
            &format!("/leads/{}", Uuid::new_v4()),
            Some(json!({ "source": "tiktok" })),
            None,
        ),
    )
    .await;
    assert!(
        status.is_client_error(),
        "generic patch with unknown source must 4xx, got {status}: {text}"
    );
    assert_ne!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // The generic PATCH is loosely typed (a field map, not a typed DTO), so the interesting
    // case is an EXISTING row: the patch must still refuse the unknown source client-side,
    // never reach the DB enum cast, and never answer a 500.
    let guarded = app().await;
    let company = Uuid::new_v4();
    let (status, body) = send(
        guarded,
        req(
            "POST",
            "/leads",
            Some(json!({ "leadName": "C5 patch target", "phone": "+62 811-000-0007" })),
            Some(&token(company)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let target: Uuid = serde_json::from_value(body["id"].clone()).unwrap();
    let (status, text) = send_raw(
        router.clone(),
        req(
            "PATCH",
            &format!("/leads/{target}"),
            Some(json!({ "source": "tiktok" })),
            None,
        ),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "loosely-typed patch must never 500: {status} {text}"
    );
    assert!(
        status.is_client_error(),
        "generic patch of a live row with unknown source must 4xx, got {status}: {text}"
    );
    assert!(
        text.contains("tiktok") || LEAD_SOURCE_VARIANTS.iter().any(|v| text.contains(v)),
        "rejection is specific to the source value: {text}"
    );

    // The bulk PATCH shares the same loosely-typed field-map path — same refusal, never a 500.
    let (status, text) = send_raw(
        router.clone(),
        req(
            "PATCH",
            "/leads/bulk",
            Some(json!({ "ids": [target], "patch": { "source": "tiktok" } })),
            None,
        ),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "bulk patch must never 500: {status} {text}"
    );
    assert!(
        status.is_client_error(),
        "bulk patch with unknown source must 4xx, got {status}: {text}"
    );
    // And the stored row still carries its previous source.
    let check = pool().await;
    let stored: String = sqlx::query_scalar("SELECT source::text FROM lead.leads WHERE id=$1")
        .bind(target)
        .fetch_one(&check)
        .await
        .unwrap();
    assert_eq!(
        stored, "whatsapp",
        "a refused patch must not change the stored source"
    );
}
