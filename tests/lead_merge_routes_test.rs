//! HTTP-surface tests for the guarded lead router (mirrors docs/business-flows/golden-cases.md).
//!
//! Runs the real router in-process via `tower::ServiceExt::oneshot` against live Postgres, with
//! HS256 company tokens forged the way the family's guard tests do. Requires DATABASE_URL.
//!
//! R-1  route map + camelCase bodies: capture 201, duplicates-candidates 200 (shape), both
//!      merge verbs 200 (shape), typed 422s for bad input
//! R-2  company_auth: absent/tenantless token -> 401 on every verb, an unmatched path stays
//!      404 (route_layer, not layer), cross-tenant merge id -> the fence-shaped 404
//! R-3  the generated read surface rides along unchanged (and unauthenticated — the host
//!      wraps its own auth around the whole mount)

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use backbone_auth::company::CompanyVerifier;
use backbone_lead::domain::event::{LeadConversionEvent, LeadEventSink};
use backbone_lead::presentation::http::create_guarded_lead_routes_with_sink;
use backbone_lead::LeadModule;

const SECRET: &[u8] = b"lead-merge-routes-test-secret";

#[derive(Serialize)]
struct TestClaims {
    sub: String,
    exp: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    company_id: Option<Uuid>,
}

/// Mint an HS256 token. `company_id = None` models an authenticated principal with no tenant.
fn token(company_id: Option<Uuid>) -> String {
    let claims = TestClaims { sub: "agent-1".into(), exp: 9_999_999_999, company_id };
    encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(SECRET)).unwrap()
}

/// Records every published event so the router tests can see `LeadMerged` went through the
/// composer-supplied sink.
#[derive(Default)]
struct RecordingSink(Mutex<Vec<LeadConversionEvent>>);
impl LeadEventSink for RecordingSink {
    fn publish(&self, event: &LeadConversionEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://serpa:serpa_dev_password@127.0.0.1:5432/lead_merge_test".into());
    PgPool::connect(&url).await.expect("connect DB")
}

async fn app() -> (axum::Router, Arc<RecordingSink>) {
    let pool = pool().await;
    let module = LeadModule::builder().with_database(pool.clone()).build().unwrap();
    let sink = Arc::new(RecordingSink::default());
    let router = create_guarded_lead_routes_with_sink(
        &module,
        pool,
        CompanyVerifier::hs256(SECRET),
        sink.clone(),
    );
    (router, sink)
}

fn req(method: &str, uri: &str, body: Option<Value>, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri).header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = bearer {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    b.body(Body::from(body.map(|v| v.to_string()).unwrap_or_default())).unwrap()
}

async fn send(router: axum::Router, r: Request<Body>) -> (StatusCode, Value) {
    let resp = router.oneshot(r).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn capture(router: axum::Router, bearer: &str, name: &str, phone: Option<&str>) -> Uuid {
    let mut b = json!({ "leadName": name });
    if let Some(p) = phone {
        b["phone"] = json!(p);
    }
    let (status, body) = send(router, req("POST", "/leads", Some(b), Some(bearer))).await;
    assert_eq!(status, StatusCode::CREATED, "capture failed: {body}");
    serde_json::from_value(body["id"].clone()).unwrap()
}

// ── R-1: route map + camelCase bodies ─────────────────────────────────────────

#[tokio::test]
async fn r1_route_map_and_camelcase_bodies() {
    let (router, sink) = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(Some(company));

    // Capture 201 with camelCase in/out, assignment stored as given (no policy here).
    let owner = Uuid::new_v4();
    let team = Uuid::new_v4();
    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            "/leads",
            Some(json!({
                "leadName": "R1 Andi",
                "phone": "+62 826-111-2222",
                "ownerUserId": owner,
                "salesTeamId": team
            })),
            Some(bearer),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let a: Uuid = serde_json::from_value(body["id"].clone()).unwrap();
    let b = capture(router.clone(), bearer, "R1 Andi dupe", Some("0826-111-2222")).await;

    // The tenant came from the token, never the body: both rows belong to the token's company.
    {
        let pool = pool().await;
        let owner_stored: Option<Uuid> =
            sqlx::query_scalar("SELECT owner_user_id FROM lead.leads WHERE id=$1").bind(a).fetch_one(&pool).await.unwrap();
        assert_eq!(owner_stored, Some(owner), "assignment columns stored as given");
        let owner_co: Uuid =
            sqlx::query_scalar("SELECT company_id FROM lead.leads WHERE id=$1").bind(b).fetch_one(&pool).await.unwrap();
        assert_eq!(owner_co, company);
    }

    // Duplicates-candidates: the formatted-variant pair groups under one phone key.
    let (status, body) = send(
        router.clone(),
        req("GET", "/leads/duplicates-candidates?min_group_size=2&limit=50", None, Some(bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let groups = body["groups"].as_array().expect("groups array");
    let group = groups.iter().find(|g| g["matchReason"]["keyKind"] == "phone" && g["matchReason"]["keyValue"] == "628261112222")
        .expect("phone group for the captured pair");
    assert_eq!(group["memberCount"], json!(2));
    assert!(group["suggestedMasterId"].is_string());
    assert_eq!(group["members"].as_array().unwrap().len(), 2);
    assert!(group["members"][0].get("leadName").is_some(), "members carry camelCase projection fields");

    // Pinned merge: /leads/:id/merge with absorbIds.
    let (status, body) = send(
        router.clone(),
        req("POST", &format!("/leads/{a}/merge"), Some(json!({ "absorbIds": [b] })), Some(bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["masterId"], json!(a));
    assert_eq!(body["absorbedIds"], json!([b]));
    assert_eq!(body["redirectedFrom"], Value::Null);
    assert_eq!(body["alreadyAbsorbedElsewhere"], json!([]));

    // The merge published LeadMerged through the composer-supplied sink.
    {
        let events = sink.0.lock().unwrap();
        assert_eq!(events.len(), 1, "one HTTP merge, one event");
        match &events[0] {
            LeadConversionEvent::LeadMerged(m) => assert_eq!(m.lead_id, a),
            other => panic!("expected LeadMerged, got {other:?}"),
        }
    }

    // Auto merge: /leads/merge with leadIds (fresh pair; the master is whichever the
    // confidence order picks — the order itself is golden-tested at the service layer).
    let d = capture(router.clone(), bearer, "R1 Budi", Some("+62 827-333-4444")).await;
    let e = capture(router.clone(), bearer, "R1 Budi dupe", Some("0827-333-4444")).await;
    let (status, body) = send(
        router.clone(),
        req("POST", "/leads/merge", Some(json!({ "leadIds": [d, e] })), Some(bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let master: Uuid = serde_json::from_value(body["masterId"].clone()).unwrap();
    assert!(master == d || master == e);
    assert_eq!(body["absorbedIds"].as_array().unwrap().len(), 1);

    // Typed 422s: a malformed scan parameter answers the module's error shape, not the
    // extractor's 400; a capture with no contact channel refuses.
    let (status, body) = send(
        router.clone(),
        req("GET", "/leads/duplicates-candidates?min_group_size=abc", None, Some(bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], json!("invalid_input"));

    let (status, _) = send(
        router.clone(),
        req("POST", "/leads", Some(json!({ "leadName": "no channel" })), Some(bearer)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Static segments beat the :id param: GET on the POST-only /leads/merge is a method
    // mismatch, not a :id lookup of "merge".
    let (status, _) = send(router.clone(), req("GET", "/leads/merge", None, Some(bearer))).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

// ── R-2: company_auth on the verbs, 404 elsewhere ─────────────────────────────

#[tokio::test]
async fn r2_auth_gates_verbs_and_only_verbs() {
    let (router, _) = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(Some(company));
    let some_id = Uuid::new_v4();

    // Absent token: 401 on every verb, before any handler runs.
    let pinned_uri = format!("/leads/{some_id}/merge");
    for (method, uri, body) in [
        ("POST", "/leads", Some(json!({ "leadName": "x", "phone": "+62 828-1" }))),
        ("GET", "/leads/duplicates-candidates", None),
        ("POST", "/leads/merge", Some(json!({ "leadIds": [some_id, Uuid::new_v4()] }))),
        ("POST", pinned_uri.as_str(), Some(json!({ "absorbIds": [Uuid::new_v4()] }))),
    ] {
        let (status, _) = send(router.clone(), req(method, uri, body, None)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri} without a token");
    }

    // A token that authenticates a user but carries no tenant is equally refused.
    let (status, _) = send(
        router.clone(),
        req("POST", "/leads", Some(json!({ "leadName": "x", "phone": "+62 828-2" })), Some(&token(None))),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // route_layer (not layer): an UNMATCHED path stays a plain 404, not a 401 — the auth
    // gate must not claim routes this surface does not mount.
    let (status, _) = send(router.clone(), req("POST", "/definitely-not-a-route", Some(json!({})), None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Cross-tenant: company B pins company A's lead — the fetch simply does not resolve
    // inside B, so the answer is the fence-shaped 404, never a leak that the id exists.
    let a = capture(router.clone(), bearer, "R2 Cross", Some("+62 829-555-6666")).await;
    let (status, body) = send(
        router.clone(),
        req(
            "POST",
            &format!("/leads/{a}/merge"),
            Some(json!({ "absorbIds": [Uuid::new_v4()] })),
            Some(&token(Some(Uuid::new_v4()))),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], json!("not_found"));
}

// ── R-3: the generated read surface rides along unchanged ─────────────────────

#[tokio::test]
async fn r3_read_surface_mounted_and_unchanged() {
    let (router, _) = app().await;
    let company = Uuid::new_v4();
    let bearer = &token(Some(company));
    let a = capture(router.clone(), bearer, "R3 Read", Some("+62 8210-1")).await;

    // The generic read routes are still mounted verbatim (count answers without extractors).
    let (status, body) = send(router.clone(), req("GET", "/leads/count", None, None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.to_string().contains("count"));

    // By-id lookup still resolves the captured row.
    let (status, _) = send(router.clone(), req("GET", &format!("/leads/{a}"), None, None)).await;
    assert_eq!(status, StatusCode::OK);

    // An unknown id is the read surface's own 404, not the auth gate's 401.
    let (status, _) = send(router.clone(), req("GET", &format!("/leads/{}", Uuid::new_v4()), None, None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
