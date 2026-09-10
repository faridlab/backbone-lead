//! HTTP-surface tests for the guarded lead router (mirrors docs/business-flows/golden-cases.md).
//!
//! Runs the real router in-process via `tower::ServiceExt::oneshot` against live Postgres, with
//! the caller identity inserted as a request extension the way the composing service's auth
//! stack does in production (the module itself mounts no auth middleware). Requires DATABASE_URL.
//!
//! R-1  route map + camelCase bodies: capture 201, duplicates-candidates 200 (shape), both
//!      merge verbs 200 (shape), typed 422s for bad input
//! R-2  the `OrgContext` extractor: a request without the caller extension -> 401 on every
//!      verb; an unmatched path stays a plain 404
//! R-3  the generated read surface rides along unchanged (no extractors, no auth gate — the
//!      host wraps its own auth around the whole mount)

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::Router;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use backbone_auth::org::OrgContext;
use backbone_lead::domain::event::{LeadConversionEvent, LeadEventSink};
use backbone_lead::presentation::http::create_guarded_lead_routes_with_sink;
use backbone_lead::LeadModule;

/// The caller identity a request carries in production (inserted by the composing service's
/// org auth layer). The module's handlers only require its PRESENCE — the extractor rejects
/// an unauthenticated request 401 — and derive nothing from it; the DATABASE scope is the
/// ambient request scope the host bound.
fn caller() -> OrgContext {
    OrgContext {
        acting_unit_id: Uuid::new_v4(),
        entitled_units: vec![],
        legacy_company_id: None,
        user_id: "merge-routes-test-principal".to_string(),
    }
}

/// Wrap the router with the extension the host auth stack provides in production.
fn with_caller(router: Router) -> Router {
    let org = caller();
    router.layer(middleware::from_fn(
        move |mut req: axum::extract::Request, next: Next| {
            let org = org.clone();
            async move {
                req.extensions_mut().insert(org);
                next.run(req).await
            }
        },
    ))
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
        .unwrap_or_else(|_| "postgresql://postgres:postgres@127.0.0.1:5433/lead_merge_test".into());
    PgPool::connect(&url).await.expect("connect DB")
}

/// The guarded router WITHOUT the caller extension — tests decide which requests carry one.
async fn app() -> (axum::Router, Arc<RecordingSink>) {
    let pool = pool().await;
    let module = LeadModule::builder().with_database(pool.clone()).build().unwrap();
    let sink = Arc::new(RecordingSink::default());
    let router = create_guarded_lead_routes_with_sink(&module, pool, sink.clone());
    (router, sink)
}

fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.map(|v| v.to_string()).unwrap_or_default()))
        .unwrap()
}

async fn send(router: axum::Router, r: Request<Body>) -> (StatusCode, Value) {
    let resp = router.oneshot(r).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn capture(router: axum::Router, name: &str, phone: Option<&str>) -> Uuid {
    let mut b = json!({ "leadName": name });
    if let Some(p) = phone {
        b["phone"] = json!(p);
    }
    let (status, body) = send(router, req("POST", "/leads", Some(b))).await;
    assert_eq!(status, StatusCode::CREATED, "capture failed: {body}");
    serde_json::from_value(body["id"].clone()).unwrap()
}

// ── R-1: route map + camelCase bodies ─────────────────────────────────────────

#[tokio::test]
async fn r1_route_map_and_camelcase_bodies() {
    let (base, sink) = app().await;
    let router = with_caller(base);

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
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let a: Uuid = serde_json::from_value(body["id"].clone()).unwrap();
    let b = capture(router.clone(), "R1 Andi dupe", Some("0826-111-2222")).await;

    // Assignment columns are stored exactly as given.
    {
        let pool = pool().await;
        let owner_stored: Option<Uuid> =
            sqlx::query_scalar("SELECT owner_user_id FROM lead.leads WHERE id=$1").bind(a).fetch_one(&pool).await.unwrap();
        assert_eq!(owner_stored, Some(owner), "assignment columns stored as given");
    }

    // Duplicates-candidates: the formatted-variant pair groups under one phone key.
    let (status, body) = send(
        router.clone(),
        req("GET", "/leads/duplicates-candidates?min_group_size=2&limit=50", None),
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
        req("POST", &format!("/leads/{a}/merge"), Some(json!({ "absorbIds": [b] }))),
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
    let d = capture(router.clone(), "R1 Budi", Some("+62 827-333-4444")).await;
    let e = capture(router.clone(), "R1 Budi dupe", Some("0827-333-4444")).await;
    let (status, body) = send(
        router.clone(),
        req("POST", "/leads/merge", Some(json!({ "leadIds": [d, e] }))),
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
        req("GET", "/leads/duplicates-candidates?min_group_size=abc", None),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], json!("invalid_input"));

    let (status, _) = send(
        router.clone(),
        req("POST", "/leads", Some(json!({ "leadName": "no channel" }))),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Static segments beat the :id param: GET on the POST-only /leads/merge is a method
    // mismatch, not a :id lookup of "merge".
    let (status, _) = send(router.clone(), req("GET", "/leads/merge", None)).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

// ── R-2: the OrgContext extractor gates the verbs, 404 elsewhere ──────────────

#[tokio::test]
async fn r2_missing_caller_extension_is_401_and_unmatched_paths_stay_404() {
    let (router, _) = app().await;
    let some_id = Uuid::new_v4();

    // No caller extension in the request: the `OrgContext` extractor rejects every write
    // verb 401, before any handler runs. (In production the composing service's org auth
    // layer inserts the extension over a signed token.)
    let pinned_uri = format!("/leads/{some_id}/merge");
    for (method, uri, body) in [
        ("POST", "/leads", Some(json!({ "leadName": "x", "phone": "+62 828-1" }))),
        ("GET", "/leads/duplicates-candidates", None),
        ("POST", "/leads/merge", Some(json!({ "leadIds": [some_id, Uuid::new_v4()] }))),
        ("POST", pinned_uri.as_str(), Some(json!({ "absorbIds": [Uuid::new_v4()] }))),
    ] {
        let (status, _) = send(router.clone(), req(method, uri, body)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri} without a caller extension");
    }

    // No auth middleware is mounted on this router, so an UNMATCHED path is a plain 404 —
    // the module never claims routes this surface does not mount.
    let (status, _) = send(router.clone(), req("POST", "/definitely-not-a-route", Some(json!({})))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── R-3: the generated read surface rides along unchanged ─────────────────────

#[tokio::test]
async fn r3_read_surface_mounted_and_unchanged() {
    // The read routes carry no extractor: they answer without the caller extension, exactly
    // as an unauthenticated host-side gate would see them before its own auth layer.
    let (base, _) = app().await;
    let router = with_caller(base);
    let a = capture(router.clone(), "R3 Read", Some("+62 8210-1")).await;

    // The generic read routes are still mounted verbatim (count answers without extractors).
    let (status, body) = send(router.clone(), req("GET", "/leads/count", None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.to_string().contains("count"));

    // By-id lookup still resolves the captured row.
    let (status, _) = send(router.clone(), req("GET", &format!("/leads/{a}"), None)).await;
    assert_eq!(status, StatusCode::OK);

    // An unknown id is the read surface's own 404.
    let (status, _) = send(router.clone(), req("GET", &format!("/leads/{}", Uuid::new_v4()), None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
