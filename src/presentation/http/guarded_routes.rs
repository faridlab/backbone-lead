//! Guarded route composition — the RECOMMENDED way to mount the lead module.
//!
//! Hand-authored (user-owned; see `metaphor.codegen.yaml`), following the selling pattern.
//! Read surface = the generated GET routes; mutations = the validated write service only, so a
//! caller cannot generic-PUT a lead into an inconsistent pipeline state (bogus status,
//! overwriting `party_id`, or un-absorbing a merged dupe by nulling `merged_into_lead_id`).
//! The generic write surface is NOT mounted.
//!
//! Verbs:
//! - `POST /leads`                     — validated capture (tenant from the token, never the body);
//! - `GET  /leads/duplicates-candidates` — the duplicate scan (same tenant);
//! - `POST /leads/:id/merge`           — pinned merge: the path lead is (or redirects to) the master,
//!                                        body carries 1..=5 absorb ids;
//! - `POST /leads/merge`               — auto merge: the confidence order picks the master over
//!                                        2..=6 leads.
//!
//! Static segments (`/leads/merge`, `/leads/duplicates-candidates`) coexist with `/leads/:id` —
//! axum resolves static before param, the same coexistence the generated trash/count routes
//! already rely on.
//!
//! Cross-module re-points after a merge (deal opportunities, mail activities referencing an
//! absorbed id) are the HOST's job — the merge response carries `masterId` + `absorbedIds`
//! exactly for that. This module takes no dependency on deal/activity.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    middleware::from_fn_with_state,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use backbone_auth::company::{company_auth, CompanyContext, CompanyVerifier};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

use crate::application::service::lead_merge::MergeOutcome;
use crate::application::service::lead_write_service::{LeadError, LeadWriteService, NewLead};
use crate::domain::event::{LeadEventSink, LoggingLeadSink};
use crate::LeadModule;

use super::create_lead_read_routes;

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
    message: String,
}
#[derive(Debug, Serialize)]
struct IdResponse {
    id: Uuid,
}
fn err_response(e: LeadError) -> axum::response::Response {
    let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(ErrorBody { error: e.code(), message: e.to_string() })).into_response()
}

// ── request / response bodies ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaptureLeadBody {
    lead_name: String,
    // No `company_id`: the tenant comes from the signed token (`CompanyContext`), never the
    // body — a client must not be able to name the tenant it writes into.
    #[serde(default)]
    organization_name: Option<String>,
    #[serde(default)]
    phone: Option<String>,
    #[serde(default)]
    whatsapp_no: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    campaign_id: Option<Uuid>,
    #[serde(default)]
    notes: Option<String>,
    // Assignment is stored as given (no policy here): autofill / leader-fallback defaults are
    // the composing service's job.
    #[serde(default)]
    owner_user_id: Option<Uuid>,
    #[serde(default)]
    sales_team_id: Option<Uuid>,
}
async fn capture_lead(
    State(svc): State<Arc<LeadWriteService>>,
    tenant: CompanyContext,
    Json(b): Json<CaptureLeadBody>,
) -> axum::response::Response {
    let lead = NewLead {
        company_id: tenant.company_id,
        lead_name: b.lead_name,
        organization_name: b.organization_name,
        phone: b.phone,
        whatsapp_no: b.whatsapp_no,
        email: b.email,
        source: b.source.unwrap_or_else(|| "whatsapp".into()),
        campaign_id: b.campaign_id,
        notes: b.notes,
        owner_user_id: b.owner_user_id,
        sales_team_id: b.sales_team_id,
    };
    match svc.create_lead(lead).await {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e),
    }
}

/// Raw string params so a malformed value answers the module's typed 422, not the extractor's 400.
type RawQuery = HashMap<String, String>;
async fn duplicates_candidates(
    State(svc): State<Arc<LeadWriteService>>,
    tenant: CompanyContext,
    Query(q): Query<RawQuery>,
) -> axum::response::Response {
    let min = match q.get("min_group_size").map(|v| v.parse::<i64>()) {
        None => 2,
        Some(Ok(v)) => v,
        Some(Err(_)) => return err_response(LeadError::Invalid("min_group_size must be an integer".into())),
    };
    let limit = match q.get("limit").map(|v| v.parse::<i64>()) {
        None => 50,
        Some(Ok(v)) => v,
        Some(Err(_)) => return err_response(LeadError::Invalid("limit must be an integer".into())),
    };
    match svc.duplicate_candidate_groups(tenant.company_id, min, limit).await {
        Ok(groups) => {
            #[derive(Serialize)]
            #[serde(rename_all = "camelCase")]
            struct MatchReason<'a> {
                key_kind: &'a str,
                key_value: &'a str,
            }
            #[derive(Serialize)]
            #[serde(rename_all = "camelCase")]
            struct GroupBody<'a> {
                match_reason: MatchReason<'a>,
                member_count: i64,
                suggested_master_id: Uuid,
                members: &'a [crate::application::service::lead_merge::GroupMember],
            }
            #[derive(Serialize)]
            struct DuplicatesBody<'a> {
                groups: Vec<GroupBody<'a>>,
            }
            let body = DuplicatesBody {
                groups: groups
                    .iter()
                    .map(|g| GroupBody {
                        match_reason: MatchReason {
                            key_kind: &g.key_kind,
                            key_value: &g.key_value,
                        },
                        member_count: g.member_count,
                        suggested_master_id: g.suggested_master_id,
                        members: &g.members,
                    })
                    .collect(),
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => err_response(e),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergePinnedBody {
    absorb_ids: Vec<Uuid>,
}
async fn merge_pinned(
    State(svc): State<Arc<LeadWriteService>>,
    tenant: CompanyContext,
    Path(id): Path<Uuid>,
    Json(b): Json<MergePinnedBody>,
) -> axum::response::Response {
    match svc.merge_leads(tenant.company_id, Some(id), b.absorb_ids).await {
        Ok(outcome) => (StatusCode::OK, Json(merge_body(&outcome))).into_response(),
        Err(e) => err_response(e),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeAutoBody {
    lead_ids: Vec<Uuid>,
}
async fn merge_auto(
    State(svc): State<Arc<LeadWriteService>>,
    tenant: CompanyContext,
    Json(b): Json<MergeAutoBody>,
) -> axum::response::Response {
    match svc.merge_leads(tenant.company_id, None, b.lead_ids).await {
        Ok(outcome) => (StatusCode::OK, Json(merge_body(&outcome))).into_response(),
        Err(e) => err_response(e),
    }
}

fn merge_body(outcome: &MergeOutcome) -> impl Serialize + '_ {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MergeResponseBody<'a> {
        master_id: Uuid,
        redirected_from: Option<Uuid>,
        absorbed_ids: &'a [Uuid],
        already_absorbed_elsewhere: &'a [crate::application::service::lead_merge::AbsorbedElsewhere],
        fields_filled: &'a [&'static str],
    }
    MergeResponseBody {
        master_id: outcome.master_id,
        redirected_from: outcome.redirected_from,
        absorbed_ids: &outcome.absorbed_ids,
        already_absorbed_elsewhere: &outcome.already_absorbed_elsewhere,
        fields_filled: &outcome.fields_filled,
    }
}

// ── composition ───────────────────────────────────────────────────────────────

fn create_lead_write_routes(svc: Arc<LeadWriteService>, verifier: CompanyVerifier) -> Router {
    Router::new()
        .route("/leads", post(capture_lead))
        .route("/leads/duplicates-candidates", get(duplicates_candidates))
        .route("/leads/merge", post(merge_auto))
        .route("/leads/:id/merge", post(merge_pinned))
        // Every route above is tenant-scoped: `company_auth` rejects a request whose token is
        // absent, invalid, or carries no `company_id`, so a handler only ever runs with a proven
        // tenant — and the RLS fence scopes every statement to it.
        //
        // `route_layer`, not `layer`: `layer` would also wrap this router's fallback, so once
        // merged every *unmatched* path (e.g. the generic CRUD paths this surface deliberately
        // does not mount) would answer 401 instead of 404 — leaking "auth required" for routes
        // that do not exist, and masking the CRUD-bypass probes.
        .route_layer(from_fn_with_state(verifier, company_auth))
        .with_state(svc)
}

/// Mount the lead module: the generated read surface + validated, tenant-scoped capture and
/// merge verbs. Generic mutation is not mounted. **Prefer this over `LeadModule::all_crud_routes()`
/// for any real deployment.** Merge events go to the logging sink; use
/// [`create_guarded_lead_routes_with_sink`] to publish `LeadMerged` through a real sink.
pub fn create_guarded_lead_routes(m: &LeadModule, pool: PgPool, verifier: CompanyVerifier) -> Router {
    create_guarded_lead_routes_with_sink(m, pool, verifier, Arc::new(LoggingLeadSink))
}

/// [`create_guarded_lead_routes`] with the write service's event sink supplied by the composer
/// (outbox, bus, recorder) — `LeadMerged` publishes through it after each merge commits.
pub fn create_guarded_lead_routes_with_sink(
    m: &LeadModule,
    pool: PgPool,
    verifier: CompanyVerifier,
    sink: Arc<dyn LeadEventSink>,
) -> Router {
    let write = Arc::new(LeadWriteService::with_sink(pool, sink));
    Router::new()
        .merge(create_lead_read_routes(m.lead_service.clone()))
        .merge(create_lead_write_routes(write, verifier))
}
