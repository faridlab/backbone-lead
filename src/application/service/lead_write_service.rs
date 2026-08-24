//! The hand-authored Lead write path (user-owned; survives regen).
//!
//! Lead capture (WhatsApp-first). At least one contact channel is required. Posts NO GL.
//! The cross-module orchestration — `qualify_lead` (creates a deal Opportunity) and
//! `convert_lead` (mints a party Customer + back-fills deals) — lives in backbone-crm-app,
//! because both span lead + deal repos in one transaction. This service owns only the
//! lead-only capture. Ported from backbone-crm's `crm_write_service.rs` (lead parts).
//!
//! **This file is the hub:** it holds the write-path vocabulary (input structs, outcomes,
//! errors) and the constructors. The dedup/merge surface — the duplicate-candidate scan and
//! the merge verbs — is chunked into the sibling [`super::lead_merge`] as a second
//! `impl LeadWriteService` block over these same types.
//!
//! Events: the service carries an [`LeadEventSink`] (default: logging). Merge publishes
//! `LeadMerged` through it after commit; qualify/convert stay app-side as before.

use std::sync::Arc;

use backbone_orm::company_scope;
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::event::{LeadEventSink, LoggingLeadSink};
use crate::infrastructure::persistence::{LeadRepository, NewLeadRow};

#[derive(Debug, thiserror::Error)]
pub enum LeadError {
    #[error("db: {0}")]
    Db(#[from] sqlx::Error),
    #[error("not found: {0}")]
    NotFound(&'static str),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("party rejected: {0}")]
    PartyRejected(String),
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
    #[error("lead {0} is converted and can never be absorbed into another lead")]
    AbsorbConverted(Uuid),
    #[error("a lead cannot be absorbed into itself")]
    AbsorbSelf,
    #[error("absorb chain exceeded the maximum depth")]
    ChainTooDeep,
}

impl LeadError {
    /// Stable machine-readable code for API responses (the selling error shape).
    pub fn code(&self) -> String {
        match self {
            LeadError::Db(_) => "internal_error".into(),
            LeadError::NotFound(_) => "not_found".into(),
            LeadError::Invalid(_) => "invalid_input".into(),
            LeadError::PartyRejected(_) => "party_rejected".into(),
            LeadError::InvalidBatch(_) => "invalid_batch".into(),
            LeadError::AbsorbConverted(_) => "absorb_converted".into(),
            LeadError::AbsorbSelf => "absorb_self".into(),
            LeadError::ChainTooDeep => "chain_too_deep".into(),
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            LeadError::Db(_) | LeadError::ChainTooDeep => 500,
            LeadError::NotFound(_) => 404,
            _ => 422,
        }
    }
}

pub struct NewLead {
    pub company_id: Uuid,
    pub lead_name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
    pub source: String, // lead_source variant
    pub campaign_id: Option<Uuid>,
    pub notes: Option<String>,
    /// Assigned salesperson — STORED only; assignment policy (autofill, leader fallback) is the
    /// composing service's job.
    pub owner_user_id: Option<Uuid>,
    /// Assigned sales team — STORED only, same host-side policy rule as the owner.
    pub sales_team_id: Option<Uuid>,
}

pub struct LeadWriteService {
    pub(super) pool: PgPool,
    pub(super) leads: LeadRepository,
    pub(super) sink: Arc<dyn LeadEventSink>,
}

impl LeadWriteService {
    pub fn new(pool: PgPool) -> Self {
        Self::with_sink(pool, Arc::new(LoggingLeadSink))
    }

    /// Construct with a custom event sink (outbox, bus, recorder) — merge publishes
    /// `LeadMerged` through it.
    pub fn with_sink(pool: PgPool, sink: Arc<dyn LeadEventSink>) -> Self {
        let leads = LeadRepository::new(pool.clone());
        Self { pool, leads, sink }
    }

    /// Capture a lead (WhatsApp-first). At least one contact channel is required.
    pub async fn create_lead(&self, l: NewLead) -> Result<Uuid, LeadError> {
        if l.whatsapp_no.is_none() && l.phone.is_none() && l.email.is_none() {
            return Err(LeadError::Invalid("a lead needs at least one contact channel".into()));
        }
        // RLS scope (ADR-0008): company on the DTO — bind it for the body so the insert passes the
        // WITH CHECK fence. The explicit `company_id` bind stays as defense-in-depth.
        let company = l.company_id;
        company_scope::with_company_scope(Some(company), async move {
            let id = Uuid::new_v4();
            self.leads
                .insert_lead(&self.pool, &NewLeadRow {
                    id,
                    company_id: l.company_id,
                    lead_name: &l.lead_name,
                    organization_name: l.organization_name.as_deref(),
                    phone: l.phone.as_deref(),
                    whatsapp_no: l.whatsapp_no.as_deref(),
                    email: l.email.as_deref(),
                    source: &l.source,
                    campaign_id: l.campaign_id,
                    notes: l.notes.as_deref(),
                    owner_user_id: l.owner_user_id,
                    sales_team_id: l.sales_team_id,
                })
                .await?;
            Ok(id)
        })
        .await
    }
}
