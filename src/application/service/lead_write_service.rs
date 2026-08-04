//! The hand-authored Lead write path (user-owned; survives regen).
//!
//! Lead capture (WhatsApp-first). At least one contact channel is required. Posts NO GL.
//! The cross-module orchestration — `qualify_lead` (creates a deal Opportunity) and
//! `convert_lead` (mints a party Customer + back-fills deals) — lives in backbone-crm-app,
//! because both span lead + deal repos in one transaction. This service owns only the
//! lead-only capture. Ported from backbone-crm's `crm_write_service.rs` (lead parts).

use backbone_orm::company_scope;
use sqlx::PgPool;
use uuid::Uuid;

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
}

pub struct LeadWriteService {
    pool: PgPool,
    leads: LeadRepository,
}

impl LeadWriteService {
    pub fn new(pool: PgPool) -> Self {
        let leads = LeadRepository::new(pool.clone());
        Self { pool, leads }
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
                })
                .await?;
            Ok(id)
        })
        .await
    }
}
