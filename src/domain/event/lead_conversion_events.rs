//! Lead conversion domain events (hand-authored, user-owned) — the public extension surface.
//!
//! Distinct from the generated CRUD-lifecycle `LeadEvent`: these are the funnel signals the
//! write path publishes. `LeadQualified` (a lead became an opportunity — emitted by the app's
//! qualify orchestration) and `LeadConverted` (a party minted a Customer — emitted by the app's
//! convert orchestration). Ported from backbone-crm's `crm_events.rs`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A lead was qualified into an opportunity. (Published by the app's qualify_lead tx.)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LeadQualified {
    pub lead_id: Uuid,
    pub opportunity_id: Uuid,
    pub company_id: Uuid,
}

/// A lead was resolved to a Customer — the identity ACL (party minted the Customer).
/// (Published by the app's convert_lead tx.)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LeadConverted {
    pub lead_id: Uuid,
    pub party_id: Uuid,
    pub company_id: Uuid,
}

/// The lead conversion-event union (distinct from the generated CRUD `LeadEvent`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum LeadConversionEvent {
    LeadQualified(LeadQualified),
    LeadConverted(LeadConverted),
}

/// Sink the write path publishes to. A consuming service (the app) supplies its own (bus, outbox, …).
pub trait LeadEventSink: Send + Sync {
    fn publish(&self, event: &LeadConversionEvent);
}

/// A no-op/logging sink for tests and single-process composition.
#[derive(Debug, Default, Clone)]
pub struct LoggingLeadSink;

impl LeadEventSink for LoggingLeadSink {
    fn publish(&self, event: &LeadConversionEvent) {
        tracing::info!(?event, "lead event");
    }
}
