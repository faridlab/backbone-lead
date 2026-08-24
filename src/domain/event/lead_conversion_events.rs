//! Lead funnel domain events (hand-authored, user-owned) — the public extension surface.
//!
//! Distinct from the generated CRUD-lifecycle `LeadEvent`: these are the funnel signals the
//! write path publishes. `LeadQualified` (a lead became an opportunity — emitted by the app's
//! qualify orchestration) and `LeadConverted` (a party minted a Customer — emitted by the app's
//! convert orchestration). Ported from backbone-crm's `crm_events.rs`.
//!
//! `LeadMerged` (duplicate leads soft-absorbed into a master) is published by THIS module's
//! merge verb after its transaction commits — merge is single-table, unlike the cross-module
//! qualify/convert orchestrations. The union is historically named `LeadConversionEvent`; it
//! carries all the funnel signals, merge included.

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

/// Duplicate leads were soft-absorbed into a master. Published after the merge transaction
/// commits, only when at least one lead was newly absorbed (an idempotent no-op re-merge emits
/// nothing). Downstream consumers re-point their references from the absorbed ids to the master
/// — the cross-module re-point itself is the composing service's job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LeadMerged {
    pub lead_id: Uuid,
    pub absorbed_ids: Vec<Uuid>,
    pub company_id: Uuid,
}

/// The lead funnel-event union (distinct from the generated CRUD `LeadEvent`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum LeadConversionEvent {
    LeadQualified(LeadQualified),
    LeadConverted(LeadConverted),
    LeadMerged(LeadMerged),
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
