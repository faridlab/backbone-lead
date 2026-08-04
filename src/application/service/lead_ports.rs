//! Lead's forward-conversion port — the exit where a Lead becomes a party Customer.
//!
//! backbone-lead holds only this trait + its DTOs; a composing service (backbone-crm-app)
//! implements it over backbone-party. **Zero normal Cargo edge** to backbone-party — the DTOs
//! are the wire contract, duplicated per consumer by design. Ported from backbone-crm's
//! `crm_ports.rs` (PartyPort + CustomerFromLead + PartyAck); the SellingPort stays in
//! backbone-deal.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Mint a Customer from a lead (the identity ACL — a Lead is NOT a Party).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomerFromLead {
    pub company_id: Uuid,
    pub lead_id: Uuid,
    pub name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
}

/// The minted Customer's party id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartyAck {
    pub party_id: Uuid,
}

/// A downstream rejection surfaced to the lead module.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrmRejected {
    pub code: String,
    pub message: String,
}

/// The party seam — a composing service implements it over backbone-party.
#[async_trait::async_trait]
pub trait PartyPort: Send + Sync {
    async fn mint_customer(&self, req: &CustomerFromLead) -> Result<PartyAck, CrmRejected>;
}
