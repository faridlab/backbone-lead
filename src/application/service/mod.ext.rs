// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

pub mod lead_ports;
pub mod lead_write_service;
pub mod lead_merge;
pub use lead_ports::{PartyPort, CustomerFromLead, PartyAck, CrmRejected};
pub use lead_write_service::{LeadWriteService, NewLead, LeadError};
pub use lead_merge::{DuplicateGroup, GroupMember, MergeOutcome, AbsorbedElsewhere};
