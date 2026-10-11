// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

// The hand-written website intake persistence (user-owned).
mod lead_website_visitor_repository;
pub use lead_repository::{
    DuplicateKeyGroupRow, LeadForConvertRow, LeadForQualifyRow, LeadMatchRow, NewLeadRow,
};
pub use lead_website_visitor_repository::{
    find_capture_match_on_conn, insert_lead_on_conn, leads_for_visitor_on_conn,
    link_website_visitor_on_conn, WebsiteCaptureMatch,
};
