use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::LeadSource;
use super::LeadStatus;
use super::AuditMetadata;

/// Strongly-typed ID for Lead
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeadId(pub Uuid);

impl LeadId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for LeadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for LeadId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for LeadId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<LeadId> for Uuid {
    fn from(id: LeadId) -> Self { id.0 }
}

impl AsRef<Uuid> for LeadId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for LeadId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Lead {
    pub id: Uuid,
    pub company_id: Uuid,
    pub lead_name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
    pub source: LeadSource,
    pub campaign_id: Option<Uuid>,
    pub status: LeadStatus,
    pub party_id: Option<Uuid>,
    pub converted_at: Option<DateTime<Utc>>,
    pub notes: Option<String>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl Lead {
    /// Create a builder for Lead
    pub fn builder() -> LeadBuilder {
        LeadBuilder::default()
    }

    /// Create a new Lead with required fields
    pub fn new(company_id: Uuid, lead_name: String, source: LeadSource, status: LeadStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            lead_name,
            organization_name: None,
            phone: None,
            whatsapp_no: None,
            email: None,
            source,
            campaign_id: None,
            status,
            party_id: None,
            converted_at: None,
            notes: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> LeadId {
        LeadId(self.id)
    }

    /// Get when this entity was created
    pub fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.created_at.as_ref()
    }

    /// Get when this entity was last updated
    pub fn updated_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.updated_at.as_ref()
    }

    /// Check if this entity is soft deleted
    pub fn is_deleted(&self) -> bool {
        self.metadata.deleted_at.is_some()
    }

    /// Check if this entity is active (not deleted)
    pub fn is_active(&self) -> bool {
        self.metadata.deleted_at.is_none()
    }

    /// Get when this entity was deleted
    pub fn deleted_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.deleted_at.as_ref()
    }

    /// Get who created this entity
    pub fn created_by(&self) -> Option<&Uuid> {
        self.metadata.created_by.as_ref()
    }

    /// Get who last updated this entity
    pub fn updated_by(&self) -> Option<&Uuid> {
        self.metadata.updated_by.as_ref()
    }

    /// Get who deleted this entity
    pub fn deleted_by(&self) -> Option<&Uuid> {
        self.metadata.deleted_by.as_ref()
    }

    /// Get the current status
    pub fn status(&self) -> &LeadStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the organization_name field (chainable)
    pub fn with_organization_name(mut self, value: String) -> Self {
        self.organization_name = Some(value);
        self
    }

    /// Set the phone field (chainable)
    pub fn with_phone(mut self, value: String) -> Self {
        self.phone = Some(value);
        self
    }

    /// Set the whatsapp_no field (chainable)
    pub fn with_whatsapp_no(mut self, value: String) -> Self {
        self.whatsapp_no = Some(value);
        self
    }

    /// Set the email field (chainable)
    pub fn with_email(mut self, value: String) -> Self {
        self.email = Some(value);
        self
    }

    /// Set the campaign_id field (chainable)
    pub fn with_campaign_id(mut self, value: Uuid) -> Self {
        self.campaign_id = Some(value);
        self
    }

    /// Set the party_id field (chainable)
    pub fn with_party_id(mut self, value: Uuid) -> Self {
        self.party_id = Some(value);
        self
    }

    /// Set the converted_at field (chainable)
    pub fn with_converted_at(mut self, value: DateTime<Utc>) -> Self {
        self.converted_at = Some(value);
        self
    }

    /// Set the notes field (chainable)
    pub fn with_notes(mut self, value: String) -> Self {
        self.notes = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "company_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.company_id = v; }
                }
                "lead_name" => {
                    if let Ok(v) = serde_json::from_value(value) { self.lead_name = v; }
                }
                "organization_name" => {
                    if let Ok(v) = serde_json::from_value(value) { self.organization_name = v; }
                }
                "phone" => {
                    if let Ok(v) = serde_json::from_value(value) { self.phone = v; }
                }
                "whatsapp_no" => {
                    if let Ok(v) = serde_json::from_value(value) { self.whatsapp_no = v; }
                }
                "email" => {
                    if let Ok(v) = serde_json::from_value(value) { self.email = v; }
                }
                "source" => {
                    if let Ok(v) = serde_json::from_value(value) { self.source = v; }
                }
                "campaign_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.campaign_id = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "party_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.party_id = v; }
                }
                "converted_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.converted_at = v; }
                }
                "notes" => {
                    if let Ok(v) = serde_json::from_value(value) { self.notes = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for Lead {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "Lead"
    }
}

impl backbone_core::PersistentEntity for Lead {
    fn entity_id(&self) -> String {
        self.id.to_string()
    }
    fn set_entity_id(&mut self, id: String) {
        if let Ok(uuid) = uuid::Uuid::parse_str(&id) {
            self.id = uuid;
        }
    }
    fn created_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.created_at
    }
    fn set_created_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.created_at = Some(ts);
    }
    fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.updated_at
    }
    fn set_updated_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.updated_at = Some(ts);
    }
    fn deleted_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.deleted_at
    }
    fn set_deleted_at(&mut self, ts: Option<chrono::DateTime<chrono::Utc>>) {
        self.metadata.deleted_at = ts;
    }
}

impl backbone_orm::EntityRepoMeta for Lead {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("campaign_id".to_string(), "uuid".to_string());
        m.insert("party_id".to_string(), "uuid".to_string());
        m.insert("source".to_string(), "lead_source".to_string());
        m.insert("status".to_string(), "lead_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["lead_name"]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for Lead entity
///
/// Provides a fluent API for constructing Lead instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct LeadBuilder {
    company_id: Option<Uuid>,
    lead_name: Option<String>,
    organization_name: Option<String>,
    phone: Option<String>,
    whatsapp_no: Option<String>,
    email: Option<String>,
    source: Option<LeadSource>,
    campaign_id: Option<Uuid>,
    status: Option<LeadStatus>,
    party_id: Option<Uuid>,
    converted_at: Option<DateTime<Utc>>,
    notes: Option<String>,
}

impl LeadBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the lead_name field (required)
    pub fn lead_name(mut self, value: String) -> Self {
        self.lead_name = Some(value);
        self
    }

    /// Set the organization_name field (optional)
    pub fn organization_name(mut self, value: String) -> Self {
        self.organization_name = Some(value);
        self
    }

    /// Set the phone field (optional)
    pub fn phone(mut self, value: String) -> Self {
        self.phone = Some(value);
        self
    }

    /// Set the whatsapp_no field (optional)
    pub fn whatsapp_no(mut self, value: String) -> Self {
        self.whatsapp_no = Some(value);
        self
    }

    /// Set the email field (optional)
    pub fn email(mut self, value: String) -> Self {
        self.email = Some(value);
        self
    }

    /// Set the source field (default: `LeadSource::default()`)
    pub fn source(mut self, value: LeadSource) -> Self {
        self.source = Some(value);
        self
    }

    /// Set the campaign_id field (optional)
    pub fn campaign_id(mut self, value: Uuid) -> Self {
        self.campaign_id = Some(value);
        self
    }

    /// Set the status field (default: `LeadStatus::default()`)
    pub fn status(mut self, value: LeadStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the party_id field (optional)
    pub fn party_id(mut self, value: Uuid) -> Self {
        self.party_id = Some(value);
        self
    }

    /// Set the converted_at field (optional)
    pub fn converted_at(mut self, value: DateTime<Utc>) -> Self {
        self.converted_at = Some(value);
        self
    }

    /// Set the notes field (optional)
    pub fn notes(mut self, value: String) -> Self {
        self.notes = Some(value);
        self
    }

    /// Build the Lead entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<Lead, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let lead_name = self.lead_name.ok_or_else(|| "lead_name is required".to_string())?;

        Ok(Lead {
            id: Uuid::new_v4(),
            company_id,
            lead_name,
            organization_name: self.organization_name,
            phone: self.phone,
            whatsapp_no: self.whatsapp_no,
            email: self.email,
            source: self.source.unwrap_or(LeadSource::default()),
            campaign_id: self.campaign_id,
            status: self.status.unwrap_or(LeadStatus::default()),
            party_id: self.party_id,
            converted_at: self.converted_at,
            notes: self.notes,
            metadata: AuditMetadata::default(),
        })
    }
}
