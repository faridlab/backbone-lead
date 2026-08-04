use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "lead_status", rename_all = "snake_case")]
pub enum LeadStatus {
    New,
    Contacted,
    Qualified,
    Converted,
    Junk,
    Lost,
}

impl std::fmt::Display for LeadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::New => write!(f, "new"),
            Self::Contacted => write!(f, "contacted"),
            Self::Qualified => write!(f, "qualified"),
            Self::Converted => write!(f, "converted"),
            Self::Junk => write!(f, "junk"),
            Self::Lost => write!(f, "lost"),
        }
    }
}

impl FromStr for LeadStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "new" => Ok(Self::New),
            "contacted" => Ok(Self::Contacted),
            "qualified" => Ok(Self::Qualified),
            "converted" => Ok(Self::Converted),
            "junk" => Ok(Self::Junk),
            "lost" => Ok(Self::Lost),
            _ => Err(format!("Unknown LeadStatus variant: {}", s)),
        }
    }
}

impl Default for LeadStatus {
    fn default() -> Self {
        Self::New
    }
}
