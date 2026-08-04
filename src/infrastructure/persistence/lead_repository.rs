//! Repository for Lead entities
//!
//! Generated skeleton, now **user-owned** — this exact path is declared under `user_owned` in
//! `metaphor.codegen.yaml`, so the generator skips it wholesale. The custom methods below hold the
//! hand-written Lead SQL — the capture + the once-only conversion claim (4-layer rule: services
//! orchestrate and own the unit of work, repositories hold the SQL). Ported from backbone-crm with
//! two adaptations for the split: table `crm.leads` → `lead.leads`, and the convert-once fix
//! `converted_party_id` → `party_id` (the resolved-customer anchor; many leads may share one party
//! over time, conversion stays once-per-lead via the `party_id IS NULL` CAS).
//!
//! Thin newtype over `backbone_orm::GenericCrudRepository<Lead, backbone_orm::SoftDelete>`.
//! All standard CRUD methods are available via `Deref`.

use anyhow::Result;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use backbone_orm::company_scope;

use crate::domain::entity::Lead;

/// Table name for Lead entities
pub const TABLE_NAME: &str = "lead.leads";

/// Repository for Lead entities.
///
/// All standard CRUD, soft-delete, pagination, and bulk methods are
/// provided automatically via `Deref` to `backbone_orm::GenericCrudRepository`.
pub struct LeadRepository(
    backbone_orm::GenericCrudRepository<Lead, backbone_orm::SoftDelete>,
);

impl std::ops::Deref for LeadRepository {
    type Target = backbone_orm::GenericCrudRepository<Lead, backbone_orm::SoftDelete>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl LeadRepository {
    /// Create a new repository instance.
    pub fn new(pool: PgPool) -> Self {
        Self(backbone_orm::GenericCrudRepository::new(pool, TABLE_NAME))
    }
}

/// The exact row a lead capture writes.
///
/// Mirrors the raw column shape rather than the `Lead` entity: `source` is carried as a free string and
/// cast at the DB (`$8::lead_source`), so a bad source fails as a DB error rather than a deserialize
/// panic.
pub struct NewLeadRow<'a> {
    pub id: Uuid,
    pub company_id: Uuid,
    pub lead_name: &'a str,
    pub organization_name: Option<&'a str>,
    pub phone: Option<&'a str>,
    pub whatsapp_no: Option<&'a str>,
    pub email: Option<&'a str>,
    pub source: &'a str,
    pub campaign_id: Option<Uuid>,
    pub notes: Option<&'a str>,
}

/// The qualification pre-flight projection: the company to bind, the live status, the party the
/// opportunity inherits, and the campaign it snapshots for attribution.
pub struct LeadForQualifyRow {
    pub company_id: Uuid,
    pub status: String,
    pub party_id: Option<Uuid>,
    pub campaign_id: Option<Uuid>,
}

/// The conversion pre-flight projection: the identity fields the party ACL mints a Customer from, plus
/// the once-only gate (`party_id`).
pub struct LeadForConvertRow {
    pub company_id: Uuid,
    pub lead_name: String,
    pub organization_name: Option<String>,
    pub phone: Option<String>,
    pub whatsapp_no: Option<String>,
    pub email: Option<String>,
    pub status: String,
    pub party_id: Option<Uuid>,
}

/// Hand-written Lead SQL. Lives here (not in the write service) per the module's 4-layer rule.
impl LeadRepository {
    /// Capture a lead.
    ///
    /// A write outside any transaction: takes the pool and runs `execute_scoped` so the RLS fence
    /// (ADR-0008) applies. The caller wraps this in `with_company_scope(Some(company))` — the company is
    /// on the DTO, and that scope is what satisfies the INSERT's WITH CHECK fence. The explicit
    /// `company_id` bind stays as defense-in-depth.
    pub async fn insert_lead(&self, pool: &PgPool, l: &NewLeadRow<'_>) -> Result<(), sqlx::Error> {
        company_scope::execute_scoped(
            pool,
            sqlx::query(
                r#"INSERT INTO lead.leads
                     (id, company_id, lead_name, organization_name, phone, whatsapp_no, email,
                      source, campaign_id, status, notes)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8::lead_source,$9,'new'::lead_status,$10)"#,
            )
            .bind(l.id).bind(l.company_id).bind(l.lead_name).bind(l.organization_name).bind(l.phone)
            .bind(l.whatsapp_no).bind(l.email).bind(l.source).bind(l.campaign_id).bind(l.notes),
        )
        .await?;
        Ok(())
    }

    /// Read what the qualification decision needs.
    ///
    /// ID-only read: no company argument to scope from up front. `fetch_optional_row_scoped` rides the
    /// REQUEST-dedicated connection (which carries the caller's `app.company_id`), so another company's
    /// lead simply isn't found. The company on the returned row is what the caller binds onto its own
    /// transaction.
    pub async fn find_for_qualify(&self, pool: &PgPool, lead_id: Uuid) -> Result<Option<LeadForQualifyRow>, sqlx::Error> {
        let row = company_scope::fetch_optional_row_scoped(
            pool,
            sqlx::query(
                r#"SELECT company_id, status::text AS status, party_id, campaign_id
                   FROM lead.leads WHERE id=$1 AND (metadata->>'deleted_at') IS NULL"#,
            )
            .bind(lead_id),
        )
        .await?;
        Ok(row.map(|r| LeadForQualifyRow {
            company_id: r.get("company_id"),
            status: r.get("status"),
            party_id: r.get("party_id"),
            campaign_id: r.get("campaign_id"),
        }))
    }

    /// Read the identity + gate the conversion decision needs. ID-only read — same fencing as
    /// `find_for_qualify`.
    pub async fn find_for_convert(&self, pool: &PgPool, lead_id: Uuid) -> Result<Option<LeadForConvertRow>, sqlx::Error> {
        let row = company_scope::fetch_optional_row_scoped(
            pool,
            sqlx::query(
                r#"SELECT company_id, lead_name, organization_name, phone, whatsapp_no, email,
                          status::text AS status, party_id
                   FROM lead.leads WHERE id=$1 AND (metadata->>'deleted_at') IS NULL"#,
            )
            .bind(lead_id),
        )
        .await?;
        Ok(row.map(|r| LeadForConvertRow {
            company_id: r.get("company_id"),
            lead_name: r.get("lead_name"),
            organization_name: r.get("organization_name"),
            phone: r.get("phone"),
            whatsapp_no: r.get("whatsapp_no"),
            email: r.get("email"),
            status: r.get("status"),
            party_id: r.get("party_id"),
        }))
    }

    /// Advance a lead to qualified — but keep 'converted' if it already was (the status IN filter is
    /// what preserves that).
    ///
    /// Takes the CALLER'S connection so it commits with the opportunity it was qualified into. The
    /// caller has already bound the company on that connection — don't re-bind here.
    pub async fn mark_qualified(&self, conn: &mut sqlx::PgConnection, lead_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE lead.leads SET status='qualified'::lead_status
               WHERE id=$1 AND status IN ('new','contacted','qualified')"#,
        )
        .bind(lead_id)
        .execute(conn)
        .await?;
        Ok(())
    }

    /// Claim the conversion exactly once (CAS on `party_id IS NULL`). Returns rows affected
    /// (0 = a concurrent conversion won; the caller re-reads the winner's party).
    ///
    /// convert-once fix: `party_id` is the resolved-customer anchor. The CAS is per-lead (a lead
    /// resolves at most once), but the same party may be claimed by other leads over time — a
    /// converted Customer can re-enter the pipeline as a new Lead.
    ///
    /// Takes the CALLER'S connection so the claim and the opportunity back-fill commit as one unit. The
    /// caller has already bound the company on that connection — don't re-bind here.
    pub async fn claim_conversion(
        &self,
        conn: &mut sqlx::PgConnection,
        lead_id: Uuid,
        party_id: Uuid,
    ) -> Result<u64, sqlx::Error> {
        let moved = sqlx::query(
            r#"UPDATE lead.leads
               SET party_id=$2, status='converted'::lead_status, converted_at=now()
               WHERE id=$1 AND party_id IS NULL"#,
        )
        .bind(lead_id)
        .bind(party_id)
        .execute(conn)
        .await?;
        Ok(moved.rows_affected())
    }

    /// Re-read the winner's party id after a losing conversion CAS.
    ///
    /// A read outside the (rolled-back) transaction: `fetch_one_scalar_scoped` applies the RLS fence.
    /// The caller wraps this in `with_company_scope(Some(company_id))`.
    pub async fn fetch_party_id(&self, pool: &PgPool, lead_id: Uuid) -> Result<Uuid, sqlx::Error> {
        company_scope::fetch_one_scalar_scoped(
            pool,
            sqlx::query_scalar("SELECT party_id FROM lead.leads WHERE id=$1").bind(lead_id),
        )
        .await
    }
}

backbone_core::impl_crud_repository!(LeadRepository, Lead, soft_delete);
