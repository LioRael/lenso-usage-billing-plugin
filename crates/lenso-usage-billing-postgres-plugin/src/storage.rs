use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct MeterView {
    pub mapping_id: String,
    pub source_scope_kind: String,
    pub source_scope_id: String,
    pub source_subject: String,
    pub source_meter: String,
    pub sink_meter: String,
    pub unit_price_minor: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct AccountView {
    pub organization_id: String,
    pub account_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub currency: String,
    pub status: String,
    pub revision: String,
    pub created_at: String,
    pub updated_at: String,
    pub meters: Vec<MeterView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct LineView {
    pub delivery_id: String,
    pub mapping_id: String,
    pub source_meter: String,
    pub sink_meter: String,
    pub quantity: String,
    pub usage_revision: i64,
    pub unit_price_minor: String,
    pub amount_minor: String,
    pub status: String,
    pub attempts: i64,
    pub provider_reference: Option<String>,
    pub failure_code: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PeriodView {
    pub organization_id: String,
    pub account_id: String,
    pub period_id: String,
    pub period_start: String,
    pub period_end: String,
    pub currency: String,
    pub status: String,
    pub account_revision: String,
    pub total_amount_minor: String,
    pub created_at: String,
    pub lines: Vec<LineView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DeliveryView {
    pub organization_id: String,
    pub account_id: String,
    pub period_id: String,
    pub delivery_id: String,
    pub mapping_id: String,
    pub status: String,
    pub attempts: i64,
    pub provider_reference: Option<String>,
    pub failure_code: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryClaim {
    pub period_id: String,
    pub delivery_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub sink_meter: String,
    pub quantity: String,
    pub occurred_at: String,
    pub attempt: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotLine {
    pub delivery_id: String,
    pub mapping_id: String,
    pub source_meter: String,
    pub sink_meter: String,
    pub quantity: String,
    pub usage_revision: i64,
    pub unit_price_minor: String,
    pub amount_minor: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DomainFailure {
    NotFound,
    RevisionConflict,
    IdempotencyConflict,
    PeriodConflict,
    AccountDisabled,
    DeliveryNotFound,
    InvalidResolution,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StorageError {
    #[error("usage billing domain failure: {0:?}")]
    Domain(DomainFailure),
    #[error("PostgreSQL operation `{operation}` failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("stored Usage Billing value is invalid: {0}")]
    InvalidStored(String),
    #[error("Usage Billing response serialization failed")]
    Serialization(#[from] serde_json::Error),
}

impl From<DomainFailure> for StorageError {
    fn from(value: DomainFailure) -> Self {
        Self::Domain(value)
    }
}

fn database(operation: &'static str, source: sqlx::Error) -> StorageError {
    StorageError::Database { operation, source }
}

pub(crate) struct PutAccount<'a> {
    pub caller: &'a str,
    pub actor: &'a str,
    pub organization_id: &'a str,
    pub account_id: &'a str,
    pub scope_kind: &'a str,
    pub scope_id: &'a str,
    pub subject: &'a str,
    pub currency: &'a str,
    pub status: &'a str,
    pub expected_revision: i64,
    pub idempotency_key: &'a str,
    pub request_hash: &'a [u8],
    pub meters: &'a [MeterView],
}

pub(crate) async fn put_account(
    postgres: &OwnedPostgres,
    input: PutAccount<'_>,
) -> Result<AccountView, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin put billing account", source))?;
    if let Some(response) = replay(
        &mut transaction,
        input.caller,
        input.actor,
        "put_account",
        input.idempotency_key,
        input.request_hash,
    )
    .await?
    {
        return serde_json::from_value(response).map_err(Into::into);
    }
    let existing = sqlx::query(
        "SELECT organization_id,revision FROM billing_accounts WHERE account_id=$1 FOR UPDATE",
    )
    .bind(input.account_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|source| database("lock billing account", source))?;
    match existing {
        None if input.expected_revision == 0 => {
            sqlx::query("INSERT INTO billing_accounts(account_id,organization_id,scope_kind,scope_id,subject,currency,status,revision) VALUES($1,$2,$3,$4,$5,$6,$7,1)")
                .bind(input.account_id).bind(input.organization_id).bind(input.scope_kind)
                .bind(input.scope_id).bind(input.subject).bind(input.currency).bind(input.status)
                .execute(&mut *transaction).await
                .map_err(|source| database("insert billing account", source))?;
        }
        Some(row) => {
            let organization_id: String = row
                .try_get("organization_id")
                .map_err(|source| database("decode billing account organization", source))?;
            let revision: i64 = row
                .try_get("revision")
                .map_err(|source| database("decode billing account revision", source))?;
            if organization_id != input.organization_id {
                return Err(DomainFailure::NotFound.into());
            }
            if revision != input.expected_revision {
                return Err(DomainFailure::RevisionConflict.into());
            }
            sqlx::query("UPDATE billing_accounts SET scope_kind=$2,scope_id=$3,subject=$4,currency=$5,status=$6,revision=revision+1,updated_at=transaction_timestamp() WHERE account_id=$1")
                .bind(input.account_id).bind(input.scope_kind).bind(input.scope_id).bind(input.subject)
                .bind(input.currency).bind(input.status).execute(&mut *transaction).await
                .map_err(|source| database("update billing account", source))?;
        }
        None => return Err(DomainFailure::RevisionConflict.into()),
    }
    sqlx::query("DELETE FROM billing_meter_mappings WHERE account_id=$1")
        .bind(input.account_id)
        .execute(&mut *transaction)
        .await
        .map_err(|source| database("replace billing meter mappings", source))?;
    for meter in input.meters {
        sqlx::query("INSERT INTO billing_meter_mappings(account_id,mapping_id,source_scope_kind,source_scope_id,source_subject,source_meter,sink_meter,unit_price_minor) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(input.account_id).bind(&meter.mapping_id).bind(&meter.source_scope_kind)
            .bind(&meter.source_scope_id).bind(&meter.source_subject).bind(&meter.source_meter)
            .bind(&meter.sink_meter).bind(&meter.unit_price_minor)
            .execute(&mut *transaction).await
            .map_err(|source| database("insert billing meter mapping", source))?;
    }
    let view = load_account_tx(&mut transaction, input.organization_id, input.account_id).await?;
    store_command(
        &mut transaction,
        input.caller,
        input.actor,
        "put_account",
        input.idempotency_key,
        input.request_hash,
        &serde_json::to_value(&view)?,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit put billing account", source))?;
    Ok(view)
}

pub(crate) async fn get_account(
    postgres: &OwnedPostgres,
    organization_id: &str,
    account_id: &str,
) -> Result<AccountView, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin read billing account", source))?;
    let view = load_account_tx(&mut transaction, organization_id, account_id).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit read billing account", source))?;
    Ok(view)
}

pub(crate) async fn list_accounts(
    postgres: &OwnedPostgres,
    organization_id: &str,
    cursor: Option<&str>,
    limit: i64,
) -> Result<(Vec<AccountView>, Option<String>), StorageError> {
    let rows = sqlx::query("SELECT account_id FROM billing_accounts WHERE organization_id=$1 AND ($2::text IS NULL OR account_id>$2) ORDER BY account_id LIMIT $3")
        .bind(organization_id).bind(cursor).bind(limit + 1)
        .fetch_all(postgres.pool()).await
        .map_err(|source| database("list billing accounts", source))?;
    let has_more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
    let mut accounts = Vec::new();
    for row in rows.into_iter().take(usize::try_from(limit).unwrap_or(200)) {
        let account_id: String = row
            .try_get("account_id")
            .map_err(|source| database("decode listed account", source))?;
        accounts.push(get_account(postgres, organization_id, &account_id).await?);
    }
    let next = has_more
        .then(|| accounts.last().map(|account| account.account_id.clone()))
        .flatten();
    Ok((accounts, next))
}

pub(crate) struct ClosePeriod<'a> {
    pub caller: &'a str,
    pub actor: &'a str,
    pub organization_id: &'a str,
    pub account_id: &'a str,
    pub period_id: &'a str,
    pub period_start: OffsetDateTime,
    pub period_end: OffsetDateTime,
    pub expected_account_revision: i64,
    pub idempotency_key: &'a str,
    pub request_hash: &'a [u8],
    pub total_amount_minor: &'a str,
    pub lines: &'a [SnapshotLine],
}

pub(crate) async fn close_period(
    postgres: &OwnedPostgres,
    input: ClosePeriod<'_>,
) -> Result<PeriodView, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin close billing period", source))?;
    if let Some(response) = replay(
        &mut transaction,
        input.caller,
        input.actor,
        "close_period",
        input.idempotency_key,
        input.request_hash,
    )
    .await?
    {
        return serde_json::from_value(response).map_err(Into::into);
    }
    let row = sqlx::query("SELECT organization_id,currency,status,revision FROM billing_accounts WHERE account_id=$1 FOR UPDATE")
        .bind(input.account_id).fetch_optional(&mut *transaction).await
        .map_err(|source| database("lock account for billing period", source))?
        .ok_or(DomainFailure::NotFound)?;
    let organization_id: String = row
        .try_get("organization_id")
        .map_err(|source| database("decode period organization", source))?;
    if organization_id != input.organization_id {
        return Err(DomainFailure::NotFound.into());
    }
    let account_status: String = row
        .try_get("status")
        .map_err(|source| database("decode period account status", source))?;
    if account_status != "active" {
        return Err(DomainFailure::AccountDisabled.into());
    }
    let revision: i64 = row
        .try_get("revision")
        .map_err(|source| database("decode period account revision", source))?;
    if revision != input.expected_account_revision {
        return Err(DomainFailure::RevisionConflict.into());
    }
    let overlap = sqlx::query("SELECT period_id FROM billing_periods WHERE account_id=$1 AND period_start<$3 AND period_end>$2 LIMIT 1")
        .bind(input.account_id).bind(input.period_start).bind(input.period_end)
        .fetch_optional(&mut *transaction).await
        .map_err(|source| database("detect overlapping billing period", source))?;
    if overlap.is_some() {
        return Err(DomainFailure::PeriodConflict.into());
    }
    let currency: String = row
        .try_get("currency")
        .map_err(|source| database("decode period currency", source))?;
    let status = if input.lines.is_empty() {
        "delivered"
    } else {
        "pending"
    };
    sqlx::query("INSERT INTO billing_periods(period_id,account_id,organization_id,period_start,period_end,currency,status,account_revision,total_amount_minor) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(input.period_id).bind(input.account_id).bind(input.organization_id)
        .bind(input.period_start).bind(input.period_end).bind(&currency).bind(status)
        .bind(revision).bind(input.total_amount_minor).execute(&mut *transaction).await
        .map_err(|source| {
            if unique_violation(&source) { StorageError::Domain(DomainFailure::PeriodConflict) }
            else { database("insert billing period", source) }
        })?;
    for line in input.lines {
        sqlx::query("INSERT INTO billing_period_lines(delivery_id,period_id,mapping_id,source_meter,sink_meter,quantity,usage_revision,unit_price_minor,amount_minor,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')")
            .bind(&line.delivery_id).bind(input.period_id).bind(&line.mapping_id)
            .bind(&line.source_meter).bind(&line.sink_meter).bind(&line.quantity)
            .bind(line.usage_revision).bind(&line.unit_price_minor).bind(&line.amount_minor)
            .execute(&mut *transaction).await
            .map_err(|source| database("insert billing period line", source))?;
    }
    let view = load_period_tx(&mut transaction, input.organization_id, input.period_id).await?;
    store_command(
        &mut transaction,
        input.caller,
        input.actor,
        "close_period",
        input.idempotency_key,
        input.request_hash,
        &serde_json::to_value(&view)?,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit billing period", source))?;
    Ok(view)
}

pub(crate) async fn get_period(
    postgres: &OwnedPostgres,
    organization_id: &str,
    period_id: &str,
) -> Result<PeriodView, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin read billing period", source))?;
    let view = load_period_tx(&mut transaction, organization_id, period_id).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit read billing period", source))?;
    Ok(view)
}

pub(crate) async fn list_periods(
    postgres: &OwnedPostgres,
    organization_id: &str,
    account_id: &str,
    cursor: Option<&str>,
    limit: i64,
) -> Result<(Vec<PeriodView>, Option<String>), StorageError> {
    let rows = sqlx::query("SELECT period_id FROM billing_periods WHERE organization_id=$1 AND account_id=$2 AND ($3::text IS NULL OR period_id>$3) ORDER BY period_id LIMIT $4")
        .bind(organization_id).bind(account_id).bind(cursor).bind(limit + 1)
        .fetch_all(postgres.pool()).await.map_err(|source| database("list billing periods", source))?;
    let has_more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
    let mut periods = Vec::new();
    for row in rows.into_iter().take(usize::try_from(limit).unwrap_or(200)) {
        let period_id: String = row
            .try_get("period_id")
            .map_err(|source| database("decode listed period", source))?;
        periods.push(get_period(postgres, organization_id, &period_id).await?);
    }
    let next = has_more
        .then(|| periods.last().map(|period| period.period_id.clone()))
        .flatten();
    Ok((periods, next))
}

pub(crate) async fn claim_next_delivery(
    postgres: &OwnedPostgres,
    worker_id: &str,
    lease_seconds: i64,
    max_attempts: i32,
) -> Result<Option<DeliveryClaim>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin claim billing delivery", source))?;
    let exhausted = sqlx::query("UPDATE billing_period_lines SET status='failed',failure_code='attempts_exhausted',lease_owner=NULL,lease_expires_at=NULL,updated_at=transaction_timestamp() WHERE status='pending' AND attempts >= $1 RETURNING period_id")
        .bind(max_attempts).fetch_all(&mut *transaction).await
        .map_err(|source| database("fail exhausted billing deliveries", source))?;
    for row in exhausted {
        let period_id: String = row
            .try_get("period_id")
            .map_err(|source| database("decode exhausted period", source))?;
        refresh_period_status(&mut transaction, &period_id).await?;
    }
    let row = sqlx::query("WITH candidate AS (SELECT l.delivery_id FROM billing_period_lines l WHERE (l.status='pending' OR (l.status='in_flight' AND l.lease_expires_at<=transaction_timestamp())) AND l.attempts<$1 ORDER BY l.updated_at,l.delivery_id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE billing_period_lines l SET status='in_flight',attempts=attempts+1,lease_owner=$2,lease_expires_at=transaction_timestamp()+make_interval(secs=>$3::double precision),failure_code=NULL,updated_at=transaction_timestamp() FROM candidate WHERE l.delivery_id=candidate.delivery_id RETURNING l.delivery_id")
        .bind(max_attempts).bind(worker_id).bind(lease_seconds)
        .fetch_optional(&mut *transaction).await
        .map_err(|source| database("claim billing delivery", source))?;
    let Some(row) = row else {
        transaction
            .commit()
            .await
            .map_err(|source| database("commit idle delivery claim", source))?;
        return Ok(None);
    };
    let delivery_id: String = row
        .try_get("delivery_id")
        .map_err(|source| database("decode claimed delivery", source))?;
    let claim_row = sqlx::query("SELECT p.organization_id,p.account_id,p.period_id,p.period_end,a.scope_kind,a.scope_id,a.subject,l.delivery_id,l.sink_meter,l.quantity,l.attempts FROM billing_period_lines l JOIN billing_periods p ON p.period_id=l.period_id JOIN billing_accounts a ON a.account_id=p.account_id WHERE l.delivery_id=$1")
        .bind(&delivery_id).fetch_one(&mut *transaction).await
        .map_err(|source| database("load claimed billing delivery", source))?;
    let period_end: OffsetDateTime = claim_row
        .try_get("period_end")
        .map_err(|source| database("decode delivery occurrence", source))?;
    let claim = DeliveryClaim {
        period_id: claim_row
            .try_get("period_id")
            .map_err(|source| database("decode delivery period", source))?,
        delivery_id: claim_row
            .try_get("delivery_id")
            .map_err(|source| database("decode delivery id", source))?,
        scope_kind: claim_row
            .try_get("scope_kind")
            .map_err(|source| database("decode delivery scope kind", source))?,
        scope_id: claim_row
            .try_get("scope_id")
            .map_err(|source| database("decode delivery scope id", source))?,
        subject: claim_row
            .try_get("subject")
            .map_err(|source| database("decode delivery subject", source))?,
        sink_meter: claim_row
            .try_get("sink_meter")
            .map_err(|source| database("decode delivery meter", source))?,
        quantity: claim_row
            .try_get("quantity")
            .map_err(|source| database("decode delivery quantity", source))?,
        occurred_at: format_time(period_end)?,
        attempt: claim_row
            .try_get("attempts")
            .map_err(|source| database("decode delivery attempt", source))?,
    };
    transaction
        .commit()
        .await
        .map_err(|source| database("commit delivery claim", source))?;
    Ok(Some(claim))
}

pub(crate) async fn complete_delivery(
    postgres: &OwnedPostgres,
    delivery_id: &str,
    worker_id: &str,
    attempt: i32,
    status: &str,
    provider_reference: Option<&str>,
    failure_code: Option<&str>,
) -> Result<(), StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin complete billing delivery", source))?;
    let row = sqlx::query("UPDATE billing_period_lines SET status=$2,lease_owner=NULL,lease_expires_at=NULL,provider_reference=$3,failure_code=$4,updated_at=transaction_timestamp() WHERE delivery_id=$1 AND status='in_flight' AND lease_owner=$5 AND attempts=$6 RETURNING period_id")
        .bind(delivery_id).bind(status).bind(provider_reference).bind(failure_code).bind(worker_id).bind(attempt)
        .fetch_optional(&mut *transaction).await.map_err(|source| database("complete billing delivery", source))?
        .ok_or(DomainFailure::DeliveryNotFound)?;
    let period_id: String = row
        .try_get("period_id")
        .map_err(|source| database("decode completed period", source))?;
    refresh_period_status(&mut transaction, &period_id).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit completed delivery", source))?;
    Ok(())
}

pub(crate) async fn inspect_delivery(
    postgres: &OwnedPostgres,
    organization_id: &str,
    delivery_id: &str,
) -> Result<DeliveryView, StorageError> {
    let row = sqlx::query("SELECT p.organization_id,p.account_id,p.period_id,l.delivery_id,l.mapping_id,l.status,l.attempts,l.provider_reference,l.failure_code,l.updated_at FROM billing_period_lines l JOIN billing_periods p ON p.period_id=l.period_id WHERE p.organization_id=$1 AND l.delivery_id=$2")
        .bind(organization_id).bind(delivery_id).fetch_optional(postgres.pool()).await
        .map_err(|source| database("inspect billing delivery", source))?
        .ok_or(DomainFailure::DeliveryNotFound)?;
    delivery_from_row(&row)
}

pub(crate) struct ResolveDelivery<'a> {
    pub caller: &'a str,
    pub actor: &'a str,
    pub organization_id: &'a str,
    pub delivery_id: &'a str,
    pub resolution: &'a str,
    pub provider_reference: Option<&'a str>,
    pub reason: &'a str,
    pub idempotency_key: &'a str,
    pub request_hash: &'a [u8],
}

pub(crate) async fn resolve_delivery(
    postgres: &OwnedPostgres,
    input: ResolveDelivery<'_>,
) -> Result<DeliveryView, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin resolve billing delivery", source))?;
    if let Some(response) = replay(
        &mut transaction,
        input.caller,
        input.actor,
        "resolve_delivery",
        input.idempotency_key,
        input.request_hash,
    )
    .await?
    {
        return serde_json::from_value(response).map_err(Into::into);
    }
    let row = sqlx::query("SELECT p.period_id,l.status FROM billing_period_lines l JOIN billing_periods p ON p.period_id=l.period_id WHERE p.organization_id=$1 AND l.delivery_id=$2 FOR UPDATE OF l")
        .bind(input.organization_id).bind(input.delivery_id).fetch_optional(&mut *transaction).await
        .map_err(|source| database("lock billing delivery resolution", source))?
        .ok_or(DomainFailure::DeliveryNotFound)?;
    let current: String = row
        .try_get("status")
        .map_err(|source| database("decode delivery resolution state", source))?;
    if !matches!(current.as_str(), "failed" | "unknown") {
        return Err(DomainFailure::InvalidResolution.into());
    }
    let period_id: String = row
        .try_get("period_id")
        .map_err(|source| database("decode delivery resolution period", source))?;
    let (status, provider_reference, failure_code) = match input.resolution {
        "retry" => ("pending", None, None),
        "delivered" if input.provider_reference.is_some() => {
            ("delivered", input.provider_reference, None)
        }
        "failed" => ("failed", input.provider_reference, Some(input.reason)),
        _ => return Err(DomainFailure::InvalidResolution.into()),
    };
    sqlx::query("UPDATE billing_period_lines SET status=$2,lease_owner=NULL,lease_expires_at=NULL,provider_reference=$3,failure_code=$4,updated_at=transaction_timestamp() WHERE delivery_id=$1")
        .bind(input.delivery_id).bind(status).bind(provider_reference).bind(failure_code)
        .execute(&mut *transaction).await.map_err(|source| database("apply billing delivery resolution", source))?;
    refresh_period_status(&mut transaction, &period_id).await?;
    let row = sqlx::query("SELECT p.organization_id,p.account_id,p.period_id,l.delivery_id,l.mapping_id,l.status,l.attempts,l.provider_reference,l.failure_code,l.updated_at FROM billing_period_lines l JOIN billing_periods p ON p.period_id=l.period_id WHERE l.delivery_id=$1")
        .bind(input.delivery_id).fetch_one(&mut *transaction).await
        .map_err(|source| database("load resolved billing delivery", source))?;
    let view = delivery_from_row(&row)?;
    store_command(
        &mut transaction,
        input.caller,
        input.actor,
        "resolve_delivery",
        input.idempotency_key,
        input.request_hash,
        &serde_json::to_value(&view)?,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit billing delivery resolution", source))?;
    Ok(view)
}

async fn load_account_tx(
    transaction: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    account_id: &str,
) -> Result<AccountView, StorageError> {
    let row = sqlx::query("SELECT organization_id,account_id,scope_kind,scope_id,subject,currency,status,revision,created_at,updated_at FROM billing_accounts WHERE organization_id=$1 AND account_id=$2")
        .bind(organization_id).bind(account_id).fetch_optional(&mut **transaction).await
        .map_err(|source| database("read billing account", source))?
        .ok_or(DomainFailure::NotFound)?;
    let meters = load_meters_tx(transaction, account_id).await?;
    Ok(AccountView {
        organization_id: row
            .try_get("organization_id")
            .map_err(|source| database("decode account organization", source))?,
        account_id: row
            .try_get("account_id")
            .map_err(|source| database("decode account id", source))?,
        scope_kind: row
            .try_get("scope_kind")
            .map_err(|source| database("decode account scope kind", source))?,
        scope_id: row
            .try_get("scope_id")
            .map_err(|source| database("decode account scope id", source))?,
        subject: row
            .try_get("subject")
            .map_err(|source| database("decode account subject", source))?,
        currency: row
            .try_get("currency")
            .map_err(|source| database("decode account currency", source))?,
        status: row
            .try_get("status")
            .map_err(|source| database("decode account status", source))?,
        revision: row
            .try_get::<i64, _>("revision")
            .map_err(|source| database("decode account revision", source))?
            .to_string(),
        created_at: format_time(
            row.try_get("created_at")
                .map_err(|source| database("decode account creation", source))?,
        )?,
        updated_at: format_time(
            row.try_get("updated_at")
                .map_err(|source| database("decode account update", source))?,
        )?,
        meters,
    })
}

async fn load_meters_tx(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<Vec<MeterView>, StorageError> {
    let rows = sqlx::query("SELECT mapping_id,source_scope_kind,source_scope_id,source_subject,source_meter,sink_meter,unit_price_minor FROM billing_meter_mappings WHERE account_id=$1 ORDER BY mapping_id")
        .bind(account_id).fetch_all(&mut **transaction).await
        .map_err(|source| database("read billing meter mappings", source))?;
    rows.into_iter()
        .map(|row| {
            Ok(MeterView {
                mapping_id: row
                    .try_get("mapping_id")
                    .map_err(|source| database("decode mapping id", source))?,
                source_scope_kind: row
                    .try_get("source_scope_kind")
                    .map_err(|source| database("decode source scope kind", source))?,
                source_scope_id: row
                    .try_get("source_scope_id")
                    .map_err(|source| database("decode source scope id", source))?,
                source_subject: row
                    .try_get("source_subject")
                    .map_err(|source| database("decode source subject", source))?,
                source_meter: row
                    .try_get("source_meter")
                    .map_err(|source| database("decode source meter", source))?,
                sink_meter: row
                    .try_get("sink_meter")
                    .map_err(|source| database("decode sink meter", source))?,
                unit_price_minor: row
                    .try_get("unit_price_minor")
                    .map_err(|source| database("decode unit price", source))?,
            })
        })
        .collect()
}

async fn load_period_tx(
    transaction: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    period_id: &str,
) -> Result<PeriodView, StorageError> {
    let row = sqlx::query("SELECT organization_id,account_id,period_id,period_start,period_end,currency,status,account_revision,total_amount_minor,created_at FROM billing_periods WHERE organization_id=$1 AND period_id=$2")
        .bind(organization_id).bind(period_id).fetch_optional(&mut **transaction).await
        .map_err(|source| database("read billing period", source))?
        .ok_or(DomainFailure::NotFound)?;
    let line_rows = sqlx::query("SELECT delivery_id,mapping_id,source_meter,sink_meter,quantity,usage_revision,unit_price_minor,amount_minor,status,attempts,provider_reference,failure_code FROM billing_period_lines WHERE period_id=$1 ORDER BY mapping_id")
        .bind(period_id).fetch_all(&mut **transaction).await
        .map_err(|source| database("read billing period lines", source))?;
    let mut lines = Vec::with_capacity(line_rows.len());
    for line in line_rows {
        lines.push(LineView {
            delivery_id: line
                .try_get("delivery_id")
                .map_err(|source| database("decode period delivery id", source))?,
            mapping_id: line
                .try_get("mapping_id")
                .map_err(|source| database("decode period mapping id", source))?,
            source_meter: line
                .try_get("source_meter")
                .map_err(|source| database("decode period source meter", source))?,
            sink_meter: line
                .try_get("sink_meter")
                .map_err(|source| database("decode period sink meter", source))?,
            quantity: line
                .try_get("quantity")
                .map_err(|source| database("decode period quantity", source))?,
            usage_revision: line
                .try_get("usage_revision")
                .map_err(|source| database("decode usage revision", source))?,
            unit_price_minor: line
                .try_get("unit_price_minor")
                .map_err(|source| database("decode period unit price", source))?,
            amount_minor: line
                .try_get("amount_minor")
                .map_err(|source| database("decode period amount", source))?,
            status: line
                .try_get("status")
                .map_err(|source| database("decode period line status", source))?,
            attempts: i64::from(
                line.try_get::<i32, _>("attempts")
                    .map_err(|source| database("decode period attempts", source))?,
            ),
            provider_reference: line
                .try_get("provider_reference")
                .map_err(|source| database("decode provider reference", source))?,
            failure_code: line
                .try_get("failure_code")
                .map_err(|source| database("decode failure code", source))?,
        });
    }
    let period_start: OffsetDateTime = row
        .try_get("period_start")
        .map_err(|source| database("decode period start", source))?;
    let period_end: OffsetDateTime = row
        .try_get("period_end")
        .map_err(|source| database("decode period end", source))?;
    Ok(PeriodView {
        organization_id: row
            .try_get("organization_id")
            .map_err(|source| database("decode period organization", source))?,
        account_id: row
            .try_get("account_id")
            .map_err(|source| database("decode period account", source))?,
        period_id: row
            .try_get("period_id")
            .map_err(|source| database("decode period id", source))?,
        period_start: format_time(period_start)?,
        period_end: format_time(period_end)?,
        currency: row
            .try_get("currency")
            .map_err(|source| database("decode period currency", source))?,
        status: row
            .try_get("status")
            .map_err(|source| database("decode period status", source))?,
        account_revision: row
            .try_get::<i64, _>("account_revision")
            .map_err(|source| database("decode period account revision", source))?
            .to_string(),
        total_amount_minor: row
            .try_get("total_amount_minor")
            .map_err(|source| database("decode period total", source))?,
        created_at: format_time(
            row.try_get("created_at")
                .map_err(|source| database("decode period creation", source))?,
        )?,
        lines,
    })
}

async fn refresh_period_status(
    transaction: &mut Transaction<'_, Postgres>,
    period_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE billing_periods p SET status=CASE WHEN EXISTS(SELECT 1 FROM billing_period_lines l WHERE l.period_id=p.period_id AND l.status='unknown') THEN 'unknown' WHEN EXISTS(SELECT 1 FROM billing_period_lines l WHERE l.period_id=p.period_id AND l.status='failed') THEN 'failed' WHEN EXISTS(SELECT 1 FROM billing_period_lines l WHERE l.period_id=p.period_id AND l.status<>'delivered') THEN 'pending' ELSE 'delivered' END WHERE p.period_id=$1")
        .bind(period_id).execute(&mut **transaction).await
        .map_err(|source| database("refresh billing period status", source))?;
    Ok(())
}

fn delivery_from_row(row: &sqlx::postgres::PgRow) -> Result<DeliveryView, StorageError> {
    Ok(DeliveryView {
        organization_id: row
            .try_get("organization_id")
            .map_err(|source| database("decode delivery organization", source))?,
        account_id: row
            .try_get("account_id")
            .map_err(|source| database("decode delivery account", source))?,
        period_id: row
            .try_get("period_id")
            .map_err(|source| database("decode delivery period", source))?,
        delivery_id: row
            .try_get("delivery_id")
            .map_err(|source| database("decode delivery id", source))?,
        mapping_id: row
            .try_get("mapping_id")
            .map_err(|source| database("decode delivery mapping", source))?,
        status: row
            .try_get("status")
            .map_err(|source| database("decode delivery status", source))?,
        attempts: i64::from(
            row.try_get::<i32, _>("attempts")
                .map_err(|source| database("decode delivery attempts", source))?,
        ),
        provider_reference: row
            .try_get("provider_reference")
            .map_err(|source| database("decode delivery provider reference", source))?,
        failure_code: row
            .try_get("failure_code")
            .map_err(|source| database("decode delivery failure", source))?,
        updated_at: format_time(
            row.try_get("updated_at")
                .map_err(|source| database("decode delivery update", source))?,
        )?,
    })
}

async fn replay(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    actor: &str,
    operation: &str,
    idempotency_key: &str,
    request_hash: &[u8],
) -> Result<Option<Value>, StorageError> {
    let row = sqlx::query("SELECT request_hash,response FROM billing_commands WHERE caller_instance=$1 AND actor_subject=$2 AND operation=$3 AND idempotency_key=$4")
        .bind(caller).bind(actor).bind(operation).bind(idempotency_key)
        .fetch_optional(&mut **transaction).await
        .map_err(|source| database("read billing command", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let stored_hash: Vec<u8> = row
        .try_get("request_hash")
        .map_err(|source| database("decode billing command hash", source))?;
    if stored_hash != request_hash {
        return Err(DomainFailure::IdempotencyConflict.into());
    }
    row.try_get("response")
        .map(Some)
        .map_err(|source| database("decode billing command response", source))
}

async fn store_command(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    actor: &str,
    operation: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    response: &Value,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO billing_commands(caller_instance,actor_subject,operation,idempotency_key,request_hash,response) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(caller).bind(actor).bind(operation).bind(idempotency_key).bind(request_hash).bind(response)
        .execute(&mut **transaction).await
        .map_err(|source| database("store billing command", source))?;
    Ok(())
}

fn format_time(value: OffsetDateTime) -> Result<String, StorageError> {
    value
        .format(&Rfc3339)
        .map_err(|error| StorageError::InvalidStored(error.to_string()))
}

fn unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}
