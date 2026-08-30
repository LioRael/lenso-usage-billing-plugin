//! PostgreSQL-backed Usage Billing snapshots and provider-neutral delivery outbox.

mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod schema;
mod storage;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration};

use lenso::prelude::*;
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionVerifier, ActorProjectionError, AssertionClock, TypedActor,
};
use lenso_capability_access_control as access;
use lenso_capability_access_control::{
    AccessControlInvocationError, CheckPermissionRequest, CheckPermissionRequestScope,
};
use lenso_capability_billing_meter_sink as sink;
use lenso_capability_billing_meter_sink::{
    BillingMeterSinkInvocationError, PublishMeterEventError, PublishMeterEventRequest,
};
use lenso_capability_organization_membership as membership;
use lenso_capability_organization_membership::{
    CheckMembershipRequest, OrganizationMembershipInvocationError,
};
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_capability_usage_billing as billing;
use lenso_capability_usage_billing::{
    AccountResponse, ClosePeriodError, ClosePeriodRequest, DeliveryResponse, GetAccountError,
    GetAccountRequest, GetPeriodError, GetPeriodRequest, InspectDeliveryError,
    InspectDeliveryRequest, ListAccountsError, ListAccountsRequest, ListAccountsResponse,
    ListPeriodsError, ListPeriodsRequest, ListPeriodsResponse, PeriodResponse, PutAccountError,
    PutAccountRequest, PutAccountRequestStatus, ReconcileNextError, ReconcileNextRequest,
    ReconcileNextResponse, ReconcileNextResponseStatus, ResolveDeliveryError,
    ResolveDeliveryRequest, ResolveDeliveryRequestResolution,
};
use lenso_capability_usage_meter as usage;
use lenso_capability_usage_meter::{
    ReadUsageWindowRequest, UsageMeterReadUsageWindowInvocationError,
};
use lenso_kernel::{PluginDependencies, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use zeroize::Zeroizing;

use crate::storage::{DomainFailure, StorageError};

pub use operator::{UsageBillingOperator, UsageBillingOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLERS: usize = 64;
const MAX_ID_BYTES: usize = 512;
const MAX_REASON_BYTES: usize = 2_000;
const MAX_IDEMPOTENCY_BYTES: usize = 200;
const MAX_PAGE_SIZE: i64 = 200;

const BILLING_READ: &str = "billing.accounts.read";
const BILLING_WRITE: &str = "billing.accounts.write";
const BILLING_CLOSE: &str = "billing.periods.close";
const BILLING_RESOLVE: &str = "billing.deliveries.resolve";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UsageBillingConfig {
    schema: String,
    database_url_secret: String,
    auth_issuer: String,
    auth_assertion_public_key: String,
    admin_callers: Vec<String>,
    worker_callers: Vec<String>,
    delivery_lease_seconds: i64,
    max_delivery_attempts: i32,
    max_meters_per_account: usize,
}

impl UsageBillingConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        auth_issuer: impl Into<String>,
        auth_assertion_public_key: impl Into<String>,
        admin_callers: Vec<String>,
        worker_callers: Vec<String>,
        delivery_lease_seconds: i64,
        max_delivery_attempts: i32,
        max_meters_per_account: usize,
    ) -> Result<Self, UsageBillingConfigError> {
        let config = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            auth_issuer: auth_issuer.into(),
            auth_assertion_public_key: auth_assertion_public_key.into(),
            admin_callers,
            worker_callers,
            delivery_lease_seconds,
            max_delivery_attempts,
            max_meters_per_account,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), UsageBillingConfigError> {
        schema::schema_plan(self.schema.clone())
            .map_err(|_| UsageBillingConfigError::InvalidSchema)?;
        if !valid_identifier(&self.database_url_secret, 256) {
            return Err(UsageBillingConfigError::InvalidSecretReference);
        }
        if !valid_identifier(&self.auth_issuer, 256) {
            return Err(UsageBillingConfigError::InvalidAuthIssuer);
        }
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| UsageBillingConfigError::InvalidAuthPublicKey)?;
        validate_callers(&self.admin_callers)
            .map_err(|()| UsageBillingConfigError::InvalidAdminCallers)?;
        validate_callers(&self.worker_callers)
            .map_err(|()| UsageBillingConfigError::InvalidWorkerCallers)?;
        if !(5..=3_600).contains(&self.delivery_lease_seconds) {
            return Err(UsageBillingConfigError::InvalidLease);
        }
        if !(1..=100).contains(&self.max_delivery_attempts) {
            return Err(UsageBillingConfigError::InvalidAttempts);
        }
        if !(1..=200).contains(&self.max_meters_per_account) {
            return Err(UsageBillingConfigError::InvalidMeterLimit);
        }
        Ok(())
    }

    fn verifier(&self) -> Result<ActorAssertionVerifier, RuntimeFailure> {
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| RuntimeFailure::InvalidResolvedPlan {
            detail: "Usage Billing Auth verification key is invalid".to_owned(),
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UsageBillingConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("invalid Auth issuer")]
    InvalidAuthIssuer,
    #[error("invalid Auth assertion public key")]
    InvalidAuthPublicKey,
    #[error("admin_callers must contain unique exact Instance keys")]
    InvalidAdminCallers,
    #[error("worker_callers must contain unique exact Instance keys")]
    InvalidWorkerCallers,
    #[error("delivery_lease_seconds must be between 5 and 3600")]
    InvalidLease,
    #[error("max_delivery_attempts must be between 1 and 100")]
    InvalidAttempts,
    #[error("max_meters_per_account must be between 1 and 200")]
    InvalidMeterLimit,
}

fn validate_config(config: &UsageBillingConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("Usage Billing configuration is invalid: {error}"),
        })
}

#[derive(Clone, Debug)]
struct PreparedUsageBilling {
    postgres: OwnedPostgres,
}

#[lenso::plugin(
    lifecycle,
    configuration_schema = "configuration.schema.json",
    validate = validate_config
)]
#[derive(Clone)]
struct PostgresUsageBillingPlugin {
    #[config]
    config: UsageBillingConfig,
    secrets: Port<secrets::SecretsClient>,
    membership: Port<membership::OrganizationMembershipClient>,
    access: Port<access::AccessControlClient>,
    usage: Port<usage::UsageMeterClient>,
    sink: Port<sink::BillingMeterSinkClient>,
    prepared: Rc<RefCell<Option<PreparedUsageBilling>>>,
}

impl fmt::Debug for PostgresUsageBillingPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresUsageBillingPlugin")
            .field("schema", &self.config.schema)
            .field("prepared", &self.prepared.borrow().is_some())
            .field("admin_caller_count", &self.config.admin_callers.len())
            .field("worker_caller_count", &self.config.worker_callers.len())
            .finish_non_exhaustive()
    }
}

#[lenso::provides(billing::UsageBilling)]
impl PostgresUsageBillingPlugin {}

impl PostgresUsageBillingPlugin {
    async fn put_account(
        &self,
        context: Ctx,
        request: PutAccountRequest,
    ) -> PluginResult<AccountResponse, PutAccountError> {
        let (caller, actor) = self
            .authorize_admin(
                &context,
                billing::PUT_ACCOUNT_OPERATION,
                &request.organization_id,
                BILLING_WRITE,
            )
            .await
            .map_err(map_put_authorization)?;
        let expected_revision = request.expected_revision.parse::<i64>().ok();
        let status = match request.status {
            PutAccountRequestStatus::Active => "active",
            PutAccountRequestStatus::Disabled => "disabled",
        };
        let meters = validate_meters(&request, self.config.max_meters_per_account)
            .ok_or_else(|| PluginError::domain(PutAccountError::InvalidRequest))?;
        if !valid_id(&request.organization_id)
            || !valid_id(&request.account_id)
            || !valid_text(&request.scope_kind, 128, false)
            || !valid_id(&request.scope_id)
            || !valid_id(&request.subject)
            || !valid_currency(&request.currency)
            || expected_revision.is_none_or(|value| value < 0)
            || !valid_idempotency_key(&request.idempotency_key)
        {
            return Err(PluginError::domain(PutAccountError::InvalidRequest));
        }
        let request_hash = request_hash(&request)?;
        let result = storage::put_account(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            storage::PutAccount {
                caller: &caller,
                actor: &actor,
                organization_id: &request.organization_id,
                account_id: &request.account_id,
                scope_kind: &request.scope_kind,
                scope_id: &request.scope_id,
                subject: &request.subject,
                currency: &request.currency,
                status,
                expected_revision: expected_revision.unwrap_or_default(),
                idempotency_key: &request.idempotency_key,
                request_hash: &request_hash,
                meters: &meters,
            },
        )
        .await;
        wire_cast(&map_storage(result, map_put_domain)?)
    }

    async fn get_account(
        &self,
        context: Ctx,
        request: GetAccountRequest,
    ) -> PluginResult<AccountResponse, GetAccountError> {
        self.authorize_admin(
            &context,
            billing::GET_ACCOUNT_OPERATION,
            &request.organization_id,
            BILLING_READ,
        )
        .await
        .map_err(map_get_account_authorization)?;
        if !valid_id(&request.organization_id) || !valid_id(&request.account_id) {
            return Err(PluginError::domain(GetAccountError::InvalidRequest));
        }
        let result = storage::get_account(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.account_id,
        )
        .await;
        wire_cast(&map_storage(result, map_get_account_domain)?)
    }

    async fn list_accounts(
        &self,
        context: Ctx,
        request: ListAccountsRequest,
    ) -> PluginResult<ListAccountsResponse, ListAccountsError> {
        self.authorize_admin(
            &context,
            billing::LIST_ACCOUNTS_OPERATION,
            &request.organization_id,
            BILLING_READ,
        )
        .await
        .map_err(map_list_accounts_authorization)?;
        if !valid_id(&request.organization_id)
            || !(1..=MAX_PAGE_SIZE).contains(&request.limit)
            || request
                .cursor
                .as_deref()
                .is_some_and(|value| !valid_id(value))
        {
            return Err(PluginError::domain(ListAccountsError::InvalidRequest));
        }
        let result = storage::list_accounts(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            request.cursor.as_deref(),
            request.limit,
        )
        .await;
        let (accounts, next_cursor) = map_storage(result, map_list_accounts_domain)?;
        Ok(ListAccountsResponse {
            accounts: wire_cast(&accounts)?,
            next_cursor,
        })
    }

    async fn close_period(
        &self,
        context: Ctx,
        request: ClosePeriodRequest,
    ) -> PluginResult<PeriodResponse, ClosePeriodError> {
        let (caller, actor) = self
            .authorize_admin(
                &context,
                billing::CLOSE_PERIOD_OPERATION,
                &request.organization_id,
                BILLING_CLOSE,
            )
            .await
            .map_err(map_close_authorization)?;
        let period_start = OffsetDateTime::parse(&request.period_start, &Rfc3339).ok();
        let period_end = OffsetDateTime::parse(&request.period_end, &Rfc3339).ok();
        let expected_revision = request.expected_account_revision.parse::<i64>().ok();
        if !valid_id(&request.organization_id)
            || !valid_id(&request.account_id)
            || !valid_id(&request.period_id)
            || !valid_idempotency_key(&request.idempotency_key)
            || expected_revision.is_none_or(|value| value <= 0)
            || period_start
                .zip(period_end)
                .is_none_or(|(start, end)| end <= start)
        {
            return Err(PluginError::domain(ClosePeriodError::InvalidRequest));
        }
        let account = storage::get_account(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.account_id,
        )
        .await
        .map_err(|error| storage_to_plugin(error, map_close_domain))?;
        if account.status != "active" {
            return Err(PluginError::domain(ClosePeriodError::AccountDisabled));
        }
        if account.revision != request.expected_account_revision {
            return Err(PluginError::domain(ClosePeriodError::RevisionConflict));
        }
        let mut lines = Vec::with_capacity(account.meters.len());
        let mut total = BigInt::from(0);
        for meter in &account.meters {
            let usage = self
                .usage
                .read_usage_window_with_context(
                    context.clone(),
                    ReadUsageWindowRequest {
                        meter: meter.source_meter.clone(),
                        scope_id: meter.source_scope_id.clone(),
                        scope_kind: meter.source_scope_kind.clone(),
                        subject: meter.source_subject.clone(),
                        window_end: request.period_end.clone(),
                        window_start: request.period_start.clone(),
                    },
                )
                .await
                .map_err(|error| match error {
                    UsageMeterReadUsageWindowInvocationError::Domain(_) => {
                        PluginError::domain(ClosePeriodError::SourceRejected)
                    }
                    UsageMeterReadUsageWindowInvocationError::Runtime(error) => {
                        PluginError::runtime(error)
                    }
                })?;
            let quantity = usage
                .quantity
                .parse::<BigInt>()
                .map_err(|_| PluginError::domain(ClosePeriodError::SourceRejected))?;
            let unit_price = meter
                .unit_price_minor
                .parse::<BigInt>()
                .map_err(|_| PluginError::domain(ClosePeriodError::InvalidRequest))?;
            let amount = &quantity * unit_price;
            total += &amount;
            lines.push(storage::SnapshotLine {
                delivery_id: stable_delivery_id(&request.period_id, &meter.mapping_id),
                mapping_id: meter.mapping_id.clone(),
                source_meter: meter.source_meter.clone(),
                sink_meter: meter.sink_meter.clone(),
                quantity: usage.quantity,
                usage_revision: usage.aggregate_revision,
                unit_price_minor: meter.unit_price_minor.clone(),
                amount_minor: amount.to_string(),
            });
        }
        let request_hash = request_hash(&request)?;
        let result = storage::close_period(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            storage::ClosePeriod {
                caller: &caller,
                actor: &actor,
                organization_id: &request.organization_id,
                account_id: &request.account_id,
                period_id: &request.period_id,
                period_start: period_start.unwrap_or(OffsetDateTime::UNIX_EPOCH),
                period_end: period_end.unwrap_or(OffsetDateTime::UNIX_EPOCH),
                expected_account_revision: expected_revision.unwrap_or_default(),
                idempotency_key: &request.idempotency_key,
                request_hash: &request_hash,
                total_amount_minor: &total.to_string(),
                lines: &lines,
            },
        )
        .await;
        wire_cast(&map_storage(result, map_close_domain)?)
    }

    async fn get_period(
        &self,
        context: Ctx,
        request: GetPeriodRequest,
    ) -> PluginResult<PeriodResponse, GetPeriodError> {
        self.authorize_admin(
            &context,
            billing::GET_PERIOD_OPERATION,
            &request.organization_id,
            BILLING_READ,
        )
        .await
        .map_err(map_get_period_authorization)?;
        if !valid_id(&request.organization_id) || !valid_id(&request.period_id) {
            return Err(PluginError::domain(GetPeriodError::InvalidRequest));
        }
        let result = storage::get_period(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.period_id,
        )
        .await;
        wire_cast(&map_storage(result, map_get_period_domain)?)
    }

    async fn list_periods(
        &self,
        context: Ctx,
        request: ListPeriodsRequest,
    ) -> PluginResult<ListPeriodsResponse, ListPeriodsError> {
        self.authorize_admin(
            &context,
            billing::LIST_PERIODS_OPERATION,
            &request.organization_id,
            BILLING_READ,
        )
        .await
        .map_err(map_list_periods_authorization)?;
        if !valid_id(&request.organization_id)
            || !valid_id(&request.account_id)
            || !(1..=MAX_PAGE_SIZE).contains(&request.limit)
            || request
                .cursor
                .as_deref()
                .is_some_and(|value| !valid_id(value))
        {
            return Err(PluginError::domain(ListPeriodsError::InvalidRequest));
        }
        let result = storage::list_periods(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.account_id,
            request.cursor.as_deref(),
            request.limit,
        )
        .await;
        let (periods, next_cursor) = map_storage(result, map_list_periods_domain)?;
        Ok(ListPeriodsResponse {
            periods: wire_cast(&periods)?,
            next_cursor,
        })
    }

    async fn reconcile_next(
        &self,
        context: Ctx,
        request: ReconcileNextRequest,
    ) -> PluginResult<ReconcileNextResponse, ReconcileNextError> {
        let caller = Self::allowed_caller(&context, &self.config.worker_callers)
            .ok_or_else(|| PluginError::domain(ReconcileNextError::Forbidden))?;
        if request.worker_id != caller || !valid_id(&request.worker_id) {
            return Err(PluginError::domain(ReconcileNextError::InvalidRequest));
        }
        let claim = storage::claim_next_delivery(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.worker_id,
            self.config.delivery_lease_seconds,
            self.config.max_delivery_attempts,
        )
        .await
        .map_err(storage_runtime)?;
        let Some(claim) = claim else {
            return Ok(ReconcileNextResponse {
                delivery_id: None,
                period_id: None,
                status: ReconcileNextResponseStatus::Idle,
            });
        };
        let outcome = self
            .sink
            .publish_meter_event_with_context(
                context,
                PublishMeterEventRequest {
                    delivery_id: claim.delivery_id.clone(),
                    scope_kind: claim.scope_kind,
                    scope_id: claim.scope_id,
                    subject: claim.subject,
                    meter: claim.sink_meter,
                    quantity: claim.quantity,
                    occurred_at: claim.occurred_at,
                },
            )
            .await;
        match outcome {
            Ok(response) => {
                storage::complete_delivery(
                    &self.prepared().map_err(PluginError::runtime)?.postgres,
                    &claim.delivery_id,
                    &request.worker_id,
                    claim.attempt,
                    "delivered",
                    Some(&response.provider_reference),
                    None,
                )
                .await
                .map_err(storage_runtime)?;
                Ok(ReconcileNextResponse {
                    delivery_id: Some(claim.delivery_id),
                    period_id: Some(claim.period_id),
                    status: ReconcileNextResponseStatus::Delivered,
                })
            }
            Err(BillingMeterSinkInvocationError::Domain(error)) => {
                let (status, response_status, failure) = sink_failure(&error);
                storage::complete_delivery(
                    &self.prepared().map_err(PluginError::runtime)?.postgres,
                    &claim.delivery_id,
                    &request.worker_id,
                    claim.attempt,
                    status,
                    None,
                    Some(failure),
                )
                .await
                .map_err(storage_runtime)?;
                Ok(ReconcileNextResponse {
                    delivery_id: Some(claim.delivery_id),
                    period_id: Some(claim.period_id),
                    status: response_status,
                })
            }
            Err(BillingMeterSinkInvocationError::Runtime(error)) => {
                storage::complete_delivery(
                    &self.prepared().map_err(PluginError::runtime)?.postgres,
                    &claim.delivery_id,
                    &request.worker_id,
                    claim.attempt,
                    "unknown",
                    None,
                    Some("sink_runtime_failure"),
                )
                .await
                .map_err(storage_runtime)?;
                Err(PluginError::runtime(error))
            }
        }
    }

    async fn inspect_delivery(
        &self,
        context: Ctx,
        request: InspectDeliveryRequest,
    ) -> PluginResult<DeliveryResponse, InspectDeliveryError> {
        self.authorize_admin(
            &context,
            billing::INSPECT_DELIVERY_OPERATION,
            &request.organization_id,
            BILLING_READ,
        )
        .await
        .map_err(map_inspect_authorization)?;
        if !valid_id(&request.organization_id) || !valid_id(&request.delivery_id) {
            return Err(PluginError::domain(InspectDeliveryError::InvalidRequest));
        }
        let result = storage::inspect_delivery(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.delivery_id,
        )
        .await;
        wire_cast(&map_storage(result, map_inspect_domain)?)
    }

    async fn resolve_delivery(
        &self,
        context: Ctx,
        request: ResolveDeliveryRequest,
    ) -> PluginResult<DeliveryResponse, ResolveDeliveryError> {
        let (caller, actor) = self
            .authorize_admin(
                &context,
                billing::RESOLVE_DELIVERY_OPERATION,
                &request.organization_id,
                BILLING_RESOLVE,
            )
            .await
            .map_err(map_resolve_authorization)?;
        let resolution = match request.resolution {
            ResolveDeliveryRequestResolution::Retry => "retry",
            ResolveDeliveryRequestResolution::Delivered => "delivered",
            ResolveDeliveryRequestResolution::Failed => "failed",
        };
        let request_hash = request_hash(&request)?;
        let provider_reference = request.provider_reference.flatten();
        if !valid_id(&request.organization_id)
            || !valid_id(&request.delivery_id)
            || !valid_text(&request.reason, MAX_REASON_BYTES, false)
            || !valid_idempotency_key(&request.idempotency_key)
            || provider_reference
                .as_deref()
                .is_some_and(|value| !valid_id(value))
        {
            return Err(PluginError::domain(ResolveDeliveryError::InvalidRequest));
        }
        let result = storage::resolve_delivery(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            storage::ResolveDelivery {
                caller: &caller,
                actor: &actor,
                organization_id: &request.organization_id,
                delivery_id: &request.delivery_id,
                resolution,
                provider_reference: provider_reference.as_deref(),
                reason: &request.reason,
                idempotency_key: &request.idempotency_key,
                request_hash: &request_hash,
            },
        )
        .await;
        wire_cast(&map_storage(result, map_resolve_domain)?)
    }

    fn prepared(&self) -> Result<PreparedUsageBilling, RuntimeFailure> {
        self.prepared
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Usage Billing Plugin is not prepared".to_owned(),
            })
    }

    async fn authorize_admin(
        &self,
        context: &Ctx,
        operation: &str,
        organization_id: &str,
        permission: &str,
    ) -> Result<(String, String), AuthorizationFailure> {
        let caller = Self::allowed_caller(context, &self.config.admin_callers)
            .ok_or(AuthorizationFailure::Forbidden)?;
        let actor = self
            .authenticated_subject(context, operation)
            .map_err(|()| AuthorizationFailure::Unauthenticated)?;
        let member = self
            .membership
            .check_membership_with_context(
                context.clone(),
                CheckMembershipRequest {
                    organization_id: organization_id.to_owned(),
                    subject: actor.clone(),
                },
            )
            .await
            .map(|response| response.active)
            .map_err(|error| match error {
                OrganizationMembershipInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                    detail: "Organization Membership rejected a Usage Billing authorization query"
                        .to_owned(),
                },
                OrganizationMembershipInvocationError::Runtime(error) => error,
            })
            .map_err(AuthorizationFailure::Runtime)?;
        if !member {
            return Err(AuthorizationFailure::Forbidden);
        }
        let allowed = self
            .access
            .check_permission_with_context(
                context.clone(),
                CheckPermissionRequest {
                    subject: actor.clone(),
                    scope: CheckPermissionRequestScope {
                        kind: "organization".to_owned(),
                        id: organization_id.to_owned(),
                    },
                    permission: permission.to_owned(),
                },
            )
            .await
            .map(|response| response.allowed)
            .map_err(|error| match error {
                AccessControlInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                    detail: "Access Control rejected a Usage Billing authorization query"
                        .to_owned(),
                },
                AccessControlInvocationError::Runtime(error) => error,
            })
            .map_err(AuthorizationFailure::Runtime)?;
        if !allowed {
            return Err(AuthorizationFailure::Forbidden);
        }
        Ok((caller, actor))
    }

    fn authenticated_subject(&self, context: &Ctx, operation: &str) -> Result<String, ()> {
        let actor = self
            .config
            .verifier()
            .map_err(|_| ())?
            .project_context::<BillingActor>(context, billing::CAPABILITY_ID, operation, &UtcClock)
            .map_err(|_| ())?;
        valid_id(&actor.subject).then_some(actor.subject).ok_or(())
    }

    fn allowed_caller(context: &Ctx, allowed: &[String]) -> Option<String> {
        context.caller_instance().and_then(|caller| {
            allowed
                .iter()
                .any(|entry| entry == caller)
                .then(|| caller.to_owned())
        })
    }
}

impl Lifecycle for PostgresUsageBillingPlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let database_url = resolve_secret(
            &self.secrets,
            context.dependencies(),
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        self.prepared
            .borrow_mut()
            .replace(PreparedUsageBilling { postgres });
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct BillingActor {
    subject: String,
}

impl TypedActor for BillingActor {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        Ok(Self {
            subject: assertion.subject().to_owned(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct UtcClock;

impl AssertionClock for UtcClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug)]
enum AuthorizationFailure {
    Unauthenticated,
    Forbidden,
    Runtime(RuntimeFailure),
}

async fn resolve_secret(
    secrets: &SecretsClient,
    dependencies: &PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|response| Zeroizing::new(response.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("database URL secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}

fn validate_meters(request: &PutAccountRequest, maximum: usize) -> Option<Vec<storage::MeterView>> {
    if request.meters.is_empty() || request.meters.len() > maximum {
        return None;
    }
    let mut mapping_ids = BTreeSet::new();
    let mut source_keys = BTreeSet::new();
    request
        .meters
        .iter()
        .map(|meter| {
            let unit_price = meter.unit_price_minor.parse::<BigInt>().ok()?;
            if !valid_id(&meter.mapping_id)
                || !valid_text(&meter.source_scope_kind, 128, false)
                || !valid_id(&meter.source_scope_id)
                || !valid_id(&meter.source_subject)
                || !valid_text(&meter.source_meter, 128, false)
                || !valid_text(&meter.sink_meter, 100, false)
                || unit_price < BigInt::from(0)
                || !mapping_ids.insert(&meter.mapping_id)
                || !source_keys.insert((
                    &meter.source_scope_kind,
                    &meter.source_scope_id,
                    &meter.source_subject,
                    &meter.source_meter,
                ))
            {
                return None;
            }
            Some(storage::MeterView {
                mapping_id: meter.mapping_id.clone(),
                source_scope_kind: meter.source_scope_kind.clone(),
                source_scope_id: meter.source_scope_id.clone(),
                source_subject: meter.source_subject.clone(),
                source_meter: meter.source_meter.clone(),
                sink_meter: meter.sink_meter.clone(),
                unit_price_minor: unit_price.to_string(),
            })
        })
        .collect()
}

fn sink_failure(
    error: &PublishMeterEventError,
) -> (&'static str, ReconcileNextResponseStatus, &'static str) {
    match error {
        PublishMeterEventError::EffectUnknown | PublishMeterEventError::IdempotencyConflict => (
            "unknown",
            ReconcileNextResponseStatus::Unknown,
            "sink_effect_unknown",
        ),
        PublishMeterEventError::Forbidden => (
            "failed",
            ReconcileNextResponseStatus::Failed,
            "sink_forbidden",
        ),
        PublishMeterEventError::InvalidEvent => (
            "failed",
            ReconcileNextResponseStatus::Failed,
            "sink_invalid_event",
        ),
        PublishMeterEventError::AccountNotFound => (
            "failed",
            ReconcileNextResponseStatus::Failed,
            "sink_account_not_found",
        ),
        PublishMeterEventError::MeterNotConfigured => (
            "failed",
            ReconcileNextResponseStatus::Failed,
            "sink_meter_not_configured",
        ),
        PublishMeterEventError::Unknown(_) => (
            "unknown",
            ReconcileNextResponseStatus::Unknown,
            "sink_unknown_domain_error",
        ),
    }
}

fn stable_delivery_id(period_id: &str, mapping_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(period_id.as_bytes());
    digest.update([0]);
    digest.update(mapping_id.as_bytes());
    format!("billing_delivery_{}", encode_hex(&digest.finalize()[..16]))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn map_storage<T, E>(
    result: Result<T, StorageError>,
    map_domain: fn(DomainFailure) -> E,
) -> PluginResult<T, E> {
    match result {
        Ok(value) => Ok(value),
        Err(StorageError::Domain(failure)) => Err(PluginError::domain(map_domain(failure))),
        Err(error) => Err(storage_runtime(error)),
    }
}

fn storage_to_plugin<E>(error: StorageError, map_domain: fn(DomainFailure) -> E) -> PluginError<E> {
    match error {
        StorageError::Domain(failure) => PluginError::domain(map_domain(failure)),
        error => storage_runtime(error),
    }
}

macro_rules! map_domain {
    ($name:ident, $error:ty) => {
        fn $name(failure: DomainFailure) -> $error {
            match failure {
                DomainFailure::NotFound => <$error>::NotFound,
                DomainFailure::RevisionConflict => <$error>::RevisionConflict,
                DomainFailure::IdempotencyConflict => <$error>::IdempotencyConflict,
                DomainFailure::PeriodConflict => <$error>::PeriodConflict,
                DomainFailure::AccountDisabled => <$error>::AccountDisabled,
                DomainFailure::DeliveryNotFound => <$error>::DeliveryNotFound,
                DomainFailure::InvalidResolution => <$error>::InvalidResolution,
            }
        }
    };
}

map_domain!(map_put_domain, PutAccountError);
map_domain!(map_get_account_domain, GetAccountError);
map_domain!(map_list_accounts_domain, ListAccountsError);
map_domain!(map_close_domain, ClosePeriodError);
map_domain!(map_get_period_domain, GetPeriodError);
map_domain!(map_list_periods_domain, ListPeriodsError);
map_domain!(map_inspect_domain, InspectDeliveryError);
map_domain!(map_resolve_domain, ResolveDeliveryError);

macro_rules! map_authorization {
    ($name:ident, $error:ty) => {
        fn $name(failure: AuthorizationFailure) -> PluginError<$error> {
            match failure {
                AuthorizationFailure::Unauthenticated => {
                    PluginError::domain(<$error>::Unauthenticated)
                }
                AuthorizationFailure::Forbidden => PluginError::domain(<$error>::Forbidden),
                AuthorizationFailure::Runtime(error) => PluginError::runtime(error),
            }
        }
    };
}

map_authorization!(map_put_authorization, PutAccountError);
map_authorization!(map_get_account_authorization, GetAccountError);
map_authorization!(map_list_accounts_authorization, ListAccountsError);
map_authorization!(map_close_authorization, ClosePeriodError);
map_authorization!(map_get_period_authorization, GetPeriodError);
map_authorization!(map_list_periods_authorization, ListPeriodsError);
map_authorization!(map_inspect_authorization, InspectDeliveryError);
map_authorization!(map_resolve_authorization, ResolveDeliveryError);

fn request_hash<T: Serialize, E>(request: &T) -> Result<Vec<u8>, PluginError<E>> {
    serde_json::to_vec(request)
        .map(|wire| Sha256::digest(wire).to_vec())
        .map_err(serialization_runtime)
}

fn wire_cast<T: DeserializeOwned, E>(value: &impl Serialize) -> Result<T, PluginError<E>> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(serialization_runtime)
}

#[allow(clippy::needless_pass_by_value)]
fn serialization_runtime<E>(error: serde_json::Error) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::Internal {
        detail: format!("Usage Billing wire serialization failed: {error}"),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn storage_runtime<E>(error: StorageError) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    })
}

fn valid_currency(value: &str) -> bool {
    value.len() == 3 && value.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn valid_id(value: &str) -> bool {
    valid_text(value, MAX_ID_BYTES, false)
}

fn valid_idempotency_key(value: &str) -> bool {
    valid_text(value, MAX_IDEMPOTENCY_BYTES, false)
}

fn valid_text(value: &str, max_bytes: usize, allow_empty: bool) -> bool {
    value.len() <= max_bytes
        && (allow_empty || !value.trim().is_empty())
        && !value.chars().any(char::is_control)
}

fn valid_identifier(value: &str, max_bytes: usize) -> bool {
    valid_text(value, max_bytes, false) && !value.chars().any(char::is_whitespace)
}

fn validate_callers(callers: &[String]) -> Result<(), ()> {
    if callers.is_empty() || callers.len() > MAX_CALLERS {
        return Err(());
    }
    let mut unique = BTreeSet::new();
    for caller in callers {
        if !valid_identifier(caller, 256) || !unique.insert(caller) {
            return Err(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_auth_sdk::ActorAssertionIssuer;
    use lenso_kernel::{CancellationToken, InvocationContext};
    use lenso_native_adapter::NativePluginRegistry;

    fn config() -> UsageBillingConfig {
        let issuer = ActorAssertionIssuer::new("auth.users", b"usage-billing-test-key");
        UsageBillingConfig::new(
            "usage_billing",
            "usage-billing/database-url",
            "auth.users",
            issuer.public_key_base64(),
            vec!["billing-api".to_owned()],
            vec!["billing-worker".to_owned()],
            60,
            5,
            50,
        )
        .unwrap()
    }

    fn plugin() -> PostgresUsageBillingPlugin {
        PostgresUsageBillingPlugin {
            config: config(),
            secrets: Port::default(),
            membership: Port::default(),
            access: Port::default(),
            usage: Port::default(),
            sink: Port::default(),
            prepared: Rc::new(RefCell::new(None)),
        }
    }

    fn context(caller: &str) -> InvocationContext {
        InvocationContext::new(1, None, CancellationToken::new()).with_caller_instance(caller)
    }

    #[test]
    fn descriptor_declares_exact_provider_and_dependencies() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        let provided = descriptor["provided_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["capability_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(provided, BTreeSet::from([billing::CAPABILITY_ID]));
        let required = descriptor["required_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["capability_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            required,
            BTreeSet::from([
                secrets::CAPABILITY_ID,
                membership::CAPABILITY_ID,
                access::CAPABILITY_ID,
                usage::CAPABILITY_ID,
                sink::CAPABILITY_ID,
            ])
        );
        assert_eq!(
            NativePluginRegistry::new()
                .with_linked_factories()
                .factories()
                .filter(|factory| factory.package_id() == PACKAGE_ID)
                .count(),
            1
        );
    }

    #[test]
    fn untrusted_worker_is_rejected_before_storage_and_sink() {
        let request = ReconcileNextRequest {
            worker_id: "billing-worker".to_owned(),
        };
        let result = futures::executor::block_on(
            plugin().reconcile_next(context("untrusted-worker"), request),
        );
        assert_eq!(
            result,
            Err(PluginError::Domain(ReconcileNextError::Forbidden))
        );
    }

    #[test]
    fn integer_amounts_do_not_overflow_i64() {
        let quantity = "9223372036854775808".parse::<BigInt>().unwrap();
        let price = "1000000".parse::<BigInt>().unwrap();
        assert_eq!((quantity * price).to_string(), "9223372036854775808000000");
    }

    #[test]
    fn configuration_rejects_duplicate_worker_callers() {
        let mut invalid = config();
        invalid.worker_callers.push("billing-worker".to_owned());
        assert_eq!(
            invalid.validate(),
            Err(UsageBillingConfigError::InvalidWorkerCallers)
        );
    }
}
