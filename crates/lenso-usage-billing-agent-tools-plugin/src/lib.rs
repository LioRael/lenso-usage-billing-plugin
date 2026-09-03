//! Read-only Console Agent Tools over an explicitly bound Usage Billing capability.

use lenso::prelude::*;
use lenso_capability_agent_tool_provider::{
    self as tools, CatalogRequest, CatalogResponse, ContentType, ExecuteError, ExecuteRequest,
    ExecuteResponse, ExecutionFailedPayload, ToolDefinition, ToolExecutionClass,
};
use lenso_capability_usage_billing::{
    self as billing, GetAccountRequest, GetPeriodRequest, InspectDeliveryRequest,
    ListAccountsRequest, ListPeriodsRequest,
};
use lenso_kernel::RuntimeFailure;
use serde::{Serialize, de::DeserializeOwned};

const GET_ACCOUNT: &str = "usage_billing_get_account";
const LIST_ACCOUNTS: &str = "usage_billing_list_accounts";
const GET_PERIOD: &str = "usage_billing_get_period";
const LIST_PERIODS: &str = "usage_billing_list_periods";
const INSPECT_DELIVERY: &str = "usage_billing_inspect_delivery";

#[lenso::plugin]
#[derive(Clone, Debug)]
struct UsageBillingAgentToolsPlugin {
    billing: Port<billing::UsageBillingClient>,
}

#[lenso::provides(tools::ToolProvider)]
impl UsageBillingAgentToolsPlugin {
    fn catalog(
        &self,
        _context: Ctx,
        _request: CatalogRequest,
    ) -> impl std::future::Future<Output = PluginResult<CatalogResponse, tools::CatalogError>> {
        let _ = self;
        futures::future::ready(Ok(CatalogResponse {
            tools: tool_definitions(),
        }))
    }

    async fn execute(
        &self,
        context: Ctx,
        request: ExecuteRequest,
    ) -> PluginResult<ExecuteResponse, ExecuteError> {
        macro_rules! invoke {
            ($ty:ty, $method:ident, $domain:path, $runtime:path, $name:expr) => {{
                let input = decode::<$ty>(&request)?;
                match self.billing.$method(context, input).await {
                    Ok(value) => success($name, &value),
                    Err($domain(error)) => Err(PluginError::domain(map_error(&error))),
                    Err($runtime(error)) => Err(PluginError::runtime(error)),
                }
            }};
        }
        match request.name.as_str() {
            GET_ACCOUNT => invoke!(
                GetAccountRequest,
                get_account_with_context,
                billing::UsageBillingGetAccountInvocationError::Domain,
                billing::UsageBillingGetAccountInvocationError::Runtime,
                GET_ACCOUNT
            ),
            LIST_ACCOUNTS => invoke!(
                ListAccountsRequest,
                list_accounts_with_context,
                billing::UsageBillingListAccountsInvocationError::Domain,
                billing::UsageBillingListAccountsInvocationError::Runtime,
                LIST_ACCOUNTS
            ),
            GET_PERIOD => invoke!(
                GetPeriodRequest,
                get_period_with_context,
                billing::UsageBillingGetPeriodInvocationError::Domain,
                billing::UsageBillingGetPeriodInvocationError::Runtime,
                GET_PERIOD
            ),
            LIST_PERIODS => invoke!(
                ListPeriodsRequest,
                list_periods_with_context,
                billing::UsageBillingListPeriodsInvocationError::Domain,
                billing::UsageBillingListPeriodsInvocationError::Runtime,
                LIST_PERIODS
            ),
            INSPECT_DELIVERY => invoke!(
                InspectDeliveryRequest,
                inspect_delivery_with_context,
                billing::UsageBillingInspectDeliveryInvocationError::Domain,
                billing::UsageBillingInspectDeliveryInvocationError::Runtime,
                INSPECT_DELIVERY
            ),
            _ => Err(PluginError::domain(ExecuteError::NotFound)),
        }
    }
}

fn tool_definitions() -> Vec<ToolDefinition> {
    vec![
        tool(
            GET_ACCOUNT,
            "Get one billable account for authorized inspection.",
            include_str!(
                "../../lenso-capability-usage-billing/schemas/get-account-request.schema.json"
            ),
        ),
        tool(
            LIST_ACCOUNTS,
            "List billable accounts with bounded cursor pagination.",
            include_str!(
                "../../lenso-capability-usage-billing/schemas/list-accounts-request.schema.json"
            ),
        ),
        tool(
            GET_PERIOD,
            "Get one immutable usage-billing period snapshot.",
            include_str!(
                "../../lenso-capability-usage-billing/schemas/get-period-request.schema.json"
            ),
        ),
        tool(
            LIST_PERIODS,
            "List immutable usage-billing period snapshots with bounded cursor pagination.",
            include_str!(
                "../../lenso-capability-usage-billing/schemas/list-periods-request.schema.json"
            ),
        ),
        tool(
            INSPECT_DELIVERY,
            "Inspect one provider-neutral billing delivery without reconciling or resolving it.",
            include_str!(
                "../../lenso-capability-usage-billing/schemas/inspect-delivery-request.schema.json"
            ),
        ),
    ]
}

fn tool(name: &str, description: &str, schema: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.to_owned(),
        description: description.to_owned(),
        input_schema_json: serde_json::from_str::<serde_json::Value>(schema)
            .expect("Usage Billing Tool schema must be valid JSON")
            .to_string()
            .try_into()
            .expect("Usage Billing Tool schema must remain valid JSON"),
        execution: ToolExecutionClass::ParallelSafe,
    }
}
fn decode<T: DeserializeOwned>(request: &ExecuteRequest) -> PluginResult<T, ExecuteError> {
    serde_json::from_str(request.arguments_json.as_str())
        .map_err(|_| PluginError::domain(ExecuteError::InvalidArguments))
}
fn success<T: Serialize>(name: &str, value: &T) -> PluginResult<ExecuteResponse, ExecuteError> {
    let content = serde_json::to_string_pretty(value).map_err(|error| {
        PluginError::runtime(RuntimeFailure::PluginFailure {
            detail: format!("Usage Billing Tool could not serialize its response: {error}"),
        })
    })?;
    Ok(ExecuteResponse {
        content_blocks: None,
        content,
        content_type: ContentType::Text,
        metadata_json: serde_json::json!({ "tool": name, "read_only": true })
            .to_string()
            .try_into()
            .expect("Usage Billing Tool metadata must be valid JSON"),
    })
}
trait DomainError {
    fn tool_error(&self) -> ExecuteError;
}
fn map_error(error: &impl DomainError) -> ExecuteError {
    error.tool_error()
}
fn rejected(code: &str) -> ExecuteError {
    ExecuteError::ExecutionFailed {
        payload: ExecutionFailedPayload {
            reason_code: code.to_owned(),
            message: "Usage Billing rejected the inspection operation.".to_owned(),
            details_json: serde_json::json!({ "domain_error": code })
                .to_string()
                .try_into()
                .expect("Usage Billing Tool error metadata must be valid JSON"),
        },
    }
}
macro_rules! impl_domain_error {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl DomainError for $ty {
                fn tool_error(&self) -> ExecuteError {
                    match self {
                        Self::InvalidRequest => ExecuteError::InvalidArguments,
                        Self::DeliveryNotFound | Self::NotFound => ExecuteError::NotFound,
                        Self::Forbidden | Self::Unauthenticated => ExecuteError::PermissionDenied,
                        Self::AccountDisabled => rejected("account_disabled"),
                        Self::IdempotencyConflict => rejected("idempotency_conflict"),
                        Self::InvalidResolution => rejected("invalid_resolution"),
                        Self::PeriodConflict => rejected("period_conflict"),
                        Self::RevisionConflict => rejected("revision_conflict"),
                        Self::SourceRejected => rejected("source_rejected"),
                        Self::Unknown(_) => rejected("unknown_domain_error"),
                    }
                }
            }
        )+
    };
}
impl_domain_error!(
    billing::GetAccountError,
    billing::GetPeriodError,
    billing::InspectDeliveryError,
    billing::ListAccountsError,
    billing::ListPeriodsError
);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn descriptor_and_catalog_are_read_only() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        assert_eq!(descriptor["plugin_id"], "lenso.usage-billing.agent-tools");
        let required = descriptor["required_capabilities"].as_array().unwrap();
        assert_eq!(required.len(), 1);
        assert_eq!(required[0]["capability_id"], "lenso.usage-billing@1");
        let tools = tool_definitions();
        assert_eq!(tools.len(), 5);
        assert!(
            tools
                .iter()
                .map(|tool| tool.name.as_str())
                .all(|name| !name.contains("put")
                    && !name.contains("close")
                    && !name.contains("reconcile")
                    && !name.contains("resolve"))
        );
    }
}
