use lenso_postgres_kit::OwnedPostgres;
use sqlx::{AssertSqlSafe, Executor as _};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{UsageBillingOperator, schema, storage};

#[tokio::test]
async fn account_period_and_delivery_survive_restart() {
    let Ok(database_url) = std::env::var("LENSO_USAGE_BILLING_TEST_DATABASE_URL") else {
        return;
    };
    let schema_name = format!("usage_billing_test_{}", Uuid::new_v4().simple());
    UsageBillingOperator::setup(&database_url, &schema_name)
        .await
        .unwrap();
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();
    let meters = vec![storage::MeterView {
        mapping_id: "tokens".to_owned(),
        source_scope_kind: "organization".to_owned(),
        source_scope_id: "org_1".to_owned(),
        source_subject: "org_1".to_owned(),
        source_meter: "tokens".to_owned(),
        sink_meter: "ai_tokens".to_owned(),
        unit_price_minor: "2".to_owned(),
    }];
    let account = storage::put_account(
        &postgres,
        storage::PutAccount {
            caller: "billing-api",
            actor: "usr_billing",
            organization_id: "org_1",
            account_id: "account_1",
            scope_kind: "organization",
            scope_id: "org_1",
            subject: "org_1",
            currency: "USD",
            status: "active",
            expected_revision: 0,
            idempotency_key: "account-create",
            request_hash: &[1],
            meters: &meters,
        },
    )
    .await
    .unwrap();
    assert_eq!(account.revision, "1");
    let start = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let end = start + time::Duration::hours(1);
    let lines = vec![storage::SnapshotLine {
        delivery_id: "billing_delivery_1".to_owned(),
        mapping_id: "tokens".to_owned(),
        source_meter: "tokens".to_owned(),
        sink_meter: "ai_tokens".to_owned(),
        quantity: "10".to_owned(),
        usage_revision: 7,
        unit_price_minor: "2".to_owned(),
        amount_minor: "20".to_owned(),
    }];
    storage::close_period(
        &postgres,
        storage::ClosePeriod {
            caller: "billing-api",
            actor: "usr_billing",
            organization_id: "org_1",
            account_id: "account_1",
            period_id: "period_1",
            period_start: start,
            period_end: end,
            expected_account_revision: 1,
            idempotency_key: "period-close",
            request_hash: &[2],
            total_amount_minor: "20",
            lines: &lines,
        },
    )
    .await
    .unwrap();
    postgres.pool().close().await;

    let restarted = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();
    let claim = storage::claim_next_delivery(&restarted, "billing-worker", 60, 5)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.quantity, "10");
    storage::complete_delivery(
        &restarted,
        &claim.delivery_id,
        "billing-worker",
        claim.attempt,
        "delivered",
        Some("provider-event-1"),
        None,
    )
    .await
    .unwrap();
    let period = storage::get_period(&restarted, "org_1", "period_1")
        .await
        .unwrap();
    assert_eq!(period.status, "delivered");
    assert_eq!(
        period.lines[0].provider_reference.as_deref(),
        Some("provider-event-1")
    );

    restarted.pool().close().await;
    let cleanup = sqlx::PgPool::connect(&database_url).await.unwrap();
    cleanup
        .execute(AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema_name}\" CASCADE"
        )))
        .await
        .unwrap();
    cleanup.close().await;
}
