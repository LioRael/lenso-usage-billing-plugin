CREATE TABLE billing_accounts (
    account_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (organization_id, scope_kind, scope_id, subject)
);

CREATE INDEX billing_accounts_org_cursor_idx ON billing_accounts (organization_id, account_id);

CREATE TABLE billing_meter_mappings (
    account_id TEXT NOT NULL REFERENCES billing_accounts(account_id) ON DELETE CASCADE,
    mapping_id TEXT NOT NULL,
    source_scope_kind TEXT NOT NULL,
    source_scope_id TEXT NOT NULL,
    source_subject TEXT NOT NULL,
    source_meter TEXT NOT NULL,
    sink_meter TEXT NOT NULL,
    unit_price_minor TEXT NOT NULL,
    PRIMARY KEY (account_id, mapping_id),
    UNIQUE (account_id, source_scope_kind, source_scope_id, source_subject, source_meter)
);

CREATE TABLE billing_periods (
    period_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES billing_accounts(account_id),
    organization_id TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed', 'unknown')),
    account_revision BIGINT NOT NULL,
    total_amount_minor TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    CHECK (period_end > period_start),
    UNIQUE (account_id, period_start, period_end)
);

CREATE INDEX billing_periods_account_cursor_idx ON billing_periods (organization_id, account_id, period_id);

CREATE TABLE billing_period_lines (
    delivery_id TEXT PRIMARY KEY,
    period_id TEXT NOT NULL REFERENCES billing_periods(period_id) ON DELETE CASCADE,
    mapping_id TEXT NOT NULL,
    source_meter TEXT NOT NULL,
    sink_meter TEXT NOT NULL,
    quantity TEXT NOT NULL,
    usage_revision BIGINT NOT NULL,
    unit_price_minor TEXT NOT NULL,
    amount_minor TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_flight', 'delivered', 'failed', 'unknown')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    provider_reference TEXT,
    failure_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (period_id, mapping_id),
    CHECK (
      (status = 'in_flight' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
      OR (status <> 'in_flight' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX billing_period_lines_claim_idx ON billing_period_lines (status, updated_at, delivery_id)
    WHERE status IN ('pending', 'in_flight');

CREATE TABLE billing_commands (
    caller_instance TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (caller_instance, actor_subject, operation, idempotency_key)
);
