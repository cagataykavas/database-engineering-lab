CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL CHECK (amount >= 0),
    country CHAR(2) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('approved', 'declined', 'review')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE account_balances (
    account_id TEXT PRIMARY KEY,
    balance NUMERIC(16, 2) NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transfers (
    transfer_id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    source_account_id TEXT NOT NULL REFERENCES account_balances(account_id),
    destination_account_id TEXT NOT NULL REFERENCES account_balances(account_id),
    amount NUMERIC(16, 2) NOT NULL CHECK (amount > 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'committed')),
    source_balance_after NUMERIC(16, 2),
    destination_balance_after NUMERIC(16, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at TIMESTAMPTZ,
    CHECK (source_account_id <> destination_account_id),
    CHECK (
        (status = 'pending' AND source_balance_after IS NULL AND destination_balance_after IS NULL)
        OR
        (status = 'committed' AND source_balance_after IS NOT NULL AND destination_balance_after IS NOT NULL)
    )
);

CREATE TABLE ledger_entries (
    entry_id BIGSERIAL PRIMARY KEY,
    transfer_id UUID NOT NULL REFERENCES transfers(transfer_id),
    account_id TEXT NOT NULL REFERENCES account_balances(account_id),
    direction TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount NUMERIC(16, 2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(transfer_id, direction)
);
