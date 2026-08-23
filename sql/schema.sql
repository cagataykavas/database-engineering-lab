CREATE TABLE IF NOT EXISTS transactions (
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

CREATE INDEX IF NOT EXISTS idx_transactions_customer_created
    ON transactions(customer_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_review_amount
    ON transactions(amount DESC)
    WHERE status = 'review';

CREATE INDEX IF NOT EXISTS idx_transactions_merchant_created
    ON transactions(merchant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_metadata_gin
    ON transactions USING GIN(metadata);

CREATE TABLE IF NOT EXISTS account_balances (
    account_id TEXT PRIMARY KEY,
    balance NUMERIC(16, 2) NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
