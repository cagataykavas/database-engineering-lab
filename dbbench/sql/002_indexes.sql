CREATE INDEX idx_transactions_customer_created
    ON transactions(customer_id, created_at DESC, id DESC);

CREATE INDEX idx_transactions_review_amount
    ON transactions(amount DESC)
    WHERE status = 'review';

CREATE INDEX idx_transactions_merchant_created
    ON transactions(merchant_id, created_at DESC);

CREATE INDEX idx_transactions_metadata_gin
    ON transactions USING GIN(metadata);

CREATE INDEX idx_ledger_account_created
    ON ledger_entries(account_id, created_at DESC, entry_id DESC);
