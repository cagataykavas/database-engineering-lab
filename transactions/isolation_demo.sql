-- PostgreSQL transaction isolation lab
-- Run each SESSION block in a separate psql terminal.

CREATE TABLE IF NOT EXISTS inventory_counters (
    id BIGINT PRIMARY KEY,
    item_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0)
);

INSERT INTO inventory_counters(id, item_name, quantity)
VALUES (1, 'widget', 100), (2, 'gadget', 100)
ON CONFLICT (id) DO UPDATE SET quantity = EXCLUDED.quantity;

-- READ COMMITTED: each statement sees a fresh committed snapshot.
-- SESSION A
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT quantity FROM inventory_counters WHERE id = 1;
-- run SESSION B and commit, then query again
SELECT quantity FROM inventory_counters WHERE id = 1;
COMMIT;

-- SESSION B
BEGIN;
UPDATE inventory_counters SET quantity = quantity + 10 WHERE id = 1;
COMMIT;

-- REPEATABLE READ: SESSION A keeps one transaction snapshot.
-- SESSION A
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT quantity FROM inventory_counters WHERE id = 1;
-- run SESSION B and commit, then query again
SELECT quantity FROM inventory_counters WHERE id = 1;
COMMIT;

-- SESSION B
BEGIN;
UPDATE inventory_counters SET quantity = quantity + 10 WHERE id = 1;
COMMIT;

-- Explicit row locking. Lock rows in deterministic ID order to reduce deadlock risk.
BEGIN;
SELECT id, quantity FROM inventory_counters WHERE id IN (1, 2) ORDER BY id FOR UPDATE;
UPDATE inventory_counters SET quantity = quantity - 5 WHERE id = 1;
UPDATE inventory_counters SET quantity = quantity + 5 WHERE id = 2;
COMMIT;

SELECT SUM(quantity) AS total_quantity FROM inventory_counters;
