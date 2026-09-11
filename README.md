# PostgreSQL Transaction & Performance Engineering Lab

A runnable PostgreSQL system for studying the two things a production data layer must get right: **correctness under concurrency** and **performance under evidence**.

The repository implements checksum-verified migrations, an idempotent double-entry transfer ledger, deterministic row locking, a durable `SKIP LOCKED` worker queue, compound keyset pagination, synthetic workload generation and machine-readable `EXPLAIN (ANALYZE, BUFFERS)` regression reports.

It is deliberately more than a folder of SQL snippets. The CLI executes the complete system against PostgreSQL 16 and CI publishes the resulting ledger, lease, pagination and query-plan evidence.

## Architecture

```mermaid
flowchart TD
    CLI[db-bench CLI] --> Migrations[Checksum migration runner]
    CLI --> Ledger[Transfer service]
    CLI --> Jobs[Durable job queue]
    CLI --> Workload[Workload generator]
    Migrations --> PG[(PostgreSQL 16)]
    Ledger --> PG
    Jobs --> PG
    Workload --> PG
    PG --> Explain[EXPLAIN ANALYZE BUFFERS]
    Explain --> Budget[Plan parser and budgets]
    Budget --> Evidence[CI JSON artifacts]
```

## Correctness invariants

| Risk | Implemented control |
|---|---|
| Migration edited after deployment | SHA-256 checksum comparison |
| Two deployers migrate concurrently | Transaction-scoped PostgreSQL advisory lock |
| Client retries transfer | Unique idempotency key and canonical request hash |
| Same key, different transfer | Explicit `TransferConflict` |
| Money disappears during partial write | Balance updates, transfer status and two ledger entries in one transaction |
| Concurrent transfers deadlock | Account rows locked in deterministic ID order |
| Two workers execute one job | `FOR UPDATE SKIP LOCKED` claim and owner lease |
| Worker crashes | Expired running lease is reclaimable |
| Poison job retries forever | Attempt budget, delayed retry and `dead_letter` state |
| Offset pagination skips/duplicates rows | `(created_at, id)` keyset cursor |
| Query regression goes unnoticed | Parsed execution, scan and buffer budgets |

## Versioned migrations

Migration SQL is packaged inside the wheel:

```text
dbbench/sql/
├── 001_core.sql
├── 002_indexes.sql
└── 003_jobs.sql
```

`MigrationRunner` discovers files in numeric order, hashes their exact contents and records the checksum and duration in `schema_migrations`. It obtains:

```sql
SELECT pg_advisory_xact_lock(hashtext('dbbench:migrations'));
```

before comparing or applying versions. An already-applied version is skipped only when its stored checksum still matches. Changing historical SQL raises `MigrationDriftError`; corrections require a new migration.

```bash
db-bench migrate
```

## Idempotent double-entry transfers

A transfer first inserts an intent using a unique idempotency key. A matching retry returns the original transfer; a different request using the same key fails. Account rows are selected in sorted order with `FOR UPDATE`, then both balances and the debit/credit ledger entries commit together.

```python
request = TransferRequest(
    source_account_id="account-a",
    destination_account_id="account-b",
    amount=Decimal("125.50"),
    idempotency_key="mobile-request-8841",
)
result = LedgerService(connection).transfer(request)
```

The invariant query groups entries by transfer and requires exactly two entries whose signed net is zero. The end-to-end verification also checks that total account balance remains unchanged.

This is an educational ledger, not a claim to be a complete banking core. A real ledger would use currency-specific minor units, immutable reversal entries, account/currency partitions, authorization/settlement states and external reconciliation.

## Durable concurrent jobs

Workers claim one eligible row using a CTE and `FOR UPDATE SKIP LOCKED`. The claim changes state, increments attempts and writes the lease owner/expiry atomically.

```sql
WITH candidate AS (
    SELECT job_id
    FROM durable_jobs
    WHERE queue_name = $1
      AND status IN ('queued', 'running')
      AND (lease_until IS NULL OR lease_until <= now())
    ORDER BY available_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE durable_jobs AS job
SET status = 'running', leased_by = $2, attempts = attempts + 1
FROM candidate
WHERE job.job_id = candidate.job_id
RETURNING job.*;
```

Only the active owner may succeed or fail the job. Failure releases the lease and applies bounded exponential delay; exhausting `max_attempts` moves the job to `dead_letter`.

## Keyset pagination

Offset pagination becomes unstable when rows are inserted between requests and grows expensive at high offsets. The transaction API instead orders by a unique compound position:

```sql
WHERE customer_id = $1
  AND (created_at, id) < ($cursor_time, $cursor_id)
ORDER BY created_at DESC, id DESC
LIMIT $page_size + 1;
```

`TransactionCursor` encodes both values as URL-safe base64 JSON and validates timezone and row ID when decoding. Fetching one extra row determines whether another page exists without a separate `COUNT(*)`.

## Query-plan analysis

The benchmark captures PostgreSQL JSON plans recursively and records:

- node, relation and index type;
- planned versus actual cardinality;
- actual rows, loops and execution time;
- shared hit/read blocks and cache-hit ratio;
- rows discarded by filters;
- sequential scans, index scans and nested loops.

`PlanBudget` turns expected behavior into a regression contract: maximum execution time, sequential scan count and shared disk reads. A sequential scan is not automatically condemned—on a small table it may be correct—but an unexpected scan on a known indexed workload becomes visible.

## Run the full verification

```bash
docker compose up -d postgres
pip install -e '.[dev]'
db-bench verify --seed-rows 3000 > platform-verification.json
db-bench benchmark --iterations 20 --output query-plans.json
```

`verify` runs all of the following against a real PostgreSQL instance:

1. discovers and applies three packaged migrations;
2. creates two accounts and transfers `125.50`;
3. replays the request and rejects a conflicting replay;
4. verifies double-entry balance and money conservation;
5. enqueues, exclusively leases and completes a durable job;
6. seeds a deterministic transaction workload;
7. walks two keyset pages and checks for overlap;
8. captures a real partial-index query plan.

The command exits non-zero if any computed invariant is false.

## CLI

```text
db-bench migrate
db-bench verify --seed-rows 3000
db-bench init --seed-rows 100000
db-bench benchmark --iterations 20 --output benchmark.json
```

The default DSN is `postgresql://postgres:postgres@localhost:5432/db_lab`; every command accepts `--dsn` before its subcommand.

## CI evidence

GitHub Actions provisions PostgreSQL 16 and runs:

- Ruff lint and formatting verification;
- unit tests for migration discovery, cursor validation and plan budgets;
- the complete transaction/lease/pagination verification;
- the query-plan benchmark;
- JSON artifact upload;
- wheel build and clean-environment resource discovery;
- Docker Compose configuration validation.

No benchmark number is hard-coded into the README. Performance depends on the runner and dataset; CI preserves the measured JSON so claims remain inspectable.

## Repository layout

```text
dbbench/
├── cli.py          command boundary
├── errors.py       typed operational failures
├── jobs.py         SKIP LOCKED queue and leases
├── ledger.py       idempotent transfer and double-entry checks
├── migrations.py   discovery, checksum and advisory locking
├── pagination.py   compound seek cursor
├── plans.py        EXPLAIN tree and plan budgets
├── scenarios.py    real PostgreSQL evidence run
├── workload.py     deterministic seeding and timing
└── sql/             packaged versioned migrations
tests/               fast deterministic regression tests
transactions/        manual isolation-level exercises
docker-compose.yml   local PostgreSQL 16
```

## Interview surface

`PostgreSQL` · `ACID` · `row locking` · `deadlock prevention` · `idempotency` · `double-entry ledger` · `advisory locks` · `schema migrations` · `SKIP LOCKED` · `leases` · `dead-letter queues` · `keyset pagination` · `EXPLAIN ANALYZE` · `buffer metrics` · `B-tree/GIN/partial indexes`
