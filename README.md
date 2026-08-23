# PostgreSQL Engineering & Benchmark Lab

A runnable PostgreSQL project for exploring **indexes, query plans, cardinality estimates, transactions and workload performance** instead of treating SQL as a collection of syntax exercises.

The project provisions PostgreSQL locally, generates a synthetic transaction workload, runs representative queries repeatedly, captures `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` output and turns the plan tree into machine-readable findings.

## Architecture

```mermaid
flowchart LR
    GEN[Synthetic workload generator] --> PG[(PostgreSQL 16)]
    SCHEMA[Schema + indexes] --> PG
    CLI[db-bench CLI] --> PG
    PG --> PLAN[EXPLAIN ANALYZE JSON]
    PLAN --> PARSE[Plan-tree analyzer]
    PARSE --> REPORT[Latency + scans + cardinality findings]
```

## What it demonstrates

- composite indexes;
- partial indexes;
- JSONB + GIN indexing;
- `EXPLAIN ANALYZE` plan parsing;
- sequential scan vs index scan reasoning;
- cardinality-estimation errors;
- repeatable workload timing;
- transaction/isolation examples under `transactions/`;
- Dockerized PostgreSQL setup;
- CI integration against a real Postgres service.

## Start PostgreSQL

```bash
docker compose up -d postgres
pip install -e '.[dev]'
```

Initialize schema and seed 100k synthetic transactions:

```bash
db-bench init --seed-rows 100000
```

Run the standard workload suite:

```bash
db-bench benchmark --iterations 20 --output benchmark.json
```

The default DSN is:

```text
postgresql://postgres:postgres@localhost:5432/db_lab
```

Override it using `--dsn` when testing another PostgreSQL instance.

## Workloads

The benchmark currently exercises three distinct access patterns:

1. recent transactions for one customer — benefits from `(customer_id, created_at DESC)`;
2. high-value manual-review queue — demonstrates a partial index over `status = 'review'`;
3. merchant aggregation — scan/aggregate workload useful for discussing OLTP vs analytical access patterns.

## Plan analysis

`dbbench/plans.py` recursively walks PostgreSQL JSON plan trees and records:

- node type;
- relation/index;
- planned vs actual rows;
- costs;
- actual timing;
- loops;
- filters;
- tree depth.

It then flags examples such as:

- large sequential scans;
- 10x+ cardinality underestimates;
- 10x+ cardinality overestimates;
- queries exceeding a simple slow-query threshold.

The aim is not to pretend every sequential scan is bad. A sequential scan on a small table can be exactly what the optimizer should choose. The tool gives you the evidence needed to discuss **why** the plan is sensible or suspicious.

## Schema highlights

```sql
CREATE INDEX idx_transactions_customer_created
    ON transactions(customer_id, created_at DESC);

CREATE INDEX idx_transactions_review_amount
    ON transactions(amount DESC)
    WHERE status = 'review';

CREATE INDEX idx_transactions_metadata_gin
    ON transactions USING GIN(metadata);
```

This provides concrete examples for composite, partial and GIN indexes.

## Repository layout

```text
database-engineering-lab/
├── dbbench/
│   ├── cli.py
│   ├── plans.py
│   └── workload.py
├── sql/
│   ├── schema.sql
│   └── indexing.sql
├── transactions/
├── tests/test_plans.py
├── docker-compose.yml
├── pyproject.toml
└── .github/workflows/ci.yml
```

## Existing transaction labs

The `transactions/` directory contains examples around isolation and locking. These complement the benchmark project because database performance and database correctness are separate concerns: making a query faster is useless if concurrent updates make its result wrong.

## Next extensions

- connection-pool benchmark;
- optimistic vs pessimistic concurrency demo;
- reproducible deadlock scenario;
- table partitioning benchmark;
- materialized views;
- logical replication concepts;
- pg_stat_statements reporting;
- BRIN vs B-tree comparison on time-ordered data;
- benchmark before/after index creation.

## Interview topics demonstrated

`PostgreSQL` · `EXPLAIN ANALYZE` · `B-tree` · `GIN` · `partial index` · `composite index` · `cardinality estimation` · `query planner` · `transactions` · `isolation levels` · `locking` · `OLTP vs OLAP`
