from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random
from statistics import mean, median
import time
from typing import Iterable

import psycopg
from psycopg.rows import dict_row

from dbbench.plans import PlanSummary, parse_explain_json, plan_findings


@dataclass(frozen=True)
class QueryMeasurement:
    name: str
    iterations: int
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    execution_plan: PlanSummary | None
    findings: tuple[str, ...]


def connect(dsn: str):
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def execute_sql_file(connection, path: str | Path) -> None:
    sql = Path(path).read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql)


def seed_transactions(connection, rows: int = 100_000, seed: int = 42) -> None:
    rng = random.Random(seed)
    customer_ids = [f"customer-{index:05d}" for index in range(5000)]
    merchant_ids = [f"merchant-{index:04d}" for index in range(600)]

    batch: list[tuple] = []
    with connection.cursor() as cursor:
        for index in range(rows):
            customer_id = rng.choice(customer_ids)
            merchant_id = rng.choice(merchant_ids)
            amount = round(max(0.5, rng.lognormvariate(3.7, 1.0)), 2)
            country = rng.choice(["TR", "DE", "GB", "NL", "US"])
            status = rng.choices(
                ["approved", "declined", "review"],
                weights=[0.92, 0.05, 0.03],
                k=1,
            )[0]
            batch.append((f"txn-{index:09d}", customer_id, merchant_id, amount, country, status))
            if len(batch) >= 2000:
                cursor.executemany(
                    """
                    INSERT INTO transactions(
                        transaction_id, customer_id, merchant_id, amount, country, status
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    batch,
                )
                batch.clear()
        if batch:
            cursor.executemany(
                """
                INSERT INTO transactions(
                    transaction_id, customer_id, merchant_id, amount, country, status
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                batch,
            )


def explain(connection, sql: str, params: tuple = ()) -> PlanSummary:
    with connection.cursor() as cursor:
        cursor.execute(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql,
            params,
        )
        row = cursor.fetchone()
    return parse_explain_json(row["QUERY PLAN"])


def benchmark_query(
    connection,
    *,
    name: str,
    sql: str,
    params: tuple = (),
    iterations: int = 20,
    include_plan: bool = True,
) -> QueryMeasurement:
    timings: list[float] = []
    with connection.cursor() as cursor:
        for _ in range(iterations):
            started = time.perf_counter()
            cursor.execute(sql, params)
            cursor.fetchall()
            timings.append((time.perf_counter() - started) * 1000)

    plan = explain(connection, sql, params) if include_plan else None
    return QueryMeasurement(
        name=name,
        iterations=iterations,
        mean_ms=mean(timings),
        median_ms=median(timings),
        min_ms=min(timings),
        max_ms=max(timings),
        execution_plan=plan,
        findings=tuple(plan_findings(plan)) if plan else (),
    )


def standard_workloads() -> list[tuple[str, str, tuple]]:
    return [
        (
            "customer_recent_transactions",
            """
            SELECT transaction_id, amount, status, created_at
            FROM transactions
            WHERE customer_id = %s
            ORDER BY created_at DESC
            LIMIT 100
            """,
            ("customer-00042",),
        ),
        (
            "high_value_review_queue",
            """
            SELECT transaction_id, customer_id, merchant_id, amount, country
            FROM transactions
            WHERE status = 'review' AND amount >= %s
            ORDER BY amount DESC
            LIMIT 200
            """,
            (150.0,),
        ),
        (
            "merchant_aggregate",
            """
            SELECT merchant_id, count(*) AS transactions, avg(amount) AS avg_amount
            FROM transactions
            WHERE created_at >= now() - interval '30 days'
            GROUP BY merchant_id
            ORDER BY transactions DESC
            LIMIT 50
            """,
            (),
        ),
    ]
