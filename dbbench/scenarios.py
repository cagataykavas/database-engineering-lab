from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from typing import Any
from uuid import UUID

from dbbench.errors import TransferConflict
from dbbench.jobs import PostgresJobQueue
from dbbench.ledger import LedgerService, TransferRequest
from dbbench.migrations import MigrationRunner
from dbbench.pagination import list_customer_transactions
from dbbench.workload import benchmark_query, seed_transactions


def verify_platform(connection, *, seed_rows: int = 3000) -> dict[str, Any]:
    migrations = MigrationRunner(connection).apply()
    migration_replay = MigrationRunner(connection).apply()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            TRUNCATE ledger_entries, transfers, durable_jobs, account_balances, transactions
            RESTART IDENTITY CASCADE
            """
        )

    ledger = LedgerService(connection)
    ledger.create_account("account-a", Decimal("1000.00"))
    ledger.create_account("account-b", Decimal("250.00"))
    request = TransferRequest(
        "account-a",
        "account-b",
        Decimal("125.50"),
        "verification-transfer-1",
    )
    transfer_id = UUID("00000000-0000-0000-0000-000000000001")
    transfer = ledger.transfer(request, transfer_id=transfer_id)
    replay = ledger.transfer(request)
    conflicting_replay_rejected = False
    try:
        ledger.transfer(
            TransferRequest(
                "account-a",
                "account-b",
                Decimal("999.00"),
                "verification-transfer-1",
            )
        )
    except TransferConflict:
        conflicting_replay_rejected = True

    queue = PostgresJobQueue(connection)
    job = queue.enqueue(
        "risk-scoring",
        {"transaction_id": "txn-000000001"},
        max_attempts=3,
        job_id=UUID("00000000-0000-0000-0000-000000000002"),
    )
    claimed = queue.claim("risk-scoring", "worker-a")
    second_claim = queue.claim("risk-scoring", "worker-b")
    completed = queue.succeed(job.job_id, "worker-a")

    seed_transactions(connection, rows=seed_rows, seed=42)
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO transactions(
                transaction_id, customer_id, merchant_id, amount, country, status, created_at
            ) VALUES (%s, 'pagination-customer', 'merchant-demo', %s, 'TR', 'approved', %s)
            """,
            [
                (f"pagination-{index}", Decimal("10.00") + index, f"2026-01-0{index}T00:00:00Z")
                for index in range(1, 6)
            ],
        )
    page_one = list_customer_transactions(connection, "pagination-customer", limit=2)
    page_two = (
        list_customer_transactions(
            connection,
            "pagination-customer",
            limit=2,
            cursor=page_one.next_cursor,
        )
        if page_one.next_cursor
        else None
    )
    measurement = benchmark_query(
        connection,
        name="review_queue",
        sql="""
            SELECT transaction_id, amount FROM transactions
            WHERE status = 'review' AND amount >= %s
            ORDER BY amount DESC LIMIT 100
        """,
        params=(Decimal("100.00"),),
        iterations=2,
        include_plan=True,
    )
    page_one_ids = {row["id"] for row in page_one.rows}
    page_two_ids = {row["id"] for row in page_two.rows} if page_two else set()
    ledger_report = ledger.invariant_report()
    return {
        "migrations": [asdict(result) for result in migrations],
        "migration_replay": [asdict(result) for result in migration_replay],
        "transfer": {
            "transfer_id": str(transfer.transfer_id),
            "source_balance": str(transfer.source_balance),
            "destination_balance": str(transfer.destination_balance),
            "replay_detected": replay.replayed,
        },
        "ledger": ledger_report,
        "job": {
            "job_id": str(job.job_id),
            "claimed_by": claimed.lease_owner if claimed else None,
            "second_worker_claimed": second_claim is not None,
            "final_status": completed.status,
        },
        "pagination": {
            "first_page_rows": len(page_one.rows),
            "second_page_rows": len(page_two.rows) if page_two else 0,
            "overlap": sorted(page_one_ids & page_two_ids),
        },
        "query": {
            "name": measurement.name,
            "median_ms": measurement.median_ms,
            "findings": list(measurement.findings),
            "execution_time_ms": (
                measurement.execution_plan.execution_time_ms if measurement.execution_plan else None
            ),
            "index_scans": (
                measurement.execution_plan.index_scan_count if measurement.execution_plan else None
            ),
        },
        "invariants": {
            "migration_count": len(migrations) == 3,
            "migrations_are_idempotent": not any(result.applied for result in migration_replay),
            "transfer_is_idempotent": replay.transfer_id == transfer.transfer_id,
            "conflicting_replay_rejected": conflicting_replay_rejected,
            "double_entry_balanced": ledger_report["balanced"],
            "money_conserved": ledger_report["total_account_balance"] == "1250.00",
            "job_exclusively_leased": claimed is not None and second_claim is None,
            "job_completed": completed.status == "succeeded",
            "keyset_pages_do_not_overlap": not (page_one_ids & page_two_ids),
            "keyset_page_is_complete": len(page_one_ids) == 2 and len(page_two_ids) == 2,
        },
    }


def print_report(report: dict[str, Any]) -> None:
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
