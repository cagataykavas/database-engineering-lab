from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID, uuid4

from dbbench.errors import InsufficientFunds, TransferConflict


@dataclass(frozen=True, slots=True)
class TransferRequest:
    source_account_id: str
    destination_account_id: str
    amount: Decimal
    idempotency_key: str

    def __post_init__(self) -> None:
        if not self.source_account_id or not self.destination_account_id:
            raise ValueError("both account ids are required")
        if self.source_account_id == self.destination_account_id:
            raise ValueError("source and destination accounts must differ")
        if self.amount <= 0:
            raise ValueError("transfer amount must be positive")
        if not self.idempotency_key.strip():
            raise ValueError("idempotency key is required")

    @property
    def request_hash(self) -> str:
        payload = {
            "source": self.source_account_id,
            "destination": self.destination_account_id,
            "amount": str(self.amount.quantize(Decimal("0.01"))),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class TransferResult:
    transfer_id: UUID
    source_balance: Decimal
    destination_balance: Decimal
    replayed: bool


class LedgerService:
    """Idempotent double-entry transfer with deterministic row-lock ordering."""

    def __init__(self, connection) -> None:
        self.connection = connection

    def create_account(self, account_id: str, opening_balance: Decimal) -> None:
        if opening_balance < 0:
            raise ValueError("opening balance cannot be negative")
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO account_balances(account_id, balance)
                VALUES (%s, %s)
                ON CONFLICT(account_id) DO NOTHING
                """,
                (account_id, opening_balance),
            )

    def transfer(
        self,
        request: TransferRequest,
        *,
        transfer_id: UUID | None = None,
    ) -> TransferResult:
        selected_id = transfer_id or uuid4()
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO transfers(
                    transfer_id, idempotency_key, request_hash, source_account_id,
                    destination_account_id, amount, status
                ) VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT(idempotency_key) DO NOTHING
                RETURNING transfer_id
                """,
                (
                    selected_id,
                    request.idempotency_key,
                    request.request_hash,
                    request.source_account_id,
                    request.destination_account_id,
                    request.amount,
                ),
            )
            inserted = cursor.fetchone()
            if inserted is None:
                return self._replay(cursor, request)

            account_ids = sorted((request.source_account_id, request.destination_account_id))
            cursor.execute(
                """
                SELECT account_id, balance FROM account_balances
                WHERE account_id = ANY(%s)
                ORDER BY account_id
                FOR UPDATE
                """,
                (account_ids,),
            )
            balances = {row["account_id"]: row["balance"] for row in cursor.fetchall()}
            missing = set(account_ids) - set(balances)
            if missing:
                raise KeyError(f"unknown accounts: {sorted(missing)}")
            source_balance = balances[request.source_account_id]
            if source_balance < request.amount:
                raise InsufficientFunds(
                    f"account {request.source_account_id!r} has insufficient funds"
                )

            cursor.execute(
                """
                UPDATE account_balances
                SET balance = balance - %s, version = version + 1, updated_at = now()
                WHERE account_id = %s
                RETURNING balance
                """,
                (request.amount, request.source_account_id),
            )
            new_source = cursor.fetchone()["balance"]
            cursor.execute(
                """
                UPDATE account_balances
                SET balance = balance + %s, version = version + 1, updated_at = now()
                WHERE account_id = %s
                RETURNING balance
                """,
                (request.amount, request.destination_account_id),
            )
            new_destination = cursor.fetchone()["balance"]
            cursor.executemany(
                """
                INSERT INTO ledger_entries(transfer_id, account_id, direction, amount)
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (selected_id, request.source_account_id, "debit", request.amount),
                    (selected_id, request.destination_account_id, "credit", request.amount),
                ],
            )
            cursor.execute(
                """
                UPDATE transfers SET status = 'committed', committed_at = now(),
                    source_balance_after = %s, destination_balance_after = %s
                WHERE transfer_id = %s
                """,
                (new_source, new_destination, selected_id),
            )
            return TransferResult(selected_id, new_source, new_destination, False)

    @staticmethod
    def _replay(cursor, request: TransferRequest) -> TransferResult:
        cursor.execute(
            """
            SELECT transfer_id, request_hash, status,
                   source_balance_after, destination_balance_after
            FROM transfers WHERE idempotency_key = %s
            """,
            (request.idempotency_key,),
        )
        existing = cursor.fetchone()
        if existing["request_hash"] != request.request_hash:
            raise TransferConflict("idempotency key was reused with a different transfer")
        if existing["status"] != "committed":
            raise TransferConflict("matching transfer is still pending")
        return TransferResult(
            existing["transfer_id"],
            existing["source_balance_after"],
            existing["destination_balance_after"],
            True,
        )

    def invariant_report(self) -> dict[str, object]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT transfer_id,
                       count(*) AS entries,
                       sum(CASE direction WHEN 'credit' THEN amount ELSE -amount END) AS net
                FROM ledger_entries
                GROUP BY transfer_id
                HAVING count(*) <> 2
                    OR sum(CASE direction WHEN 'credit' THEN amount ELSE -amount END) <> 0
                """
            )
            violations = cursor.fetchall()
            cursor.execute("SELECT coalesce(sum(balance), 0) AS total FROM account_balances")
            total = cursor.fetchone()["total"]
        return {
            "balanced": not violations,
            "violating_transfer_ids": [str(row["transfer_id"]) for row in violations],
            "total_account_balance": str(total),
        }
