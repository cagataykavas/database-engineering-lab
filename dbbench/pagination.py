from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class TransactionCursor:
    created_at: datetime
    row_id: int

    def encode(self) -> str:
        if self.created_at.tzinfo is None:
            raise ValueError("cursor timestamp must be timezone-aware")
        payload = json.dumps(
            {"created_at": self.created_at.astimezone(UTC).isoformat(), "row_id": self.row_id},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(payload).decode().rstrip("=")

    @classmethod
    def decode(cls, value: str) -> TransactionCursor:
        try:
            padded = value + "=" * (-len(value) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode())
            created_at = datetime.fromisoformat(payload["created_at"])
            row_id = int(payload["row_id"])
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            raise ValueError("invalid transaction cursor") from exc
        if created_at.tzinfo is None or row_id < 1:
            raise ValueError("invalid transaction cursor")
        return cls(created_at.astimezone(UTC), row_id)


@dataclass(frozen=True, slots=True)
class TransactionPage:
    rows: tuple[dict, ...]
    next_cursor: str | None


def list_customer_transactions(
    connection,
    customer_id: str,
    *,
    limit: int = 100,
    cursor: str | None = None,
) -> TransactionPage:
    if not customer_id:
        raise ValueError("customer_id is required")
    bounded_limit = max(1, min(limit, 500))
    parameters: list[object] = [customer_id]
    seek_clause = ""
    if cursor:
        position = TransactionCursor.decode(cursor)
        seek_clause = "AND (created_at, id) < (%s, %s)"
        parameters.extend((position.created_at, position.row_id))
    parameters.append(bounded_limit + 1)
    with connection.cursor() as database_cursor:
        database_cursor.execute(
            f"""
            SELECT id, transaction_id, amount, status, created_at
            FROM transactions
            WHERE customer_id = %s
              {seek_clause}
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            parameters,
        )
        selected = database_cursor.fetchall()
    has_more = len(selected) > bounded_limit
    rows = selected[:bounded_limit]
    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = TransactionCursor(last["created_at"], last["id"]).encode()
    return TransactionPage(tuple(dict(row) for row in rows), next_cursor)
