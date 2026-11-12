from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import UUID

import pytest

from dbbench.errors import LeaseConflict
from dbbench.jobs import PostgresJobQueue

JOB_ID = UUID("00000000-0000-0000-0000-000000000042")


def _job_row(*, attempts: int = 2) -> dict[str, object]:
    return {
        "job_id": JOB_ID,
        "queue_name": "risk-scoring",
        "payload": {"case_id": "case-42"},
        "status": "running",
        "attempts": attempts,
        "max_attempts": 3,
        "leased_by": "worker-a",
        "lease_until": datetime.now(UTC) + timedelta(minutes=2),
    }


class FakeCursor:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.row = row
        self.query = ""
        self.params: tuple[object, ...] = ()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.query = " ".join(query.split())
        self.params = params

    def fetchone(self) -> dict[str, object] | None:
        return self.row


class FakeConnection:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.cursor_instance = FakeCursor(row)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def transaction(self) -> FakeConnection:
        return self

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


def test_renew_requires_owner_attempt_and_unexpired_lease() -> None:
    connection = FakeConnection(_job_row())
    queue = PostgresJobQueue(connection)

    renewed = queue.renew(
        JOB_ID,
        "worker-a",
        expected_attempt=2,
        lease_seconds=120,
    )

    assert renewed.attempts == 2
    assert "greatest(" in connection.cursor_instance.query
    assert "attempts = %s" in connection.cursor_instance.query
    assert "lease_until > now()" in connection.cursor_instance.query
    assert connection.cursor_instance.params == (120, JOB_ID, "worker-a", 2)


def test_completion_is_fenced_by_claim_attempt_and_expiry() -> None:
    connection = FakeConnection(_job_row())
    queue = PostgresJobQueue(connection)

    completed = queue.succeed(JOB_ID, "worker-a", expected_attempt=2)

    assert completed.job_id == JOB_ID
    assert "attempts = %s" in connection.cursor_instance.query
    assert "lease_until > now()" in connection.cursor_instance.query
    assert connection.cursor_instance.params == ("succeeded", JOB_ID, "worker-a", 2)


def test_failure_is_fenced_and_bounds_persisted_error() -> None:
    connection = FakeConnection(_job_row())
    queue = PostgresJobQueue(connection)

    queue.fail(JOB_ID, "worker-a", "x" * 3_000, expected_attempt=2)

    assert "attempts = %s" in connection.cursor_instance.query
    assert "lease_until > now()" in connection.cursor_instance.query
    assert connection.cursor_instance.params == ("x" * 2_000, JOB_ID, "worker-a", 2)


@pytest.mark.parametrize("operation", ["renew", "succeed", "fail"])
def test_stale_or_expired_claim_is_rejected(operation: str) -> None:
    queue = PostgresJobQueue(FakeConnection(None))

    with pytest.raises(LeaseConflict, match="active fenced claim"):
        if operation == "renew":
            queue.renew(JOB_ID, "worker-a", expected_attempt=1)
        elif operation == "succeed":
            queue.succeed(JOB_ID, "worker-a", expected_attempt=1)
        else:
            queue.fail(JOB_ID, "worker-a", "failed", expected_attempt=1)


@pytest.mark.parametrize("owner", ["", " worker-a", "worker-a ", "worker a", "x" * 129])
def test_invalid_owner_is_rejected(owner: str) -> None:
    queue = PostgresJobQueue(FakeConnection(_job_row()))

    with pytest.raises(ValueError, match="owner"):
        queue.succeed(JOB_ID, owner, expected_attempt=1)


@pytest.mark.parametrize("attempt", [0, -1, True, 1.5])
def test_invalid_fencing_token_is_rejected(attempt: object) -> None:
    queue = PostgresJobQueue(FakeConnection(_job_row()))

    with pytest.raises(ValueError, match="expected_attempt"):
        queue.succeed(JOB_ID, "worker-a", expected_attempt=attempt)  # type: ignore[arg-type]


@pytest.mark.parametrize("lease_seconds", [0, 3_601, True, 1.5])
def test_invalid_lease_duration_is_rejected(lease_seconds: object) -> None:
    queue = PostgresJobQueue(FakeConnection(_job_row()))

    with pytest.raises(ValueError, match="lease_seconds"):
        queue.renew(
            JOB_ID,
            "worker-a",
            expected_attempt=1,
            lease_seconds=lease_seconds,  # type: ignore[arg-type]
        )


def test_claim_enforces_lease_duration_limit() -> None:
    queue = PostgresJobQueue(FakeConnection(None))

    with pytest.raises(ValueError, match="lease_seconds"):
        queue.claim("risk-scoring", "worker-a", lease_seconds=3_601)
