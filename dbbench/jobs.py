from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from dbbench.errors import LeaseConflict

MAX_LEASE_SECONDS = 3_600
MAX_OWNER_LENGTH = 128
_OWNER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")


@dataclass(frozen=True, slots=True)
class DurableJob:
    job_id: UUID
    queue_name: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    lease_owner: str | None
    lease_until: datetime | None


class PostgresJobQueue:
    """Concurrent worker queue using `FOR UPDATE SKIP LOCKED` claiming."""

    def __init__(self, connection) -> None:
        self.connection = connection

    def enqueue(
        self,
        queue_name: str,
        payload: dict[str, Any],
        *,
        max_attempts: int = 3,
        job_id: UUID | None = None,
    ) -> DurableJob:
        if not queue_name or not 1 <= max_attempts <= 20:
            raise ValueError("queue name and max_attempts in [1, 20] are required")
        selected_id = job_id or uuid4()
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO durable_jobs(job_id, queue_name, payload, status, max_attempts)
                VALUES (%s, %s, %s, 'queued', %s)
                RETURNING *
                """,
                (selected_id, queue_name, Jsonb(payload), max_attempts),
            )
            return self._job(cursor.fetchone())

    def claim(
        self,
        queue_name: str,
        owner: str,
        *,
        lease_seconds: int = 60,
    ) -> DurableJob | None:
        self._validate_lease(owner, lease_seconds=lease_seconds)
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                WITH candidate AS (
                    SELECT job_id FROM durable_jobs
                    WHERE queue_name = %s
                      AND status IN ('queued', 'running')
                      AND attempts < max_attempts
                      AND available_at <= now()
                      AND (lease_until IS NULL OR lease_until <= now())
                    ORDER BY available_at, created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE durable_jobs AS job
                SET status = 'running', attempts = attempts + 1, leased_by = %s,
                    lease_until = now() + make_interval(secs => %s), updated_at = now()
                FROM candidate
                WHERE job.job_id = candidate.job_id
                RETURNING job.*
                """,
                (queue_name, owner, lease_seconds),
            )
            row = cursor.fetchone()
            return self._job(row) if row else None

    def renew(
        self,
        job_id: UUID,
        owner: str,
        *,
        expected_attempt: int,
        lease_seconds: int = 60,
    ) -> DurableJob:
        """Renew an unexpired lease held by the expected claim attempt."""

        self._validate_lease(
            owner,
            expected_attempt=expected_attempt,
            lease_seconds=lease_seconds,
        )
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE durable_jobs
                SET lease_until = greatest(
                        lease_until,
                        now() + make_interval(secs => %s)
                    ),
                    updated_at = now()
                WHERE job_id = %s AND leased_by = %s AND status = 'running'
                  AND attempts = %s AND lease_until > now()
                RETURNING *
                """,
                (lease_seconds, job_id, owner, expected_attempt),
            )
            row = cursor.fetchone()
            if not row:
                raise LeaseConflict("lease renewal requires the active fenced claim")
            return self._job(row)

    def succeed(
        self,
        job_id: UUID,
        owner: str,
        *,
        expected_attempt: int,
    ) -> DurableJob:
        return self._finish(
            job_id,
            owner,
            "succeeded",
            expected_attempt=expected_attempt,
        )

    def fail(
        self,
        job_id: UUID,
        owner: str,
        error: str,
        *,
        expected_attempt: int,
    ) -> DurableJob:
        self._validate_lease(owner, expected_attempt=expected_attempt)
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE durable_jobs
                SET status = CASE WHEN attempts >= max_attempts THEN 'dead_letter' ELSE 'queued' END,
                    available_at = CASE
                        WHEN attempts >= max_attempts THEN available_at
                        ELSE now() + make_interval(secs => least(300, power(2, attempts)::int))
                    END,
                    last_error = %s, leased_by = NULL, lease_until = NULL, updated_at = now()
                WHERE job_id = %s AND leased_by = %s AND status = 'running'
                  AND attempts = %s AND lease_until > now()
                RETURNING *
                """,
                (error[:2000], job_id, owner, expected_attempt),
            )
            row = cursor.fetchone()
            if not row:
                raise LeaseConflict("failure requires the active fenced claim")
            return self._job(row)

    def _finish(
        self,
        job_id: UUID,
        owner: str,
        status: str,
        *,
        expected_attempt: int,
    ) -> DurableJob:
        self._validate_lease(owner, expected_attempt=expected_attempt)
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE durable_jobs SET status = %s, leased_by = NULL, lease_until = NULL,
                    updated_at = now()
                WHERE job_id = %s AND leased_by = %s AND status = 'running'
                  AND attempts = %s AND lease_until > now()
                RETURNING *
                """,
                (status, job_id, owner, expected_attempt),
            )
            row = cursor.fetchone()
            if not row:
                raise LeaseConflict("completion requires the active fenced claim")
            return self._job(row)

    @staticmethod
    def _validate_lease(
        owner: str,
        *,
        expected_attempt: int | None = None,
        lease_seconds: int | None = None,
    ) -> None:
        if not isinstance(owner, str) or not _OWNER_ID.fullmatch(owner):
            raise ValueError(
                f"owner must be a constrained identifier of at most {MAX_OWNER_LENGTH} characters"
            )
        if expected_attempt is not None and (
            isinstance(expected_attempt, bool)
            or not isinstance(expected_attempt, int)
            or expected_attempt < 1
        ):
            raise ValueError("expected_attempt must be a positive integer")
        if lease_seconds is not None and (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 1 <= lease_seconds <= MAX_LEASE_SECONDS
        ):
            raise ValueError(f"lease_seconds must be in [1, {MAX_LEASE_SECONDS}]")

    @staticmethod
    def _job(row) -> DurableJob:
        return DurableJob(
            job_id=row["job_id"],
            queue_name=row["queue_name"],
            payload=row["payload"],
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            lease_owner=row["leased_by"],
            lease_until=row["lease_until"],
        )
