from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from dbbench.errors import LeaseConflict


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
        if not owner or lease_seconds < 1:
            raise ValueError("owner and positive lease duration are required")
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

    def succeed(self, job_id: UUID, owner: str) -> DurableJob:
        return self._finish(job_id, owner, "succeeded")

    def fail(self, job_id: UUID, owner: str, error: str) -> DurableJob:
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
                RETURNING *
                """,
                (error[:2000], job_id, owner),
            )
            row = cursor.fetchone()
            if not row:
                raise LeaseConflict("only the active lease owner can fail a running job")
            return self._job(row)

    def _finish(self, job_id: UUID, owner: str, status: str) -> DurableJob:
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE durable_jobs SET status = %s, leased_by = NULL, lease_until = NULL,
                    updated_at = now()
                WHERE job_id = %s AND leased_by = %s AND status = 'running'
                RETURNING *
                """,
                (status, job_id, owner),
            )
            row = cursor.fetchone()
            if not row:
                raise LeaseConflict("only the active lease owner can finish a running job")
            return self._job(row)

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
