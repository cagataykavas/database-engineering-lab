from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

SETTINGS_SQL = """
SELECT
    current_setting('max_connections')::integer AS max_connections,
    current_setting('reserved_connections')::integer AS reserved_connections,
    current_setting('superuser_reserved_connections')::integer
        AS superuser_reserved_connections
"""

ACTIVITY_SQL = """
SELECT
    count(*)::integer AS client_connections,
    count(*) FILTER (WHERE state = 'active')::integer AS active_connections,
    count(*) FILTER (WHERE state = 'idle')::integer AS idle_connections,
    count(*) FILTER (
        WHERE state LIKE 'idle in transaction%'
    )::integer AS idle_in_transaction_connections,
    count(*) FILTER (
        WHERE state = 'active'
          AND wait_event_type IS NOT NULL
          AND wait_event_type <> 'Client'
    )::integer AS waiting_connections,
    COALESCE(
        max(EXTRACT(EPOCH FROM (clock_timestamp() - state_change))) FILTER (
            WHERE state LIKE 'idle in transaction%'
        ),
        0
    )::double precision AS oldest_idle_in_transaction_seconds
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND datname = current_database()
"""


def _require_non_negative_integer(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _require_finite_non_negative(name: str, value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite non-negative number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite non-negative number")


@dataclass(frozen=True, slots=True)
class ConnectionSnapshot:
    max_connections: int
    reserved_connections: int
    superuser_reserved_connections: int
    client_connections: int
    active_connections: int
    idle_connections: int
    idle_in_transaction_connections: int
    waiting_connections: int
    oldest_idle_in_transaction_seconds: float

    def __post_init__(self) -> None:
        integer_fields = (
            "max_connections",
            "reserved_connections",
            "superuser_reserved_connections",
            "client_connections",
            "active_connections",
            "idle_connections",
            "idle_in_transaction_connections",
            "waiting_connections",
        )
        for field_name in integer_fields:
            _require_non_negative_integer(field_name, getattr(self, field_name))

        if self.max_connections == 0:
            raise ValueError("max_connections must be greater than zero")
        total_reserved = self.reserved_connections + self.superuser_reserved_connections
        if total_reserved >= self.max_connections:
            raise ValueError("reserved connection slots must be below max_connections")
        classified_connections = (
            self.active_connections + self.idle_connections + self.idle_in_transaction_connections
        )
        if classified_connections > self.client_connections:
            raise ValueError("classified connection counts exceed client_connections")
        if self.waiting_connections > self.active_connections:
            raise ValueError("waiting_connections cannot exceed active_connections")

        _require_finite_non_negative(
            "oldest_idle_in_transaction_seconds",
            self.oldest_idle_in_transaction_seconds,
        )
        if (
            self.idle_in_transaction_connections == 0
            and self.oldest_idle_in_transaction_seconds != 0
        ):
            raise ValueError(
                "oldest_idle_in_transaction_seconds must be zero when no idle transaction exists"
            )


@dataclass(frozen=True, slots=True)
class ConnectionPolicy:
    pool_size_per_replica: int = 10
    replicas: int = 1
    min_free_connections: int = 5
    max_utilization: float = 0.8
    max_idle_in_transaction: int = 0
    max_idle_in_transaction_seconds: float = 60.0
    max_waiting_connections: int = 0

    def __post_init__(self) -> None:
        for field_name in (
            "pool_size_per_replica",
            "replicas",
            "min_free_connections",
            "max_idle_in_transaction",
            "max_waiting_connections",
        ):
            _require_non_negative_integer(field_name, getattr(self, field_name))
        if self.pool_size_per_replica == 0:
            raise ValueError("pool_size_per_replica must be greater than zero")
        if self.replicas == 0:
            raise ValueError("replicas must be greater than zero")
        if (
            isinstance(self.max_utilization, bool)
            or not isinstance(self.max_utilization, (int, float))
            or not math.isfinite(self.max_utilization)
            or not 0 < self.max_utilization <= 1
        ):
            raise ValueError("max_utilization must be finite and in the interval (0, 1]")
        _require_finite_non_negative(
            "max_idle_in_transaction_seconds",
            self.max_idle_in_transaction_seconds,
        )


@dataclass(frozen=True, slots=True)
class ConnectionReport:
    passed: bool
    reasons: tuple[str, ...]
    snapshot: ConnectionSnapshot
    policy: ConnectionPolicy
    usable_connections: int
    requested_pool_connections: int
    pool_capacity_limit: int
    free_usable_connections: int
    observed_utilization: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        payload["observed_utilization"] = round(self.observed_utilization, 6)
        return payload


def evaluate_connection_budget(
    snapshot: ConnectionSnapshot,
    policy: ConnectionPolicy,
) -> ConnectionReport:
    usable_connections = (
        snapshot.max_connections
        - snapshot.reserved_connections
        - snapshot.superuser_reserved_connections
    )
    requested_pool_connections = policy.pool_size_per_replica * policy.replicas
    pool_capacity_limit = max(usable_connections - policy.min_free_connections, 0)
    free_usable_connections = max(usable_connections - snapshot.client_connections, 0)
    observed_utilization = snapshot.client_connections / usable_connections

    reasons: list[str] = []
    if requested_pool_connections > pool_capacity_limit:
        reasons.append("pool_capacity_exceeded")
    if observed_utilization > policy.max_utilization:
        reasons.append("observed_utilization_exceeded")
    if snapshot.idle_in_transaction_connections > policy.max_idle_in_transaction:
        reasons.append("idle_in_transaction_count_exceeded")
    if (
        snapshot.idle_in_transaction_connections > 0
        and snapshot.oldest_idle_in_transaction_seconds > policy.max_idle_in_transaction_seconds
    ):
        reasons.append("idle_in_transaction_age_exceeded")
    if snapshot.waiting_connections > policy.max_waiting_connections:
        reasons.append("waiting_connections_exceeded")

    return ConnectionReport(
        passed=not reasons,
        reasons=tuple(reasons),
        snapshot=snapshot,
        policy=policy,
        usable_connections=usable_connections,
        requested_pool_connections=requested_pool_connections,
        pool_capacity_limit=pool_capacity_limit,
        free_usable_connections=free_usable_connections,
        observed_utilization=observed_utilization,
    )


def _require_row(row: object, expected_fields: tuple[str, ...]) -> Mapping[str, Any]:
    if not isinstance(row, Mapping):
        raise TypeError("PostgreSQL returned a malformed connection-capacity row")
    missing = [field for field in expected_fields if field not in row]
    if missing:
        raise ValueError(f"PostgreSQL connection-capacity row is missing: {', '.join(missing)}")
    return row


def collect_connection_snapshot(connection: Any) -> ConnectionSnapshot:
    with connection.cursor() as cursor:
        cursor.execute(SETTINGS_SQL)
        settings = _require_row(
            cursor.fetchone(),
            (
                "max_connections",
                "reserved_connections",
                "superuser_reserved_connections",
            ),
        )
        cursor.execute(ACTIVITY_SQL)
        activity = _require_row(
            cursor.fetchone(),
            (
                "client_connections",
                "active_connections",
                "idle_connections",
                "idle_in_transaction_connections",
                "waiting_connections",
                "oldest_idle_in_transaction_seconds",
            ),
        )

    try:
        return ConnectionSnapshot(
            max_connections=settings["max_connections"],
            reserved_connections=settings["reserved_connections"],
            superuser_reserved_connections=settings["superuser_reserved_connections"],
            client_connections=activity["client_connections"],
            active_connections=activity["active_connections"],
            idle_connections=activity["idle_connections"],
            idle_in_transaction_connections=activity["idle_in_transaction_connections"],
            waiting_connections=activity["waiting_connections"],
            oldest_idle_in_transaction_seconds=activity["oldest_idle_in_transaction_seconds"],
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("PostgreSQL returned invalid connection-capacity values") from exc
