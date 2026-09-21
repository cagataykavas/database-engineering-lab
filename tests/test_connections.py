import json

import pytest

from dbbench.connections import (
    ConnectionPolicy,
    ConnectionSnapshot,
    evaluate_connection_budget,
)


def snapshot(**overrides) -> ConnectionSnapshot:
    values = {
        "max_connections": 100,
        "reserved_connections": 0,
        "superuser_reserved_connections": 3,
        "client_connections": 20,
        "active_connections": 8,
        "idle_connections": 12,
        "idle_in_transaction_connections": 0,
        "waiting_connections": 0,
        "oldest_idle_in_transaction_seconds": 0.0,
    }
    values.update(overrides)
    return ConnectionSnapshot(**values)


def test_safe_snapshot_and_pool_budget_pass() -> None:
    report = evaluate_connection_budget(
        snapshot(),
        ConnectionPolicy(pool_size_per_replica=20, replicas=3),
    )

    assert report.passed is True
    assert report.reasons == ()
    assert report.usable_connections == 97
    assert report.requested_pool_connections == 60
    assert report.pool_capacity_limit == 92
    assert report.free_usable_connections == 77


def test_pool_budget_accounts_for_all_replicas_and_reserved_capacity() -> None:
    report = evaluate_connection_budget(
        snapshot(reserved_connections=2),
        ConnectionPolicy(
            pool_size_per_replica=24,
            replicas=4,
            min_free_connections=5,
        ),
    )

    assert report.passed is False
    assert report.reasons == ("pool_capacity_exceeded",)
    assert report.requested_pool_connections == 96
    assert report.usable_connections == 95
    assert report.pool_capacity_limit == 90


def test_all_runtime_violations_are_reported_in_stable_order() -> None:
    report = evaluate_connection_budget(
        snapshot(
            client_connections=90,
            active_connections=50,
            idle_connections=38,
            idle_in_transaction_connections=2,
            waiting_connections=4,
            oldest_idle_in_transaction_seconds=121.5,
        ),
        ConnectionPolicy(
            pool_size_per_replica=60,
            replicas=2,
            max_utilization=0.8,
            max_idle_in_transaction=1,
            max_idle_in_transaction_seconds=60,
            max_waiting_connections=1,
        ),
    )

    assert report.reasons == (
        "pool_capacity_exceeded",
        "observed_utilization_exceeded",
        "idle_in_transaction_count_exceeded",
        "idle_in_transaction_age_exceeded",
        "waiting_connections_exceeded",
    )


def test_idle_transaction_age_is_only_checked_when_one_exists() -> None:
    report = evaluate_connection_budget(
        snapshot(
            client_connections=21,
            idle_connections=12,
            idle_in_transaction_connections=1,
            oldest_idle_in_transaction_seconds=10.0,
        ),
        ConnectionPolicy(
            max_idle_in_transaction=1,
            max_idle_in_transaction_seconds=10.0,
        ),
    )

    assert report.passed is True


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"max_connections": 0}, "max_connections must be greater than zero"),
        (
            {"reserved_connections": 50, "superuser_reserved_connections": 50},
            "reserved connection slots must be below max_connections",
        ),
        (
            {"client_connections": 1, "active_connections": 2, "idle_connections": 0},
            "classified connection counts exceed client_connections",
        ),
        (
            {"active_connections": 1, "waiting_connections": 2},
            "waiting_connections cannot exceed active_connections",
        ),
        (
            {"oldest_idle_in_transaction_seconds": float("nan")},
            "oldest_idle_in_transaction_seconds must be a finite non-negative number",
        ),
        (
            {"oldest_idle_in_transaction_seconds": 1.0},
            "oldest_idle_in_transaction_seconds must be zero",
        ),
    ],
)
def test_malformed_snapshots_fail_closed(changes, message) -> None:
    with pytest.raises(ValueError, match=message):
        snapshot(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"pool_size_per_replica": 0},
        {"replicas": 0},
        {"min_free_connections": -1},
        {"max_utilization": 0},
        {"max_utilization": 1.01},
        {"max_utilization": float("inf")},
        {"max_idle_in_transaction": -1},
        {"max_idle_in_transaction_seconds": float("nan")},
        {"max_waiting_connections": -1},
    ],
)
def test_invalid_policies_fail_closed(changes) -> None:
    with pytest.raises(ValueError):
        ConnectionPolicy(**changes)


def test_report_is_json_ready_and_rounds_observed_ratio() -> None:
    report = evaluate_connection_budget(
        snapshot(client_connections=1, active_connections=1, idle_connections=0),
        ConnectionPolicy(),
    )
    payload = report.to_dict()

    assert payload["observed_utilization"] == 0.010309
    assert payload["reasons"] == []
    assert json.loads(json.dumps(payload))["snapshot"]["max_connections"] == 100
