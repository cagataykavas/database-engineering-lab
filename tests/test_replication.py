from __future__ import annotations

import json
import math
import subprocess
import sys
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dbbench.replication import (
    ReplicationEvidenceError,
    ReplicationPolicy,
    audit_replication_readiness,
    parse_lsn,
)

EVALUATED_AT = datetime(2026, 9, 25, 5, 0, tzinfo=UTC)


def sample(
    offset: int,
    *,
    replica_id: str = "replica-a",
    lag_bytes: int = 1_024,
    delay_seconds: float = 2.0,
    state: str = "streaming",
    sync_state: str = "sync",
    timeline_id: int = 7,
) -> dict[str, object]:
    primary = 0x200000000 + offset * 0x10000
    sent = primary - 128
    write = sent - 128
    flush = write - 128
    replay = primary - lag_bytes

    def lsn(value: int) -> str:
        return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"

    return {
        "observed_at": (EVALUATED_AT - timedelta(seconds=50 - offset * 2))
        .isoformat()
        .replace("+00:00", "Z"),
        "replica_id": replica_id,
        "state": state,
        "sync_state": sync_state,
        "primary_lsn": lsn(primary),
        "sent_lsn": lsn(sent),
        "write_lsn": lsn(write),
        "flush_lsn": lsn(flush),
        "replay_lsn": lsn(replay),
        "replay_delay_seconds": delay_seconds,
        "timeline_id": timeline_id,
    }


def passing_samples(*, replicas: tuple[str, ...] = ("replica-a",)) -> list[dict[str, object]]:
    return [sample(index, replica_id=replica) for replica in replicas for index in range(6)]


def codes(report: object) -> set[str]:
    return {finding.code for finding in report.findings}


def test_parse_lsn_uses_postgresql_64_bit_wal_position() -> None:
    assert parse_lsn("1/00000010") == 2**32 + 16
    assert parse_lsn("a/ff") == 10 * 2**32 + 255


@pytest.mark.parametrize("value", [None, 1, "", "1", "1/2/3", "GG/1", "123456789/1"])
def test_parse_lsn_rejects_malformed_values(value: object) -> None:
    with pytest.raises(ReplicationEvidenceError, match="LSN"):
        parse_lsn(value)


def test_ready_report_contains_deterministic_bounded_evidence() -> None:
    samples = passing_samples(replicas=("replica-b", "replica-a"))
    first = audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)
    second = audit_replication_readiness(reversed(samples), evaluated_at=EVALUATED_AT)

    assert first.ready is True
    assert first.findings == ()
    assert [summary.replica_id for summary in first.replica_summaries] == [
        "replica-a",
        "replica-b",
    ]
    assert first.input_sha256 == second.input_sha256
    assert first.evidence_sha256 == second.evidence_sha256
    assert first.to_dict() == second.to_dict()
    json.dumps(first.to_dict(), allow_nan=False)


def test_breach_fraction_uses_all_samples_and_reports_specific_causes() -> None:
    samples = passing_samples()
    samples[0] = sample(0, lag_bytes=20 * 1024 * 1024, delay_seconds=31.0)
    report = audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)

    assert report.ready is False
    assert codes(report) == {"SAMPLE_BREACH_FRACTION_EXCEEDED"}
    assert report.replica_summaries[0].breach_count == 1
    assert report.replica_summaries[0].breach_fraction == pytest.approx(1 / 6)
    assert report.violations[0].codes == (
        "REPLAY_LAG_BYTES_EXCEEDED",
        "REPLAY_DELAY_EXCEEDED",
    )


def test_breach_fraction_boundary_is_inclusive() -> None:
    samples = [sample(index) for index in range(20)]
    samples[0] = sample(0, lag_bytes=20 * 1024 * 1024)
    report = audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)
    assert report.ready is True
    assert report.replica_summaries[0].breach_fraction == 0.05


def test_latest_replica_state_is_a_hard_gate() -> None:
    samples = passing_samples()
    samples[-1] = sample(5, state="catchup")
    report = audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)
    assert "LATEST_REPLICA_NOT_STREAMING" in codes(report)


def test_synchronous_policy_checks_latest_state_and_sample_budget() -> None:
    samples = passing_samples()
    samples[-1] = sample(5, sync_state="async")
    report = audit_replication_readiness(
        samples,
        evaluated_at=EVALUATED_AT,
        policy={"require_synchronous": True},
    )
    assert codes(report) == {
        "LATEST_SYNC_STATE_REJECTED",
        "SAMPLE_BREACH_FRACTION_EXCEEDED",
    }
    assert report.violations[-1].codes == ("SYNCHRONOUS_STATE_REQUIRED",)


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (lambda values: values.__setitem__(0, sample(0, timeline_id=6)), "TIMELINE_CHANGED"),
        (
            lambda values: values.__setitem__(
                5,
                {
                    **sample(5),
                    "observed_at": (EVALUATED_AT - timedelta(seconds=300))
                    .isoformat()
                    .replace("+00:00", "Z"),
                },
            ),
            "SAMPLE_GAP_EXCEEDED",
        ),
    ],
)
def test_temporal_continuity_failures(
    mutate: Callable[[list[dict[str, object]]], None], expected: str
) -> None:
    samples = passing_samples()
    mutate(samples)
    report = audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)
    assert expected in codes(report)


def test_stale_latest_evidence_fails_closed() -> None:
    samples = passing_samples()
    for index, item in enumerate(samples):
        item["observed_at"] = (EVALUATED_AT - timedelta(seconds=300 - index * 10)).isoformat()
    report = audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)
    assert codes(report) == {"STALE_EVIDENCE"}


def test_insufficient_replicas_and_samples_are_independent_findings() -> None:
    policy = ReplicationPolicy(min_replicas=2, min_samples_per_replica=7)
    report = audit_replication_readiness(
        passing_samples(), evaluated_at=EVALUATED_AT, policy=policy
    )
    assert codes(report) == {"INSUFFICIENT_REPLICAS", "INSUFFICIENT_SAMPLES"}


def test_violation_evidence_is_bounded_without_losing_total() -> None:
    samples = [sample(index, lag_bytes=20 * 1024 * 1024) for index in range(6)]
    report = audit_replication_readiness(
        samples,
        evaluated_at=EVALUATED_AT,
        policy={"max_reported_violations": 2},
    )
    assert len(report.violations) == 2
    assert report.total_violation_count == 6
    assert report.violations_truncated is True


def test_streaming_input_is_stopped_at_the_sample_budget() -> None:
    consumed = 0

    def evidence_stream() -> object:
        nonlocal consumed
        for index in range(100):
            consumed += 1
            yield sample(index)

    with pytest.raises(ReplicationEvidenceError, match="max_samples"):
        audit_replication_readiness(
            evidence_stream(),
            evaluated_at=EVALUATED_AT,
            policy={"max_samples": 6},
        )
    assert consumed == 7


def test_reversed_lsn_chain_is_malformed_not_a_policy_rejection() -> None:
    samples = passing_samples()
    samples[0]["replay_lsn"] = samples[0]["primary_lsn"]
    samples[0]["flush_lsn"] = "1/0"
    with pytest.raises(ReplicationEvidenceError, match="primary >= sent"):
        audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("observed_at", "2026-09-25T04:59:10", "timezone"),
        ("replica_id", "replica a", "replica_id"),
        ("state", "unknown", "state"),
        ("sync_state", "maybe", "sync_state"),
        ("replay_delay_seconds", math.nan, "finite"),
        ("timeline_id", True, "integer"),
    ],
)
def test_malformed_sample_fields_fail_closed(field: str, value: object, message: str) -> None:
    samples = passing_samples()
    samples[0][field] = value
    with pytest.raises(ReplicationEvidenceError, match=message):
        audit_replication_readiness(samples, evaluated_at=EVALUATED_AT)


def test_duplicate_and_future_samples_fail_closed() -> None:
    duplicate = passing_samples()
    duplicate.append(deepcopy(duplicate[0]))
    with pytest.raises(ReplicationEvidenceError, match="duplicate"):
        audit_replication_readiness(duplicate, evaluated_at=EVALUATED_AT)

    future = passing_samples()
    future[-1]["observed_at"] = (EVALUATED_AT + timedelta(seconds=6)).isoformat()
    with pytest.raises(ReplicationEvidenceError, match="future skew"):
        audit_replication_readiness(future, evaluated_at=EVALUATED_AT)


@pytest.mark.parametrize(
    "policy",
    [
        {"unknown": 1},
        {"max_samples": True},
        {"max_breach_fraction": math.inf},
        {"require_synchronous": 1},
        {"min_replicas": 2, "max_replicas": 1},
        {"min_samples_per_replica": 7, "max_samples": 6},
    ],
)
def test_invalid_policy_fails_closed(policy: dict[str, object]) -> None:
    with pytest.raises(ReplicationEvidenceError):
        audit_replication_readiness(passing_samples(), evaluated_at=EVALUATED_AT, policy=policy)


def test_cli_distinguishes_ready_rejected_and_malformed_inputs(tmp_path: Path) -> None:
    base = {
        "evaluated_at": EVALUATED_AT.isoformat(),
        "samples": passing_samples(),
        "policy": {},
    }

    def run(payload: object) -> subprocess.CompletedProcess[str]:
        path = tmp_path / "evidence.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.run(
            [sys.executable, "-m", "dbbench.replication", str(path), "--require-ready"],
            check=False,
            capture_output=True,
            text=True,
        )

    accepted = run(base)
    assert accepted.returncode == 0
    assert json.loads(accepted.stdout)["ready"] is True

    rejected_payload = deepcopy(base)
    rejected_payload["samples"][0]["replay_delay_seconds"] = 31.0
    rejected = run(rejected_payload)
    assert rejected.returncode == 2
    assert json.loads(rejected.stdout)["ready"] is False

    malformed_payload = deepcopy(base)
    malformed_payload["samples"][0]["primary_lsn"] = "not-an-lsn"
    malformed = run(malformed_payload)
    assert malformed.returncode == 3
    assert json.loads(malformed.stdout)["error"] == "MALFORMED_EVIDENCE"


def test_cli_rejects_duplicate_json_fields(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text(
        '{"evaluated_at":"2026-09-25T05:00:00Z","samples":[],"samples":[]}',
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "-m", "dbbench.replication", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 3
    assert json.loads(result.stdout)["error"] == "MALFORMED_EVIDENCE"
