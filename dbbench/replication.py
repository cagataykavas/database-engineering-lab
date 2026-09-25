"""Fail-closed PostgreSQL replication readiness audit.

The audit consumes bounded, point-in-time samples produced by a trusted collector.  It
does not connect to PostgreSQL, mutate replication state, or attempt a failover.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

_LSN_PATTERN = re.compile(r"^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$")
_REPLICA_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_ALLOWED_STATES = frozenset({"startup", "catchup", "streaming", "backup", "stopping"})
_ALLOWED_SYNC_STATES = frozenset({"async", "potential", "sync", "quorum"})
_MAX_INPUT_BYTES = 8 * 1024 * 1024


class ReplicationEvidenceError(ValueError):
    """Evidence or policy could not be safely evaluated."""


@dataclass(frozen=True)
class ReplicationSample:
    observed_at: datetime
    replica_id: str
    state: str
    sync_state: str
    primary_lsn: str
    sent_lsn: str
    write_lsn: str
    flush_lsn: str
    replay_lsn: str
    replay_delay_seconds: float
    timeline_id: int


@dataclass(frozen=True)
class ReplicationPolicy:
    min_replicas: int = 1
    min_samples_per_replica: int = 6
    max_samples: int = 10_000
    max_replicas: int = 100
    max_replica_id_chars: int = 128
    max_sample_gap_seconds: float = 120.0
    max_evidence_age_seconds: float = 120.0
    max_future_skew_seconds: float = 5.0
    max_replay_lag_bytes: int = 16 * 1024 * 1024
    max_replay_delay_seconds: float = 30.0
    max_breach_fraction: float = 0.05
    require_synchronous: bool = False
    max_reported_violations: int = 100


@dataclass(frozen=True)
class Finding:
    code: str
    replica_id: str | None
    observed_at: str | None


@dataclass(frozen=True)
class ReplicaSummary:
    replica_id: str
    sample_count: int
    latest_state: str
    latest_sync_state: str
    timeline_id: int
    latest_age_seconds: float
    max_sample_gap_seconds: float
    max_replay_lag_bytes: int
    p95_replay_lag_bytes: int
    max_replay_delay_seconds: float
    p95_replay_delay_seconds: float
    breach_count: int
    breach_fraction: float


@dataclass(frozen=True)
class Violation:
    replica_id: str
    observed_at: str
    codes: tuple[str, ...]
    replay_lag_bytes: int
    replay_delay_seconds: float


@dataclass(frozen=True)
class ReplicationReport:
    ready: bool
    evaluated_at: str
    policy: dict[str, Any]
    input_sha256: str
    evidence_sha256: str
    replica_summaries: tuple[ReplicaSummary, ...]
    findings: tuple[Finding, ...]
    violations: tuple[Violation, ...]
    total_violation_count: int
    violations_truncated: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_lsn(value: object) -> int:
    """Convert a PostgreSQL WAL LSN into its monotonic byte position."""
    if not isinstance(value, str) or not _LSN_PATTERN.fullmatch(value):
        raise ReplicationEvidenceError("LSN must match HEX/HEX with at most 8 digits per part")
    high, low = value.split("/", maxsplit=1)
    return (int(high, 16) << 32) + int(low, 16)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ReplicationEvidenceError(f"{field} must be an ISO-8601 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ReplicationEvidenceError(f"{field} must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplicationEvidenceError(f"{field} must include a timezone offset")
    return parsed.astimezone(UTC)


def _utc_string(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _strict_int(value: object, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReplicationEvidenceError(f"{field} must be an integer")
    if not minimum <= value <= maximum:
        raise ReplicationEvidenceError(f"{field} must be between {minimum} and {maximum}")
    return value


def _finite_float(value: object, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReplicationEvidenceError(f"{field} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or not minimum <= parsed <= maximum:
        raise ReplicationEvidenceError(
            f"{field} must be finite and between {minimum} and {maximum}"
        )
    return parsed


def _parse_policy(value: object | None) -> ReplicationPolicy:
    if value is None:
        policy = ReplicationPolicy()
    elif not isinstance(value, Mapping):
        raise ReplicationEvidenceError("policy must be an object")
    else:
        if any(not isinstance(key, str) for key in value):
            raise ReplicationEvidenceError("policy field names must be strings")
        allowed = set(ReplicationPolicy.__dataclass_fields__)
        unknown = set(value) - allowed
        if unknown:
            raise ReplicationEvidenceError(
                f"policy has unknown fields: {', '.join(sorted(unknown))}"
            )
        try:
            policy = ReplicationPolicy(**value)
        except TypeError as exc:
            raise ReplicationEvidenceError("policy fields have invalid values") from exc

    _strict_int(policy.min_replicas, "policy.min_replicas", 1, 100)
    _strict_int(policy.min_samples_per_replica, "policy.min_samples_per_replica", 2, 10_000)
    _strict_int(policy.max_samples, "policy.max_samples", 2, 100_000)
    _strict_int(policy.max_replicas, "policy.max_replicas", 1, 1_000)
    _strict_int(policy.max_replica_id_chars, "policy.max_replica_id_chars", 1, 512)
    _finite_float(policy.max_sample_gap_seconds, "policy.max_sample_gap_seconds", 0.001, 86_400)
    _finite_float(policy.max_evidence_age_seconds, "policy.max_evidence_age_seconds", 0.0, 86_400)
    _finite_float(policy.max_future_skew_seconds, "policy.max_future_skew_seconds", 0.0, 300)
    _strict_int(policy.max_replay_lag_bytes, "policy.max_replay_lag_bytes", 0, 2**63 - 1)
    _finite_float(policy.max_replay_delay_seconds, "policy.max_replay_delay_seconds", 0.0, 86_400)
    _finite_float(policy.max_breach_fraction, "policy.max_breach_fraction", 0.0, 1.0)
    if not isinstance(policy.require_synchronous, bool):
        raise ReplicationEvidenceError("policy.require_synchronous must be boolean")
    _strict_int(policy.max_reported_violations, "policy.max_reported_violations", 0, 10_000)
    if policy.min_replicas > policy.max_replicas:
        raise ReplicationEvidenceError("policy.min_replicas cannot exceed policy.max_replicas")
    if policy.min_samples_per_replica > policy.max_samples:
        raise ReplicationEvidenceError(
            "policy.min_samples_per_replica cannot exceed policy.max_samples"
        )
    return policy


def _parse_sample(value: object, index: int, policy: ReplicationPolicy) -> ReplicationSample:
    if not isinstance(value, Mapping):
        raise ReplicationEvidenceError(f"samples[{index}] must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ReplicationEvidenceError(f"samples[{index}] field names must be strings")
    expected = set(ReplicationSample.__dataclass_fields__)
    missing = expected - set(value)
    unknown = set(value) - expected
    if missing:
        raise ReplicationEvidenceError(f"samples[{index}] is missing: {', '.join(sorted(missing))}")
    if unknown:
        raise ReplicationEvidenceError(
            f"samples[{index}] has unknown fields: {', '.join(sorted(unknown))}"
        )

    replica_id = value["replica_id"]
    if (
        not isinstance(replica_id, str)
        or len(replica_id) > policy.max_replica_id_chars
        or not _REPLICA_ID_PATTERN.fullmatch(replica_id)
    ):
        raise ReplicationEvidenceError(f"samples[{index}].replica_id is invalid")
    state = value["state"]
    if not isinstance(state, str) or state not in _ALLOWED_STATES:
        raise ReplicationEvidenceError(f"samples[{index}].state is invalid")
    sync_state = value["sync_state"]
    if not isinstance(sync_state, str) or sync_state not in _ALLOWED_SYNC_STATES:
        raise ReplicationEvidenceError(f"samples[{index}].sync_state is invalid")

    lsn_fields = ("primary_lsn", "sent_lsn", "write_lsn", "flush_lsn", "replay_lsn")
    parsed_lsns = [parse_lsn(value[field]) for field in lsn_fields]
    if any(left < right for left, right in pairwise(parsed_lsns)):
        raise ReplicationEvidenceError(
            f"samples[{index}] LSNs must satisfy primary >= sent >= write >= flush >= replay"
        )

    return ReplicationSample(
        observed_at=_parse_timestamp(value["observed_at"], f"samples[{index}].observed_at"),
        replica_id=replica_id,
        state=state,
        sync_state=sync_state,
        primary_lsn=value["primary_lsn"].upper(),
        sent_lsn=value["sent_lsn"].upper(),
        write_lsn=value["write_lsn"].upper(),
        flush_lsn=value["flush_lsn"].upper(),
        replay_lsn=value["replay_lsn"].upper(),
        replay_delay_seconds=_finite_float(
            value["replay_delay_seconds"],
            f"samples[{index}].replay_delay_seconds",
            0.0,
            86_400.0,
        ),
        timeline_id=_strict_int(value["timeline_id"], f"samples[{index}].timeline_id", 1, 2**31),
    )


def _nearest_rank(values: Sequence[int] | Sequence[float], percentile: float) -> int | float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _normalized_sample(sample: ReplicationSample) -> dict[str, Any]:
    result = asdict(sample)
    result["observed_at"] = _utc_string(sample.observed_at)
    return result


def audit_replication_readiness(
    samples: Iterable[object],
    *,
    evaluated_at: datetime | str,
    policy: ReplicationPolicy | Mapping[str, object] | None = None,
) -> ReplicationReport:
    """Evaluate bounded replication evidence and return deterministic release evidence."""
    parsed_policy = policy if isinstance(policy, ReplicationPolicy) else _parse_policy(policy)
    if isinstance(policy, ReplicationPolicy):
        parsed_policy = _parse_policy(asdict(policy))
    evaluated = (
        _parse_timestamp(evaluated_at, "evaluated_at")
        if isinstance(evaluated_at, str)
        else evaluated_at
    )
    if (
        not isinstance(evaluated, datetime)
        or evaluated.tzinfo is None
        or evaluated.utcoffset() is None
    ):
        raise ReplicationEvidenceError("evaluated_at must be timezone-aware")
    evaluated = evaluated.astimezone(UTC)

    if isinstance(samples, (str, bytes, Mapping)):
        raise ReplicationEvidenceError("samples must be an array")
    raw_samples: list[object] = []
    try:
        for sample in samples:
            raw_samples.append(sample)
            if len(raw_samples) > parsed_policy.max_samples:
                raise ReplicationEvidenceError("sample count exceeds policy.max_samples")
    except TypeError as exc:
        raise ReplicationEvidenceError("samples must be iterable") from exc
    if not raw_samples:
        raise ReplicationEvidenceError("samples must not be empty")

    parsed = [
        _parse_sample(sample, index, parsed_policy) for index, sample in enumerate(raw_samples)
    ]
    groups: dict[str, list[ReplicationSample]] = {}
    for sample in parsed:
        groups.setdefault(sample.replica_id, []).append(sample)
    if len(groups) > parsed_policy.max_replicas:
        raise ReplicationEvidenceError("replica count exceeds policy.max_replicas")

    seen_keys: set[tuple[str, datetime]] = set()
    for sample in parsed:
        key = (sample.replica_id, sample.observed_at)
        if key in seen_keys:
            raise ReplicationEvidenceError("duplicate replica_id and observed_at sample")
        seen_keys.add(key)

    future_limit = evaluated.timestamp() + parsed_policy.max_future_skew_seconds
    if any(sample.observed_at.timestamp() > future_limit for sample in parsed):
        raise ReplicationEvidenceError("sample timestamp exceeds allowed future skew")

    findings: list[Finding] = []
    violations: list[Violation] = []
    summaries: list[ReplicaSummary] = []
    if len(groups) < parsed_policy.min_replicas:
        findings.append(Finding("INSUFFICIENT_REPLICAS", None, None))

    for replica_id in sorted(groups):
        replica_samples = sorted(groups[replica_id], key=lambda item: item.observed_at)
        latest = replica_samples[-1]
        gaps = [
            (current.observed_at - previous.observed_at).total_seconds()
            for previous, current in pairwise(replica_samples)
        ]
        max_gap = max(gaps, default=0.0)
        latest_age = max(0.0, (evaluated - latest.observed_at).total_seconds())
        timeline_ids = {sample.timeline_id for sample in replica_samples}
        lags = [
            parse_lsn(sample.primary_lsn) - parse_lsn(sample.replay_lsn)
            for sample in replica_samples
        ]
        delays = [sample.replay_delay_seconds for sample in replica_samples]
        replica_breach_count = 0

        if len(replica_samples) < parsed_policy.min_samples_per_replica:
            findings.append(Finding("INSUFFICIENT_SAMPLES", replica_id, None))
        if max_gap > parsed_policy.max_sample_gap_seconds:
            findings.append(Finding("SAMPLE_GAP_EXCEEDED", replica_id, None))
        if latest_age > parsed_policy.max_evidence_age_seconds:
            findings.append(Finding("STALE_EVIDENCE", replica_id, _utc_string(latest.observed_at)))
        if len(timeline_ids) != 1:
            findings.append(Finding("TIMELINE_CHANGED", replica_id, None))
        if latest.state != "streaming":
            findings.append(
                Finding("LATEST_REPLICA_NOT_STREAMING", replica_id, _utc_string(latest.observed_at))
            )
        if parsed_policy.require_synchronous and latest.sync_state not in {"sync", "quorum"}:
            findings.append(
                Finding("LATEST_SYNC_STATE_REJECTED", replica_id, _utc_string(latest.observed_at))
            )

        for sample, lag in zip(replica_samples, lags, strict=True):
            codes: list[str] = []
            if sample.state != "streaming":
                codes.append("NOT_STREAMING")
            if parsed_policy.require_synchronous and sample.sync_state not in {"sync", "quorum"}:
                codes.append("SYNCHRONOUS_STATE_REQUIRED")
            if lag > parsed_policy.max_replay_lag_bytes:
                codes.append("REPLAY_LAG_BYTES_EXCEEDED")
            if sample.replay_delay_seconds > parsed_policy.max_replay_delay_seconds:
                codes.append("REPLAY_DELAY_EXCEEDED")
            if codes:
                replica_breach_count += 1
                violations.append(
                    Violation(
                        replica_id=replica_id,
                        observed_at=_utc_string(sample.observed_at),
                        codes=tuple(codes),
                        replay_lag_bytes=lag,
                        replay_delay_seconds=sample.replay_delay_seconds,
                    )
                )

        breach_fraction = replica_breach_count / len(replica_samples)
        if breach_fraction > parsed_policy.max_breach_fraction:
            findings.append(Finding("SAMPLE_BREACH_FRACTION_EXCEEDED", replica_id, None))
        summaries.append(
            ReplicaSummary(
                replica_id=replica_id,
                sample_count=len(replica_samples),
                latest_state=latest.state,
                latest_sync_state=latest.sync_state,
                timeline_id=latest.timeline_id,
                latest_age_seconds=latest_age,
                max_sample_gap_seconds=max_gap,
                max_replay_lag_bytes=max(lags),
                p95_replay_lag_bytes=int(_nearest_rank(lags, 0.95)),
                max_replay_delay_seconds=max(delays),
                p95_replay_delay_seconds=float(_nearest_rank(delays, 0.95)),
                breach_count=replica_breach_count,
                breach_fraction=breach_fraction,
            )
        )

    normalized_samples = sorted(
        (_normalized_sample(sample) for sample in parsed),
        key=lambda item: (item["replica_id"], item["observed_at"]),
    )
    ordered_findings = tuple(
        sorted(
            findings, key=lambda item: (item.replica_id or "", item.code, item.observed_at or "")
        )
    )
    ordered_violations = sorted(
        violations, key=lambda item: (item.replica_id, item.observed_at, item.codes)
    )
    bounded_violations = tuple(ordered_violations[: parsed_policy.max_reported_violations])
    policy_dict = asdict(parsed_policy)
    report_without_digest = {
        "ready": not ordered_findings,
        "evaluated_at": _utc_string(evaluated),
        "policy": policy_dict,
        "input_sha256": _sha256(normalized_samples),
        "replica_summaries": [asdict(summary) for summary in summaries],
        "findings": [asdict(finding) for finding in ordered_findings],
        "violations": [asdict(violation) for violation in bounded_violations],
        "total_violation_count": len(ordered_violations),
        "violations_truncated": len(ordered_violations) > len(bounded_violations),
    }
    return ReplicationReport(
        ready=report_without_digest["ready"],
        evaluated_at=report_without_digest["evaluated_at"],
        policy=policy_dict,
        input_sha256=report_without_digest["input_sha256"],
        evidence_sha256=_sha256(report_without_digest),
        replica_summaries=tuple(summaries),
        findings=ordered_findings,
        violations=bounded_violations,
        total_violation_count=len(ordered_violations),
        violations_truncated=len(ordered_violations) > len(bounded_violations),
    )


def _read_json(path: Path) -> object:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ReplicationEvidenceError("input contains a duplicate JSON field")
            result[key] = value
        return result

    try:
        with path.open("rb") as handle:
            content = handle.read(_MAX_INPUT_BYTES + 1)
        if len(content) > _MAX_INPUT_BYTES:
            raise ReplicationEvidenceError("input exceeds the 8 MiB document budget")
        return json.loads(content.decode("utf-8"), object_pairs_hook=reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ReplicationEvidenceError("input must be a readable UTF-8 JSON document") from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit PostgreSQL replication readiness evidence")
    parser.add_argument("evidence", type=Path)
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="return exit code 2 when valid evidence fails the readiness policy",
    )
    args = parser.parse_args(argv)
    try:
        payload = _read_json(args.evidence)
        if not isinstance(payload, Mapping):
            raise ReplicationEvidenceError("input root must be an object")
        unknown = set(payload) - {"evaluated_at", "samples", "policy"}
        missing = {"evaluated_at", "samples"} - set(payload)
        if missing:
            raise ReplicationEvidenceError(f"input is missing: {', '.join(sorted(missing))}")
        if unknown:
            raise ReplicationEvidenceError(
                f"input has unknown fields: {', '.join(sorted(unknown))}"
            )
        report = audit_replication_readiness(
            payload["samples"],
            evaluated_at=payload["evaluated_at"],
            policy=payload.get("policy"),
        )
    except ReplicationEvidenceError as exc:
        print(json.dumps({"error": "MALFORMED_EVIDENCE", "message": str(exc)}, sort_keys=True))
        return 3

    print(json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":")))
    return 2 if args.require_ready and not report.ready else 0


if __name__ == "__main__":
    sys.exit(main())
