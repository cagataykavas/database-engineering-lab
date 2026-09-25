# PostgreSQL replication readiness audit

`dbbench.replication` turns a bounded sequence of primary-side replication samples into
deterministic release evidence. It is intended for deployment and failover runbook gates where a
single healthy-looking `pg_stat_replication` row would be too weak: the gate checks sampling
continuity, freshness, WAL lag, replay delay, timeline stability, streaming state, and (when
required) synchronous-replication state.

The auditor is deliberately side-effect free. It never connects to PostgreSQL, changes replication
configuration, promotes a standby, or claims that a failover succeeded.

## Evidence contract

The input is a UTF-8 JSON object with an explicit evaluation time, an optional policy, and samples:

```json
{
  "evaluated_at": "2026-09-25T05:00:00Z",
  "policy": {
    "min_replicas": 1,
    "min_samples_per_replica": 6,
    "max_sample_gap_seconds": 120,
    "max_evidence_age_seconds": 120,
    "max_replay_lag_bytes": 16777216,
    "max_replay_delay_seconds": 30,
    "max_breach_fraction": 0.05,
    "require_synchronous": false
  },
  "samples": [
    {
      "observed_at": "2026-09-25T04:59:10Z",
      "replica_id": "replica-a",
      "state": "streaming",
      "sync_state": "async",
      "primary_lsn": "2/600000",
      "sent_lsn": "2/5FFF80",
      "write_lsn": "2/5FFF00",
      "flush_lsn": "2/5FFE80",
      "replay_lsn": "2/5FFC00",
      "replay_delay_seconds": 2.0,
      "timeline_id": 7
    }
  ]
}
```

Every sample must contain exactly the documented fields. Timestamps must be timezone-aware. WAL
positions must use PostgreSQL's `HEX/HEX` LSN representation and satisfy
`primary >= sent >= write >= flush >= replay`. Duplicate `(replica_id, observed_at)` observations,
duplicate JSON fields, unknown enum values, non-finite numbers, excessive evidence, and unknown
policy fields are rejected as malformed instead of being silently ignored. The CLI also applies an
8 MiB document limit before parsing.

The default policy requires six samples per replica, at least one replica, a latest observation no
older than 120 seconds, and no sampling gap larger than 120 seconds. A replica's latest observation
must be `streaming`; `sync` or `quorum` is additionally required when `require_synchronous` is true.
Per-sample state, synchronous-state, WAL-lag, and replay-delay breaches contribute to the allowed
breach fraction. Timeline changes, insufficient evidence, stale evidence, and latest-state failures
are always hard findings.

Run the gate with:

```bash
python -m dbbench.replication evidence.json --require-ready
```

Exit code `0` means the evidence was evaluated and passed. Exit code `2` means valid evidence failed
the policy. Exit code `3` means the evidence or policy was malformed. Without `--require-ready`, a
policy rejection is reported as JSON but does not change the exit code, which is useful for advisory
collection jobs.

The report contains per-replica maxima and nearest-rank p95 values, stable finding codes, a bounded
set of violating observations, a SHA-256 digest of all normalized input samples, and a second digest
covering the decision evidence. The violation list can be truncated without hiding its total count.
No connection strings, SQL text, host addresses, or exception strings are emitted.

## Collection guidance

A trusted collector can obtain most fields from the primary's `pg_stat_replication` view together
with `pg_current_wal_lsn()`. The collector must bind every row to one monotonic sampling cycle and a
timezone-aware `observed_at`; it should map a durable deployment identity to `replica_id` instead of
using a transient client address.

`replay_delay_seconds` is intentionally producer-supplied. A primary-side collector may combine the
standby's last replay timestamp with its observation time, but it must define how idle replicas are
handled: `pg_last_xact_replay_timestamp()` can remain unchanged when no transactions are replayed, so
blindly interpreting it as wall-clock lag creates false alarms. The evidence producer should be
tested independently and its clock source monitored.

`timeline_id` should come from a trusted control-plane or WAL-inspection source. A changed timeline
within one evidence window fails the gate because samples before and after a promotion are not a
single stationary readiness window. Collect a fresh window after the topology stabilizes.

## Trust boundaries and limitations

- LSN distance measures WAL bytes, not network latency, storage durability, or elapsed recovery time.
- The audit does not verify replication slots, WAL retention, archive recovery, quorum semantics,
  fencing, split-brain prevention, DNS/service discovery, or application reconnect behavior.
- A passing synchronous-state check reflects the supplied `pg_stat_replication` state; it does not
  prove that transaction-level `synchronous_commit` settings meet the intended durability policy.
- SHA-256 digests make accidental evidence changes visible but are not signatures. Store reports in
  immutable storage and sign them when producer authenticity matters.
- Thresholds require workload-specific calibration. Sparse writes, large transactions, bursty WAL,
  network jitter, and cross-region replicas need different lag and sampling budgets.
- Passing this audit is not a disaster-recovery test. A scheduled promotion rehearsal should also
  measure achieved RPO/RTO, data consistency, client recovery, and restoration to the original
  topology.

The next production increment is a least-privilege collector that signs primary and standby
observations and a scheduled failover rehearsal that links its achieved RPO/RTO evidence to this
pre-flight report.
