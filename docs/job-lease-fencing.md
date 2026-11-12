# Fenced PostgreSQL job leases

`FOR UPDATE SKIP LOCKED` prevents two transactions from claiming the same job
at once, but ownership still needs a fence after the transaction commits. A
worker can pause beyond its lease, another worker can reclaim the row, and the
stale worker can later resume. Owner names alone are insufficient when worker
identities are reused.

`PostgresJobQueue` therefore treats the incremented `attempts` value returned
by `claim` as a fencing token. Every renewal, success, and failure must match:

- the job ID;
- the lease owner;
- `status = 'running'`;
- the exact claim attempt; and
- an unexpired `lease_until` value.

The check and state transition occur in one PostgreSQL `UPDATE`, so there is no
verification-to-write race.

```python
claimed = queue.claim("risk-scoring", "worker-a", lease_seconds=60)
assert claimed is not None

renewed = queue.renew(
    claimed.job_id,
    "worker-a",
    expected_attempt=claimed.attempts,
    lease_seconds=120,
)
queue.succeed(
    renewed.job_id,
    "worker-a",
    expected_attempt=renewed.attempts,
)
```

Lease durations are bounded to one hour, renewal never shortens a live lease,
and owner identifiers use a constrained character set. Failure messages remain truncated before persistence.
An expired or superseded claim raises `LeaseConflict`; callers must stop work
and must not retry completion under a newer token they did not claim.

## Operational boundaries

- PostgreSQL's clock is authoritative for lease expiry. Workers should not make
  commit decisions from their local clocks.
- The attempt counter fences this database row; it does not fence side effects
  already sent to an external API, object store, or message broker.
- Long-running handlers must renew before expiry and leave enough time for
  transaction and network latency.
- Queue payload size, worker authentication, and dead-letter remediation remain
  separate controls.

The next increment should propagate the fencing token into idempotency keys or
conditional writes at downstream systems, then retain an append-only claim and
completion audit for incident reconstruction.
