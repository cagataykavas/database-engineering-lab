from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from dbbench.connections import (
    ConnectionPolicy,
    collect_connection_snapshot,
    evaluate_connection_budget,
)
from dbbench.migrations import MigrationRunner
from dbbench.scenarios import print_report, verify_platform
from dbbench.workload import (
    benchmark_query,
    connect,
    seed_transactions,
    standard_workloads,
)


def _serialize_measurement(measurement) -> dict:
    payload = asdict(measurement)
    plan = measurement.execution_plan
    if plan is not None:
        payload["execution_plan"] = {
            "planning_time_ms": plan.planning_time_ms,
            "execution_time_ms": plan.execution_time_ms,
            "sequential_scan_count": plan.sequential_scan_count,
            "index_scan_count": plan.index_scan_count,
            "nested_loop_count": plan.nested_loop_count,
            "nodes": [asdict(node) for node in plan.nodes],
        }
    return payload


def command_init(args: argparse.Namespace) -> int:
    with connect(args.dsn) as connection:
        migrations = MigrationRunner(connection).apply()
        if args.seed_rows:
            seed_transactions(connection, rows=args.seed_rows, seed=args.seed)
    print(
        json.dumps(
            {
                "status": "initialized",
                "seed_rows": args.seed_rows,
                "migrations_applied": sum(result.applied for result in migrations),
            },
            indent=2,
        )
    )
    return 0


def command_benchmark(args: argparse.Namespace) -> int:
    results = []
    with connect(args.dsn) as connection:
        for name, sql, params in standard_workloads():
            measurement = benchmark_query(
                connection,
                name=name,
                sql=sql,
                params=params,
                iterations=args.iterations,
                include_plan=True,
            )
            results.append(_serialize_measurement(measurement))

    payload = {"iterations": args.iterations, "workloads": results}
    text = json.dumps(payload, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


def command_migrate(args: argparse.Namespace) -> int:
    with connect(args.dsn) as connection:
        results = MigrationRunner(connection).apply()
    print(json.dumps({"migrations": [asdict(result) for result in results]}, indent=2))
    return 0


def command_verify(args: argparse.Namespace) -> int:
    with connect(args.dsn) as connection:
        report = verify_platform(connection, seed_rows=args.seed_rows)
    print_report(report)
    if not all(report["invariants"].values()):
        return 1
    return 0


def command_connections(args: argparse.Namespace) -> int:
    policy = ConnectionPolicy(
        pool_size_per_replica=args.pool_size_per_replica,
        replicas=args.replicas,
        min_free_connections=args.min_free_connections,
        max_utilization=args.max_utilization,
        max_idle_in_transaction=args.max_idle_in_transaction,
        max_idle_in_transaction_seconds=args.max_idle_in_transaction_seconds,
        max_waiting_connections=args.max_waiting_connections,
    )
    with connect(args.dsn) as connection:
        snapshot = collect_connection_snapshot(connection)
    report = evaluate_connection_budget(snapshot, policy)
    text = json.dumps(report.to_dict(), indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if report.passed else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="db-bench",
        description="Seed and benchmark PostgreSQL workloads with EXPLAIN ANALYZE output.",
    )
    parser.add_argument(
        "--dsn",
        default="postgresql://postgres:postgres@localhost:5432/db_lab",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    initialize = subparsers.add_parser("init")
    initialize.add_argument("--seed-rows", type=int, default=100000)
    initialize.add_argument("--seed", type=int, default=42)
    initialize.set_defaults(handler=command_init)

    migrate = subparsers.add_parser("migrate")
    migrate.set_defaults(handler=command_migrate)

    benchmark = subparsers.add_parser("benchmark")
    benchmark.add_argument("--iterations", type=int, default=20)
    benchmark.add_argument("--output")
    benchmark.set_defaults(handler=command_benchmark)

    connections = subparsers.add_parser(
        "connections",
        help="Evaluate live PostgreSQL usage and configured pool capacity.",
    )
    connections.add_argument("--pool-size-per-replica", type=int, default=10)
    connections.add_argument("--replicas", type=int, default=1)
    connections.add_argument("--min-free-connections", type=int, default=5)
    connections.add_argument("--max-utilization", type=float, default=0.8)
    connections.add_argument("--max-idle-in-transaction", type=int, default=0)
    connections.add_argument(
        "--max-idle-in-transaction-seconds",
        type=float,
        default=60.0,
    )
    connections.add_argument("--max-waiting-connections", type=int, default=0)
    connections.add_argument("--output")
    connections.set_defaults(handler=command_connections)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--seed-rows", type=int, default=3000)
    verify.set_defaults(handler=command_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
