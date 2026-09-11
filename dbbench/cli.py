from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

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

    verify = subparsers.add_parser("verify")
    verify.add_argument("--seed-rows", type=int, default=3000)
    verify.set_defaults(handler=command_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
