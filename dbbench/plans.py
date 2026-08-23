from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class PlanNode:
    node_type: str
    relation: str | None
    startup_cost: float
    total_cost: float
    plan_rows: int
    actual_rows: int | None
    actual_total_time: float | None
    loops: int | None
    filter: str | None
    index_name: str | None
    depth: int


@dataclass(frozen=True)
class PlanSummary:
    planning_time_ms: float | None
    execution_time_ms: float | None
    nodes: tuple[PlanNode, ...]

    @property
    def sequential_scan_count(self) -> int:
        return sum(node.node_type == "Seq Scan" for node in self.nodes)

    @property
    def index_scan_count(self) -> int:
        return sum("Index" in node.node_type for node in self.nodes)

    @property
    def nested_loop_count(self) -> int:
        return sum(node.node_type == "Nested Loop" for node in self.nodes)

    @property
    def total_actual_rows(self) -> int:
        return sum(node.actual_rows or 0 for node in self.nodes)


def _walk(node: dict[str, Any], depth: int = 0) -> Iterable[PlanNode]:
    yield PlanNode(
        node_type=str(node.get("Node Type", "Unknown")),
        relation=node.get("Relation Name"),
        startup_cost=float(node.get("Startup Cost", 0.0)),
        total_cost=float(node.get("Total Cost", 0.0)),
        plan_rows=int(node.get("Plan Rows", 0)),
        actual_rows=(int(node["Actual Rows"]) if "Actual Rows" in node else None),
        actual_total_time=(
            float(node["Actual Total Time"]) if "Actual Total Time" in node else None
        ),
        loops=(int(node["Actual Loops"]) if "Actual Loops" in node else None),
        filter=node.get("Filter"),
        index_name=node.get("Index Name"),
        depth=depth,
    )
    for child in node.get("Plans", []):
        yield from _walk(child, depth + 1)


def parse_explain_json(payload: list[dict[str, Any]] | dict[str, Any]) -> PlanSummary:
    """Parse PostgreSQL ``EXPLAIN (ANALYZE, FORMAT JSON)`` output."""
    if isinstance(payload, list):
        if not payload:
            raise ValueError("EXPLAIN payload is empty")
        root = payload[0]
    else:
        root = payload
    if "Plan" not in root:
        raise ValueError("EXPLAIN payload has no Plan node")
    return PlanSummary(
        planning_time_ms=(
            float(root["Planning Time"]) if "Planning Time" in root else None
        ),
        execution_time_ms=(
            float(root["Execution Time"]) if "Execution Time" in root else None
        ),
        nodes=tuple(_walk(root["Plan"])),
    )


def plan_findings(summary: PlanSummary) -> list[str]:
    findings: list[str] = []
    for node in summary.nodes:
        if node.node_type == "Seq Scan" and node.actual_rows is not None and node.actual_rows > 10000:
            relation = node.relation or "unknown_relation"
            findings.append(f"large sequential scan on {relation}: {node.actual_rows} rows")
        if (
            node.actual_rows is not None
            and node.plan_rows > 0
            and node.actual_rows / node.plan_rows >= 10
        ):
            findings.append(
                f"cardinality underestimate at {node.node_type}: "
                f"planned {node.plan_rows}, actual {node.actual_rows}"
            )
        if (
            node.actual_rows is not None
            and node.actual_rows > 0
            and node.plan_rows / node.actual_rows >= 10
        ):
            findings.append(
                f"cardinality overestimate at {node.node_type}: "
                f"planned {node.plan_rows}, actual {node.actual_rows}"
            )
    if summary.execution_time_ms is not None and summary.execution_time_ms > 1000:
        findings.append(f"slow execution: {summary.execution_time_ms:.1f} ms")
    return findings
