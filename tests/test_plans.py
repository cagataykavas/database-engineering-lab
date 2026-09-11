from dbbench.plans import PlanBudget, parse_explain_json, plan_findings


def test_explain_parser_extracts_scan_counts_and_timing() -> None:
    payload = [
        {
            "Plan": {
                "Node Type": "Nested Loop",
                "Startup Cost": 0.0,
                "Total Cost": 100.0,
                "Plan Rows": 100,
                "Actual Rows": 125,
                "Actual Total Time": 4.2,
                "Actual Loops": 1,
                "Shared Hit Blocks": 90,
                "Shared Read Blocks": 10,
                "Plans": [
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "customers",
                        "Index Name": "customers_pkey",
                        "Startup Cost": 0.0,
                        "Total Cost": 8.0,
                        "Plan Rows": 1,
                        "Actual Rows": 1,
                        "Actual Total Time": 0.1,
                        "Actual Loops": 1,
                    },
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "transactions",
                        "Startup Cost": 0.0,
                        "Total Cost": 90.0,
                        "Plan Rows": 1000,
                        "Actual Rows": 25000,
                        "Actual Total Time": 3.8,
                        "Actual Loops": 1,
                        "Filter": "amount > 100",
                        "Rows Removed by Filter": 20000,
                    },
                ],
            },
            "Planning Time": 0.5,
            "Execution Time": 4.7,
        }
    ]
    summary = parse_explain_json(payload)
    assert summary.index_scan_count == 1
    assert summary.sequential_scan_count == 1
    assert summary.nested_loop_count == 1
    assert summary.execution_time_ms == 4.7
    assert summary.cache_hit_ratio == 0.9
    findings = plan_findings(summary)
    assert any("large sequential scan" in finding for finding in findings)
    assert any("cardinality underestimate" in finding for finding in findings)
    assert any("filter discarded" in finding for finding in findings)


def test_plan_budget_reports_each_regression_dimension() -> None:
    summary = parse_explain_json(
        {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "transactions",
                "Plan Rows": 10,
                "Actual Rows": 10,
                "Shared Read Blocks": 50,
            },
            "Execution Time": 25.0,
        }
    )
    violations = PlanBudget(
        max_execution_ms=10,
        max_sequential_scans=0,
        max_shared_read_blocks=20,
    ).violations(summary)
    assert len(violations) == 3
    assert any("execution" in violation for violation in violations)
    assert any("sequential scans" in violation for violation in violations)
    assert any("shared reads" in violation for violation in violations)


def test_missing_plan_is_rejected() -> None:
    try:
        parse_explain_json([{"Execution Time": 1.0}])
    except ValueError as exc:
        assert "no Plan" in str(exc)
    else:
        raise AssertionError("expected invalid EXPLAIN payload to fail")
