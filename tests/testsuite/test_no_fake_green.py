from __future__ import annotations

import ast
from pathlib import Path

from tests.core.test_trading_engine import FakeDB, STATUS_INFLIGHT


TEST_FILES = (
    "tests/core/test_trading_engine.py",
    "tests/guardrails/test_runtime_entrypoints.py",
    "tests/smoke/test_headless_boot_smoke.py",
)


def _has_real_assertion(test_fn: ast.FunctionDef) -> bool:
    for node in ast.walk(test_fn):
        if isinstance(node, ast.Assert):
            return True
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id.startswith("_assert_"):
                return True
        if isinstance(node, ast.With):
            for item in node.items:
                ctx_expr = item.context_expr
                if isinstance(ctx_expr, ast.Call) and isinstance(ctx_expr.func, ast.Attribute):
                    if ctx_expr.func.attr == "raises":
                        return True
    return False


def test_no_fake_green():
    for relpath in TEST_FILES:
        source = Path(relpath).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=relpath)
        test_functions = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
        ]
        assert test_functions, f"{relpath} must define at least one test_* function"
        for test_fn in test_functions:
            assert _has_real_assertion(
                test_fn
            ), f"{relpath}:{test_fn.name} passes without a real assertion"


def test_fake_db_duplicate_contract_requires_real_row():
    db = FakeDB()

    assert db.find_duplicate_order(customer_ref="CUST-MISSING") is None
    assert db.find_duplicate_order(correlation_id="CID-MISSING") is None

    duplicate_order_id = db.insert_order(
        {
            "customer_ref": "CUST-1",
            "correlation_id": "CID-1",
            "status": STATUS_INFLIGHT,
            "payload": {},
        }
    )

    assert db.find_duplicate_order(customer_ref="CUST-1") == duplicate_order_id
    assert db.find_duplicate_order(correlation_id="CID-1") == duplicate_order_id


def test_fake_db_order_exists_inflight_requires_real_row():
    db = FakeDB()
    db.force_order_exists_inflight = True

    assert db.order_exists_inflight(customer_ref="CUST-UNKNOWN") is False
    assert db.order_exists_inflight(correlation_id="CID-UNKNOWN") is False
