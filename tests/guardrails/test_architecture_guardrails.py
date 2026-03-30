from pathlib import Path

import pytest


ROOT = Path(".")


DEPRECATED_IMPORTS = {
    "event_bus.py": [
        "from event_bus import EventBus",
        "import event_bus",
    ],
    "market_tracker.py": [
        "from market_tracker import",
        "import market_tracker",
    ],
}

CANONICAL_IMPORT_RULES = {
    "core/trading_engine.py": [
        "from order_manager import OrderManager",
    ],
}


def _iter_py_files():
    for path in ROOT.rglob("*.py"):
        if ".venv" in path.parts or "__pycache__" in path.parts:
            continue
        yield path


@pytest.mark.guardrail
def test_no_deprecated_root_event_bus_imports_outside_allowed_files():
    offenders = []

    for path in _iter_py_files():
        rel = path.as_posix()

        if rel in {"event_bus.py", "tests/guardrails/test_architecture_guardrails.py"}:
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")

        for forbidden in DEPRECATED_IMPORTS["event_bus.py"]:
            if forbidden in text:
                offenders.append((rel, forbidden))

    assert not offenders, f"Import deprecati trovati: {offenders}"


@pytest.mark.guardrail
def test_canonical_import_rules_hold():
    offenders = []

    for rel, required_snippets in CANONICAL_IMPORT_RULES.items():
        path = ROOT / rel
        assert path.exists(), f"Manca file canonico {rel}"
        text = path.read_text(encoding="utf-8", errors="ignore")
        for snippet in required_snippets:
            if snippet not in text:
                offenders.append((rel, snippet))

    assert not offenders, f"Regole import canonici violate: {offenders}"


@pytest.mark.guardrail
def test_no_root_core_duplicate_runtime_entrypoints_used_by_import():
    forbidden_pairs = [
        ("core/event_bus.py", "event_bus.py"),
        ("core/market_tracker.py", "market_tracker.py"),
        ("core/tick_dispatcher.py", "tick_dispatcher.py"),
        ("core/pnl_engine.py", "pnl_engine.py"),
    ]

    missing = []
    for a, b in forbidden_pairs:
        if not (ROOT / a).exists():
            missing.append(a)
        if not (ROOT / b).exists():
            missing.append(b)

    assert not missing, f"Attesi doppioni root/core mancanti: {missing}"