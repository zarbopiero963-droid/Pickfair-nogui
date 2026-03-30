from pathlib import Path

import pytest


ROOT = Path(".")


DEPRECATED_IMPORT_RULES = {
    "event_bus_root": {
        "forbidden_snippets": [
            "from event_bus import EventBus",
            "import event_bus",
        ],
        "allowed_files": {
            "tests/guardrails/test_architecture_guardrails.py",
        },
    },
    "market_tracker_root": {
        "forbidden_snippets": [
            "from market_tracker import",
            "import market_tracker",
        ],
        "allowed_files": {
            "tests/guardrails/test_architecture_guardrails.py",
        },
    },
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

    rule = DEPRECATED_IMPORT_RULES["event_bus_root"]

    for path in _iter_py_files():
        rel = path.as_posix()

        if rel in rule["allowed_files"]:
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")

        for forbidden in rule["forbidden_snippets"]:
            if forbidden in text:
                offenders.append((rel, forbidden))

    assert not offenders, f"Import deprecati event_bus root trovati: {offenders}"


@pytest.mark.guardrail
def test_no_deprecated_root_market_tracker_imports_outside_allowed_files():
    offenders = []

    rule = DEPRECATED_IMPORT_RULES["market_tracker_root"]

    for path in _iter_py_files():
        rel = path.as_posix()

        if rel in rule["allowed_files"]:
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")

        for forbidden in rule["forbidden_snippets"]:
            if forbidden in text:
                offenders.append((rel, forbidden))

    assert not offenders, f"Import deprecati market_tracker root trovati: {offenders}"


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
def test_core_canonical_modules_exist():
    required = [
        "core/event_bus.py",
        "core/trading_engine.py",
        "core/runtime_controller.py",
        "core/risk_middleware.py",
    ]

    missing = [rel for rel in required if not (ROOT / rel).exists()]
    assert not missing, f"Moduli canonici core mancanti: {missing}"


@pytest.mark.guardrail
def test_root_core_duplicate_pairs_do_not_require_legacy_roots():
    """
    Guardrail corretto per il repo attuale:
    - i moduli canonici core devono esistere
    - i legacy root possono anche NON esistere
    - se esistono entrambi, non è automaticamente errore
    """
    pairs = [
        ("core/event_bus.py", "event_bus.py"),
        ("core/market_tracker.py", "market_tracker.py"),
        ("core/tick_dispatcher.py", "tick_dispatcher.py"),
        ("core/pnl_engine.py", "pnl_engine.py"),
    ]

    missing_core = []
    observed = []

    for core_rel, root_rel in pairs:
        core_exists = (ROOT / core_rel).exists()
        root_exists = (ROOT / root_rel).exists()

        if not core_exists:
            missing_core.append(core_rel)

        observed.append(
            {
                "core": core_rel,
                "core_exists": core_exists,
                "root": root_rel,
                "root_exists": root_exists,
            }
        )

    assert not missing_core, f"Moduli core canonici mancanti: {missing_core}"

    # sanity check: il test osserva davvero almeno una coppia con root mancante,
    # così evitiamo di tornare implicitamente alla vecchia assunzione sbagliata.
    assert any(not item["root_exists"] for item in observed)