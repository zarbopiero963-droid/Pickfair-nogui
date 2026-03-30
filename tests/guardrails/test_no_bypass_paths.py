from pathlib import Path

import pytest


ROOT = Path(".")


BYPASS_PATTERNS = {
    "core/trading_engine.py": [
        "place_order(",
    ],
    "core/risk_middleware.py": [
        "CMD_",
        "REQ_",
    ],
}


@pytest.mark.guardrail
def test_trading_engine_file_exists():
    assert (ROOT / "core/trading_engine.py").exists()


@pytest.mark.guardrail
def test_risk_middleware_file_exists():
    assert (ROOT / "core/risk_middleware.py").exists()


@pytest.mark.guardrail
def test_no_direct_order_submission_from_telegram_layers():
    offenders = []
    targets = [
        ROOT / "telegram_module.py",
        ROOT / "telegram_listener.py",
        ROOT / "controllers/telegram_controller.py",
        ROOT / "services/telegram_signal_processor.py",
        ROOT / "services/telegram_bet_resolver.py",
    ]

    for path in targets:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if ".place_order(" in text or "place_order(" in text:
            offenders.append(path.as_posix())

    assert not offenders, f"Bypass OrderManager trovato nei layer Telegram: {offenders}"


@pytest.mark.guardrail
def test_no_direct_client_order_submission_outside_allowed_layers():
    offenders = []

    allowed = {
        "order_manager.py",
        "services/betfair_service.py",
        "betfair_client.py",
        "tests",
    }

    for path in ROOT.rglob("*.py"):
        rel = path.as_posix()
        if any(rel == a or rel.startswith(f"{a}/") for a in allowed):
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")
        if ".place_bet(" in text or ".place_orders(" in text:
            offenders.append(rel)

    assert not offenders, f"Bypass diretto client ordini fuori layer consentiti: {offenders}"