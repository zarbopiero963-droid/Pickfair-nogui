from pathlib import Path

import pytest


ROOT = Path(".")


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

    allowed_exact = {
        "order_manager.py",
        "services/betfair_service.py",
        "betfair_client.py",
        "simulation_broker.py",
        "core/order_router.py",
    }

    for path in ROOT.rglob("*.py"):
        rel = path.as_posix()

        if rel.startswith("tests/"):
            continue
        if rel in allowed_exact:
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")

        if ".place_bet(" in text or ".place_orders(" in text:
            offenders.append(rel)

    assert not offenders, (
        "Bypass diretto client ordini fuori layer consentiti: "
        f"{offenders}"
    )


@pytest.mark.guardrail
def test_no_direct_trading_engine_submit_from_telegram_layers():
    """
    I layer Telegram non devono chiamare direttamente il trading engine.
    Devono produrre REQ/CMD sul bus o passare dai servizi/mediatori consentiti.
    """
    offenders = []

    targets = [
        ROOT / "telegram_module.py",
        ROOT / "telegram_listener.py",
        ROOT / "controllers/telegram_controller.py",
        ROOT / "services/telegram_signal_processor.py",
        ROOT / "services/telegram_bet_resolver.py",
    ]

    forbidden_snippets = [
        ".submit_quick_bet(",
        "submit_quick_bet(",
    ]

    for path in targets:
        if not path.exists():
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")

        for snippet in forbidden_snippets:
            if snippet in text:
                offenders.append((path.as_posix(), snippet))

    assert not offenders, (
        "Bypass diretto TradingEngine dai layer Telegram trovato: "
        f"{offenders}"
    )


@pytest.mark.guardrail
def test_req_cmd_conventions_present_in_risk_and_trading_layers():
    """
    Guardrail leggero architetturale:
    - RiskMiddleware deve parlare il linguaggio REQ/CMD
    - TradingEngine deve essere subscriber dei CMD/REQ attesi
    """
    risk_path = ROOT / "core/risk_middleware.py"
    trading_path = ROOT / "core/trading_engine.py"

    if risk_path.exists():
        risk_text = risk_path.read_text(encoding="utf-8", errors="ignore")
        assert "REQ_" in risk_text or "CMD_" in risk_text

    trading_text = trading_path.read_text(encoding="utf-8", errors="ignore")
    assert "REQ_QUICK_BET" in trading_text
    assert "CMD_QUICK_BET" in trading_text