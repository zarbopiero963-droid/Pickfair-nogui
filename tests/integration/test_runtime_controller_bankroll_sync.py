from __future__ import annotations

import threading
from datetime import datetime, timedelta

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode


class _Bus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def subscribe(self, *_args):
        return None

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload or {})))


class _DB:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = False
        return cfg

    def load_market_data_config(self):
        return {
            "market_data_mode": "poll",
            "enabled": False,
            "market_ids": [],
            "snapshot_fallback_enabled": True,
            "snapshot_fallback_interval_sec": 1,
        }


class _Betfair:
    def __init__(self, responses):
        self._responses = list(responses)

    def set_simulation_mode(self, enabled):
        _ = enabled
        return None

    def get_account_funds(self):
        if not self._responses:
            return {"available": 0.0}
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def status(self):
        return {"connected": True}

    def get_live_client(self):
        return object()

    def get_market_book_snapshot(self, market_id):
        _ = market_id
        return None

    def ensure_stream_session_ready(self):
        return True


class _Telegram:
    def start(self):
        return {"started": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


def _make_controller(*, responses):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    return rc, bus


@pytest.mark.integration
def test_runtime_controller_bankroll_sync_prefers_exchange_over_local_pnl():
    rc, bus = _make_controller(responses=[{"available": 120.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-prec",
            "table_id": 1,
            "batch_id": "batch-prec",
            "correlation_id": "corr-prec",
            "gross_pnl": 20.0,
            "commission_amount": 0.9,
            "net_pnl": 19.1,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    assert float(rc.risk_desk.bankroll_current) == 120.0
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert rc._last_bankroll_sync_result["balance_source"] == "exchange_available"
    assert any(topic == "BANKROLL_SYNC_RESULT" for topic, _ in bus.events)


@pytest.mark.integration
def test_runtime_controller_close_payload_preserves_settlement_provenance_fields():
    rc, bus = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-prov",
            "table_id": 1,
            "batch_id": "batch-prov",
            "correlation_id": "corr-prov",
            "gross_pnl": 20.0,
            "commission_amount": 0.9,
            "net_pnl": 19.1,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
            # conflicting legacy alias should not override explicit net_pnl
            "pnl": 999.0,
        }
    )

    closed = [payload for topic, payload in bus.events if topic == "BATCH_POSITION_CLOSED"]
    assert len(closed) == 1
    payload = closed[0]
    assert payload["gross_pnl"] == 20.0
    assert payload["commission_amount"] == 0.9
    assert payload["net_pnl"] == 19.1
    assert payload["commission_pct"] == 4.5
    assert payload["settlement_basis"] == "market_net_realized"
    assert payload["settlement_source"] == "test_settlement"
    assert payload["settlement_kind"] == "realized_settlement"
    assert payload["settlement_authority"] == "explicit_contract"
    assert payload["settlement_validation"] == "accepted"
    assert payload["settlement_acceptance"] == "ACCEPT_REALIZED_SETTLEMENT"
    assert payload["pnl"] == 19.1
    assert float(rc.risk_desk.realized_pnl) == 19.1


@pytest.mark.integration
def test_runtime_controller_close_payload_rejects_legacy_non_canonical_settlement_payload():
    rc, bus = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-null-net",
            "table_id": 1,
            "batch_id": "batch-null-net",
            "correlation_id": "corr-null-net",
            "gross_pnl": 13.0,
            "commission_amount": 0.5,
            "net_pnl": None,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "pnl": 12.5,
        }
    )

    closed = [payload for topic, payload in bus.events if topic == "BATCH_POSITION_CLOSED"]
    assert closed == []
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "LEGACY_SETTLEMENT_NON_AUTHORITATIVE"
    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_REJECTED_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0


@pytest.mark.integration
def test_runtime_controller_non_settlement_paths_do_not_trigger_bankroll_sync():
    rc, _ = _make_controller(responses=[{"available": 777.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_quick_bet_filled({"event_key": "evt-filled", "table_id": 1})

    result = rc._last_bankroll_sync_result
    assert result["bankroll_sync_status"] == "NOT_SETTLED"
    assert float(rc.risk_desk.bankroll_current) == 100.0


@pytest.mark.integration
def test_runtime_controller_status_exposes_last_bankroll_sync_result():
    rc, _ = _make_controller(responses=[{"available": 130.0}, {"available": 130.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-status",
            "table_id": 1,
            "batch_id": "batch-status",
            "correlation_id": "corr-status",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "net_pnl": 9.55,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    status = rc.get_status()
    assert "bankroll_sync" in status
    assert status["bankroll_sync"]["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert status["bankroll_sync"]["correlation_id"] == "corr-status"


_BREACHING_SETTLEMENT = {
    "event_key": "evt-daily-loss-trigger",
    "table_id": 1,
    "batch_id": "batch-daily-loss-trigger",
    "correlation_id": "corr-daily-loss-trigger",
    "gross_pnl": -15.0,
    "commission_amount": 0.0,
    "net_pnl": -15.0,
    "commission_pct": 4.5,
    "settlement_source": "test_settlement",
    "settlement_kind": "realized_settlement",
    "settlement_basis": "market_net_realized",
}


@pytest.mark.integration
def test_runtime_controller_daily_loss_breach_triggers_synchronous_emergency_stop():
    """Fase 1.4 (comportamento cambiato): un settlement che sfonda max_daily_loss
    scatena un emergency stop SINCRONO dentro _on_close_position (fail-closed) —
    mode=LOCKDOWN, _emergency_stopped, evento EMERGENCY_STOP_TRIGGERED — non piu'
    solo alert. Lo stato del monitor resta coerente."""
    rc, bus = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(dict(_BREACHING_SETTLEMENT))

    assert rc._emergency_stopped is True
    assert rc.mode == RuntimeMode.LOCKDOWN
    assert any(topic == "EMERGENCY_STOP_TRIGGERED" for topic, _ in bus.events)
    status = rc.get_status()
    assert status["daily_loss_monitor"]["breached"] is True
    assert status["daily_loss_monitor"]["threshold"] == 10.0
    assert status["daily_loss_monitor"]["daily_loss_amount"] >= 15.0


@pytest.mark.integration
def test_daily_loss_breach_stops_before_auto_trade_evaluation():
    """Codex P1 (niente ordine dopo il breach): lo stop e' SINCRONO e avviene
    PRIMA della valutazione/submission dell'auto-trade. Lo spy registra che, al
    momento in cui l'auto-trade viene valutato, l'emergenza e' GIA' attiva (con
    l'ordine vecchio — monitor dopo l'auto-trade — sarebbe stata False e un
    ordine sarebbe potuto partire). Nessun ordine risulta submesso."""
    rc, bus = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.sync_bankroll(100.0)

    seen: dict = {}
    original = rc._evaluate_and_maybe_submit_auto_next_trade

    def _spy(**kwargs):
        seen["emergency_at_eval"] = rc._emergency_stopped
        seen["mode_at_eval"] = rc.mode
        return original(**kwargs)

    rc._evaluate_and_maybe_submit_auto_next_trade = _spy

    payload = dict(_BREACHING_SETTLEMENT)
    payload.update({"cycle_executor_enabled": True, "auto_trade_enabled": True})
    rc._on_close_position(payload)

    assert seen["emergency_at_eval"] is True
    assert seen["mode_at_eval"] == RuntimeMode.LOCKDOWN
    assert rc._emergency_stopped is True
    auto = [p for topic, p in bus.events if topic == "AUTO_TRADE_MM_RESULT"][-1]
    assert auto.get("submitted") is False


@pytest.mark.integration
def test_daily_loss_status_poll_does_not_stop_when_not_active():
    """Greptile P1 (no stop da sola lettura): un breach osservato da get_status()
    (il monitor gira anche li') NON forza l'emergenza — l'enforcement avviene
    solo da _on_close_position e solo se ACTIVE. Evita uno start() bloccato
    dietro un'emergenza innescata da una status-poll all'avvio."""
    rc, bus = _make_controller(responses=[{"available": 50.0}] * 4)
    rc.mode = RuntimeMode.STOPPED
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.apply_closed_pnl(-50.0)  # perdita realized oltre soglia

    status = rc.get_status()

    assert status["daily_loss_monitor"]["breached"] is True
    assert rc._emergency_stopped is False
    assert rc.mode == RuntimeMode.STOPPED
    assert not any(topic == "EMERGENCY_STOP_TRIGGERED" for topic, _ in bus.events)


@pytest.mark.integration
def test_enforce_daily_loss_hard_stop_gated_on_active_and_breached():
    """L'enforcement ferma solo se breached AND runtime ACTIVE."""
    breach = {"breached": True, "daily_loss_amount": 15.0}

    rc, _ = _make_controller(responses=[{"available": 50.0}] * 4)
    rc.mode = RuntimeMode.STOPPED
    assert rc._enforce_daily_loss_hard_stop(breach) is False
    assert rc._emergency_stopped is False

    rc.mode = RuntimeMode.ACTIVE
    assert rc._enforce_daily_loss_hard_stop({"breached": False}) is False
    assert rc._emergency_stopped is False

    assert rc._enforce_daily_loss_hard_stop(breach) is True
    assert rc._emergency_stopped is True
    assert rc.mode == RuntimeMode.LOCKDOWN


@pytest.mark.integration
def test_enforce_daily_loss_hard_stop_atomic_under_concurrent_dispatch():
    """Greptile P1 (no doppio stop): sotto dispatch concorrente (piu' worker
    EventBus che chiamano _on_close_position) il check-and-stop atomico fa si'
    che UN SOLO chiamante esegua davvero lo stop."""
    rc, bus = _make_controller(responses=[{"available": 50.0}] * 8)
    rc.mode = RuntimeMode.ACTIVE
    breach = {"breached": True, "daily_loss_amount": 15.0}

    barrier = threading.Barrier(6)

    def _worker():
        barrier.wait()
        rc._enforce_daily_loss_hard_stop(breach)

    threads = [threading.Thread(target=_worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert rc._emergency_stopped is True
    assert sum(1 for topic, _ in bus.events if topic == "EMERGENCY_STOP_TRIGGERED") == 1


@pytest.mark.integration
def test_enforce_daily_loss_hard_stop_idempotent():
    """Gia' in emergenza: l'enforcement non ri-esegue lo stop (guard)."""
    rc, bus = _make_controller(responses=[{"available": 50.0}] * 4)
    rc.mode = RuntimeMode.ACTIVE
    breach = {"breached": True, "daily_loss_amount": 15.0}

    assert rc._enforce_daily_loss_hard_stop(breach) is True
    first = sum(1 for topic, _ in bus.events if topic == "EMERGENCY_STOP_TRIGGERED")
    assert first == 1

    rc.mode = RuntimeMode.ACTIVE  # anche se ri-attivato, il guard blocca il re-stop
    assert rc._enforce_daily_loss_hard_stop(breach) is True
    assert sum(1 for topic, _ in bus.events if topic == "EMERGENCY_STOP_TRIGGERED") == first


@pytest.mark.integration
def test_emergency_stop_cancel_all_survives_status_snapshot_failure():
    """Codex P1 (cancel-all in outage): se get_status dentro force_lockdown
    solleva (es. get_account_funds in timeout), emergency_stop NON deve abortire
    prima del cancel-all — il flag e' gia' settato/persistito, quindi garantisce
    LOCKDOWN, registra l'errore di lockdown e prosegue."""
    rc, bus = _make_controller(responses=[RuntimeError("BROKER_OUTAGE")])
    rc.mode = RuntimeMode.ACTIVE

    result = rc.emergency_stop(reason="DAILY_LOSS_BREACH:50")  # NON deve sollevare

    assert rc._emergency_stopped is True
    assert rc.mode == RuntimeMode.LOCKDOWN
    assert any(topic == "EMERGENCY_STOP_TRIGGERED" for topic, _ in bus.events)
    assert any(e.get("stage") == "force_lockdown" for e in result.get("cancel_errors", []))


@pytest.mark.integration
def test_daily_loss_enforced_before_bankroll_sync_network_window():
    """Codex P1: l'enforcement avviene PRIMA del bankroll sync (rete). Lo spy
    registra che, quando _sync_bankroll_post_settlement viene chiamato,
    l'emergenza e' GIA' attiva — cosi' nella finestra di rete del sync nessun
    SIGNAL_RECEIVED concorrente puo' piazzare un ordine dopo il breach."""
    rc, _ = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.sync_bankroll(100.0)

    seen: dict = {}
    original = rc._sync_bankroll_post_settlement

    def _spy(payload):
        seen["emergency_at_sync"] = rc._emergency_stopped
        return original(payload)

    rc._sync_bankroll_post_settlement = _spy

    rc._on_close_position(dict(_BREACHING_SETTLEMENT))

    assert seen["emergency_at_sync"] is True
    assert rc.mode == RuntimeMode.LOCKDOWN


@pytest.mark.integration
def test_daily_loss_breach_while_paused_hard_stops():
    """Codex P1: un settlement che sfonda il limite mentre il runtime e' PAUSED
    fa scattare l'emergency stop (non solo da ACTIVE) — niente bypass."""
    rc, bus = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.mode = RuntimeMode.PAUSED
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(dict(_BREACHING_SETTLEMENT))

    assert rc._emergency_stopped is True
    assert rc.mode == RuntimeMode.LOCKDOWN
    assert any(topic == "EMERGENCY_STOP_TRIGGERED" for topic, _ in bus.events)


@pytest.mark.integration
def test_resume_refused_after_daily_loss_breach():
    """Codex P1: resume() non riattiva il trading se la perdita giornaliera e'
    gia' sfondata (stesso giorno): fail-closed, niente bypass dell'emergenza."""
    rc, _ = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.mode = RuntimeMode.PAUSED
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.apply_closed_pnl(-50.0)  # breach registrato

    out = rc.resume()

    assert out["resumed"] is False
    assert out["reason"] == "daily_loss_breached"
    assert rc.mode != RuntimeMode.ACTIVE


@pytest.mark.integration
def test_daily_loss_precheck_enforced_on_rejected_settlement_branch():
    """Codex P1: con la perdita realized GIA' oltre il limite, anche un
    settlement che esce dal ramo REJECTED (early-return) viene hard-stoppato
    dal precheck a inizio _on_close_position. Il payload usa il campo legacy
    `pnl` (niente contratto canonico) cosi' _extract_settlement_contract lo
    classifica DAVVERO come rejected_non_canonical_settlement."""
    rc, _ = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.mode = RuntimeMode.ACTIVE
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.apply_closed_pnl(-50.0)
    # monitor gia' breached (come l'avrebbe lasciato un settlement precedente),
    # ma non ancora fermato (corner breached-but-not-stopped)
    rc._daily_loss_monitor_state = dict(rc._monitor_daily_loss_breach(source="PRIME"))
    rc._emergency_stopped = False
    rc.mode = RuntimeMode.ACTIVE

    rejected_legacy = {
        "event_key": "evt-rejected-legacy",
        "table_id": 1,
        "batch_id": "batch-rejected-legacy",
        "correlation_id": "corr-rejected-legacy",
        "pnl": -15.0,  # legacy/non-canonico -> ramo rejected reale
    }
    rc._on_close_position(rejected_legacy)

    assert rc._emergency_stopped is True
    assert rc.mode == RuntimeMode.LOCKDOWN


@pytest.mark.integration
def test_daily_loss_enforced_on_recovery_ambiguous_self_crossing_settlement():
    """Codex P1 (round-5): un settlement ACCETTATO che da solo sfonda il limite,
    ma che esce dal ramo recovery fail-closed (recovery-store ambiguo, PnL NON
    applicato), viene comunque hard-stoppato grazie alla PROIEZIONE del PnL nel
    check del ramo recovery (fail-closed: meglio un falso stop da duplicato che
    un kill-switch mancato)."""
    rc, _ = _make_controller(responses=[{"available": 85.0}] * 4)
    rc.mode = RuntimeMode.ACTIVE
    rc.config.max_daily_loss = 10.0
    # nessuna perdita pregressa: e' QUESTO settlement (-50) che sfonda
    rc._read_cycle_recovery_state = lambda _key: {"status": "RECOVERY_STATE_AMBIGUOUS"}

    rc._on_close_position(dict(_BREACHING_SETTLEMENT, net_pnl=-50.0, gross_pnl=-50.0))

    assert rc._emergency_stopped is True
    assert rc.mode == RuntimeMode.LOCKDOWN


@pytest.mark.integration
def test_start_refuses_live_when_daily_loss_breached_same_day():
    """Codex P1: una posizione live settlata DOPO stop() (che lascia STOPPED ma
    non chiude le posizioni) registra il breach giornaliero; un start() in LIVE
    deve RIFIUTARE (fail-closed, reason=daily_loss_breached) invece di riprendere
    il trading nello stesso giorno con _emergency_stopped=False."""
    rc, bus = _make_controller(responses=[{"available": 50.0}] * 4)
    # le settings devono fornire la soglia: start() ricarica la config.
    cfg = RoserpinaConfig()
    cfg.anti_duplication_enabled = False
    cfg.max_daily_loss = 10.0
    rc.settings_service.load_roserpina_config = lambda: cfg
    rc.mode = RuntimeMode.STOPPED  # scenario reale: dopo stop()
    rc.risk_desk.apply_closed_pnl(-50.0)  # breach realized registrato da STOPPED

    out = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert out["ok"] is False
    assert out["reason"] == "daily_loss_breached"
    assert rc.mode != RuntimeMode.ACTIVE
    # CodeRabbit: lo stato deve essere sincronizzato a SIMULATION, non lasciare
    # il controller live-capable dopo il rifiuto.
    assert rc.execution_mode == "SIMULATION"
    assert rc.live_enabled is False
    assert rc.get_effective_execution_mode() == "SIMULATION"
    assert any(
        topic == "LIVE_EXECUTION_REFUSED" and payload.get("reason_code") == "DAILY_LOSS_BREACHED"
        for topic, payload in bus.events
    )


@pytest.mark.integration
def test_breach_refusal_survives_broker_outage_on_start_and_resume():
    """CodeRabbit: i rami di rifiuto fail-closed (start/resume) NON devono
    dipendere dall'I/O broker. Con get_account_funds() che solleva (outage),
    start(LIVE) e resume() devono comunque RITORNARE il rifiuto daily_loss
    invece di propagare l'eccezione (status snapshot degrada su stato locale)."""
    outage = RuntimeError("BROKER_OUTAGE")

    rc, _ = _make_controller(responses=[outage, outage, outage, outage])
    cfg = RoserpinaConfig()
    cfg.anti_duplication_enabled = False
    cfg.max_daily_loss = 10.0
    rc.settings_service.load_roserpina_config = lambda: cfg
    rc.mode = RuntimeMode.STOPPED
    rc.risk_desk.apply_closed_pnl(-50.0)

    out_start = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
    assert out_start["ok"] is False
    assert out_start["reason"] == "daily_loss_breached"

    rc2, _ = _make_controller(responses=[outage, outage, outage, outage])
    rc2.config.max_daily_loss = 10.0
    rc2.mode = RuntimeMode.PAUSED
    rc2.risk_desk.apply_closed_pnl(-50.0)

    out_resume = rc2.resume()
    assert out_resume["resumed"] is False
    assert out_resume["reason"] == "daily_loss_breached"


@pytest.mark.integration
def test_runtime_controller_daily_loss_breach_state_is_persistent_until_day_rollover():
    # Isola la logica di PERSISTENZA dello stato del monitor dal kill-switch
    # (testato a parte): in mode non-ACTIVE l'enforcement non scatta, quindi
    # nessun emergency_stop interferisce con alert_count/last_alert_event.
    rc, bus = _make_controller(responses=[{"available": 85.0}, {"available": 120.0}, {"available": 120.0}])
    rc.mode = RuntimeMode.STOPPED
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-daily-loss-persist-1",
            "table_id": 1,
            "batch_id": "batch-daily-loss-persist-1",
            "correlation_id": "corr-daily-loss-persist-1",
            "gross_pnl": -15.0,
            "commission_amount": 0.0,
            "net_pnl": -15.0,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )
    rc._on_close_position(
        {
            "event_key": "evt-daily-loss-persist-2",
            "table_id": 2,
            "batch_id": "batch-daily-loss-persist-2",
            "correlation_id": "corr-daily-loss-persist-2",
            "gross_pnl": 35.0,
            "commission_amount": 1.575,
            "net_pnl": 33.425,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    status = rc.get_status()
    assert status["daily_loss_monitor"]["breached"] is True
    assert status["daily_loss_monitor"]["reason"] == "daily_loss_breach_persistent_until_day_rollover"
    assert status["daily_loss_monitor"]["alert_count"] == 1
    assert status["daily_loss_monitor"]["last_alert_event"] == "DAILY_LOSS_BREACH_TRIGGERED"


@pytest.mark.integration
def test_runtime_controller_daily_loss_monitor_rollover_uses_day_baseline_not_lifetime_realized():
    rc, _ = _make_controller(responses=[{"available": 100.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.config.max_daily_loss = 10.0
    rc.risk_desk.sync_bankroll(100.0)
    rc.risk_desk.realized_pnl = -50.0

    yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    rc._daily_loss_monitor_state = {
        "day_utc": yesterday,
        "threshold": 10.0,
        "realized_pnl_day_baseline": 0.0,
        "realized_pnl": -50.0,
        "intraday_realized_pnl": -50.0,
        "daily_loss_amount": 50.0,
        "breached": True,
        "breached_at": "2026-04-17T00:00:00",
        "last_status": "DAILY_LOSS_BREACHED",
        "reason": "daily_loss_threshold_exceeded",
        "alert_count": 1,
        "last_alert_event": "DAILY_LOSS_BREACH_TRIGGERED",
        "last_checked_at": "2026-04-17T23:59:59",
    }

    status = rc.get_status()
    monitor = status["daily_loss_monitor"]
    assert monitor["breached"] is False
    assert monitor["daily_loss_amount"] == 0.0
    assert monitor["intraday_realized_pnl"] == 0.0
    assert monitor["realized_pnl_day_baseline"] == -50.0

@pytest.mark.integration
def test_runtime_controller_rejects_ambiguous_zero_fallback_balance_payload():
    rc, _ = _make_controller(responses=[{"available": 0.0, "exposure": 0.0, "total": 0.0, "simulated": False}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-zero-fallback-int",
            "table_id": 1,
            "batch_id": "batch-zero-fallback-int",
            "correlation_id": "corr-zero-fallback-int",
            "gross_pnl": 20.0,
            "commission_amount": 0.9,
            "net_pnl": 19.1,
            "commission_pct": 4.5,
            "settlement_source": "simulation_broker",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    assert float(rc.risk_desk.bankroll_current) == 100.0
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_BALANCE_UNAVAILABLE"


@pytest.mark.integration
def test_runtime_controller_close_updates_realized_pnl_even_with_exchange_first_bankroll_sync():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-realized-int",
            "table_id": 1,
            "batch_id": "batch-realized-int",
            "correlation_id": "corr-realized-int",
            "gross_pnl": 8.0,
            "commission_amount": 0.36,
            "net_pnl": 7.64,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    assert float(rc.risk_desk.realized_pnl) == 7.64


    assert float(rc.risk_desk.bankroll_current) == 150.0


@pytest.mark.integration
def test_runtime_controller_rejects_ambiguous_contract_without_explicit_or_legacy_net():
    rc, _ = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-reject-ambiguous",
            "table_id": 1,
            "batch_id": "batch-reject-ambiguous",
            "correlation_id": "corr-reject-ambiguous",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "commission_pct": 4.5,
            "settlement_source": "simulation_broker",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "MISSING_CANONICAL_SETTLEMENT_FIELDS"
    assert rc._last_bankroll_sync_result["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0


@pytest.mark.integration
def test_runtime_controller_rejects_mark_to_market_settlement_kind_for_close_processing():
    rc, _ = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-reject-mtm",
            "table_id": 1,
            "batch_id": "batch-reject-mtm",
            "correlation_id": "corr-reject-mtm",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "net_pnl": 9.55,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "mark_to_market_estimate",
            "settlement_basis": "market_net_realized",
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "SETTLEMENT_KIND_NOT_REALIZED"
    assert rc._last_bankroll_sync_result["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0


@pytest.mark.integration
def test_runtime_controller_rejected_settlement_does_not_release_table_or_mutate_recovery_loss():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-reject-ordering",
        exposure=20.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.table_manager.release(1, pnl=-10.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-reject-ordering",
        exposure=15.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.duplication_guard.acquire("evt-reject-ordering")
    before = rc.table_manager.get_table(1)
    assert before is not None
    assert before.current_event_key == "evt-reject-ordering"
    assert float(before.loss_amount) == 10.0
    assert before.in_recovery is True

    rc._on_close_position(
        {
            "event_key": "evt-reject-ordering",
            "table_id": 1,
            "batch_id": "batch-reject-ordering",
            "correlation_id": "corr-reject-ordering",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "net_pnl": 9.55,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "mark_to_market_estimate",
            "settlement_basis": "market_net_realized",
        }
    )

    after = rc.table_manager.get_table(1)
    assert after is not None
    assert after.current_event_key == "evt-reject-ordering"
    assert float(after.loss_amount) == 10.0
    assert after.in_recovery is True
    assert any(k["event_key"] == "evt-reject-ordering" for k in rc.duplication_guard.snapshot()["active_keys"])


@pytest.mark.integration
def test_runtime_controller_legacy_non_canonical_settlement_does_not_release_table_or_duplication_key():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-legacy-unlock",
        exposure=20.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.table_manager.release(1, pnl=-10.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-legacy-unlock",
        exposure=15.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.duplication_guard.acquire("evt-legacy-unlock")

    rc._on_close_position(
        {
            "event_key": "evt-legacy-unlock",
            "table_id": 1,
            "batch_id": "batch-legacy-unlock",
            "correlation_id": "corr-legacy-unlock",
            "gross_pnl": 13.0,
            "commission_amount": 0.5,
            "net_pnl": None,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "pnl": 12.5,
            "mm_context": {"cycle_active": True},
        }
    )

    after = rc.table_manager.get_table(1)
    assert after is not None
    assert after.current_event_key == "evt-legacy-unlock"
    assert float(after.loss_amount) == 10.0
    assert after.in_recovery is True
    assert any(k["event_key"] == "evt-legacy-unlock" for k in rc.duplication_guard.snapshot()["active_keys"])
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "LEGACY_SETTLEMENT_NON_AUTHORITATIVE"
    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_REJECTED_SETTLEMENT"


@pytest.mark.integration
def test_runtime_controller_accepted_settlement_still_releases_table_and_updates_recovery():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-accept-ordering",
        exposure=20.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.table_manager.release(1, pnl=-10.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-accept-ordering",
        exposure=15.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.duplication_guard.acquire("evt-accept-ordering")

    rc._on_close_position(
        {
            "event_key": "evt-accept-ordering",
            "table_id": 1,
            "batch_id": "batch-accept-ordering",
            "correlation_id": "corr-accept-ordering",
            "gross_pnl": 5.0,
            "commission_amount": 0.225,
            "net_pnl": 4.775,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    after = rc.table_manager.get_table(1)
    assert after is not None
    assert after.current_event_key == ""
    assert float(after.loss_amount) == 5.225
    assert after.in_recovery is True
    assert all(k["event_key"] != "evt-accept-ordering" for k in rc.duplication_guard.snapshot()["active_keys"])


@pytest.mark.integration
def test_runtime_controller_helper_like_close_payload_does_not_become_authoritative_realized_settlement():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-helper-like-contract",
            "table_id": 1,
            "batch_id": "batch-helper-like-contract",
            "correlation_id": "corr-helper-like-contract",
            "gross_pnl": 30.0,
            "commission_amount": 1.35,
            "net_pnl": 28.65,
            "commission_pct": 4.5,
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "MISSING_CANONICAL_SETTLEMENT_FIELDS"
    assert rc._last_bankroll_sync_result["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0
