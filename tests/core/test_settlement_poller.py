"""Test hard del poller settlement del RuntimeController (PR
`runtime_settlement_wiring`) e della facade fail-closed
``BetfairService.list_cleared_orders``.

Il poller e' il ciclo di chiusura reale: listClearedOrders(SETTLED,
group_by=MARKET) => PnLEngine.apply_cleared_market_settlement =>
RUNTIME_CLOSE_POSITION => contratto settlement => realized PnL => daily-loss.
Su questo percorso un errore mascherato da "nessun settlement" e' una perdita
invisibile al kill-switch giornaliero: ogni ramo va provato fail-closed.

Copertura:
- PASS: emissione canonica (parametri fetch inclusi), accettazione dal
  contratto VERO, applicazione al daily-loss fino al breach+emergency stop.
- BLOCK: default OFF, SIM => zero rete, runtime non ACTIVE => zero rete,
  fetch fallito => round abortito e ritentabile, righe malformate scartate
  (mai inventare un profit), dedupe in-memory e durevole (riavvio), stato
  recovery illeggibile => emissione sospesa ma ritentabile, config
  malformata => poller disabilitato, lifecycle thread start/stop pulito.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode
from services.betfair_service import BetfairService


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_args):
        return None

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload or {})))


class _DB:
    """Fake db con cycle recovery state configurabile.

    ``recovery``: mappa settlement_key -> stato (dict) oppure Exception da
    sollevare. ``fail_reads=True`` fa fallire OGNI lettura (db illeggibile).
    """

    def __init__(self, recovery=None):
        self.recovery = dict(recovery or {})
        self.fail_reads = False

    def get_cycle_recovery_state(self, key):
        if self.fail_reads:
            raise RuntimeError("DB_UNAVAILABLE")
        item = self.recovery.get(key)
        if isinstance(item, Exception):
            raise item
        return item

    def _execute(self, *_args, **_kwargs):
        return None

    def _fetch_one(self, *_args, **_kwargs):
        return None

    def _fetch_all(self, *_args, **_kwargs):
        return []


class _Settings:
    def __init__(self, extra=None):
        self.extra = dict(extra or {})

    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = False
        cfg.max_daily_loss = 100.0
        cfg.max_drawdown_hard_stop_pct = 20.0
        cfg.max_open_exposure = 250.0
        return cfg

    def load_market_data_config(self):
        return {"market_data_mode": "poll", "enabled": False, "market_ids": []}

    def get_all_settings(self):
        return dict(self.extra)


class _SettingsRaising(_Settings):
    def get_all_settings(self):
        raise RuntimeError("SETTINGS_UNAVAILABLE")


class _BetfairService:
    def __init__(self, results=None):
        self.calls = []
        self._results = list(results or [])

    def set_simulation_mode(self, _enabled):
        return None

    def list_cleared_orders(self, **kwargs):
        self.calls.append(dict(kwargs))
        if not self._results:
            return []
        item = self._results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def get_account_funds(self):
        return {"available": 100.0}

    def status(self):
        return {"connected": True}

    def get_live_client(self):
        return object()

    def get_market_book_snapshot(self, _market_id):
        return None

    def ensure_stream_session_ready(self):
        return True

    def disconnect(self):
        return None

    def cancel_all_orders(self):
        return {"ok": True}


class _Telegram:
    def start(self):
        return {"started": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


_POLL_CFG = {"enabled": True, "poll_sec": 60.0, "lookback_hours": 24.0}


def _controller(*, results=None, recovery=None, settings=None, db=None):
    rc = RuntimeController(
        bus=_Bus(),
        db=db if db is not None else _DB(recovery=recovery),
        settings_service=settings or _Settings(),
        betfair_service=_BetfairService(results=results),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    rc.simulation_mode = False
    rc._settlement_poll_cfg = dict(_POLL_CFG)
    return rc


def _closes(rc):
    return [p for t, p in rc.bus.events if t == "RUNTIME_CLOSE_POSITION"]


_ROW = {
    "marketId": "1.100",
    "profit": -40.0,
    "commission": 0.0,
    "settledDate": "2026-08-24T09:00:00Z",
}


# ===========================================================================
# PASS — emissione canonica e wiring fino al daily-loss
# ===========================================================================

@pytest.mark.integration
def test_poll_emette_settlement_canonico_con_parametri_fetch_corretti():
    rc = _controller(results=[[dict(_ROW)]])

    before = datetime.now(timezone.utc)
    rc._poll_cleared_settlements()

    call = rc.betfair_service.calls[0]
    assert call["bet_status"] == "SETTLED"
    assert call["group_by"] == "MARKET"
    settled_after = datetime.fromisoformat(call["settled_after"])
    atteso = before - timedelta(hours=24)
    assert abs((settled_after - atteso).total_seconds()) < 300

    closes = _closes(rc)
    assert len(closes) == 1
    payload = closes[0]
    assert payload["event_key"] == "cleared:1.100"
    assert payload["gross_pnl"] == pytest.approx(-40.0)
    assert payload["commission_amount"] == pytest.approx(0.0)
    assert payload["net_pnl"] == pytest.approx(-40.0)
    assert payload["settlement_source"] == "betfair_cleared_orders"
    assert payload["settlement_kind"] == "realized_settlement"
    assert payload["settlement_basis"] == "market_net_realized"

    contract = RuntimeController._extract_settlement_contract(payload)
    assert contract["settlement_validation"] == "accepted"
    assert contract["settlement_acceptance"] == "ACCEPT_REALIZED_SETTLEMENT"


@pytest.mark.integration
def test_settlement_emesso_arriva_al_daily_loss_fino_al_breach():
    """Il filo intero: perdita reale => realized applicato => monitor
    daily-loss => al superamento del limite (100) hard-stop di emergenza.
    Prima perdita -40: contata, nessun breach. Seconda -70: cumulata -110
    => breach => emergency stop DUREVOLE."""
    rc = _controller(results=[[dict(_ROW)]])

    rc._poll_cleared_settlements()
    payload_1 = _closes(rc)[0]
    rc._on_close_position(payload_1)

    assert rc.risk_desk.realized_pnl == pytest.approx(-40.0)
    stato = dict(rc._daily_loss_monitor_state)
    assert stato["daily_loss_amount"] == pytest.approx(40.0)
    assert bool(stato["breached"]) is False
    assert rc._emergency_stopped is False

    payload_2 = rc.pnl_engine.apply_cleared_market_settlement(
        market_id="1.200", gross_pnl=-70.0,
    )
    rc._on_close_position(payload_2)

    assert rc.risk_desk.realized_pnl == pytest.approx(-110.0)
    stato = dict(rc._daily_loss_monitor_state)
    assert stato["daily_loss_amount"] == pytest.approx(110.0)
    assert bool(stato["breached"]) is True
    assert rc._emergency_stopped is True


# ===========================================================================
# BLOCK — fail-closed su ogni ramo
# ===========================================================================

@pytest.mark.integration
def test_poll_in_simulation_mode_nessuna_chiamata():
    rc = _controller(results=[[dict(_ROW)]])
    rc.simulation_mode = True
    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls == []
    assert _closes(rc) == []


@pytest.mark.integration
def test_poll_runtime_non_active_nessuna_chiamata():
    rc = _controller(results=[[dict(_ROW)]])
    rc.mode = RuntimeMode.STOPPED
    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls == []
    assert _closes(rc) == []


@pytest.mark.integration
def test_poll_fetch_fallito_round_abortito_poi_recupera():
    rc = _controller(
        results=[RuntimeError("NOT_AUTHENTICATED"), [dict(_ROW)]]
    )

    rc._poll_cleared_settlements()
    assert _closes(rc) == []  # round abortito, nessun "nessun settlement" finto

    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1  # ritentato e recuperato


@pytest.mark.integration
def test_poll_righe_malformate_scartate_mai_inventare_profit():
    righe = [
        {"marketId": "", "profit": 10.0},
        {"marketId": "1.2", "profit": "12"},
        {"marketId": "1.3", "profit": float("nan")},
        {"marketId": "1.4"},
        {"marketId": "1.6", "profit": True},
        "spazzatura",
        {"marketId": "1.5", "profit": 25.0},
    ]
    rc = _controller(results=[righe])

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert len(closes) == 1
    assert closes[0]["event_key"] == "cleared:1.5"
    assert closes[0]["commission_amount"] == pytest.approx(1.125)
    assert closes[0]["net_pnl"] == pytest.approx(23.875)


@pytest.mark.integration
def test_poll_dedupe_in_memory_stesso_mercato_due_giri():
    rc = _controller(results=[[dict(_ROW)], [dict(_ROW)]])
    rc._poll_cleared_settlements()
    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1


@pytest.mark.integration
def test_poll_dedupe_durevole_dopo_riavvio_nessuna_riemissione():
    """Riavvio simulato: controller NUOVO (set in-memory vuoto) ma il
    checkpoint durevole del consumer esiste => la stessa settlement_key
    non viene ri-emessa (il PnL non si applica due volte)."""
    rc = _controller(
        results=[[dict(_ROW)]],
        recovery={"cleared:1.100": {"exists": True, "processed": True}},
    )
    rc._poll_cleared_settlements()
    assert _closes(rc) == []
    assert "cleared:1.100" in rc._settlement_emitted_keys


@pytest.mark.integration
def test_poll_recovery_state_illeggibile_sospende_senza_marcare():
    """DB illeggibile: nessuna emissione (rischio doppio conteggio) ma la
    chiave NON viene marcata vista => quando il db risponde si recupera."""
    db = _DB()
    db.fail_reads = True
    rc = _controller(results=[[dict(_ROW)], [dict(_ROW)]], db=db)

    rc._poll_cleared_settlements()
    assert _closes(rc) == []
    assert "cleared:1.100" not in rc._settlement_emitted_keys

    db.fail_reads = False
    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1


@pytest.mark.integration
def test_poller_disabilitato_di_default_nessun_thread():
    rc = _controller(settings=_Settings())  # nessuna chiave settlement.*
    rc._start_settlement_poller()
    assert rc._settlement_poll_thread is None
    assert rc.betfair_service.calls == []


@pytest.mark.integration
def test_poller_lifecycle_start_stop_pulito():
    rc = _controller(settings=_Settings({"settlement.poll_enabled": True}))
    rc.mode = RuntimeMode.STOPPED  # il giro nel thread esce subito, zero rete

    rc._start_settlement_poller()
    thread = rc._settlement_poll_thread
    assert thread is not None and thread.is_alive()

    rc._stop_settlement_poller()
    assert rc._settlement_poll_thread is None
    assert not thread.is_alive()
    assert rc.betfair_service.calls == []


@pytest.mark.integration
def test_poller_non_parte_in_simulation_mode_anche_se_abilitato():
    rc = _controller(settings=_Settings({"settlement.poll_enabled": True}))
    rc.simulation_mode = True
    rc._start_settlement_poller()
    assert rc._settlement_poll_thread is None


@pytest.mark.integration
def test_config_malformata_o_illeggibile_poller_disabilitato():
    rc = _controller(settings=_SettingsRaising())
    cfg = rc._load_settlement_poll_config()
    assert cfg["enabled"] is False

    rc2 = _controller(settings=_Settings({
        "settlement.poll_enabled": True,
        "settlement.poll_sec": "abc",
        "settlement.lookback_hours": 24,
    }))
    cfg2 = rc2._load_settlement_poll_config()
    assert cfg2["enabled"] is False  # parse fallito => default fail-closed


@pytest.mark.integration
def test_config_clamp_poll_sec_e_lookback():
    rc = _controller(settings=_Settings({
        "settlement.poll_enabled": True,
        "settlement.poll_sec": 1,
        "settlement.lookback_hours": 999,
    }))
    cfg = rc._load_settlement_poll_config()
    assert cfg["enabled"] is True
    assert cfg["poll_sec"] == pytest.approx(5.0)
    assert cfg["lookback_hours"] == pytest.approx(168.0)


# ===========================================================================
# Facade BetfairService.list_cleared_orders — fail-closed
# ===========================================================================

class _ClearedClient:
    def __init__(self, result=None, error=None):
        self.calls = []
        self._result = result if result is not None else []
        self._error = error

    def list_cleared_orders(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self._error is not None:
            raise self._error
        return self._result


def _service(*, simulation=False, session_invalid=False, client=None):
    svc = BetfairService(object())
    svc.simulation_mode = simulation
    svc._session_invalid = session_invalid
    svc._session_invalid_reason = "SESSION_EXPIRED" if session_invalid else ""
    svc.client = client
    return svc


@pytest.mark.unit
def test_facade_sim_mode_solleva_niente_lista_vuota_finta():
    client = _ClearedClient()
    svc = _service(simulation=True, client=client)
    with pytest.raises(RuntimeError, match="CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION"):
        svc.list_cleared_orders()
    assert client.calls == []


@pytest.mark.unit
def test_facade_sessione_invalida_solleva():
    svc = _service(session_invalid=True, client=_ClearedClient())
    with pytest.raises(RuntimeError, match="LIVE_BLOCKED_SESSION_INVALID"):
        svc.list_cleared_orders()


@pytest.mark.unit
def test_facade_client_assente_solleva():
    svc = _service(client=None)
    with pytest.raises(RuntimeError, match="NO_LIVE_CLIENT"):
        svc.list_cleared_orders()


@pytest.mark.unit
def test_facade_inoltra_parametri_e_filtra_righe_non_dict():
    client = _ClearedClient(result=[{"marketId": "1.1"}, "x", None, {"marketId": "1.2"}])
    svc = _service(client=client)

    rows = svc.list_cleared_orders(
        bet_status="SETTLED",
        settled_after="2026-08-23T00:00:00+00:00",
        group_by="MARKET",
    )

    assert client.calls == [{
        "bet_status": "SETTLED",
        "settled_after": "2026-08-23T00:00:00+00:00",
        "settled_before": None,
        "group_by": "MARKET",
    }]
    assert rows == [{"marketId": "1.1"}, {"marketId": "1.2"}]


@pytest.mark.unit
def test_facade_session_expired_instrada_recovery_e_ripropaga():
    client = _ClearedClient(error=RuntimeError("ANGX-0003 SESSION_EXPIRED"))
    svc = _service(client=client)
    chiamate = {}
    svc.handle_session_expiry = lambda reason="": chiamate.setdefault("reason", reason)

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        svc.list_cleared_orders()

    assert "SESSION_EXPIRED" in chiamate["reason"]
