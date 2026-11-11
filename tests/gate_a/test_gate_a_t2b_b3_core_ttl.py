"""Gate A T2b batch b3: core/runtime_controller.py — poller TTL DIRECT unmatched (BEX/EXEC)."""
from __future__ import annotations

import logging
import time
from types import SimpleNamespace

import core.runtime_controller as rc_mod
from core.runtime_controller import RuntimeController


class _Config:
    table_count = 1
    anti_duplication_enabled = False
    allow_recovery = False
    auto_reset_drawdown_pct = 90
    defense_drawdown_pct = 7.5
    lockdown_drawdown_pct = 95
    direct_unmatched_ttl_enabled = False
    direct_unmatched_ttl_sec = 120.0
    direct_unmatched_ttl_poll_sec = 30.0

    def __getattr__(self, _name):
        return 0


class _Settings:
    @staticmethod
    def load_roserpina_config():
        return _Config()


class _Bus:
    def __init__(self):
        self.events = []

    @staticmethod
    def subscribe(*_a, **_k):
        return None

    def publish(self, name, payload=None):
        self.events.append((name, payload or {}))


class _Db:
    @staticmethod
    def _execute(*_a, **_k):
        return None

    @staticmethod
    def get_pending_sagas():
        return []


class _Client:
    def __init__(self, name, *, ok=True, on_call=None):
        self.name = name
        self.calls = []
        self._ok = ok
        self._on_call = on_call

    def cancel_orders(self, *, market_id, bet_ids=None, instructions=None, customer_ref=""):
        ids = [str(b) for b in (bet_ids or [])] or [str(i.get("betId")) for i in (instructions or [])]
        if self._on_call:
            self._on_call()
        self.calls.append((market_id, tuple(ids), "bet_ids" if bet_ids else "instructions"))
        status = "SUCCESS" if self._ok else "FAILURE"
        return {"status": "SUCCESS", "instructionReports": [{"status": status} for _ in ids]}


class _Betfair:
    _session_invalid = False

    def __init__(self, *, orders=None, orders_raises=False, sim=None, live=None):
        self._orders = orders if orders is not None else []
        self._orders_raises = orders_raises
        self.sim = sim
        self.live = live
        self.fetches = 0

    def list_current_orders(self, market_ids=None):
        self.fetches += 1
        if self._orders_raises:
            raise RuntimeError("list boom")
        return list(self._orders)

    def get_simulation_broker(self):
        return self.sim

    def get_live_client(self):
        return self.live

    @staticmethod
    def set_simulation_mode(*_a, **_k):
        return None

    @staticmethod
    def connect(**_k):
        return {}

    @staticmethod
    def disconnect():
        return None

    @staticmethod
    def get_account_funds():
        return {"available": 0.0}

    @staticmethod
    def status():
        return {"connected": False}


class _Telegram:
    @staticmethod
    def start():
        return {}

    @staticmethod
    def stop():
        return None

    @staticmethod
    def status():
        return {}


def _make_rc(*, orders=None, orders_raises=False, sim=None, live=None, enabled=True):
    bf = _Betfair(orders=orders, orders_raises=orders_raises, sim=sim, live=live)
    rc = RuntimeController(bus=_Bus(), db=_Db(), settings_service=_Settings(),
                           betfair_service=bf, telegram_service=_Telegram())
    rc.simulation_mode = True
    rc.config.direct_unmatched_ttl_enabled = enabled
    rc._last_direct_ttl_poll_at = float("-inf")
    return rc


def _order(bet_id, *, market_id="1.1", age=1000.0):
    return {"betId": bet_id, "marketId": market_id, "sizeMatched": 0.0,
            "sizeRemaining": 5.0, "placed_epoch": time.time() - age}


def _track(rc, cref, bet_id):
    rc._on_quick_bet_accepted({"customer_ref": cref, "bet_id": bet_id,
                               "best_price_source": "LIVE_BOOK_DIRECT"})


def _names(rc):
    return [n for n, _ in rc.bus.events if n.startswith("DIRECT_UNMATCHED")]


def test_trg_628_b_1056(monkeypatch):
    """ID: TRG-628-B-1056 / PF-B1-R2-TTL-POLL-GATE

    Il poller TTL ritorna senza fetch/cancel se il flag e' assente (default
    OFF) o falso, oppure entro l'intervallo; intervallo default 30s se non
    impostato, clamp minimo 1s.
    """
    sim = _Client("sim")
    rc = _make_rc(orders=[_order("B1")], sim=sim, enabled=False)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 0 and sim.calls == []
    rc.config = SimpleNamespace()                       # flag assente => default OFF
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 0 and sim.calls == []

    clock = {"t": 1000.0}
    monkeypatch.setattr(rc_mod.time, "monotonic", lambda: clock["t"])

    # poll_sec 0.2 => clamp a 1s
    rc = _make_rc(orders=[], sim=sim)
    rc.config.direct_unmatched_ttl_poll_sec = 0.2
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 1
    clock["t"] = 1000.5
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 1               # entro 1s: nessun fetch
    clock["t"] = 1001.1
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 2

    # poll_sec non impostato (None) => 30s
    rc = _make_rc(orders=[], sim=sim)
    rc.config.direct_unmatched_ttl_poll_sec = None
    _track(rc, "C1", "B1")
    clock["t"] = 2000.0
    rc._poll_direct_unmatched_ttl()
    clock["t"] = 2029.0
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 1
    clock["t"] = 2030.5
    rc._poll_direct_unmatched_ttl()
    assert rc.betfair_service.fetches == 2


def test_trg_628_b_1057(monkeypatch, caplog):
    """ID: TRG-628-B-1057 / PF-B1-R2-TTL-FETCH-FAIL

    Fetch degli ordini correnti fallito => il giro abortisce con warning, senza
    selezionare candidati ne' cancellare; registry intatto.
    """
    selected = []
    monkeypatch.setattr(rc_mod, "select_expired_unmatched", lambda **kw: selected.append(kw) or [])
    sim = _Client("sim")
    rc = _make_rc(orders_raises=True, sim=sim)
    _track(rc, "C1", "B1")
    with caplog.at_level(logging.WARNING, logger=rc_mod.logger.name):
        rc._poll_direct_unmatched_ttl()
    assert selected == []
    assert sim.calls == []
    assert rc.direct_unmatched_bet_ids == {"B1"}
    assert _names(rc) == []
    assert any("list_current_orders fallita" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)


def test_trg_628_b_1059():
    """ID: TRG-628-B-1059 / PF-B1-R2-TTL-NORETRY

    Il bet_id esce dal registry PRIMA della chiamata di cancel al broker; esito
    pubblicato come CANCELLED (confermato) o CANCEL_FAILED; nessun secondo
    tentativo.
    """
    seen = {}
    rc_holder = {}
    sim = _Client("sim", on_call=lambda: seen.setdefault("reg", set(rc_holder["rc"].direct_unmatched_bet_ids)))
    rc = _make_rc(orders=[_order("B1", market_id="1.5")], sim=sim)
    rc_holder["rc"] = rc
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert seen["reg"] == set()                          # gia' rimosso al momento del cancel
    assert _names(rc) == ["DIRECT_UNMATCHED_CANCELLED"]

    bad = _Client("sim", ok=False)
    rc = _make_rc(orders=[_order("B2")], sim=bad)
    _track(rc, "C2", "B2")
    rc._poll_direct_unmatched_ttl()
    rc._last_direct_ttl_poll_at = float("-inf")
    rc._poll_direct_unmatched_ttl()
    assert _names(rc) == ["DIRECT_UNMATCHED_CANCEL_FAILED"]
    assert len(bad.calls) == 1                           # NO RETRY
    assert rc.direct_unmatched_bet_ids == set()


def test_trg_628_b_1060():
    """ID: TRG-628-B-1060 / PF-B1-R2-TTL-CLIENT-MODE

    L'adapter di cancel risolve SIM/LIVE in modo lazy al momento del cancel:
    un cambio di modalita' dopo la costruzione dell'adapter usa il client della
    modalita' corrente (LIVE: firma bet_ids; SIM: instructions).
    """
    sim, live = _Client("sim"), _Client("live")
    rc = _make_rc(sim=sim, live=live)
    _track(rc, "C1", "B1")
    _track(rc, "C2", "B2")
    adapter = rc._direct_ttl_cancel_adapter()            # costruito in SIM
    rc.simulation_mode = False
    rc._cancel_direct_unmatched(adapter, {"market_id": "1.1", "bet_id": "B1"})
    assert live.calls == [("1.1", ("B1",), "bet_ids")] and sim.calls == []
    rc.simulation_mode = True
    rc._cancel_direct_unmatched(adapter, {"market_id": "1.2", "bet_id": "B2"})
    assert sim.calls == [("1.2", ("B2",), "instructions")]
    assert len(live.calls) == 1
