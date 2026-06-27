"""Proof B6.2 — wiring best price DIRECT in RuntimeController (flag default-OFF).

Verifica il seam `_apply_direct_best_price`, invocato in `_on_signal_received`
DOPO il recheck daily-loss e PRIMA di `CMD_QUICK_BET`:

- flag assente/False (default) => no-op assoluto: nessun fetch del book,
  nessuna mutazione del payload (comportamento byte-identico a oggi);
- flag ON + best price live entro tolleranza => override col best difensivo;
- flag ON + anomalia (mercato sospeso / no liquidity / oltre tolleranza /
  book assente) => fallback al master price (l'estrattore puro e' fail-closed);
- flag ON + snapshot che SOLLEVA => master price invariato, nessun crash.

Test pure-method su istanza con servizi finti (nessun lifecycle/thread/IO).
`core/runtime_controller.py` e' file critico: qui SOLO test, nessuna modifica
al codice di produzione.
"""
from __future__ import annotations

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import DeskMode, RuntimeMode

# Il job CI "unit" gira `-m unit`: senza marker questi test verrebbero deselezionati.
pytestmark = pytest.mark.unit


class _Config:
    """Config Roserpina minima; default inerti per gli attributi non noti."""

    table_count = 1
    anti_duplication_enabled = False
    allow_recovery = False
    auto_reset_drawdown_pct = 90
    defense_drawdown_pct = 7.5
    lockdown_drawdown_pct = 95
    # NB: use_best_price_direct NON definito di proposito => getattr default False.
    # Tolleranza come attributo REALE: il catch-all __getattr__ qui sotto
    # ritorna 0 per gli attributi ignoti (e 0 NON solleva AttributeError, quindi
    # il default di getattr non si applica). In produzione la config Roserpina e'
    # una dataclass senza catch-all, dove `getattr(cfg, ..., 2.0)` rende 2.0.
    best_price_max_deviation_pct = 2.0

    def __getattr__(self, _name):
        return 0


class _Settings:
    @staticmethod
    def load_roserpina_config():
        return _Config()


class _Bus:
    def __init__(self):
        self.published = []

    @staticmethod
    def subscribe(*_args, **_kwargs):
        return None

    def publish(self, event, payload=None):
        self.published.append((event, payload or {}))


class _Db:
    @staticmethod
    def _execute(*_args, **_kwargs):
        return None

    @staticmethod
    def get_pending_sagas():
        return []


class _Betfair:
    """BetfairService finto con get_market_book_snapshot pilotabile."""

    def __init__(self, book=None, raises=False):
        self._book = book
        self._raises = raises
        self.snapshot_calls = []
        self._session_invalid = False

    def get_market_book_snapshot(self, market_id):
        self.snapshot_calls.append(market_id)
        if self._raises:
            raise RuntimeError("snapshot boom")
        return self._book

    @staticmethod
    def set_simulation_mode(*_args, **_kwargs):
        return None

    @staticmethod
    def get_live_client():
        return None

    @staticmethod
    def connect(**_kwargs):
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


def _make_rc(betfair=None):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=betfair or _Betfair(),
        telegram_service=_Telegram(),
    )
    return rc, bus


def _payload(price=2.50, market_id="1.234", selection_id=55, bet_type="BACK"):
    return {
        "market_id": market_id,
        "selection_id": selection_id,
        "bet_type": bet_type,
        "price": price,
    }


def _book(*, status="OPEN", selection_id=55, runner_status="ACTIVE", backs=None, lays=None):
    ex = {}
    if backs is not None:
        ex["availableToBack"] = [dict(lv, size=lv.get("size", 100.0)) for lv in backs]
    if lays is not None:
        ex["availableToLay"] = [dict(lv, size=lv.get("size", 100.0)) for lv in lays]
    return {
        "status": status,
        "runners": [{"selectionId": selection_id, "status": runner_status, "ex": ex}],
    }


# =========================================================
# Flag OFF (default) => no-op assoluto
# =========================================================
def test_flag_off_is_total_noop():
    bf = _Betfair(book=_book(backs=[{"price": 2.52}]))
    rc, _ = _make_rc(bf)
    payload = _payload(price=2.50)
    # Ritorna False => il chiamante NON riesegue i gate pre-submit (no-op).
    assert rc._apply_direct_best_price(payload) is False
    # Nessun fetch del book, payload immutato, nessun campo audit aggiunto.
    assert bf.snapshot_calls == []
    assert payload == _payload(price=2.50)
    assert "best_price_source" not in payload
    assert "best_price_reason" not in payload


# =========================================================
# Flag ON => override col best difensivo entro tolleranza
# =========================================================
def test_flag_on_overrides_with_live_best_within_tolerance():
    bf = _Betfair(book=_book(backs=[{"price": 2.52}], lays=[{"price": 2.60}]))
    rc, _ = _make_rc(bf)
    rc.config.use_best_price_direct = True
    payload = _payload(price=2.50, bet_type="BACK")
    # Ritorna True => snapshot tentato, il chiamante DEVE rieseguire i gate.
    assert rc._apply_direct_best_price(payload) is True
    assert bf.snapshot_calls == ["1.234"]
    assert payload["price"] == 2.52
    assert payload["best_price_source"] == "LIVE_BOOK_DIRECT"
    assert payload["best_price_reason"] == "ok"


def test_flag_on_lay_does_not_cross_spread():
    # LAY con solo liquidita' BACK => fallback master, mai il prezzo BACK.
    bf = _Betfair(book=_book(backs=[{"price": 2.40}], lays=[]))
    rc, _ = _make_rc(bf)
    rc.config.use_best_price_direct = True
    payload = _payload(price=2.50, bet_type="LAY")
    rc._apply_direct_best_price(payload)
    assert payload["price"] == 2.50
    assert payload["best_price_source"] == "FALLBACK_MASTER"
    assert payload["best_price_reason"] == "no_side_liquidity"


# =========================================================
# Flag ON + anomalie => fallback master (fail-closed)
# =========================================================
def test_flag_on_market_suspended_falls_back_to_master():
    bf = _Betfair(book=_book(status="SUSPENDED", backs=[{"price": 2.52}]))
    rc, _ = _make_rc(bf)
    rc.config.use_best_price_direct = True
    payload = _payload(price=2.50)
    rc._apply_direct_best_price(payload)
    assert payload["price"] == 2.50
    assert payload["best_price_source"] == "FALLBACK_MASTER"
    assert payload["best_price_reason"] == "market_not_open"


def test_flag_on_out_of_tolerance_falls_back_to_master():
    bf = _Betfair(book=_book(backs=[{"price": 3.00}]))  # +20% vs 2.50
    rc, _ = _make_rc(bf)
    rc.config.use_best_price_direct = True
    payload = _payload(price=2.50)
    rc._apply_direct_best_price(payload)
    assert payload["price"] == 2.50
    assert payload["best_price_source"] == "FALLBACK_MASTER"
    assert payload["best_price_reason"] == "out_of_tolerance"


def test_flag_on_book_absent_falls_back_to_master():
    bf = _Betfair(book=None)
    rc, _ = _make_rc(bf)
    rc.config.use_best_price_direct = True
    payload = _payload(price=2.50)
    rc._apply_direct_best_price(payload)
    assert payload["price"] == 2.50
    assert payload["best_price_source"] == "FALLBACK_MASTER"
    assert payload["best_price_reason"] == "no_book"


# =========================================================
# Flag ON + snapshot che solleva => master invariato, nessun crash
# =========================================================
def test_flag_on_snapshot_raises_keeps_master_no_crash():
    bf = _Betfair(raises=True)
    rc, _ = _make_rc(bf)
    rc.config.use_best_price_direct = True
    payload = _payload(price=2.50)
    # Anche su eccezione ritorna True: lo snapshot e' stato tentato => il
    # chiamante DEVE rieseguire i gate (il fetch puo' aver invalidato la sessione).
    assert rc._apply_direct_best_price(payload) is True
    assert payload["price"] == 2.50
    # L'override non e' avvenuto: nessun campo audit (eccezione prima dello stamp).
    assert "best_price_source" not in payload


# =========================================================
# Seam in _on_signal_received: lo snapshot va PRIMA dei gate finali
# pre-submit, che vengono RIESEGUITI dopo di esso (Codex P2 #1/#3).
# =========================================================
class _Decision:
    approved = True
    table_id = 1
    recommended_stake = 10.0
    desk_mode = DeskMode.NORMAL
    reason = "ok"
    metadata = {}


def _make_active_rc(betfair=None, *, live=False):
    """rc ACTIVE con allocazione/MM upstream bypassate: isola il seam pre-submit."""
    rc, bus = _make_rc(betfair)
    rc.mode = RuntimeMode.ACTIVE
    rc._emergency_stopped = False
    rc.execution_mode = "LIVE" if live else "SIMULATION"
    if live:
        rc.live_enabled = True
        rc.live_readiness_ok = True
        rc.enforce_deploy_gate = lambda **_k: {"allowed": True, "reason": "ok", "reasons": []}
    # Bypassa i gate upstream (MM/tavolo): vogliamo testare SOLO il seam finale.
    rc.mm.calculate = lambda **_k: _Decision()
    rc.table_manager.allocate = lambda **_k: object()
    rc.table_manager.activate = lambda **_k: None
    rc.table_manager.total_exposure = lambda: 0.0
    rc.table_manager.force_unlock = lambda *_a, **_k: None
    rc._event_current_exposure = lambda _ek: 0.0
    rc._daily_loss_entry_blocked = lambda: False
    return rc, bus


def _signal(price=2.50, market_id="1.234", selection_id=55, bet_type="BACK"):
    return {
        "market_id": market_id,
        "selection_id": selection_id,
        "bet_type": bet_type,
        "price": price,
        "stake": 10.0,
    }


def _events(bus, name):
    return [p for e, p in bus.published if e == name]


def test_seam_flag_off_publishes_master_unchanged():
    # Flag OFF: nessuno snapshot, nessun re-gate nuovo => CMD_QUICK_BET col master.
    bf = _Betfair(book=_book(backs=[{"price": 2.52}]))
    rc, bus = _make_active_rc(bf)
    rc._on_signal_received(_signal(price=2.50))
    cmds = _events(bus, "CMD_QUICK_BET")
    assert len(cmds) == 1
    assert cmds[0]["price"] == 2.50
    assert "best_price_source" not in cmds[0]
    assert bf.snapshot_calls == []
    assert _events(bus, "SIGNAL_APPROVED")


def test_seam_flag_on_happy_publishes_override_after_gates():
    bf = _Betfair(book=_book(backs=[{"price": 2.52}]))
    rc, bus = _make_active_rc(bf)
    rc.config.use_best_price_direct = True
    rc._on_signal_received(_signal(price=2.50, bet_type="BACK"))
    cmds = _events(bus, "CMD_QUICK_BET")
    assert len(cmds) == 1
    assert cmds[0]["price"] == 2.52
    assert cmds[0]["best_price_source"] == "LIVE_BOOK_DIRECT"
    assert _events(bus, "SIGNAL_APPROVED")


def test_seam_daily_loss_armed_during_snapshot_blocks_submit():
    # Il daily-loss si arma DURANTE lo snapshot: il recheck (ora DOPO lo
    # snapshot) deve bloccare SIA SIGNAL_APPROVED SIA CMD_QUICK_BET.
    rc_holder = {}

    def snap(_mid):
        rc_holder["rc"]._daily_loss_entry_blocked = lambda: True
        return _book(backs=[{"price": 2.52}])

    bf = _Betfair()
    bf.get_market_book_snapshot = snap
    rc, bus = _make_active_rc(bf)
    rc_holder["rc"] = rc
    rc.config.use_best_price_direct = True
    rc._on_signal_received(_signal(price=2.50))
    assert _events(bus, "CMD_QUICK_BET") == []
    assert _events(bus, "SIGNAL_APPROVED") == []
    reasons = [p.get("reason") for p in _events(bus, "SIGNAL_REJECTED")]
    assert "emergency_stop_active:pre_submit_recheck" in reasons


def test_seam_session_invalidated_during_snapshot_blocks_submit_live():
    # LIVE: lo snapshot intercetta SESSION_EXPIRED e marca la sessione invalida.
    # Il session re-guard (ora DOPO lo snapshot) deve bloccare il submit.
    def snap(_mid):
        bf._session_invalid = True   # come get_market_book_snapshot dopo recovery
        return None

    bf = _Betfair()
    bf.get_market_book_snapshot = snap
    rc, bus = _make_active_rc(bf, live=True)
    rc.config.use_best_price_direct = True
    rc._on_signal_received(_signal(price=2.50))
    assert _events(bus, "CMD_QUICK_BET") == []
    assert _events(bus, "SIGNAL_APPROVED") == []
    reasons = [p.get("reason") for p in _events(bus, "SIGNAL_REJECTED")]
    assert "session_invalid_live_blocked:pre_submit_recheck" in reasons
