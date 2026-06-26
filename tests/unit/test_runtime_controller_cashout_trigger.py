"""Trigger cashout in RuntimeController._on_signal_received (Fase 2.1-B2.4b-2).

Usa istanze bare (``object.__new__``) per evitare il costruttore pesante:
si testano solo il gating del cashout e ``_route_cashout_signal``.
"""

import threading

from core.runtime_controller import RuntimeController


class _SyncBus:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload=None):
        self.published.append((topic, payload))

    def topics(self):
        return [t for t, _ in self.published]


class _Cfg:
    commission_pct = 4.5


class _Svc:
    """BetfairService fake: nessuna posizione bot, book vuoto."""

    def __init__(self):
        self.current_orders_raises = False

    def is_simulation_mode(self):
        return True

    def list_current_orders(self, market_ids=None):
        if self.current_orders_raises:
            raise RuntimeError("session invalid")
        return []

    def get_market_book_snapshot(self, market_id):
        return None

    def get_live_client(self):
        return None

    def get_simulation_broker(self):
        return None


class _DB:
    def __init__(self, bot_orders=None, raises=False):
        self._bot_orders = bot_orders or []
        self._raises = raises

    def get_bot_active_orders(self):
        if self._raises:
            raise RuntimeError("db down")
        return list(self._bot_orders)


def _bare_controller(bus, svc, db):
    rc = object.__new__(RuntimeController)
    rc.bus = bus
    rc.betfair_service = svc
    rc.db = db
    rc.config = _Cfg()
    return rc


# =========================================================
# _route_cashout_signal — pubblica solo via router, mai CMD diretto
# =========================================================
def test_route_with_no_positions_publishes_nothing():
    bus = _SyncBus()
    rc = _bare_controller(bus, _Svc(), _DB(bot_orders=[]))
    rc._route_cashout_signal({"signal_type": "CASHOUT_ALL", "event_name": ""})
    # Nessuna posizione del bot => nessun REQ, e MAI un CMD diretto dal runtime.
    assert "CMD_EXECUTE_CASHOUT" not in bus.topics()
    assert "REQ_EXECUTE_CASHOUT" not in bus.topics()


def test_route_db_error_is_failclosed_cashout_failed():
    bus = _SyncBus()
    rc = _bare_controller(bus, _Svc(), _DB(raises=True))
    rc._route_cashout_signal({"signal_type": "CASHOUT_ALL", "event_name": ""})
    # Errore di lettura ordini bot => CASHOUT_FAILED (router o wrapper), mai CMD.
    assert "CASHOUT_FAILED" in bus.topics()
    assert "CMD_EXECUTE_CASHOUT" not in bus.topics()


def test_route_never_emits_cmd_directly():
    bus = _SyncBus()
    rc = _bare_controller(bus, _Svc(), _DB(bot_orders=[]))
    rc._route_cashout_signal({"signal_type": "CASHOUT", "event_name": "A v B"})
    assert "CMD_EXECUTE_CASHOUT" not in bus.topics()


# =========================================================
# _on_signal_received — cashout instradato PRIMA del required-check
# =========================================================
def _bare_for_on_signal(bus, *, signal_type_router):
    rc = object.__new__(RuntimeController)
    rc.bus = bus
    rc.last_signal_at = ""
    rc._daily_loss_stop_lock = threading.Lock()
    rc._emergency_stopped = False
    rc._daily_loss_pending_stop = False
    rc._emergency_stopped_at = ""
    rc.execution_mode = "SIMULATION"
    rc._runtime_active = lambda: True
    rc._routed = []
    rc._rejected = []
    rc._route_cashout_signal = lambda sig: rc._routed.append(sig)
    rc._reject_signal = lambda sig, reason: rc._rejected.append((reason, sig))
    return rc


def test_cashout_all_routed_before_required_field_check():
    # CASHOUT_ALL non ha market_id/selection_id: senza il branch verrebbe
    # scartato come campi_mancanti. Deve invece essere instradato.
    bus = _SyncBus()
    rc = _bare_for_on_signal(bus, signal_type_router=True)
    rc._on_signal_received({"signal_type": "CASHOUT_ALL"})
    assert len(rc._routed) == 1
    assert rc._routed[0]["signal_type"] == "CASHOUT_ALL"
    assert rc._rejected == []  # mai campi_mancanti


def test_single_cashout_routed():
    bus = _SyncBus()
    rc = _bare_for_on_signal(bus, signal_type_router=True)
    rc._on_signal_received({"signal_type": "CASHOUT", "event_name": "A v B"})
    assert len(rc._routed) == 1
    assert rc._rejected == []


def test_emergency_stop_blocks_cashout_upstream():
    # E1: l'emergency-stop a monte rifiuta il cashout senza instradarlo.
    bus = _SyncBus()
    rc = _bare_for_on_signal(bus, signal_type_router=True)
    rc._emergency_stopped = True
    rc._on_signal_received({"signal_type": "CASHOUT_ALL"})
    assert rc._routed == []
    assert rc._rejected and "emergency_stop_active" in rc._rejected[0][0]


def test_runtime_inactive_blocks_cashout_upstream():
    bus = _SyncBus()
    rc = _bare_for_on_signal(bus, signal_type_router=True)
    rc._runtime_active = lambda: False

    class _Mode:
        value = "STOPPED"
    rc.mode = _Mode()
    rc._on_signal_received({"signal_type": "CASHOUT_ALL"})
    assert rc._routed == []
    assert rc._rejected and "runtime_non_attivo" in rc._rejected[0][0]


def test_none_broker_guard_cancel_returns_false():
    # I lambda live/sim cancel del wiring devono ritornare False (non crashare)
    # se get_live_client/get_simulation_broker tornano None.
    from cashout_cancel_adapter import CashoutCancelAdapter

    class _NoneSvc:
        def is_simulation_mode(self):
            return False  # LIVE => usa live_cancel

        def get_live_client(self):
            return None

        def get_simulation_broker(self):
            return None

    svc = _NoneSvc()
    adapter = CashoutCancelAdapter(
        is_simulation=svc.is_simulation_mode,
        live_cancel=lambda **kw: (c.cancel_orders(**kw) if (c := svc.get_live_client()) is not None else False),
        sim_cancel=lambda **kw: (b.cancel_orders(**kw) if (b := svc.get_simulation_broker()) is not None else False),
    )
    assert adapter.cancel("1.2", ["B1"]) is False  # nessun crash, cancel non confermato


def test_non_cashout_signal_not_routed_as_cashout():
    # Un segnale normale non deve finire nel branch cashout.
    bus = _SyncBus()
    rc = _bare_for_on_signal(bus, signal_type_router=True)
    # market_id/selection_id presenti: prosegue oltre il branch cashout senza
    # essere instradato come cashout (poi continuerebbe il flusso normale, che
    # qui non testiamo: basta che NON sia stato instradato al router cashout).
    try:
        rc._on_signal_received({"market_id": "1.1", "selection_id": 5})
    except Exception:
        pass  # il flusso normale a valle tocca attributi non settati: irrilevante qui
    assert rc._routed == []
