"""PR26-a (#461) — customer_ref stabile per i produttori d'ordine esistenti.

Prima di questa PR il RuntimeController (segnale e auto-trade), le gambe del
dutching e ``manual_bet`` pubblicavano ``CMD_QUICK_BET`` SENZA ``customer_ref``:
il TradingEngine lo esige in normalizzazione (``CUSTOMER_REF_REQUIRED``) e
rifiutava ogni ordine come ``INVALID_REQUEST``. Il RiskMiddleware, nell'inoltro
REQ_QUICK_BET -> CMD_QUICK_BET, scartava il ``customer_ref`` del chiamante.

La correzione sta nei PRODUTTORI, non nell'engine:

- PASS: ogni intento logico arriva all'engine con un ref Betfair-conforme;
  la stessa intenzione (riconsegna anche dopo restart con received_at nuovo,
  stake MM diverso, ricalcolo del batch) produce lo STESSO ref e l'engine
  blocca il doppione; intenti diversi (contenuti/chat, settlement, gambe,
  click manuali) hanno ref diversi; un ref a monte si preserva (reso
  conforme se Betfair lo scarterebbe); il ref della gamba arriva nel
  registro durevole del batch.
- BLOCK: l'engine rifiuta ancora un ordine senza ref; l'inoltro del middleware
  non inventa un ref; il fallback compat REQ_QUICK_BET di Telegram (senza gate
  runtime) resta rifiutato.

Catena reale: bus sincrono -> RuntimeController -> TradingEngine -> Database
SQLite su disco -> SimulationBroker (risolto come in headless_main, via
``betfair_service.get_client`` in SIMULATION).
"""
from __future__ import annotations

import re
from types import SimpleNamespace

import pytest

from controllers.dutching_controller import DutchingController
from core.dutching_batch_manager import DutchingBatchManager
from core.risk_middleware import RiskMiddleware
from core.runtime_controller import RuntimeController
from core.system_state import DeskMode, RoserpinaConfig, RuntimeMode
from core.trading_engine import TradingEngine
from database import Database
from simulation_broker import SimulationBroker

pytestmark = pytest.mark.integration

# Vincolo Betfair su customerRef (stesso di BetfairClient._normalize_customer_ref).
BETFAIR_CUSTOMER_REF = re.compile(r"[A-Za-z0-9\-._+*:;~]{1,32}")


# =========================================================================
# Harness
# =========================================================================
class _SyncBus:
    """Bus sincrono: stesso routing topic -> subscriber dell'EventBus reale."""

    def __init__(self):
        self.subscribers = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscribers.setdefault(topic, []).append(handler)

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload) if isinstance(payload, dict) else payload))
        for handler in list(self.subscribers.get(topic, [])):
            handler(payload)

    def payloads(self, topic):
        return [p for t, p in self.events if t == topic]


class _Settings:
    def __init__(self, anti_duplication=False):
        self._anti_duplication = anti_duplication

    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = self._anti_duplication
        return cfg

    def load_market_data_config(self):
        return {
            "market_data_mode": "poll",
            "enabled": False,
            "market_ids": [],
            "snapshot_fallback_enabled": True,
            "snapshot_fallback_interval_sec": 1,
        }


class _BetfairService:
    """Come BetfairService in SIMULATION: get_client() -> simulation_broker."""

    def __init__(self, broker):
        self.simulation_broker = broker

    def get_client(self):
        return self.simulation_broker

    def set_simulation_mode(self, _enabled):
        return None

    def get_account_funds(self):
        return {"available": 1000.0, "ok": True}

    def status(self):
        return {"connected": True}

    def get_live_client(self):
        return None

    def get_market_book_snapshot(self, _market_id):
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


class _Executor:
    def submit(self, _name, fn, *args, **kwargs):
        return fn(*args, **kwargs)


def _decision(stake):
    def _calculate(**kwargs):
        table = kwargs.get("table")
        return SimpleNamespace(
            approved=True,
            recommended_stake=float(stake),
            table_id=int(getattr(table, "table_id", 1)),
            reason="ok",
            desk_mode=DeskMode.NORMAL,
            metadata={},
        )

    return _calculate


def _chain(tmp_path, *, stake=4.0, anti_duplication=False, db_name="pr26a.db"):
    bus = _SyncBus()
    broker = SimulationBroker(starting_balance=1000.0)
    betfair = _BetfairService(broker)
    db = Database(str(tmp_path / db_name))
    engine = TradingEngine(
        bus=bus, db=db, client_getter=betfair.get_client, executor=_Executor()
    )
    rc = RuntimeController(
        bus=bus,
        db=db,
        settings_service=_Settings(anti_duplication),
        betfair_service=betfair,
        telegram_service=_Telegram(),
        trading_engine=engine,
    )
    engine.runtime_controller = rc
    rc.mode = RuntimeMode.ACTIVE
    rc.mm.calculate = _decision(stake)
    return SimpleNamespace(bus=bus, broker=broker, engine=engine, rc=rc, db=db)


def _broker_refs(broker):
    return [o.customer_ref for o in broker.state.orders.values()]


def _signal(**overrides):
    signal = {
        "market_id": "1.234",
        "selection_id": 11,
        "price": 2.0,
        "bet_type": "BACK",
        "simulation_mode": True,
        "chat_id": -100123,
        "received_at": "2026-10-08T10:00:00.123456+00:00",
        "event": "Alpha v Beta",
        "market": "Over/Under 2.5",
        "selection": "Over 2.5",
    }
    signal.update(overrides)
    return signal


def _failed_reasons(bus):
    return [p for p in bus.payloads("QUICK_BET_FAILED") if isinstance(p, dict)]


# =========================================================================
# Produttore 1 — RuntimeController._on_signal_received (Telegram/UI)
# =========================================================================
def test_pass_signal_order_reaches_sim_broker_with_compliant_customer_ref(tmp_path):
    c = _chain(tmp_path)

    c.rc._on_signal_received(_signal())

    cmds = c.bus.payloads("CMD_QUICK_BET")
    assert len(cmds) == 1
    ref = cmds[0].get("customer_ref")
    assert ref, "il runtime pubblica CMD_QUICK_BET senza customer_ref"
    assert BETFAIR_CUSTOMER_REF.fullmatch(ref), ref
    assert _failed_reasons(c.bus) == []
    assert _broker_refs(c.broker) == [ref]


def test_pass_signal_redelivery_keeps_identity_and_engine_blocks_duplicate(tmp_path):
    # anti-duplicazione runtime spenta: e' l'engine, col customer_ref stabile,
    # a fermare la riconsegna dello STESSO messaggio.
    c = _chain(tmp_path, anti_duplication=False)
    signal = _signal()

    c.rc._on_signal_received(dict(signal))
    c.rc._on_signal_received(dict(signal))

    refs = [p.get("customer_ref") for p in c.bus.payloads("CMD_QUICK_BET")]
    assert len(refs) == 2
    assert refs[0] and refs[0] == refs[1]
    assert _broker_refs(c.broker) == [refs[0]], "la riconsegna ha piazzato un secondo ordine"


def test_pass_redelivery_after_restart_with_new_received_at_is_blocked(tmp_path):
    # Bot API / Telethon possono riconsegnare lo stesso messaggio dopo un
    # restart: il listener rigenera received_at. Il ref NON deve cambiare.
    first = _chain(tmp_path, db_name="shared.db")
    first.rc._on_signal_received(_signal(received_at="2026-10-08T10:00:00.100000+00:00"))
    ref = first.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref")
    assert _broker_refs(first.broker) == [ref]

    # Restart: processo nuovo, stesso DB e stesso broker (stesso conto).
    second = _chain(tmp_path, db_name="shared.db")
    second.rc.betfair_service.simulation_broker = first.broker
    second.rc._on_signal_received(_signal(received_at="2026-10-08T10:03:00.900000+00:00"))

    assert second.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref") == ref
    assert _broker_refs(first.broker) == [ref], "la riconsegna post-restart ha piazzato un secondo ordine"


def _gui_publish(c, listener_signal, *, stake):
    """Percorso mini-GUI reale: TelegramModule normalizza il segnale del
    listener, costruisce il payload DIRECT, ci copia ``raw_signal`` (con il
    ``received_at`` di consegna annidato) e lo instrada al gate runtime."""
    from telegram_module import TelegramModule
    from telegram_sanitizer import sanitize_telegram_payload

    class _Gui(TelegramModule):
        def __init__(self, bus):
            self.bus = bus
            self.simulation_mode = True

    gui = _Gui(c.bus)
    # Come _handle_telegram_signal: normalizza, poi risolve il normalizzato e
    # gli copia sopra raw_signal = normalizzato (che a sua volta annida il
    # segnale del listener in raw_signal).
    normalized = gui._get_signal_processor().normalize_ingestion_signal(dict(listener_signal))
    signal_data = dict(normalized.get("normalized_signal") or {})
    payload, mode = gui._resolve_signal_to_payload(signal_data, stake=stake)
    assert mode == "DIRECT" and payload
    payload["raw_signal"] = sanitize_telegram_payload(dict(signal_data))
    payload["resolution_mode"] = mode
    assert gui._publish_order_signal(payload) == "SIGNAL_RECEIVED"


def test_pass_gui_redelivery_with_nested_received_at_is_blocked(tmp_path):
    # Codex P1 (head 9eb6c7f): sul percorso mini-GUI il received_at di
    # consegna sopravvive annidato in raw_signal. Riconsegna post-restart con
    # received_at nuovo (e stake GUI cambiato) = stesso intento = stesso ref.
    listener = {
        "market_id": "1.234", "selection_id": 11, "price": 2.0,
        "bet_type": "BACK", "chat_id": -100123, "raw_text": "OVER 2.5 @2.0",
        "event": "Alpha v Beta", "market": "Over/Under 2.5", "selection": "Over 2.5",
    }
    first = _chain(tmp_path, db_name="gui.db")
    _gui_publish(first, {**listener, "received_at": "2026-10-08T10:00:00.1+00:00"}, stake=3.0)
    ref = first.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref")
    assert ref and _broker_refs(first.broker) == [ref]

    second = _chain(tmp_path, db_name="gui.db")
    second.rc.betfair_service.simulation_broker = first.broker
    _gui_publish(second, {**listener, "received_at": "2026-10-08T10:02:30.9+00:00"}, stake=5.0)

    assert second.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref") == ref
    assert _broker_refs(first.broker) == [ref], "riconsegna GUI post-restart: secondo ordine piazzato"

    # Messaggio diverso nella stessa chat = intento diverso.
    _gui_publish(second, {**listener, "selection_id": 12, "selection": "Under 2.5",
                          "raw_text": "UNDER 2.5 @2.0", "received_at": "2026-10-08T10:03:00+00:00"}, stake=5.0)
    other = second.bus.payloads("CMD_QUICK_BET")[1].get("customer_ref")
    assert other and other != ref


def test_pass_same_message_same_identity_across_headless_and_gui_entrypoints(tmp_path):
    # Codex P1 (head 7c5a921): headless (TelegramService pubblica il dict del
    # listener) e mini-GUI (raw_signal annidato) devono dare lo STESSO ref allo
    # stesso messaggio, anche passando da un entrypoint all'altro con restart.
    from services.telegram_service import TelegramService

    listener = {
        "market_id": "1.234", "selection_id": 11, "price": 2.0,
        "bet_type": "BACK", "chat_id": -100123, "raw_text": "OVER 2.5 @2.0",
        "event": "Alpha v Beta", "market": "Over/Under 2.5", "selection": "Over 2.5",
        "simulation_mode": True,
    }
    headless = _chain(tmp_path, db_name="entry.db")
    service = TelegramService(settings_service=None, db=headless.db, bus=headless.bus)
    service._handle_signal({**listener, "received_at": "2026-10-08T10:00:00.1+00:00"})
    ref = headless.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref")
    assert ref and _broker_refs(headless.broker) == [ref]

    gui = _chain(tmp_path, db_name="entry.db")
    gui.rc.betfair_service.simulation_broker = headless.broker
    _gui_publish(gui, {**listener, "received_at": "2026-10-08T10:01:00.2+00:00"}, stake=5.0)

    assert gui.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref") == ref
    assert _broker_refs(headless.broker) == [ref], "cambio entrypoint: secondo ordine piazzato"


def test_pass_distinct_signals_get_distinct_identities(tmp_path):
    c = _chain(tmp_path)

    c.rc._on_signal_received(_signal())
    c.rc._on_signal_received(_signal(selection_id=12, selection="Under 2.5"))
    c.rc._on_signal_received(_signal(chat_id=-100999))
    c.rc._on_signal_received(_signal(price=2.2))

    refs = [p.get("customer_ref") for p in c.bus.payloads("CMD_QUICK_BET")]
    assert len(refs) == 4 and all(refs)
    assert len(set(refs)) == 4
    assert sorted(_broker_refs(c.broker)) == sorted(refs)


def test_pass_sim_and_live_runs_of_same_message_keep_distinct_identities(tmp_path):
    # Il messaggio e' lo stesso, ma il modo effettivo dell'ordine resta
    # identita': un ordine SIM non deve mai far scartare quello LIVE.
    c = _chain(tmp_path)
    c.rc._on_signal_received(_signal())
    c.rc._on_signal_received(_signal(simulation_mode=False))
    published = c.bus.payloads("CMD_QUICK_BET")
    assert [p["simulation_mode"] for p in published] == [True, False]
    assert published[0]["customer_ref"] != published[1]["customer_ref"]


def test_pass_signal_identity_survives_restart_and_ignores_mm_stake(tmp_path):
    first = _chain(tmp_path, stake=4.0, db_name="a.db")
    first.rc._on_signal_received(_signal())
    # "Restart": nuovo processo, nuovo RuntimeController/engine, stake MM diverso.
    second = _chain(tmp_path, stake=7.5, db_name="b.db")
    second.rc._on_signal_received(_signal())

    ref_a = first.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref")
    ref_b = second.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref")
    assert ref_a and ref_a == ref_b
    assert first.bus.payloads("CMD_QUICK_BET")[0]["stake"] == 4.0
    assert second.bus.payloads("CMD_QUICK_BET")[0]["stake"] == 7.5


def test_pass_upstream_customer_ref_is_preserved(tmp_path):
    c = _chain(tmp_path)

    c.rc._on_signal_received(_signal(customer_ref="tg-777"))

    assert c.bus.payloads("CMD_QUICK_BET")[0].get("customer_ref") == "tg-777"
    assert _broker_refs(c.broker) == ["tg-777"]


def test_pass_noncompliant_upstream_ref_becomes_stable_compliant_ref(tmp_path):
    # Un ref a monte che Betfair scarterebbe (oltre 32 / spazi) non deve
    # togliere il de-dup lato Betfair: diventa un ref conforme e stabile.
    bad = "telegram message 4242 from master channel alpha"
    c = _chain(tmp_path)

    c.rc._on_signal_received(_signal(customer_ref=bad))
    c.rc._on_signal_received(_signal(customer_ref=bad, chat_id=-100555))

    refs = [p.get("customer_ref") for p in c.bus.payloads("CMD_QUICK_BET")]
    assert refs[0] != bad and BETFAIR_CUSTOMER_REF.fullmatch(refs[0])
    assert refs[0] == refs[1], "stesso ref a monte = stessa identita'"
    assert _broker_refs(c.broker) == [refs[0]]


# =========================================================================
# Produttore 2 — auto-trade dopo settlement (timer/ciclo MM)
# =========================================================================
def _close_payload(batch_id, **overrides):
    payload = {
        "event_key": "evt-pr26a",
        "table_id": 1,
        "batch_id": batch_id,
        # Stessa correlation per tutti: a distinguere gli intenti dev'essere il
        # settlement (settlement_key), non la sola correlation sorgente.
        "correlation_id": "corr-pr26a",
        "gross_pnl": 10.0,
        "commission_amount": 0.45,
        "net_pnl": 9.55,
        "commission_pct": 4.5,
        "settlement_source": "test_issue461_pr26a",
        "settlement_kind": "realized_settlement",
        "settlement_basis": "market_net_realized",
        "pnl": 9.55,
        "auto_trade_enabled": True,
        "cycle_executor_enabled": True,
        # DB reale con checkpoint: il submit dopo recovery va abilitato esplicito.
        "resume_submit_enabled": True,
        "mm_context": {
            "cycle_active": True,
            "cycle_id": "cycle-pr26a",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {
                "market_id": "1.555",
                "selection_id": 8,
                "price": 2.0,
                "simulation_mode": True,
                # Un template riusato fra step del ciclo NON deve dare l'identita'.
                "customer_ref": "template-ref",
            },
        },
    }
    payload.update(overrides)
    return payload


def _auto_trade_ref(c, batch_id):
    before = len(c.bus.payloads("CMD_QUICK_BET"))
    c.rc._on_close_position(_close_payload(batch_id))
    cmds = c.bus.payloads("CMD_QUICK_BET")[before:]
    assert len(cmds) == 1, c.rc._last_auto_trade_result
    return cmds[0].get("customer_ref")


def test_pass_auto_trade_order_identity_bound_to_settlement(tmp_path):
    c = _chain(tmp_path, db_name="at.db")
    c.rc.risk_desk.sync_bankroll(100.0)

    ref_1 = _auto_trade_ref(c, "batch-pr26a-1")
    ref_2 = _auto_trade_ref(c, "batch-pr26a-2")

    assert ref_1 and ref_2
    assert BETFAIR_CUSTOMER_REF.fullmatch(ref_1) and BETFAIR_CUSTOMER_REF.fullmatch(ref_2)
    assert ref_1 != ref_2, "due settlement diversi = due intenti diversi"
    assert "template-ref" not in (ref_1, ref_2)
    assert sorted(_broker_refs(c.broker)) == sorted([ref_1, ref_2])

    # Restart: stesso settlement rivalutato da un processo nuovo -> stesso ref.
    fresh = _chain(tmp_path, db_name="at-restart.db")
    fresh.rc.risk_desk.sync_bankroll(100.0)
    assert _auto_trade_ref(fresh, "batch-pr26a-1") == ref_1


# =========================================================================
# Produttore 3 — gambe del dutching (CMD_PLACE_DUTCHING -> submit_dutching)
# =========================================================================
class _DutchRuntime:
    class _Mode:
        value = "ACTIVE"

    class _Config:
        anti_duplication_enabled = True
        allow_recovery = True
        max_total_exposure_pct = 100.0
        max_event_exposure_pct = 100.0
        max_single_bet_pct = 100.0

    class _Guard:
        def is_duplicate(self, _key):
            return False

        def register(self, _key):
            return None

        def acquire(self, _key):
            return True

        def release(self, _key):
            return None

    class _Tables:
        def total_exposure(self):
            return 0.0

        def find_by_event_key(self, _key):
            return None

        def allocate(self, event_key=None, allow_recovery=True):
            return SimpleNamespace(table_id=7)

        def activate(self, **_kwargs):
            return None

        def force_unlock(self, _table_id):
            return None

    def __init__(self):
        self.mode = self._Mode()
        self.config = self._Config()
        self.risk_desk = SimpleNamespace(bankroll_current=1000.0)
        self.duplication_guard = self._Guard()
        self.table_manager = self._Tables()
        self.dutching_batch_manager = None


def _dutching_payload():
    return {
        "market_id": "1.700",
        "event_name": "Gamma v Delta",
        "market_name": "Match Odds",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 10, "price": 2.0, "side": "BACK"},
            {"selectionId": 20, "price": 3.0, "side": "BACK"},
            {"selectionId": 30, "price": 6.0, "side": "BACK"},
        ],
    }


def _patch_dutching(monkeypatch):
    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 10, "price": 2.0, "stake": 50.0, "side": "BACK"},
                {"selectionId": 20, "price": 3.0, "stake": 33.33, "side": "BACK"},
                {"selectionId": 30, "price": 6.0, "stake": 16.67, "side": "BACK"},
            ],
            1.0,
            100.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching", fake_calculate_dutching
    )


def test_pass_dutching_legs_distinct_stable_and_all_reach_engine(tmp_path, monkeypatch):
    _patch_dutching(monkeypatch)
    c = _chain(tmp_path, db_name="dutch.db")
    runtime = _DutchRuntime()
    runtime.dutching_batch_manager = DutchingBatchManager(c.db)

    result = DutchingController(bus=c.bus, runtime_controller=runtime).submit_dutching(
        _dutching_payload()
    )
    assert result["ok"] is True, result

    legs = c.bus.payloads("CMD_QUICK_BET")
    refs = [leg.get("customer_ref") for leg in legs]
    assert len(refs) == 3 and all(refs)
    assert all(BETFAIR_CUSTOMER_REF.fullmatch(r) for r in refs)
    assert len(set(refs)) == 3, "le gambe di uno stesso batch non sono doppioni"
    assert sorted(_broker_refs(c.broker)) == sorted(refs)
    # L'identita' arriva anche nel registro durevole delle gambe (reconcile).
    persisted = runtime.dutching_batch_manager.get_batch_legs(result["batch_id"])
    assert [leg["customer_ref"] for leg in sorted(persisted, key=lambda l: l["leg_index"])] == refs

    # Ricalcolo dello STESSO batch da un controller nuovo (restart: guard di
    # idempotenza in-memory vuoto): stessi ref -> l'engine blocca i doppioni.
    DutchingController(bus=c.bus, runtime_controller=_DutchRuntime()).submit_dutching(
        _dutching_payload()
    )
    replay_refs = [leg.get("customer_ref") for leg in c.bus.payloads("CMD_QUICK_BET")[3:]]
    assert replay_refs == refs
    assert len(c.broker.state.orders) == 3, "il ricalcolo ha piazzato gambe doppie"


def test_pass_dutching_leg_identity_ignores_leg_order(tmp_path, monkeypatch):
    # Codex P1 (head 7c5a921): lo stesso insieme di gambe ricalcolato dopo un
    # restart con le selezioni in ordine diverso e' lo stesso intento.
    stakes = {10: 50.0, 20: 33.33, 30: 16.67}

    def fake_calculate_dutching(selections, total_stake):
        _ = total_stake
        return (
            [{**sel, "stake": stakes[sel["selectionId"]]} for sel in selections],
            1.0,
            100.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching", fake_calculate_dutching
    )
    c = _chain(tmp_path, db_name="dutch_order.db")
    first = _dutching_payload()
    DutchingController(bus=c.bus, runtime_controller=_DutchRuntime()).submit_dutching(first)
    refs = {leg["selection_id"]: leg["customer_ref"] for leg in c.bus.payloads("CMD_QUICK_BET")}
    assert len(set(refs.values())) == 3 and all(refs.values())

    permuted = _dutching_payload()
    permuted["selections"] = [permuted["selections"][i] for i in (2, 0, 1)]
    DutchingController(bus=c.bus, runtime_controller=_DutchRuntime()).submit_dutching(permuted)
    replay = {leg["selection_id"]: leg["customer_ref"] for leg in c.bus.payloads("CMD_QUICK_BET")[3:]}

    assert replay == refs
    assert len(c.broker.state.orders) == 3, "ordine gambe diverso: batch piazzato due volte"


# =========================================================================
# Produttore 4 — DutchingController.manual_bet (oggi senza chiamanti)
# =========================================================================
def _manual_payload(**overrides):
    payload = {
        "market_id": "1.42",
        "selection_id": 5,
        "price": 2.0,
        "stake": 10.0,
        "bet_type": "BACK",
        "simulation_mode": True,
    }
    payload.update(overrides)
    return payload


def test_pass_manual_bet_each_call_is_a_new_intent_unless_upstream_ref():
    bus = _SyncBus()
    ctrl = DutchingController(bus=bus, runtime_controller=_DutchRuntime())

    assert ctrl.manual_bet(_manual_payload())["ok"] is True
    assert ctrl.manual_bet(_manual_payload())["ok"] is True
    assert ctrl.manual_bet(_manual_payload(customer_ref="ui-42"))["ok"] is True
    assert ctrl.manual_bet(_manual_payload(customer_ref="ui 42 con spazi"))["ok"] is True

    refs = [p.get("customer_ref") for p in bus.payloads("CMD_QUICK_BET")]
    assert all(BETFAIR_CUSTOMER_REF.fullmatch(r) for r in refs), refs
    # Due click identici senza ref a monte = due intenti distinti.
    assert refs[0] != refs[1]
    assert refs[2] == "ui-42"
    assert refs[3] != "ui 42 con spazi"


# =========================================================================
# Inoltro — RiskMiddleware REQ_QUICK_BET -> CMD_QUICK_BET
# =========================================================================
def test_pass_risk_middleware_forward_preserves_upstream_customer_ref():
    bus = _SyncBus()
    RiskMiddleware(bus, topics=("REQ_QUICK_BET",))

    bus.publish(
        "REQ_QUICK_BET",
        {"market_id": "1.9", "selection_id": 3, "price": 2.5, "stake": 2.0, "customer_ref": "ui-9"},
    )

    bus.publish(
        "REQ_QUICK_BET",
        {"market_id": "1.9", "selection_id": 4, "price": 2.5, "stake": 2.0,
         "customer_ref": "x" * 40},
    )

    cmds = bus.payloads("CMD_QUICK_BET")
    assert len(cmds) == 2
    assert cmds[0].get("customer_ref") == "ui-9"
    assert cmds[1].get("customer_ref") != "x" * 40
    assert BETFAIR_CUSTOMER_REF.fullmatch(cmds[1].get("customer_ref"))


def test_block_risk_middleware_forward_does_not_invent_customer_ref(tmp_path):
    c = _chain(tmp_path, db_name="mw.db")
    RiskMiddleware(c.bus, topics=("REQ_QUICK_BET",))
    # Isola l'inoltro: solo il CMD_QUICK_BET del middleware arriva all'engine.
    c.bus.subscribers["REQ_QUICK_BET"] = [
        h for h in c.bus.subscribers["REQ_QUICK_BET"]
        if getattr(h, "__self__", None) is not c.engine
    ]

    c.bus.publish("REQ_QUICK_BET", {"market_id": "1.9", "selection_id": 3, "price": 2.5, "stake": 2.0})

    cmds = c.bus.payloads("CMD_QUICK_BET")
    assert len(cmds) == 1
    assert not cmds[0].get("customer_ref")
    assert len(_failed_reasons(c.bus)) == 1
    assert c.broker.state.orders == {}


# =========================================================================
# BLOCK — l'engine resta fail-closed
# =========================================================================
def test_block_engine_still_rejects_missing_customer_ref(tmp_path):
    c = _chain(tmp_path, db_name="eng.db")

    result = c.engine.submit_quick_bet(
        {"market_id": "1.234", "selection_id": 11, "bet_type": "BACK", "price": 2.0,
         "stake": 4.0, "simulation_mode": True}
    )

    assert result["status"] == "FAILED"
    assert result["reason"] == "INVALID_REQUEST"
    assert "CUSTOMER_REF_REQUIRED" in str(result.get("error"))
    assert c.broker.state.orders == {}


def test_block_telegram_compat_fallback_without_runtime_gate_stays_rejected(tmp_path):
    from telegram_module import TelegramModule

    class _Harness(TelegramModule):
        def __init__(self, bus):
            self.bus = bus

    bus = _SyncBus()
    broker = SimulationBroker(starting_balance=1000.0)
    engine = TradingEngine(
        bus=bus, db=Database(str(tmp_path / "tg.db")),
        client_getter=lambda: broker, executor=_Executor(),
    )
    assert "SIGNAL_RECEIVED" not in bus.subscribers  # nessun gate runtime

    route = _Harness(bus)._publish_order_signal(
        {
            "telegram_boundary_stage": TelegramModule.TELEGRAM_BOUNDARY_STAGE,
            "market_id": "1.234", "selection_id": 11, "bet_type": "BACK",
            "price": 2.0, "stake": 4.0, "simulation_mode": True,
        }
    )

    assert route == "REQ_QUICK_BET"
    failed = _failed_reasons(bus)
    assert len(failed) == 1 and failed[0].get("customer_ref") == "UNKNOWN"
    assert broker.state.orders == {}
    assert engine is not None


# =========================================================================
# Helper — derivazione deterministica e conforme
# =========================================================================
def test_pass_helper_is_deterministic_compliant_and_preserves_upstream():
    from betfair_client import BetfairClient
    from core.order_identity import derive_customer_ref, resolve_customer_ref

    a = derive_customer_ref("sig", {"b": 2, "a": 1, "nested": {"y": [1, 2], "x": None}})
    b = derive_customer_ref("sig", {"nested": {"x": None, "y": [1, 2]}, "a": 1, "b": 2})
    assert a == b
    assert a.startswith("sig-") and len(a) <= 32
    assert BETFAIR_CUSTOMER_REF.fullmatch(a)
    assert BetfairClient._normalize_customer_ref(a) == a  # non viene omesso
    assert derive_customer_ref("sig", {"a": 1, "b": 3}) != a
    assert derive_customer_ref("dut", {"a": 1, "b": 2}) != derive_customer_ref("sig", {"a": 1, "b": 2})

    assert resolve_customer_ref("  up-1 ", "sig", {"a": 1}) == "up-1"
    assert resolve_customer_ref("", "sig", {"a": 1}) == derive_customer_ref("sig", {"a": 1})
    assert resolve_customer_ref(None, "sig", {"a": 1}) == derive_customer_ref("sig", {"a": 1})


def test_pass_helper_signal_material_ignores_delivery_and_derived_fields():
    from core.order_identity import derive_customer_ref, signal_identity_material

    def ref(signal):
        return derive_customer_ref("sig", signal_identity_material(signal))

    raw = {"raw_text": "OVER 2.5", "chat_id": -1, "received_at": "t1",
           "nested": [{"received_at": "t1", "k": 1}]}
    gui = {"raw_signal": raw, "price": 2.0, "stake": 3.0, "event_key": "e1"}
    # Riconsegna: received_at nuovo a ogni profondita', prezzo risolto e stake GUI diversi.
    redelivered = {"raw_signal": {**raw, "received_at": "t2",
                                  "nested": [{"received_at": "t2", "k": 1}]},
                   "price": 2.1, "stake": 5.0, "event_key": "e2"}
    assert ref(gui) == ref(redelivered)
    # Messaggio diverso o chat diversa = intento diverso.
    assert ref(gui) != ref({**gui, "raw_signal": {**raw, "raw_text": "UNDER 2.5"}})
    assert ref(gui) != ref({**gui, "raw_signal": {**raw, "chat_id": -2}})
    assert ref(gui) != ref({**gui, "raw_signal": {**raw, "nested": [{"k": 2}]}})
    # chat_id solo al primo livello (raw_signal senza chat) resta identita'.
    bare = {"raw_signal": {"raw_text": "OVER 2.5"}, "chat_id": -1}
    assert ref(bare) != ref({**bare, "chat_id": -2})
    # Stesso messaggio del listener nelle due forme reali: headless (dict del
    # listener + simulation_mode forzato da TelegramService) e mini-GUI
    # (payload -> normalizzato -> listener, campi derivati al primo livello).
    listener = {"raw_text": "OVER 2.5", "chat_id": -1, "price": 2.0, "received_at": "t1"}
    headless_form = {**listener, "simulation_mode": False}
    gui_form = {"price": 2.3, "stake": 9.0, "simulation_mode": True,
                "raw_signal": {"boundary_stage": "x", "price": 2.0,
                               "raw_signal": {**listener, "received_at": "t2"}}}
    assert signal_identity_material(headless_form) == signal_identity_material(gui_form)
    # Headless (senza raw_signal): il prezzo del messaggio resta identita'.
    headless = {"price": 2.0, "chat_id": -1, "received_at": "t1"}
    assert ref(headless) == ref({**headless, "received_at": "t2"})
    assert ref(headless) != ref({**headless, "price": 2.2})


def test_pass_helper_upstream_normalization_and_operation_refs():
    from betfair_client import BetfairClient
    from core.order_identity import (
        is_betfair_customer_ref,
        new_operation_customer_ref,
        normalize_upstream_customer_ref,
    )

    samples = ["ok-1", "A.b_c+d*e:f;g~h", "x" * 32, "x" * 33, "con spazio", "àccento", "a/b"]
    for value in samples:
        # Stesso verdetto del client Betfair reale.
        assert is_betfair_customer_ref(value) == (BetfairClient._normalize_customer_ref(value) == value)

    assert normalize_upstream_customer_ref(None) == ""
    assert normalize_upstream_customer_ref("   ") == ""
    assert normalize_upstream_customer_ref(" ok-1 ") == "ok-1"
    mapped = normalize_upstream_customer_ref("x" * 33)
    assert mapped.startswith("up-") and is_betfair_customer_ref(mapped)
    assert normalize_upstream_customer_ref("x" * 33) == mapped
    assert normalize_upstream_customer_ref("x" * 34) != mapped

    op_a, op_b = new_operation_customer_ref("man"), new_operation_customer_ref("man")
    assert op_a != op_b
    assert op_a.startswith("man-") and is_betfair_customer_ref(op_a) and len(op_a) == 32
    with pytest.raises(ValueError):
        new_operation_customer_ref("MAN")


@pytest.mark.parametrize("prefix", ["", "TOOLONG", "s g", "sig-", None])
def test_block_helper_rejects_invalid_prefix(prefix):
    from core.order_identity import derive_customer_ref

    with pytest.raises(ValueError):
        derive_customer_ref(prefix, {"a": 1})
