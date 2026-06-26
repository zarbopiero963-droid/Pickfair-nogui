"""PnLEngine come sorgente posizioni del cashout mirror (Fase 2.1-B0).

Verifica che la PnLEngine sia cablata in tracking-only (auto_close=False:
nessuna auto-chiusura MTM che pubblicherebbe RUNTIME_CLOSE_POSITION), che
`snapshot()` esponga le posizioni dai fill, e che il lock regga l'enumerazione
concorrente.
"""

import threading

from core.pnl_engine import PnLEngine
from core.runtime_controller import RuntimeController


class _Bus:
    def __init__(self):
        self.events = []
        self.subscriptions = {}

    def subscribe(self, event_name, handler):
        self.subscriptions.setdefault(event_name, []).append(handler)

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


def _fill(event_key="E1", market="1.100", sel=7, side="BACK", price=2.0, size=5.0, bet_id=None):
    f = {
        "event_key": event_key,
        "market_id": market,
        "selection_id": sel,
        "bet_type": side,
        "avg_price_matched": price,
        "matched_size": size,
    }
    if bet_id is not None:
        # fill_id distinto: il PositionLedger deduplica per fill_id.
        f["bet_id"] = bet_id
    return f


# ---------------------------------------------------------------------------
# auto_close gating — il cuore della sicurezza del prereq
# ---------------------------------------------------------------------------


def test_auto_close_false_does_not_subscribe_market_book_update():
    bus = _Bus()
    PnLEngine(bus=bus, auto_close=False)
    assert "QUICK_BET_FILLED" in bus.subscriptions
    assert "QUICK_BET_PARTIAL" in bus.subscriptions
    # Niente MARKET_BOOK_UPDATE => niente auto-chiusura MTM.
    assert "MARKET_BOOK_UPDATE" not in bus.subscriptions


def test_auto_close_true_preserves_legacy_subscription():
    bus = _Bus()
    PnLEngine(bus=bus, auto_close=True)
    assert "MARKET_BOOK_UPDATE" in bus.subscriptions


def test_default_auto_close_is_true_backward_compatible():
    bus = _Bus()
    pe = PnLEngine(bus=bus)
    assert pe.auto_close is True


# ---------------------------------------------------------------------------
# tracking via snapshot
# ---------------------------------------------------------------------------


def test_snapshot_tracks_filled_position():
    bus = _Bus()
    pe = PnLEngine(bus=bus, auto_close=False)
    pe._on_filled(_fill(side="BACK", price=2.0, size=5.0))

    snap = pe.snapshot()
    assert snap["open_positions"] == 1
    pos = snap["positions"][0]
    assert pos["market_id"] == "1.100"
    assert pos["selection_id"] == 7
    assert pos["side"] == "BACK"
    assert pos["price"] == 2.0
    assert pos["stake"] == 5.0


def test_flat_position_is_pruned_not_kept_at_zero_stake():
    # In tracking-only una posizione azzerata da un fill opposto va POTATA: senza
    # _on_market non c'è altro a rimuoverla, e snapshot() non deve esporre
    # posizioni a stake 0 (il routing cashout proverebbe a chiuderle).
    pe = PnLEngine(bus=None, auto_close=False)
    pe._on_filled(_fill(event_key="E1", side="BACK", price=2.0, size=5.0, bet_id="B1"))
    assert pe.snapshot()["open_positions"] == 1

    # bet_id distinto: senza, il ledger deduplica il secondo fill.
    pe._on_filled(_fill(event_key="E1", side="LAY", price=2.0, size=5.0, bet_id="B2"))
    assert pe.snapshot()["open_positions"] == 0


def test_auto_close_closes_profitable_position_and_publishes():
    # Verifica che il close-path (refactor publish-fuori-dal-lock) funzioni:
    # posizione profittevole => RUNTIME_CLOSE_POSITION pubblicato + posizione rimossa.
    bus = _Bus()
    pe = PnLEngine(bus=bus, auto_close=True)
    pe._on_filled(_fill(event_key="E1", market="1.100", sel=7, side="BACK", price=2.0, size=10.0))
    assert pe.snapshot()["open_positions"] == 1

    book = {
        "marketId": "1.100",
        "runners": [
            {"selectionId": 7, "ex": {
                "availableToBack": [{"price": 1.1}],
                "availableToLay": [{"price": 1.1}],
            }},
        ],
    }
    pe._on_market(book)

    assert pe.snapshot()["open_positions"] == 0
    assert any(name == "RUNTIME_CLOSE_POSITION" for name, _ in bus.events)


def test_tracking_only_never_publishes_close():
    # Anche alimentando una posizione, in tracking-only NESSUN
    # RUNTIME_CLOSE_POSITION deve mai essere pubblicato (auto_close off).
    bus = _Bus()
    pe = PnLEngine(bus=bus, auto_close=False)
    pe._on_filled(_fill())
    # Un eventuale MARKET_BOOK_UPDATE non raggiunge la engine (non sottoscritta).
    assert all(name != "RUNTIME_CLOSE_POSITION" for name, _ in bus.events)


# ---------------------------------------------------------------------------
# lock / concorrenza
# ---------------------------------------------------------------------------


def test_snapshot_concurrent_with_fills_does_not_raise():
    pe = PnLEngine(bus=None, auto_close=False)
    errors = []

    def writer():
        try:
            for i in range(200):
                pe._on_filled(_fill(event_key=f"E{i}", sel=7 + (i % 5)))
        except Exception as exc:  # pragma: no cover - solo in caso di bug lock
            errors.append(exc)

    def reader():
        try:
            for _ in range(200):
                pe.snapshot()
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=writer), threading.Thread(target=reader),
               threading.Thread(target=reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []


# ---------------------------------------------------------------------------
# wiring nel RuntimeController
# ---------------------------------------------------------------------------


class _Db:
    def __init__(self):
        class _Cipher:
            key_source = "env"
        self._cipher = _Cipher()

    def _execute(self, *_a, **_k):
        return None


class _Settings:
    def load_roserpina_config(self):
        class Cfg:
            table_count = 2
            anti_duplication_enabled = False
            allow_recovery = False
            max_daily_loss = 100.0

            def __getattr__(self, _name):
                return 0
        return Cfg()

    def load_live_enabled(self):
        return False

    def load_live_readiness_ok(self):
        return False


class _Betfair:
    simulation_mode = None

    def set_simulation_mode(self, enabled):
        self.simulation_mode = bool(enabled)

    def status(self):
        return {"connected": True}


class _Telegram:
    def status(self):
        return {"connected": True}


def _make_runtime(pnl_engine=None):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
        pnl_engine=pnl_engine,
    )


def test_runtime_wires_pnl_engine_tracking_only():
    rc = _make_runtime()
    assert isinstance(rc.pnl_engine, PnLEngine)
    # Tracking-only: nessuna auto-chiusura.
    assert rc.pnl_engine.auto_close is False
    # snapshot() accessibile dal runtime (sorgente del cashout sizing 2.1-B).
    assert rc.pnl_engine.snapshot()["open_positions"] == 0


def test_runtime_accepts_injected_pnl_engine():
    sentinel = PnLEngine(bus=None, auto_close=False)
    rc = _make_runtime(pnl_engine=sentinel)
    assert rc.pnl_engine is sentinel
