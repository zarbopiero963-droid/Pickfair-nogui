"""Unit test per Database.get_bot_active_orders (Fase 2.1-B2.2, identità I1).

Design owner **X**: il ledger live autoritativo del percorso headless è la
tabella ``orders`` (TradingEngine senza OrderManager → niente ``order_saga``),
con betId nel ``response_json`` e market_id/event_name nel ``payload_json``.
"""

import pytest

from database import Database


@pytest.fixture()
def db():
    return Database(":memory:")


def _place_live_order(db, *, ref, market_id, event_name, bet_id,
                      status="MATCHED", response=None):
    """Inserisce un ordine live nella tabella ``orders`` e lo porta a ``status``.

    Mirror del runtime: insert_order(INFLIGHT, payload=request) →
    update_order(status, response={bet_id}). ``response=None`` usa il betId dato.
    """
    order_id = db.insert_order({
        "customer_ref": ref,
        "correlation_id": f"corr-{ref}",
        "status": "INFLIGHT",
        "payload": {"market_id": market_id, "event_name": event_name, "selection_id": 7},
    })
    if status is not None:
        if response is None:
            response = {"bet_id": bet_id, "placed": True}
        db.update_order(order_id, {"status": status, "response": response})
    return order_id


# ---------------------------------------------------------------------------


def test_empty_db_returns_empty(db):
    assert db.get_bot_active_orders() == []


def test_sim_bet_is_returned_with_event_name(db):
    db.save_simulation_bet({
        "bet_id": "S1", "market_id": "1.1", "selection_id": "7",
        "side": "BACK", "status": "EXECUTION_COMPLETE", "event_name": "Inter vs Milan",
    })
    out = db.get_bot_active_orders()
    assert out == [{"bet_id": "S1", "market_id": "1.1", "event_name": "Inter vs Milan"}]


def test_sim_cancelled_bet_is_excluded(db):
    db.save_simulation_bet({
        "bet_id": "S9", "market_id": "1.1", "selection_id": "7",
        "status": "CANCELLED", "event_name": "x",
    })
    assert db.get_bot_active_orders() == []


def test_sim_bet_without_bet_id_raises_on_save(db):
    with pytest.raises(RuntimeError):
        db.save_simulation_bet({"market_id": "1.1", "event_name": "x"})
    assert db.get_bot_active_orders() == []


def test_live_order_matched_is_returned(db):
    # Codex P1: il percorso headless live persiste nella tabella orders.
    _place_live_order(db, ref="C1", market_id="1.2", event_name="Roma vs Lazio",
                      bet_id="L1", status="MATCHED")
    out = db.get_bot_active_orders()
    assert out == [{"bet_id": "L1", "market_id": "1.2", "event_name": "Roma vs Lazio"}]


def test_live_order_partially_matched_is_included(db):
    # Codex P1 (L1288): PARTIALLY_MATCHED è una posizione aperta da chiudere.
    _place_live_order(db, ref="C2", market_id="1.3", event_name="Napoli vs Juve",
                      bet_id="L2", status="PARTIALLY_MATCHED")
    out = db.get_bot_active_orders()
    assert out == [{"bet_id": "L2", "market_id": "1.3", "event_name": "Napoli vs Juve"}]


def test_live_order_inflight_without_betid_is_excluded(db):
    # INFLIGHT pre-piazzamento: nessuna response/betId => escluso.
    _place_live_order(db, ref="C3", market_id="1.4", event_name="x",
                      bet_id="", status=None)
    assert db.get_bot_active_orders() == []


def test_live_order_betid_falls_back_to_raw_instruction_reports(db):
    # betId assente al top-level del response: si ripiega su raw.instructionReports.
    _place_live_order(db, ref="C4", market_id="1.5", event_name="Atalanta vs Bologna",
                      bet_id="", status="MATCHED",
                      response={"placed": True, "raw": {"instructionReports": [{"betId": "RAW9"}]}})
    out = db.get_bot_active_orders()
    assert out == [{"bet_id": "RAW9", "market_id": "1.5", "event_name": "Atalanta vs Bologna"}]


def test_sim_and_live_are_unioned(db):
    db.save_simulation_bet({
        "bet_id": "S1", "market_id": "1.1", "selection_id": "7",
        "status": "EXECUTABLE", "event_name": "Inter vs Milan",
    })
    _place_live_order(db, ref="C1", market_id="1.2", event_name="Roma vs Lazio",
                      bet_id="L1")
    out = {o["bet_id"]: o for o in db.get_bot_active_orders()}
    assert set(out) == {"S1", "L1"}
    assert out["S1"]["market_id"] == "1.1"
    assert out["L1"]["event_name"] == "Roma vs Lazio"


def test_duplicate_bet_id_is_deduplicated(db):
    # Stesso betId in due righe (caso patologico): ritornato una sola volta.
    db.save_simulation_bet({
        "bet_id": "D1", "market_id": "1.1", "selection_id": "7",
        "status": "EXECUTABLE", "event_name": "e",
    })
    _place_live_order(db, ref="C1", market_id="1.1", event_name="e", bet_id="D1")
    out = db.get_bot_active_orders()
    assert [o["bet_id"] for o in out] == ["D1"]


def test_keys_match_router_contract(db):
    db.save_simulation_bet({
        "bet_id": "S1", "market_id": "1.1", "selection_id": "7",
        "status": "EXECUTABLE", "event_name": "e",
    })
    order = db.get_bot_active_orders()[0]
    assert set(order) == {"bet_id", "market_id", "event_name"}
