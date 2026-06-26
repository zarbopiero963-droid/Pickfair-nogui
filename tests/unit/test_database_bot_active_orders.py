"""Unit test per Database.get_bot_active_orders (Fase 2.1-B2.2, identità I1)."""

import pytest

from database import Database


@pytest.fixture()
def db():
    return Database(":memory:")


def _place_live_saga(db, *, ref, market_id, event_key, event_name, bet_id, status="PLACED"):
    """Crea un saga live e lo porta a placed con bet_id (come il runtime)."""
    db.create_order_saga(
        customer_ref=ref,
        batch_id="B1",
        event_key=event_key,
        table_id=None,
        market_id=market_id,
        selection_id="7",
        bet_type="BACK",
        price=2.0,
        stake=10.0,
        payload={"event_name": event_name} if event_name is not None else {},
        status="PENDING",
    )
    db.update_order_saga(customer_ref=ref, status=status, bet_id=bet_id)


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


def test_sim_bet_without_bet_id_is_excluded(db):
    # bet_id mancante => RuntimeError sul save; usiamo un save valido e poi
    # verifichiamo che una riga senza bet_id non sia mai prodotta.
    with pytest.raises(RuntimeError):
        db.save_simulation_bet({"market_id": "1.1", "event_name": "x"})
    assert db.get_bot_active_orders() == []


def test_live_saga_event_name_from_payload(db):
    _place_live_saga(db, ref="C1", market_id="1.2", event_key="ek",
                     event_name="Roma vs Lazio", bet_id="L1")
    out = db.get_bot_active_orders()
    assert out == [{"bet_id": "L1", "market_id": "1.2", "event_name": "Roma vs Lazio"}]


def test_live_saga_without_payload_event_name_is_empty_not_event_key(db):
    # Greptile P1: event_key è uno slug interno di deduplica, NON il nome della
    # partita. Senza event_name nel payload si ritorna '' (fail-closed: il router
    # salta il CASHOUT singolo per quel mercato), MAI lo slug event_key.
    _place_live_saga(db, ref="C2", market_id="1.3", event_key="evt::1.3::7::slug",
                     event_name=None, bet_id="L2")
    out = db.get_bot_active_orders()
    assert out == [{"bet_id": "L2", "market_id": "1.3", "event_name": ""}]


def test_live_saga_without_bet_id_is_excluded(db):
    # Saga PENDING senza bet_id (non ancora piazzato) => escluso.
    db.create_order_saga(
        customer_ref="C3", batch_id="B1", event_key="ek", table_id=None,
        market_id="1.4", selection_id="7", bet_type="BACK", price=2.0, stake=10.0,
        payload={"event_name": "x"}, status="PENDING",
    )
    assert db.get_bot_active_orders() == []


def test_sim_and_live_are_unioned(db):
    db.save_simulation_bet({
        "bet_id": "S1", "market_id": "1.1", "selection_id": "7",
        "status": "EXECUTABLE", "event_name": "Inter vs Milan",
    })
    _place_live_saga(db, ref="C1", market_id="1.2", event_key="ek",
                     event_name="Roma vs Lazio", bet_id="L1")
    out = {o["bet_id"]: o for o in db.get_bot_active_orders()}
    assert set(out) == {"S1", "L1"}
    assert out["S1"]["market_id"] == "1.1"
    assert out["L1"]["event_name"] == "Roma vs Lazio"


def test_keys_match_router_contract(db):
    # Il CashoutRouter legge bet_id / market_id / event_name da ogni ordine bot.
    db.save_simulation_bet({
        "bet_id": "S1", "market_id": "1.1", "selection_id": "7",
        "status": "EXECUTABLE", "event_name": "e",
    })
    order = db.get_bot_active_orders()[0]
    assert set(order) == {"bet_id", "market_id", "event_name"}
