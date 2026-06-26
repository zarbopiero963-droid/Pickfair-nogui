"""Unit test per cashout_resolver (Fase 2.1-B, design A3)."""

from cashout_resolver import build_cashout_request, reconstruct_open_positions


def _order(market="1.100", sel=7, side="BACK", matched=10.0, avg=2.0,
           price_size=None, remaining=0.0, ref="BOT-1"):
    o = {
        "marketId": market,
        "selectionId": sel,
        "side": side,
        "sizeMatched": matched,
        "sizeRemaining": remaining,
        "customerOrderRef": ref,
    }
    if avg is not None:
        o["averagePriceMatched"] = avg
    if price_size is not None:
        o["priceSize"] = {"price": price_size, "size": matched}
    return o


def _all(_o):
    """Predicate bot-order che accetta tutto (per i test non-filtro)."""
    return True


# ---------------------------------------------------------------------------
# reconstruct_open_positions
# ---------------------------------------------------------------------------


def test_single_back_order_becomes_back_position():
    pos = reconstruct_open_positions([_order(side="BACK", matched=10.0, avg=2.0)], is_bot_order=_all)
    assert len(pos) == 1
    assert pos[0]["market_id"] == "1.100"
    assert pos[0]["selection_id"] == 7
    assert pos[0]["side"] == "BACK"
    assert pos[0]["stake"] == 10.0
    assert pos[0]["price"] == 2.0
    assert pos[0]["resting_remainder"] == 0.0


def test_single_lay_order_becomes_lay_position():
    pos = reconstruct_open_positions([_order(side="LAY", matched=8.0, avg=1.9)], is_bot_order=_all)
    assert len(pos) == 1
    assert pos[0]["side"] == "LAY"
    assert pos[0]["stake"] == 8.0
    assert pos[0]["price"] == 1.9


def test_mixed_back_and_lay_selection_is_skipped_fail_closed():
    # N2: una selezione con matched su ENTRAMBI i lati viene SALTATA (mai un
    # hedge potenzialmente opposto). Vale anche se "coperta" o sbilanciata.
    assert reconstruct_open_positions([
        _order(side="BACK", matched=10.0, avg=2.0),
        _order(side="LAY", matched=4.0, avg=1.8),
    ], is_bot_order=_all) == []
    assert reconstruct_open_positions([
        _order(side="BACK", matched=5.0, avg=2.0),
        _order(side="LAY", matched=5.0, avg=2.0),
    ], is_bot_order=_all) == []


def test_weighted_average_price_across_back_fills():
    # BACK 10@2.0 + BACK 10@3.0 (stesso lato) => 20 @2.5.
    pos = reconstruct_open_positions([
        _order(side="BACK", matched=10.0, avg=2.0),
        _order(side="BACK", matched=10.0, avg=3.0),
    ], is_bot_order=_all)
    assert pos[0]["stake"] == 20.0
    assert pos[0]["price"] == 2.5


def test_only_bot_orders_are_aggregated():
    # Il feed è account-level: un ordine non-bot (ref diverso) sullo stesso conto
    # NON deve diventare una posizione da chiudere.
    def is_bot(o):
        return str(o.get("customerOrderRef") or "").startswith("BOT-")

    pos = reconstruct_open_positions([
        _order(market="1.1", sel=7, side="BACK", matched=10.0, avg=2.0, ref="BOT-1"),
        _order(market="1.2", sel=9, side="LAY", matched=8.0, avg=1.5, ref="MANUAL-x"),
    ], is_bot_order=is_bot)
    assert len(pos) == 1
    assert pos[0]["market_id"] == "1.1"


def test_resting_remainder_is_exposed():
    # Ordine parzialmente abbinato: matched + sizeRemaining resting => la posizione
    # espone resting_remainder per il cancel a monte del routing.
    pos = reconstruct_open_positions(
        [_order(side="BACK", matched=6.0, avg=2.0, remaining=4.0)], is_bot_order=_all
    )
    assert len(pos) == 1
    assert pos[0]["stake"] == 6.0
    assert pos[0]["resting_remainder"] == 4.0


def test_sim_proxy_price_when_no_average_matched():
    # In SIM manca averagePriceMatched: si usa priceSize.price come proxy.
    pos = reconstruct_open_positions(
        [_order(side="BACK", matched=5.0, avg=None, price_size=2.2)], is_bot_order=_all
    )
    assert len(pos) == 1
    assert pos[0]["price"] == 2.2


def test_zero_average_price_falls_back_to_proxy():
    # averagePriceMatched=0.0 (presente ma non valido) NON deve scartare il row:
    # si ripiega su priceSize.price.
    pos = reconstruct_open_positions(
        [_order(side="BACK", matched=5.0, avg=0.0, price_size=2.3)], is_bot_order=_all
    )
    assert len(pos) == 1
    assert pos[0]["price"] == 2.3


def test_skips_zero_matched_and_invalid_rows():
    rows = [
        _order(side="BACK", matched=0.0, avg=2.0),          # niente matched
        _order(side="", matched=5.0, avg=2.0),               # side invalido
        {"selectionId": 7, "side": "BACK", "sizeMatched": 5.0, "averagePriceMatched": 2.0},  # no market
        _order(side="BACK", matched=5.0, avg=1.0),           # prezzo <= 1
        "not-a-dict",
    ]
    assert reconstruct_open_positions(rows, is_bot_order=_all) == []


def test_multiple_markets_and_selections():
    pos = reconstruct_open_positions([
        _order(market="1.1", sel=7, side="BACK", matched=10.0, avg=2.0),
        _order(market="1.2", sel=9, side="LAY", matched=8.0, avg=1.5),
    ], is_bot_order=_all)
    assert len(pos) == 2
    by_market = {p["market_id"]: p for p in pos}
    assert by_market["1.1"]["side"] == "BACK"
    assert by_market["1.2"]["side"] == "LAY"


# ---------------------------------------------------------------------------
# build_cashout_request
# ---------------------------------------------------------------------------


def test_build_back_position_places_lay_with_green_up():
    pos = {"market_id": "1.100", "selection_id": 7, "side": "BACK", "stake": 10.0, "price": 2.0}
    # commission deve rispettare la policy Betfair Italia (4.5) enforced da dutching.
    req = build_cashout_request(pos, current_price=1.5, commission=4.5, source="TELEGRAM")

    assert req is not None
    assert req["market_id"] == "1.100"
    assert req["selection_id"] == 7
    assert req["side"] == "LAY"          # lato opposto da piazzare
    assert req["stake"] > 0.0
    assert req["price"] == 1.5
    assert "green_up" in req
    assert req["original_pos"] == {"side": "BACK", "stake": 10.0, "price": 2.0}
    assert req["source"] == "TELEGRAM"


def test_build_lay_position_places_back():
    pos = {"market_id": "1.100", "selection_id": 7, "side": "LAY", "stake": 6.0, "price": 1.8}
    req = build_cashout_request(pos, current_price=2.2)
    assert req is not None
    assert req["side"] == "BACK"
    assert req["stake"] > 0.0


def test_build_payload_matches_req_execute_cashout_contract():
    # Stesse chiavi che risk_middleware._handle_cashout normalizza.
    pos = {"market_id": "1.100", "selection_id": 7, "side": "BACK", "stake": 10.0, "price": 2.0}
    req = build_cashout_request(pos, current_price=1.6)
    assert set(req) >= {"market_id", "selection_id", "side", "stake", "price", "green_up", "source"}


def test_build_fail_closed_on_policy_violating_commission():
    # Una commissione fuori policy fa sollevare dutching: build deve ritornare
    # None (fail-closed), non propagare l'eccezione nel routing.
    pos = {"market_id": "1.1", "selection_id": 7, "side": "BACK", "stake": 10.0, "price": 2.0}
    assert build_cashout_request(pos, current_price=1.5, commission=2.0) is None


def test_build_rejects_when_stake_rounds_to_zero():
    # Stake di cashout sub-cent (arrotonda a 0.0): l'executor lo rifiuterebbe,
    # quindi build deve ritornare None (Greptile P2).
    pos = {"market_id": "1.1", "selection_id": 7, "side": "BACK", "stake": 0.01, "price": 1.01}
    assert build_cashout_request(pos, current_price=900.0) is None


def test_build_rejects_invalid_position():
    assert build_cashout_request(
        {"market_id": "1.1", "selection_id": 7, "side": "BACK", "stake": 0.0, "price": 2.0},
        current_price=1.5,
    ) is None
    assert build_cashout_request(
        {"market_id": "1.1", "selection_id": 7, "side": "BACK", "stake": 10.0, "price": 2.0},
        current_price=1.0,  # prezzo corrente non valido
    ) is None
    assert build_cashout_request(
        {"market_id": "1.1", "selection_id": 7, "side": "NOPE", "stake": 10.0, "price": 2.0},
        current_price=1.5,
    ) is None
