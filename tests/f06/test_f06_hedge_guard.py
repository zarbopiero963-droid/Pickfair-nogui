"""F-06 (#586, CSH-05, MM-02, NF-01) — l'hedge del cashout deve essere coerente con la posizione (RED prima della patch).

Prima: `CashoutExecutor` piazzava qualunque lato/size arrivasse nel comando (un hedge BACK su una posizione BACK la
RADDOPPIA; un LAY 50 su una posizione da 0.23 apre una nuova esposizione), e `RiskMiddleware` defaultava `side="LAY"`
quando mancava (l'auto-close del monitor SL/TP manda il comando senza lato). Atteso (Roserpina unica autorita',
PF-EXEC-03 chiusura protettiva senza inseguimento, D-582-03): lato = opposto della posizione, stake <= stake di
chiusura dell'esposizione ABBINATA sull'exchange (nessun cap di stake, solo coerenza: `RiskGate.protective_hedge`);
altrimenti `CASHOUT_FAILED` `REJECTED` `hedge_non_coerente:*`; lato mancante => rifiuto, mai LAY di default.
"""
from __future__ import annotations

import pytest

from cashout_executor import CASHOUT_FAILED, CASHOUT_SUCCESS, CashoutExecutor
from core.risk_gate import RiskGate

pytestmark = [pytest.mark.safety]


class _Bus:
    def __init__(self):
        self.published = []

    def subscribe(self, *_a):
        pass

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def last(self, topic):
        return next((p for t, p in reversed(self.published) if t == topic), None)


class _Router:
    def __init__(self):
        self.calls = []

    def place(self, payload):
        self.calls.append(payload)
        return {"ok": True, "placed": True, "order_unknown": False, "status": "SUCCESS",
                "matched": payload["stake"], "bet_id": "H1", "error": None}


def _order(side="BACK", matched=0.23, price=2.0, selection=55, ref="sig-entry", market="1.222"):
    return {"betId": "B1", "marketId": market, "selectionId": selection, "side": side, "sizeMatched": matched,
            "averagePriceMatched": price, "priceSize": {"price": price, "size": matched}, "customerOrderRef": ref}


def _cmd(side="LAY", stake=0.26, price=1.8, orig_side="BACK", orig_stake=0.23, orig_price=2.0, **over):
    p = {"market_id": "1.222", "selection_id": 55, "side": side, "stake": stake, "price": price, "green_up": 0.03,
         "original_pos": {"side": orig_side, "stake": orig_stake, "price": orig_price}, "source": "UI"}
    p.update(over)
    return p


def _run(cmd, orders=None, fetch=True):
    bus, router = _Bus(), _Router()
    kw = {}
    if fetch:
        kw["fetch_current_orders"] = (lambda: orders) if not callable(orders) else orders
    CashoutExecutor(bus, router, **kw).on_cmd_execute_cashout(cmd)
    return bus, router


def test_hedge_wrong_side_rejected():
    # D-582-03: hedge BACK 0.23 su una posizione BACK la raddoppia invece di chiuderla
    bus, router = _run(_cmd(side="BACK", stake=0.23), orders=[_order()])
    assert router.calls == [], "hedge dello stesso lato della posizione piazzato"
    failed = bus.last(CASHOUT_FAILED)
    assert failed["status"] == "REJECTED" and failed["reason"] == "hedge_non_coerente:lato"


def test_hedge_oversized_rejected():
    # D-582-03: LAY 50 su una posizione BACK da 0.23 (chiusura massima 0.23*2.0/1.8 = 0.26) apre nuova esposizione
    bus, router = _run(_cmd(stake=50.0), orders=[_order()])
    assert router.calls == [], "hedge oltre l'esposizione abbinata piazzato"
    assert bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_coherent_hedge_is_placed():
    bus, router = _run(_cmd(), orders=[_order()])
    assert len(router.calls) == 1 and router.calls[0]["bet_type"] == "LAY"
    assert bus.last(CASHOUT_SUCCESS) is not None


def test_size_is_measured_on_the_exchange_not_on_the_payload():
    # il payload dichiara una posizione da 30, l'exchange ne ha abbinata 0.23: vale l'exchange
    bus, router = _run(_cmd(stake=33.0, orig_stake=30.0), orders=[_order()])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_neutral_pairs_and_other_selections_or_markets_are_no_position():
    from core.hedge_ref import new_hedge_ref
    orders = [_order(side="BACK", matched=40.0, ref=new_hedge_ref()), _order(matched=40.0, selection=99),
              _order(matched=40.0, market="1.999"), _order(side="LAY", matched=40.0)]
    bus, router = _run(_cmd(stake=5.0), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_assente"


@pytest.mark.parametrize("orders", [None, "x", [1], [{"betId": "B1", "marketId": "1.222"}],
                                    [dict(_order(), sizeMatched="NaN")], [dict(_order(), sizeMatched=float("inf"))]])
def test_unreadable_exchange_position_never_places(orders):
    bus, router = _run(_cmd(), orders=orders)
    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["reason"].startswith("hedge_non_coerente:")


def test_exchange_read_error_never_places():
    def boom():
        raise RuntimeError("down")
    bus, router = _run(_cmd(), orders=boom)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_non_verificabile"


@pytest.mark.parametrize("orig", [None, {}, {"side": "X", "stake": 1, "price": 2}, "BACK"])
def test_missing_or_invalid_original_position_rejected(orig):
    bus, router = _run(_cmd(original_pos=orig), orders=[_order()])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"].startswith("hedge_non_coerente:")


def test_without_exchange_source_the_payload_position_still_bounds_the_hedge():
    # doppi di test senza fetch: la coerenza si verifica comunque sulla posizione del comando, mai saltata
    bus, router = _run(_cmd(stake=50.0), fetch=False)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"
    bus, router = _run(_cmd(), fetch=False)
    assert len(router.calls) == 1


@pytest.mark.parametrize("pos_side,hedge_side,stake,ok,reason", [
    ("BACK", "LAY", 0.26, True, None),
    ("LAY", "BACK", 0.26, True, None),
    ("BACK", "BACK", 0.1, False, "hedge_non_coerente:lato"),
    ("LAY", "LAY", 0.1, False, "hedge_non_coerente:lato"),
    ("BACK", "LAY", 0.27, False, "hedge_non_coerente:size"),     # 0.46/1.8 = 0.2556 => max 0.26 al centesimo
    ("BACK", "LAY", 0.0, False, "hedge_non_coerente:size"),
    ("BACK", "LAY", float("nan"), False, "hedge_non_coerente:size"),
])
def test_risk_gate_protective_hedge_verdict(pos_side, hedge_side, stake, ok, reason):
    v = RiskGate.protective_hedge(position_side=pos_side, hedge_side=hedge_side, hedge_stake=stake,
                                  hedge_price=1.8, matched_notional=0.46)
    assert v["allowed"] is ok and v["reason"] == reason


def test_middleware_missing_side_rejected():
    # NF-01: l'auto-close del monitor manda REQ_EXECUTE_CASHOUT senza lato; prima diventava un LAY di default
    from core.risk_middleware import RiskMiddleware
    bus = _Bus()
    mw = RiskMiddleware.__new__(RiskMiddleware)
    mw.bus = bus
    mw._is_duplicate = lambda _p: False
    mw._handle_cashout({"market_id": "1.222", "selection_id": 55, "stake": 3.0, "reason": "STOP_LOSS_REACHED",
                        "source": "AUTO_MONITOR"})
    assert not [t for t, _ in bus.published if t == "CMD_EXECUTE_CASHOUT"], "hedge con lato inventato inoltrato"
    assert bus.last("CASHOUT_FAILED") is not None


@pytest.mark.parametrize("side", ["", "X", None, 1, "BACKLAY"])
def test_middleware_invalid_side_rejected(side):
    from core.risk_middleware import RiskMiddleware
    bus = _Bus()
    mw = RiskMiddleware.__new__(RiskMiddleware)
    mw.bus = bus
    mw._is_duplicate = lambda _p: False
    mw._handle_cashout({"market_id": "1.222", "selection_id": 55, "side": side, "stake": 3.0, "price": 1.8})
    assert not [t for t, _ in bus.published if t == "CMD_EXECUTE_CASHOUT"]
    assert bus.last("CASHOUT_FAILED") is not None


def test_matched_size_uses_the_average_matched_price_not_the_requested_price():
    # ingresso richiesto a 5.0 ma abbinato in media a 2.0: l'esposizione e' 0.23*2.0 = 0.46, la chiusura 0.26
    # (il comando dichiara 5.0: vale l'abbinato dell'exchange, non il prezzo richiesto)
    orders = [dict(_order(), averagePriceMatched=2.0, priceSize={"price": 5.0, "size": 0.23})]
    bus, router = _run(_cmd(stake=0.5, orig_price=5.0), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_hedge_coherence_is_enforced_on_the_ledger_path_of_the_real_wiring(tmp_path):
    # wiring reale (cashout_wiring): executor con ledger `orders` => il controllo gira dentro il preflight del ledger
    from database import Database
    db = Database(str(tmp_path / "f06.db"))
    for cmd, placed in ((_cmd(side="BACK", stake=0.23, cashout_request_id="r1"), False),
                        (_cmd(stake=50.0, cashout_request_id="r2"), False),
                        (_cmd(cashout_request_id="r3"), True)):
        bus, router = _Bus(), _Router()
        CashoutExecutor(bus, router, ledger=db, fetch_current_orders=lambda: [_order()]).on_cmd_execute_cashout(cmd)
        assert bool(router.calls) is placed, cmd
        if not placed:
            assert bus.last(CASHOUT_FAILED)["reason"].startswith("hedge_non_coerente:")
    rows = db._execute("SELECT status FROM orders", fetch=True, commit=False) or []
    assert len(rows) == 1, "un hedge incoerente non deve lasciare righe nel ledger"


def _resolver_cmd(orders, current_price):
    from cashout_resolver import build_cashout_request, reconstruct_open_positions
    positions = reconstruct_open_positions(orders, is_bot_order=lambda r: True)
    assert len(positions) == 1
    return build_cashout_request(positions[0], current_price=current_price, commission=4.5)


@pytest.mark.parametrize("side", ["BACK", "LAY"])
def test_every_green_up_built_by_the_real_resolver_is_accepted(side):
    # il resolver arrotonda lo stake ROUND_HALF_UP al centesimo: il controllo non deve mai rifiutare un cashout
    # legittimo al confine del mezzo centesimo (secondo arrotondamento su un float leggermente diverso)
    rejected = []
    for matched in (0.23, 0.35, 1.0, 1.05, 2.15, 3.33, 7.77, 10.0, 12.5):
        for entry in (1.01, 1.37, 1.85, 2.0, 2.62, 3.45, 5.5, 11.0):
            for cur in (1.01, 1.29, 1.8, 2.06, 2.5, 3.15, 4.4, 9.8):
                orders = [_order(side=side, matched=matched, price=entry)]
                cmd = _resolver_cmd(orders, cur)
                if cmd is None:
                    continue
                bus, router = _run(cmd, orders=orders)
                if not router.calls:
                    rejected.append((matched, entry, cur, cmd["stake"], bus.last(CASHOUT_FAILED)["reason"]))
    assert rejected == [], rejected[:5]


def test_invalid_average_price_falls_back_to_the_order_price_like_the_resolver():
    # averagePriceMatched <= 1 (0.0 dell'API su ordini appena abbinati) => priceSize.price, come _row_matched_price
    orders = [dict(_order(), averagePriceMatched=1.0)]
    cmd = _resolver_cmd(orders, 1.8)
    bus, router = _run(cmd, orders=orders)
    assert len(router.calls) == 1, bus.last(CASHOUT_FAILED)


def test_neutral_manual_pair_on_the_selection_never_widens_the_hedge():
    # Sol su a16f0d3: BACK 0.23@2 da chiudere + coppia manuale BACK 10@2 / LAY 10@2 gia' neutra. Sommando solo il lato
    # della posizione il tetto diventava 20.46/2 e passava un LAY 10@2: nuova esposizione invece della chiusura
    orders = [_order(), dict(_order(matched=10.0, ref="manual-1"), betId="M1"),
              dict(_order(side="LAY", matched=10.0, ref="manual-2"), betId="M2")]
    bus, router = _run(_cmd(stake=10.0, price=2.0), orders=orders)
    assert router.calls == [], "hedge oltre la posizione identificata piazzato"
    assert bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"
    bus, router = _run(_cmd(stake=0.23, price=2.0), orders=orders)
    assert len(router.calls) == 1, bus.last(CASHOUT_FAILED)


def test_exchange_position_larger_than_the_identified_one_never_widens_the_hedge():
    # altri ingressi dello stesso lato (manuali o di altri tavoli): il tetto resta la posizione del comando
    orders = [_order(), dict(_order(matched=10.0, ref="other-table"), betId="O1")]
    bus, router = _run(_cmd(stake=5.0, price=2.0), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_matched_opposite_side_orders_reduce_the_exposure():
    # un hedge precedente gia' abbinato (anche fuori dal ledger) riduce l'esposizione: niente sovracopertura
    from core.hedge_ref import new_hedge_ref
    orders = [_order(), dict(_order(side="LAY", matched=0.2, ref=new_hedge_ref()), betId="H0")]
    bus, router = _run(_cmd(stake=0.26), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


@pytest.mark.parametrize("orig", [{"side": "BACK"}, {"side": "BACK", "stake": "x", "price": 2.0},
                                  {"side": "BACK", "stake": 0.23, "price": float("nan")}])
def test_unreadable_identified_position_size_rejected(orig):
    bus, router = _run(_cmd(original_pos=orig), orders=[_order()])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_assente"



def test_previous_opposite_position_neutralised_by_its_hedge_still_allows_the_close():
    # Sol su 73296a3: LAY 1@2 gia' chiuso da un hedge BACK 1@2, poi un nuovo BACK 0.23@2 da chiudere. Escludendo
    # l'hedge del lato della posizione il netto era -1.54 (cashout valido rifiutato): la coppia chiusa si annulla
    from core.hedge_ref import new_hedge_ref
    orders = [dict(_order(side="LAY", matched=1.0, ref="old-lay"), betId="L0"),
              dict(_order(side="BACK", matched=1.0, ref=new_hedge_ref()), betId="H0"), _order()]
    bus, router = _run(_cmd(), orders=orders)
    assert len(router.calls) == 1, bus.last(CASHOUT_FAILED)
    bus, router = _run(_cmd(stake=1.0), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_same_side_matched_hedges_count_in_the_net_exposure():
    # l'esposizione e' quella reale sull'exchange: un hedge abbinato dello stesso lato pesa come ogni altro ordine
    from core.hedge_ref import new_hedge_ref
    orders = [dict(_order(side="LAY", matched=1.0, ref="old-lay"), betId="L0"),
              dict(_order(side="BACK", matched=1.0, ref=new_hedge_ref()), betId="H0")]
    bus, router = _run(_cmd(), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_assente"


@pytest.mark.parametrize("matched", [None, "", "x", "MISSING"])
def test_unreadable_matched_size_on_any_selection_order_never_places(matched):
    # Codex P1 su 73296a3: un LAY opposto con sizeMatched assente/non numerico diventava 0.0 e spariva dal netto
    # (la posizione poteva essere gia' coperta) => esposizione non verificabile, nessun hedge
    row = dict(_order(side="LAY", matched=0.23, ref="other"), betId="L1")
    if matched == "MISSING":
        row.pop("sizeMatched")
    else:
        row["sizeMatched"] = matched
    bus, router = _run(_cmd(), orders=[_order(), row])
    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_non_verificabile"


def test_declaring_the_opposite_position_side_never_doubles_the_real_position():
    # Grok su a16f0d3: original_pos.side e' nel payload. Posizione reale BACK 0.23@2; comando che dichiara LAY e manda
    # un hedge BACK (stesso lato della reale): il netto sul lato dichiarato e' negativo => rifiuto, mai un raddoppio
    bus, router = _run(_cmd(side="BACK", stake=0.23, price=2.0, orig_side="LAY"), orders=[_order()])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_assente"


def _covering_lay(**over):
    row = dict(_order(side="LAY", matched=0.23, ref="cover"), betId="C1")
    row.update(over)
    return row


@pytest.mark.parametrize("over", [{"side": None}, {"side": ""}, {"side": "X"}, {"side": 1}, {"side": "SIDE"}])
def test_unreadable_side_on_the_target_selection_never_places(over):
    # Codex P1 su 954c2cd: un LAY che gia' copre la posizione, con lato illeggibile, era scartato dal netto
    bus, router = _run(_cmd(), orders=[_order(), _covering_lay(**over)])
    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_non_verificabile"


def test_padded_side_is_normalised_and_counted():
    # " lay " e' un LAY: conta nel netto (la posizione e' gia' coperta), non sparisce
    bus, router = _run(_cmd(), orders=[_order(), _covering_lay(side=" lay ")])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_assente"


@pytest.mark.parametrize("over", [{"selectionId": None}, {"selectionId": ""}, {"marketId": None, "market_id": None},
                                  {"marketId": "  "}])
def test_unreadable_market_or_selection_identity_never_places(over):
    # stessa causa: una riga che non si puo' escludere con certezza dalla selezione non dimostra nulla
    bus, router = _run(_cmd(), orders=[_order(), _covering_lay(**over)])
    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_non_verificabile"
