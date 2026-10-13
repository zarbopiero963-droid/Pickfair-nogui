"""close02 v19 — CL18-CASH-LEDGER-001 e CL19-CASH-REF-001 (hedge del cashout).

CL18: l'hedge piazzato da CashoutExecutor -> OrderRouter non entrava nel ledger ordini del bot, quindi il
poller LIVE dei settlement scartava la sua riga cleared: P/L realizzato +10 invece di -50/7 (fonte
#461/6085516609 V1 'fill completo->riconciliare payoff netto', PF-CASH-21; #506/6124429007 P&L realizzato
solo da listClearedOrders). Correzione: scrittura del ledger PRIMA dell'invio (INFLIGHT), aggiornata con
l'esito e il betId; scrittura fallita => nessun ordine (fail-closed).

CL19: il customerRef dell'hedge era il campo `source` ("TELEGRAM"/"UI"): stesso ref per ogni cashout, de-dup
Betfair di 60 s (#506 BF2-D001) => un CASHOUT_ALL su piu' mercati poteva perdere gli hedge successivi.
Correzione: ref deterministico per intento (mercato, selezione, lato, prezzo, stake, evento), conforme."""
import inspect
from pathlib import Path

import pytest

import cashout_wiring
from cashout_executor import CashoutExecutor
from core.order_identity import is_betfair_customer_ref
from database import Database
from tests.core.test_settlement_poller import _closes, _controller

M = "1.230000777"
CMD = {"market_id": M, "selection_id": 1222347, "side": "LAY", "price": 7.0, "stake": 2.86,
       "green_up": -7.15, "source": "TELEGRAM", "event_name": "Squadra A v Squadra B", "event_key": "ek-1"}
ROOT = Path(__file__).resolve().parents[2]


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_a):
        pass

    def publish(self, name, payload=None):
        self.events.append((name, payload))


class _Router:
    def __init__(self, result=None, exc=None):
        self.calls, self.result, self.exc = [], result, exc

    def place(self, payload):
        self.calls.append(dict(payload))
        if self.exc:
            raise self.exc
        return dict(self.result)


MATCHED = {"ok": True, "placed": True, "order_unknown": False, "status": "SUCCESS", "matched": 2.86,
           "bet_id": "900000000123", "error": None, "raw": {}}


def _rows(db):
    return db._execute("SELECT customer_ref, status, payload_json, response_json FROM orders",
                       fetch=True, commit=False) or []


def _exec(db, router):
    bus = _Bus()
    ex = CashoutExecutor(bus, router, ledger=db)
    ex.on_cmd_execute_cashout(dict(CMD))
    return bus


def test_hedge_registrato_nel_ledger_bot_con_bet_id(tmp_path):
    db = Database(str(tmp_path / "l.sqlite"))
    _exec(db, _Router(MATCHED))
    bot = db.get_bot_active_orders()
    assert {"bet_id": "900000000123", "market_id": M} == {k: bot[0][k] for k in ("bet_id", "market_id")}


def test_poller_live_realizza_anche_l_hedge_payoff_netto(tmp_path):
    db = Database(str(tmp_path / "l.sqlite"))
    _exec(db, _Router(MATCHED))
    ledger = db.get_bot_active_orders() + [{"bet_id": "101", "market_id": M, "event_name": "A v B"}]
    rows = [{"betId": "101", "marketId": M, "profit": 10.0, "commission": 0.0, "settledDate": "2026-10-13T10:00:00Z"},
            {"betId": "900000000123", "marketId": M, "profit": -17.16, "commission": 0.0,
             "settledDate": "2026-10-13T10:00:00Z"}]
    rc = _controller(results=[rows], bot_orders=ledger)
    rc._poll_cleared_settlements()
    gross = round(sum(float(p["gross_pnl"]) for p in _closes(rc)), 2)
    assert gross == -7.16  # +10 - 6*2.86 (hedge arrotondato al tick 2.86)


def test_scrittura_ledger_fallita_nessun_ordine(tmp_path):
    class _Rotto:
        def insert_order(self, _p):
            raise OSError("disk I/O error")

    router, bus = _Router(MATCHED), _Bus()
    CashoutExecutor(bus, router, ledger=_Rotto()).on_cmd_execute_cashout(dict(CMD))
    assert router.calls == []
    fails = [p for n, p in bus.events if n == "CASHOUT_FAILED"]
    assert fails and fails[0]["status"] == "REJECTED" and "ledger" in fails[0]["reason"]


@pytest.mark.parametrize("router,stato", [
    (_Router(MATCHED), "MATCHED"),
    (_Router({**MATCHED, "ok": False, "placed": False, "order_unknown": True, "bet_id": None, "matched": 0.0}),
     "AMBIGUOUS"),
    (_Router(exc=TimeoutError("read timeout")), "AMBIGUOUS"),
    (_Router({**MATCHED, "ok": False, "matched": 0.0}), "PLACED"),
    (_Router({**MATCHED, "ok": False, "placed": False, "bet_id": None, "matched": 0.0, "error": "INSUFFICIENT"}),
     "FAILED"),
])
def test_esito_registrato_mai_lasciato_inflight(tmp_path, router, stato):
    db = Database(str(tmp_path / "l.sqlite"))
    _exec(db, router)
    rows = _rows(db)
    assert len(rows) == 1 and rows[0]["status"] == stato


def test_customer_ref_per_intento_conforme_e_non_source(tmp_path):
    db = Database(str(tmp_path / "l.sqlite"))
    router = _Router(MATCHED)
    ex = CashoutExecutor(_Bus(), router, ledger=db)
    ex.on_cmd_execute_cashout(dict(CMD))
    ex.on_cmd_execute_cashout(dict(CMD, market_id="1.230000888"))  # CASHOUT_ALL: altro mercato
    ex.on_cmd_execute_cashout(dict(CMD))  # stesso intento ripetuto
    refs = [c["customer_ref"] for c in router.calls]
    assert all(is_betfair_customer_ref(r) and r not in ("TELEGRAM", "UI") for r in refs), refs
    assert refs[0] != refs[1] and refs[0] == refs[2]
    assert [r["customer_ref"] for r in _rows(db)] == refs  # wire e ledger allineati


def test_wiring_passa_il_ledger_agli_entrypoint():
    bus = _Bus()

    class _Svc:
        pass

    led = object()
    catena = cashout_wiring.cabla_catena_cashout(bus=bus, betfair_service=_Svc(), persist=lambda r: None,
                                                 notify=lambda *a, **k: None, ledger=led)
    assert catena.cashout_executor.ledger is led
    for f in ("headless_main.py", "mini_gui.py"):
        src = (ROOT / f).read_text(encoding="utf-8")
        i = src.index("cabla_catena_cashout(")
        assert "ledger=self.db" in src[i:i + 600], f


def test_red_api_esistente_customer_ref_non_e_il_source():
    # RED su v18 senza nuova API: oggi il ref sul wire e' 'TELEGRAM' per ogni cashout.
    router = _Router(MATCHED)
    ex = CashoutExecutor(_Bus(), router)
    ex.on_cmd_execute_cashout(dict(CMD))
    ex.on_cmd_execute_cashout(dict(CMD, market_id="1.230000888"))
    refs = [c["customer_ref"] for c in router.calls]
    assert refs[0] != refs[1] and "TELEGRAM" not in refs, refs
