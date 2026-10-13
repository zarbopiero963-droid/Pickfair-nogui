"""close02 v24 — CL26-CASH-PARTIAL-001: un hedge di cashout abbinato solo in parte non e' un cashout completato.

Fonti (#461/6085516609, righe verbatim): «PF-CASH-14 | Fill completo verificato», «PF-CASH-15 | Fill parziale e
residuo protetto»; V2 «parziale→contabilizzare solo eseguito e mantenere residuo». RED su v23 (e main/#505):
hedge LAY 2.86 abbinato per 1.00 => CASHOUT_SUCCESS status DONE con green_up intero (-7.15), e quindi broadcast
«CASHOUT ESEGUITO» ai follower (telegram_sender._on_cashout_success)."""
import pytest

from cashout_executor import CashoutExecutor
from cashout_residual_handler import CashoutResidualHandler
from database import Database

CMD = {"market_id": "1.230000777", "selection_id": 1222347, "side": "LAY", "price": 7.0, "stake": 2.86,
       "green_up": -7.15, "source": "TELEGRAM", "event_name": "A v B"}


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_a):
        pass

    def publish(self, name, payload=None):
        self.events.append((name, payload))


class _Router:
    def __init__(self, matched):
        self.matched = matched

    def place(self, _p):
        return {"ok": self.matched > 0, "placed": True, "order_unknown": False, "status": "SUCCESS",
                "matched": self.matched, "bet_id": "900000000555", "error": None, "raw": {}}


def _run(matched, ledger=None):
    bus = _Bus()
    CashoutExecutor(bus, _Router(matched), ledger=ledger).on_cmd_execute_cashout(dict(CMD))
    return bus.events


def test_parziale_non_e_un_successo_ne_un_done():
    ev = _run(1.0)
    assert [n for n, _ in ev if n == "CASHOUT_SUCCESS"] == []
    failed = [p for n, p in ev if n == "CASHOUT_FAILED"]
    assert failed and failed[0]["status"] == "PARTIAL"
    assert failed[0]["matched"] == 1.0 and failed[0]["remaining"] == pytest.approx(1.86)
    assert failed[0]["bet_id"] == "900000000555"


def test_completo_resta_successo():
    ev = _run(2.86)
    succ = [p for n, p in ev if n == "CASHOUT_SUCCESS"]
    assert succ and succ[0]["status"] == "DONE" and succ[0]["green_up"] == -7.15


@pytest.mark.parametrize("matched", [2.855, 2.86])
def test_bordo_al_centesimo(matched):
    # 2.855 arrotondato al centesimo e' 2.86: abbinato per intero; nessun falso parziale per rumore float
    assert [n for n, _ in _run(matched) if n == "CASHOUT_SUCCESS"]


def test_ledger_registra_parziale(tmp_path):
    db = Database(str(tmp_path / "l.sqlite"))
    _run(1.0, ledger=db)
    rows = db._execute("SELECT status FROM orders", fetch=True, commit=False)
    assert [r["status"] for r in rows] == ["PARTIAL"]
    assert [o["bet_id"] for o in db.get_bot_active_orders()] == ["900000000555"]  # resta nell'identita' del bot


def test_gestore_residuo_notifica_alta_e_non_cancella_l_hedge():
    notes, cancels, records = [], [], []
    h = CashoutResidualHandler(cancel=lambda m, ids: cancels.append((m, ids)) or True,
                               persist=records.append, notify=lambda t, severity="HIGH": notes.append((t, severity)))
    h.on_cashout_failed({"status": "PARTIAL", "reason": "partial_fill", "bet_id": "900000000555",
                         "market_id": "1.230000777", "matched": 1.0, "remaining": 1.86})
    assert cancels == []  # l'hedge residuo a riposo riduce il rischio se si abbina: non si cancella alla cieca
    assert notes and notes[0][1] == "HIGH" and "PARTIAL" in notes[0][0]
    assert records and records[0]["status"] == "PARTIAL"
    assert records[0]["matched"] == 1.0 and records[0]["remaining"] == 1.86
