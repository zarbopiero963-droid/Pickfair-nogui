"""close02 v21 — CL21-RACE-ADMIT-001: ammissioni concorrenti serializzate.

Fonte: L6 RACE (#461/6085510337) «più segnali/tavoli non possono perdere limiti, prenotazioni o identità della
configurazione»; owner Q3 (#426/6141450045). Il bus reale consegna con 4 worker (core/event_bus.py), quindi due
SIGNAL_RECEIVED possono essere elaborati insieme. RED su v20: con cap 1,00 due segnali da 0,60 passavano entrambi
e prendevano lo stesso tavolo (esposizione registrata 0,60 invece di 1,20). Interleaving forzato con una barriera
nel sizing: con la serializzazione il secondo segnale non arriva alla barriera finche' il primo non ha prenotato."""
import threading
from pathlib import Path

from controllers.dutching_controller import DutchingController
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal


def _chain(tmp_path, cap):
    c = _catena(Path(tmp_path))
    for k, v in {"max_open_exposure": cap, "max_order_exposure": 10.0, "max_market_exposure": 10.0,
                 "max_event_exposure_abs": 10.0, "max_drawdown_abs": 10.0}.items():
        setattr(c.rc.config, k, v)
    return c


def _barriera_nel_sizing(c, n):
    barrier = threading.Barrier(n, timeout=0.5)
    orig = c.rc.mm.calculate

    def calc(**kw):
        d = orig(**kw)
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass
        return d
    c.rc.mm.calculate = calc


def _segnali_concorrenti(c, n, stake=0.6):
    sigs = [_signal(stake=stake, bet_type="BACK", market_id=f"1.80000000{i}", selection_id=10 + i,
                    event=f"Evento {i}") for i in range(n)]
    ts = [threading.Thread(target=c.rc._on_signal_received, args=(s,)) for s in sigs]
    [t.start() for t in ts]
    [t.join(10) for t in ts]


def test_due_segnali_concorrenti_non_superano_il_cap(tmp_path):
    c = _chain(tmp_path, 1.0)
    _barriera_nel_sizing(c, 2)
    _segnali_concorrenti(c, 2)
    assert len(c.broker.state.orders) == 1
    assert any(r.startswith("max_open_exposure_exceeded") for r in _rifiuti(c)), _rifiuti(c)


def test_due_segnali_concorrenti_uguale_al_cap_prenotazioni_distinte(tmp_path):
    c = _chain(tmp_path, 1.2)
    _barriera_nel_sizing(c, 2)
    _segnali_concorrenti(c, 2)
    assert len(c.broker.state.orders) == 2
    occupati = [t for t in c.rc.table_manager._tables.values() if t.status != "FREE"]
    assert len(occupati) == 2 and abs(c.rc.table_manager.total_exposure() - 1.2) < 1e-9


def test_quattro_segnali_concorrenti_esposizione_mai_oltre_il_cap(tmp_path):
    c = _chain(tmp_path, 1.8)
    _barriera_nel_sizing(c, 4)
    _segnali_concorrenti(c, 4)
    assert len(c.broker.state.orders) == 3
    assert c.rc.table_manager.total_exposure() <= 1.8 + 1e-9


def test_reload_config_e_ammissione_usano_lo_stesso_lock(tmp_path):
    c = _chain(tmp_path, 1.0)
    lock = c.rc._risk_admission_lock
    preso = threading.Event()
    rilascia = threading.Event()

    def tieni():
        with lock:
            preso.set()
            rilascia.wait(5)
    t = threading.Thread(target=tieni)
    t.start()
    preso.wait(5)
    esito = {}
    s = threading.Thread(target=lambda: esito.setdefault("r", c.rc._on_signal_received(_signal(stake=0.5))))
    s.start()
    s.join(0.3)
    assert s.is_alive(), "l'ammissione deve attendere il lock tenuto da un altro thread"
    rilascia.set()
    t.join(5)
    s.join(5)
    assert len(c.broker.state.orders) == 1


def test_manuale_gui_concorrente_al_segnale_rispetta_il_cap(tmp_path):
    c = _chain(tmp_path, 1.0)
    _barriera_nel_sizing(c, 2)
    man = DutchingController(c.bus, c.rc)
    sig = _signal(stake=0.6, bet_type="BACK", market_id="1.800000001", selection_id=10)
    t1 = threading.Thread(target=c.rc._on_signal_received, args=(sig,))
    t2 = threading.Thread(target=man.manual_bet, args=({"market_id": "1.900000001", "selection_id": 20,
                                                        "price": 2.0, "stake": 0.6, "bet_type": "BACK",
                                                        "event_key": "man-race"},))
    t1.start(); t2.start(); t1.join(10); t2.join(10)
    assert len(c.broker.state.orders) <= 1, len(c.broker.state.orders)


def _attende_il_lock(c, chiamata):
    lock = c.rc._risk_admission_lock
    preso, rilascia = threading.Event(), threading.Event()

    def tieni():
        with lock:
            preso.set()
            rilascia.wait(5)
    t = threading.Thread(target=tieni)
    t.start()
    preso.wait(5)

    def esegui():
        try:
            chiamata()
        except Exception:
            pass  # interessa solo che la chiamata attenda il lock
    s = threading.Thread(target=esegui)
    s.start()
    s.join(0.3)
    in_attesa = s.is_alive()
    rilascia.set()
    t.join(5)
    s.join(5)
    return in_attesa


def test_reload_config_attende_il_lock_di_ammissione(tmp_path):
    c = _chain(tmp_path, 1.0)
    assert _attende_il_lock(c, lambda: c.rc.reload_config())


def test_auto_trade_attende_il_lock_di_ammissione(tmp_path):
    c = _chain(tmp_path, 1.0)
    assert _attende_il_lock(c, lambda: c.rc._evaluate_and_maybe_submit_auto_next_trade(payload={}, sync_result={}))


def test_execute_dutching_attende_il_lock_di_ammissione(tmp_path):
    c = _chain(tmp_path, 1.0)
    d = DutchingController(c.bus, c.rc)
    assert _attende_il_lock(c, lambda: d._execute_impl({"market_id": "1.234", "selections": []}))
