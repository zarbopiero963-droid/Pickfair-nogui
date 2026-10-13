"""close02 (Claude) — L6 RACE: ammissioni concorrenti non devono superare i limiti di rischio.
Fonte (phase_row_verbatim L6 RACE, #461/6085510337): «Una modifica concorrente o più segnali/tavoli non possono
perdere limiti, prenotazioni o identità della configurazione»; owner Q3 (#426/6141450045, cap: uguale ammesso,
oltre bloccato). Il bus reale (core/event_bus.py) consegna con 4 worker: due SIGNAL_RECEIVED possono girare insieme.
Interleaving forzato in modo deterministico: una barriera dentro il sizing tiene il primo segnale fra la lettura
dell'esposizione e la prenotazione finche' il secondo non ha letto la stessa esposizione. Nessuna rete."""
import json, os, sys, tempfile, threading
from pathlib import Path
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import logging; logging.disable(logging.CRITICAL)
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402


def run(cap_key, cap, stake, n=2, interleave=True):
    c = _catena(Path(tempfile.mkdtemp()))
    for k, v in {"max_open_exposure": 10.0, "max_order_exposure": 10.0, "max_market_exposure": 10.0,
                 "max_event_exposure_abs": 10.0, "max_drawdown_abs": 10.0}.items():
        setattr(c.rc.config, k, v)
    setattr(c.rc.config, cap_key, cap)
    barrier = threading.Barrier(n, timeout=2.0)
    orig = c.rc.mm.calculate

    def calc(**kw):
        d = orig(**kw)
        if interleave:
            try:
                barrier.wait()  # tutti hanno letto l'esposizione prima che qualcuno prenoti
            except threading.BrokenBarrierError:
                pass  # serializzazione reale nell'albero: il secondo non arriva qui finche' il primo non esce
        return d
    c.rc.mm.calculate = calc
    sigs = [_signal(stake=stake, bet_type="BACK", market_id=f"1.80000000{i}", selection_id=10 + i,
                    event=f"Evento {i}") for i in range(n)]
    ts = [threading.Thread(target=c.rc._on_signal_received, args=(s,)) for s in sigs]
    [t.start() for t in ts]; [t.join(10) for t in ts]
    total = float(c.rc.table_manager.total_exposure())
    return {"ordini": len(c.broker.state.orders), "esposizione_tavoli": total, "rifiuti": _rifiuti(c)}


out = {}
for cid, key, cap, stake, exp_max in (
        ("RACE-OPEN", "max_open_exposure", 1.0, 0.6, 1),     # 0.6+0.6 > 1.0: al massimo uno
        ("RACE-OPEN-EQ", "max_open_exposure", 1.2, 0.6, 2),  # 0.6+0.6 = 1.2: entrambi ammessi (Q3)
):
    o = run(key, cap, stake)
    ok = o["ordini"] <= exp_max and o["esposizione_tavoli"] <= cap + 1e-9 if exp_max == 1 else o["ordini"] == 2
    out[cid] = {"cap": {key: cap}, "stake_per_segnale": stake, "atteso_max_ordini": exp_max, "osservato": o,
                "verdetto": "CONFORME" if ok else "NON_CONFORME"}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
for k, v in out.items():
    print(k, v["verdetto"], v["osservato"]["ordini"], "ordini, esposizione", v["osservato"]["esposizione_tavoli"])
