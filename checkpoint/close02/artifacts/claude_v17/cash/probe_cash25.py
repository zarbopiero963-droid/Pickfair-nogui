"""close02 (Claude) — sonda dinamica PF-CASH-25: fencing contro ingressi in volo per tutto lo scope cashout.

Fonte: #461/6085516609 (sha256 56fc7836...), riga PF-CASH-25 e paragrafo «Dopo richiesta cashout» (V2, V5).
Catena reale (_catena: RuntimeController + MM vero + SimulationBroker) dell'albero PROBE_ROOT. Nessuna rete.
Il cashout "in corso" e' rappresentato dalla barriera del runtime (_begin_cashout_barrier/_end_cashout_barrier)
quando esiste; se l'albero non ha alcun meccanismo di fencing il caso e' NON_CONFORME per assenza."""
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = os.environ["PROBE_ROOT"]
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402

CFG = {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0,
       "max_event_exposure_abs": 2.0, "max_drawdown_abs": 10.0}


def chain():
    c = _catena(Path(tempfile.mkdtemp()))
    for k, v in CFG.items():
        setattr(c.rc.config, k, v)
    return c


def has_fencing(rc):
    return all(callable(getattr(rc, n, None)) for n in ("_begin_cashout_barrier", "_end_cashout_barrier",
                                                         "_capture_admission_fence"))


def obs(c):
    return {"orders": len(c.broker.state.orders), "rejects": _rifiuti(c)}


def entry(**kw):
    s = _signal(stake=0.5, bet_type="BACK")
    s.update(kw)
    return s


out = {}
base = entry()
M, S = base["market_id"], base["selection_id"]


def record(cid, desc, expected, o, note=""):
    admitted = o.get("orders") == 1 and not o.get("rejects")
    ok = admitted if expected == "AMMESSO" else (o.get("orders") == 0 and bool(o.get("rejects")))
    out[cid] = {"descrizione": desc, "atteso": expected, "osservato": o, "nota": note,
                "verdetto": ("CONFORME" if ok else "NON_CONFORME") if o.get("fencing", True) else "NON_CONFORME"}


# CTRL: nessun cashout in corso => ingresso ammesso
c = chain(); c.rc._on_signal_received(entry())
record("CASH-25-CTRL", "nessun cashout in corso", "AMMESSO", obs(c))

# R1: cashout in corso sullo scope mercato => ingresso stesso mercato bloccato
c = chain()
if has_fencing(c.rc):
    bid, scope = c.rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": M})
    c.rc._on_signal_received(entry())
    o = obs(c); o["scope_barriera"] = sorted(map(list, scope)); c.rc._end_cashout_barrier(bid)
else:
    o = {"fencing": False, "orders": None, "rejects": [], "motivo": "nessun meccanismo di fencing nell'albero"}
record("CASH-25-R1", "cashout in corso (scope mercato) + ingresso stesso mercato", "BLOCCATO", o)

# R1b: cashout su (mercato, selezione) + ingresso stesso mercato, ALTRA selezione (oracolo: scope = mercato del tavolo)
c = chain()
if has_fencing(c.rc):
    bid, scope = c.rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": M, "selection_id": S})
    other = entry(selection_id=int(S) + 1 if str(S).isdigit() else 99)
    c.rc._on_signal_received(other)
    o = obs(c); o["scope_barriera"] = sorted(map(list, scope)); c.rc._end_cashout_barrier(bid)
else:
    o = {"fencing": False, "orders": None, "rejects": []}
record("CASH-25-R1b", "cashout su selezione + ingresso altra selezione stesso mercato", "BLOCCATO", o,
       "oracolo CASH-25-R1: scope del tavolo = mercato; la barriera per selezione non copre le altre selezioni")

# R2: ingresso accodato PRIMA del cashout, eseguito dopo l'inizio/fine della barriera
c = chain()
if has_fencing(c.rc):
    sig = entry()
    fence = c.rc._capture_admission_fence(sig)
    bid, _ = c.rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": M})
    c.rc._end_cashout_barrier(bid)
    lock = getattr(c.rc, "_risk_admission_lock", None)
    if lock is not None:
        lock.acquire()  # come nel percorso reale: il chiamante tiene il lock, il serial lo rilascia
    c.rc._on_signal_received_serial(sig, admission_fence=fence)
    o = obs(c)
else:
    o = {"fencing": False, "orders": None, "rejects": []}
record("CASH-25-R2", "ingresso accodato prima del cashout, eseguito dopo", "BLOCCATO", o)

# R3: cashout concluso con hedge parziale (residuo aperto) => fencing finche' lo scope ha residuo aperto
c = chain()
if has_fencing(c.rc):
    bid, _ = c.rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": M})
    t = c.rc.table_manager._tables[2]  # residuo dell'hedge parziale ancora aperto sul mercato
    t.status = "ACTIVE"; t.market_id = str(M); t.current_exposure = 0.5
    t.current_event_key = c.rc.duplication_guard.build_event_key(entry())
    c.rc._end_cashout_barrier(bid)  # la richiesta di cashout e' terminata (parziale)
    c.rc._on_signal_received(entry())
    o = obs(c)
else:
    o = {"fencing": False, "orders": None, "rejects": []}
record("CASH-25-R3", "cashout concluso parziale, residuo aperto, nuovo ingresso nello scope", "BLOCCATO", o,
       "oracolo: il fencing resta finche' lo scope ha residuo aperto")

json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False, default=str)
for k, v in out.items():
    print(k, v["verdetto"], v["osservato"].get("orders"), (v["osservato"].get("rejects") or [""])[0][:40])
