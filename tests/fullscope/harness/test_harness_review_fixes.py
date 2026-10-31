"""F-33 — rilievi Codex nell'harness. Su de1c980: quiete del bus, selezione dopo il riavvio, pipe del child.
Su 0d9e091: marker di checkpoint stantio, SHA del runtime con PF_TREE, oracolo e fill successivi dell'exchange.

Ogni test riproduce il difetto con il codice reale (EventBus, child runner, driver) e fallisce se torna.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from decimal import Decimal as D

from core.event_bus import EventBus
from tests.fullscope.harness import combi_runner as C
from tests.fullscope.harness import oracle as O
from tests.fullscope.harness import runner as H
from tests.fullscope.harness.events import bus_drained
from tests.fullscope.requirements import tracker as trk


def _wait(pred, s=5.0):
    t = time.time() + s
    while time.time() < t:
        if pred():
            return True
        time.sleep(0.01)
    return False


def test_bus_is_not_drained_while_a_dequeued_handler_is_still_running():
    """Riproduzione Codex: un evento con due subscriber, poi un handler bloccato. Coda vuota e contatori
    pubblicati/consegnati pari (2 == 2): il vecchio predicato dichiarava il bus fermo con l'handler in corso."""
    bus = EventBus(workers=2)
    release = threading.Event()
    entered = threading.Event()
    try:
        bus.subscribe("A", lambda d: None)
        bus.subscribe("A", lambda d: None)

        def blocked(_d):
            entered.set()
            release.wait(10)
        bus.subscribe("B", blocked)
        bus.publish("A", {})
        assert _wait(lambda: bus.delivered_total_count() == 2)
        bus.publish("B", {})
        assert entered.wait(5)
        old_predicate = bus.queue_depth() == 0 and bus.delivered_total_count() >= bus.published_total_count()
        assert old_predicate, "precondizione della riproduzione: il vecchio predicato e' vero qui"
        assert bus_drained(bus) is False, "bus dichiarato fermo con un handler ancora in esecuzione"
        release.set()
        assert _wait(lambda: bus_drained(bus)), "bus mai fermo dopo la fine dell'handler"
    finally:
        release.set()
        bus.stop()


def test_bus_without_pressure_snapshot_is_never_declared_drained():
    class NoSnapshot:
        def queue_depth(self):
            return 0
    assert bus_drained(NoSnapshot()) is False


def test_restart_settles_the_persisted_selection_not_the_default():
    """Riproduzione Codex: BACK abbinato sulla selezione 42, kill al checkpoint, riavvio, settle(win). Prima il
    book di chiusura usava la selezione 11: exchange UNSUPPORTED_RUNNER_STATUS, esposizione aperta, nessun
    settlement. Atteso: il mercato e' regolato sull'exchange dopo il riavvio."""
    home = tempfile.mkdtemp(prefix="pf_fullscope_sel_")
    first = H.run([{"op": "signal", "m": "1.951", "price": 2.0, "stake": 3.0, "sel": 42},
                   {"op": "checkpoint", "name": "after_signal", "hold_s": 10}],
                  home=home, kill_at_checkpoint="after_signal", scenario="review_sel_restart")
    assert first["_killed_at"] == "after_signal"
    assert any(o["market_id"] == "1.951" and int(o["selection_id"]) == 42 and float(o.get("matched") or 0) > 0
               for o in first["snaps"][-1]["exch_orders"]), first["snaps"][-1]["exch_orders"]
    second = H.run([{"op": "settle", "m": "1.951", "win": True}, {"op": "wait", "s": 1}],
                   home=home, scenario="review_sel_restart")
    esiti = [x["esito"] for x in second.get("settle_results", []) if x["m"] == "1.951"]
    assert esiti and "UNSUPPORTED_RUNNER_STATUS" not in str(esiti), esiti
    s = second["snaps"][-1]
    assert any(x["market_id"] == "1.951" for x in s["exch_settlements"]), "mercato non regolato dopo il riavvio"


def test_child_output_larger_than_a_pipe_buffer_does_not_block(tmp_path, monkeypatch):
    """Riproduzione Codex: il padre non leggeva le pipe durante il polling; un child con piu' output del buffer
    si bloccava in scrittura fino al timeout. Child minimo: 2 MB su stderr e stdout, poi il risultato."""
    child = tmp_path / "loud_child.py"
    child.write_text(textwrap.dedent('''
        import json, sys
        out = sys.argv[2]
        chunk = "x" * 1023 + "\\n"
        for _ in range(2048):
            sys.stderr.write(chunk)
            sys.stdout.write(chunk)
        sys.stderr.write("CODA-FINALE\\n")
        with open(out, "w") as fh:
            json.dump({"snaps": [], "events": []}, fh)
    '''))
    monkeypatch.setattr(H, "CHILD", str(child))
    monkeypatch.setattr(H, "PY", sys.executable)
    t0 = time.time()
    r = H.run([], timeout_s=30, scenario="review_loud_child")
    assert time.time() - t0 < 30
    assert r["_rc"] == 0
    assert r["_stderr_tail"].rstrip().endswith("CODA-FINALE")


def test_same_checkpoint_name_on_same_home_kills_each_run_at_its_own_checkpoint():
    """Riproduzione Codex: due run consecutivi con il checkpoint ``repeat`` sullo stesso HOME. Il marker del primo
    restava su disco e il secondo child veniva ucciso subito, senza risultato (rc=-9)."""
    home = tempfile.mkdtemp(prefix="pf_fullscope_repeat_")
    for n in range(2):
        r = H.run([{"op": "wait", "s": 0.2}, {"op": "checkpoint", "name": "repeat", "hold_s": 10}],
                  home=home, kill_at_checkpoint="repeat", scenario="review_repeat_ckpt")
        assert r["_killed_at"] == "repeat", (n, r.get("_killed_at"))
        assert r["snaps"][-1]["tag"].endswith("checkpoint:repeat"), (n, [x["tag"] for x in r["snaps"]])


def test_runtime_sha_is_the_checkout_selected_by_pf_tree(tmp_path, monkeypatch):
    """Riproduzione Codex: con ``PF_TREE`` su un altro checkout il record citava lo SHA dell'harness."""
    other = tmp_path / "other_checkout"
    other.mkdir()
    git = ["git", "-C", str(other), "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run(git[:3] + ["init", "-q"], check=True)
    (other / "f.txt").write_text("x")
    subprocess.run(git + ["add", "f.txt"], check=True)
    subprocess.run(git + ["commit", "-qm", "c"], check=True)
    other_sha = subprocess.run(git[:3] + ["rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    monkeypatch.setattr(H, "TREE", str(other))
    assert C.runtime_sha() == other_sha
    assert C.runtime_sha() != trk.head_sha()


def _partial_then_filled_snapshot():
    """Forma reale degli snapshot del child (riprodotta su 0d9e091): ledger con la risposta del piazzamento
    (matched 1), exchange con il fill cumulativo (matched 3) sullo stesso customer ref."""
    ref = "sig-f288cbe31377144ee5bc8b65602d"
    return {"orders": [{"market_id": "1.961", "side": "BACK", "stake": 3.0, "matched": 1.0, "price": 2.0,
                        "status": "SUBMITTED", "customer_ref": ref}],
            "exch_orders": [{"ref": ref, "market_id": "1.961", "side": "BACK", "price": 2.0, "size": 3.0,
                             "matched": 3.0, "avg_price": 2.0, "status": "EXECUTION_COMPLETE"}]}


def test_oracle_uses_cumulative_exchange_fill_for_a_ledger_leg():
    snap = _partial_then_filled_snapshot()
    comm = D("0.045")
    o = O.Oracle(snap, {"1.961": True}, start=D("100"), commission=comm, extra_legs=O.exchange_legs(snap))
    assert o.pnl_market("1.961") == D("3") * D("1") * (1 - comm)     # 2.865, come exchange e runtime
    assert snap["orders"][0]["matched"] == 1.0, "lo snapshot non deve essere modificato dall'oracolo"
    assert len(o.orders_of("1.961")) == 1, "la gamba dell'exchange non va contata due volte"


def test_oracle_uses_average_matched_price_when_reported():
    snap = _partial_then_filled_snapshot()
    snap["exch_orders"][0]["avg_price"] = 2.1
    o = O.Oracle(snap, {"1.961": True}, start=D("100"), commission=D(0), extra_legs=O.exchange_legs(snap))
    assert o.pnl_market("1.961") == D("3") * D("1.1")


def test_partial_then_filled_order_end_to_end_matches_runtime_realized():
    """Riproduzione Codex con il child reale: BACK 3 a 2.0 abbinato per 1, book aggiornato, settlement vincente.
    Runtime ed exchange realizzano 2.865; l'oracolo deve dare lo stesso valore."""
    r = H.run([{"op": "mode", "m": "1.961", "mode": "partial"},
               {"op": "signal", "m": "1.961", "price": 2.0, "stake": 3.0},
               {"op": "mode", "m": "1.961", "mode": "ok"},
               {"op": "book", "m": "1.961", "price": 3.0},
               {"op": "settle", "m": "1.961", "win": True}, {"op": "wait", "s": 1}],
              scenario="review_partial_fill")
    pre = [s for s in r["snaps"] if ":book:" in s["tag"]][-1]
    last = r["snaps"][-1]
    comm = D(str(last["sim_config"].get("commission_pct", 4.5))) / 100
    o = O.Oracle(last, {"1.961": True}, start=D("1000"), commission=comm, extra_legs=O.exchange_legs(pre))
    assert abs(D(str(last["realized"])) - o.pnl_market("1.961")) <= D("0.01"), (last["realized"], o.pnl_market("1.961"))
