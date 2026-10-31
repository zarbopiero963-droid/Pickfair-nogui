"""F-33 — rilievi Codex su de1c980 nell'harness: quiete del bus, selezione dopo il riavvio, pipe del child.

Ogni test riproduce il difetto con il codice reale (EventBus, child runner, driver) e fallisce se torna.
"""
from __future__ import annotations

import sys
import tempfile
import textwrap
import threading
import time

from core.event_bus import EventBus
from tests.fullscope.harness import runner as H
from tests.fullscope.harness.events import bus_drained


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
