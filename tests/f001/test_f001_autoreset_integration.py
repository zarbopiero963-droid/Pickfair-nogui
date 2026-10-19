"""PF-ALL-F024 sul percorso ROSERPINA_AUTO_RESET (HeadlessApp reale, ExecutorManager reale, SIM).

L'auto-reset da drawdown liberava tutti i tavoli e tutte le chiavi anti-duplicato, compresi quelli di
un ordine AMBIGUOUS gia' inviato all'exchange (osservato nel replay avversariale #510). Atteso:
la riserva incerta sopravvive al reset; il tavolo con esito certo viene liberato.
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TREE = os.environ.get("PF_TREE") or os.path.dirname(os.path.dirname(HERE))
PY = os.environ.get("PF_PY", sys.executable)


def test_autoreset_keeps_ambiguous_reservation():
    out = os.path.join(tempfile.mkdtemp(prefix="f024ar_"), "o.json")
    env = dict(os.environ)
    if os.environ.get("PF_NETGUARD"):
        env["PYTHONPATH"] = os.environ["PF_NETGUARD"]
    subprocess.run([PY, os.path.join(HERE, "child_autoreset.py"), TREE, out], env=env,
                   capture_output=True, text=True, timeout=180)
    r = json.load(open(out))
    assert "error" not in r, r.get("error")
    assert r["before"]["orders"] == {"1.811": "AMBIGUOUS", "1.812": "SUBMITTED"}, r["before"]
    assert r["auto_reset_events"] >= 1, "precondizione: l'auto-reset deve scattare"
    before = [t for t in r["before"]["tables"] if t[1] == "1.811"]
    held = [t for t in r["after"]["tables"] if t[1] == "1.811"]
    assert before and before[0][2] > 0
    assert held and held[0][2] == before[0][2], f"F024: tavolo dell'ordine incerto liberato dall'auto-reset {r['after']}"
    assert not [t for t in r["after"]["tables"] if t[1] == "1.812"], "il tavolo con esito certo e' liberato dal reset"
    assert r["ambiguous_key_still_held"] is True, "F024: chiave anti-duplicato dell'ordine incerto liberata"
