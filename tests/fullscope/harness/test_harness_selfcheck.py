"""F-33 — autodiagnosi dell'harness: istanza isolata che parte e si ferma pulita, netguard attivo, SIGKILL esterno,
riavvio sullo stesso HOME con DB e stato exchange conservati, artifact scritti."""
import json
import os
import subprocess
import sys
import tempfile

import pytest

from tests.fullscope.harness import runner as H

pytestmark = [pytest.mark.fullscope, pytest.mark.integration]


def test_isolated_instance_starts_and_stops_clean():
    r = H.run([{"op": "wait", "s": 0.5}], scenario="selfcheck_start_stop")
    assert r["_rc"] == 0 and r["first_run"] is True
    assert "started': True" in r["start"] or '"started": true' in r["start"].lower(), r["start"]
    assert r["mode_after_start"] in ("ACTIVE", "RuntimeMode.ACTIVE"), r["mode_after_start"]
    assert r["stop"], "stop non eseguito"
    s = r["snaps"][0]
    assert s["funds"]["available"] == 100.0 and s["tables"] == [] and s["orders"] == []
    assert os.path.exists(os.path.join(r["_home"], "pickfair.db"))


def test_netguard_blocks_non_local_connections():
    env = dict(os.environ)
    env["PYTHONPATH"] = H.NETGUARD
    code = ("import socket\n"
            "try:\n"
            "    socket.create_connection(('93.184.216.34', 80), timeout=1)\n"
            "except OSError as e:\n"
            "    print('BLOCKED' if 'PF_NETGUARD' in str(e) else 'OTHER:' + str(e))\n"
            "else:\n"
            "    print('OPEN')\n")
    out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=30)
    assert out.stdout.strip() == "BLOCKED", out.stdout + out.stderr


def test_external_sigkill_at_checkpoint_then_restart_keeps_db_and_exchange_state():
    home = tempfile.mkdtemp(prefix="pf_fullscope_kill_")
    first = H.run([{"op": "signal", "m": "1.901", "price": 2.0, "stake": 3.0},
                   {"op": "checkpoint", "name": "after_signal", "hold_s": 10},
                   {"op": "wait", "s": 1}],
                  home=home, kill_at_checkpoint="after_signal", scenario="selfcheck_kill")
    assert first["_killed_at"] == "after_signal" and first["_rc"] not in (0,), first["_rc"]
    assert first["snaps"][-1]["tag"].endswith("checkpoint:after_signal")
    assert any(o["market_id"] == "1.901" for o in first["snaps"][-1]["orders"]), "ordine non nel ledger prima del kill"
    second = H.run([{"op": "wait", "s": 1}], home=home, scenario="selfcheck_kill")
    assert second["first_run"] is False
    s0 = second["snaps"][0]
    assert any(o["market_id"] == "1.901" for o in s0["orders"]), "ledger SQLite perso al riavvio"
    assert any(o["market_id"] == "1.901" for o in s0["exch_orders"]), "stato exchange SIM perso al riavvio"


def test_artifacts_written_with_sha_dir(pf_artifact_dir):
    r = H.run([{"op": "wait", "s": 0.2}], scenario="selfcheck_artifact")
    d = os.path.join(pf_artifact_dir, "scenarios")
    files = [f for f in os.listdir(d) if f.startswith("selfcheck_artifact_")]
    assert files, os.listdir(d)
    data = json.load(open(os.path.join(d, sorted(files)[-1])))
    assert data["_home"] == r["_home"] and "seed" in data
