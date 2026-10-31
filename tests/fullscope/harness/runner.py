"""F-33 — driver del child runner: istanza Pickfair isolata per scenario, crash esterno (SIGKILL), riavvio, artifact.

- ``run(steps, home=..., settings=..., seed=..., kill_at_checkpoint=...)`` esegue il child in un HOME dedicato (DB
  SQLite e stato SIM del broker in quella directory); un secondo ``run`` sullo stesso ``home`` e' un RIAVVIO reale
  (stesso DB, stesso stato exchange persistito, nuovo processo).
- ``kill_at_checkpoint="X"``: appena il child scrive ``HOME/.ckpt_X`` il padre lo uccide (``Popen.kill``: SIGKILL su
  POSIX, TerminateProcess su Windows): crash esterno vero, non un ``os._exit`` cooperativo.
- Il sottoprocesso gira con il netguard (``PYTHONPATH`` -> sitecustomize): nessuna connessione non locale.
- Ogni risultato viene salvato come artifact JSON in ``art/fullscope/<sha>/scenarios/`` (o ``$PF_FULLSCOPE_ART``).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
TREE = os.environ.get("PF_TREE") or os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
PY = os.environ.get("PF_PY", sys.executable)
CHILD = os.path.join(HERE, "child_runner.py")
NETGUARD = os.path.join(HERE, "netguard")

# PFC-008: le percentuali di rischio non hanno default di codice; l'harness le dichiara (valori storici 3/18/35/18).
BASE_SETTINGS: Dict[str, Any] = {
    "roserpina.target_profit_cycle_pct": 3.0, "roserpina.max_single_bet_pct": 18.0,
    "roserpina.max_total_exposure_pct": 35.0, "roserpina.max_event_exposure_pct": 18.0,
    "roserpina.lockdown_drawdown_pct": 60.0,
}


def _artifact_dir() -> str:
    from tests.fullscope.requirements import tracker as trk  # import tardivo: solo per il path
    d = os.path.join(trk.artifact_dir(trk.head_sha()), "scenarios")
    os.makedirs(d, exist_ok=True)
    return d


def run(steps: List[Dict[str, Any]], *, home: Optional[str] = None, settings: Optional[Dict[str, Any]] = None,
        seed: int = 0, requirements: Optional[List[str]] = None, kill_at_checkpoint: Optional[str] = None,
        timeout_s: float = 300.0, scenario: str = "scenario", **flags: Any) -> Dict[str, Any]:
    home = home or tempfile.mkdtemp(prefix="pf_fullscope_")
    out = os.path.join(tempfile.mkdtemp(prefix="pf_fullscope_out_"), "result.json")
    spec = {"steps": steps, "settings": dict(BASE_SETTINGS, **(settings or {})), "seed": int(seed),
            "requirements": list(requirements or []), **flags}
    env = dict(os.environ)
    env["PYTHONPATH"] = NETGUARD + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    t0 = time.time()
    killed_at: Optional[str] = None
    marker = os.path.join(home, f".ckpt_{kill_at_checkpoint}") if kill_at_checkpoint else None
    if marker and os.path.exists(marker):
        # marker di un run precedente sullo stesso HOME: va tolto PRIMA di avviare il child, altrimenti il nuovo
        # processo verrebbe ucciso subito invece che al checkpoint (rilievo Codex su 0d9e091)
        os.remove(marker)
    # stdout/stderr su file e non su PIPE: il padre non legge le pipe durante il polling, e un child che scrive piu'
    # del buffer di una pipe si bloccava fino al timeout (rilievo Codex su de1c980)
    out_dir = os.path.dirname(out)
    stdout_f = open(os.path.join(out_dir, "child.stdout"), "w+")
    stderr_f = open(os.path.join(out_dir, "child.stderr"), "w+")
    proc = subprocess.Popen([PY, CHILD, TREE, out, json.dumps(spec), home], env=env,
                            stdout=stdout_f, stderr=stderr_f, text=True)
    try:
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            if marker and os.path.exists(marker):
                # Crash esterno non cooperativo: SIGKILL su POSIX, TerminateProcess su Windows (rilievo Grok:
                # `signal.SIGKILL` non esiste su Windows). In entrambi i casi il child non esegue nulla dopo.
                proc.kill()
                killed_at = kill_at_checkpoint
                proc.wait(timeout=30)
                break
            if time.time() - t0 > timeout_s:
                proc.kill()
                proc.wait(timeout=30)
                raise TimeoutError(f"child runner oltre {timeout_s}s (scenario {scenario})")
            time.sleep(0.05)
    finally:
        try:
            proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            pass
        stderr = _read_and_close(stderr_f)
        _read_and_close(stdout_f)
    if not os.path.exists(out):
        raise RuntimeError(f"nessun risultato dal child (rc={proc.returncode}, killed_at={killed_at}):\n{stderr[-3000:]}")
    with open(out, "r") as fh:
        r = json.load(fh)
    r["_rc"] = proc.returncode
    r["_home"] = home
    r["_killed_at"] = killed_at
    r["_duration_s"] = round(time.time() - t0, 3)
    r["_stderr_tail"] = stderr[-1500:]
    with _open_new_artifact(_artifact_dir(), scenario) as fh:
        json.dump(r, fh, indent=1, default=str)
    if killed_at is None:
        assert "error" not in r, r.get("error")
    return r


def _read_and_close(fh) -> str:
    try:
        fh.flush()
        size = fh.seek(0, os.SEEK_END)
        fh.seek(max(0, size - 8000))       # basta la coda: il risultato cita al massimo gli ultimi 3000 caratteri
        return fh.read()
    except Exception:  # noqa: BLE001
        return ""
    finally:
        fh.close()


def _open_new_artifact(art: str, scenario: str, limit: int = 100000):
    """Apre in creazione ESCLUSIVA il primo ``<scenario>_NN.json`` libero: due worker xdist con lo stesso scenario non
    possono scegliere lo stesso nome e sovrascriversi (rilievo Codex su a721e09: conteggio + open non atomici)."""
    for n in range(limit):
        path = os.path.join(art, f"{scenario}_{n:02d}.json")
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            continue
        return os.fdopen(fd, "w")
    raise RuntimeError(f"nessun nome di artifact libero per lo scenario {scenario} in {art}")


def events(r: Dict[str, Any], name: str, market: Optional[str] = None) -> List[Dict[str, Any]]:
    return [e for e in r.get("events", []) if e["e"] == name and (market is None or str(e.get("market_id")) == str(market))]
