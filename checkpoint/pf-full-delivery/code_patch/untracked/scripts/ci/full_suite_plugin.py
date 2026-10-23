"""Plugin pytest del check `full-suite` (PFC-002): registra cosa e' stato
raccolto e come e' finito ogni test, in un JSON che `full_suite_gate.py` giudica.

Serve a non fidarsi del solo exit code. Un `pytest` verde non dice se la
raccolta era completa, se qualcosa e' stato deselezionato o se un test raccolto
non ha mai prodotto un esito: e' esattamente il modo in cui una suite "verde"
puo' non aver eseguito i test che contano.

Il percorso del report arriva da `FULL_SUITE_REPORT`. Senza la variabile il
plugin non scrive nulla.
"""
from __future__ import annotations

import json
import os
import time

_STATO = {
    "collected": [],
    "deselected": [],
    "collect_errors": [],
    "outcomes": {},
    "started_at": time.time(),
}


def pytest_collectreport(report):
    if report.failed:
        _STATO["collect_errors"].append(str(report.nodeid or report.fspath))


def pytest_deselected(items):
    _STATO["deselected"].extend(item.nodeid for item in items)


def pytest_collection_finish(session):
    _STATO["collected"] = [item.nodeid for item in session.items]


def _esito(report) -> str | None:
    if hasattr(report, "wasxfail"):
        if report.skipped:
            return "xfailed"
        if report.passed:
            return "xpassed"
    if report.when == "call":
        return report.outcome  # passed / failed / skipped
    if report.failed:
        return "error"  # setup o teardown falliti
    if report.when == "setup" and report.skipped:
        return "skipped"
    return None


def pytest_runtest_logreport(report):
    esito = _esito(report)
    if esito is None:
        return
    precedente = _STATO["outcomes"].get(report.nodeid)
    # un errore di teardown dopo un "passed" resta un errore: vince il peggiore
    if precedente in ("failed", "error"):
        return
    _STATO["outcomes"][report.nodeid] = esito


def pytest_sessionfinish(session, exitstatus):
    percorso = os.environ.get("FULL_SUITE_REPORT")
    if not percorso:
        return
    dati = dict(_STATO)
    dati["exitstatus"] = int(exitstatus)
    dati["duration_s"] = round(time.time() - _STATO["started_at"], 1)
    with open(percorso, "w", encoding="utf-8") as fh:
        json.dump(dati, fh)
