"""F-33 — plugin pytest del tracciatore dei requisiti (marker ``pf_requirement``).

Registrato dal ``conftest.py`` di ROOT (``pytest_plugins``) e non dal conftest annidato di ``tests/fullscope``: con
``pytest -n auto ... tests`` (``pr-check.yml``) il controller xdist carica solo i conftest dei path passati e dei loro
genitori, quindi un hook di aggregazione in ``tests/fullscope/conftest.py`` non girava mai nel controller e il report
non veniva scritto (rilievo Codex su a721e09).

Comportamento:
- registra i test DOPO la deselezione (``pytest_collection_finish``): un ``-m``/``-k`` non produce record "non
  eseguiti" che finirebbero nel report;
- senza xdist un solo processo scrive ``requirements_report.json``;
- con xdist ogni worker scrive solo i propri record in ``requirements_records.<worker>.json``; il controller li
  aggrega e scrive l'unico report, poi cancella i parziali;
- nessun report se nessun test ``pf_requirement`` e' stato eseguito: una sessione che non tocca l'harness (es. la
  passata ``-m serial``) non sovrascrive il report della passata precedente.

Import privo di effetti collaterali: nessun file e nessuna directory finche' non c'e' un test tracciato da scrivere.
"""
from __future__ import annotations

import glob
import json
import os

import pytest

from tests.fullscope.requirements import tracker as _trk

_TRACKER_KEY = "_pf_fullscope_tracker"


def _xdist_controller(config) -> bool:
    return not _trk.worker_id() and bool(getattr(config.option, "numprocesses", None))


def pytest_configure(config):
    setattr(config, _TRACKER_KEY, _trk.RequirementTracker())
    if _xdist_controller(config):
        # controller xdist: record parziali di una sessione precedente non devono entrare in questa
        out_dir = _trk.artifact_dir(_trk.head_sha(), create=False)
        for stale in glob.glob(_trk.partial_records_path(out_dir, "*")):
            try:
                os.remove(stale)
            except OSError:
                pass


def pytest_collection_finish(session):
    t = getattr(session.config, _TRACKER_KEY, None)
    if t is None:
        return
    for item in session.items:
        ids = []
        for mark in item.iter_markers(name="pf_requirement"):
            ids.extend(str(a) for a in mark.args)
        if ids:
            t.register(item.nodeid, ids)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    t = getattr(item.config, _TRACKER_KEY, None)
    if t is None:
        return
    t.record(item.nodeid, rep.when, rep.outcome, xfail=hasattr(rep, "wasxfail"))


def _executed(records) -> bool:
    return any(r.get("outcome") is not None for r in records.values())


def pytest_sessionfinish(session, exitstatus):
    t = getattr(session.config, _TRACKER_KEY, None)
    if t is None:
        return
    worker = _trk.worker_id()
    if worker:
        if _executed(t.records):
            out_dir = _trk.artifact_dir(_trk.head_sha())
            with open(_trk.partial_records_path(out_dir, worker), "w", encoding="utf-8") as fh:
                json.dump(t.records, fh)
        return
    sha = _trk.head_sha()
    partials = []
    if _xdist_controller(session.config):
        partials = sorted(glob.glob(_trk.partial_records_path(_trk.artifact_dir(sha, create=False), "*")))
    for path in partials:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                t.merge_records(json.load(fh))
        except (OSError, ValueError):
            # record di un worker illeggibili: il report lo dichiara invece di fingere copertura
            t.records.setdefault(f"<worker records illeggibili: {os.path.basename(path)}>",
                                 {"ids": [], "outcome": "error", "phases": {}})
    if not _executed(t.records):
        return
    out_dir = _trk.artifact_dir(sha)
    report = t.report(sha, _trk.seed())
    report["workers_merged"] = [os.path.basename(p) for p in partials]
    path = os.path.join(out_dir, "requirements_report.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=1, ensure_ascii=False)
    for p in partials:
        try:
            os.remove(p)
        except OSError:
            pass
    tr = session.config.pluginmanager.get_plugin("terminalreporter")
    if tr is not None:
        c = report["counts"]
        tr.write_line(f"[F-33] requisiti: catalogo {c['catalog']} · coperti {c['covered']} · tentati non coperti "
                      f"{c['attempted_not_covered']} · senza test {c['missing']} · id sconosciuti {c['unknown_ids']} "
                      f"-> {path}")
