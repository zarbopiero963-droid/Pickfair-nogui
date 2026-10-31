"""F-33 — conftest dell'harness full-scope: tracciabilita' dei requisiti e seed riproducibile.

Hook pytest (nessun plugin esterno): raccoglie i marker ``pf_requirement``, registra gli esiti reali e scrive il
report a fine sessione in ``art/fullscope/<sha>/requirements_report.json`` (o ``$PF_FULLSCOPE_ART``).
"""
from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tests.fullscope.requirements import tracker as _trk  # noqa: E402

_TRACKER_KEY = "_pf_fullscope_tracker"


def pytest_configure(config):
    setattr(config, _TRACKER_KEY, _trk.RequirementTracker())
    if not _trk.worker_id():
        # processo principale: record parziali di una sessione precedente non devono entrare in questa
        import glob
        for stale in glob.glob(_trk.partial_records_path(_trk.artifact_dir(_trk.head_sha()), "*")):
            try:
                os.remove(stale)
            except OSError:
                pass


def pytest_collection_modifyitems(config, items):
    t = getattr(config, _TRACKER_KEY, None)
    if t is None:
        return
    for item in items:
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
    xfail = hasattr(rep, "wasxfail")
    t.record(item.nodeid, rep.when, rep.outcome, xfail=xfail)


def pytest_sessionfinish(session, exitstatus):
    """Senza xdist: un solo processo scrive il report. Con xdist (``pr-check.yml``): ogni worker scrive SOLO i propri
    record in ``requirements_records.<worker>.json``; il controller, che finisce per ultimo, li aggrega e scrive l'unico
    ``requirements_report.json`` (rilievo Codex/Sol su d46e6da: i worker si sovrascrivevano lo stesso file)."""
    import glob
    import json
    t = getattr(session.config, _TRACKER_KEY, None)
    if t is None:
        return
    sha = _trk.head_sha()
    out_dir = _trk.artifact_dir(sha)
    worker = _trk.worker_id()
    if worker:
        if t.records:
            with open(_trk.partial_records_path(out_dir, worker), "w", encoding="utf-8") as fh:
                json.dump(t.records, fh)
        return
    partials = sorted(glob.glob(_trk.partial_records_path(out_dir, "*")))
    for path in partials:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                t.merge_records(json.load(fh))
        except (OSError, ValueError):
            # record di un worker illeggibile: il report lo dichiara invece di fingere copertura
            t.records.setdefault(f"<worker records illeggibili: {os.path.basename(path)}>",
                                 {"ids": [], "outcome": "error", "phases": {}})
    if not t.records:
        return
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


@pytest.fixture(scope="session")
def pf_seed() -> int:
    """Seed riproducibile per generatore e scenari (``PF_SEED``, default 0)."""
    return _trk.seed()


@pytest.fixture(scope="session")
def pf_artifact_dir() -> str:
    return _trk.artifact_dir(_trk.head_sha())
