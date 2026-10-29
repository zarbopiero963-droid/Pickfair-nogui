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
    t = getattr(session.config, _TRACKER_KEY, None)
    if t is None or not t.records:
        return
    sha = _trk.head_sha()
    report = t.report(sha, _trk.seed())
    out_dir = _trk.artifact_dir(sha)
    path = os.path.join(out_dir, "requirements_report.json")
    import json
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=1, ensure_ascii=False)
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
