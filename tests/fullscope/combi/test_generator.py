"""F-33 — test del generatore combinatorio (tools/combi/generator.py) e dell'inventario delle 190 chiavi.

Nessun import dal runtime di produzione. Ogni test e' deterministico.
"""
import copy
import itertools
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.combi import generator as G  # noqa: E402

pytestmark = [pytest.mark.unit, pytest.mark.fullscope]


def _sample_params():
    return [
        G.Parameter("a", (1, 2, 3)),
        G.Parameter("b", ("x", "y")),
        G.Parameter("c", (None, 0.5, 10.0)),
        G.Parameter("d", (True, False)),
        G.Parameter("e", ("L", "M", "H")),
    ]


def _brute_force_check(report, params, constraints=()):
    """Controllo indipendente dal verificatore del modulo: ogni tupla t-way ammessa compare in un caso."""
    by = {p.name: p for p in params}
    for names in itertools.combinations(report.parameters, report.strength):
        for vals in itertools.product(*(by[n].values for n in names)):
            partial = dict(zip(names, vals))
            if not all(c(partial) for c in constraints):
                continue
            if G._complete(partial, params, constraints) is None:
                continue
            assert any(all(G._key(case[n]) == G._key(partial[n]) for n in names) for case in report.cases), \
                f"tupla scoperta: {partial}"


def test_pairwise_coverage_is_complete_for_sample_inventory():
    params = _sample_params()
    rep = G.generate(params, strength=2, seed=3)
    assert rep.tuples_covered == rep.tuples_required > 0
    assert rep.coverage_pct == 100.0
    _brute_force_check(rep, params)
    # molto meno del prodotto cartesiano (3*2*3*2*3 = 108)
    assert len(rep.cases) < 108 // 3


def test_threeway_coverage_report_matches_cases():
    params = _sample_params()
    rep = G.generate(params, strength=3, seed=11)
    assert rep.tuples_covered == rep.tuples_required
    _brute_force_check(rep, params)
    d = rep.to_dict()
    assert d["complete"] is True and d["n_cases"] == len(rep.cases) and d["strength"] == 3


def test_same_seed_same_cases_different_seed_may_differ():
    params = _sample_params()
    r1 = G.generate(params, strength=2, seed=42)
    r2 = G.generate(params, strength=2, seed=42)
    assert r1.cases == r2.cases
    assert r1.to_dict() == r2.to_dict()


def test_constraints_never_violated_and_excluded_tuples_counted():
    params = [G.Parameter("defense", (1.0, 5.0, 20.0)), G.Parameter("lockdown", (5.0, 20.0, 50.0)), G.Parameter("flag", (True, False))]
    lt = G._lt("defense", "lockdown")
    rep = G.generate(params, strength=2, seed=0, constraints=[lt], constraint_names=["defense<lockdown"])
    assert rep.tuples_excluded_by_constraints > 0
    for case in rep.cases:
        assert case["defense"] < case["lockdown"], case
    assert rep.tuples_covered == rep.tuples_required
    _brute_force_check(rep, params, [lt])
    assert G.verify(rep, params, [lt])["ok"] is True


def test_unsatisfiable_tuples_are_excluded_not_counted_as_required():
    # defense=100 non puo' mai stare sotto nessun lockdown: le coppie che lo contengono NON sono richieste
    params = [G.Parameter("other", (1, 2)), G.Parameter("defense", (5.0, 100.0)), G.Parameter("lockdown", (20.0, 50.0))]
    lt = G._lt("defense", "lockdown")
    rep = G.generate(params, strength=2, seed=0, constraints=[lt])
    assert all(c["defense"] == 5.0 for c in rep.cases)
    assert rep.coverage_pct == 100.0 and rep.tuples_covered == rep.tuples_required


def test_verify_detects_tampered_report():
    params = _sample_params()
    rep = G.generate(params, strength=2, seed=1)
    tampered = copy.deepcopy(rep)
    tampered.cases = tampered.cases[: max(1, len(tampered.cases) // 3)]
    check = G.verify(tampered, params)
    assert check["ok"] is False and check["missing_tuples"]


def test_parameter_validation():
    with pytest.raises(ValueError):
        G.Parameter("a", (1,))
    with pytest.raises(ValueError):
        G.Parameter("a", (1, 1))
    with pytest.raises(ValueError):
        G.generate(_sample_params(), strength=9)


def test_inventory_counts_and_classes():
    inv = G.load_inventory()
    assert inv["count"] == len(inv["parameters"]) == 190
    classes = {}
    for p in inv["parameters"]:
        classes[p["test_class"]] = classes.get(p["test_class"], 0) + 1
    assert classes == {"ATTIVA": 139, "LEGACY": 51}
    assert sum(1 for p in inv["parameters"] if p["roserpina42"]) == 42
    keys = [p["key"] for p in inv["parameters"]]
    assert len(set(keys)) == 190
    for p in inv["parameters"]:
        assert p["l1_mode"] in {"settings_roundtrip", "load_only", "static_or_manual"}
        if p["l1_mode"] == "settings_roundtrip":
            assert p["test_class"] == "ATTIVA"
            assert len(p["domain"].get("valid") or []) >= 2, p["key"]


@pytest.mark.parametrize("group", sorted(G.GROUPS))
def test_every_group_builds_complete_verified_report(group):
    inv = G.load_inventory()
    rep = G.build_group(group, inv, seed=7)
    assert rep.coverage_pct == 100.0, rep.to_dict()
    assert rep.inventory_sha256 and len(rep.inventory_sha256) == 64
    ids = [i for i, _c in G.pytest_params(rep)]
    assert len(ids) == len(set(ids)) == len(rep.cases)


def test_cli_writes_report_and_is_reproducible(tmp_path):
    out1 = tmp_path / "r1.json"
    out2 = tmp_path / "r2.json"
    assert G.main(["--group", "c_esecuzione_sim", "--seed", "5", "--out", str(out1)]) == 0
    assert G.main(["--group", "c_esecuzione_sim", "--seed", "5", "--out", str(out2)]) == 0
    d1, d2 = json.load(open(out1)), json.load(open(out2))
    assert d1 == d2 and d1["complete"] is True and d1["schema"] == "pf-combi-coverage/1"
