"""Registro degli scenari G7: ID stabili e unici, requisiti validi, file esistenti dove il codice e' presente."""
from __future__ import annotations

import json
import re

import pytest

from tools.sim_report import run_g7


def test_registro_valido() -> None:
    reg = run_g7.load_registry()
    assert reg["schema"] == "pf-g7-scenarios/1"
    for sc in reg["scenarios"]:
        assert re.fullmatch(r"G7-S\d{2}", sc["id"]), sc
        assert sc["requirements"] and all(re.fullmatch(r"G7-R(0[1-9]|1[0-4])", r) for r in sc["requirements"]), sc
        assert all(isinstance(s, int) for s in sc["seeds"])
    covered = {r for sc in reg["scenarios"] for r in sc["requirements"]}
    assert covered == {f"G7-R{i:02d}" for i in range(1, 15)}, sorted(covered)


def test_id_duplicati_rifiutati(tmp_path) -> None:
    reg = run_g7.load_registry()
    reg["scenarios"].append(dict(reg["scenarios"][0]))
    p = tmp_path / "r.json"
    p.write_text(json.dumps(reg), encoding="utf-8")
    with pytest.raises(ValueError):
        run_g7.load_registry(str(p))


@pytest.mark.parametrize("rc,out,atteso", [
    (0, "3 passed in 1s", "PASS"),
    (1, "1 failed, 2 passed in 1s", "FAIL"),
    (0, "1 passed, 1 xfailed in 1s", "PASS"),
    (0, "1 xfailed in 1s", "XFAIL"),
    (0, "2 skipped in 0.1s", "NOT_TESTED"),
    (5, "no tests ran in 0.1s", "NOT_TESTED"),
])
def test_classificazione_degli_esiti(rc, out, atteso) -> None:
    assert run_g7.classify(rc, out) == atteso


def test_scenario_su_file_assente_e_not_tested() -> None:
    row = run_g7.run_scenario({"id": "G7-S99", "test": "tests/non/esiste.py", "requirements": ["G7-R01"], "seeds": [],
                               "requires": "x"})
    assert row["outcome"] == "NOT_TESTED"
