"""PFC-001: i test che usano widget Tk reali sul display X condiviso sono marcati ``serial``.

Il marker serve all'esecuzione parallela (xdist): un passo seriale separato esegue ``-m serial``, quello
parallelo ``-m "not serial"``, e l'unione resta l'intera suite. Questo guard impedisce che un nuovo test
con un display reale finisca in parallelo senza marker, e che il marker sparisca dalla configurazione.
"""
import ast
import configparser
from pathlib import Path

import pytest

pytestmark = pytest.mark.guardrail

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
SELF = Path(__file__).resolve()


def _is_serial_mark(node: ast.AST) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    return isinstance(target, ast.Attribute) and target.attr == "serial"


def _uses_real_display(func: ast.FunctionDef) -> bool:
    for node in ast.walk(func):
        if isinstance(node, ast.Constant) and node.value == "DISPLAY":
            return True
    return False


def _module_is_serial(tree: ast.Module) -> bool:
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "pytestmark" for t in node.targets):
            values = node.value.elts if isinstance(node.value, (ast.List, ast.Tuple)) else [node.value]
            if any(_is_serial_mark(v) for v in values):
                return True
    return False


def test_il_marker_serial_e_dichiarato():
    cfg = configparser.ConfigParser()
    cfg.read(ROOT / "pytest.ini", encoding="utf-8")
    markers = [line.split(":", 1)[0].strip() for line in cfg["pytest"]["markers"].splitlines() if line.strip()]
    assert "serial" in markers


def test_ogni_test_con_display_reale_e_marcato_serial():
    offenders = []
    for path in sorted(TESTS.rglob("test_*.py")):
        if path.resolve() == SELF:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_serial = _module_is_serial(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_") and _uses_real_display(node):
                if not (module_serial or any(_is_serial_mark(d) for d in node.decorator_list)):
                    offenders.append(f"{path.relative_to(ROOT)}::{node.name}")
    assert offenders == [], "test con display X reale senza @pytest.mark.serial: " + ", ".join(offenders)


def test_il_test_gui_reale_noto_e_davvero_rilevato():
    # sensibilita' del guard: il caso reale noto deve essere visto come test con display
    path = TESTS / "acceptance" / "test_issue461_pr04_quater.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
             and n.name == "test_gui_save_reopen_file_pickers_and_live_boundary"]
    assert found and _uses_real_display(found[0]) and any(_is_serial_mark(d) for d in found[0].decorator_list)
