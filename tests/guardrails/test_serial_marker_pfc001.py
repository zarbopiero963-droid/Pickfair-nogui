"""PFC-001: i test che usano widget Tk reali sul display X condiviso sono marcati ``serial``.

Il marker serve all'esecuzione parallela (xdist): un passo seriale separato esegue ``-m serial``, quello
parallelo ``-m "not serial"``, e l'unione resta l'intera suite. Questo guard impedisce che un nuovo test
con un display reale finisca in parallelo senza marker, e che il marker sparisca dalla configurazione.

Un test (o una fixture che il test richiede) usa un display reale se:
- legge la variabile ``DISPLAY``;
- costruisce una radice Tk (``Tk``, ``CTk``, ``Toplevel``);
- costruisce ``MiniPickfairGUI`` senza ``test_mode=True`` letterale (il default di ``test_mode`` e' False).
"""
import ast
import configparser
from pathlib import Path

import pytest

pytestmark = pytest.mark.guardrail

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
SELF = Path(__file__).resolve()
_TK_ROOTS = {"Tk", "CTk", "Toplevel"}


def _is_serial_mark(node: ast.AST) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    return isinstance(target, ast.Attribute) and target.attr == "serial"


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _gui_with_real_widgets(call: ast.Call) -> bool:
    for kw in call.keywords:
        if kw.arg == "test_mode":
            return not (isinstance(kw.value, ast.Constant) and kw.value.value is True)
    return True  # test_mode assente: default False => widget reali


def uses_real_display(func: ast.AST) -> bool:
    for node in ast.walk(func):
        if isinstance(node, ast.Constant) and node.value == "DISPLAY":
            return True
        if isinstance(node, ast.Call):
            name = _call_name(node)
            if name in _TK_ROOTS:
                return True
            if name == "MiniPickfairGUI" and _gui_with_real_widgets(node):
                return True
    return False


def _is_fixture(func: ast.FunctionDef) -> bool:
    for deco in func.decorator_list:
        target = deco.func if isinstance(deco, ast.Call) else deco
        if (isinstance(target, ast.Attribute) and target.attr == "fixture") or \
                (isinstance(target, ast.Name) and target.id == "fixture"):
            return True
    return False


def _module_is_serial(tree: ast.Module) -> bool:
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "pytestmark" for t in node.targets):
            values = node.value.elts if isinstance(node.value, (ast.List, ast.Tuple)) else [node.value]
            if any(_is_serial_mark(v) for v in values):
                return True
    return False


def display_fixtures(trees) -> set:
    """Nomi delle fixture (moduli di test e conftest) che usano un display reale."""
    names = set()
    for tree in trees:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and _is_fixture(node) and uses_real_display(node):
                names.add(node.name)
    return names


def offenders_in(tree: ast.Module, fixtures: set, label: str) -> list:
    out = []
    module_serial = _module_is_serial(tree)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and node.name.startswith("test_")):
            continue
        args = {a.arg for a in node.args.args}
        if uses_real_display(node) or (args & fixtures):
            if not (module_serial or any(_is_serial_mark(d) for d in node.decorator_list)):
                out.append(f"{label}::{node.name}")
    return out


def test_il_marker_serial_e_dichiarato():
    cfg = configparser.ConfigParser()
    cfg.read(ROOT / "pytest.ini", encoding="utf-8")
    markers = [line.split(":", 1)[0].strip() for line in cfg["pytest"]["markers"].splitlines() if line.strip()]
    assert "serial" in markers


def test_ogni_test_con_display_reale_e_marcato_serial():
    parsed = {}
    for path in sorted(list(TESTS.rglob("test_*.py")) + list(TESTS.rglob("conftest.py")) + [ROOT / "conftest.py"]):
        if path.resolve() == SELF or not path.is_file():
            continue
        parsed[path] = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    fixtures = display_fixtures(parsed.values())
    offenders = []
    for path, tree in parsed.items():
        if path.name.startswith("test_"):
            offenders += offenders_in(tree, fixtures, str(path.relative_to(ROOT)))
    assert offenders == [], "test con display X reale senza @pytest.mark.serial: " + ", ".join(offenders)


def test_il_test_gui_reale_noto_e_davvero_rilevato():
    # sensibilita' del guard: il caso reale noto deve essere visto come test con display
    path = TESTS / "acceptance" / "test_issue461_pr04_quater.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
             and n.name == "test_gui_save_reopen_file_pickers_and_live_boundary"]
    assert found and uses_real_display(found[0]) and any(_is_serial_mark(d) for d in found[0].decorator_list)


_SYNTHETIC = '''
import pytest

@pytest.fixture
def real_root():
    import tkinter as tk
    root = tk.Tk()
    yield root

@pytest.fixture
def headless_gui():
    return MiniPickfairGUI(test_mode=True)

def test_via_fixture(real_root):
    assert real_root

def test_gui_default_mode():
    MiniPickfairGUI(force_simulation=True)

def test_gui_headless(headless_gui):
    assert headless_gui

@pytest.mark.serial
def test_gui_default_mode_marked():
    MiniPickfairGUI()
'''


def test_il_guard_vede_display_reali_anche_senza_la_parola_display():
    # Codex P2 su 0fd0ba2: widget reali creati in una fixture o con test_mode di default, senza "DISPLAY"
    tree = ast.parse(_SYNTHETIC)
    fixtures = display_fixtures([tree])
    assert fixtures == {"real_root"}
    assert offenders_in(tree, fixtures, "synthetic") == ["synthetic::test_via_fixture",
                                                         "synthetic::test_gui_default_mode"]
