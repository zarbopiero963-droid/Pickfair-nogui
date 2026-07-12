"""Hard test dell'entry point GUI `mini_gui.main`.

Regressione del bug del test VPS (issue #351): `main.py:run_gui()` fa
`from mini_gui import main as gui_main`, ma `mini_gui.py` non esponeva alcuna
funzione `main` module-level => `ImportError: cannot import name 'main' from
'mini_gui'` a ogni avvio in modalita' GUI.

I test NON aprono una finestra Tk reale (richiederebbe un display): la classe
`MiniPickfairGUI` viene sostituita da un fake che registra costruzione,
mainloop e destroy. Cosi' il gate resta headless-safe pur esercitando il
codice reale di `mini_gui.main` / `main.run_gui`.
"""

import importlib

import pytest


class _FakeGUI:
    """Registra il ciclo di vita costruzione -> mainloop -> destroy."""

    instances = []

    def __init__(self):
        self.mainloop_calls = 0
        self.destroy_calls = 0
        type(self).instances.append(self)

    def mainloop(self):
        self.mainloop_calls += 1

    def destroy(self):
        self.destroy_calls += 1


class _RaisingGUI(_FakeGUI):
    def mainloop(self):
        super().mainloop()
        raise RuntimeError("boom mainloop")


@pytest.fixture(autouse=True)
def _reset_instances():
    _FakeGUI.instances = []
    yield
    _FakeGUI.instances = []


def test_mini_gui_exposes_callable_main():
    # BLOCK: prima del fix `from mini_gui import main` sollevava ImportError.
    import mini_gui

    assert hasattr(mini_gui, "main"), "mini_gui deve esporre main() module-level"
    assert callable(mini_gui.main)


def test_main_builds_gui_and_runs_mainloop(monkeypatch):
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _FakeGUI)

    rc = mini_gui.main()

    assert rc == 0
    assert len(_FakeGUI.instances) == 1
    app = _FakeGUI.instances[0]
    assert app.mainloop_calls == 1  # la GUI e' stata effettivamente avviata
    assert app.destroy_calls == 1   # cleanup nel finally


def test_main_returns_error_code_on_gui_failure(monkeypatch):
    # Fail-closed: un errore nel mainloop non propaga, ritorna codice 1 e
    # comunque distrugge la finestra.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _RaisingGUI)

    rc = mini_gui.main()

    assert rc == 1
    assert len(_FakeGUI.instances) == 1
    assert _FakeGUI.instances[0].destroy_calls == 1


def test_main_py_run_gui_wires_to_mini_gui_main(monkeypatch):
    # End-to-end del wiring rotto sul VPS: main.run_gui() deve importare
    # `main` da mini_gui e avviare la GUI senza ImportError.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _FakeGUI)
    main_module = importlib.import_module("main")

    rc = main_module.run_gui()

    assert rc == 0
    assert len(_FakeGUI.instances) == 1
    assert _FakeGUI.instances[0].mainloop_calls == 1
