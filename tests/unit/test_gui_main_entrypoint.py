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
import sys

import pytest


class _FakeGUI:
    """Registra il ciclo di vita costruzione -> mainloop -> destroy.

    Cattura anche il kwarg `force_simulation` passato al costruttore: il
    fail-closed #355 e' garantito **alla costruzione** (non con una chiamata
    successiva), quindi il contratto dell'entry point e' che la GUI venga
    costruita con `force_simulation=True`.
    """

    instances = []

    def __init__(self, force_simulation=False):
        self.mainloop_calls = 0
        self.destroy_calls = 0
        self.force_simulation = bool(force_simulation)
        type(self).instances.append(self)

    def mainloop(self):
        self.mainloop_calls += 1

    def destroy(self):
        self.destroy_calls += 1


class _RaisingGUI(_FakeGUI):
    def mainloop(self):
        super().mainloop()
        raise RuntimeError("boom mainloop")


class _InterruptGUI(_FakeGUI):
    def mainloop(self):
        super().mainloop()
        raise KeyboardInterrupt


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
    assert app.force_simulation is True  # costruita fail-closed SIMULATION (#355)


def test_main_returns_error_code_on_gui_failure(monkeypatch):
    # Fail-closed: un errore nel mainloop non propaga, ritorna codice 1 e
    # comunque distrugge la finestra.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _RaisingGUI)

    rc = mini_gui.main()

    assert rc == 1
    assert len(_FakeGUI.instances) == 1
    assert _FakeGUI.instances[0].destroy_calls == 1


def test_main_returns_130_on_keyboard_interrupt(monkeypatch):
    # Fail-closed: KeyboardInterrupt (Ctrl-C) => codice 130 e cleanup nel finally.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _InterruptGUI)

    rc = mini_gui.main()

    assert rc == 130
    assert len(_FakeGUI.instances) == 1
    assert _FakeGUI.instances[0].destroy_calls == 1


def test_main_constructs_gui_forced_simulation(monkeypatch):
    """#355 hardening: l'entry point costruisce la GUI con force_simulation=True.

    Il fail-closed vale dalla COSTRUZIONE (non dopo): un `execution_mode=LIVE`
    persistito non viene mai sincronizzato al runtime, quindi non c'e' finestra
    LIVE transitoria in `__init__`. BLOCK: con `MiniPickfairGUI()` senza il kwarg
    (vecchio codice), `force_simulation` resta False => il test fallisce.
    """
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _FakeGUI)

    rc = mini_gui.main()

    assert rc == 0
    assert _FakeGUI.instances[0].force_simulation is True


def test_run_gui_propagates_gui_failure_exit_code(monkeypatch):
    """#354: main.run_gui() PROPAGA il codice fail-closed di mini_gui.main().

    Un crash del mainloop deve uscire con codice 1, non essere mascherato da 0.
    BLOCK: col vecchio `gui_main(); return 0`, rc sarebbe 0 e il test fallisce.
    """
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _RaisingGUI)
    sys.modules.pop("main", None)
    main_module = importlib.import_module("main")

    rc = main_module.run_gui()

    assert rc == 1
    assert _FakeGUI.instances[0].destroy_calls == 1


def test_run_gui_propagates_keyboard_interrupt_exit_code(monkeypatch):
    """#354: anche il codice 130 (KeyboardInterrupt/Ctrl-C) arriva al chiamante."""
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _InterruptGUI)
    sys.modules.pop("main", None)
    main_module = importlib.import_module("main")

    rc = main_module.run_gui()

    assert rc == 130
    assert _FakeGUI.instances[0].destroy_calls == 1


def test_main_py_run_gui_wires_to_mini_gui_main(monkeypatch):
    # End-to-end del wiring rotto sul VPS: main.run_gui() deve importare
    # `main` da mini_gui e avviare la GUI senza ImportError.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _FakeGUI)
    # Forza un import fresco di `main` cosi' il path `from mini_gui import main`
    # (che pre-fix sollevava ImportError) viene realmente rieseguito e non
    # servito dalla cache di un test precedente.
    sys.modules.pop("main", None)
    main_module = importlib.import_module("main")

    rc = main_module.run_gui()

    assert rc == 0
    assert len(_FakeGUI.instances) == 1
    assert _FakeGUI.instances[0].mainloop_calls == 1
    assert _FakeGUI.instances[0].destroy_calls == 1  # il finally di main() viene esercitato
