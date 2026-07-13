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
    """Registra il ciclo di vita costruzione -> force_sim -> mainloop -> destroy."""

    instances = []

    def __init__(self):
        self.mainloop_calls = 0
        self.destroy_calls = 0
        self.force_sim_calls = 0
        # True se SIMULATION e' stata forzata PRIMA che il mainloop partisse.
        self.forced_sim_before_mainloop = None
        type(self).instances.append(self)

    def force_simulation_startup(self):
        self.force_sim_calls += 1

    def mainloop(self):
        # Cattura l'ordine: il fail-closed richiede SIM forzata prima del loop.
        self.forced_sim_before_mainloop = self.force_sim_calls >= 1
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
    assert app.force_sim_calls == 1           # SIMULATION forzata all'avvio (#355)
    assert app.forced_sim_before_mainloop is True  # prima del mainloop


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


def test_main_forces_simulation_before_starting_gui(monkeypatch):
    # #355 hardening: l'entry point forza SIMULATION PRIMA del mainloop, cosi'
    # un `execution_mode=LIVE` persistito non parte mai da `python main.py`.
    # BLOCK: senza `app.force_simulation_startup()` in main(), force_sim_calls
    # resta 0 e forced_sim_before_mainloop None => il test fallisce.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _FakeGUI)

    rc = mini_gui.main()

    assert rc == 0
    app = _FakeGUI.instances[0]
    assert app.force_sim_calls == 1
    assert app.forced_sim_before_mainloop is True


def test_run_gui_propagates_gui_failure_exit_code(monkeypatch):
    # #354: main.run_gui() deve PROPAGARE il codice fail-closed di
    # mini_gui.main(), non scartarlo ritornando 0.
    # BLOCK: col vecchio `gui_main(); return 0`, rc sarebbe 0 e il test fallisce.
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _RaisingGUI)
    sys.modules.pop("main", None)
    main_module = importlib.import_module("main")

    rc = main_module.run_gui()

    assert rc == 1
    assert _FakeGUI.instances[0].destroy_calls == 1


def test_run_gui_propagates_keyboard_interrupt_exit_code(monkeypatch):
    # #354: anche il codice 130 (KeyboardInterrupt) deve arrivare al chiamante.
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
