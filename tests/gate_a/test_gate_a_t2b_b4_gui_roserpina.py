"""Gate A T2b batch b4 — gui_roserpina: costruzione dei controlli della tab Roserpina, headless.

Nessun tkinter/X11: si esegue il metodo reale `_build_roserpina_tab` con `_test_mode=False` sostituendo
`mini_gui.ctk` con un modulo fittizio che registra ogni widget (tipo, testo, variabile). Si verifica il
cablaggio label/checkbox -> variabile, non la resa grafica (quella resta MANUAL_ONLY/Xvfb)."""
from __future__ import annotations

import pytest

import mini_gui


class _Rec:
    widgets = []

    def __init__(self, kind, *args, **kw):
        self.kind, self.args, self.kw = kind, args, kw
        _Rec.widgets.append(self)

    def pack(self, *a, **k):
        return None

    grid = place = configure = pack


class _FakeCtk:
    def __getattr__(self, name):
        return lambda *a, **k: _Rec(name, *a, **k)


class _Gui(mini_gui.MiniPickfairGUI):
    def __getattr__(self, name):
        if name.startswith("rs_"):
            return f"<{name}>"
        raise AttributeError(name)


@pytest.fixture
def widgets(monkeypatch):
    _Rec.widgets = []
    monkeypatch.setattr(mini_gui, "ctk", _FakeCtk())
    g = _Gui.__new__(_Gui)
    g._test_mode = False
    g.tab_roserpina = object()
    mini_gui.MiniPickfairGUI._build_roserpina_tab(g)
    return list(_Rec.widgets)


def _entry_for_label(ws, text):
    for i, w in enumerate(ws):
        if w.kind == "CTkLabel" and w.kw.get("text") == text:
            nxt = ws[i + 1]
            assert nxt.kind == "CTkEntry"
            return nxt.kw.get("textvariable")
    raise AssertionError(f"label assente: {text}")


def _checkbox(ws, text):
    found = [w for w in ws if w.kind == "CTkCheckBox" and w.kw.get("text") == text]
    assert len(found) == 1, text
    return found[0].kw.get("variable")


def test_trg_628_b_0944(widgets):
    """ID: TRG-628-B-0944 / PF-B1-GUI-RO-AUTO-GREEN-DELAY-SEC

    Campo "Auto-green delay (s) — grace prima del cashout (0.1-30)" collegato a rs_auto_green_delay_sec_var."""
    assert _entry_for_label(widgets, "Auto-green delay (s) — grace prima del cashout (0.1-30)") \
        == "<rs_auto_green_delay_sec_var>"


def test_trg_628_b_0949(widgets):
    """ID: TRG-628-B-0949 / PF-B1-GUI-RO-LIQ-WARNING-ONLY

    Checkbox "Liquidity: solo avviso" (togliere = BLOCCA il submit) collegata a rs_liq_warning_only_var."""
    text = "Liquidity: solo avviso (⚠ togliere = BLOCCA il submit; dati assenti bloccano sempre)"
    assert _checkbox(widgets, text) == "<rs_liq_warning_only_var>"


def test_trg_628_b_0951(widgets):
    """ID: TRG-628-B-0951 / PF-B1-GUI-RO-AUTO-GREEN-DELAY-ENABLED

    Checkbox "Auto-green: attiva grace prima del cashout (ritardo)" collegata a rs_auto_green_delay_enabled_var."""
    assert _checkbox(widgets, "Auto-green: attiva grace prima del cashout (ritardo)") \
        == "<rs_auto_green_delay_enabled_var>"
