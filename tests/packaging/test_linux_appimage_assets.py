"""Guardrail sugli asset di packaging AppImage Linux (PR-1).

Blinda il contratto usato da ``.github/workflows/build-linux.yml`` +
``packaging/build_appimage.sh`` senza dipendere da GitHub/rete:
- il ``.desktop`` ha i campi obbligatori (altrimenti l'app non compare nel menu);
- l'icona è un PNG valido 256x256 ed è riproducibile da ``make_icon.py``;
- lo script di build e l'``AppRun`` sono bash sintatticamente validi;
- il workflow Linux invoca lo script e pubblica l'artifact AppImage.

Sono controlli statici (nessun build reale): il build PyInstaller + appimagetool
gira in CI. `unit` marker per girare nella suite veloce.
"""
from __future__ import annotations

import struct
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PKG = ROOT / "packaging"

pytestmark = pytest.mark.unit


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def test_desktop_has_mandatory_fields():
    text = _read(PKG / "pickfair.desktop")
    assert text.startswith("[Desktop Entry]"), "manca l'header [Desktop Entry]"
    fields = dict(
        line.split("=", 1)
        for line in text.splitlines()
        if "=" in line and not line.startswith("[")
    )
    # Senza questi l'icona/lancio nel menu applicazioni non funzionano.
    assert fields.get("Type") == "Application"
    assert fields.get("Name") == "Pickfair"
    assert fields.get("Exec") == "pickfair"
    assert fields.get("Icon") == "pickfair"


def test_icon_is_valid_256_png():
    data = (PKG / "pickfair.png").read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "non è un PNG"
    width, height = struct.unpack(">II", data[16:24])
    assert (width, height) == (256, 256), f"icona {width}x{height}, attesa 256x256"


def test_icon_is_reproducible_from_generator(tmp_path):
    # BLOCK: l'icona committata deve combaciare con l'output di make_icon.py,
    # così non diverge silenziosamente dal generatore.
    out = tmp_path / "regen.png"
    subprocess.run(
        [sys.executable, str(PKG / "make_icon.py"), str(out)],
        check=True,
        cwd=ROOT,
    )
    assert out.read_bytes() == (PKG / "pickfair.png").read_bytes(), (
        "packaging/pickfair.png non combacia con make_icon.py: rigenera con "
        "python3 packaging/make_icon.py"
    )


@pytest.mark.parametrize("script", ["build_appimage.sh", "AppRun"])
def test_shell_scripts_are_valid_bash(script):
    # `bash -n` = parse-only (nessuna esecuzione), cattura errori di sintassi.
    result = subprocess.run(
        ["bash", "-n", str(PKG / script)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, f"{script} non valido: {result.stderr}"


def test_build_appimage_script_rejects_missing_binary():
    # BLOCK fail-closed: senza binario lo script esce != 0, non genera un
    # AppImage vuoto/rotto.
    result = subprocess.run(
        ["bash", str(PKG / "build_appimage.sh"), "/non/esiste/pickfair"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    assert result.returncode != 0


def test_workflow_wires_appimage():
    wf = _read(ROOT / ".github" / "workflows" / "build-linux.yml")
    assert "packaging/build_appimage.sh" in wf, "il workflow non chiama lo script"
    assert "pickfair-linux-appimage" in wf, "manca l'upload dell'artifact AppImage"
    # il job Windows non deve essere toccato da questo file
    assert "windows" not in wf.lower()
