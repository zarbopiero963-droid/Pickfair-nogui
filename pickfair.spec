# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for the Pickfair Windows executables.

Builds TWO single-file executables from ``main.py`` sharing one Analysis:

- ``pickfair.exe``          — windowed GUI (``console=False``), the default
  double-click desktop app.
- ``pickfair-headless.exe`` — console build (``console=True``) for the
  Telegram-driven / server ``--headless`` mode, so terminal logging and Ctrl+C
  work (a windowed exe has no attached stdio on Windows).

Built on a Windows runner by ``.github/workflows/build-windows-exe.yml``; see
``ops/windows_exe.md``.
"""
from PyInstaller.utils.hooks import collect_all

# customtkinter ships theme/asset data files that must travel with the exe.
ctk_datas, ctk_binaries, ctk_hiddenimports = collect_all("customtkinter")

# main.py imports the GUI/headless entrypoints lazily (inside functions), and
# the UI panels live in a namespace package — name them so PyInstaller's static
# analysis bundles them. darkdetect is imported by customtkinter at runtime for
# appearance detection and is a separate top-level package, so list it too.
hiddenimports = ctk_hiddenimports + [
    "darkdetect",
    "mini_gui",
    "headless_main",
    "telegram_tab_ui",
    "theme",
    "ui_panels.alerts_panel",
    "ui_panels.audit_panel",
    "ui_panels.export_panel",
    "ui_panels.health_panel",
    "ui_panels.incident_timeline_panel",
    "ui_panels.incidents_panel",
    "ui_panels.metrics_panel",
    "ui_panels.observability_panel",
    "ui_panels.safe_mode_panel",
]

a = Analysis(
    ["main.py"],
    pathex=[],
    binaries=ctk_binaries,
    datas=ctk_datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe_gui = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="pickfair",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=False,  # windowed GUI — no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

exe_headless = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="pickfair-headless",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=True,  # console build so `--headless` has terminal stdio / Ctrl+C
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
