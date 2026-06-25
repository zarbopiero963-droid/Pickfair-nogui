# Pickfair Windows executable (.exe)

Pickfair ships a desktop GUI (`mini_gui` — customtkinter/tkinter). This page
explains how to get a double-clickable `pickfair.exe` so you can run it on a
Windows PC **without installing Python**.

## Get the exe (GitHub Action — recommended)

A Windows runner builds the exe for you; you just download it.

1. Open the repo on GitHub → **Actions** tab.
2. Select the **"Build Windows EXE"** workflow.
3. Click **"Run workflow"** (on `main`) and wait for it to finish (~a few minutes).
4. Open the completed run → **Artifacts** → download **`pickfair-windows-exe`**.
5. Unzip it → you have **`pickfair.exe`**. Double-click to run.

The build also runs automatically when a `v*` tag is pushed (e.g. `v1.0.0`),
so tagged releases produce an exe artifact.

## Run it

- **Double-click `pickfair.exe`** → starts the desktop GUI (default mode).
- **Headless / no-GUI** (Telegram-driven, server style): run from a terminal
  with `pickfair.exe --headless`.

## Build locally (alternative)

On a Windows machine with Python 3.11:

```bat
pip install -r requirements.txt customtkinter pyinstaller
pyinstaller --noconfirm --clean pickfair.spec
```

The exe is written to `dist\pickfair.exe`.

## Notes / caveats

- **24/7 trading:** a real-money bot only trades while the exe is running and
  the PC is on and online. If the PC sleeps/shuts down/loses connectivity,
  trading stops (risk of orphan/ghost orders, missed signals). For unattended
  operation prefer a server with the headless mode.
- **Antivirus / SmartScreen:** unsigned PyInstaller exes can trigger a Windows
  SmartScreen warning ("More info" → "Run anyway"). The build is not code-signed.
- **Missing modules at runtime:** if a `ModuleNotFoundError` appears at startup,
  add the missing module to `hiddenimports` in `pickfair.spec` and rebuild.
  `customtkinter` data files are bundled via `collect_all("customtkinter")`.
