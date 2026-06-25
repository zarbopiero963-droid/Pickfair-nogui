# Pickfair Windows executables (.exe)

Pickfair ships a desktop GUI (`mini_gui` — customtkinter/tkinter). This page
explains how to get double-clickable executables so you can run Pickfair on a
Windows PC **without installing Python**.

Two executables are produced:

- **`pickfair.exe`** — the windowed desktop **GUI** (default; double-click).
- **`pickfair-headless.exe`** — a **console** build for the Telegram-driven /
  server `--headless` mode, so you get terminal logging and Ctrl+C (a windowed
  exe has no attached console on Windows, so the GUI exe can't log to a terminal).

## Get the executables (GitHub Action — recommended)

A Windows runner builds them for you; you just download them.

1. Open the repo on GitHub → **Actions** tab.
2. Select the **"Build Windows EXE"** workflow.
3. Click **"Run workflow"** (on `main`) and wait (~a few minutes).
4. Open the completed run → **Artifacts** → download **`pickfair-windows-exe`**.
5. Unzip → you have **`pickfair.exe`** and **`pickfair-headless.exe`**.

### Tagged releases

Pushing a `v*` tag (e.g. `v1.0.0`) builds the executables and **attaches them to
the matching GitHub Release** (on the repo's **Releases** page) as durable
download assets — unlike Actions artifacts, which expire under retention.

## Run it

- **Double-click `pickfair.exe`** → starts the desktop GUI.
- **Headless / no-GUI** (Telegram-driven, server style): run from a terminal:
  `pickfair-headless.exe --headless`.

## Build locally (alternative)

On a Windows machine with Python 3.11:

```bat
pip install -r requirements.txt customtkinter "pyinstaller>=6,<7"
pyinstaller --noconfirm --clean pickfair.spec
```

The executables are written to `dist\pickfair.exe` and `dist\pickfair-headless.exe`.

## Notes / caveats

- **24/7 trading:** a real-money bot only trades while the exe is running and
  the PC is on and online. If the PC sleeps/shuts down/loses connectivity,
  trading stops (risk of orphan/ghost orders, missed signals). For unattended
  operation prefer a server with the headless mode.
- **Antivirus / SmartScreen:** unsigned PyInstaller exes can trigger a Windows
  SmartScreen warning ("More info" → "Run anyway"). The build is not code-signed.
- **Slower first start:** one-file exes unpack to a temp dir on each launch, so
  startup is a little slower than an installed app.
- **Missing modules at runtime:** if a `ModuleNotFoundError` appears at startup,
  add the missing module to `hiddenimports` in `pickfair.spec` and rebuild.
  `customtkinter` data files are bundled via `collect_all("customtkinter")`.
