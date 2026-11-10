"""Gate di rilascio PF-RELEASE-GATE (decisione owner PF-CROSS-PLATFORM-EXECUTION-20261110, D02/D04; #625).

Nessun eseguibile o launcher capace di avviare il trading puo' essere pubblicato se puo' partire SENZA la scatola
nera BB-1. Il job `release-gate` dei workflow di build esegue questo file con `--runxfail`: un test marcato
`xfail` (lacuna nota, BB-1G non ancora integrata) li' FALLISCE, quindi build e release restano bloccati.

Nella CI ordinaria gli `xfail(strict=True)` passano come «fallimento atteso»; quando BB-1G chiude la lacuna
diventano XPASS e, essendo strict, falliscono: il marker va tolto nella stessa PR di BB-1G, e da quel momento il
gate di rilascio diventa verde solo se la protezione resta vera.

I test non aprono finestre ne' avviano il runtime: le classi che costruiscono il motore vengono sostituite da fake
che registrano solo la costruzione. La scatola nera viene resa NON disponibile; il contratto e' che nessun motore
capace di trading venga costruito e che il processo esca con errore.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

BB1G_PENDING = pytest.mark.xfail(
    strict=True,
    reason="BB-1G non integrata: la GUI avvia RuntimeController senza scatola nera (#625, D02). "
           "Il gate di rilascio esegue con --runxfail e resta rosso finche' la lacuna esiste.",
)


class _Recorder:
    """Sostituto di una classe che costruisce un motore capace di trading: registra solo la costruzione."""

    built = []

    def __init__(self, *args, **kwargs):
        type(self).built.append((args, kwargs))

    def mainloop(self):
        return None

    def destroy(self):
        return None

    def start(self, *args, **kwargs):
        return 0

    def stop(self, *args, **kwargs):
        return None


@pytest.fixture
def blackbox_unavailable(monkeypatch):
    from core import blackbox as bb
    from core import blackbox_runtime

    def refuse(*args, **kwargs):
        raise bb.BlackBoxUnavailable("BLACKBOX_DIR_INSECURE: simulazione del gate di rilascio")

    monkeypatch.setattr(blackbox_runtime, "install", refuse)
    _Recorder.built = []
    yield
    _Recorder.built = []


def test_headless_entrypoint_never_builds_the_runtime_without_the_blackbox(blackbox_unavailable, monkeypatch):
    import headless_main

    monkeypatch.setattr(headless_main, "HeadlessApp", _Recorder)
    monkeypatch.setattr(sys, "argv", ["pickfair-headless", "--headless", "--simulation"])
    rc = headless_main.main()
    assert rc != 0
    assert _Recorder.built == [], "headless ha costruito il motore senza scatola nera"


@BB1G_PENDING
def test_gui_entrypoint_never_builds_the_runtime_without_the_blackbox(blackbox_unavailable, monkeypatch):
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _Recorder)
    monkeypatch.setattr(sys, "argv", ["pickfair"])
    rc = mini_gui.main()
    assert _Recorder.built == [], "la GUI ha costruito il motore senza scatola nera"
    assert rc != 0


@BB1G_PENDING
def test_default_launcher_main_py_never_builds_the_runtime_without_the_blackbox(blackbox_unavailable, monkeypatch):
    import main
    import mini_gui

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", _Recorder)
    monkeypatch.setattr(sys, "argv", ["pickfair"])
    rc = main.main()
    assert _Recorder.built == [], "main.py (GUI predefinita) ha costruito il motore senza scatola nera"
    assert rc != 0


# --- inventario dei launcher: un nuovo modo di avvio non puo' entrare senza essere coperto da questo gate ---

# Script di ingresso noti degli eseguibili e dei pacchetti. Aggiungerne uno richiede un test sopra.
PACKAGED_ENTRY_SCRIPTS = ["main.py"]
# Launcher .bat -> script Python; quelli che non possono operare sono elencati con il motivo.
BAT_SCRIPTS_NOT_TRADING = {
    "betfair_market_api.py": "dati di mercato in sola lettura, nessun endpoint di ordine (in ritiro, #491)",
}
ORDER_MARKERS = re.compile(r"placeOrders|place_order|cancelOrders|cancel_order|replaceOrders|RuntimeController")


def test_pyinstaller_spec_packages_only_the_known_entry_scripts():
    tree = ast.parse((ROOT / "pickfair.spec").read_text(encoding="utf-8"))
    scripts = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "Analysis":
            first = node.args[0] if node.args else None
            assert isinstance(first, ast.List), "Analysis senza lista di script: inventario non verificabile"
            scripts.extend(ast.literal_eval(first))
    assert scripts == PACKAGED_ENTRY_SCRIPTS


def test_appimage_runs_the_packaged_launcher_only():
    apprun = (ROOT / "packaging" / "AppRun").read_text(encoding="utf-8")
    execs = re.findall(r"^\s*exec\s+(\S+)", apprun, flags=re.MULTILINE)
    assert execs == ['"${HERE}/usr/bin/pickfair"'], f"AppRun avvia un launcher non coperto: {execs}"


def test_bat_launchers_start_only_covered_or_non_trading_scripts():
    covered = set(PACKAGED_ENTRY_SCRIPTS) | {"headless_main.py", "mini_gui.py"}
    found = {}
    for bat in sorted(ROOT.glob("*.bat")):
        for script in re.findall(r"python[0-9.]*(?:\.exe)?\s+(?:-\S+\s+)*([\w./\\-]+\.py)",
                                 bat.read_text(encoding="utf-8", errors="replace"), flags=re.IGNORECASE):
            found.setdefault(Path(script).name, []).append(bat.name)
    unknown = {s: b for s, b in found.items() if s not in covered and s not in BAT_SCRIPTS_NOT_TRADING}
    assert not unknown, f"launcher .bat non coperti dal gate: {unknown}"
    for script in BAT_SCRIPTS_NOT_TRADING:
        if script in found:
            text = (ROOT / script).read_text(encoding="utf-8")
            assert not ORDER_MARKERS.search(text), f"{script} e' dichiarato non operativo ma contiene ordini/runtime"


# --- cablaggio del gate nei workflow di build/rilascio: togliere `needs` o `--runxfail` deve far fallire la CI ---

@pytest.mark.parametrize("workflow", ["build-linux.yml", "build-windows-exe.yml"])
def test_build_and_release_jobs_depend_on_the_release_gate(workflow):
    import yaml

    jobs = yaml.safe_load((ROOT / ".github" / "workflows" / workflow).read_text(encoding="utf-8"))["jobs"]
    gate = jobs["release-gate"]
    assert "if" not in gate, "il gate non deve avere condizioni che lo saltino"
    runs = " ".join(str(step.get("run", "")) for step in gate["steps"])
    assert "--runxfail" in runs and "tests/packaging/test_release_requires_blackbox.py" in runs
    assert "continue-on-error" not in gate and all("continue-on-error" not in step for step in gate["steps"])
    for name, job in jobs.items():
        if name == "release-gate":
            continue
        needs = job.get("needs", [])
        needs = [needs] if isinstance(needs, str) else list(needs)
        assert "release-gate" in needs or "build" in needs, f"{workflow}: il job {name} non dipende dal gate"
    assert jobs["build"]["needs"] in ("release-gate", ["release-gate"])


# Script alla radice eseguibili direttamente che costruiscono o avviano il runtime. Non sono impacchettati: li copre
# il gate comune di BB-1G in RuntimeController.start (audit Claude D, #625). Un nuovo script deve essere aggiunto
# qui consapevolmente.
ROOT_SCRIPTS_STARTING_THE_RUNTIME = {
    "main.py", "headless_main.py", "mini_gui.py", "hard_verify_issue_320.py", "test_zero_touch_sync.py",
    "test_gui_hard.py", "test_gui_parser_management.py",
}


def test_root_scripts_that_can_start_the_runtime_are_inventoried():
    found = set()
    for script in sorted(ROOT.glob("*.py")):
        text = script.read_text(encoding="utf-8", errors="replace")
        if re.search(r"if __name__\s*==\s*['\"]__main__['\"]", text) and re.search(
                r"RuntimeController|HeadlessApp|MiniPickfairGUI|from (?:headless_main|mini_gui) import|import (?:headless_main|mini_gui)\b",
                text):
            found.add(script.name)
    assert found <= ROOT_SCRIPTS_STARTING_THE_RUNTIME, f"script non inventariati: {found - ROOT_SCRIPTS_STARTING_THE_RUNTIME}"
