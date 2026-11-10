"""Integrita' del gate di rilascio PF-RELEASE-GATE (#625; addendum owner RELEASE GATE INTEGRITY, #626).

Questi test girano nella CI ordinaria e proteggono il MECCANISMO del gate, non il comportamento dei launcher (che sta
in `tests/packaging/release_gate_launchers.py` ed e' eseguito solo da `scripts/release_gate.py`):

- inventario dei launcher (spec PyInstaller, AppRun, .bat, script alla radice): un nuovo modo di avvio non entra
  senza essere coperto;
- i test obbligatori del gate esistono con il loro nome, coincidono con `REQUIRED_TESTS` e non hanno skip/xfail;
- lo script del gate rifiuta report con test mancanti, saltati, xfail, falliti, in errore, di un altro modulo o
  illeggibili (scenari di mutazione dell'enforcement);
- nei workflow di build `build` e `release` dipendono dal gate senza condizioni di stato ne' `continue-on-error`, e il
  gate esegue esattamente `python scripts/release_gate.py`;
- l'API legacy ritirata non torna.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
GATE_MODULE_PATH = ROOT / "tests" / "packaging" / "release_gate_launchers.py"


def _release_gate():
    import importlib.util

    spec = importlib.util.spec_from_file_location("release_gate_under_test", ROOT / "scripts" / "release_gate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- inventario dei launcher: un nuovo modo di avvio non puo' entrare senza essere coperto da questo gate ---

# Script di ingresso noti degli eseguibili e dei pacchetti. Aggiungerne uno richiede un test obbligatorio nel
# modulo del gate e in `REQUIRED_TESTS`.
PACKAGED_ENTRY_SCRIPTS = ["main.py"]
# Launcher .bat -> script Python; quelli che non possono operare sono elencati con il motivo.
# Vuoto dal ritiro di betfair_market_api.py, AVVIA_API.bat e INSTALLA.bat (decisione owner
# PF-BETFAIR-API-RETIREMENT-20261110): nessun launcher legacy puo' fare login Betfair senza scatola nera.
BAT_SCRIPTS_NOT_TRADING: dict = {}
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


# --- cablaggio del gate nei workflow di build/rilascio ---

STATUS_FUNCTIONS = re.compile(r"\b(always|failure|cancelled)\s*\(")
GATE_COMMAND = "python scripts/release_gate.py"


def _jobs(workflow):
    return yaml.safe_load((ROOT / ".github" / "workflows" / workflow).read_text(encoding="utf-8"))["jobs"]


def _needs(job):
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


@pytest.mark.parametrize("workflow", ["build-linux.yml", "build-windows-exe.yml"])
def test_build_and_release_cannot_run_without_a_green_release_gate(workflow):
    jobs = _jobs(workflow)
    gate = jobs["release-gate"]
    assert "if" not in gate, "il gate non deve avere condizioni che lo saltino"
    gate_runs = [str(step["run"]).strip() for step in gate["steps"] if "run" in step]
    assert GATE_COMMAND in gate_runs, f"il gate deve eseguire esattamente {GATE_COMMAND!r}"
    assert not any("pytest" in run for run in gate_runs), "il gate non chiama pytest direttamente (niente -k/-m/filtri)"
    for block in [gate, *gate["steps"]]:
        overridden = {"GITHUB_ACTIONS", "GITHUB_REF", "PYTEST_ADDOPTS", "PYTEST_PLUGINS",
                      "PYTEST_DISABLE_PLUGIN_AUTOLOAD"} & set(block.get("env") or {})
        assert not overridden, f"{workflow}: il gate non deve sovrascrivere {sorted(overridden)}"
    for name, job in jobs.items():
        for where, block in [(name, job), *((f"{name}/step", st) for st in job.get("steps", []))]:
            assert "continue-on-error" not in block, f"{workflow}: continue-on-error in {where}"
            cond = str(block.get("if", ""))
            assert not STATUS_FUNCTIONS.search(cond), f"{workflow}: {where} aggira il gate con {cond!r}"
    # ogni job diverso dal gate dipende, direttamente o tramite altri job, dal gate
    for name in jobs:
        if name == "release-gate":
            continue
        seen, todo = set(), list(_needs(jobs[name]))
        while todo:
            dep = todo.pop()
            if dep not in seen:
                seen.add(dep)
                todo.extend(_needs(jobs.get(dep, {})))
        assert "release-gate" in seen, f"{workflow}: il job {name} non dipende dal gate"
    assert _needs(jobs["build"]) == ["release-gate"]


@pytest.mark.parametrize("mutation", ["always()", "failure()", "cancelled()", "!cancelled()", "continue-on-error"])
def test_wiring_check_detects_status_bypasses(mutation, tmp_path, monkeypatch):
    """Scenario di mutazione: la stessa verifica sul workflow alterato deve fallire."""
    jobs = _jobs("build-linux.yml")
    if mutation == "continue-on-error":
        jobs["release-gate"]["continue-on-error"] = True
    else:
        jobs["release"]["if"] = f"{mutation} && startsWith(github.ref, 'refs/tags/')"
    altered = tmp_path / ".github" / "workflows"
    altered.mkdir(parents=True)
    (altered / "build-linux.yml").write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    with pytest.raises(AssertionError):
        test_build_and_release_cannot_run_without_a_green_release_gate("build-linux.yml")


@pytest.mark.parametrize("mutation", ["needs_removed", "gate_pytest_filter", "gate_command_changed", "ref_suppressed"])
def test_wiring_check_detects_dependency_and_command_bypasses(mutation, tmp_path, monkeypatch):
    jobs = _jobs("build-windows-exe.yml")
    gate_steps = jobs["release-gate"]["steps"]
    if mutation == "needs_removed":
        jobs["build"].pop("needs")
    elif mutation == "gate_pytest_filter":
        gate_steps.append({"run": "python -m pytest -k headless tests/packaging/release_gate_launchers.py"})
    elif mutation == "ref_suppressed":
        jobs["release-gate"]["env"] = {"GITHUB_ACTIONS": ""}
    else:
        for step in gate_steps:
            if str(step.get("run", "")).strip() == GATE_COMMAND:
                step["run"] = "python scripts/release_gate.py || true"
    altered = tmp_path / ".github" / "workflows"
    altered.mkdir(parents=True)
    (altered / "build-windows-exe.yml").write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    with pytest.raises(AssertionError):
        test_build_and_release_cannot_run_without_a_green_release_gate("build-windows-exe.yml")


# --- identita' e integrita' dei test obbligatori del gate ---

def _gate_module_tree():
    return ast.parse(GATE_MODULE_PATH.read_text(encoding="utf-8"))


def test_gate_module_is_not_collected_by_the_ordinary_ci():
    """I test di sicurezza falliscono finche' manca BB-1G: la CI ordinaria non deve raccoglierli (resterebbe rossa),
    solo il gate li esegue. pytest.ini raccoglie `test_*.py`."""
    assert not GATE_MODULE_PATH.name.startswith("test_")
    ini = (ROOT / "pytest.ini").read_text(encoding="utf-8")
    assert re.search(r"^python_files\s*=\s*test_\*\.py\s*$", ini, flags=re.MULTILINE)


def test_required_tests_are_exactly_the_tests_of_the_gate_module():
    names = {node.name for node in _gate_module_tree().body
             if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    assert names == set(_release_gate().REQUIRED_TESTS), "test obbligatori e modulo del gate non coincidono"


def test_every_packaged_launcher_has_a_required_gate_test():
    required = set(_release_gate().REQUIRED_TESTS)
    assert "test_default_launcher_main_py_never_builds_the_runtime_without_the_blackbox" in required
    assert "test_gui_entrypoint_never_builds_the_runtime_without_the_blackbox" in required
    assert "test_headless_entrypoint_never_builds_the_runtime_without_the_blackbox" in required
    assert PACKAGED_ENTRY_SCRIPTS == ["main.py"], "nuovo script impacchettato: aggiungere il suo test obbligatorio"


def test_gate_module_has_no_skip_xfail_or_selection_escape():
    tree = _gate_module_tree()
    text = GATE_MODULE_PATH.read_text(encoding="utf-8")
    assert "pytestmark" not in text
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in {"skip", "skipif", "xfail", "importorskip"}:
            raise AssertionError(f"il modulo del gate usa pytest.{node.attr}: un test saltato non prova nulla")
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            assert not node.decorator_list, f"{node.name}: decoratori non ammessi sui test obbligatori"


# --- scenari di mutazione dello script del gate: nessun falso PASS dal rapporto ---

_CLS = "tests.packaging.release_gate_launchers"


def _junit(*cases):
    body = "".join(
        f'<testcase classname="{cls}" name="{name}">{extra}</testcase>' for cls, name, extra in cases)
    return f'<testsuites><testsuite name="pytest">{body}</testsuite></testsuites>'


_INTEGRITY = "tests.packaging.test_release_requires_blackbox"


def _all_passed(integrity=None, **override):
    gate = _release_gate()
    count = gate.MIN_INTEGRITY_TESTS if integrity is None else integrity
    cases = [(_INTEGRITY, f"test_integrita_{i}", "") for i in range(count)]
    for name in sorted(gate.REQUIRED_TESTS):
        extra = override.get(name, "")
        if extra is not None:
            cases.append((_CLS, name, extra))
    return gate, _junit(*cases)


def test_gate_report_with_every_required_test_passed_is_green():
    gate, xml = _all_passed()
    assert gate.evaluate(xml) == (True, [])


@pytest.mark.parametrize("outcome", [
    '<failure message="motore costruito"/>', '<error message="import"/>', '<skipped message="skip"/>',
    '<skipped type="pytest.xfail" message="xfail"/>'])
def test_gate_report_with_a_required_test_not_passed_is_red(outcome):
    gate = _release_gate()
    name = "test_gui_entrypoint_never_builds_the_runtime_without_the_blackbox"
    _, xml = _all_passed(**{name: outcome})
    ok, reasons = gate.evaluate(xml)
    assert not ok and any(name in r for r in reasons)


def test_gate_report_missing_a_required_test_is_red():
    gate = _release_gate()
    name = "test_default_launcher_main_py_never_builds_the_runtime_without_the_blackbox"
    _, xml = _all_passed(**{name: None})
    ok, reasons = gate.evaluate(xml)
    assert not ok and any("non raccolto" in r for r in reasons)


def test_gate_report_from_another_module_or_innocuous_tests_is_red():
    gate = _release_gate()
    integrity = [("tests.packaging.test_release_requires_blackbox", f"test_integrita_{i}", "")
                 for i in range(gate.MIN_INTEGRITY_TESTS)]
    impostors = [("tests.packaging.altro", name, "") for name in sorted(gate.REQUIRED_TESTS)]
    ok, reasons = gate.evaluate(_junit(*integrity, *impostors))
    assert ok is False and all("non raccolto" in r for r in reasons)
    assert gate.evaluate(_junit(*integrity, (_CLS, "test_innocuo", "")))[0] is False


@pytest.mark.parametrize("report", ["", "<non-xml", "<testsuites/>"])
def test_gate_report_unreadable_or_empty_is_red(report):
    assert _release_gate().evaluate(report)[0] is False


def test_gate_requires_a_non_empty_required_set():
    gate, xml = _all_passed()
    assert gate.evaluate(xml, required=()) [0] is False


def test_gate_rejects_a_pytest_failure_even_with_a_green_report(monkeypatch, tmp_path):
    """Il report e' generato DENTRO l'esecuzione del gate: un report preesistente non viene letto, e un rc != 0 di
    pytest rende rosso il gate anche se il rapporto sembra pulito."""
    gate = _release_gate()
    _, xml = _all_passed()

    def fake_run(cmd, cwd, env, check):
        assert "PYTEST_ADDOPTS" not in env, "il gate deve ignorare PYTEST_ADDOPTS (filtri iniettati)"
        assert "PYTEST_PLUGINS" not in env and env.get("PYTEST_DISABLE_PLUGIN_AUTOLOAD") == "1"
        assert any(a.startswith("--junitxml=") for a in cmd) and gate.GATE_MODULE in cmd
        pytest_args = cmd[cmd.index("pytest") + 1:]
        assert not {"-k", "-m", "--deselect", "--lf", "--last-failed"} & set(pytest_args), pytest_args
        report = next(a.split("=", 1)[1] for a in cmd if a.startswith("--junitxml="))
        Path(report).write_text(xml, encoding="utf-8")
        return type("P", (), {"returncode": 1})()

    monkeypatch.setenv("PYTEST_ADDOPTS", "-k headless")
    monkeypatch.setenv("PYTEST_PLUGINS", "plugin_malevolo")
    monkeypatch.setattr(gate.subprocess, "run", fake_run)
    ok, reasons = gate.run()
    assert not ok


def test_gate_without_a_report_is_red(monkeypatch):
    gate = _release_gate()
    monkeypatch.setattr(gate.subprocess, "run", lambda *a, **k: type("P", (), {"returncode": 0})())
    assert gate.run()[0] is False


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


def test_legacy_market_api_is_retired():
    """PF-BETFAIR-API-RETIREMENT-20261110: login Betfair reale senza scatola nera, API su 0.0.0.0 senza
    autenticazione e CORS '*'. Ritirata; il catalogo mercati resta in-process (services/catalog_sync_service.py) e
    la lettura esterna passa dalla Control API (#491). Il ritorno del file o del launcher fa fallire il gate."""
    for name in ("betfair_market_api.py", "AVVIA_API.bat", "INSTALLA.bat"):
        assert not (ROOT / name).exists(), f"{name} ritirato non deve tornare"


# --- controprova di Claude D (OWNER GO PF-OWNER-626-SECURITY-HARDENING-20261110, D04): exit code != 0 non basta ---

def _gate_checks():
    import importlib.util

    spec = importlib.util.spec_from_file_location("release_gate_launchers_under_test", GATE_MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_bb1g_builds_runtime_then_fails():
    """Finta BB-1G: la scatola nera manca, ma il motore viene costruito per un'altra strada e poi si esce con 1."""
    from core.runtime_controller import RuntimeController

    try:
        RuntimeController()
    except Exception:
        pass
    return 1


def _fake_bb1g_hides_the_engine():
    from core.trading_engine import TradingEngine

    try:
        TradingEngine()
    except BaseException:  # noqa: BLE001 - anche chi inghiotte tutto viene scoperto
        pass
    return 1


def _fake_bb1g_ignores_install_failure():
    from core import blackbox_runtime
    import mini_gui

    try:
        blackbox_runtime.install(entrypoint="mini_gui")
    except Exception:
        pass
    app = mini_gui.MiniPickfairGUI(force_simulation=True)
    return 1 if app is None else 0


def _fake_bb1g_logs_in_to_betfair():
    import betfair_client

    betfair_client.BetfairClient(username="x", app_key="y", cert_pem="c", key_pem="k")
    return 1


def _fake_bb1g_refuses_but_exits_zero():
    return 0


def _fake_bb1g_builds_engine_in_a_thread():
    import threading
    from core.trading_engine import TradingEngine

    def later():
        try:
            TradingEngine()
        except BaseException:  # noqa: BLE001
            pass

    threading.Thread(target=later, name="motore-in-ritardo").start()
    return 1


def _fake_bb1g_runs_the_runtime_in_a_child_process():
    import subprocess
    import sys as _sys

    try:
        subprocess.Popen([_sys.executable, "-c", "import headless_main; headless_main.HeadlessApp()"])
    except BaseException:  # noqa: BLE001
        pass
    return 1


def _fake_bb1g_sys_exit_zero():
    import sys as _sys

    _sys.exit(0)


def _fake_bb1g_sends_through_an_instance_without_init():
    import simulation_broker

    broker = simulation_broker.SimulationBroker.__new__(simulation_broker.SimulationBroker)
    try:
        broker.place_bet()
    except BaseException:  # noqa: BLE001
        pass
    return 1


@pytest.mark.parametrize("fake", [
    _fake_bb1g_builds_runtime_then_fails, _fake_bb1g_hides_the_engine, _fake_bb1g_ignores_install_failure,
    _fake_bb1g_logs_in_to_betfair, _fake_bb1g_refuses_but_exits_zero, _fake_bb1g_builds_engine_in_a_thread,
    _fake_bb1g_runs_the_runtime_in_a_child_process, _fake_bb1g_sys_exit_zero,
    _fake_bb1g_sends_through_an_instance_without_init,
])
def test_gate_check_rejects_fake_bb1g(fake, monkeypatch):
    checks = _gate_checks()
    with pytest.raises(AssertionError):
        checks.assert_launcher_is_safe(monkeypatch, fake, ["pickfair"])


def test_gate_check_accepts_a_real_safe_refusal(monkeypatch):
    """BB-1G corretta: la scatola nera si installa PRIMA di qualsiasi componente operativo; se manca, rc 1."""
    checks = _gate_checks()

    def correct_bb1g():
        from core import blackbox_runtime

        try:
            blackbox_runtime.install(entrypoint="mini_gui")
        except Exception:
            return 1
        import mini_gui
        return mini_gui.main()

    checks.assert_launcher_is_safe(monkeypatch, correct_bb1g, ["pickfair"])


def test_gate_check_accepts_a_safe_refusal_by_sys_exit(monkeypatch):
    checks = _gate_checks()

    def refuses_with_sys_exit():
        import sys as _sys

        _sys.exit(1)

    checks.assert_launcher_is_safe(monkeypatch, refuses_with_sys_exit, ["pickfair"])


def test_gate_guards_every_operative_entry_point():
    """Le sentinelle coprono costruzione e avvio del runtime, motore, app headless, GUI e client Betfair."""
    points = set(_gate_checks().OPERATIVE_ENTRY_POINTS)
    assert {("core.runtime_controller", "RuntimeController", "__init__"),
            ("core.runtime_controller", "RuntimeController", "start"),
            ("core.trading_engine", "TradingEngine", "__init__"),
            ("headless_main", "HeadlessApp", "__init__"),
            ("mini_gui", "MiniPickfairGUI", "__init__"),
            ("betfair_client", "BetfairClient", "__init__"),
            ("order_manager", "OrderManager", "__init__"),
            ("services.betfair_service", "BetfairService", "get_live_client"),
            ("simulation_broker", "SimulationBroker", "__init__"),
            ("simulation_broker", "SimulationBroker", "place_bet"),
            ("betfair_client", "BetfairClient", "place_bet")} <= points
    spawn = set(_gate_checks().PROCESS_SPAWN_POINTS)
    assert ("subprocess", "Popen", "__init__") in spawn
    assert {"fork", "execv", "system", "posix_spawn"} <= set(_gate_checks().OS_SPAWN_FUNCTIONS)


# --- ref ammessi (audit Claude D, L1): niente artifact da branch diversi da main ---

@pytest.mark.parametrize("env,ok", [
    ({}, True),
    ({"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main"}, True),
    ({"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/tags/v1.2.3"}, True),
    ({"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/feature-x"}, False),
    ({"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/pull/626/merge"}, False),
    ({"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/tags/nightly"}, False),
    ({"GITHUB_ACTIONS": "true"}, False),
])
def test_gate_only_allows_main_or_release_tags_in_actions(env, ok):
    assert _release_gate().ref_allowed(env)[0] is ok


def test_gate_main_refuses_an_unauthorized_ref_before_running_tests(monkeypatch):
    gate = _release_gate()
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/feature-x")
    monkeypatch.setattr(gate, "run", lambda: (_ for _ in ()).throw(AssertionError("non deve eseguire i test")))
    assert gate.main() == 1


def test_gate_check_rejects_a_thread_still_running_after_the_launcher(monkeypatch):
    """Un thread lasciato vivo dal launcher potrebbe costruire il motore DOPO la verifica: il gate lo rifiuta."""
    import threading

    checks = _gate_checks()
    monkeypatch.setattr(checks, "THREAD_JOIN_TIMEOUT_S", 0.1)
    release = threading.Event()

    def launcher_leaving_a_thread():
        threading.Thread(target=release.wait, args=(5,), name="motore-differito").start()
        return 1

    try:
        with pytest.raises(AssertionError, match="ancora attivi"):
            checks.assert_launcher_is_safe(monkeypatch, launcher_leaving_a_thread, ["pickfair"])
    finally:
        release.set()


# --- rilievi GPT-6.1 Sol su b5c7c117: inventario nel gate e thread discendenti ---

def test_gate_runs_the_inventory_and_integrity_checks_too(monkeypatch):
    """Una build da tag non dipende dalla CI ordinaria: il gate esegue anche questo modulo."""
    gate = _release_gate()
    seen = {}

    def fake_run(cmd, cwd, env, check):
        seen["cmd"] = cmd
        return type("P", (), {"returncode": 1})()

    monkeypatch.setattr(gate.subprocess, "run", fake_run)
    gate.run()
    assert gate.GATE_MODULE in seen["cmd"] and gate.INTEGRITY_MODULE in seen["cmd"]
    assert gate.INTEGRITY_MODULE == "tests/packaging/test_release_requires_blackbox.py"


@pytest.mark.parametrize("outcome", ['<failure message="launcher non censito"/>', '<skipped message="skip"/>',
                                     '<error message="x"/>'])
def test_gate_is_red_when_an_integrity_check_does_not_pass(outcome):
    gate, xml = _all_passed()
    xml = xml.replace('name="test_integrita_0"></testcase>', f'name="test_integrita_0">{outcome}</testcase>')
    ok, reasons = gate.evaluate(xml)
    assert not ok and any("integrita'" in r for r in reasons)


def test_gate_is_red_when_too_few_integrity_checks_ran():
    gate, xml = _all_passed(integrity=3)
    ok, reasons = gate.evaluate(xml)
    assert not ok and any("minimo" in r for r in reasons)


def test_integrity_module_has_at_least_the_minimum_number_of_checks():
    import subprocess
    import sys

    out = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider",
                          "-o", "addopts=", str(Path(__file__))], cwd=str(ROOT), capture_output=True, text=True)
    collected = [line for line in out.stdout.splitlines() if "::" in line]
    assert len(collected) >= _release_gate().MIN_INTEGRITY_TESTS


def test_gate_check_rejects_a_grandchild_thread(monkeypatch):
    """Un thread crea un discendente e termina: il discendente non deve sfuggire al controllo."""
    import threading

    checks = _gate_checks()
    monkeypatch.setattr(checks, "THREAD_JOIN_TIMEOUT_S", 1.0)
    release = threading.Event()

    def launcher_with_a_grandchild():
        def parent():
            release.wait(0.2)   # il discendente nasce DOPO la prima fotografia dei thread
            threading.Thread(target=release.wait, args=(5,), name="nipote").start()
        threading.Thread(target=parent, name="figlio").start()
        return 1

    try:
        with pytest.raises(AssertionError, match="nipote"):
            checks.assert_launcher_is_safe(monkeypatch, launcher_with_a_grandchild, ["pickfair"])
    finally:
        release.set()
