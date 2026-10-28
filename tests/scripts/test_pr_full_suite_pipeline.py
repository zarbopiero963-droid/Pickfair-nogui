"""Test del generatore della pipeline `full-suite` (#562 PR-A, `scripts/pr_full_suite_pipeline.py`).

Il generatore gira sulla lane NON fidata, ma preso da `origin/main`: questi test fissano che produca solo step sulla
coda `pickfair-dedicated`, con lo SHA verificato, comandi bash validi e nessun segreto, e che resti EQUIVALENTE ai
workflow GitHub Actions che girano sulle PR (mapping completo: ogni workflow e' coperto o escluso con un motivo).
"""
from __future__ import annotations

import importlib.util
import io
import json
import shutil
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("pr_full_suite_pipeline", ROOT / "scripts" / "pr_full_suite_pipeline.py")
fsp = importlib.util.module_from_spec(_spec)
sys.modules["pr_full_suite_pipeline"] = fsp
_spec.loader.exec_module(fsp)

WORKFLOWS = ROOT / ".github" / "workflows"
AGGREGATORS = ("ci-dynamic-intelligent", "ci-master-gate")


def _workflow(name: str) -> dict:
    for ext in (".yml", ".yaml"):
        path = WORKFLOWS / f"{name}{ext}"
        if path.exists():
            return yaml.safe_load(path.read_text(encoding="utf-8"))
    raise FileNotFoundError(name)


def _triggers(wf: dict) -> set:
    on = wf.get(True, wf.get("on"))
    return set(on) if isinstance(on, (dict, list)) else {on}


def _pr_workflows() -> set:
    """Workflow che girano su una PR: trigger `pull_request`/`pull_request_target` diretto o chiamati da un
    aggregatore che gira su `pull_request`."""
    names = {p.stem for p in WORKFLOWS.glob("*.y*ml")}
    direct = {n for n in names if _triggers(_workflow(n)) & {"pull_request", "pull_request_target"}}
    called = set()
    for agg in AGGREGATORS:
        for job in (_workflow(agg).get("jobs") or {}).values():
            uses = job.get("uses") if isinstance(job, dict) else None
            if isinstance(uses, str) and uses.startswith("./.github/workflows/"):
                called.add(Path(uses).stem)
    return direct | called


def _pipeline() -> dict:
    out = io.StringIO()
    with redirect_stdout(out):
        assert fsp.main([]) == 0
    return json.loads(out.getvalue())


def test_main_prints_the_pipeline_json_and_refuses_arguments(capsys):
    steps = _pipeline()["steps"]
    assert len(steps) == len(fsp.JOBS) == 52
    assert fsp.main(["--anything"]) == 2


def test_every_step_runs_on_the_untrusted_queue_with_the_sha_check_first():
    for step in _pipeline()["steps"]:
        assert step["agents"] == {"queue": "pickfair-dedicated"}
        assert set(step) == {"label", "key", "agents", "timeout_in_minutes", "priority", "artifact_paths", "command"}
        lines = step["command"].splitlines()
        assert lines[:len(fsp.PREAMBLE)] == list(fsp.PREAMBLE), step["key"]
        assert lines[1] == '[ "$(git rev-parse HEAD)" = "$BUILDKITE_COMMIT" ] || { echo "SHA mismatch"; exit 2; }'
        assert isinstance(step["timeout_in_minutes"], int) and 1 <= step["timeout_in_minutes"] <= 60
        assert step["priority"] in (0, 1, 2)


def test_step_keys_are_unique_and_well_formed():
    keys = [s["key"] for s in _pipeline()["steps"]]
    assert len(keys) == len(set(keys))
    assert all(fsp._KEY_RE.fullmatch(k) for k in keys)


def test_commands_are_valid_bash_and_never_double_dollar():
    # l'upload usa `--no-interpolation`: `$$` (escape della UI) lascerebbe un `$` doppio nel comando eseguito
    if shutil.which("bash") is None:
        pytest.skip("bash assente")
    for step in _pipeline()["steps"]:
        assert "$$" not in step["command"], step["key"]
        proc = subprocess.run(["bash", "-n"], input=step["command"], text=True, capture_output=True, check=False)
        assert proc.returncode == 0, (step["key"], proc.stderr)


def test_no_secret_token_or_live_reference_in_any_command():
    text = json.dumps(_pipeline()).lower()
    for forbidden in ("secrets.", "github_token", "gh_token", "bkua_", "betfair_app_key", "app_key", "password",
                      "telegram_bot_token", "--live", "live_mode=1", "pf-trusted-lane"):
        assert forbidden not in text, forbidden


def test_fast_checks_run_before_modules_and_heavy_suites():
    steps = _pipeline()["steps"]
    prio = [s["priority"] for s in steps]
    assert prio == sorted(prio, reverse=True)                       # ordine di elenco = ordine di priorita'
    heavy = {s["key"] for s in steps if s["priority"] == 0}
    assert heavy == {"w1-pr-check", "w2-backstop", "w4-merge-simulation", "w1-mutation", "w5-build-linux-appimage"}


def test_module_table_matches_the_workflow_inputs():
    for m in fsp.MODULES:
        jobs = _workflow(m.workflow)["jobs"]
        calls = [j["with"] for j in jobs.values()
                 if isinstance(j, dict) and str(j.get("uses", "")).endswith("_module-ultra-check.yml")]
        assert len(calls) == 1, m.workflow
        w = calls[0]
        assert w["module_path"] == m.module_path, m.workflow
        assert w.get("test_paths", "") == m.test_paths, m.workflow
        declared = tuple(w[k] for k in ("spec_path", "contract_path", "state_model_path", "mutation_path") if w.get(k))
        assert declared == m.guardrails, m.workflow


def test_every_module_workflow_with_ultra_check_is_in_the_table():
    users = set()
    for path in WORKFLOWS.glob("*.y*ml"):
        for job in (yaml.safe_load(path.read_text(encoding="utf-8")).get("jobs") or {}).values():
            if isinstance(job, dict) and str(job.get("uses", "")).endswith("_module-ultra-check.yml"):
                users.add(path.stem)
    assert users == {m.workflow for m in fsp.MODULES}


def test_every_pr_workflow_is_mapped_or_excluded_with_a_reason():
    pr = _pr_workflows()
    mapped, excluded = set(fsp.WORKFLOW_MAP), set(fsp.WORKFLOW_EXCLUDED)
    assert not mapped & excluded
    assert pr - mapped - excluded == set(), "workflow su PR senza step equivalente ne' esclusione motivata"
    assert all(isinstance(r, str) and len(r) > 10 for r in fsp.WORKFLOW_EXCLUDED.values())
    keys = {j.key for j in fsp.JOBS}
    for wf, steps in fsp.WORKFLOW_MAP.items():
        assert steps and set(steps) <= keys, wf
        assert (WORKFLOWS / f"{wf}.yml").exists() or (WORKFLOWS / f"{wf}.yaml").exists(), wf


def test_every_step_is_mapped_to_a_workflow():
    assert not set(fsp.WORKFLOW_MAP) & set(fsp.WORKFLOW_EXTRA)
    for wf in fsp.WORKFLOW_EXTRA:
        # piu' ampia di Actions: workflow che su Actions non girano sulle PR
        assert not _triggers(_workflow(wf)) & {"pull_request", "pull_request_target"}, wf
        assert wf not in _pr_workflows(), wf
    used = {k for m in (fsp.WORKFLOW_MAP, fsp.WORKFLOW_EXTRA) for steps in m.values() for k in steps}
    assert used == {j.key for j in fsp.JOBS}


@pytest.mark.parametrize("bad", ["/etc/passwd", "../x.py", "-rf.py", "a b.py", "x;rm.py", "$(id).py"])
def test_unsafe_module_paths_are_refused(bad):
    with pytest.raises(ValueError):
        fsp.ultra_check(fsp.Module("x", bad, (), "tests"))


@pytest.mark.parametrize("bad", ["/etc", "../x", "-rf", "x;rm", "$(id)", "a`b`", "tests/../..", "a|b"])
def test_unsafe_test_patterns_are_refused(bad):
    # i pattern sono separati da spazio come nel workflow: ognuno deve essere un percorso/glob sicuro
    with pytest.raises(ValueError):
        fsp.ultra_check(fsp.Module("x", "ok.py", (), f"tests {bad}"))


def test_ultra_check_skips_an_absent_module_like_gha(tmp_path):
    if shutil.which("bash") is None:
        pytest.skip("bash assente")
    cmds = fsp.ultra_check(fsp.Module("x", "absent_module.py", (), "tests"))
    # dal secondo comando: il modulo assente esce 0 prima di qualunque altro controllo (come `skipped-report`)
    script = "set -euo pipefail\n" + "\n".join(cmds[1:])
    proc = subprocess.run(["bash", "-c", script], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert proc.returncode == 0 and "assente" in proc.stdout


def test_ultra_check_fails_on_a_missing_declared_guardrail(tmp_path):
    if shutil.which("bash") is None:
        pytest.skip("bash assente")
    (tmp_path / "mod.py").write_text("x = 1\n")
    cmds = fsp.ultra_check(fsp.Module("x", "mod.py", ("guardrails/specs/mod.json",), "tests"))
    script = "set -euo pipefail\n" + "\n".join(cmds[1:])
    proc = subprocess.run(["bash", "-c", script], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert proc.returncode == 1 and "Missing guardrails/specs/mod.json" in proc.stdout


def test_quarantine_step_takes_the_checker_from_main_not_from_the_pr():
    (step,) = [s for s in _pipeline()["steps"] if s["key"] == "w1-quarantine"]
    assert 'git worktree add -q --detach "$B" FETCH_HEAD' in step["command"]
    assert 'python3 "$B/scripts/ci/check_ci_quarantine.py" .' in step["command"]


# ----------------------------------------------------------------------------------------------- M2 (#562 PR-B)
def _job(key):
    return next(j for j in fsp.JOBS if j.key == key)


def _commands(key):
    return "\n".join(_job(key).commands)


def test_every_workflow_file_is_mapped_extra_excluded_or_not_migrated_with_a_reason():
    names = {p.stem for p in WORKFLOWS.glob("*.y*ml")}
    groups = (set(fsp.WORKFLOW_MAP), set(fsp.WORKFLOW_EXTRA), set(fsp.WORKFLOW_EXCLUDED),
              set(fsp.WORKFLOW_NOT_MIGRATED))
    assert sum(len(g) for g in groups) == len(set().union(*groups)), "un workflow in due gruppi"
    assert names == set().union(*groups), "workflow senza step ne' motivo di esclusione"
    assert all(isinstance(r, str) and len(r) > 10 for r in fsp.WORKFLOW_NOT_MIGRATED.values())
    # i non migrati non girano sulle PR: quelli che ci girano devono essere mappati o esclusi esplicitamente
    assert not set(fsp.WORKFLOW_NOT_MIGRATED) & _pr_workflows()


def test_limits_are_declared_only_for_covered_workflows():
    covered = set(fsp.WORKFLOW_MAP) | set(fsp.WORKFLOW_EXTRA)
    assert set(fsp.WORKFLOW_LIMITS) <= covered
    assert {"telegram-live-integration", "build-linux"} <= set(fsp.WORKFLOW_LIMITS)
    assert all(len(r) > 20 for r in fsp.WORKFLOW_LIMITS.values())


def test_real_telegram_and_windows_workflows_are_never_migrated():
    assert "telegram-test" in fsp.WORKFLOW_NOT_MIGRATED and "build-windows-exe" in fsp.WORKFLOW_NOT_MIGRATED
    text = json.dumps(_pipeline()).lower()
    for forbidden in ("api.telegram.org", "sendmessage", "tg_bot_token", "tg_session_string", "windows"):
        assert forbidden not in text, forbidden


def test_build_linux_job_mirrors_the_workflow_build_steps():
    wf = json.dumps(_workflow("build-linux")["jobs"]["build"]["steps"])
    cmd = _commands("w5-build-linux-appimage")
    for piece in ("--require-hashes -r requirements-build-linux.lock", "python -m pytest tests/packaging -q",
                  "pyinstaller --noconfirm --clean pickfair.spec", "chmod +x dist/pickfair dist/pickfair-headless",
                  "packaging/build_appimage.sh dist/pickfair", "import tkinter"):
        assert piece in wf and piece in cmd, piece


def test_build_linux_job_never_publishes_and_never_downloads_tools():
    cmd = _commands("w5-build-linux-appimage")
    for forbidden in ("gh ", "release create", "release upload", "curl", "wget", "APPIMAGETOOL_URL"):
        assert forbidden not in cmd, forbidden
    assert "DRY-RUN" in cmd and "SHA256SUMS" in cmd
    assert fsp.APPIMAGETOOL_DIR in cmd and "APPIMAGETOOL_SHA256=" in cmd


def test_build_linux_job_installs_the_lock_with_a_fresh_pip_never_upgraded():
    cmd = _job("w5-build-linux-appimage").commands
    after = cmd[cmd.index("deactivate"):]
    assert not any("--upgrade pip" in c for c in after)            # il pip aggiornato del preambolo resta fuori
    install = next(i for i, c in enumerate(after) if "--require-hashes" in c)
    assert any("BVENV" in c and "-m venv" in c for c in after[:install])


def test_lockfile_job_mirrors_the_workflow_pins_and_arguments():
    wf = json.dumps(_workflow("generate-linux-lockfile"))
    cmd = _commands("w5-lockfile-fresh")
    for piece in ('pip==24.3.1', 'pip-tools==7.4.1', "--generate-hashes --allow-unsafe --no-annotate",
                  "requirements-build-linux.in"):
        assert piece in wf and piece in cmd, piece


def _bash(script, cwd, env=None):
    return subprocess.run(["bash", "-c", "set -euo pipefail\n" + script], cwd=cwd, env=env,
                          capture_output=True, text=True)


@pytest.mark.skipif(shutil.which("bash") is None or shutil.which("git") is None, reason="bash/git assenti")
def test_lockfile_removed_in_the_pr_fails_against_main(tmp_path):
    guard = next(c for c in _job("w5-lockfile-fresh").commands if "presente su main ma rimosso" in c)
    run = lambda *a: subprocess.run(["git", *a], cwd=tmp_path, check=True, capture_output=True)
    run("init", "-q", "-b", "main")
    (tmp_path / "requirements-build-linux.lock").write_text("x==1 --hash=sha256:00\n")
    run("add", ".")
    run("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "main")
    run("update-ref", "FETCH_HEAD", "HEAD")
    assert _bash(guard, tmp_path).returncode == 0                   # lock presente: nessun errore
    (tmp_path / "requirements-build-linux.lock").unlink()
    res = _bash(guard, tmp_path)
    assert res.returncode == 1 and "rimosso nella PR" in res.stdout


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash assente")
def test_telegram_sim_job_refuses_telegram_credentials_in_the_lane(tmp_path):
    guard = _job("w5-telegram-live-sim").commands[0]
    clean = {"PATH": "/usr/bin:/bin"}
    assert _bash(guard, tmp_path, clean).returncode == 0
    assert _bash(guard, tmp_path, {**clean, "TG_BOT_TOKEN": "x"}).returncode == 1
    assert "tests/integration/test_telegram_live_sim.py" in _commands("w5-telegram-live-sim")


@pytest.mark.skipif(shutil.which("bash") is None or shutil.which("sha256sum") is None, reason="bash assente")
@pytest.mark.parametrize("assets,ok", [((), False), (("bin",), False), (("app",), False), (("bin", "app"), True),
                                       (("bin", "bin2", "app"), False)])
def test_dry_run_release_requires_exactly_both_assets(tmp_path, assets, ok):
    cmds = _job("w5-build-linux-appimage").commands
    start = next(i for i, c in enumerate(cmds) if c.startswith("shopt -s nullglob"))
    out = tmp_path / "bk-out"
    out.mkdir()
    names = {"bin": "Pickfair-Linux-bin-v1-20261028.tar.gz", "bin2": "Pickfair-Linux-bin-v2-20261028.tar.gz",
             "app": "Pickfair-v1-x86_64.AppImage"}
    for a in assets:
        (out / names[a]).write_bytes(b"x")
    res = _bash("\n".join(cmds[start:]), tmp_path)
    assert (res.returncode == 0) is ok, res.stdout + res.stderr
    assert (out / "SHA256SUMS").exists() is ok
