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
    assert len(steps) == len(fsp.JOBS) == 49
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
    assert heavy == {"w1-pr-check", "w2-backstop", "w4-merge-simulation", "w1-mutation"}


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


def _git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd, text=True,
                          capture_output=True, check=True).stdout.strip()


def test_merge_simulation_runs_again_in_the_same_persistent_checkout(tmp_path):
    """Sol su #584: gli agent Buildkite riusano il checkout (`git clean -ffxdq`, `git checkout -f`), che non
    cancella i branch locali. La seconda build nello stesso checkout non deve fallire perche' il branch della
    simulazione di merge esiste gia': la suite sarebbe rossa senza alcuna regressione."""
    if shutil.which("bash") is None or shutil.which("git") is None:
        pytest.skip("bash o git assenti")
    origin, work = tmp_path / "origin.git", tmp_path / "checkout"
    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-q", "-b", "main")
    (seed / "a.txt").write_text("main\n")
    _git(seed, "add", "a.txt")
    _git(seed, "commit", "-q", "-m", "main")
    _git(tmp_path, "clone", "-q", "--bare", str(seed), str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(work))
    _git(work, "checkout", "-q", "-b", "feature")
    (work / "b.txt").write_text("pr\n")
    _git(work, "add", "b.txt")
    _git(work, "commit", "-q", "-m", "pr")
    head = _git(work, "rev-parse", "HEAD")
    (job,) = [j for j in fsp.JOBS if j.key == "w4-merge-simulation"]
    script = "set -euo pipefail\n" + "\n".join(job.commands[:3])   # fetch, branch, merge con main
    for _ in range(2):                                             # due build sullo stesso checkout
        _git(work, "checkout", "-q", "-f", head)                   # come il checkout dell'agent
        _git(work, "clean", "-ffxdq")
        proc = subprocess.run(["bash", "-c", script], cwd=work, text=True, capture_output=True, check=False)
        assert proc.returncode == 0, proc.stderr
        assert _git(work, "merge-base", "--is-ancestor", head, "HEAD") == ""
