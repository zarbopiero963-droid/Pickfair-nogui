#!/usr/bin/env python3
"""Pipeline Buildkite `full-suite` della lane non fidata (issue #562, PR-A: suite completa automatica su ogni PR).

Stampa su stdout la pipeline in JSON per `buildkite-agent pipeline upload --no-interpolation`. Lo step di avvio
della pipeline `pf-dedicated-ci` (configurato da Grok nella UI di Buildkite, non nel repository) prende QUESTO file
da `origin/main` con `git show`, mai dal checkout della PR:

    git fetch -q origin main
    git show FETCH_HEAD:scripts/pr_full_suite_pipeline.py > "$TMPDIR/pf_full_suite_pipeline.py"
    /var/lib/buildkite/tools/py311/bin/python3.11 -I "$TMPDIR/pf_full_suite_pipeline.py" > "$TMPDIR/pipeline.json"
    buildkite-agent pipeline upload --no-interpolation "$TMPDIR/pipeline.json"

Una PR cambia quindi il codice sotto test ma non l'elenco dei controlli: non puo' togliere uno step ne' sostituirlo
con `exit 0`. Il dispatcher trusted (`scripts/pr_trusted_dispatcher.py`, `FULL_SUITE_STEP_KEYS`) pubblica `success`
sul contesto `full-suite` solo se la build e' `passed` e contiene TUTTI gli step attesi, ciascuno `passed`.

Gli step sono le Onde W1-W4 del collaudo #562 (c.6420456700, c.6421723959, c.6422118053, c.6422637166) sulla nuova
VPS: stesso preambolo (SHA verificato, venv per job, HOME e TMP per job) e stessi comandi, equivalenti ai workflow
GitHub Actions di `WORKFLOW_MAP` (test: `tests/scripts/test_pr_full_suite_pipeline.py`). Prima i controlli veloci
(priorita' alta), poi i moduli, poi suite complete, merge simulato e mutation.

Nessun segreto, nessun token, nessuna rete oltre a GitHub e PyPI. Solo libreria standard.
"""
from __future__ import annotations

import json
import re
import shlex
import sys
from dataclasses import dataclass

QUEUE = "pickfair-dedicated"
PYTHON = "/var/lib/buildkite/tools/py311/bin/python3.11"
ARTIFACTS = "bk-out/*"

#: preambolo comune a ogni job: lo SHA eseguito e' quello chiesto, venv e HOME isolati per job e cancellati all'uscita
PREAMBLE = (
    "set -euo pipefail",
    '[ "$(git rev-parse HEAD)" = "$BUILDKITE_COMMIT" ] || { echo "SHA mismatch"; exit 2; }',
    'echo "tested_sha=$(git rev-parse HEAD)"',
    'export HOME="$TMPDIR/home-$BUILDKITE_JOB_ID" XDG_CONFIG_HOME="$TMPDIR/home-$BUILDKITE_JOB_ID/.config" '
    'XDG_CACHE_HOME="$TMPDIR/home-$BUILDKITE_JOB_ID/.cache"',
    'mkdir -p "$XDG_CONFIG_HOME" "$XDG_CACHE_HOME"',
    f'VENV="$TMPDIR/venv-$BUILDKITE_JOB_ID"; {PYTHON} -m venv "$VENV"; . "$VENV/bin/activate"',
    "trap 'rm -rf \"$VENV\" \"$HOME\"' EXIT",
    "python --version; python -m pip install -q --upgrade pip",
    "mkdir -p bk-out",
)

TEST_DEPS = "pip install -q -r requirements-test.txt"
FULL_DEPS = "pip install -q -r requirements.txt -r requirements-dev.txt pytest pytest-xdist"
RUNTIME_DEPS = "pip install -q -r requirements.txt -r requirements-dev.txt pytest"
PYTEST_ONLY = "pip install -q pytest"

#: appimagetool della lane (M2, PR-B), installato da Grok con il suo sha256 accanto: il job non lo scarica mai
#: (su Actions `build_appimage.sh` prende la release `continuous` senza pin) e senza file si ferma fail-closed
APPIMAGETOOL_DIR = "/var/lib/buildkite/tools/appimagetool"

_PATH_RE = re.compile(r"[A-Za-z0-9_./-]+", re.ASCII)
_GLOB_RE = re.compile(r"[A-Za-z0-9_./*-]+", re.ASCII)
_KEY_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}", re.ASCII)


@dataclass(frozen=True)
class Job:
    key: str
    label: str
    timeout: int            # minuti
    priority: int           # 3 = mutation (per primo), 2 = veloce, 1 = moduli, 0 = pesante
    commands: tuple


@dataclass(frozen=True)
class Module:
    """Parametri di `_module-ultra-check.yml` chiamato dal workflow del modulo (`with:`)."""
    workflow: str
    module_path: str
    guardrails: tuple        # spec, contract, state model, mutation (quelli dichiarati)
    test_paths: str          # come nel workflow: percorsi o glob separati da spazio


MODULES = (
    Module("money-management", "core/money_management.py",
           ("guardrails/specs/core.money_management.json", "guardrails/contracts/core.money_management.json",
            "guardrails/state_models/core.money_management.json", "guardrails/mutations/core.money_management.json"),
           "tests/integration/test_money_management*"),
    Module("dutching", "dutching.py",
           ("guardrails/specs/dutching.json", "guardrails/contracts/dutching.json",
            "guardrails/state_models/dutching.json", "guardrails/mutations/dutching.json"),
           "tests"),
    Module("pnl-engine", "pnl_engine.py",
           ("guardrails/specs/pnl_engine.json", "guardrails/contracts/pnl_engine.json",
            "guardrails/state_models/pnl_engine.json", "guardrails/mutations/pnl_engine.json"),
           "tests/invariant/test_pnl_engine_drift_and_stress.py tests/invariant/test_core_pnl_engine_drift_and_stress.py"),
    Module("order-manager", "order_manager.py",
           ("guardrails/specs/order_manager.json", "guardrails/contracts/order_manager.json",
            "guardrails/state_models/order_manager.json", "guardrails/mutations/order_manager.json"),
           "tests/integration/test_order_manager* tests/recovery/test_order_manager*"),
    Module("risk-middleware", "core/risk_middleware.py",
           ("guardrails/specs/core.risk_middleware.json", "guardrails/contracts/core.risk_middleware.json",
            "guardrails/state_models/core.risk_middleware.json", "guardrails/mutations/core.risk_middleware.json"),
           "tests/integration/test_risk_middleware_copy_metadata.py "
           "tests/integration/test_risk_middleware_pattern_metadata.py"),
    Module("rate-limiter", "rate_limiter.py",
           ("guardrails/specs/rate_limiter.json", "guardrails/contracts/rate_limiter.json",
            "guardrails/state_models/rate_limiter.json", "guardrails/mutations/rate_limiter.json"),
           "tests/test_rate_limiter.py tests/unit/test_rate_limiter.py tests/integration/test_rate_limiter.py"),
    Module("reconciliation-engine", "core/reconciliation_engine.py", (), "tests/reconciliation"),
    Module("session-manager", "session_manager.py",
           ("guardrails/specs/session_manager.json", "guardrails/contracts/session_manager.json",
            "guardrails/state_models/session_manager.json", "guardrails/mutations/session_manager.json"),
           "tests/test_session_manager.py tests/unit/test_session_manager.py tests/integration/test_session_manager.py"),
    Module("execution-guard", "core/execution_guard.py",
           ("guardrails/specs/core.execution_guard.json", "guardrails/contracts/core.execution_guard.json",
            "guardrails/state_models/core.execution_guard.json", "guardrails/mutations/core.execution_guard.json"),
           "tests/integration/test_duplication_guard_copy_events.py "
           "tests/integration/test_duplication_guard_pattern_events.py"),
    Module("trading-engine", "core/trading_engine.py",
           ("guardrails/specs/core.trading_engine.json", "guardrails/contracts/core.trading_engine.json",
            "guardrails/state_models/core.trading_engine.json", "guardrails/mutations/core.trading_engine.json"),
           "tests/integration/test_trading_engine*"),
    Module("copy-engine", "copy_engine.py",
           ("guardrails/specs/copy_engine.json", "guardrails/contracts/copy_engine.json",
            "guardrails/state_models/copy_engine.json", "guardrails/mutations/copy_engine.json"),
           "tests/integration/test_copy_engine_dispatch.py tests/guardrails/test_copy_runtime_entrypoints.py"),
    Module("simulation-broker", "simulation_broker.py",
           ("guardrails/specs/simulation_broker.json", "guardrails/contracts/simulation_broker.json",
            "guardrails/state_models/simulation_broker.json", "guardrails/mutations/simulation_broker.json"),
           "tests/integration/test_simulation_broker*"),
    Module("runtime-controller", "core/runtime_controller.py",
           ("guardrails/specs/core.runtime_controller.json", "guardrails/contracts/core.runtime_controller.json",
            "guardrails/state_models/core.runtime_controller.json",
            "guardrails/mutations/core.runtime_controller.json"),
           "tests/integration/test_runtime_controller_copy_flow.py "
           "tests/integration/test_runtime_controller_pattern_flow.py "
           "tests/integration/test_runtime_controller_live_sim_parity.py"),
    Module("telegram-listener", "telegram_listener.py",
           ("guardrails/specs/telegram_listener.json", "guardrails/contracts/telegram_listener.json",
            "guardrails/state_models/telegram_listener.json", "guardrails/mutations/telegram_listener.json"),
           "tests/integration/test_telegram_listener*"),
)


def _q(path: str) -> str:
    if not _PATH_RE.fullmatch(path) or path.startswith(("/", "-")) or ".." in path.split("/"):
        raise ValueError(f"percorso non ammesso: {path!r}")
    return shlex.quote(path)


def ultra_check(m: Module) -> tuple:
    """`_module-ultra-check.yml`: modulo assente => skip come `skipped-report` di GHA; guardrail dichiarati presenti e
    JSON validi; `guardrail_runner.py` sul modulo; test mirati esistenti (glob espansi, percorsi mancanti saltati)."""
    _q(m.module_path)
    patterns = m.test_paths.split()
    for p in patterns:
        if not _GLOB_RE.fullmatch(p) or p.startswith(("/", "-")) or ".." in p.split("/"):
            raise ValueError(f"percorso di test non ammesso: {p!r}")
    module_name = m.module_path[:-3].replace("/", ".")
    cmds = [FULL_DEPS,
            f'[ -f {_q(m.module_path)} ] || {{ echo "modulo {m.module_path} assente: skip come GHA"; exit 0; }}']
    for g in m.guardrails:
        cmds.append(f'test -f {_q(g)} || {{ echo "Missing {g}"; exit 1; }}')
        cmds.append(f"python -c 'import json,sys; json.load(open(sys.argv[1])); print(\"OK JSON\", sys.argv[1])' {_q(g)}")
    cmds.append(f"python scripts/guardrail_runner.py --module {shlex.quote(module_name)}")
    # glob senza virgolette (come `for path in $TEST_PATHS` del workflow), validati sopra: solo [A-Za-z0-9_./*-]
    cmds.append(f'E=""; for q in {" ".join(patterns)}; do [ -e "$q" ] && E="$E $q"; done')
    cmds.append('if [ -n "$E" ]; then echo "focused:$E"; pytest -q -x -p no:cacheprovider $E '
                '--junitxml=bk-out/focused.xml; else echo "WARNING: nessun test mirato"; fi')
    return tuple(cmds)


def _jobs() -> tuple:
    fast = [
        Job("w1-repo-guardrails-ext", "W1 repo-guardrails-extended", 10, 2,
            ("python scripts/validate_guardrails.py", "python scripts/check_merge_markers.py")),
        Job("w1-repo-guardrails", "W1 repo-guardrails", 20, 2, (
            TEST_DEPS, "export PYTHONPATH=.",
            "pytest -q tests/guardrails/test_import_smoke_repo.py tests/guardrails/test_contract_snapshot_repo.py "
            "tests/guardrails/test_architecture_guardrails.py tests/guardrails/test_no_bypass_paths.py "
            "tests/guardrails/test_runtime_entrypoints.py --junitxml=bk-out/repo-guardrails.xml")),
        Job("w1-quarantine", "W1 ci-quarantine-guard (checker da main)", 10, 2, (
            'B="$TMPDIR/base-$BUILDKITE_JOB_ID"; git fetch -q origin main; git worktree add -q --detach "$B" '
            'FETCH_HEAD; echo "base=$(git -C "$B" rev-parse HEAD)"',
            'RC=0; python3 "$B/scripts/ci/check_ci_quarantine.py" . || RC=$?',
            '[ $RC -eq 0 ] && { python3 "$B/tests/guardrails/test_ci_quarantine.py" || RC=$?; }',
            'git worktree remove --force "$B"; exit $RC')),
        Job("w4-noise-free", "W4 noise-free-pr-gate", 10, 2, (
            "python scripts/check_merge_markers.py", "python scripts/validate_guardrails.py",
            "git fetch -q origin main", "python scripts/ci_changed_modules.py origin/main")),
        Job("w4-ci-dynamic-detect", "W4 ci-dynamic-intelligent detect", 10, 2, (
            "git fetch -q origin main",
            "python scripts/ci_changed_modules.py origin/main > bk-out/changed_modules.json",
            "cat bk-out/changed_modules.json")),
        Job("w4-readiness-gate", "W4 readiness-gate", 10, 2,
            (PYTEST_ONLY, "pytest -q tests/scripts/test_readiness_gate.py -x")),
        Job("w4-rollback-gate", "W4 rollback-gate", 10, 2,
            (PYTEST_ONLY, "pytest -q tests/scripts/test_rollback_gate.py -x")),
        Job("w4-live-gate", "W4 live-gate (solo test dello script, nessun ordine)", 10, 2,
            (PYTEST_ONLY, "pytest -q tests/scripts/test_live_gate.py -x")),
        Job("w4-incident-snapshot", "W4 incident-snapshot", 10, 2,
            (PYTEST_ONLY, "pytest -q tests/scripts/test_incident_snapshot.py -x")),
        Job("w3-observability-minimum", "W3 observability-minimum", 10, 2, (
            PYTEST_ONLY, "pytest -q tests/scripts/test_observability_minimum_check.py -x",
            "python scripts/observability_minimum_check.py --doc-path ops/observability_minimum.md")),
        Job("w1-unit", "W1 unit", 20, 2,
            (TEST_DEPS, 'pytest tests/unit -q -m "unit" --junitxml=bk-out/unit.xml')),
        Job("w1-core-guardrails", "W1 core-guardrails", 20, 2, (
            TEST_DEPS, "export PYTHONPATH=.",
            'pytest tests/unit -q -m "unit or guardrail" --junitxml=bk-out/core-guardrails.xml')),
        Job("w1-core-invariants", "W1 core-invariants", 20, 2, (
            TEST_DEPS, "export PYTHONPATH=.",
            'pytest tests/unit -q -m "core or concurrency or failure or invariant" --maxfail=1 --disable-warnings '
            "--cov=core --cov-report=term-missing --cov-report=xml:bk-out/coverage.xml "
            "--junitxml=bk-out/core-invariants.xml")),
        Job("w1-recovery-guardrails", "W1 recovery-guardrails", 20, 2, (
            TEST_DEPS, "export PYTHONPATH=.",
            "pytest -q tests/recovery/test_recovery_crash_mid_order.py "
            "tests/recovery/test_recovery_restart_with_pending_saga.py "
            "tests/recovery/test_recovery_reconcile_post_restart.py tests/recovery/test_recovery_invariants.py "
            "--junitxml=bk-out/recovery.xml")),
        Job("w3-smoke-core-fast", "W3 smoke-core-fast", 20, 2, (
            RUNTIME_DEPS,
            'if [ -d tests/unit ]; then pytest -q -x tests/unit; else echo "No unit tests dir"; fi',
            'if [ -d tests/smoke ]; then pytest -q -x tests/smoke; else echo "No smoke tests dir"; fi')),
    ]
    modules = [Job(f"w2-{m.workflow}", f"W2 {m.workflow} (ultra-check)", 25, 1, ultra_check(m)) for m in MODULES]
    modules += [
        Job("w2-pnl-invariants", "W2 pnl-engine pnl-invariants", 20, 1, (
            f"{TEST_DEPS} pytest", "test -f pnl_engine.py && test -f core/pnl_engine.py",
            "pytest -q --tb=short tests/invariant/test_pnl_engine_drift_and_stress.py "
            "tests/invariant/test_core_pnl_engine_drift_and_stress.py --junitxml=bk-out/pnl-inv.xml")),
        Job("w2-trading-engine-hard", "W2 trading-engine-hard-tests", 20, 1, (
            f"{TEST_DEPS} pytest",
            "pytest -q tests/core/test_trading_engine.py tests/core/test_trading_engine_internal_contracts.py "
            "--junitxml=bk-out/te-hard.xml")),
        Job("w2-stateful-integrity", "W2 stateful-integrity", 25, 1, (
            RUNTIME_DEPS,
            'for d in tests/recovery tests/reconciliation; do if [ -d "$d" ]; then pytest "$d" -q; '
            'else echo "no $d"; fi; done',
            "for f in tests/integration/test_duplication_guard_copy_events.py "
            "tests/integration/test_duplication_guard_pattern_events.py; do if [ -f \"$f\" ]; then pytest -q \"$f\"; "
            'else echo "SKIP $f"; fi; done')),
        Job("w3-integration", "W3 integration", 25, 1,
            (TEST_DEPS, 'pytest tests/integration -q -m "integration" --junitxml=bk-out/integration.xml')),
        Job("w3-e2e", "W3 e2e", 20, 1, (TEST_DEPS, 'pytest tests/e2e -q -m "e2e" --junitxml=bk-out/e2e.xml')),
        Job("w3-smoke", "W3 smoke", 20, 1, (TEST_DEPS, 'pytest tests/smoke -q -m "smoke" --junitxml=bk-out/smoke.xml')),
        Job("w3-failure", "W3 failure", 20, 1,
            (TEST_DEPS, 'pytest tests/failure -q -m "failure" --junitxml=bk-out/failure.xml')),
        Job("w3-net", "W3 net (rete isolata)", 20, 1, (TEST_DEPS, 'pytest tests/net -q -m "net" --junitxml=bk-out/net.xml')),
        Job("w3-chaos-critical", "W3 chaos-critical", 25, 1, (
            RUNTIME_DEPS,
            'if [ -d tests/chaos ]; then pytest -q -x tests/chaos; else echo "tests/chaos assente: skip"; fi',
            "if [ -f tests/integration/test_betfair_timeout_and_ghost_orders.py ]; then pytest -q -x "
            "tests/integration/test_betfair_timeout_and_ghost_orders.py; else echo skip; fi")),
        Job("w3-chaos-runtime", "W3 chaos-runtime", 25, 1, (
            RUNTIME_DEPS,
            'if [ -d tests/chaos ]; then pytest tests/chaos -q; else echo "No chaos tests dir"; fi',
            "if [ -f tests/integration/test_betfair_timeout_and_ghost_orders.py ]; then pytest "
            "tests/integration/test_betfair_timeout_and_ghost_orders.py -q; else echo skip; fi")),
        Job("w3-chaos-stateful", "W3 chaos-stateful", 30, 1, (
            RUNTIME_DEPS,
            *(f"pytest -q -x tests/chaos/test_chaos_stateful_{n}.py" for n in (
                "timeout_restart_reconcile", "partial_failure_replay", "concurrent_restart_integrity",
                "diagnostics_recovery", "reconcile_multi_pass", "dedup_after_restart")),
            "if [ -f tests/integration/test_betfair_timeout_and_ghost_orders.py ]; then pytest -q -x "
            "tests/integration/test_betfair_timeout_and_ghost_orders.py; else echo skip; fi",
            'for d in tests/recovery tests/reconciliation; do if [ -d "$d" ]; then pytest -q -x "$d"; '
            'else echo "no $d"; fi; done')),
        Job("w3-core-chaos", "W3 core-chaos", 25, 1, (
            TEST_DEPS,
            'pytest tests/unit tests/chaos -q -m "chaos" --maxfail=1 --disable-warnings',
            'pytest tests/unit -q -m "core and (concurrency or failure or invariant)"')),
        Job("w3-observability-runtime", "W3 observability-runtime", 25, 1, (
            RUNTIME_DEPS,
            "python -m compileall -q headless_main.py observability services/telegram_alerts_service.py "
            "services/telegram_service.py telegram_sender.py tests/observability tests/smoke/test_headless_boot_smoke.py "
            "tests/smoke/test_observability_full_flow.py tests/core/test_trading_engine_metrics_smoke.py "
            "tests/services/test_telegram_alert_pipeline.py",
            "pytest -q -x tests/observability/test_runtime_invariants.py tests/observability/test_anomaly_signals.py",
            "pytest -q tests/observability -x",
            "for t in tests/smoke/test_headless_boot_smoke.py tests/smoke/test_observability_full_flow.py "
            "tests/core/test_trading_engine_metrics_smoke.py tests/services/test_telegram_alert_pipeline.py "
            'tests/observability/test_diagnostics_export_smoke.py; do pytest -q "$t" -x; done')),
        Job("w3-observability-tests", "W3 observability-tests", 25, 1, (
            "pip install -q -r requirements.txt -r requirements-dev.txt pytest setuptools wheel",
            "pytest tests/observability -q -x")),
        Job("w4-live-sim-parity", "W4 live-sim-parity (solo SIM)", 20, 1, (
            RUNTIME_DEPS,
            *(f'if [ -f tests/integration/{t}.py ]; then pytest -q tests/integration/{t}.py; else echo "SKIP {t}"; fi'
              for t in ("test_trading_engine_live_sim_contract_parity", "test_order_manager_live_sim_parity",
                        "test_money_management_live_sim_parity", "test_runtime_controller_live_sim_parity",
                        "test_simulation_broker_pattern_parity", "test_simulation_broker_copy_parity",
                        "test_simulation_broker_cashout_parity")),
            "pytest -q tests/integration/test_simulation_broker_dutching_parity.py")),
        Job("w4-build-linux-tests", "W4 build-linux (solo tests/packaging)", 20, 1, (
            'python -c "import tkinter" 2>/dev/null || echo "NOTE: tkinter assente (GHA usa apt python3-tk)"',
            RUNTIME_DEPS, "python -m pytest tests/packaging -q")),
        # --- M2 (#562 PR-B): workflow Linux fuori dalle PR o finora esclusi -------------------------------------
        Job("w5-lockfile-fresh", "W5 generate-linux-lockfile (lock hash ancora valido)", 20, 1, (
            'python -m pip install -q "pip==24.3.1" "pip-tools==7.4.1"',
            'FRESH="$TMPDIR/fresh-linux-$BUILDKITE_JOB_ID.lock"',
            'if [ -f requirements-build-linux.lock ]; then cp requirements-build-linux.lock "$FRESH"; fi',
            # header canonico come nel workflow Actions: senza, pip-compile scrive il percorso temporaneo
            # dell'output e il diff con il lock committato fallisce sempre (Sol e Codex su #585)
            "CUSTOM_COMPILE_COMMAND='pip-compile --generate-hashes --allow-unsafe --no-annotate --output-file "
            "requirements-build-linux.lock requirements-build-linux.in' "
            'pip-compile --generate-hashes --allow-unsafe --no-annotate --output-file "$FRESH" '
            "requirements-build-linux.in",
            'cp "$FRESH" bk-out/requirements-build-linux.fresh.lock',
            "git fetch -q origin main",
            'if git cat-file -e FETCH_HEAD:requirements-build-linux.lock 2>/dev/null '
            '&& [ ! -f requirements-build-linux.lock ]; then '
            'echo "requirements-build-linux.lock presente su main ma rimosso nella PR"; exit 1; fi',
            'if [ -f requirements-build-linux.lock ]; then diff -u requirements-build-linux.lock "$FRESH" '
            '|| { echo "requirements-build-linux.lock STALE: rigeneralo"; exit 1; }; '
            'echo "OK: il lock combacia con la rigenerazione"; '
            'else echo "NOTE: nessun requirements-build-linux.lock committato"; fi')),
        Job("w5-telegram-live-sim", "W5 telegram-live-integration in simulazione (nessun messaggio reale)", 15, 1, (
            'if env | grep -q "^TG_"; then echo "credenziali Telegram nella lane non fidata: fermo"; exit 1; fi',
            RUNTIME_DEPS,
            "pytest -q tests/integration/test_telegram_live_sim.py --junitxml=bk-out/telegram-live-sim.xml")),
    ]
    heavy = [
        Job("w1-pr-check", "W1 pr-check (suite completa)", 40, 0, (
            "pip install -q -r requirements.txt -r requirements-test.txt",
            'pytest -q -n 4 --dist worksteal -m "not serial" --junitxml=bk-out/prcheck-par.xml',
            "pytest -q -m serial --junitxml=bk-out/prcheck-serial.xml")),
        Job("w2-backstop", "W2 ultra-check backstop (suite completa)", 40, 0, (
            FULL_DEPS,
            'pytest -q -n 2 --dist worksteal -m "not serial" --junitxml=bk-out/bs-par.xml',
            "pytest -q -m serial --junitxml=bk-out/bs-serial.xml")),
        Job("w4-merge-simulation", "W4 merge-simulation-hard (merge con main + suite completa)", 45, 0, (
            "git -c user.name=ci-bot -c user.email=ci@bot.local fetch -q origin main",
            "git checkout -q -B merge-sim-branch",   # -B: il checkout persistente conserva i branch locali (Sol)
            'git -c user.name=ci-bot -c user.email=ci@bot.local merge --no-ff --no-edit origin/main '
            '|| { echo "MERGE CONFLICT"; exit 1; }',
            "python scripts/check_merge_markers.py", FULL_DEPS, "python scripts/validate_guardrails.py",
            'pytest -q -n 2 --dist worksteal -m "not serial" -x', "pytest -q -x -m serial")),
        # priorita' massima: lo step piu' lungo parte per primo (FULL_SUITE_PERFORMANCE_REMEDIATION, owner #562)
        Job("w1-mutation", "W1 mutation-guardrails", 45, 3, (
            FULL_DEPS,
            "python scripts/run_mutation_guardrails.py --fail-under 80 --jobs 2 "
            "--output bk-out/mutation_guardrails_report.json")),
        Job("w5-build-linux-appimage", "W5 build-linux: eseguibili, AppImage e release in DRY-RUN", 45, 0, (
            'python -c "import tkinter; print(\'Tk\', tkinter.TkVersion)" '
            '|| { echo "tkinter assente nel python della lane: GUI non impacchettabile (fail-closed)"; exit 1; }',
            '[ -f requirements-build-linux.lock ] || { echo "requirements-build-linux.lock mancante (fail-closed)"; '
            "exit 1; }",
            f'[ -x {APPIMAGETOOL_DIR}/appimagetool ] && [ -s {APPIMAGETOOL_DIR}/appimagetool.sha256 ] '
            f'|| {{ echo "appimagetool non installato in {APPIMAGETOOL_DIR} (infrastruttura): fermo"; exit 1; }}',
            # venv dedicato SENZA aggiornare pip: il lock hash-verificato e' l'unica sorgente della toolchain
            "deactivate",
            f'BVENV="$TMPDIR/bvenv-$BUILDKITE_JOB_ID"; {PYTHON} -m venv "$BVENV"; . "$BVENV/bin/activate"',
            "trap 'rm -rf \"$VENV\" \"$HOME\" \"$BVENV\" Pickfair.AppDir' EXIT",
            "python -m pip install -q --require-hashes -r requirements-build-linux.lock",
            "python -m pytest tests/packaging -q",
            'ver=$(python -c "import tomllib; print(tomllib.load(open(\'pyproject.toml\', \'rb\'))[\'project\'][\'version\'])")',
            '[ -n "$ver" ] || { echo "version mancante in pyproject.toml"; exit 1; }',
            'stamp=$(date -u +%Y%m%d); BINTAR="Pickfair-Linux-bin-v${ver}-${stamp}"; '
            'APPIMAGE="Pickfair-v${ver}-x86_64.AppImage"',
            "pyinstaller --noconfirm --clean pickfair.spec",
            "chmod +x dist/pickfair dist/pickfair-headless",
            'tar -czf "bk-out/${BINTAR}.tar.gz" -C dist pickfair pickfair-headless',
            f'export PATH="{APPIMAGETOOL_DIR}:$PATH" APPIMAGETOOL_SHA256="$(awk \'{{print $1; exit}}\' {APPIMAGETOOL_DIR}/appimagetool.sha256)"',
            "chmod +x packaging/build_appimage.sh packaging/AppRun",
            'packaging/build_appimage.sh dist/pickfair "bk-out/${APPIMAGE}"',
            'python -c "import sys; [sys.exit(f + \' non ELF\') for f in sys.argv[1:] '
            'if open(f, \'rb\').read(4) != b\'\\x7fELF\']" dist/pickfair dist/pickfair-headless "bk-out/${APPIMAGE}"',
            # release in DRY-RUN: stessi controlli di `build-linux.yml` (entrambi gli asset), nessuna pubblicazione
            "shopt -s nullglob; bins=(bk-out/Pickfair-Linux-bin-*.tar.gz); apps=(bk-out/Pickfair-*-x86_64.AppImage)",
            '[ ${#bins[@]} -eq 1 ] && [ ${#apps[@]} -eq 1 ] '
            '|| { echo "asset mancante (binari: ${#bins[@]}, AppImage: ${#apps[@]}): release non preparata"; exit 1; }',
            '(cd bk-out && sha256sum "${bins[0]#bk-out/}" "${apps[0]#bk-out/}" > SHA256SUMS && cat SHA256SUMS)',
            'echo "DRY-RUN release: asset e SHA256SUMS pronti come artifact; nessuna release GitHub pubblicata"')),
    ]
    first = [j for j in heavy if j.priority == 3]                     # il mutation, lo step piu' lungo
    return tuple(first + fast + modules + [j for j in heavy if j.priority != 3])


JOBS = _jobs()

#: workflow GitHub Actions che girano su `pull_request` (anche tramite `ci-dynamic-intelligent`/`ci-master-gate`)
#: -> step della full-suite che li rende equivalenti. Base del mapping dei required check (D-3 della #562).
WORKFLOW_MAP = {
    "unit": ("w1-unit",), "pr-check": ("w1-pr-check",), "repo-guardrails": ("w1-repo-guardrails",),
    "repo-guardrails-extended": ("w1-repo-guardrails-ext",), "core-guardrails": ("w1-core-guardrails",),
    "core-invariants": ("w1-core-invariants",), "recovery-guardrails": ("w1-recovery-guardrails",),
    "ci-quarantine-guard": ("w1-quarantine",), "mutation-guardrails": ("w1-mutation",),
    **{m.workflow: (f"w2-{m.workflow}",) for m in MODULES},
    "trading-engine-hard-tests": ("w2-trading-engine-hard",), "stateful-integrity": ("w2-stateful-integrity",),
    "integration": ("w3-integration",), "e2e": ("w3-e2e",), "smoke": ("w3-smoke",), "failure": ("w3-failure",),
    "net": ("w3-net",), "chaos-critical": ("w3-chaos-critical",),
    "observability-minimum": ("w3-observability-minimum",), "observability-runtime": ("w3-observability-runtime",),
    "observability-tests": ("w3-observability-tests",), "smoke-core-fast": ("w3-smoke-core-fast",),
    "readiness-gate": ("w4-readiness-gate",), "rollback-gate": ("w4-rollback-gate",),
    "noise-free-pr-gate": ("w4-noise-free",), "ci-dynamic-intelligent": ("w4-ci-dynamic-detect", "w2-backstop"),
    "merge-simulation-hard": ("w4-merge-simulation",), "live-sim-parity": ("w4-live-sim-parity",),
    "live-gate": ("w4-live-gate",), "incident-snapshot": ("w4-incident-snapshot",),
    "generate-linux-lockfile": ("w5-lockfile-fresh",),
}
# `pnl-engine.yml` ha anche il job `pnl-invariants`
WORKFLOW_MAP["pnl-engine"] = ("w2-pnl-engine", "w2-pnl-invariants")
#: workflow che su Actions NON girano sulle PR (solo `workflow_dispatch`/`push`) ma che la full-suite esegue comunque:
#: copertura piu' ampia di quella di Actions, non un'equivalenza (#562 W3/W4)
WORKFLOW_EXTRA = {
    "chaos-runtime": ("w3-chaos-runtime",), "chaos-stateful": ("w3-chaos-stateful",),
    "core-chaos": ("w3-core-chaos",),
    "build-linux": ("w4-build-linux-tests", "w5-build-linux-appimage"),
    "telegram-live-integration": ("w5-telegram-live-sim",),
}

#: workflow su `pull_request` NON portati nella full-suite, con il motivo (restano su Actions o su altro percorso)
WORKFLOW_EXCLUDED = {
    "ci-master-gate": "aggregatore: i workflow che chiama sono mappati singolarmente",
    "pr-guard": "metadati della PR (titolo, corpo, file) e registro dello scope: resta su Actions (check `guard`)",
    "pr-overlap-guard": "lane trusted (`overlap-check` del dispatcher) e required check di Actions",
    "pr-merge-readiness": "lane trusted (`pr-merge-readiness` del dispatcher) e `Merge readiness` di Actions",
    "complete-task-on-merge": "contabilita' dei task alla chiusura della PR, non un controllo",
    "pr-review-openrouter-gpt56-sol": "review AI: `pull_request_target` con secret, resta su Actions",
    "pr-review-xai-grok46": "review AI: `pull_request_target` con secret, resta su Actions",
    "pr-review-claude-fable5": "reviewer sospeso (P41)",
    "pr-review-openrouter-fugu-ultra": "reviewer sospeso (P41)",
    "pr-review-openrouter-gpt-astra": "reviewer sospeso (P41)",
}


#: workflow fuori dalle PR (M2, #562 PR-B) NON migrati, con il motivo: nessuno e' dichiarato equivalente
WORKFLOW_NOT_MIGRATED = {
    "build-windows-exe": "build Windows: esclusa dal mandato #562 (solo workflow Linux)",
    "telegram-test": "invia un messaggio Telegram reale: vietato (nessun invio Telegram reale)",
    "merge-simulation": "variante manuale di `merge-simulation-hard`, gia' coperta da w4-merge-simulation",
    "queue-on-pr-closed": "recupero manuale della coda task: scrive sul repository, non e' un controllo",
    "pr-overlap-guard-diagnostic": "diagnostica manuale della lane trusted (overlap-check), non della full-suite",
    "_module-ultra-check": "workflow riusabile: eseguito tramite i moduli (`ultra_check`, `MODULES`)",
}

#: cio' che la migrazione NON certifica, workflow per workflow: mai dichiarato PASS
WORKFLOW_LIMITS = {
    "telegram-live-integration": "simulazione offline degli stessi 7 scenari: trasporto Telethon/Bot API, "
                                 "consegna e chat reale NON certificati (servono credenziali e GO owner)",
    "build-linux": "release in DRY-RUN (asset e SHA256SUMS come artifact Buildkite): la pubblicazione della "
                   "GitHub Release sui tag resta su Actions; avvio grafico dell'AppImage NON eseguito",
    "generate-linux-lockfile": "la Job Summary di Actions diventa l'artifact requirements-build-linux.fresh.lock",
}


def step(job: Job) -> dict:
    if not _KEY_RE.fullmatch(job.key):
        raise ValueError(f"chiave di step non ammessa: {job.key!r}")
    return {"label": job.label, "key": job.key, "agents": {"queue": QUEUE}, "timeout_in_minutes": job.timeout,
            "priority": job.priority, "artifact_paths": ARTIFACTS,
            "command": "\n".join((*PREAMBLE, *job.commands))}


def pipeline() -> dict:
    keys = [j.key for j in JOBS]
    if len(set(keys)) != len(keys):
        raise ValueError("chiavi di step duplicate")
    return {"steps": [step(j) for j in JOBS]}


def main(argv: list) -> int:
    if argv:
        print("uso: pr_full_suite_pipeline.py (nessun argomento): stampa la pipeline JSON", file=sys.stderr)
        return 2
    json.dump(pipeline(), sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
