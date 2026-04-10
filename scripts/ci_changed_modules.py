from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


MODULE_RULES = [
    {
        "name": "trading-engine",
        "paths": [
            "core/trading_engine.py",
            "guardrails/specs/core.trading_engine.json",
            "guardrails/contracts/core.trading_engine.json",
            "guardrails/state_models/core.trading_engine.json",
            "guardrails/mutations/core.trading_engine.json",
            "tests/integration/test_trading_engine",
        ],
    },
    {
        "name": "order-manager",
        "paths": [
            "order_manager.py",
            "guardrails/specs/order_manager.json",
            "guardrails/contracts/order_manager.json",
            "guardrails/state_models/order_manager.json",
            "guardrails/mutations/order_manager.json",
            "tests/integration/test_order_manager",
        ],
    },
    {
        "name": "execution-guard",
        "paths": [
            "core/execution_guard.py",
            "guardrails/specs/core.execution_guard.json",
            "guardrails/contracts/core.execution_guard.json",
            "guardrails/state_models/core.execution_guard.json",
            "guardrails/mutations/core.execution_guard.json",
            "tests/integration/test_duplication_guard",
        ],
    },
    {
        "name": "risk-middleware",
        "paths": [
            "core/risk_middleware.py",
            "guardrails/specs/core.risk_middleware.json",
            "guardrails/contracts/core.risk_middleware.json",
            "guardrails/state_models/core.risk_middleware.json",
            "guardrails/mutations/core.risk_middleware.json",
            "tests/integration/test_risk_middleware",
        ],
    },
    {
        "name": "runtime-controller",
        "paths": [
            "core/runtime_controller.py",
            "guardrails/specs/core.runtime_controller.json",
            "guardrails/contracts/core.runtime_controller.json",
            "guardrails/state_models/core.runtime_controller.json",
            "guardrails/mutations/core.runtime_controller.json",
            "tests/integration/test_runtime_controller",
        ],
    },
    {
        "name": "money-management",
        "paths": [
            "core/money_management.py",
            "guardrails/specs/core.money_management.json",
            "guardrails/contracts/core.money_management.json",
            "guardrails/state_models/core.money_management.json",
            "guardrails/mutations/core.money_management.json",
            "tests/integration/test_money_management",
        ],
    },
    {
        "name": "telegram-listener",
        "paths": [
            "telegram_listener.py",
            "guardrails/specs/telegram_listener.json",
            "guardrails/contracts/telegram_listener.json",
            "guardrails/state_models/telegram_listener.json",
            "guardrails/mutations/telegram_listener.json",
            "tests/integration/test_telegram_listener",
        ],
    },
    {
        "name": "copy-engine",
        "paths": [
            "copy_engine.py",
            "guardrails/specs/copy_engine.json",
            "guardrails/contracts/copy_engine.json",
            "guardrails/state_models/copy_engine.json",
            "guardrails/mutations/copy_engine.json",
            "tests/integration/test_copy_engine",
            "tests/guardrails/test_copy_runtime_entrypoints.py",
        ],
    },
    {
        "name": "simulation-broker",
        "paths": [
            "simulation_broker.py",
            "guardrails/specs/simulation_broker.json",
            "guardrails/contracts/simulation_broker.json",
            "guardrails/state_models/simulation_broker.json",
            "guardrails/mutations/simulation_broker.json",
            "tests/integration/test_simulation_broker",
        ],
    },
    {
        "name": "session-manager",
        "paths": [
            "session_manager.py",
            "guardrails/specs/session_manager.json",
            "guardrails/contracts/session_manager.json",
            "guardrails/state_models/session_manager.json",
            "guardrails/mutations/session_manager.json",
            "tests/test_session_manager.py",
            "tests/unit/test_session_manager.py",
            "tests/integration/test_session_manager.py",
        ],
    },
    {
        "name": "rate-limiter",
        "paths": [
            "rate_limiter.py",
            "guardrails/specs/rate_limiter.json",
            "guardrails/contracts/rate_limiter.json",
            "guardrails/state_models/rate_limiter.json",
            "guardrails/mutations/rate_limiter.json",
            "tests/test_rate_limiter.py",
            "tests/unit/test_rate_limiter.py",
            "tests/integration/test_rate_limiter.py",
        ],
    },
    {
        "name": "live-gate",
        "paths": [
            "live_gate.py",
            "guardrails/specs/live_gate.json",
            "guardrails/contracts/live_gate.json",
            "guardrails/state_models/live_gate.json",
            "guardrails/mutations/live_gate.json",
            "tests/test_live_gate.py",
            "tests/unit/test_live_gate.py",
            "tests/integration/test_live_gate.py",
        ],
    },
]


def get_changed_files(base_ref: str) -> list[str]:
    cmd = ["git", "diff", "--name-only", base_ref, "HEAD"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(proc.returncode)
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def matches_rule(changed_file: str, rule_paths: list[str]) -> bool:
    for path in rule_paths:
        if changed_file == path:
            return True
        if path.endswith("/") and changed_file.startswith(path):
            return True
        if changed_file.startswith(path):
            return True
    return False


def main() -> int:
    base_ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    changed_files = get_changed_files(base_ref)

    selected = []
    for rule in MODULE_RULES:
        if any(matches_rule(changed, rule["paths"]) for changed in changed_files):
            selected.append(rule["name"])

    result = {
        "base_ref": base_ref,
        "changed_files": changed_files,
        "selected_modules": selected,
    }

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())