from __future__ import annotations

from pathlib import Path
import re
import shutil
import sys

WORKFLOWS_DIR = Path(".github/workflows")

KEEP_AUTOMATIC = {
    "ci-dynamic-intelligent.yml",
    "noise-free-pr-gate.yml",
    "repo-guardrails-extended.yml",
    "merge-simulation-hard.yml",
    "_module-ultra-check.yml",
}

REUSABLE_AND_MANUAL = {
    "trading-engine.yml",
    "order-manager.yml",
    "execution-guard.yml",
    "risk-middleware.yml",
    "runtime-controller.yml",
    "money-management.yml",
    "telegram-listener.yml",
    "copy-engine.yml",
    "simulation-broker.yml",
    "session-manager.yml",
    "rate-limiter.yml",
    "live-gate.yml",
    "chaos-critical.yml",
    "mutation-guardrails.yml",
}

MANUAL_ONLY = {
    "unit.yml",
    "failure.yml",
    "net.yml",
    "e2e.yml",
    "integration.yml",
    "smoke.yml",
    "smoke-core-fast.yml",
    "core-guardrails.yml",
    "core-chaos.yml",
    "core-invariants.yml",
    "recovery-guardrails.yml",
    "repo-guardrails.yml",
    "chaos-runtime.yml",
    "chaos-stateful.yml",
    "observability-tests.yml",
    "observability-runtime.yml",
    "trading-engine-hard-tests.yml",
    "reconciliation-engine.yml",
    "dutching.yml",
    "pnl-engine.yml",
    "merge-simulation.yml",
    "live-sim-parity.yml",
    "stateful-integrity.yml",
    "pr-guard.yml",
    "pr-overlap-guard.yml",
    "codex-bug-gate.yml",
    "ci-master-gate.yml",
}

ON_BLOCK_RE = re.compile(
    r"(?ms)^on:\n(?:^[ \t].*\n|^\n)*"
)


def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def replacement_for(filename: str) -> str | None:
    if filename in KEEP_AUTOMATIC:
        return None
    if filename in REUSABLE_AND_MANUAL:
        return "on:\n  workflow_call:\n  workflow_dispatch:\n"
    if filename in MANUAL_ONLY:
        return "on:\n  workflow_dispatch:\n"
    return None


def normalize_file(path: Path) -> tuple[bool, str]:
    filename = path.name
    replacement = replacement_for(filename)

    if replacement is None:
        return False, "kept as-is"

    text = path.read_text(encoding="utf-8")

    if not ON_BLOCK_RE.search(text):
        return False, "missing on: block"

    backup_path = path.with_suffix(path.suffix + ".bak")
    shutil.copy2(path, backup_path)

    new_text = ON_BLOCK_RE.sub(replacement, text, count=1)

    if new_text == text:
        return False, "no change"

    path.write_text(new_text, encoding="utf-8")
    return True, f"{replacement.strip().replace(chr(10), ' | ')} (backup: {backup_path.name})"


def main() -> int:
    if not WORKFLOWS_DIR.exists():
        fail(f"workflow directory not found: {WORKFLOWS_DIR}")

    changed = []
    skipped = []

    for path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        updated, info = normalize_file(path)
        if updated:
            changed.append((path.name, info))
        else:
            skipped.append((path.name, info))

    print("=" * 80)
    print("CI TRIGGER NORMALIZATION REPORT")
    print("=" * 80)

    if changed:
        print("\nUPDATED FILES")
        for name, info in changed:
            print(f" - {name}: {info}")
    else:
        print("\nNo workflow files updated.")

    if skipped:
        print("\nSKIPPED FILES")
        for name, info in skipped:
            print(f" - {name}: {info}")

    print("\nSummary:")
    print(f" - Updated: {len(changed)}")
    print(f" - Skipped: {len(skipped)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())