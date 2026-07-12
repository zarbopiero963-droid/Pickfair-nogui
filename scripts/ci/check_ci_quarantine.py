#!/usr/bin/env python3
"""Verifica che i workflow privilegiati restino fuori dalla directory attiva."""

from __future__ import annotations

import re
import sys
from pathlib import Path

QUARANTINED_WORKFLOWS: tuple[str, ...] = (
    "auto-pr-codex-fix.yml",
    "auto-pr-codex-supervisor.yml",
    "auto-pr-e2e-autofix.yml",
    "new-task-codex-pr.yml",
    "pr-autofix-safe-supervisor.yml",
    "pr-autofix-selfhosted.yml",
    "pr-automation-controller.yml",
    "pr-automation-controller-v2.yml",
)


def _dispatch_patterns(workflow_name: str) -> tuple[re.Pattern[str], ...]:
    # Robustezza del guard di sicurezza (fail-closed):
    # - GitHub Actions esegue sia `.yml` sia `.yaml`, e `gh workflow run` accetta
    #   anche il nome SENZA estensione: l'estensione va resa opzionale, altrimenti
    #   la variante `.yaml`/senza-estensione sfuggirebbe (Greptile P1, Codacy).
    # - `gh workflow run` può avere FLAG prima del nome (es. `--ref main`): non
    #   devono far evadere il controllo (Qodo). Si consente qualsiasi testo sulla
    #   stessa riga tra `run` e il nome del workflow.
    stem = re.escape(Path(workflow_name).stem)
    ext = r"(?:\.ya?ml)?"
    return (
        re.compile(rf"\bgh\s+workflow\s+run\b[^\n]*?['\"]?{stem}{ext}(?:['\"\s\\]|$)"),
        re.compile(rf"/actions/workflows/{stem}{ext}/dispatches\b"),
        re.compile(rf"\buses:\s*\.?/?\.github/workflows/{stem}{ext}(?:@|\s|$)"),
    )


def check_repository(repository_root: Path) -> list[str]:
    """Restituisce tutte le violazioni della quarantena rilevate nel repository."""

    active_dir = repository_root / ".github" / "workflows"
    quarantine_dir = repository_root / ".github" / "quarantined-workflows"
    errors: list[str] = []

    for workflow_name in QUARANTINED_WORKFLOWS:
        stem = Path(workflow_name).stem
        archived_path = quarantine_dir / f"{workflow_name}.disabled"
        # GitHub Actions esegue sia `.yml` sia `.yaml`: un workflow quarantinato
        # riattivato con l'estensione alternativa sarebbe eseguibile ma sfuggirebbe
        # al controllo di sola esistenza `.yml` (Greptile P1). Controlla entrambe.
        for extension in (".yml", ".yaml"):
            active_path = active_dir / f"{stem}{extension}"
            if active_path.exists():
                errors.append(f"workflow privilegiato nuovamente attivo: {active_path}")
        if not archived_path.is_file():
            errors.append(f"copia di quarantena mancante: {archived_path}")

    if not active_dir.is_dir():
        errors.append(f"directory workflow attivi mancante: {active_dir}")
        return errors

    active_workflows = sorted((*active_dir.glob("*.yml"), *active_dir.glob("*.yaml")))
    for active_path in active_workflows:
        try:
            content = active_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            errors.append(f"workflow attivo non leggibile come UTF-8: {active_path}")
            continue

        for workflow_name in QUARANTINED_WORKFLOWS:
            if any(pattern.search(content) for pattern in _dispatch_patterns(workflow_name)):
                errors.append(
                    f"dispatch verso workflow quarantinato {workflow_name}: {active_path}"
                )

    return errors


def main() -> int:
    repository_root = Path(__file__).resolve().parents[2]
    errors = check_repository(repository_root)
    if errors:
        print("CI quarantine guard: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(
        "CI quarantine guard: PASS "
        f"({len(QUARANTINED_WORKFLOWS)} workflow privilegiati non eseguibili)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
