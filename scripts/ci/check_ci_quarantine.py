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
        # `uses:` locale (`./.github/workflows/x`) o forma pienamente qualificata
        # same-repo (`owner/repo/.github/workflows/x@ref`), accettata da Actions.
        re.compile(
            rf"\buses:\s*(?:[\w.-]+/[\w.-]+/)?\.?/?\.github/workflows/{stem}{ext}(?:@|\s|$)"
        ),
    )


def _scan_active_workflows_for_dispatch(active_dir: Path) -> list[str]:
    """Segnala i dispatch verso workflow quarantinati nei workflow attivi."""
    errors: list[str] = []
    active_workflows = sorted((*active_dir.glob("*.yml"), *active_dir.glob("*.yaml")))
    for active_path in active_workflows:
        # I workflow attivi non devono essere symlink: un symlink in una PR ostile
        # potrebbe puntare fuori dalla root ispezionata (albero `_pr`) e far leggere
        # path arbitrari; lo trattiamo come violazione fail-closed (Fable).
        if active_path.is_symlink():
            errors.append(f"workflow attivo è un symlink (non consentito): {active_path}")
            continue
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


def check_repository(repository_root: Path) -> list[str]:
    """Restituisce tutte le violazioni della quarantena rilevate nel repository."""
    # Fail-closed su root inesistente (es. checkout dell'albero PR mancante): il
    # guard non deve "passare" per assenza di file (Fable).
    if not repository_root.is_dir():
        return [f"root da ispezionare inesistente: {repository_root}"]

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

    return errors + _scan_active_workflows_for_dispatch(active_dir)


def main(argv: list[str] | None = None) -> int:
    # La root da ispezionare è parametrizzabile (primo argomento posizionale):
    # così il guard CI può eseguire QUESTO checker FIDATO (dal branch base) contro
    # l'albero-file di una PR non fidata, senza mai eseguire codice della PR
    # (fail-closed). Senza argomento ricade sulla root del proprio repository
    # (uso locale / self-check / push su main).
    args = sys.argv[1:] if argv is None else list(argv)
    if args:
        repository_root = Path(args[0]).resolve()
    else:
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
