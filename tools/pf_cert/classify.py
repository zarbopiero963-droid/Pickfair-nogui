"""Selezione automatica degli scenari di certificazione Betfair in base ai file cambiati da una PR.

Gira nel workflow trusted ``pf-cert-request.yml`` dal branch BASE (mai il codice della PR) e, in modo identico, sulla
VPS, che ricalcola gli scenari con questo stesso file preso da ``main`` e non si fida della lista nella richiesta.

Fail-closed:
- un file di prodotto senza regola esplicita => intero set obbligatorio (mai «nessuno scenario» per ignoranza);
- un percorso illeggibile (non stringa, assoluto, con ``..``) => intero set obbligatorio;
- ``live-real-order`` e' informativo: sempre NOT_RUN finche' l'owner non autorizza una campagna LIVE, quindi non e'
  mai obbligatorio (un check verde non puo' dipendere da un ordine reale, e non puo' nemmeno restare rosso per sempre).
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import sys
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Tuple

CATALOG_VERSION = "1.0.0"

#: scenario -> obbligatorio? (catalogo v1 dell'orchestratore, c.6575246644 su #586)
CATALOG: Dict[str, bool] = {
    "sim-replay-microstake": True,
    "pytest-microstake-recovery": True,
    "pytest-reconciliation": True,
    "live-real-order": False,
}
FULL_MANDATORY: Tuple[str, ...] = tuple(sorted(s for s, required in CATALOG.items() if required))

#: file che non toccano il comportamento: nessuno scenario
_NEUTRAL = (
    "docs/*", "*.md", "tests/*", "tools/*", ".guardrails/*", "guardrails/*", "CHANGELOG.md", "LICENSE*",
    ".github/*", "scripts/*", "results/*",
)

#: regole esplicite (pattern fnmatch su percorso POSIX relativo) -> scenari
_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("simulation_broker.py", ("sim-replay-microstake", "pytest-microstake-recovery")),
    ("order_manager*.py", ("sim-replay-microstake", "pytest-microstake-recovery")),
    ("core/order_router.py", ("sim-replay-microstake", "pytest-microstake-recovery")),
    ("core/order_ref.py", ("sim-replay-microstake", "pytest-reconciliation")),
    ("core/trading_engine.py", ("sim-replay-microstake", "pytest-microstake-recovery")),
    ("core/microstake*.py", ("sim-replay-microstake", "pytest-microstake-recovery")),
    ("core/pnl_engine.py", ("sim-replay-microstake",)),
    ("pnl_engine.py", ("sim-replay-microstake",)),
    ("core/stake_precision.py", ("sim-replay-microstake",)),
    ("money_management*.py", ("sim-replay-microstake",)),
    ("dutching*.py", ("sim-replay-microstake",)),
    ("cashout*.py", ("sim-replay-microstake",)),
    ("core/risk_*.py", ("sim-replay-microstake",)),
    ("core/reconciliation*.py", ("pytest-reconciliation",)),
    ("core/state_recovery.py", ("pytest-reconciliation", "pytest-microstake-recovery")),
    ("core/runtime_controller.py", ("pytest-reconciliation", "pytest-microstake-recovery")),
    ("database*.py", ("pytest-reconciliation", "pytest-microstake-recovery")),
    ("betfair_client.py", ("sim-replay-microstake", "live-real-order")),
    ("services/betfair_service.py", ("sim-replay-microstake", "live-real-order")),
    ("core/microstake_live_port.py", ("sim-replay-microstake", "pytest-microstake-recovery", "live-real-order")),
    # configurazione: tocca stake, limiti e modalita' => intero set obbligatorio
    ("config*.json", FULL_MANDATORY),
    ("*trading_config*", FULL_MANDATORY),
    ("core/config_registry*.py", FULL_MANDATORY),
)


def _safe_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw or raw.startswith("/") or "\x00" in raw or "\\" in raw:
        return ""
    parts = PurePosixPath(raw).parts
    if any(p in ("..", "") for p in parts):
        return ""
    return raw


def classify(changed_paths: Iterable[object]) -> Dict[str, object]:
    """``{catalog_version, scenarios, mandatory, informational, reasons}`` per i file cambiati."""
    selected: Dict[str, List[str]] = {}
    reasons: List[str] = []
    paths = list(changed_paths) if changed_paths is not None else [None]
    for raw in paths:
        path = _safe_path(raw)
        if not path:
            reasons.append("percorso_illeggibile")
            for s in FULL_MANDATORY:
                selected.setdefault(s, []).append("percorso_illeggibile")
            continue
        hits = [scen for pattern, scen in _RULES if fnmatch.fnmatchcase(path, pattern)]
        if hits:
            for scen in hits:
                for s in scen:
                    selected.setdefault(s, []).append(path)
            continue
        if any(fnmatch.fnmatchcase(path, p) for p in _NEUTRAL):
            continue
        if path.endswith(".py") or "." not in PurePosixPath(path).name:
            reasons.append(f"senza_regola:{path}")
            for s in FULL_MANDATORY:
                selected.setdefault(s, []).append(f"senza_regola:{path}")
        # altri file (config d'esempio, asset): nessuno scenario
    scenarios = sorted(selected)
    return {
        "catalog_version": CATALOG_VERSION,
        "scenarios": scenarios,
        "mandatory": [s for s in scenarios if CATALOG.get(s)],
        "informational": [s for s in scenarios if not CATALOG.get(s)],
        "reasons": sorted(set(reasons)),
    }


def classifier_digest() -> str:
    """sha256 di questo file: la VPS confronta la versione del classificatore usata dal workflow con quella di main."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def summary_markdown(result: Dict[str, object]) -> str:
    """Tabella per il job summary del workflow (solo metadati: nessun dato Betfair)."""
    rows = ["## pf-cert: scenari", "", "| scenario | obbligatorio |", "|---|---|"]
    mandatory = set(result.get("mandatory") or [])
    rows += [f"| {s} | {'si' if s in mandatory else 'no (informativo)'} |" for s in result.get("scenarios") or []]
    if not result.get("scenarios"):
        rows.append("| (nessuno) | - |")
    return "\n".join(rows) + "\n"


def main(argv: List[str]) -> int:
    """``classify.py <files.json> [<summary.md>]``: stampa il risultato JSON; opzionale il summary markdown."""
    if len(argv) not in (2, 3):
        print("uso: classify.py <files.json> [<summary.md>]", file=sys.stderr)
        return 2
    try:
        files = json.loads(Path(argv[1]).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        files = None
    result = classify(files if isinstance(files, list) else [None])
    result["classifier_sha256"] = classifier_digest()
    print(json.dumps(result, sort_keys=True))
    if len(argv) == 3:
        with open(argv[2], "a", encoding="utf-8") as fh:
            fh.write(summary_markdown(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
