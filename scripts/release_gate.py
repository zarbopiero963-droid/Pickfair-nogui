"""Gate di rilascio PF-RELEASE-GATE (#625; decisioni owner D02/D04 e addendum RELEASE GATE INTEGRITY, #626).

Separa due condizioni, entrambe obbligatorie per un esito verde:

1. **Integrita' dei controlli**: ogni test di sicurezza obbligatorio (``REQUIRED_TESTS``) e' stato raccolto, eseguito
   e superato. Un test mancante, rinominato, deselezionato, saltato (skip), marcato xfail o in errore => gate ROSSO.
   Un esito verde non puo' quindi nascere dal fatto che un test non gira piu'.
2. **Sicurezza effettiva**: i test obbligatori verificano il comportamento reale dei launcher con la scatola nera NON
   disponibile (nessun motore capace di trading costruito, uscita con errore). Finche' la GUI non installa BB-1 (BB-1G)
   i suoi test falliscono => gate rosso e nessun eseguibile si costruisce. Dopo BB-1G diventano verdi solo se il
   rifiuto sicuro e' vero: nessun marker da togliere, nessun criterio da cambiare.

Uso nei workflow di build: ``python scripts/release_gate.py`` (exit 0 = verde, 1 = rosso). Fail-closed: qualsiasi
errore di esecuzione o di lettura del rapporto => rosso.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
GATE_MODULE = "tests/packaging/release_gate_launchers.py"
REQUIRED_TESTS = frozenset({
    "test_headless_entrypoint_never_builds_the_runtime_without_the_blackbox",
    "test_gui_entrypoint_never_builds_the_runtime_without_the_blackbox",
    "test_default_launcher_main_py_never_builds_the_runtime_without_the_blackbox",
})
_CLASSNAME = "tests.packaging.release_gate_launchers"
_NOT_PASSED = ("failure", "error", "skipped")


def evaluate(junit_xml: str, required: Iterable[str] = REQUIRED_TESTS) -> tuple[bool, list[str]]:
    """Verdetto dal rapporto JUnit di pytest. Verde solo se OGNI test richiesto compare una volta, nel modulo del gate,
    senza failure/error/skipped (xfail e skip compaiono come ``skipped``)."""
    required = set(required)
    if not required:
        return False, ["nessun test obbligatorio dichiarato"]
    try:
        root = ET.fromstring(junit_xml)
    except ET.ParseError as exc:
        return False, [f"rapporto JUnit illeggibile: {exc}"]
    seen: dict[str, list[str]] = {}
    for case in root.iter("testcase"):
        name = str(case.get("name", "")).split("[", 1)[0]
        if case.get("classname") != _CLASSNAME or name not in required:
            continue
        outcomes = [child.tag for child in case if child.tag in _NOT_PASSED]
        seen.setdefault(name, []).append(outcomes[0] if outcomes else "passed")
    reasons = []
    for name in sorted(required):
        results = seen.get(name)
        if not results:
            reasons.append(f"{name}: non raccolto o non eseguito")
        elif any(r != "passed" for r in results):
            reasons.append(f"{name}: {', '.join(results)}")
    return not reasons, reasons


def ref_allowed(env: dict) -> tuple[bool, str]:
    """In GitHub Actions il gate e' verde solo su `refs/heads/main` o su un tag `v*` (audit Claude D, L1): un
    `workflow_dispatch` su un altro branch userebbe workflow e test di quel branch. Fuori da Actions (esecuzione locale)
    il ref non si applica. Limite dichiarato: un branch puo' modificare questo stesso controllo; la protezione completa
    resta nelle impostazioni GitHub (ruleset sui tag, ambienti protetti)."""
    if not env.get("GITHUB_ACTIONS"):
        return True, ""
    ref = str(env.get("GITHUB_REF", ""))
    if ref == "refs/heads/main" or ref.startswith("refs/tags/v"):
        return True, ""
    return False, f"build da ref non autorizzato: {ref!r} (ammessi refs/heads/main e refs/tags/v*)"


def run() -> tuple[bool, list[str]]:
    # Nessun filtro o plugin iniettato dall'ambiente: un plugin (hook makereport) potrebbe riscrivere gli esiti.
    env = {k: v for k, v in os.environ.items() if k not in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS")}
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    with tempfile.TemporaryDirectory() as tmp:
        report = Path(tmp) / "release_gate.xml"
        cmd = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", "-o", "addopts=", "--strict-markers",
               f"--junitxml={report}", GATE_MODULE]
        proc = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)
        if not report.is_file():
            return False, [f"rapporto JUnit assente (pytest rc={proc.returncode})"]
        ok, reasons = evaluate(report.read_text(encoding="utf-8"))
    if proc.returncode != 0:
        ok = False
        reasons = reasons or [f"pytest rc={proc.returncode}"]
    return ok, reasons


def main() -> int:
    allowed, why = ref_allowed(dict(os.environ))
    if allowed and not os.environ.get("GITHUB_ACTIONS"):
        print("RELEASE_GATE: controllo dei ref NON applicato (esecuzione fuori da GitHub Actions)")
    if not allowed:
        print("RELEASE_GATE: FAIL")
        print(f"  - {why}")
        return 1
    try:
        ok, reasons = run()
    except Exception as exc:  # noqa: BLE001 - fail-closed
        ok, reasons = False, [f"errore del gate: {type(exc).__name__}: {exc}"]
    if ok:
        print("RELEASE_GATE: PASS — tutti i launcher rifiutano l'avvio senza scatola nera")
        return 0
    print("RELEASE_GATE: FAIL")
    for reason in reasons:
        print(f"  - {reason}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
