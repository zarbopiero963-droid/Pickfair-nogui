#!/usr/bin/env python3
"""Check `full-suite` (PFC-002): l'intera suite, una volta per push, con prova.

Prima della PFC-002 la suite completa girava 17 volte per push, sempre come
`pytest -q` nudo: verde se l'exit code era 0. Un exit 0 non distingue pero' una
suite completa da una raccolta parziale, da test deselezionati o da test
raccolti e mai eseguiti. Questo gate giudica il REPORT del plugin
`full_suite_plugin.py`, non l'exit code:

- l'exit code di pytest deve essere 0 e nessun modulo deve fallire la raccolta;
- nessun test deselezionato, nessun argomento che restringe la selezione;
- almeno `min_collected` test raccolti (contratto in `full_suite_contract.json`);
- ogni test raccolto ha un esito, e nessuno e' `failed`/`error`/`xpassed`;
- skipped e xfailed entro il contratto: un test che smette di girare si vede.

Se il report manca o non si legge, l'esito e' FAIL. Produce l'evidenza
`full-suite-evidence.json` con lo SHA su cui ha girato.

    full_suite_gate.py run       # esegue pytest con il plugin e giudica
    full_suite_gate.py evaluate --report r.json   # giudica un report esistente
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CONTRACT = HERE / "full_suite_contract.json"
BAD = {"failed", "error", "xpassed"}


def load_contract(path: Path = CONTRACT) -> dict[str, Any]:
    dati = json.loads(path.read_text(encoding="utf-8"))
    for chiave in ("min_collected", "max_skipped", "max_xfailed"):
        if not isinstance(dati.get(chiave), int) or dati[chiave] < 0:
            raise ValueError(f"contratto full-suite: {chiave} assente o non valido")
    if dati["min_collected"] <= 0:
        raise ValueError("contratto full-suite: min_collected deve essere positivo")
    return dati


def evaluate(report: Any, contract: dict[str, Any], pytest_args: list[str] | None = None) -> dict[str, Any]:
    """Verdetto sul report del plugin. `ok` e' True solo se ogni condizione regge."""
    motivi: list[str] = []
    args = list(pytest_args or [])
    vietati = set(contract.get("forbidden_pytest_args") or [])
    usati = sorted({a.split("=", 1)[0] for a in args if a.split("=", 1)[0] in vietati})
    if usati:
        motivi.append("argomenti pytest che restringono la selezione: " + ", ".join(usati))
    if not isinstance(report, dict):
        return {"ok": False, "reasons": motivi + ["report del plugin assente o illeggibile"], "counts": {}}

    raccolti = list(report.get("collected") or [])
    esiti = dict(report.get("outcomes") or {})
    conteggi: dict[str, int] = {}
    for esito in esiti.values():
        conteggi[esito] = conteggi.get(esito, 0) + 1

    if report.get("exitstatus") != 0:
        motivi.append(f"pytest e' uscito con {report.get('exitstatus')!r}, atteso 0")
    if report.get("collect_errors"):
        motivi.append(f"{len(report['collect_errors'])} errori di raccolta: "
                      + ", ".join(report["collect_errors"][:10]))
    if report.get("deselected"):
        motivi.append(f"{len(report['deselected'])} test deselezionati")
    if len(raccolti) != len(set(raccolti)):
        motivi.append("nodeid raccolti duplicati: report incoerente")
    if len(raccolti) < contract["min_collected"]:
        motivi.append(f"raccolti {len(raccolti)} test, il contratto ne richiede almeno {contract['min_collected']}")
    senza_esito = [n for n in raccolti if n not in esiti]
    if senza_esito:
        motivi.append(f"{len(senza_esito)} test raccolti senza esito (mai eseguiti): " + ", ".join(senza_esito[:10]))
    estranei = [n for n in esiti if n not in set(raccolti)]
    if estranei:
        motivi.append(f"{len(estranei)} esiti di test non raccolti: report incoerente")
    cattivi = sorted(n for n, e in esiti.items() if e in BAD)
    if cattivi:
        motivi.append(f"{len(cattivi)} test falliti, in errore o xpassed: " + ", ".join(cattivi[:10]))
    if conteggi.get("skipped", 0) > contract["max_skipped"]:
        motivi.append(f"{conteggi['skipped']} test skipped, il contratto ne ammette {contract['max_skipped']}")
    if conteggi.get("xfailed", 0) > contract["max_xfailed"]:
        motivi.append(f"{conteggi['xfailed']} test xfailed, il contratto ne ammette {contract['max_xfailed']}")
    if conteggi.get("passed", 0) <= 0:
        motivi.append("nessun test passato")
    return {
        "ok": not motivi,
        "reasons": motivi,
        "counts": {"collected": len(raccolti), **dict(sorted(conteggi.items()))},
    }


def _evidence(verdetto: dict[str, Any], report: Any) -> dict[str, Any]:
    return {
        "check": "full-suite",
        "ok": verdetto["ok"],
        "reasons": verdetto["reasons"],
        "counts": verdetto["counts"],
        "duration_s": (report or {}).get("duration_s") if isinstance(report, dict) else None,
        "github_sha": os.environ.get("GITHUB_SHA", ""),
        "pr_head_sha": os.environ.get("PR_HEAD_SHA", ""),
        "event": os.environ.get("GITHUB_EVENT_NAME", ""),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
    }


def _read_report(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def cmd_run(args: argparse.Namespace) -> int:
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    report_path = out / "full-suite-report.json"
    if report_path.exists():
        report_path.unlink()  # un report vecchio non deve valere per questo run
    contract = load_contract(Path(args.contract))
    pytest_args = ["-q", "-p", "no:cacheprovider", "-p", "full_suite_plugin"] + list(args.pytest_args or [])
    env = dict(os.environ)
    env["FULL_SUITE_REPORT"] = str(report_path)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(HERE), env.get("PYTHONPATH", "")) if p)
    rc = subprocess.call([sys.executable, "-m", "pytest", *pytest_args], cwd=str(ROOT), env=env)
    report = _read_report(report_path)
    if isinstance(report, dict) and rc != 0:
        report["exitstatus"] = rc  # vince il codice reale del processo, se non e' 0
    verdetto = evaluate(report, contract, list(args.pytest_args or []))
    evidenza = _evidence(verdetto, report)
    _write(out / "full-suite-evidence.json", evidenza)
    print(json.dumps(evidenza, indent=2, ensure_ascii=False))
    return 0 if verdetto["ok"] else 1


def cmd_evaluate(args: argparse.Namespace) -> int:
    report = _read_report(Path(args.report))
    verdetto = evaluate(report, load_contract(Path(args.contract)), list(args.pytest_args or []))
    print(json.dumps(verdetto, indent=2, ensure_ascii=False))
    return 0 if verdetto["ok"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="check full-suite")
    sub = parser.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--out-dir", default="full-suite")
    r.add_argument("--contract", default=str(CONTRACT))
    r.add_argument("pytest_args", nargs="*")
    e = sub.add_parser("evaluate")
    e.add_argument("--report", required=True)
    e.add_argument("--contract", default=str(CONTRACT))
    e.add_argument("pytest_args", nargs="*")
    args = parser.parse_args(argv)
    return cmd_run(args) if args.cmd == "run" else cmd_evaluate(args)


if __name__ == "__main__":
    sys.exit(main())
