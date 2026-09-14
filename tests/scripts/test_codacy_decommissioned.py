"""Codacy è DISMESSO: nessuna API, nessun gate, nessun blocco.

Questi test blindano la rimozione (PR "rimozione Codacy"):
- PASS: la readiness continua a funzionare senza alcuna evidenza Codacy;
- BLOCK: se qualcuno reintroduce l'API Codacy, o fa tornare un check Codacy
  tra i blockers, o indebolisce il fail-closed su DeepSource required, il test
  FALLISCE. Non sono asserzioni cosmetiche: chiamano il codice reale.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from scripts import pr_automation_controller as controller
from scripts import pr_flow_automation as flow

REPO_ROOT = Path(__file__).resolve().parents[2]


#: Questo file CONTIENE di proposito le stringhe vietate (sono le asserzioni),
#: quindi va escluso dalla scansione: altrimenti il test troverebbe se stesso.
_SELF = Path(__file__).resolve()


def _tracked_sources() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "*.py", "*.yml", "*.yaml"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [p for f in out if (p := (REPO_ROOT / f).resolve()) != _SELF]


# ---------------------------------------------------------------------------
# BLOCK — nessuna chiamata di rete a Codacy deve tornare nel repository
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_no_codacy_api_endpoint_anywhere():
    """Nessun sorgente tracciato deve contenere l'endpoint API di Codacy."""
    offenders = [
        str(p.relative_to(REPO_ROOT))
        for p in _tracked_sources()
        if p.is_file() and "api.codacy.com" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert offenders == [], f"API Codacy reintrodotta in: {offenders}"


@pytest.mark.unit
def test_no_codacy_api_token_anywhere():
    """Il segreto CODACY_API_TOKEN non deve essere più letto da nessuna parte."""
    offenders = [
        str(p.relative_to(REPO_ROOT))
        for p in _tracked_sources()
        if p.is_file() and "CODACY_API_TOKEN" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert offenders == [], f"CODACY_API_TOKEN reintrodotto in: {offenders}"


@pytest.mark.unit
def test_codacy_api_helpers_are_gone():
    """Le funzioni che parlavano con l'API Codacy non devono esistere più."""
    for name in (
        "codacy_api_token", "codacy_url", "codacy_https_json", "fetch_codacy_pr_issues",
        "cmd_codacy_task", "controller_codacy_blocking_evidence", "codacy_evidence_from_api",
    ):
        assert not hasattr(controller, name), f"{name} è tornata nel controller"
    for name in ("codacy_api_token", "fetch_codacy_pr_issues", "cmd_codacy_task", "fetch_codacy_json"):
        assert not hasattr(flow, name), f"{name} è tornata in pr_flow_automation"


# ---------------------------------------------------------------------------
# BLOCK — un check Codacy residuo non deve MAI bloccare
# ---------------------------------------------------------------------------
def _codacy_check() -> dict[str, str]:
    return {"name": "Codacy Static Code Analysis", "conclusion": "ACTION_REQUIRED", "detailsUrl": ""}


def _real_check() -> dict[str, str]:
    return {"name": "Unit tests", "conclusion": "FAILURE", "detailsUrl": ""}


@pytest.mark.unit
def test_codacy_check_is_always_filtered_out_of_blockers():
    """L'esclusione è INCONDIZIONATA: vale anche con evidenza ignored=False."""
    checks = [_codacy_check(), _real_check()]
    for evidence in ({}, {"ignored": False}, {"ignored": True}):
        kept = controller.filter_ignored_codacy_checks(checks, evidence)
        names = [c["name"] for c in kept]
        assert "Codacy Static Code Analysis" not in names, f"Codacy blocca ancora con {evidence}"
        # BLOCK: il check reale NON deve sparire — si toglie solo Codacy.
        assert "Unit tests" in names, "un check reale è stato filtrato per sbaglio"


@pytest.mark.unit
def test_codacy_evidence_is_decommissioned_and_makes_no_network_call(monkeypatch):
    """codacy_evidence_for_checks non fa rete e dichiara il servizio dismesso."""
    def _boom(*_a, **_k):  # pragma: no cover
        raise AssertionError("nessuna chiamata di rete deve partire")
    monkeypatch.setattr("urllib.request.urlopen", _boom)

    evidence = controller.codacy_evidence_for_checks("owner/repo", "1", [_codacy_check()])
    assert evidence["ignored"] is True
    assert evidence["classification"] == "decommissioned"
    assert evidence["issues_returned"] == 0


# ---------------------------------------------------------------------------
# DeepSource advisory: PASS senza Codacy, BLOCK se required+failing
# ---------------------------------------------------------------------------
def _advisory_context(**over) -> dict:
    ctx = {
        "current_head_sha": "abc123",
        "evidence_head_sha": "abc123",
        "deepsource_advisory_evidence": [{"name": "DeepSource Python"}],
        "deepsource_required_current_head_check_failing": False,
    }
    ctx.update(over)
    return ctx


@pytest.mark.unit
def test_deepsource_advisory_does_not_require_codacy_evidence():
    """PASS: senza NESSUNA chiave Codacy il contesto advisory resta valido.

    Prima della dismissione la catena pretendeva codacy_state == "SUCCESS":
    con Codacy disinstallato quella condizione non sarebbe mai stata vera e
    DeepSource sarebbe tornato BLOCCANTE (CI più rossa, non più verde).
    """
    ctx = _advisory_context()
    assert "codacy_state" not in ctx
    assert flow._deepsource_autoderived_context_missing(ctx) is False


@pytest.mark.unit
def test_deepsource_required_failing_still_blocks():
    """BLOCK: il fail-closed su DeepSource required non è stato indebolito."""
    ctx = _advisory_context(deepsource_required_current_head_check_failing=True)
    assert flow._deepsource_autoderived_context_missing(ctx) is True


@pytest.mark.unit
@pytest.mark.parametrize("missing_key", ["current_head_sha", "evidence_head_sha", "deepsource_advisory_evidence"])
def test_deepsource_advisory_still_fail_closed_on_missing_evidence(missing_key):
    """BLOCK: evidenza mancante continua a bloccare (nessun fail-open)."""
    ctx = _advisory_context()
    ctx[missing_key] = "" if isinstance(ctx[missing_key], str) else []
    assert flow._deepsource_autoderived_context_missing(ctx) is True


# ---------------------------------------------------------------------------
# BLOCK — il workflow di readiness resta fail-closed sui blocker reali
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_merge_readiness_workflow_has_no_codacy_and_stays_fail_closed():
    """Niente Codacy nel gate, ma una readiness negativa DEVE ancora fallire."""
    wf = (REPO_ROOT / ".github/workflows/pr-merge-readiness.yml").read_text(encoding="utf-8")
    assert "codacy-task" not in wf
    assert "CODACY_API_TOKEN" not in wf
    assert "codacy-context" not in wf
    # fail-closed preservato: il ramo "non mergiabile" esce comunque con errore
    assert re.search(r'if \[ "\$CAN_MERGE" != "true" \];.*?exit 1', wf, re.S), \
        "il gate non fallisce più sui blocker reali (fail-open!)"
