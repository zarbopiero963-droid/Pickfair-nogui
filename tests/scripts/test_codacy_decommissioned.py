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


# ---------------------------------------------------------------------------
# Rilievo Codex P1 (#462): il gate CI gira `pr_merge_readiness.py`, che delega a
# `pr_flow_automation`. Il filtro del controller NON e' su quel percorso: finche'
# la GitHub App dismessa pubblica `Codacy Static Code Analysis =
# ACTION_REQUIRED`, `split_checks` lo mette nei blockers e il gate esce 1.
# E' ESATTAMENTE il falso rosso che questa PR deve togliere.
# ---------------------------------------------------------------------------
def _codacy_rollup_check(state: str = "ACTION_REQUIRED") -> dict:
    return {
        "__typename": "CheckRun",
        "name": "Codacy Static Code Analysis",
        "status": "COMPLETED",
        "conclusion": state,
    }


@pytest.mark.unit
def test_codacy_check_is_never_a_blocker_in_the_workflow_path():
    """Il percorso REALE del workflow non deve mai bloccare per Codacy."""
    pr = {
        "statusCheckRollup": [
            _codacy_rollup_check(),
            {"__typename": "CheckRun", "name": "unit",
             "status": "COMPLETED", "conclusion": "SUCCESS"},
        ]
    }
    out = flow.split_checks(pr)
    names = [c["name"] for c in out["blockers"]]
    assert "Codacy Static Code Analysis" not in names, (
        "il check della GitHub App dismessa blocca ancora la readiness"
    )
    # BLOCK: un check REALE rosso deve continuare a bloccare.
    pr_real = {
        "statusCheckRollup": [
            {"__typename": "CheckRun", "name": "unit",
             "status": "COMPLETED", "conclusion": "FAILURE"},
        ]
    }
    assert [c["name"] for c in flow.split_checks(pr_real)["blockers"]] == ["unit"], (
        "un check reale fallito non blocca piu': esclusione troppo larga"
    )


@pytest.mark.unit
@pytest.mark.parametrize("state", ["ACTION_REQUIRED", "FAILURE", "PENDING", "QUEUED", ""])
def test_codacy_check_never_blocks_nor_hangs_readiness(state):
    """Ne' blocker ne' pending: un servizio dismesso non e' piu' un segnale."""
    out = flow.split_checks({"statusCheckRollup": [_codacy_rollup_check(state)]})
    assert out["blockers"] == [], f"Codacy blocca con conclusion={state!r}"
    assert out["pending"] == [], f"Codacy tiene la readiness in attesa con {state!r}"


# ---------------------------------------------------------------------------
# Rilievo Codex P2 (#462): con Codacy dismesso nessun contesto reale puo' avere
# un `codacy_conclusion == SUCCESS`, quindi i gate finali restano IRRAGGIUNGIBILI
# per sempre. Un gate superabile solo fabbricando evidenza di un servizio che
# non esiste piu' e' peggio che inutile: invita a inventarla (vietato da AGENTS).
# ---------------------------------------------------------------------------
def _clean_merge_context() -> dict:
    """Contesto REALISTICO post-dismissione: tutto verde, zero dati Codacy."""
    return {
        "automation_mode": "live",
        "automation_flags": {
            "SAFE_AUTOFIX_ENABLED": True,
            "AUTO_RESOLVE_ENABLED": True,
            "AUTO_RERUN_ENABLED": True,
            "AUTO_PUSH_ENABLED": True,
            "AUTO_MERGE_ENABLED": True,
            "GITHUB_MUTATION_ENABLED": True,
            "EXTERNAL_SIDE_EFFECT_ENABLED": True,
            "REPORTING_ENABLED": True,
        },
        "task_no_commit_push": False,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "bad": [],
        "blockers": [],
        "pending": [],
        "unresolved_active": 0,
        "current_head_matches": True,
        "explicit_merge_authorization": True,
    }


@pytest.mark.unit
def test_auto_merge_is_reachable_without_any_codacy_evidence():
    result = controller.can_auto_merge(_clean_merge_context())
    reason = str(result.get("reason") or "")
    # La proprieta' vera: senza UN SOLO dato Codacy il merge dev'essere
    # RAGGIUNGIBILE. Asserire solo "reason non contiene codacy" sarebbe debole:
    # un gate reintrodotto con un altro nome passerebbe inosservato.
    assert result["allowed"] is True, f"auto-merge irraggiungibile: {result}"
    assert "codacy" not in reason.lower(), f"gate Codacy ancora attivo: {reason}"
    assert "annotations" not in reason.lower(), f"gate annotazioni Codacy attivo: {reason}"


@pytest.mark.unit
def test_auto_merge_still_blocks_on_every_real_guard():
    """BLOCK: togliere i gate Codacy non deve aprire nessun'altra porta."""
    for key, value, expected in (
        ("mergeable", "CONFLICTING", "mergeable_not_mergeable"),
        ("mergeStateStatus", "BLOCKED", "merge_state_not_clean"),
        ("blockers", [{"name": "unit"}], "bad_checks_present"),
        ("pending", [{"name": "unit"}], "pending_checks_present"),
        ("unresolved_active", 2, "unresolved_reviews_present"),
        ("current_head_matches", False, "current_head_mismatch"),
        ("explicit_merge_authorization", False, "explicit_merge_authorization_required"),
    ):
        ctx = _clean_merge_context()
        ctx[key] = value
        result = controller.can_auto_merge(ctx)
        assert not result["allowed"], f"{key}={value!r} non blocca piu' il merge"
        assert expected in str(result.get("reason") or ""), (
            f"{key}={value!r}: motivo atteso {expected}, ottenuto {result.get('reason')!r}"
        )


@pytest.mark.unit
def test_ready_to_merge_is_reachable_without_any_codacy_evidence():
    ctx = {
        "next_action": "ready_to_merge",
        "bad": [], "pending": [], "blockers": [],
        "mergeStateStatus": "CLEAN", "unresolved_active": 0,
    }
    assert controller._report_ready_to_merge(ctx) is True, (
        "READY_TO_MERGE irraggiungibile senza evidenza di un servizio dismesso"
    )
    # BLOCK: i gate reali restano.
    for key, value in (("blockers", [{"name": "x"}]), ("pending", [{"name": "x"}]),
                       ("unresolved_active", 1), ("mergeStateStatus", "DIRTY")):
        broken = dict(ctx)
        broken[key] = value
        assert controller._report_ready_to_merge(broken) is False, (
            f"{key}={value!r} non impedisce piu' READY_TO_MERGE"
        )


# ---------------------------------------------------------------------------
# Giro 3 — rilievo convergente di Codex E Fable 5 (full-range) su #462:
# il matcher `"codacy" in nome` e' un vettore FAIL-OPEN sul gate di merge.
# Un check futuro con "codacy" nel nome (una guardia di dismissione, un
# workflow rinominato) sparirebbe dai blockers anche se FAILURE.
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize("name", [
    "codacy-decommission-guard",          # una guardia NOSTRA sulla dismissione
    "verify no codacy references",        # un check di repo-guardrail
    "Codacy Static Code Analysis (fork)", # un check omonimo ma non quello dell'App
    "my-codacy-migration-check",
])
def test_only_the_exact_retired_codacy_check_is_exempt(name):
    """Solo il check ESATTO della App dismessa e' esente: tutto il resto blocca."""
    pr = {"statusCheckRollup": [
        {"__typename": "CheckRun", "name": name,
         "status": "COMPLETED", "conclusion": "FAILURE"},
    ]}
    blockers = [c["name"] for c in flow.split_checks(pr)["blockers"]]
    assert blockers == [name], (
        f"il check {name!r} e' stato escluso dai blockers: esenzione troppo larga "
        "(fail-open sul gate di merge)"
    )


@pytest.mark.unit
def test_exact_retired_codacy_check_is_still_exempt():
    """PASS: il check reale della GitHub App dismessa resta esente."""
    for spelling in ("Codacy Static Code Analysis", "codacy static code analysis",
                     "  Codacy Static Code Analysis  "):
        pr = {"statusCheckRollup": [
            {"__typename": "CheckRun", "name": spelling,
             "status": "COMPLETED", "conclusion": "ACTION_REQUIRED"},
        ]}
        out = flow.split_checks(pr)
        assert out["blockers"] == [], f"{spelling!r} blocca ancora"
        assert out["pending"] == [], f"{spelling!r} tiene in attesa"


# ---------------------------------------------------------------------------
# Rilievo Codex (giro 3): `build_next_action_context` scrive SEMPRE
# `codacy.github_codacy_state`, quindi `_codacy_review_evidence_present` e'
# sempre vero e `_codacy_review_evidence_green` sempre falso => il planner di
# rerun passivo accodava `codacy_not_green` PER SEMPRE. Stesso deadlock
# fail-closed di can_auto_merge, su un altro gate.
# ---------------------------------------------------------------------------
def _decommissioned_rerun_context() -> dict:
    codacy = controller.codacy_evidence_for_checks("repo", 1, [])
    codacy["github_codacy_state"] = ""
    codacy.setdefault("github_annotations", 0)
    return {
        "codacy": codacy,
        "current_head_sha": "abc123",
        "evidence_head_sha": "abc123",
        "checks_green": True,
        "pending_checks": [],
        "failing_checks": [],
    }


@pytest.mark.unit
def test_passive_rerun_is_not_blocked_forever_by_a_dismissed_service():
    plan = controller.build_passive_rerun_readiness_plan(_decommissioned_rerun_context())
    blockers = [str(b) for b in (plan.get("blocked_reasons") or [])]
    assert "codacy_not_green" not in blockers, (
        f"il rerun passivo resta bloccato da un servizio dismesso: {blockers}"
    )


@pytest.mark.unit
def test_passive_rerun_still_blocks_on_real_conditions():
    """BLOCK: togliere il gate Codacy non deve aprire gli altri."""
    for key, value, expected in (
        ("checks_green", False, "checks_not_green"),
        ("pending_checks", [{"name": "unit"}], "pending_checks"),
        ("failing_checks", [{"name": "unit"}], "failing_checks"),
        ("evidence_head_sha", "deadbee", "evidence_head_mismatch"),
    ):
        ctx = _decommissioned_rerun_context()
        ctx[key] = value
        blockers = [str(b) for b in (controller.build_passive_rerun_readiness_plan(ctx).get("blocked_reasons") or [])]
        assert expected in blockers, (
            f"{key}={value!r}: atteso blocker {expected}, ottenuti {blockers}"
        )


# ---------------------------------------------------------------------------
# Giro 4 — le review full-range hanno trovato lo STESSO difetto specchiato:
# al giro 3 ho stretto il matcher in `pr_flow_automation`, ma quello del
# CONTROLLER (`is_codacy_check`) era rimasto per SOTTOSTRINGA su nome + url.
# Il controller e' un percorso decisionale separato: stesso fail-open, altro
# ingresso. Rilievo convergente di Fugu Ultra, Claude Fable 5 e Codex.
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize("name", [
    "codacy-decommission-guard",
    "verify no codacy references",
    "my-codacy-migration-check",
])
def test_controller_matcher_is_exact_too(name):
    check = {"name": name, "conclusion": "FAILURE", "status": "COMPLETED"}
    assert controller.is_codacy_check(check) is False, (
        f"{name!r} classificato come check Codacy dismesso: fail-open nel controller"
    )
    kept = [c["name"] for c in controller.filter_ignored_codacy_checks([check], {})]
    assert name in kept, f"{name!r} rimosso dai blockers dal filtro del controller"


@pytest.mark.unit
def test_controller_matcher_still_exempts_the_retired_check():
    check = {"name": "Codacy Static Code Analysis", "conclusion": "ACTION_REQUIRED",
             "status": "COMPLETED"}
    assert controller.is_codacy_check(check) is True
    assert controller.filter_ignored_codacy_checks([check], {}) == []


@pytest.mark.unit
def test_controller_matcher_does_not_trust_the_url():
    """Un check con URL codacy ma nome diverso NON e' il check dismesso.

    Prima bastava la stringa "codacy" nell'URL per far sparire un check dai
    blockers: un link in una descrizione era sufficiente.
    """
    check = {"name": "security-scan", "conclusion": "FAILURE", "status": "COMPLETED",
             "detailsUrl": "https://app.codacy.com/gh/owner/repo/pull-requests/1"}
    assert controller.is_codacy_check(check) is False
    assert [c["name"] for c in controller.filter_ignored_codacy_checks([check], {})] == ["security-scan"]


# --- Fable: `api_ok: True` e' evidenza FABBRICATA per un'API inesistente ----
@pytest.mark.unit
def test_decommissioned_evidence_does_not_claim_a_working_api():
    ev = controller.codacy_evidence_for_checks("repo", 1, [])
    assert ev.get("api_ok") is not True, (
        "lo stub dichiara api_ok=True per un'API che non esiste piu': "
        "evidenza fabbricata"
    )


# --- Codex: il micro-audit finale OBBLIGATORIO non veniva piu' schedulato ---
@pytest.mark.unit
def test_final_micro_audit_is_still_scheduled_after_decommission(tmp_path):
    audit = tmp_path / "audit.md"
    audit.write_text("micro-audit", encoding="utf-8")
    decision = {
        "pending": [], "bad": [], "blockers": [],
        "mergeStateStatus": "CLEAN", "mergeable": "MERGEABLE",
        "codacy_classification": "decommissioned",   # <- cio' che scrive oggi il controller
    }
    state = {"active_final_micro_audit_path": str(audit)}
    assert controller.should_run_final_micro_audit(decision, state) is True, (
        "il micro-audit finale obbligatorio non viene piu' schedulato: "
        "la classificazione 'decommissioned' non era accettata"
    )


@pytest.mark.unit
def test_final_micro_audit_still_requires_its_real_conditions(tmp_path):
    """BLOCK: togliere il gate Codacy non deve schedularlo a vuoto."""
    audit = tmp_path / "audit.md"
    audit.write_text("micro-audit", encoding="utf-8")
    base = {
        "pending": [], "bad": [], "blockers": [],
        "mergeStateStatus": "CLEAN", "mergeable": "MERGEABLE",
        "codacy_classification": "decommissioned",
    }
    state = {"active_final_micro_audit_path": str(audit)}
    for key, value in (("pending", [{"name": "x"}]), ("bad", [{"name": "x"}]),
                       ("mergeStateStatus", "DIRTY")):
        broken = dict(base); broken[key] = value
        assert controller.should_run_final_micro_audit(broken, state) is False, (
            f"{key}={value!r} non impedisce piu' la schedulazione"
        )
    # file di audit assente => niente schedulazione
    assert controller.should_run_final_micro_audit(base, {"active_final_micro_audit_path": ""}) is False


@pytest.mark.unit
def test_the_two_decision_paths_share_one_identity():
    """I due percorsi NON possono divergere: e' la causa radice del giro 4.

    Al giro 3 avevo stretto il matcher del gate CI (`pr_flow_automation`) e
    lasciato largo quello del controller: stesso fail-open, altro ingresso.
    Ora l'allowlist e' UNA SOLA, e questo test fallisce se qualcuno la duplica.
    """
    assert flow.DECOMMISSIONED_CODACY_CHECK_NAMES is controller.DECOMMISSIONED_CODACY_CHECK_NAMES

    # e i due predicati devono dare lo STESSO verdetto sugli stessi check
    for name in ("Codacy Static Code Analysis", "codacy-decommission-guard",
                 "verify no codacy references", "unit"):
        check = {"name": name, "conclusion": "FAILURE", "status": "COMPLETED"}
        assert flow.is_decommissioned_codacy_check(check) == controller.is_codacy_check(check), (
            f"i due percorsi divergono su {name!r}"
        )
