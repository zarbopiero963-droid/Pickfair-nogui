"""Contratto di autonomia end-to-end (owner 08/10/2026): casi eseguibili.

Norma: docs/auto_pr_flow_spec.md §0. Ogni gruppo di test corrisponde a un
gruppo di casi richiesti dall'owner (finding, reviewer, merge, fix loop,
PR successiva) più i legami con i file reali: workflow Sol/Grok, pr-guard e
readiness. Nessun test chiama la rete.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import pr_autonomy_policy as policy
from scripts import pr_fix_loop_policy as fix_policy
from scripts import pr_flow_automation as flow

ROOT = Path(__file__).resolve().parents[2]
HEAD = "0123456789abcdef0123456789abcdef01234567"

def hunks(simple: str) -> str:
    """Da «+++ b/f + righe» a un patch git reale (header + hunk con conteggi):
    dal #499 il parser legge le righe aggiunte solo dentro hunk dichiarati."""
    blocks: list[tuple[str, list[str]]] = []
    for line in simple.splitlines():
        if line.startswith("+++ b/"):
            blocks.append((line[6:], []))
        elif blocks:
            blocks[-1][1].append(line)
    out = []
    for path, body in blocks:
        added = sum(1 for b in body if b.startswith("+"))
        removed = sum(1 for b in body if b.startswith("-"))
        out += [f"diff --git a/{path} b/{path}", f"--- a/{path}", f"+++ b/{path}",
                f"@@ -{1 if removed else 0},{removed} +1,{added} @@", *body]
    return "\n".join(out) + "\n"
BASE = "fedcba9876543210fedcba9876543210fedcba98"


# ---------------------------------------------------------------------------
# Finding
# ---------------------------------------------------------------------------
def finding(origin: str, severity: str = "P2_COMPLETENESS", **extra):
    return {"origin": origin, "severity": severity, "current_head_sha": HEAD,
            "evidence": "riprodotto su base e head con test mirato", **extra}


def deferrable(**extra):
    base = {
        "proven_on_base": True, "recording_verified": True,
        "introduced_by_pr": False, "aggravated_by_pr": False, "activated_by_pr": False,
        "needed_for_acceptance": False, "compromises_delivered_path": False,
        "owner_forbids_defer": False, "future_card": "#461 PR27",
        "technical_owner": "PR27", "finding_id": "F-498-1",
        "future_blockers": ["BLOCKS_LIVE"],
    }
    base.update(extra)
    return finding("CURRENT_DEFECT_PREEXISTING", **base)


def theoretical(**extra):
    base = {k: True for k in policy.THEORETICAL_REQUIRED_TRUE}
    base.update(extra)
    return finding("THEORETICAL_MUTATION", **base)


def test_introdotto_dalla_pr_e_blocker_con_fix_ora():
    result = policy.classify_finding(finding("CURRENT_DEFECT_INTRODUCED_BY_PR", "P1_SAFETY"))
    assert result["disposition"] == "CURRENT_BLOCKER"
    assert result["patch"] and result["blocks_merge"] and not result["resolve_thread"]
    assert not result["owner_required"] and not result["mark_fixed"]


def test_introdotto_che_richiede_decisione_di_prodotto_va_all_owner():
    result = policy.classify_finding(finding("CURRENT_DEFECT_INTRODUCED_BY_PR",
                                             fix_requires_product_decision=True))
    assert result["owner_required"] and result["blocks_merge"] and not result["patch"]


def test_preesistente_indipendente_e_rinviabile_per_policy():
    result = policy.classify_finding(deferrable())
    assert result["disposition"] == "DEFERRED_BY_POLICY"
    assert result["resolve_thread"] and not result["blocks_merge"]
    assert result["blockers"] == ["BLOCKS_LIVE"] and not result["mark_fixed"]


@pytest.mark.parametrize("flag", ["activated_by_pr", "aggravated_by_pr", "needed_for_acceptance",
                                  "compromises_delivered_path"])
def test_preesistente_attivato_o_necessario_diventa_blocker(flag):
    result = policy.classify_finding(deferrable(**{flag: True}))
    assert result["disposition"] == "CURRENT_BLOCKER" and result["blocks_merge"]


@pytest.mark.parametrize("missing", ["proven_on_base", "recording_verified", "future_card",
                                     "technical_owner", "finding_id", "future_blockers",
                                     "owner_forbids_defer"])
def test_preesistente_senza_tutte_le_condizioni_non_e_rinviabile(missing):
    result = policy.classify_finding(deferrable(**{missing: None}))
    assert result["disposition"] == "NOT_DEFERRABLE_YET"
    assert result["blocks_merge"] and not result["resolve_thread"]
    assert missing in result["missing"]


def test_blocker_futuro_fuori_tassonomia_non_vale():
    result = policy.classify_finding(deferrable(future_blockers=["BLOCKS_SOMETHING"]))
    assert result["disposition"] == "NOT_DEFERRABLE_YET"


def test_p1_preesistente_segue_le_stesse_condizioni_strette():
    assert policy.classify_finding(deferrable(severity="P1_SAFETY"))["disposition"] == "DEFERRED_BY_POLICY"
    assert policy.classify_finding(deferrable(severity="P1_SAFETY", activated_by_pr=True))["blocks_merge"]


def test_teorico_completo_si_risolve_senza_patch():
    result = policy.classify_finding(theoretical())
    assert result["disposition"] == "THEORETICAL_MUTATION"
    assert result["resolve_thread"] and not result["patch"] and not result["blocks_merge"]


@pytest.mark.parametrize("missing", policy.THEORETICAL_REQUIRED_TRUE)
def test_teorico_incompleto_non_si_risolve(missing):
    result = policy.classify_finding(theoretical(**{missing: False}))
    assert result["disposition"] == "NOT_THEORETICAL"
    assert not result["resolve_thread"] and result["blocks_merge"]


def test_teorico_con_severita_denaro_non_e_teorico():
    assert policy.classify_finding(theoretical(severity="P0_REAL_MONEY"))["disposition"] == "NOT_THEORETICAL"


def test_p0_denaro_raggiungibile_ferma_e_non_si_rinvia():
    result = policy.classify_finding(deferrable(severity="P0_REAL_MONEY", reachable_from_pr=True,
                                                fixable_within_contract=True))
    assert result["disposition"] == "P0_FAIL_CLOSED"
    assert result["action"] == "STOP_AFFECTED_PATH_AND_FIX"
    assert result["blocks_merge"] and result["patch"] and not result["resolve_thread"]


def test_p0_fuori_contratto_va_all_owner_e_raggiungibilita_ignota_e_manuale():
    owner = policy.classify_finding(deferrable(severity="P0_SECURITY", reachable_from_pr=False,
                                               fixable_within_contract=False))
    assert owner["owner_required"] and owner["blocks_merge"]
    unknown = policy.classify_finding(deferrable(severity="P0_SECURITY"))
    assert unknown["disposition"] == "NEEDS_MANUAL" and "reachable_from_pr" in unknown["missing"]


def test_stale_provato_si_risolve():
    assert policy.classify_finding(finding("STALE", proven_absent_on_head=True))["resolve_thread"]
    assert not policy.classify_finding(finding("STALE"))["resolve_thread"]


def test_duplicato_rimanda_al_canonico():
    result = policy.classify_finding(finding("DUPLICATE", canonical_finding="F-498-1"))
    assert result["action"] == "LINK_CANONICAL_AND_RESOLVE" and not result["blocks_merge"]
    assert policy.classify_finding(finding("DUPLICATE"))["blocks_merge"]


def test_already_covered_richiede_prova_esistente():
    assert policy.classify_finding(finding("ALREADY_COVERED", covering_evidence="tests/x.py::t"))["resolve_thread"]
    assert not policy.classify_finding(finding("ALREADY_COVERED"))["resolve_thread"]


def test_churn_non_inventa_patch():
    result = policy.classify_finding(finding("REVIEW_CHURN", equivalent_to="F-1", current_head_correct=True))
    assert result["disposition"] == "REVIEW_CHURN" and not result["patch"]


def test_limitazione_owner_non_copre_p0_e_richiede_link():
    assert policy.classify_finding(finding("KNOWN_LIMITATION_ACCEPTED_BY_OWNER", "P0_REAL_MONEY",
                                           owner_acceptance_url="u"))["blocks_merge"]
    assert policy.classify_finding(finding("KNOWN_LIMITATION_ACCEPTED_BY_OWNER",
                                           owner_acceptance_url="u"))["resolve_thread"]


@pytest.mark.parametrize("bad", [{}, {"origin": "X", "severity": "P3_DOCS"},
                                 {"origin": "STALE", "severity": "P9"},
                                 {"origin": "STALE", "severity": "P3_DOCS"}])
def test_finding_malformato_e_fail_closed(bad):
    result = policy.classify_finding(bad)
    assert result["disposition"] == "NEEDS_MANUAL" and result["blocks_merge"]


def test_triage_codex_completa_solo_con_prove_e_zero_thread():
    ok = [finding("STALE", proven_absent_on_head=True)]
    assert policy.codex_triage_complete(ok, 0)["complete"]
    assert not policy.codex_triage_complete(ok, 1)["complete"]
    assert not policy.codex_triage_complete([finding("STALE")], 0)["complete"]
    assert not policy.codex_triage_complete(ok, None)["complete"]


# ---------------------------------------------------------------------------
# Reviewer
# ---------------------------------------------------------------------------
def review_body(reviewer: str, head: str = HEAD, bloccanti: str = "Nessun bloccante evidente.") -> str:
    marker = policy.ACTIVE_REVIEWERS[reviewer]
    return (f"<!-- {marker}:pickfair-nogui:{BASE[:12]}...{head[:12]} -->\n"
            f"# Review\n\n## Bloccanti\n{bloccanti}\n\n## Verdetto finale\nOK\n")


def review(reviewer: str, **extra):
    data = {"reviewer": reviewer, "author": policy.REVIEW_BOT_LOGIN, "body": review_body(reviewer)}
    data.update(extra)
    return data


@pytest.mark.parametrize("reviewer", ["sol", "grok"])
def test_review_sol_grok_valida_sul_current_head(reviewer):
    assert policy.validate_review(review(reviewer), HEAD)["status"] == "VALID_CLEAN"


@pytest.mark.parametrize("reviewer", ["fugu", "fable", "astra"])
def test_reviewer_sospesi_ignorati(reviewer):
    assert policy.validate_review({"reviewer": reviewer}, HEAD)["status"] == "IGNORED_SUSPENDED"


def test_codex_e_advisory():
    assert policy.validate_review({"reviewer": "codex"}, HEAD)["status"] == "ADVISORY"


def test_job_verde_ma_marker_assente_fallisce():
    body = "# GPT Review\n\n## Bloccanti\nNessun bloccante evidente.\n"
    assert policy.validate_review(review("sol", body=body), HEAD)["status"] == "MISSING_MARKER"


def test_errore_api_e_schema_incompleto_sono_unknown():
    assert policy.validate_review(review("sol", api_error="HTTP 502"), HEAD)["status"] == "UNKNOWN"
    assert policy.validate_review(review("sol", body=None), HEAD)["status"] == "UNKNOWN"
    no_section = review_body("sol").replace("## Bloccanti", "## Altro")
    assert policy.validate_review(review("sol", body=no_section), HEAD)["status"] == "UNKNOWN"


def test_review_su_head_vecchio_o_autore_falso_non_conta():
    stale = review("grok", body=review_body("grok", head="aaaaaaaaaaaa" + "0" * 28))
    assert policy.validate_review(stale, HEAD)["status"] == "STALE_HEAD"
    assert policy.validate_review(review("grok", author="someone"), HEAD)["status"] == "INVALID"


def test_review_con_bloccanti_non_e_pulita():
    body = review_body("sol", bloccanti="- P1: manca il fail-closed")
    assert policy.validate_review(review("sol", body=body), HEAD)["status"] == "VALID_WITH_BLOCKERS"


def test_prefisso_nessun_bloccante_seguito_da_bloccante_non_e_pulita():
    body = review_body("sol", bloccanti="Nessun bloccante evidente.\n- P1: il gate passa senza prova")
    assert policy.validate_review(review("sol", body=body), HEAD)["status"] == "VALID_WITH_BLOCKERS"
    for pulita in ("Nessun bloccante evidente", "- Nessun bloccante.", "nessun bloccante"):
        body = review_body("grok", bloccanti=pulita)
        assert policy.validate_review(review("grok", body=body), HEAD)["status"] == "VALID_CLEAN", pulita


@pytest.mark.parametrize("errore,atteso", [
    ("HTTP Error 401: Unauthorized", "PROVIDER_ERROR"),
    ("HTTP Error 400: invalid model", "PROVIDER_ERROR"),
    ("HTTP Error 503: Service Unavailable", "PROVIDER_ERROR"),
    ("HTTP Error 429: credit_balance_exhausted", "QUOTA"),
    ("The read operation timed out", "TIMEOUT"),
])
def test_errori_provider_non_sono_timeout(errore, atteso):
    body = f"## Review Grok 4.7 non completata\n\nErrore sintetico:\n\n```text\n{errore}\n```"
    status = policy.validate_review({"reviewer": "grok", "author": policy.REVIEW_BOT_LOGIN, "body": body}, HEAD)
    assert status["status"] == atteso
    gate = policy.reviewer_gate({"sol": {"status": "VALID_CLEAN"}, "grok": status}, 1)["status"]
    assert gate == {"PROVIDER_ERROR": "BLOCKED", "QUOTA": "STOP_OWNER", "TIMEOUT": "RERUN_GROK_ONCE"}[atteso]


@pytest.mark.parametrize("body_fn", [
    lambda r: review_body(r).replace("## Verdetto finale\nOK\n", ""),
    lambda r: review_body(r).replace("## Verdetto finale\nOK\n", "## Verdetto finale\n"),
    lambda r: review_body(r, bloccanti=""),
])
def test_review_con_schema_incompleto_e_unknown(body_fn):
    assert policy.validate_review(review("sol", body=body_fn("sol")), HEAD)["status"] == "UNKNOWN"


def test_import_con_alias_e_lista_vede_tutti_i_moduli():
    patch = ("+++ b/core/x.py\n+import requests as req, missing_package\n"
             "+from other_missing.sub import y as z\n+from . import locale\n"
             "+from yaml_like import (\n")
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["core/x.py"], patch_text=hunks(patch),
                                  declared_files=["core/x.py"], local_modules=["core"],
                                  declared_dependencies=["requests"])
    assert diff["new_dependencies"] == ["missing_package", "other_missing", "yaml_like"]
    assert "undeclared_new_dependency" in diff["problems"]


@pytest.mark.parametrize("sol,atteso", [
    ("VALID_WITH_BLOCKERS", "BLOCKED"), ("STALE_HEAD", "BLOCKED"), ("MISSING_MARKER", "BLOCKED"),
    ("UNKNOWN", "UNKNOWN"), (None, "UNKNOWN"),
])
def test_secondo_timeout_grok_non_nasconde_blocchi_di_sol(sol, atteso):
    results = {"sol": {"status": sol} if sol else {}, "grok": {"status": "TIMEOUT"}}
    res = policy.reviewer_gate(results, 2)
    assert res["status"] == atteso and "PRONTA PER MERGE" not in res["reason"]


def test_grok_primo_timeout_rerun_secondo_stop():
    timeout = {"reviewer": "grok", "author": policy.REVIEW_BOT_LOGIN,
               "body": "Review Grok non completata: The read operation timed out"}
    status = policy.validate_review(timeout, HEAD)
    assert status["status"] == "TIMEOUT"
    results = {"sol": {"status": "VALID_CLEAN"}, "grok": status}
    assert policy.reviewer_gate(results, 1)["status"] == "RERUN_GROK_ONCE"
    second = policy.reviewer_gate(results, 2)
    assert second["status"] == "STOP_OWNER" and "Grok assente per timeout" in second["reason"]


def test_quota_esaurita_ferma_per_p41():
    quota = {"reviewer": "sol", "author": policy.REVIEW_BOT_LOGIN, "body": "HTTP 402 Payment Required"}
    results = {"sol": policy.validate_review(quota, HEAD), "grok": {"status": "VALID_CLEAN"}}
    assert policy.reviewer_gate(results, 1) == {"status": "STOP_OWNER", "reason": "CREDITI_ESAURITI (P41)"}


def test_gate_reviewer_pass_solo_con_entrambi_puliti():
    clean = {"status": "VALID_CLEAN"}
    assert policy.reviewer_gate({"sol": clean, "grok": clean}, 1)["status"] == "PASS"
    assert policy.reviewer_gate({"sol": clean}, 1)["status"] == "UNKNOWN"
    assert policy.reviewer_gate({"sol": clean, "grok": {"status": "STALE_HEAD"}}, 1)["status"] == "BLOCKED"


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------
def full_diff(manual=False, status="PASS"):
    return {"status": status, "owner_manual_merge_required": manual}


def merge_state(**extra):
    state = {
        "evaluated_head": HEAD, "current_head": HEAD,
        "scope_valid": True, "acceptance_complete": True, "suite_pass": True,
        "hard_verify_pass": True, "codex_triaged": True, "fix_loop_valid": True,
        "dependencies_satisfied": True, "pertinent_owner_decision_open": False,
        "manual_stop": False, "manual_label_present": False,
        "preexisting_activated_or_aggravated": False, "unresolved_threads": 0,
        "introduced_p0_p1_open": 0, "reviewer_gate": "PASS", "merge_readiness": "PASS",
        "full_diff": full_diff(), "fix_loop_exhausted": False,
    }
    state.update(extra)
    return state


def test_runtime_safety_critical_con_tutti_i_gate_va_in_auto_merge():
    diff = policy.full_diff_check(
        base_sha=BASE, head_sha=HEAD, files=["core/runtime_controller.py", "tests/core/test_x.py",
                                             ".guardrails/allowed_scope.json"],
        patch_text="", declared_files=["core/runtime_controller.py", "tests/core/test_x.py",
                                       ".guardrails/allowed_scope.json"],
        local_modules=[], declared_dependencies=[])
    assert diff["classification"]["safety_critical"]
    assert diff["status"] == "PASS" and diff["owner_manual_merge_required"] is False
    label = policy.manual_label_decision(["core/runtime_controller.py"])
    assert label["label"] == "NOT_REQUIRED"
    assert policy.merge_decision(merge_state(full_diff=diff))["status"] == "READY_FOR_AUTO_MERGE"


def test_file_gate_resta_merge_manuale_owner():
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["AGENTS.md"], patch_text="",
                                  declared_files=["AGENTS.md"], local_modules=[], declared_dependencies=[])
    assert diff["owner_manual_merge_required"]
    assert policy.merge_decision(merge_state(full_diff=diff))["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert policy.manual_label_decision(["AGENTS.md"])["label"] == "REQUIRED"


def test_pr_di_autorita_etichettata_arriva_a_ready_for_owner_manual_merge():
    files = ["AGENTS.md", "scripts/pr_autonomy_policy.py", ".guardrails/allowed_scope.json"]
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text="",
                                  declared_files=files, local_modules=[], declared_dependencies=[])
    assert policy.manual_label_decision(files)["label"] == "REQUIRED"
    res = policy.merge_decision(merge_state(full_diff=diff, manual_label_present=True))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"


def test_decisione_owner_aperta_pertinente_blocca():
    result = policy.merge_decision(merge_state(pertinent_owner_decision_open=True))
    assert result["status"] == "BLOCKED" and "pertinent_owner_decision_open" in result["reasons"]


def test_thread_irrisolto_blocca():
    assert policy.merge_decision(merge_state(unresolved_threads=1))["status"] == "BLOCKED"


def test_head_cambiato_invalida():
    assert policy.merge_decision(merge_state(current_head="f" * 40))["status"] == "INVALIDATED"


def test_full_diff_fuori_scope_blocca():
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["core/a.py", "core/b.py"],
                                  patch_text="", declared_files=["core/a.py"],
                                  local_modules=[], declared_dependencies=[])
    assert diff["status"] == "BLOCK" and "scope_mismatch_full_diff" in diff["problems"]
    assert policy.merge_decision(merge_state(full_diff=diff))["status"] == "BLOCKED"


@pytest.mark.parametrize("key", ["suite_pass", "codex_triaged", "unresolved_threads", "reviewer_gate",
                                 "merge_readiness", "full_diff", "manual_label_present"])
def test_stato_incompleto_e_needs_manual(key):
    state = merge_state()
    state.pop(key)
    assert policy.merge_decision(state)["status"] == "NEEDS_MANUAL"


def test_label_manuale_instrada_al_merge_owner_e_stop_reviewer_e_manuale():
    # La label non blocca: vieta l'auto-merge e porta al merge MANUALE owner.
    res = policy.merge_decision(merge_state(manual_label_present=True))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and "manual_review_label" in res["reasons"]
    # ...ma non scavalca gli altri gate.
    assert policy.merge_decision(merge_state(manual_label_present=True,
                                             unresolved_threads=1))["status"] == "BLOCKED"
    assert policy.merge_decision(merge_state(reviewer_gate="STOP_OWNER"))["status"] == "NEEDS_MANUAL"
    assert policy.merge_decision(merge_state(fix_loop_exhausted=True))["status"] == "NEEDS_MANUAL"


def test_label_manuale_solo_per_motivi_ammessi():
    assert policy.manual_label_decision([], ["FIX_LOOP_EXHAUSTED"])["label"] == "REQUIRED"
    assert policy.manual_label_decision([], ["SAFETY_CRITICAL"])["removal_allowed"] is False
    assert policy.manual_label_decision([], [], compare_truncated=True)["label"] == "REQUIRED"
    assert policy.manual_label_decision(["services/telegram_service.py"])["removal_allowed"]


def test_full_diff_blocca_segreti_input_guard_e_dipendenze_non_dichiarate():
    patch = "+++ b/core/x.py\n+import requests_toolbelt\n+from core import y\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD,
                                  files=["core/x.py", ".env", "pr_meta.json"], patch_text=hunks(patch),
                                  declared_files=["core/x.py", ".env", "pr_meta.json"],
                                  local_modules=["core"], declared_dependencies=["requests"])
    assert {"secret_material_in_diff", "guard_generated_input_in_diff",
            "undeclared_new_dependency"} <= set(diff["problems"])
    assert diff["new_dependencies"] == ["requests_toolbelt"]


def test_full_diff_segreto_cancellato_e_template_non_bloccano():
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["config.json", ".env.example"],
                                  patch_text="", declared_files=["config.json", ".env.example"],
                                  local_modules=[], declared_dependencies=[],
                                  deleted_files=["config.json"])
    assert diff["status"] == "PASS"


def test_full_diff_riporta_nuovi_chiamanti_del_percorso_denaro_e_dipendenze_test():
    patch = ("+++ b/services/new.py\n+    client.place_orders(market, ins)\n"
             "+++ b/tests/test_new.py\n+    from PIL import ImageGrab\n")
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD,
                                  files=["services/new.py", "tests/test_new.py"], patch_text=hunks(patch),
                                  declared_files=["services/new.py", "tests/test_new.py"],
                                  local_modules=[], declared_dependencies=[])
    assert diff["status"] == "PASS"
    assert diff["new_money_path_callers"] == ["services/new.py"]
    assert diff["new_test_only_dependencies"] == ["PIL"]


def test_full_diff_menzione_o_commento_non_e_un_chiamante():
    patch = ("+++ b/scripts/x.py\n+RE = r\"place_bet|place_orders\"\n"
             "+    # place_bet(x) era il vecchio percorso\n")
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["scripts/x.py"], patch_text=hunks(patch),
                                  declared_files=["scripts/x.py"], local_modules=[],
                                  declared_dependencies=[])
    assert diff["new_money_path_callers"] == []


# Valori finti costruiti a pezzi: il sorgente del test non ha forma di segreto.
_FAKE_GH = "gh" + "p_" + "A1b2C3d4" * 5
_FAKE_KEY = "-----BEGIN " + "RSA PRIVATE KEY-----"


@pytest.mark.parametrize("path,line", [
    ("docs/note.md", f"incollato per errore {_FAKE_GH}"),
    (".github/workflows/x.yml", f"  - run: echo {_FAKE_GH}"),
    ("core/a.py", f"PEM = '{_FAKE_KEY}'"),
])
def test_full_diff_contenuto_segreto_in_qualunque_file_blocca(path, line):
    patch = f"+++ b/{path}\n+{line}\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=[path], patch_text=hunks(patch),
                                  declared_files=[path], local_modules=[], declared_dependencies=[])
    assert diff["status"] == "BLOCK"
    assert "secret_like_content_in_diff" in diff["problems"]
    assert diff["secret_like_content"] == [path]
    # Il report porta solo path e codici: il valore non esce mai.
    assert _FAKE_GH not in json.dumps(diff) and "PRIVATE KEY" not in json.dumps(diff)


def test_full_diff_segreto_nei_test_senza_marker_blocca():
    patch = f"+++ b/tests/fixtures/tls.py\n+KEY = '{_FAKE_KEY}'\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["tests/fixtures/tls.py"],
                                  patch_text=hunks(patch), declared_files=["tests/fixtures/tls.py"],
                                  local_modules=[], declared_dependencies=[])
    assert diff["status"] == "BLOCK" and diff["secret_like_content"] == ["tests/fixtures/tls.py"]


def test_full_diff_chiave_sintetica_dichiarata_nei_test_va_a_merge_owner():
    patch = f"+++ b/tests/fixtures/tls.py\n+SYNTHETIC_KEY = '{_FAKE_KEY}'\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["tests/fixtures/tls.py"],
                                  patch_text=hunks(patch), declared_files=["tests/fixtures/tls.py"],
                                  local_modules=[], declared_dependencies=[])
    assert diff["status"] == "PASS"
    assert diff["secret_like_content_tests"] == ["tests/fixtures/tls.py"]
    assert diff["owner_manual_merge_required"] is True


def test_full_diff_riga_rimossa_con_segreto_non_blocca():
    patch = f"+++ b/docs/note.md\n-incollato {_FAKE_GH}\n+incollato <rimosso>\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["docs/note.md"], patch_text=hunks(patch),
                                  declared_files=["docs/note.md"], local_modules=[], declared_dependencies=[])
    assert diff["status"] == "PASS"


def test_pattern_segreti_non_corrispondono_ai_sorgenti_del_contratto():
    for rel in ("scripts/pr_autonomy_policy.py", "tests/scripts/test_pr_autonomy_policy.py",
                ".github/workflows/pr-guard.yml", "docs/auto_pr_flow_spec.md"):
        testo = (ROOT / rel).read_text(encoding="utf-8")
        assert not any(policy.SECRET_CONTENT_RE.search(r) for r in testo.splitlines()), rel


def test_mcp_documentazione_non_e_violazione_adapter():
    patch = "+++ b/docs/limits.md\n+MCP non chiama mai api.betfair.com\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["docs/limits.md"], patch_text=hunks(patch),
                                  declared_files=["docs/limits.md"], local_modules=[],
                                  declared_dependencies=[], repo_kind="mcp")
    assert diff["mcp_adapter_violations"] == [] and diff["status"] == "PASS"


def test_pr_guard_patch_copre_tutti_i_file():
    wf = (ROOT / ".github/workflows/pr-guard.yml").read_text(encoding="utf-8")
    assert ("git diff --no-ext-diff --no-textconv --text --no-renames -U0 \"${PR_BASE_SHA}\"...HEAD"
            " > pr_full_diff.patch") in wf
    assert "-- '*.py' > pr_full_diff.patch" not in wf


def test_manifest_toccato_non_copre_import_non_dichiarato():
    patch = "+++ b/core/x.py\n+import missing_package\n+++ b/requirements.txt\n+# commento\n"
    files = ["core/x.py", "requirements.txt"]
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=hunks(patch),
                                  declared_files=files, local_modules=["core"],
                                  declared_dependencies=["requests"])
    assert diff["status"] == "BLOCK" and "undeclared_new_dependency" in diff["problems"]
    assert diff["owner_manual_merge_required"] is True


def test_dipendenze_dichiarate_in_pyproject_contano():
    deps = policy._pyproject_dependencies(str(ROOT / "pyproject.toml"))
    assert {"betfairlightweight", "telethon", "dateutil", "pytest_xdist"} <= deps


def test_mcp_test_e_commenti_non_sono_violazioni():
    patch = ("+++ b/tests/test_tools.py\n+assert 'betfair_client' not in src\n"
             "+++ b/src/tools.py\n+# MCP non chiama mai betfair_client\n")
    files = ["tests/test_tools.py", "src/tools.py"]
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=hunks(patch),
                                  declared_files=files, local_modules=[], declared_dependencies=[],
                                  repo_kind="mcp")
    assert diff["mcp_adapter_violations"] == [] and diff["status"] == "PASS"


@pytest.mark.parametrize("path", ["scripts/pr_clean_scope_rebuild.py", "scripts/pr_refresh_self_checks.py",
                                  "scripts/pr_costo_review.py", "scripts/ci/check_ci_quarantine.py"])
def test_script_di_automazione_pr_sono_a_merge_owner(path):
    assert policy.is_owner_manual_path(path)
    assert policy.manual_label_decision([path])["label"] == "REQUIRED"


def test_full_diff_header_git_tra_virgolette_e_decodificato(tmp_path):
    """Rilievo Codex #499: path non ASCII → header quotato, il file non va saltato."""
    repo = tmp_path / "r"
    repo.mkdir()
    run = lambda *a: subprocess.run(["git", *a], cwd=repo, check=True, capture_output=True, text=True)
    run("init", "-q")
    run("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "base")
    (repo / "tést.py").write_text(f"import modulo_ignoto\nX = '{_FAKE_GH}'\n", encoding="utf-8")
    run("add", "-A")
    run("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x")
    patch = run("diff", "--no-renames", "-U0", "HEAD~1", "HEAD").stdout
    assert '+++ "b/t\\303\\251st.py"' in patch  # git quota davvero il path
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["tést.py"], patch_text=patch,
                                  declared_files=["tést.py"], local_modules=[], declared_dependencies=[])
    assert diff["secret_like_content"] == ["tést.py"]
    assert diff["new_dependencies"] == ["modulo_ignoto"]
    assert diff["status"] == "BLOCK"


@pytest.mark.parametrize("header", ['+++ "b/rotto.py', '+++ "b/x\\q.py"', "+++ c/strano.py"])
def test_full_diff_header_non_interpretabile_e_unknown(header):
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["rotto.py"],
                                  patch_text=f"{header}\n+import os\n",
                                  declared_files=["rotto.py"], local_modules=[], declared_dependencies=[])
    assert diff["status"] == "UNKNOWN" and "patch_unreadable" in diff["problems"]


def test_full_diff_file_cancellato_dev_null_non_e_errore():
    patch = "diff --git a/old.py b/old.py\ndeleted file mode 100644\n--- a/old.py\n+++ /dev/null\n@@ -1 +0,0 @@\n-import os\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["old.py"], patch_text=patch,
                                  declared_files=["old.py"], local_modules=[], declared_dependencies=[],
                                  deleted_files=["old.py"])
    assert diff["status"] == "PASS"


def test_import_con_tab_e_visto():
    patch = "+++ b/core/x.py\n+import\tpacchetto_tab\n+from\taltro_tab import y\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["core/x.py"], patch_text=hunks(patch),
                                  declared_files=["core/x.py"], local_modules=["core"], declared_dependencies=[])
    assert diff["new_dependencies"] == ["altro_tab", "pacchetto_tab"] and diff["status"] == "BLOCK"


def test_marker_sintetico_dentro_il_valore_non_conta():
    token = "gh" + "p_" + "fake" + "A1b2C3d4" * 5
    patch = f"+++ b/tests/test_x.py\n+T = '{token}'\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["tests/test_x.py"], patch_text=hunks(patch),
                                  declared_files=["tests/test_x.py"], local_modules=[], declared_dependencies=[])
    assert diff["status"] == "BLOCK" and diff["secret_like_content"] == ["tests/test_x.py"]


def test_riga_aggiunta_che_inizia_con_piu_piu_non_e_un_header():
    patch = ("diff --git a/core/x.py b/core/x.py\n--- a/core/x.py\n+++ b/core/x.py\n"
             "@@ -0,0 +1,2 @@\n+++ counter\n+import missing_package\n")
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["core/x.py"], patch_text=patch,
                                  declared_files=["core/x.py"], local_modules=["core"], declared_dependencies=[])
    assert diff["new_dependencies"] == ["missing_package"] and diff["status"] == "BLOCK"


@pytest.mark.parametrize("patch", [
    "--- a/x.py\n+++ b/x.py\n@@ -0,0 +1,3 @@\n+a\n",              # hunk incompleto
    "--- a/x.py\n+++ b/x.py\n@@ -0,0 +1,1 @@\n+a\n+b\n",          # riga oltre il conteggio
    "--- a/x.py\n+++ b/x.py\n+import fuori_hunk\n",                  # aggiunta senza hunk
])
def test_hunk_malformato_e_unknown(patch):
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["x.py"], patch_text=patch,
                                  declared_files=["x.py"], local_modules=[], declared_dependencies=[])
    assert diff["status"] == "UNKNOWN" and "patch_unreadable" in diff["problems"]


def test_dipendenze_di_prodotto_separate_da_quelle_di_test():
    patch = ("+++ b/core/new.py\n+import pytest\n+++ b/tests/test_new.py\n+import pytest\n"
             "+++ b/scripts/tool.py\n+import pytest\n")
    files = ["core/new.py", "tests/test_new.py", "scripts/tool.py"]
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=hunks(patch),
                                  declared_files=files, local_modules=["core"],
                                  declared_dependencies=["requests", "pytest"],
                                  declared_production_dependencies=["requests"])
    assert diff["new_dependencies"] == ["pytest"] and diff["status"] == "BLOCK"
    assert diff["new_test_only_dependencies"] == []


def test_cli_dipendenze_di_prodotto_lette_da_requirements_e_pyproject():
    prod = policy._pyproject_dependencies(str(ROOT / "pyproject.toml"), include_optional=False)
    assert "betfairlightweight" in prod and "pytest" not in prod and "pytest_xdist" not in prod


@pytest.mark.parametrize("valore", [None, "true", 1, "__omesso__"])
def test_fix_loop_esaurito_deve_essere_booleano(valore):
    state = merge_state()
    if valore == "__omesso__":
        state.pop("fix_loop_exhausted")
    else:
        state["fix_loop_exhausted"] = valore
    assert policy.merge_decision(state)["status"] == "NEEDS_MANUAL"
    assert policy.merge_decision(merge_state(fix_loop_exhausted=True))["status"] == "NEEDS_MANUAL"


def test_file_di_testo_mostrato_come_binario_non_e_ispezionato():
    patch = ("diff --git a/core/x.py b/core/x.py\nindex 1..2 100644\n"
             "Binary files a/core/x.py and b/core/x.py differ\n"
             "diff --git a/img/logo.png b/img/logo.png\nBinary files a/img/logo.png and b/img/logo.png differ\n")
    files = ["core/x.py", "img/logo.png"]
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=patch,
                                  declared_files=files, local_modules=[], declared_dependencies=[])
    assert diff["status"] == "UNKNOWN" and diff["unscanned_text_files"] == ["core/x.py"]


def test_gitattributes_meno_diff_non_nasconde_il_file_con_i_flag_di_pr_guard(tmp_path):
    repo = tmp_path / "r"
    repo.mkdir()
    run = lambda *a: subprocess.run(["git", *a], cwd=repo, check=True, capture_output=True, text=True)
    run("init", "-q")
    run("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "base")
    (repo / ".gitattributes").write_text("*.py -diff\n", encoding="utf-8")
    (repo / "x.py").write_text("import modulo_nascosto\n", encoding="utf-8")
    run("add", "-A")
    run("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x")
    files = [".gitattributes", "x.py"]
    nudo = run("diff", "--no-renames", "-U0", "HEAD~1", "HEAD").stdout
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=nudo,
                                  declared_files=files, local_modules=[], declared_dependencies=[])
    assert diff["status"] == "UNKNOWN" and "x.py" in diff["unscanned_text_files"]
    guard = run("diff", "--no-ext-diff", "--no-textconv", "--text", "--no-renames", "-U0", "HEAD~1", "HEAD").stdout
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=guard,
                                  declared_files=files, local_modules=[], declared_dependencies=[])
    assert diff["new_dependencies"] == ["modulo_nascosto"] and diff["status"] == "BLOCK"


def test_full_diff_illeggibile_e_unknown():
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=None, patch_text="",
                                  declared_files=[], local_modules=[], declared_dependencies=[])
    assert diff["status"] == "UNKNOWN" and diff["owner_manual_merge_required"]


def test_mcp_resta_adapter():
    patch = "+++ b/src/tools.py\n+URL = 'https://api.betfair.com/exchange'\n"
    diff = policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=["src/tools.py"], patch_text=hunks(patch),
                                  declared_files=["src/tools.py"], local_modules=[],
                                  declared_dependencies=[], repo_kind="mcp")
    assert "mcp_adapter_violation" in diff["problems"]


@pytest.mark.parametrize("raw,expected", [
    ({"can_merge": True, "reasons": []}, "PASS"),
    ({"can_merge": False, "reasons": ["x"]}, "BLOCKED"),
    ({"can_merge": True}, "UNKNOWN"),
    ({"can_merge": True, "reasons": [], "errors": ["boom"]}, "UNKNOWN"),
    ({"can_merge": True, "reasons": [], "pagination_complete": False}, "UNKNOWN"),
    ({"can_merge": True, "reasons": [], "pagination_complete": "false"}, "UNKNOWN"),
    ({"can_merge": True, "reasons": [], "pagination_complete": 0}, "UNKNOWN"),
    ({"can_merge": True, "reasons": [], "pagination_complete": None}, "UNKNOWN"),
    ({"can_merge": True, "reasons": [], "pagination_complete": True}, "PASS"),
    ({"can_merge": False, "reasons": ["review_threads_api_unavailable"]}, "NEEDS_MANUAL"),
    (None, "UNKNOWN"),
])
def test_readiness_interpretata_fail_closed(raw, expected):
    assert policy.interpret_readiness(raw) == expected


# ---------------------------------------------------------------------------
# Readiness reale: risposta GraphQL inattesa -> errore, mai "zero thread"
# ---------------------------------------------------------------------------
def _page(nodes, page_info):
    conn = {"nodes": nodes}
    if page_info is not None:
        conn["pageInfo"] = page_info
    return {"data": {"repository": {"pullRequest": {"reviewThreads": conn}}}}


@pytest.mark.parametrize("raw", [
    {"errors": [{"message": "rate limited"}]},
    {"data": {"repository": None}},
    {"data": {"repository": {"pullRequest": {"reviewThreads": None}}}},
    _page("x", {"hasNextPage": False}),
    _page([], None),
    _page([], {"hasNextPage": True, "endCursor": None}),
    _page([], {"hasNextPage": "yes"}),
])
def test_thread_review_illeggibili_sollevano(monkeypatch, raw):
    monkeypatch.setattr(flow, "gh_json", lambda *_a, **_k: raw)
    with pytest.raises(ValueError):
        flow.fetch_all_review_threads("owner/repo", "1")


def test_cursore_ripetuto_non_cicla(monkeypatch):
    monkeypatch.setattr(flow, "gh_json",
                        lambda *_a, **_k: _page([], {"hasNextPage": True, "endCursor": "c1"}))
    with pytest.raises(ValueError):
        flow.fetch_all_review_threads("owner/repo", "1")


def test_readiness_con_thread_illeggibili_non_puo_mergiare(monkeypatch):
    monkeypatch.setattr(flow, "fetch_all_review_threads",
                        lambda *_a: (_ for _ in ()).throw(ValueError("schema")))
    monkeypatch.setattr(flow, "build_decision",
                        lambda *_a, **_k: {"can_merge": True, "reasons": [], "next_action": "merge"})
    decision = flow._readiness_decision("owner/repo", "1", True)
    assert decision["can_merge"] is False
    assert policy.interpret_readiness(decision) == "NEEDS_MANUAL"


# ---------------------------------------------------------------------------
# Fix loop (ledger reale di pr_fix_loop_policy)
# ---------------------------------------------------------------------------
def _assessment(kind="CURRENT_DEFECT"):
    return {"class": kind, "thread_id": "T1", "current_head_sha": "head",
            "evidence": "test mirato riproducibile", "pr_branch": "branch",
            "current_head_correct": kind != "CURRENT_DEFECT", "material": False,
            "contract_critical": False, **{r: False for r in fix_policy.PROTECTED_RISKS}}


def test_cap_fix_loop_resta_cinque_cumulativo():
    assert fix_policy.MAX_FIX_LOOP_ITERATIONS_PER_PR == 5


def test_fix_loop_senza_reset_e_override_limitato(tmp_path):
    ledger = fix_policy.FixLoopLedger(tmp_path / "b.sqlite", "owner/repo", 7)
    ledger.initialize(5, "storia completa attestata: 5 cicli")
    reopened = fix_policy.FixLoopLedger(tmp_path / "b.sqlite", "owner/repo", 7)
    reopened.initialize(0, "nuovo head")  # non azzera la storia
    assert reopened.status()["completed_count"] == 5
    stop = reopened.reserve("6", _assessment())
    assert not stop["allowed"] and stop["REASON"] == "fix_loop_budget_exhausted"
    with pytest.raises(ValueError):
        reopened.grant_override({"author": "owner", "repo": "owner/repo", "pr": 7,
                                 "body": "IGNORE_FIX_LOOP_LIMIT=true"})


def test_churn_non_consuma_un_ciclo_artificiale():
    assert fix_policy.triage(_assessment("DUPLICATE"))["decision"] != "PATCH_REQUIRED"
    churn = policy.classify_finding(finding("REVIEW_CHURN", equivalent_to="F-1", current_head_correct=True))
    assert churn["patch"] is False


# ---------------------------------------------------------------------------
# PR successiva e parallelismo
# ---------------------------------------------------------------------------
def next_state(**extra):
    state = {"merge_verified": True, "merge_sha": "a193909", "main_reread": True,
             "roadmap_reread": True, "open_prs_same_repo": 0, "next_card": "PR26",
             "dependencies_satisfied": True, "pertinent_open_owner_decisions": [],
             "unrelated_open_owner_decisions": ["MANUAL via MCP"], "active_stop_conditions": []}
    state.update(extra)
    return state


def test_merge_verificato_repo_libero_dipendenze_ok_parte_phase0():
    assert policy.next_pr_decision(next_state()) == {"status": "START_PHASE_0", "card": "PR26", "reason": ""}


@pytest.mark.parametrize("campo", ["pertinent_open_owner_decisions", "active_stop_conditions"])
@pytest.mark.parametrize("valore", ["__omesso__", None, "nessuna", 0])
def test_prove_su_stop_e_decisioni_devono_essere_esplicite(campo, valore):
    state = next_state()
    if valore == "__omesso__":
        state.pop(campo)
    else:
        state[campo] = valore
    res = policy.next_pr_decision(state)
    assert res["status"] == "STOP_OWNER" and res["reason"] == "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE"


def test_decisione_aperta_pertinente_ferma():
    assert policy.next_pr_decision(next_state(pertinent_open_owner_decisions=["X"]))["status"] == "STOP_OWNER"


def test_dipendenza_bloccante_ferma():
    assert policy.next_pr_decision(next_state(dependencies_satisfied=False))["status"] == "STOP_BLOCKING_DEPENDENCY"


def test_decisione_aperta_non_correlata_non_ferma():
    assert policy.next_pr_decision(next_state())["status"] == "START_PHASE_0"


def test_merge_non_verificato_pr_aperta_e_scheda_gia_soddisfatta():
    assert policy.next_pr_decision(next_state(merge_verified=False))["status"] == "WAIT_MERGE_NOT_VERIFIED"
    assert policy.next_pr_decision(next_state(open_prs_same_repo=1))["status"] == "BLOCKED_OPEN_PR"
    assert policy.next_pr_decision(next_state(card_already_satisfied=True))["status"] == \
        "CARD_SATISFIED_RECORD_EVIDENCE"
    assert policy.next_pr_decision(next_state(active_stop_conditions=["MANUAL_VIA_MCP"]))["status"] == "STOP_OWNER"


def _pf(**extra):
    return {"changes_control_api_contract": False, **extra}


def _mcp(**extra):
    return {"exposes_mutation": False, "core_authority_ready": False,
            "consumes_control_api_contract": False, **extra}


def test_parallelismo_cross_repo():
    assert policy.parallel_classification(_pf(), _mcp(exposes_mutation=True)) == "FORBIDDEN_PARALLEL"
    assert policy.parallel_classification(_pf(changes_control_api_contract=True),
                                          _mcp(consumes_control_api_contract=True)) == "FORBIDDEN_PARALLEL"
    assert policy.parallel_classification(_pf(unmerged_outputs=["PF-API-1"]),
                                          _mcp(depends_on=["PF-API-1"])) == "DEPENDENT"
    assert policy.parallel_classification(_pf(), _mcp(depends_on=[])) == "SAFE_PARALLEL"
    assert policy.parallel_classification(_pf(), _mcp(exposes_mutation=True,
                                                      core_authority_ready=True)) == "SAFE_PARALLEL"


@pytest.mark.parametrize("pf,mcp", [
    (_pf(), _mcp(exposes_mutation=True, core_authority_ready="false")),
    (_pf(), {"exposes_mutation": False}),
    (_pf(changes_control_api_contract="no"), _mcp()),
    (_pf(depends_on="PF-API-1"), _mcp()),
])
def test_parallelismo_prove_non_booleane_sono_vietate(pf, mcp):
    assert policy.parallel_classification(pf, mcp) == "FORBIDDEN_PARALLEL"


def test_stop_owner_solo_per_le_dieci_condizioni():
    assert len(policy.OWNER_STOP_CONDITIONS) == 10
    assert policy.owner_stop_required([])["stop"] is False
    assert policy.owner_stop_required(["FIX_LOOP_EXHAUSTED"])["stop"] is True
    assert policy.owner_stop_required(["SCELTA_TECNICA"])["stop"] is True  # ignoto: fail-closed


# ---------------------------------------------------------------------------
# Legami con i file reali
# ---------------------------------------------------------------------------
REVIEWER_ATTIVI = (".github/workflows/pr-review-openrouter-gpt56-sol.yml",
                   ".github/workflows/pr-review-xai-grok46.yml")


def _manual_patterns(workflow: str) -> list[str]:
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    righe = testo.splitlines()
    inizio = next(i for i, r in enumerate(righe) if r.strip() == "MANUAL_AUTHORITY_PATTERNS = [")
    fine = next(j for j in range(inizio + 1, len(righe)) if righe[j].strip() == "]")
    return re.findall(r'^\s+r"([^"]+)",\s*$', "\n".join(righe[inizio:fine]), re.MULTILINE)


@pytest.mark.parametrize("workflow", REVIEWER_ATTIVI)
def test_workflow_attivi_usano_la_lista_manuale_canonica(workflow):
    assert _manual_patterns(workflow) == list(policy.MANUAL_AUTHORITY_PATTERNS)
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert "if manual_files or compare_truncated:" in testo
    assert "if critical_files or compare_truncated:" not in testo
    assert "Safety-critical (informativo)" in testo


@pytest.mark.parametrize("path", sorted(policy.OWNER_MANUAL_MERGE_FILES))
def test_ogni_file_a_merge_owner_mette_la_label(path):
    assert any(re.search(p, path) for p in policy.MANUAL_AUTHORITY_PATTERNS)


@pytest.mark.parametrize("path", ["core/runtime_controller.py", "services/telegram_service.py",
                                  "betfair_client.py", "dutching.py", "tests/core/test_x.py"])
def test_runtime_core_non_mette_la_label(path):
    assert not policy.is_owner_manual_path(path)


def test_pr_guard_lancia_il_controllo_full_diff_isolato():
    testo = (ROOT / ".github/workflows/pr-guard.yml").read_text(encoding="utf-8")
    eseguibili = "\n".join(r.split("#", 1)[0] for r in testo.splitlines())
    assert "python -I scripts/pr_autonomy_policy.py full-diff" in eseguibili
    assert "pr_full_diff.patch; do" in eseguibili  # guardia anti-symlink estesa


def test_cli_full_diff_end_to_end(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps({"title": "[TASK: k] x"}), encoding="utf-8")
    (tmp_path / "files.json").write_text(json.dumps(["docs/x.md"]), encoding="utf-8")
    (tmp_path / "scope.json").write_text(json.dumps({"tasks": {"k": {"files": ["docs/x.md"]}}}),
                                         encoding="utf-8")
    (tmp_path / "p.patch").write_text("", encoding="utf-8")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/x.md").write_text("x", encoding="utf-8")
    res = subprocess.run(
        [sys.executable, "-I", str(ROOT / "scripts/pr_autonomy_policy.py"), "full-diff",
         "--meta", "meta.json", "--files", "files.json", "--patch", "p.patch",
         "--scope", "scope.json", "--base-sha", BASE, "--head-sha", HEAD],
        cwd=tmp_path, capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stdout + res.stderr
    assert json.loads(res.stdout)["status"] == "PASS"
    (tmp_path / "files.json").write_text(json.dumps(["docs/x.md", "docs/y.md"]), encoding="utf-8")
    res = subprocess.run(
        [sys.executable, "-I", str(ROOT / "scripts/pr_autonomy_policy.py"), "full-diff",
         "--meta", "meta.json", "--files", "files.json", "--patch", "p.patch",
         "--scope", "scope.json", "--base-sha", BASE, "--head-sha", HEAD],
        cwd=tmp_path, capture_output=True, text=True, check=False)
    assert res.returncode == 1 and "scope_mismatch_full_diff" in res.stdout


@pytest.mark.parametrize("meta", [
    {"title": "senza marker", "body": "TASK: k"},
    {"title": "senza marker", "branch": "x", "commit_messages": ["[TASK: k] fix"]},
    {"title": "senza marker", "latest_commit_message": "[TASK: k] fix"},
])
def test_cli_full_diff_risolve_il_task_come_il_guard(tmp_path, meta):
    (tmp_path / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (tmp_path / "files.json").write_text(json.dumps(["docs/x.md"]), encoding="utf-8")
    (tmp_path / "scope.json").write_text(json.dumps({"tasks": {"k": {"files": ["docs/x.md"]}}}),
                                         encoding="utf-8")
    (tmp_path / "p.patch").write_text("", encoding="utf-8")
    res = subprocess.run(
        [sys.executable, "-I", str(ROOT / "scripts/pr_autonomy_policy.py"), "full-diff",
         "--meta", "meta.json", "--files", "files.json", "--patch", "p.patch",
         "--scope", "scope.json", "--base-sha", BASE, "--head-sha", HEAD],
        cwd=tmp_path, capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stdout + res.stderr
    out = json.loads(res.stdout)
    assert out["status"] == "PASS" and out["task_key"] == "k"


def test_cli_dipendenza_dichiarata_nel_manifest_del_diff_passa(tmp_path):
    """Rilievo Grok #499: senza la scorciatoia sul manifest, una dipendenza
    DICHIARATA nel manifest del diff resta dichiarata (si legge il manifest
    dell'head), quindi PASS; solo l'import non dichiarato blocca."""
    files = ["core/x.py", "requirements.txt"]
    (tmp_path / "meta.json").write_text(json.dumps({"title": "[TASK: k] x"}), encoding="utf-8")
    (tmp_path / "files.json").write_text(json.dumps(files), encoding="utf-8")
    (tmp_path / "scope.json").write_text(json.dumps({"tasks": {"k": {"files": files}}}), encoding="utf-8")
    (tmp_path / "core").mkdir()
    (tmp_path / "core/x.py").write_text("import nuovo_pacchetto\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("requests\nnuovo-pacchetto>=1.0\n", encoding="utf-8")
    patch = ("+++ b/core/x.py\n+import nuovo_pacchetto\n"
             "+++ b/requirements.txt\n+nuovo-pacchetto>=1.0\n")
    (tmp_path / "p.patch").write_text(hunks(patch), encoding="utf-8")
    cmd = [sys.executable, "-I", str(ROOT / "scripts/pr_autonomy_policy.py"), "full-diff",
           "--meta", "meta.json", "--files", "files.json", "--patch", "p.patch",
           "--scope", "scope.json", "--base-sha", BASE, "--head-sha", HEAD]
    res = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True, check=False)
    out = json.loads(res.stdout)
    assert res.returncode == 0 and out["status"] == "PASS", res.stdout
    assert out["owner_manual_merge_required"] is True  # manifest → merge owner
    (tmp_path / "p.patch").write_text(hunks(patch + "+++ b/core/x.py\n+import non_dichiarato\n"), encoding="utf-8")
    res = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True, check=False)
    assert res.returncode == 1 and json.loads(res.stdout)["new_dependencies"] == ["non_dichiarato"]


def test_docs_dichiarano_il_contratto_e_il_principio_verbatim():
    spec = (ROOT / "docs/auto_pr_flow_spec.md").read_text(encoding="utf-8")
    for frase in (
        "L’agente non deve comportarsi come un esecutore che chiede conferma per ogni scelta tecnica.",
        "L’owner non deve essere coinvolto per risolvere problemi che il contratto già rende deterministici.",
    ):
        assert frase in spec
    for nome in policy.ORIGINS + policy.SEVERITIES + policy.BLOCKERS + policy.OWNER_STOP_CONDITIONS:
        assert nome in spec, nome
    for doc in ("AGENTS.md", "CLAUDE.md"):
        assert "scripts/pr_autonomy_policy.py" in (ROOT / doc).read_text(encoding="utf-8")
