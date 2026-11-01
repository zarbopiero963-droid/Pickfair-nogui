"""§0.9-bis — auto-merge certificato delle PR finanziarie critiche (owner 01/11/2026, #586).

PASS + BLOCK: ogni test fallisce se una PR finanziaria arriva all'auto-merge senza la certificazione rafforzata
provata sull'head, se un'area sempre a merge owner (credenziali, LIVE, protezioni fondamentali) passa in
automatico, o se la PR successiva parte senza la CI post-merge verde su main.
"""
from __future__ import annotations

import pytest

from scripts import pr_autonomy_policy as policy

BASE, HEAD = "a" * 40, "0123456789abcdef0123456789abcdef01234567"
SCOPE = ".guardrails/allowed_scope.json"


def _diff(files, patch=""):
    return policy.full_diff_check(base_sha=BASE, head_sha=HEAD, files=files, patch_text=patch,
                                  declared_files=files, local_modules=[], declared_dependencies=[])


def _cert(**extra):
    cert = {"head_sha": HEAD, "full_suite_pass": True, "mutation_gate_pass": True, "financial_tests_pass": True,
            "codex_no_blockers": True, "reviews_complete": True, "scope_verified": True,
            "known_financial_regression_unmanaged": False, "tests_skipped_or_disabled": False,
            "branch_protection_override": False}
    cert.update(extra)
    return cert


def _state(diff, **extra):
    safety = diff["classification"]["safety_critical"]
    state = {
        "evaluated_head": HEAD, "current_head": HEAD,
        "scope_valid": True, "acceptance_complete": True, "suite_pass": True, "hard_verify_pass": True,
        "codex_triaged": True, "fix_loop_valid": True, "dependencies_satisfied": True,
        "pertinent_owner_decision_open": False, "manual_stop": False, "manual_label_present": False,
        "preexisting_activated_or_aggravated": False, "unresolved_threads": 0, "introduced_p0_p1_open": 0,
        "reviewer_gate": "PASS", "merge_readiness": "PASS", "full_diff": diff, "fix_loop_exhausted": False,
        "safety_critical_declaration": {"head_sha": HEAD, "declared": True, "paths": safety or ["n/a"]},
        "financial_certification": _cert(),
    }
    state.update(extra)
    return state


FIN_FILES = ["cashout_executor.py", "core/runtime_controller.py", "tests/f05/test_x.py", SCOPE]


def test_pr_finanziaria_certificata_va_in_auto_merge():
    diff = _diff(FIN_FILES)
    assert diff["classification"]["financial_critical"] == ["cashout_executor.py", "core/runtime_controller.py"]
    assert diff["financial_owner_merge_required"] is False
    res = policy.merge_decision(_state(diff))
    assert res["status"] == "READY_FOR_AUTO_MERGE", res
    assert res["financial_critical"] == ["cashout_executor.py", "core/runtime_controller.py"]


@pytest.mark.parametrize("cert,reason", [
    (None, "missing"),
    ("certificata", "missing"),
    ({**{}, "head_sha": "f" * 40}, "stale_or_missing_head"),
])
def test_certificazione_assente_o_vecchia_va_a_merge_owner(cert, reason):
    res = policy.merge_decision(_state(_diff(FIN_FILES), financial_certification=cert))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert res["reasons"] == [f"financial_certification:{reason}"]


@pytest.mark.parametrize("key", policy.FINANCIAL_CERTIFICATION_TRUE)
@pytest.mark.parametrize("value", [False, None, "true", 1])
def test_ogni_prova_positiva_mancante_va_a_merge_owner(key, value):
    res = policy.merge_decision(_state(_diff(FIN_FILES), financial_certification=_cert(**{key: value})))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert f"financial_certification:{key}_not_proven" in res["reasons"]


@pytest.mark.parametrize("key", policy.FINANCIAL_CERTIFICATION_FALSE)
@pytest.mark.parametrize("value", [True, None, "false", 0])
def test_ogni_esclusione_non_provata_va_a_merge_owner(key, value):
    res = policy.merge_decision(_state(_diff(FIN_FILES), financial_certification=_cert(**{key: value})))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert f"financial_certification:{key}_not_excluded" in res["reasons"]


def test_deroga_di_review_su_pr_finanziaria_va_a_merge_owner():
    waiver = [{"valid": True, "head": HEAD, "reviewer": "grok", "reason": "timeout"}]
    res = policy.merge_decision(_state(_diff(FIN_FILES), reviewer_gate="PASS_WITH_OWNER_WAIVER",
                                       reviewer_waivers=waiver))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert "financial_certification:financial_review_waiver" in res["reasons"]


@pytest.mark.parametrize("path", [
    "core/safety_layer.py", "core/risk_gate.py", "core/loss_limits.py", "core/duplication_guard.py",
    "core/stake_precision.py", "circuit_breaker.py", "auto_throttle.py", "safe_mode.py", "market_validator.py",
    "ai/ai_guardrail.py", "core/sl_tp_pnl.py",                                       # protezioni fondamentali
    "betfair_client.py", "core/token_store.py", "core/secret_cipher.py", "telegram_sanitizer.py",
    "services/betfair_auth.py",                                                     # credenziali e auth
    "core/runtime_authority.py", "core/microstake_live_port.py", "core/execution_guard.py",
    "trading_config.py", "config_registry.py", "config.example.json", "scripts/live_gate.py",  # LIVE / config
    # Codex su 4fb12ff: credenziali e limiti di rischio fuori dall'elenco esplicito
    "services/setting_service.py", "services/settings_service.py", "core/system_state.py", "core/validators.py",
])
def test_aree_sempre_owner_anche_con_certificazione_completa(path):
    diff = _diff([path, SCOPE])
    assert diff["financial_owner_merge_required"] is True and path in diff["classification"]["financial_owner_only"]
    res = policy.merge_decision(_state(diff))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and res["reasons"] == ["financial_owner_only_area"]


@pytest.mark.parametrize("line", [
    '+    "live_enabled": true,', "+LIVE_ARMED = True", "+    simulation_mode = False",
    "+cfg['execution_mode'] = 'live'", "+    allow_live: 1",
    # Sol su 4fb12ff: il valore non conta, conta toccare l'interruttore (parentesi, piu' righe, indirezione)
    "+allow_live = (True)", "+allow_live = (", "+setattr(cfg, 'simulation_mode', flag)",
    "+cfg.update(live_enabled=x)", "+    broker.connect(simulation=False)", "+GO_LIVE = env_flag",
    # Codex su 4fb12ff: assegnazioni annotate e default di parametro annotati
    "+    live_enabled: bool = True", "+    simulation_mode: bool = False", '+    execution_mode: str = "LIVE"',
    "+def start(self, *, simulation_mode: bool = False) -> None:",
])
def test_riga_che_attiva_il_live_va_a_merge_owner(line):
    patch = f"diff --git a/order_manager.py b/order_manager.py\n--- a/order_manager.py\n+++ b/order_manager.py\n@@ -1,0 +1,1 @@\n{line}\n"
    diff = _diff(["order_manager.py", SCOPE], patch)
    assert diff["classification"]["live_activation"] == ["order_manager.py"], diff
    res = policy.merge_decision(_state(diff))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and res["reasons"] == ["financial_owner_only_area"]


def test_attivazione_live_in_test_o_commento_non_conta():
    patch = ("diff --git a/tests/x/test_live.py b/tests/x/test_live.py\n--- a/tests/x/test_live.py\n"
             "+++ b/tests/x/test_live.py\n@@ -1,0 +1,1 @@\n+simulation_mode = False\n"
             "diff --git a/order_manager.py b/order_manager.py\n--- a/order_manager.py\n+++ b/order_manager.py\n"
             "@@ -1,0 +1,2 @@\n+    # simulation_mode = False non va mai scritto qui\n"
             "+    sim = self.service.is_simulation_mode()   # leggere la modalita' non la cambia\n")
    diff = _diff(["tests/x/test_live.py", "order_manager.py", SCOPE], patch)
    assert diff["classification"]["live_activation"] == []
    assert policy.merge_decision(_state(diff))["status"] == "READY_FOR_AUTO_MERGE"


def test_solo_test_e_docs_finanziari_non_sono_pr_finanziaria():
    diff = _diff(["tests/f05/test_cashout.py", "docs/cashout_overview.md", SCOPE])
    assert diff["classification"]["financial_critical"] == []
    res = policy.merge_decision(_state(diff, financial_certification=None))
    assert res["status"] == "READY_FOR_AUTO_MERGE"


@pytest.mark.parametrize("path", ["core/async_db_writer.py", "services/telegram_service.py", "tick_dispatcher.py",
                                  "ui_panels/dashboard.py", "parsers/v2.py"])
def test_tutto_il_codice_di_prodotto_richiede_la_certificazione(path):
    # Codex su 4fb12ff: un elenco di moduli finanziari lascia fuori il writer del DB; fail-closed su tutto il prodotto
    diff = _diff([path, SCOPE])
    assert diff["classification"]["financial_critical"] == [path] and diff["financial_owner_merge_required"] is False
    res = policy.merge_decision(_state(diff, financial_certification=None))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and res["reasons"] == ["financial_certification:missing"]
    assert policy.merge_decision(_state(diff))["status"] == "READY_FOR_AUTO_MERGE"


def test_pr_solo_tooling_non_richiede_certificazione():
    diff = _diff(["scripts/ci_changed_modules.py", "tests/scripts/test_x.py", SCOPE])
    assert diff["classification"]["financial_critical"] == []
    res = policy.merge_decision(_state(diff, financial_certification=None))
    assert res["status"] == "READY_FOR_AUTO_MERGE"


def test_full_diff_senza_classificazione_finanziaria_e_fail_closed():
    diff = dict(_diff(FIN_FILES))
    diff.pop("financial_owner_merge_required")
    assert policy.merge_decision(_state(diff))["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    diff = _diff(FIN_FILES)
    diff["classification"] = {k: v for k, v in diff["classification"].items() if k != "financial_critical"}
    assert policy.merge_decision(_state(diff))["status"] == "NEEDS_MANUAL"


def test_file_di_governance_restano_owner_prima_della_certificazione():
    diff = _diff(["scripts/pr_autonomy_policy.py", "cashout_executor.py", SCOPE])
    res = policy.merge_decision(_state(diff))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and res["reasons"] == ["owner_manual_merge_files"]


def _next(**extra):
    state = {"merge_verified": True, "merge_sha": "a193909", "main_reread": True, "roadmap_reread": True,
             "open_prs_same_repo": 0, "next_card": "F-01", "dependencies_satisfied": True,
             "pertinent_open_owner_decisions": [], "active_stop_conditions": [], "main_post_merge_ci": "GREEN"}
    state.update(extra)
    return state


def test_pr_successiva_solo_con_ci_post_merge_verde_su_main():
    assert policy.next_pr_decision(_next())["status"] == "START_PHASE_0"
    assert policy.next_pr_decision(_next(main_post_merge_ci="RED"))["status"] == "STOP_MAIN_CI_RED"
    for value in ("PENDING", None, "green", True):
        assert policy.next_pr_decision(_next(main_post_merge_ci=value))["status"] == "WAIT_MAIN_CI_NOT_VERIFIED"
    state = _next()
    del state["main_post_merge_ci"]
    assert policy.next_pr_decision(state)["status"] == "WAIT_MAIN_CI_NOT_VERIFIED"


def test_test_e_docs_delle_aree_owner_non_le_rendono_owner():
    diff = _diff(["tests/core/test_safety_layer.py", "docs/sl_tp.md", SCOPE])
    assert diff["classification"]["financial_owner_only"] == [] and diff["financial_owner_merge_required"] is False


@pytest.mark.parametrize("path", ["guardrails/mutation_catalog.json", "scripts/run_mutation_guardrails.py",
                                  ".github/workflows/x.yml", "tools/combi/generator.py"])
def test_tooling_con_parole_chiave_non_diventa_area_owner_finanziaria(path):
    assert policy.classify_paths([path])["financial_owner_only"] == []
