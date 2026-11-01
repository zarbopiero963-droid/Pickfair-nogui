"""§0.9-ter — delega owner temporanea di merge per le schede della roadmap V2 (owner 01/11/2026, #586).

PASS + BLOCK: la delega porta all'auto-merge SOLO una PR della roadmap le cui aree sempre-owner sono tutte protezioni
finanziarie fondamentali, con certificazione rafforzata completa sull'head; credenziali, segreti, config, attivazione
LIVE, governance e ogni gate mancante restano a merge owner o NEEDS_MANUAL. La delega vive nel codice della policy
(merge owner), non in un commento: ogni test fallisce se una di queste regole si rompe.
"""
from __future__ import annotations

import pytest

from scripts import pr_autonomy_policy as policy
from tests.scripts.test_certified_financial_automerge import HEAD, SCOPE, _cert, _diff, _state

DELEGATION = "PF-ROADMAP-V2-MERGE-DELEGATION"


def _deleg(**extra):
    d = {"id": DELEGATION, "card": "F-09", "head_sha": HEAD}
    d.update(extra)
    return d


PROTECTION = ["core/sl_tp_pnl.py", "core/risk_gate.py", "core/duplication_guard.py", "core/money_management.py",
              "core/system_state.py", "stake_mode.py", "market_validator.py", "ai/ai_guardrail.py"]


@pytest.mark.parametrize("path", PROTECTION)
def test_delega_porta_in_auto_merge_una_protezione_fondamentale_certificata(path):
    diff = _diff([path, SCOPE])
    assert diff["financial_owner_merge_required"] is True and diff["financial_owner_delegation_eligible"] is True
    res = policy.merge_decision(_state(diff, owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_AUTO_MERGE", res
    assert res["owner_delegation"] == {"id": DELEGATION, "card": "F-09"}


@pytest.mark.parametrize("path", PROTECTION)
def test_senza_delega_la_protezione_resta_owner(path):
    res = policy.merge_decision(_state(_diff([path, SCOPE])))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and res["reasons"] == ["financial_owner_only_area"]


@pytest.mark.parametrize("path", [
    "betfair_client.py", "core/token_store.py", "core/secret_cipher.py", "core/redaction.py", "telegram_sanitizer.py",
    "services/betfair_auth.py", "core/session_manager.py",                         # credenziali, auth, segreti
    "core/runtime_authority.py", "core/microstake_live_port.py", "core/execution_guard.py", "core/bridge_mode.py",
    "core/config_store.py", "trading_config.py", "config_registry.py", "config.example.json",
    "scripts/live_gate.py",                                                        # attivazione LIVE / config
    "services/setting_service.py", "core/live_risk_guard.py",                      # rete per parola chiave
])
def test_credenziali_segreti_config_e_live_non_sono_mai_delegabili(path):
    diff = _diff([path, SCOPE])
    assert diff["financial_owner_delegation_eligible"] is False and path in \
        diff["classification"]["financial_owner_non_delegable"], diff["classification"]
    res = policy.merge_decision(_state(diff, owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert "owner_delegation:area_not_delegable" in res["reasons"]


def test_una_sola_area_non_delegabile_blocca_tutta_la_pr():
    diff = _diff(["core/risk_gate.py", "betfair_client.py", SCOPE])
    res = policy.merge_decision(_state(diff, owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and "owner_delegation:area_not_delegable" in res["reasons"]


def test_riga_che_attiva_il_live_non_e_delegabile_anche_in_una_protezione():
    patch = ("diff --git a/core/risk_gate.py b/core/risk_gate.py\n--- a/core/risk_gate.py\n+++ b/core/risk_gate.py\n"
             "@@ -1,0 +1,1 @@\n+    self.set_simulation_mode(False)\n")
    diff = _diff(["core/risk_gate.py", SCOPE], patch)
    assert diff["classification"]["live_activation"] == ["core/risk_gate.py"]
    res = policy.merge_decision(_state(diff, owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and "owner_delegation:area_not_delegable" in res["reasons"]


def test_file_di_governance_restano_owner_anche_con_delega():
    diff = _diff(["core/risk_gate.py", "scripts/pr_autonomy_policy.py", SCOPE])
    res = policy.merge_decision(_state(diff, owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and "owner_manual_merge_files" in res["reasons"]


@pytest.mark.parametrize("requested,reason", [
    ("PF-ROADMAP-V2-MERGE-DELEGATION", "malformed"),
    ({"id": "PF-ALTRA-DELEGA", "card": "F-09", "head_sha": HEAD}, "unknown_delegation"),
    ({"card": "F-09", "head_sha": HEAD}, "unknown_delegation"),
    (_deleg(card="F-34"), "card_not_covered"),
    (_deleg(card="F-9"), "card_not_covered"),
    (_deleg(card=None), "card_not_covered"),
    (_deleg(head_sha="f" * 40), "stale_or_missing_head"),
    (_deleg(head_sha=None), "stale_or_missing_head"),
])
def test_delega_malformata_sconosciuta_fuori_scheda_o_vecchia_va_a_merge_owner(requested, reason):
    res = policy.merge_decision(_state(_diff(["core/risk_gate.py", SCOPE]), owner_delegation=requested))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert f"owner_delegation:{reason}" in res["reasons"], res


def test_delega_disattivata_dall_owner_non_vale(monkeypatch):
    record = dict(policy.OWNER_MERGE_DELEGATIONS[DELEGATION], active=False)
    monkeypatch.setitem(policy.OWNER_MERGE_DELEGATIONS, DELEGATION, record)
    res = policy.merge_decision(_state(_diff(["core/risk_gate.py", SCOPE]), owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE" and "owner_delegation:inactive" in res["reasons"]


@pytest.mark.parametrize("key", policy.FINANCIAL_CERTIFICATION_TRUE)
def test_delega_non_toglie_la_certificazione_rafforzata(key):
    res = policy.merge_decision(_state(_diff(["core/risk_gate.py", SCOPE]), owner_delegation=_deleg(),
                                       financial_certification=_cert(**{key: False})))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert f"financial_certification:{key}_not_proven" in res["reasons"]


@pytest.mark.parametrize("key", policy.FINANCIAL_CERTIFICATION_FALSE)
def test_delega_non_toglie_le_esclusioni_della_certificazione(key):
    res = policy.merge_decision(_state(_diff(["core/risk_gate.py", SCOPE]), owner_delegation=_deleg(),
                                       financial_certification=_cert(**{key: True})))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert f"financial_certification:{key}_not_excluded" in res["reasons"]


def test_delega_non_vale_con_una_deroga_di_review():
    waiver = [{"valid": True, "head": HEAD, "reviewer": "grok", "reason": "timeout"}]
    res = policy.merge_decision(_state(_diff(["core/risk_gate.py", SCOPE]), owner_delegation=_deleg(),
                                       reviewer_gate="PASS_WITH_OWNER_WAIVER", reviewer_waivers=waiver))
    assert res["status"] == "READY_FOR_OWNER_MANUAL_MERGE"
    assert "financial_certification:financial_review_waiver" in res["reasons"]


@pytest.mark.parametrize("extra,expected", [
    ({"unresolved_threads": 1}, "BLOCKED"), ({"introduced_p0_p1_open": 1}, "BLOCKED"),
    ({"merge_readiness": "BLOCKED"}, "BLOCKED"), ({"reviewer_gate": "BLOCKERS"}, "BLOCKED"),
    ({"suite_pass": False}, "BLOCKED"), ({"pertinent_owner_decision_open": True}, "BLOCKED"),
    ({"fix_loop_exhausted": True}, "NEEDS_MANUAL"), ({"merge_readiness": "UNKNOWN"}, "NEEDS_MANUAL"),
    ({"manual_label_present": True}, "READY_FOR_OWNER_MANUAL_MERGE"),
])
def test_delega_non_scavalca_nessun_gate_di_merge(extra, expected):
    res = policy.merge_decision(_state(_diff(["core/risk_gate.py", SCOPE]), owner_delegation=_deleg(), **extra))
    assert res["status"] == expected, res


def test_pr_senza_aree_owner_non_usa_la_delega():
    # la certificazione 0.9-bis basta gia': il verdetto non dichiara una delega che non serviva
    res = policy.merge_decision(_state(_diff(["core/runtime_controller.py", SCOPE]), owner_delegation=_deleg()))
    assert res["status"] == "READY_FOR_AUTO_MERGE" and "owner_delegation" not in res


def test_registro_della_delega_copre_esattamente_le_33_schede():
    record = policy.OWNER_MERGE_DELEGATIONS[DELEGATION]
    assert record["issue"] == 586 and record["active"] is True
    assert record["cards"] == tuple(f"F-{n:02d}" for n in range(1, 34))


def test_il_registro_delle_deleghe_e_a_merge_owner():
    # il registro sta nel codice della policy: modificarlo e' sempre una PR di governance a merge owner
    assert policy.classify_paths(["scripts/pr_autonomy_policy.py"])["owner_manual"] == ["scripts/pr_autonomy_policy.py"]
