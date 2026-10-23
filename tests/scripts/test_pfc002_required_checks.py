"""PFC-002: Merge Readiness esige SUCCESS sui check obbligatori.

RED: senza manifest (il comportamento di main) un check obbligatorio SKIPPED o
assente lascia `can_merge=True` — skipped era un OK_STATE per tutti. GREEN: con
il manifest solo SUCCESS sullo head conta; SKIPPED, NEUTRAL, assente, su un altro
SHA, CANCELLED, TIMED_OUT e FAILURE bloccano. Per i check NON obbligatori
SKIPPED resta accettabile: le lane di modulo si saltano da sole.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import pr_flow_automation as flow  # noqa: E402

HEAD = "d" * 40
REQ = ("full-suite", "mutation-guardrails / mutation", "GPT-6.1 Sol push-range review",
       "Grok 4.7 push-range review")


def _check(name, conclusion="SUCCESS", status="COMPLETED", sha=None):
    c = {"__typename": "CheckRun", "name": name, "status": status, "conclusion": conclusion,
         "detailsUrl": f"https://github.com/o/r/actions/runs/1/job/{abs(hash(name)) % 1000}"}
    if sha:
        c["checkSuite"] = {"headSha": sha}
    return c


def _pr(checks):
    return {"state": "OPEN", "isDraft": False, "mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN",
            "headRefOid": HEAD, "statusCheckRollup": checks}


def _tutti_verdi(**sostituzioni):
    checks = [_check(n) for n in REQ if n not in sostituzioni]
    checks += [c for c in sostituzioni.values() if c is not None]
    checks.append(_check("dutching / ultra-check", conclusion="SKIPPED"))  # lane non toccata: ok
    return checks


def _decidi(monkeypatch, checks, required):
    monkeypatch.setattr(flow, "pr_view", lambda *_a: _pr(checks))
    return flow.build_decision("o/r", "1", review_threads=[], required_checks=required)


def test_red_senza_manifest_un_obbligatorio_skipped_passa(monkeypatch):
    checks = _tutti_verdi(**{"full-suite": _check("full-suite", conclusion="SKIPPED")})
    assert _decidi(monkeypatch, checks, ())["can_merge"] is True  # il falso verde di prima


def test_green_tutti_success_si_puo_mergiare(monkeypatch):
    d = _decidi(monkeypatch, _tutti_verdi(), REQ)
    assert d["can_merge"] is True, d["reasons"]


@pytest.mark.parametrize("stato", ["SKIPPED", "NEUTRAL"])
@pytest.mark.parametrize("nome", REQ)
def test_obbligatorio_skipped_o_neutral_blocca(monkeypatch, nome, stato):
    d = _decidi(monkeypatch, _tutti_verdi(**{nome: _check(nome, conclusion=stato)}), REQ)
    assert d["can_merge"] is False
    assert any(b.get("required") and b["name"] == nome for b in d["blockers"])


@pytest.mark.parametrize("stato", ["FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE"])
def test_obbligatorio_rosso_blocca(monkeypatch, stato):
    d = _decidi(monkeypatch, _tutti_verdi(**{"full-suite": _check("full-suite", conclusion=stato)}), REQ)
    assert d["can_merge"] is False and any(b["name"] == "full-suite" for b in d["blockers"])


@pytest.mark.parametrize("nome", REQ)
def test_obbligatorio_assente_resta_in_attesa_e_non_si_mergia(monkeypatch, nome):
    d = _decidi(monkeypatch, _tutti_verdi(**{nome: None}), REQ)
    assert d["can_merge"] is False
    assert any(p["name"] == nome and p["state"] == "MISSING" for p in d["pending"])


def test_obbligatorio_su_un_altro_sha_blocca(monkeypatch):
    vecchio = _check("full-suite", sha="e" * 40)
    d = _decidi(monkeypatch, _tutti_verdi(**{"full-suite": vecchio}), REQ)
    assert d["can_merge"] is False
    assert any("head is" in b.get("reason", "") for b in d["blockers"])


def test_due_esecuzioni_dello_stesso_obbligatorio_una_skipped_blocca(monkeypatch):
    checks = _tutti_verdi() + [_check("full-suite", conclusion="SKIPPED")]
    assert _decidi(monkeypatch, checks, REQ)["can_merge"] is False


def test_un_workflow_alternativo_con_nome_diverso_non_sostituisce_l_obbligatorio(monkeypatch):
    checks = _tutti_verdi(**{"full-suite": _check("full-suite-alt")})
    assert _decidi(monkeypatch, checks, REQ)["can_merge"] is False


def test_manifest_del_repository_letto_e_coerente():
    assert flow.load_required_checks() == REQ


@pytest.mark.parametrize("contenuto", ["", "{}", '{"required_checks": []}', '{"required_checks": [""]}', "[1]"])
def test_manifest_malformato_rifiutato(tmp_path, contenuto):
    p = tmp_path / "m.json"
    p.write_text(contenuto)
    with pytest.raises((ValueError, json.JSONDecodeError)):
        flow.load_required_checks(p)


def test_gate_con_manifest_illeggibile_non_e_mai_verde(monkeypatch):
    def rotto(*_a, **_k):
        raise ValueError("manifest rotto")

    monkeypatch.setattr(flow, "load_required_checks", rotto)
    monkeypatch.setattr(flow, "fetch_all_review_threads", lambda *_a: [])
    monkeypatch.setattr(flow, "pr_view", lambda *_a: _pr(_tutti_verdi()))
    d = flow._readiness_decision("o/r", "1", True)
    assert d["can_merge"] is False
    assert any("manifest unreadable" in p["name"] for p in d["pending"])


# D3 (audit indipendente su #539): il nome non basta, conta la provenienza.
def test_d3_status_omonimo_non_sostituisce_il_check_obbligatorio(monkeypatch):
    # check reale assente; uno StatusContext con lo stesso nome (basta un token con statuses:write)
    status = {"__typename": "StatusContext", "context": "full-suite", "state": "SUCCESS"}
    d = _decidi(monkeypatch, _tutti_verdi(**{"full-suite": status}), REQ)
    assert d["can_merge"] is False
    assert any(b.get("name") == "full-suite" and b.get("required") for b in d["blockers"])


def test_d3_check_senza_tipo_dichiarato_non_conta_come_obbligatorio(monkeypatch):
    anonimo = {"name": "full-suite", "status": "COMPLETED", "conclusion": "SUCCESS"}
    d = _decidi(monkeypatch, _tutti_verdi(**{"full-suite": anonimo}), REQ)
    assert d["can_merge"] is False
