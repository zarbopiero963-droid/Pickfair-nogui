"""Offline regression checks for the real operational docs and issue mirrors.

These do not attest GitHub synchronization: READY also requires live read-back.
Mutation tests remove/replace requirements and must reject the altered contract.
"""
import copy
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs/mcp_operational_contract.md"
MIRRORS = ROOT / "docs/governance/mcp_issue_sections.json"
ENTRY = "#491 → #489 → #461 → #426 → #453 → #351 quando serve"


def check_contract(text, policies, sections):
    errors = []

    def require(document, phrase, label):
        if phrase not in document:
            errors.append(label)

    require(text, ENTRY, "entrypoint")
    for policy in policies:
        require(policy, ENTRY, "policy_entrypoint")
        require(policy, "per repository", "policy_seriality")
        require(policy, "#426/P41", "reviewer_precedence")
        require(policy, "docs/mcp_operational_contract.md", "policy_contract_link")
    if re.search(r"una (?:sola )?PR globale|one global (?:open )?PR", "\n".join([text, *policies]), re.I):
        errors.append("global_seriality")
    for phrase in ["PR26-a → PR26 authority → PR27 → PR28 → PR29",
                   "Owner PR primaria", "reload_config | PR28",
                   "P35 lifecycle | PR25", "AMBIGUOUS cashout | PR45",
                   "### A. MCP SIM", "### B. MCP LIVE",
                   "### C. CHIUSURA TOTALE PICKFAIR", "MCP-06 LIVE non serve",
                   "MANUAL via MCP = DECISIONE ANCORA APERTA",
                   "Telegram disconnected NON blocca LIVE",
                   "UPDATER_SCOPE = OWNER_OPEN", "PR58 → PR59 totale → PR60",
                   "PAUSE:", "STOP/RISK_STOP:", "EMERGENCY:", "RESUME:",
                   "prompt injection", "Network allowlist", "nessun replay automatico",
                   "NON apre", "neppure in sola lettura", "request_id", "operation_id",
                   "PF-API-0 → PF-API-1 → PF-API commands",
                   "MCP-01 → MCP-02 → MCP-03 → MCP-04 → MCP-05 → MCP-06 → MCP-07"]:
        require(text, phrase, phrase)
    if not re.search(r"^\| reload_config \| PR28 \|", text, re.M):
        errors.append("reload_config_single_owner")
    if set(sections) != {"1", "351", "426", "453", "461", "489", "491"}:
        errors.append("issue_set")
    for number, body in sections.items():
        require(body, ENTRY, f"issue_{number}_entrypoint")
        require(body, f"<!-- MCP-OPERATIVE-{number}-BEGIN -->", "managed_begin")
        require(body, f"<!-- MCP-OPERATIVE-{number}-END -->", "managed_end")
    master = sections.get("1", "")
    for phrase in ["PR23 = Formula MM", "PR24 = SL/TP", "PR25 = Trailing persistente",
                   "PR45 = AMBIGUOUS cashout", "26 difetti confermati",
                   "PR33 EVIDENZA INSUFFICIENTE / BLOCKED_ENV",
                   "0 GIÀ CORRETTO CON PROVA", "NON apre il DB Pickfair, neppure in sola lettura",
                   "NON implementa master/copy/follow/fan-out",
                   "PF-API-0 → PF-API-1 → PF-API commands",
                   "MCP-01 → MCP-02 → MCP-03 → MCP-04 → MCP-05 → MCP-06 → MCP-07",
                   "MANUAL via MCP = DECISIONE ANCORA APERTA"]:
        require(master, phrase, f"master_{phrase}")
    # A correct sentence must not mask a contradictory current instruction.
    current = "\n".join([text, *sections.values()])
    contradictions = [
        r"PR23 = SL/TP", r"PR24 = Trailing", r"PR25 = SL/TP",
        r"MCP-PR01[–…-]0?5", r"MANUAL via MCP = (?:DECISA|DECISO)",
        r"Telegram disconnected blocca LIVE", r"(?:PUT|POST) /v1/config",
        r"(?:può|puo) leggere il DB Pickfair",
        r"(?<!NON )implementa master/copy/follow/fan-out",
        r"(?:entrypoint|partire da) #452",
    ]
    for pattern in contradictions:
        if re.search(pattern, current, re.I):
            errors.append("contradictory_current_instruction")
    # Parse the actual current route table, not prose that merely mentions a route.
    rows = re.findall(r"^\| (Read|Command) \| (GET|POST|PATCH) \| ([^|]+) \|", master, re.M)
    expected_read = {"/v1/meta", "/v1/status", "/v1/account", "/v1/exposure",
                     "/v1/config", "/v1/events", "/v1/markets",
                     "/v1/markets/{id}/book", "/v1/orders", "/v1/positions",
                     "/v1/operations/{id}", "/v1/operations?status=pending",
                     "/v1/reconciliation/status", "/v1/audit/{correlation_id}"}
    expected_commands = {(method, route) for method, route in [
        ("POST", "/v1/preview"), ("POST", "/v1/orders"), ("POST", "/v1/dutching"),
        ("POST", "/v1/orders/{id}/cancel"), ("POST", "/v1/orders/{id}/replace"),
        ("POST", "/v1/cashout"), ("POST", "/v1/stop"), ("POST", "/v1/resume"),
        ("POST", "/v1/reconcile"), ("PATCH", "/v1/config"), ("POST", "/v1/mode")]}
    actual_reads = {route.strip() for kind, method, route in rows if kind == "Read" and method == "GET"}
    actual_commands = {(method, route.strip()) for kind, method, route in rows if kind == "Command"}
    if actual_reads != expected_read or actual_commands != expected_commands or len(rows) != 25:
        errors.append("unique_api_table")
    state = sections.get("461", "")
    for phrase in ["### STATO CORRENTE", "### REGOLE VIGENTI", "#490/#492/#493 MERGIATE",
                   "PR05 NON è la prossima attività automatica", "PR26-a customer_ref/provenance",
                   "Rileggere main e PR aperte"]:
        require(state, phrase, f"state_{phrase}")
    if re.search(r"#(?:490|492|493) (?:APERTA|aperta)|PR05 successiva", state):
        errors.append("old_operational_state")
    for number in ["1", "351", "491"]:
        body = sections.get(number, "")
        for phrase in ["### A. MCP SIM", "### B. MCP LIVE", "### C. CHIUSURA TOTALE PICKFAIR",
                       "Telegram disconnected NON blocca LIVE", "Prompt injection",
                       "Network allowlist", "nessun replay automatico"]:
            require(body, phrase, f"issue_{number}_{phrase}")
    return errors


def inputs():
    return (CONTRACT.read_text(), [(ROOT / p).read_text() for p in ["AGENTS.md", "CLAUDE.md"]],
            json.loads(MIRRORS.read_text())["sections"])


def test_real_operational_documents_and_issue_sections():
    assert check_contract(*inputs()) == []


@pytest.mark.parametrize("old,new", [
    (ENTRY, "#461 → #426"),
    ("PR26-a → PR26 authority → PR27 → PR28 → PR29", "PR27 → PR26 → PR29/28"),
    ("PF-API-0 → PF-API-1 → PF-API commands", "PF-API-1 → PF-API-2"),
    ("MCP-01 → MCP-02 → MCP-03 → MCP-04 → MCP-05 → MCP-06 → MCP-07", "MCP-PR01…05"),
    ("MANUAL via MCP = DECISIONE ANCORA APERTA", "MANUAL via MCP = DECISA"),
    ("Telegram disconnected NON blocca LIVE", "Telegram disconnected blocca LIVE"),
    ("### A. MCP SIM", "### Traguardo unico"),
    ("### B. MCP LIVE", "### Traguardo unico"),
    ("### C. CHIUSURA TOTALE PICKFAIR", "### Traguardo unico"),
    ("UPDATER_SCOPE = OWNER_OPEN", "UPDATER_SCOPE = RINVIATO"),
    ("reload_config | PR28", "reload_config | PR28/PR29"),
    ("P35 lifecycle | PR25", "P35 lifecycle | PR45"),
])
def test_contract_regressions_are_rejected(old, new):
    text, policies, sections = inputs()
    assert old in text
    assert check_contract(text.replace(old, new), policies, sections)


@pytest.mark.parametrize("old,new", [
    ("PR23 = Formula MM", "PR23 = SL/TP"),
    ("PR24 = SL/TP", "PR24 = Trailing"),
    ("PR25 = Trailing persistente", "PR25 = SL/TP"),
    ("NON apre il DB Pickfair, neppure in sola lettura", "Può leggere il DB Pickfair"),
    ("NON implementa master/copy/follow/fan-out", "Implementa master/copy/follow/fan-out"),
    ("| Command | PATCH | /v1/config |", "| Command | POST | /v1/config |"),
    ("| Command | POST | /v1/cashout |", "| Command | POST | /v1/cashout/all |"),
    ("PF-API-0 → PF-API-1 → PF-API commands", "PF-API-1 → PF-API-2"),
    ("MCP-01 → MCP-02 → MCP-03 → MCP-04 → MCP-05 → MCP-06 → MCP-07", "MCP-PR01…05"),
])
def test_master_regressions_are_rejected(old, new):
    text, policies, sections = inputs()
    sections = copy.deepcopy(sections)
    assert old in sections["1"]
    sections["1"] = sections["1"].replace(old, new)
    assert check_contract(text, policies, sections)


def test_old_issue461_state_is_rejected():
    text, policies, sections = inputs()
    sections["461"] += "\n#490 APERTA; PR05 successiva; PR29/28\n"
    assert "old_operational_state" in check_contract(text, policies, sections)


def test_global_pr_limit_is_rejected():
    text, policies, sections = inputs()
    assert "global_seriality" in check_contract(text + "\nuna sola PR globale\n", policies, sections)


@pytest.mark.parametrize("policy_index", [0, 1])
def test_policy_entrypoint_removal_is_rejected(policy_index):
    text, policies, sections = inputs()
    policies[policy_index] = policies[policy_index].replace(ENTRY, "#461 → #426")
    assert "policy_entrypoint" in check_contract(text, policies, sections)


@pytest.mark.parametrize("regression", [
    "PR23 = SL/TP", "PR24 = Trailing", "PR25 = SL/TP", "MCP-PR01…05",
    "MANUAL via MCP = DECISA", "Telegram disconnected blocca LIVE",
    "POST /v1/config", "Può leggere il DB Pickfair",
    "implementa master/copy/follow/fan-out", "entrypoint #452",
])
def test_added_contradictions_cannot_hide_behind_valid_text(regression):
    text, policies, sections = inputs()
    sections["1"] += "\n" + regression
    assert "contradictory_current_instruction" in check_contract(text, policies, sections)


@pytest.mark.parametrize("policy_index", [0, 1])
def test_global_rule_in_either_policy_is_rejected(policy_index):
    text, policies, sections = inputs()
    policies[policy_index] += "\nuna sola PR globale\n"
    assert "global_seriality" in check_contract(text, policies, sections)
