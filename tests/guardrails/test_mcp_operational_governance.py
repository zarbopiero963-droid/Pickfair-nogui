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


def normalize(document):
    """Markdown fuori: link -> testo, niente grassetto/codice. Le righe restano."""
    document = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", document)
    return document.replace("**", "").replace("`", "")


def flat(document):
    """Testo normalizzato su una riga: una clausola a capo resta la stessa clausola."""
    return re.sub(r"\s+", " ", normalize(document))


# Una negazione vale solo se precede direttamente l'istruzione: al massimo un
# verbo di divieto e un articolo. «non solo X» o una negazione lontana non bastano.
NEGATION = re.compile(
    r"\b(?:non|vietato|vietata|nessun|nessuna|mai)"
    r"(?:\s+(?:usare|eseguire|creare|introdurre|implementare|esporre|aggiungere))?"
    r"(?:\s+(?:un|una|uno|il|la|lo|i|le|gli))?\s*$", re.I)

ENTRY_SENTENCE = "partire obbligatoriamente da Pickfair-nogui #491"
POLICY_ENTRY_SENTENCE = "partire da Pickfair-nogui #491"
REVIEWER_CLAUSES = ["Per reviewer/merge applicare #426/P41",
                    "non riattivare workflow o label sospese"]
NO_BYPASS = ["NON importa BetfairClient, TradingEngine o OrderManager",
             "NON apre il DB Pickfair, neppure in sola lettura",
             "NON usa credenziali Betfair",
             "NON parla direttamente con Betfair",
             "NON duplica formule MM, stake calculation, cashout o reconciliation",
             "NON implementa master/copy/follow"]
STOP_SEMANTICS = [
    "PAUSE: blocca nuovi ordini; NON cancella unmatched; NON avvia cashout.",
    "STOP/RISK_STOP: blocca nuovi ordini; cancella unmatched pertinenti; "
    "tenta cashout delle posizioni pertinenti via authority Pickfair.",
    "EMERGENCY: barriera massima immediata, nessuna eccezione generica.",
    "RESUME: soltanto dopo nuova readiness completa."]
STOP_SECTIONS = {"1", "351", "426", "489", "491"}
STATE_PHRASES = ["#490/#492/#493 MERGIATE", "zero PR aperte al preflight",
                 "PR05 NON è la prossima attività automatica",
                 "PR26-a customer_ref/provenance"]
RESULT_STATES = "PASS/FAIL/BLOCKED_ENV/NOT_RUN/RINVIATO separati"
RESULT_SECTIONS = {"1", "351", "491"}
SIMLIVE_CORE = [
    "SIM usa la vera Betfair Delayed App Key",
    "Esecuzione finale: SimulationBroker. Nessun ordine reale deve essere inviato a Betfair.",
    "LIVE usa la vera Betfair Live App Key",
    "Gli ordini vengono inviati a Betfair soltanto se tutti i gate LIVE sono soddisfatti.",
    "stesso Money Management", "stessi Risk/Safety gate",
    "Delayed App Key NON significa dati fittizi",
    "SIM NON significa mockare il mondo esterno",
    "Live App Key NON deve essere usata in SIM",
    "Delayed App Key NON deve essere usata per piazzare ordini reali",
    "LIVE richiede Live App Key e readiness LIVE completa",
    "non dedurre l'execution mode dalla sola App Key",
    "SIM = saldo/bankroll fittizio", "LIVE = saldo reale Betfair",
    "SIM/LIVE PARITY", "MCP: un solo set di tool", "MCP non implementa due motori",
    "MCP non decide autonomamente di andare LIVE", "LIVE non pronto → BLOCKED",
    "nessun passaggio implicito SIM→LIVE",
    "peak SIM non contamina drawdown LIVE", "balance snapshot mode-scoped",
    "SIM→LIVE non trasforma il bankroll SIM nel riferimento di rischio LIVE",
    "La futura PR15 deve implementare/testare questo contratto"]
SIMLIVE_CORE_SECTIONS = {"1", "351", "426", "461", "491"}
SIMLIVE_POINTER = ["SIM = Delayed App Key reale + dati Betfair reali delayed + SimulationBroker + bankroll fittizio",
                   "LIVE = Live App Key + dati reali + Betfair reale + saldo reale, solo con tutti i gate LIVE",
                   "un solo set di tool MCP", "nessun passaggio implicito SIM→LIVE",
                   "PR15: peak SIM non contamina drawdown LIVE"]
SIMLIVE_MATRIX = [
    "| BACK | simulato | reale | stesso percorso prima del broker |",
    "| LAY | simulato | reale | stesso percorso prima del broker |",
    "| cashout | simulato | reale | stesso calcolo/intento |",
    "| reconciliation | SIM | Betfair | stesso contratto di stato |",
    "| AMBIGUOUS | simulato/testabile | reale | stesso lifecycle |",
    "SIM non può raggiungere Betfair order transport",
    "SIM non può utilizzare Live App Key per piazzare ordini",
    "LIVE non può usare SimulationBroker per fingere successo",
    "stessa richiesta non può produrre prima ordine SIM e poi ordine LIVE per replay/retry",
    "Gate di certificazione SIM/LIVE — stato NOT_RUN finché eseguiti con prova sullo SHA",
    "crash/restart; concurrency; SQLite reale; SimulationBroker reale; "
    "Betfair reale/LIVE quando autorizzato; whole-wiring cross-repo"]
SIMLIVE_MATRIX_SECTIONS = {"1", "351"}
# Istruzioni correnti che ribalterebbero una clausola vigente. Valgono ovunque
# (contratto, policy, ogni sezione issue) e cedono solo a una negazione diretta.
REVERSALS = [
    # entrypoint diverso da #491, anche formattato
    r"partire\s+(?:obbligatoriamente\s+)?da\s+(?:Pickfair-nogui\s+)?#(?!491\b)\d+",
    # no-bypass
    r"importa\s+(?:BetfairClient|TradingEngine|OrderManager)",
    r"usa\s+(?:le\s+)?credenziali\s+Betfair",
    r"parla\s+direttamente\s+con\s+Betfair",
    r"duplica\s+(?:le\s+)?(?:formule|MM\b|stake|cashout|reconcil)",
    r"(?:apre|legge|accede\s+al)\s+(?:il\s+)?DB\s+Pickfair",
    # serialità globale, anche con «attiva»/«aperta»
    r"una\s+(?:sola\s+)?PR\s+(?:attiva\s+|aperta\s+)?globale",
    r"PR\s+(?:attiva|aperta)\s+globale", r"serialità\s+globale",
    r"one\s+global\s+(?:open\s+|active\s+)?PR",
    # reviewer/merge
    r"#426/P(?!41\b)\d+", r"riattivare\s+(?:i\s+)?workflow",
    # updater
    r"UPDATER_SCOPE\s*=\s*(?!OWNER_OPEN\b)[A-Z_]+",
    # route cashout alternative o per modalità
    r"/v1/cashout/[\w{]", r"/v1/cashout-\w", r"/v1/(?:sim|live)/",
    # PAUSE / STOP / EMERGENCY / RESUME
    r"(?:PAUSE|STOP/RISK_STOP|RISK_STOP|STOP|EMERGENCY)\s*:?\s+(?:consente|permette|ammette)\s+nuovi\s+ordini",
    r"RESUME\s*:?\s+(?:senza|immediat|subito|automatic)",
    r"EMERGENCY\s*:?\s+(?:con\s+eccezioni|ammette\s+eccezioni)",
    # stati di esito distinti
    r"(?:PASS|FAIL|BLOCKED_ENV|NOT_RUN|RINVIATO)\s*(?:e|ed|/|=)\s*(?:PASS|FAIL|BLOCKED_ENV|NOT_RUN|RINVIATO)\s+(?:equivalenti|uguali|identici|coincidono)",
    r"(?:NOT_RUN|BLOCKED_ENV|RINVIATO)\s+(?:=|vale\s+come|equivale\s+a|conta\s+come)\s+PASS",
    # SIM/LIVE
    r"SIM\s+(?:è|e'|=)\s+(?:un\s+)?mock", r"SIM\s+(?:significa|=)\s+mockare",
    r"SIM\s+(?:usa|con)\s+(?:dati|quote|mercati)\s+(?:fittizi|mock)",
    r"Delayed\s+App\s+Key\s+(?:significa|=)\s+dati\s+fittizi",
    r"SIM\s+(?:senza|non\s+usa)\s+(?:la\s+)?(?:vera\s+)?(?:Betfair\s+)?Delayed\s+App\s+Key",
    r"LIVE\s+(?:senza|non\s+richiede)\s+(?:la\s+)?(?:vera\s+)?(?:Betfair\s+)?Live\s+App\s+Key",
    r"Live\s+App\s+Key\s+(?:deve\s+essere\s+|può\s+essere\s+)?usata\s+in\s+SIM",
    r"SIM\s+(?:può|puo|deve)\s+(?:inviare|piazzare|raggiungere)\s+(?:ordini|Betfair)",
    r"LIVE\s*=\s*saldo(?:/bankroll)?\s+fittizio", r"SIM\s*=\s*saldo(?:/bankroll)?\s+reale",
    r"LIVE\s+con\s+saldo\s+fittizio", r"SIM\s+con\s+saldo\s+reale",
    r"SIM\s+semplificata",
    r"(?:MM|Money\s+Management)\s+(?:diverso|separato|dedicato)\s+(?:per|fra|tra)\s+(?:SIM|LIVE|modalità)",
    r"command\s+contract\s+(?:diverso|separato)",
    r"\b(?:sim|live)_(?:place_order|cashout|cancel|replace|stop|resume|reconcile)\b",
    r"(?:passaggio|switch|fallback)\s+(?:automatico|implicito)\s+(?:a\s+|verso\s+|SIM\s*→\s*)LIVE",
    r"MCP\s+(?:può|puo)\s+decidere\s+(?:autonomamente\s+)?di\s+andare\s+LIVE",
    r"auto-LIVE",
    r"LIVE\s+senza\s+(?:gate\s+)?readiness", r"LIVE\s+non\s+pronto\s*→(?!\s*BLOCKED)",
    r"peak\s+SIM\s+contamina", r"contaminazione\s+SIM\s*(?:→|->)\s*LIVE\s+(?:ammessa|consentita)",
    r"bankroll\s+SIM\s+(?:diventa|è|come)\s+(?:il\s+)?riferimento\s+di\s+rischio\s+LIVE",
    r"\b(?:LIVE_DATA_SIM|PAPER|HYBRID)\b",
]


def check_contract(text, policies, sections):
    errors = []

    def require(document, phrase, label):
        if phrase not in document:
            errors.append(label)

    def has_positive_instruction(pattern, document):
        for match in re.finditer(pattern, document, re.I):
            start = max(document.rfind("\n", 0, match.start()),
                        document.rfind(";", 0, match.start())) + 1
            prefix = re.sub(r"[*`]", "", document[start:match.start()])
            if not NEGATION.search(prefix):
                return True
        return False

    # Ogni documento corrente, normalizzato: la formattazione non nasconde nulla.
    every = {"contract": text, **{f"policy{i}": p for i, p in enumerate(policies)},
             **{f"issue_{k}": v for k, v in sections.items()}}
    for name, document in every.items():
        plain = normalize(document)
        for pattern in REVERSALS:
            if has_positive_instruction(pattern, plain):
                errors.append(f"reversed_clause:{name}:{pattern}")
    # (1) entrypoint operativo, anche nella forma con link/grassetto
    require(flat(text), ENTRY_SENTENCE, "entry_sentence_contract")
    for link in re.findall(r"partire obbligatoriamente da\s*\[[^\]]*\]\(([^)]*)\)", text):
        if not link.endswith("/issues/491"):
            errors.append("entry_sentence_contract")
    for i, policy in enumerate(policies):
        require(flat(policy), POLICY_ENTRY_SENTENCE, f"entry_sentence_policy{i}")
    for number, body in sections.items():
        require(flat(body), ENTRY_SENTENCE, f"entry_sentence_issue_{number}")
        # (4) P41 e divieto di riattivare, in ogni mirror
        for clause in REVIEWER_CLAUSES:
            require(flat(body), clause, f"reviewer_issue_{number}")
        # (8) stato operativo corrente in ogni mirror
        for phrase in STATE_PHRASES:
            require(flat(body), phrase, f"state_issue_{number}")
        if re.search(r"#(?:490|492|493)\s+(?:APERTA|aperta)|PR05 successiva|"
                     r"PR05\s+(?:è|e')\s+la\s+prossima|(?<!zero )\buna PR aperta al preflight",
                     flat(body)):
            errors.append(f"old_operational_state_issue_{number}")
    # (2) no-bypass completo nel contratto e nella master
    for clause in NO_BYPASS:
        require(flat(text), clause, "no_bypass_contract")
        require(flat(sections.get("1", "")), clause, "no_bypass_master")
    # (5) UPDATER aperto anche nella fonte delle decisioni owner
    require(sections.get("426", ""), "UPDATER_SCOPE = OWNER_OPEN", "updater_open_426")
    # (7) semantica completa PAUSE/STOP/EMERGENCY/RESUME
    for clause in STOP_SEMANTICS:
        require(flat(text), clause, "stop_semantics_contract")
        for number in STOP_SECTIONS:
            require(flat(sections.get(number, "")), clause, f"stop_semantics_issue_{number}")
    # (9) esiti distinti
    require(flat(text), RESULT_STATES, "result_states_contract")
    for number in RESULT_SECTIONS:
        require(flat(sections.get(number, "")), RESULT_STATES, f"result_states_issue_{number}")
    # SIM/LIVE decisione owner 07/10
    for phrase in SIMLIVE_CORE:
        require(flat(text), phrase, "simlive_contract")
        for number in SIMLIVE_CORE_SECTIONS:
            require(flat(sections.get(number, "")), phrase, f"simlive_issue_{number}")
    for number in set(sections) - SIMLIVE_CORE_SECTIONS:
        for phrase in SIMLIVE_POINTER:
            require(flat(sections[number]), phrase, f"simlive_pointer_issue_{number}")
    for phrase in SIMLIVE_MATRIX:
        require(flat(text), phrase, "simlive_matrix_contract")
        for number in SIMLIVE_MATRIX_SECTIONS:
            require(flat(sections.get(number, "")), phrase, f"simlive_matrix_issue_{number}")
    for i, policy in enumerate(policies):
        require(flat(policy), "decisione owner SIM/LIVE del contratto consolidato", f"simlive_policy{i}")

    require(text, ENTRY, "entrypoint")
    for policy in policies:
        require(policy, ENTRY, "policy_entrypoint")
        require(policy, "per repository", "policy_seriality")
        require(policy, "#426/P41", "reviewer_precedence")
        require(policy, "docs/mcp_operational_contract.md", "policy_contract_link")
    operative_clauses = [
        ["Only one active task per repository is allowed at a time.",
         "Only one open pull request per repository is allowed at a time.",
         "Never execute multiple tasks in parallel within this repository.",
         "Never create a second PR while another PR is open in this repository."],
        ["UNA SOLA PR aperta / UN SOLO task attivo alla volta PER REPOSITORY",
         "mai task in parallelo nello stesso repository"],
    ]
    for policy, clauses in zip(policies, operative_clauses):
        for clause in clauses:
            require(policy, clause, "operative_seriality")
    if has_positive_instruction(r"una (?:sola )?PR globale|one global (?:open )?PR",
                                "\n".join([text, *policies, *sections.values()])):
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
    split_order = "MCP-05 → MCP-07 (SIM) → MCP-06 → MCP-07 (finale LIVE/cross-repo)"
    require(text, split_order, "sim_before_live")
    require(master, split_order, "sim_before_live")
    # A correct sentence must not mask a contradictory current instruction.
    current = "\n".join([text, *sections.values(), *policies])
    contradictions = [
        r"PR23 = SL/TP", r"PR24 = Trailing", r"PR25 = SL/TP",
        r"MCP-PR01[–…-]0?5", r"MANUAL via MCP = (?:DECISA|DECISO)",
        r"Telegram disconnected blocca LIVE", r"(?:PUT|POST) /v1/config",
        r"(?:può|puo) leggere il DB Pickfair",
        r"implementa master/copy/follow(?:/fan-out)?",
        r"PF-API-1\s*→\s*PF-API-2",
        r"Ordine operativo autorizzato:[*`\s]*(?:MCP-\d+\s*→\s*)*MCP-05\s*→\s*MCP-06\s*→\s*MCP-07",
        r"(?:entrypoint|partire da) #452",
    ]
    for pattern in contradictions:
        if has_positive_instruction(pattern, current):
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


@pytest.mark.parametrize("policy_index,old,new", [
    (0, "Only one active task per repository is allowed at a time.", "Only one active task is allowed at a time."),
    (0, "Only one open pull request per repository is allowed at a time.", "Only one open pull request is allowed at a time."),
    (0, "Never execute multiple tasks in parallel within this repository.", "Never execute multiple tasks in parallel."),
    (0, "Never create a second PR while another PR is open in this repository.", "Never create a second PR while another PR is open."),
    (1, "UNA SOLA PR aperta / UN SOLO task attivo alla volta PER REPOSITORY", "UNA SOLA PR aperta / UN SOLO task attivo alla volta"),
    (1, "mai task in parallelo nello stesso repository", "mai task in parallelo"),
])
def test_actual_operative_policy_reversions_are_rejected(policy_index, old, new):
    text, policies, sections = inputs()
    assert old in policies[policy_index]
    policies[policy_index] = policies[policy_index].replace(old, new)
    assert "operative_seriality" in check_contract(text, policies, sections)


def test_sim_certification_is_explicitly_before_live():
    text, _, sections = inputs()
    order = "MCP-05 → MCP-07 (SIM) → MCP-06 → MCP-07 (finale LIVE/cross-repo)"
    assert order in text
    assert order in sections["1"]


@pytest.mark.parametrize("document", ["contract", "master"])
def test_sim_gate_removal_is_rejected(document):
    text, policies, sections = inputs()
    old = "MCP-05 → MCP-07 (SIM) → MCP-06 → MCP-07 (finale LIVE/cross-repo)"
    new = "MCP-05 → MCP-06 → MCP-07"
    if document == "contract":
        text = text.replace(old, new)
    else:
        sections["1"] = sections["1"].replace(old, new)
    assert "sim_before_live" in check_contract(text, policies, sections)


@pytest.mark.parametrize("valid_negative", [
    "non può leggere il DB Pickfair", "non partire da #452", "vietato POST /v1/config",
])
def test_explicit_negations_are_valid_governance(valid_negative):
    text, policies, sections = inputs()
    assert check_contract(text + "\n" + valid_negative, policies, sections) == []


@pytest.mark.parametrize("policy_index", [0, 1])
@pytest.mark.parametrize("regression", [
    "POST /v1/config", "Telegram disconnected blocca LIVE", "Può leggere il DB Pickfair",
])
def test_other_current_contradictions_in_policies_are_rejected(policy_index, regression):
    text, policies, sections = inputs()
    policies[policy_index] += "\n" + regression
    assert "contradictory_current_instruction" in check_contract(text, policies, sections)


def test_one_prohibition_does_not_mask_a_later_positive_instruction():
    text, policies, sections = inputs()
    text += "\nvietato POST /v1/config; POST /v1/config\n"
    assert "contradictory_current_instruction" in check_contract(text, policies, sections)


@pytest.mark.parametrize("issue", ["1", "351", "426", "453", "461", "489", "491"])
def test_global_seriality_in_each_managed_issue_is_rejected(issue):
    text, policies, sections = inputs()
    sections[issue] += "\nuna sola PR globale\n"
    assert "global_seriality" in check_contract(text, policies, sections)


def test_short_contract_master_copy_follow_prohibition_cannot_be_reversed():
    text, policies, sections = inputs()
    old = "NON implementa master/copy/follow."
    assert old in text
    text = text.replace(old, "implementa master/copy/follow.")
    assert "contradictory_current_instruction" in check_contract(text, policies, sections)


@pytest.mark.parametrize("document", ["contract", "1", "351", "426", "453", "461", "489", "491"])
def test_extra_live_before_sim_instruction_is_rejected(document):
    text, policies, sections = inputs()
    bad = "\nOrdine operativo autorizzato: MCP-05 → MCP-06 → MCP-07\n"
    if document == "contract":
        text += bad
    else:
        sections[document] += bad
    assert "contradictory_current_instruction" in check_contract(text, policies, sections)


@pytest.mark.parametrize("occurrence", [0, 1])
def test_each_pf_api_sequence_occurrence_is_protected(occurrence):
    text, policies, sections = inputs()
    good = "PF-API-0 → PF-API-1 → PF-API commands"
    matches = list(re.finditer(re.escape(good), text))
    assert len(matches) == 2
    match = matches[occurrence]
    text = text[:match.start()] + "PF-API-1 → PF-API-2" + text[match.end():]
    assert "contradictory_current_instruction" in check_contract(text, policies, sections)


# ---------------------------------------------------------------------------
# Quarto ciclo #494: nove rilievi Codex + decisione owner SIM/LIVE 07/10.
# Ogni caso ribalta o aggiunge una clausola nel documento REALE e deve essere
# rifiutato. Il guard sul «cambiato davvero» impedisce una mutazione a vuoto.
# ---------------------------------------------------------------------------
ALL_ISSUES = ["1", "351", "426", "453", "461", "489", "491"]
CORE_TARGETS = ["contract", "1", "351", "426", "461", "491"]


def _mutate(target, old, new, append=None):
    text, policies, sections = inputs()
    sections = copy.deepcopy(sections)
    policies = list(policies)
    if target == "contract":
        doc = text
    elif target.startswith("policy"):
        doc = policies[int(target[-1])]
    else:
        doc = sections[target]
    changed = re.sub(old, new, doc) if old else doc
    if append:
        changed += "\n" + append + "\n"
    assert changed != doc, f"mutazione a vuoto su {target}: {old!r}"
    if target == "contract":
        text = changed
    elif target.startswith("policy"):
        policies[int(target[-1])] = changed
    else:
        sections[target] = changed
    return text, policies, sections


FINDING_CASES = (
    # 1. entrypoint #491 anche formattato
    [(f, r"partire obbligatoriamente da \*\*Pickfair-nogui #491\*\*",
      "partire obbligatoriamente da **Pickfair-nogui #452**", None) for f in ALL_ISSUES]
    + [("contract", r"\[Pickfair-nogui #491\]\(([^)]*)/issues/491\)",
        r"[Pickfair-nogui #452](\1/issues/452)", None)]
    # 2. ogni divieto no-bypass, nel contratto e nella master
    + [(t, old, new, None) for t in ["contract", "1"] for old, new in [
        (r"NON parla\s+direttamente con Betfair", "parla direttamente con Betfair"),
        (r"NON usa credenziali Betfair", "usa credenziali Betfair"),
        (r"NON duplica formule MM", "duplica formule MM"),
        (r"NON importa BetfairClient", "importa BetfairClient"),
        (r"NON apre\s+il DB Pickfair", "apre il DB Pickfair"),
        (r"NON implementa master/copy/follow", "implementa master/copy/follow")]]
    # 3. «Una PR attiva globale»
    + [(t, "Una PR attiva per repository", "Una PR attiva globale", None)
       for t in ["contract", *ALL_ISSUES]]
    # 4. P41 e «non riattivare» in ogni mirror
    + [(f, r"#426/P41", "#426/P40", None) for f in ALL_ISSUES]
    + [(f, r"non riattivare workflow", "riattivare workflow", None) for f in ALL_ISSUES]
    # 5. UPDATER aperto anche in #426
    + [("426", r"UPDATER_SCOPE = OWNER_OPEN", "UPDATER_SCOPE = CASO_B_AUTORIZZATO", None)]
    # 6. route cashout alternative
    + [("1", r"Vietato creare /v1/cashout/all", "Creare /v1/cashout/all", None),
       ("1", None, None, "Creare /v1/cashout/all e /v1/cashout/target."),
       ("contract", None, None, "Aggiungere POST /v1/cashout/selection.")]
    # 7. semantica PAUSE / STOP / EMERGENCY / RESUME
    + [(t, old, new, None) for t in ["contract", "1", "351", "426", "489", "491"] for old, new in [
        (r"PAUSE:\*\* blocca nuovi ordini", "PAUSE:** consente nuovi ordini"),
        (r"STOP/RISK_STOP:\*\* blocca nuovi ordini", "STOP/RISK_STOP:** consente nuovi ordini"),
        (r"RESUME:\*\* soltanto dopo nuova readiness completa", "RESUME:** senza nuova readiness"),
        (r"EMERGENCY:\*\* barriera massima immediata, nessuna eccezione generica",
         "EMERGENCY:** barriera con eccezioni")]]
    # 8. stato operativo obsoleto in ogni issue
    + [(f, old, new, None) for f in ALL_ISSUES for old, new in [
        (r"#490/#492/#493 MERGIATE", "#490 APERTA"),
        (r"zero PR aperte al preflight", "una PR aperta al preflight"),
        (r"PR05 NON è la prossima attività automatica", "PR05 è la prossima attività automatica")]]
    # 9. esiti distinti
    + [(t, r"PASS/FAIL/BLOCKED_ENV/NOT_RUN/RINVIATO separati", "PASS e NOT_RUN equivalenti", None)
       for t in ["contract", "351", "1", "491"]]
)


@pytest.mark.parametrize("target,old,new,append", FINDING_CASES)
def test_codex_findings_round4_are_rejected(target, old, new, append):
    assert check_contract(*_mutate(target, old, new, append))


SIMLIVE_CASES = [(t, old, new, add) for t in CORE_TARGETS for old, new, add in [
    # SIM mock-only
    (r"SIM NON significa\s+mockare", "SIM significa mockare", None),
    (None, None, "La SIM è un mock del mondo esterno."),
    # SIM senza Delayed App Key reale
    (r"SIM\*\* usa la vera Betfair Delayed App Key", "SIM** non usa la Delayed App Key", None),
    # LIVE senza Live App Key
    (r"LIVE richiede Live App Key", "LIVE senza Live App Key", None),
    # SIM che invia ordini Betfair
    (r"Nessun ordine reale deve essere inviato a Betfair\.", "SIM può inviare ordini reali a Betfair.", None),
    # saldi invertiti
    (r"LIVE = saldo reale Betfair", "LIVE = saldo fittizio", None),
    (r"SIM = saldo/bankroll fittizio", "SIM = saldo reale", None),
    # MM / command contract diversi
    (r"vietato creare un MM diverso per modalità", "usare un MM diverso per modalità", None),
    (r"vietato creare un command contract diverso", "esiste un command contract diverso", None),
    # tool duplicati per modalità
    (None, None, "Tool MCP: sim_place_order e live_place_order."),
    # passaggio automatico a LIVE
    (r"MCP non decide autonomamente di andare LIVE", "MCP può decidere autonomamente di andare LIVE", None),
    (None, None, "Fallback automatico a LIVE se la SIM non è pronta."),
    # LIVE senza readiness
    (r"LIVE non pronto → BLOCKED", "LIVE non pronto → ordine inviato", None),
    (None, None, "LIVE senza readiness ammesso per urgenza."),
    # contaminazione SIM→LIVE
    (r"peak SIM non contamina drawdown LIVE", "peak SIM contamina drawdown LIVE", None),
    (None, None, "Contaminazione SIM→LIVE ammessa al cambio modalità."),
    # PR15 scollegata
    (r"La futura PR15 deve implementare/testare questo contratto\.", "PR15 non riguarda le modalità.", None),
    # nuove modalità
    (None, None, "Modalità PAPER disponibile."),
]]


@pytest.mark.parametrize("target,old,new,append", SIMLIVE_CASES)
def test_simlive_regressions_are_rejected(target, old, new, append):
    assert check_contract(*_mutate(target, old, new, append))


@pytest.mark.parametrize("target,old,new,append", [
    (t, None, None, "SIM con saldo reale e LIVE con saldo fittizio.") for t in ["453", "489"]] + [
    (t, r"PR15: peak SIM non contamina drawdown LIVE", "PR15: nessun vincolo", None) for t in ["453", "489"]] + [
    (f"policy{i}", r"decisione owner SIM/LIVE del contratto consolidato", "decisione storica", None) for i in [0, 1]])
def test_simlive_pointer_and_policy_rinvio_are_protected(target, old, new, append):
    assert check_contract(*_mutate(target, old, new, append))


@pytest.mark.parametrize("valid", [
    "Vietato creare /v1/cashout/all.", "Non usa credenziali Betfair.",
    "Vietato creare `sim_cashout`.", "nessun passaggio implicito SIM→LIVE",
    "La SIM non può inviare ordini reali a Betfair.",
])
def test_round4_explicit_negations_stay_valid(valid):
    text, policies, sections = inputs()
    assert check_contract(text + "\n" + valid + "\n", policies, sections) == []


def test_negation_must_be_adjacent():
    """Una negazione lontana non copre l'istruzione positiva che segue."""
    text, policies, sections = inputs()
    text += "\nNon è un problema: la SIM può inviare ordini reali a Betfair.\n"
    assert check_contract(text, policies, sections)
