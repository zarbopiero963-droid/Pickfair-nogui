#!/usr/bin/env python3
"""Contratto di autonomia end-to-end dell'agente (owner, 08/10/2026).

Norma: docs/auto_pr_flow_spec.md §0. Questo modulo la rende eseguibile e
testabile. È puro (nessuna chiamata di rete, nessuna scrittura) e usa solo la
libreria standard: `pr-guard.yml` lo esegue con `python -I`, quindi non può
importare moduli del repository.

Principio: l'owner decide COSA (prodotto e vincoli), l'agente decide COME.
Ogni funzione è fail-closed: prova mancante, tipo inatteso o stato
sconosciuto non diventano mai PASS/permesso.

Il modulo NON modifica runtime, Betfair, MCP, Control API, denaro o modalità
SIM/LIVE: classifica finding, review, path, merge e PR successiva.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from typing import Any, Iterable, Mapping, Sequence

# ---------------------------------------------------------------------------
# Tassonomia a due assi (§0.3). Origine/disposizione e severità sono separate:
# una severità non decide l'origine e viceversa.
# ---------------------------------------------------------------------------
ORIGINS = (
    "CURRENT_DEFECT_INTRODUCED_BY_PR",
    "CURRENT_DEFECT_PREEXISTING",
    "CURRENT_DEFECT_OUT_OF_SCOPE",
    "GUARDRAIL_GAP",
    "THEORETICAL_MUTATION",
    "STALE",
    "DUPLICATE",
    "ALREADY_COVERED",
    "KNOWN_LIMITATION_ACCEPTED_BY_OWNER",
    "REVIEW_CHURN",
)
SEVERITIES = (
    "P0_SECURITY",
    "P0_REAL_MONEY",
    "P1_SAFETY",
    "P1_ARCHITECTURE",
    "P2_COMPLETENESS",
    "P3_DOCS",
)
P0_SEVERITIES = frozenset({"P0_SECURITY", "P0_REAL_MONEY"})
P1_SEVERITIES = frozenset({"P1_SAFETY", "P1_ARCHITECTURE"})

# Blocker strutturati (§0.5): un rinvio deve dire QUALE traguardo blocca.
BLOCKERS = (
    "BLOCKS_NEXT_PR",
    "BLOCKS_LIVE",
    "BLOCKS_MCP_MUTATION",
    "BLOCKS_DUTCHING_ACTIVATION",
    "BLOCKS_CASHOUT_ACTIVATION",
    "BLOCKS_SIM_CERTIFICATION",
    "BLOCKS_FINAL_CERTIFICATION",
    "BLOCKS_DISTRIBUTION",
    "BLOCKS_CONCURRENT_OPERATION",
    "BLOCKS_SECURITY_ACCEPTANCE",
)

# Condizioni minime di STOP owner (§0.13). Fuori da queste l'agente decide.
OWNER_STOP_CONDITIONS = (
    "NEW_PRODUCT_DECISION",
    "SPEND_OR_INFRASTRUCTURE",
    "CREDENTIAL_OR_SERVICE",
    "NEW_P0_NOT_FIXABLE_WITHIN_CONTRACT",
    "CONTRACT_CHANGE_OR_RISK_ACCEPTANCE",
    "MANUAL_VIA_MCP",
    "FIX_LOOP_EXHAUSTED",
    "SUBSTANTIVE_SOURCE_CONFLICT",
    "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE",
    "REAL_ACTION_REQUIRES_OWNER_COMMAND",
)

# ---------------------------------------------------------------------------
# Reviewer (§0.7, owner #426/P41).
# ---------------------------------------------------------------------------
ACTIVE_REVIEWERS = {
    "sol": "gpt56sol-pr-review-done",
    "grok": "grok46-pr-review-done",
}
SUSPENDED_REVIEWERS = frozenset({"fugu", "fable", "astra"})
ADVISORY_REVIEWERS = frozenset({"codex"})
REVIEW_BOT_LOGIN = "github-actions[bot]"
MAX_GROK_ATTEMPTS = 2  # un solo rerun dopo il primo timeout

_QUOTA_HINTS = (
    "crediti esauriti", "insufficient credits", "insufficient_quota",
    "quota exceeded", "usage-quota", "http 402", "http error 402", "payment required",
    "credit_balance_exhausted", "credit balance", "exceeded your current quota",
    "insufficient_balance", "out of credits",
    "rate limit", "rate-limit", "rate_limit", "http 429", "http error 429", "too many requests",
)
# Solo segnali di timeout VERI (rilievo Codex #499): l'intestazione generica
# «non completata» compare in OGNI errore del provider (auth, modello invalido,
# 5xx) e non dice nulla sulla causa. Un errore non quota e non timeout è
# PROVIDER_ERROR: non dà diritto al rerun automatico di Grok.
_TIMEOUT_HINTS = (
    "timed out", "timeout", "read operation timed out", "http error 408",
)
_INCOMPLETE_HINTS = ("non completata",)
# La sezione Bloccanti è pulita solo se è INTERAMENTE la risposta «nessun
# bloccante» (eventualmente «evidente/rilevato»): un prefisso seguito da un
# bloccante elencato non è una review pulita (rilievo Codex #499).
_NO_BLOCKERS = re.compile(
    r"[-*]?\s*nessun(?:o)?\s+bloccante(?:\s+(?:evidente|rilevato|rilevante|trovato|individuato))?\s*[.!]?",
    re.IGNORECASE)

# ---------------------------------------------------------------------------
# Path (§0.8–§0.10). La lista è il MINIMO a merge manuale owner (item 19):
# file che definiscono autorità, gate, label, review o merge.
# ---------------------------------------------------------------------------
OWNER_MANUAL_MERGE_FILES = frozenset({
    "AGENTS.md",
    "CLAUDE.md",
    "docs/auto_pr_flow_spec.md",
    "docs/hard_verify_spec.md",
    ".github/workflows/pr-review-openrouter-gpt56-sol.yml",
    ".github/workflows/pr-review-xai-grok46.yml",
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml",
    ".github/workflows/pr-review-claude-fable5.yml",
    ".github/workflows/pr-review-openrouter-gpt-astra.yml",
    ".github/workflows/pr-merge-readiness.yml",
    ".github/workflows/ci-quarantine-guard.yml",
    ".github/workflows/pr-guard.yml",
    "scripts/guardrail_check.py",
    "scripts/pr_autonomy_policy.py",
    "scripts/pr_fix_loop_policy.py",
    "scripts/pr_flow_automation.py",
    "scripts/pr_merge_readiness.py",
    "scripts/pr_automation_controller.py",
    "scripts/pr_clean_scope_rebuild.py",
    "scripts/pr_refresh_self_checks.py",
    "scripts/ci/check_ci_quarantine.py",
    # Lane Buildkite trusted (#563): gli script che decidono overlap e merge readiness sono gate come
    # pr_merge_readiness.py (rilievo Codex P1 su #563).
    "scripts/ci/pr_overlap_guard.sh",
    "scripts/ci/pr_merge_readiness.sh",
    "scripts/ci/trusted_pr_guard.py",
})

# Gli stessi criteri come regex: Sol e Grok ne tengono una copia identica
# (`MANUAL_AUTHORITY_PATTERNS`), verificata da tests/scripts/test_pr_autonomy_policy.py.
# Solo questi applicano `manual-review-required`.
MANUAL_AUTHORITY_PATTERNS = (
    r"(^|/)\.github/workflows/",
    r"^(CLAUDE|AGENTS)\.md$",
    r"^docs/(auto_pr_flow_spec|hard_verify_spec)\.md$",
    r"^scripts/(guardrail_check|pr_[a-z0-9_]+|ci/check_ci_quarantine)\.py$",
    r"^scripts/ci/(pr_[a-z0-9_]+\.sh|trusted_pr_guard\.py)$",
    r"(^|/)requirements[^/]*\.(txt|in|lock)$",
    r"(^|/)pyproject\.toml$",
    r"(^|/)poetry\.lock$",
    r"(^|/)\.env($|\.)",
    r"(?i)(^|/)[^/]*\.(pem|pfx|p12|key|keystore|jks)$",
    r"(?i)(^|/)(id_rsa|id_dsa|id_ecdsa|id_ed25519)$",
)
_MANUAL_RE = tuple(re.compile(p) for p in MANUAL_AUTHORITY_PATTERNS)

# Materiale segreto: mai in un commit (AGENTS.md). Nel full diff è BLOCK.
SECRET_MATERIAL_PATTERNS = (
    r"(^|/)\.env($|\.)",
    r"(?i)(^|/)[^/]*\.(pem|pfx|p12|key|keystore|jks|crt|cer)$",
    r"(?i)(^|/)(id_rsa|id_dsa|id_ecdsa|id_ed25519)$",
    r"(^|/)config\.json$",
    r"(?i)\.(db|sqlite|sqlite3)$",
    r"(?i)\.log$",
)
_SECRET_RE = tuple(re.compile(p) for p in SECRET_MATERIAL_PATTERNS)

# Input generati da pr-guard: non devono mai stare nel diff.
GUARD_GENERATED_FILES = frozenset({
    "pr_meta.json", "pr_files_raw.json", "allowed_scope_base.json",
    "pr_full_diff.patch",
})

DEPENDENCY_MANIFEST_PATTERNS = (
    r"(^|/)requirements[^/]*\.(txt|in|lock)$",
    r"(^|/)pyproject\.toml$",
    r"(^|/)poetry\.lock$",
)
_DEPENDENCY_RE = tuple(re.compile(p) for p in DEPENDENCY_MANIFEST_PATTERNS)

# Safety-critical INFORMATIVO (§0.8): non applica la label manuale e non blocca
# da solo l'auto-merge; impone review e hard verify completi.
SAFETY_CRITICAL_PATTERNS = (
    r"^(core|services|controllers)/",
    r"(?i)telegram|betfair|parser|roserpina|dutching|money|safety|reconcil|runtime|order_manager|catalog",
    r"(?i)cashout|exposure|risk|stake|database",
    r"(?i)secret|credential|token|auth|encrypt|decrypt|certlogin|app_?key|config|settings",
)
_SAFETY_RE = tuple(re.compile(p) for p in SAFETY_CRITICAL_PATTERNS)

# §0.9-bis — PR finanziarie critiche (owner 01/11/2026 «GOVERNANCE AUTO-MERGE CERTIFICATO», #586).
# Le aree qui sotto restano SEMPRE a merge owner, anche con certificazione completa: credenziali,
# autenticazione e segreti; attivazione LIVE / ordini reali; protezioni finanziarie fondamentali.
# Valgono per il codice di prodotto (tests/ e docs non contano: vedi `_is_product_path`).
FINANCIAL_OWNER_ONLY_PATTERNS = (
    # credenziali, autenticazione, segreti
    r"^core/(secret_cipher|token_store|redaction)\.py$",
    r"^telegram_sanitizer\.py$",
    r"^betfair_client\.py$",
    r"(?i)(^|/)[^/]*(auth|credential|certlogin|session_manager)[^/]*\.py$",
    # attivazione LIVE / ordini reali, configurazione di esecuzione
    r"^core/(runtime_authority|execution_guard|microstake_live_port|bridge_mode|config_store)\.py$",
    r"^(trading_config|config_registry)\.py$",
    r"^scripts/live_gate\.py$",
    r"^config\.example\.json$",
    # protezioni finanziarie fondamentali
    r"^core/(safety_layer|safety_guard|risk_gate|risk_middleware|risk_stop|risk_desk|loss_limits"
    r"|daily_loss_store|duplication_guard|signal_dedupe|stake_precision)\.py$",
    r"^(circuit_breaker|auto_throttle|safe_mode|safe_mode_manager|market_validator)\.py$",
    r"^ai/ai_guardrail\.py$",
    # Codex su 0c050e6: cap dello stake, limiti di esposizione e kill switch del daily-loss (AGENTS.md: i limiti
    # di denaro sono decisione owner)
    r"^(core/money_management|stake_mode)\.py$",
    r"(?i)(stop_?loss|take_?profit|sl_tp|kill_?switch|exposure)",
    # Rete larga per parola chiave nel NOME del file (Codex su 4fb12ff: un elenco esplicito lascia sempre fuori
    # qualcosa, es. services/setting_service.py con password e app key, core/system_state.py con i limiti di
    # esposizione Roserpina). Un falso positivo costa un merge owner, un falso negativo un auto-merge non dovuto.
    r"(?i)(^|/)[^/]*(secret|token|credential|auth|passw|certlogin|cipher|sanitiz|redact|setting|config"
    r"|risk|loss_limit|safety|safe_mode|guard|throttle|circuit_breaker|validator|live|system_state"
    r"|stake_precision|dedupe|duplication|runtime_authority|bridge_mode)[^/]*$",
)
_FIN_OWNER_RE = tuple(re.compile(p) for p in FINANCIAL_OWNER_ONLY_PATTERNS)

# §0.9-ter — delega owner temporanea (owner 01/11/2026 «TEMPORARY MERGE DELEGATION — ROADMAP V2», #586).
# Dentro FINANCIAL_OWNER_ONLY, la delega copre SOLO le protezioni finanziarie fondamentali. Credenziali,
# autenticazione, segreti, impostazioni/configurazione e attivazione LIVE NON sono mai delegabili: un file che
# corrisponde anche a uno solo di questi pattern resta a merge owner (rete larga, come quella di sopra).
FINANCIAL_NON_DELEGABLE_PATTERNS = (
    r"^core/(secret_cipher|token_store|redaction)\.py$",
    r"^telegram_sanitizer\.py$",
    r"^betfair_client\.py$",
    r"(?i)(^|/)[^/]*(auth|credential|certlogin|session_manager)[^/]*\.py$",
    r"^core/(runtime_authority|execution_guard|microstake_live_port|bridge_mode|config_store)\.py$",
    r"^(trading_config|config_registry)\.py$",
    r"^scripts/live_gate\.py$",
    r"^config\.example\.json$",
    # Codex P1 su ebdd11e: moduli MISTI, protezioni finanziarie insieme a configurazione con credenziali
    # (`core/system_state.py`: `BetfairConfig` con app key e chiave privata, `TelegramRuntimeConfig` con session
    # string e bot token). Il nome non lo dice, quindi va elencato: un modulo misto resta a merge owner.
    r"^core/system_state\.py$",
    # Codex P1 su 665ecb6: `core/safety_layer.py` ospita il gate LIVE (`assert_live_gate_or_refuse`) accanto alle
    # protezioni: modulo misto LIVE/protezione, resta a merge owner.
    r"^core/safety_layer\.py$",
    r"(?i)(^|/)[^/]*(secret|token|credential|auth|passw|certlogin|cipher|sanitiz|redact|setting|config|live"
    r"|runtime_authority|bridge_mode)[^/]*$",
)
_FIN_NON_DELEGABLE_RE = tuple(re.compile(p) for p in FINANCIAL_NON_DELEGABLE_PATTERNS)
# §0.9-ter, contenuto (Codex P1 su ebdd11e): una riga AGGIUNTA o RIMOSSA in codice di prodotto che tocca un
# identificatore di credenziale/segreto rende la PR non delegabile, qualunque sia il nome del file (un elenco di
# file lascia sempre fuori un modulo misto). Rete larga: un falso positivo costa solo un merge owner.
DELEGATION_CREDENTIAL_CONTENT_RE = re.compile(
    r"(?i)(app_?key|api_?key|passw|secret|token|private_?key|cert_?(file|path|login|pem)|certlogin|credential"
    r"|session_?string|auth|cipher|redact|sanitiz)"
)


# §0.9-ter, contenuto LIVE (Codex P1 su 665ecb6): rete PIU' larga di LIVE_ACTIVATION_CONTENT_RE, solo per la delega.
# Una riga aggiunta o rimossa che nomina il LIVE (`live_readiness_ok`, `assert_live_gate_or_refuse`, `real_money`,
# modalita' di simulazione...) rende la PR non delegabile. Un falso positivo costa solo un merge owner.
DELEGATION_LIVE_CONTENT_RE = re.compile(r"(?i)(live|real_?money|simulation|execution_?mode)")


def _changed_lines_by_file(patch_text: str) -> dict[str, list[str]] | None:
    """Righe aggiunte E rimosse per file, per il controllo di contenuto della delega (§0.9-ter).

    Sol su 665ecb6: un parser semplice saltava gli header git tra virgolette (path Unicode) e il controllo falliva
    aperto. Si riusa `_parse_patch_numbered` (header quotati decodificati, conteggi degli hunk): patch non
    interpretabile => ``None``, che il chiamante tratta come NON delegabile (fail-closed)."""
    removed: dict[str, list[str]] = {}
    try:
        added, _binary = _parse_patch_numbered(patch_text or "", removed=removed)
    except (ValueError, UnicodeDecodeError):
        return None
    out: dict[str, list[str]] = {path: [line[1:] for _, line in rows] for path, rows in added.items()}
    for path, lines in removed.items():
        out.setdefault(path, []).extend(lines)
    return out


def _removed_lines_by_file(patch_text: str) -> dict[str, list[str]] | None:
    """Solo le righe RIMOSSE per file (stesso parser robusto); ``None`` se il patch non e' interpretabile."""
    removed: dict[str, list[str]] = {}
    try:
        _parse_patch_numbered(patch_text or "", removed=removed)
    except (ValueError, UnicodeDecodeError):
        return None
    return removed

# Registro delle deleghe owner. Sta in questo file (a merge MANUALE dell'owner, §0.9): il merge dell'owner E' la
# prova della delega, perche' un commento su una issue non e' autenticabile (gli account agente pubblicano come
# owner, #472). L'agente non puo' crearla, allargarla o prolungarla: ogni modifica qui torna al merge owner.
# Per chiuderla l'owner mette `active: False` o rimuove la voce (al completamento della roadmap V2).
OWNER_MERGE_DELEGATIONS: dict[str, dict[str, Any]] = {
    "PF-ROADMAP-V2-MERGE-DELEGATION": {
        "issue": 586,
        "granted": "2026-11-01",
        "active": True,
        "cards": tuple(f"F-{n:02d}" for n in range(1, 34)),       # le 33 schede funzionali della roadmap V2
    },
}
_ROADMAP_CARD_RE = re.compile(r"^F-[0-9]{2}$")
# Script di prodotto che attivano il LIVE: fuori dal codice di prodotto ma sempre merge owner.
FINANCIAL_OWNER_ONLY_SCRIPTS = frozenset({"scripts/live_gate.py"})

# PR finanziarie critiche: auto-merge SOLO con la certificazione rafforzata (§0.9-bis) provata sull'head.
# Fail-closed (Codex su 4fb12ff, `core/async_db_writer.py` sfuggiva all'elenco): e' finanziario critico TUTTO il
# codice di prodotto (vedi `_is_product_path`). In questo bot ogni modulo di prodotto puo' toccare segnali,
# ordini, denaro o il loro storico; la certificazione la produce comunque l'agente sull'head finale.
FINANCIAL_CRITICAL_PATTERNS = (r".",)
_FIN_RE = tuple(re.compile(p) for p in FINANCIAL_CRITICAL_PATTERNS)

# Righe aggiunte di prodotto che TOCCANO l'attivazione LIVE o il denaro reale: sempre merge owner.
# Si guarda la MENZIONE dell'identificatore, non il valore assegnato (Sol su 4fb12ff: `allow_live = (True)`,
# assegnazioni su piu' righe, indirezioni via variabile o setattr sfuggono a qualunque regex sul valore).
LIVE_ACTIVATION_CONTENT_RE = re.compile(
    # Nessun `\b` intorno agli interruttori (Codex su 0923d3d): `set_simulation_mode(False)`, `_live_enabled`,
    # `self._execution_mode` sono identificatori composti che i confini di parola non vedevano. Unica eccezione:
    # la LETTURA `is_simulation_mode` (lookbehind), che non cambia la modalita'.
    r"(?i)(live_enabled|enable_live|allow_live|live_armed|real_money|go_live|execution_mode|live_gate"
    r"|(?<!is_)simulation_mode|set_simulation)"
    # `simulation` da solo: come chiave fra apici (dict, subscript, getattr) o come parametro/assegnazione
    # (`simulation=`, `simulation:`). Sol su 0c050e6: `{"simulation": False}` e `cfg["simulation"] = False`.
    r"|[\"']simulation[\"']"
    r"|\bsimulation\b\s*(:|=(?!=))"
)

# Certificazione rafforzata richiesta per l'auto-merge di una PR finanziaria critica (§0.9-bis).
FINANCIAL_CERTIFICATION_TRUE = ("full_suite_pass", "mutation_gate_pass", "financial_tests_pass",
                                "codex_no_blockers", "reviews_complete", "scope_verified")
FINANCIAL_CERTIFICATION_FALSE = ("known_financial_regression_unmanaged", "tests_skipped_or_disabled",
                                 "branch_protection_override")

# Nuovi chiamanti di ingressi del percorso denaro: riportati dal full diff.
# Solo forme di chiamata (`nome(`) o l'evento citato fra apici: la semplice
# menzione del nome (docstring, una regex come questa) non è un chiamante.
MONEY_PATH_CALL_RE = re.compile(
    r"\b(?:place_bet|place_orders|placeOrders|cancel_orders|cancelOrders|"
    r"replace_orders|replaceOrders|update_orders|updateOrders|execute_cashout)\s*\("
    r"|[\"']CMD_QUICK_BET[\"']"
)

# MCP è solo adapter (§0.12): nessun accesso diretto a Betfair, DB o credenziali.
MCP_FORBIDDEN_RE = re.compile(
    r"(?i)(betfair\.com|identitysso|api\.betfair|certlogin|app_?key|"
    r"sqlite3|\bpickfair\.db\b|betfair_client|betfairlightweight)"
)

# Contenuto con forma di segreto (§0.10, rilievo Grok su #499): il controllo per
# path non vede un token incollato in un .md/.yml/.py. Il report riporta SOLO il
# path, mai la riga: nessun valore finisce in log o summary. I pattern sono
# scritti in modo da non corrispondere al proprio sorgente.
SECRET_CONTENT_RE = re.compile(
    r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----"
    r"|\bgh[pousr]_[A-Za-z0-9]{36,}"
    r"|\bgithub_pat_[A-Za-z0-9_]{40,}"
    r"|\bxox[abprs]-[A-Za-z0-9-]{10,}"
    r"|\bsk-[A-Za-z0-9_-]{32,}"
    r"|\bxai-[A-Za-z0-9]{40,}"
    r"|\bAKIA[0-9A-Z]{16}\b"
    r"|\b[0-9]{8,10}:AA[A-Za-z0-9_-]{33}\b"
)

SYNTHETIC_MARKER_RE = re.compile(r"(?i)(synthetic|sintetic|fake|dummy|placeholder|not[-_ ]a[-_ ]real)")

STDLIB_EXTRA = frozenset({"__future__"})


# ---------------------------------------------------------------------------
# Helpers fail-closed
# ---------------------------------------------------------------------------
def _is_true(value: Any) -> bool:
    return value is True


def _is_false(value: Any) -> bool:
    return value is False


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _decision(**fields: Any) -> dict[str, Any]:
    base = {
        "disposition": "NEEDS_MANUAL",
        "action": "STOP",
        "patch": False,
        "resolve_thread": False,
        "mark_fixed": False,
        "blocks_merge": True,
        "owner_required": False,
        "blockers": [],
        "missing": [],
        "reason": "",
    }
    base.update(fields)
    return base


# ---------------------------------------------------------------------------
# Finding (§0.3–§0.6)
# ---------------------------------------------------------------------------
DEFER_REQUIRED_TRUE = ("proven_on_base", "recording_verified")
DEFER_REQUIRED_FALSE = (
    "introduced_by_pr",
    "aggravated_by_pr",
    "activated_by_pr",
    "needed_for_acceptance",
    "compromises_delivered_path",
    "owner_forbids_defer",
)
THEORETICAL_REQUIRED_TRUE = (
    "no_real_caller",
    "no_runtime_wiring",
    "no_reachable_config",
    "no_behaviour_change",
    "no_safety_security_money_impact",
    "no_imminent_activation",
)


def _current_blocker(finding: Mapping[str, Any], reason: str) -> dict[str, Any]:
    if _is_true(finding.get("fix_requires_product_decision")):
        return _decision(
            disposition="NEEDS_MANUAL",
            action="STOP_OWNER_PRODUCT_DECISION",
            owner_required=True,
            reason=reason + "; il fix richiede una nuova decisione di prodotto",
        )
    return _decision(
        disposition="CURRENT_BLOCKER",
        action="FIX_NOW_TEST_THEN_MERGE",
        patch=True,
        reason=reason,
    )


def _defer_or_block(finding: Mapping[str, Any], origin: str) -> dict[str, Any]:
    severity = finding.get("severity")
    if any(_is_true(finding.get(k)) for k in ("introduced_by_pr", "aggravated_by_pr", "activated_by_pr")):
        return _current_blocker(finding, f"{origin} introdotto/aggravato/attivato dalla PR: blocker corrente")
    if _is_true(finding.get("needed_for_acceptance")) or _is_true(finding.get("compromises_delivered_path")):
        return _current_blocker(finding, f"{origin} necessario all'accettazione o compromette il percorso consegnato")
    if severity in P0_SEVERITIES:
        reachable = finding.get("reachable_from_pr")
        if not isinstance(reachable, bool):
            return _decision(reason="P0: raggiungibilità dalla PR non provata (fail-closed)", missing=["reachable_from_pr"])
        if _is_true(finding.get("fixable_within_contract")):
            return _decision(
                disposition="P0_FAIL_CLOSED",
                action="STOP_AFFECTED_PATH_AND_FIX",
                patch=True,
                blocks_merge=reachable,
                reason="P0 senza auto-defer generico: percorso fermato, fix entro il contratto",
            )
        return _decision(
            disposition="NEEDS_MANUAL",
            action="STOP_OWNER_P0",
            owner_required=True,
            blocks_merge=True,
            reason="P0 non correggibile entro il contratto deciso",
        )
    missing = [k for k in DEFER_REQUIRED_TRUE if not _is_true(finding.get(k))]
    missing += [k for k in DEFER_REQUIRED_FALSE if not _is_false(finding.get(k))]
    if not _nonempty_str(finding.get("future_card")):
        missing.append("future_card")
    if not _nonempty_str(finding.get("technical_owner")):
        missing.append("technical_owner")
    if not _nonempty_str(finding.get("finding_id")):
        missing.append("finding_id")
    blockers = finding.get("future_blockers")
    if (not isinstance(blockers, (list, tuple)) or not blockers
            or any(b not in BLOCKERS for b in blockers)):
        missing.append("future_blockers")
    if missing:
        return _decision(
            disposition="NOT_DEFERRABLE_YET",
            action="COMPLETE_EVIDENCE_OR_FIX",
            missing=sorted(set(missing)),
            reason="DEFERRED_BY_POLICY richiede TUTTE le condizioni: thread non risolvibile",
        )
    return _decision(
        disposition="DEFERRED_BY_POLICY",
        action="COMMENT_EVIDENCE_AND_RESOLVE",
        resolve_thread=True,
        blocks_merge=False,
        blockers=list(blockers),
        reason="preesistente indipendente: rinviato alla scheda futura, mai FIXED",
    )


def classify_finding(finding: Mapping[str, Any]) -> dict[str, Any]:
    """Classifica un finding secondo §0.3–§0.6 e restituisce l'azione."""
    if not isinstance(finding, Mapping):
        return _decision(reason="finding non strutturato")
    origin = finding.get("origin")
    severity = finding.get("severity")
    if origin not in ORIGINS:
        return _decision(reason=f"origine sconosciuta: {origin!r}", missing=["origin"])
    if severity not in SEVERITIES:
        return _decision(reason=f"severità sconosciuta: {severity!r}", missing=["severity"])
    if not _nonempty_str(finding.get("current_head_sha")) or not _nonempty_str(finding.get("evidence")):
        return _decision(reason="prova sul current head assente", missing=["current_head_sha", "evidence"])

    if origin == "CURRENT_DEFECT_INTRODUCED_BY_PR":
        return _current_blocker(finding, "difetto introdotto dalla PR: fix ora, test, niente merge finché aperto")
    if origin == "GUARDRAIL_GAP" and _is_true(finding.get("introduced_by_pr")):
        return _current_blocker(finding, "gap di guardrail introdotto dalla PR")
    if origin in ("CURRENT_DEFECT_PREEXISTING", "CURRENT_DEFECT_OUT_OF_SCOPE", "GUARDRAIL_GAP"):
        return _defer_or_block(finding, origin)

    if origin == "THEORETICAL_MUTATION":
        missing = [k for k in THEORETICAL_REQUIRED_TRUE if not _is_true(finding.get(k))]
        if severity in P0_SEVERITIES or severity == "P1_SAFETY":
            missing.append("severity_without_safety_security_money_impact")
        if missing:
            return _decision(
                disposition="NOT_THEORETICAL",
                action="RECLASSIFY_WITH_EVIDENCE",
                missing=sorted(set(missing)),
                reason="THEORETICAL_MUTATION richiede tutte le prove: da riclassificare, non risolvibile",
            )
        return _decision(
            disposition="THEORETICAL_MUTATION",
            action="COMMENT_EVIDENCE_AND_RESOLVE",
            resolve_thread=True,
            blocks_merge=False,
            reason="mutazione teorica provata sul current head: nessuna patch",
        )

    if origin == "STALE":
        if _is_true(finding.get("proven_absent_on_head")):
            return _decision(disposition="STALE", action="COMMENT_EVIDENCE_AND_RESOLVE",
                             resolve_thread=True, blocks_merge=False,
                             reason="assente sul current head, provato")
        return _decision(disposition="NOT_STALE", action="RECLASSIFY_WITH_EVIDENCE",
                         missing=["proven_absent_on_head"], reason="STALE non provato sul current head")
    if origin == "DUPLICATE":
        if _nonempty_str(finding.get("canonical_finding")):
            return _decision(disposition="DUPLICATE", action="LINK_CANONICAL_AND_RESOLVE",
                             resolve_thread=True, blocks_merge=False,
                             reason="duplicato: vale il finding canonico")
        return _decision(disposition="NOT_DUPLICATE", action="RECLASSIFY_WITH_EVIDENCE",
                         missing=["canonical_finding"], reason="duplicato senza finding canonico")
    if origin == "ALREADY_COVERED":
        if _nonempty_str(finding.get("covering_evidence")):
            return _decision(disposition="ALREADY_COVERED", action="COMMENT_EVIDENCE_AND_RESOLVE",
                             resolve_thread=True, blocks_merge=False,
                             reason="comportamento/test esistente mostrato")
        return _decision(disposition="NOT_COVERED", action="RECLASSIFY_WITH_EVIDENCE",
                         missing=["covering_evidence"], reason="copertura non mostrata")
    if origin == "KNOWN_LIMITATION_ACCEPTED_BY_OWNER":
        if severity in P0_SEVERITIES or severity == "P1_SAFETY":
            return _decision(reason="una limitazione nota non copre P0/safety", missing=["severity"])
        if _nonempty_str(finding.get("owner_acceptance_url")):
            return _decision(disposition="KNOWN_LIMITATION_ACCEPTED_BY_OWNER",
                             action="LINK_OWNER_ACCEPTANCE_AND_RESOLVE",
                             resolve_thread=True, blocks_merge=False,
                             reason="accettazione owner tracciata; non è FIXED")
        return _decision(missing=["owner_acceptance_url"], owner_required=True,
                         reason="limitazione senza accettazione owner tracciata")
    # REVIEW_CHURN
    if _nonempty_str(finding.get("equivalent_to")) and _is_true(finding.get("current_head_correct")):
        return _decision(disposition="REVIEW_CHURN", action="COMMENT_EVIDENCE_AND_RESOLVE",
                         resolve_thread=True, blocks_merge=False,
                         reason="ripetizione equivalente senza nuovo difetto: nessuna patch artificiale")
    return _decision(disposition="NOT_CHURN", action="RECLASSIFY_WITH_EVIDENCE",
                     missing=["equivalent_to", "current_head_correct"],
                     reason="churn non provato")


def codex_triage_complete(findings: Sequence[Mapping[str, Any]], unresolved_threads: Any) -> dict[str, Any]:
    """§0.7: ogni rilievo Codex classificato con prova; zero thread aperti."""
    if not isinstance(unresolved_threads, int) or isinstance(unresolved_threads, bool):
        return {"complete": False, "reason": "conteggio thread sconosciuto"}
    results = [classify_finding(f) for f in findings]
    if any(r["missing"] or r["disposition"] == "NEEDS_MANUAL" for r in results):
        return {"complete": False, "reason": "rilievi non classificati con prova"}
    if any(r["blocks_merge"] for r in results):
        return {"complete": False, "reason": "rilievi classificati ma ancora bloccanti"}
    if unresolved_threads != 0:
        return {"complete": False, "reason": f"{unresolved_threads} thread non risolti"}
    return {"complete": True, "reason": ""}


# ---------------------------------------------------------------------------
# Review (§0.7)
# ---------------------------------------------------------------------------
# PF-REVIEW-RECOVERY-01 (#550). Il blocco fra i due delimitatori e' copiato
# IDENTICO nei workflow di Sol e Grok, che non fanno checkout e non possono
# importarlo; tests/scripts/test_pr_review_recovery.py verifica che le tre copie
# coincidano. Solo libreria standard (`re`), nessun effetto collaterale.
# >>> REVIEW_RECOVERY_CORE >>>
RR_VALID_PASS = "VALID_PASS"
RR_VALID_BLOCKERS = "VALID_BLOCKERS"
# Governance post-#556: review ben formata e senza bloccanti ma con copertura dichiarata PARZIALE o un verdetto
# che la contraddice (troncato, non verificabile, merge non approvato, impossibile concludere). Valida (marker
# di completamento, nessun altro tentativo a pagamento: ripetere non toglie un troncamento) ma MAI un PASS.
RR_VALID_INCONCLUSIVE = "VALID_INCONCLUSIVE"
RR_INCOMPLETE = "INCOMPLETE"
RR_TIMEOUT = "TIMEOUT"
RR_PROVIDER_ERROR = "PROVIDER_ERROR"
RR_CREDITS_EXHAUSTED = "CREDITS_EXHAUSTED"
RR_UNKNOWN = "UNKNOWN"
RR_STATES = (RR_VALID_PASS, RR_VALID_BLOCKERS, RR_VALID_INCONCLUSIVE, RR_INCOMPLETE, RR_TIMEOUT,
             RR_PROVIDER_ERROR, RR_CREDITS_EXHAUSTED, RR_UNKNOWN)
RR_VALID_STATES = (RR_VALID_PASS, RR_VALID_BLOCKERS, RR_VALID_INCONCLUSIVE)
# Tentativi TOTALI per reviewer e range: l'originale piu' una sola ripetizione.
RR_MAX_ATTEMPTS = 2
RR_OWNER_DECISION = "REVIEW_UNKNOWN_OWNER_DECISION"

_RR_CREDIT_HINTS = (
    "crediti esauriti", "insufficient credits", "insufficient_quota",
    "quota exceeded", "usage-quota", "http 402", "http error 402", "payment required",
    "credit_balance_exhausted", "credit balance", "exceeded your current quota",
    "insufficient_balance", "out of credits",
    # Rate limit = stessa fermata dei crediti (AGENTS.md, «OUT OF CREDITS»): mai un errore derogabile.
    "rate limit", "rate-limit", "rate_limit", "http 429", "http error 429", "too many requests",
)
_RR_TIMEOUT_HINTS = ("timed out", "timeout", "read operation timed out", "http error 408")
_RR_NO_BLOCKERS = re.compile(
    r"[-*]?\s*nessun(?:o)?\s+bloccante(?:\s+(?:evidente|rilevato|rilevante|trovato|individuato))?\s*[.!]?",
    re.IGNORECASE)
_RR_H2 = re.compile(r"^##(?!#)[ \t]*(.*?)[ \t]*#*[ \t]*$", re.MULTILINE)
# Prima riga del «Verdetto finale»: dichiarazione strutturata della copertura (chiesta dal prompt), cosi' il
# giudizio non dipende da parole isolate nel testo libero. Grassetto e trattini ammessi.
_RR_COVERAGE = re.compile(r"[*_\s]*copertura[*_\s]*:[*_\s]*(completa|parziale)\b", re.IGNORECASE)
# Conclusioni che negano la completezza dell'esame. Si cercano in tutto il verdetto: con «Copertura: COMPLETA»
# sono una contraddizione, e una contraddizione non e' mai un PASS. Fail-closed: anche un «non approvo il merge»
# di ruolo conta, percio' il prompt chiede di non pronunciarsi sul merge. «Test non eseguiti» non e' cercato:
# i reviewer sono diff-only per contratto.
_RR_NEGATIVE_VERDICT = (
    ("merge_non_approvato", re.compile(
        r"\bmerge\s+non\s+approvat|\bnon\s+approv[oai]\w*\s+(?:il\s+)?merge|\bnot\s+approv|\bdo\s+not\s+merge"
        r"|\bnon\s+(?:e'|è|e)?\s*(?:mergeabil|mergiabil)", re.IGNORECASE)),
    ("troncato", re.compile(r"\b(?:troncat|truncat)\w*", re.IGNORECASE)),
    ("non_verificabile", re.compile(
        r"\bnon\s+(?:(?:e['’]?|è)\s+)?verificabil\w*|\bnot\s+verifiable|\bunverifiable|\bnon\s+(?:ho\s+)?potuto\s+verificare",
        re.IGNORECASE)),
    ("non_conclusiva", re.compile(
        r"\bimpossibile\s+(?:concludere|verificare|confermare|certificare)"
        r"|\bnon\s+(?:posso|si\s+pu[oò]|(?:e['’]?|è)\s+(?:\w+\s+){0,2}possibile)\s+"
        r"(?:confermare|concludere|certificare|verificare)"
        r"|\breview\s+non\s+(?:(?:e['’]?|è)\s+)?(?:chiusa|conclusa|completa)"
        r"|\blimitat\w*\s+(?:a(?:lle?|i|gli)?\s+)?(?:porzion|part|tratt)\w*\s+visibil"
        r"|\bcannot\s+(?:conclude|confirm|certify)",
        re.IGNORECASE)),
)
_RR_CUT_LINE = re.compile(r"^\.\.\.\[PATCH FILE TRONCATO PER BUDGET TOKEN\]\.\.\.$", re.MULTILINE)
_RR_TRUNC_NEGATED = re.compile(r"\b(?:non|nessun\w*|senza|niente|not|no)\s+(?:\w+\s+){0,2}$", re.IGNORECASE)
_RR_ATTEMPT_RE = re.compile(
    r"<!-- ([a-z0-9]+)-pr-review-attempt:pickfair-nogui:([0-9a-f]{7,40})\.\.\.([0-9a-f]{7,40}):([0-9]+):([A-Z_]+) -->")


def rr_sanitize(text):
    """L'output del modello e' contenuto non fidato: nessun commento HTML, quindi nessun marker forgiato."""
    return str(text or "").replace("<!--", "&lt;!--").replace("-->", "--&gt;")


def rr_classify_error(error_text):
    """Chiamata al modello fallita: crediti/quota, timeout, oppure errore del provider."""
    low = str(error_text or "").lower()
    if any(h in low for h in _RR_CREDIT_HINTS):
        return RR_CREDITS_EXHAUSTED
    if any(h in low for h in _RR_TIMEOUT_HINTS):
        return RR_TIMEOUT
    return RR_PROVIDER_ERROR


def rr_coverage_gaps(files, diff_text, compare_truncated):
    """Parti del range che il modello NON ha visto, misurate sul payload inviato (lista vuota = nessuna).

    Un file con patch ma senza blocco `### FILE:` e' stato escluso per budget; il segnaposto di taglio
    indica una patch inviata solo in parte. I file senza patch (binari) non sono testo da rivedere. Blocco
    e segnaposto contano solo come righe proprie, scritte dal builder: le righe di una patch iniziano
    sempre con `+`, `-`, spazio, `@@` o `\\`, quindi il testo del diff non puo' fingere un blocco in piu'
    ne' un taglio (rilievo Codex P2 su #559: una riga `+` con il testo del segnaposto non e' un taglio)."""
    sent_text = str(diff_text or "")
    with_patch = sum(1 for f in (files or ()) if isinstance(f, dict) and f.get("patch"))
    sent = sent_text.count("\n\n### FILE: ")
    cut = len(_RR_CUT_LINE.findall(sent_text))
    gaps = []
    if compare_truncated:
        gaps.append("compare_api_troncata")
    if with_patch > sent:
        gaps.append(f"{with_patch - sent}_file_oltre_budget")
    if cut:
        gaps.append(f"{cut}_patch_tagliate")
    return gaps


def rr_verdict_concerns(verdict):
    """Motivi per cui un «Verdetto finale» non certifica un esame completo (lista vuota = nessuno).

    1. La prima riga non vuota deve dichiarare `Copertura: COMPLETA` o `Copertura: PARZIALE`: assente =>
       `copertura_non_dichiarata`; PARZIALE => `copertura_parziale`.
    2. Nel testo, conclusioni che negano la completezza: merge non approvato, troncato (salvo negato: «nessun
       file troncato»), non verificabile, impossibile concludere. Con COMPLETA sono una contraddizione."""
    text = str(verdict or "").strip()
    concerns = []
    first = next((line for line in text.splitlines() if line.strip()), "")
    coverage = _RR_COVERAGE.match(first)
    if coverage is None:
        concerns.append("copertura_non_dichiarata")
    elif coverage.group(1).lower() == "parziale":
        concerns.append("copertura_parziale")
    for name, pattern in _RR_NEGATIVE_VERDICT:
        for match in pattern.finditer(text):
            if name == "troncato" and _RR_TRUNC_NEGATED.search(text[max(0, match.start() - 40):match.start()]):
                continue
            concerns.append(name)
            break
    return concerns


def rr_classify_output(text, truncated=False, coverage_gaps=()):
    """(stato, motivo) del TESTO restituito dal modello, prima di qualunque marker.

    Valida e' solo una review con le due sezioni obbligatorie, uniche, in ordine
    e non vuote. Il testo troncato, vuoto o senza sezioni e' INCOMPLETE; uno
    schema inatteso (sezioni duplicate o in piu', commenti HTML) e' UNKNOWN.
    Senza bloccanti: VALID_PASS solo con `Copertura: COMPLETA` e nessuna conclusione
    che la contraddica; verdetto senza la riga di copertura => INCOMPLETE (si ripete);
    copertura parziale o contraddetta => VALID_INCONCLUSIVE (mai PASS, escalation).
    `coverage_gaps` sono i buchi MISURATI dal workflow (file con patch non inviati per
    budget, patch tagliate, Compare API troncata): non dipendono da cio' che il modello
    scrive, e con un buco la review non e' mai un PASS.
    """
    body = str(text or "").strip()
    if truncated:
        return RR_INCOMPLETE, "testo troncato (finish_reason=length)"
    if not body:
        return RR_INCOMPLETE, "testo vuoto"
    if "<!--" in body or "-->" in body:
        return RR_UNKNOWN, "commento HTML nell'output: possibile marker forgiato"
    titles = [t.strip().lower() for t in _RR_H2.findall(body)]
    for title in ("bloccanti", "verdetto finale"):
        if titles.count(title) > 1:
            return RR_UNKNOWN, f"sezione '{title}' duplicata"
    if "bloccanti" not in titles or "verdetto finale" not in titles:
        return RR_INCOMPLETE, "sezioni obbligatorie assenti"
    if titles != ["bloccanti", "verdetto finale"]:
        return RR_UNKNOWN, "sezioni fuori schema o fuori ordine"
    parts = _RR_H2.split(body)
    # split con un gruppo: [prima, titolo1, corpo1, titolo2, corpo2]
    blockers, verdict = parts[2].strip(), parts[4].strip()
    if parts[0].strip():
        return RR_UNKNOWN, "testo prima della sezione Bloccanti"
    if not blockers:
        return RR_INCOMPLETE, "sezione Bloccanti vuota"
    if not verdict:
        return RR_INCOMPLETE, "Verdetto finale vuoto"
    if not _RR_NO_BLOCKERS.fullmatch(blockers):
        return RR_VALID_BLOCKERS, "bloccanti da triagiare"
    # I buchi misurati entrano PRIMA della decisione sulla ripetizione (rilievo Codex P2 su #559): con un buco
    # ripetere non recupera il codice escluso, quindi la riga di copertura assente non basta per un retry.
    concerns = rr_verdict_concerns(verdict) + [f"diff_non_visto:{gap}" for gap in (coverage_gaps or ())]
    if concerns == ["copertura_non_dichiarata"]:
        return RR_INCOMPLETE, "riga «Copertura:» assente nel Verdetto finale"
    if concerns:
        return RR_VALID_INCONCLUSIVE, "verdetto non conclusivo: " + ", ".join(concerns)
    return RR_VALID_PASS, ""


def rr_attempt_marker(review_id, base, head, number, state):
    return (f"<!-- {review_id}-pr-review-attempt:pickfair-nogui:"
            f"{str(base)[:12]}...{str(head)[:12]}:{int(number)}:{state} -->")


def rr_owner_decision_marker(review_id, base, head):
    return f"<!-- {review_id}-pr-review-owner-decision:pickfair-nogui:{str(base)[:12]}...{str(head)[:12]} -->"


def rr_attempts(body, review_id, base, head):
    """Stati dei tentativi registrati per QUESTO reviewer e range, in ordine di numero (uno per numero)."""
    found = {}
    for m in _RR_ATTEMPT_RE.finditer(str(body or "")):
        if m.group(1) != review_id or m.group(2) != str(base)[:12] or m.group(3) != str(head)[:12]:
            continue
        state = m.group(5) if m.group(5) in RR_STATES else RR_UNKNOWN
        found.setdefault(int(m.group(4)), state)
    return [found[n] for n in sorted(found)]


def rr_counted(attempts):
    """Tentativi che consumano il tetto: i crediti esauriti no (il provider rifiuta, nessuna review
    pagata), cosi' dopo la ricarica il range si puo' ancora revisionare con un rerun."""
    return [a for a in (attempts or []) if a != RR_CREDITS_EXHAUSTED]


def rr_next_action(attempts, done_present):
    """SKIP_DONE: review valida gia' registrata; OWNER_DECISION: tentativi finiti, nessuna nuova
    chiamata a pagamento; CALL: si puo' chiamare il modello (primo tentativo o l'unica ripetizione)."""
    if done_present:
        return "SKIP_DONE"
    if len(rr_counted(attempts)) >= RR_MAX_ATTEMPTS:
        return "OWNER_DECISION"
    return "CALL"


def rr_owner_decision_note(model_label, reviewer_key, pr_number, head, states, run_url):
    """Richiesta di decisione all'owner, scritta una sola volta per range (il commento e' per range)."""
    return (
        f"\n## {RR_OWNER_DECISION}\n\n"
        f"{model_label} non ha prodotto una review valida in {len(states)} tentativi su questo range "
        f"({', '.join(states)}). Non e' un PASS e non parte nessun altro tentativo a pagamento.\n\n"
        f"- Reviewer: `{reviewer_key}`\n"
        f"- Head: `{head}`\n"
        f"- Run: {run_url}\n"
        "- Prove sostitutive da valutare: review dell'altro reviewer sul current head, CI verde, "
        "hard verify, audit dell'agente sul diff completo.\n"
        "- Deroga: SOLO l'owner, scritta a mano su GitHub (non tramite app o agente), in una riga:\n\n"
        f"  `PF-REVIEW-WAIVER pr={pr_number} head={head} reviewer={reviewer_key} reason=<motivo>`\n\n"
        "  Vale solo per questa PR, questo head e questo reviewer: un nuovo push la fa scadere. "
        "Non copre bloccanti, crediti esauriti, CI rossa, hard verify o altri gate.\n"
    )
# <<< REVIEW_RECOVERY_CORE <<<

_DONE_RE = re.compile(r"<!-- ([a-z0-9]+-pr-review-done):pickfair-nogui:([0-9a-f]{7,40})\.\.\.([0-9a-f]{7,40}) -->")


def _bloccanti_section(body: str) -> str | None:
    return _section(body, "Bloccanti")


def _section(body: str, title: str) -> str | None:
    """Contenuto dell'UNICA sezione `## <title>`. Assente o duplicata → None
    (rilievo Codex #499: con due «Bloccanti» la prima pulita nascondeva la
    seconda): il chiamante la tratta come schema inatteso, UNKNOWN."""
    found = re.findall(rf"^##\s*{title}\s*$(.*?)(?=^##\s|\Z)", body, re.MULTILINE | re.DOTALL)
    return found[0].strip() if len(found) == 1 else None


_REVIEW_IDS = {"sol": "gpt56sol", "grok": "grok46"}
_OWNER_DECISION_RE = re.compile(
    r"<!-- ([a-z0-9]+)-pr-review-owner-decision:pickfair-nogui:([0-9a-f]{7,40})\.\.\.([0-9a-f]{7,40}) -->")
# Stato del tentativo (scritto dal workflow fidato) -> stato del gate.
_ATTEMPT_STATUS = {
    RR_CREDITS_EXHAUSTED: "QUOTA",
    RR_TIMEOUT: "TIMEOUT",
    RR_PROVIDER_ERROR: "PROVIDER_ERROR",
    RR_INCOMPLETE: "INCOMPLETE",
    RR_UNKNOWN: "INCOMPLETE",
}
_RETRYABLE = frozenset({"TIMEOUT", "PROVIDER_ERROR", "INCOMPLETE"})
_RANGE_MARKER_RE = re.compile(r"<!-- ([a-z0-9]+)-pr-review:pickfair-nogui:([0-9a-f]{7,40})\.\.\.([0-9a-f]{7,40}) -->")


def _same_head(range_head: str, head_sha: str) -> bool:
    return str(head_sha).startswith(range_head) or range_head.startswith(str(head_sha))


def _first_line_markers(body: str, reviewer: str) -> dict[str, Any]:
    """Marker del bot letti SOLO dalla prima riga del commento, dove li scrive il workflow.

    Tutto il resto del commento contiene testo non fidato (output del modello,
    nomi file): un marker li' non conta (rilievo della review indipendente su
    #552). Il range e' quello del marker del commento; done, tentativi e
    decisione owner contano solo se combaciano esattamente con quel range."""
    first = body.split("\n", 1)[0]
    review_id = _REVIEW_IDS[reviewer]
    ranges = [(m.group(2), m.group(3)) for m in _RANGE_MARKER_RE.finditer(first) if m.group(1) == review_id]
    dones = [(m.group(2), m.group(3)) for m in _DONE_RE.finditer(first) if m.group(1) == ACTIVE_REVIEWERS[reviewer]]
    if len(set(ranges)) > 1 or len(set(dones)) > 1:
        return {"error": "piu' range nella stessa intestazione"}
    key = ranges[0] if ranges else (dones[0] if dones else None)
    if key is None:
        return {"key": None, "done": False, "attempts": [], "decision": False}
    if len(key[0]) != 12 or len(key[1]) != 12:
        return {"error": "range del marker non a 12 caratteri"}
    if dones and dones[0] != key:
        return {"error": "marker di completamento di un altro range"}
    decision = any(m.group(1) == review_id and (m.group(2), m.group(3)) == key
                   for m in _OWNER_DECISION_RE.finditer(first))
    return {"key": key, "done": bool(dones), "attempts": rr_attempts(first, review_id, key[0], key[1]),
            "decision": decision}


def validate_review(review: Mapping[str, Any], head_sha: Any) -> dict[str, Any]:
    """Una review conta solo se è leggibile, marcata, sul current head e senza bloccanti.

    Oltre a `status` riporta `classification` (stati di PF-REVIEW-RECOVERY-01),
    `attempts` (tentativi che consumano il tetto, registrati dal workflow sul
    range) e `owner_decision` (tentativi esauriti: decide l'owner)."""
    if not isinstance(review, Mapping):
        return {"status": "UNKNOWN", "reason": "review non strutturata"}
    reviewer = str(review.get("reviewer") or "").lower()
    if reviewer in SUSPENDED_REVIEWERS:
        return {"status": "IGNORED_SUSPENDED", "reason": "reviewer sospeso da P41: non si attende"}
    if reviewer in ADVISORY_REVIEWERS:
        return {"status": "ADVISORY", "reason": "Codex è advisory: assenza né PASS né blocker"}
    if reviewer not in ACTIVE_REVIEWERS:
        return {"status": "UNKNOWN", "reason": f"reviewer sconosciuto: {reviewer!r}"}
    if not _nonempty_str(head_sha):
        return {"status": "UNKNOWN", "reason": "head corrente sconosciuto"}
    if review.get("api_error"):
        return {"status": "UNKNOWN", "reason": "errore API nella lettura della review"}
    body = review.get("body")
    author = review.get("author")
    if not isinstance(body, str) or not _nonempty_str(author):
        return {"status": "UNKNOWN", "reason": "schema review incompleto"}
    if author != REVIEW_BOT_LOGIN:
        return {"status": "INVALID", "reason": "autore non è il bot dei workflow"}
    markers = _first_line_markers(body, reviewer)
    if "error" in markers:
        return {"status": "UNKNOWN", "classification": RR_UNKNOWN, "reason": markers["error"],
                "attempts": 0, "owner_decision": False}
    result = _validate_review_body(body, str(head_sha), markers)
    counted = rr_counted(markers["attempts"])
    # Una review con marker di completamento conta come un tentativo anche se
    # scritta da un workflow precedente, che non registrava i tentativi (#547).
    result["attempts"] = max(len(counted), 1 if markers["done"] else 0)
    result["owner_decision"] = result["status"] == RR_OWNER_DECISION
    return result


def _validate_review_body(body: str, head_sha: str, markers: Mapping[str, Any]) -> dict[str, Any]:
    lowered = body.lower()
    key, attempts = markers["key"], markers["attempts"]
    if key is not None and not _same_head(key[1], head_sha):
        return {"status": "STALE_HEAD", "classification": RR_UNKNOWN, "reason": "il range non termina sul current head"}
    if not markers["done"]:
        if attempts:
            last = attempts[-1]
            if last == RR_CREDITS_EXHAUSTED:
                # Prima del tetto dei tentativi (rilievo Codex P1 su #552): i crediti
                # esauriti restano STOP crediti, mai una decisione owner derogabile.
                return {"status": "QUOTA", "classification": last, "reason": "crediti/quota esauriti"}
            if any(a in RR_VALID_STATES for a in attempts):
                # Un tentativo valido scrive SEMPRE il marker di completamento:
                # uno stato valido senza marker (anche non l'ultimo) = storia
                # alterata o riscritta, mai una decisione owner derogabile.
                return {"status": "UNKNOWN", "classification": RR_UNKNOWN,
                        "reason": "tentativo valido senza marker di completamento"}
            counted = rr_counted(attempts)
            # Il marker di decisione conta solo con il tetto davvero raggiunto (rilievo
            # Codex P1 su #552): i rifiuti per crediti non consumano la ripetizione.
            if len(counted) >= RR_MAX_ATTEMPTS:
                return {"status": RR_OWNER_DECISION, "classification": last,
                        "reason": f"{len(counted)} tentativi non validi ({', '.join(attempts)}): decide l'owner"}
            return {"status": _ATTEMPT_STATUS.get(last, "INCOMPLETE"), "classification": last,
                    "reason": f"tentativo {len(counted)}/{RR_MAX_ATTEMPTS} non valido: {last}"}
        if markers["decision"]:
            # Decisione owner senza tentativi registrati: storia incoerente, mai derogabile.
            return {"status": "UNKNOWN", "classification": RR_UNKNOWN,
                    "reason": "decisione owner senza tentativi registrati"}
        if any(h in lowered for h in _QUOTA_HINTS):
            return {"status": "QUOTA", "classification": RR_CREDITS_EXHAUSTED, "reason": "crediti/quota esauriti"}
        if any(h in lowered for h in _TIMEOUT_HINTS):
            return {"status": "TIMEOUT", "classification": RR_TIMEOUT, "reason": "review non completata per timeout"}
        if any(h in lowered for h in _INCOMPLETE_HINTS):
            return {"status": "PROVIDER_ERROR", "classification": RR_PROVIDER_ERROR,
                    "reason": "review non completata: errore del provider, non timeout"}
        return {"status": "MISSING_MARKER", "classification": RR_UNKNOWN,
                "reason": "job verde ma marker di completamento assente"}
    if markers["decision"]:
        return {"status": "UNKNOWN", "classification": RR_UNKNOWN,
                "reason": "marker di completamento insieme alla decisione owner: commento incoerente"}
    if attempts and attempts[-1] not in RR_VALID_STATES:
        return {"status": "UNKNOWN", "classification": RR_UNKNOWN,
                "reason": "marker di completamento con ultimo tentativo non valido"}
    section = _bloccanti_section(body)
    if section is None:
        return {"status": "UNKNOWN", "classification": RR_INCOMPLETE,
                "reason": "sezione Bloccanti assente o duplicata: schema inatteso"}
    # Schema a due sezioni completo (rilievi Codex/Sol #499): «Bloccanti» vuota
    # o «Verdetto finale» assente/vuoto = review incompleta, mai pulita.
    if not section:
        return {"status": "UNKNOWN", "classification": RR_INCOMPLETE, "reason": "sezione Bloccanti vuota: review incompleta"}
    verdict = _section(body, "Verdetto finale")
    if not verdict:
        return {"status": "UNKNOWN", "classification": RR_INCOMPLETE,
                "reason": "Verdetto finale assente o vuoto: review incompleta"}
    if not _NO_BLOCKERS.fullmatch(section.strip()):
        return {"status": "VALID_WITH_BLOCKERS", "classification": RR_VALID_BLOCKERS,
                "reason": "bloccanti da triagiare"}
    # Governance post-#556: senza bloccanti non basta. Lo stato registrato dal workflow (buchi di copertura
    # misurati) e il verdetto riletto qui (commenti scritti prima di questa regola, deriva fra le copie)
    # devono entrambi certificare un esame completo; altrimenti escalation, mai PASS.
    concerns = rr_verdict_concerns(verdict)
    if attempts and attempts[-1] == RR_VALID_INCONCLUSIVE and not concerns:
        concerns = ["copertura_misurata_parziale"]
    if concerns:
        return {"status": "VALID_INCONCLUSIVE", "classification": RR_VALID_INCONCLUSIVE, "concerns": concerns,
                "reason": "verdetto non conclusivo: " + ", ".join(concerns)}
    return {"status": "VALID_CLEAN", "classification": RR_VALID_PASS, "reason": ""}


# ---------------------------------------------------------------------------
# Deroga owner (PF-REVIEW-RECOVERY-01, requisito 5)
# ---------------------------------------------------------------------------
WAIVER_RE = re.compile(
    r"^PF-REVIEW-WAIVER[ \t]+pr=([0-9]+)[ \t]+head=([0-9a-f]{40})[ \t]+reviewer=(sol|grok)"
    r"[ \t]+reason=(\S.*?)[ \t]*$", re.MULTILINE)
# Firma dei commenti scritti dall'agente: l'agente usa l'account dell'owner,
# quindi un suo commento non è mai un atto dell'owner.
_AGENT_SIGNATURES = ("generated by [claude code]", "generated with [claude code]", "claude.ai/code")


def parse_owner_waiver(comment: Mapping[str, Any], *, owner_login: Any, pr_number: Any,
                       head_sha: Any) -> dict[str, Any]:
    """Deroga valida SOLO se scritta a mano dall'owner, per questa PR e questo head esatto.

    Il commento arriva dalla REST API delle issue comment: `user.login`,
    `author_association`, `body` e `performed_via_github_app` sono obbligatori.
    Un commento creato tramite un'app (l'agente lavora con l'account dell'owner
    attraverso la GitHub App di Claude) o firmato dall'agente non è una deroga.
    """
    def no(problem: str) -> dict[str, Any]:
        return {"valid": False, "problem": problem}
    if not isinstance(comment, Mapping):
        return no("commento non strutturato")
    if not _nonempty_str(owner_login) or not _nonempty_str(head_sha) or not re.fullmatch(r"[0-9a-f]{40}", str(head_sha)):
        return no("owner o head corrente sconosciuti")
    if not isinstance(pr_number, int) or isinstance(pr_number, bool):
        return no("numero PR sconosciuto")
    user = comment.get("user")
    login = user.get("login") if isinstance(user, Mapping) else None
    if login != owner_login:
        return no("autore diverso dall'owner")
    if comment.get("author_association") != "OWNER":
        return no("author_association diversa da OWNER")
    if "performed_via_github_app" not in comment:
        return no("provenienza del commento non verificabile")
    if comment.get("performed_via_github_app") is not None:
        return no("commento creato tramite un'app: non è un atto manuale dell'owner")
    created, updated = comment.get("created_at"), comment.get("updated_at")
    if not _nonempty_str(created) or updated != created:
        # Un commento modificato non e' piu' l'atto scritto a mano: l'agente, tramite
        # l'app, potrebbe aggiungere la riga a un vecchio commento dell'owner.
        return no("commento modificato dopo la creazione o date assenti")
    body = comment.get("body")
    if not isinstance(body, str):
        return no("corpo assente")
    if any(sig in body.lower() for sig in _AGENT_SIGNATURES):
        return no("commento firmato dall'agente")
    found = WAIVER_RE.findall(body)
    if len(found) != 1:
        return no("deroga assente o ambigua (serve una sola riga PF-REVIEW-WAIVER)")
    pr, head, reviewer, reason = found[0]
    if int(pr) != pr_number:
        return no("deroga per un'altra PR")
    if head != head_sha:
        return no("deroga per un altro head: scaduta con il nuovo push")
    return {"valid": True, "reviewer": reviewer, "head": head, "reason": reason.strip(),
            "comment_id": comment.get("id"), "problem": ""}


def collect_owner_waivers(comments: Any, *, owner_login: Any, pr_number: Any, head_sha: Any) -> dict[str, dict[str, Any]]:
    """Deroghe valide per reviewer (la prima valida vince; le altre righe non contano)."""
    waivers: dict[str, dict[str, Any]] = {}
    for comment in comments if isinstance(comments, list) else []:
        parsed = parse_owner_waiver(comment, owner_login=owner_login, pr_number=pr_number, head_sha=head_sha)
        if parsed["valid"]:
            waivers.setdefault(parsed["reviewer"], parsed)
    return waivers


def reviewer_gate(results: Mapping[str, Mapping[str, Any]], grok_attempts: Any = None, *,
                  waivers: Mapping[str, Mapping[str, Any]] | None = None, head_sha: Any = None) -> dict[str, Any]:
    """Esito combinato Sol + Grok. Sospesi e advisory non entrano nel gate.

    Una review non valida (timeout, errore del provider, testo incompleto) ha
    diritto a UNA sola ripetizione (`RERUN_REVIEW_ONCE`); dopo il secondo
    tentativo non valido decide l'owner (`REVIEW_UNKNOWN_OWNER_DECISION`). La
    deroga owner copre solo quel caso, per quel reviewer e quel head: mai una
    review con bloccanti, mai crediti esauriti."""
    if grok_attempts is not None and (not isinstance(grok_attempts, int) or isinstance(grok_attempts, bool)
                                      or grok_attempts < 1):
        return {"status": "UNKNOWN", "reason": "tentativi Grok sconosciuti"}
    statuses: dict[str, Any] = {}
    attempts: dict[str, Any] = {}
    for name in ACTIVE_REVIEWERS:
        entry = results.get(name) or {}
        statuses[name] = entry.get("status")
        recorded = entry.get("attempts", 1)
        if name == "grok" and grok_attempts is not None:
            recorded = max(grok_attempts, recorded) if isinstance(recorded, int) else grok_attempts
        attempts[name] = recorded
    for name, status in statuses.items():
        value = attempts[name]
        if (status in _RETRYABLE or status == RR_OWNER_DECISION) and \
                (not isinstance(value, int) or isinstance(value, bool) or value < 1):
            return {"status": "UNKNOWN", "reason": f"tentativi {name} sconosciuti"}
    if any(s == "QUOTA" for s in statuses.values()):
        return {"status": "STOP_OWNER", "reason": "CREDITI_ESAURITI (P41)"}
    rerun = sorted(n for n, s in statuses.items() if s in _RETRYABLE and attempts[n] < RR_MAX_ATTEMPTS)
    if rerun:
        return {"status": "RERUN_REVIEW_ONCE", "reviewers": rerun,
                "reason": f"un solo nuovo tentativo per {', '.join(rerun)} (max {RR_MAX_ATTEMPTS} in totale)"}
    exhausted = sorted(n for n, s in statuses.items() if s == RR_OWNER_DECISION or s in _RETRYABLE)
    others = {n: s for n, s in statuses.items() if n not in exhausted}
    if any(s in (None, "UNKNOWN") for s in others.values()):
        return {"status": "UNKNOWN", "reason": "review non leggibile o assente"}
    bad = {n: s for n, s in others.items() if s not in ("VALID_CLEAN", "VALID_INCONCLUSIVE")}
    if bad:
        return {"status": "BLOCKED", "reason": f"review non valide sul current head: {bad}"}
    inconclusive = sorted(n for n, s in others.items() if s == "VALID_INCONCLUSIVE")
    if inconclusive:
        # Review senza bloccanti ma che non certifica un esame completo: decide l'owner. Nessuna
        # deroga PF-REVIEW-WAIVER la copre (vale solo per un reviewer senza esito) e non si ripete:
        # un nuovo tentativo sullo stesso range non toglie un troncamento.
        return {"status": "STOP_OWNER", "inconclusive": inconclusive,
                "reason": f"review non conclusiva ({', '.join(inconclusive)}): escalation owner, mai PASS"}
    if not exhausted:
        return {"status": "PASS", "reason": ""}
    if not others:
        # Nessuna review valida da nessun reviewer (rilievo Codex P1 su #552): la
        # deroga copre un reviewer assente, non l'assenza di ogni review.
        return {"status": "BLOCKED", "owner_decision": exhausted,
                "reason": f"nessuna review valida: {', '.join(exhausted)} senza esito ({RR_OWNER_DECISION})"}

    def waived(name: str) -> bool:
        waiver = (waivers or {}).get(name)
        return (isinstance(waiver, Mapping) and waiver.get("valid") is True and waiver.get("reviewer") == name
                and _nonempty_str(head_sha) and waiver.get("head") == head_sha)

    missing = [n for n in exhausted if not waived(n)]
    if missing:
        return {"status": "STOP_OWNER", "owner_decision": missing,
                "reason": f"PRONTA PER MERGE — {', '.join(missing)} assente: {RR_OWNER_DECISION}"}
    return {"status": "PASS_WITH_OWNER_WAIVER", "owner_waivers": [dict(waivers[n]) for n in exhausted],
            "reason": f"deroga owner per {', '.join(exhausted)} sull'head {str(head_sha)[:12]}"}


def review_gate_from_comments(payload: Any) -> dict[str, Any]:
    """Gate dei reviewer attivi calcolato dai commenti grezzi della REST API.

    Input: `head_sha` (40 hex), `pr_number`, `owner_login`, `comments` (lista
    delle issue comment così come le restituisce GitHub). Per ogni reviewer si
    legge il commento del bot il cui range finisce sul current head; le
    deroghe si leggono dagli stessi commenti con `parse_owner_waiver`."""
    if not isinstance(payload, Mapping):
        return {"gate": {"status": "UNKNOWN", "reason": "input non strutturato"}}
    head = payload.get("head_sha")
    comments = payload.get("comments")
    if not isinstance(head, str) or not re.fullmatch(r"[0-9a-f]{40}", head) or not isinstance(comments, list):
        return {"gate": {"status": "UNKNOWN", "reason": "head o commenti illeggibili"}}
    results: dict[str, dict[str, Any]] = {}
    for name, review_id in _REVIEW_IDS.items():
        candidates = []
        for c in comments:
            if not isinstance(c, Mapping):
                continue
            login = (c.get("user") or {}).get("login") if isinstance(c.get("user"), Mapping) else None
            body = c.get("body") if isinstance(c.get("body"), str) else ""
            if login != REVIEW_BOT_LOGIN:
                continue
            for m in _RANGE_MARKER_RE.finditer(body):
                if m.group(1) == review_id and _same_head(m.group(3), head):
                    candidates.append(c)
                    break
        if not candidates:
            results[name] = {"status": None, "reason": "nessuna review del bot sul current head"}
            continue
        # Con piu' range sullo stesso head vince quello completato, poi il piu' recente.
        done_id = ACTIVE_REVIEWERS[name]
        chosen = sorted(candidates, key=lambda c: (done_id in (c.get("body") or ""), c.get("id") or 0))[-1]
        results[name] = validate_review({"reviewer": name, "author": REVIEW_BOT_LOGIN, "body": chosen.get("body")}, head)
    waivers = collect_owner_waivers(comments, owner_login=payload.get("owner_login"),
                                    pr_number=payload.get("pr_number"), head_sha=head)
    gate = reviewer_gate(results, waivers=waivers, head_sha=head)
    return {"gate": gate, "reviews": results, "waivers": waivers}


# ---------------------------------------------------------------------------
# Path, label e full diff (§0.8–§0.10)
# ---------------------------------------------------------------------------
# Modelli di esempio senza segreti (es. `.env.example`): né segreto né manuale.
_TEMPLATE_SUFFIXES = (".example", ".sample", ".template", ".dist")


def _is_template(path: str) -> bool:
    return path.endswith(_TEMPLATE_SUFFIXES)


def is_owner_manual_path(path: str) -> bool:
    if path in OWNER_MANUAL_MERGE_FILES:
        return True
    return not _is_template(path) and any(r.search(path) for r in _MANUAL_RE)


def is_secret_material_path(path: str) -> bool:
    return not _is_template(path) and any(r.search(path) for r in _SECRET_RE)


def _is_product_path(path: str) -> bool:
    """Codice o configurazione di prodotto: esclusi test, documentazione e tooling del repository."""
    return not (path.startswith(("tests/", "docs/", "scripts/", "tools/", "guardrails/", ".guardrails/", ".github/"))
                or path.endswith(".md") or _is_template(path))


def classify_paths(paths: Iterable[str]) -> dict[str, list[str]]:
    paths = [p for p in paths if isinstance(p, str)]
    product = [p for p in paths if _is_product_path(p)]
    return {
        # aree sempre-owner: codice di prodotto, piu' gli script che attivano il LIVE (scripts/live_gate.py).
        # Il tooling (guardrails/, scripts/ di CI, test, docs) non conta: `guardrail` non e' una protezione.
        "financial_owner_only": sorted(p for p in paths
                                       if (_is_product_path(p) or p in FINANCIAL_OWNER_ONLY_SCRIPTS)
                                       and any(r.search(p) for r in _FIN_OWNER_RE)),
        # §0.9-ter: parte NON delegabile delle aree sempre-owner (credenziali, segreti, config, LIVE)
        "financial_owner_non_delegable": sorted(p for p in paths
                                                if (_is_product_path(p) or p in FINANCIAL_OWNER_ONLY_SCRIPTS)
                                                and any(r.search(p) for r in _FIN_OWNER_RE)
                                                and any(r.search(p) for r in _FIN_NON_DELEGABLE_RE)),
        "financial_critical": sorted(p for p in product if any(r.search(p) for r in _FIN_RE)),
        "owner_manual": sorted(p for p in paths if is_owner_manual_path(p)),
        "secret_material": sorted(p for p in paths if is_secret_material_path(p)),
        "guard_generated": sorted(p for p in paths if p in GUARD_GENERATED_FILES),
        "dependency_manifest": sorted(p for p in paths if any(r.search(p) for r in _DEPENDENCY_RE)),
        "safety_critical": sorted(p for p in paths if any(r.search(p) for r in _SAFETY_RE)),
    }


LABEL_OWNER_REASONS = frozenset({
    "OWNER_DECISION_REQUIRED",
    "EXPLICIT_STOP",
    "FIX_LOOP_EXHAUSTED",
    "RISK_ACCEPTANCE",
    "POLICY_REQUIRES_OWNER",
})


def manual_label_decision(full_diff_paths: Iterable[str], recorded_reasons: Iterable[str] = (),
                          compare_truncated: bool = False) -> dict[str, Any]:
    """La label manuale è un blocker reale: solo per i motivi di §0.8."""
    reasons = sorted(set(r for r in recorded_reasons if r in LABEL_OWNER_REASONS))
    unknown = sorted(set(r for r in recorded_reasons if r not in LABEL_OWNER_REASONS))
    manual = classify_paths(full_diff_paths)["owner_manual"]
    if unknown:
        return {"label": "REQUIRED", "reasons": ["UNKNOWN_REASON"] + unknown,
                "files": manual, "removal_allowed": False}
    if manual:
        reasons.append("AUTHORITY_OR_GOVERNANCE_FILES")
    if compare_truncated:
        reasons.append("DIFF_NOT_FULLY_VISIBLE")
    return {
        "label": "REQUIRED" if reasons else "NOT_REQUIRED",
        "reasons": reasons,
        "files": manual,
        "removal_allowed": not reasons,
    }


_IMPORT_RE = re.compile(r"^\+\s*(?:from\s+([A-Za-z_][\w.]*)\s+import\b|import\s+([A-Za-z_][\w.]*(?:\s*,\s*[A-Za-z_][\w.]*)*))")


# Intestazione di istruzione composta su una riga (`try:`, `else:`, `if x:`...):
# se la riga intera non è parsabile da sola, si riprova sul resto dopo i due punti.
_COMPOUND_HEADER_RE = re.compile(
    r"^(?:try|else|finally|(?:except|with|for|while|if|elif|async)\b[^:#]*)\s*:\s*")
# Ultimo fallback, ANCORATO a inizio riga (es. `from x import (` su più righe):
# la prosa di docstring/commenti che cita «: import y» non diventa dipendenza.
_LINE_IMPORT_RE = re.compile(
    r"^(?:from\s+([A-Za-z_][\w.]*)\s+import\b|import\s+([A-Za-z_][\w.]*"
    r"(?:\s+as\s+\w+)?(?:\s*,\s*[A-Za-z_][\w.]*(?:\s+as\s+\w+)?)*))")


def _ast_imports(src: str) -> list[str] | None:
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    mods: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            mods.append(node.module.split(".")[0])
    return mods


def _top_modules(line: str) -> list[str]:
    """Moduli di primo livello importati da una riga aggiunta.

    Parsing strutturale con `ast` di TUTTI i nodi Import/ImportFrom della riga
    (rilievi Codex #499: alias multipli, import dopo `:` o `;`); se la riga non è
    parsabile da sola si toglie l'intestazione composta e si riprova; infine un
    fallback regex ancorato a inizio riga (blocchi su più righe)."""
    src = line[1:].strip() if line.startswith("+") else line.strip()
    if not re.search(r"\bimport\b", src):
        return []
    mods = _ast_imports(src)
    if mods is not None:
        return mods
    header = _COMPOUND_HEADER_RE.match(src)
    if header:
        src = src[header.end():]
        mods = _ast_imports(src)
        if mods is not None:
            return mods
    m = _LINE_IMPORT_RE.match(src)
    if m is None:
        return []
    if m.group(1):
        return [m.group(1).split(".")[0]]
    return [part.strip().split()[0].split(".")[0] for part in m.group(2).split(",") if part.strip()]


_C_ESCAPES = {"a": 7, "b": 8, "t": 9, "n": 10, "v": 11, "f": 12, "r": 13, '"': 34, "\\": 92}


def _unquote_git_path(text: str) -> str:
    """Path del header `+++` come lo scrive git: tra virgolette con escape C e
    byte ottali (default per path non ASCII) oppure nudo. Formato non
    riconosciuto → ValueError (il chiamante va in UNKNOWN, mai PASS)."""
    if not text.startswith('"'):
        return text
    if len(text) < 2 or not text.endswith('"'):
        raise ValueError("header git tra virgolette non chiuso")
    body, out, i = text[1:-1], bytearray(), 0
    while i < len(body):
        ch = body[i]
        if ch != "\\":
            out.extend(ch.encode("utf-8"))
            i += 1
            continue
        nxt = body[i + 1:i + 2]
        if nxt in _C_ESCAPES:
            out.append(_C_ESCAPES[nxt])
            i += 2
        elif len(body[i + 1:i + 4]) == 3 and all(c in "01234567" for c in body[i + 1:i + 4]):
            out.append(int(body[i + 1:i + 4], 8))
            i += 4
        else:
            raise ValueError("escape non riconosciuto nel header git")
    return out.decode("utf-8")


_HUNK_RE = re.compile(r"^@@ -\d+(?:,(?P<old>\d+))? \+(?P<start>\d+)(?:,(?P<new>\d+))? @@")
_BINARY_RE = re.compile(r"^Binary files (.+) and (.+) differ$")
# File che devono essere ispezionabili come testo: se il diff li dà «Binary»
# (es. `.gitattributes` con `-diff`) il controllo non li ha visti → UNKNOWN.
SCANNABLE_SUFFIXES = (".py", ".pyi", ".md", ".rst", ".txt", ".yml", ".yaml", ".json", ".toml",
                      ".cfg", ".ini", ".sh", ".in", ".lock", ".env", ".csv", ".sql", ".js",
                      ".ts", ".html", ".css", ".xml", ".example", ".sample", ".template")


_HEADER_PREFIXES = ("diff --git ", "index ", "--- ", "new file mode", "deleted file mode", "old mode",
                    "new mode", "similarity index", "dissimilarity index", "rename from", "rename to",
                    "copy from", "copy to", "\\ ")


def _parse_patch(patch_text: str) -> tuple[dict[str, list[str]], set[str]]:
    """Righe aggiunte per file + file mostrati come binari (vedi _parse_patch_numbered)."""
    numbered, binary = _parse_patch_numbered(patch_text)
    return {path: [line for _, line in rows] for path, rows in numbered.items()}, binary


def _parse_patch_numbered(patch_text: str, removed: dict[str, list[str]] | None = None
                          ) -> tuple[dict[str, list[tuple[int, str]]], set[str]]:
    """Righe aggiunte per file, col numero di riga nel file NUOVO, + file binari.

    Parser a stati sui conteggi degli hunk (rilievo Codex #499): una riga
    aggiunta il cui contenuto inizia con `++ ` appare come `+++ ` ma DENTRO un
    hunk è contenuto, non un header. Header `+++` tra virgolette decodificato;
    header o hunk non interpretabili → ValueError (il chiamante va in UNKNOWN),
    mai un file saltato in silenzio."""
    out: dict[str, list[tuple[int, str]]] = {}
    binary: set[str] = set()
    current: str | None = None
    old_path: str | None = None     # §0.9-ter: file delle righe RIMOSSE (anche se cancellato, `+++ /dev/null`)
    old_left = new_left = 0
    new_no = 0
    # split("\n") e non splitlines(): \r, \x0c, \x1c… dentro una riga di
    # contenuto non devono spezzarla e sfasare i conteggi dell'hunk.
    for line in patch_text.split("\n"):
        if old_left > 0 or new_left > 0:
            if line.startswith("+"):
                new_left -= 1
                if current is not None:
                    out[current].append((new_no, line))
                new_no += 1
            elif line.startswith("-"):
                old_left -= 1
                # Sol su 860a8b1: una riga RIMOSSA appartiene al file di ORIGINE (`--- a/`), non a quello di
                # destinazione: un rename `core/x.py` -> `tests/x.py` che toglie un gate LIVE resta codice di prodotto.
                owner_file = old_path or current
                if removed is not None and owner_file is not None:
                    removed.setdefault(owner_file, []).append(line[1:])
            elif line.startswith(" "):
                old_left -= 1
                new_left -= 1
                new_no += 1
            elif line.startswith("\\"):
                continue
            else:
                raise ValueError("hunk troncato o malformato")
            if old_left < 0 or new_left < 0:
                raise ValueError("hunk con più righe del dichiarato")
            continue
        if line.startswith("diff --git "):
            current = old_path = None
            continue
        if line.startswith("--- "):
            source = _unquote_git_path(line[4:].rstrip("\t").strip())
            old_path = source[2:] if source.startswith("a/") else None
            continue
        if line.startswith("+++ "):
            target = _unquote_git_path(line[4:].rstrip("\t").strip())
            if target == "/dev/null":
                current = None
                continue
            if not target.startswith("b/"):
                raise ValueError(f"header +++ inatteso: {target[:40]!r}")
            current = target[2:]
            out.setdefault(current, [])
            continue
        hunk = _HUNK_RE.match(line)
        if hunk:
            old_left = int(hunk.group("old")) if hunk.group("old") is not None else 1
            new_left = int(hunk.group("new")) if hunk.group("new") is not None else 1
            new_no = int(hunk.group("start"))
            continue
        binary_line = _BINARY_RE.match(line)
        if binary_line:
            target = _unquote_git_path(binary_line.group(2).strip())
            if target.startswith("b/"):
                binary.add(target[2:])
            continue
        if line and not line.startswith(_HEADER_PREFIXES):
            raise ValueError("riga fuori hunk non riconosciuta")
    if old_left > 0 or new_left > 0:
        raise ValueError("hunk incompleto a fine patch")
    return out, binary


def _logical_lines(lines: Sequence[str]) -> tuple[list[tuple[str, str]], tuple[str, str] | None]:
    """Righe fisiche CONTIGUE → istruzioni logiche (rilievo Codex #499).

    Unisce continuazioni `\\`, parentesi aperte e stringhe triple, come il
    tokenizer Python. Ritorna (istruzioni, istruzione_incompleta_finale); ogni
    istruzione è (testo, solo_codice) con stringhe e commenti oscurati."""
    statements: list[tuple[str, str]] = []
    text: list[str] = []
    code: list[str] = []
    depth, quote, triple = 0, "", False
    for raw in lines:
        i, n = 0, len(raw)
        continued = False
        while i < n:
            ch = raw[i]
            if quote:
                if ch == "\\" and i + 1 < n:
                    i += 2
                    code.append("  ")
                    continue
                if raw.startswith(quote * (3 if triple else 1), i):
                    i += 3 if triple else 1
                    quote, triple = "", False
                    code.append(" ")
                    continue
                code.append(" ")
                i += 1
                continue
            if ch == "#":
                break
            if ch in "\"'":
                triple = raw.startswith(ch * 3, i)
                quote = ch
                i += 3 if triple else 1
                code.append(" ")
                continue
            if ch == "\\" and raw[i + 1:].strip() == "":
                continued = True
                break
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth = max(depth - 1, 0)
            code.append(ch)
            i += 1
        text.append(raw)
        code.append("\n")
        if quote and not triple:
            quote = ""  # stringa semplice non chiusa: Python la rifiuta, la riga finisce
        if not (continued or depth or (quote and triple)):
            statements.append(("\n".join(text), "".join(code)))
            text, code = [], []
    pending = ("\n".join(text), "".join(code)) if text else None
    return statements, pending


def _statement_imports(text: str, code: str) -> tuple[list[str], bool]:
    """Moduli di un'istruzione logica; True se contiene `import` come codice ma
    non è analizzabile (fail-closed: UNKNOWN, mai PASS)."""
    if not re.search(r"\bimport\b", code):
        return [], False
    mods = _top_modules(text)
    if mods:
        return mods, False
    src = text.strip()
    header = _COMPOUND_HEADER_RE.match(src)
    if _ast_imports(src) is not None or (header and _ast_imports(src[header.end():]) is not None):
        return [], False  # codice valido senza import di primo livello (es. relativo)
    return [], True


def _added_python_imports(path: str, rows: Sequence[tuple[int, str]],
                          head_sources: Mapping[str, Any] | None) -> tuple[list[str], bool]:
    """Import toccati dalle righe aggiunte di un file .py.

    Preferito: AST del file COMPLETO all'head (se fornito, parsabile e coerente
    col patch riga per riga): ogni nodo Import/ImportFrom che interseca una riga
    aggiunta conta, anche se la sua prima riga è contesto invariato. Altrimenti
    istruzioni logiche ricostruite dai blocchi contigui di righe aggiunte; una
    continuazione lasciata aperta o un `import` non analizzabile → True."""
    source = head_sources.get(path) if isinstance(head_sources, Mapping) else None
    if isinstance(source, str) and rows:
        src_lines = source.split("\n")
        coherent = all(0 < no <= len(src_lines) and src_lines[no - 1] == line[1:] for no, line in rows)
        try:
            tree = ast.parse(source) if coherent else None
        except (SyntaxError, ValueError):
            tree = None
        if tree is not None:
            added = {no for no, _ in rows}
            mods: list[str] = []
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    end = getattr(node, "end_lineno", None) or node.lineno
                    if not any(no in added for no in range(node.lineno, end + 1)):
                        continue
                    if isinstance(node, ast.Import):
                        mods.extend(alias.name.split(".")[0] for alias in node.names)
                    elif node.level == 0 and node.module:
                        mods.append(node.module.split(".")[0])
            return mods, False
    mods, unverifiable = [], False
    blocks: list[list[str]] = []
    previous = None
    for no, line in rows:
        if previous is None or no != previous + 1:
            blocks.append([])
        blocks[-1].append(line[1:])
        previous = no
    for block in blocks:
        statements, pending = _logical_lines(block)
        for text, code in statements:
            found, bad = _statement_imports(text, code)
            mods.extend(found)
            unverifiable = unverifiable or bad
        if pending is not None and (re.search(r"\bimport\b", pending[1])
                                    or re.match(r"\s*from\b", pending[1])):
            # Istruzione import aperta oltre il blocco visibile: i moduli
            # leggibili si riportano comunque, e il file resta non verificato.
            mods.extend(_top_modules(pending[0]))
            unverifiable = True
    return mods, unverifiable


def _patch_by_file(patch_text: str) -> dict[str, list[str]]:
    return _parse_patch(patch_text)[0]


def full_diff_check(*, base_sha: Any, head_sha: Any, files: Any, patch_text: Any,
                    declared_files: Any, local_modules: Iterable[str],
                    declared_dependencies: Iterable[str], repo_kind: str = "pickfair",
                    deleted_files: Iterable[str] = (),
                    declared_production_dependencies: Iterable[str] | None = None,
                    local_root_modules: Iterable[str] | None = None,
                    head_sources: Mapping[str, str] | None = None) -> dict[str, Any]:
    """§0.10: controllo sull'INTERO diff assemblato della PR (base...head).

    Un file segreto CANCELLATO dalla PR (es. #429, `config.json` tolto dal
    tracking) è una rimozione, non un'introduzione: non blocca.
    """
    problems: list[str] = []
    if not _nonempty_str(base_sha) or not _nonempty_str(head_sha):
        problems.append("base_or_head_unknown")
    if not isinstance(files, list) or not files or not all(_nonempty_str(f) for f in files):
        return {"status": "UNKNOWN", "problems": problems + ["full_diff_unreadable"],
                "classification": {}, "new_dependencies": [], "new_money_path_callers": [],
                "owner_manual_merge_required": True}
    if not isinstance(patch_text, str):
        problems.append("patch_unreadable")
        patch_text = ""
    classification = classify_paths(files)
    deleted = set(deleted_files)
    classification["secret_material"] = [p for p in classification["secret_material"] if p not in deleted]
    if classification["secret_material"]:
        problems.append("secret_material_in_diff")
    if classification["guard_generated"]:
        problems.append("guard_generated_input_in_diff")
    if not isinstance(declared_files, list):
        problems.append("declared_scope_unknown")
    elif sorted(set(declared_files)) != sorted(set(files)):
        problems.append("scope_mismatch_full_diff")

    stdlib = set(getattr(sys, "stdlib_module_names", ())) | STDLIB_EXTRA
    local = set(local_modules)
    # Il codice di prodotto può importare solo moduli/package alla RADICE del
    # repository (rilievo Codex #499: `import cleanup_service` non è
    # importabile solo perché esiste observability/cleanup_service.py).
    # tests/ e scripts/ girano con la propria cartella in sys.path: tutti.
    local_roots = local if local_root_modules is None else set(local_root_modules)
    deps = {d.lower().replace("-", "_") for d in declared_dependencies}
    # Il codice di prodotto si confronta SOLO con le dipendenze di produzione
    # (rilievo Codex #499: `import pytest` in core/ non è dichiarato per
    # un'installazione normale). tests/ e scripts/ usano tutte le dichiarate.
    prod_deps = (deps if declared_production_dependencies is None
                 else {d.lower().replace("-", "_") for d in declared_production_dependencies})
    new_deps: set[str] = set()
    test_deps: set[str] = set()
    callers: list[str] = []
    mcp_violations: list[str] = []
    secret_content: set[str] = set()
    secret_content_tests: set[str] = set()
    live_activation: set[str] = set()
    try:
        numbered, binary_files = _parse_patch_numbered(patch_text)
    except (ValueError, UnicodeDecodeError):
        problems.append("patch_unreadable")
        numbered, binary_files = {}, set()
    by_file = {path: [line for _, line in rows] for path, rows in numbered.items()}
    unverifiable_imports: set[str] = set()
    unscanned = sorted(p for p in binary_files
                       if p.lower().endswith(SCANNABLE_SUFFIXES) or "." not in p.rsplit("/", 1)[-1])
    if unscanned:
        problems.append("unscanned_text_file")
    for path, added in by_file.items():
        is_py = path.endswith(".py")
        if is_py:
            # Import analizzati come ISTRUZIONI, non righe (rilievo Codex #499:
            # `from \\` + `pkg import x`, parentesi, stringhe triple).
            tooling = path.startswith(("tests/", "scripts/"))
            allowed = deps if tooling else prod_deps
            importable = local if tooling else local_roots
            mods, unverifiable = _added_python_imports(path, numbered[path], head_sources)
            if unverifiable:
                unverifiable_imports.add(path)
            for mod in mods:
                if mod and mod not in stdlib and mod not in importable and mod.lower() not in allowed:
                    (test_deps if path.startswith("tests/") else new_deps).add(mod)
        for line in added:
            if SECRET_CONTENT_RE.search(line):
                # Il rilevatore non distingue una fixture da una credenziale
                # (rilievo Codex #499): blocca SEMPRE, salvo una riga sotto
                # tests/ che si DICHIARA sintetica (marker sulla stessa riga);
                # anche allora la PR va a merge owner, che verifica.
                # Il marker conta solo FUORI dal valore con forma di segreto
                # (rilievo Sol #499: un token reale che contiene «fake»).
                if path.startswith("tests/") and SYNTHETIC_MARKER_RE.search(SECRET_CONTENT_RE.sub(" ", line)):
                    secret_content_tests.add(path)
                else:
                    secret_content.add(path)
            if (_is_product_path(path) and LIVE_ACTIVATION_CONTENT_RE.search(line)
                    and not line[1:].lstrip().startswith("#")):
                live_activation.add(path)       # §0.9-bis: attivazione LIVE => merge owner
            if is_py:
                if (MONEY_PATH_CALL_RE.search(line) and not path.startswith("tests/")
                        and not line[1:].lstrip().startswith("#")):
                    callers.append(path)
            if (is_py and repo_kind == "mcp" and MCP_FORBIDDEN_RE.search(line)
                    and not path.startswith("tests/") and not line[1:].lstrip().startswith("#")):
                mcp_violations.append(path)
    if secret_content:
        problems.append("secret_like_content_in_diff")
    if new_deps:
        problems.append("undeclared_new_dependency")
    if mcp_violations:
        problems.append("mcp_adapter_violation")
    if unverifiable_imports:
        problems.append("import_statement_unverifiable")
    if not stdlib:
        problems.append("stdlib_list_unavailable")
    # §0.9-ter: codice di prodotto con righe (aggiunte o rimosse) che toccano credenziali o LIVE => non delegabile
    changed = _changed_lines_by_file(patch_text if isinstance(patch_text, str) else "")
    # Codex P1 su 665ecb6: RIMUOVERE un interruttore/gate LIVE e' come attivarlo. La regola §0.9-bis guardava solo
    # le righe aggiunte: una riga rimossa di prodotto che menziona un interruttore LIVE porta a merge owner.
    removed_only = _removed_lines_by_file(patch_text if isinstance(patch_text, str) else "")
    if removed_only is None:
        # Grok su 860a8b1: righe rimosse non verificabili => mai un esito PASS (full diff UNKNOWN, fail-closed)
        problems.append("patch_unreadable")
    for path, lines in (removed_only or {}).items():
        if _is_product_path(path) and any(LIVE_ACTIVATION_CONTENT_RE.search(x)
                                          and not x.lstrip().startswith("#") for x in lines):
            live_activation.add(path)
    classification["live_activation"] = sorted(live_activation)
    classification["delegation_credential_content"] = (["<patch-unparsable>"] if changed is None else sorted(
        path for path, lines in changed.items()
        if _is_product_path(path) and any(DELEGATION_CREDENTIAL_CONTENT_RE.search(x) for x in lines)))
    classification["delegation_live_content"] = (["<patch-unparsable>"] if changed is None else sorted(
        path for path, lines in changed.items()
        if _is_product_path(path) and any(DELEGATION_LIVE_CONTENT_RE.search(x) for x in lines)))
    status = "BLOCK" if problems else "PASS"
    if problems and set(problems) <= {"base_or_head_unknown", "patch_unreadable",
                                     "declared_scope_unknown", "stdlib_list_unavailable",
                                     "unscanned_text_file", "import_statement_unverifiable"}:
        status = "UNKNOWN"
    return {
        "status": status,
        "problems": sorted(set(problems)),
        "classification": classification,
        "new_dependencies": sorted(new_deps),
        "new_test_only_dependencies": sorted(test_deps),
        "new_money_path_callers": sorted(set(callers)),
        "mcp_adapter_violations": sorted(set(mcp_violations)),
        "secret_like_content": sorted(secret_content),
        "unscanned_text_files": unscanned,
        "unverifiable_import_files": sorted(unverifiable_imports),
        "secret_like_content_tests": sorted(secret_content_tests),
        "owner_manual_merge_required": bool(classification["owner_manual"]
                                            or classification["dependency_manifest"]
                                            or secret_content_tests),
        # §0.9-bis: aree finanziarie sempre a merge owner (credenziali, LIVE, protezioni fondamentali).
        "financial_owner_merge_required": bool(classification["financial_owner_only"] or live_activation),
        # §0.9-ter: la delega owner puo' coprire la PR solo se le sue aree sempre-owner sono TUTTE protezioni
        # fondamentali: nessuna credenziale/segreto/config/LIVE, nessuna riga che attiva il LIVE, nessun segreto.
        "financial_owner_delegation_eligible": bool(classification["financial_owner_only"]
                                                    and not classification["financial_owner_non_delegable"]
                                                    and not classification["delegation_credential_content"]
                                                    and not classification["delegation_live_content"]
                                                    and not live_activation
                                                    and not classification["secret_material"]
                                                    and not secret_content and not secret_content_tests),
    }


# ---------------------------------------------------------------------------
# Readiness fail-closed (§0.11)
# ---------------------------------------------------------------------------
def interpret_readiness(raw: Any) -> str:
    """PASS solo con struttura completa; tutto il resto UNKNOWN/NEEDS_MANUAL."""
    if not isinstance(raw, Mapping):
        return "UNKNOWN"
    if raw.get("error") or raw.get("errors"):
        return "UNKNOWN"
    can_merge = raw.get("can_merge")
    reasons = raw.get("reasons")
    if not isinstance(can_merge, bool) or not isinstance(reasons, list):
        return "UNKNOWN"
    if "pagination_complete" in raw and raw.get("pagination_complete") is not True:
        return "UNKNOWN"  # presente ma non True (False, "false", 0, null) → mai PASS
    if "review_threads_api_unavailable" in reasons:
        return "NEEDS_MANUAL"
    if can_merge and not reasons:
        return "PASS"
    return "BLOCKED"


# ---------------------------------------------------------------------------
# Decisione di merge (§0.9)
# ---------------------------------------------------------------------------
_MERGE_BOOL_TRUE = ("scope_valid", "acceptance_complete", "suite_pass", "hard_verify_pass",
                    "codex_triaged", "fix_loop_valid", "dependencies_satisfied")
_MERGE_BOOL_FALSE = ("pertinent_owner_decision_open", "manual_stop",
                     "preexisting_activated_or_aggravated")


def merge_decision(state: Mapping[str, Any]) -> dict[str, Any]:
    """READY_FOR_AUTO_MERGE solo se TUTTE le condizioni di §0.9 sono provate."""
    if not isinstance(state, Mapping):
        return {"status": "NEEDS_MANUAL", "reasons": ["state_unreadable"]}
    reasons: list[str] = []
    stops: list[str] = []
    evaluated, current = state.get("evaluated_head"), state.get("current_head")
    if not _nonempty_str(evaluated) or not _nonempty_str(current):
        return {"status": "NEEDS_MANUAL", "reasons": ["head_unknown"]}
    if evaluated != current:
        return {"status": "INVALIDATED", "reasons": ["head_changed_reevaluate"]}
    for key in _MERGE_BOOL_TRUE:
        value = state.get(key)
        if not isinstance(value, bool):
            stops.append(f"{key}_unknown")
        elif not value:
            reasons.append(f"{key}_false")
    for key in _MERGE_BOOL_FALSE:
        value = state.get(key)
        if not isinstance(value, bool):
            stops.append(f"{key}_unknown")
        elif value:
            reasons.append(key)
    # `manual-review-required` non è un blocco: instrada al merge MANUALE
    # dell'owner (§0.8/§0.9). Una PR di autorità correttamente etichettata deve
    # poter arrivare a READY_FOR_OWNER_MANUAL_MERGE (rilievo Codex #499).
    manual_label = state.get("manual_label_present")
    if not isinstance(manual_label, bool):
        stops.append("manual_label_present_unknown")
    unresolved = state.get("unresolved_threads")
    if not isinstance(unresolved, int) or isinstance(unresolved, bool):
        stops.append("unresolved_threads_unknown")
    elif unresolved:
        reasons.append("unresolved_threads")
    introduced = state.get("introduced_p0_p1_open")
    if not isinstance(introduced, int) or isinstance(introduced, bool):
        stops.append("introduced_p0_p1_unknown")
    elif introduced:
        reasons.append("introduced_p0_p1_open")
    gate = state.get("reviewer_gate")
    waivers_used: list[dict[str, Any]] = []
    if gate == "STOP_OWNER":
        stops.append("reviewer_stop_owner")
    elif gate == "PASS_WITH_OWNER_WAIVER":
        # PF-REVIEW-RECOVERY-01: la deroga vale solo se riportata per esteso e
        # per l'head valutato; non sostituisce nessun altro gate qui sotto.
        listed = state.get("reviewer_waivers")
        if (not isinstance(listed, list) or not listed
                or not all(isinstance(w, Mapping) and w.get("valid") is True and w.get("head") == current
                           and w.get("reviewer") in ACTIVE_REVIEWERS and _nonempty_str(w.get("reason"))
                           for w in listed)):
            stops.append("reviewer_waiver_unverified")
        else:
            waivers_used = [dict(w) for w in listed]
    elif gate != "PASS":
        (stops if gate in (None, "UNKNOWN") else reasons).append(f"reviewer_gate_{gate}")
    readiness = state.get("merge_readiness")
    if readiness != "PASS":
        (stops if readiness in (None, "UNKNOWN", "NEEDS_MANUAL") else reasons).append(f"merge_readiness_{readiness}")
    full = state.get("full_diff")
    if not isinstance(full, Mapping) or full.get("status") not in ("PASS", "BLOCK", "UNKNOWN"):
        stops.append("full_diff_unknown")
    elif full.get("status") != "PASS":
        (stops if full.get("status") == "UNKNOWN" else reasons).append("full_diff_" + str(full.get("status")))
    safety: list[str] = []
    if isinstance(full, Mapping):
        cls = full.get("classification")
        listed = cls.get("safety_critical") if isinstance(cls, Mapping) else None
        if not isinstance(listed, list) or not all(_nonempty_str(p) for p in listed):
            stops.append("safety_critical_classification_unknown")
        else:
            safety = sorted(set(listed))
    exhausted = state.get("fix_loop_exhausted")
    if not isinstance(exhausted, bool):  # prova esplicita (rilievo Codex #499)
        stops.append("fix_loop_exhausted_unknown")
    elif exhausted:
        stops.append("fix_loop_exhausted")
    if stops:
        return {"status": "NEEDS_MANUAL", "reasons": sorted(set(stops + reasons))}
    if reasons:
        return {"status": "BLOCKED", "reasons": sorted(set(reasons))}
    if manual_label or full.get("owner_manual_merge_required") is not False:
        why = (["owner_manual_merge_files"] if full.get("owner_manual_merge_required") is not False else [])
        return {"status": "READY_FOR_OWNER_MANUAL_MERGE",
                "reasons": why + (["manual_review_label"] if manual_label else []),
                "safety_critical": safety, "owner_waivers": waivers_used}
    # PR safety-critical: l'auto-merge richiede la DICHIARAZIONE della policy
    # (`PR safety-critical: yes — <file>`) provata sull'head corrente (rilievo
    # Codex #499). Non è un'approvazione owner e non rende la PR manuale: prova
    # che il percorso safety-critical è stato riconosciuto. Assente/malformata/
    # vecchia/incompleta → niente auto-merge (NEEDS_MANUAL).
    if safety:
        problem = _safety_declaration_problem(state.get("safety_critical_declaration"), current, safety)
        if problem:
            return {"status": "NEEDS_MANUAL", "reasons": [problem], "safety_critical": safety}
    # §0.9-bis: aree finanziarie sempre a merge owner. Campo assente = non provato => owner (fail-closed).
    # §0.9-ter: unica eccezione, una delega owner registrata in OWNER_MERGE_DELEGATIONS che copre la PR; anche
    # allora resta obbligatoria la certificazione rafforzata qui sotto (nessun gate in meno).
    delegation: dict[str, Any] | None = None
    if full.get("financial_owner_merge_required") is not False:
        requested = state.get("owner_delegation")
        if requested is None:
            return {"status": "READY_FOR_OWNER_MANUAL_MERGE", "reasons": ["financial_owner_only_area"],
                    "safety_critical": safety, "owner_waivers": waivers_used}
        problems = _owner_delegation_problems(requested, current, full)
        if problems:
            return {"status": "READY_FOR_OWNER_MANUAL_MERGE",
                    "reasons": ["financial_owner_only_area"] + [f"owner_delegation:{p}" for p in problems],
                    "safety_critical": safety, "owner_waivers": waivers_used}
        delegation = {"id": requested["id"], "card": requested["card"]}
    financial = cls.get("financial_critical") if isinstance(cls, Mapping) else None
    if not isinstance(financial, list) or not all(_nonempty_str(p) for p in financial):
        return {"status": "NEEDS_MANUAL", "reasons": ["financial_classification_unknown"], "safety_critical": safety}
    if delegation is not None and not financial:
        # un'area sempre-owner e' codice di prodotto, quindi finanziaria critica: lista vuota = classificazione
        # incoerente, mai un auto-merge delegato senza certificazione rafforzata
        return {"status": "NEEDS_MANUAL", "reasons": ["owner_delegation:financial_classification_inconsistent"],
                "safety_critical": safety}
    if financial:
        # PR finanziaria critica: auto-merge solo con la certificazione rafforzata provata sull'head corrente.
        # Una deroga di review o una certificazione assente/vecchia/incompleta = merge owner, mai auto-merge.
        problems = _financial_certification_problems(state.get("financial_certification"), current)
        if waivers_used:
            problems.append("financial_review_waiver")
        if problems:
            return {"status": "READY_FOR_OWNER_MANUAL_MERGE",
                    "reasons": ([] if delegation is None else ["financial_owner_only_area"])
                    + [f"financial_certification:{p}" for p in problems],
                    "safety_critical": safety, "financial_critical": sorted(set(financial)),
                    "owner_waivers": waivers_used}
    result = {"status": "READY_FOR_AUTO_MERGE", "reasons": [], "safety_critical": safety,
              "financial_critical": sorted(set(financial)), "owner_waivers": waivers_used}
    if delegation is not None:
        result["owner_delegation"] = delegation      # tracciabile: il verdetto dichiara la delega usata
    return result


def _owner_delegation_problems(requested: Any, current_head: str, full: Mapping[str, Any]) -> list[str]:
    """§0.9-ter: motivi per cui la delega owner richiesta NON copre la PR (vuoto = copre)."""
    if not isinstance(requested, Mapping):
        return ["malformed"]
    problems: list[str] = []
    record = OWNER_MERGE_DELEGATIONS.get(requested.get("id")) if isinstance(requested.get("id"), str) else None
    if not isinstance(record, Mapping):
        return ["unknown_delegation"]
    if record.get("active") is not True:
        problems.append("inactive")
    card = requested.get("card")
    if not isinstance(card, str) or not _ROADMAP_CARD_RE.match(card) or card not in record.get("cards", ()):
        problems.append("card_not_covered")
    if requested.get("head_sha") != current_head:
        problems.append("stale_or_missing_head")
    if full.get("financial_owner_delegation_eligible") is not True:
        problems.append("area_not_delegable")
    return problems


def _financial_certification_problems(cert: Any, current_head: str) -> list[str]:
    """§0.9-bis: elenco dei motivi per cui la certificazione finanziaria NON e' provata (vuoto = provata)."""
    if not isinstance(cert, Mapping):
        return ["missing"]
    if cert.get("head_sha") != current_head:
        return ["stale_or_missing_head"]
    problems = [f"{k}_not_proven" for k in FINANCIAL_CERTIFICATION_TRUE if cert.get(k) is not True]
    problems += [f"{k}_not_excluded" for k in FINANCIAL_CERTIFICATION_FALSE if cert.get(k) is not False]
    return problems


def _safety_declaration_problem(declaration: Any, current_head: str, safety: Sequence[str]) -> str | None:
    if declaration is None:
        return "safety_critical_declaration_missing"
    if not isinstance(declaration, Mapping):
        return "safety_critical_declaration_malformed"
    paths = declaration.get("paths")
    if (declaration.get("declared") is not True or not _nonempty_str(declaration.get("head_sha"))
            or not isinstance(paths, list) or not paths or not all(_nonempty_str(p) for p in paths)):
        return "safety_critical_declaration_malformed"
    if declaration["head_sha"] != current_head:
        return "safety_critical_declaration_stale"
    if not set(safety) <= set(paths):
        return "safety_critical_declaration_incomplete"
    return None


# ---------------------------------------------------------------------------
# PR successiva (§0.14) e parallelismo cross-repo (§0.12)
# ---------------------------------------------------------------------------
def next_pr_decision(state: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(state, Mapping):
        return {"status": "STOP_OWNER", "reason": "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE"}
    if not _is_true(state.get("merge_verified")) or not _nonempty_str(state.get("merge_sha")):
        return {"status": "WAIT_MERGE_NOT_VERIFIED", "reason": "merge/SHA non verificati sul main"}
    if not _is_true(state.get("main_reread")) or not _is_true(state.get("roadmap_reread")):
        return {"status": "STOP_OWNER", "reason": "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE"}
    # §0.14 (owner 01/11/2026): la CI post-merge su main va verificata PRIMA della PR successiva.
    main_ci = state.get("main_post_merge_ci")
    if main_ci == "RED":
        return {"status": "STOP_MAIN_CI_RED", "reason": "CI post-merge rossa su main: va sistemata prima"}
    if main_ci != "GREEN":
        return {"status": "WAIT_MAIN_CI_NOT_VERIFIED", "reason": "CI post-merge su main non ancora verificata verde"}
    open_prs = state.get("open_prs_same_repo")
    if not isinstance(open_prs, int) or isinstance(open_prs, bool):
        return {"status": "STOP_OWNER", "reason": "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE"}
    if open_prs:
        return {"status": "BLOCKED_OPEN_PR", "reason": "una sola PR attiva per repository"}
    # Prove esplicite, non default (rilievo Codex #499): campo assente o non
    # lista = non verificabile, mai «nessuno stop / nessuna decisione aperta».
    active = state.get("active_stop_conditions")
    open_decisions = state.get("pertinent_open_owner_decisions")
    if (not isinstance(active, list) or not isinstance(open_decisions, list)
            or not all(isinstance(x, str) and x for x in active)):
        return {"status": "STOP_OWNER", "reason": "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE"}
    stops = [s for s in active if s in OWNER_STOP_CONDITIONS]
    unknown_stops = [s for s in active if s not in OWNER_STOP_CONDITIONS]
    if stops or unknown_stops:
        return {"status": "STOP_OWNER", "reason": ",".join(stops + unknown_stops)}
    card = state.get("next_card")
    if not _nonempty_str(card):
        return {"status": "STOP_OWNER", "reason": "prossima scheda non determinabile dalla roadmap"}
    if open_decisions:
        return {"status": "STOP_OWNER", "reason": "decisione owner aperta pertinente alla scheda"}
    deps = state.get("dependencies_satisfied")
    if deps is not True:
        return {"status": "STOP_BLOCKING_DEPENDENCY", "reason": "dipendenza bloccante non soddisfatta"}
    if state.get("card_already_satisfied") is True:
        return {"status": "CARD_SATISFIED_RECORD_EVIDENCE", "card": card,
                "reason": "nessuna PR artificiale: evidenza e scheda successiva"}
    return {"status": "START_PHASE_0", "card": card, "reason": ""}


def parallel_classification(pickfair: Mapping[str, Any], mcp: Mapping[str, Any]) -> str:
    """SAFE_PARALLEL / DEPENDENT / FORBIDDEN_PARALLEL fra una PR per repository."""
    if not isinstance(pickfair, Mapping) or not isinstance(mcp, Mapping):
        return "FORBIDDEN_PARALLEL"
    # Solo booleani e liste veri (rilievo Codex #499): "false" stringa non autorizza.
    flags = (mcp.get("exposes_mutation"), mcp.get("core_authority_ready"),
             pickfair.get("changes_control_api_contract"), mcp.get("consumes_control_api_contract"))
    if not all(isinstance(f, bool) for f in flags):
        return "FORBIDDEN_PARALLEL"
    lists = (mcp.get("depends_on", []), pickfair.get("depends_on", []),
             pickfair.get("unmerged_outputs", []), mcp.get("unmerged_outputs", []))
    if not all(isinstance(x, list) for x in lists):
        return "FORBIDDEN_PARALLEL"
    # Ogni elemento deve essere un identificativo stringa non vuoto (rilievo
    # Codex #499): `{}` faceva TypeError, `1`/`True` arrivavano a SAFE_PARALLEL.
    if not all(_nonempty_str(item) for x in lists for item in x):
        return "FORBIDDEN_PARALLEL"
    if mcp["exposes_mutation"] and not mcp["core_authority_ready"]:
        return "FORBIDDEN_PARALLEL"
    if pickfair["changes_control_api_contract"] and mcp["consumes_control_api_contract"]:
        return "FORBIDDEN_PARALLEL"
    needs = set(lists[0]) | set(lists[1])
    unmerged = set(lists[2]) | set(lists[3])
    if needs & unmerged:
        return "DEPENDENT"
    return "SAFE_PARALLEL"


def owner_stop_required(conditions: Iterable[str]) -> dict[str, Any]:
    conditions = list(conditions)
    # Elementi non stringa (null, liste…) sono ignoti → stop, mai TypeError.
    unknown = [c if isinstance(c, str) else repr(c) for c in conditions
               if not isinstance(c, str) or c not in OWNER_STOP_CONDITIONS]
    active = [c for c in conditions if isinstance(c, str) and c in OWNER_STOP_CONDITIONS]
    return {"stop": bool(active or unknown), "conditions": active, "unknown": unknown}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
_REQ_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")

# Nomi di import che non coincidono con il nome del pacchetto pip.
IMPORT_ALIASES = {"yaml": "pyyaml", "dateutil": "python_dateutil", "telegram": "python_telegram_bot",
                  "PIL": "pillow", "dotenv": "python_dotenv", "jwt": "pyjwt", "Crypto": "pycryptodome",
                  "cryptography": "cryptography", "pytest": "pytest", "_pytest": "pytest"}


def _declared_dependencies(paths: Iterable[str]) -> set[str]:
    names: set[str] = set()
    for p in paths:
        try:
            with open(p, encoding="utf-8") as fh:
                for line in fh:
                    if line.strip().startswith(("#", "-")):
                        continue
                    m = _REQ_NAME_RE.match(line)
                    if m:
                        names.add(m.group(1).lower().replace("-", "_"))
        except OSError:
            continue
    for imp, pkg in IMPORT_ALIASES.items():
        if pkg in names:
            names.add(imp.lower())
    return names


def _pyproject_dependencies(path: str, *, include_optional: bool = True) -> set[str]:
    """[project] dependencies e optional-dependencies (tomllib, stdlib 3.11+)."""
    try:
        import tomllib
        with open(path, "rb") as fh:
            data = tomllib.load(fh)
    except (ImportError, OSError, ValueError):
        return set()
    project = data.get("project") if isinstance(data, dict) else None
    if not isinstance(project, dict):
        return set()
    specs = list(project.get("dependencies") or [])
    if include_optional:
        for group in (project.get("optional-dependencies") or {}).values():
            specs.extend(group or [])
    names = set()
    for spec in specs:
        m = _REQ_NAME_RE.match(str(spec))
        if m:
            names.add(m.group(1).lower().replace("-", "_"))
    for imp, pkg in IMPORT_ALIASES.items():
        if pkg in names:
            names.add(imp.lower())
    return names


# Manifest runtime autorevoli (README: `pip install -r requirements.txt`).
# NON sono produzione: lock con tooling/test (requirements-lock.txt contiene
# pytest), requirements-dev/test, gruppi opzionali (rilievo Codex #499).
PRODUCTION_REQUIREMENTS = ("requirements.txt",)


def _production_dependencies(root: str) -> set[str]:
    """Dipendenze dichiarate per il codice di PRODOTTO."""
    import os
    files = [os.path.join(root, f) for f in PRODUCTION_REQUIREMENTS if os.path.isfile(os.path.join(root, f))]
    return (_declared_dependencies(files)
            | _pyproject_dependencies(os.path.join(root, "pyproject.toml"), include_optional=False))


def _all_declared_dependencies(root: str) -> set[str]:
    """Tutte le dichiarate (anche dev/test/lock/opzionali): per tests/ e scripts/."""
    import glob
    import os
    req = [f for pattern in ("requirements*.txt", "requirements*.in", "requirements*.lock")
           for f in sorted(glob.glob(os.path.join(glob.escape(root), pattern)))]
    return _declared_dependencies(req) | _pyproject_dependencies(os.path.join(root, "pyproject.toml"))


def _head_python_sources(files: Any) -> dict[str, str]:
    """Sorgenti .py all'head (checkout) per l'analisi AST degli import; solo
    path relativi, file regolari, niente symlink."""
    import os
    out: dict[str, str] = {}
    for f in files if isinstance(files, list) else []:
        if (not isinstance(f, str) or not f.endswith(".py") or f.startswith("/")
                or ".." in f.split("/") or os.path.islink(f) or not os.path.isfile(f)):
            continue
        try:
            with open(f, encoding="utf-8", errors="replace", newline="") as fh:
                out[f] = fh.read()
        except OSError:
            continue
    return out


_SKIP_DIRS = frozenset({".git", "venv", ".venv", "node_modules", "__pycache__", "build", "dist"})


def _root_modules(root: str) -> set[str]:
    """Solo moduli/package importabili dalla radice del repository."""
    import os
    mods: set[str] = set()
    for entry in os.listdir(root):
        full = os.path.join(root, entry)
        if os.path.isdir(full) and entry not in _SKIP_DIRS and not entry.startswith("."):
            mods.add(entry)
        elif entry.endswith(".py"):
            mods.add(entry[:-3])
    return mods


def _local_modules(root: str) -> set[str]:
    """Moduli e package del repository (anche sotto scripts/ e tests/)."""
    import os
    mods: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS and not d.startswith(".")]
        mods.update(dirnames)
        mods.update(f[:-3] for f in filenames if f.endswith(".py"))
    return mods


def _resolve_task_key(meta: Any, files: Any, scope: Any) -> str | None:
    """Stessa risoluzione di `guardrail_check.resolve_task` (titolo, body, branch,
    ultimo commit, storico commit), caricato per path: con `python -I` la
    cartella scripts/ non è in sys.path. Errore di caricamento → None, quindi
    `declared_scope_unknown` (fail-closed)."""
    import importlib.util
    import os
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        spec = importlib.util.spec_from_file_location(
            "_pr_autonomy_guardrail_check", os.path.join(here, "guardrail_check.py"))
        if spec is None or spec.loader is None:
            return None
        guard = importlib.util.module_from_spec(spec)
        previous, sys.dont_write_bytecode = sys.dont_write_bytecode, True
        try:  # nessun __pycache__ scritto nel checkout della PR
            spec.loader.exec_module(guard)
        finally:
            sys.dont_write_bytecode = previous
        tasks = (scope or {}).get("tasks") if isinstance(scope, dict) else None
        allowed = {str(k) for k in tasks} if isinstance(tasks, dict) else set()
        changed = [f for f in files if isinstance(f, str)] if isinstance(files, list) else []
        task = guard.resolve_task(meta if isinstance(meta, dict) else {}, changed, allowed)[0]
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    return task if isinstance(task, str) and task else None


def _load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _cmd_full_diff(args: argparse.Namespace) -> int:
    import os
    try:
        meta = _load_json(args.meta)
        raw_files = _load_json(args.files)
        scope = _load_json(args.scope)
        with open(args.patch, encoding="utf-8", errors="replace", newline="") as fh:
            patch_text = fh.read()
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "UNKNOWN", "problems": [f"input_unreadable:{type(exc).__name__}"]}))
        return 1
    files = raw_files if isinstance(raw_files, list) else (raw_files or {}).get("files")
    key = _resolve_task_key(meta, files, scope)
    task = ((scope or {}).get("tasks") or {}).get(key) if key else None
    declared = task.get("files") if isinstance(task, dict) else None
    deps = _all_declared_dependencies(".")
    prod_deps = _production_dependencies(".")
    result = full_diff_check(
        base_sha=args.base_sha, head_sha=args.head_sha, files=files, patch_text=patch_text,
        declared_files=declared, local_modules=_local_modules("."), declared_dependencies=deps,
        local_root_modules=_root_modules("."),
        repo_kind=args.repo_kind,
        deleted_files=[f for f in files or [] if isinstance(f, str) and not os.path.lexists(f)],
        declared_production_dependencies=prod_deps,
        head_sources=_head_python_sources(files),
    )
    result["task_key"] = key
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


def _cmd_eval(func: Any, args: argparse.Namespace) -> int:
    try:
        payload = _load_json(args.input)
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "UNKNOWN", "reason": f"input_unreadable:{type(exc).__name__}"}))
        return 2
    print(json.dumps(func(payload), indent=2, sort_keys=True, ensure_ascii=False))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    fd = sub.add_parser("full-diff", help="controllo sul diff assemblato (pr-guard)")
    fd.add_argument("--meta", required=True)
    fd.add_argument("--files", required=True)
    fd.add_argument("--patch", required=True)
    fd.add_argument("--scope", required=True)
    fd.add_argument("--base-sha", required=True)
    fd.add_argument("--head-sha", required=True)
    fd.add_argument("--repo-kind", choices=("pickfair", "mcp"), default="pickfair")
    for name in ("classify-finding", "merge-decision", "next-pr", "review-gate"):
        p = sub.add_parser(name)
        p.add_argument("--input", required=True, help="file JSON con le prove")
    args = parser.parse_args(argv)
    if args.cmd == "full-diff":
        return _cmd_full_diff(args)
    func = {"classify-finding": classify_finding, "merge-decision": merge_decision,
            "next-pr": next_pr_decision, "review-gate": review_gate_from_comments}[args.cmd]
    return _cmd_eval(func, args)


if __name__ == "__main__":
    sys.exit(main())
