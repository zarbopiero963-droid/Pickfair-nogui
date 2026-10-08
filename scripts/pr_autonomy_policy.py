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
    "quota exceeded", "usage-quota", "http 402", "payment required",
)
_TIMEOUT_HINTS = (
    "timed out", "timeout", "read operation timed out",
    "remote end closed", "non completata",
)
_NO_BLOCKERS = re.compile(r"^\s*[-*]?\s*nessun(?:o)?\s+bloccante", re.IGNORECASE)

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
})

# Gli stessi criteri come regex: Sol e Grok ne tengono una copia identica
# (`MANUAL_AUTHORITY_PATTERNS`), verificata da tests/scripts/test_pr_autonomy_policy.py.
# Solo questi applicano `manual-review-required`.
MANUAL_AUTHORITY_PATTERNS = (
    r"(^|/)\.github/workflows/",
    r"^(CLAUDE|AGENTS)\.md$",
    r"^docs/(auto_pr_flow_spec|hard_verify_spec)\.md$",
    r"^scripts/(guardrail_check|pr_autonomy_policy|pr_fix_loop_policy|pr_flow_automation|pr_merge_readiness|pr_automation_controller)\.py$",
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
_DONE_RE = re.compile(r"<!-- ([a-z0-9]+-pr-review-done):pickfair-nogui:([0-9a-f]{7,40})\.\.\.([0-9a-f]{7,40}) -->")


def _bloccanti_section(body: str) -> str | None:
    m = re.search(r"^##\s*Bloccanti\s*$(.*?)(?=^##\s|\Z)", body, re.MULTILINE | re.DOTALL)
    return None if m is None else m.group(1).strip()


def validate_review(review: Mapping[str, Any], head_sha: Any) -> dict[str, Any]:
    """Una review conta solo se è leggibile, marcata, sul current head e senza bloccanti."""
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
    lowered = body.lower()
    done = [m for m in _DONE_RE.finditer(body) if m.group(1) == ACTIVE_REVIEWERS[reviewer]]
    if not done:
        if any(h in lowered for h in _QUOTA_HINTS):
            return {"status": "QUOTA", "reason": "crediti/quota esauriti"}
        if any(h in lowered for h in _TIMEOUT_HINTS):
            return {"status": "TIMEOUT", "reason": "review non completata per timeout"}
        return {"status": "MISSING_MARKER", "reason": "job verde ma marker di completamento assente"}
    range_head = done[-1].group(3)
    if not str(head_sha).startswith(range_head) and not range_head.startswith(str(head_sha)):
        return {"status": "STALE_HEAD", "reason": "il range non termina sul current head"}
    section = _bloccanti_section(body)
    if section is None:
        return {"status": "UNKNOWN", "reason": "sezione Bloccanti assente: schema inatteso"}
    if not section or _NO_BLOCKERS.match(section):
        return {"status": "VALID_CLEAN", "reason": ""}
    return {"status": "VALID_WITH_BLOCKERS", "reason": "bloccanti da triagiare"}


def reviewer_gate(results: Mapping[str, Mapping[str, Any]], grok_attempts: Any) -> dict[str, Any]:
    """Esito combinato Sol + Grok. Sospesi e advisory non entrano nel gate."""
    if not isinstance(grok_attempts, int) or isinstance(grok_attempts, bool) or grok_attempts < 1:
        return {"status": "UNKNOWN", "reason": "tentativi Grok sconosciuti"}
    statuses = {name: (results.get(name) or {}).get("status") for name in ACTIVE_REVIEWERS}
    if any(s == "QUOTA" for s in statuses.values()):
        return {"status": "STOP_OWNER", "reason": "CREDITI_ESAURITI (P41)"}
    if statuses.get("grok") == "TIMEOUT":
        if grok_attempts < MAX_GROK_ATTEMPTS:
            return {"status": "RERUN_GROK_ONCE", "reason": "primo timeout Grok: un solo rerun"}
        return {"status": "STOP_OWNER", "reason": "PRONTA PER MERGE — Grok assente per timeout"}
    if any(s in (None, "UNKNOWN") for s in statuses.values()):
        return {"status": "UNKNOWN", "reason": "review non leggibile o assente"}
    bad = {n: s for n, s in statuses.items() if s != "VALID_CLEAN"}
    if bad:
        return {"status": "BLOCKED", "reason": f"review non valide sul current head: {bad}"}
    return {"status": "PASS", "reason": ""}


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


def classify_paths(paths: Iterable[str]) -> dict[str, list[str]]:
    paths = [p for p in paths if isinstance(p, str)]
    return {
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


def _top_modules(line: str) -> list[str]:
    m = _IMPORT_RE.match(line)
    if not m:
        return []
    if m.group(1):
        return [m.group(1).split(".")[0]]
    return [part.strip().split(".")[0].split()[0] for part in m.group(2).split(",") if part.strip()]


def _patch_by_file(patch_text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    current = None
    for line in patch_text.splitlines():
        if line.startswith("+++ "):
            target = line[4:].strip()
            current = target[2:] if target.startswith("b/") else None
            if current is not None:
                out.setdefault(current, [])
            continue
        if current is not None and line.startswith("+") and not line.startswith("+++"):
            out[current].append(line)
    return out


def full_diff_check(*, base_sha: Any, head_sha: Any, files: Any, patch_text: Any,
                    declared_files: Any, local_modules: Iterable[str],
                    declared_dependencies: Iterable[str], repo_kind: str = "pickfair",
                    deleted_files: Iterable[str] = ()) -> dict[str, Any]:
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
    deps = {d.lower().replace("-", "_") for d in declared_dependencies}
    new_deps: set[str] = set()
    test_deps: set[str] = set()
    callers: list[str] = []
    mcp_violations: list[str] = []
    secret_content: set[str] = set()
    secret_content_tests: set[str] = set()
    for path, added in _patch_by_file(patch_text).items():
        is_py = path.endswith(".py")
        for line in added:
            if SECRET_CONTENT_RE.search(line):
                # In tests/ esistono chiavi SINTETICHE (fixture TLS, redazione):
                # non blocca ma porta la PR a merge owner, che verifica.
                (secret_content_tests if path.startswith("tests/") else secret_content).add(path)
            if is_py:
                for mod in _top_modules(line):
                    if mod and mod not in stdlib and mod not in local and mod.lower() not in deps:
                        (test_deps if path.startswith("tests/") else new_deps).add(mod)
                if (MONEY_PATH_CALL_RE.search(line) and not path.startswith("tests/")
                        and not line[1:].lstrip().startswith("#")):
                    callers.append(path)
            if is_py and repo_kind == "mcp" and MCP_FORBIDDEN_RE.search(line):
                mcp_violations.append(path)
    if secret_content:
        problems.append("secret_like_content_in_diff")
    if new_deps and not classification["dependency_manifest"]:
        problems.append("undeclared_new_dependency")
    if mcp_violations:
        problems.append("mcp_adapter_violation")
    if not stdlib:
        problems.append("stdlib_list_unavailable")
    status = "BLOCK" if problems else "PASS"
    if problems and set(problems) <= {"base_or_head_unknown", "patch_unreadable",
                                     "declared_scope_unknown", "stdlib_list_unavailable"}:
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
        "secret_like_content_tests": sorted(secret_content_tests),
        "owner_manual_merge_required": bool(classification["owner_manual"]
                                            or classification["dependency_manifest"]
                                            or secret_content_tests),
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
    if raw.get("pagination_complete") is False:
        return "UNKNOWN"
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
                     "manual_label_present", "preexisting_activated_or_aggravated")


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
    if gate == "STOP_OWNER":
        stops.append("reviewer_stop_owner")
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
    if state.get("fix_loop_exhausted") is True:
        stops.append("fix_loop_exhausted")
    if stops:
        return {"status": "NEEDS_MANUAL", "reasons": sorted(set(stops + reasons))}
    if reasons:
        return {"status": "BLOCKED", "reasons": sorted(set(reasons))}
    if full.get("owner_manual_merge_required") is not False:
        return {"status": "READY_FOR_OWNER_MANUAL_MERGE",
                "reasons": ["owner_manual_merge_files"]}
    return {"status": "READY_FOR_AUTO_MERGE", "reasons": []}


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
    open_prs = state.get("open_prs_same_repo")
    if not isinstance(open_prs, int) or isinstance(open_prs, bool):
        return {"status": "STOP_OWNER", "reason": "AUTHORIZATION_OR_GATE_NOT_VERIFIABLE"}
    if open_prs:
        return {"status": "BLOCKED_OPEN_PR", "reason": "una sola PR attiva per repository"}
    stops = [s for s in state.get("active_stop_conditions") or [] if s in OWNER_STOP_CONDITIONS]
    unknown_stops = [s for s in state.get("active_stop_conditions") or [] if s not in OWNER_STOP_CONDITIONS]
    if stops or unknown_stops:
        return {"status": "STOP_OWNER", "reason": ",".join(stops + unknown_stops)}
    card = state.get("next_card")
    if not _nonempty_str(card):
        return {"status": "STOP_OWNER", "reason": "prossima scheda non determinabile dalla roadmap"}
    if state.get("pertinent_open_owner_decisions"):
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
    if mcp.get("exposes_mutation") and not mcp.get("core_authority_ready"):
        return "FORBIDDEN_PARALLEL"
    if pickfair.get("changes_control_api_contract") and mcp.get("consumes_control_api_contract"):
        return "FORBIDDEN_PARALLEL"
    needs = set(mcp.get("depends_on") or []) | set(pickfair.get("depends_on") or [])
    unmerged = set(pickfair.get("unmerged_outputs") or []) | set(mcp.get("unmerged_outputs") or [])
    if needs & unmerged:
        return "DEPENDENT"
    return "SAFE_PARALLEL"


def owner_stop_required(conditions: Iterable[str]) -> dict[str, Any]:
    conditions = list(conditions)
    unknown = [c for c in conditions if c not in OWNER_STOP_CONDITIONS]
    active = [c for c in conditions if c in OWNER_STOP_CONDITIONS]
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


_SKIP_DIRS = frozenset({".git", "venv", ".venv", "node_modules", "__pycache__", "build", "dist"})


def _local_modules(root: str) -> set[str]:
    """Moduli e package del repository (anche sotto scripts/ e tests/)."""
    import os
    mods: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS and not d.startswith(".")]
        mods.update(dirnames)
        mods.update(f[:-3] for f in filenames if f.endswith(".py"))
    return mods


def _task_key(title: str) -> str | None:
    m = re.search(r"\[TASK:\s*([^\]\s]+)\s*\]", title or "")
    return m.group(1) if m else None


def _load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _cmd_full_diff(args: argparse.Namespace) -> int:
    import glob
    import os
    try:
        meta = _load_json(args.meta)
        raw_files = _load_json(args.files)
        scope = _load_json(args.scope)
        with open(args.patch, encoding="utf-8", errors="replace") as fh:
            patch_text = fh.read()
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "UNKNOWN", "problems": [f"input_unreadable:{type(exc).__name__}"]}))
        return 1
    files = raw_files if isinstance(raw_files, list) else (raw_files or {}).get("files")
    key = _task_key(str((meta or {}).get("title") or ""))
    task = ((scope or {}).get("tasks") or {}).get(key) if key else None
    declared = task.get("files") if isinstance(task, dict) else None
    req_files = sorted(glob.glob("requirements*.txt")) + sorted(glob.glob("requirements*.in"))
    deps = _declared_dependencies(req_files)
    result = full_diff_check(
        base_sha=args.base_sha, head_sha=args.head_sha, files=files, patch_text=patch_text,
        declared_files=declared, local_modules=_local_modules("."), declared_dependencies=deps,
        repo_kind=args.repo_kind,
        deleted_files=[f for f in files or [] if isinstance(f, str) and not os.path.lexists(f)],
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
    for name in ("classify-finding", "merge-decision", "next-pr"):
        p = sub.add_parser(name)
        p.add_argument("--input", required=True, help="file JSON con le prove")
    args = parser.parse_args(argv)
    if args.cmd == "full-diff":
        return _cmd_full_diff(args)
    func = {"classify-finding": classify_finding, "merge-decision": merge_decision,
            "next-pr": next_pr_decision}[args.cmd]
    return _cmd_eval(func, args)


if __name__ == "__main__":
    sys.exit(main())
