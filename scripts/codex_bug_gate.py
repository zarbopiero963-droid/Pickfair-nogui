#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from fnmatch import fnmatch
from typing import Any, Dict, List, Set, Tuple


APPROVED_BUG_CLASSES = {
    "state_persistence",
    "contract_mismatch",
    "state_machine_violation",
    "data_loss",
    "idempotency_break",
    "dedup_break",
    "stale_state_false_positive",
    "recovery_gap",
    "alert_lifecycle_bug",
    "filename_collision",
    "null_handling_bug",
    "time_window_bug",
}

HIGH_IMPACT_BUG_CLASSES = {
    "state_persistence",
    "contract_mismatch",
    "state_machine_violation",
    "data_loss",
    "idempotency_break",
    "dedup_break",
    "recovery_gap",
    "alert_lifecycle_bug",
    "stale_state_false_positive",
    "filename_collision",
}

CRITICAL_FILE_PATTERNS = [
    "database.py",
    "betfair_client.py",
    "order_manager.py",
    "safe_mode.py",
    "safe_mode_manager.py",
    "core/trading_engine.py",
    "core/risk_middleware.py",
    "core/safety_layer.py",
    "core/money_management.py",
    "core/risk_desk.py",
    "core/table_manager.py",
    "core/system_state.py",
    "core/duplication_guard.py",
    "observability/runtime_probe.py",
    "observability/diagnostic_bundle_builder.py",
]

SOFT_FILE_PATTERNS = [
    "*.md",
    "docs/*",
]

STRONG_ASSERTIVE_TERMS = {
    "drops",
    "discard",
    "overwrites",
    "silently",
    "always reloads defaults",
    "stale",
    "regardless of how old",
    "cannot actually configure",
    "miss degraded periods",
    "loses",
    "incorrectly",
    "violates",
    "marks",
    "treats",
    "silently dropped",
    "reducing signal quality",
    "silently discard",
}

WEAK_SPECULATIVE_TERMS = {
    "could",
    "might",
    "maybe",
    "consider",
    "cleaner",
    "improve",
    "more robust",
    "refactor",
    "style",
    "readability",
    "nit",
    "potentially",
    "possibly",
}

CONCRETE_IMPACT_TERMS = {
    "silently",
    "discard",
    "drops",
    "overwrites",
    "reloads defaults",
    "cannot actually configure",
    "miss degraded periods",
    "reducing signal quality",
    "data loss",
    "stale",
    "false positive",
    "treated as recent",
    "wrongly treated as recent",
}

REPRO_HINT_TERMS = {
    "on the next read",
    "on restart",
    "within the same second",
    "whenever any snapshot row exists",
    "regardless of how old",
    "two exports within the same second",
}


@dataclass
class ScoredFinding:
    id: str
    title: str
    path: str
    line: Any
    bug_class: str
    outdated: bool
    is_critical_file: bool
    is_high_impact_class: bool
    is_touched_file: bool
    score: int
    status: str
    reasons: List[str]
    url: str


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _contains_any(text: str, terms: set[str]) -> bool:
    t = (text or "").lower()
    return any(term in t for term in terms)


def _path_matches(path: str, patterns: List[str]) -> bool:
    return any(fnmatch(path, pat) for pat in patterns)


def _is_critical_file(path: str) -> bool:
    return _path_matches(path or "", CRITICAL_FILE_PATTERNS)


def _is_soft_file(path: str) -> bool:
    return _path_matches(path or "", SOFT_FILE_PATTERNS)


def _has_file(f: Dict[str, Any]) -> bool:
    return bool((f.get("path") or "").strip())


def _has_precise_location(f: Dict[str, Any]) -> bool:
    return _has_file(f) and f.get("line") is not None


def _has_concrete_impact(f: Dict[str, Any]) -> bool:
    body = (f.get("body") or "") + " " + (f.get("claim") or "")
    return _contains_any(body, CONCRETE_IMPACT_TERMS)


def _has_repro_hint(f: Dict[str, Any]) -> bool:
    body = (f.get("body") or "").lower()
    return _contains_any(body, REPRO_HINT_TERMS)


def _load_changed_files(path: str | None) -> Set[str]:
    if not path:
        return set()
    data = _load_json(path)
    files = data.get("files") or []
    return {str(x).strip() for x in files if str(x).strip()}


def _score_finding(f: Dict[str, Any], changed_files: Set[str]) -> Tuple[int, List[str], bool, bool, bool]:
    score = 0
    reasons: List[str] = []

    path = f.get("path") or ""
    body = (f.get("body") or "") + " " + (f.get("claim") or "")
    bug_class = f.get("bug_class") or "unknown"
    outdated = bool(f.get("outdated", False))

    is_critical_file = _is_critical_file(path)
    is_high_impact_class = bug_class in HIGH_IMPACT_BUG_CLASSES
    is_touched_file = path in changed_files if changed_files else False

    if outdated:
        score -= 100
        reasons.append("outdated comment")

    if _is_soft_file(path):
        score -= 20
        reasons.append("soft/non-critical file")

    if _has_file(f):
        score += 10
        reasons.append("has file path")
    else:
        score -= 30
        reasons.append("missing file path")

    if _has_precise_location(f):
        score += 10
        reasons.append("has precise line/location")
    else:
        score -= 10
        reasons.append("no precise line/location")

    if bug_class in APPROVED_BUG_CLASSES:
        score += 15
        reasons.append(f"approved bug class: {bug_class}")
    else:
        score -= 30
        reasons.append("unknown/unapproved bug class")

    if is_high_impact_class:
        score += 20
        reasons.append("high-impact bug class")

    if is_critical_file:
        score += 20
        reasons.append("critical file")

    if is_touched_file:
        score += 15
        reasons.append("file modified by current PR")
    else:
        score -= 20
        reasons.append("file not modified by current PR")

    if _contains_any(body, STRONG_ASSERTIVE_TERMS):
        score += 20
        reasons.append("assertive/concrete language")
    else:
        score -= 10
        reasons.append("no strong assertive language")

    if _contains_any(body, WEAK_SPECULATIVE_TERMS):
        score -= 25
        reasons.append("speculative wording detected")

    if _has_concrete_impact(f):
        score += 20
        reasons.append("concrete impact present")
    else:
        score -= 15
        reasons.append("impact not concrete enough")

    if _has_repro_hint(f):
        score += 15
        reasons.append("reproduction hint present")
    else:
        score -= 10
        reasons.append("no reproduction hint")

    return score, reasons, is_critical_file, is_high_impact_class, is_touched_file


def _classify(score: int, outdated: bool) -> str:
    if outdated:
        return "NOISE"
    if score >= 75:
        return "REAL_BUG"
    if score >= 45:
        return "MANUAL_REVIEW"
    return "NOISE"


def _should_fail(scored: ScoredFinding) -> bool:
    return (
        scored.status == "REAL_BUG"
        and scored.is_high_impact_class
        and scored.is_critical_file
        and scored.is_touched_file
        and not scored.outdated
    )


def _summarize(scored: List[ScoredFinding]) -> Dict[str, Any]:
    accepted = [asdict(x) for x in scored if x.status == "REAL_BUG"]
    review = [asdict(x) for x in scored if x.status == "MANUAL_REVIEW"]
    noise = [asdict(x) for x in scored if x.status == "NOISE"]
    fail_findings = [asdict(x) for x in scored if _should_fail(x)]

    return {
        "accepted": accepted,
        "review": review,
        "noise": noise,
        "fail_findings": fail_findings,
        "counts": {
            "REAL_BUG": len(accepted),
            "MANUAL_REVIEW": len(review),
            "NOISE": len(noise),
            "FAIL_FINDINGS": len(fail_findings),
        },
    }


def _build_markdown(result: Dict[str, Any]) -> str:
    counts = result["counts"]
    lines: List[str] = []

    lines.append("## Codex Bug Gate")
    lines.append("")
    lines.append(f"- REAL_BUG: **{counts['REAL_BUG']}**")
    lines.append(f"- MANUAL_REVIEW: **{counts['MANUAL_REVIEW']}**")
    lines.append(f"- NOISE: **{counts['NOISE']}**")
    lines.append(f"- FAIL_FINDINGS: **{counts['FAIL_FINDINGS']}**")
    lines.append("")

    if result["fail_findings"]:
        lines.append("### Blocking high-impact findings on critical files touched by this PR")
        lines.append("")
        for item in result["fail_findings"]:
            lines.append(
                f"- **{item['title']}** — `{item['bug_class']}` — `{item['path']}:{item['line']}` — score `{item['score']}` — [review]({item['url']})"
            )
        lines.append("")

    if result["review"]:
        lines.append("### Manual review findings")
        lines.append("")
        for item in result["review"][:10]:
            lines.append(
                f"- **{item['title']}** — `{item['bug_class']}` — `{item['path']}:{item['line']}` — score `{item['score']}` — [review]({item['url']})"
            )
        lines.append("")

    if not result["accepted"] and not result["review"] and not result["fail_findings"]:
        lines.append("No blocking Codex findings detected after gate filtering.")
        lines.append("")

    lines.append("<sub>Generated by codex-bug-gate final hedge-fund grade.</sub>")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json", help="Path to codex_findings.json")
    parser.add_argument("--changed-files-json", default="changed_files.json")
    parser.add_argument("--output", default="codex_gate_result.json")
    parser.add_argument("--comment-output", default="codex_gate_comment.md")
    parser.add_argument("--fail-on-real", action="store_true")
    args = parser.parse_args()

    data = _load_json(args.input_json)
    findings = data.get("findings") or []
    changed_files = _load_changed_files(args.changed_files_json)

    scored: List[ScoredFinding] = []
    for f in findings:
        score, reasons, is_critical_file, is_high_impact_class, is_touched_file = _score_finding(
            f, changed_files
        )
        status = _classify(score, bool(f.get("outdated", False)))
        scored.append(
            ScoredFinding(
                id=f.get("id", ""),
                title=f.get("title", ""),
                path=f.get("path", ""),
                line=f.get("line"),
                bug_class=f.get("bug_class", "unknown"),
                outdated=bool(f.get("outdated", False)),
                is_critical_file=is_critical_file,
                is_high_impact_class=is_high_impact_class,
                is_touched_file=is_touched_file,
                score=score,
                status=status,
                reasons=reasons,
                url=f.get("url", ""),
            )
        )

    result = _summarize(scored)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    with open(args.comment_output, "w", encoding="utf-8") as f:
        f.write(_build_markdown(result))

    print(json.dumps(result["counts"], indent=2))

    if args.fail_on_real and result["counts"]["FAIL_FINDINGS"] > 0:
        print("Failing because blocking high-impact Codex findings were detected on critical files touched by this PR.")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())