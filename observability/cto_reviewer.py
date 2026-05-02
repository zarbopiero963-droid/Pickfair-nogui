from __future__ import annotations

import time
from collections import deque
from typing import Any, Deque, Dict, List, Tuple

from .cto_rules import evaluate_cto_rules


_SEVERITY_RANK = {"info": 10, "warning": 20, "high": 30, "critical": 40, "error": 40}


class CtoReviewer:
    def __init__(self, *, history_window: int = 6, cooldown_sec: int = 60) -> None:
        self.history_window = max(2, int(history_window or 2))
        self.cooldown_sec = max(0, int(cooldown_sec or 0))
        self._history: Deque[Dict[str, Any]] = deque(maxlen=self.history_window)
        self._last_emit: Dict[Tuple[str, str], float] = {}

    def evaluate(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        now = float(payload.get("now_ts") or time.time())
        self._history.append(dict(payload))
        enriched = self._build_rule_payload(payload)
        findings = evaluate_cto_rules(enriched)

        counts: Dict[str, int] = {}
        for snap in self._history:
            snap_findings = evaluate_cto_rules(self._build_rule_payload(snap))
            for finding in snap_findings:
                key = str(finding.get("rule_name") or "")
                counts[key] = counts.get(key, 0) + 1

        out: List[Dict[str, Any]] = []
        for finding in findings:
            rule_name = str(finding.get("rule_name") or "UNKNOWN")
            runtime_probe = payload.get("runtime_probe_state") or payload.get("runtime_probe") or {}
            context_key = str((runtime_probe if isinstance(runtime_probe, dict) else {}).get("component") or payload.get("component") or "global")
            emit_key = (rule_name, context_key)
            has_last = emit_key in self._last_emit
            last_ts = float(self._last_emit.get(emit_key, 0.0) or 0.0)
            if has_last and self.cooldown_sec > 0 and (now - last_ts) < self.cooldown_sec:
                continue

            evidence_count = int(counts.get(rule_name, 0))
            severity = str(finding.get("severity", "warning") or "warning").lower()
            if evidence_count >= 3:
                severity = self._bump(severity)

            evidence_summary = {
                "rule_hits_in_window": evidence_count,
                "history_window": len(self._history),
            }

            item = {
                **finding,
                "rule_name": rule_name,
                "severity": severity,
                "timestamp": now,
                # Backward-compatible aliases derived from canonical evidence_summary.
                "evidence_count": evidence_summary["rule_hits_in_window"],
                "history_size": evidence_summary["history_window"],
                "evidence_summary": evidence_summary,
                "reasoning_payload": {
                    "anomaly_alert_count": len(payload.get("anomaly_alerts") or []),
                    "forensics_alert_count": len(payload.get("forensics_alerts") or []),
                    "open_incidents": int((payload.get("incidents_snapshot") or {}).get("open_count", 0) or 0),
                    "anomaly_codes": [str(x.get("code") or "") for x in (payload.get("anomaly_alerts") or []) if str(x.get("code") or "")],
                    "forensics_codes": [str(x.get("code") or "") for x in (payload.get("forensics_alerts") or []) if str(x.get("code") or "")],
                },
            }
            self._last_emit[emit_key] = now
            out.append(item)
        return out

    def current_rule_names(self, payload: Dict[str, Any]) -> set[str]:
        """Return currently-active CTO rule names without cooldown suppression."""
        findings = evaluate_cto_rules(self._build_rule_payload(payload))
        return {str(item.get("rule_name") or "") for item in findings if str(item.get("rule_name") or "")}

    def _build_rule_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metrics_snapshot = dict(payload.get("metrics_snapshot") or {})
        gauges = dict(metrics_snapshot.get("gauges") or metrics_snapshot)
        diagnostics = dict(payload.get("diagnostics_bundle") or {})
        if not diagnostics and payload.get("diagnostics_metadata"):
            diagnostics = dict(payload.get("diagnostics_metadata") or {})
        return {
            "health": dict(payload.get("health_snapshot") or {}),
            "metrics": gauges,
            "anomaly_alerts": list(payload.get("anomaly_alerts") or []),
            "forensics_alerts": list(payload.get("forensics_alerts") or []),
            "incidents": dict(payload.get("incidents_snapshot") or {}),
            "runtime_probe": dict(payload.get("runtime_probe_state") or payload.get("runtime_probe") or {}),
            "diagnostics": diagnostics,
        }

    def _bump(self, severity: str) -> str:
        current = _SEVERITY_RANK.get(severity, 20)
        if current >= 40:
            return "critical"
        if current >= 30:
            return "critical"
        if current >= 20:
            return "high"
        return "warning"
