from __future__ import annotations

from typing import Any, Dict, Iterable, List


class AnomalyEngine:
    def __init__(self, rules: Iterable[Any]) -> None:
        self.rules = list(rules)
        self.state: Dict[str, Dict[str, Any]] = {}

    def evaluate(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        anomalies: List[Dict[str, Any]] = []
        for rule in self.rules:
            rule_name = getattr(rule, "__name__", "rule")
            rule_state = self.state.setdefault(rule_name, {})
            item = rule(context, rule_state)
            if item:
                anomalies.append(item)
        return self._apply_progressions(anomalies)

    def _apply_progressions(self, anomalies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply deterministic anomaly progressions used by the reviewer."""
        has_detected = any(
            str(item.get("code", "") or "") == "GHOST_ORDER_DETECTED"
            for item in anomalies
        )
        if not has_detected:
            return anomalies

        progressed: List[Dict[str, Any]] = []
        for item in anomalies:
            code = str(item.get("code", "") or "")
            if code == "GHOST_ORDER_SUSPECTED":
                continue
            if code == "GHOST_ORDER_DETECTED":
                details = item.get("details")
                if isinstance(details, dict):
                    enriched = dict(details)
                    enriched["progressed_from"] = "GHOST_ORDER_SUSPECTED"
                    item = dict(item)
                    item["details"] = enriched
            progressed.append(item)
        return progressed
