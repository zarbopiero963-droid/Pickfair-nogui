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
        return anomalies
