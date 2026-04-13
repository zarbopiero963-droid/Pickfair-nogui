from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)


class ForensicsEngine:
    def __init__(self, rules: Iterable[Any]) -> None:
        self.rules = list(rules)
        self.state: Dict[str, Dict[str, Any]] = {}
        self.rule_errors: Dict[str, int] = {}

    def evaluate(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        findings: List[Dict[str, Any]] = []
        for rule in self.rules:
            rule_name = getattr(rule, "__name__", "rule")
            rule_state = self.state.setdefault(rule_name, {})
            try:
                item = rule(context, rule_state)
            except Exception:
                self.rule_errors[rule_name] = self.rule_errors.get(rule_name, 0) + 1
                logger.exception(
                    "forensics rule %s raised — isolated, continuing pass",
                    rule_name,
                )
                continue
            if item:
                findings.append(item)
        return findings
