#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

CHANGED: list[str] = []


def write_if_changed(path: Path, new_text: str) -> None:
    old_text = path.read_text(encoding="utf-8") if path.exists() else ""
    if old_text != new_text:
        path.write_text(new_text, encoding="utf-8")
        CHANGED.append(str(path))


def ensure_method_docstring(text: str, name: str, doc: str) -> str:
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = re.match(rf"^(\s*)def {re.escape(name)}\b", line)
        if not m:
            continue

        # Find the end of a possibly multi-line signature.
        j = i
        while j < len(lines) and not lines[j].rstrip().endswith(":"):
            j += 1
        if j >= len(lines):
            return text

        body_indent = m.group(1) + "    "
        k = j + 1
        while k < len(lines) and not lines[k].strip():
            k += 1

        if k < len(lines) and lines[k].lstrip().startswith(('"""', "'''")):
            return text

        lines.insert(j + 1, f'{body_indent}"""{doc}"""')
        return "\n".join(lines) + ("\n" if text.endswith("\n") else "")

    return text


def fix_pnl_engine() -> None:
    path = Path("pnl_engine.py")
    if not path.exists():
        return

    text = path.read_text(encoding="utf-8")

    # DeepSource: _finite_float does not use self.
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = re.match(r"^(\s*)def _finite_float\(self,\s*", line)
        if not m:
            continue

        indent = m.group(1)
        lines[i] = re.sub(
            r"def _finite_float\(self,\s*",
            "def _finite_float(",
            line,
            count=1,
        )
        if indent and (i == 0 or lines[i - 1].strip() != "@staticmethod"):
            lines.insert(i, f"{indent}@staticmethod")
        break

    text = "\n".join(lines) + ("\n" if text.endswith("\n") else "")

    # Codacy/CodeRabbit: do not store a non-finite commission_pct in state.
    text = re.sub(
        r"(?m)^(\s*)self\.commission_pct\s*=\s*float\(commission_pct\s+or\s+0\.0\)\s*$",
        r"\1self.commission_pct = self._finite_float(commission_pct, default=0.0)",
        text,
        count=1,
    )
    text = re.sub(
        r"(?m)^(\s*)self\.commission_pct\s*=\s*commission_pct\s*$",
        r"\1self.commission_pct = self._finite_float(commission_pct, default=0.0)",
        text,
        count=1,
    )

    # Codacy: sanitize fallback self.commission_pct too.
    text = re.sub(
        r"(?m)^(\s*)pct\s*=\s*self\.commission_pct\s+if\s+commission_pct\s+is\s+None\s+else\s+self\._finite_float\(commission_pct,\s*default=0\.0\)\s*$",
        (
            r"\1base_pct = self._finite_float(self.commission_pct, default=0.0)\n"
            r"\1pct = base_pct if commission_pct is None else self._finite_float(commission_pct, default=base_pct)"
        ),
        text,
        count=1,
    )
    text = re.sub(
        r"(?m)^(\s*)pct\s*=\s*self\._finite_float\(commission_pct,\s*default=0\.0\)\s+if\s+commission_pct\s+is\s+not\s+None\s+else\s+self\.commission_pct\s*$",
        (
            r"\1base_pct = self._finite_float(self.commission_pct, default=0.0)\n"
            r"\1pct = self._finite_float(commission_pct, default=base_pct) if commission_pct is not None else base_pct"
        ),
        text,
        count=1,
    )

    docs = {
        "__init__": "Initialize the PnL engine with a finite commission percentage.",
        "_finite_float": "Convert a numeric value to a finite float or raise ValueError.",
        "_safe_side": "Normalize and validate a betting side value.",
        "_commission_amount": "Calculate commission for a positive gross profit amount.",
        "_resolve_policy_commission_pct": "Resolve the effective finite commission percentage.",
        "mark_to_market_pnl": "Calculate mark-to-market PnL for an open position.",
        "calculate_position_pnl": "Calculate preview PnL for a position using finite numeric inputs.",
        "calculate_settlement_pnl": "Calculate settlement PnL using finite numeric inputs.",
        "calculate_green_up_size": "Calculate hedge size using finite green-up inputs.",
    }
    for name, doc in docs.items():
        text = ensure_method_docstring(text, name, doc)

    write_if_changed(path, text)


def fix_dutching() -> None:
    path = Path("dutching.py")
    if not path.exists():
        return

    text = path.read_text(encoding="utf-8")

    # Codacy: unused import.
    if "math." not in text:
        text = re.sub(r"(?m)^import math\n", "", text)

    # Codacy: force Decimal start value for inv_sum.
    replacements = [
        r'(?m)^(\s*)inv_sum\s*=\s*sum\(\s*Decimal\("1"\)\s*/\s*o\s+for\s+o\s+in\s+odds_d\s*\)\s*$',
        r'(?m)^(\s*)inv_sum\s*=\s*sum\(\s*\(Decimal\("1"\)\s*/\s*o\)\s+for\s+o\s+in\s+odds_d\s*\)\s*$',
        r'(?m)^(\s*)inv_sum\s*=\s*sum\(\s*\(Decimal\("1"\)\s*/\s*o\s+for\s+o\s+in\s+odds_d\)\s*\)\s*$',
    ]
    for pattern in replacements:
        text = re.sub(
            pattern,
            r'\1inv_sum = sum((Decimal("1") / o for o in odds_d), Decimal("0"))',
            text,
            count=1,
        )

    write_if_changed(path, text)


def ensure_import(text: str, import_line: str) -> str:
    if import_line in text:
        return text
    return import_line + "\n" + text


def append_tests_if_missing() -> None:
    path = Path("tests/unit/test_pr5_numeric_guardrails.py")
    if not path.exists():
        return

    text = path.read_text(encoding="utf-8")
    text = ensure_import(text, "import pytest")
    text = ensure_import(text, "from dutching import calculate_dutching_stakes")
    text = ensure_import(text, "from pnl_engine import PnLEngine")

    text = re.sub(
        r"(?m)^(\s*)if decision\.recommended_stake:\n\1    pytest\.fail\([^\n]*\)\n",
        r'\1assert decision.recommended_stake == pytest.approx(0.0), "recommended stake must be exactly zero"\n',
        text,
    )

    text = re.sub(
        r"(?m)^(\s*)assert result\.gross_pnl == (?!pytest\.approx\()([^\n#]+?)(\s*(?:#.*)?)$",
        r"\1assert result.gross_pnl == pytest.approx(\2)\3",
        text,
    )

    additions = []

    if "def test_dutching_rejects_non_finite_string_odds" not in text:
        additions.append(
            r'''
@pytest.mark.unit
@pytest.mark.parametrize(
    "odds",
    [
        ["NaN", "2.0"],
        ["nan", "2.0"],
        [" INF ", "2.0"],
        ["+infinity", "2.0"],
        ["-INF", "2.0"],
    ],
)
def test_dutching_rejects_non_finite_string_odds(odds):
    result = calculate_dutching_stakes(odds, "100.0")

    assert result["stakes"] == []
    assert "Invalid odds" in (result.get("error") or "")
'''
        )

    if "def test_dutching_rejects_non_finite_string_stakes" not in text:
        additions.append(
            r'''
@pytest.mark.unit
@pytest.mark.parametrize(
    "stake",
    [
        "NaN",
        "nan",
        " INF ",
        "+infinity",
        "-INF",
    ],
)
def test_dutching_rejects_non_finite_string_stakes(stake):
    result = calculate_dutching_stakes(["2.0", "3.0"], stake)

    assert result["stakes"] == []
    assert "Invalid stake" in (result.get("error") or "")
'''
        )

    if "def test_dutching_accepts_normalized_string_inputs" not in text:
        additions.append(
            r'''
@pytest.mark.unit
def test_dutching_accepts_normalized_string_inputs():
    result = calculate_dutching_stakes(["2,0", " 3,0 "], " 100,0 ")

    assert result["stakes"]
    assert not (result.get("error") or "")
'''
        )

    if "def test_pnl_engine_rejects_non_finite_commission_pct" not in text:
        additions.append(
            r'''
@pytest.mark.unit
def test_pnl_engine_rejects_non_finite_commission_pct():
    with pytest.raises(ValueError):
        PnLEngine(commission_pct=float("nan"))
'''
        )

    if additions:
        text = text.rstrip() + "\n\n" + "\n".join(additions).strip() + "\n"

    write_if_changed(path, text)


def update_guardrail_json() -> None:
    test_path = "tests/unit/test_pr5_numeric_guardrails.py"
    targets = [
        Path("guardrails/specs/dutching.json"),
        Path("guardrails/specs/pnl_engine.json"),
        Path("guardrails/state_models/dutching.json"),
        Path("guardrails/state_models/pnl_engine.json"),
    ]

    for path in targets:
        if not path.exists():
            continue

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue

        focused = data.get("focused_tests")
        if not isinstance(focused, list):
            continue

        if test_path not in focused:
            focused.append(test_path)
            write_if_changed(
                path,
                json.dumps(data, ensure_ascii=False, indent=2) + "\n",
            )


def main() -> int:
    fix_pnl_engine()
    fix_dutching()
    append_tests_if_missing()
    update_guardrail_json()

    if CHANGED:
        print("Deterministic fallback changed:")
        for path in CHANGED:
            print(f"- {path}")
    else:
        print("No deterministic fallback changes were necessary.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
