from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


REQUIRED_KINDS = ("specs", "contracts", "state_models", "mutations")


def find_repo_root(start: Path | None = None) -> Path:
    base = (start or Path(__file__).resolve()).resolve()
    current = base if base.is_dir() else base.parent
    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists() or (candidate / "pyproject.toml").exists():
            return candidate
        if (candidate / ".github").exists() and (candidate / "guardrails").exists():
            return candidate
    raise RuntimeError("Could not detect repository root from script location")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON must be object: {path}")
    return payload


def validate_module_guardrails(
    module: str,
    guardrails_root: Path,
    repo_root: Path,
    fail_on_missing_tests: bool = True,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    checked_files: list[str] = []

    for kind in REQUIRED_KINDS:
        path = guardrails_root / kind / f"{module}.json"
        checked_files.append(str(path))
        if not path.exists():
            errors.append(f"Missing required guardrail file: {path}")
            continue

        try:
            payload = _load_json(path)
        except Exception as exc:
            errors.append(f"Invalid JSON in {path}: {exc}")
            continue

        if payload.get("module") != module:
            errors.append(f"Module mismatch in {path}: expected '{module}'")

        module_paths = payload.get("module_paths")
        if not isinstance(module_paths, list) or not module_paths:
            errors.append(f"module_paths missing/empty in {path}")
        else:
            for rel in module_paths:
                rel_str = str(rel).strip()
                if not rel_str:
                    errors.append(f"Empty module_paths entry in {path}")
                    continue
                if not (repo_root / rel_str).exists():
                    errors.append(f"Missing module path '{rel_str}' referenced by {path}")

        focused_tests = payload.get("focused_tests")
        if focused_tests is not None and not isinstance(focused_tests, list):
            errors.append(f"focused_tests must be list in {path}")
        elif fail_on_missing_tests and isinstance(focused_tests, list):
            for rel in focused_tests:
                rel_str = str(rel).strip()
                if rel_str and not (repo_root / rel_str).exists():
                    errors.append(f"Missing focused test '{rel_str}' referenced by {path}")

        if kind == "mutations":
            mutations = payload.get("mutations")
            if not isinstance(mutations, list) or not mutations:
                errors.append(f"mutations missing/empty in {path}")
            else:
                for idx, entry in enumerate(mutations):
                    if not isinstance(entry, dict):
                        errors.append(f"mutation[{idx}] is not object in {path}")
                        continue
                    if not str(entry.get("id", "")).strip():
                        errors.append(f"mutation[{idx}] missing id in {path}")
                    if "expected_failure" not in entry:
                        errors.append(f"mutation[{idx}] missing expected_failure in {path}")

    return {
        "module": module,
        "ok": not errors,
        "checked_files": checked_files,
        "errors": errors,
        "warnings": warnings,
    }


def run_mutation_delegate(module: str, timeout_sec: float, repo_root: Path) -> dict[str, Any]:
    script = repo_root / "scripts" / "run_mutation_guardrails.py"
    if not script.exists():
        return {"ok": False, "error": f"Missing delegate script: {script}"}

    with tempfile.NamedTemporaryFile(prefix="guardrail_runner_mut_", suffix=".json", delete=False) as tmp:
        output_path = Path(tmp.name)

    cmd = [
        sys.executable,
        str(script),
        "--repo-root",
        str(repo_root),
        "--module",
        module,
        "--timeout-sec",
        str(int(timeout_sec) if float(timeout_sec).is_integer() else timeout_sec),
        "--output",
        str(output_path),
    ]
    proc = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True)

    result: dict[str, Any] = {
        "ok": proc.returncode == 0,
        "return_code": proc.returncode,
        "output": str(output_path),
        "stdout": proc.stdout[-2000:],
        "stderr": proc.stderr[-2000:],
    }
    try:
        payload = _load_json(output_path)
        summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
        total = summary.get("total")
        result["summary"] = summary
        if total == 0:
            result["ok"] = False
            result["error"] = "Mutation delegate produced total=0"
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"Failed reading mutation delegate output: {exc}"
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and optionally run module guardrails.")
    parser.add_argument("--module", required=True)
    parser.add_argument("--guardrails-root", default="guardrails")
    parser.add_argument("--run-mutations", action="store_true")
    parser.add_argument("--mutation-timeout-sec", type=float, default=1)
    parser.add_argument("--output")
    parser.add_argument("--fail-on-missing-tests", default="true", choices=["true", "false"])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    repo_root = find_repo_root()
    fail_on_missing_tests = args.fail_on_missing_tests.lower() == "true"
    report = validate_module_guardrails(
        module=args.module,
        guardrails_root=(repo_root / args.guardrails_root),
        repo_root=repo_root,
        fail_on_missing_tests=fail_on_missing_tests,
    )

    if args.run_mutations:
        mutation_result = run_mutation_delegate(args.module, args.mutation_timeout_sec, repo_root)
        report["mutation_result"] = mutation_result
        if not mutation_result.get("ok"):
            report["errors"].append(f"Mutation delegation failed: {mutation_result.get('error', 'unknown')}")
            report["ok"] = False

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(f"Guardrail runner module={args.module} ok={report['ok']}")
    if report["errors"]:
        print("Errors:")
        for err in report["errors"]:
            print(f" - {err}")

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
