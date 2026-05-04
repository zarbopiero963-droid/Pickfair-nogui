from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


REQUIRED_KINDS = ("specs", "contracts", "state_models", "mutations")
_MODULE_ARG_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


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


def _validate_relative_file_path(value: Any, *, repo_root: Path, source: Path, label: str) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return f"Missing {label} path in {source}"
    rel_str = value.strip()
    rel_path = Path(rel_str)
    if rel_path.is_absolute():
        return f"{label}: path is absolute: {rel_str}"
    resolved = (repo_root / rel_path).resolve()
    try:
        resolved.relative_to(repo_root.resolve())
    except ValueError:
        return f"{label} path '{rel_str}' escapes repo_root"
    if not resolved.exists():
        return f"Missing {label} path '{rel_str}' referenced by {source}"
    if not resolved.is_file():
        return f"{label} path '{rel_str}' not a file"
    return None


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
                err = _validate_relative_file_path(rel, repo_root=repo_root, source=path, label="module_paths")
                if err:
                    errors.append(err)

        focused_tests = payload.get("focused_tests")
        if focused_tests is not None and not isinstance(focused_tests, list):
            errors.append(f"focused_tests must be list in {path}")
        elif fail_on_missing_tests and isinstance(focused_tests, list):
            for rel in focused_tests:
                err = _validate_relative_file_path(rel, repo_root=repo_root, source=path, label="focused_tests")
                if err:
                    errors.append(err)

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


def run_mutation_delegate(module: str, timeout_sec: int, repo_root: Path) -> dict[str, Any]:
    if not isinstance(module, str) or not module.strip() or not _MODULE_ARG_RE.fullmatch(module.strip()):
        return {"ok": False, "fatal": True, "error": f"Invalid module name: {module!r}"}
    safe_module = module.strip()
    resolved_repo_root = repo_root.resolve()
    script = (resolved_repo_root / "scripts" / "run_mutation_guardrails.py").resolve()
    if not script.exists():
        return {"ok": False, "fatal": True, "error": f"Missing delegate script: {script}"}
    if not script.is_file():
        return {"ok": False, "fatal": True, "error": f"Delegate script is not a file: {script}"}
    try:
        script.relative_to(resolved_repo_root)
    except ValueError:
        return {"ok": False, "fatal": True, "error": f"Delegate script is not repo-local: {script}"}

    with tempfile.TemporaryDirectory(prefix="guardrail_runner_mut_") as tmp_dir:
        output_path = Path(tmp_dir) / "report.json"
        cmd = [
            sys.executable,
            str(script),
            "--repo-root",
            str(resolved_repo_root),
            "--module",
            safe_module,
            "--timeout-sec",
            str(timeout_sec),
            "--output",
            str(output_path),
        ]
        try:
            wrapper_timeout = max(timeout_sec + 30, timeout_sec * 4)
            # Security: command is an argv list, shell=False (default), with repo-local resolved script path.
            # Module input is allowlist-validated and passed as argv (no shell interpolation).
            # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
            proc = subprocess.run(
                cmd,
                cwd=str(resolved_repo_root),
                capture_output=True,
                text=True,
                timeout=wrapper_timeout,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "fatal": True,
                "error": f"Mutation delegate wrapper timeout after {wrapper_timeout}s",
            }

        result: dict[str, Any] = {
            "ok": False,
            "fatal": True,
            "return_code": proc.returncode,
            "stdout": proc.stdout[-2000:],
            "stderr": proc.stderr[-2000:],
        }
        try:
            payload = _load_json(output_path)
        except Exception as exc:
            result["error"] = f"Failed reading mutation delegate output: {exc}"
            return result

    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    total = summary.get("total")
    killed = summary.get("killed")
    survived = summary.get("survived")
    score = summary.get("score")

    result.update({"total": total, "killed": killed, "survived": survived, "score": score})

    if not isinstance(total, int):
        result["error"] = "Mutation delegate did not provide parseable integer total"
        return result
    if total == 0:
        result["error"] = "Mutation delegate produced total=0"
        return result
    if total < 0:
        result["error"] = f"Mutation delegate produced total < 0 ({total})"
        return result

    result["fatal"] = False
    result["ok"] = True
    if proc.returncode != 0:
        result["warning"] = (
            "Mutation delegate returned non-zero exit; tolerated because total>0 and summary parse succeeded"
        )
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and optionally run module guardrails.")
    parser.add_argument("--module", required=True)
    parser.add_argument("--guardrails-root", default="guardrails")
    parser.add_argument("--run-mutations", action="store_true")
    parser.add_argument("--mutation-timeout-sec", type=int, default=1)
    parser.add_argument("--output")
    parser.add_argument("--fail-on-missing-tests", default="true", choices=["true", "false"])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.mutation_timeout_sec <= 0:
        print("Invalid --mutation-timeout-sec: must be > 0")
        return 1
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
        elif mutation_result.get("warning"):
            report["warnings"].append(str(mutation_result["warning"]))

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
