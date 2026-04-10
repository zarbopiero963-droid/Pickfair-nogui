# scripts/run_mutation_guardrails.py
from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class MutationResult:
    module: str
    mutation_id: str
    expected_failure: str
    killed: bool
    return_code: int
    stdout_tail: str
    stderr_tail: str
    error: str = ""


class MutationRunnerError(Exception):
    pass


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _default_test_paths(module_name: str) -> list[str]:
    mapping = {
        "core.trading_engine": ["tests"],
        "order_manager": ["tests"],
        "core.reconciliation_engine": ["tests"],
        "core.execution_guard": ["tests"],
        "core.risk_middleware": ["tests"],
        "core.runtime_controller": ["tests"],
        "core.money_management": ["tests"],
        "dutching": ["tests"],
        "pnl_engine": ["tests"],
        "telegram_listener": ["tests"],
        "copy_engine": ["tests"],
        "simulation_broker": ["tests"],
        "session_manager": ["tests"],
        "rate_limiter": ["tests"],
        "live_gate": ["tests"],
    }
    return mapping.get(module_name, ["tests"])


def _sanitize_name(value: str) -> str:
    return value.replace("/", "_").replace("\\", "_").replace(".", "_").replace(" ", "_")


def _discover_mutation_files(root: Path, modules: list[str] | None) -> list[Path]:
    base = root / "guardrails" / "mutations"
    if not base.exists():
        raise MutationRunnerError(f"Missing mutations directory: {base}")

    files = sorted(base.rglob("*.json"))
    if not modules:
        return files

    wanted = set(modules)
    selected: list[Path] = []
    for path in files:
        payload = _load_json(path)
        module = str(payload.get("module", "")).strip()
        if module in wanted:
            selected.append(path)
    return selected


def _patch_for_mutation(
    payload: dict[str, Any],
    mutation: dict[str, Any],
) -> dict[str, Any]:
    mutated = copy.deepcopy(payload)
    mutated["_mutation_meta"] = {
        "active": True,
        "id": mutation.get("id", "unknown"),
        "type": mutation.get("type", "unknown"),
        "target": mutation.get("target", ""),
        "expected_failure": mutation.get("expected_failure", "tests"),
    }
    return mutated


def _run_pytest(
    repo_root: Path,
    module_name: str,
    mutation_id: str,
    test_paths: list[str],
    pytest_args: list[str],
    timeout_sec: int,
) -> tuple[int, str, str]:
    env = os.environ.copy()
    env["PICKFAIR_MUTATION_MODE"] = "1"
    env["PICKFAIR_MUTATION_MODULE"] = module_name
    env["PICKFAIR_MUTATION_ID"] = mutation_id

    cmd = [sys.executable, "-m", "pytest", "-q", "-x", *test_paths, *pytest_args]
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _run_one_mutation(
    repo_root: Path,
    mutation_file: Path,
    mutation: dict[str, Any],
    pytest_args: list[str],
    timeout_sec: int,
) -> MutationResult:
    payload = _load_json(mutation_file)
    module_name = str(payload.get("module", mutation_file.stem))
    mutation_id = str(mutation.get("id", "unknown"))
    expected_failure = str(mutation.get("expected_failure", "tests"))

    mutated_payload = _patch_for_mutation(payload, mutation)
    _write_json(mutation_file, mutated_payload)

    try:
        rc, stdout, stderr = _run_pytest(
            repo_root=repo_root,
            module_name=module_name,
            mutation_id=mutation_id,
            test_paths=_default_test_paths(module_name),
            pytest_args=pytest_args,
            timeout_sec=timeout_sec,
        )
        killed = rc != 0
        return MutationResult(
            module=module_name,
            mutation_id=mutation_id,
            expected_failure=expected_failure,
            killed=killed,
            return_code=rc,
            stdout_tail=_tail(stdout),
            stderr_tail=_tail(stderr),
        )
    except subprocess.TimeoutExpired as exc:
        return MutationResult(
            module=module_name,
            mutation_id=mutation_id,
            expected_failure=expected_failure,
            killed=False,
            return_code=124,
            stdout_tail=_tail(exc.stdout or ""),
            stderr_tail=_tail(exc.stderr or ""),
            error=f"timeout after {timeout_sec}s",
        )
    finally:
        _write_json(mutation_file, payload)


def _summarize(results: list[MutationResult]) -> dict[str, Any]:
    total = len(results)
    killed = sum(1 for r in results if r.killed)
    survived = total - killed
    score = round((killed / total) * 100, 2) if total else 0.0

    by_module: dict[str, dict[str, Any]] = {}
    for r in results:
        bucket = by_module.setdefault(
            r.module,
            {"total": 0, "killed": 0, "survived": 0, "score": 0.0, "survivors": []},
        )
        bucket["total"] += 1
        if r.killed:
            bucket["killed"] += 1
        else:
            bucket["survived"] += 1
            bucket["survivors"].append(
                {
                    "mutation_id": r.mutation_id,
                    "expected_failure": r.expected_failure,
                    "return_code": r.return_code,
                    "error": r.error,
                }
            )

    for module_name, bucket in by_module.items():
        total_mod = int(bucket["total"])
        killed_mod = int(bucket["killed"])
        bucket["score"] = round((killed_mod / total_mod) * 100, 2) if total_mod else 0.0

    return {
        "total": total,
        "killed": killed,
        "survived": survived,
        "score": score,
        "by_module": by_module,
    }


def _print_console_report(results: list[MutationResult], summary: dict[str, Any]) -> None:
    print("=" * 80)
    print("MUTATION GUARDRAILS REPORT")
    print("=" * 80)
    print(
        f"Total: {summary['total']} | "
        f"Killed: {summary['killed']} | "
        f"Survived: {summary['survived']} | "
        f"Score: {summary['score']}%"
    )
    print()

    for module_name, bucket in summary["by_module"].items():
        print(
            f"[{module_name}] "
            f"total={bucket['total']} "
            f"killed={bucket['killed']} "
            f"survived={bucket['survived']} "
            f"score={bucket['score']}%"
        )
        for survivor in bucket["survivors"]:
            print(
                f"  - SURVIVED: {survivor['mutation_id']} "
                f"(expected_failure={survivor['expected_failure']}, "
                f"rc={survivor['return_code']}, error={survivor['error']})"
            )
        print()

    survivors = [r for r in results if not r.killed]
    if survivors:
        print("SURVIVOR DETAILS")
        print("-" * 80)
        for r in survivors:
            print(f"{r.module} :: {r.mutation_id}")
            if r.error:
                print(f"  error: {r.error}")
            if r.stdout_tail:
                print("  stdout tail:")
                print(_indent(r.stdout_tail, 4))
            if r.stderr_tail:
                print("  stderr tail:")
                print(_indent(r.stderr_tail, 4))
            print()


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())


def _write_report(output_path: Path, results: list[MutationResult], summary: dict[str, Any]) -> None:
    payload = {
        "summary": summary,
        "results": [
            {
                "module": r.module,
                "mutation_id": r.mutation_id,
                "expected_failure": r.expected_failure,
                "killed": r.killed,
                "return_code": r.return_code,
                "error": r.error,
                "stdout_tail": r.stdout_tail,
                "stderr_tail": r.stderr_tail,
            }
            for r in results
        ],
    }
    _write_json(output_path, payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run mutation guardrails against pytest.")
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing guardrails/ and tests/ (default: current dir)",
    )
    parser.add_argument(
        "--module",
        action="append",
        default=[],
        help="Run only specific module(s), e.g. --module core.trading_engine",
    )
    parser.add_argument(
        "--output",
        default="mutation_guardrails_report.json",
        help="Write JSON report to this path",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=180,
        help="Per-mutation pytest timeout in seconds",
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Extra pytest arg, repeatable, e.g. --pytest-arg=-k --pytest-arg=trading_engine",
    )
    parser.add_argument(
        "--fail-under",
        type=float,
        default=80.0,
        help="Exit non-zero if global mutation score is below this threshold",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    output_path = Path(args.output).resolve()

    mutation_files = _discover_mutation_files(repo_root, args.module or None)
    if not mutation_files:
        print("No mutation JSON files found for selection.", file=sys.stderr)
        return 2

    all_results: list[MutationResult] = []

    for mutation_file in mutation_files:
        payload = _load_json(mutation_file)
        module_name = str(payload.get("module", mutation_file.stem))
        mutations = payload.get("mutations", [])
        if not isinstance(mutations, list):
            raise MutationRunnerError(f"'mutations' must be a list in {mutation_file}")

        print(f"\n>>> MODULE: {module_name} ({mutation_file})")
        for mutation in mutations:
            if not isinstance(mutation, dict):
                raise MutationRunnerError(f"Invalid mutation entry in {mutation_file}: {mutation!r}")
            mutation_id = str(mutation.get("id", "unknown"))
            print(f"  -> running mutation: {mutation_id}")
            result = _run_one_mutation(
                repo_root=repo_root,
                mutation_file=mutation_file,
                mutation=mutation,
                pytest_args=args.pytest_arg,
                timeout_sec=args.timeout_sec,
            )
            status = "KILLED" if result.killed else "SURVIVED"
            print(f"     {status} (rc={result.return_code})")
            all_results.append(result)

    summary = _summarize(all_results)
    _print_console_report(all_results, summary)
    _write_report(output_path, all_results, summary)
    print(f"JSON report written to: {output_path}")

    if float(summary["score"]) < float(args.fail_under):
        print(
            f"Mutation score {summary['score']}% is below threshold {args.fail_under}%",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MutationRunnerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)