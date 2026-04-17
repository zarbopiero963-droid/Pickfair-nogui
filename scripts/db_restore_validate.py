from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


REQUIRED_TABLES = (
    "orders",
    "order_saga",
    "audit_events",
    "cycle_recovery_checkpoints",
)


@dataclass
class ValidationReport:
    status: str
    db_path: str
    passed_checks: list[str]
    failed_checks: list[str]
    missing_tables: list[str]
    reasons: list[str]


def _read_table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def validate_db(db_path: str) -> ValidationReport:
    target = Path(db_path)
    passed_checks: list[str] = []
    failed_checks: list[str] = []
    reasons: list[str] = []
    missing_tables: list[str] = []

    if not target.exists():
        failed_checks.append("db_file_exists")
        reasons.append("database file does not exist")
        return ValidationReport(
            status="FAIL",
            db_path=str(target),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            missing_tables=missing_tables,
            reasons=reasons,
        )

    try:
        conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        failed_checks.append("db_open_readonly")
        reasons.append(f"unable to open database read-only: {type(exc).__name__}: {exc}")
        return ValidationReport(
            status="FAIL",
            db_path=str(target),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            missing_tables=missing_tables,
            reasons=reasons,
        )

    try:
        conn.execute("PRAGMA query_only=ON")
        passed_checks.append("db_open_readonly")

        try:
            table_names = _read_table_names(conn)
            passed_checks.append("schema_readable")
        except sqlite3.Error as exc:
            failed_checks.append("schema_readable")
            reasons.append(f"failed to read sqlite schema: {type(exc).__name__}: {exc}")
            return ValidationReport(
                status="FAIL",
                db_path=str(target),
                passed_checks=passed_checks,
                failed_checks=failed_checks,
                missing_tables=missing_tables,
                reasons=reasons,
            )

        for table in REQUIRED_TABLES:
            if table not in table_names:
                missing_tables.append(table)

        if missing_tables:
            failed_checks.append("required_tables_present")
            reasons.append("one or more critical tables are missing")
            return ValidationReport(
                status="FAIL",
                db_path=str(target),
                passed_checks=passed_checks,
                failed_checks=failed_checks,
                missing_tables=sorted(missing_tables),
                reasons=reasons,
            )

        passed_checks.append("required_tables_present")

        for table in REQUIRED_TABLES:
            check_name = f"table_readable:{table}"
            try:
                conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                passed_checks.append(check_name)
            except sqlite3.Error as exc:
                failed_checks.append(check_name)
                reasons.append(f"table query failed for {table}: {type(exc).__name__}: {exc}")

        status = "PASS" if not failed_checks else "FAIL"
        if status == "FAIL" and not reasons:
            reasons.append("validation failed for unspecified reason (fail-closed)")

        return ValidationReport(
            status=status,
            db_path=str(target),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            missing_tables=sorted(missing_tables),
            reasons=reasons,
        )
    finally:
        conn.close()


def _write_report(report_path: str, payload: dict[str, Any]) -> None:
    target = Path(report_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate restored SQLite DB candidate.")
    parser.add_argument("--db-path", required=True, help="Path to sqlite database file")
    parser.add_argument(
        "--report-path",
        default="",
        help="Optional path to write JSON report",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    report = validate_db(args.db_path)
    payload = asdict(report)

    if args.report_path:
        _write_report(args.report_path, payload)

    print(json.dumps(payload, sort_keys=True))
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
