from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.db_restore_validate import REQUIRED_TABLES, main, validate_db


def _create_minimal_db(path: Path, *, include_all: bool = True) -> None:
    conn = sqlite3.connect(path)
    try:
        tables = list(REQUIRED_TABLES)
        if not include_all:
            tables = [t for t in tables if t != "audit_events"]

        for table in tables:
            conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()


def test_runbook_requires_sidecar_restore_set_and_warns_main_db_only() -> None:
    runbook = Path("ops/db_backup_restore.md").read_text(encoding="utf-8")

    assert "filesystem snapshot restore" in runbook.lower()
    assert "<DB_PATH>-wal" in runbook
    assert "<DB_PATH>-shm" in runbook
    assert "restoring only `<DB_PATH>` is unsafe" in runbook
    assert "committed-but-uncheckpointed transactions" in runbook


def test_validator_uses_constant_time_readability_probe_not_count_scan() -> None:
    src = Path("scripts/db_restore_validate.py").read_text(encoding="utf-8")

    assert "SELECT COUNT(*)" not in src
    assert "SELECT 1 FROM {table} LIMIT 1" in src


def test_valid_minimal_db_passes(tmp_path: Path) -> None:
    db_path = tmp_path / "ok.sqlite"
    _create_minimal_db(db_path, include_all=True)

    report = validate_db(str(db_path))

    assert report.status == "PASS"
    assert report.failed_checks == []
    assert report.missing_tables == []


def test_missing_db_file_fails_with_non_zero_exit(tmp_path: Path) -> None:
    db_path = tmp_path / "missing.sqlite"

    exit_code = main(["--db-path", str(db_path)])

    assert exit_code != 0


def test_missing_critical_table_fails_and_reports_missing_table(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_table.sqlite"
    _create_minimal_db(db_path, include_all=False)

    report = validate_db(str(db_path))

    assert report.status == "FAIL"
    assert "audit_events" in report.missing_tables


def test_corrupted_db_fails_without_false_pass(tmp_path: Path) -> None:
    db_path = tmp_path / "corrupted.sqlite"
    db_path.write_bytes(b"this is not sqlite")

    report = validate_db(str(db_path))

    assert report.status == "FAIL"
    assert report.failed_checks


def test_report_file_written_when_requested(tmp_path: Path) -> None:
    db_path = tmp_path / "ok_for_report.sqlite"
    report_path = tmp_path / "reports" / "restore_report.json"
    _create_minimal_db(db_path, include_all=True)

    exit_code = main([
        "--db-path",
        str(db_path),
        "--report-path",
        str(report_path),
    ])

    assert exit_code == 0
    assert report_path.exists()

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["db_path"] == str(db_path)
    assert sorted(payload.keys()) == [
        "db_path",
        "failed_checks",
        "missing_tables",
        "passed_checks",
        "reasons",
        "status",
    ]
