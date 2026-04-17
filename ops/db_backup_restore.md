# DB Backup / Restore Runbook (SQLite)

## Scope
This runbook defines **manual, fail-closed** operational steps for backing up and restoring the local SQLite database used by Pickfair.

## Hard Safety Rule
**Do not resume live operation until restore validation returns `PASS`.**

## Database file to protect
- Primary SQLite file: `pickfair.db` (or the configured DB path in your environment).
- SQLite sidecar files that may exist while WAL is active:
  - `pickfair.db-wal`
  - `pickfair.db-shm`

When creating a filesystem-level backup, copy all present files above as one consistency set.

## Preconditions (before backup)
1. Identify the active DB path.
2. Stop write traffic from application/runtime processes.
3. Confirm no writer process is still holding the DB open.
4. Choose a timestamped backup destination, for example:
   - `backups/pickfair-YYYYMMDD-HHMMSS/`

If any precondition cannot be confirmed, **abort backup**.

## Safe backup procedure
Preferred method (SQLite-native logical copy, single backup artifact):
1. Ensure target backup directory exists.
2. Run:

```bash
sqlite3 "<DB_PATH>" ".backup '<BACKUP_DB_PATH>'"
```

Filesystem snapshot method (raw file copy):
1. Copy `<DB_PATH>`.
2. If present, copy `<DB_PATH>-wal` and `<DB_PATH>-shm`.
3. Treat these copied files as one inseparable consistency set.

## Safe restore procedure (to temp path first)
1. Never restore directly over the active production DB first.
2. Build a restore candidate at a temporary path.
3. Choose restore mode explicitly:
   - **Logical backup artifact restore (`.backup` output):** restore the single backup DB artifact.
   - **Filesystem snapshot restore:** restore the full SQLite consistency set from the same snapshot (`<DB_PATH>`, `<DB_PATH>-wal` if present in backup, `<DB_PATH>-shm` if present in backup).
4. For filesystem snapshot restores in WAL mode, restoring only `<DB_PATH>` is unsafe and can lose committed-but-uncheckpointed transactions.
5. If a snapshot restore does not contain the expected sidecar set, **do not trust the candidate**.
6. Run validation against candidate:

```bash
python scripts/db_restore_validate.py \
  --db-path restore_candidate/pickfair.restore.sqlite \
  --report-path restore_candidate/restore_validation_report.json
```

7. Inspect JSON report:
   - `status` must be `PASS`
   - `failed_checks` must be empty
   - `missing_tables` must be empty

If validation is not `PASS`, **do not trust the restore**.

## Critical schema/state required before trust
Validation must confirm at minimum these tables exist and are readable:
- `orders`
- `order_saga`
- `audit_events`
- `cycle_recovery_checkpoints`

## If validation fails
1. Keep runtime stopped for live operations.
2. Preserve failed candidate and JSON report for incident analysis.
3. Retry using another backup snapshot.
4. Do not overwrite the active DB with an unvalidated candidate.

## Cutover after successful validation
Only after `PASS`:
1. Stop runtime writes (if not already stopped).
2. Replace active DB with validated candidate (or restore from the same validated artifact).
3. Keep the validation report with incident/change records.
4. Resume runtime.

## Explicit non-go conditions
Do **not** resume live operation when any of the following is true:
- validation `status` is `FAIL`
- any required table is missing
- database is unreadable/corrupted
- report is missing or malformed
