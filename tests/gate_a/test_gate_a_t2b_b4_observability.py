"""Gate A T2b batch b4 — observability: retention DB e log tail.

DB reale sqlite in memoria per il fallback SQL; callback stub; file di log in tmp_path."""
from __future__ import annotations

import sqlite3
import time

from observability.log_tail import tail_text_from_files
from observability.retention_manager import RetentionManager

DAY = 86400


class _SqlDb:
    """Espone solo `execute` (nessun callback dedicato) su sqlite reale."""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        for t in ("observability_snapshots", "diagnostics_exports"):
            self.conn.execute(f"CREATE TABLE {t} (id INTEGER, created_at REAL)")

    def execute(self, sql, params=()):
        self.conn.execute(sql, params)

    def ids(self, table):
        return sorted(r[0] for r in self.conn.execute(f"SELECT id FROM {table}"))


def _seed(db, table, now):
    db.conn.executemany(f"INSERT INTO {table} VALUES (?, ?)",
                        [(1, now - 10 * DAY), (2, now - 1 * DAY), (3, now)])


class _CallbackDb(_SqlDb):
    def __init__(self, fail=False):
        super().__init__()
        self.fail = fail
        self.cutoffs = {}

    def _cb(self, name, cutoff):
        self.cutoffs[name] = cutoff
        if self.fail:
            raise RuntimeError("boom")

    def delete_old_observability_snapshots(self, cutoff):
        self._cb("snap", cutoff)

    def delete_old_diagnostics_exports(self, cutoff):
        self._cb("exp", cutoff)


def test_trg_628_b_0790(tmp_path):
    """ID: TRG-628-B-0790 / PF-B1-DIAGNOSTIC-DB-SNAPSHOTS

    Snapshot piu' vecchi del cutoff (now - snapshots_max_age_days) cancellati: via callback dedicato se
    presente (SQL non toccato), altrimenti — o se il callback solleva — via fallback SQL."""
    now = time.time()
    db = _SqlDb()
    _seed(db, "observability_snapshots", now)
    RetentionManager(db=db, snapshots_max_age_days=7)._cleanup_db_snapshots()
    assert db.ids("observability_snapshots") == [2, 3]

    cb = _CallbackDb()
    _seed(cb, "observability_snapshots", now)
    RetentionManager(db=cb, snapshots_max_age_days=7)._cleanup_db_snapshots()
    assert abs(cb.cutoffs["snap"] - (now - 7 * DAY)) < 60
    assert cb.ids("observability_snapshots") == [1, 2, 3], "callback riuscito: nessun doppio delete SQL"

    bad = _CallbackDb(fail=True)
    _seed(bad, "observability_snapshots", now)
    RetentionManager(db=bad, snapshots_max_age_days=7)._cleanup_db_snapshots()
    assert bad.ids("observability_snapshots") == [2, 3], "callback fallito: fallback SQL non eseguito"

    RetentionManager(db=None)._cleanup_db_snapshots()   # nessun DB: no-op senza eccezioni


def test_trg_628_b_0791(tmp_path):
    """ID: TRG-628-B-0791 / PF-B1-DIAGNOSTIC-DB-EXPORTS

    Registri export piu' vecchi del cutoff (now - exports_max_age_days) cancellati, stessi rami
    callback/fallback; non tocca la tabella snapshot."""
    now = time.time()
    db = _SqlDb()
    _seed(db, "diagnostics_exports", now)
    _seed(db, "observability_snapshots", now)
    RetentionManager(db=db, exports_max_age_days=2)._cleanup_db_export_rows()
    assert db.ids("diagnostics_exports") == [2, 3]
    assert db.ids("observability_snapshots") == [1, 2, 3]

    cb = _CallbackDb()
    _seed(cb, "diagnostics_exports", now)
    RetentionManager(db=cb, exports_max_age_days=2)._cleanup_db_export_rows()
    assert abs(cb.cutoffs["exp"] - (now - 2 * DAY)) < 60
    assert cb.ids("diagnostics_exports") == [1, 2, 3]

    bad = _CallbackDb(fail=True)
    _seed(bad, "diagnostics_exports", now)
    RetentionManager(db=bad, exports_max_age_days=2)._cleanup_db_export_rows()
    assert bad.ids("diagnostics_exports") == [2, 3]


def test_trg_628_b_1043(tmp_path):
    """ID: TRG-628-B-1043 / PF-B1-R2-LOG-TAIL

    Legge solo gli ultimi max_bytes di ogni file, decodifica utf-8 con replacement (byte invalidi non
    sollevano), intesta ogni blocco con il percorso; file assenti saltati."""
    log = tmp_path / "a.log"
    log.write_bytes(b"HEAD-" * 100 + b"\xff\xfeTAIL")
    out = tail_text_from_files([str(log), str(tmp_path / "missing.log")], max_bytes_per_file=8)
    assert f"===== LOG FILE: {log} =====" in out
    assert out.count("===== LOG FILE") == 1
    body = out.split("=====\n", 1)[1]
    assert body == "D-" + "\ufffd" * 2 + "TAIL\n", repr(body)
