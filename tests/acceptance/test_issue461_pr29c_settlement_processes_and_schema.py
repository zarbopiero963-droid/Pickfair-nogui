"""#461 PR29-c — registro ``settlement_effects`` con processi del sistema operativo reali e database esistenti.

Mandato PF-PR29-C FINAL VALIDATION, punti 4, 5 e 10: crash vero di un processo durante la transazione di
settlement (``os._exit``, nessun cleanup), due processi indipendenti sullo stesso file SQLite, compatibilita' con
un database creato prima della tabella e con la versione precedente della tabella. Nessun mock: ``Database`` reale.
"""
from __future__ import annotations

import multiprocessing
import os

import pytest
import sqlite3
import subprocess
import sys

from database import Database

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHIAVE = "cleared:1.1:B1"


def _figlio(codice: str, percorso: str) -> int:
    return subprocess.run([sys.executable, "-c", codice, percorso], cwd=ROOT, timeout=120).returncode


_CRASH_PRIMA_DEL_COMMIT = """
import os, sys
from database import Database
db = Database(sys.argv[1])
def crash(*_a, **_k):
    os._exit(7)                              # processo ucciso a meta' transazione: riga inserita, nessun commit
db._set_setting = crash
db.record_settlement_effect("cleared:1.1:B1", {"outcome": "released", "table_id": 1, "pnl": -10.0},
                            {"pr29c.stato_tavoli": "dopo"})
"""

_CRASH_DOPO_IL_COMMIT = """
import os, sys
from database import Database
db = Database(sys.argv[1])
assert db.record_settlement_effect("cleared:1.1:B1", {"outcome": "released", "table_id": 1, "pnl": -10.0},
                                   {"pr29c.stato_tavoli": "dopo"})
os._exit(9)                                  # crash prima della marcatura del realizzato
"""


def test_crash_reale_prima_del_commit_nessuna_riga_ne_stato_poi_una_volta(tmp_path):
    percorso = str(tmp_path / "crash.db")
    Database(percorso).save_settings({"pr29c.stato_tavoli": "prima"})
    assert _figlio(_CRASH_PRIMA_DEL_COMMIT, percorso) == 7
    db = Database(percorso)                  # riavvio
    assert db.get_settlement_effect(CHIAVE) is None
    assert db.get_settings()["pr29c.stato_tavoli"] == "prima", "stato dei tavoli scritto senza la riga"
    assert db.record_settlement_effect(CHIAVE, {"outcome": "released", "table_id": 1, "pnl": -10.0},
                                       {"pr29c.stato_tavoli": "dopo"}) is True
    assert db.record_settlement_effect(CHIAVE, {"outcome": "released"}, {"pr29c.stato_tavoli": "x"}) is False
    assert db.get_settings()["pr29c.stato_tavoli"] == "dopo"


def test_crash_reale_dopo_il_commit_riga_e_stato_durevoli_replay_senza_effetto(tmp_path):
    percorso = str(tmp_path / "crash2.db")
    Database(percorso)
    assert _figlio(_CRASH_DOPO_IL_COMMIT, percorso) == 9
    db = Database(percorso)
    riga = db.get_settlement_effect(CHIAVE)
    assert riga["outcome"] == "released" and riga["realized_applied"] == 0
    assert db.get_settings()["pr29c.stato_tavoli"] == "dopo"
    assert db.record_settlement_effect(CHIAVE, {"outcome": "released"}, {"pr29c.stato_tavoli": "replay"}) is False
    assert db.get_settings()["pr29c.stato_tavoli"] == "dopo"


def _processo(percorso, indice, barriera, esiti):
    import threading

    db = Database(percorso)
    locali = []

    def registra(i):
        barriera.wait()
        locali.append(db.record_settlement_effect(CHIAVE, {"outcome": "released", "table_id": 1, "pnl": -10.0,
                                                           "instance_id": f"p{indice}-t{i}"},
                                                  {"pr29c.vincitore": f"p{indice}-t{i}"}))

    fili = [threading.Thread(target=registra, args=(i,)) for i in range(10)]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    esiti.put((os.getpid(), locali))


def test_due_processi_os_indipendenti_sullo_stesso_sqlite_un_solo_effetto(tmp_path):
    percorso = str(tmp_path / "condiviso.db")
    Database(percorso)
    ctx = multiprocessing.get_context("spawn")
    barriera, esiti = ctx.Barrier(20), ctx.Queue()
    processi = [ctx.Process(target=_processo, args=(percorso, i, barriera, esiti)) for i in range(2)]
    for p in processi:
        p.start()
    raccolti = [esiti.get(timeout=120) for _ in processi]
    for p in processi:
        p.join(timeout=120)
        assert p.exitcode == 0
    assert len({pid for pid, _ in raccolti}) == 2 and os.getpid() not in {pid for pid, _ in raccolti}
    tutti = [e for _, locali in raccolti for e in locali]
    assert tutti.count(True) == 1 and tutti.count(False) == 19, tutti
    db = Database(percorso)
    assert db.get_settings()["pr29c.vincitore"] == db.get_settlement_effect(CHIAVE)["instance_id"]


# ---------------------------------------------------------------- punto 10: compatibilita' dello schema
def test_database_esistente_senza_la_tabella_migra_e_conserva_i_dati(tmp_path):
    percorso = str(tmp_path / "esistente.db")
    db = Database(percorso)
    db.save_settings({"chiave.esistente": "valore"})
    conn = sqlite3.connect(percorso)
    conn.execute("DROP TABLE settlement_effects")         # come un database creato prima di PR29-c
    conn.commit()
    conn.close()
    for _ in range(2):                                    # migrazione e riavvio successivo (idempotente)
        db = Database(percorso)
        assert db.get_settings()["chiave.esistente"] == "valore"
        assert db.list_held_settlement_effects() == []
    assert db.record_settlement_effect(CHIAVE, {"outcome": "held", "payload": {"market_id": "1.1"}}) is True
    assert Database(percorso).list_held_settlement_effects()[0]["payload"] == {"market_id": "1.1"}


def test_tabella_della_versione_precedente_riceve_la_colonna_payload(tmp_path):
    percorso = str(tmp_path / "vecchia.db")
    Database(percorso)
    conn = sqlite3.connect(percorso)
    conn.execute("DROP TABLE settlement_effects")
    conn.execute("""CREATE TABLE settlement_effects (settlement_key TEXT PRIMARY KEY, outcome TEXT NOT NULL DEFAULT '',
        table_id INTEGER, event_key TEXT NOT NULL DEFAULT '', customer_ref TEXT NOT NULL DEFAULT '',
        reservation_id TEXT NOT NULL DEFAULT '', bet_id TEXT NOT NULL DEFAULT '', market_id TEXT NOT NULL DEFAULT '',
        pnl REAL NOT NULL DEFAULT 0.0, instance_id TEXT NOT NULL DEFAULT '',
        realized_applied INTEGER NOT NULL DEFAULT 0, recorded_at REAL NOT NULL)""")
    conn.execute("INSERT INTO settlement_effects(settlement_key, outcome, pnl, recorded_at) VALUES ('k0', 'held', -1, 0)")
    conn.commit()
    conn.close()
    db = Database(percorso)
    righe = db.list_held_settlement_effects()
    assert [(r["settlement_key"], r["pnl"], r["payload"]) for r in righe] == [("k0", -1.0, {})]
    assert db.upgrade_held_settlement_effect("k0", {"outcome": "released", "table_id": 1}) is True
    assert db.upgrade_held_settlement_effect("k0", {"outcome": "released", "table_id": 1}) is False


# ---------------------------------------------------------------- un solo runtime attivo per database (Sol su c6d2338)
_TIENE_IL_LOCK = """
import fcntl, sys, time
fh = open(sys.argv[1] + ".runtime.lock", "a+")
fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
print("PRESO", flush=True)
time.sleep(30)
"""


def _controller_su(percorso):
    from core.runtime_controller import RuntimeController

    rc = RuntimeController.__new__(RuntimeController)
    rc.db = Database(percorso)
    return rc


def test_secondo_runtime_sullo_stesso_database_non_ottiene_il_lock(tmp_path):
    percorso = str(tmp_path / "unico.db")
    primo, secondo = _controller_su(percorso), _controller_su(percorso)
    assert primo._acquire_runtime_instance_lock() == ""
    assert primo._acquire_runtime_instance_lock() == ""               # idempotente per lo stesso controller
    assert secondo._acquire_runtime_instance_lock() == "runtime_already_active_on_database"
    altro = _controller_su(str(tmp_path / "altro.db"))
    assert altro._acquire_runtime_instance_lock() == ""               # un altro database e' indipendente


@pytest.mark.skipif(os.name == "nt", reason="il processo figlio usa fcntl (POSIX); il runtime usa msvcrt su Windows")
def test_processo_os_che_tiene_il_lock_blocca_l_avvio_e_alla_sua_fine_lo_libera(tmp_path):
    percorso = str(tmp_path / "unico_os.db")
    Database(percorso)
    figlio = subprocess.Popen([sys.executable, "-c", _TIENE_IL_LOCK, percorso], stdout=subprocess.PIPE, text=True)
    try:
        assert figlio.stdout.readline().strip() == "PRESO"
        assert _controller_su(percorso)._acquire_runtime_instance_lock() == "runtime_already_active_on_database"
    finally:
        figlio.kill()                                                 # crash: il sistema operativo libera il lock
        figlio.wait(10)
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == ""


def test_start_rifiutato_se_un_altro_runtime_e_attivo(tmp_path):
    import inspect
    from core.runtime_controller import RuntimeController

    sorgente = inspect.getsource(RuntimeController.start)
    assert sorgente.index("_acquire_runtime_instance_lock") < sorgente.index("self.reload_config(reset_session=True)")
    assert '"reason_code": "RUNTIME_INSTANCE_ACTIVE"' in sorgente
