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
import time
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


def _app_su(percorso, monkeypatch):
    """HeadlessApp reale (SIM) sullo stesso file di database."""
    import headless_main

    monkeypatch.setattr(headless_main, "Database", lambda: Database(percorso))
    app = headless_main.HeadlessApp()
    app.build(start_services=False)
    app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
    app.telegram_service.stop = lambda *a, **k: None
    return app


def test_start_rifiutato_se_un_altro_runtime_e_attivo_e_riammesso_dopo_lo_stop(tmp_path, monkeypatch):
    # Owner (PR29-c FINAL VALIDATION, punto 4): vincolo applicato da start() reale, non solo dal sorgente
    from core.system_state import RuntimeMode

    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "unico_app.db")
    primo, secondo = _app_su(percorso, monkeypatch), _app_su(percorso, monkeypatch)
    rifiuti = []
    secondo.bus.subscribe("RUNTIME_START_REFUSED", lambda p: rifiuti.append(dict(p)))
    connessioni = []
    originale = secondo.runtime.betfair_service.connect
    secondo.runtime.betfair_service.connect = lambda *a, **k: (connessioni.append(1), originale(*a, **k))[1]
    try:
        assert primo.runtime.start(execution_mode="SIMULATION").get("started") is True
        esito = secondo.runtime.start(execution_mode="SIMULATION")
        assert esito["started"] is False and esito["reason_code"] == "RUNTIME_INSTANCE_ACTIVE"
        assert secondo.runtime.mode is RuntimeMode.STOPPED and connessioni == []
        fine = time.monotonic() + 5.0                                  # il bus consegna in modo asincrono
        while not rifiuti and time.monotonic() < fine:
            time.sleep(0.02)
        assert rifiuti and rifiuti[0]["reason_code"] == "RUNTIME_INSTANCE_ACTIVE"
        primo.runtime.stop()                                          # STOP pulito: il lock si libera
        assert secondo.runtime.start(execution_mode="SIMULATION").get("started") is True
        assert primo.runtime.start(execution_mode="SIMULATION")["reason_code"] == "RUNTIME_INSTANCE_ACTIVE"
    finally:
        for app in (secondo, primo):
            try:
                app.stop()
            except Exception:
                pass


def test_avvio_non_riuscito_rilascia_il_lock(tmp_path, monkeypatch):
    percorso = str(tmp_path / "avvio_fallito.db")
    app = _app_su(percorso, monkeypatch)
    monkeypatch.chdir(tmp_path)

    def _guasto(*_a, **_k):
        raise RuntimeError("guasto all'avvio")

    monkeypatch.setattr(app.runtime, "reload_config", _guasto)
    with pytest.raises(RuntimeError):
        app.runtime.start(execution_mode="SIMULATION")
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == "", "avvio fallito ha trattenuto il lock"


def test_avvio_ripetuto_fallito_non_libera_il_lock_di_un_runtime_attivo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "attivo.db")
    app = _app_su(percorso, monkeypatch)
    try:
        assert app.runtime.start(execution_mode="SIMULATION").get("started") is True

        def _guasto(*_a, **_k):
            raise RuntimeError("guasto al secondo avvio")

        monkeypatch.setattr(app.runtime, "reload_config", _guasto)
        with pytest.raises(RuntimeError):
            app.runtime.start(execution_mode="SIMULATION")
        assert _controller_su(percorso)._acquire_runtime_instance_lock() == "runtime_already_active_on_database", \
            "un avvio ripetuto fallito ha liberato il lock del runtime attivo"
    finally:
        monkeypatch.undo()
        app.stop()


def test_stop_concorrente_attende_l_avvio_in_corso_e_il_lock_resta_coerente(tmp_path, monkeypatch):
    # Sol su e3fe4d7: STOP durante uno start() in corso non deve chiudere il lock che lo start sta per usare
    import threading
    from core.system_state import RuntimeMode

    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "corsa.db")
    app = _app_su(percorso, monkeypatch)
    rc = app.runtime
    dentro, via = threading.Event(), threading.Event()
    originale = rc.reload_config

    def _lento(*a, **k):
        dentro.set()
        via.wait(10)
        return originale(*a, **k)

    rc.reload_config = _lento
    avvio = threading.Thread(target=lambda: rc.start(execution_mode="SIMULATION"))
    arresto_fatto = threading.Event()
    arresto = threading.Thread(target=lambda: (rc.stop(), arresto_fatto.set()))
    try:
        avvio.start()
        assert dentro.wait(10)
        arresto.start()
        assert not arresto_fatto.wait(0.5), "STOP eseguito in parallelo con uno start() in corso"
        via.set()
        avvio.join(20)
        arresto.join(20)
        assert arresto_fatto.is_set() and rc.mode is RuntimeMode.STOPPED
        assert _controller_su(percorso)._acquire_runtime_instance_lock() == "", "runtime fermo con il lock ancora preso"
    finally:
        via.set()
        rc.reload_config = originale
        app.stop()


def test_avvio_fallito_dopo_la_connessione_smonta_i_servizi_prima_del_lock(tmp_path, monkeypatch):
    # Codex su e3fe4d7: niente lock libero con una sessione Betfair o un listener Telegram ancora vivi
    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "parziale.db")
    app = _app_su(percorso, monkeypatch)
    rc = app.runtime
    smontati = []
    rc.telegram_service.stop = lambda *a, **k: smontati.append("telegram")
    disconnetti = rc.betfair_service.disconnect
    rc.betfair_service.disconnect = lambda *a, **k: (smontati.append("betfair"), disconnetti(*a, **k))[1]

    def _telegram_guasto(*_a, **_k):
        raise RuntimeError("listener Telegram non avviabile")   # dopo connect() e lettura dei fondi

    rc.telegram_service.start = _telegram_guasto
    with pytest.raises(RuntimeError):
        rc.start(execution_mode="SIMULATION")
    assert "telegram" in smontati and "betfair" in smontati
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == ""


def test_smontaggio_fallito_dopo_un_avvio_fallito_trattiene_il_lock(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "trattenuto.db")
    app = _app_su(percorso, monkeypatch)
    rc = app.runtime

    def _guasto(*_a, **_k):
        raise RuntimeError("guasto")

    rc.telegram_service.start = _guasto
    monkeypatch.setattr(rc.betfair_service, "disconnect", _guasto)
    with pytest.raises(RuntimeError):
        rc.start(execution_mode="SIMULATION")
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == "runtime_already_active_on_database", \
        "lock liberato con una sessione Betfair forse ancora viva"


def test_due_grafie_dello_stesso_database_prendono_lo_stesso_lock(tmp_path, monkeypatch):
    # Grok su e3fe4d7: percorso relativo e link simbolico dello stesso file => un solo runtime
    percorso = str(tmp_path / "canonico.db")
    Database(percorso)
    primo = _controller_su(percorso)
    assert primo._acquire_runtime_instance_lock() == ""
    monkeypatch.chdir(tmp_path)
    relativo = _controller_su("canonico.db")
    assert relativo._acquire_runtime_instance_lock() == "runtime_already_active_on_database"
    if os.name != "nt":
        link = tmp_path / "alias.db"
        link.symlink_to(percorso)
        assert _controller_su(str(link))._acquire_runtime_instance_lock() == "runtime_already_active_on_database"


def test_lock_su_windows_blocca_sempre_il_byte_zero(tmp_path, monkeypatch):
    # Grok su e3fe4d7: con il file di lock non vuoto l'apertura in append parte dalla fine
    import types

    percorso = str(tmp_path / "win.db")
    Database(percorso)
    with open(percorso + ".runtime.lock", "w") as fh:
        fh.write("residuo")
    posizioni = []
    finto = types.SimpleNamespace(LK_NBLCK=2, locking=lambda fd, mode, n: posizioni.append(os.lseek(fd, 0, os.SEEK_CUR)))
    monkeypatch.setitem(sys.modules, "msvcrt", finto)
    rc = _controller_su(percorso)
    monkeypatch.setattr(os, "name", "nt")
    try:
        assert rc._acquire_runtime_instance_lock() == ""
    finally:
        monkeypatch.setattr(os, "name", "posix")
    assert posizioni == [0], f"byte bloccato all'offset {posizioni}"


def test_smontaggio_fallito_poi_avvio_rifiutato_non_libera_il_lock_finche_uno_stop_riesce(tmp_path, monkeypatch):
    # Sol su ad315ca: teardown fallito -> nuovo avvio rifiutato -> un altro controller non deve ottenere il lock
    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "sequenza.db")
    app = _app_su(percorso, monkeypatch)
    rc = app.runtime
    disconnetti = rc.betfair_service.disconnect

    def _guasto(*_a, **_k):
        raise RuntimeError("guasto")

    rc.telegram_service.start = _guasto
    rc.betfair_service.disconnect = _guasto
    with pytest.raises(RuntimeError):
        rc.start(execution_mode="SIMULATION")
    esito = rc.start(execution_mode="LIVE", live_enabled=False)      # rifiutato prima della connessione
    assert esito.get("started") is False
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == "runtime_already_active_on_database", \
        "avvio rifiutato ha liberato il lock con lo smontaggio ancora non riuscito"
    with pytest.raises(RuntimeError):
        rc.stop()                                                     # smontaggio ancora fallito: lock trattenuto
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == "runtime_already_active_on_database"
    rc.betfair_service.disconnect = disconnetti
    rc.stop()                                                         # smontaggio riuscito: lock libero
    assert _controller_su(percorso)._acquire_runtime_instance_lock() == ""
