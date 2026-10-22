"""#536 PF-RUNTIME-LOCK-01 — esclusivita' del runtime per database: lock OS + identita' del file + lease con fencing.

Mandato owner PF-536-ISOLATED-IMPLEMENTATION-01 (22/10/2026). Phase 0 su main 152613b: con il file
``.runtime.lock`` cancellato o sostituito un secondo runtime partiva sullo stesso database (P1/P2) e il primo
restava ACTIVE e continuava a operare (P3). Qui: prove REALI (processi OS, SQLite su file, catena
runtime -> engine -> broker SIM), nessun mock dei componenti sotto prova. Nessun test LIVE.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from core.runtime_authority import RuntimeAuthority, RuntimeAuthorityLost, _holder_alive
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from database import Database, RuntimeFenceViolation
from tests.acceptance.test_issue461_pr29c_settlement_processes_and_schema import (
    ROOT, _RUNTIME_SIM_ATTIVO, _app_su, _controller_su)
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal

POSIX = os.name != "nt"


def _lock_path(percorso: str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(percorso))) + ".runtime.lock"


def _arma(c):
    """Stesse chiamate di `start()`: lock OS, poi lease + fence + heartbeat."""
    assert c.rc._acquire_runtime_instance_lock() == ""
    assert c.rc._acquire_runtime_lease() == ""
    return c.rc.runtime_authority


def _catena_armata(tmp_path, nome="autorita.db", **kw):
    c = _chain(tmp_path, db_name=nome, **kw)
    _arma(c)
    return c


def _ferma(*apps):
    for app in apps:
        try:
            app.stop()
        except Exception:
            pass


def _attendi(cond, timeout=5.0):
    fine = time.monotonic() + timeout
    while not cond() and time.monotonic() < fine:
        time.sleep(0.02)
    return cond()


def _ordini(c):
    return [o.customer_ref for o in c.broker.state.orders.values()]


# ---------------------------------------------------------------- lock cancellato / sostituito (Phase 0 P1-P3)
def test_t01_lock_cancellato_secondo_runtime_rifiutato_e_primo_in_lockdown(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t01.db")
    a, b = _app_su(p, monkeypatch), _app_su(p, monkeypatch)
    persi = []
    a.bus.subscribe("RUNTIME_AUTHORITY_LOST", lambda pl: persi.append(dict(pl)))
    try:
        assert a.runtime.start(execution_mode="SIMULATION").get("started") is True
        token_a = a.runtime.runtime_authority.token
        os.unlink(_lock_path(p))
        esito = b.runtime.start(execution_mode="SIMULATION")
        assert esito.get("started") is False and esito["reason_code"] == "RUNTIME_LEASE_HELD", esito
        assert b.runtime.mode is RuntimeMode.STOPPED
        # A se ne accorge all'heartbeat (thread ogni 10 s in produzione) e si ferma da solo, senza cancel-all
        assert a.runtime.runtime_authority.heartbeat_once() is False
        assert a.runtime.mode is RuntimeMode.LOCKDOWN and a.runtime.is_live_allowed() is False
        # B, tentando l'avvio, ha ricreato il file sul percorso: per A e' "sostituito" (inode diverso)
        assert a.runtime._authority_lost_reason in ("lock_file_deleted", "lock_file_replaced")
        assert a.runtime._emergency_stopped is False, "perdita dell'autorita' non deve fare cancel-all"
        assert _attendi(lambda: persi) and persi[0]["reason"] == a.runtime._authority_lost_reason
        # lease scaduta da A: ora B puo' subentrare, con token maggiore; A non puo' piu' scrivere
        assert b.runtime.start(execution_mode="SIMULATION").get("started") is True
        assert b.runtime.runtime_authority.token > token_a
        with pytest.raises(RuntimeFenceViolation):
            a.runtime.db.record_settlement_effect("k-a", {"outcome": "released", "table_id": 1, "pnl": -1.0}, None)
    finally:
        _ferma(b, a)


def test_t02_lock_sostituito_inode_diverso(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t02.db")
    a, b = _app_su(p, monkeypatch), _app_su(p, monkeypatch)
    try:
        assert a.runtime.start(execution_mode="SIMULATION").get("started") is True
        lp = _lock_path(p)
        os.unlink(lp)
        with open(lp, "w") as fh:
            fh.write("sostituito")
        assert b.runtime.start(execution_mode="SIMULATION")["reason_code"] == "RUNTIME_LEASE_HELD"
        assert a.runtime.runtime_authority.lock_identity_problem() == "lock_file_replaced"
        assert a.runtime.runtime_authority.heartbeat_once() is False
        assert a.runtime.mode is RuntimeMode.LOCKDOWN
        assert a.runtime._authority_lost_reason == "lock_file_replaced"
    finally:
        _ferma(b, a)


def test_t03_due_avvii_simultanei_uno_solo_parte(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t03.db")
    apps = [_app_su(p, monkeypatch) for _ in range(3)]
    esiti = [None] * 3
    barriera = threading.Barrier(3)

    def go(i):
        barriera.wait()
        esiti[i] = apps[i].runtime.start(execution_mode="SIMULATION")

    fili = [threading.Thread(target=go, args=(i,)) for i in range(3)]
    for f in fili:
        f.start()
    for f in fili:
        f.join(60)
    try:
        partiti = [e for e in esiti if e and e.get("started")]
        assert len(partiti) == 1, esiti
        assert all((e or {}).get("reason_code") in ("RUNTIME_INSTANCE_ACTIVE", "RUNTIME_LEASE_HELD")
                   for e in esiti if e and not e.get("started"))
        lease = apps[0].runtime.db.read_runtime_lease()
        vincitore = next(app for app in apps if app.runtime.mode is RuntimeMode.ACTIVE)
        assert lease["owner_id"] == vincitore.runtime.runtime_authority.owner_id
    finally:
        _ferma(*apps)


# ---------------------------------------------------------------- processi OS reali
@pytest.mark.skipif(not POSIX, reason="kill e pid del figlio: semantica POSIX")
def test_t04_due_processi_os_lock_cancellato_lease_viva_poi_crash_del_proprietario(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t04.db")
    Database(p)
    figlio = subprocess.Popen([sys.executable, "-c", _RUNTIME_SIM_ATTIVO, p], cwd=ROOT,
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    app = _app_su(p, monkeypatch)
    try:
        assert figlio.stdout.readline().strip() == "ATTIVO"
        lease = app.runtime.db.read_runtime_lease()
        assert lease and int(lease["pid"]) == figlio.pid
        assert app.runtime.start(execution_mode="SIMULATION")["reason_code"] == "RUNTIME_INSTANCE_ACTIVE"
        os.unlink(_lock_path(p))                               # il flock del figlio e' ora su un inode orfano
        esito = app.runtime.start(execution_mode="SIMULATION")
        assert esito["reason_code"] == "RUNTIME_LEASE_HELD", esito   # la lease del figlio VIVO lo protegge
        assert f"pid={figlio.pid}" in esito["reason"]
    finally:
        figlio.kill()                                          # crash del proprietario
        figlio.wait(15)
    # lease non scaduta ma detentore dimostrabilmente morto sullo stesso host => subentro immediato
    assert app.runtime.start(execution_mode="SIMULATION").get("started") is True
    nuova = app.runtime.db.read_runtime_lease()
    assert int(nuova["fencing_token"]) > int(lease["fencing_token"]) and nuova["owner_id"] != lease["owner_id"]
    app.stop()
    assert _controller_su(p)._acquire_runtime_instance_lock() == ""


_CRASH_MULTI_TAVOLO = """
import os, sys
import headless_main
from database import Database
percorso = sys.argv[1]
headless_main.Database = lambda: Database(percorso)
app = headless_main.HeadlessApp()
app.build(start_services=False)
app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
app.telegram_service.stop = lambda *a, **k: None
assert app.runtime.start(execution_mode="SIMULATION").get("started")
tm = app.runtime.table_manager
tm.activate(table_id=1, event_key="1.1:1:BACK", exposure=4.0, market_id="1.1", selection_id=1, meta={"customer_ref": "r1"})
tm.activate(table_id=2, event_key="1.1:2:BACK", exposure=4.0, market_id="1.1", selection_id=2, meta={"customer_ref": "r2"})
assert app.runtime._persist_recovery_tables()
auth = app.runtime.runtime_authority
print("ATTIVO", auth.owner_id, auth.token, flush=True)
os._exit(9)                                   # crash improvviso con due tavoli attivi: nessun cleanup
"""


@pytest.mark.skipif(not POSIX, reason="processo figlio con os._exit: semantica POSIX")
def test_t05_crash_durante_attivita_multi_tavolo_e_ricostruzione_dell_autorita(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t05.db")
    Database(p)
    out = subprocess.run([sys.executable, "-c", _CRASH_MULTI_TAVOLO, p], cwd=ROOT, capture_output=True,
                         text=True, timeout=120)
    assert out.returncode == 9, out.stderr[-800:]
    _, owner_vecchio, token_vecchio = out.stdout.strip().split()
    lease = Database(p).read_runtime_lease()
    assert lease["owner_id"] == owner_vecchio and int(lease["fencing_token"]) == int(token_vecchio)
    app = _app_su(p, monkeypatch)
    try:
        assert app.runtime.start(execution_mode="SIMULATION").get("started") is True   # detentore morto
        nuova = app.runtime.db.read_runtime_lease()
        assert int(nuova["fencing_token"]) == int(token_vecchio) + 1
        # un "fantasma" del vecchio proprietario (stesso owner/token) non puo' piu' scrivere
        fantasma = Database(p)
        fantasma.bind_runtime_fence(owner_vecchio, int(token_vecchio))
        with pytest.raises(RuntimeFenceViolation):
            fantasma.save_settings({"pr536.fantasma": "x"})
        assert "pr536.fantasma" not in (Database(p).get_settings() or {})
        # il nuovo runtime gestisce piu' tavoli: nessun limite introdotto
        tm = app.runtime.table_manager
        tm.activate(table_id=1, event_key="1.2:1:BACK", exposure=4.0, market_id="1.2", selection_id=1, meta={"customer_ref": "n1"})
        tm.activate(table_id=2, event_key="1.2:2:BACK", exposure=4.0, market_id="1.2", selection_id=2, meta={"customer_ref": "n2"})
        assert app.runtime._persist_recovery_tables()
    finally:
        _ferma(app)


# ---------------------------------------------------------------- riavvio, STOP, token monotono
def test_t06_riavvio_nello_stesso_processo_token_monotono(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t06.db")
    app = _app_su(p, monkeypatch)
    try:
        assert app.runtime.start(execution_mode="SIMULATION").get("started") is True
        t1, owner1 = app.runtime.runtime_authority.token, app.runtime.runtime_authority.owner_id
        app.runtime.stop()
        assert app.runtime.db.runtime_fence() is None
        assert float(app.runtime.db.read_runtime_lease()["expires_at"]) < time.time()
        assert app.runtime.start(execution_mode="SIMULATION").get("started") is True
        assert app.runtime.runtime_authority.token > t1
        stale = Database(p)
        stale.bind_runtime_fence(owner1, t1)
        with pytest.raises(RuntimeFenceViolation):
            stale._execute("INSERT INTO settings (key, value) VALUES (?, ?)", ("pr536.stale", "x"))
    finally:
        _ferma(app)


def test_t07_stop_fallito_ripetuto_e_concorrente(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t07.db")
    app = _app_su(p, monkeypatch)
    rc = app.runtime
    disconnetti = rc.betfair_service.disconnect

    def _guasto(*_a, **_k):
        raise RuntimeError("disconnessione fallita")

    assert rc.start(execution_mode="SIMULATION").get("started") is True
    rc.betfair_service.disconnect = _guasto
    with pytest.raises(RuntimeError):
        rc.stop()                                              # STOP fallito: lock e lease restano
    assert _controller_su(p)._acquire_runtime_instance_lock() == "runtime_already_active_on_database"
    assert float(rc.db.read_runtime_lease()["expires_at"]) > time.time()
    rc.betfair_service.disconnect = disconnetti
    rc.stop()                                                  # STOP ripetuto: rilascio
    assert float(rc.db.read_runtime_lease()["expires_at"]) < time.time()
    assert _controller_su(p)._acquire_runtime_instance_lock() == ""
    assert rc.start(execution_mode="SIMULATION").get("started") is True
    errori = []

    def _stop():
        try:
            rc.stop()
        except Exception as exc:                               # pragma: no cover
            errori.append(exc)

    fili = [threading.Thread(target=_stop) for _ in range(3)]
    for f in fili:
        f.start()
    for f in fili:
        f.join(30)
    assert errori == [] and rc.mode is RuntimeMode.STOPPED
    assert rc.db.runtime_fence() is None and getattr(rc, "_instance_lock_fh", None) is None
    assert _controller_su(p)._acquire_runtime_instance_lock() == ""


# ---------------------------------------------------------------- lease scaduta, DB non disponibile
def test_t08_lease_scaduta_con_vecchio_processo_vivo(tmp_path, monkeypatch):
    monkeypatch.setattr(RuntimeController, "RUNTIME_LEASE_TTL_SEC", 1.0)
    monkeypatch.setattr(RuntimeController, "RUNTIME_LEASE_HEARTBEAT_SEC", 0.3)
    a = _catena_armata(tmp_path, "t08.db")
    a.rc.runtime_authority.stop_heartbeat()                    # il vecchio processo "si blocca" e non rinnova
    os.unlink(_lock_path(str(tmp_path / "t08.db")))           # ... e intanto il suo file di lock e' stato rimosso
    time.sleep(1.3)
    b = _catena_armata(tmp_path, "t08.db")                     # lease scaduta => subentro
    assert b.rc.runtime_authority.token > a.rc.runtime_authority.token
    # il vecchio processo e' vivo e crede di essere ACTIVE: ogni percorso finanziario e' rifiutato
    a.rc._on_signal_received(_signal())
    rifiuti = a.bus.payloads("SIGNAL_REJECTED")
    assert rifiuti and str(rifiuti[-1].get("reason", "")).startswith("runtime_authority_lost:"), rifiuti
    assert a.bus.payloads("CMD_QUICK_BET") == [] and _ordini(a) == []
    assert a.rc.mode is RuntimeMode.LOCKDOWN
    with pytest.raises(RuntimeFenceViolation) as exc:
        a.db.save_settings({"pr536.vecchio": "x"})
    assert exc.value.reason == "fencing_token_superseded"      # il token del vecchio proprietario e' superato
    assert a.rc.reset_cycle()["reset"] is False
    # il nuovo proprietario opera normalmente
    b.rc._on_signal_received(_signal())
    assert len(b.bus.payloads("CMD_QUICK_BET")) == 1 and len(_ordini(b)) == 1
    b.rc.runtime_authority.release()


def test_t08b_lease_superata_nel_database_con_lock_intatto(tmp_path):
    # difesa in profondita': la riga della lease cambia proprietario (altro host, DB ripristinato da backup) mentre
    # il file di lock di A e' intatto => A lo scopre alla verifica e si dichiara superato
    p = str(tmp_path / "t08b.db")
    db_a, db_altro = Database(p), Database(p)
    auth = RuntimeAuthority(db_a, p, lease_ttl_sec=30.0, heartbeat_sec=5.0)
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    auth.verify("ok")
    db_altro._execute("UPDATE runtime_lease SET expires_at = ? WHERE id = 1", (time.time() - 1,))
    assert db_altro.acquire_runtime_lease("ALTRO", ttl_sec=30, now=time.time(), pid=1, host="h2",
                                          lock_identity="z")["acquired"]
    with pytest.raises(RuntimeAuthorityLost) as exc:
        auth.verify("x")
    assert exc.value.reason == "lease_superseded" and auth.lost_reason == "lease_superseded"
    assert auth.heartbeat_once() is False
    with pytest.raises(RuntimeFenceViolation):
        db_a.save_settings({"pr536.a": "1"})
    auth.release()


def test_t09_database_temporaneamente_indisponibile_rifiuto_senza_perdita_definitiva(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t09.db")

    def _giu(*_a, **_k):
        raise sqlite3.OperationalError("database is locked")

    with monkeypatch.context() as m:
        m.setattr(c.db, "read_runtime_lease", _giu)
        c.rc._on_signal_received(_signal())
        rifiuti = c.bus.payloads("SIGNAL_REJECTED")
        assert rifiuti and rifiuti[-1]["reason"] == "runtime_authority_lost:lease_unreadable"
        assert c.bus.payloads("CMD_QUICK_BET") == []
        assert c.rc.mode is RuntimeMode.ACTIVE and c.rc._authority_lost_reason == ""   # transitorio
    c.rc._on_signal_received(_signal(selection_id=12, selection="Under 2.5"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1                               # DB tornato: si opera
    # heartbeat con DB giu': tollerato finche' la lease resta rinnovabile prima della scadenza, poi perdita
    auth = c.rc.runtime_authority
    auth.stop_heartbeat()
    with monkeypatch.context() as m:
        m.setattr(c.db, "renew_runtime_lease", _giu)
        assert auth.heartbeat_once() is True and auth.lost_reason == ""
        auth.lease_expires_at = time.time() + auth.heartbeat_sec / 2       # non piu' rinnovabile in tempo
        assert auth.heartbeat_once() is False and auth.lost_reason == "lease_unrenewable"
    assert c.rc.mode is RuntimeMode.LOCKDOWN


# ---------------------------------------------------------------- perdita durante ammissione ed esecuzione
def test_t10_perdita_dell_autorita_durante_l_ammissione_nessun_ordine(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t10.db")
    lp = _lock_path(str(tmp_path / "t10.db"))
    originale = c.rc._apply_direct_best_price

    def _durante_ammissione(payload):
        os.unlink(lp)                                          # l'operatore cancella il lock a meta' ammissione
        return originale(payload)

    monkeypatch.setattr(c.rc, "_apply_direct_best_price", _durante_ammissione)
    c.rc._on_signal_received(_signal())
    rifiuti = c.bus.payloads("SIGNAL_REJECTED")
    assert rifiuti and rifiuti[-1]["reason"] == "runtime_authority_lost:lock_file_deleted", rifiuti
    assert c.bus.payloads("CMD_QUICK_BET") == [] and _ordini(c) == []
    assert c.rc.mode is RuntimeMode.LOCKDOWN
    assert all(not str(t.get("current_event_key") or "") for t in c.rc.table_manager.snapshot()), \
        "tavolo lasciato riservato da un ingresso rifiutato"


def test_t11_perdita_fra_verifica_e_invio_segnalata_e_lockdown(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t11.db")
    lp = _lock_path(str(tmp_path / "t11.db"))
    eventi = []
    c.bus.subscribe("RUNTIME_AUTHORITY_LOST_AFTER_SEND", lambda pl: eventi.append(dict(pl)))
    originale = c.engine._route_native_microstake

    def _dopo_la_verifica(ctx, payload, modalita):
        os.unlink(lp)                                          # finestra TOCTOU: dopo verify, prima dell'invio
        return originale(ctx, payload, modalita)

    monkeypatch.setattr(c.engine, "_route_native_microstake", _dopo_la_verifica)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert eventi and eventi[0]["reason"].startswith("runtime_authority_lost:lock_file_deleted"), eventi
    assert c.rc.mode is RuntimeMode.LOCKDOWN and c.rc.is_live_allowed() is False
    # l'ordine puo' essere arrivato all'exchange: resta alla riconciliazione, nessun secondo invio
    c.rc._on_signal_received(_signal(selection_id=12, selection="Under 2.5"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert len(_ordini(c)) <= 1


# ---------------------------------------------------------------- fencing nel database
def test_t12_fence_rifiuta_le_scritture_del_proprietario_superato_ma_non_le_letture(tmp_path):
    p = str(tmp_path / "t12.db")
    db1, db2 = Database(p), Database(p)
    now = time.time()
    l1 = db1.acquire_runtime_lease("A", ttl_sec=30, now=now, pid=1, host="h", lock_identity="x")
    assert l1["acquired"] and l1["fencing_token"] == 1
    db1.bind_runtime_fence("A", 1)
    assert db1.record_settlement_effect("k1", {"outcome": "released", "table_id": 1, "pnl": -1.0}, None)
    assert db2.acquire_runtime_lease("B", ttl_sec=30, now=now, pid=2, host="h", lock_identity="y")["acquired"] is False
    assert db1.release_runtime_lease("A", 1, now=now)
    l2 = db2.acquire_runtime_lease("B", ttl_sec=30, now=now, pid=2, host="h", lock_identity="y")
    assert l2["acquired"] and l2["fencing_token"] == 2
    violazioni = []
    db1.bind_runtime_fence("A", 1, on_violation=lambda r: violazioni.append(r))
    with pytest.raises(RuntimeFenceViolation):
        db1.record_settlement_effect("k2", {"outcome": "released", "table_id": 1, "pnl": -1.0}, None)
    with pytest.raises(RuntimeFenceViolation):
        db1._execute("INSERT INTO settings (key, value) VALUES (?, ?)", ("pr536.k", "v"))
    with pytest.raises(RuntimeFenceViolation):
        db1.save_settings({"pr536.k2": "v"})
    with pytest.raises(RuntimeFenceViolation):
        db1.mark_settlement_realized("k1")
    assert violazioni and set(violazioni) == {"fencing_token_superseded"}
    assert db1.get_settlement_effect("k1") is not None and db1.get_settlement_effect("k2") is None   # letture ok
    assert db2.get_settings().get("pr536.k") is None and db2.get_settings().get("pr536.k2") is None
    assert db1.get_settlement_effect("k1")["realized_applied"] == 0
    # renew/release non scavalcano mai un altro proprietario
    assert db1.renew_runtime_lease("A", 1, ttl_sec=30, now=now) is None
    assert db1.release_runtime_lease("A", 1, now=now) is False
    assert db2.read_runtime_lease()["owner_id"] == "B"


def test_t12b_lease_scaduta_rifiutata_anche_al_proprietario(tmp_path):
    db = Database(str(tmp_path / "t12b.db"))
    now = time.time()
    db.acquire_runtime_lease("A", ttl_sec=1, now=now - 5, pid=1, host="h", lock_identity="x")
    db.bind_runtime_fence("A", 1)
    with pytest.raises(RuntimeFenceViolation) as exc:
        db.save_settings({"pr536.x": "1"})
    assert exc.value.reason == "lease_expired"
    assert db.renew_runtime_lease("A", 1, ttl_sec=30, now=now) is None       # scaduta: non si rinnova
    db.unbind_runtime_fence()
    assert db.acquire_runtime_lease("B", ttl_sec=30, now=now, pid=2, host="h", lock_identity="y")["acquired"]


# ---------------------------------------------------------------- nessuna duplicazione, nessun rilascio prematuro
def test_t13_runtime_superato_non_rilascia_riserve_ne_scrive_effetti(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t13.db")
    c.rc._on_signal_received(_signal())
    assert len(_ordini(c)) == 1
    tavolo = next(t for t in c.rc.table_manager.snapshot() if str(t.get("current_event_key") or ""))
    os.unlink(_lock_path(str(tmp_path / "t13.db")))
    assert c.rc.runtime_authority.heartbeat_once() is False
    assert c.rc.mode is RuntimeMode.LOCKDOWN
    assert c.rc._release_final_unmatched_orders() == []                       # F028 senza autorita': nulla
    assert c.rc.reset_cycle()["reset"] is False
    ancora = c.rc.table_manager.get_table(int(tavolo["table_id"]))
    assert ancora.current_event_key == tavolo["current_event_key"], "riserva rilasciata senza autorita'"
    prima = c.db.get_settings()
    bet = next(str(o.bet_id) for o in c.broker.state.orders.values())
    c.rc._on_close_position({"event_key": f"cleared:1.234:{bet}", "market_id": "1.234", "table_id": None,
                             "batch_id": "", "pnl": -4.0, "gross_pnl": -4.0, "net_pnl": -4.0,
                             "commission_amount": 0.0, "commission_pct": 4.5, "settlement_ref": bet,
                             "settlement_source": "betfair_cleared_orders", "settlement_kind": "realized_settlement"})
    assert c.db.get_settlement_effect(f"cleared:1.234:{bet}") is None, "effetto scritto da un runtime superato"
    assert c.db.get_settings() == prima, "stato persistito cambiato da un runtime superato"


# ---------------------------------------------------------------- invarianti di avvio e unita'
def test_t15_start_arma_sempre_l_autorita_su_un_database_su_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t15.db")
    app = _app_su(p, monkeypatch)
    rt = app.runtime
    try:
        assert rt.start(execution_mode="SIMULATION").get("started") is True
        auth = rt.runtime_authority
        lease = rt.db.read_runtime_lease()
        assert lease["owner_id"] == auth.owner_id and int(lease["fencing_token"]) == auth.token
        assert rt.db.runtime_fence() == {"owner_id": auth.owner_id, "token": auth.token}
        assert lease["lock_identity"] == auth.lock_identity and auth.lock_identity
        with open(_lock_path(p)) as fh:
            record = json.loads(fh.readline())
        assert record["pid"] == os.getpid() and record["owner_id"] == auth.owner_id
        assert auth._thread is not None and auth._thread.is_alive()
        assert rt._runtime_authority_block("test") == ""
    finally:
        _ferma(app)
    assert rt.db.runtime_fence() is None and rt.runtime_authority.released


def test_t16_heartbeat_rinnova_la_lease_e_non_cambia_il_token(tmp_path, monkeypatch):
    monkeypatch.setattr(RuntimeController, "RUNTIME_LEASE_TTL_SEC", 1.0)
    monkeypatch.setattr(RuntimeController, "RUNTIME_LEASE_HEARTBEAT_SEC", 0.25)
    c = _catena_armata(tmp_path, "t16.db")
    token = c.rc.runtime_authority.token
    time.sleep(2.2)                                            # > 2 TTL: senza heartbeat sarebbe scaduta
    lease = c.db.read_runtime_lease()
    assert float(lease["expires_at"]) > time.time() and int(lease["fencing_token"]) == token
    assert c.rc.mode is RuntimeMode.ACTIVE and c.rc._runtime_authority_block("test") == ""
    c.rc.runtime_authority.release()


def test_t17_holder_alive_dimostrabile_solo_sullo_stesso_host():
    me = __import__("socket").gethostname()
    assert _holder_alive(os.getpid(), me, my_host=me) is True
    assert _holder_alive(os.getpid(), "altro-host", my_host=me) is None
    assert _holder_alive(0, me, my_host=me) is None and _holder_alive("x", me, my_host=me) is None
    if POSIX:
        morto = subprocess.Popen([sys.executable, "-c", "pass"])
        morto.wait(30)
        assert _holder_alive(morto.pid, me, my_host=me) is False


def test_t18_verify_senza_lease_e_dopo_rilascio_fail_closed(tmp_path):
    p = str(tmp_path / "t18.db")
    db = Database(p)
    auth = RuntimeAuthority(db, p, lease_ttl_sec=2.0, heartbeat_sec=0.5)
    with pytest.raises(RuntimeAuthorityLost):
        auth.verify("x")                                       # lock mai preso
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    auth.verify("x")
    auth.release()
    with pytest.raises(RuntimeAuthorityLost) as exc:
        auth.verify("x")
    assert exc.value.reason == "released"
    with pytest.raises(ValueError):
        RuntimeAuthority(db, p, lease_ttl_sec=1.0, heartbeat_sec=2.0)   # heartbeat >= ttl: configurazione rifiutata


# ---------------------------------------------------------------- auto-next, engine, cashout
def test_t19_auto_next_rifiutato_se_l_autorita_cade_prima_dell_invio(tmp_path, monkeypatch):
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _close_payload

    c = _catena_armata(tmp_path, "t19.db")
    c.rc.risk_desk.sync_bankroll(100.0)
    lp = _lock_path(str(tmp_path / "t19.db"))
    originale = c.rc._attach_single_entry_liquidity

    def _prima_dell_invio(payload):
        os.unlink(lp)                                          # fra ammissione dell'auto-next e CMD_QUICK_BET
        return originale(payload)

    monkeypatch.setattr(c.rc, "_attach_single_entry_liquidity", _prima_dell_invio)
    c.rc._on_close_position(_close_payload("batch-t19"))
    assert c.bus.payloads("CMD_QUICK_BET") == [] and _ordini(c) == []
    esito = c.rc._last_auto_trade_result
    assert esito["submitted"] is False and str(esito["reason"]).startswith("runtime_authority_lost:"), esito
    assert c.rc.mode is RuntimeMode.LOCKDOWN
    assert all(not str(t.get("current_event_key") or "") for t in c.rc.table_manager.snapshot()), \
        "tavolo dell'auto-next lasciato riservato"


def test_t20_il_trading_engine_rifiuta_l_invio_senza_autorita_anche_se_il_runtime_non_lo_ferma(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t20.db")
    c.rc._on_signal_received(_signal())
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    assert len(_ordini(c)) == 1
    os.unlink(_lock_path(str(tmp_path / "t20.db")))
    # il runtime viene "ingannato" (gate a monte neutralizzato): deve fermarsi il choke point dell'engine
    monkeypatch.setattr(c.rc, "_runtime_authority_block", lambda context: "" if context == "signal_submit" else
                        RuntimeController._runtime_authority_block(c.rc, context))
    secondo = dict(cmd)
    secondo["customer_ref"] = cmd["customer_ref"][:-3] + "zz9"
    secondo["correlation_id"] = "t20-secondo"
    secondo["selection_id"] = 12
    esito = c.engine.submit_quick_bet(secondo)
    assert str(esito.get("status") or "").upper() not in ("SUBMITTED", "ACCEPTED", "SUCCESS"), esito
    assert len(_ordini(c)) == 1, "ordine inviato dall'engine senza autorita'"
    # a fermarlo e' l'engine all'ammissione, prima di persistere l'intento (non il fence del DB a valle)
    assert str(esito.get("reason") or "").startswith("runtime_authority_lost:lock_file_deleted"), esito.get("reason")
    assert esito["status"] == "DENIED"


def test_t20b_verifica_anche_subito_prima_del_trasporto(tmp_path, monkeypatch):
    """L'autorita' persa fra ammissione e trasporto: l'ultima verifica prima dell'exchange ferma l'invio."""
    c = _catena_armata(tmp_path, "t20b.db")
    c.rc._on_signal_received(_signal())
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    originale = c.rc.assert_runtime_authority
    lp = _lock_path(str(tmp_path / "t20b.db"))

    def _perde_dopo_l_ammissione(context=""):
        if context == "order_admission":
            originale(context)
            os.unlink(lp)                                      # perdita subito dopo l'ammissione
            return None
        return originale(context)

    monkeypatch.setattr(c.rc, "assert_runtime_authority", _perde_dopo_l_ammissione)
    secondo = dict(cmd, customer_ref=cmd["customer_ref"][:-3] + "zz7", correlation_id="t20b", selection_id=14)
    esito = c.engine.submit_quick_bet(secondo)
    assert len(_ordini(c)) == 1, "ordine inviato all'exchange senza autorita'"
    assert str(esito.get("error") or "").startswith("runtime_authority_lost:lock_file_deleted"), esito.get("error")
    assert c.rc.mode is RuntimeMode.LOCKDOWN


def test_t21_cashout_rifiutato_senza_autorita(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t21.db")
    instradati = []
    monkeypatch.setattr(c.rc, "_cashout_chain_wired", lambda: True)
    monkeypatch.setattr(c.rc, "_execute_cashout_route", lambda signal: instradati.append(dict(signal)))
    c.rc._route_cashout_signal_under_barrier({"signal_type": "CASHOUT_ALL"}, 1)
    assert len(instradati) == 1                                # con autorita': instradato
    os.unlink(_lock_path(str(tmp_path / "t21.db")))
    c.rc._route_cashout_signal_under_barrier({"signal_type": "CASHOUT_ALL"}, 2)
    assert len(instradati) == 1, "cashout instradato senza autorita'"
    rifiuti = c.bus.payloads("SIGNAL_REJECTED")
    assert rifiuti and rifiuti[-1]["reason"].startswith("runtime_authority_lost:lock_file_deleted")
    assert c.rc.mode is RuntimeMode.LOCKDOWN


# ---------------------------------------------------------------- mutazioni sopravvissute al primo giro (i536_*)
def test_t22_sweep_f028_non_rilascia_nulla_senza_autorita(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t22.db")
    monkeypatch.setattr(c.rc, "_settlement_reconciliation_allowed", lambda: True)
    monkeypatch.setattr(c.rc, "_uncertain_reservations_map",
                        lambda: (_ for _ in ()).throw(AssertionError("sweep F028 avviato senza autorita'")))
    os.unlink(_lock_path(str(tmp_path / "t22.db")))
    assert c.rc._release_final_unmatched_orders() == []
    assert c.rc.mode is RuntimeMode.LOCKDOWN


def test_t23_perdita_registrata_blocca_anche_senza_oggetto_autorita(tmp_path):
    """Il fence puo' segnalare la perdita quando `runtime_authority` non c'e' piu' (release concorrente):
    il motivo registrato sul controller basta a rifiutare ogni percorso finanziario."""
    c = _chain(tmp_path, db_name="t23.db")
    assert c.rc.runtime_authority is None
    c.rc._on_runtime_authority_lost("fence_violation")
    assert c.rc._runtime_authority_block("signal_submit") == "runtime_authority_lost:fence_violation"
    with pytest.raises(RuntimeAuthorityLost):
        c.rc.assert_runtime_authority("order_submit")
    c.rc._on_signal_received(_signal())
    assert c.bus.payloads("CMD_QUICK_BET") == [] and _ordini(c) == []


def test_t24_is_live_allowed_false_dopo_la_perdita_dell_autorita(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t24.db")
    c.rc.execution_mode = "LIVE"
    c.rc.live_enabled = True
    monkeypatch.setattr(c.rc, "get_deploy_gate_status", lambda **kw: {"allowed": True, "readiness": "READY"})
    assert c.rc.is_live_allowed() is True                      # tutti gli altri gate aperti (SIM broker, nessuna chiamata live)
    os.unlink(_lock_path(str(tmp_path / "t24.db")))
    assert c.rc.is_live_allowed() is False                     # verifica lock + lease, non solo lo stato in memoria
    assert c.rc._authority_lost_reason == "lock_file_deleted" and c.rc.live_enabled is False
    c.rc.live_enabled = True                                   # riabilitato senza un nuovo start(): resta vietato
    assert c.rc.is_live_allowed() is False
    assert c.rc.get_effective_execution_mode() == "SIMULATION"


def test_t25_fence_rifiuta_un_token_superato_anche_dello_stesso_owner(tmp_path):
    p = str(tmp_path / "t25.db")
    db = Database(p)
    primo = db.acquire_runtime_lease("owner-x", ttl_sec=30.0, now=time.time(), pid=1, host="h", lock_identity="1:1")
    assert primo["acquired"] and primo["fencing_token"] == 1
    violazioni = []
    db.bind_runtime_fence("owner-x", 1, on_violation=violazioni.append)
    with db.transaction() as conn:
        conn.execute("INSERT INTO cycle_recovery_checkpoints (settlement_key, cycle_id, created_at, updated_at) VALUES (?, ?, ?, ?)", ("k1", "ok", "t", "t"))
    secondo = db.acquire_runtime_lease("owner-x", ttl_sec=30.0, now=time.time(), pid=1, host="h", lock_identity="1:1")
    assert secondo["acquired"] and secondo["fencing_token"] == 2      # stesso owner, riavvio: token maggiore
    with pytest.raises(RuntimeFenceViolation) as exc:
        with db.transaction() as conn:
            conn.execute("INSERT INTO cycle_recovery_checkpoints (settlement_key, cycle_id, created_at, updated_at) VALUES (?, ?, ?, ?)", ("k2", "vecchio", "t", "t"))
    assert exc.value.reason == "fencing_token_superseded" and violazioni == ["fencing_token_superseded"]
    with pytest.raises(RuntimeFenceViolation):
        db._execute("INSERT INTO cycle_recovery_checkpoints (settlement_key, cycle_id, created_at, updated_at) VALUES (?, ?, ?, ?)", ("k3", "vecchio2", "t", "t"))
    assert db._execute("SELECT COUNT(*) AS n FROM cycle_recovery_checkpoints WHERE cycle_id LIKE 'vecchio%'", fetchone=True)["n"] == 0


def test_t26_verify_rifiuta_una_lease_scaduta_senza_aspettare_l_heartbeat(tmp_path):
    p = str(tmp_path / "t26.db")
    db = Database(p)
    orologio = [1000.0]
    auth = RuntimeAuthority(db, p, lease_ttl_sec=5.0, heartbeat_sec=1.0, clock=lambda: orologio[0])
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    auth.verify("x")
    orologio[0] += 5.5                                          # oltre la scadenza, nessun rinnovo nel frattempo
    with pytest.raises(RuntimeAuthorityLost) as exc:
        auth.verify("x")
    assert exc.value.reason == "lease_expired" and auth.lost_reason == "lease_expired"
    assert db.renew_runtime_lease(auth.owner_id, int(auth.token), ttl_sec=5.0, now=orologio[0]) is None
    auth.release()


# ---------------------------------------------------------------- hedge del cashout (rilievo interno su fc8f136)
def _cashout_wired(c):
    import cashout_wiring
    return cashout_wiring.cabla_catena_cashout(
        bus=c.bus, betfair_service=c.rc.betfair_service, persist=lambda r: None,
        notify=lambda text, *, severity="HIGH": None,
        microstake_executor_provider=c.engine.native_microstake_executor,
        runtime_authority_check=c.rc.cashout_authority_check,
        runtime_authority_operation=c.rc.authority_operation)


def test_t27_hedge_del_cashout_rifiutato_al_piazzamento_senza_autorita(tmp_path):
    """Il bus e' asincrono: l'hedge parte dall'esecutore dopo il routing. Phase 0 su fc8f136: con il lock
    cancellato l'hedge arrivava al broker. Ora l'esecutore verifica l'autorita' subito prima dell'invio."""
    from tests.test_microstake_cashout_and_recovery import _book, _cashout_cmd

    c = _catena_armata(tmp_path, "t27.db")
    _cashout_wired(c)
    _book(c.broker, "1.100", 7, lay=(3.0, 50.0))
    c.bus.publish("CMD_EXECUTE_CASHOUT", _cashout_cmd(2.0))
    assert len(c.broker.state.orders) == 1                     # con autorita': l'hedge parte
    os.unlink(_lock_path(str(tmp_path / "t27.db")))
    c.bus.publish("CMD_EXECUTE_CASHOUT", {**_cashout_cmd(2.0), "position_id": "p2"})
    assert len(c.broker.state.orders) == 1, "hedge piazzato da un runtime senza autorita'"
    falliti = c.bus.payloads("CASHOUT_FAILED")
    assert falliti and falliti[-1]["status"] == "REJECTED"
    assert falliti[-1]["reason"].startswith("runtime_authority_lost:lock_file_deleted")
    assert c.rc.mode is RuntimeMode.LOCKDOWN


def test_t28_esecutore_fail_closed_se_la_verifica_dell_autorita_solleva(tmp_path):
    from tests.test_microstake_cashout_and_recovery import _book, _cashout_cmd

    c = _catena_armata(tmp_path, "t28.db")
    catena = _cashout_wired(c)
    catena.cashout_executor.authority_check = lambda: (_ for _ in ()).throw(RuntimeError("db giu'"))
    _book(c.broker, "1.100", 7, lay=(3.0, 50.0))
    c.bus.publish("CMD_EXECUTE_CASHOUT", _cashout_cmd(2.0))
    assert not c.broker.state.orders
    assert c.bus.payloads("CASHOUT_FAILED")[-1]["reason"] == "runtime_authority_check_failed"


def test_t29_headless_cabla_la_verifica_dell_autorita_nell_esecutore_del_cashout(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t29.db")
    app = _app_su(p, monkeypatch)
    try:
        assert app.runtime.start(execution_mode="SIMULATION").get("started") is True
        check = app.cashout_executor.authority_check
        assert callable(check) and check() == ""
        with app.cashout_executor.authority_operation():        # l'hedge in volo e' contato dal runtime
            assert app.runtime._authority_ops == 1
        assert app.runtime._authority_ops == 0
        os.unlink(_lock_path(p))
        assert check().startswith("runtime_authority_lost:lock_file_deleted")
        assert app.runtime.mode is RuntimeMode.LOCKDOWN
    finally:
        _ferma(app)


def test_t30_la_gui_cabla_la_verifica_dell_autorita_nell_esecutore_del_cashout(tmp_path):
    """Stesso filo della GUI (MiniPickfairGUI reale in test_mode): l'esecutore non resta senza verifica."""
    import mini_gui
    from tests.guardrails.test_cablaggio_runtime import _TelegramTabUISenzaWidget

    mp = pytest.MonkeyPatch()
    mp.setattr(mini_gui, "Database", lambda: Database(str(tmp_path / "gui.db")))
    mp.setattr(mini_gui, "TelegramTabUI", _TelegramTabUISenzaWidget)
    try:
        gui = mini_gui.MiniPickfairGUI(test_mode=True, force_simulation=True)
        try:
            check = gui.cashout_executor.authority_check
            assert callable(check) and check() == ""           # runtime costruito, non avviato: nulla da verificare
            with gui.cashout_executor.authority_operation():    # l'hedge in volo e' contato dal runtime
                assert gui.runtime._authority_ops == 1
            gui.runtime._on_runtime_authority_lost("fence_violation")
            assert check() == "runtime_authority_lost:fence_violation"
        finally:
            gui.shutdown.shutdown()
    finally:
        mp.undo()


# ---------------------------------------------------------------- rilievi Codex su fc8f136 (RED prima della patch)
def test_t31_scrittura_autocommit_serializzata_con_il_subentro(tmp_path):
    """Codex P1 database.py:260: controllo del fence e scrittura autocommit nella stessa transazione. Un subentro
    tentato fra il controllo e la scrittura deve attendere il commit, mai precederlo."""
    p = str(tmp_path / "t31.db")
    db, altro = Database(p), Database(p)
    assert db.acquire_runtime_lease("a", ttl_sec=30.0, now=time.time(), pid=1, host="h", lock_identity="")["acquired"]
    db.bind_runtime_fence("a", 1)
    originale = db._assert_runtime_fence
    eventi = []

    def _subentro():
        esito = altro.acquire_runtime_lease("b", ttl_sec=30.0, now=time.time(), pid=2, host="h", lock_identity="",
                                            holder_alive=lambda pid, host: False)
        eventi.append(("subentro", esito["acquired"]))

    def _controllo_poi_subentro(conn, name):
        originale(conn, name)
        t = threading.Thread(target=_subentro, daemon=True)
        t.start()
        t.join(0.5)                                            # se il subentro non e' serializzato, finisce qui

    db._assert_runtime_fence = _controllo_poi_subentro
    db._execute("INSERT INTO cycle_recovery_checkpoints (settlement_key, cycle_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?)", ("k31", "a", "t", "t"))
    eventi.append(("scrittura", True))
    assert _attendi(lambda: len(eventi) == 2, 5.0)
    assert eventi[0] == ("scrittura", True), f"subentro completato prima del commit della scrittura: {eventi}"


def test_t32_dopo_lo_stop_nessun_invio_finanziario(tmp_path, monkeypatch):
    """Codex P1 runtime_controller.py:2773: un'autorita' rilasciata allo STOP non vale come 'nessuna autorita''."""
    monkeypatch.chdir(tmp_path)
    app = _app_su(str(tmp_path / "t32.db"), monkeypatch)
    try:
        rt = app.runtime
        assert rt.start(execution_mode="SIMULATION").get("started") is True
        assert rt.stop().get("stopped", True) is not False
        assert rt.runtime_authority is not None and rt.runtime_authority.released
        assert rt._runtime_authority_block("order_submit").startswith("runtime_authority_released")
        with pytest.raises(RuntimeAuthorityLost):
            rt.assert_runtime_authority("order_submit")
    finally:
        _ferma(app)


def test_t33_ttl_unmatched_non_cancella_senza_autorita(tmp_path, monkeypatch):
    """Codex P1 runtime_controller.py:2818: il feed resta vivo dopo la perdita; il TTL non deve cancellare."""
    c = _catena_armata(tmp_path, "t33.db")
    monkeypatch.setattr(c.rc.config, "direct_unmatched_ttl_enabled", True, raising=False)
    c.rc._direct_order_bet_ids = {"ref-1": "B1"}
    c.rc._last_direct_ttl_poll_at = -1e9
    chiamate = []
    monkeypatch.setattr(c.rc.betfair_service, "list_current_orders", lambda *a, **k: chiamate.append(1) or [],
                        raising=False)
    os.unlink(_lock_path(str(tmp_path / "t33.db")))
    c.rc._poll_direct_unmatched_ttl()
    assert chiamate == [], "TTL unmatched eseguito da un runtime senza autorita'"


def test_t34_start_rifiutato_finche_lo_stop_non_ha_smontato_l_autorita_persa(tmp_path):
    """Codex P1 runtime_controller.py:2729: lease scaduta/irrinnovabile con lock intatto => niente nuova epoca
    sulla stessa autorita' senza STOP."""
    c = _catena_armata(tmp_path, "t34.db")
    c.rc._on_runtime_authority_lost("lease_unrenewable")
    assert c.rc.runtime_authority.lost_reason == "lease_unrenewable"
    token_prima = int(c.db.read_runtime_lease()["fencing_token"])
    blocco = c.rc._acquire_runtime_lease()
    assert blocco == "runtime_authority_lost:lease_unrenewable:stop_required", blocco
    assert int(c.db.read_runtime_lease()["fencing_token"]) == token_prima, "nuova lease scritta senza STOP"
    assert c.rc._authority_lost_reason == "lease_unrenewable"   # la perdita non viene cancellata


def test_t35_rinnovo_misurato_dopo_l_attesa_del_lock_del_database(tmp_path):
    """Codex P1 runtime_authority.py:232: l'istante del rinnovo si legge dopo BEGIN IMMEDIATE, non prima."""
    p = str(tmp_path / "t35.db")
    db = Database(p)
    orologio = [1000.0]
    auth = RuntimeAuthority(db, p, lease_ttl_sec=30.0, heartbeat_sec=10.0, clock=lambda: orologio[0])
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    orologio[0] = 1010.0                                       # heartbeat a T+10
    originale = db._lease_transaction

    def _attesa_del_lock(name, fn):
        orologio[0] = 1035.0                                   # il lock arriva a T+35: lease gia' scaduta a T+30
        return originale(name, fn)

    db._lease_transaction = _attesa_del_lock
    assert auth.heartbeat_once() is False
    assert auth.lost_reason in ("lease_superseded", "lease_expired"), auth.lost_reason
    riga = db.read_runtime_lease()
    assert float(riga["expires_at"]) <= 1035.0, "lease scaduta resuscitata dal rinnovo tardivo"
    db._lease_transaction = originale
    auth.release()


def test_t36_nessun_deadlock_fra_heartbeat_e_violazione_del_fence(tmp_path):
    """Codex P2 runtime_authority.py:223: il thread che tiene il write lock del DB e segnala la perdita non deve
    attendere l'heartbeat, che a sua volta attende il write lock."""
    p = str(tmp_path / "t36.db")
    db = Database(p)
    auth = RuntimeAuthority(db, p, lease_ttl_sec=30.0, heartbeat_sec=10.0)
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    fatto = []

    def _thread_db():
        with db._write_lock:                                   # come una transazione che rileva la violazione
            hb = threading.Thread(target=auth.heartbeat_once, daemon=True)
            hb.start()
            time.sleep(0.3)                                    # l'heartbeat e' in attesa del write lock
            auth.mark_lost("fencing_token_superseded")
            fatto.append("mark_lost")

    t = threading.Thread(target=_thread_db, daemon=True)
    t.start()
    t.join(5.0)
    assert fatto == ["mark_lost"], "deadlock fra heartbeat e segnalazione della perdita"
    assert auth.lost_reason == "fencing_token_superseded"


# ---------------------------------------------------------------- rilievi su 125aa05 (Codex, Grok) — RED prima della patch
def test_t37_cancel_del_cashout_e_del_ttl_rifiutati_senza_autorita(tmp_path, monkeypatch):
    """Codex P1 runtime_controller.py:3200: i cancel del router e del TTL verificano l'autorita' al momento
    della chiamata al broker, non solo al routing."""
    import core.runtime_controller as modulo
    from cashout_cancel_adapter import CashoutCancelAdapter

    c = _catena_armata(tmp_path, "t37.db")
    costruiti = []

    class _Registra(CashoutCancelAdapter):
        def __init__(self, **kw):
            super().__init__(**kw)
            costruiti.append(self)

    monkeypatch.setattr(modulo, "CashoutCancelAdapter", _Registra)
    monkeypatch.setattr(c.rc.betfair_service, "is_simulation_mode", lambda: True, raising=False)
    c.rc._execute_cashout_route({"signal_type": "CASHOUT", "market_id": "1.1", "selection_id": 7})
    adapter_ttl = c.rc._direct_ttl_cancel_adapter()
    assert len(costruiti) == 2 and adapter_ttl in costruiti
    chiamate = []
    for adapter in costruiti:
        adapter.sim_cancel = adapter.live_cancel = (
            lambda **kw: chiamate.append(kw) or {"instructionReports": [{"status": "SUCCESS"}]})
        assert adapter.cancel("1.1", ["B0"]) is True                # con autorita': confermato
    assert len(chiamate) == 2
    chiamate.clear()
    os.unlink(_lock_path(str(tmp_path / "t37.db")))
    for adapter in costruiti:
        assert adapter.cancel("1.1", ["B1"]) is False
    assert chiamate == [], "cancel inviato al broker da un runtime senza autorita'"


def test_t38_cancel_del_residuo_del_cashout_rifiutato_senza_autorita(tmp_path, monkeypatch):
    c = _catena_armata(tmp_path, "t38.db")
    monkeypatch.setattr(c.rc.betfair_service, "is_simulation_mode", lambda: True, raising=False)
    catena = _cashout_wired(c)
    adapter = catena.cashout_residual_handler._cancel.__self__
    chiamate = []
    adapter.sim_cancel = adapter.live_cancel = lambda **kw: chiamate.append(kw) or {"instructionReports": [{"status": "SUCCESS"}]}
    assert adapter.cancel("1.1", ["B0"]) is True and len(chiamate) == 1   # con autorita': confermato
    chiamate.clear()
    os.unlink(_lock_path(str(tmp_path / "t38.db")))
    assert adapter.cancel("1.1", ["B1"]) is False and chiamate == []


def test_t39_nessuna_riga_inflight_se_l_autorita_manca_all_ammissione(tmp_path):
    """Codex P2 trading_engine.py:2175: la verifica precede la persistenza dell'intento."""
    c = _catena_armata(tmp_path, "t39.db")
    c.rc._on_signal_received(_signal())
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    righe_prima = len(c.db._execute("SELECT id FROM orders", fetch=True))
    os.unlink(_lock_path(str(tmp_path / "t39.db")))
    secondo = dict(cmd, customer_ref=cmd["customer_ref"][:-3] + "zz8", correlation_id="t39", selection_id=13)
    esito = c.engine.submit_quick_bet(secondo)
    assert "runtime_authority_lost" in str(esito.get("error") or esito.get("reason") or ""), esito
    audit = [e["type"] for e in (esito.get("audit") or {}).get("events", [])]
    assert "PERSIST_INFLIGHT" not in audit, audit
    assert len(c.db._execute("SELECT id FROM orders", fetch=True)) == righe_prima


def test_t40_stop_attende_le_operazioni_in_volo_prima_di_sciogliere_il_fence(tmp_path, monkeypatch):
    """Codex P1 runtime_authority.py:320: lo STOP non scioglie fence e lease mentre un invio e' in volo; finche'
    non si svuota nessun nuovo invio e nessuna nuova epoca."""
    monkeypatch.setattr(RuntimeController, "RUNTIME_AUTHORITY_DRAIN_SEC", 0.5)
    c = _catena_armata(tmp_path, "t40.db")
    auth = c.rc.runtime_authority
    with c.rc.authority_operation():
        c.rc.mode = RuntimeMode.STOPPED
        c.rc._release_runtime_instance_lock_if_stopped()
        assert not auth.released and c.db.runtime_fence() is not None, "fence sciolto con un invio in volo"
        assert c.rc._runtime_authority_block("order_submit") == "runtime_authority_draining"
        assert c.rc._acquire_runtime_lease() == "runtime_authority_draining"
    c.rc._release_runtime_instance_lock_if_stopped()
    assert auth.released and c.db.runtime_fence() is None


def test_t41_rilascio_della_lease_fallito_non_dichiara_lo_stop_pulito(tmp_path, monkeypatch):
    """Codex P2 runtime_authority.py:317: lease non rilasciata => autorita', lock e fence restano."""
    c = _catena_armata(tmp_path, "t41.db")
    auth = c.rc.runtime_authority

    def _db_giu(*a, **k):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(c.db, "release_runtime_lease", _db_giu)
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._release_runtime_instance_lock_if_stopped()
    assert not auth.released and auth.lock_fh is not None and c.db.runtime_fence() is not None
    assert c.rc._instance_lock_fh is auth.lock_fh
    monkeypatch.undo()
    c.rc._release_runtime_instance_lock_if_stopped()           # al ritentativo il rilascio riesce
    assert auth.released and c.db.read_runtime_lease()["expires_at"] < time.time()


def test_t42_rinnovo_riuscito_dopo_una_perdita_concorrente_viene_annullato(tmp_path):
    """Grok su 125aa05: se la perdita scatta mentre il rinnovo e' in corso, la lease rinnovata torna scaduta."""
    p = str(tmp_path / "t42.db")
    db = Database(p)
    auth = RuntimeAuthority(db, p, lease_ttl_sec=30.0, heartbeat_sec=10.0)
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    originale = db.renew_runtime_lease

    def _perdita_durante_il_rinnovo(*a, **k):
        out = originale(*a, **k)
        with auth._lock:
            auth.lost_reason = "fencing_token_superseded"      # mark_lost concorrente gia' passato
        return out

    db.renew_runtime_lease = _perdita_durante_il_rinnovo
    assert auth.heartbeat_once() is False
    assert float(db.read_runtime_lease()["expires_at"]) < time.time(), "lease rinnovata per un'autorita' persa"


def test_t43_lo_stop_attende_anche_l_hedge_del_cashout_in_volo(tmp_path, monkeypatch):
    """Grok su 1542b5d: l'hedge del cashout e' un'operazione in volo come l'invio dell'engine; lo STOP non
    scioglie il fence mentre l'esecutore sta piazzando."""
    from tests.test_microstake_cashout_and_recovery import _book, _cashout_cmd

    monkeypatch.setattr(RuntimeController, "RUNTIME_AUTHORITY_DRAIN_SEC", 0.3)
    c = _catena_armata(tmp_path, "t43.db")
    catena = _cashout_wired(c)
    _book(c.broker, "1.100", 7, lay=(3.0, 50.0))
    auth = c.rc.runtime_authority
    visto = {}
    originale = catena.order_router.place

    def _stop_durante_il_piazzamento(payload):
        c.rc.mode = RuntimeMode.STOPPED
        c.rc._release_runtime_instance_lock_if_stopped()        # STOP concorrente all'hedge
        visto["fence"] = c.db.runtime_fence()
        visto["released"] = auth.released
        return originale(payload)

    monkeypatch.setattr(catena.order_router, "place", _stop_durante_il_piazzamento)
    c.bus.publish("CMD_EXECUTE_CASHOUT", _cashout_cmd(2.0))
    assert visto["released"] is False and visto["fence"] is not None, "fence sciolto con un hedge in volo"
    c.rc._release_runtime_instance_lock_if_stopped()            # finito l'hedge, lo STOP successivo rilascia
    assert auth.released


# ---------------------------------------------------------------- rilievi Codex su 1542b5d (RED prima della patch)
def test_t44_transazione_esplicita_fenced_e_serializzata_col_subentro(tmp_path):
    """Codex P1 database.py:256: begin_transaction()/commit_transaction() (riconciliazione) passano dal fence."""
    p = str(tmp_path / "t44.db")
    db, altro = Database(p), Database(p)
    assert db.acquire_runtime_lease("a", ttl_sec=30.0, now=time.time(), pid=1, host="h", lock_identity="")["acquired"]
    db.bind_runtime_fence("a", 1)
    eventi = []

    def _subentro():
        esito = altro.acquire_runtime_lease("b", ttl_sec=30.0, now=time.time(), pid=2, host="h", lock_identity="",
                                            holder_alive=lambda pid, host: False)
        eventi.append(("subentro", esito["acquired"]))

    db.begin_transaction()
    t = threading.Thread(target=_subentro, daemon=True)
    t.start()
    t.join(0.5)
    db._execute("INSERT INTO cycle_recovery_checkpoints (settlement_key, cycle_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?)", ("k44", "a", "t", "t"))
    db.commit_transaction()
    eventi.append(("commit", True))
    assert _attendi(lambda: len(eventi) == 2, 5.0)
    assert eventi[0] == ("commit", True), f"subentro completato dentro la transazione esplicita: {eventi}"
    with pytest.raises(RuntimeFenceViolation):                 # ora la lease e' di "b": nessuna nuova transazione
        db.begin_transaction()
    assert not db._in_transaction()


def test_t45_lo_stop_ritenta_la_scadenza_della_lease_dopo_una_perdita(tmp_path, monkeypatch):
    """Codex P2 runtime_authority.py:328: mark_lost non riesce a scadere la lease => lo STOP ritenta."""
    p = str(tmp_path / "t45.db")
    db = Database(p)
    auth = RuntimeAuthority(db, p, lease_ttl_sec=30.0, heartbeat_sec=10.0)
    assert auth.acquire_os_lock() == "" and auth.acquire_lease() == ""
    originale = db.release_runtime_lease

    def _db_giu(*a, **k):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(db, "release_runtime_lease", _db_giu)
    auth.mark_lost("lease_unrenewable")
    assert float(db.read_runtime_lease()["expires_at"]) > time.time()   # lease ancora viva
    monkeypatch.setattr(db, "release_runtime_lease", originale)
    assert auth.release() is True
    assert float(db.read_runtime_lease()["expires_at"]) < time.time(), "lease lasciata viva dopo lo STOP"


def test_t46_emergency_stop_non_cancella_da_un_runtime_superato(tmp_path, monkeypatch):
    """Codex P1 runtime_controller.py:2857: il cancel-all di mercato richiede l'autorita' corrente."""
    c = _catena_armata(tmp_path, "t46.db")
    chiamate = []

    class _Live:
        def cancel_orders(self, **kw):
            chiamate.append(kw)
            return {"ok": True, "cancelled_count": 1}

    monkeypatch.setattr(c.rc.db, "get_pending_sagas", lambda: [{"market_id": "1.1", "bet_id": "B1"}], raising=False)
    monkeypatch.setattr(c.rc.betfair_service, "get_live_client", lambda: _Live(), raising=False)
    os.unlink(_lock_path(str(tmp_path / "t46.db")))
    esito = c.rc.emergency_stop("prova")
    assert chiamate == [], "cancel-all di mercato inviato da un runtime senza autorita'"
    assert any(e.get("stage") == "runtime_authority" for e in esito.get("cancel_errors", [])), esito


def test_t47_candidato_ttl_conservato_se_l_autorita_blocca_il_cancel(tmp_path):
    """Codex P2 runtime_controller.py:6596: un cancel non tentato per autorita' non dimostrata non consuma
    l'unico tentativo del TTL."""
    c = _catena_armata(tmp_path, "t47.db")
    c.rc._direct_order_bet_ids = {"ref-1": "B1"}
    adapter = c.rc._direct_ttl_cancel_adapter()
    adapter.authority_check = lambda: "runtime_authority_lost:lease_unreadable"
    adapter.sim_cancel = adapter.live_cancel = lambda **kw: pytest.fail("cancel inviato senza autorita'")
    c.rc._cancel_direct_unmatched(adapter, {"market_id": "1.1", "bet_id": "B1"})
    assert "B1" in c.rc.direct_unmatched_bet_ids, "candidato TTL perso senza alcun tentativo"
    assert c.bus.payloads("DIRECT_UNMATCHED_CANCEL_FAILED") == []


def test_t48_start_non_si_dichiara_attivo_se_l_autorita_cade_durante_l_avvio(tmp_path, monkeypatch):
    """Codex P2 runtime_controller.py:2448: autorita' persa durante connessione/recovery => niente ACTIVE."""
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "t48.db")
    app = _app_su(p, monkeypatch)
    avviati = []
    app.bus.subscribe("RUNTIME_STARTED", lambda pl: avviati.append(pl))
    originale = app.runtime.betfair_service.connect

    def _connetti_e_perdi(*a, **k):
        out = originale(*a, **k)
        os.unlink(_lock_path(p))                              # lock rimosso mentre l'avvio e' in corso
        return out

    app.runtime.betfair_service.connect = _connetti_e_perdi
    try:
        esito = app.runtime.start(execution_mode="SIMULATION")
        assert esito.get("started") is not True, esito
        assert app.runtime.mode is not RuntimeMode.ACTIVE
        time.sleep(0.3)
        assert avviati == []
    finally:
        _ferma(app)


# ---------------------------------------------------------------- rilievi Sol su 9a80162 (RED prima della patch)
def test_t49_emergency_stop_riverifica_l_autorita_prima_di_ogni_mercato(tmp_path, monkeypatch):
    """Sol su 9a80162: autorita' persa durante un cancel lento => nessun cancel-all sui mercati successivi."""
    c = _catena_armata(tmp_path, "t49.db")
    lp = _lock_path(str(tmp_path / "t49.db"))
    chiamate = []

    class _Live:
        def cancel_orders(self, **kw):
            chiamate.append(kw)
            if len(chiamate) == 1:
                os.unlink(lp)                                  # perdita durante il primo cancel
            return {"ok": True, "cancelled_count": 1}

    monkeypatch.setattr(c.rc.db, "get_pending_sagas",
                        lambda: [{"market_id": "1.1", "bet_id": "B1"}, {"market_id": "1.2", "bet_id": "B2"}],
                        raising=False)
    monkeypatch.setattr(c.rc.betfair_service, "get_live_client", lambda: _Live(), raising=False)
    esito = c.rc.emergency_stop("prova")
    assert [k["market_id"] for k in chiamate] == ["1.1"], f"cancel-all dopo la perdita: {chiamate}"
    assert any(e.get("stage") == "runtime_authority" for e in esito.get("cancel_errors", [])), esito


def test_t50_tentativo_reale_del_ttl_consuma_il_candidato_no_retry(tmp_path):
    """Complemento di T47 (Sol su 9a80162): con autorita' valida il cancel parte e il candidato esce dal registry
    anche se il broker non conferma (NO RETRY invariato); solo il rifiuto per autorita' lo conserva."""
    c = _catena_armata(tmp_path, "t50.db")
    c.rc._direct_order_bet_ids = {"ref-1": "B1"}
    adapter = c.rc._direct_ttl_cancel_adapter()
    chiamate = []
    adapter.sim_cancel = adapter.live_cancel = lambda **kw: chiamate.append(kw) or {"ok": False}
    c.rc._cancel_direct_unmatched(adapter, {"market_id": "1.1", "bet_id": "B1"})
    assert len(chiamate) == 1 and adapter.last_block == ""
    assert "B1" not in c.rc.direct_unmatched_bet_ids
    assert c.bus.payloads("DIRECT_UNMATCHED_CANCEL_FAILED"), "esito del tentativo non pubblicato"


# ---------------------------------------------------------------- rilievi Grok su 9a80162 (RED prima della patch)
def test_t51_begin_transaction_non_lascia_aperta_la_transazione_se_la_verifica_fallisce(tmp_path, monkeypatch):
    """Grok su 9a80162: un errore qualsiasi nella verifica dopo BEGIN IMMEDIATE fa rollback; il lock di scrittura
    non resta al runtime."""
    p = str(tmp_path / "t51.db")
    db, altro = Database(p), Database(p)
    assert db.acquire_runtime_lease("a", ttl_sec=30.0, now=time.time(), pid=1, host="h", lock_identity="")["acquired"]
    db.bind_runtime_fence("a", 1)

    def _db_guasto(conn, name):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(db, "_assert_runtime_fence", _db_guasto)
    with pytest.raises(sqlite3.OperationalError):
        db.begin_transaction()
    assert not db._in_transaction() and not db._get_connection().in_transaction
    altro._get_connection().execute("PRAGMA busy_timeout = 200")
    assert altro.acquire_runtime_lease("b", ttl_sec=30.0, now=time.time(), pid=2, host="h", lock_identity="",
                                       holder_alive=lambda pid, host: False)["acquired"], "lock di scrittura trattenuto"


def test_t52_esecutore_fail_closed_se_l_operazione_in_volo_non_si_registra(tmp_path):
    """Grok su 9a80162: l'ingresso nell'operazione in volo che solleva chiude il cashout come REJECTED."""
    from tests.test_microstake_cashout_and_recovery import _book, _cashout_cmd

    c = _catena_armata(tmp_path, "t52.db")
    catena = _cashout_wired(c)

    class _Rotto:
        def __enter__(self):
            raise RuntimeError("contatore non disponibile")

        def __exit__(self, *exc):
            return False

    catena.cashout_executor.authority_operation = lambda: _Rotto()
    _book(c.broker, "1.100", 7, lay=(3.0, 50.0))
    c.bus.publish("CMD_EXECUTE_CASHOUT", _cashout_cmd(2.0))
    assert not c.broker.state.orders
    falliti = c.bus.payloads("CASHOUT_FAILED")
    assert falliti and falliti[-1]["status"] == "REJECTED", falliti


def test_t53_transazione_fenced_non_fallisce_se_l_heartbeat_rinnova_nel_mezzo(tmp_path):
    """CI su fc8f136/9a80162 (F032_01): con il fence, transaction() leggeva la lease in una BEGIN differita e poi
    scriveva; un rinnovo dell'heartbeat su un'altra connessione nel mezzo rendeva impossibile l'upgrade a scrittura
    ('database is locked' immediato, senza busy timeout) => save_settings della baseline F032 perso."""
    p = str(tmp_path / "t53.db")
    db, heartbeat = Database(p), Database(p)
    assert db.acquire_runtime_lease("a", ttl_sec=30.0, now=time.time(), pid=1, host="h", lock_identity="")["acquired"]
    db.bind_runtime_fence("a", 1)
    originale = db._assert_runtime_fence
    esiti = []

    def _rinnovo_nel_mezzo(conn, name):
        originale(conn, name)
        t = threading.Thread(target=lambda: esiti.append(
            heartbeat.renew_runtime_lease("a", 1, ttl_sec=30.0, now=time.time())), daemon=True)
        t.start()
        t.join(0.3)                                            # il rinnovo parte fra la verifica e la scrittura

    db._assert_runtime_fence = _rinnovo_nel_mezzo
    db.save_settings({"roserpina.f032_probe": "1"})            # deve riuscire
    db._assert_runtime_fence = originale
    assert _attendi(lambda: len(esiti) == 1, 5.0) and esiti[0] is not None, "rinnovo dell'heartbeat perso"
    assert (db.get_settings() or {}).get("roserpina.f032_probe") == "1"


# ---------------------------------------------------------------- rilievi Grok su 589cdb5 (RED prima della patch)
def test_t54_esito_del_cancel_ttl_non_dipende_da_stato_condiviso_dell_adapter(tmp_path):
    """Grok su 589cdb5: ``last_block`` e' stato d'istanza; un cancel concorrente sullo stesso adapter lo azzera
    e il rifiuto per autorita' verrebbe letto come tentativo (candidato perso, unmatched vivo). L'esito deve
    essere locale alla chiamata."""
    c = _catena_armata(tmp_path, "t54.db")
    c.rc._direct_order_bet_ids = {"ref-1": "B1"}
    adapter = c.rc._direct_ttl_cancel_adapter()
    adapter.authority_check = lambda: "runtime_authority_lost:lease_unreadable"
    adapter.sim_cancel = adapter.live_cancel = lambda **kw: pytest.fail("cancel inviato senza autorita'")
    for nome in ("cancel", "cancel_with_outcome"):
        originale = getattr(adapter, nome, None)
        if originale is None:
            continue

        def _con_concorrente(m, ids, _orig=originale):
            out = _orig(m, ids)
            adapter.last_block = ""                     # effetto di un cancel concorrente riuscito sullo stesso adapter
            return out

        setattr(adapter, nome, _con_concorrente)
    c.rc._cancel_direct_unmatched(adapter, {"market_id": "1.1", "bet_id": "B1"})
    assert c.rc._direct_order_bet_ids == {"ref-1": "B1"}, "candidato TTL perso senza alcun tentativo"
    assert c.bus.payloads("DIRECT_UNMATCHED_CANCEL_FAILED") == []
    assert c.bus.payloads("DIRECT_UNMATCHED_CANCEL_DEFERRED")


def test_t55_bet_id_fuori_dal_registry_durante_la_chiamata_al_broker(tmp_path):
    """Grok su 589cdb5: durante il cancel il bet_id non deve restare candidato, altrimenti un giro TTL
    sovrapposto (stream + REST fallback) invia un secondo cancel sullo stesso ordine."""
    c = _catena_armata(tmp_path, "t55.db")
    c.rc._direct_order_bet_ids = {"ref-1": "B1"}
    adapter = c.rc._direct_ttl_cancel_adapter()
    visto = []
    adapter.sim_cancel = adapter.live_cancel = (
        lambda **kw: visto.append("B1" in c.rc.direct_unmatched_bet_ids) or {"ok": True, "cancelled_count": 1})
    c.rc._cancel_direct_unmatched(adapter, {"market_id": "1.1", "bet_id": "B1"})
    assert visto == [False], "bet_id ancora candidato mentre il cancel e' in corso"
    assert "B1" not in c.rc.direct_unmatched_bet_ids


def test_t56_candidato_gia_preso_in_carico_non_genera_un_secondo_cancel(tmp_path):
    """Grok su 589cdb5: un giro sovrapposto che ha selezionato lo stesso ordine prima della presa in carico
    non invia un secondo cancel."""
    c = _catena_armata(tmp_path, "t56.db")
    c.rc._direct_order_bet_ids = {}                              # gia' preso in carico dal giro concorrente
    adapter = c.rc._direct_ttl_cancel_adapter()
    adapter.sim_cancel = adapter.live_cancel = lambda **kw: pytest.fail("secondo cancel sullo stesso ordine")
    c.rc._cancel_direct_unmatched(adapter, {"market_id": "1.1", "bet_id": "B1"})
    assert c.bus.payloads("DIRECT_UNMATCHED_CANCELLED") == []


class _MortoDelProcesso(BaseException):
    """Interruzione del processo nel mezzo del cancel: non e' un ``Exception``, nessun handler la intercetta."""


@pytest.mark.parametrize("quando", ["prima_del_broker", "dopo_il_broker"])
def test_t57_crash_fra_presa_in_carico_ed_esito_del_cancel_ttl_ordine_non_dimenticato(tmp_path, monkeypatch, quando):
    """Owner sul ciclo 5: Pickfair muore dopo aver preso in carico l'ordine e prima di conoscere l'esito del cancel.
    Al riavvio l'ordine resta nel ledger persistente del bot, lo stato vero e' quello dell'exchange e nessun cancel
    parte alla cieca (il registry TTL e' in memoria per scelta di main: vuoto al riavvio)."""
    c = _catena_armata(tmp_path, "t57.db")
    c.rc._on_signal_received(_signal(price=50.0))
    (bet_id,) = [o.bet_id for o in c.broker.state.orders.values()]
    c.rc._direct_order_bet_ids = {"ref-1": bet_id}
    adapter = c.rc._direct_ttl_cancel_adapter()

    def _muore(**kw):
        if quando == "dopo_il_broker":
            ids = kw.get("bet_ids") or [i["betId"] for i in kw.get("instructions") or []]
            c.broker.cancel_orders(market_id=kw["market_id"], instructions=[{"betId": b} for b in ids])
            # l'exchange ha cancellato, l'esito non torna mai
        raise _MortoDelProcesso()

    adapter.sim_cancel = adapter.live_cancel = _muore
    with pytest.raises(_MortoDelProcesso):
        c.rc._cancel_direct_unmatched(adapter, {"market_id": "1.234", "bet_id": bet_id})
    # morte del processo: heartbeat fermo, descrittore del lock chiuso dal SO, lease mai rilasciata e poi scaduta
    vecchia = c.rc.runtime_authority
    vecchia.stop_heartbeat()
    vecchia.lock_fh.close()
    token_vecchio = int(c.db.read_runtime_lease()["fencing_token"])
    raw = sqlite3.connect(str(tmp_path / "t57.db"))
    raw.execute("UPDATE runtime_lease SET expires_at = 0 WHERE id = 1")
    raw.commit()
    raw.close()

    r = _chain(tmp_path, db_name="t57.db")                 # riavvio: nuovo runtime sullo stesso DB e lo stesso exchange
    r.rc.betfair_service.simulation_broker = c.broker
    _arma(r)
    assert int(r.db.read_runtime_lease()["fencing_token"]) == token_vecchio + 1
    assert bet_id in {o["bet_id"] for o in r.db.get_bot_active_orders()}, "ordine dimenticato dopo il crash"
    vivo = {o.get("betId") for o in r.rc.betfair_service.list_current_orders()
            if str(o.get("status") or "").upper() == "EXECUTABLE"}
    assert (bet_id in vivo) is (quando == "prima_del_broker"), "stato dell'exchange non riconciliabile"
    assert r.rc._direct_order_bet_ids == {}
    inviati = []
    originale = c.broker.cancel_orders
    monkeypatch.setattr(c.broker, "cancel_orders", lambda **kw: inviati.append(kw) or originale(**kw))
    monkeypatch.setattr(r.rc.config, "direct_unmatched_ttl_enabled", True, raising=False)
    r.rc._last_direct_ttl_poll_at = -1e9
    r.rc._poll_direct_unmatched_ttl()
    assert inviati == [], "cancel alla cieca dopo il riavvio"
