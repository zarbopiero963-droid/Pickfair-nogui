"""Sonde Phase 0 #536 sulla baseline 152613b (devono FALLIRE = difetto riprodotto)."""
import os, threading, time
import pytest
from tests.acceptance.test_issue461_pr29c_settlement_processes_and_schema import _app_su, _controller_su
from core.system_state import RuntimeMode


def _lock_path(percorso):
    return os.path.normcase(os.path.realpath(os.path.abspath(percorso))) + ".runtime.lock"


def test_p1_lock_cancellato_secondo_runtime_parte(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "p1.db")
    a, b = _app_su(p, monkeypatch), _app_su(p, monkeypatch)
    try:
        assert a.runtime.start(execution_mode="SIMULATION").get("started") is True
        os.unlink(_lock_path(p))
        esito = b.runtime.start(execution_mode="SIMULATION")
        assert esito.get("started") is not True, "secondo runtime avviato con il lock cancellato"
    finally:
        for app in (b, a):
            try: app.stop()
            except Exception: pass


def test_p2_lock_sostituito_secondo_runtime_parte(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "p2.db")
    a, b = _app_su(p, monkeypatch), _app_su(p, monkeypatch)
    try:
        assert a.runtime.start(execution_mode="SIMULATION").get("started") is True
        lp = _lock_path(p); os.unlink(lp); open(lp, "w").write("sostituito")
        esito = b.runtime.start(execution_mode="SIMULATION")
        assert esito.get("started") is not True, "secondo runtime avviato con il lock sostituito"
    finally:
        for app in (b, a):
            try: app.stop()
            except Exception: pass


def test_p3_runtime_che_ha_perso_l_esclusivita_continua_a_operare(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "p3.db")
    a, b = _app_su(p, monkeypatch), _app_su(p, monkeypatch)
    try:
        assert a.runtime.start(execution_mode="SIMULATION").get("started") is True
        os.unlink(_lock_path(p))
        b.runtime.start(execution_mode="SIMULATION")
        a.runtime.runtime_authority.heartbeat_once()       # rilevazione (in produzione: thread ogni 10 s)
        assert a.runtime.mode is not RuntimeMode.ACTIVE, "A resta ACTIVE dopo aver perso l'esclusivita'"
        assert a.runtime.is_live_allowed() is False
    finally:
        for app in (b, a):
            try: app.stop()
            except Exception: pass


def test_p4_due_avvii_simultanei_uno_solo_parte(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = str(tmp_path / "p4.db")
    apps = [_app_su(p, monkeypatch) for _ in range(2)]
    esiti = [None, None]; barrier = threading.Barrier(2)
    def go(i):
        barrier.wait(); esiti[i] = apps[i].runtime.start(execution_mode="SIMULATION")
    ts = [threading.Thread(target=go, args=(i,)) for i in range(2)]
    [t.start() for t in ts]; [t.join(30) for t in ts]
    try:
        assert sum(1 for e in esiti if e and e.get("started")) == 1, esiti
    finally:
        for app in apps:
            try: app.stop()
            except Exception: pass
