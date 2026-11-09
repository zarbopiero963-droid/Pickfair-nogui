"""PF-BLACKBOX-01 (BB-1): scatola nera del processo e cablaggio nell'entrypoint headless."""
import json
import sqlite3
import sys
import threading

import pytest

import headless_main
from core import blackbox as bb
from core import blackbox_runtime as rt


@pytest.fixture
def bbpath(tmp_path, monkeypatch):
    path = tmp_path / "data" / "blackbox.sqlite"
    monkeypatch.setenv(bb.ENV_PATH, str(path))
    yield path
    rt.shutdown("TEST_TEARDOWN")


def _events(path):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM events ORDER BY seq")]
    conn.close()
    return rows


def test_main_records_process_start_first_and_shutdown_last(bbpath, monkeypatch):
    seen = {}

    def fake_start(self):
        seen["types_at_start"] = [e["type"] for e in _events(bbpath)]
        return 0

    monkeypatch.setattr(headless_main.HeadlessApp, "start", fake_start)
    assert headless_main.main() == 0
    assert seen["types_at_start"] == ["PROCESS_START"], "la scatola nera parte prima dell'app"
    ev = _events(bbpath)
    assert [e["type"] for e in ev] == ["PROCESS_START", "SHUTDOWN"] and ev[-1]["reason_code"] == "STOP"
    assert sys.excepthook is not rt._excepthook and threading.excepthook is not rt._threading_hook
    assert bb.BlackBox(str(bbpath)).open().verify()["ok"]


def test_main_refuses_to_start_when_the_blackbox_cannot_open(tmp_path, monkeypatch):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    monkeypatch.setenv(bb.ENV_PATH, str(blocker / "sub" / "blackbox.sqlite"))   # genitore e' un file
    built = []
    monkeypatch.setattr(headless_main.HeadlessApp, "__init__", lambda self: built.append(1))
    assert headless_main.main() == 1
    assert built == [], "nessun componente operativo senza registro"
    assert rt.get() is None


def test_fatal_error_in_main_is_recorded_before_shutdown(bbpath, monkeypatch):
    def boom(self):
        raise RuntimeError("guasto")

    monkeypatch.setattr(headless_main.HeadlessApp, "start", boom)
    assert headless_main.main() == 1
    ev = _events(bbpath)
    assert [e["type"] for e in ev] == ["PROCESS_START", "UNCAUGHT_EXCEPTION", "SHUTDOWN"]
    assert ev[1]["reason_code"] == "RuntimeError" and ev[-1]["reason_code"] == "EXIT_1"


def test_uncaught_exceptions_in_process_and_threads_are_recorded(bbpath):
    rt.install()
    try:
        raise ValueError("segreto-non-qui")
    except ValueError:
        sys.excepthook(*sys.exc_info())

    t = threading.Thread(target=lambda: (_ for _ in ()).throw(KeyError("k")), name="worker-x")
    t.start(); t.join()
    rt.shutdown("STOP")
    types = [e["type"] for e in _events(bbpath)]
    assert "UNCAUGHT_EXCEPTION" in types and "TASK_EXCEPTION" in types
    task = [e for e in _events(bbpath) if e["type"] == "TASK_EXCEPTION"][0]
    assert task["component"] == "thread:worker-x" and "KeyError" in json.loads(task["data"])["traceback"]


def test_native_crash_dump_of_the_previous_run_is_recorded_and_rotated(bbpath):
    bbpath.parent.mkdir(parents=True)
    fault = bbpath.parent / "fault.log"
    fault.write_text("Fatal Python error: Segmentation fault\n  File \"x.py\", line 1\n")
    rt.install()
    rt.shutdown("STOP")
    ev = _events(bbpath)
    crash = [e for e in ev if e["type"] == "CRASH_RECOVERY"][0]
    assert "Segmentation fault" in json.loads(crash["data"])["tail"]
    assert (bbpath.parent / "fault.log.1").exists() and not fault.exists()


def test_password_arguments_never_reach_the_record(bbpath, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--password=hunter2-segreto", "--simulation",
                                      "--password", "valore-separato-segreto", "--api-key", "KEY-SEGRETA"])
    rt.install()
    rt.shutdown("STOP")
    raw = json.dumps(_events(bbpath))
    for secret in ("hunter2-segreto", "valore-separato-segreto", "KEY-SEGRETA"):
        assert secret not in raw
    assert "--simulation" in raw


def test_record_without_an_installed_blackbox(monkeypatch):
    assert rt.get() is None
    assert rt.record("API_REQUEST") is None
    with pytest.raises(bb.BlackBoxUnavailable):
        rt.record("ORDER_PLACE_REQUEST", critical=True)


def test_second_runtime_process_is_refused_and_does_not_start(tmp_path):
    """D3 tra PROCESSI reali: mentre un runtime tiene il registro, un secondo processo che installa la scatola nera
    viene rifiutato (RUNTIME_ALREADY_ACTIVE) e non scrive nulla; nessun falso UNCLEAN_TERMINATION per il primo."""
    import os
    import subprocess
    path = str(tmp_path / "data" / "blackbox.sqlite")
    first = bb.BlackBox(path).open()
    first.start_session()
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    code = ("import sys; sys.path.insert(0, %r)\n"
            "from core import blackbox_runtime as rt\n"
            "try:\n    rt.install(%r)\nexcept Exception as e:\n    print(type(e).__name__, e); sys.exit(3)\n"
            "sys.exit(0)\n") % (root, path)
    p = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=60)
    assert p.returncode == 3 and "RUNTIME_ALREADY_ACTIVE" in p.stdout, (p.stdout, p.stderr)
    types = [e["type"] for e in _events(path)]
    assert types == ["PROCESS_START"], "il secondo processo non ha scritto nulla"
    first.end_session("STOP"); first.close()


def test_emergency_stop_still_runs_when_the_blackbox_is_unavailable(tmp_path, monkeypatch):
    """Codex #617 (3b6848d6), P1: `--emergency-stop` e' un'uscita protettiva (cancel-all). Con la scatola nera non
    disponibile l'avvio normale resta rifiutato, ma l'emergency stop deve partire comunque, senza registro."""
    blocker = tmp_path / "file"
    blocker.write_text("x")
    monkeypatch.setenv(bb.ENV_PATH, str(blocker / "sub" / "blackbox.sqlite"))   # registro impossibile
    calls = []
    monkeypatch.setattr(headless_main.HeadlessApp, "__init__", lambda self: None)
    monkeypatch.setattr(headless_main.HeadlessApp, "start", lambda self: calls.append("start") or 0)
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--emergency-stop"])
    assert headless_main.main() == 0 and calls == ["start"], "emergency stop bloccato dal registro"
    calls.clear()
    monkeypatch.setattr(sys, "argv", ["headless_main.py"])
    assert headless_main.main() == 1 and calls == [], "avvio normale senza registro: sempre rifiutato"


def test_exception_text_is_redacted_before_it_is_truncated(bbpath):
    """Codex #617 (3b6848d6): un token Telegram a cavallo del limite di 500 caratteri non lascia frammenti nel
    `reason` (prima: troncato e poi redatto, il frammento non era piu' riconoscibile)."""
    token = "1234567" + ":" + "AbCdEfGhIjKlMnOpQrStUvWxYz012345"
    rt.install()
    try:
        raise ValueError("x" * 480 + token)
    except ValueError:
        sys.excepthook(*sys.exc_info())
    rt.shutdown("STOP")
    ev = [e for e in _events(bbpath) if e["type"] == "UNCAUGHT_EXCEPTION"][0]
    blob = json.dumps(ev)
    assert "1234567:" not in blob and "AbCdEfGhIj" not in blob, ev["reason"][-40:]


def test_native_crash_dump_is_kept_until_its_import_is_durable(bbpath, monkeypatch):
    """Codex #617 (3b6848d6): se l'avvio della sessione fallisce dopo la lettura del dump nativo, il dump resta
    disponibile e viene importato al primo avvio riuscito."""
    bbpath.parent.mkdir(parents=True)
    fault = bbpath.parent / "fault.log"
    fault.write_text("Fatal Python error: Segmentation fault\n")
    real = bb.BlackBox.start_session

    def fail_once(self, **kw):
        raise bb.BlackBoxUnavailable("guasto all'avvio")

    monkeypatch.setattr(bb.BlackBox, "start_session", fail_once)
    with pytest.raises(bb.BlackBoxUnavailable):
        rt.install()
    monkeypatch.setattr(bb.BlackBox, "start_session", real)
    rt.install()
    rt.shutdown("STOP")
    crash = [e for e in _events(bbpath) if e["type"] == "CRASH_RECOVERY"]
    assert len(crash) == 1 and "Segmentation fault" in json.loads(crash[0]["data"])["tail"]


def test_a_new_native_dump_is_appended_to_one_still_pending(bbpath):
    """Un dump in attesa di import (`fault.log.pending`) e un dump nuovo (`fault.log`): entrambi vengono importati,
    nessuno viene sovrascritto dal nuovo faulthandler."""
    bbpath.parent.mkdir(parents=True)
    (bbpath.parent / "fault.log.pending").write_text("primo crash: Segmentation fault\n")
    (bbpath.parent / "fault.log").write_text("secondo crash: Aborted\n")
    rt.install()
    rt.shutdown("STOP")
    tail = json.loads([e for e in _events(bbpath) if e["type"] == "CRASH_RECOVERY"][0]["data"])["tail"]
    assert "primo crash" in tail and "secondo crash" in tail
    assert not (bbpath.parent / "fault.log.pending").exists()
