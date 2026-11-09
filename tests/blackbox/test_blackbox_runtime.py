"""PF-BLACKBOX-01 (BB-1): scatola nera del processo e cablaggio nell'entrypoint headless."""
import errno
import json
import os
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
    diag = json.loads(task["data"])                       # R2: diagnostica strutturata, nessun testo arbitrario
    assert task["component"] == "thread:worker-x" and diag["exception_type"] == "KeyError" and diag["frames"]
    assert "segreto-non-qui" not in json.dumps(_events(bbpath))


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


def test_runtime_lock_file_is_never_rewritten_after_the_lock_is_taken(tmp_path):
    """Grok #617 (2919d6bf): dopo il lock il file di lock non viene troncato ne' riscritto (su Windows il lock e' una
    regione del file). Il proprietario (pid, sessione) sta in un file separato `<registro>.runtime.owner`."""
    path = str(tmp_path / "b.sqlite")
    box = bb.BlackBox(path).open()
    lock = path + ".runtime.lock"
    with open(lock, "w") as fh:
        fh.write("contenuto preesistente\n")
    box.acquire_runtime()
    owner = open(path + ".runtime.owner").read()
    assert f"pid={box.pid}" in owner and f"session={box.session_id}" in owner
    box.close()
    # Sol #617 (44a6a58f): su Windows `msvcrt` blocca la regione del file e un secondo handle non la puo' leggere
    # finche' il lock e' tenuto: il contenuto si verifica dopo il rilascio (nessuna riscrittura nel frattempo).
    with open(lock) as fh:
        assert fh.read() == "contenuto preesistente\n", "file di lock modificato dopo il lock"


def test_native_dump_survives_when_it_cannot_be_moved_aside(bbpath, monkeypatch):
    """Sol #617 (d1454af3): se il dump non si puo' spostare/accodare in `.pending`, il nuovo faulthandler non deve
    troncare `fault.log`: il dump non importato resta leggibile."""
    bbpath.parent.mkdir(parents=True)
    (bbpath.parent / "fault.log.pending").write_text("in attesa\n")
    fault = bbpath.parent / "fault.log"
    fault.write_text("dump nuovo: Segmentation fault\n")
    real_open = open

    def guarded_open(file, mode="r", *a, **kw):
        if str(file).endswith("fault.log.pending") and "a" in mode:
            raise PermissionError("pending non scrivibile")
        return real_open(file, mode, *a, **kw)

    monkeypatch.setattr("builtins.open", guarded_open)
    rt.install()
    monkeypatch.setattr("builtins.open", real_open)
    rt.shutdown("STOP")
    tail = json.loads([e for e in _events(bbpath) if e["type"] == "CRASH_RECOVERY"][0]["data"])["tail"]
    assert "dump nuovo: Segmentation fault" in tail and "in attesa" in tail, "dump non importato"
    archived = (bbpath.parent / "fault.log.1").read_text()
    assert "dump nuovo: Segmentation fault" in archived, "dump perso invece che archiviato"


def test_emergency_stop_cancels_before_any_audit_write(monkeypatch):
    """Codex #617 (d1454af3), P1: nessuna scrittura sul registro (che puo' attendere un lock fino a 30 s) prima della
    cancellazione. L'ATTEMPT si registra dopo, con l'ora reale della richiesta, e precede comunque il RESULT."""
    calls = []

    class FakeRuntime:
        def emergency_stop(self, reason):
            calls.append(("stop", reason))
            return {"cancelled": 2}

    monkeypatch.setattr(rt, "record", lambda event_type, **kw: calls.append((event_type, kw.get("phase"),
                                                                             kw.get("ts_utc"))))
    monkeypatch.setattr(rt, "install", lambda *a, **kw: calls.append(("install", kw.get("emergency_stop"), None)))
    app = object.__new__(headless_main.HeadlessApp)
    app.runtime = FakeRuntime()
    assert app._emergency_stop_with_audit() == 0
    assert calls[0] == ("stop", "operator_cli_flag"), calls
    assert calls[1] == ("install", True, None), "il registro si apre solo dopo la cancellazione (Sol #617)"
    assert [c[:2] for c in calls[2:]] == [("EMERGENCY_STOP", "ATTEMPT"), ("EMERGENCY_STOP", "RESULT")]
    assert calls[2][2] is not None, "ATTEMPT con l'ora reale della richiesta"


def test_emergency_stop_audit_survives_an_unavailable_blackbox_after_cancelling(monkeypatch):
    """Sol #617 (44a6a58f): registro non apribile dopo la cancellazione (guasto o runtime che lo tiene) => nessuna
    eccezione, cancellazione gia' eseguita, esito nel log."""
    calls = []

    class FakeRuntime:
        def emergency_stop(self, reason):
            calls.append("stop")
            return {"cancelled": 1}

    def boom(*a, **kw):
        raise bb.RuntimeAlreadyActive("RUNTIME_ALREADY_ACTIVE")

    monkeypatch.setattr(rt, "install", boom)
    app = object.__new__(headless_main.HeadlessApp)
    app.runtime = FakeRuntime()
    assert app._emergency_stop_with_audit() == 0 and calls == ["stop"]


@pytest.mark.parametrize("flag", ["--emergency-stop", "--preflight", "--telegram-login"])
def test_protective_and_diagnostic_commands_never_open_the_runtime_registry_before_running(monkeypatch, flag):
    """Sol #617: `--emergency-stop` non scrive nulla (lock SQLite fino a 30 s) prima del cancel-all. Codex #617:
    `--preflight` e `--telegram-login` non prendono il lock del runtime finanziario, quindi funzionano anche con un
    runtime attivo. Il normale avvio invece apre sempre il registro per primo."""
    calls = []
    monkeypatch.setattr(rt, "install", lambda *a, **kw: calls.append("install"))
    monkeypatch.setattr(rt, "shutdown", lambda *a, **kw: None)
    monkeypatch.setattr(headless_main.HeadlessApp, "__init__", lambda self: None)
    monkeypatch.setattr(headless_main.HeadlessApp, "start", lambda self: calls.append("start") or 0)
    monkeypatch.setattr(sys, "argv", ["headless_main.py", flag])
    assert headless_main.main() == 0 and calls == ["start"], calls
    calls.clear()
    monkeypatch.setattr(sys, "argv", ["headless_main.py"])
    assert headless_main.main() == 0 and calls == ["install", "start"], calls


def test_chained_sensitive_flags_never_leak_the_value():
    """Sol #617 (44a6a58f): `--password --api-key valore` lasciava `valore` in chiaro in PROCESS_START.argv."""
    out = rt.safe_argv(["--password", "--api-key", "valore-segreto", "--live"])
    assert "valore-segreto" not in " ".join(out), out
    assert out == [f"--password={bb.REDACTED}", f"--api-key={bb.REDACTED}", "--live"]
    assert rt.safe_argv(["--token=abc", "--simulation"]) == [f"--token={bb.REDACTED}", "--simulation"]
    # R2 (allowlist): un flag sconosciuto non e' "sicuro" e non resta in chiaro
    assert rt.safe_argv(["--password", "pw", "--verbose"]) == [f"--password={bb.REDACTED}", bb.REDACTED]


@pytest.mark.parametrize("result,outcome", [
    ({"cancel_error_count": 0, "cancel_errors": [], "cancel_results": [{"ok": True}], "markets_attempted": 1,
      "live_client_available": True, "persist_error": None}, "VERIFIED"),
    ({"cancel_error_count": 2, "cancel_errors": [], "cancel_results": [], "markets_attempted": 1}, "FAILED"),
    ({"cancel_error_count": 0, "cancel_results": [{"ok": False, "error": "x"}], "markets_attempted": 1}, "FAILED"),
    ({"cancel_error_count": 0, "cancel_errors": [{"stage": "get_live_client", "error": "x"}]}, "FAILED"),
    ({"cancel_error_count": 0, "cancel_errors": [], "markets_attempted": 2, "live_client_available": False}, "FAILED"),
    ({"cancel_error_count": 0, "cancel_errors": [], "persist_error": "disco pieno"}, "UNKNOWN"),
    ("non un dict", "UNKNOWN"),
])
def test_emergency_stop_result_outcome_comes_from_the_cancellation_result(monkeypatch, result, outcome):
    """Codex #617 (761a52f2), P2: `emergency_stop()` cattura gli errori di cancellazione e li riporta nel risultato;
    un cancel fallito o incompleto non deve risultare VERIFIED."""
    calls = []

    class FakeRuntime:
        def emergency_stop(self, reason):
            return result

    monkeypatch.setattr(rt, "install", lambda *a, **kw: None)
    monkeypatch.setattr(rt, "record", lambda event_type, **kw: calls.append((kw.get("phase"), kw.get("outcome"))))
    app = object.__new__(headless_main.HeadlessApp)
    app.runtime = FakeRuntime()
    assert app._emergency_stop_with_audit() == 0
    assert calls[-1] == ("RESULT", outcome), calls


def test_operational_flag_after_a_sensitive_flag_stays_in_the_audit():
    """Grok #617 (374ce396): `--password --live` scartava `--live` da PROCESS_START.argv."""
    assert rt.safe_argv(["--password", "--live"]) == [f"--password={bb.REDACTED}", "--live"]
    assert rt.safe_argv(["--token", "--emergency-stop", "--simulation"]) == [
        f"--token={bb.REDACTED}", "--emergency-stop", "--simulation"]
    out = rt.safe_argv(["--password", "--segreto-che-sembra-un-flag", "--live"])
    assert "--segreto-che-sembra-un-flag" not in out and out == [f"--password={bb.REDACTED}", "--live"]


def test_windows_lock_writes_the_locked_byte_before_locking_an_empty_file(tmp_path, monkeypatch):
    """Grok #617 (374ce396): su Windows `msvcrt.locking` blocca 1 byte; su un file di lock appena creato (vuoto) il
    byte deve esistere prima del lock. Simulato: os.name='nt' e un msvcrt finto che verifica la dimensione."""
    import types
    seen = []

    def locking(fd, mode, nbytes):
        seen.append(os.fstat(fd).st_size)

    fake = types.SimpleNamespace(LK_NBLCK=2, locking=locking)
    monkeypatch.setitem(sys.modules, "msvcrt", fake)
    monkeypatch.setattr(bb.os, "name", "nt")
    box = bb.BlackBox(str(tmp_path / "w.sqlite"))
    box.acquire_runtime()
    assert seen and seen[0] >= 1, "lock su un file vuoto"
    box._writer_lock.close()


@pytest.mark.skipif(os.name != "posix", reason="backend POSIX (fcntl); il backend Windows ha il suo test con msvcrt finto")
@pytest.mark.parametrize("err,expected", [
    (errno.EWOULDBLOCK, "RuntimeAlreadyActive"), (errno.EAGAIN, "RuntimeAlreadyActive"),
    (errno.EIO, "BlackBoxUnavailable"), (errno.ENOLCK, "BlackBoxUnavailable"),
])
def test_only_a_busy_lock_means_another_runtime(tmp_path, monkeypatch, err, expected):
    """Grok #617 (374ce396): ogni OSError diventava RuntimeAlreadyActive. Ora solo un lock occupato lo e'; ogni altro
    errore e' BlackBoxUnavailable (avvio rifiutato comunque, con il motivo giusto)."""
    import fcntl

    def busy(fd, op):
        raise OSError(err, os.strerror(err))

    monkeypatch.setattr(fcntl, "flock", busy)
    box = bb.BlackBox(str(tmp_path / "l.sqlite"))
    with pytest.raises(getattr(bb, expected)):
        box.acquire_runtime()


def test_failed_crash_recovery_import_refuses_start_and_keeps_the_dump(bbpath, monkeypatch):
    """Sol #617 (97b4c8fc): CRASH_RECOVERY e' critico. Se non si scrive, `install()` non deve restituire un registro
    degradato (il processo partirebbe anche in LIVE): solleva, e il dump resta in attesa per il prossimo avvio."""
    bbpath.parent.mkdir(parents=True)
    (bbpath.parent / "fault.log").write_text("Fatal Python error: Segmentation fault\n")
    real = bb.BlackBox.record

    def fail_crash_recovery(self, event_type, **kw):
        if event_type == "CRASH_RECOVERY":
            raise bb.BlackBoxUnavailable("disco pieno")
        return real(self, event_type, **kw)

    monkeypatch.setattr(bb.BlackBox, "record", fail_crash_recovery)
    with pytest.raises(bb.BlackBoxUnavailable):
        rt.install()
    assert rt._box is None, "registro degradato installato"
    assert "Segmentation fault" in (bbpath.parent / "fault.log.pending").read_text()
    monkeypatch.setattr(bb.BlackBox, "record", real)
    rt.install()                                           # al riavvio successivo il dump viene importato
    rt.shutdown("STOP")
    crash = [e for e in _events(bbpath) if e["type"] == "CRASH_RECOVERY"]
    assert len(crash) == 1 and "Segmentation fault" in json.loads(crash[0]["data"])["tail"]


# ------------------------------------------------- R2 (decisione owner PF-BB1-SOL-R2-NEEDS-MANUAL-20261109)
_FAKE_PW = "FAKEpw-Zq81xLm93Kp"                          # segreti FINTI, mai registrati: il caso R2
_FAKE_KEY = "FAKEappkey0123456789AbCdEf"


@pytest.mark.parametrize("argv", [
    [_FAKE_PW],                                           # posizionale
    [f"--bf-pass={_FAKE_PW}"],                            # flag sconosciuto con =
    ["--login", _FAKE_PW],                                # flag sconosciuto + valore separato
    ["--simulation", _FAKE_PW, "--live"],
])
def test_r2_unregistered_secret_in_argv_never_reaches_the_record(bbpath, monkeypatch, argv):
    """R2: argv con allowlist. Prima un segreto posizionale o sotto un flag sconosciuto finiva in chiaro in
    PROCESS_START.data.argv (riprodotto su 487d8dcb)."""
    monkeypatch.setattr(sys, "argv", ["headless_main.py"] + argv)
    rt.install()
    rt.shutdown("STOP")
    assert _FAKE_PW not in json.dumps(_events(bbpath))
    start = json.loads([e for e in _events(bbpath) if e["type"] == "PROCESS_START"][0]["data"])
    assert all(a in rt.OPERATIONAL_FLAGS or a == bb.REDACTED or a.endswith("=" + bb.REDACTED) for a in start["argv"])


def test_r2_operational_flags_stay_readable_for_the_audit():
    assert rt.safe_argv(["--simulation", "--emergency-stop", "--api-id", "12345", "x"]) == [
        "--simulation", "--emergency-stop", f"--api-id={bb.REDACTED}", bb.REDACTED]
    assert rt.safe_argv(["LIVE", "sim"]) == ["live", "sim"]           # forme posizionali di _parse_args
    assert rt.safe_argv(["--betfair-password", "X1", "--pwd", "X2"]) == [bb.REDACTED] * 4   # casi di Claude D


@pytest.mark.parametrize("hook", ["process", "thread"])
def test_r2_unregistered_secret_in_an_exception_never_reaches_the_record(bbpath, hook):
    """R2: il testo di un'eccezione e' arbitrario (es. un errore di login con la app key nel messaggio). Si registrano
    tipo, catena delle cause, frame e un'impronta del messaggio, mai il testo (riprodotto su 487d8dcb)."""
    rt.install()
    if hook == "process":
        try:
            raise ValueError(f"betfair login error appKey={_FAKE_KEY} pw {_FAKE_PW}")
        except ValueError:
            sys.excepthook(*sys.exc_info())
    else:
        def boom():
            raise RuntimeError(f"GET https://api.example/x?session={_FAKE_KEY} -> 401")
        t = threading.Thread(target=boom, name="w-r2")
        t.start(); t.join()
    rt.shutdown("STOP")
    blob = json.dumps(_events(bbpath))
    assert _FAKE_KEY not in blob and _FAKE_PW not in blob
    ev = [e for e in _events(bbpath) if e["type"] in ("UNCAUGHT_EXCEPTION", "TASK_EXCEPTION")][0]
    diag = json.loads(ev["data"])
    assert diag["message_recorded"] is False and diag["message_len"] > 0 and len(diag["message_fingerprint"]) == 32
    # impronta con chiave (Claude D): non e' lo sha256 nudo del messaggio, quindi non si verifica a dizionario
    import hashlib
    assert all(not hashlib.sha256(m.encode()).hexdigest().startswith(diag["message_fingerprint"][:16])
               for m in (f"betfair login error appKey={_FAKE_KEY} pw {_FAKE_PW}",
                         f"GET https://api.example/x?session={_FAKE_KEY} -> 401"))
    assert diag["frames"] and {"file", "line", "function"} <= set(diag["frames"][-1])   # crash ricostruibile


def test_r2_emergency_stop_errors_are_fingerprinted_not_stored(monkeypatch):
    calls = []

    class FakeRuntime:
        def emergency_stop(self, reason):
            return {"cancel_error_count": 1, "cancel_errors": [{"stage": "get_live_client",
                                                                "error": f"auth failed key={_FAKE_KEY}"}],
                    "markets_attempted": 0}

    monkeypatch.setattr(rt, "install", lambda *a, **kw: None)
    monkeypatch.setattr(rt, "record", lambda event_type, **kw: calls.append(kw))
    app = object.__new__(headless_main.HeadlessApp)
    app.runtime = FakeRuntime()
    app._emergency_stop_with_audit()
    assert _FAKE_KEY not in json.dumps(calls, default=str)
    err = calls[-1]["data"]["result"]["cancel_errors"][0]
    assert err["stage"] == "get_live_client" and err["error"]["redacted"] is True


@pytest.mark.skipif(os.name != "posix", reason="permessi POSIX; su Windows valgono le ACL della cartella dati")
def test_registry_files_are_private_to_the_owner(bbpath):
    """Decisione owner (chiave HMAC accettata nel registro, a permessi corretti): file del registro 0600. Prima 0644,
    leggibili da ogni utente del sistema (riprodotto su 487d8dcb)."""
    rt.install()
    rt.record("API_REQUEST", source="SYSTEM")
    import stat
    d = bbpath.parent
    for name in (bbpath.name, bbpath.name + "-wal", bbpath.name + ".runtime.lock", bbpath.name + ".runtime.owner",
                 "fault.log"):
        p = d / name
        if p.exists():
            assert stat.S_IMODE(p.stat().st_mode) == 0o600, (name, oct(p.stat().st_mode))
    rt.shutdown("STOP")


# ------------------------------------------- ciclo 10 (decisione owner PF-BB1-R2-FINAL-CYCLE-10-20261109)
# le sei credenziali FINTE usate da Claude D nella sua riproduzione di R2 su 487d8dcb
_D_FAKES = ("FAKE-POSITIONAL-PW-111", "FAKE-BETFAIR-PW-222", "FAKE-PWD-333", "FAKEAPPKEY444", "FAKESESSION555",
            "--api_token_x9")


def test_r2_claude_d_fake_credentials_never_reach_any_persistent_file(bbpath, monkeypatch):
    """Tutti i percorsi R2 insieme; si cercano i sei valori finti in OGNI file persistente accanto al registro (db,
    -wal, -shm, lock, owner, fault.log) e si controlla che tipo, catena delle cause e frame restino ricostruibili."""
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--simulation", _D_FAKES[0], "--betfair-password",
                                      _D_FAKES[1], "--pwd", _D_FAKES[2], "--password", _D_FAKES[5]])
    rt.install()
    try:
        try:
            raise KeyError("causa interna")
        except KeyError as inner:
            raise RuntimeError(f"login appKey={_D_FAKES[3]} sessionToken: {_D_FAKES[4]}") from inner
    except RuntimeError:
        sys.excepthook(*sys.exc_info())
    ev = [e for e in _events(bbpath) if e["type"] == "UNCAUGHT_EXCEPTION"][0]
    diag = json.loads(ev["data"])
    assert ev["reason"] == "builtins.RuntimeError" and diag["exception_type"] == "RuntimeError"
    assert diag["cause_chain"] == ["builtins.KeyError"] and diag["frames"][-1]["function"]
    rt.shutdown("STOP")
    for p in bbpath.parent.iterdir():
        if p.is_file():
            content = p.read_bytes()
            for fake in _D_FAKES:
                assert fake.encode() not in content, (p.name, fake)


def test_r2_fingerprint_key_is_domain_separated_and_registry_specific(tmp_path):
    """HMAC con separazione di dominio: chiave derivata dal contesto esplicito, mai la chiave degli alias diretta;
    stabile nello stesso registro, diversa fra registri; nessuna chiave negli eventi esportati."""
    import hashlib
    import hmac as _hmac
    a = bb.BlackBox(str(tmp_path / "a.sqlite")).open()
    b = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    msg = "login fallito pw hunter2"
    fa = a.fingerprint(msg)
    assert fa == a.fingerprint(msg) and fa != b.fingerprint(msg)
    assert fa != _hmac.new(a._alias_key, msg.encode(), hashlib.sha256).hexdigest()[:32], "chiave degli alias usata diretta"
    derived = _hmac.new(a._alias_key, bb.FINGERPRINT_KEY_CONTEXT, hashlib.sha256).digest()
    assert fa == _hmac.new(derived, msg.encode(), hashlib.sha256).hexdigest()[:32]
    assert fa != hashlib.sha256(msg.encode()).hexdigest()[:32]
    from core import blackbox_query as bq
    blob = json.dumps(bq.BlackBoxReader(a).query({}), default=str)
    assert a._alias_key.hex() not in blob and derived.hex() not in blob
    a.close(); b.close()


@pytest.mark.skipif(os.name != "posix", reason="permessi POSIX; verifica delle ACL Windows NOT_RUN")
def test_existing_registry_files_are_restricted_and_symlinks_are_refused(tmp_path):
    """Permessi: file gia' esistenti a 0644 ristretti a 0600; un symlink al posto di un file del registro e' rifiutato
    e la sua destinazione non viene toccata."""
    import stat
    path = tmp_path / "r.sqlite"
    for name in ("r.sqlite", "r.sqlite-wal", "r.sqlite-shm", "r.sqlite.runtime.lock", "r.sqlite.runtime.owner"):
        (tmp_path / name).write_bytes(b"" if name != "r.sqlite.runtime.lock" else b"x")
        os.chmod(tmp_path / name, 0o644)
    (tmp_path / "r.sqlite-wal").unlink(); (tmp_path / "r.sqlite-shm").unlink()   # lasciati creare a SQLite
    box = bb.BlackBox(str(path)).open()
    box.acquire_runtime()
    box.record("API_REQUEST", source="SYSTEM")
    for name in ("r.sqlite", "r.sqlite-wal", "r.sqlite-shm", "r.sqlite.runtime.lock", "r.sqlite.runtime.owner"):
        p = tmp_path / name
        if p.exists():
            assert stat.S_IMODE(p.stat().st_mode) == 0o600, (name, oct(p.stat().st_mode))
    box.close()

    victim = tmp_path / "vittima.txt"
    victim.write_text("non toccare")
    os.chmod(victim, 0o644)
    link_dir = tmp_path / "s"
    link_dir.mkdir()
    os.symlink(victim, link_dir / "x.sqlite.runtime.lock")
    sbox = bb.BlackBox(str(link_dir / "x.sqlite")).open()
    with pytest.raises((OSError, bb.RuntimeAlreadyActive, bb.BlackBoxUnavailable)):
        sbox.acquire_runtime()
    assert stat.S_IMODE(victim.stat().st_mode) == 0o644 and victim.read_text() == "non toccare"
    sbox.close()
    os.symlink(victim, tmp_path / "y.sqlite")
    with pytest.raises(OSError):
        bb.BlackBox(str(tmp_path / "y.sqlite")).open()
    assert stat.S_IMODE(victim.stat().st_mode) == 0o644


# --- Ciclo 11 (OWNER AUTHORIZATION PR #617, EXCEPTIONAL REVIEW CYCLE 11): i due P2 di Codex su c59de537 ---------

_C11_FAKE = "FAKE-C11-APPKEY-7x9Qw"


def test_r2_persist_error_and_error_lists_are_fingerprinted_with_the_classification_kept(monkeypatch):
    """Codex P2 su c59de537: `persist_error` e' `str(exc)` del salvataggio fallito e restava in chiaro. Ora ogni campo
    d'errore (`*_error`, `*_errors`, liste e dizionari annidati) diventa lunghezza + impronta; il codice tecnico noto
    (`db_save_settings_unavailable`) resta leggibile, e l'esito del RESULT (classificazione) non cambia."""
    calls = []

    class FakeRuntime:
        def emergency_stop(self, reason):
            return {"cancel_error_count": 0, "cancel_errors": [], "markets_attempted": 0,
                    "persist_error": f"save failed with appKey={_C11_FAKE}",
                    "errors": [f"a {_C11_FAKE}", {"detail": f"b {_C11_FAKE}"}],
                    "error": {"detail": f"c {_C11_FAKE}", "nested": [f"d {_C11_FAKE}"]}}

    monkeypatch.setattr(rt, "install", lambda *a, **kw: None)
    monkeypatch.setattr(rt, "record", lambda event_type, **kw: calls.append(kw))
    app = object.__new__(headless_main.HeadlessApp)
    app.runtime = FakeRuntime()
    app._emergency_stop_with_audit()
    assert _C11_FAKE not in json.dumps(calls, default=str)
    res = calls[-1]
    assert res["outcome"] == "UNKNOWN", "classificazione dell'esito persa"
    pe = res["data"]["result"]["persist_error"]
    assert pe["redacted"] is True and pe["len"] == len(f"save failed with appKey={_C11_FAKE}") and "fingerprint" in pe
    assert rt.scrub_error_texts({"persist_error": "db_save_settings_unavailable"}) == {
        "persist_error": "db_save_settings_unavailable"}
    assert rt.scrub_error_texts({"persist_error": ""}) == {"persist_error": ""}


def test_r2_cancel_results_response_keeps_structure_and_never_free_error_text():
    """Audit di `cancel_results[].response` (owner, ciclo 11): la risposta di `BetfairClient.cancel_orders` e' un
    dizionario strutturato (id, status, outcome, conteggi, risposta Betfair); il solo testo libero e' `error`, che
    viene impronta anche annidato in `response`. I campi strutturati restano per l'audit."""
    from betfair_client import BetfairClient
    client = object.__new__(BetfairClient)
    resp = client._cancel_result("1.23", ["b1"], {"status": "FAILURE", "errorCode": "BET_ACTION_ERROR",
                                                 "instructionReports": [{"status": "FAILURE",
                                                                         "instruction": {"betId": "b1"}}]})
    resp["error"] = f"{resp.get('error')} {_C11_FAKE}"            # testo libero ipotetico nel campo d'errore
    resp["result"]["detail_error"] = f"x {_C11_FAKE}"
    out = rt.scrub_error_texts({"cancel_results": [{"market_id": "1.23", "ok": False, "response": resp}]})
    blob = json.dumps(out)
    assert _C11_FAKE not in blob
    r = out["cancel_results"][0]["response"]
    assert r["market_id"] == "1.23" and r["status"] == "FAILURE" and r["outcome"] == "FAILED"
    assert r["bet_ids"] == ["b1"] and r["result"]["errorCode"] == "BET_ACTION_ERROR"


def _fake_secret_file(tmp_path, name="credenziali.txt"):
    victim = tmp_path / name
    victim.write_text(f"appKey={_C11_FAKE}\n")
    os.chmod(victim, 0o644)
    return victim


def _assert_untouched(victim):
    import stat
    assert victim.read_text() == f"appKey={_C11_FAKE}\n" and not victim.is_symlink()
    assert stat.S_IMODE(victim.stat().st_mode) == 0o644


def _assert_fake_not_persisted(bbpath, victim=None):
    """Nessun file scritto dalla scatola nera contiene il segreto. Un nome che E' il bersaglio stesso (link non
    rimovibile messo dall'attaccante: stesso inode) non e' una copia e si esclude."""
    from core import blackbox_query as bq
    for p in bbpath.parent.iterdir():
        if p.is_file() and not p.is_symlink():
            if victim is not None and os.path.samefile(p, victim):
                continue
            assert _C11_FAKE.encode() not in p.read_bytes(), p.name
    box = bb.BlackBox(str(bbpath)).open()
    try:
        assert _C11_FAKE not in json.dumps(bq.BlackBoxReader(box).query({}), default=str)
    finally:
        box.close()


@pytest.mark.skipif(os.name != "posix", reason="symlink/hardlink POSIX; su Windows NOT_RUN")
@pytest.mark.parametrize("name", ["fault.log", "fault.log.pending", "fault.log.1"])
def test_fault_dump_paths_never_follow_a_symlink(bbpath, tmp_path, name):
    """Codex P2 su c59de537: un symlink al posto di `fault.log`, `.pending` o `.1` veniva seguito (lettura, append o
    rotazione) e il contenuto del bersaglio finiva nel CRASH_RECOVERY, leggibile da `query()`. Ora il link viene
    rimosso (solo il link) senza leggerlo ne' scriverci; il bersaglio resta identico."""
    bbpath.parent.mkdir(parents=True)
    victim = _fake_secret_file(tmp_path)
    os.symlink(victim, bbpath.parent / name)
    if name != "fault.log":
        (bbpath.parent / "fault.log").write_text("Fatal Python error: Segmentation fault\n")
    rt.install()
    rt.shutdown("STOP")
    _assert_untouched(victim)
    _assert_fake_not_persisted(bbpath)
    for n in ("fault.log", "fault.log.pending", "fault.log.1"):
        assert not (bbpath.parent / n).is_symlink(), n
    if name != "fault.log":                                    # il dump legittimo e' comunque importato
        crash = [e for e in _events(bbpath) if e["type"] == "CRASH_RECOVERY"]
        assert crash and "Segmentation fault" in json.loads(crash[0]["data"])["tail"]


@pytest.mark.skipif(os.name != "posix", reason="hardlink POSIX; su Windows NOT_RUN")
def test_fault_dump_hardlinked_to_another_file_is_never_read_or_chmodded(bbpath, tmp_path):
    """Sostituzione del percorso con un hardlink (stesso inode di un altro file): niente lettura, niente append,
    niente chmod del bersaglio."""
    bbpath.parent.mkdir(parents=True)
    victim = _fake_secret_file(tmp_path)
    os.link(victim, bbpath.parent / "fault.log")
    rt.install()
    rt.shutdown("STOP")
    _assert_untouched(victim)
    _assert_fake_not_persisted(bbpath)


@pytest.mark.skipif(os.name != "posix", reason="O_NOFOLLOW POSIX; su Windows NOT_RUN")
def test_fault_dump_symlink_swapped_in_after_the_check_is_still_not_followed(bbpath, tmp_path, monkeypatch):
    """Race (TOCTOU): anche se il controllo preliminare non vede il link (sostituito dopo), l'apertura con
    O_NOFOLLOW lo rifiuta: il bersaglio non viene letto."""
    bbpath.parent.mkdir(parents=True)
    victim = _fake_secret_file(tmp_path)
    os.symlink(victim, bbpath.parent / "fault.log")
    monkeypatch.setattr(rt, "_drop_if_not_private", lambda p: None)   # il link compare DOPO il controllo
    tail = rt._previous_fault_dump(str(bbpath.parent / "fault.log"))
    assert not tail or _C11_FAKE not in tail
    monkeypatch.undo()
    _assert_untouched(victim)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO POSIX")
def test_fault_dump_fifo_never_blocks_startup(bbpath):
    """Un FIFO al posto del dump non blocca l'avvio (apertura non bloccante, solo file regolari)."""
    bbpath.parent.mkdir(parents=True)
    os.mkfifo(bbpath.parent / "fault.log.pending")
    out = {}
    t = threading.Thread(target=lambda: out.setdefault("tail", rt._previous_fault_dump(
        str(bbpath.parent / "fault.log"))), daemon=True)
    t.start()
    t.join(5)
    assert not t.is_alive(), "avvio bloccato da un FIFO"


@pytest.mark.skipif(os.name != "posix", reason="hardlink POSIX; su Windows NOT_RUN")
def test_private_file_refuses_a_hardlinked_registry_file_without_chmodding_the_target(tmp_path):
    """Ciclo 11: `private_file` rifiuta un hardlink (stesso inode di un altro file) PRIMA di `fchmod`, cosi' i
    permessi del bersaglio non cambiano mai."""
    victim = _fake_secret_file(tmp_path)
    os.link(victim, tmp_path / "r.sqlite.runtime.owner")
    with pytest.raises(OSError):
        bb.private_file(str(tmp_path / "r.sqlite.runtime.owner"))
    _assert_untouched(victim)


# --- Ciclo 11, verifiche richieste dall'owner prima del push -------------------------------------------------------

@pytest.mark.skipif(os.name != "posix", reason="symlink/hardlink POSIX; su Windows NOT_RUN")
@pytest.mark.parametrize("kind", ["symlink", "hardlink"])
@pytest.mark.parametrize("name", ["fault.log", "fault.log.pending", "fault.log.1"])
def test_failed_removal_of_a_suspicious_dump_file_stays_fail_closed(bbpath, tmp_path, monkeypatch, kind, name):
    """Owner, punto 1: se la rimozione del link fallisce, il bersaglio non viene comunque letto ne' modificato
    (l'apertura con O_NOFOLLOW e i controlli su file regolare/un solo nome lo rifiutano) e l'avvio prosegue."""
    bbpath.parent.mkdir(parents=True)
    victim = _fake_secret_file(tmp_path)
    (os.symlink if kind == "symlink" else os.link)(victim, bbpath.parent / name)
    if name != "fault.log":
        (bbpath.parent / "fault.log").write_text("Fatal Python error: Segmentation fault\n")
    real_unlink = os.unlink

    def refuse_unlink(p, *a, **kw):
        if os.path.basename(str(p)) == name:
            raise PermissionError(errno.EACCES, "rimozione negata", str(p))
        return real_unlink(p, *a, **kw)

    monkeypatch.setattr(rt.os, "unlink", refuse_unlink)
    rt.install()
    rt.shutdown("STOP")
    monkeypatch.undo()
    _assert_untouched(victim)
    _assert_fake_not_persisted(bbpath, victim)
    if kind == "hardlink" and name != "fault.log.1":   # mai rinominato; `.1` viene solo sostituito dal dump legittimo
        assert os.path.samefile(bbpath.parent / name, victim)


@pytest.mark.skipif(os.name != "posix", reason="O_NOFOLLOW POSIX; su Windows NOT_RUN")
@pytest.mark.parametrize("name", ["fault.log.pending", "fault.log.1"])
def test_concurrent_link_swap_on_append_targets_never_writes_the_target(bbpath, tmp_path, monkeypatch, name):
    """Owner, punto 2: un link che compare su `.pending` o `.1` DOPO il controllo (race) non riceve l'accodamento
    del dump: il bersaglio resta identico, contenuto e permessi."""
    bbpath.parent.mkdir(parents=True)
    victim = _fake_secret_file(tmp_path)
    fault = bbpath.parent / "fault.log"
    fault.write_text("Fatal Python error: Segmentation fault\n")
    os.symlink(victim, bbpath.parent / name)
    monkeypatch.setattr(rt, "_drop_if_not_private", lambda p: None)   # controllo superato prima dello scambio
    if name == "fault.log.pending":
        rt._previous_fault_dump(str(fault))
    else:
        rt._consume_fault_dump(str(fault))
    monkeypatch.undo()
    _assert_untouched(victim)


@pytest.mark.parametrize("result", [
    {"cancel_error_count": 0, "cancel_errors": [], "cancel_results": [{"ok": True, "market_id": "1.1",
                                                                         "order_count": 2}],
     "markets_attempted": 1, "live_client_available": True, "persist_error": None, "cancelled_count": 2},
    {"cancel_error_count": 2, "cancel_errors": [], "cancel_results": [], "markets_attempted": 1},
    {"cancel_error_count": 0, "cancel_results": [{"ok": False, "error": "x", "market_id": "1.2"}],
     "markets_attempted": 1},
    {"cancel_error_count": 0, "cancel_errors": [{"stage": "get_live_client", "error": "x"}]},
    {"cancel_error_count": 0, "cancel_errors": [{"stage": "runtime_authority", "market_id": "1.3",
                                                  "error": "lost"}]},
    {"cancel_error_count": 0, "cancel_errors": [], "markets_attempted": 2, "live_client_available": False},
    {"cancel_error_count": 0, "cancel_errors": [], "persist_error": "disco pieno"},
    {"cancel_error_count": 0, "cancel_errors": [], "persist_error": "db_save_settings_unavailable"},
    {"cancel_error_count": 0, "cancel_errors": [], "persist_error": ""},
])
def test_error_scrubbing_keeps_the_outcome_and_the_recovery_structure(result):
    """Owner, punto 3: la sanitizzazione non cambia l'esito VERIFIED/FAILED/UNKNOWN (ricalcolato sul risultato
    sanitizzato) e conserva i campi strutturali necessari alla recovery: conteggi, mercati, stage, ok."""
    scrubbed = rt.scrub_error_texts(result)
    assert headless_main._emergency_stop_outcome(scrubbed) == headless_main._emergency_stop_outcome(result)
    for k, v in result.items():
        if not rt._is_error_key(k):
            if k == "cancel_results":
                for orig, new in zip(v, scrubbed[k]):
                    for f in ("ok", "market_id", "order_count"):
                        assert new.get(f) == orig.get(f)
            else:
                assert scrubbed[k] == v, k
    for orig, new in zip(result.get("cancel_errors") or [], scrubbed.get("cancel_errors") or []):
        assert new.get("stage") == orig.get("stage") and new.get("market_id") == orig.get("market_id")


# --- Ciclo 12 (OWNER AUTHORIZATION PR #617 — CYCLE 12): i due rilievi di Sol su 2847ca0e ----------------------------

_POSIX_ONLY = pytest.mark.skipif(os.name != "posix", reason="symlink/hardlink/FIFO POSIX; su Windows NOT_RUN")


def _victim_box(tmp_path):
    victim = _fake_secret_file(tmp_path)
    d = tmp_path / "reg"
    d.mkdir()
    return victim, d, bb.BlackBox(str(d / "r.sqlite")).open()


@_POSIX_ONLY
@pytest.mark.parametrize("suffix", [".runtime.owner", ".runtime.lock"])
def test_c12_runtime_file_swapped_after_its_check_never_reaches_the_target(tmp_path, monkeypatch, suffix):
    """Sol su 2847ca0e (riprodotto): fra la verifica di `private_file` e la riapertura per nome, un symlink al posto
    di `.runtime.owner` faceva troncare e riscrivere il bersaglio (`open(..., "w")`), e uno al posto di
    `.runtime.lock` faceva prendere il lock su un altro file. Ora nessuna riapertura per nome: il bersaglio resta
    identico e il lock, se preso, e' sul file del registro."""
    victim, d, box = _victim_box(tmp_path)
    path = str(d / ("r.sqlite" + suffix))
    real_private = bb.private_file

    def racing(p):
        real_private(p)
        if p == path:                                     # sostituzione subito dopo la verifica
            os.remove(p)
            os.symlink(victim, p)

    monkeypatch.setattr(bb, "private_file", racing)
    try:
        box.acquire_runtime()
    except (OSError, bb.BlackBoxUnavailable, bb.RuntimeAlreadyActive):
        pass
    monkeypatch.undo()
    _assert_untouched(victim)
    if box._writer_lock is not None:
        st = os.fstat(box._writer_lock.fileno())
        assert (st.st_dev, st.st_ino) != (victim.stat().st_dev, victim.stat().st_ino), "lock preso sul bersaglio"
    box.close()


@_POSIX_ONLY
@pytest.mark.parametrize("suffix", [".runtime.owner", ".runtime.lock"])
def test_c12_runtime_file_swapped_right_before_the_open_syscall_without_o_nofollow(tmp_path, monkeypatch, suffix):
    """Fallback multipiattaforma (simulato: O_NOFOLLOW assente, come su Windows): un link che compare subito prima
    della chiamata di apertura viene rilevato dal confronto fra il file aperto e il nome nella cartella, e
    l'operazione e' rifiutata senza scrivere nel bersaglio. Simulazione POSIX: NON e' un PASS Windows."""
    victim, d, box = _victim_box(tmp_path)
    path = str(d / ("r.sqlite" + suffix))
    if suffix == ".runtime.owner":
        open(path, "w").close()
    else:
        with open(path, "w") as fh:
            fh.write("\n")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    real_open = os.open
    swapped = []

    def racing_open(p, flags, *a, **kw):
        if str(p) == path and not swapped:
            swapped.append(1)
            os.remove(p)
            os.symlink(victim, p)
        return real_open(p, flags, *a, **kw)

    monkeypatch.setattr(os, "open", racing_open)
    try:
        box.acquire_runtime()
    except (OSError, bb.BlackBoxUnavailable, bb.RuntimeAlreadyActive):
        pass
    monkeypatch.undo()
    assert swapped, "l'apertura non e' passata da os.open"
    _assert_untouched(victim)
    if box._writer_lock is not None:
        st = os.fstat(box._writer_lock.fileno())
        assert (st.st_dev, st.st_ino) != (victim.stat().st_dev, victim.stat().st_ino), "lock preso sul bersaglio"
    box.close()


@_POSIX_ONLY
@pytest.mark.parametrize("suffix", [".runtime.owner", ".runtime.lock"])
@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo"])
def test_c12_runtime_files_refuse_links_and_non_regular_files(tmp_path, kind, suffix):
    """Hardlink, symlink e FIFO preesistenti al posto di `.runtime.owner`/`.runtime.lock`: mai seguiti ne' scritti;
    il lock non si prende su un file non privato (avvio rifiutato, fail-closed); l'owner, solo informativo, non
    viene scritto. Il bersaglio resta identico."""
    victim, d, box = _victim_box(tmp_path)
    path = d / ("r.sqlite" + suffix)
    if kind == "symlink":
        os.symlink(victim, path)
    elif kind == "hardlink":
        os.link(victim, path)
    else:
        os.mkfifo(path)
    out = {}

    def run():
        try:
            box.acquire_runtime()
            out["ok"] = True
        except (OSError, bb.BlackBoxUnavailable, bb.RuntimeAlreadyActive) as exc:
            out["exc"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(5)
    assert not t.is_alive(), "bloccato su un FIFO"
    _assert_untouched(victim)
    if suffix == ".runtime.lock":
        assert "exc" in out and box._writer_lock is None, "lock preso su un file non privato"
    box.close()


@_POSIX_ONLY
def test_c12_runtime_lock_and_owner_keep_exclusivity_and_content(tmp_path):
    """Il comportamento di ownership e locking resta quello di prima: un secondo runtime e' rifiutato, il file owner
    contiene pid e sessione del runtime attivo, entrambi a 0600."""
    import stat
    path = str(tmp_path / "r.sqlite")
    a = bb.BlackBox(path).open()
    a.acquire_runtime()
    b = bb.BlackBox(path).open()
    with pytest.raises(bb.RuntimeAlreadyActive):
        b.acquire_runtime()
    owner = (tmp_path / "r.sqlite.runtime.owner").read_text()
    assert owner == f"pid={a.pid} session={a.session_id}\n"
    for suffix in (".runtime.owner", ".runtime.lock"):
        assert stat.S_IMODE(os.stat(path + suffix).st_mode) == 0o600
    a.close()
    b.acquire_runtime()                                   # rilasciato alla chiusura: il nuovo runtime parte
    assert (tmp_path / "r.sqlite.runtime.owner").read_text() == f"pid={b.pid} session={b.session_id}\n"
    b.close()


@_POSIX_ONLY
@pytest.mark.parametrize("name", ["fault.log", "fault.log.pending", "fault.log.1"])
def test_c12_dump_link_not_removable_and_no_o_nofollow_stays_fail_closed(bbpath, tmp_path, monkeypatch, name):
    """Sol su 2847ca0e: senza O_NOFOLLOW (Windows) e con la rimozione del link fallita, il link era seguito e il
    bersaglio importato in CRASH_RECOVERY. Ora il nome che non e' un file regolare con un solo nome viene rifiutato
    prima dell'apertura e dopo (confronto file aperto / nome): nessuna lettura, nessuna scrittura. Simulazione POSIX
    del fallback: NON e' un PASS Windows."""
    bbpath.parent.mkdir(parents=True)
    victim = _fake_secret_file(tmp_path)
    os.symlink(victim, bbpath.parent / name)
    if name != "fault.log":
        (bbpath.parent / "fault.log").write_text("Fatal Python error: Segmentation fault\n")
    real_unlink = os.unlink

    def refuse_unlink(p, *a, **kw):
        if os.path.basename(str(p)) == name:
            raise PermissionError(errno.EACCES, "rimozione negata", str(p))
        return real_unlink(p, *a, **kw)

    monkeypatch.setattr(rt.os, "unlink", refuse_unlink)
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    rt.install()
    rt.shutdown("STOP")
    monkeypatch.undo()
    _assert_untouched(victim)
    _assert_fake_not_persisted(bbpath, victim)


@_POSIX_ONLY
def test_c12_private_file_without_o_nofollow_refuses_a_symlinked_registry(tmp_path, monkeypatch):
    """Fallback multipiattaforma di `private_file` (simulato: O_NOFOLLOW assente). Prima, fuori da POSIX la funzione
    non controllava nulla; ora un symlink al posto di un file del registro e' rifiutato ovunque e il bersaglio non
    viene toccato. NON e' un PASS Windows."""
    victim = _fake_secret_file(tmp_path)
    os.symlink(victim, tmp_path / "x.sqlite")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    with pytest.raises(OSError):
        bb.private_file(str(tmp_path / "x.sqlite"))
    monkeypatch.undo()
    _assert_untouched(victim)


@_POSIX_ONLY
def test_c12_creation_never_follows_a_link_that_appears_after_the_existence_check(tmp_path, monkeypatch):
    """Creazione: se un link (anche pendente) compare fra il controllo di esistenza e la creazione, la creazione e'
    esclusiva (O_EXCL) e fallisce: nessun file creato nel percorso puntato, anche senza O_NOFOLLOW."""
    target = tmp_path / "creato_altrove.txt"
    path = tmp_path / "r.sqlite.runtime.owner"
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    real_lstat = os.lstat
    state = {}

    def racing_lstat(p, *a, **kw):
        if str(p) == str(path) and "done" not in state:
            state["done"] = True
            os.symlink(target, path)                     # link pendente creato subito dopo il controllo
            raise FileNotFoundError(errno.ENOENT, "assente", str(p))
        return real_lstat(p, *a, **kw)

    monkeypatch.setattr(os, "lstat", racing_lstat)
    with pytest.raises(OSError):
        os.close(bb.open_private(str(path), os.O_WRONLY | os.O_CREAT))
    monkeypatch.undo()
    assert state.get("done") and not target.exists()


@_POSIX_ONLY
def test_c12_link_swapped_in_for_the_open_and_back_out_is_still_refused(tmp_path, monkeypatch):
    """Il caso che solo il confronto d'identita' copre: senza O_NOFOLLOW, il nome diventa un link al momento
    dell'apertura (che quindi apre il bersaglio) e torna subito un file regolare. Il descrittore non e' il file che
    il nome indica: rifiutato prima di qualsiasi scrittura. Simulazione del fallback: NON e' un PASS Windows."""
    victim = _fake_secret_file(tmp_path)
    path = tmp_path / "r.sqlite.runtime.owner"
    path.write_text("")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    real_open = os.open
    done = []

    def swap_in_and_out(p, flags, *a, **kw):
        if str(p) == str(path) and not done:
            done.append(1)
            os.replace(path, tmp_path / "originale")
            os.symlink(victim, path)
            fd = real_open(p, flags, *a, **kw)            # segue il link: e' il bersaglio
            os.remove(path)
            path.write_text("")                           # di nuovo un file regolare, ma un altro inode
            return fd
        return real_open(p, flags, *a, **kw)

    monkeypatch.setattr(os, "open", swap_in_and_out)
    with pytest.raises(OSError):
        os.close(bb.open_private(str(path), os.O_WRONLY | os.O_CREAT))
    monkeypatch.undo()
    assert done
    _assert_untouched(victim)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO POSIX")
def test_c12_fifo_is_refused_before_opening_even_without_o_nonblock(tmp_path, monkeypatch):
    """Il caso che solo il controllo PRIMA dell'apertura copre: senza O_NONBLOCK (es. Windows) l'apertura in
    lettura di un FIFO si bloccherebbe per sempre. Il nome non regolare e' rifiutato senza aprirlo."""
    fifo = tmp_path / "fault.log.pending"
    os.mkfifo(fifo)
    monkeypatch.delattr(os, "O_NONBLOCK", raising=False)
    out = {}

    def run():
        try:
            os.close(bb.open_private(str(fifo), os.O_RDONLY))
            out["opened"] = True
        except OSError as exc:
            out["exc"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(5)
    blocked = t.is_alive()
    if blocked:                                           # sblocca il thread prima di fallire
        os.close(os.open(fifo, os.O_WRONLY | getattr(os, "O_NONBLOCK", 0o4000)))
        t.join(5)
    monkeypatch.undo()
    assert not blocked, "apertura bloccata su un FIFO"
    assert "exc" in out
