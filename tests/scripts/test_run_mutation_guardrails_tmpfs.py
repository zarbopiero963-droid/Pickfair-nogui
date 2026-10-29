"""Mutation runner: sandbox e file temporanei dei test su tmpfs (FULL_SUITE_PERFORMANCE_REMEDIATION, GO owner #562).

Stesse mutazioni e stessi criteri: cambia solo dove scrivono i test. Qui si prova che il tmpfs si sceglie solo
quando e' davvero un tmpfs utilizzabile, che altrimenti si resta su disco dicendolo, che ogni worker ha la sua
directory temporanea svuotata a ogni esecuzione, che niente resta in RAM a fine run e che spazio o memoria
esauriti non diventano mai una mutazione intercettata.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "run_mutation_guardrails_tmpfs", ROOT / "scripts" / "run_mutation_guardrails.py"
)
runner = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = runner
_spec.loader.exec_module(runner)

#: scelta del tmpfs (percorsi e mount POSIX) e pulizia per proprietario (os.getuid): solo POSIX. Su Windows il
#: runner resta su disco e la pulizia non cancella niente (provato dai test senza questo segno) (GPT-6.1 Sol su #585)
POSIX_ONLY = pytest.mark.skipif(os.name != "posix" or not hasattr(os, "getuid"),
                                reason="tmpfs e pulizia per proprietario solo POSIX")

MODULO = "def liability(stake, price):\n    return stake * (price - 1.0)\n"
LIAB = "    return stake * (price - 1.0)"
#: il test scrive un database nel suo tmp_path e annota dove: prova quale TMPDIR vede pytest
TEST_SRC = """\
import os
import sqlite3
import tempfile

from calc import liability


def test_liability_lay(tmp_path):
    db = sqlite3.connect(tmp_path / "x.sqlite3")
    db.execute("create table t(v)")
    db.execute("insert into t values (1)")
    db.commit()
    db.close()
    with open(os.environ["PF_PROBE"], "a", encoding="utf-8") as fh:
        fh.write(tempfile.gettempdir() + "\\n")
    assert liability(10.0, 3.0) == 20.0
"""


def _repo(tmp_path: Path, mutations) -> Path:
    repo = tmp_path / "repo"
    (repo / "tests").mkdir(parents=True)
    (repo / "guardrails").mkdir()
    (repo / "calc.py").write_text(MODULO, encoding="utf-8")
    (repo / "tests" / "test_calc.py").write_text(TEST_SRC, encoding="utf-8")
    catalog = {"required_areas": ["lay_liability"], "optional_areas": [], "mutations": mutations}
    (repo / "guardrails" / "mutation_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    return repo


def _mut(mid, mutated):
    return {"id": mid, "module": "calc", "area": "lay_liability", "file": "calc.py", "original": LIAB,
            "mutated": mutated, "tests": ["tests/test_calc.py"], "rationale": "test"}


MUTS = [_mut("zero", "    return 0.0"), _mut("double", "    return stake * (price - 1.0) * 2"),
        _mut("neg", "    return -stake * (price - 1.0)")]


def _mounts(tmp_path: Path, *rows) -> str:
    path = tmp_path / "mounts"
    lines = "".join(f"dev {r[0]} {r[1]} {r[2] if len(r) > 2 else 'rw,nosuid,nodev'} 0 0\n" for r in rows)
    path.write_text(lines, encoding="utf-8")
    return str(path)


def _run(repo, tmp_path, monkeypatch, *extra):
    probe = tmp_path / "probe.txt"
    monkeypatch.setenv("PF_PROBE", str(probe))
    out = tmp_path / "report.json"
    rc = runner.main(["--repo-root", str(repo), "--output", str(out), "--timeout-sec", "60", *extra])
    report = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    seen = probe.read_text(encoding="utf-8").split() if probe.exists() else []
    return rc, report, seen


def _fake_tmpfs(tmp_path, monkeypatch):
    monkeypatch.delenv(runner.NESTED_ENV, raising=False)      # tmpfs finto: solo la directory di questo test
    shm = tmp_path / "shm"
    shm.mkdir()
    monkeypatch.setattr(runner, "TMPFS_CANDIDATES", (str(shm),))
    monkeypatch.setattr(runner, "MOUNTS_FILE", _mounts(tmp_path, ("/", "ext4"), (str(shm.resolve()), "tmpfs")))
    monkeypatch.setattr(runner, "TMPFS_MIN_FREE_BYTES", 1)
    return shm.resolve()


# --- scelta del tmpfs

@POSIX_ONLY
def test_only_a_real_writable_tmpfs_with_space_is_chosen(tmp_path):
    shm = tmp_path / "shm"
    shm.mkdir()
    real = str(shm.resolve())
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, ("/", "ext4"), (real, "tmpfs")), 1)
    assert root == real and why.startswith("tmpfs ")
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, ("/", "ext4")), 1)
    assert root is None and "filesystem ext4, non tmpfs" in why
    root, why = runner.choose_tmp_root((str(tmp_path / "missing"),), _mounts(tmp_path, ("/", "tmpfs")), 1)
    assert root is None and "assente" in why
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, (real, "tmpfs")), 2**62)
    assert root is None and "MiB liberi, minimo" in why


def test_mounts_unreadable_means_disk_never_a_guess(tmp_path):
    shm = tmp_path / "shm"
    shm.mkdir()
    root, why = runner.choose_tmp_root((str(shm),), str(tmp_path / "no-mounts"), 1)
    assert root is None and "filesystem sconosciuto" in why


def test_longest_mount_point_decides_the_filesystem(tmp_path):
    mounts = _mounts(tmp_path, ("/", "tmpfs"), ("/var", "ext4"), ("/var/lib/x", "tmpfs"))
    assert runner._mount_fstype(Path("/var/lib/buildkite"), mounts) == "ext4"
    assert runner._mount_fstype(Path("/var/lib/x/y"), mounts) == "tmpfs"
    assert runner._mount_fstype(Path("/varx"), mounts) == "tmpfs"


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="root (o Windows) ignora i permessi")
def test_non_writable_tmpfs_is_refused(tmp_path):
    shm = tmp_path / "shm"
    shm.mkdir(mode=0o500)
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, (str(shm.resolve()), "tmpfs")), 1)
    assert root is None and "non scrivibile" in why


# --- run reale sul tmpfs (finto: directory annunciata come tmpfs) e su disco

@POSIX_ONLY
def test_run_on_tmpfs_puts_sandbox_and_test_files_there_and_leaves_nothing(tmp_path, monkeypatch):
    shm = _fake_tmpfs(tmp_path, monkeypatch)
    rc, report, seen = _run(_repo(tmp_path, MUTS), tmp_path, monkeypatch, "--jobs", "2")
    assert rc == 0 and [r["status"] for r in report["results"]] == [runner.KILLED] * 3
    assert report["summary"]["tmp_root"].startswith(f"tmpfs {shm}")
    assert seen and all(s.startswith(str(shm) + "/" + runner.TMP_PREFIX) for s in seen)
    assert all(s.endswith("-tmp") for s in seen)                 # TMPDIR del worker, non la sandbox
    assert not list(shm.iterdir()), "workspace rimasto in RAM a fine run"


@POSIX_ONLY
def test_each_worker_has_its_own_test_tmpdir(tmp_path, monkeypatch):
    _fake_tmpfs(tmp_path, monkeypatch)
    _, _, seen = _run(_repo(tmp_path, MUTS), tmp_path, monkeypatch, "--jobs", "3")
    assert len(set(seen)) == 3


@POSIX_ONLY
def test_tmpfs_off_and_fallback_keep_results_identical(tmp_path, monkeypatch):
    _fake_tmpfs(tmp_path, monkeypatch)
    rc_t, rep_t, _ = _run(_repo(tmp_path / "a", MUTS), tmp_path / "a", monkeypatch, "--jobs", "2")
    rc_d, rep_d, seen_d = _run(_repo(tmp_path / "b", MUTS), tmp_path / "b", monkeypatch, "--jobs", "2",
                               "--tmpfs", "off")
    assert rc_t == rc_d == 0
    assert [r["status"] for r in rep_t["results"]] == [r["status"] for r in rep_d["results"]]
    assert rep_d["summary"]["tmp_root"] == "disco (--tmpfs off)"
    assert seen_d and not any(s.startswith(str(tmp_path / "shm")) for s in seen_d)


@POSIX_ONLY
def test_nested_runner_stays_on_disk_and_never_cleans_the_shared_tmpfs(tmp_path, monkeypatch):
    shm = _fake_tmpfs(tmp_path, monkeypatch)
    outer = shm / (runner.TMP_PREFIX + "outer-run")
    outer.mkdir()
    past = time.time() - runner.STALE_TMP_SECONDS - 60
    os.utime(outer, (past, past))
    monkeypatch.setenv(runner.NESTED_ENV, "1")
    rc, report, seen = _run(_repo(tmp_path, MUTS[:1]), tmp_path, monkeypatch)
    assert rc == 0 and report["summary"]["tmp_root"] == "disco (runner annidato in una mutazione)"
    assert outer.exists() and seen and not any(s.startswith(str(shm)) for s in seen)


def test_pytest_children_are_marked_nested(tmp_path, monkeypatch):
    monkeypatch.delenv(runner.NESTED_ENV, raising=False)       # il segno deve venire dal runner, non dall'ambiente
    seen = {}

    class Proc:
        returncode = 0

        def __init__(self, cmd, cwd, env, **kw):
            seen.update(env)

        def communicate(self, timeout=None):
            return "", None

    monkeypatch.setattr(runner.subprocess, "Popen", Proc)
    sandbox = tmp_path / "repo0"
    sandbox.mkdir()
    runner._run_pytest(sandbox, ["tests/x.py"], [], 5)
    assert seen[runner.NESTED_ENV] == "1" and seen["TMPDIR"] == str(runner.worker_tmp_dir(sandbox))


def test_unusable_tmpfs_falls_back_to_disk_and_says_so(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv(runner.NESTED_ENV, raising=False)
    monkeypatch.setattr(runner, "TMPFS_CANDIDATES", (str(tmp_path / "missing"),))
    rc, report, seen = _run(_repo(tmp_path, MUTS[:1]), tmp_path, monkeypatch)
    assert rc == 0 and report["summary"]["tmp_root"].startswith("disco (")
    assert "Temporary workspaces: disco (" in capsys.readouterr().out and seen


# --- nessun falso KILLED, nessun residuo

def test_test_tmpdir_is_emptied_before_every_pytest_run(tmp_path):
    sandbox = tmp_path / "repo0"
    sandbox.mkdir()
    tmpdir = runner.worker_tmp_dir(sandbox)
    tmpdir.mkdir()
    (tmpdir / "leftover.sqlite3").write_text("vecchio", encoding="utf-8")
    runner._run_pytest(sandbox, ["tests/none.py"], [], 30)
    assert tmpdir.is_dir() and not (tmpdir / "leftover.sqlite3").exists()


def test_exhausted_space_is_never_counted_as_killed(tmp_path, monkeypatch):
    def full(sandbox, tests, pytest_args, timeout_sec):
        mutated = (sandbox / "calc.py").read_text(encoding="utf-8") != MODULO
        if mutated:
            return 1, 0.1, "E   OSError: [Errno 28] No space left on device"
        return 0, 0.1, "1 passed"
    monkeypatch.setattr(runner, "_run_pytest", full)
    rc, report, _ = _run(_repo(tmp_path, MUTS[:1]), tmp_path, monkeypatch)
    res = report["results"][0]
    assert res["status"] == runner.INCONCLUSIVE and "exhausted temporary space" in res["detail"]
    assert rc != 0 and report["summary"]["gate_passed"] is False


def test_low_free_space_stops_the_run_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_free_bytes", lambda path: 0)
    with pytest.raises(runner.MutationRunnerError, match="temporary space below"):
        _run(_repo(tmp_path, MUTS[:1]), tmp_path, monkeypatch, "--tmpfs", "off")
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["summary"]["gate_passed"] is False


@POSIX_ONLY
def test_stale_workspaces_of_killed_runs_are_removed_only_when_old(tmp_path):
    old, fresh, other = tmp_path / "pf_mutation_old", tmp_path / "pf_mutation_new", tmp_path / "keep"
    for d in (old, fresh, other):
        d.mkdir()
    past = time.time() - runner.STALE_TMP_SECONDS - 60
    for d in (old, other):
        os.utime(d, (past, past))
    assert runner.clean_stale_tmp(str(tmp_path)) == ["pf_mutation_old"]
    assert not old.exists() and fresh.exists() and other.exists()


@POSIX_ONLY
def test_stale_cleanup_never_follows_a_symlink(tmp_path):
    target = tmp_path / "precious"
    target.mkdir()
    (target / "data").write_text("x", encoding="utf-8")
    link = tmp_path / "pf_mutation_link"
    link.symlink_to(target)
    past = time.time() - runner.STALE_TMP_SECONDS - 60
    os.utime(link, (past, past), follow_symlinks=False)
    assert runner.clean_stale_tmp(str(tmp_path)) == []
    assert (target / "data").exists()


@pytest.mark.parametrize("line", ["E   OSError: [Errno 28] No space left on device",
                                  "E       MemoryError",
                                  "E   sqlite3.OperationalError: database or disk is full"])
def test_space_exhaustion_lines_are_recognised(line):
    assert runner._NO_SPACE_RE.search("....\n" + line + "\n1 failed")


def test_the_words_alone_in_printed_source_are_not_space_exhaustion():
    out = ('    def full():\n>       return 1, 0.1, "E   OSError: [Errno 28] No space left on device"\n'
           "E   AssertionError: assert 'KILLED' == 'INCONCLUSIVE'\n")
    assert not runner._NO_SPACE_RE.search(out)


@POSIX_ONLY
def test_workspace_left_on_tmpfs_after_the_run_is_an_error(tmp_path, monkeypatch):
    shm = _fake_tmpfs(tmp_path, monkeypatch)

    class NoCleanup:
        def __init__(self, prefix, dir, ignore_cleanup_errors):
            self.name = str(Path(dir) / (prefix + "stuck"))

        def __enter__(self):
            Path(self.name).mkdir()
            return self.name

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(runner.tempfile, "TemporaryDirectory", NoCleanup)
    with pytest.raises(runner.MutationRunnerError, match="not removed from tmpfs"):
        _run(_repo(tmp_path, MUTS[:1]), tmp_path, monkeypatch)
    assert (shm / (runner.TMP_PREFIX + "stuck")).exists()


def test_free_space_uses_a_portable_api(tmp_path, monkeypatch):
    """GPT-6.1 Sol su #585: `os.statvfs` non esiste su Windows; lo spazio libero si legge con `shutil.disk_usage`."""
    class OsWithoutStatvfs:                       # come su Windows: il modulo `os` del runner senza statvfs
        def __getattr__(self, name):
            if name == "statvfs":
                raise AttributeError(name)
            return getattr(os, name)

    monkeypatch.setattr(runner, "os", OsWithoutStatvfs())
    assert runner._free_bytes(tmp_path) > 0


def test_module_collects_without_posix_only_apis():
    """L'import del modulo di test non chiama API solo POSIX (skipif valutato alla raccolta)."""
    src = Path(__file__).read_text(encoding="utf-8")
    assert "skipif(os." + "geteuid()" not in src


# --- Codex e Grok su #585 (fb3ffd8): noexec, residui prima dello spazio, nessun os.getuid

@POSIX_ONLY
def test_noexec_tmpfs_is_refused_because_tests_execute_scripts_there(tmp_path):
    shm = tmp_path / "shm"
    shm.mkdir()
    real = str(shm.resolve())
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, (real, "tmpfs", "rw,nosuid,nodev,noexec")), 1)
    assert root is None and "montato noexec" in why
    root, _ = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, (real, "tmpfs", "rw,nosuid,nodev")), 1)
    assert root == real


@POSIX_ONLY
def test_stale_workspaces_are_removed_before_the_free_space_check(tmp_path, monkeypatch):
    shm = tmp_path / "shm"
    stale = shm / (runner.TMP_PREFIX + "killed-run")
    stale.mkdir(parents=True)
    past = time.time() - runner.STALE_TMP_SECONDS - 60
    os.utime(stale, (past, past))
    # il tmpfs e' "pieno" finche' il residuo esiste
    monkeypatch.setattr(runner, "_free_bytes", lambda path: 0 if stale.exists() else 2**30)
    real = str(shm.resolve())
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, (real, "tmpfs")), 2**20)
    assert root == real and "rimossi 1 workspace residui" in why and not stale.exists()


def test_stale_cleanup_deletes_nothing_without_getuid(tmp_path, monkeypatch):
    old = tmp_path / (runner.TMP_PREFIX + "old")
    old.mkdir()
    past = time.time() - runner.STALE_TMP_SECONDS - 60
    os.utime(old, (past, past))

    class OsWithoutGetuid:                        # come su Windows: proprietario non verificabile
        def __getattr__(self, name):
            if name == "getuid":
                raise AttributeError(name)
            return getattr(os, name)

    monkeypatch.setattr(runner, "os", OsWithoutGetuid())
    assert runner.clean_stale_tmp(str(tmp_path)) == [] and old.exists()


def test_mount_options_are_read_from_the_longest_mount_point(tmp_path):
    mounts = _mounts(tmp_path, ("/", "ext4", "rw"), ("/dev/shm", "tmpfs", "rw,noexec"))
    assert runner._mount_info(Path("/dev/shm/x"), mounts) == ("tmpfs", frozenset({"rw", "noexec"}))
    assert runner._mount_info(Path("/home"), mounts) == ("ext4", frozenset({"rw"}))


@pytest.mark.skipif(os.name != "posix", reason="prova di esecuzione solo POSIX")
def test_real_execution_probe_runs_a_script_and_leaves_nothing(tmp_path):
    assert runner._can_execute_in(tmp_path) is True
    assert not list(tmp_path.iterdir()), "script di prova non rimosso"


@POSIX_ONLY
def test_tmpfs_where_scripts_cannot_run_is_refused_even_without_noexec(tmp_path, monkeypatch):
    shm = tmp_path / "shm"
    shm.mkdir()
    real = str(shm.resolve())
    monkeypatch.setattr(runner, "_can_execute_in", lambda directory: False)
    root, why = runner.choose_tmp_root((str(shm),), _mounts(tmp_path, (real, "tmpfs", "rw,nosuid,nodev")), 1)
    assert root is None and "esecuzione di script non consentita" in why


@pytest.mark.skipif(os.name != "posix", reason="prova di esecuzione solo POSIX")
def test_execution_probe_failure_is_false(tmp_path, monkeypatch):
    def denied(*a, **kw):
        raise PermissionError("noexec")
    monkeypatch.setattr(runner.subprocess, "run", denied)
    assert runner._can_execute_in(tmp_path) is False
    assert not list(tmp_path.iterdir())


def test_tmpfs_and_owner_cleanup_tests_are_skipped_where_posix_apis_are_missing():
    """GPT-6.1 Sol su #585: i test che dipendono da os.getuid o dai mount POSIX portano il segno POSIX_ONLY."""
    src = Path(__file__).read_text(encoding="utf-8")
    for name in ("test_stale_workspaces_are_removed_before_the_free_space_check",
                 "test_stale_workspaces_of_killed_runs_are_removed_only_when_old",
                 "test_stale_cleanup_never_follows_a_symlink"):
        assert f"@POSIX_ONLY\ndef {name}(" in src, name
