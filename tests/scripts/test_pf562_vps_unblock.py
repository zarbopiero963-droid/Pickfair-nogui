"""ops/ci/PF562_VPS_UNBLOCK.sh (#562): sblocco della lane in un comando, provato senza VPS.

I comandi di sistema (id, uname, systemctl, pgrep, runuser, journalctl) sono stub in un PATH di prova, GitHub e' un
repository git locale con il dispatcher e il generatore di `main`. Si prova che il preflight si ferma sulla VPS o
sull'argomento sbagliato, che appimagetool si installa solo con lo sha256 atteso, che il dry-run non tocca niente,
che `--apply` aggiorna il dispatcher da `main` con backup e permessi corretti, e che il rollback ripristina tutto.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ops" / "ci" / "PF562_VPS_UNBLOCK.sh"
PY = "/usr/bin/python3.12"
#: utente fittizio del job, sempre diverso da chi esegue i test (che possiede i file creati)
AGENT = "daemon" if subprocess.run(["id", "-un"], capture_output=True, text=True).stdout.strip() == "nobody" else "nobody"

pytestmark = pytest.mark.skipif(
    os.name != "posix" or shutil.which("bash") is None or shutil.which("git") is None or not Path(PY).exists()
    or subprocess.run([PY, "-c", "import tkinter"], capture_output=True).returncode != 0,
    reason="servono bash, git e un Python con tkinter")

STUBS = {
    "id": '#!/bin/bash\nif [ "$1" = "-u" ] && [ $# -eq 1 ]; then echo "${STUB_UID:-0}"; else exec /usr/bin/id "$@"; fi\n',
    "uname": '#!/bin/bash\ncase "$1" in -s) echo Linux;; -m) echo "${STUB_ARCH:-x86_64}";; *) exec /usr/bin/uname "$@";; esac\n',
    "systemctl": """#!/bin/bash
case "$1" in
  cat) [ "${STUB_UNIT:-1}" = 1 ];;
  show) echo "{ path=$STUB_PY ; argv[]=$STUB_PY -I $STUB_DISP serve --config $STUB_CFG ; }";;
  restart|stop|start) echo "$1" >> "$STUB_LOG";;
  is-active) exit 0;;
  *) exit 0;;
esac
""",
    "pgrep": '#!/bin/bash\necho "${STUB_PROCS:-1}"\n',
    # l'utente del job: scrive/esegue solo cio' che i bit "other" concedono (i file del test sono di root)
    "runuser": """#!/bin/bash
f="${@: -1}"; op="${@: -2:1}"
m=$(stat -c %a "$f"); o=${m: -1}
case "$op" in -w) [ $((o & 2)) -ne 0 ];; -x) [ $((o & 1)) -ne 0 ];; *) exit 1;; esac
""",
    "journalctl": '#!/bin/bash\nprintf "%s" "${STUB_JOURNAL:-}"\n',
}


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def lane(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in STUBS.items():
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    # "main" su un repository locale: dispatcher e generatore del checkout corrente
    work = tmp_path / "src"
    (work / "scripts").mkdir(parents=True)
    for f in ("pr_trusted_dispatcher.py", "pr_full_suite_pipeline.py"):
        shutil.copy(ROOT / "scripts" / f, work / "scripts" / f)
    _git("init", "-q", "-b", "main", cwd=work)
    _git("add", ".", cwd=work)
    _git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "main", cwd=work)
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=work, capture_output=True, text=True).stdout.strip()
    bare = tmp_path / "remote.git"
    subprocess.run(["git", "clone", "-q", "--bare", str(work), str(bare)], check=True)
    _git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=bare)
    # dispatcher "installato": versione vecchia, configurazione valida, archivio esistente
    inst = tmp_path / "opt"
    inst.mkdir()
    disp = inst / "pr_trusted_dispatcher.py"
    disp.write_text("# dispatcher installato (vecchio)\n")
    disp.chmod(0o644)
    bk_secret_file = tmp_path / "bk-token"
    bk_secret_file.write_text("bkua_" + "a" * 40)
    bk_secret_file.chmod(0o600)
    cfg = {
        "github": {"token_command": ["/usr/local/bin/pf-dispatch-github-token"], "token_command_timeout_seconds": 20},
        "rerun": {"allowed_logins": ["zarbopiero963-droid"]},
        "buildkite": {"organization": "org", "token_file": str(bk_secret_file),
                      "result_artifacts": {"overlap": ".trusted-guard/overlap.json",
                                           "readiness": ".trusted-guard/readiness.json"},
                      "artifact_download_hosts": ["s3.example.com"]},
        "publisher": {"command": ["/usr/local/bin/pf-status-publish-client"], "timeout_seconds": 120},
        "state_db": str(tmp_path / "state.sqlite3"), "retention_hours": 168, "poll_interval_seconds": 60,
        "build_timeout_seconds": {"overlap": 600, "readiness": 900}, "http_timeout_seconds": 30,
        "readiness_wait": {"enabled": True, "max_seconds": 7200, "settle_polls": 2, "min_delay_seconds": 120},
        "full_suite": {"enabled": True, "build_timeout_seconds": 7200},
    }
    cfg_path = tmp_path / "dispatch.json"
    cfg_path.write_text(json.dumps(cfg))
    import sqlite3
    sqlite3.connect(cfg["state_db"]).execute("create table t(x)").connection.commit()
    agent_cfg = tmp_path / "agent.cfg"
    agent_cfg.write_text('name="pickfair-dedicated-vps-01-%n"\ntags="queue=pickfair-dedicated,os=linux"\n')
    tools = tmp_path / "tools"
    tools.mkdir()
    ait = tmp_path / "appimagetool-x86_64.AppImage"
    ait.write_bytes(b"\x7fELF" + b"fake appimagetool" * 100)
    backups = tmp_path / "backups"
    backups.mkdir()
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path),
        "PF562_TOOLS_DIR": str(tools), "PF562_LANE_PYTHON": PY, "PF562_PY_MINOR": "12",
        "PF562_AGENT_CFG": str(agent_cfg), "PF562_AGENT_USER": AGENT, "PF562_BACKUP_BASE": str(backups),
        # installazione col proprietario corrente: i test girano anche su runner non-root (Sol su #587)
        "PF562_INSTALL_OWNER": subprocess.run(["id", "-un"], capture_output=True, text=True).stdout.strip(),
        "PF562_INSTALL_GROUP": subprocess.run(["id", "-gn"], capture_output=True, text=True).stdout.strip(),
        "PF562_REPO_URL": f"file://{bare}", "STUB_PY": PY, "STUB_DISP": str(disp), "STUB_CFG": str(cfg_path),
        "STUB_LOG": str(tmp_path / "systemctl.log"),
    }
    return {"env": env, "sha": sha, "ait": ait, "ait_sha": hashlib.sha256(ait.read_bytes()).hexdigest(),
            "disp": disp, "tools": tools, "backups": backups, "cfg": cfg_path, "tmp": tmp_path}


def _run(lane, *args, **extra_env):
    env = dict(lane["env"], **extra_env)
    return subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=120)


def _base(lane):
    return ["--main-sha", lane["sha"], "--appimagetool-sha256", lane["ait_sha"], "--appimagetool-file", str(lane["ait"])]


# --- argomenti e preflight

@pytest.mark.parametrize("args", [[], ["--main-sha", "abc", "--appimagetool-sha256", "0" * 64],
                                  ["--main-sha", "a" * 40, "--appimagetool-sha256", "xyz"], ["--boh"]])
def test_bad_arguments_are_refused(lane, args):
    assert _run(lane, *args).returncode == 2


@pytest.mark.parametrize("env, reason", [({"STUB_UID": "1000"}, "serve root"),
                                         ({"STUB_ARCH": "aarch64"}, "non x86_64"),
                                         ({"STUB_UNIT": "0"}, "VPS sbagliata"),
                                         ({"PF562_AGENT_QUEUE": "altra-coda"}, "VPS sbagliata"),
                                         ({"PF562_AGENT_NAME": "altra-vps"}, "VPS sbagliata")])
def test_preflight_stops_on_the_wrong_host(lane, env, reason):
    res = _run(lane, *_base(lane), **env)
    assert res.returncode == 1 and reason in res.stdout


def test_main_sha_must_be_the_current_main(lane):
    args = _base(lane)
    args[1] = "b" * 40
    res = _run(lane, *args)
    assert res.returncode == 1 and "diverso da origin/main" in res.stdout


def test_appimagetool_with_another_hash_is_never_installed(lane):
    args = _base(lane)
    args[3] = "0" * 64
    res = _run(lane, *args, "--apply")
    assert res.returncode == 1 and "diverso dall'atteso" in res.stdout
    assert not (lane["tools"] / "appimagetool").exists() and not list(lane["backups"].glob("pf562-unblock-*"))


def test_dry_run_changes_nothing(lane):
    before = lane["disp"].read_text()
    res = _run(lane, *_base(lane))
    assert res.returncode == 0, res.stdout + res.stderr
    assert "step: 52 generati = 52 obbligatori" in res.stdout and "DRY-RUN" in res.stdout
    assert lane["disp"].read_text() == before and not (lane["tools"] / "appimagetool").exists()
    assert not Path(lane["env"]["STUB_LOG"]).exists()


# --- applicazione, idempotenza, rollback

def test_apply_installs_verified_tool_and_main_dispatcher_with_backup(lane):
    old = lane["disp"].read_text()
    res = _run(lane, *_base(lane), "--apply")
    assert res.returncode == 0, res.stdout + res.stderr
    tool = lane["tools"] / "appimagetool" / "appimagetool"
    assert hashlib.sha256(tool.read_bytes()).hexdigest() == lane["ait_sha"]
    assert stat.S_IMODE(tool.stat().st_mode) == 0o555
    assert stat.S_IMODE((tool.parent / "appimagetool.sha256").stat().st_mode) == 0o444
    assert (tool.parent / "appimagetool.sha256").read_text().split()[0] == lane["ait_sha"]
    assert lane["disp"].read_text() == (ROOT / "scripts" / "pr_trusted_dispatcher.py").read_text()
    assert stat.S_IMODE(lane["disp"].stat().st_mode) == 0o644
    bk = next(lane["backups"].glob("pf562-unblock-*"))
    assert (bk / "dispatcher.py").read_text() == old and (bk / "config.json").exists() and (bk / "dispatcher.db").exists()
    assert (bk / "manifest.sha256").exists() and "rollback:" in (bk / "report.txt").read_text()
    assert Path(lane["env"]["STUB_LOG"]).read_text().split() == ["restart"]
    assert "bkua_" not in res.stdout + res.stderr


def test_apply_is_idempotent(lane):
    assert _run(lane, *_base(lane), "--apply").returncode == 0
    Path(lane["env"]["STUB_LOG"]).unlink()
    res = _run(lane, *_base(lane), "--apply")
    assert res.returncode == 0 and "(gia aggiornato)" in res.stdout and "(gia installato)" in res.stdout
    assert not Path(lane["env"]["STUB_LOG"]).exists(), "riavvio non necessario"


def test_two_dispatcher_processes_trigger_the_automatic_rollback(lane):
    old = lane["disp"].read_text()
    res = _run(lane, *_base(lane), "--apply", STUB_PROCS="2")
    assert res.returncode in (1, 3) and "Rollback automatico" in res.stdout
    assert lane["disp"].read_text() == old
    assert not (lane["tools"] / "appimagetool").exists()


def test_startup_errors_in_the_journal_trigger_the_rollback(lane):
    old = lane["disp"].read_text()
    res = _run(lane, *_base(lane), "--apply", STUB_JOURNAL="Traceback (most recent call last)")
    assert "Rollback automatico" in res.stdout and lane["disp"].read_text() == old


def test_manual_rollback_restores_the_previous_state(lane):
    old = lane["disp"].read_text()
    assert _run(lane, *_base(lane), "--apply").returncode == 0
    bk = next(lane["backups"].glob("pf562-unblock-*"))
    res = _run(lane, "--rollback", str(bk), STUB_PROCS="1")
    assert res.returncode == 0, res.stdout + res.stderr
    assert lane["disp"].read_text() == old and not (lane["tools"] / "appimagetool").exists()


def test_tampered_backup_is_refused(lane):
    assert _run(lane, *_base(lane), "--apply").returncode == 0
    bk = next(lane["backups"].glob("pf562-unblock-*"))
    (bk / "dispatcher.py").write_text("alterato\n")
    assert _run(lane, "--rollback", str(bk)).returncode == 3


def test_script_never_downloads_without_a_hash_and_never_runs_the_tool():
    src = SCRIPT.read_text()
    assert "--appimagetool-sha256" in src and "die \"sha256 di appimagetool" in src
    body = [ln for ln in src.splitlines() if not ln.lstrip().startswith("#")]
    assert not any('"$AIT_CAND" ' in ln and "sha256sum" not in ln and "cp " not in ln and "install " not in ln
                   and "curl" not in ln and "head -c" not in ln for ln in body), "il candidato non si esegue"
    assert "--appimage-extract" not in src and "Betfair" not in "\n".join(body)



# --- GPT-6.1 Sol su #587: directory superiori scrivibili dal job, errori dopo il backup

def test_a_tools_directory_writable_by_the_job_stops_the_preflight(lane):
    lane["tools"].chmod(0o777)                                     # il job potrebbe rinominare appimagetool/
    res = _run(lane, *_base(lane), "--apply")
    assert res.returncode == 1 and f"scrivibile da {AGENT}" in res.stdout and str(lane["tools"]) in res.stdout
    assert not (lane["tools"] / "appimagetool").exists() and not list(lane["backups"].glob("pf562-unblock-*"))


def test_a_sticky_directory_of_another_user_is_not_a_risk(lane):
    lane["tools"].chmod(0o1777)                                    # come /tmp: il job non rinomina file altrui
    res = _run(lane, *_base(lane))
    assert res.returncode == 0, res.stdout + res.stderr


def test_a_write_error_after_the_backup_rolls_back(lane):
    old = lane["disp"].read_text()
    ait_dir = lane["tools"] / "appimagetool"
    ait_dir.mkdir()
    (ait_dir / "appimagetool.sha256.new").mkdir()                  # la scrittura del checksum fallira'
    res = _run(lane, *_base(lane), "--apply")
    assert res.returncode in (1, 3) and "errore non previsto" in res.stdout and "Rollback automatico" in res.stdout
    assert lane["disp"].read_text() == old and not (ait_dir / "appimagetool").exists()
