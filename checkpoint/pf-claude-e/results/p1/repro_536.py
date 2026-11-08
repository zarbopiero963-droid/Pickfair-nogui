"""Riproduzione indipendente #536 — due processi OS reali sullo stesso SQLite.

Uso: python repro_536.py <repo_root> <workdir> <delete|replace>
A avvia il runtime SIM, il driver cancella (o sostituisce) <db>.runtime.lock, poi avvia B in un altro processo.
A attende 13 s (> heartbeat 10 s del fix) e riporta modo, motivo e se una scrittura nel DB e' ancora ammessa.
"""
import os
import subprocess
import sys
import time

ROOT, WORK, MODE = sys.argv[1], sys.argv[2], sys.argv[3]

RUNTIME = r"""
import os, sys, time
import headless_main
from database import Database
percorso, ruolo, attesa = sys.argv[1], sys.argv[2], float(sys.argv[3])
headless_main.Database = lambda: Database(percorso)
app = headless_main.HeadlessApp()
app.build(start_services=False)
app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
app.telegram_service.stop = lambda *a, **k: None
esito = app.runtime.start(execution_mode="SIMULATION")
print(ruolo, "START", "started=%s" % esito.get("started"), "reason_code=%s" % esito.get("reason_code"),
      "mode=%s" % getattr(app.runtime.mode, "value", app.runtime.mode), flush=True)
if not esito.get("started"):
    print(ruolo, "REASON", esito.get("reason"), flush=True)
    sys.exit(0)
time.sleep(attesa)
auth = getattr(app.runtime, "runtime_authority", None)
print(ruolo, "AFTER", "mode=%s" % getattr(app.runtime.mode, "value", app.runtime.mode),
      "lost=%s" % getattr(app.runtime, "_authority_lost_reason", None),
      "live_allowed=%s" % app.runtime.is_live_allowed(), flush=True)
try:
    app.runtime.db.save_settings({"repro536.%s" % ruolo: "x"})
    print(ruolo, "WRITE", "accepted", flush=True)
except Exception as exc:
    print(ruolo, "WRITE", "rejected:%s:%s" % (exc.__class__.__name__, exc), flush=True)
sys.stdout.flush()
time.sleep(30)
"""

os.makedirs(WORK, exist_ok=True)
db = os.path.join(WORK, "repro.db")
for suffix in ("", ".runtime.lock", "-wal", "-shm"):
    try:
        os.unlink(db + suffix)
    except FileNotFoundError:
        pass
env = dict(os.environ, PYTHONPATH=ROOT)
script = os.path.join(WORK, "runtime_child.py")
with open(script, "w") as fh:
    fh.write(RUNTIME)
lock = os.path.realpath(db) + ".runtime.lock"

a = subprocess.Popen([sys.executable, script, db, "A", "13"], cwd=WORK, env=env, stdout=subprocess.PIPE,
                     stderr=subprocess.DEVNULL, text=True)
print(a.stdout.readline().strip(), flush=True)
print("DRIVER lock exists:", os.path.exists(lock), flush=True)
os.unlink(lock)
if MODE == "replace":
    with open(lock, "w") as fh:
        fh.write("sostituito")
print("DRIVER lock", MODE, "-> exists now:", os.path.exists(lock), flush=True)
b = subprocess.Popen([sys.executable, script, db, "B", "1"], cwd=WORK, env=env, stdout=subprocess.PIPE,
                     stderr=subprocess.DEVNULL, text=True)
b_lines = [b.stdout.readline().strip()]
if "started=True" not in b_lines[0]:
    b_lines.append(b.stdout.readline().strip())
else:
    b_lines += [b.stdout.readline().strip(), b.stdout.readline().strip()]
for line in b_lines:
    print(line, flush=True)
for _ in range(2):
    print(a.stdout.readline().strip(), flush=True)
both = ("started=True" in b_lines[0])
print("RESULT two_runtimes_started_concurrently=%s" % both, flush=True)
for p in (a, b):
    p.kill()
    p.wait(15)
