"""Esecutore della campagna: lancia child_campaign.py per ogni scenario (fase 0, e fase 1 dopo crash),
in parallelo, con HOME isolata per scenario (DB SQLite e stato SIM persistono fra le fasi).
Uso: python run.py <scenarios.jsonl|dir> <outdir> [--workers N] [--limit N] [--ids S1,S2]
"""
import argparse, glob, json, os, shutil, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
PY = "/tmp/claude-0/-home-user-Pickfair-nogui/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/venv312/bin/python"
NET = "/tmp/claude-0/-home-user-Pickfair-nogui/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/netguard"


def load(src):
    if os.path.isdir(src):
        return [json.load(open(p)) for p in sorted(glob.glob(os.path.join(src, "*.json")))]
    return [json.loads(l) for l in open(src) if l.strip()]


TIMEOUT = 240


def run_one(spec, outdir, tree):
    sdir = os.path.join(outdir, spec["id"])
    shutil.rmtree(sdir, ignore_errors=True)           # scenario interrotto: si riparte da zero, mai a meta'
    os.makedirs(sdir, exist_ok=True)
    spath = os.path.join(sdir, "spec.json")
    json.dump(spec, open(spath, "w"))
    home = os.path.join(sdir, "home")
    rec = {"id": spec["id"], "phases": []}
    for ph in range(len(spec["phases"])):
        out = os.path.join(sdir, f"out{ph}.json")
        t = time.time()
        try:
            p = subprocess.run([PY, os.path.join(HERE, "child_campaign.py"), tree, out, spath, home, str(ph)],
                               env=dict(os.environ, PYTHONPATH=NET), capture_output=True, text=True, timeout=TIMEOUT)
            rc = p.returncode
            if rc not in (0, 1) or not os.path.exists(out):
                open(os.path.join(sdir, f"stderr{ph}.txt"), "w").write(p.stderr[-6000:])
        except subprocess.TimeoutExpired:
            rc = "TIMEOUT"
        rec["phases"].append({"phase": ph, "rc": rc, "sec": round(time.time() - t, 1), "out": os.path.exists(out)})
    json.dump(rec, open(os.path.join(sdir, "run.json"), "w"))
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src"); ap.add_argument("outdir")
    ap.add_argument("--tree", default=os.path.join(HERE, "tree"))
    ap.add_argument("--workers", type=int, default=4); ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--ids", default="")
    ap.add_argument("--timeout", type=int, default=240)
    a = ap.parse_args()
    global TIMEOUT
    TIMEOUT = a.timeout
    scen = load(a.src)
    if a.ids:
        want = set(a.ids.split(",")); scen = [s for s in scen if s["id"] in want]
    if a.limit:
        scen = scen[:a.limit]
    a.outdir = os.path.abspath(a.outdir); a.tree = os.path.abspath(a.tree)
    os.makedirs(a.outdir, exist_ok=True)
    done = {os.path.basename(os.path.dirname(p)) for p in glob.glob(os.path.join(a.outdir, "*", "run.json"))}
    todo = [s for s in scen if s["id"] not in done]
    print(json.dumps({"total": len(scen), "already_done": len(scen) - len(todo), "todo": len(todo)}), flush=True)
    t0 = time.time(); n = 0
    with ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(run_one, s, a.outdir, a.tree) for s in todo]
        for f in as_completed(futs):
            n += 1
            r = f.result()
            if n % 25 == 0 or any(p["rc"] not in (0, 1) for p in r["phases"]):
                print(f"[{n}/{len(todo)}] {time.time() - t0:.0f}s last={r['id']} {r['phases']}", flush=True)
    print("RUN_DONE", len(todo), f"{time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
