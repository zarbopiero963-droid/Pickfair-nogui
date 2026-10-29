# scripts/run_mutation_guardrails.py
"""Mutation testing reale sul codice sorgente (#525 / PF-CI-MUTATION-RECOVERY).

Il runner precedente scriveva un blocco ``_mutation_meta`` nei JSON di
``guardrails/mutations/`` e rilanciava l'intera suite: nessun file leggeva quei
valori, quindi il codice non veniva mai mutato e ogni "mutazione" sopravviveva.

Questo runner applica mutazioni concrete, elencate in
``guardrails/mutation_catalog.json``: ogni voce sostituisce una riga esatta di
un file sorgente con una variante dannosa e indica i test che devono
accorgersene. Regole:

- il repository non viene mai toccato: ogni mutazione gira in una copia
  isolata dei file tracciati, e il file mutato viene ripristinato e
  ricontrollato (sha256) prima della mutazione successiva;
- la riga originale deve comparire ESATTAMENTE una volta, altrimenti la
  mutazione e' NOT_APPLICABLE: il catalogo non puo' smettere in silenzio di
  colpire codice che e' stato cambiato;
- il sorgente mutato deve compilare (INVALID altrimenti): un errore di sintassi
  farebbe fallire i test e passerebbe per una mutazione intercettata;
- i test di ogni mutazione girano prima sul codice originale: se sono gia'
  rossi, l'esito e' INCONCLUSIVE;
- pytest rc 1 = KILLED, rc 0 = SURVIVED, ogni altro rc (collezione fallita,
  nessun test, interruzione) o timeout = INCONCLUSIVE;
- le mutazioni equivalenti sono dichiarate nel catalogo con la motivazione,
  restano nel report e non entrano nel punteggio;
- sandbox e file temporanei dei test (``TMPDIR``, quindi ``tmp_path``) stanno
  su tmpfs quando ``/dev/shm`` e' un tmpfs scrivibile con spazio sufficiente,
  altrimenti su disco, e il report dice quale (``tmp_root``). Ogni worker ha la
  sua directory temporanea, svuotata prima di ogni esecuzione di pytest; spazio
  esaurito o ``No space left on device`` nell'output non diventano mai KILLED.

Il punteggio si calcola solo su KILLED / (KILLED + SURVIVED). Il gate fallisce
se: nessuna mutazione valutabile, punteggio sotto ``--fail-under``, almeno un
esito INCONCLUSIVE, NOT_APPLICABLE o INVALID, oppure un'area obbligatoria del
catalogo (``required_areas``) senza nessuna mutazione valutabile.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

CATALOG_RELATIVE_PATH = Path("guardrails") / "mutation_catalog.json"

KILLED = "KILLED"
SURVIVED = "SURVIVED"
INCONCLUSIVE = "INCONCLUSIVE"
NOT_APPLICABLE = "NOT_APPLICABLE"
INVALID = "INVALID"
EQUIVALENT = "EQUIVALENT"

_REQUIRED_KEYS = ("id", "module", "area", "file", "original", "mutated", "tests", "rationale")
_COPY_IGNORE = {".git", "__pycache__", ".pytest_cache", ".venv", "venv", "node_modules"}

#: tmpfs per sandbox e file temporanei dei test (FULL_SUITE_PERFORMANCE_REMEDIATION, GO owner #562): sul disco
#: della lane i commit SQLite dei test (~9.500 `fdatasync` per `test_pr_trusted_dispatcher.py`) saturano il journal;
#: stesse mutazioni, stessi test, stessi criteri. Solo directory gia' montate come tmpfs: mai `/tmp` per ipotesi.
TMPFS_CANDIDATES = ("/dev/shm",)
#: spazio libero minimo per scegliere il tmpfs e per avviare ogni esecuzione di pytest
TMPFS_MIN_FREE_BYTES = 512 * 1024 * 1024
#: residui di run uccisi senza pulizia (kill del job): rimossi solo se piu' vecchi di cosi'
STALE_TMP_SECONDS = 3 * 3600
TMP_PREFIX = "pf_mutation_"
MOUNTS_FILE = "/proc/mounts"
#: impostata nei pytest lanciati dal runner: un runner annidato (i test del runner stesso, anche mutati) resta su
#: disco e non tocca mai la directory tmpfs condivisa, dove sta il workspace del runner che lo esegue
NESTED_ENV = "PF_MUTATION_NESTED"
#: solo righe di eccezione di pytest (`E   ...`): il testo nel sorgente stampato di un test non conta
_NO_SPACE_RE = re.compile(
    r"^E\s+(?:\w+\.)*(?:OSError|IOError).*\[Errno 28\]|^E\s+MemoryError\b"
    r"|^E\s+(?:\w+\.)*OperationalError: database or disk is full", re.MULTILINE)


class MutationRunnerError(Exception):
    pass


@dataclass
class MutationResult:
    mutation_id: str
    module: str
    area: str
    file: str
    status: str
    return_code: int | None = None
    duration_sec: float = 0.0
    detail: str = ""
    stdout_tail: str = ""
    tests: list[str] = field(default_factory=list)


def _tail(text: str | bytes | None, limit: int = 3000) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    return text if len(text) <= limit else text[-limit:]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_catalog(repo_root: Path) -> dict[str, Any]:
    path = repo_root / CATALOG_RELATIVE_PATH
    if not path.is_file():
        raise MutationRunnerError(f"Missing mutation catalog: {path}")
    with path.open("r", encoding="utf-8") as fh:
        catalog = json.load(fh)
    if not isinstance(catalog, dict) or not isinstance(catalog.get("mutations"), list):
        raise MutationRunnerError("mutation catalog must be an object with a 'mutations' list")
    required_areas = catalog.get("required_areas", [])
    if not isinstance(required_areas, list) or not all(isinstance(a, str) for a in required_areas):
        raise MutationRunnerError("'required_areas' must be a list of strings")
    seen: set[str] = set()
    for idx, entry in enumerate(catalog["mutations"]):
        if not isinstance(entry, dict):
            raise MutationRunnerError(f"mutation[{idx}] is not an object")
        missing = [k for k in _REQUIRED_KEYS if k not in entry]
        if missing:
            raise MutationRunnerError(f"mutation[{idx}] missing keys: {missing}")
        if entry["id"] in seen:
            raise MutationRunnerError(f"duplicate mutation id: {entry['id']}")
        seen.add(entry["id"])
        if entry["original"] == entry["mutated"]:
            raise MutationRunnerError(f"{entry['id']}: 'mutated' equals 'original'")
        if not isinstance(entry["tests"], list) or not entry["tests"]:
            raise MutationRunnerError(f"{entry['id']}: 'tests' must be a non-empty list")
        # i percorsi restano dentro la sandbox: niente assoluti, niente '..'
        for rel in [entry["file"], *(str(t).split("::")[0] for t in entry["tests"])]:
            rel_path = Path(str(rel))
            if not str(rel) or rel_path.is_absolute() or ".." in rel_path.parts:
                raise MutationRunnerError(f"{entry['id']}: path outside the repository: {rel!r}")
        if entry["area"] not in required_areas and not entry.get("equivalent"):
            # aree fuori elenco sono ammesse, ma vanno dichiarate: niente refusi silenziosi
            if entry["area"] not in catalog.get("optional_areas", []):
                raise MutationRunnerError(f"{entry['id']}: unknown area {entry['area']!r}")
    return catalog


def _tracked_files(repo_root: Path) -> list[str] | None:
    try:
        proc = subprocess.run(
            ["git", "ls-files", "-z"], cwd=str(repo_root), capture_output=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return [p for p in proc.stdout.decode("utf-8", errors="replace").split("\0") if p]


def build_sandbox(repo_root: Path, dest: Path) -> None:
    """Copia i file tracciati (o tutto, senza cache e .git) in ``dest``."""
    tracked = _tracked_files(repo_root)
    if tracked is None:
        shutil.copytree(
            repo_root, dest, ignore=shutil.ignore_patterns(*_COPY_IGNORE), dirs_exist_ok=True
        )
        return
    for rel in tracked:
        src = repo_root / rel
        if not src.is_file() or src.is_symlink():
            continue
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)


def _mount_fstype(path: Path, mounts_file: str | None = None) -> str | None:
    """Tipo di filesystem del mount point piu' lungo che contiene ``path`` (``/proc/mounts``)."""
    try:
        lines = Path(mounts_file or MOUNTS_FILE).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    best, fstype = "", None
    target = str(path)
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        mnt = parts[1].replace("\\040", " ")
        inside = target == mnt or target.startswith(mnt.rstrip("/") + "/")
        if inside and len(mnt) >= len(best):
            best, fstype = mnt, parts[2]
    return fstype


def _free_bytes(path: Path) -> int:
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


def choose_tmp_root(candidates: tuple | None = None, mounts_file: str | None = None,
                    min_free: int | None = None) -> tuple[str | None, str]:
    """Primo candidato che e' davvero un tmpfs, scrivibile e con spazio; altrimenti ``None`` (disco) e il motivo."""
    min_free = TMPFS_MIN_FREE_BYTES if min_free is None else min_free
    motivi = []
    for cand in TMPFS_CANDIDATES if candidates is None else candidates:
        path = Path(cand)
        try:
            if not path.is_dir():
                motivi.append(f"{cand}: assente")
                continue
            real = path.resolve()
            fstype = _mount_fstype(real, mounts_file)
            if fstype != "tmpfs":
                motivi.append(f"{cand}: filesystem {fstype or 'sconosciuto'}, non tmpfs")
                continue
            if not os.access(real, os.W_OK | os.X_OK):
                motivi.append(f"{cand}: non scrivibile")
                continue
            free = _free_bytes(real)
        except OSError as exc:
            motivi.append(f"{cand}: {type(exc).__name__}")
            continue
        if free < min_free:
            motivi.append(f"{cand}: {free // 2**20} MiB liberi, minimo {min_free // 2**20}")
            continue
        return str(real), f"tmpfs {real} ({free // 2**20} MiB liberi)"
    return None, "disco (" + "; ".join(motivi or ["nessun candidato"]) + ")"


def clean_stale_tmp(root: str, max_age: int | None = None, now: float | None = None) -> list[str]:
    """Rimuove le directory ``pf_mutation_*`` di questo utente piu' vecchie di ``max_age`` (run uccisi)."""
    removed = []
    max_age = STALE_TMP_SECONDS if max_age is None else max_age
    now = time.time() if now is None else now
    for entry in Path(root).glob(TMP_PREFIX + "*"):
        try:
            st = entry.lstat()
            if not entry.is_dir() or entry.is_symlink() or st.st_uid != os.getuid() or now - st.st_mtime <= max_age:
                continue
        except OSError:
            continue
        shutil.rmtree(entry, ignore_errors=True)
        removed.append(entry.name)
    return removed


def worker_tmp_dir(sandbox: Path) -> Path:
    """``TMPDIR`` dei test di un worker: accanto alla sua sandbox, mai condiviso con un altro worker."""
    return sandbox.parent / f"{sandbox.name}-tmp"


def _reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    if path.exists():
        raise MutationRunnerError(f"temporary directory not cleaned: {path}")
    path.mkdir(parents=True)


def _kill_process_tree(proc: subprocess.Popen) -> None:
    """Termina pytest e cio' che ha lanciato. Su POSIX l'intero gruppo di
    processi; dove `killpg` non esiste (Windows) `taskkill /T`, e in ultima
    istanza il solo processo."""
    killpg = getattr(os, "killpg", None)
    if killpg is not None and hasattr(signal, "SIGKILL"):
        try:
            killpg(proc.pid, signal.SIGKILL)
            return
        except (ProcessLookupError, PermissionError):
            pass
    elif os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True, check=False
        )
    try:
        proc.kill()
    except ProcessLookupError:
        pass


def _run_pytest(sandbox: Path, tests: list[str], pytest_args: list[str], timeout_sec: int):
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    # file temporanei dei test (tmp_path, SQLite) nella directory del worker, svuotata a ogni esecuzione: niente
    # residui fra una mutazione e l'altra, niente contaminazioni fra worker
    test_tmp = worker_tmp_dir(sandbox)
    _reset_dir(test_tmp)
    if _free_bytes(test_tmp) < TMPFS_MIN_FREE_BYTES:
        raise MutationRunnerError(f"temporary space below {TMPFS_MIN_FREE_BYTES // 2**20} MiB in {test_tmp}")
    env["TMPDIR"] = str(test_tmp)
    env[NESTED_ENV] = "1"
    env["PYTHONPATH"] = str(sandbox) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    cmd = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *tests, *pytest_args]
    start = time.monotonic()
    # gruppo di processi proprio: al timeout si termina anche cio' che i test
    # hanno lanciato, che altrimenti sopravviverebbe alla sandbox
    proc = subprocess.Popen(
        cmd, cwd=str(sandbox), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, start_new_session=True,
    )
    try:
        out, _ = proc.communicate(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc)
        out, _ = proc.communicate()
        return None, time.monotonic() - start, _tail(out)
    return proc.returncode, time.monotonic() - start, _tail(out)


def _status_from_rc(rc: int | None) -> str:
    if rc == 1:
        return KILLED
    if rc == 0:
        return SURVIVED
    return INCONCLUSIVE


def run_mutation(
    entry: dict[str, Any],
    sandbox: Path,
    baseline: dict[tuple[str, ...], int | None],
    pytest_args: list[str],
    timeout_sec: int,
) -> MutationResult:
    tests = list(entry["tests"])
    result = MutationResult(
        mutation_id=entry["id"], module=entry["module"], area=entry["area"],
        file=entry["file"], status=INCONCLUSIVE, tests=tests,
    )
    target = sandbox / entry["file"]
    if not target.is_file():
        result.status = NOT_APPLICABLE
        result.detail = "target file not found"
        return result
    original_bytes = target.read_bytes()
    original_hash = hashlib.sha256(original_bytes).hexdigest()
    source = original_bytes.decode("utf-8")
    occurrences = source.count(entry["original"])
    if occurrences != 1:
        result.status = NOT_APPLICABLE
        result.detail = f"original snippet found {occurrences} times (expected exactly 1)"
        return result

    if entry.get("equivalent"):
        result.status = EQUIVALENT
        result.detail = str(entry["equivalent"])
        return result

    mutated_source = source.replace(entry["original"], entry["mutated"], 1)
    try:
        compile(mutated_source, entry["file"], "exec")
    except SyntaxError as exc:
        result.status = INVALID
        result.detail = f"mutated source does not compile: {exc}"
        return result

    key = tuple(tests)
    if key not in baseline:
        rc, _, out = _run_pytest(sandbox, tests, pytest_args, timeout_sec)
        baseline[key] = rc
        if rc != 0:
            print(f"     baseline NOT green for {tests} (rc={rc})\n{_indent(out[-1500:], 6)}")
    if baseline[key] != 0:
        result.status = INCONCLUSIVE
        result.detail = f"baseline tests not green on original code (rc={baseline[key]})"
        return result

    try:
        target.write_text(mutated_source, encoding="utf-8")
        rc, duration, out = _run_pytest(sandbox, tests, pytest_args, timeout_sec)
    finally:
        target.write_bytes(original_bytes)
    if _sha256(target) != original_hash:
        raise MutationRunnerError(f"{entry['id']}: failed to restore {entry['file']} in sandbox")

    result.return_code = rc
    result.duration_sec = round(duration, 2)
    result.stdout_tail = out
    result.status = _status_from_rc(rc)
    if result.status == KILLED and _NO_SPACE_RE.search(out or ""):
        # un test caduto per spazio o memoria esauriti non ha intercettato la mutazione
        result.status = INCONCLUSIVE
        result.detail = "test failed for exhausted temporary space or memory, not for the mutation"
        return result
    if rc is None:
        result.detail = f"timeout after {timeout_sec}s"
    elif result.status == INCONCLUSIVE:
        result.detail = f"pytest rc={rc} (not a test failure)"
    return result


def summarize(results: list[MutationResult], required_areas: list[str], fail_under: float) -> dict[str, Any]:
    counts = {s: 0 for s in (KILLED, SURVIVED, INCONCLUSIVE, NOT_APPLICABLE, INVALID, EQUIVALENT)}
    for r in results:
        counts[r.status] += 1
    valid = counts[KILLED] + counts[SURVIVED]
    score = round(counts[KILLED] / valid * 100, 2) if valid else 0.0

    areas: dict[str, dict[str, int]] = {}
    for r in results:
        bucket = areas.setdefault(r.area, {s: 0 for s in counts})
        bucket[r.status] += 1
    uncovered = [
        a for a in required_areas
        if (areas.get(a, {}).get(KILLED, 0) + areas.get(a, {}).get(SURVIVED, 0)) == 0
    ]

    failures: list[str] = []
    if valid == 0:
        failures.append("no valid (KILLED/SURVIVED) mutation executed")
    if score < fail_under:
        failures.append(f"mutation score {score}% below threshold {fail_under}%")
    for status in (INCONCLUSIVE, NOT_APPLICABLE, INVALID):
        if counts[status]:
            failures.append(f"{counts[status]} mutation(s) {status}")
    if uncovered:
        failures.append(f"required areas without a valid mutation: {uncovered}")

    return {
        "total": len(results),
        "counts": counts,
        "valid": valid,
        "score": score,
        "fail_under": fail_under,
        "areas": areas,
        "uncovered_required_areas": uncovered,
        "gate_passed": not failures,
        "gate_failures": failures,
    }


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())


def _print_report(results: list[MutationResult], summary: dict[str, Any]) -> None:
    print("=" * 80)
    print("MUTATION GUARDRAILS REPORT (source-level)")
    print("=" * 80)
    c = summary["counts"]
    print(
        f"Total: {summary['total']} | Valid: {summary['valid']} | Killed: {c[KILLED]} | "
        f"Survived: {c[SURVIVED]} | Inconclusive: {c[INCONCLUSIVE]} | "
        f"Not applicable: {c[NOT_APPLICABLE]} | Invalid: {c[INVALID]} | "
        f"Equivalent: {c[EQUIVALENT]} | Score: {summary['score']}%"
    )
    for area, bucket in sorted(summary["areas"].items()):
        print(f"  [{area}] " + " ".join(f"{k.lower()}={v}" for k, v in bucket.items() if v))
    for r in results:
        if r.status != KILLED:
            print(f"  - {r.status}: {r.mutation_id} ({r.file}) {r.detail}")
            if r.status == SURVIVED and r.stdout_tail:
                print(_indent(r.stdout_tail[-800:], 6))
    print("GATE: " + ("PASS" if summary["gate_passed"] else "FAIL -> " + "; ".join(summary["gate_failures"])))


def _write_report(path: Path, results: list[MutationResult], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summary, "results": [asdict(r) for r in results]}
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run source-level mutation guardrails against pytest.")
    parser.add_argument("--repo-root", default=".", help="Repository root (default: current dir)")
    parser.add_argument(
        "--module", action="append", default=[],
        help="Run only mutations of these module(s), e.g. --module core.trading_engine",
    )
    parser.add_argument("--output", default="mutation_guardrails_report.json", help="JSON report path")
    parser.add_argument("--timeout-sec", type=int, default=180, help="Per-mutation pytest timeout (s)")
    parser.add_argument(
        "--pytest-arg", action="append", default=[], help="Extra argument passed to pytest (repeatable)"
    )
    parser.add_argument("--fail-under", type=float, default=80.0, help="Minimum mutation score (percent)")
    parser.add_argument(
        "--jobs", type=_jobs, default=1,
        help="Mutations run in parallel, one sandbox per worker (default 1: sequential)",
    )
    parser.add_argument(
        "--tmpfs", choices=("auto", "off"), default="auto",
        help="auto: sandbox e file temporanei dei test su /dev/shm se e' un tmpfs utilizzabile; off: su disco",
    )
    return parser.parse_args(argv)


def _jobs(value: str) -> int:
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError("--jobs must be >= 1")
    return n


def _run_all(entries: list[dict[str, Any]], repo_root: Path, tmp: str, jobs: int,
             pytest_args: list[str], timeout_sec: int) -> list[MutationResult]:
    """Esegue le mutazioni, in parallelo con `jobs` > 1 (PFC-003, M157).

    Ogni worker ha la SUA sandbox: una mutazione modifica un file e lo
    ripristina, quindi due worker sulla stessa copia si vedrebbero a vicenda il
    codice mutato. I risultati tornano nell'ordine del catalogo e l'output di
    ogni mutazione si stampa in un blocco solo, cosi' il report e' identico al
    run sequenziale.
    """
    results: list[MutationResult | None] = [None] * len(entries)
    baseline: dict[tuple[str, ...], int | None] = {}
    lock = threading.Lock()
    prossima = iter(range(len(entries)))
    errori: list[BaseException] = []

    def worker(numero: int) -> None:
        sandbox = Path(tmp) / f"repo{numero}"
        build_sandbox(repo_root, sandbox)
        mia_baseline: dict[tuple[str, ...], int | None] = {}
        while True:
            with lock:
                if errori:
                    return
                indice = next(prossima, None)
                mia_baseline.update(baseline)
            if indice is None:
                return
            entry = entries[indice]
            try:
                result = run_mutation(entry, sandbox, mia_baseline, pytest_args, timeout_sec)
            except BaseException as exc:  # il primo errore ferma tutti i worker
                with lock:
                    errori.append(exc)
                return
            with lock:
                baseline.update(mia_baseline)
                results[indice] = result
                print(f"-> {entry['id']} [{entry['area']}] {entry['file']}\n"
                      f"   {result.status} (rc={result.return_code}, {result.duration_sec}s) {result.detail}",
                      flush=True)

    threads = [threading.Thread(target=worker, args=(n,), daemon=True) for n in range(min(jobs, len(entries)))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errori:
        raise errori[0]
    if any(r is None for r in results):
        raise MutationRunnerError("mutation run incomplete: some mutations were not executed")
    return [r for r in results if r is not None]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    output_path = Path(args.output).resolve()
    # un report di un run precedente non deve sopravvivere a un run interrotto
    _write_report(output_path, [], {"gate_passed": False, "gate_failures": ["run not completed"]})
    catalog = load_catalog(repo_root)
    entries = catalog["mutations"]
    required_areas = list(catalog.get("required_areas", []))
    if args.module:
        wanted = set(args.module)
        entries = [e for e in entries if e["module"] in wanted]
        # con un filtro per modulo l'obbligo di copertura vale solo per le aree selezionate
        required_areas = sorted({e["area"] for e in entries if e["area"] in required_areas})
    if not entries:
        print("No mutations selected: refusing to report a PASS without executing anything.", file=sys.stderr)
        return 2

    watched = {e["file"] for e in entries}
    before = {f: _sha256(repo_root / f) for f in watched if (repo_root / f).is_file()}

    if args.tmpfs == "off":
        tmp_root, tmp_reason = None, "disco (--tmpfs off)"
    elif os.environ.get(NESTED_ENV):
        tmp_root, tmp_reason = None, "disco (runner annidato in una mutazione)"
    else:
        tmp_root, tmp_reason = choose_tmp_root()
    if tmp_root:
        stale = clean_stale_tmp(tmp_root)
        if stale:
            print(f"Removed stale mutation workspaces: {', '.join(stale)}")
    print(f"Temporary workspaces: {tmp_reason}", flush=True)
    with tempfile.TemporaryDirectory(prefix=TMP_PREFIX, dir=tmp_root, ignore_cleanup_errors=True) as tmp:
        results = _run_all(entries, repo_root, tmp, args.jobs, args.pytest_arg, args.timeout_sec)
    if tmp_root and Path(tmp).exists():
        raise MutationRunnerError(f"mutation workspace not removed from tmpfs: {tmp}")

    after = {f: _sha256(repo_root / f) for f in before}
    if after != before:
        raise MutationRunnerError("repository files changed during the mutation run")

    summary = summarize(results, required_areas, args.fail_under)
    summary["tmp_root"] = tmp_reason
    # Moduli con una specifica in guardrails/mutations/ ma nessuna mutazione
    # reale nel catalogo: elencati in ogni report, mai esclusi in silenzio.
    spec_modules = set()
    for spec in sorted((repo_root / "guardrails" / "mutations").glob("*.json")):
        try:
            spec_modules.add(str(json.loads(spec.read_text(encoding="utf-8")).get("module") or spec.stem))
        except (OSError, ValueError):
            spec_modules.add(spec.stem)
    summary["spec_modules_without_mutations"] = sorted(spec_modules - {e["module"] for e in catalog["mutations"]})
    _print_report(results, summary)
    if summary["spec_modules_without_mutations"]:
        print("Modules with a spec but no source mutation (tracked gap): "
              + ", ".join(summary["spec_modules_without_mutations"]))
    _write_report(output_path, results, summary)
    print(f"JSON report written to: {output_path}")
    return 0 if summary["gate_passed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MutationRunnerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
