from __future__ import annotations

import json
import re
import sys
from pathlib import Path


CRITICAL_FILES = {
    "core/trading_engine.py",
    "order_manager.py",
    "core/reconciliation_engine.py",
    "core/state_recovery.py",
    "database.py",
    "core/execution_guard.py",
    "core/risk_middleware.py",
    "core/runtime_controller.py",
    "core/money_management.py",
    "dutching.py",
    "pnl_engine.py",
    "telegram_listener.py",
    "copy_engine.py",
    "simulation_broker.py",
    "session_manager.py",
    "rate_limiter.py",
    "live_gate.py",
}

TASK_PATTERNS = (
    # Legacy/explicit format: [TASK: task_key]
    re.compile(r"\[TASK:\s*([^\]]+)\]", re.IGNORECASE),
    # Plain marker accepted by PR body, commit subject, and task marker files:
    # TASK: task_key
    # # TASK: task_key
    re.compile(r"(?im)^\s*(?:[#>*-]\s*)*TASK:\s*([a-z0-9][a-z0-9_-]*)\b"),
)
TASK_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
PLACEHOLDER_TASKS = {"todo"}
APPROVED_TASK_PREFIXES = (
    "audit_",
    "ci_",
    "commission_",
    "observability_phase",
    "runtime_",
    "betfair_",
    "hardening_",
    "workflow_hygiene_",
)
CLAUDE_BUG_TASK_PATTERN = re.compile(r"^claude_bug_pr\d+[a-z]_[a-z0-9_]+$")
EXACT_ALLOWED_TASKS = {"pr_guard"}


def load_json(path: str) -> dict | list:
    p = Path(path)
    if not p.exists():
        fail(f"Missing required file: {path}")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"Invalid JSON in {path}: {exc}")
    except Exception as exc:
        fail(f"Unable to read {path}: {exc}")


def fail(message: str) -> None:
    print(f"❌ {message}")
    raise SystemExit(1)


def warn(message: str) -> None:
    print(f"⚠️ {message}")


def info(message: str) -> None:
    print(f"ℹ️ {message}")


def normalize_changed_files(raw: list[dict] | list[str]) -> list[str]:
    changed_files: list[str] = []
    for item in raw:
        if isinstance(item, str):
            filename = item.strip()
            if filename:
                changed_files.append(filename)
            continue
        if isinstance(item, dict) and "filename" in item:
            filename = str(item["filename"]).strip()
            if filename:
                changed_files.append(filename)
    return changed_files


def extract_tasks(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for pattern in TASK_PATTERNS:
        for match in pattern.finditer(text or ""):
            task = (match.group(1) or "").strip()
            task_norm = task.lower()
            if task and task_norm not in seen:
                out.append(task)
                seen.add(task_norm)
    return out


def _is_placeholder_or_invalid(task: str) -> bool:
    norm = (task or "").strip().lower()
    if not norm:
        return True
    if norm in PLACEHOLDER_TASKS:
        return True
    if all(ch == "." for ch in norm):
        return True
    return TASK_KEY_PATTERN.fullmatch(norm) is None


def _is_approved_task_family(task: str) -> bool:
    if any(task.startswith(prefix) for prefix in APPROVED_TASK_PREFIXES):
        return True
    return CLAUDE_BUG_TASK_PATTERN.fullmatch(task) is not None


def _is_allowed_task_key(task: str) -> bool:
    return task in EXACT_ALLOWED_TASKS or _is_approved_task_family(task)


def _select_task_candidate(task_norm: str, allowed_tasks: set[str]) -> bool:
    if task_norm in allowed_tasks:
        return True
    if task_norm in EXACT_ALLOWED_TASKS:
        return True
    if _is_approved_task_family(task_norm):
        return True
    return False


def resolve_task(pr_meta: dict, changed_files: list[str], allowed_tasks: set[str]) -> tuple[str | None, str | None, list[tuple[str, str]], list[tuple[str, str]]]:
    sources = [
        ("pr_title", str(pr_meta.get("title", "") or "")),
        ("pr_body", str(pr_meta.get("body", "") or "")),
        ("branch", str(pr_meta.get("branch", "") or pr_meta.get("head_ref", "") or "")),
        ("latest_commit_message", str(pr_meta.get("latest_commit_message", "") or "")),
    ]
    unknown_candidates: list[tuple[str, str]] = []
    ignored_candidates: list[tuple[str, str]] = []
    for source_name, text in sources:
        for task in extract_tasks(text):
            task_norm = task.strip().lower()
            if _is_placeholder_or_invalid(task_norm):
                ignored_candidates.append((source_name, task_norm))
                continue
            if _select_task_candidate(task_norm, allowed_tasks):
                return task_norm, source_name, unknown_candidates, ignored_candidates
            unknown_candidates.append((source_name, task_norm))
    commit_messages = pr_meta.get("commit_messages")
    if isinstance(commit_messages, list):
        for msg in commit_messages:
            for task in extract_tasks(str(msg or "")):
                task_norm = task.strip().lower()
                if _is_placeholder_or_invalid(task_norm):
                    ignored_candidates.append(("commit_messages", task_norm))
                    continue
                if _select_task_candidate(task_norm, allowed_tasks):
                    return task_norm, "commit_messages", unknown_candidates, ignored_candidates
                unknown_candidates.append(("commit_messages", task_norm))

    task_path_hits = [
        path for path in changed_files
        if path.startswith("ops/tasks/") or path.startswith("ops/tasks_done/")
    ]
    if task_path_hits:
        return "task_file_change", "changed_task_files", unknown_candidates, ignored_candidates
    return None, None, unknown_candidates, ignored_candidates


def touches_critical_files(changed_files: list[str]) -> list[str]:
    return [path for path in changed_files if path in CRITICAL_FILES]


def validate_task_selection(task: str | None, critical_touched: list[str], allowed_tasks: set[str], unknown_candidates: list[tuple[str, str]]) -> None:
    if not task:
        if unknown_candidates:
            rendered = ", ".join(f"{source}:{value}" for source, value in unknown_candidates)
            fail(f"Unknown TASK tag candidates: {rendered}. Must be one of configured task keys.")
        fail("Missing TASK marker/source across title/body/branch/commit/task files. TASK validation is fail-closed.")
    if task == "task_file_change" and critical_touched:
        fail("PRs inferred from task-file changes must not also touch critical files.")
    if task != "task_file_change" and task not in allowed_tasks and task not in EXACT_ALLOWED_TASKS and not _is_approved_task_family(task):
        fail(f"Unknown TASK tag: {task}. Must be one of configured task keys.")


SCOPE_REGISTRY = ".guardrails/allowed_scope.json"
SCOPE_REGISTRY_BASE = "allowed_scope_base.json"

# `task_file_change` e' il task sintetico delle PR dedotte dai soli file-task.
# NON e' esente dall'invariante di scope, e la prima versione di questo guard
# sbagliava a esentarlo (P1 Codex, #470): `validate_task_selection` lo tiene
# fuori solo da CRITICAL_FILES, che NON contiene `.github/workflows/*` — quindi
# una PR con un file sotto `ops/tasks/` piu' un workflow-gate risolveva al task
# sintetico e saltava il controllo sull'INTERO diff. Proprio il caso che questo
# guard esiste per chiudere. Ora non ha entry nel registro, quindi non ha scope
# dichiarato, quindi blocca come qualunque altro task non registrato.
SYNTHETIC_TASK = "task_file_change"


def declared_scope_for(task: str | None, allowed_scope: dict) -> set[str] | None:
    """I `files` dichiarati dal task nel registro.

    ``None`` = il task non ha una entry (nessuno scope dichiarato). Una entry
    presente ma senza `files` vale come scope VUOTO, non come assenza: chi
    scrive una entry vuota non sta dichiarando "tutto", sta dichiarando niente.
    """
    if not task:
        return None
    tasks = allowed_scope.get("tasks")
    if not isinstance(tasks, dict):
        return None
    entry = tasks.get(task)
    if not isinstance(entry, dict):
        return None
    files = entry.get("files")
    if not isinstance(files, list):
        return set()
    return {str(f).strip() for f in files if str(f).strip()}


def scope_mismatch(declared: set[str], changed_files: list[str]) -> tuple[list[str], list[str]]:
    """(toccati non dichiarati, dichiarati non toccati).

    La prima lista e' la falla vera — la #463 ha toccato cosi' un workflow-gate
    e un file-policy fuori dal proprio scope. La seconda e' il cricchetto:
    scope riservato e non usato, che resta nel registro per dopo.
    """
    changed = {str(c).strip() for c in changed_files if str(c).strip()}
    return sorted(changed - declared), sorted(declared - changed)


def registry_tampering(task: str | None, scope: dict, base_scope: dict) -> list[str]:
    """Modifiche al registro FUORI dalla entry del task corrente.

    Una PR puo' registrare (e aggiornare) la PROPRIA chiave: e' quello che la
    policy le impone di fare. Non puo' toccare `default`, che vale per tutti,
    ne' le entry di altri task, che riscrivono cio' che era stato concesso
    altrove.
    """
    violazioni: list[str] = []
    if scope.get("default") != base_scope.get("default"):
        violazioni.append("la sezione `default` e' stata modificata")

    tasks = scope.get("tasks") if isinstance(scope.get("tasks"), dict) else {}
    base_tasks = base_scope.get("tasks") if isinstance(base_scope.get("tasks"), dict) else {}
    for key in sorted(set(tasks) | set(base_tasks)):
        if key == task:
            continue
        if key not in base_tasks:
            violazioni.append(f"chiave aggiunta oltre a quella del task: {key}")
        elif key not in tasks:
            violazioni.append(f"chiave rimossa: {key}")
        elif tasks[key] != base_tasks[key]:
            violazioni.append(f"entry modificata di un altro task: {key}")
    return violazioni


def validate_declared_scope(task: str | None, changed_files: list[str], allowed_scope: dict) -> None:
    """L'invariante: i `files` della task key coincidono col diff della PR.

    Scritto nella policy dalla #469, non applicato da nessuna parte fino a qui.
    Tutti e quattro i reviewer pagati l'hanno detto nello stesso giro: un
    vincolo affidato alla sola dichiarazione dell'agente, su un file che
    l'agente stesso scrive, non e' un vincolo.
    """
    declared = declared_scope_for(task, allowed_scope)
    if declared is None:
        extra = (
            f" Il task sintetico '{SYNTHETIC_TASK}' non fa eccezione: una PR "
            "dedotta dai file-task puo' toccare qualunque altro file, inclusi i "
            "workflow-gate, e deve dichiarare il proprio scope come ogni altra."
            if task == SYNTHETIC_TASK else ""
        )
        fail(
            f"TASK '{task}' non ha una entry in {SCOPE_REGISTRY}. "
            "Nessuno scope dichiarato = scope illimitato: registra la chiave "
            f"coi suoi `files` nello STESSO PR (fail-closed).{extra}"
        )

    fuori, non_toccati = scope_mismatch(declared, changed_files)
    if fuori:
        rendered = "\n".join(f"   + {p}" for p in fuori)
        fail(
            f"File toccati FUORI dallo scope dichiarato di '{task}':\n{rendered}\n"
            f"Dichiarali in {SCOPE_REGISTRY} o toglili dal diff. "
            "E' il caso della #463: un workflow-gate e un file-policy toccati "
            "fuori scope, mergiati senza che nessuno se ne accorgesse."
        )
    if non_toccati:
        rendered = "\n".join(f"   - {p}" for p in non_toccati)
        fail(
            f"File dichiarati da '{task}' ma NON toccati dalla PR:\n{rendered}\n"
            "Una dichiarazione piu' larga del diff e' scope riservato e non "
            f"usato: allinea `files` in {SCOPE_REGISTRY} al diff reale."
        )
    info(f"Scope invariant: `files` di '{task}' coincide col diff ({len(declared)} file)")


CAMPI_CHE_AUTORIZZANO = ("files", "description")


def _vista_autorizzante(entry: dict) -> tuple:
    """I soli campi della entry che concedono qualcosa.

    `files` normalizzato come in ``declared_scope_for`` — insieme di stringhe
    ripulite — cosi' un semplice riordino della lista non si spaccia per una
    modifica: e' lo stesso scope, scritto in un altro ordine.
    """
    files = entry.get("files")
    normalizzati = (
        frozenset(str(f).strip() for f in files if str(f).strip())
        if isinstance(files, list)
        else None
    )
    return (normalizzati, entry.get("description"))


def validate_own_entry_declared(task: str | None, scope: dict, base_scope: dict) -> None:
    """Riscrivere i `files` della PROPRIA chiave va DICHIARATO, non fatto e basta.

    `registry_tampering` salta la entry del task corrente — e deve, perche' la
    policy impone a ogni PR di registrarsi li'. Ma cosi', da sola, bastava
    mettere nel marker una chiave gia' esistente e sostituirne i `files` col
    proprio diff: l'invariante vedeva coincidenza esatta e il guard passava
    (P1 Codex, #470). La policy chiede gia' che un'estensione in corso d'opera
    sia dichiarata nella `description` della chiave; qui quella richiesta
    smette di essere prosa.

    Limite dichiarato: il guard verifica che la `description` sia CAMBIATA
    insieme ai `files`, non che dica il vero. Trasforma una riscrittura muta in
    una dichiarata — che e' ispezionabile — non in una impossibile.
    """
    if not task:
        return
    base_entry = (base_scope.get("tasks") or {}).get(task)
    head_entry = (scope.get("tasks") or {}).get(task)

    # La registrazione dev'essere MATERIALE, non apparente (P1 Codex, terzo
    # giro, #470). La versione precedente pretendeva che il path del registro
    # fosse nel diff: ma un `chmod +x .guardrails/allowed_scope.json` lo mette
    # nel `git diff --name-only` senza toccare un byte, e il riuso di una
    # chiave storica passava lo stesso — riprodotto su `exposure_total_clamp_m06`.
    # La domanda giusta non e' "il file e' nel diff" ma "questa chiave e' stata
    # registrata o aggiornata QUI".
    if base_entry == head_entry:
        fail(
            f"La entry di '{task}' e' identica a quella sul branch base: questa "
            "PR non ha registrato ne' aggiornato la propria task key, sta "
            "riusando un'autorizzazione concessa a un lavoro diverso. La policy "
            "chiede di registrare la chiave NELLO STESSO PR."
        )

    if not isinstance(base_entry, dict) or not isinstance(head_entry, dict):
        # Chiave nuova: e' una registrazione, non un allargamento. Niente da
        # dichiarare, perche' non c'e' nulla di precedente.
        return

    # Non basta che la entry sia DIVERSA: deve essere diversa in un campo che
    # porta autorizzazione (P1 Codex, settimo giro, #470). La entry ne ha altri
    # — `max_files`, che la policy dichiara esplicitamente NON applicato, e
    # `allow_tests` — e bumpare uno di quelli rendeva le entry diverse, faceva
    # passare il controllo d'identita', e subito dopo `files == files` faceva
    # tornare la funzione. Riprodotto: `max_files` da 3 a 4 su
    # `exposure_total_clamp_m06` bastava a riusare un'autorizzazione di mesi
    # prima per un lavoro tutt'altro, exit 0. Sbloccare un controllo con
    # l'unico campo che per ammissione non conta e' il caso peggiore.
    #
    # Il confronto sopra resta per il caso in cui le entry non siano dizionari:
    # per due dizionari questo lo sussume, perche' entry identiche hanno vista
    # identica.
    #
    # NON e' l'asse indecidibile dichiarato in #471 (la `description` verificata
    # come CAMBIATA e non come VERA): qui la proprieta' — *quale* campo e'
    # cambiato — e' decidibile, quindi si chiude invece di fermarsi.
    if _vista_autorizzante(base_entry) == _vista_autorizzante(head_entry):
        fail(
            f"La entry di '{task}' cambia, ma in nessun campo che porti "
            "autorizzazione (`files`, `description`): questa PR sta riusando "
            "un'autorizzazione concessa a un lavoro diverso. `max_files` e "
            "`allow_tests` non contano — il primo la policy lo dichiara non "
            "applicato. Registra o aggiorna la chiave NELLO STESSO PR."
        )
    if base_entry.get("files") == head_entry.get("files"):
        return
    if base_entry.get("description") != head_entry.get("description"):
        info(f"Scope di '{task}' esteso e dichiarato nella description: OK")
        return
    fail(
        f"I `files` di '{task}' cambiano rispetto al base, ma la sua "
        "`description` e' identica: un'estensione di scope non dichiarata. "
        "Scrivi nella description cosa hai aggiunto e perche', oppure usa una "
        "chiave nuova per un lavoro nuovo."
    )


def load_base_registry(changed_files: list[str]) -> dict:
    """Il registro come sta sul branch base. Sempre richiesto."""
    base_scope = load_json(SCOPE_REGISTRY_BASE)
    if not isinstance(base_scope, dict):
        fail(f"{SCOPE_REGISTRY_BASE} deve contenere un oggetto JSON")
    return base_scope


def validate_registry_untouched_elsewhere(task: str | None, base_scope: dict, allowed_scope: dict) -> None:
    """Il registro non si tocca fuori dalla entry del task corrente."""
    violazioni = registry_tampering(task, allowed_scope, base_scope)
    if violazioni:
        rendered = "\n".join(f"   - {v}" for v in violazioni)
        fail(
            f"Il registro e' stato modificato oltre la entry di '{task}':\n{rendered}\n"
            "Toccare `default` o le chiavi di altri task riscrive cio' che era "
            "stato concesso altrove: e' sempre need-manual, mai auto-merge."
        )
    info("Registro: modificata solo la entry del task corrente")


def main() -> int:
    pr_meta = load_json("pr_meta.json")
    pr_files_raw = load_json("pr_files_raw.json")

    if not isinstance(pr_meta, dict):
        fail("pr_meta.json must contain a JSON object")
    if not isinstance(pr_files_raw, list):
        fail("pr_files_raw.json must contain a JSON array")

    changed_files = normalize_changed_files(pr_files_raw)
    critical_touched = touches_critical_files(changed_files)

    allowed_scope = load_json(".guardrails/allowed_scope.json")
    allowed_tasks = set()
    if isinstance(allowed_scope, dict):
        tasks = allowed_scope.get("tasks")
        if isinstance(tasks, dict):
            allowed_tasks = {str(k) for k in tasks.keys()}
    task, task_source, unknown_candidates, ignored_candidates = resolve_task(pr_meta, changed_files, allowed_tasks)

    print("=" * 80)
    print("PR GUARD REPORT")
    print("=" * 80)
    info(f"Changed files: {len(changed_files)}")
    if changed_files:
        for path in changed_files:
            print(f" - {path}")

    print()
    if critical_touched:
        info("Critical files touched:")
        for path in critical_touched:
            print(f" - {path}")
    else:
        info("No critical files touched")

    print()
    validate_task_selection(task, critical_touched, allowed_tasks, unknown_candidates)
    info(f"TASK source found ({task_source}): {task}")

    print()
    scope_obj = allowed_scope if isinstance(allowed_scope, dict) else {}
    base_scope = load_base_registry(changed_files)
    validate_declared_scope(task, changed_files, scope_obj)
    validate_own_entry_declared(task, scope_obj, base_scope)
    validate_registry_untouched_elsewhere(task, base_scope, scope_obj)
    if ignored_candidates:
        warn(f"Ignored placeholder/invalid TASK markers: {ignored_candidates}")

    # Optional hygiene warnings
    if len(changed_files) > 25:
        warn(
            f"PR changes {len(changed_files)} files. "
            "Consider splitting if this was intended to be a focused PR."
        )

    if task and len(task) < 3:
        warn("TASK tag looks unusually short; verify it is meaningful.")

    print()
    print("✅ PR guard completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
