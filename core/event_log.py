"""PR-14: log persistente + contatori di stato (logica pura, testabile).

Cuore di #11 (l'utente capisce sempre cosa succede), fuori dalla GUI:

- log **persistente** in `<config_dir>/logs/bridge-YYYY-MM-DD.log` (append): lo
  storico sopravvive a chiusura/riavvio dell'app;
- **livelli** INFO / WARNING / ERROR / SIGNAL e filtro per livello;
- **contatori** di stato (messaggi/segnali/errori + ultimi valori) per la dashboard.

Nessuna dipendenza da GUI/Telegram/CSV. La scrittura è best-effort: un problema
di filesystem non deve mai far crashare il bridge (si perde solo lo storico su
disco, non il funzionamento). NB: `append_entry` applica `redact_secrets` come
**difesa in profondità** (audit #259 D4), oltre alla redazione che i chiamanti
(`App._log`, `settings_validation`) già fanno a monte: nessun caller diretto può
scrivere un segreto in chiaro nel log persistente.
"""

import functools
import os
import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from urllib.parse import quote

from . import config_store

LEVELS = ("INFO", "WARNING", "ERROR", "SIGNAL")
DEFAULT_LEVEL = "INFO"


def normalize_level(level) -> str:
    """Normalizza il livello a uno di LEVELS; ignoto/mancante → DEFAULT_LEVEL."""
    lvl = str(level or "").strip().upper()
    return lvl if lvl in LEVELS else DEFAULT_LEVEL


# Pattern di un bot token Telegram: <id numerico>:<~35 caratteri>. Va mascherato
# ovunque possa finire in un log (es. un'eccezione che incorpora il token), per
# rispettare l'invariante "mai token in chiaro nei log".
_TELEGRAM_TOKEN_RE = re.compile(r"\d{6,}:[A-Za-z0-9_-]{20,}")
_REDACTED = "[REDACTED_TOKEN]"

# Registro di segreti ESATTI da mascherare per-literal, OLTRE alla regex (issue #184 M7).
# La regex copre solo lo shape CANONICO del token (<id>:<20+ char>): una forma non-standard
# (porzione segreta < 20 char, URL-encoded, spezzata su righe) le sfuggirebbe. Registrando il
# token VIVO da config (`register_secret`), lo si maschera comunque ovunque compaia, in
# qualunque forma. Soglia minima di lunghezza per non mascherare frammenti banali. Thread-safe
# (additivo). Mirror della stessa idea in `betfair/log_safety`.
_MIN_SECRET_LEN = 8
_secret_lock = threading.Lock()
_secret_literals: set = set()


def register_secret(value) -> bool:
    """Registra un valore segreto (es. il bot token da config) da mascherare per-literal in
    `redact_secrets`, ovunque compaia e in QUALSIASI forma. Ritorna `True` se registrato.
    Valori vuoti/non-stringa o troppo corti (< 8 char) sono ignorati (`False`): meglio non
    mascherare un frammento banale che inquinerebbe i log."""
    if not value:
        return False
    s = str(value)
    if len(s) < _MIN_SECRET_LEN:
        return False
    with _secret_lock:
        _secret_literals.add(s)
    return True


def unregister_secret(value) -> None:
    """Rimuove un segreto dal registro (es. quando il token cambia)."""
    if not value:
        return
    with _secret_lock:
        _secret_literals.discard(str(value))
    # Non trattenere il segreto nella cache delle regex CR/LF oltre la sua registrazione.
    _crlf_tolerant_re.cache_clear()


def clear_secrets() -> None:
    """Svuota il registro dei segreti (utile nei test e in un reset completo)."""
    with _secret_lock:
        _secret_literals.clear()
    _crlf_tolerant_re.cache_clear()    # nessun segreto deve sopravvivere nella cache regex


def _secret_forms(secret: str):
    """Forme derivate di un segreto da mascherare, OLTRE al literal grezzo: la forma
    **URL-encoded** (`:`→`%3A`, ecc.), realistica quando il token finisce in un URL/HTTP (es. il
    path `…/bot<token>/…` o una query) dentro il testo di un'eccezione o nella diagnostica.

    Senza questa derivazione, registrare il token GREZZO (come fa `app._register_secret_token`)
    non maschererebbe la sua forma encoded, che né la regex né il match grezzo riconoscono
    (Codex #184 M7). Coprire OGNI possibile re-encoding/normalizzazione è impossibile: si coprono
    le forme realistiche; resta il limite residuo documentato in `redact_secrets`."""
    forms = {secret}
    enc = quote(secret, safe="")
    if enc != secret:
        forms.add(enc)
    return forms


@functools.lru_cache(maxsize=256)
def _crlf_tolerant_re(sec: str):
    """Regex che matcha `sec` anche se spezzato da CR/LF tra i caratteri (#203).

    Un token registrato può finire **wrappato** su più righe in un log/traceback
    (`123456789:\\nSecret…`): un `str.replace` esatto non lo riconoscerebbe e lo scriverebbe in
    chiaro. Inserendo `[\\r\\n]*` tra ogni carattere si maschera anche quella forma, restando
    retro-compatibile con le occorrenze su singola riga (zero newline → match identico). Si
    tollerano solo CR/LF (non spazi generici) per non sovra-redarre testo non correlato. Cache
    per non ricompilare lo stesso pattern a ogni redazione."""
    return re.compile(r"[\r\n]*".join(re.escape(ch) for ch in sec))


def redact_secrets(text: str) -> str:
    """Maschera i segreti nei log (GUI e file): un token incorporato per sbaglio (es. nel testo
    di un'eccezione) non viene mai scritto in chiaro.

    Due livelli (issue #184 M7):
    1. **regex** sullo shape CANONICO del bot token Telegram (`<id>:<20+ char>`) — euristica che
       intercetta anche token NON registrati (es. di un'altra fonte);
    2. **per-literal** dei segreti registrati con `register_secret` (es. il token VIVO da
       config), mascherati nelle loro forme derivate (`_secret_forms`: grezzo + URL-encoded) E
       tolleranti ai CR/LF inseriti tra i caratteri (`_crlf_tolerant_re`, #203) — così è coperta
       sia la forma encoded che la regex non riconosce (registrando solo il token GREZZO, Codex
       #184 M7), sia il token **spezzato su più righe** dal wrapping di un log/traceback.

    Limite residuo onesto: un segreto MAI registrato, o in una forma derivata non prevista
    (es. doppia codifica, separatori diversi da CR/LF), può ancora sfuggire; per questo il token
    di config va registrato (lo fa `app` a load/save)."""
    s = _TELEGRAM_TOKEN_RE.sub(_REDACTED, str(text or ""))
    # Espande ogni literal registrato nelle sue forme derivate (grezzo + URL-encoded), poi
    # sostituisce le più LUNGHE prima: evita che un segreto contenuto in un altro venga
    # mascherato a metà lasciando un frammento dell'altro in chiaro. Ogni forma è matchata in
    # modo CR/LF-tollerante così un token wrappato su più righe non sfugge.
    with _secret_lock:
        literals = list(_secret_literals)
    forms = set()
    for sec in literals:
        forms.update(_secret_forms(sec))
    for sec in sorted(forms, key=len, reverse=True):
        if not sec:
            continue
        s = _crlf_tolerant_re(sec).sub(_REDACTED, s)
    return s


def _secret_spans(text: str):
    """Span `(start, end)` di tutti i segreti in `text`: regex del token canonico + literal
    registrati nelle loro forme derivate (`_secret_forms`). Usato da `redact_preview` per sapere
    se un segreto attraversa il confine del budget.

    I literal sono cercati con lo STESSO match CR/LF-tollerante di `redact_secrets`
    (`_crlf_tolerant_re`, #203 Codex): se gli span usassero il `find` esatto mentre la redazione
    è CR/LF-tollerante, `redact_preview` taglierebbe a metà un token wrappato — il prefisso
    raggiungerebbe la sostituzione senza il resto del token e un frammento finirebbe nell'anteprima."""
    spans = []
    for m in _TELEGRAM_TOKEN_RE.finditer(text):
        spans.append((m.start(), m.end()))
    with _secret_lock:
        literals = list(_secret_literals)
    forms = set()
    for sec in literals:
        forms.update(_secret_forms(sec))
    for sec in forms:
        if not sec:
            continue
        for m in _crlf_tolerant_re(sec).finditer(text):
            spans.append((m.start(), m.end()))
    return spans


def redact_preview(text: str, budget: int) -> str:
    """Come `redact_secrets`, ma rivela al più `budget` caratteri **grezzi** di `text` (issue #184
    M8, P2 Codex). Serve all'anteprima privacy di `log_privacy`: redarre l'intera riga PRIMA di
    tagliare trascinerebbe nell'anteprima testo oltre il confine originale dei `budget` char
    (un token lungo si accorcia a `[REDACTED_TOKEN]` e fa salire del testo che la privacy non
    doveva mostrare); tagliare PRIMA di redarre lascerebbe un token a metà sul confine.

    Regola: un segreto che **inizia entro** il budget è mascherato per intero anche se lo sfora,
    ma **nessun contenuto non-segreto che inizia oltre** il budget viene mostrato."""
    s = str(text or "")
    budget = max(0, int(budget))
    cut = min(budget, len(s))
    # Estende il taglio per includere INTERAMENTE ogni segreto che attraversa il confine. Fixpoint:
    # estendere può portare il confine dentro lo span di un altro segreto sovrapposto. Si estende
    # solo fino alla FINE dei segreti coinvolti → nessun contenuto non-segreto oltre il budget.
    changed = True
    while changed:
        changed = False
        for start, end in _secret_spans(s):
            if start < cut < end:
                cut = end
                changed = True
    return redact_secrets(s[:cut])


# Marker emoji con cui la GUI prefissa i messaggi → livello di log. Serve a
# derivare automaticamente il livello quando il chiamante non lo passa, così lo
# storico persistente distingue errori/segnali e `filter_by_level` è utile (#11).
_MARKER_LEVEL = (
    ("❌", "ERROR"),
    ("⚠️", "WARNING"),
    ("📱", "SIGNAL"),
)


def classify(message: str) -> str:
    """Deriva il livello dal marker iniziale del messaggio (❌→ERROR, ⚠️→WARNING,
    📱→SIGNAL); altrimenti INFO. Permette di classificare lo storico senza dover
    annotare a mano ogni punto di log della GUI."""
    text = str(message or "").lstrip()
    for marker, level in _MARKER_LEVEL:
        if text.startswith(marker):
            return level
    return DEFAULT_LEVEL


def format_entry(message: str, level=DEFAULT_LEVEL, when: datetime = None) -> str:
    """Riga di log formattata: ``[HH:MM:SS] [LEVEL] messaggio``.

    Il messaggio è ridotto a **una sola riga fisica**: CR/LF vengono sostituiti da
    uno spazio. Senza questo, un messaggio multiriga (es. un EventName estratto su
    più righe da un parser custom) spezzerebbe la entry e `read_entries` vedrebbe
    le continuazioni come entry separate, o un messaggio potrebbe forgiare un
    header di livello falso."""
    when = when or datetime.now()
    safe = str(message).replace("\r", " ").replace("\n", " ")
    return f"[{when:%H:%M:%S}] [{normalize_level(level)}] {safe}"


def log_dir(base: str = None) -> str:
    """Cartella dei log: `<config_dir>/logs` (o `<base>/logs` nei test)."""
    base = base if base is not None else config_store.config_dir()
    return os.path.join(base, "logs")


def log_path(base: str = None, when: datetime = None) -> str:
    """Percorso del file di log del giorno (`bridge-YYYY-MM-DD.log`)."""
    when = when or datetime.now()
    return os.path.join(log_dir(base), f"bridge-{when:%Y-%m-%d}.log")


def append_entry(message: str, level=DEFAULT_LEVEL, *, base: str = None,
                 when: datetime = None) -> str:
    """Appende una riga formattata al log del giorno (best-effort, non solleva).
    Ritorna la riga (anche se la scrittura su disco fallisce).

    Difesa in profondità (audit #259 D4): il messaggio è ripassato da
    `redact_secrets` **anche qui**, non solo nel chiamante. `App._log` già redige a
    monte e la ri-redazione è idempotente (un token già `[REDACTED_TOKEN]` non
    contiene più il segreto), ma così un eventuale caller diretto di `append_entry`
    non può scrivere in chiaro un token nel log persistente."""
    when = when or datetime.now()
    line = format_entry(redact_secrets(message), level, when)
    try:
        os.makedirs(log_dir(base), exist_ok=True)
        with open(log_path(base, when), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass
    return line


def read_entries(base: str = None, when: datetime = None) -> list:
    """Righe storiche del log del giorno (lista, vuota se il file non esiste).
    Permette di rileggere lo storico dopo un riavvio dell'app."""
    try:
        with open(log_path(base, when), "r", encoding="utf-8") as f:
            return [line.rstrip("\n") for line in f]
    except OSError:
        return []


# ── Retention: pulizia automatica dei log vecchi (#11) ──────────────────────
# Opzioni offerte dalla GUI (giorni). 0 = "Mai" (conserva tutto).
RETENTION_OPTIONS = (5, 15, 30)

# Nome esatto di un file di log giornaliero: SOLO questi vengono mai cancellati
# (mai altri file nella cartella) — `bridge-AAAA-MM-GG.log`.
_LOG_FILE_RE = re.compile(r"^bridge-(\d{4})-(\d{2})-(\d{2})\.log$")


def retention_days(cfg: dict) -> int:
    """Giorni di conservazione dei log dalla config (`log_retention_days`),
    ristretti alle opzioni offerte dalla GUI (`RETENTION_OPTIONS`): solo `5`/`15`/`30`
    sono effettivi; qualsiasi altro valore (assente, 0, negativo, non numerico, o un
    intero non in menu come `7`) → 0 = conserva tutto. Così comportamento ed etichetta
    della tendina restano sempre coerenti (Codex): niente purge "nascosto" mostrato come
    «Mai»."""
    raw = (cfg or {}).get("log_retention_days", 0)
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return 0
    return n if n in RETENTION_OPTIONS else 0


def purge_old_logs(days: int, *, base: str = None, when: datetime = None) -> list:
    """Cancella i file di log **più vecchi di `days` giorni** (best-effort).

    Sicuro: `days <= 0` → no-op; tocca SOLO i file `bridge-AAAA-MM-GG.log` (mai la
    cartella né altri file); un nome non conforme o un errore filesystem viene
    saltato. Ritorna la lista (ordinata) dei nomi rimossi."""
    if not days or days <= 0:
        return []
    when = when or datetime.now()
    cutoff = (when - timedelta(days=int(days))).date()
    folder = log_dir(base)
    removed = []
    try:
        names = os.listdir(folder)
    except OSError:
        return []
    for name in names:
        m = _LOG_FILE_RE.match(name)
        if not m:
            continue
        try:
            file_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            continue
        if file_date < cutoff:
            try:
                os.remove(os.path.join(folder, name))
                removed.append(name)
            except OSError:
                pass
    return sorted(removed)


def clear_all_logs(base: str = None) -> list:
    """Rimuove TUTTI i file di log `bridge-*.log` ("Svuota log adesso", best-effort).
    Come `purge_old_logs`, tocca solo i file di log conformi. Ritorna i nomi rimossi."""
    folder = log_dir(base)
    removed = []
    try:
        names = os.listdir(folder)
    except OSError:
        return []
    for name in names:
        if not _LOG_FILE_RE.match(name):
            continue
        try:
            os.remove(os.path.join(folder, name))
            removed.append(name)
        except OSError:
            pass
    return sorted(removed)


# Header di una entry: ``[HH:MM:SS] [LEVEL] ...``. Si estrae il livello SOLO da
# questo campo strutturale, non cercandolo in tutta la riga: un ``[ERROR]`` nel
# testo del messaggio non deve far classificare la entry come ERROR.
_ENTRY_RE = re.compile(r"^\[\d{2}:\d{2}:\d{2}\] \[([A-Z]+)\] ")


def entry_level(line: str):
    """Livello di una riga formattata (dal campo header), o None se non combacia."""
    m = _ENTRY_RE.match(str(line or ""))
    return m.group(1) if m else None


def filter_by_level(lines, level) -> list:
    """Filtra righe formattate (`format_entry`) per livello, leggendo SOLO il
    campo livello dell'header (non il testo del messaggio)."""
    lvl = normalize_level(level)
    return [line for line in lines if entry_level(line) == lvl]


@dataclass
class Counters:
    """Contatori di stato del bridge per la dashboard (#11): quante cose sono
    successe e qual è l'ultima di ogni tipo. Pura, senza I/O."""

    messages: int = 0
    signals: int = 0
    errors: int = 0
    last_message: str = ""
    last_signal: str = ""
    last_error: str = ""

    def record_message(self, text: str = "") -> None:
        self.messages += 1
        if text:
            self.last_message = text

    def record_signal(self, text: str = "") -> None:
        self.signals += 1
        if text:
            self.last_signal = text

    def record_error(self, text: str = "") -> None:
        self.errors += 1
        if text:
            self.last_error = text
