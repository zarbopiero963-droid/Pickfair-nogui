"""Contratto CSV XTrader e scrittura righe (nessuna dipendenza dalla GUI).

Header reale a 14 colonne basato sui CSV di esempio del team XTrader.
Scrittura: UTF-8 con BOM (utf-8-sig) e tutti i campi tra virgolette (QUOTE_ALL).
Vedi docs/xtrader_csv_contract.md.
"""

import csv
import errno
import logging
import os
import random
import re
import threading
import time
from typing import Callable, Optional

from . import atomic_io, mapping, numbers_re

# Logger di modulo: un file esistente che NON è un CSV del bridge non viene ripulito;
# prima era un return silenzioso, ora si logga il motivo per la diagnosi (audit #105 P2).
logger = logging.getLogger(__name__)

# Lock condiviso: serializza scrittura segnale e svuotamento, eliminando la
# race tra il thread del bot (write_csv) e il timer di auto-clear (init_csv).
_write_lock = threading.Lock()

CSV_HEADER = [
    "Provider", "EventId", "EventName", "MarketId", "MarketName",
    "MarketType", "SelectionId", "SelectionName", "Handicap", "Price",
    "MinPrice", "MaxPrice", "BetType", "Points"
]

# Valori di default coerenti con gli esempi reali XTrader.
DEFAULT_POINTS = ""      # Points lasciato vuoto: lo stake/moltiplicatore lo gestisce XTrader.
DEFAULT_HANDICAP = "0"   # Handicap "0" come negli esempi reali.

# BetType nel CSV XTrader è in italiano: PUNTA (back) / BANCA (lay).
BETTYPE_MAP = {
    "BACK": "PUNTA",
    "LAY":  "BANCA",
}

CSV_ENCODING = "utf-8-sig"  # BOM richiesto dagli esempi XTrader.

# ── Lingua CSV / separatore decimale (#342, fondazione multilingua #343) ────────────────────
# Il supporto XTrader ha confermato che la versione ITALIANA (attuale) legge i decimali di
# quote/points con la VIRGOLA («1,85»), quella inglese col punto. Internamente il bridge resta
# CANONICO col punto (validatori/dedup/pipeline invariati): la localizzazione avviene SOLO qui,
# al confine di scrittura del file. Lingue supportate dal CSV XTrader/Betting Toolkit: IT/EN/ES.
CSV_LANGUAGES = ("IT", "EN", "ES")
DEFAULT_CSV_LANGUAGE = "IT"   # default sicuro per il target principale (XTrader ITA).
# Lingue che scrivono la VIRGOLA decimale. ES segue la convenzione spagnola (virgola) — da
# confermare col supporto Betting Toolkit; se differisse, la correzione è QUESTA riga.
_COMMA_DECIMAL_LANGUAGES = frozenset({"IT", "ES"})
# Colonne del contratto con valori decimali da localizzare (decisione proprietario #342:
# anche Points e Handicap, non solo le quote).
CSV_DECIMAL_COLS = ("Handicap", "Price", "MinPrice", "MaxPrice", "Points")

# Numero "puro" con segno (stesso frammento condiviso di _NUMERIC_RE): SOLO un valore così
# viene localizzato; qualsiasi altra cosa resta INVARIATA (fail-closed: un valore malformato
# è già stato rifiutato a monte dai validatori, qui non va "aggiustato").
_LOCALIZABLE_RE = re.compile(numbers_re.SIGNED_DECIMAL)

_csv_language = DEFAULT_CSV_LANGUAGE
_csv_language_lock = threading.Lock()


def normalize_csv_language(value) -> str:
    """Normalizza la lingua CSV a ``IT``/``EN``/``ES`` (case-insensitive, spazi ignorati).
    Valore mancante/non-stringa/sconosciuto → default sicuro ``IT`` (fail-closed: una config
    vecchia o sporca non cambia il formato del CSV del target principale). Unica fonte di
    verità, usata anche dalla coercion di `config_store.load_config`."""
    if isinstance(value, str) and value.strip().upper() in CSV_LANGUAGES:
        return value.strip().upper()
    return DEFAULT_CSV_LANGUAGE


def set_csv_language(value) -> str:
    """Imposta la lingua del CSV per le PROSSIME scritture (normalizzata fail-closed).
    Chiamata da `config_store.load_config`/`save_config` così startup, Salva e caricamento
    profili restano sempre allineati. Ritorna la lingua effettiva."""
    global _csv_language
    lang = normalize_csv_language(value)
    with _csv_language_lock:
        _csv_language = lang
    return lang


def get_csv_language() -> str:
    """Lingua CSV corrente (``IT``/``EN``/``ES``)."""
    with _csv_language_lock:
        return _csv_language


def _localize_decimal(value, lang: str):
    """Serializza il valore di una colonna DECIMALE per la lingua: virgola per IT/ES, punto
    per EN. Regola UNIFORME e deterministica (review #344 Fable/GLM/Fugu):

    - il valore esce **sempre trimmato** (una colonna decimale non porta MAI padding verso
      XTrader, il cui parser numerico non è garantito tolleri spazi);
    - il separatore viene convertito SOLO se il valore è un numero puro
      (``[+-]?\\d+([.,]\\d+)?``): niente parsing float, niente arrotondamenti — è uno swap di
      carattere sulla sola serializzazione;
    - qualsiasi altro contenuto (testo, doppio separatore malformato) resta INVARIATO nel
      contenuto (già rifiutato a monte dai validatori: qui non va "aggiustato")."""
    s = ("" if value is None else str(value)).strip()
    if s and _LOCALIZABLE_RE.fullmatch(s):
        return s.replace(".", ",") if lang in _COMMA_DECIMAL_LANGUAGES else s.replace(",", ".")
    return s


def _localize_row(row: dict, lang: str) -> dict:
    """Copia della riga con le colonne decimali (`CSV_DECIMAL_COLS`) localizzate alla lingua."""
    out = dict(row)
    for col in CSV_DECIMAL_COLS:
        if col in out:
            out[col] = _localize_decimal(out[col], lang)
    return out


def localize_row(row: dict, lang: str = None) -> dict:
    """Wrapper PUBBLICO di `_localize_row` per le ANTEPRIME (GUI «Prova messaggio»): copia
    della riga coi decimali nel formato della lingua CSV (`lang`, default = lingua corrente),
    cioè come usciranno davvero nel file. Stessa fonte di verità del write-path (#342): la
    preview non può divergere dal CSV reale. NON usare nel percorso interno (validatori/dedup
    lavorano sui valori canonici col punto)."""
    return _localize_row(row, get_csv_language() if lang is None else lang)

# Nome del temporaneo della scrittura atomica del CSV: fonte unica di verità, usata sia
# per scrivere (`_atomic_write_locked`) sia per spazzare gli orfani allo startup
# (`sweep_orphan_temps`), così i due non possono divergere.
_CSV_TMP_PREFIX = ".segnali_"
_CSV_TMP_SUFFIX = ".tmp"

MARKET_MAPPING = {
    "GOL SECONDO TEMPO": "NEXT_GOAL",
    "OVER 0.5":          "OVER_UNDER_05",
    "OVER 1.5":          "OVER_UNDER_15",
    "OVER 2.5":          "OVER_UNDER_25",
    "OVER 3.5":          "OVER_UNDER_35",
    "OVER 4.5":          "OVER_UNDER_45",
    "MATCH ODDS":        "MATCH_ODDS",
    "1X2":               "MATCH_ODDS",
    "GG":                "BOTH_TEAMS_TO_SCORE",
    "GOAL GOAL":         "BOTH_TEAMS_TO_SCORE",
    "NEXT GOAL":         "NEXT_GOAL",
    "DOPPIA CHANCE":     "DOUBLE_CHANCE",
}


def build_csv_row(parsed: dict, provider: str) -> dict:
    """Converte i dati parsati in una riga XTrader.

    Se l'alias del segnale è riconosciuto nel dizionario (PR-07/08) usa i valori
    italiani ufficiali (MarketType/MarketName/SelectionName); altrimenti ricade
    sul mapping legacy. Il `bet_type` viene sempre dal segnale (PUNTA/BANCA).
    """
    teams = parsed['teams']
    parts = teams.split(' v ')
    home = parts[0].strip() if parts else teams
    away = parts[1].strip() if len(parts) > 1 else ""

    # XTrader usa PUNTA/BANCA; il segnale interno resta BACK/LAY (default BACK).
    # Un bet_type sconosciuto NON deve essere mappato silenziosamente: piazzerebbe
    # il lato opposto della scommessa. Lo blocchiamo (safety-critical).
    raw_bet_type = str(parsed.get('bet_type', 'BACK')).strip().upper()
    if raw_bet_type not in BETTYPE_MAP:
        raise ValueError(f"bet_type non supportato: {raw_bet_type!r} (atteso BACK o LAY)")
    bet_type = BETTYPE_MAP[raw_bet_type]

    resolved = mapping.resolve_shorthand(parsed['signal_type'], home=home, away=away)
    if resolved:
        market_type = resolved["MarketType"]
        market_name = resolved["MarketName"]
        selection = resolved["SelectionName"]
        handicap = resolved["Handicap"]   # già normalizzato in mapping.resolve()
    elif mapping.is_known_shorthand(parsed['signal_type']):
        # Alias noto ma non risolvibile (es. nome squadra mancante per 1/2):
        # NON ripiegare su una selezione errata né scrivere il placeholder.
        # SelectionName vuoto → il riconoscimento (PR-06) scarta la riga.
        market_type = ""
        market_name = parsed['signal_type']
        selection = ""
        handicap = DEFAULT_HANDICAP
    else:
        # Fallback legacy: mapping inglese del SOLO tipo mercato. La **selezione NON va
        # inventata** (A1): "OVER 2.5" non è "Over 0.5 Goals" (mercato/linea diversi) e un
        # MATCH ODDS non è sempre l'home (può essere away/pareggio). Sintetizzarla
        # piazzerebbe la selezione SBAGLIATA. SelectionName vuoto → scartato dal
        # riconoscimento (PR-06), fail-closed: meglio nessuna riga che una riga sbagliata.
        signal_upper = parsed['signal_type'].upper()
        market_name = parsed['signal_type']
        handicap = DEFAULT_HANDICAP
        market_type = ""
        for key, val in MARKET_MAPPING.items():
            if key in signal_upper:
                market_type = val
                break
        selection = ""

    # Gli ID (EventId/MarketId/SelectionId) non sono presenti nel messaggio
    # Telegram: restano vuoti. XTrader valida via EventName+MarketType+SelectionName.
    return {
        'Provider':      provider,
        'EventId':       '',
        'EventName':     teams,
        'MarketId':      '',
        'MarketName':    market_name,
        'MarketType':    market_type,
        'SelectionId':   '',
        'SelectionName': selection,
        'Handicap':      handicap,
        'Price':         parsed.get('quota', ''),
        'MinPrice':      '',
        'MaxPrice':      '',
        'BetType':       bet_type,
        'Points':        DEFAULT_POINTS,
    }


# errno STRUTTURALI/permanenti per `os.replace`: ritentarli è inutile e ritarda solo
# l'escalation di ~1s a ogni segnale (audit/M5 #184). Non sono contese transitorie del lock
# di lettura di XTrader: src inesistente, destinazione che è una directory, componente di
# percorso non-directory, rename cross-device, filesystem/destinazione di sola lettura,
# permesso negato (es. dir read-only), nome troppo lungo. Si rilanciano SUBITO.
_PERMANENT_REPLACE_ERRNOS = frozenset(
    e for e in (
        getattr(errno, "ENOENT", None), getattr(errno, "EISDIR", None),
        getattr(errno, "ENOTDIR", None), getattr(errno, "EXDEV", None),
        getattr(errno, "EROFS", None), getattr(errno, "EACCES", None),
        getattr(errno, "EPERM", None), getattr(errno, "ENAMETOOLONG", None),
    ) if e is not None
)
# Windows: codici `winerror` che indicano una contesa TRANSITORIA del file (XTrader lo tiene
# aperto in lettura) e quindi vanno ritentati. ERROR_SHARING_VIOLATION=32, ERROR_LOCK_VIOLATION=33
# e — soprattutto — ERROR_ACCESS_DENIED=5: il read-lock di XTrader fa fallire `MoveFileEx`/
# `os.replace` proprio con ACCESS_DENIED quando la destinazione è aperta in lettura (è il caso
# PIÙ comune, Codex #201 P1). Un read-only/ACL permanente surfacerebbe anch'esso come 5 — i due
# non sono distinguibili dall'errore — ma poiché il lock è la causa tipica si preferisce ritentare
# (al più ~1s sprecato su un raro read-only Windows) piuttosto che perdere il retry sul lock vero.
# Su POSIX, dove il rename atomico NON ha contese di lock, l'`EACCES` resta invece permanente.
_RETRYABLE_REPLACE_WINERRORS = frozenset({5, 32, 33})


def _is_retryable_replace_error(exc: OSError) -> bool:
    """True se l'errore di `os.replace` è una contesa TRANSITORIA che vale la pena ritentare
    (lock di lettura di XTrader su Windows); False se è strutturale/permanente (escalation
    immediata, M5 #184).

    - Su **Windows** (`winerror` valorizzato) si ritenta le contese di lock/condivisione
      `32`/`33` E l'access-denied `5` (il read-lock di XTrader surfacea tipicamente come
      ACCESS_DENIED, Codex #201 P1); gli altri `winerror` sono strutturali.
    - Su **POSIX**/errore generico (niente `winerror`) il rename atomico non ha contese di lock
      transitorie: si ritenta solo se l'`errno` NON è chiaramente permanente — così un errore
      SENZA `errno` (es. il lock simulato nei test/edge inattesi) resta ritentabile, mentre
      ENOENT/EISDIR/EACCES/EXDEV/... escalano subito invece di sprecare ~1s."""
    winerror = getattr(exc, "winerror", None)
    if winerror is not None:
        return winerror in _RETRYABLE_REPLACE_WINERRORS
    return exc.errno not in _PERMANENT_REPLACE_ERRNOS


# Parametri del retry di `os.replace` sotto contesa di lock (#311-3.5-e). Backoff ESPONENZIALE
# con jitter invece del vecchio passo FISSO 0.1s: assorbe meglio un lock di XTrader un po' più
# lungo con poche iterazioni, senza penalizzare il percorso live (il retry gira tenendo
# `_write_lock`). Vincolo DOMINANTE: il budget totale di attesa `_REPLACE_BUDGET` (≤1.5s), oltre
# il quale ci si arrende (propaga → rollback fail-safe → retry event-driven a valle). `attempts`
# è un tetto ASSOLUTO di sicurezza; col backdown+budget il budget lo raggiunge quasi sempre prima.
_REPLACE_ATTEMPTS = 10       # tetto massimo di tentativi di os.replace
_REPLACE_BASE_DELAY = 0.05   # attesa iniziale (s); poi raddoppia a ogni retry
_REPLACE_MAX_DELAY = 0.4     # cap della singola attesa (s)
_REPLACE_BUDGET = 1.5        # budget totale di attesa (s) — vincolo dominante sul percorso live
_REPLACE_JITTER_FRAC = 0.1   # jitter ±10% per de-sincronizzare retry concorrenti


def _replace_with_retry(src: str, dst: str, attempts: int = _REPLACE_ATTEMPTS, *,
                        sleep: Optional[Callable[[float], None]] = None,
                        rng: Optional[Callable[[], float]] = None) -> None:
    """`os.replace` con retry: su Windows il file può essere momentaneamente bloccato da
    XTrader che lo sta leggendo. Backoff **esponenziale** (`_REPLACE_BASE_DELAY`·2^i, con
    **jitter** ±`_REPLACE_JITTER_FRAC` e **cap** `_REPLACE_MAX_DELAY`) fino a un **budget totale**
    `_REPLACE_BUDGET` (~1.5s): il vecchio passo fisso 0.1s (10×0.1s, audit C3) è stato sostituito
    da un backoff che assorbe un lock un po' più lungo con poche iterazioni (#311-3.5-e). Il budget
    è il vincolo DOMINANTE — oltre quello ci si arrende e l'errore propaga (il chiamante fa il
    rollback fail-safe e il retry event-driven a valle riprova) — così il percorso live, che gira
    tenendo `_write_lock`, non resta bloccato troppo a lungo. `attempts` è un tetto ASSOLUTO.

    Solo gli errori TRANSITORI vengono ritentati (`_is_retryable_replace_error`, M5 #184): un
    errore strutturale (dir read-only/EACCES, EISDIR, ENOENT, cross-device) si propaga SUBITO,
    senza sprecare il budget per ogni segnale prima dell'escalation.

    `sleep`/`rng` sono iniettabili (default `time.sleep`/`random.random`) per test deterministici;
    il budget si misura sui delay NOMINALI accumulati, non sul tempo di parete, quindi è
    riproducibile a prescindere dall'attesa reale."""
    sleep = time.sleep if sleep is None else sleep
    rng = random.random if rng is None else rng
    elapsed = 0.0
    for i in range(attempts):
        try:
            os.replace(src, dst)
            return
        except OSError as exc:
            if i == attempts - 1 or not _is_retryable_replace_error(exc):
                raise
            # Backoff esponenziale con jitter, poi CAP per-attesa (il jitter non può sforare il cap).
            delay = _REPLACE_BASE_DELAY * (2 ** i)
            delay *= 1.0 + (rng() * 2.0 - 1.0) * _REPLACE_JITTER_FRAC
            delay = min(delay, _REPLACE_MAX_DELAY)
            # Budget totale: esaurito → arrenditi (propaga l'errore corrente). Altrimenti clampa
            # l'ultima attesa al residuo, così la somma dei delay non supera mai `_REPLACE_BUDGET`.
            remaining = _REPLACE_BUDGET - elapsed
            if remaining <= 0.0:
                raise
            delay = min(delay, remaining)
            elapsed += delay
            sleep(delay)


# Caratteri che, se in TESTA a una cella, possono far interpretare la cella come
# formula/comando da un reader formula-aware (Excel/LibreOffice/Google Sheets) o
# iniettare un controllo di riga: `= + - @` e i control-char TAB/CR/LF. `QUOTE_ALL`
# mette in sicurezza il PARSING ma non neutralizza questi prefissi (audit B1).
_CSV_FORMULA_CHARS = ("=", "+", "-", "@")
_CSV_CTRL_CHARS = ("\t", "\r", "\n")
# Numero "puro" (segno opzionale + decimale con . o ,): es. Handicap "-1"/"+1,5", Price
# "1.85". Un numero legittimo NON va prefissato, altrimenti XTrader leggerebbe "'-1" come
# testo e il contratto numerico si romperebbe. Frammento condiviso (anti-drift, audit L4).
_NUMERIC_RE = re.compile(numbers_re.SIGNED_DECIMAL)


def _sanitize_cell(value):
    """Neutralizza l'iniezione formula/control-char nel CSV (audit B1): se una cella inizia
    con `= + - @` (e NON è un numero) o con un control-char (TAB/CR/LF), antepone un apice
    ``'`` (mitigazione standard OWASP). I numeri (Handicap negativo, Price…) restano intatti.
    Non-stringa/vuoto: invariato."""
    s = "" if value is None else str(value)
    if not s:
        return s
    first = s[0]
    if first in _CSV_CTRL_CHARS:
        return "'" + s
    if first in _CSV_FORMULA_CHARS and not _NUMERIC_RE.fullmatch(s.strip()):
        return "'" + s
    return s


def _sanitize_row(row: dict) -> dict:
    """Copia della riga con ogni cella passata da `_sanitize_cell` (anti CSV-injection)."""
    return {k: _sanitize_cell(v) for k, v in row.items()}


def _atomic_write(path: str, write_rows) -> None:
    """Scrive il CSV in modo atomico (helper condiviso `atomic_io.atomic_write`): file
    temporaneo nella stessa cartella, flush + fsync, poi rename atomico sul file finale.

    Così XTrader non legge mai un file parziale. In caso di errore il file temporaneo
    viene rimosso e l'eccezione propagata (il CSV esistente resta intatto). Il rename usa
    `_replace_with_retry` (retry su lock Windows). Tutto sotto `_write_lock` per
    serializzare write/clear.
    """
    with _write_lock:
        _atomic_write_locked(path, write_rows)


def _atomic_write_locked(path: str, write_rows) -> None:
    """Come `_atomic_write` ma **assume che `_write_lock` sia GIÀ tenuto dal chiamante**.

    Serve a chi deve fare un'operazione composita atomica sotto lo stesso lock — es.
    `clear_stale_csv` legge l'header e poi svuota: il check-then-clear dev'essere un blocco
    unico, senza che una `write_csv` concorrente si inserisca in mezzo (issue #184 H3). NON
    riacquisisce `_write_lock` (che è un `threading.Lock` non rientrante: riacquisirlo dallo
    stesso thread sarebbe un deadlock)."""
    def _write_csv(f):
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        write_rows(writer)

    atomic_io.atomic_write(path, _write_csv, prefix=_CSV_TMP_PREFIX, suffix=_CSV_TMP_SUFFIX,
                           encoding=CSV_ENCODING, newline="", replace=_replace_with_retry)


def sweep_orphan_temps(path: str) -> int:
    """Rimuove i temporanei orfani del CSV (`.segnali_…​.tmp`) nella cartella di `path`,
    lasciati da un crash/blackout TRA la creazione del tmp e il rename (issue #184 LOW).

    Va chiamata **allo startup**, quando il listener è ancora spento: nessuna scrittura è
    in volo, quindi ogni `.segnali_*.tmp` è orfano di un processo morto. Best-effort: non
    solleva mai e non tocca il CSV reale (nome senza prefisso/suffisso). Ritorna quanti ne
    ha rimossi. È solo igiene del disco: il CSV finale era già intatto (il rename, se non
    avvenuto, non lo ha mai sovrascritto)."""
    p = str(path or "").strip()
    if not p:
        return 0
    d = os.path.dirname(os.path.abspath(p)) or "."
    return atomic_io.sweep_orphan_temps(d, _CSV_TMP_PREFIX, _CSV_TMP_SUFFIX)


def init_csv(path: str):
    """Crea/svuota il CSV lasciando solo l'header (scrittura atomica)."""
    _atomic_write(path, lambda writer: None)


def clear_stale_csv(path: str, *, on_mismatch=None) -> bool:
    """Svuota il CSV (solo header) **se il file esiste già**: rimuove una riga
    lasciata da una sessione precedente terminata male (crash, blackout, chiusura).

    Ritorna ``True`` se ha ripulito un file esistente, ``False`` se non c'era nulla
    da fare (``path`` vuoto o file assente). **Non crea** il file se manca: all'avvio
    non si tocca un path non ancora usato.

    `on_mismatch` (opzionale): callback ``(msg: str) -> None`` invocato quando il file
    esiste ma NON è un CSV del bridge (header diverso). Serve a **far emergere** la
    diagnosi nel log del bridge/GUI: il solo `logging.warning` non è visibile in un EXE
    Windows ``--windowed`` (niente stdio) e non passa per il sink del bridge (#105 P2,
    Codex). Il messaggio contiene SOLO metadati strutturali (niente contenuto/segreti),
    lo stesso del warning. È invocato **best-effort**: se solleva, l'eccezione viene
    **ignorata** (non blocca il return né il cleanup del chiamante).

    Difesa anti-segnale-stantio: se il processo muore mentre nel CSV c'è una riga
    attiva, il timer di auto-clear non può girare. Richiamando questa funzione
    all'avvio dell'app (prima che il listener riparta) e alla chiusura/STOP, il CSV
    torna a solo header, così XTrader non legge un segnale orfano.

    **Sicurezza (anti data-loss):** ripulisce SOLO un file che è già un CSV del
    bridge, cioè la cui prima riga è esattamente `CSV_HEADER`. Se `csv_path` punta
    per errore a un file NON-bridge (typo/path riusato in config), il file **non**
    viene toccato: aprire/chiudere l'app non deve poter distruggere un file
    arbitrario dell'utente.

    **Atomicità check-then-clear (issue #184 H3):** la lettura dell'header E il successivo
    svuotamento avvengono SOTTO lo stesso `_write_lock`. Prima il check era fuori dal lock
    e lo svuotamento (`init_csv`) lo riprendeva: una `write_csv` concorrente dal thread del
    bot poteva inserirsi TRA la lettura dell'header e il clear, e un segnale appena scritto
    veniva azzerato a solo-header. Ora la sequenza è un blocco unico serializzato con le
    scritture."""
    if not path:
        return False
    with _write_lock:
        if not os.path.exists(path):
            return False
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                first_row = next(csv.reader(f), None)
        except (UnicodeDecodeError, csv.Error):
            # File non decodificabile/non parsabile (CSV ANSI, binario scelto per
            # errore…) → non è un CSV del bridge: non toccarlo (e niente crash).
            return False
        # NB: un OSError (permessi/lock di Windows, es. file tenuto aperto) NON è
        # catturato qui: si propaga al chiamante, che lo segnala come cleanup fallito
        # invece di silenziarlo come se il file fosse assente/non-bridge.
        if first_row != CSV_HEADER:
            # File esistente e decodificabile ma con header diverso da CSV_HEADER: NON è un CSV
            # del bridge → non si tocca (anti data-loss). Prima il return era SILENZIOSO: ora si
            # logga un avviso per la diagnosi, ma **senza il contenuto** dell'header. Se per
            # errore `csv_path` punta a un file con un segreto (es. un token nella prima riga, più
            # corto di una troncatura) NON deve finire nei log, e questo sink non passa per la
            # redazione di `event_log` (Codex P2). Si riportano solo METADATI strutturali (numero
            # colonne e lunghezza), che bastano a capire che è il file sbagliato.
            n_cols = len(first_row or [])
            n_chars = sum(len(str(c)) for c in (first_row or []))
            msg = ("CSV non ripulito: %s non è un CSV del bridge (header atteso %d colonne; "
                   "rilevate %d colonne, %d caratteri — contenuto non loggato). "
                   "Controlla csv_path." % (path, len(CSV_HEADER), n_cols, n_chars))
            logger.warning("%s", msg)
            # Fa emergere la diagnosi anche nel log del bridge/GUI (visibile in EXE --windowed,
            # dove lo stderr del logging non c'è): #105 P2, Codex. Best-effort ENFORCED: un sink
            # log/GUI che solleva (es. `_log` su una root Tk distrutta) NON deve propagare e
            # rompere il cleanup anti-segnale-stantio all'avvio/STOP — il file resta comunque
            # intatto e il warning è già stato loggato (Codex P2 su #266).
            if on_mismatch is not None:
                try:
                    on_mismatch(msg)
                except Exception:   # noqa: BLE001 — diagnostica best-effort, mai bloccare il return
                    pass
            return False   # non è un CSV del bridge → non sovrascrivere
        # svuotamento SOTTO lo stesso lock (no init_csv: riacquisirebbe _write_lock → deadlock)
        _atomic_write_locked(path, lambda writer: None)
        return True


def has_active_row(path: str) -> bool:
    """``True`` se ``path`` è un CSV del bridge ESISTENTE con ALMENO una riga dati (oltre
    l'header). **Read-only e best-effort**: ``path`` vuoto/assente/non-bridge/illeggibile →
    ``False``.

    Serve a distinguere «riga stantia realmente presente» da «CSV già a solo header» PRIMA di
    un cleanup, così il diario eventi (#234) non registra un crash-recovery/clear su una
    riscrittura idempotente a solo-header (falso positivo). Il check è serializzato col
    ``_write_lock`` come `clear_stale_csv`, così non legge uno stato a metà scrittura."""
    if not path:
        return False
    with _write_lock:
        if not os.path.exists(path):
            return False
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                if next(reader, None) != CSV_HEADER:
                    return False   # non è un CSV del bridge (o file vuoto): nessuna riga attiva
                # Una riga dati "attiva" è una riga con almeno una cella non vuota dopo l'header.
                return any(any((c or "").strip() for c in row) for row in reader)
        except (OSError, UnicodeDecodeError, csv.Error):
            return False


def is_bridge_csv(path: str) -> bool:
    """``True`` se ``path`` esiste ed è un CSV del bridge, cioè la cui prima riga è
    esattamente `CSV_HEADER`. File assente/vuoto/illeggibile/non-bridge → ``False``.

    Read-only e best-effort, serializzato con `_write_lock` come `has_active_row`/
    `clear_stale_csv`, così non legge uno stato a metà scrittura. Serve alla feature
    «📄 Crea CSV» (#286) per l'**anti data-loss**: prima di rigenerare un CSV a solo
    header su un percorso già esistente si distingue un CSV del bridge (sovrascrivibile
    a solo header) da un file estraneo dell'utente (da NON distruggere senza conferma
    esplicita)."""
    if not path:
        return False
    with _write_lock:
        if not os.path.exists(path):
            return False
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                return next(csv.reader(f), None) == CSV_HEADER
        except (OSError, UnicodeDecodeError, csv.Error):
            return False


# Esiti di `create_header_only_csv` (#286): guidano il messaggio/conferma del chiamante GUI.
CSV_CREATE_DONE = "done"                  # creato / rigenerato a solo header
CSV_CREATE_REFUSED_FOREIGN = "foreign"    # file estraneo (header ≠ CSV_HEADER): non toccato
CSV_CREATE_REFUSED_ACTIVE = "active"      # CSV del bridge con riga attiva: non toccato


def create_header_only_csv(path: str, *, force: bool = False) -> str:
    """Crea un CSV **a solo header** su `path` in modo ATOMICO e anti data-loss: il check
    dell'header esistente E la scrittura avvengono SOTTO LO STESSO `_write_lock`, così non
    c'è finestra TOCTOU tra «guarda cos'è» e «sovrascrivi» (stesso principio di
    `clear_stale_csv`, issue #184 H3). Serve alla feature «📄 Crea CSV» (#286).

    Semantica (senza `force`):
    - `path` **assente** → creato a solo header.
    - **CSV del bridge a solo header** (nessuna riga dati) → rigenerato (idempotente).
    - **CSV del bridge con una riga attiva** → NON toccato → `CSV_CREATE_REFUSED_ACTIVE`
      (protegge un segnale non ancora letto da XTrader, es. la sessione avviata).
    - **file estraneo** (prima riga ≠ `CSV_HEADER`, o illeggibile) → NON toccato →
      `CSV_CREATE_REFUSED_FOREIGN` (anti data-loss su un file dell'utente scelto per errore).

    Con `force=True` (conferma esplicita dell'utente) i due `REFUSED_*` sono bypassati e il
    file è comunque rigenerato a solo header. Ritorna `CSV_CREATE_DONE` quando ha scritto.
    Solleva `OSError` se la scrittura fallisce (cartella assente/permessi)."""
    if not path:
        return CSV_CREATE_REFUSED_FOREIGN
    with _write_lock:
        if os.path.exists(path) and not force:
            try:
                with open(path, newline="", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    if next(reader, None) != CSV_HEADER:
                        return CSV_CREATE_REFUSED_FOREIGN     # non è un CSV del bridge
                    # È un CSV del bridge: una riga dati "attiva" è una riga con almeno una
                    # cella non vuota dopo l'header (stessa definizione di `has_active_row`).
                    if any(any((c or "").strip() for c in row) for row in reader):
                        return CSV_CREATE_REFUSED_ACTIVE      # c'è un segnale attivo: non toccare
            except (UnicodeDecodeError, csv.Error):
                # File non decodificabile/non parsabile → non è un CSV del bridge: non toccarlo.
                return CSV_CREATE_REFUSED_FOREIGN
            # NB: un OSError in lettura (permessi/lock) si propaga come per `clear_stale_csv`:
            # il chiamante lo segnala come «creazione fallita», non lo silenzia.
        _atomic_write_locked(path, lambda writer: None)       # solo header (stesso lock)
        return CSV_CREATE_DONE


def write_rows(rows, path: str):
    """Scrive PIÙ segnali nel CSV (header + una riga per ciascuno), sovrascrivendo
    in modo atomico. `rows` vuota → solo header (equivale a `init_csv`). Usato dalla
    coda dei segnali attivi (PR-22): in OVERWRITE_LAST è una sola riga, in
    APPEND_ACTIVE/QUEUE_UNTIL_CONFIRMED sono i segnali attivi correnti."""
    rows = list(rows or [])
    # Lingua catturata UNA volta per l'intera scrittura (#342): tutte le righe dello stesso
    # file usano lo stesso separatore anche se la config cambiasse a metà.
    lang = get_csv_language()

    def _emit(writer):
        for r in rows:
            # Localizzazione decimali (#342) PRIMA della sanitizzazione: XTrader ITA/ES legge
            # la virgola, EN il punto; l'interno resta canonico col punto. `_sanitize_cell`
            # riconosce i numeri con entrambi i separatori (SIGNED_DECIMAL), quindi un
            # «-1,5» localizzato NON viene apostrofato.
            # Anti CSV-injection (B1): neutralizza i prefissi formula/control-char nelle celle
            # (testo attacker-controlled da Telegram) prima di scriverle; i numeri restano intatti.
            writer.writerow(_sanitize_row(_localize_row(r, lang)))

    _atomic_write(path, _emit)


def write_csv(row: dict, path: str):
    """Scrive un singolo segnale nel CSV, sovrascrivendo (scrittura atomica)."""
    write_rows([row], path)
