"""PR-17: lettura delle notifiche di conferma di XTrader (logica pura, testabile).

XTrader (o l'utente) invia su una chat Telegram **separata** dalle sorgenti dei
segnali (`xtrader_notification_chat_id`) messaggi tipo "scommessa piazzata" o
"errore". Questo modulo **interpreta** quei messaggi per capire se un nostro
segnale in attesa è stato:

- **CONFIRMED**: piazzato con successo;
- **REJECTED**: rifiutato/errore;
- **UNKNOWN**: associato a un nostro segnale ma senza esito chiaro;
- **UNMATCHED**: la notifica non si associa a nessun nostro segnale (es. conferma
  di un'altra cosa) → da ignorare.

Il match avviene prima per **SignalRef** (se XTrader lo riporta, è il più
affidabile), altrimenti per **EventName + MarketName + SelectionName** tutti
presenti nel testo. `timed_out` copre il caso "nessuna conferma entro il timeout".

**Sola lettura**: questo modulo NON scrive il CSV e NON genera scommesse; ritorna
solo uno stato. L'aggancio al runtime (leggere la chat notifiche, confermare il
segnale nella coda) è un passo successivo. Modulo puro, interamente testabile.
"""

import math
import re
from dataclasses import dataclass

CONFIRMED = "CONFIRMED"
REJECTED = "REJECTED"
TIMEOUT = "TIMEOUT"
UNKNOWN = "UNKNOWN"        # match trovato ma esito non riconoscibile
UNMATCHED = "UNMATCHED"    # la notifica non si associa a nessun segnale nostro

DEFAULT_CONFIRM_KEYWORDS = (
    "confermata", "confermato", "piazzata", "piazzato", "eseguita", "eseguito",
    "matched", "placed", "ok",
)
DEFAULT_REJECT_KEYWORDS = (
    "rifiutata", "rifiutato", "errore", "fallita", "fallito", "annullata",
    "annullato", "rejected", "failed", "no match", "unmatched", "error",
    # Frasi NEGATE: contengono una keyword di conferma ma indicano il contrario.
    # Stanno tra i reject (valutati per primi) per non risultare un falso CONFIRMED.
    "non piazzata", "non piazzato", "non confermata", "non confermato",
    "non eseguita", "non eseguito", "not matched", "not placed", "not confirmed",
)


def normalize_keywords(value):
    """Normalizza le keyword di conferma/rifiuto (dalla config) a **lista** o None.

    Una **stringa** singola (es. un `config.json` scritto a mano con `"piazzata"`
    invece di `["piazzata"]`) NON va passata così a `classify_outcome`: essendo
    iterabile verrebbe scandita **carattere per carattere** e una notifica con una
    lettera comune (es. "a") risulterebbe CONFIRMED/REJECTED per sbaglio. Quindi una
    stringa è avvolta come **singola** keyword. Lista/tupla → stringhe non vuote.
    Vuoto/None/tipo inatteso → None (usa i default del modulo)."""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return [v] if v else None
    if isinstance(value, (list, tuple)):
        kws = [str(x).strip() for x in value if str(x).strip()]
        return kws or None
    return None


def _norm(s) -> str:
    return str(s or "").lower()


# Negatori che, nella stessa proposizione, ribaltano una keyword di conferma in rifiuto
# ("non/not ... piazzata"). NON includono `senza`/`without`: quelle sono qualificatori di
# SUCCESSO ("senza errori ... piazzata" = piazzata bene), non negazioni del piazzamento (#31
# review). E negatori che, IMMEDIATAMENTE davanti a un termine d'errore generico, lo rendono
# un esito POSITIVO ("no error"/"nessun errore"/"senza errori" = successo).
_CONFIRM_NEGATORS = ("non", "not", "nessun", "nessuna", "nessuno", "mai")
_ERROR_NEGATORS = ("no", "non", "not", "nessun", "nessuna", "nessuno", "senza", "without", "zero")
# Reject keyword generiche la cui negazione indica successo (vedi `classify_outcome`, #31).
_NEGATABLE_REJECTS = ("error", "errore")
# Confini di clausola: una negazione oltre uno di questi NON si riferisce alla keyword.
_CLAUSE_SPLIT = re.compile(r"[,.;:!?]")
# Finestra (parole prima) per la negazione di un TERMINE D'ERRORE: deve essere ADIACENTE
# ("no error"), così "no error but error occurred" non maschera il secondo errore reale
# (#31 review). La negazione di CONFERMA usa invece l'intera clausola (vedi `_occ_negation`).
_ERROR_NEG_WINDOW = 2


def _kw_pattern(keyword: str) -> str:
    """Pattern regex di `keyword` con confini di parola **adattivi**: su un bordo
    alfanumerico richiede un confine di parola (es. "ok" non scatta dentro "token"); su un
    bordo simbolo (es. "✅") `\\b` non esiste, quindi match diretto. `""` se keyword vuota."""
    kw = _norm(keyword).strip()
    if not kw:
        return ""
    left = r"(?<!\w)" if (kw[0].isalnum() or kw[0] == "_") else ""
    right = r"(?!\w)" if (kw[-1].isalnum() or kw[-1] == "_") else ""
    return left + re.escape(kw) + right


def _has_keyword(text: str, keyword: str) -> bool:
    """True se `keyword` compare delimitata in `text` (confini adattivi, vedi `_kw_pattern`)."""
    pat = _kw_pattern(keyword)
    return bool(pat) and re.search(pat, text) is not None


def _occ_negation(text: str, keyword: str, negators, window=None):
    """Esamina OGNI occorrenza di `keyword` e dice se è negata da una parola di `negators`
    che la precede nella STESSA clausola (oltre un confine `,.;:!?` la negazione non conta).

    `window` = numero di parole-prima da guardare (None = intera clausola). Ritorna la coppia
    ``(qualcuna_non_negata, qualcuna_negata)``. Serve a distinguere:
    - negazione di CONFERMA su tutta la clausola ("non ... piazzata", a qualunque distanza);
    - negazione di ERRORE solo ADIACENTE (window piccola), così un errore reale NON negato
      più avanti nel testo non viene mascherato da un "no error" precedente (#31 review)."""
    pat = _kw_pattern(keyword)
    any_unneg = any_neg = False
    if not pat:
        return (False, False)
    for m in re.finditer(pat, text):
        clause = _CLAUSE_SPLIT.split(text[:m.start()])[-1]
        words = re.findall(r"\w+", clause)
        if window is not None:
            words = words[-window:]
        if any(w in negators for w in words):
            any_neg = True
        else:
            any_unneg = True
    return (any_unneg, any_neg)


# Ref ETICHETTATO nel testo: "Ref ABC123", "Rif: 123", "ID-9", "#ABC". Serve a
# capire se la notifica è esplicitamente per un certo ref. Copre le forme comuni;
# il formato esatto di XTrader si fisserà al wiring reale.
#
# Il gruppo cattura il ref COMPLETO inclusi i suffissi `/` e `.` (`[\w/.-]`), allineato
# alla continuazione di token di `_has_ref_token`: così "Ref ABC123/4" viene estratto come
# "abc123/4" (ref di un'ALTRA scommessa), non troncato ad "abc123". Senza, la guardia
# anti-ref-estraneo in `match_pending` lo confondeva con un nostro ref "ABC123" e lasciava
# passare il fallback per nomi, confermando il segnale sbagliato (#31).
_REF_LABEL_RE = re.compile(
    r"(?:\b(?:ref|rif|reference|id)\b[^\w-]*|#)([0-9a-z][\w/.-]*)", re.IGNORECASE)


def _message_ref_tokens(text: str) -> list:
    """Token di ref etichettati trovati nel testo (minuscoli, ref completo coi suffissi).
    Vuoto se nessuno."""
    return [m.group(1).lower() for m in _REF_LABEL_RE.finditer(text)]


def _has_ref_token(text: str, ref: str) -> bool:
    """True se `ref` compare come **token intero** in `text`, delimitato da inizio/
    fine o da un carattere che NON prosegue il token (`[\\w-]`). Più stretto di
    `\\b`: un ref `"ABC123"` non combacia dentro `"ABC123-4"` (ref diverso con
    suffisso), evitando di associare il segnale sbagliato."""
    r = _norm(ref).strip()
    if not r:
        return False
    # Continuazione del token: oltre a word/`-`, anche `/` e `.` (suffissi comuni
    # nei ref), così "ABC123" non combacia dentro "ABC123-4"/"ABC123/4"/"ABC123.4".
    return re.search(r"(?<![\w/.-])" + re.escape(r) + r"(?![\w/.-])", text) is not None


@dataclass
class ConfirmationResult:
    """Esito dell'interpretazione di una notifica XTrader."""

    status: str                 # CONFIRMED | REJECTED | UNKNOWN | UNMATCHED
    signal_id: object = None    # id del segnale associato (None se UNMATCHED)


def classify_outcome(text: str, confirm_keywords=None, reject_keywords=None):
    """Esito dichiarato dal testo: CONFIRMED / REJECTED / None (non riconosciuto).

    I **reject** hanno la precedenza: un messaggio d'errore che contenga per caso
    una parola di conferma non deve risultare confermato (fail-safe)."""
    t = _norm(text)
    rej = reject_keywords or DEFAULT_REJECT_KEYWORDS
    con = confirm_keywords or DEFAULT_CONFIRM_KEYWORDS
    # I reject hanno la precedenza, MA un termine d'errore generico si ignora SOLO se TUTTE
    # le sue occorrenze sono negate adiacenti ("no error"/"nessun errore" = successo). Se ne
    # resta anche una NON negata (es. "no error but error occurred"), è un rifiuto reale:
    # fail-safe, non si maschera un errore (#31 review). Gli altri reject (incl. le frasi
    # negate esplicite come "non piazzata") restano hard-reject.
    for k in rej:
        if not _has_keyword(t, k):
            continue
        if _norm(k).strip() in _NEGATABLE_REJECTS:
            unneg, _ = _occ_negation(t, k, _ERROR_NEGATORS, window=_ERROR_NEG_WINDOW)
            if not unneg:
                continue
        return REJECTED
    # Conferma: una keyword positiva conferma, salvo una negazione nella STESSA clausola che
    # la ribalta in rifiuto (fail-safe, a qualunque distanza nella clausola). Una negazione in
    # una clausola SEPARATA non conta (#31): "non serve altro, scommessa piazzata" è conferma.
    confirmed = negated = False
    for k in con:
        if not _has_keyword(t, k):
            continue
        unneg, neg = _occ_negation(t, k, _CONFIRM_NEGATORS)
        if unneg:
            confirmed = True
        elif neg:
            negated = True
    if confirmed:
        return CONFIRMED
    if negated:
        return REJECTED
    return None


def match_pending(text: str, pending):
    """Il segnale (dict) a cui la notifica si riferisce, o None se nessuno/ambiguo.

    `pending`: lista di dict con `signal_id`, opzionale `ref` (SignalRef) e i campi
    nome `EventName`/`MarketName`/`SelectionName`. Match per `ref` se presente nel
    testo (più affidabile); altrimenti per tutti i campi nome presenti. Se più di
    un candidato combacia → None (ambiguo: non si associa a caso)."""
    t = _norm(text)
    # Match per ref come TOKEN intero: un ref "123" non combacia dentro "ABC1234"
    # né "ABC123" dentro "ABC123-4" (ref diverso): non si associa il segnale sbagliato.
    by_ref = [p for p in pending
              if _norm(p.get("ref")).strip() and _has_ref_token(t, p.get("ref"))]
    if len(by_ref) == 1:
        return by_ref[0]
    if len(by_ref) > 1:
        return None

    def _remove_first(text: str, phrase: str) -> str:
        """Rimuove la prima occorrenza (parola intera) di `phrase` da `text`."""
        return re.sub(r"\b" + re.escape(phrase) + r"\b", " ", text, count=1)

    def all_name_fields_present(p) -> bool:
        # Fallback: servono TUTTI E TRE i campi identità, non vuoti e presenti nel
        # testo come PAROLE INTERE e su porzioni DISTINTE: si consuma il testo via
        # via, così una selezione contenuta nell'evento (es. "Inter" dentro
        # "Inter v Milan") non viene contata due volte. Un sottoinsieme NON basta:
        # meglio nessun match che il segnale sbagliato.
        ev = _norm(p.get("EventName")).strip()
        mk = _norm(p.get("MarketName")).strip()
        sel = _norm(p.get("SelectionName")).strip()
        if not (ev and mk and sel):
            return False
        if not _has_keyword(t, ev):
            return False
        rest = _remove_first(t, ev)
        if not _has_keyword(rest, mk):
            return False
        rest = _remove_first(rest, mk)
        return _has_keyword(rest, sel)

    # Se la notifica porta un ref ETICHETTATO ("Ref/Rif/ID/#") che NON è di
    # nessun nostro segnale, è esplicitamente per un'altra scommessa: niente
    # fallback per nomi (eviterebbe di confermare un nostro segnale senza ref con i
    # nomi coincidenti — es. scommesse ripetute sullo stesso mercato). Fail-safe:
    # nel dubbio non si associa.
    pending_refs = {_norm(p.get("ref")).strip() for p in pending if _norm(p.get("ref")).strip()}
    msg_refs = _message_ref_tokens(t)
    if msg_refs and not any(r in pending_refs for r in msg_refs):
        return None

    # Il fallback per nomi vale SOLO per i segnali SENZA ref: se un segnale ha un
    # SignalRef, va confermato solo via quel ref.
    by_fields = [p for p in pending
                 if not _norm(p.get("ref")).strip() and all_name_fields_present(p)]
    return by_fields[0] if len(by_fields) == 1 else None


def interpret(text: str, pending, *, confirm_keywords=None,
              reject_keywords=None) -> ConfirmationResult:
    """Interpreta una notifica XTrader rispetto ai segnali in attesa.

    Se non si associa a nessun segnale → UNMATCHED (da ignorare, anche se il testo
    sembra una conferma: è di qualcun altro). Se associato ma senza esito chiaro →
    UNKNOWN. Altrimenti CONFIRMED/REJECTED."""
    matched = match_pending(text, pending)
    if matched is None:
        return ConfirmationResult(UNMATCHED, None)
    outcome = classify_outcome(text, confirm_keywords, reject_keywords)
    sid = matched.get("signal_id")
    if outcome is None:
        return ConfirmationResult(UNKNOWN, sid)
    return ConfirmationResult(outcome, sid)


def _require_finite(value, name: str) -> float:
    """`value` come float finito, altrimenti ValueError. Un `NaN`/`inf` (accettato
    da `json.load` su stato persistito) romperebbe i confronti temporali."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} non valido: {value!r}")
    if not math.isfinite(f):
        raise ValueError(f"{name} deve essere un numero finito (ricevuto {value!r})")
    return f


def timed_out(added_at: float, now: float, timeout: float) -> bool:
    """True se è trascorso almeno `timeout` dalla creazione del segnale senza
    conferma: il chiamante può marcarlo TIMEOUT.

    Tutti i valori devono essere finiti e `timeout > 0`: un `NaN` (anche in
    `added_at`/`now`, da stato persistito via `json.load`) renderebbe il confronto
    sempre falso → segnale mai in TIMEOUT; `now=inf` scadrebbe sempre; `timeout<=0`
    scadrebbe subito. Fail-fast con ValueError (come la coda dei segnali)."""
    a = _require_finite(added_at, "added_at")
    n = _require_finite(now, "now")
    t = _require_finite(timeout, "timeout")
    if t <= 0:
        raise ValueError(f"timeout deve essere > 0 (ricevuto {timeout!r})")
    return (n - a) >= t
