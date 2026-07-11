"""PR-15: ciclo di vita del segnale e deduplica (logica pura, testabile).

Riduce il rischio di **doppia scommessa** (#5): lo stesso messaggio non deve
generare due segnali, e una raffica anomala va limitata. È logica pura, separata
da GUI/CSV/Telegram: la deduplica **non altera** il CSV XTrader, decide solo se
un segnale va processato.

Componenti:
- `message_hash(text)`: impronta stabile del messaggio (normalizzato su spazi),
  per riconoscere lo stesso messaggio anche con spaziatura diversa.
- `SignalTracker`: ricorda gli hash recenti in una **finestra** temporale e
  applica un **limite al minuto**. `register(text)` ritorna NEW / DUPLICATE /
  RATE_LIMITED senza scrivere nulla. `mark_seen(key)` (#192 kyW) registra un
  marcatore di sola **deduplica** (voce *shadow*) che NON conta verso il limite/
  minuto: serve alla riconciliazione **cross-namespace** hash-messaggio ↔ chiave
  per-riga alla transizione di modalità del parser (anti-doppia-scommessa).
- `state()` / `restore_state()` + `save_state`/`load_state` su file: gli hash
  recenti sopravvivono a un **riavvio** (history giornaliera), così un duplicato
  ravvicinato è riconosciuto anche dopo il restart.

Il vocabolario del ciclo di vita (`STATES`) è qui come riferimento per le fasi
successive (PR-16 coda, PR-17 conferma XTrader); l'aggancio al runtime è separato.

**Clock-skew e wall-clock (issue #184 LOW).** Sia la finestra di deduplica sia il limite
al minuto usano l'orologio di sistema (`time.time()`), e lo stato è PERSISTITO su disco:
un salto dell'orologio (correzione NTP, cambio manuale, fuso) sposta il riferimento
temporale. Comportamento garantito:

- salto **indietro** (now diventa più piccolo): le voci già memorizzate risultano nel
  futuro (`t > now`) ma restano **valide** e NON vengono mai eliminate da `_prune`
  (vedi la condizione `t > now`), quindi i duplicati continuano a essere bloccati — nessuna
  doppia scommessa;
- salto **in avanti** (now diventa più grande): le voci invecchiano di colpo e possono
  uscire dalla finestra prima del previsto; un duplicato ravvicinato potrebbe essere
  riaccettato come `NEW`. È una **finestra transitoria inerente** alla persistenza
  wall-clock: non è distinguibile dal normale scorrere del tempo (idle lungo) e quindi
  non si "corregge" senza un orologio monotòno (che però non sopravvive ai riavvii). La
  protezione forte anti-doppia-scommessa resta a valle (rollback atomico di coda/CSV).
"""

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass, field

from . import atomic_io, validators

# Stati del ciclo di vita del segnale (vocabolario condiviso; usati appieno in
# PR-16/PR-17). DUPLICATE/RATE_LIMITED sono gli esiti decisi qui.
STATES = (
    "RECEIVED", "PARSED", "VALIDATED", "CSV_WRITTEN", "WAITING_XTRADER",
    "CONFIRMED", "TIMEOUT", "FAILED", "DUPLICATE",
)

NEW = "NEW"
DUPLICATE = "DUPLICATE"
RATE_LIMITED = "RATE_LIMITED"

DEFAULT_DEDUPE_WINDOW = 300     # secondi: finestra entro cui un messaggio è "lo stesso"
DEFAULT_MAX_PER_MINUTE = 20     # segnali nuovi ammessi al minuto

_WS = re.compile(r"\s+")


# Validatori difensivi (audit #105 P2): allineano `SignalTracker` allo stile di
# `safety_guard.DailyLimiter` — un parametro/timestamp malformato non deve rendere
# la deduplica/limite inefficaci o sempre bloccanti. Fonte UNICA condivisa in
# `validators` (era duplicato qui e in safety_guard, #133 item 6, parte "validatori").


def message_hash(text: str) -> str:
    """Hash SHA-256 del messaggio normalizzato (trim + spazi collassati), così
    differenze di sola spaziatura non sfuggono alla deduplica."""
    norm = _WS.sub(" ", str(text or "").strip())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


# Campi identificativi della riga per la deduplica PER-RIGA (#192). Provider+evento+mercato+
# selezione+lato individuano univocamente una scommessa: due righe dello stesso messaggio che
# differiscono in uno di questi campi NON sono duplicati, mentre una riga identica reinviata sì.
_ROW_KEY_FIELDS = ("Provider", "EventName", "MarketType", "SelectionName", "BetType", "Handicap")


def row_dedup_key(text: str, row: dict) -> str:
    """Chiave di deduplica PER-RIGA (#192): combina l'hash del messaggio con i campi
    identificativi della riga (`_ROW_KEY_FIELDS`). Così un singolo messaggio che genera più
    mercati/selezioni non auto-dedupa le sue righe diverse, ma lo stesso identico segnale
    (stesso messaggio + stessi campi) resta un duplicato. Per il single-row la chiave dipende
    comunque dal messaggio, quindi un messaggio ripetuto è ancora bloccato come prima."""
    parts = [message_hash(text)]
    parts += [str((row or {}).get(k, "") or "").strip() for k in _ROW_KEY_FIELDS]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


@dataclass
class RegisterResult:
    """Esito di `SignalTracker.register`."""

    status: str            # NEW | DUPLICATE | RATE_LIMITED
    hash: str

    @property
    def accepted(self) -> bool:
        return self.status == NEW


@dataclass
class SignalTracker:
    """Tiene gli hash recenti per deduplica e limite al minuto. In-memory, ma lo
    stato è serializzabile (`state`/`restore_state`) per sopravvivere al riavvio."""

    dedupe_window: int = DEFAULT_DEDUPE_WINDOW
    max_per_minute: int = DEFAULT_MAX_PER_MINUTE
    # Voci `(hash, epoch_seconds, real)`. `real=True` → registrazione di un segnale realmente
    # processato (conta verso il limite/minuto). `real=False` → marcatore SHADOW di sola deduplica
    # (`mark_seen`, riconciliazione cross-namespace #192 kyW): NON conta verso il rate-limit.
    _seen: list = field(default_factory=list)

    def __post_init__(self):
        # Parametri validati come in DailyLimiter (audit #105 P2): una finestra/limite
        # malformato (bool/NaN/<=0) renderebbe la protezione inefficace o sempre bloccante.
        self.dedupe_window = validators.require_positive_int(self.dedupe_window, "dedupe_window")
        self.max_per_minute = validators.require_positive_int(self.max_per_minute, "max_per_minute")

    def _prune(self, now: float) -> None:
        # Si conserva la storia per il MASSIMO tra finestra dedup e 60s: altrimenti
        # con una finestra dedup < 60s il conteggio al minuto verrebbe falsato
        # (voci rimosse prima di contarle) e il limite sarebbe aggirabile.
        cutoff = now - max(self.dedupe_window, 60)
        # Clock-skew (issue #184 LOW). La deduplica è su WALL-CLOCK e persistita: un salto
        # INDIETRO dell'orologio (correzione NTP, cambio manuale, fuso) rende `now` più
        # PICCOLO dei timestamp già memorizzati, che risultano quindi nel FUTURO (t > now).
        # Quelle voci sono VALIDE e devono restare a proteggere dai duplicati: non vanno mai
        # scartate. Col cutoff attuale sono già conservate (t > now >= cutoff); la condizione
        # esplicita `t > now` codifica la garanzia e la rende robusta a futuri refactor della
        # finestra. (Il caso opposto — un salto in AVANTI che invecchia di colpo le voci e
        # potrebbe far sfuggire un duplicato — è inerente alla persistenza wall-clock e non
        # distinguibile dal normale scorrere del tempo: vedi nota nel docstring del modulo.)
        self._seen = [(h, t, r) for (h, t, r) in self._seen if t >= cutoff or t > now]

    def register(self, text: str, *, now: float = None, key: str = None) -> RegisterResult:
        """Registra un messaggio e decide il suo esito (senza scrivere nulla):

        - **DUPLICATE**: stesso hash già visto nella finestra di deduplica;
        - **RATE_LIMITED**: troppi segnali NUOVI nell'ultimo minuto;
        - **NEW**: accettato (e memorizzato).

        Un DUPLICATE o un RATE_LIMITED NON vengono memorizzati come nuovi.

        `key` (#192): se fornita, è la chiave di deduplica usata al posto dell'hash del
        messaggio (vedi `row_dedup_key` per la deduplica PER-RIGA del multi-output). Assente
        (default) → `message_hash(text)`, comportamento single-row invariato."""
        now = time.time() if now is None else validators.require_finite_now(now)
        self._prune(now)
        h = message_hash(text) if key is None else str(key)
        # Duplicato: stesso hash entro la finestra di deduplica (NON l'intera storia conservata,
        # che può essere più lunga per il conteggio al minuto). Vale sia per le voci REALI sia per
        # i marcatori SHADOW (`mark_seen`): entrambi bloccano un duplicato cross-namespace (#192).
        dedupe_cutoff = now - self.dedupe_window
        if any(hh == h and t >= dedupe_cutoff for (hh, t, _real) in self._seen):
            return RegisterResult(DUPLICATE, h)
        minute_ago = now - 60
        # Rate-limit: conta SOLO le registrazioni REALI. I marcatori shadow di sola deduplica
        # (mark_seen) non sono segnali processati e NON devono consumare capacità/minuto (#192 kyW).
        recent = sum(1 for (_hh, t, real) in self._seen if real and t >= minute_ago)
        if recent >= self.max_per_minute:
            return RegisterResult(RATE_LIMITED, h)
        self._seen.append((h, now, True))
        return RegisterResult(NEW, h)

    def mark_seen(self, key: str, *, now: float = None) -> None:
        """Marca `key` come GIÀ VISTA ai SOLI fini di deduplica (#192 kyW), senza contare verso il
        limite/minuto e senza cambiare alcun esito. Serve alla riconciliazione **cross-namespace**
        tra il percorso single-row (hash-messaggio) e multi-riga (chiave per-riga): dopo una
        scrittura reale su un percorso si "ombreggia" la chiave dell'ALTRO percorso, così un retry
        dello stesso messaggio DOPO un cambio di modalità del parser è riconosciuto come DUPLICATE
        (anti-doppia-scommessa). No-op se la chiave è già presente (reale o shadow): non aggiunge
        voci ridondanti né declassa una voce reale a shadow."""
        now = time.time() if now is None else validators.require_finite_now(now)
        self._prune(now)
        key = str(key)
        # No-op SOLO se la chiave è già vista ENTRO la finestra di dedup (reale o shadow): allora
        # blocca già i duplicati. Se invece esiste solo FUORI finestra — con `dedupe_window < 60s`
        # `_prune` la conserva comunque per il conteggio al minuto — va RINFRESCATA con un marcatore
        # a `now`, altrimenti una transizione di modalità subito dopo la vedrebbe come NEW e il
        # duplicato sfuggirebbe (Codex #192 kyW). Non si tocca l'eventuale voce reale stantia (resta
        # per il rate-limit): si aggiunge solo la shadow in-finestra.
        dedupe_cutoff = now - self.dedupe_window
        if any(hh == key and t >= dedupe_cutoff for (hh, t, _real) in self._seen):
            return
        self._seen.append((key, now, False))

    def is_seen(self, key: str, *, now: float = None) -> bool:
        """True se `key` è già presente (reale o shadow) ENTRO la finestra di deduplica (#192 kyW).
        Lettura PURA: non registra nulla e non conta verso il rate-limit. Serve ai commit per un
        controllo cross-namespace PRE-scrittura, utile soprattutto con uno stato persistito da una
        versione precedente che aveva un SOLO namespace di chiavi (solo hash-messaggio o solo
        chiavi per-riga): fail-closed contro la doppia scommessa alla transizione di modalità."""
        now = time.time() if now is None else validators.require_finite_now(now)
        self._prune(now)
        dedupe_cutoff = now - self.dedupe_window
        return any(hh == str(key) and t >= dedupe_cutoff for (hh, t, _real) in self._seen)

    # ── persistenza (riconoscimento duplicati dopo un riavvio) ───────────────

    def state(self) -> list:
        """Stato serializzabile: lista di [hash, timestamp, real]."""
        return [[h, t, r] for (h, t, r) in self._seen]

    def restore_state(self, data) -> None:
        """Ripristina lo stato da `state()` (tollerante a voci malformate).

        Scarta le voci con timestamp NON FINITO (NaN/inf): `json.load` accetta di default
        `Infinity`/`NaN`, e uno `dedupe_state.json` corrotto/manomesso con `inf`
        bloccherebbe per sempre quell'hash come DUPLICATE attraverso i riavvii
        (`inf >= qualsiasi cutoff`), mentre con `-inf` indebolirebbe il rate-limit. È il
        layer di persistenza dell'anti-doppia-scommessa, quindi fail-closed: voce non finita
        → scartata (mirror di `validators.require_finite_now`), issue #184 H4."""
        restored = []
        for item in data or []:
            try:
                h, t = item[0], item[1]
                # `real` (#192 kyW): assente nei vecchi state a 2 elementi → True (fail-closed: una
                # voce storica è una registrazione reale, conservativa per il rate-limit).
                r = bool(item[2]) if len(item) >= 3 else True
                tf = float(t)
            except (ValueError, TypeError, LookupError):
                # LookupError = IndexError (lista troppo corta) + KeyError (voce dict `{}` da state
                # corrotto/manomesso): va SCARTATA, non deve far crashare `load_state`/lo START.
                continue
            if not math.isfinite(tf):
                continue                    # NaN/inf da state corrotto/manomesso → scartato
            restored.append((str(h), tf, r))
        self._seen = restored


def save_state(tracker: SignalTracker, path: str) -> bool:
    """Salva lo stato del tracker su file JSON **atomicamente** (best-effort) via
    `atomic_io.atomic_write_json`: si scrive un temporaneo e poi `os.replace`. Così
    un'interruzione/errore lascia la history precedente intatta, invece di troncarla
    e perdere la protezione anti-duplicato dopo un riavvio. True se riuscito."""
    try:
        atomic_io.atomic_write_json(path, tracker.state(), prefix=".dedupe_", suffix=".tmp")
        return True
    except OSError:
        return False


def load_state(tracker: SignalTracker, path: str) -> bool:
    """Carica lo stato nel tracker da file JSON (best-effort). True se riuscito;
    file assente/corrotto → lascia il tracker invariato e ritorna False."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, ValueError):
        return False
    if not isinstance(data, list):
        return False
    tracker.restore_state(data)
    return True
