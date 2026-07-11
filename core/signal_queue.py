"""PR-16: coda dei segnali attivi (logica pura, testabile).

Gestisce uno o più segnali "attivi" (le righe che XTrader dovrebbe vedere nel
CSV) con un **timeout per singolo segnale** e tre modalità:

- ``OVERWRITE_LAST`` (default): un solo segnale attivo alla volta — un nuovo
  segnale sostituisce il precedente. È il comportamento storico del bridge
  (one-signal-at-a-time), il più conservativo per il rischio di doppia scommessa.
- ``APPEND_ACTIVE``: più segnali attivi contemporaneamente (più righe), ognuno
  con il proprio timeout.
- ``QUEUE_UNTIL_CONFIRMED``: come append, ma i segnali restano finché non sono
  **confermati** (o scadono per timeout).

Invarianti di sicurezza preservate:
- nessun **vecchio segnale** resta per sempre: ogni segnale scade comunque per
  timeout, anche se mai confermato;
- la coda gestisce **solo le righe**; non scrive il CSV e non tocca l'header (lo
  mantiene `csv_writer`). L'aggancio al runtime è un passo successivo.

Modulo puro: nessuna dipendenza da GUI/CSV/Telegram, interamente testabile.
"""

import math
import time
from dataclasses import dataclass, field

OVERWRITE_LAST = "OVERWRITE_LAST"
APPEND_ACTIVE = "APPEND_ACTIVE"
QUEUE_UNTIL_CONFIRMED = "QUEUE_UNTIL_CONFIRMED"
MODES = (OVERWRITE_LAST, APPEND_ACTIVE, QUEUE_UNTIL_CONFIRMED)
DEFAULT_MODE = OVERWRITE_LAST
# Fail-safe del timeout per-segnale quando il config non fornisce un valore valido.
# Coincide *intenzionalmente* con il default dell'auto-clear (settings_validation.
# DEFAULT_TIMEOUT = 90): è lo stesso "default timeout" del bridge. Resta una costante
# PROPRIA di questo modulo (pura, autocontenuta) per non dipendere dal layer settings.
DEFAULT_TIMEOUT = 90        # secondi di vita di un segnale se non confermato/sostituito


def normalize_mode(mode) -> str:
    """Normalizza la modalità a una di MODES; valore ignoto/mancante → DEFAULT_MODE
    (il default conservativo: un solo segnale attivo)."""
    m = str(mode or "").strip().upper()
    return m if m in MODES else DEFAULT_MODE


def timeout_from_config(cfg) -> float:
    """Timeout per-segnale della coda ricavato dal config (PR-17b).

    - In ``QUEUE_UNTIL_CONFIRMED`` il timeout è ``confirmation_timeout`` (per quanto
      tempo un segnale resta in attesa della conferma XTrader prima di scadere);
    - nelle altre modalità (``OVERWRITE_LAST``/``APPEND_ACTIVE``) è ``clear_delay``
      (auto-clear), come storicamente.

    Fail-safe: un valore mancante/non valido (non numerico, ``NaN``/``inf``, ``<=0``)
    ricade su ``DEFAULT_TIMEOUT`` — un segnale deve scadere COMUNQUE (mai immortale)."""
    cfg = cfg if isinstance(cfg, dict) else {}
    key = "confirmation_timeout" if normalize_mode(cfg.get("queue_mode")) == QUEUE_UNTIL_CONFIRMED \
        else "clear_delay"
    raw = cfg.get(key)
    # Rifiuta i bool PRIMA di float(): `float(True)` è `1.0` e bypasserebbe il
    # fail-safe → ogni segnale scadrebbe dopo 1s (la riga sparirebbe prima che XTrader
    # la legga, polling 10–15s). `True`/`False` da JSON = config malformata → default.
    if isinstance(raw, bool):
        return DEFAULT_TIMEOUT
    try:
        t = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT
    return t if math.isfinite(t) and t > 0 else DEFAULT_TIMEOUT


def delay_until(expires_at: float, now: float) -> float:
    """Ritardo (secondi, mai negativo) prima di `expires_at` rispetto a `now`, sullo
    STESSO clock monotòno della coda (audit A3).

    Una scadenza già passata (`expires_at <= now`) → `0.0`: il tick di scadenza parte
    subito, senza un ritardo negativo che manderebbe il timer in busy-loop o lo farebbe
    scattare nel passato. Usata da `App._schedule_expiry` con `next_expiry()`."""
    return max(0.0, expires_at - now)


@dataclass
class ActiveSignal:
    """Un segnale attualmente attivo nella coda."""

    signal_id: str
    row: dict
    added_at: float
    timeout: float
    # Chiave di deduplica PER-RIGA (#192) con cui il segnale è stato piazzato: memorizzata al
    # momento dell'add così il commit multi può riconoscere una riga duplicata che è ANCORA attiva
    # con la PROVENIENZA esatta (stessa chiave = stesso messaggio/riga), invece di ricalcolarla dal
    # testo corrente combinato con righe di altri messaggi (Codex #281). `""` se non fornita.
    dedup_key: str = ""

    def expires_at(self) -> float:
        return self.added_at + self.timeout


@dataclass
class SignalQueue:
    """Coda dei segnali attivi. Pura: gestisce righe, non scrive il CSV."""

    mode: str = DEFAULT_MODE
    default_timeout: float = DEFAULT_TIMEOUT
    # Tetto di righe attive simultanee (anti-overbetting nelle modalità multi-riga, #136
    # punto 5). 0 = nessun tetto (default, retro-compatibile). Un NUOVO segnale oltre il
    # tetto viene BLOCCATO (`add` ritorna None); un aggiornamento di un segnale già attivo
    # non è bloccato. OVERWRITE_LAST tiene sempre una sola riga → il tetto è ininfluente.
    max_active: int = 0
    _active: list = field(default_factory=list)   # ActiveSignal, in ordine d'arrivo
    _counter: int = 0

    def __post_init__(self):
        self.mode = normalize_mode(self.mode)
        self.default_timeout = self._validate_timeout(self.default_timeout)
        self.max_active = self._validate_max_active(self.max_active)

    @staticmethod
    def _validate_max_active(value) -> int:
        """Tetto come intero >= 0 (0 = illimitato). Un valore malformato (bool/NaN/inf/
        negativo/non intero) → 0 (illimitato, fail-safe: non blocca segnali per sbaglio)."""
        if isinstance(value, bool):
            return 0
        try:
            f = float(value)
        except (TypeError, ValueError):
            return 0
        if not math.isfinite(f) or f < 0 or f != int(f):
            return 0
        return int(f)

    @staticmethod
    def _validate_timeout(value) -> float:
        """Un timeout deve essere un numero FINITO e > 0. Un valore non valido
        (None già gestito a monte, ma anche `NaN`/`inf`/negativo/non numerico)
        romperebbe l'invariante "nessun vecchio segnale resta attivo per sempre":
        es. con `NaN`, `expires_at()` non è mai `<= now` e il segnale non
        scadrebbe MAI. Quindi si fallisce subito (fail-fast)."""
        try:
            t = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"timeout non valido: {value!r}")
        if not math.isfinite(t) or t <= 0:
            raise ValueError(f"timeout deve essere un numero finito > 0 (ricevuto {value!r})")
        return t

    @staticmethod
    def _resolve_now(now) -> float:
        """`now` corrente se assente; altrimenti il valore fornito, che deve essere
        FINITO. Un `now` `NaN`/`inf` salvato in `added_at` renderebbe `expires_at()`
        mai `<= now` → segnale immortale: stessa ragione per cui si valida il timeout.

        Il default usa **`time.monotonic()`** (audit A3): la scadenza è una decisione su
        TEMPO TRASCORSO e la coda è **in-memory** (non persistita su disco), quindi un salto
        del wallclock (NTP/VM-pause) NON deve far scadere in anticipo un segnale ancora valido
        (riga rimossa dal CSV → bet mancata) né tenerne uno scaduto. Il chiamante (`app`) passa
        coerentemente `time.monotonic()` per `expire`/`add` e per programmare il tick."""
        if now is None:
            return time.monotonic()
        try:
            t = float(now)
        except (TypeError, ValueError):
            raise ValueError(f"now non valido: {now!r}")
        if not math.isfinite(t):
            raise ValueError(f"now deve essere un numero finito (ricevuto {now!r})")
        return t

    def add(self, row: dict, *, signal_id: str = None, now: float = None,
            timeout: float = None, force: bool = False, dedup_key: str = None):
        """Aggiunge un segnale e ritorna il suo `signal_id`, oppure ``None`` se è stato
        BLOCCATO dal tetto `max_active` (#136 punto 5).

        - ``OVERWRITE_LAST``: sostituisce tutti i segnali attivi con questo (mai bloccato);
        - altre modalità: aggiorna se `signal_id` è già presente, altrimenti accoda — ma un
          NUOVO segnale che porterebbe oltre `max_active` (se > 0) viene **bloccato**
          (ritorna ``None``, coda invariata) così non si superano N scommesse simultanee.

        `force=True` (auto-raise del tetto, decisione proprietario #192): accoda il segnale
        SENZA applicare il tetto `max_active`. Serve al commit MULTI-RIGA (`commit_signals`) per
        NON spezzare mai il blocco coerente di UN singolo messaggio: tutte le righe di
        quell'istruzione restano attive insieme invece di essere troncate al tetto (che
        lascerebbe righe fuori in silenzio). Non incide sull'aggiornamento di un segnale già
        attivo (mai bloccato comunque) né su `OVERWRITE_LAST`.

        `dedup_key` (#192): chiave di deduplica per-riga con cui la riga è piazzata, memorizzata
        sul segnale per il riconoscimento provenienza-esatta dei duplicati ancora attivi.

        `signal_id` assente → generato automaticamente. `timeout` assente →
        `default_timeout`."""
        now = self._resolve_now(now)
        timeout = self.default_timeout if timeout is None else self._validate_timeout(timeout)
        if signal_id is None:
            # Id auto-generato che NON collide con un id fornito dal chiamante:
            # altrimenti un "s1" esplicito verrebbe sovrascritto dal primo add()
            # senza id (stesso "s1") e una riga ancora attiva sparirebbe.
            existing = {a.signal_id for a in self._active}
            self._counter += 1
            while f"s{self._counter}" in existing:
                self._counter += 1
            signal_id = f"s{self._counter}"
        sig = ActiveSignal(signal_id, dict(row), now, timeout, str(dedup_key or ""))
        if self.mode == OVERWRITE_LAST:
            self._active = [sig]
            return signal_id
        is_update = any(a.signal_id == signal_id for a in self._active)
        # Tetto: blocca SOLO un nuovo segnale (non l'aggiornamento di uno già attivo) che
        # porterebbe il numero di righe attive oltre `max_active` (#136 punto 5). `force=True`
        # bypassa il tetto per NON spezzare il blocco di un singolo messaggio multi (#192).
        if not force and not is_update and self.max_active and len(self._active) >= self.max_active:
            return None
        self._active = [a for a in self._active if a.signal_id != signal_id]
        self._active.append(sig)
        return signal_id

    def replace_block(self, rows, *, now: float = None, timeout: float = None,
                      keys=None) -> list:
        """Sostituisce TUTTI i segnali attivi con il BLOCCO `rows`: un'unica «ultima istruzione»
        composta da più righe (#192). Usato dal commit MULTI-RIGA in `OVERWRITE_LAST`, dove un
        messaggio che genera N righe le tiene TUTTE attive sostituendo il blocco precedente (l'add
        per-riga, invece, ne lascerebbe attiva una sola). Ogni riga riceve un `signal_id` proprio.

        `keys` (#192): lista parallela a `rows` con le chiavi di deduplica per-riga, memorizzate sui
        segnali per il riconoscimento provenienza-esatta dei duplicati ancora attivi al commit
        successivo. Assente → chiavi vuote.

        Ritorna la lista degli id (vuota se `rows` è vuota → coda svuotata)."""
        now = self._resolve_now(now)
        timeout = self.default_timeout if timeout is None else self._validate_timeout(timeout)
        keys = list(keys or [])
        sigs = []
        for i, row in enumerate(rows or []):
            self._counter += 1
            k = str(keys[i]) if i < len(keys) else ""
            sigs.append(ActiveSignal(f"s{self._counter}", dict(row), now, timeout, k))
        self._active = list(sigs)
        return [s.signal_id for s in sigs]

    def expire(self, now: float = None) -> list:
        """Rimuove i segnali scaduti (timeout raggiunto) e ne ritorna gli id.
        Garantisce che nessun vecchio segnale resti attivo per sempre."""
        now = self._resolve_now(now)
        expired = [a.signal_id for a in self._active if a.expires_at() <= now]
        if expired:
            self._active = [a for a in self._active if a.expires_at() > now]
        return expired

    def remove(self, signal_id: str) -> bool:
        """Rimuove un segnale per id. True se era presente."""
        before = len(self._active)
        self._active = [a for a in self._active if a.signal_id != signal_id]
        return len(self._active) < before

    def confirm(self, signal_id: str) -> bool:
        """Un segnale confermato (es. da XTrader, PR-17) viene rimosso dagli attivi.
        Alias semantico di `remove`."""
        return self.remove(signal_id)

    def active_ids(self) -> list:
        return [a.signal_id for a in self._active]

    def active_keys(self, now: float = None) -> list:
        """Chiavi di deduplica per-riga dei segnali attualmente attivi (#192), nello stesso ordine
        di `active_rows`. Con `now` le righe già scadute sono escluse (come `active_rows`). Serve al
        commit MULTI-RIGA per riconoscere, con la PROVENIENZA esatta, una riga duplicata che è
        ANCORA attiva (stessa chiave = stessa riga già piazzata), senza ricalcolare la chiave dal
        testo corrente combinato con righe di altri messaggi (Codex #281)."""
        if now is None:
            return [a.dedup_key for a in self._active]
        now = self._resolve_now(now)
        return [a.dedup_key for a in self._active if a.expires_at() > now]

    def active_rows(self, now: float = None) -> list:
        """Righe attualmente attive (copie difensive), in ordine d'arrivo. È ciò
        che andrebbe scritto nel CSV sotto l'header.

        Con `now` (clock monotòno, audit A3) le righe **già scadute** vengono
        **escluse** dal risultato: così, anche se il chiamante non ha invocato
        `expire()` subito prima, una riga oltre il suo timeout non può essere
        ESPOSTA/SCRITTA come attiva (#30, Codex). È una lettura **pura** (non muta la
        coda — la rimozione effettiva resta a `expire()`); `now=None` → tutte le
        attive (retro-compatibile)."""
        if now is None:
            return [dict(a.row) for a in self._active]
        now = self._resolve_now(now)
        return [dict(a.row) for a in self._active if a.expires_at() > now]

    def is_empty(self) -> bool:
        return not self._active

    def pending(self) -> list:
        """Segnali attivi nel formato atteso da `confirmation_reader.interpret`:
        ogni voce è `{"signal_id": id, ...campi della riga}` (copia difensiva). Serve
        ad associare una notifica XTrader a un segnale in attesa (PR-23)."""
        return [{"signal_id": a.signal_id, **dict(a.row)} for a in self._active]

    def next_expiry(self, default=None):
        """L'istante di scadenza (`expires_at`) più vicino fra i segnali attivi, o
        `default` se la coda è vuota. Serve a programmare il prossimo controllo di
        scadenza al momento giusto invece che con un ritardo fisso (così un segnale
        più vecchio non resta oltre il suo timeout quando ne arrivano di nuovi)."""
        return min((a.expires_at() for a in self._active), default=default)

    def state(self) -> list:
        """Stato serializzabile (snapshot per rollback / persistenza): lista di dict
        con `signal_id`, `row`, `added_at`, `timeout`, `dedup_key`."""
        return [{"signal_id": a.signal_id, "row": dict(a.row),
                 "added_at": a.added_at, "timeout": a.timeout,
                 "dedup_key": a.dedup_key} for a in self._active]

    def restore_state(self, data) -> None:
        """Ripristina gli attivi da `state()` (tollerante a voci malformate). Usato
        per annullare un add quando la scrittura del CSV fallisce, riportando la coda
        allo stato precedente (allineato a ciò che è ancora su disco)."""
        restored = []
        for item in data or []:
            try:
                restored.append(ActiveSignal(
                    str(item["signal_id"]), dict(item["row"]),
                    self._resolve_now(item["added_at"]),
                    self._validate_timeout(item["timeout"]),
                    str(item.get("dedup_key", "") or "")))
            except (KeyError, TypeError, ValueError):
                continue
        self._active = restored
