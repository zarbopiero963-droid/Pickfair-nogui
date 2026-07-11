"""PR-19: guardrail di sicurezza (logica pura, testabile headless).

Riduce il rischio di uso pericoloso del bridge senza trasformarlo in un bot di
puntata aggressivo. Tre responsabilità, tutte **pure** (nessuna GUI/CSV/Telegram):

- **DRY_RUN (simulazione)**: `is_dry_run(cfg)` / `should_write_operational_csv(cfg)`
  decidono se il CSV operativo va scritto. Default **sicuro**: se il campo manca
  (config vecchia o prima installazione) il bridge è in simulazione (`True`), così
  non si genera una scommessa reale per sbaglio dopo un aggiornamento.
- **Warning modalità reale**: `real_mode_warning(cfg)` ritorna un avviso quando la
  simulazione è disattivata, che la GUI mostra come banner (qui solo il testo).
- **Limite giornaliero**: `DailyLimiter` applica un tetto di segnali **al giorno**
  (UTC) con **reset automatico** al cambio di data. Complementare al limite/minuto
  già in `signal_dedupe` (PR-15). Fail-safe: parametri non validi → ValueError.

**Sola decisione**: questo modulo non scrive il CSV e non piazza scommesse; il
wiring GUI/runtime (toggle, banner, blocco START) è un passo successivo.
"""

import json
import time
from dataclasses import dataclass

from . import atomic_io, validators

DEFAULT_MAX_PER_DAY = 200      # tetto di segnali nuovi accettati in un giorno (UTC)

# Sentinel "giorno sconosciuto": stato fresco o `day` corrotto/non interpretabile. `_roll` lo
# tratta fail-closed (adotta oggi CONSERVANDO il conteggio), distinto da un giorno valido
# diverso da oggi (→ reset legittimo). Fonte unica per non ripetere `self._day or ""` (Sourcery).
_UNKNOWN_DAY = ""

# Valori stringa interpretati come "spento" = modalità REALE (per config che
# arrivano da campi testuali GUI o da un vecchio config.json scritto a mano).
# NB: solo valori OFF **espliciti**. Vuoto / `None` / `"none"` NON sono qui: un
# valore non impostato o malformato deve fallire **chiuso** in simulazione, mai
# abilitare la scrittura del CSV reale (fail-safe).
_FALSEY = {"0", "false", "no", "off", "n"}


def is_dry_run(cfg) -> bool:
    """True se il bridge è in **simulazione** (non deve scrivere il CSV operativo).

    Default **sicuro**: campo assente → `True`. Un valore esplicito viene
    interpretato in modo robusto: bool diretto, oppure stringa
    (``"false"/"0"/"no"/"off"`` → False; tutto il resto non vuoto → True)."""
    if not isinstance(cfg, dict) or "dry_run" not in cfg:
        return True
    val = cfg.get("dry_run")
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return val != 0
    return str(val).strip().lower() not in _FALSEY


def should_write_operational_csv(cfg) -> bool:
    """True se è consentito scrivere il CSV operativo (cioè NON in simulazione)."""
    return not is_dry_run(cfg)


def real_mode_warning(cfg) -> str:
    """Testo di avviso quando la simulazione è **disattivata** (modalità reale), da
    mostrare nella GUI. Stringa vuota se in simulazione (nessun avviso)."""
    if is_dry_run(cfg):
        return ""
    return ("ATTENZIONE: modalità REALE attiva — i segnali vengono scritti nel CSV "
            "operativo e XTrader può piazzare scommesse reali. Usa la simulazione "
            "(DRY_RUN) per i test.")


def _fmt_day(year: int, mon: int, mday: int) -> str:
    """Forma canonica zero-padded ``YYYY-MM-DD`` (fonte unica per `_day_key`/`_is_valid_day`)."""
    return f"{year:04d}-{mon:02d}-{mday:02d}"


def _day_key(now: float) -> str:
    """Chiave del giorno **LOCALE** ``YYYY-MM-DD`` per `now` (epoch), secondo il fuso e
    l'ora legale del sistema operativo (#311-3.5-f). Così il reset del tetto giornaliero
    segue la mezzanotte **locale** dell'utente, non quella UTC (per un'app desktop il fuso del
    PC è quello dell'utente).

    **DST (ora legale): gestito dal SO.** Il confronto del rollover è sulla **DATA di
    calendario** (`YYYY-MM-DD`), non sulle ore: un giorno di cambio ora — che dura 23 o 25 ore —
    non salta né duplica una data, quindi l'ordinamento cronologico delle chiavi (e con esso le
    invarianti fail-closed di `_roll`/`allow`) resta corretto. Un salto DST avviene a notte fonda
    entro lo stesso giorno di calendario, non ne crea uno nuovo."""
    t = time.localtime(now)
    return _fmt_day(t.tm_year, t.tm_mon, t.tm_mday)


def _is_valid_day(day) -> bool:
    """True se `day` è una data di CALENDARIO reale in forma canonica ``YYYY-MM-DD`` (quella
    prodotta da `_day_key`).

    Non basta il formato (Codex P1 / Sourcery): una data IMPOSSIBILE come ``2026-99-99``
    supererebbe un controllo solo-regex e, differendo dalla chiave di oggi, farebbe azzerare
    il conteggio in `_roll` → cap giornaliero pieno (overtrading, fail-open). `strptime` valida
    i range di mese/giorno; il confronto con la forma canonica zero-padded esclude varianti non
    canoniche (es. ``2026-1-1``, spazi). Tutto ciò che non è una data canonica reale è trattato
    come `_UNKNOWN_DAY` (fail-closed): il conteggio NON viene mai scartato su uno stato corrotto."""
    if not isinstance(day, str):
        return False
    try:
        t = time.strptime(day, "%Y-%m-%d")
    except ValueError:
        return False
    return day == _fmt_day(t.tm_year, t.tm_mon, t.tm_mday)


@dataclass
class DailyLimiter:
    """Tetto di segnali **al giorno** (ora **locale** del SO, #311-3.5-f) con reset automatico
    al cambio data.

    `allow(now)` ritorna True se c'è ancora capienza nel giorno corrente (e in tal
    caso conta il segnale), False se il tetto è raggiunto. Lo stato è serializzabile
    (`state`/`restore_state`) così il conteggio sopravvive a un riavvio nello stesso
    giorno (non si azzera ripartendo l'app)."""

    max_per_day: int = DEFAULT_MAX_PER_DAY
    _day: str = ""
    _count: int = 0

    def __post_init__(self):
        self.max_per_day = validators.require_positive_int(self.max_per_day, "max_per_day")

    def _roll(self, now: float) -> None:
        key = _day_key(now)
        if key == self._day:
            return
        # Giorno diverso da oggi. Se `_day` è una data VALIDA (YYYY-MM-DD) → è un nuovo giorno:
        # reset del conteggio (comportamento normale). Se invece è MALFORMATO/vuoto — stato
        # corrotto o manomesso (M4 #184) — NON si azzera: non si sa a quale giorno appartenga il
        # conteggio e fidarsi darebbe un cap PIENO oggi (overtrading, fail-OPEN). Si adotta il
        # giorno corrente CONSERVANDO il conteggio (fail-CLOSED): al più si è più restrittivi
        # oggi, mai più permissivi; al prossimo giorno reale (con `_day` valido) si azzererà.
        # `_is_valid_day` valida il CALENDARIO, non solo il formato: una data impossibile
        # (`2026-99-99`) da stato corrotto è UNKNOWN, non un giorno valido → niente reset.
        if _is_valid_day(self._day):
            # F3 #258: reset SOLO se il nuovo giorno è strettamente FUTURO (le chiavi
            # YYYY-MM-DD zero-padded ordinano cronologicamente). Un salto dell'orologio
            # all'INDIETRO (skew NTP, regolazione manuale) non deve riaprire un tetto già
            # consumato (fail-open): giorno e conteggio restano quelli correnti finché il
            # tempo reale non raggiunge di nuovo `_day` — al più si è più restrittivi
            # (anche se `_day` fosse finito nel futuro per un orologio poi corretto), mai
            # più permissivi.
            if key > self._day:
                self._count = 0
                self._day = key
            return
        self._day = key

    def allow(self, *, now: float = None) -> bool:
        """True se il segnale è ammesso oggi (e lo conta); False se tetto raggiunto.

        Durante uno skew dell'orologio all'INDIETRO (giorno registrato ancora nel futuro
        rispetto a `now`) è sempre False: vedi il commento nel corpo (#306 Codex P1)."""
        now = time.time() if now is None else now
        now = validators.require_finite_now(now)
        self._roll(now)
        # #306 Codex P1: se DOPO il roll il giorno registrato è ancora diverso da oggi,
        # siamo in skew all'indietro (orologio corretto dopo un salto avanti oltre
        # mezzanotte, o stato persistito con giorno futuro). Il conteggio in memoria
        # appartiene a un ALTRO giorno: scalarlo qui riaprirebbe un cap già consumato —
        # il salto avanti l'ha appena azzerato (es. cap 2 pieno oggi → skew a domani con
        # reset+1 → correzione a oggi con count=1 < 2 → segnale accettato, fail-open).
        # Fail-closed DURO: nessun segnale finché il tempo reale non raggiunge `_day`.
        if self._day != _day_key(now):
            return False
        if self._count >= self.max_per_day:
            return False
        self._count += 1
        return True

    def release(self) -> None:
        """Restituisce UNA slot consumata da un precedente `allow()` (decremento, mai sotto 0),
        **mantenendo** il giorno corrente già normalizzato da `_roll`. Serve a disfare il consumo
        di un esito che poi NON ha scritto (es. DRY_RUN), senza riportare indietro la
        normalizzazione del giorno: ripristinare un intero snapshot del limiter rimetterebbe un
        giorno corrotto (state file malformato) e bloccherebbe per sempre il tetto (#184 Codex)."""
        if self._count > 0:
            self._count -= 1

    def remaining(self, *, now: float = None) -> int:
        """Segnali ancora ammessi nel giorno corrente (senza consumarne)."""
        now = time.time() if now is None else now
        now = validators.require_finite_now(now)
        self._roll(now)
        # Skew all'indietro: capacità ZERO, coerente con `allow` (#306 Codex P1).
        if self._day != _day_key(now):
            return 0
        return max(0, self.max_per_day - self._count)

    def state(self) -> dict:
        """Stato serializzabile (per sopravvivere a un riavvio nello stesso giorno)."""
        return {"day": self._day, "count": self._count}

    def restore_state(self, data) -> bool:
        """Ripristina lo stato da `state()` (tollerante a dati malformati). Ritorna ``True``
        se lo stato è stato effettivamente applicato, ``False`` se i dati erano malformati
        (limiter lasciato invariato) — così `load_state` distingue un restore reale da un no-op."""
        if not isinstance(data, dict):
            return False
        day = data.get("day")
        count = data.get("count")
        # `not isinstance(count, bool)` (#184 low-bool-count): `isinstance(True, int)` è True, quindi
        # un `daily_state.json` corrotto/manomesso con `"count": true/false` verrebbe accettato come
        # 1/0 invece di essere scartato come malformato. Un conteggio è un intero, non un booleano:
        # un bool → restore fail-closed (return False, limiter invariato), come gli altri dati invalidi.
        if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
            # `day` data di calendario reale e canonica → usato così com'è; altrimenti (malformato,
            # data impossibile, non-stringa) → `_UNKNOWN_DAY` (M4 #184). Il conteggio NON viene
            # scartato (sarebbe un cap pieno = overtrading): è attribuito al giorno corrente da
            # `_roll` (fail-closed).
            self._day = day if _is_valid_day(day) else _UNKNOWN_DAY
            self._count = count
            return True
        return False


def save_state(daily: DailyLimiter, path: str) -> bool:
    """Salva lo stato del DailyLimiter su file JSON **atomicamente** (audit #105 P2): via
    `atomic_io.atomic_write_json` (`.tmp` nella stessa cartella + ``flush`` + ``os.fsync`` +
    ``os.replace``, con rimozione del temporaneo su errore) — esattamente come
    `signal_dedupe.save_state`. Prima il salvataggio era best-effort SENZA fsync: in
    crash/blackout l'ultimo conteggio giornaliero poteva perdersi, riducendo la protezione
    anti-overtrading dopo un riavvio. True se riuscito, False su errore di I/O."""
    try:
        atomic_io.atomic_write_json(path, daily.state(), prefix=".guard_", suffix=".tmp")
        return True
    except OSError:
        return False


def load_state(daily: DailyLimiter, path: str) -> bool:
    """Carica lo stato nel DailyLimiter da file JSON (best-effort). True se riuscito;
    file assente/corrotto/malformato → lascia il limiter invariato e ritorna False."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, ValueError):
        return False
    # Propaga l'esito reale del restore: un JSON valido ma con struttura inattesa
    # (ignorato da restore_state) ritorna False, non un falso "caricato" (Sourcery).
    return daily.restore_state(data)
