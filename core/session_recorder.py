"""Session Recorder (fase A): la storia di una sessione, registrabile e ricostruibile.

Serve a rispondere a «cosa e' successo davvero mentre lo usavo?» senza doverselo
ricordare. Non introduce un secondo sistema di logging: e' lo strato di SESSIONE
sopra `event_journal`, che il ledger append-only (fsync per evento, riga troncata da
crash gia' gestita, retention) ce l'ha gia'.

Cosa aggiunge, e perche' ognuna di queste cose e' necessaria:

- **identita' di sessione** (`sess`) — due avvii scrivono sullo stesso file; senza,
  ricostruire «questo avvio» significa indovinare dove finisce il precedente;
- **progressivo** (`seq`) — **un buco nella numerazione dice che si e' perso un
  evento**. E' l'unica differenza osservabile fra un ledger completo e uno amputato da
  un disco pieno: senza `seq`, il secondo sembra semplicemente piu' corto;
- **orologio monotono** (`mono`) — `ts` e' wall-clock e puo' saltare all'indietro (NTP,
  sospensione del portatile). Ogni latenza calcolata su `ts` diventa negativa o gonfiata
  *in silenzio*. `mono` non salta;
- **catena causale** (`root`/`corr`) — filtrando per `root` si ha la storia intera di
  una giocata, dal messaggio Telegram al P&L; e da un ordine strano si risale in un
  passo a cosa l'ha prodotto;
- **interruttore** e **campionamento** — perche' registrare tutto significa anche
  registrare cose ripetitive che non servono a nessuno.

Invarianti:

- **Non solleva MAI.** Il recorder e' diagnostico: un suo errore non deve toccare il
  percorso di trading. Ogni scrittura e' best-effort, esattamente come `App._journal`.
- **Non e' un gate.** Non decide niente, non blocca niente, non ritarda niente.
- **Modulo puro**: nessuna dipendenza da GUI/Telegram/Betfair. Testabile headless, con
  orologi iniettabili.

Interruttore, semantica precisa (`ALWAYS_RECORDED`): a recorder SPENTO restano
registrati gli eventi del percorso soldi e dei guasti — ordini, rischio, eccezioni,
apertura/chiusura sessione e tutto il vocabolario LEGACY. Spegnere il recorder riduce
il rumore diagnostico; **non** puo' far sparire la traccia di una scommessa o di un
errore. Il contrario — un interruttore che silenzia anche quelli — trasformerebbe una
preferenza di verbosita' in una perdita di prove.
"""

from __future__ import annotations

import contextlib
import contextvars
import math
import threading
import time
import uuid

from . import event_journal

# Eventi registrati ANCHE a recorder spento: percorso soldi, rischio, guasti e ciclo di
# vita. Il LEGACY e' incluso perche' e' cio' che il bridge scrive gia' oggi: un
# interruttore nuovo, di default spento, non deve togliere una traccia che esisteva.
ALWAYS_RECORDED = frozenset().union(
    event_journal.types_of_category("LEGACY"),
    event_journal.types_of_category("ORDER"),
    event_journal.types_of_category("RISK"),
    {
        "SESSION_START",
        "SESSION_END",
        "UNCAUGHT_EXCEPTION",
        "CRASH_RECOVERY",
        "SHUTDOWN",
        "BF_LOGIN_FAIL",
        "API_ERROR",
        "DB_WRITE_FAIL",
        "TASK_EXCEPTION",
    },
)

# Catena causale corrente. `contextvars` e non variabili d'istanza perche' la catena e'
# una proprieta' del FLUSSO di esecuzione, non del recorder: due thread che processano
# due messaggi diversi devono avere due catene distinte senza passarsele a mano.
#
# Due limiti, di segno opposto, entrambi dichiarati.
#
# 1. Un thread NUOVO non eredita il contesto di chi lo ha creato: una catena che nasce
#    nel thread Telegram e prosegue in quello Tk va ricucita con `chain(root)` nel punto
#    di destinazione. Non e' aggirabile, ed e' voluto — ereditare implicitamente
#    attraverso i thread e' proprio il modo in cui si producono correlazioni SBAGLIATE,
#    peggiori di correlazioni assenti.
#
# 2. Un thread RIUSATO (pool) conserva invece i valori lasciati dal lavoro PRECEDENTE
#    (rilievo di Fugu su #427, accolto). Un `record(..., start_chain=True)` che non
#    venga chiuso lascia la catena appesa: il messaggio successivo servito dallo stesso
#    thread erediterebbe il `root` di quello prima, e il diario mostrerebbe due giocate
#    distinte come una sola — l'errore esattamente opposto a quello che il punto 1
#    evita, e altrettanto grave.
#
#    Per questo la via maestra e' `open_chain(...)`, che apre e CHIUDE la catena per
#    costruzione. La forma `record(..., start_chain=True)` resta per chi controlla lui
#    stesso l'ambito, e va usata solo la' dove la chiusura e' garantita.
_current_root: contextvars.ContextVar = contextvars.ContextVar(
    "pickfair_recorder_root", default=None)
_current_corr: contextvars.ContextVar = contextvars.ContextVar(
    "pickfair_recorder_corr", default=None)

# Quante chiavi di campionamento tenere al massimo. Il campionamento e' per-chiave
# (es. "PRICE_SNAPSHOT:1.234567"): senza tetto, un mercato per riga farebbe crescere il
# dizionario per tutta la sessione.
_SAMPLE_KEYS_MAX = 512


def new_id() -> str:
    """Id opaco per un evento o una catena (stesso formato di `event_journal`)."""
    return uuid.uuid4().hex


class _Sampler:
    """Campionamento per-chiave a intervallo minimo, su orologio MONOTONO.

    `allow(key, min_interval, now)` e' `True` la prima volta e poi solo quando sono
    passati almeno `min_interval` secondi dall'ultimo `True` per quella chiave. Serve
    agli eventi continui (scroll, tick di prezzo): registrarli tutti riempie il ledger
    di righe che non dicono niente e affoga quelle che dicono qualcosa.

    `min_interval <= 0` significa «non campionare»: passa sempre."""

    __slots__ = ("_last", "_lock")

    def __init__(self) -> None:
        self._last: dict = {}
        self._lock = threading.Lock()

    def allow(self, key, min_interval: float, now: float) -> bool:
        if not min_interval or min_interval <= 0:
            return True
        with self._lock:
            previous = self._last.get(key)
            if previous is not None and (now - previous) < min_interval:
                return False
            if len(self._last) >= _SAMPLE_KEYS_MAX and key not in self._last:
                # Tetto raggiunto: si svuota invece di far crescere all'infinito. Il
                # costo e' che qualche chiave riparte da zero (un evento in piu' nel
                # ledger); l'alternativa sarebbe consumare memoria senza limite per un
                # sottosistema diagnostico, che e' il baratto sbagliato.
                self._last.clear()
            self._last[key] = now
            return True


class SessionRecorder:
    """Registratore di sessione sopra il ledger `event_journal`.

    `path` e' il `.jsonl` di destinazione; `None` rende il recorder un no-op completo
    (utile alle istanze headless dei test che non hanno una cartella di config).

    `mono`/`now` sono iniettabili: i test devono poter verificare progressivi e
    campionamento senza dipendere dal tempo reale.
    """

    def __init__(self, path=None, *, enabled: bool = False, session_id=None,
                 mono=time.monotonic, now=time.time) -> None:
        self.path = path or None
        self._enabled = bool(enabled)
        self._mono = mono
        self._now = now
        self._lock = threading.Lock()
        self._seq = 0
        self._sampler = _Sampler()
        self._origin = self._safe_mono()
        self.session_id = str(session_id) if session_id else self._make_session_id()

    # ── identita' e stato ────────────────────────────────────────────────────────
    def _make_session_id(self) -> str:
        """Id di sessione leggibile: `s_<epoch intero>_<4 hex>`.

        La parte casuale serve perche' due processi avviati nello stesso secondo (o un
        riavvio immediato dopo un crash) non devono condividere l'id: sarebbero due
        storie diverse fuse in una."""
        adesso = self._safe_now()
        stamp = int(adesso) if adesso is not None else 0
        return f"s_{stamp}_{uuid.uuid4().hex[:4]}"

    @property
    def enabled(self) -> bool:
        return self._enabled

    def set_enabled(self, value) -> bool:
        """Accende/spegne la registrazione estesa. Ritorna lo stato risultante.

        Non tocca `ALWAYS_RECORDED`: quegli eventi restano registrati comunque."""
        self._enabled = bool(value)
        return self._enabled

    def _safe_mono(self) -> float:
        """Orologio monotono, tollerante a un `mono` iniettato che sollevi."""
        try:
            return float(self._mono())
        except Exception:   # noqa: BLE001
            return 0.0

    def _safe_now(self):
        """Wall-clock come float finito, oppure `None` se l'orologio iniettato e' rotto
        o restituisce un valore assurdo.

        `None` non e' una perdita: `event_journal` ripiega su `time.time()`. Meglio un
        evento con un timestamp reale che nessun evento — il timestamp e' un metadato,
        il fatto che l'evento sia successo no."""
        try:
            valore = float(self._now())
        except Exception:   # noqa: BLE001
            return None
        return valore if math.isfinite(valore) else None

    def elapsed(self) -> float:
        """Secondi monotoni dall'inizio della sessione (mai negativo)."""
        return max(0.0, self._safe_mono() - self._origin)

    def _next_seq(self) -> int:
        """Progressivo di sessione. Sotto lock: `+= 1` non e' atomico e due thread
        potrebbero ricevere lo stesso numero, che e' esattamente cio' che renderebbe il
        rilevamento dei buchi inaffidabile."""
        with self._lock:
            value = self._seq
            self._seq += 1
            return value

    # ── decisione: questo evento va scritto? ─────────────────────────────────────
    def should_record(self, event_type) -> bool:
        """`True` se l'evento va scritto con lo stato attuale dell'interruttore."""
        return self._enabled or event_type in ALWAYS_RECORDED

    # ── catena causale ───────────────────────────────────────────────────────────
    @staticmethod
    def current_chain() -> tuple:
        """`(root, corr)` della catena corrente in questo flusso (`(None, None)` se
        non ce n'e' una)."""
        return _current_root.get(), _current_corr.get()

    @staticmethod
    @contextlib.contextmanager
    def chain(root, corr=None):
        """Lega il blocco a una catena esistente, ripristinando lo stato all'uscita.

        Serve quando la catena attraversa un confine che i `contextvars` non superano —
        tipicamente un altro thread o una callback della GUI: `with recorder.chain(root)`
        nel punto di destinazione ricuce la storia."""
        token_root = _current_root.set(str(root) if root is not None else None)
        token_corr = _current_corr.set(str(corr) if corr is not None else None)
        try:
            yield root
        finally:
            _current_root.reset(token_root)
            _current_corr.reset(token_corr)

    @contextlib.contextmanager
    def open_chain(self, event_type, *, level=None, **data):
        """Apre una catena registrando l'evento capostipite, e la CHIUDE all'uscita.

        E' la forma da preferire: su un thread di un pool la catena non sopravvive al
        lavoro corrente, quindi il messaggio successivo servito dallo stesso thread non
        eredita il `root` di quello prima.

        Restituisce l'id del capostipite, oppure `None` se l'evento non e' stato scritto
        (recorder spento, path assente): in quel caso il blocco gira lo stesso, e gli
        eventi interni semplicemente non avranno `root`. Un recorder spento non deve
        cambiare il flusso del chiamante.

            with recorder.open_chain("TG_MESSAGE_IN", chat=impronta) as root:
                ...   # tutto cio' che segue porta questo `root`
        """
        precedente_root = _current_root.get()
        precedente_corr = _current_corr.get()
        radice = self.record(event_type, level=level, start_chain=True, **data)
        try:
            yield radice
        finally:
            _current_root.set(precedente_root)
            _current_corr.set(precedente_corr)

    @staticmethod
    def clear_chain() -> None:
        """Chiude la catena corrente (gli eventi successivi non avranno `root`)."""
        _current_root.set(None)
        _current_corr.set(None)

    # ── registrazione ────────────────────────────────────────────────────────────
    def record(self, event_type, *, level=None, corr=None, root=None,
               start_chain: bool = False, sample_key=None, min_interval: float = 0.0,
               **data):
        """Registra un evento. Ritorna il suo id, oppure `None` se non e' stato scritto.

        `None` non e' un errore: significa «non registrato», e i motivi legittimi sono
        tre — recorder spento e tipo non in `ALWAYS_RECORDED`, campionamento che ha
        scartato la ripetizione, path non impostato.

        - `start_chain=True` apre una catena: l'evento diventa il `root` di quelli
          successivi nello stesso flusso (tipicamente il messaggio Telegram in arrivo).
          **Non la chiude**: su un thread di un pool la catena resterebbe appesa e il
          lavoro successivo erediterebbe questo `root`. Salvo che l'ambito sia sotto il
          controllo del chiamante, usare `open_chain(...)`, che chiude per costruzione;
        - `corr`/`root` espliciti vincono sulla catena corrente, per i casi in cui il
          nesso e' noto al chiamante meglio che al contesto;
        - `sample_key` + `min_interval` campionano gli eventi continui.

        I nomi `level`, `corr`, `root`, `start_chain`, `sample_key` e `min_interval` sono
        RISERVATI: un campo di `data` che si chiamasse cosi' verrebbe interpretato come
        parametro. Sono nomi improbabili in un payload, e la firma esplicita vale piu' di
        un dizionario passato a mano.

        **Non solleva mai**: tipo sconosciuto, disco pieno, JSON non serializzabile —
        qualunque errore fa tornare `None`. Il ledger e' uno strumento di ricostruzione,
        non un anello del percorso di trading.

        Nota sul progressivo: il numero e' consumato PRIMA della scrittura, quindi una
        scrittura fallita lascia un buco nella numerazione. E' voluto — un evento e'
        andato davvero perso, e il buco e' esattamente il modo in cui deve vedersi. Gli
        eventi scartati per l'interruttore o per il campionamento invece NON consumano
        progressivo: quelli non sono perdite, sono scelte, e non devono somigliare a un
        guasto quando si rilegge il diario."""
        try:
            if not self.path or not self.should_record(event_type):
                return None
            mono = self._safe_mono()
            if sample_key is not None and not self._sampler.allow(
                    sample_key, min_interval, mono):
                return None

            chain_root, chain_corr = self.current_chain()
            # L'id si genera QUI, non dentro `append_event`: l'evento che apre una catena
            # deve poter portare come `root` il PROPRIO id, e per farlo bisogna conoscerlo
            # prima di scriverlo. Cosi' filtrare per `root` restituisce anche il messaggio
            # che ha originato la giocata, non soltanto cio' che ne e' seguito.
            event_id = new_id()
            if start_chain:
                effective_root = root if root is not None else event_id
                # Il capostipite non ha una causa: `corr` resta assente, altrimenti la
                # catena sembrerebbe iniziare da qualcosa che non c'e'.
                effective_corr = corr
            else:
                effective_root = root if root is not None else chain_root
                effective_corr = corr if corr is not None else chain_corr

            event_journal.append_event(
                self.path, event_type, data,
                now=self._safe_now(),
                event_id=event_id,
                sess=self.session_id,
                seq=self._next_seq(),
                mono=mono,
                lvl=level,
                corr=effective_corr,
                root=effective_root,
            )
            if start_chain:
                _current_root.set(effective_root)
                _current_corr.set(event_id)
            elif effective_root is not None:
                # Catena in corso: il prossimo evento avra' come `corr` QUESTO, cosi' la
                # catena e' una lista concatenata (chi ha causato chi) e non un ventaglio
                # in cui tutto punta alla radice.
                _current_corr.set(event_id)
            return event_id
        except Exception:   # noqa: BLE001,S110 — diagnostico: mai propagare nel trading
            return None     # (niente log qui: il sink di log potrebbe fallire a sua volta)
