"""D-1 (P0) — hard-stop del drawdown assoluto su cassa verificata, con picco persistente.

Decisione owner PF-ROSERPINA-D1-D2-B2-B3-FINANCIAL-SAFETY-20261109 (#586 c.6747859283), in continuita' con
#426 c.6146717266 («non confondere il saldo disponibile con l'equity», «non azzerare perdite precedenti»).

Prima il controllo era ``RiskDesk.equity_peak - bankroll_current``, entrambi sul saldo DISPONIBILE:
- l'auto-reset Roserpina (``reset_recovery_cycle``) ribasava il picco e cancellava la perdita (campagna Grok DD5:
  16,86 EUR spariti);
- aprire una puntata abbassa il disponibile, quindi il picco seguiva il disponibile e non la cassa (91,07 di cassa
  contro 87,11 di disponibile: perdita reale 13,57 misurata 9,61);
- il picco viveva solo in memoria: un riavvio lo ricominciava dal saldo corrente.

Qui il picco (``peak``) e' il massimo della CASSA VERIFICATA (disponibile + impegnato della stessa lettura,
PFC-008): una riserva o una puntata aperta spostano denaro dal disponibile all'impegnato e lasciano la cassa
invariata, quindi non sono perdite. Il picco:
- sale con ogni cassa verificata osservata, si salva PRIMA di valere (``observe``);
- non scende MAI per auto-reset, reset di ciclo o riavvio: nessuna API lo ribasa;
- e' persistito in modo atomico nella tabella settings (chiave ``STATE_KEY``) con ``seq`` monotona.

Fail-closed (con ``max_drawdown_abs`` configurato): stato persistito illeggibile, salvataggio del picco fallito o
cassa corrente non verificata => nessun nuovo ingresso, con motivo esplicito. Senza ``max_drawdown_abs`` il
modulo osserva e salva il picco ma non blocca nulla (nessun cap monetario di default, rettifica owner 18/10).

Importi in Decimal (D-3): perdita = picco - cassa, confronto esatto con la soglia.

Contratto owner PF-ROSERPINA-DRAWDOWN-AND-RECOVERY-20261109: ``drawdown_abs = max(0, picco - cassa)``; il controllo
avviene prima di ogni ingresso E a ogni lettura verificata del conto (``evaluate``, chiamato dal runtime a ogni
variazione finanziaria); il superamento si MEMORIZZA (``breached``, persistito con il picco) e mantiene il blocco
anche se la cassa risale, finche' l'owner non lo sblocca con la procedura esplicita e tracciata
``owner_clear_breach`` (nessun percorso automatico la chiama). Lo stesso vale per il ribasamento del picco
(``owner_rebase_peak``): mai conseguenza automatica di una perdita.

Limite dichiarato: senza stato persistito (prima esecuzione dopo l'aggiornamento) il picco parte dalla prima cassa
verificata osservata. Non e' una ricostruzione dello storico (quella resta il metodo owner del 13/10, opzione A).
"""
from __future__ import annotations

import bisect
import json
import math
import logging
import os
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from core import validators
from core.atomic_io import atomic_write_text
from database import RuntimeFenceViolation

logger = logging.getLogger(__name__)

STATE_KEY = "roserpina.drawdown_hard_stop_state"

REASON_INVALID = "max_drawdown_abs_non_valido"
REASON_ACTIVE = "max_drawdown_abs_active"
REASON_STATE_UNREADABLE = "max_drawdown_abs_stato_illeggibile"
REASON_NOT_PERSISTED = "max_drawdown_abs_picco_non_persistito"
REASON_CASH_UNVERIFIED = "max_drawdown_abs_cassa_non_verificata"
REASON_PCT_ACTIVE = "drawdown_hard_stop_active"
REASON_BASELINE_UNVERIFIED = "drawdown_baseline_non_verificata"
# D-631-11: una lettura che non si puo' valutare perche' la cronologia necessaria non e' piu' in memoria. NON e' un
# superamento (nessuna perdita dimostrata) ma non e' nemmeno un PASS: nuovi ingressi negati con questo motivo distinto,
# stato persistito, sblocco solo con la procedura owner `owner_clear_history_insufficient`.
REASON_HISTORY_INSUFFICIENT = "drawdown_storia_insufficiente"
STATE_HISTORY_INSUFFICIENT = "DRAWDOWN_HISTORY_INSUFFICIENT"
REASON_PCT_INVALID = "max_drawdown_hard_stop_pct_non_valido"
# S18-1 (Sol su bcd19a32, decisione owner PF-OWNER-PR631-CYCLE19-GO-20261112 e conferma del piano di chiusura): lo stato
# persistito porta la PROVENIENZA (ambiente + identita' del conto che lo ha scritto). Protezione fail-closed fino alla
# separazione completa per ambiente e conto (PR-04): con un ambito diverso, uno stato senza provenienza o un conto non
# identificabile nessuna valutazione, nessuna scrittura, nessuna procedura owner (salvo l'attribuzione esplicita) e
# nessun nuovo ingresso; lo stato salvato resta intatto (nessun reset del picco).
REASON_SCOPE_MISMATCH = "drawdown_stato_di_altro_ambito"
REASON_SCOPE_UNATTRIBUTED = "drawdown_stato_senza_provenienza"
REASON_ACCOUNT_UNKNOWN = "drawdown_conto_non_identificabile"
KIND_ABS = "max_drawdown_abs"
KIND_PCT = "max_drawdown_hard_stop_pct"
# D-631-10/11: letture verificate ricordate in memoria (istante monotonic, cassa) per valutare un calo visto da una
# lettura arrivata in ritardo contro il massimo letto PRIMA del suo istante. Memoria limitata per l'intera struttura:
# al massimo RECENT_READINGS_LIMIT letture recenti piu' SUMMARY_LIMIT punti di riepilogo delle letture uscite (scala dei
# massimi crescenti e scala dei minimi successivi). Oltre, la finestra avanza: cio' che precede non e' piu' valutabile
# con esattezza e, se puo' nascondere un superamento, diventa storia insufficiente (mai un PASS).
RECENT_READINGS_LIMIT = 256
SUMMARY_LIMIT = 256
READING_HISTORY_LIMIT = RECENT_READINGS_LIMIT + SUMMARY_LIMIT


def _marker_path(db: Any) -> str:
    """RO-DD-09: marker `<db>.drawdown_peak_pending.json` accanto al DB (stesso schema di `daily_loss_store`). Vuoto
    se il DB non ha un percorso su disco (es. in memoria)."""
    try:
        path = os.fspath(getattr(db, "db_path", None))
    except TypeError:
        return ""
    if not isinstance(path, str) or path in ("", ":memory:"):
        return ""
    return path + ".drawdown_peak_pending.json"


def _parse_record(raw: Any) -> dict:
    """Record persistito validato; ValueError se illeggibile o incoerente."""
    record = json.loads(raw)
    peak = None if record.get("peak") is None else Decimal(str(record["peak"]))
    seq = record.get("seq")
    breached = record.get("breached", False)
    session_open = record.get("session_open", False)
    verified = record.get("baseline_verified", True)
    epoch = record.get("peak_epoch", 0)
    rebase_ops = record.get("rebase_ops", [])
    scope_missing = "scope" not in record               # S18-1: record scritto prima della provenienza (quarantena)
    scope = _parse_scope(record.get("scope"))
    scope_history = record.get("scope_history", [])
    if not isinstance(scope_history, list):
        raise ValueError("record non valido")
    gap = record.get("history_insufficient") or {}
    if not isinstance(gap, dict) or type(gap.get("active", False)) is not bool:
        raise ValueError("record non valido")
    if (peak is not None and (not peak.is_finite() or peak <= 0)) or type(seq) is not int or seq < 0 \
            or type(breached) is not bool or type(session_open) is not bool or type(verified) is not bool \
            or type(epoch) is not int or epoch < 0 \
            or not isinstance(rebase_ops, list) or not all(isinstance(op, str) for op in rebase_ops):
        raise ValueError("record non valido")
    return {"peak": peak, "seq": seq, "peak_epoch": epoch, "rebase_ops": list(rebase_ops), "breached": breached, "breach": dict(record.get("breach") or {}),
            "session_open": session_open, "baseline_verified": verified,
            "baseline": dict(record.get("baseline") or {}), "history_insufficient": dict(gap),
            "scope": scope, "scope_missing": scope_missing, "scope_history": list(scope_history)}


def _parse_scope(value: Any) -> Optional[dict]:
    """Provenienza ``{"env", "account"}`` (stringhe non vuote) oppure None; altro => ValueError (record illeggibile)."""
    if value is None:
        return None
    if (isinstance(value, dict) and set(value) == {"env", "account"}
            and all(isinstance(value[k], str) and value[k] for k in ("env", "account"))):
        return {"env": value["env"], "account": value["account"]}
    raise ValueError("provenienza non valida")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _cash(value: Any) -> Optional[Decimal]:
    """Cassa verificata come Decimal; None se assente, non finita o <= 0."""
    try:
        amount = validators.money_decimal(value)
    except ValueError:
        return None
    return amount if amount > 0 else None


def _observed_cash(value: Any) -> Optional[Decimal]:
    """Cassa verificata ai fini della PERDITA: anche 0 (perdita totale, Sol su dc2f5f25). None se assente, non
    finita o negativa. Il picco usa invece ``_cash`` (> 0): una cassa nulla non e' mai un picco."""
    try:
        amount = validators.money_decimal(value)
    except ValueError:
        return None
    return amount if amount >= 0 else None


class DrawdownHardStop:
    """Picco di cassa persistente e decisione dell'hard-stop ``max_drawdown_abs``."""

    def __init__(self, db: Any = None):
        self._lock = threading.RLock()
        self._db = db
        self.peak: Optional[Decimal] = None
        self.peak_epoch = 0                              # +1 a ogni ribasamento owner del picco (Sol su 6d4ac43f)
        # Istante (monotonic) dell'ultima decisione owner che chiude la cronologia: ribasamento del picco (epoca nuova)
        # o sblocco (superamento o storia insufficiente). Una lettura del conto iniziata prima e' ignorata: non rialza
        # il picco ribasato (D-631-08) e non fa rinascere cio' che l'owner ha sbloccato (Sol/Codex su b76466a3).
        self.rebased_at: Optional[float] = None
        # G17-1 (Grok su bcd19a32): istante (monotonic, del processo) dell'ultima conferma owner della baseline, propria o
        # adottata da un altro processo. Una lettura del conto iniziata prima non e' una lettura successiva al picco
        # confermato (il picco dichiarato non ha un istante noto): si valuta come lettura tardiva (`evaluate_late`).
        self.baseline_confirmed_at: Optional[float] = None
        # G17-1: (istante della conferma, picco confermato). Per una lettura dall'istante in poi il picco confermato e'
        # un massimo noto prima di lei; per una lettura iniziata prima e' solo un limite superiore (istante ignoto).
        self._confirmed_peak: Optional[tuple] = None
        # D-631-10: finestra temporale delle letture di questo processo (sola memoria, mai persistita). `_window_base` e'
        # il picco noto all'apertura (`_window_open`, monotonic): precede ogni lettura in `_history`, ordinata per
        # istante. Si riapre allo START (stato appena riletto) e a ogni ribasamento (epoca nuova).
        self._window_open: Optional[float] = None
        self._window_base: Optional[Decimal] = None
        self._history: list = []                         # letture recenti (istante, cassa), ordinate per istante
        # D-631-11: riepilogo delle letture uscite dalle recenti (tutte con istante <= della recente piu' vecchia).
        # `_ladder`: punti in cui il massimo cumulato sale (istante e valore crescenti) => massimo letto prima di t.
        # `_valleys`: scala dei minimi successivi (istante e valore crescenti) => minimo letto dopo t.
        self._ladder: list = []
        self._valleys: list = []
        # Stato persistito: lettura non valutabile per storia insufficiente ({"active": True, ...}); "cleared" tiene
        # gli sblocchi owner (operation_id monouso).
        self.history_insufficient: dict = {}
        self.rebase_ops: list = []                       # operation_id dei ribasamenti gia' usati (Grok su b9cbe0d7)
        self.seq = 0
        self.unreadable = False
        self.persist_failed = False
        self.breached = False
        self.breach: dict = {}
        # RO-DD-09 C (decisione owner PF-ROSERPINA-DD09-FAIL-CLOSED-AND-BB3-20261109): sessione di trading aperta su
        # disco e baseline verificata. Riavvio dopo una chiusura non pulita => baseline non verificata => nessun
        # ingresso finche' l'owner non la conferma (`owner_confirm_baseline`).
        self.session_open = False
        self.baseline_verified = True
        self.baseline: dict = {}
        # S18-1: ambito corrente dichiarato dal runtime (None = non dichiarato: nessun controllo, uso diretto senza runtime)
        # e provenienza dello stato persistito (None = nessuno stato ancora attribuito). `scope_legacy`: record letto
        # senza provenienza (scritto prima della protezione) => quarantena fino all'attribuzione owner.
        self.scope: Optional[dict] = None
        self.record_scope: Optional[dict] = None
        self.scope_legacy = False
        self.scope_history: list = []

    # ------------------------------------------------------------ persistenza
    def restore(self, *, reconcile: bool = True) -> None:
        """Rilegge lo stato salvato dal DB e dal marker RO-DD-09 (scritto quando il DB non salvava): vale il picco
        piu' alto, un superamento in uno dei due vale. Illeggibile => ``unreadable`` (fail-closed). Con ``reconcile``
        il marker piu' recente viene riportato nel DB e quello superato rimosso; ``reconcile=False`` (Sol su 58e635e6)
        e' la sola lettura usata prima di avere la proprieta' del runtime: nessuna scrittura, che sovrascriverebbe un
        record piu' recente del proprietario attuale. La riconciliazione si fa allo START, sotto il lock di istanza."""
        with self._lock:
            self.unreadable = False
            records = []
            try:
                getter = getattr(self._db, "get_settings", None)
                raw = (getter() or {}).get(STATE_KEY) if callable(getter) else None
                if raw not in (None, ""):
                    records.append(_parse_record(raw))
                marker = _marker_path(self._db)
                if marker and os.path.exists(marker):
                    with open(marker, encoding="utf-8") as fh:
                        records.append(dict(_parse_record(fh.read()), from_marker=True))
            except Exception:
                logger.error("D-1: stato del picco dell'hard-stop illeggibile (DB o marker): nuovi ingressi negati",
                             exc_info=True)
                self.unreadable = True
                return
            if not records:
                return                                   # nessuno stato: nuovo perimetro, picco dalla prima cassa verificata
            # Codex/Grok/Claude D su dc2f5f25: sessione, baseline e superamento li decide il record piu' recente
            # (`seq`). Un marker rimasto da un guasto ormai rientrato (rimozione fallita, file bloccato su Windows) ha
            # un `seq` inferiore al DB e non annulla uno sblocco o una conferma owner salvati dopo; a pari `seq` vale
            # l'unione (regola piu' severa).
            latest = max(r["seq"] for r in records)
            newer = latest > self.seq                    # su disco c'e' uno stato successivo a quello in memoria
            stale = [r for r in records if r["seq"] < latest]
            records = [r for r in records if r["seq"] == latest]
            # Picco: mai sotto un picco gia' osservato in questo processo (START ripetuto nello stesso processo; un
            # record piu' basso senza prova e' trattato come non autorevole). Unica eccezione (Codex su 4a171465): un
            # ribasamento owner esplicito (`owner_rebase_peak`, tracciato) apre un'epoca nuova, che vale anche verso
            # il basso.
            # Sol su 6d4ac43f: il ribasamento si riconosce dall'epoca del picco (`peak_epoch`, +1 a ogni
            # `owner_rebase_peak`), non dal valore: vale anche se dopo il ribasamento il picco e' risalito (85 -> 90).
            # Conta solo l'epoca piu' recente fra memoria e record (DB e marker): dentro quella, il massimo di tutti
            # (Claude D: un marker con picco piu' alto della stessa epoca non si perde); le epoche precedenti al
            # ribasamento non contano piu'.
            epoch = max([self.peak_epoch] + [r["peak_epoch"] for r in records + stale])
            candidates = [r["peak"] for r in records + stale if r["peak_epoch"] == epoch and r["peak"] is not None]
            if self.peak is not None and self.peak_epoch == epoch:
                candidates.append(self.peak)
            if candidates:
                self.peak = max(candidates)
            if epoch != self.peak_epoch:
                self.rebased_at = time.monotonic()       # ribasamento letto dal disco (altro processo)
                self._open_window_locked(self.rebased_at)
            self.peak_epoch = epoch
            # Grok su b9cbe0d7: gli operation_id dei ribasamenti restano consumati (unione di memoria e record).
            for r in records + stale:
                for op in r["rebase_ops"]:
                    if op not in self.rebase_ops:
                        self.rebase_ops.append(op)
            old_epoch = self.baseline_epoch()
            self.baseline = dict(records[0]["baseline"])
            # S18-1: provenienza del record piu' recente; un record senza provenienza la mette in quarantena (mai
            # attribuita d'ufficio a SIM o LIVE). Storia delle attribuzioni owner: unione (operation_id monouso).
            # Sol e Grok su 12618e75: conta la provenienza VALORIZZATA. Un record con stato finanziario (picco,
            # superamento o storia insufficiente) e provenienza assente o nulla resta in quarantena anche dopo le scritture
            # di sessione (esenti dal controllo d'ambito), che non possono attribuirlo d'ufficio.
            scoped = [r for r in records if r["scope"]]
            if scoped:
                self.scope_legacy = False
                self.record_scope = dict(scoped[0]["scope"])
            else:
                self.record_scope = None
                self.scope_legacy = any(r["peak"] is not None or r["breached"]
                                        or r["history_insufficient"].get("active") for r in records)
            for r in records + stale:
                for entry in r["scope_history"]:
                    if entry not in self.scope_history:
                        self.scope_history.append(entry)
            if newer and self.baseline_epoch() > old_epoch:
                # G17-1: conferma owner salvata da un altro processo, adottata adesso: vale da questo istante anche qui.
                self._mark_baseline_confirmed_locked()
            # RO-DD-09 C: sessione rimasta aperta (crash, stop non pulito, picco mai salvato) o baseline gia' non
            # verificata => il picco salvato potrebbe non essere il massimo: nessuna presunzione, conferma owner.
            # Codex su 58e635e6: la verifica della baseline segue il record durevole piu' recente, anche verso True:
            # letta mentre un altro processo aveva la sessione aperta, dopo il suo STOP pulito non resta falsa.
            # Il record piu' recente e' il nostro (stessa `seq`) e la sessione aperta e' quella di questa istanza:
            # rilettura durante la propria sessione attiva (procedura owner), non una chiusura non pulita.
            if self.session_open and not newer:
                pass
            elif any(r["session_open"] for r in records) or not all(r["baseline_verified"] for r in records):
                if self.baseline_verified:
                    logger.critical("D-1: chiusura precedente non pulita: baseline del drawdown NON verificata, "
                                    "nuovi ingressi negati fino alla conferma owner")
                self.baseline_verified = False
            else:
                self.baseline_verified = True
            self.seq = max([self.seq] + [r["seq"] for r in records])
            breached = [r for r in records if r["breached"]]
            if breached:                                 # un superamento salvato non si annulla mai rileggendo
                self.breached = True
                self.breach = dict(breached[0]["breach"])
            elif newer and self.breached:
                # Codex su 53c99ceb: superamento letto alla costruzione e poi sbloccato dall'owner in modo durevole da un
                # altro processo (record piu' recente non superato): vale lo sblocco, con la sua storia. Un superamento
                # solo in memoria e piu' recente (salvataggio fallito: `seq` non piu' basso del disco) resta.
                self.breached = False
                self.breach = dict(records[0]["breach"])
                self._close_history_locked()             # sblocco owner di un altro processo: come uno sblocco locale
            elif not self.breached or newer:
                # Sol/Codex su 4a171465: nessun superamento attivo, ma la storia degli sblocchi (operation_id gia'
                # usati) e dei ribasamenti si riprende dal disco: un riavvio non rende di nuovo valido un operation_id
                # consumato.
                self.breach = dict(records[0]["breach"])
            gaps = [r for r in records if r["history_insufficient"].get("active")]
            if gaps:                                     # come il superamento: rileggere non lo cancella mai
                self.history_insufficient = dict(gaps[0]["history_insufficient"])
            elif newer and self.history_insufficient.get("active"):
                self.history_insufficient = dict(records[0]["history_insufficient"])
                self._close_history_locked()
            elif newer or not self.history_insufficient.get("active"):
                # sblocco owner durevole (anche di un altro processo) o nessuno stato attivo: storia degli sblocchi
                # ripresa dal disco; uno stato attivo solo in memoria e piu' recente del disco resta.
                self.history_insufficient = dict(records[0]["history_insufficient"])
            if not reconcile:
                return
            if any(r.get("from_marker") for r in records):
                self._persist_locked()                   # RO-DD-09: il picco del marker torna nel DB
            elif any(r.get("from_marker") for r in stale):
                # Sol su e98e84c7: un marker superato puo' portare un picco piu' alto della stessa epoca. Quel picco
                # va salvato nel DB PRIMA di rimuovere il marker (il salvataggio riuscito lo rimuove; quello fallito
                # lo riscrive): un crash prima del salvataggio successivo non lo perde.
                durable = [r["peak"] for r in records if r["peak_epoch"] == epoch and r["peak"] is not None]
                if self.peak is not None and (not durable or self.peak > max(durable)):
                    self._persist_locked()
                else:
                    self._remove_stale_marker()

    def set_scope(self, scope: Optional[dict]) -> None:
        """S18-1: ambito operativo corrente (ambiente + identita' del conto), dichiarato dal runtime per ogni lettura."""
        with self._lock:
            self.scope = None if scope is None else dict(scope)

    def invalidate_scope(self) -> None:
        """S18-1 (Claude D, anteprima del ciclo 19): cambio di modalita' SIM/LIVE. L'ambito dichiarato non vale piu' finche'
        una lettura del conto nella nuova modalita' non lo ridichiara: fino ad allora conto non identificabile (nessun
        ingresso, nessuna valutazione ne' scrittura). Senza un ambito gia' dichiarato (nessuna lettura ancora) non c'e' nulla
        da invalidare: il primo ingresso legge comunque il conto e dichiara il proprio ambito."""
        with self._lock:
            if self.scope is not None:
                self.scope = {"env": "CAMBIO_MODALITA", "account": ""}

    def scope_block_reason(self) -> str:
        """S18-1: '' se lo stato persistito appartiene all'ambito corrente (o nessun ambito e' dichiarato), altrimenti il
        motivo fail-closed. Uno stato ancora senza provenienza (nessun record) si attribuisce alla prima scrittura."""
        with self._lock:
            return self._scope_block_locked()

    def _scope_block_locked(self) -> str:
        if self.scope is None:
            return ""
        if not self.scope.get("account"):
            return REASON_ACCOUNT_UNKNOWN
        if self.scope_legacy:
            return REASON_SCOPE_UNATTRIBUTED
        if self.record_scope is not None and self.record_scope != self.scope:
            return REASON_SCOPE_MISMATCH
        return ""

    def _persist_locked(self, *, marker_fallback: bool = True, scope_exempt: bool = False) -> bool:
        """Salva lo stato nel DB. ``marker_fallback`` False (Sol su 92155c24) per le transizioni che ALLENTANO un
        blocco (sblocco owner, conferma della baseline, ribasamento del picco, chiusura della sessione): se il DB non
        salva non va scritto nemmeno il marker, altrimenti un'operazione fallita (stato in RAM ripristinato) resterebbe
        su disco come record piu' recente e al riavvio varrebbe lo stesso. Il marker resta il ripiego dei soli stati
        che stringono (picco che sale, superamento, sessione aperta)."""
        saver = getattr(self._db, "save_settings", None)
        if not scope_exempt and self._scope_block_locked():
            # S18-1: mai scrivere lo stato di un altro ambito (difesa: i chiamanti non mutano lo stato fuori ambito).
            self.persist_failed = True
            return False
        if (self.record_scope is None and not self.scope_legacy and self.scope is not None
                and self.scope.get("account")):
            # prima scrittura: provenienza = ambito corrente, solo se identificato (mai un ambito invalidato o senza conto)
            # e mai su uno stato senza provenienza (Grok su 559b39ca): le scritture di sessione, esenti dal controllo
            # d'ambito, non attribuiscono il record al conto corrente; lo fa solo l'assegnazione owner.
            self.record_scope = dict(self.scope)
        newer = self._newer_on_disk_locked()
        if newer:
            # D-631-18 (Codex su 9c9121f0): su disco c'e' un record piu' recente di quello da cui parte questo stato in
            # memoria (un altro processo, o un'istanza costruita prima delle sue scritture). Salvare adesso cancellerebbe
            # il suo picco o il suo superamento con uno snapshot vecchio: nessuna scrittura (nemmeno il marker), nessun
            # ingresso da questa istanza finche' non rilegge lo stato (`restore`, allo START o nelle procedure owner).
            logger.critical("D-1: stato del drawdown su disco piu' recente (%s) di quello in memoria (seq %s): "
                            "salvataggio rifiutato, nuovi ingressi negati fino alla rilettura", newer, self.seq)
            self.persist_failed = True
            return False
        seq = self.seq + 1
        record = {"peak": None if self.peak is None else str(self.peak), "seq": seq, "peak_epoch": self.peak_epoch,
                  "rebase_ops": list(self.rebase_ops),
                  "breached": self.breached,
                  "breach": dict(self.breach), "session_open": self.session_open,
                  "baseline_verified": self.baseline_verified, "baseline": dict(self.baseline),
                  "history_insufficient": dict(self.history_insufficient),
                  "scope": None if self.record_scope is None else dict(self.record_scope),
                  "scope_history": list(self.scope_history), "updated_at": _now()}
        payload = json.dumps(record)
        marker = _marker_path(self._db)
        try:
            if not callable(saver):
                raise RuntimeError("DB senza save_settings")
            saver({STATE_KEY: payload})
        except Exception as exc:
            logger.exception("D-1: picco dell'hard-stop non salvato nel DB: nuovi ingressi negati finche' non si salva")
            self.persist_failed = True
            if isinstance(exc, RuntimeFenceViolation):
                # Sol su e98e84c7: scrittura rifiutata dal fencing = questa istanza non e' piu' il proprietario
                # (subentro fra la verifica dell'autorita' e il salvataggio). Un'istanza superata non pubblica
                # nulla, nemmeno il marker, che il nuovo proprietario leggerebbe come stato dell'hard-stop.
                return False
            # RO-DD-09 (Claude D): marker su file separato, cosi' un riavvio non riparte da un picco vecchio anche se
            # il DB non torna. Marker non scrivibile => KNOWN_LIMITATION (doppio guasto), come per la perdita del giorno.
            try:
                if marker and marker_fallback:
                    atomic_write_text(marker, payload)
                    self.seq = seq
            except Exception:
                logger.critical("D-1: neanche il marker del picco e' scrivibile (doppio guasto)", exc_info=True)
            return False
        self.seq = seq
        self.persist_failed = False
        self._remove_stale_marker()
        return True

    def _newer_on_disk_locked(self) -> str:
        """'' se il record del DB non e' piu' recente dello stato in memoria (``seq`` su disco <= ``self.seq``),
        altrimenti la sua descrizione. Concorrenza ottimistica: ogni salvataggio parte dall'ultimo record letto o scritto
        da questa istanza. DB non leggibile => '' (il salvataggio decide: se fallisce anche lui vale il ripiego)."""
        getter = getattr(self._db, "get_settings", None)
        if not callable(getter):
            return ""
        try:
            raw = (getter() or {}).get(STATE_KEY)
            if raw in (None, ""):
                return ""
            disk_seq = _parse_record(raw)["seq"]
        except Exception:
            return ""
        return f"seq {disk_seq}" if disk_seq > self.seq else ""

    def _remove_stale_marker(self) -> None:
        marker = _marker_path(self._db)
        try:
            if marker and os.path.exists(marker):
                os.remove(marker)
        except OSError:
            # Non annulla nulla: al riavvio il marker ha un `seq` inferiore al DB e non decide sessione, baseline ne'
            # superamento (vale solo il suo picco, mai superiore a quello gia' salvato). Si ritenta al prossimo restore.
            logger.warning("D-1: marker del picco non rimosso: superato dal DB (seq piu' alto), si ritenta al riavvio")

    # ------------------------------------------------------------ sessione e baseline (RO-DD-09 C)
    def open_session(self) -> bool:
        """START del trading: la sessione risulta aperta su disco finche' uno STOP pulito non la chiude. Salvataggio
        fallito => ``persist_failed`` (nessun ingresso, RO-DD-08)."""
        with self._lock:
            if self.unreadable:
                return False
            self.session_open = True
            # S18-1: solo il flag di sessione sullo stato del record (mai mutato fuori ambito): esente dal controllo
            # d'ambito, cosi' lo START non si blocca e lo stato resta attribuibile dall'owner.
            return self._persist_locked(scope_exempt=True)

    def close_session(self) -> bool:
        """STOP pulito: chiude la sessione su disco SOLO se il picco e' durevole. Picco non salvato o stato
        illeggibile => la sessione resta aperta e il prossimo avvio chiede la conferma owner della baseline."""
        with self._lock:
            if self.unreadable or self.persist_failed:
                return False
            self.session_open = False
            if not self._persist_locked(marker_fallback=False, scope_exempt=True):
                self.session_open = True
                return False
            return True

    def baseline_block_reason(self) -> str:
        with self._lock:
            return "" if self.baseline_verified else REASON_BASELINE_UNVERIFIED

    def history_block_reason(self) -> str:
        """D-631-11: '' oppure il motivo distinto della storia insufficiente (nessun nuovo ingresso)."""
        with self._lock:
            return REASON_HISTORY_INSUFFICIENT if self.history_insufficient.get("active") else ""

    def owner_confirm_baseline(self, *, authorization: str, reason: str, operation_id: str, confirm: bool,
                               confirm_again: bool, cash_equity: Any, declared_peak: Any = None,
                               checks: Optional[dict] = None) -> bool:
        """Conferma owner della baseline dopo un riavvio non pulito (RO-DD-09 C). Doppia conferma esplicita,
        autorizzazione, motivo, operation_id monouso e cassa verificata. La baseline diventa il massimo fra picco
        salvato, picco dichiarato dall'owner e cassa corrente: la conferma NON e' una prova del massimo storico e
        viene registrata come tale. Registrazione durevole prima che valga; salvataggio fallito => resta non
        verificata."""
        ref, why, op = (str(v or "").strip() for v in (authorization, reason, operation_id))
        cash = _cash(cash_equity)
        declared = None if declared_peak is None else _cash(declared_peak)
        if not (ref and why and op and confirm is True and confirm_again is True) or cash is None:
            return False
        if declared_peak is not None and declared is None:
            return False
        with self._lock:
            if self.unreadable or self.baseline_verified or self._scope_block_locked():
                return False
            history = list(self.baseline.get("confirmations", []))
            if any(isinstance(h, dict) and h.get("operation_id") == op for h in history):
                return False
            old = (self.peak, dict(self.baseline))
            epoch = len(history) + 1
            candidates = [v for v in (self.peak, declared, cash) if v is not None]
            self.peak = max(candidates)
            entry = {"at": _now(), "authorization": ref, "reason": why, "operation_id": op,
                     "previous_peak": None if old[0] is None else str(old[0]),
                     "declared_peak": None if declared is None else str(declared), "cash": str(cash),
                     "baseline_peak": str(self.peak), "checks": dict(checks or {}), "epoch": epoch,
                     "nota": "conferma owner della baseline: non e' una prova del massimo storico"}
            self.baseline = {"confirmations": history + [entry]}
            self.baseline_verified = True
            if not self._persist_locked(marker_fallback=False):
                self.peak, self.baseline = old
                self.baseline_verified = False
                return False
            # D-631-17 (Codex su 9c9121f0): il picco confermato e' il massimo noto da adesso: un calo letto dopo la
            # conferma, anche arrivato fuori ordine, si misura da questo picco. G17-1 (Grok su bcd19a32): il picco
            # dichiarato non ha un istante noto, quindi per una lettura iniziata PRIMA della conferma (che il runtime
            # valuta come tardiva anche se arriva in ordine) e' solo un limite superiore: superamento dimostrato dalle
            # letture conservate => hard-stop; solo possibile => storia insufficiente; impossibile => nessun blocco.
            # La cronologia delle letture resta (serve proprio a distinguere i tre casi).
            self._mark_baseline_confirmed_locked()
            logger.warning("D-1: baseline del drawdown confermata dall'owner (op %s): picco %s", op, self.peak)
            return True

    # ------------------------------------------------------------ osservazione
    def observe(self, cash_equity: Any) -> None:
        """Cassa verificata osservata: alza (mai abbassa) il picco e lo salva. Un salvataggio fallito si ritenta
        alla prossima osservazione; nel frattempo ``reason`` nega (``REASON_NOT_PERSISTED``)."""
        cash = _cash(cash_equity)
        if cash is None:
            return
        with self._lock:
            if self.unreadable or self._scope_block_locked():
                return                                   # mai sovrascrivere uno stato illeggibile o di altro ambito
            if self.peak is None or cash > self.peak:
                self.peak = cash
                self._persist_locked()
            elif self.persist_failed:
                self._persist_locked()

    def evaluate(self, cap_raw: Any, cash_equity: Any, *, pct_cap_raw: Any = None) -> bool:
        """Controllo a ogni variazione finanziaria (lettura verificata del conto), non solo al segnale.

        Osserva la cassa e, con un limite configurato e valido (assoluto ``cap_raw`` in euro o percentuale
        ``pct_cap_raw`` dal picco, RO-DD-04/05), MEMORIZZA il primo superamento (persistito). True se questa chiamata
        ha appena registrato un superamento (il runtime va in LOCKDOWN subito)."""
        self.observe(cash_equity)
        with self._lock:
            reference = self.peak
        return self._memorize_if_exceeded(cap_raw, pct_cap_raw, cash_equity, reference)

    def _memorize_if_exceeded(self, cap_raw: Any, pct_cap_raw: Any, cash_equity: Any,
                              reference: Optional[Decimal]) -> bool:
        """Perdita di ``cash_equity`` dal picco di riferimento ``reference``: oltre un limite valido memorizza il primo
        superamento (persistito). True se questa chiamata l'ha appena registrato."""
        cash = _observed_cash(cash_equity)
        if cash is None or reference is None:
            return False
        loss = max(Decimal(0), reference - cash)
        kind, cap = None, None
        abs_cap = validators.finite_number(cap_raw) if cap_raw is not None else None
        if abs_cap is not None and abs_cap > 0 and loss >= validators.money_decimal(abs_cap):
            kind, cap = KIND_ABS, validators.money_decimal(abs_cap)
        pct_cap = validators.finite_number(pct_cap_raw) if pct_cap_raw is not None else None
        if kind is None and pct_cap is not None and 0 < pct_cap <= 100:
            threshold = reference * validators.money_decimal(pct_cap) / Decimal(100) if reference else None
            if threshold is not None and loss >= threshold:
                kind, cap = KIND_PCT, validators.money_decimal(pct_cap)
        if kind is None:
            return False
        with self._lock:
            if self.breached or self.unreadable or self._scope_block_locked():
                return False
            self.breached = True
            self.breach = {"kind": kind, "at": _now(), "loss": str(loss), "cap": str(cap), "peak": str(reference),
                           "cash": str(validators.money_decimal(cash_equity)),
                           "cleared": list(self.breach.get("cleared", [])) if isinstance(self.breach, dict) else []}
            logger.critical("D-1: hard-stop %s superato (perdita %s, limite %s, picco %s): blocco persistente",
                            kind, loss, cap, reference)
            self._persist_locked()
            return True

    # ------------------------------------------------------------ letture fuori ordine (D-631-10/11)
    def _close_history_locked(self) -> None:
        """Decisione owner (sblocco) adottata: le letture iniziate prima non si valutano piu', il picco resta."""
        self.rebased_at = time.monotonic()
        self._open_window_locked(self.rebased_at)

    def _mark_baseline_confirmed_locked(self) -> None:
        """G17-1: conferma della baseline (propria o adottata): registra l'istante e il picco confermato."""
        self.baseline_confirmed_at = time.monotonic()
        self._confirmed_peak = None if self.peak is None else (self.baseline_confirmed_at, self.peak)

    def baseline_epoch(self) -> int:
        """G17-1: epoca della baseline = numero delle conferme owner registrate (persistite nella storia della
        baseline); 0 se mai confermata."""
        with self._lock:
            confirmations = self.baseline.get("confirmations", []) if isinstance(self.baseline, dict) else []
            return len(confirmations) if isinstance(confirmations, list) else 0

    def _open_window_locked(self, at: float) -> None:
        # Senza alcun picco noto non c'e' nulla prima dell'apertura che possa mancare: tutta la cronologia e' esatta.
        self._window_open = float(at) if self.peak is not None else -math.inf
        self._window_base = self.peak
        self._confirmed_peak = None                      # la base (picco noto adesso) lo comprende gia'
        self._history = []
        self._ladder = []
        self._valleys = []

    def open_window(self, at: float) -> None:
        """Apre la finestra delle letture all'istante ``at`` con il picco noto adesso come base (START, stato appena
        riletto; ribasamento owner). Tutto cio' che e' noto prima precede ``at``; la cronologia precedente si azzera.
        Lo stato di storia insufficiente NON si azzera qui."""
        with self._lock:
            self._open_window_locked(at)

    @staticmethod
    def _limit_exceeded(cap_raw: Any, pct_cap_raw: Any, cash: Decimal, reference: Optional[Decimal]) -> bool:
        """Come `_memorize_if_exceeded` ma senza memorizzare: la perdita di ``cash`` da ``reference`` supera un limite
        configurato e valido?"""
        if reference is None or cash is None:
            return False
        loss = max(Decimal(0), reference - cash)
        abs_cap = validators.finite_number(cap_raw) if cap_raw is not None else None
        if abs_cap is not None and abs_cap > 0 and loss >= validators.money_decimal(abs_cap):
            return True
        pct_cap = validators.finite_number(pct_cap_raw) if pct_cap_raw is not None else None
        if pct_cap is not None and 0 < pct_cap <= 100 and reference > 0:
            return loss >= reference * validators.money_decimal(pct_cap) / Decimal(100)
        return False

    def _peak_before_locked(self, at: float):
        """``(massimo letto STRETTAMENTE prima di at, esatto?)``. Prima della finestra non e' esatto: la base e' solo un
        limite superiore (il vero massimo a ``at`` non puo' superare cio' che era noto all'apertura)."""
        best, exact = self._peak_before_window_locked(at)
        if self._confirmed_peak is not None:
            # G17-1: picco confermato dall'owner. Dalla conferma in poi e' un massimo noto; prima solo un limite
            # superiore (non si sa quando e' stato toccato).
            confirmed_at, confirmed = self._confirmed_peak
            if best is None or confirmed > best:
                best = confirmed
                exact = exact and at >= confirmed_at
        return best, exact

    def _peak_before_window_locked(self, at: float):
        if self._window_open is None:
            return self.peak, False
        if at < self._window_open:
            return self._window_base, False
        best = self._window_base
        i = bisect.bisect_left(self._ladder, (at,))
        if i > 0:
            step = self._ladder[i - 1][1]
            best = step if best is None or step > best else best
        for t, cash in self._history:
            if t >= at:
                break
            if cash > 0 and (best is None or cash > best):
                best = cash
        return best, True

    def _proven_peak_before_locked(self, at: float) -> Optional[Decimal]:
        """S19-A: massimo delle sole letture VERE conservate (recenti e gradini del riepilogo) STRETTAMENTE prima di
        ``at``; None se nessuna. Mai la base della finestra ne' il picco confermato (limiti superiori senza istante): un
        calo da questo massimo e' dimostrato."""
        best = None
        i = bisect.bisect_left(self._ladder, (at,))
        if i > 0:
            best = self._ladder[i - 1][1]
        for t, cash in self._history:
            if t >= at:
                break
            if cash > 0 and (best is None or cash > best):
                best = cash
        return best

    def _min_after_locked(self, at: float):
        """``(minimo letto STRETTAMENTE dopo at fra le letture conservate, esatto?)``. Non esatto prima della finestra:
        fra ``at`` e l'apertura ci sono letture che questo processo non ha conservato."""
        low = None
        for t, cash in self._history:
            if t > at and (low is None or cash < low):
                low = cash
        i = bisect.bisect_right(self._valleys, (at, Decimal("Infinity")))
        if i < len(self._valleys):
            valley = self._valleys[i][1]
            low = valley if low is None or valley < low else low
        exact = self._window_open is not None and at >= self._window_open
        return low, exact

    def _ladder_insert_locked(self, at: float, cash: Decimal) -> None:
        if cash <= 0:
            return
        i = bisect.bisect_left(self._ladder, (at,))
        prev = self._ladder[i - 1][1] if i > 0 else self._window_base
        if prev is not None and cash <= prev:
            return                                       # non alza il massimo cumulato: non serve
        self._ladder.insert(i, (at, cash))
        j = i + 1
        while j < len(self._ladder) and self._ladder[j][1] <= cash:
            del self._ladder[j]                          # gradini successivi non piu' crescenti

    def _valley_insert_locked(self, at: float, cash: Decimal) -> None:
        i = bisect.bisect_right(self._valleys, (at, Decimal("Infinity")))
        if i < len(self._valleys) and self._valleys[i][1] <= cash:
            return                                       # un minimo successivo piu' basso lo rende superfluo
        self._valleys.insert(i, (at, cash))
        while i > 0 and self._valleys[i - 1][1] >= cash:
            del self._valleys[i - 1]
            i -= 1

    def _store_locked(self, at: float, cash: Decimal) -> None:
        """Conserva una lettura: prima della finestra entra nella base (precede ogni lettura conservata); altrimenti
        fra le recenti, e le piu' vecchie oltre il limite passano nel riepilogo. Oltre il limite del riepilogo la
        finestra avanza oltre il suo punto piu' vecchio (memoria limitata per l'intera struttura)."""
        if at < self._window_open:
            if cash > 0 and (self._window_base is None or cash > self._window_base):
                self._window_base = cash
            # S19-A (oracolo R49 di Claude D): resta anche una lettura vera con il suo istante. La base ne fa il limite
            # superiore di cio' che precede la finestra; fra le conservate e' una PROVA di calo (con un'altra lettura vera
            # prima o dopo di lei), qualunque cosa sia successa fuori dalla memoria.
        bisect.insort(self._history, (float(at), cash))
        while len(self._history) > RECENT_READINGS_LIMIT:
            t0, c0 = self._history.pop(0)
            self._ladder_insert_locked(t0, c0)
            self._valley_insert_locked(t0, c0)
        while len(self._ladder) + len(self._valleys) > SUMMARY_LIMIT:
            oldest_ladder = self._ladder[0][0] if self._ladder else math.inf
            oldest_valley = self._valleys[0][0] if self._valleys else math.inf
            if oldest_ladder <= oldest_valley:
                t0, m0 = self._ladder.pop(0)
                if self._window_base is None or m0 > self._window_base:
                    self._window_base = m0
            else:
                t0, _ = self._valleys.pop(0)
            # da qui la storia prima di t0 non e' piu' esatta: le letture a t0 o prima sono «prima della finestra»
            self._window_open = max(self._window_open, math.nextafter(t0, math.inf))

    def record_reading(self, at: float, cash_equity: Any, *, may_open: bool = False) -> None:
        """Ricorda la cassa verificata letta all'istante ``at`` (lettura corrente, in ordine). ``may_open``: senza
        finestra la apre qui, con il picco noto PRIMA di questa lettura come base."""
        cash = _observed_cash(cash_equity)
        if cash is None:
            return
        with self._lock:
            if self._scope_block_locked():
                return
            if self._window_open is None:
                if not may_open:
                    return
                self._open_window_locked(at)
            self._store_locked(float(at), cash)

    def evaluate_late(self, cap_raw: Any, at: float, cash_equity: Any, *, pct_cap_raw: Any = None) -> str:
        """Lettura verificata arrivata dopo una piu' recente (D-631-10/11). Esito: "" (niente), "breached" (superamento
        dimostrato e memorizzato), "insufficient" (storia insufficiente, memorizzata). Due verifiche, sempre contro
        letture con istante coerente, mai contro un massimo successivo:

        (A) il suo calo dal massimo letto STRETTAMENTE prima di ``at``;
        (B) se lo supera, il calo delle letture successive (minimo letto strettamente dopo ``at``) da questa lettura.

        Quando l'informazione necessaria non e' piu' in memoria (lettura precedente alla finestra) e un superamento
        e' possibile con i limiti noti, l'esito e' storia insufficiente: mai un PASS, mai un superamento inventato."""
        cash = _observed_cash(cash_equity)
        if cash is None:
            return ""
        at = float(at)
        with self._lock:
            if self.unreadable or self._scope_block_locked():
                return ""
            if self._window_open is None:
                self._open_window_locked(at)             # nessuna storia: la base e' il picco noto
            reference, ref_exact = self._peak_before_locked(at)
            proven = self._proven_peak_before_locked(at)
            known, known_exact = self._peak_before_window_locked(at)
            low, low_exact = self._min_after_locked(at)
            # Ciclo 21 (PF-OWNER-PR631-C21-EQUAL-TIMESTAMP-GO-20261112): letture conservate con lo STESSO istante di
            # inizio. L'ordine reale fra loro e questa e' ignoto: non sono ne' prima ne' dopo (nessuna prova), ma un calo
            # fra le due in uno qualsiasi dei due ordini e' possibile.
            # Anche quelle gia' passate dalle recenti al riepilogo (gradini dei massimi e dei minimi, Codex su
            # 954b226b): una gemella scartata anche dal riepilogo e' dominata da una prova gia' valutata (un massimo
            # precedente maggiore o uguale, verifica A; un minimo successivo minore o uguale, misurato al suo arrivo).
            peers = [c for t, c in (*self._history, *self._ladder, *self._valleys) if t == at]
            already = self.breached or bool(self.history_insufficient.get("active"))
            self._store_locked(at, cash)
            epoch = self.peak_epoch
        if already and self.breached:
            return ""
        # S19-A (Sol su 5bb61f7f, Claude D): PRIMA tutte le prove di un calo, POI l'incertezza. Un superamento dimostrato
        # dalle letture conservate e' un hard-stop anche quando un limite superiore (picco confermato di istante ignoto,
        # base della finestra) renderebbe possibile anche un calo non dimostrabile: mai declassato a storia insufficiente.
        # (A) calo di questa lettura dal massimo letto prima di lei: dal massimo esatto delle letture conservate (G17-1,
        # caso 105) e, se esatto, dal riferimento completo.
        if proven is not None and self._memorize_if_exceeded(cap_raw, pct_cap_raw, cash, proven):
            return "breached"
        # Massimo esatto della finestra (base = picco noto all'apertura, che precede ogni lettura dalla finestra in poi;
        # senza il picco confermato di istante ignoto): anche lui dimostra il calo (G17-1, caso 105; R49 caso 240).
        if known_exact and known is not None and self._memorize_if_exceeded(cap_raw, pct_cap_raw, cash, known):
            return "breached"
        if reference is not None and ref_exact and self._memorize_if_exceeded(cap_raw, pct_cap_raw, cash, reference):
            return "breached"
        # (B) questa lettura come massimo che rende reale un calo successivo (minimo letto dopo di lei, una lettura
        # vera). Con il massimo esatto serve solo se lo supera (altrimenti il calo successivo e' gia' stato misurato da un
        # massimo piu' alto); con il solo limite superiore il vero massimo a `at` puo' essere piu' basso di questa
        # lettura: la verifica si fa sempre (D-631-12, Sol/Claude D su f0f521dc). 110 poi 90 = 20 dimostrato (S19-A).
        check_after = cash > 0 and (not ref_exact or reference is None or cash > reference)
        if check_after and low is not None and self._memorize_if_exceeded(cap_raw, pct_cap_raw, low, cash):
            return "breached"
        # Nessuna prova: superamento solo possibile con i limiti superiori noti => storia insufficiente, mai un PASS.
        if reference is not None and not ref_exact and self._limit_exceeded(cap_raw, pct_cap_raw, cash, reference):
            return self._mark_history_insufficient("lettura_precedente_alla_storia_conservata", at, cash, epoch,
                                                   reference=reference)
        if check_after and not low_exact and self._limit_exceeded(cap_raw, pct_cap_raw, Decimal(0), cash):
            return self._mark_history_insufficient("massimo_precedente_alla_storia_conservata", at, cash, epoch,
                                                   reference=reference)
        # Ciclo 21: stesso istante d'inizio, ordine ignoto. Se in uno dei due ordini il calo raggiunge un limite, il
        # superamento e' possibile ma non dimostrabile => storia insufficiente (mai un PASS, mai un hard-stop inventato).
        for peer in peers:
            high, low_peer = (peer, cash) if peer >= cash else (cash, peer)
            if self._limit_exceeded(cap_raw, pct_cap_raw, low_peer, high):
                return self._mark_history_insufficient("letture_con_lo_stesso_istante", at, cash, epoch,
                                                       reference=high)
        return ""

    def _mark_history_insufficient(self, why: str, at: float, cash: Decimal, epoch: int,
                                   reference: Optional[Decimal] = None) -> str:
        """D-631-11: memorizza (persistito) lo stato di storia insufficiente: distinto dall'hard-stop, nessuna perdita
        dichiarata, nuovi ingressi negati fino allo sblocco owner."""
        with self._lock:
            if self.unreadable or self._scope_block_locked():
                return ""
            if self.history_insufficient.get("active"):
                return "insufficient"
            self.history_insufficient = {
                "active": True, "state": STATE_HISTORY_INSUFFICIENT, "reason": why, "at": _now(),
                "reading_monotonic": at, "cash": str(cash),
                "known_peak_bound": None if reference is None else str(reference), "peak_epoch": epoch,
                "cleared": list(self.history_insufficient.get("cleared", [])),
            }
            logger.critical("D-1: %s (%s): lettura del conto non valutabile con la cronologia in memoria (cassa %s, "
                            "epoca %s). Nessun superamento dichiarato; nuovi ingressi negati fino allo sblocco owner",
                            STATE_HISTORY_INSUFFICIENT, why, cash, epoch)
            self._persist_locked()
            return "insufficient"

    def owner_clear_history_insufficient(self, *, authorization: str, reason: str = "", operation_id: str = "",
                                         confirm: bool = False, checks: Optional[dict] = None) -> bool:
        """D-631-11: sblocco owner dello stato di storia insufficiente, con le stesse regole dello sblocco dell'hard-stop
        (autorizzazione, motivo, operation_id monouso, conferma, verifiche del chiamante registrate, salvataggio prima
        che valga). Nessun percorso automatico (riavvio, START, subentro, ribasamento) lo chiama. Non sblocca un
        superamento ne' ribasa il picco."""
        ref, why, op = (str(v or "").strip() for v in (authorization, reason, operation_id))
        if not (ref and why and op and confirm is True):
            return False
        with self._lock:
            if self.unreadable or not self.history_insufficient.get("active") or self._scope_block_locked():
                return False
            before = dict(self.history_insufficient)
            history = list(before.pop("cleared", []))
            if any(isinstance(h, dict) and h.get("operation_id") == op for h in history):
                return False
            entry = {"at": _now(), "authorization": ref, "reason": why, "operation_id": op,
                     "checks": dict(checks or {}), "state": before}
            self.history_insufficient = {"active": False, "cleared": history + [entry]}
            if not self._persist_locked(marker_fallback=False):
                self.history_insufficient = dict(before, cleared=history)
                return False
            self._close_history_locked()
            logger.warning("D-1: storia insufficiente sbloccata dall'owner (op %s, autorizzazione %s): %s", op, ref, why)
            return True

    # ------------------------------------------------------------ procedura owner (mai automatica)
    def owner_clear_breach(self, *, authorization: str, reason: str = "", operation_id: str = "",
                           confirm: bool = False, checks: Optional[dict] = None) -> bool:
        """Sblocco di un hard-stop memorizzato (contratto owner PF-ROSERPINA-D2-LOCKDOWN-UNLOCK-FINAL-20261109, punto 5).

        Solo con: riferimento d'autorizzazione owner, motivo, identificativo dell'operazione e conferma esplicita;
        ``checks`` (cassa, esposizione, ordini) verificati dal chiamante e registrati. Registrazione durevole nello
        stato persistito PRIMA che lo sblocco valga: salvataggio fallito => resta bloccato (fail-closed). NON
        ribasa il picco (nuova baseline = ``owner_rebase_peak``, operazione distinta). Nessun percorso automatico
        (riavvio, settlement, reset strategico, nuovo segnale) la chiama."""
        ref, why, op = (str(v or "").strip() for v in (authorization, reason, operation_id))
        if not (ref and why and op and confirm is True):
            return False
        with self._lock:
            if self.unreadable or not self.breached or self._scope_block_locked():
                return False
            before = dict(self.breach)
            history = list(before.pop("cleared", []))
            if any(isinstance(h, dict) and h.get("operation_id") == op for h in history):
                return False                             # operation_id gia' usato: nessun doppio sblocco

            entry = {"at": _now(), "authorization": ref, "reason": why, "operation_id": op,
                     "checks": dict(checks or {}), "breach": before}
            self.breached = False
            self.breach = {"cleared": history + [entry]}
            if not self._persist_locked(marker_fallback=False):
                self.breached, self.breach = True, dict(before, cleared=history)   # nessuno sblocco non registrato
                return False
            self._close_history_locked()
            logger.warning("D-1: hard-stop sbloccato dall'owner (op %s, autorizzazione %s): %s", op, ref, why)
            return True

    def owner_rebase_peak(self, new_peak: Any, *, authorization: str, reason: str = "", operation_id: str = "",
                          confirm: bool = False) -> bool:
        """Nuova baseline del picco: SOLO procedura owner esplicita e tracciata, distinta dallo sblocco (contratto 9).
        Salvataggio fallito => picco precedente ripristinato."""
        ref, why, op = (str(v or "").strip() for v in (authorization, reason, operation_id))
        peak = _cash(new_peak)
        if not (ref and why and op and confirm is True) or peak is None:
            return False
        with self._lock:
            if self.unreadable or self._scope_block_locked():
                return False                             # S18-1: mai il picco di un altro ambito (rebase in SIM su LIVE)
            if op in self.rebase_ops:
                return False                             # operation_id gia' usato: nessun replay (Grok su b9cbe0d7)
            old_peak, old_breach, old_epoch, old_at = self.peak, dict(self.breach), self.peak_epoch, self.rebased_at
            # S19-B (Sol su 5bb61f7f): la finestra si riapre qui PRIMA del salvataggio e azzera il picco confermato;
            # un salvataggio fallito ripristina TUTTO lo stato precedente, picco confermato e istante inclusi.
            old_window = (self._window_open, self._window_base, list(self._history), list(self._ladder),
                          list(self._valleys), self._confirmed_peak, self.baseline_confirmed_at)
            self.peak = peak
            self.peak_epoch = old_epoch + 1
            self.rebased_at = time.monotonic()
            self._open_window_locked(self.rebased_at)    # le letture dell'epoca precedente non fanno piu' da massimo
            self.rebase_ops.append(op)
            self.breach = dict(self.breach, rebased={"at": _now(), "authorization": ref, "reason": why,
                                                     "operation_id": op,
                                                     "from": None if old_peak is None else str(old_peak),
                                                     "to": str(peak)})
            if not self._persist_locked(marker_fallback=False):
                self.peak, self.breach, self.peak_epoch = old_peak, old_breach, old_epoch
                self.rebased_at = old_at
                (self._window_open, self._window_base, self._history, self._ladder, self._valleys,
                 self._confirmed_peak, self.baseline_confirmed_at) = old_window
                self.rebase_ops.remove(op)
                return False
            logger.warning("D-1: picco dell'hard-stop ribasato dall'owner %s -> %s (op %s)", old_peak, peak, op)
            return True

    def owner_assign_scope(self, *, authorization: str, reason: str = "", operation_id: str = "",
                           confirm: bool = False, checks: Optional[dict] = None) -> bool:
        """S18-1: procedura owner esplicita e tracciata che attribuisce lo stato persistito (di un altro ambito o senza
        provenienza) all'ambito corrente. NON azzera nulla: picco, superamento, storia insufficiente ed epoca restano;
        la baseline torna NON verificata (conferma owner obbligatoria, ed eventuale ribasamento tracciato del picco). Le
        letture iniziate prima non si valutano piu'. Salvataggio fallito => nulla cambia. Fino alla separazione completa
        per ambiente e conto (PR-04) e' l'unica uscita dalla protezione."""
        ref, why, op = (str(v or "").strip() for v in (authorization, reason, operation_id))
        if not (ref and why and op and confirm is True):
            return False
        with self._lock:
            if self.unreadable or self.scope is None or not self.scope.get("account"):
                return False
            if self._scope_block_locked() not in (REASON_SCOPE_MISMATCH, REASON_SCOPE_UNATTRIBUTED):
                return False                             # nulla da attribuire
            if any(isinstance(h, dict) and h.get("operation_id") == op for h in self.scope_history):
                return False
            old = (self.record_scope, self.scope_legacy, self.baseline_verified, list(self.scope_history))
            entry = {"at": _now(), "authorization": ref, "reason": why, "operation_id": op,
                     "from": "senza_provenienza" if self.scope_legacy else dict(self.record_scope or {}),
                     "to": dict(self.scope), "checks": dict(checks or {}),
                     "nota": "attribuzione owner: nessun reset; baseline da confermare"}
            self.record_scope, self.scope_legacy = dict(self.scope), False
            self.baseline_verified = False
            self.scope_history = self.scope_history + [entry]
            if not self._persist_locked(marker_fallback=False):
                self.record_scope, self.scope_legacy, self.baseline_verified, self.scope_history = old
                return False
            self._close_history_locked()
            logger.warning("D-1: stato del drawdown attribuito dall'owner all'ambito %s (op %s)", self.scope, op)
            return True

    # ------------------------------------------------------------ decisione
    def loss(self, cash_equity: Any) -> Optional[Decimal]:
        """Perdita dal picco (Decimal, >= 0); None se cassa o picco non valutabili. Cassa 0 = perdita totale."""
        cash = _observed_cash(cash_equity)
        with self._lock:
            if cash is None or self.peak is None:
                return None
            return max(Decimal(0), self.peak - cash)

    def loss_pct(self, cash_equity: Any) -> Optional[float]:
        """D-2 / RO-DD-04: drawdown % dal picco persistente (perdita di cassa, non esposizione); None se non valutabile."""
        loss = self.loss(cash_equity)
        with self._lock:
            if loss is None or self.peak is None or self.peak <= 0:
                return None
            return float(loss / self.peak * Decimal(100))

    def retry_persist(self) -> bool:
        """Ritenta il salvataggio di un picco non persistito; True se lo stato ora e' durevole."""
        with self._lock:
            if self.persist_failed and self.peak is not None and not self.unreadable:
                self._persist_locked()
            return not self.persist_failed

    def state_reliable(self) -> bool:
        """False se lo stato persistito e' illeggibile: il picco non e' noto e il desk va in LOCKDOWN (fail-closed).
        Un salvataggio fallito NON lo rende inaffidabile in memoria (il picco osservato resta valido nel processo):
        gli hard-stop configurati negano comunque (`REASON_NOT_PERSISTED`) e il salvataggio si ritenta."""
        with self._lock:
            return not self.unreadable

    def reason(self, cap_raw: Any, cash_equity: Any, *, pct_cap_raw: Any = None) -> str:
        """'' se l'ingresso e' ammesso, altrimenti il motivo.

        Un superamento memorizzato blocca SEMPRE, anche se nel frattempo il limite e' stato tolto dalla
        configurazione: solo la procedura owner lo sblocca. Senza superamento e senza limiti configurati => ''."""
        with self._lock:
            if self.breached and not self.unreadable:
                return REASON_PCT_ACTIVE if self.breach.get("kind") == KIND_PCT else REASON_ACTIVE
        if cap_raw is None and pct_cap_raw is None:
            return ""
        if cap_raw is not None:
            cap = validators.finite_number(cap_raw)
            if cap is None or cap <= 0:
                return REASON_INVALID
        if pct_cap_raw is not None:
            pct = validators.finite_number(pct_cap_raw)
            if pct is None or pct <= 0 or pct > 100:
                return REASON_PCT_INVALID
        if _observed_cash(cash_equity) is None:
            return REASON_CASH_UNVERIFIED
        self.evaluate(cap_raw, cash_equity, pct_cap_raw=pct_cap_raw)
        with self._lock:
            if self.unreadable:
                return REASON_STATE_UNREADABLE
            if self.breached:
                return REASON_PCT_ACTIVE if self.breach.get("kind") == KIND_PCT else REASON_ACTIVE
            if self.persist_failed:
                return REASON_NOT_PERSISTED
        if _cash(cash_equity) is None:
            return REASON_CASH_UNVERIFIED                # cassa 0 sotto il limite: valutata, ma nessun ingresso
        return ""

    def snapshot(self) -> dict:
        with self._lock:
            return {"peak": None if self.peak is None else float(self.peak), "seq": self.seq,
                    "unreadable": self.unreadable, "persist_failed": self.persist_failed,
                    "breached": self.breached, "breach": dict(self.breach), "session_open": self.session_open,
                    "baseline_verified": self.baseline_verified, "baseline": dict(self.baseline),
                    "history_insufficient": dict(self.history_insufficient)}
