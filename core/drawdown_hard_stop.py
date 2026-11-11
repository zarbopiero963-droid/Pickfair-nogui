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

import json
import logging
import os
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from core import validators
from core.atomic_io import atomic_write_text

logger = logging.getLogger(__name__)

STATE_KEY = "roserpina.drawdown_hard_stop_state"

REASON_INVALID = "max_drawdown_abs_non_valido"
REASON_ACTIVE = "max_drawdown_abs_active"
REASON_STATE_UNREADABLE = "max_drawdown_abs_stato_illeggibile"
REASON_NOT_PERSISTED = "max_drawdown_abs_picco_non_persistito"
REASON_CASH_UNVERIFIED = "max_drawdown_abs_cassa_non_verificata"
REASON_PCT_ACTIVE = "drawdown_hard_stop_active"
REASON_BASELINE_UNVERIFIED = "drawdown_baseline_non_verificata"
REASON_PCT_INVALID = "max_drawdown_hard_stop_pct_non_valido"
KIND_ABS = "max_drawdown_abs"
KIND_PCT = "max_drawdown_hard_stop_pct"


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
    if (peak is not None and (not peak.is_finite() or peak <= 0)) or type(seq) is not int or seq < 0 \
            or type(breached) is not bool or type(session_open) is not bool or type(verified) is not bool \
            or type(epoch) is not int or epoch < 0 \
            or not isinstance(rebase_ops, list) or not all(isinstance(op, str) for op in rebase_ops):
        raise ValueError("record non valido")
    return {"peak": peak, "seq": seq, "peak_epoch": epoch, "rebase_ops": list(rebase_ops), "breached": breached, "breach": dict(record.get("breach") or {}),
            "session_open": session_open, "baseline_verified": verified,
            "baseline": dict(record.get("baseline") or {})}


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
            self.peak_epoch = epoch
            # Grok su b9cbe0d7: gli operation_id dei ribasamenti restano consumati (unione di memoria e record).
            for r in records + stale:
                for op in r["rebase_ops"]:
                    if op not in self.rebase_ops:
                        self.rebase_ops.append(op)
            self.baseline = dict(records[0]["baseline"])
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
            elif not self.breached or newer:
                # Sol/Codex su 4a171465: nessun superamento attivo, ma la storia degli sblocchi (operation_id gia'
                # usati) e dei ribasamenti si riprende dal disco: un riavvio non rende di nuovo valido un operation_id
                # consumato.
                self.breach = dict(records[0]["breach"])
            if not reconcile:
                return
            if any(r.get("from_marker") for r in records):
                self._persist_locked()                   # RO-DD-09: il picco del marker torna nel DB
            elif any(r.get("from_marker") for r in stale):
                self._remove_stale_marker()

    def _persist_locked(self, *, marker_fallback: bool = True) -> bool:
        """Salva lo stato nel DB. ``marker_fallback`` False (Sol su 92155c24) per le transizioni che ALLENTANO un
        blocco (sblocco owner, conferma della baseline, ribasamento del picco, chiusura della sessione): se il DB non
        salva non va scritto nemmeno il marker, altrimenti un'operazione fallita (stato in RAM ripristinato) resterebbe
        su disco come record piu' recente e al riavvio varrebbe lo stesso. Il marker resta il ripiego dei soli stati
        che stringono (picco che sale, superamento, sessione aperta)."""
        saver = getattr(self._db, "save_settings", None)
        seq = self.seq + 1
        record = {"peak": None if self.peak is None else str(self.peak), "seq": seq, "peak_epoch": self.peak_epoch,
                  "rebase_ops": list(self.rebase_ops),
                  "breached": self.breached,
                  "breach": dict(self.breach), "session_open": self.session_open,
                  "baseline_verified": self.baseline_verified, "baseline": dict(self.baseline), "updated_at": _now()}
        payload = json.dumps(record)
        marker = _marker_path(self._db)
        try:
            if not callable(saver):
                raise RuntimeError("DB senza save_settings")
            saver({STATE_KEY: payload})
        except Exception:
            logger.exception("D-1: picco dell'hard-stop non salvato nel DB: nuovi ingressi negati finche' non si salva")
            self.persist_failed = True
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
            return self._persist_locked()

    def close_session(self) -> bool:
        """STOP pulito: chiude la sessione su disco SOLO se il picco e' durevole. Picco non salvato o stato
        illeggibile => la sessione resta aperta e il prossimo avvio chiede la conferma owner della baseline."""
        with self._lock:
            if self.unreadable or self.persist_failed:
                return False
            self.session_open = False
            if not self._persist_locked(marker_fallback=False):
                self.session_open = True
                return False
            return True

    def baseline_block_reason(self) -> str:
        with self._lock:
            return "" if self.baseline_verified else REASON_BASELINE_UNVERIFIED

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
            if self.unreadable or self.baseline_verified:
                return False
            history = list(self.baseline.get("confirmations", []))
            if any(isinstance(h, dict) and h.get("operation_id") == op for h in history):
                return False
            old = (self.peak, dict(self.baseline))
            candidates = [v for v in (self.peak, declared, cash) if v is not None]
            self.peak = max(candidates)
            entry = {"at": _now(), "authorization": ref, "reason": why, "operation_id": op,
                     "previous_peak": None if old[0] is None else str(old[0]),
                     "declared_peak": None if declared is None else str(declared), "cash": str(cash),
                     "baseline_peak": str(self.peak), "checks": dict(checks or {}),
                     "nota": "conferma owner della baseline: non e' una prova del massimo storico"}
            self.baseline = {"confirmations": history + [entry]}
            self.baseline_verified = True
            if not self._persist_locked(marker_fallback=False):
                self.peak, self.baseline = old
                self.baseline_verified = False
                return False
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
            if self.unreadable:
                return                                   # mai sovrascrivere uno stato che non si e' potuto leggere
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
        loss = self.loss(cash_equity)
        if loss is None:
            return False
        kind, cap = None, None
        abs_cap = validators.finite_number(cap_raw) if cap_raw is not None else None
        if abs_cap is not None and abs_cap > 0 and loss >= validators.money_decimal(abs_cap):
            kind, cap = KIND_ABS, validators.money_decimal(abs_cap)
        pct_cap = validators.finite_number(pct_cap_raw) if pct_cap_raw is not None else None
        if kind is None and pct_cap is not None and 0 < pct_cap <= 100:
            with self._lock:
                threshold = self.peak * validators.money_decimal(pct_cap) / Decimal(100) if self.peak else None
            if threshold is not None and loss >= threshold:
                kind, cap = KIND_PCT, validators.money_decimal(pct_cap)
        if kind is None:
            return False
        with self._lock:
            if self.breached or self.unreadable:
                return False
            self.breached = True
            self.breach = {"kind": kind, "at": _now(), "loss": str(loss), "cap": str(cap), "peak": str(self.peak),
                           "cash": str(validators.money_decimal(cash_equity)),
                           "cleared": list(self.breach.get("cleared", [])) if isinstance(self.breach, dict) else []}
            logger.critical("D-1: hard-stop %s superato (perdita %s, limite %s, picco %s): blocco persistente",
                            kind, loss, cap, self.peak)
            self._persist_locked()
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
            if self.unreadable or not self.breached:
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
            if self.unreadable:
                return False
            if op in self.rebase_ops:
                return False                             # operation_id gia' usato: nessun replay (Grok su b9cbe0d7)
            old_peak, old_breach, old_epoch = self.peak, dict(self.breach), self.peak_epoch
            self.peak = peak
            self.peak_epoch = old_epoch + 1
            self.rebase_ops.append(op)
            self.breach = dict(self.breach, rebased={"at": _now(), "authorization": ref, "reason": why,
                                                     "operation_id": op,
                                                     "from": None if old_peak is None else str(old_peak),
                                                     "to": str(peak)})
            if not self._persist_locked(marker_fallback=False):
                self.peak, self.breach, self.peak_epoch = old_peak, old_breach, old_epoch
                self.rebase_ops.remove(op)
                return False
            logger.warning("D-1: picco dell'hard-stop ribasato dall'owner %s -> %s (op %s)", old_peak, peak, op)
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
                    "baseline_verified": self.baseline_verified, "baseline": dict(self.baseline)}
