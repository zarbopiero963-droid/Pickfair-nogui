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
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from core import validators

logger = logging.getLogger(__name__)

STATE_KEY = "roserpina.drawdown_hard_stop_state"

REASON_INVALID = "max_drawdown_abs_non_valido"
REASON_ACTIVE = "max_drawdown_abs_active"
REASON_STATE_UNREADABLE = "max_drawdown_abs_stato_illeggibile"
REASON_NOT_PERSISTED = "max_drawdown_abs_picco_non_persistito"
REASON_CASH_UNVERIFIED = "max_drawdown_abs_cassa_non_verificata"
REASON_PCT_ACTIVE = "drawdown_hard_stop_active"
REASON_PCT_INVALID = "max_drawdown_hard_stop_pct_non_valido"
KIND_ABS = "max_drawdown_abs"
KIND_PCT = "max_drawdown_hard_stop_pct"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _cash(value: Any) -> Optional[Decimal]:
    """Cassa verificata come Decimal; None se assente, non finita o <= 0."""
    try:
        amount = validators.money_decimal(value)
    except ValueError:
        return None
    return amount if amount > 0 else None


class DrawdownHardStop:
    """Picco di cassa persistente e decisione dell'hard-stop ``max_drawdown_abs``."""

    def __init__(self, db: Any = None):
        self._lock = threading.RLock()
        self._db = db
        self.peak: Optional[Decimal] = None
        self.seq = 0
        self.unreadable = False
        self.persist_failed = False
        self.breached = False
        self.breach: dict = {}

    # ------------------------------------------------------------ persistenza
    def restore(self) -> None:
        """Rilegge lo stato salvato. Illeggibile => ``unreadable`` (fail-closed finche' non si chiarisce)."""
        with self._lock:
            self.unreadable = False
            try:
                getter = getattr(self._db, "get_settings", None)
                raw = (getter() or {}).get(STATE_KEY) if callable(getter) else None
            except Exception:
                logger.exception("D-1: stato del picco dell'hard-stop non leggibile")
                self.unreadable = True
                return
            if raw in (None, ""):
                return                                   # nessuno stato: il picco parte dalla prima cassa verificata
            try:
                record = json.loads(raw)
                peak = Decimal(str(record["peak"]))
                seq = record.get("seq")
                breached = record.get("breached", False)
                if not peak.is_finite() or peak <= 0 or type(seq) is not int or seq < 0 or type(breached) is not bool:
                    raise ValueError("record non valido")
            except Exception:
                logger.error("D-1: stato del picco dell'hard-stop illeggibile (%r): nuovi ingressi negati", raw)
                self.unreadable = True
                return
            # Mai sotto un picco gia' osservato in questo processo (START ripetuto nello stesso processo).
            if self.peak is None or peak > self.peak:
                self.peak = peak
            self.seq = max(self.seq, seq)
            if breached:                                 # un superamento salvato non si annulla mai rileggendo
                self.breached = True
                self.breach = dict(record.get("breach") or {})

    def _persist_locked(self) -> bool:
        saver = getattr(self._db, "save_settings", None)
        if not callable(saver):
            self.persist_failed = True
            return False
        seq = self.seq + 1
        record = {"peak": str(self.peak), "seq": seq, "breached": self.breached, "breach": dict(self.breach),
                  "updated_at": _now()}
        try:
            saver({STATE_KEY: json.dumps(record)})
        except Exception:
            logger.exception("D-1: picco dell'hard-stop non salvato: nuovi ingressi negati finche' non si salva")
            self.persist_failed = True
            return False
        self.seq = seq
        self.persist_failed = False
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
            if not self._persist_locked():
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
            old_peak, old_breach = self.peak, dict(self.breach)
            self.peak = peak
            self.breach = dict(self.breach, rebased={"at": _now(), "authorization": ref, "reason": why,
                                                     "operation_id": op,
                                                     "from": None if old_peak is None else str(old_peak),
                                                     "to": str(peak)})
            if not self._persist_locked():
                self.peak, self.breach = old_peak, old_breach
                return False
            logger.warning("D-1: picco dell'hard-stop ribasato dall'owner %s -> %s (op %s)", old_peak, peak, op)
            return True

    # ------------------------------------------------------------ decisione
    def loss(self, cash_equity: Any) -> Optional[Decimal]:
        """Perdita dal picco (Decimal, >= 0); None se cassa o picco non valutabili."""
        cash = _cash(cash_equity)
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
        if _cash(cash_equity) is None:
            return REASON_CASH_UNVERIFIED
        self.evaluate(cap_raw, cash_equity, pct_cap_raw=pct_cap_raw)
        with self._lock:
            if self.unreadable:
                return REASON_STATE_UNREADABLE
            if self.breached:
                return REASON_PCT_ACTIVE if self.breach.get("kind") == KIND_PCT else REASON_ACTIVE
            if self.persist_failed:
                return REASON_NOT_PERSISTED
        return ""

    def snapshot(self) -> dict:
        with self._lock:
            return {"peak": None if self.peak is None else float(self.peak), "seq": self.seq,
                    "unreadable": self.unreadable, "persist_failed": self.persist_failed,
                    "breached": self.breached, "breach": dict(self.breach)}
