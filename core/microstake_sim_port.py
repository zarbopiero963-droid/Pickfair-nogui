"""Adapter SIM del Microstake nativo (PF-MICROSTAKE-NATIVE-INTEGRATION-02, lotti M2/M3).

Collega la saga di `core.microstake_safe` al `SimulationBroker` DI PRODUZIONE usando solo operazioni pubbliche con
semantica Betfair: `place_bet`, `cancel_orders` (con `sizeReduction`), `replace_orders` (nuovo betId),
`get_current_orders`. Nessuna mutazione diretta dello stato privato del broker.

Contiene inoltre:
- `DbSagaStore`: stato della saga persistito nel database dell'applicazione (riavvio-sicuro);
- `SimulationQuoteProvider`: prova del book dal broker SIM (stub passivo che non incrocia la controparte);
- `build_native_microstake_executor`: costruzione dell'esecutore nativo per l'engine.

La disponibilita' LIVE e' obiettivo di prodotto (decisione owner PF-MICROSTAKE-NATIVE-AUTO-01) ma qui NON esiste un
adapter LIVE: in LIVE l'engine resta fail-closed (`MICROSTAKE_EXECUTOR_UNAVAILABLE`).
"""
from __future__ import annotations

import json
import logging
import threading
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from core.microstake_safe import (
    LostResponse,
    MicrostakeCoordinator,
    MicrostakePolicy,
    NativeMicrostakeExecutor,
    Phase,
    Side,
    SimOrder,
    order_exposure,
)

logger = logging.getLogger(__name__)

D = Decimal
SAGA_SETTINGS_KEY = "microstake.sagas"
#: Rifiuti PRE-invio del SimulationBroker (validazione degli argomenti): l'unica
#: prova che nulla e' partito. Qualunque altra eccezione dopo la chiamata e' un
#: esito ignoto (vedi `simulation_broker._PRE_SEND_REJECTIONS`, F10).
PRE_SEND_REJECTIONS = frozenset({"INVALID_MARKET_ID", "INVALID_SELECTION_ID", "INVALID_PRICE", "INVALID_SIZE",
                                 "INVALID_SIDE", "DUPLICATE_CUSTOMER_REF"})
_TERMINAL = frozenset({"CANCELLED", "LAPSED", "VOIDED", "EXPIRED", "SETTLED"})


class SagaStoreError(RuntimeError):
    """Stato della saga illeggibile: nessuna decisione puo' basarsi su di esso."""


def _dec(value: Any) -> Decimal:
    try:
        out = D(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"valore monetario non valido: {value!r}") from exc
    if not out.is_finite():
        raise ValueError(f"valore monetario non finito: {value!r}")
    return out


# ---------------------------------------------------------------------------- scala dei prezzi Betfair
_LADDER = ((2.0, 0.01), (3.0, 0.02), (4.0, 0.05), (6.0, 0.1), (10.0, 0.2), (20.0, 0.5),
           (30.0, 1.0), (50.0, 2.0), (100.0, 5.0), (1000.0, 10.0))


def betfair_tick(price: float) -> float:
    for upper, tick in _LADDER:
        if price < upper:
            return tick
    return 10.0


def passive_price_beyond(best: float, side: str) -> Optional[Decimal]:
    """Primo prezzo della scala che NON incrocia la controparte: sopra il best per un BACK, sotto per un LAY."""
    if side == "BACK":
        candidate = round(best + betfair_tick(best), 2)
        return _dec(candidate) if candidate <= 1000.0 else None
    candidate = round(best - betfair_tick(max(1.01, best - 1e-9)), 2)
    return _dec(candidate) if candidate >= 1.01 else None


# ---------------------------------------------------------------------------- store della saga su DB
class DbSagaStore:
    """Stessa interfaccia di `JsonSagaStore`, ma nel database dell'app (chiave settings `microstake.sagas`).

    Scrittura atomica per chiave (transazione del DB). JSON corrotto => `SagaStoreError`, mai un dizionario vuoto
    silenzioso: un saga dimenticato potrebbe lasciare uno stub da 2 EUR sull'exchange.
    """

    def __init__(self, db: Any, key: str = SAGA_SETTINGS_KEY) -> None:
        if not callable(getattr(db, "save_settings", None)) or not callable(getattr(db, "get_settings", None)):
            raise TypeError("db senza save_settings/get_settings")
        self.db = db
        self.key = key
        self._lock = threading.RLock()

    def _all(self) -> Dict[str, Dict[str, Any]]:
        raw = (self.db.get_settings() or {}).get(self.key)
        if raw in (None, ""):
            return {}
        try:
            data = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise SagaStoreError(f"SAGA_STORE_CORRUPT:{exc}") from exc
        if not isinstance(data, dict):
            raise SagaStoreError("SAGA_STORE_CORRUPT:not_an_object")
        return data

    def save(self, ref: str, **values: Any) -> None:
        with self._lock:
            data = self._all()
            current = dict(data.get(ref) or {})
            for k, v in values.items():
                if isinstance(v, Enum):
                    v = v.value
                if isinstance(v, Decimal):
                    v = str(v)
                current[k] = v
            data[ref] = current
            self.db.save_settings({self.key: json.dumps(data, sort_keys=True)})

    def load(self, ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._all().get(ref)

    def pending_refs(self) -> List[str]:
        """Ref con saga non conclusa (ne' ACTIVE ne' terminale): da riconciliare al riavvio."""
        with self._lock:
            return sorted(ref for ref, v in self._all().items()
                          if str((v or {}).get("phase")) not in {"ACTIVE", "FAILED", "ROLLED_BACK", "BLOCKED"})


# ---------------------------------------------------------------------------- porta exchange sul broker SIM
class SimulationExchangePort:
    """Interfaccia exchange della saga (`place`, `cancel_reduction`, `replace`, `cancel_all_remaining`,
    `find_by_ref`, `order`, `reserve`, `release_reservation`) sopra il `SimulationBroker` reale."""

    hide_reconciliation = False

    def __init__(self, broker: Any) -> None:
        for name in ("place_bet", "cancel_orders", "replace_orders", "list_current_orders", "get_current_orders"):
            if not callable(getattr(broker, name, None)):
                raise TypeError(f"broker senza {name}")
        self.broker = broker
        self.operation_log: List[tuple] = []
        self.snapshot_failures = 0
        self.last_snapshot_persisted = True     # esito dell'ULTIMO snapshot (rilievo Codex #512 su 034af59)
        self._faults: List[str] = []
        self._lock = threading.RLock()
        self._reservations: Dict[str, Decimal] = {}

    # -- prove di guasto (solo test) ------------------------------------------------------------------
    def inject_fault(self, fault: str) -> None:
        self._faults.append(str(fault))

    def _take(self, fault: str) -> bool:
        if fault in self._faults:
            self._faults.remove(fault)
            return True
        return False

    # -- mapping ---------------------------------------------------------------------------------------
    @staticmethod
    def _to_order(row: Dict[str, Any]) -> SimOrder:
        status = str(row.get("status") or "").upper()
        ps = row.get("priceSize") or {}
        size = _dec(ps.get("size", 0))
        matched = _dec(row.get("sizeMatched", 0))
        remaining = D("0") if status in _TERMINAL else max(D("0"), size - matched)
        return SimOrder(
            bet_id=str(row.get("betId")), market_id=str(row.get("marketId")),
            selection_id=int(row.get("selectionId")), side=Side(str(row.get("side")).upper()),
            price=_dec(ps.get("price", 0)), size=size, matched=matched, remaining=remaining,
            customer_ref=str(row.get("customerOrderRef") or ""),
        )

    def _rows(self, market_id: Optional[str] = None) -> List[Dict[str, Any]]:
        res = self.broker.list_current_orders([market_id] if market_id else None)
        return list((res or {}).get("currentOrders") or [])

    def order(self, bet_id: str) -> SimOrder:
        for row in self._rows():
            if str(row.get("betId")) == str(bet_id):
                return self._to_order(row)
        raise KeyError(bet_id)

    def find_by_ref(self, customer_ref: str) -> Optional[SimOrder]:
        if self.hide_reconciliation:
            return None
        rows = [r for r in (self.broker.get_current_orders() or []) if str(r.get("customerOrderRef") or "") == customer_ref]
        if len(rows) > 1:
            raise RuntimeError("duplicate customer reference")
        return self._to_order(rows[0]) if rows else None

    # -- riserva transitoria (in-process; la riserva cross-process durevole e' PR29, fuori scope) -------
    def reserve(self, customer_ref: str, exposure: Decimal, cap: Decimal) -> bool:
        with self._lock:
            if customer_ref in self._reservations:
                return True
            if sum(self._reservations.values(), D("0")) + exposure > cap:
                return False
            self._reservations[customer_ref] = exposure
            return True

    def release_reservation(self, customer_ref: str) -> None:
        with self._lock:
            self._reservations.pop(customer_ref, None)

    def has_reservation(self, customer_ref: str) -> bool:
        with self._lock:
            return customer_ref in self._reservations

    def hold_reservation(self, customer_ref: str, exposure: Decimal) -> None:
        """Ripristina una riserva senza verifica del cap: copre esposizione GIA' sull'exchange (saga retrocessa
        ad AMBIGUOUS dopo uno snapshot finale non persistito). Un cap superato blocca i microstake successivi,
        mai quello gia' inviato."""
        with self._lock:
            self._reservations[customer_ref] = exposure

    def _snapshot(self) -> bool:
        """Snapshot del broker dopo ogni operazione: la saga deve ritrovare lo stub anche dopo un crash.
        Un fallimento non e' silenzioso (rilievo Grok #512): viene contato e dichiarato nel risultato
        dell'esecutore; lo stub resta comunque nella saga persistita e un riavvio lo riconcilia AMBIGUOUS.
        Ritorna l'esito, memorizzato anche in `last_snapshot_persisted`."""
        persist = getattr(self.broker, "persist_snapshot", None)
        self.last_snapshot_persisted = not (callable(persist) and persist() is False)
        if not self.last_snapshot_persisted:
            self.snapshot_failures += 1
            logger.warning("Microstake: snapshot del broker SIM non persistito (fallimenti=%s)", self.snapshot_failures)
        return self.last_snapshot_persisted

    # -- operazioni exchange ---------------------------------------------------------------------------
    def place(self, *, market_id: str, selection_id: int, side: Side, size: Decimal, price: Decimal,
              customer_ref: str) -> SimOrder:
        if self.find_by_ref(customer_ref) is not None:
            raise RuntimeError("DUPLICATE_CUSTOMER_REF")
        try:
            res = self.broker.place_bet(market_id=str(market_id), selection_id=int(selection_id), side=side.value,
                                        price=float(price), size=float(size), customer_ref=customer_ref)
        except RuntimeError as exc:
            if str(exc) in PRE_SEND_REJECTIONS:
                raise                                   # rifiuto certo, nulla e' partito
            self.operation_log.append(("PLACE", "", str(size), str(price), customer_ref))
            raise LostResponse(f"place outcome unknown: {exc}") from exc
        except Exception as exc:                        # timeout, rete, guasto del trasporto: l'ordine puo' esistere
            self.operation_log.append(("PLACE", "", str(size), str(price), customer_ref))
            raise LostResponse(f"place outcome unknown: {type(exc).__name__}") from exc
        report = ((res or {}).get("instructionReports") or [{}])[0]
        bet_id = str(report.get("betId") or "")
        self.operation_log.append(("PLACE", bet_id, str(size), str(price), customer_ref))
        if str(report.get("status")).upper() != "SUCCESS" or not bet_id:
            raise RuntimeError(f"PLACE_REJECTED:{report.get('orderStatus') or report.get('errorCode') or 'FAILURE'}")
        if self._take("LOST_PLACE_RESPONSE"):
            self._snapshot()
            raise LostResponse("accepted but response lost")
        try:
            self._snapshot()
            return self.order(bet_id)
        except Exception as exc:
            # Riesame indipendente ciclo 12 (#512): ordine ACCETTATO (betId noto), lettura successiva fallita. Mai un
            # errore generico, che nel coordinator rilascerebbe la riserva e chiuderebbe FAILED con lo stub vivo:
            # esito da riconciliare per customer_ref (find_by_ref), come una risposta persa.
            raise LostResponse(f"accepted but post-accept read failed: {type(exc).__name__}") from exc

    def cancel_reduction(self, bet_id: str, reduction: Decimal) -> SimOrder:
        current = self.order(bet_id)
        try:
            res = self.broker.cancel_orders(market_id=current.market_id,
                                            instructions=[{"betId": bet_id, "sizeReduction": float(reduction)}])
        except Exception as exc:
            self.operation_log.append(("CANCEL", bet_id, str(reduction)))
            raise LostResponse(f"cancel outcome unknown: {type(exc).__name__}") from exc
        report = ((res or {}).get("instructionReports") or [{}])[0]
        self.operation_log.append(("CANCEL", bet_id, str(reduction)))
        if str(report.get("status")).upper() != "SUCCESS":
            raise RuntimeError("INVALID_SIZE_REDUCTION")
        self._snapshot()
        if self._take("LOST_CANCEL_RESPONSE"):
            raise LostResponse("cancel accepted but response lost")
        return self.order(bet_id)

    def replace(self, bet_id: str, new_price: Decimal) -> SimOrder:
        current = self.order(bet_id)
        if current.matched > 0:
            raise RuntimeError("MATCHED_STUB_CANNOT_REPLACE")
        try:
            res = self.broker.replace_orders(market_id=current.market_id, bet_id=bet_id, new_price=float(new_price))
        except Exception as exc:
            self.operation_log.append(("REPLACE", bet_id, str(new_price), ""))
            raise LostResponse(f"replace outcome unknown: {type(exc).__name__}") from exc
        report = ((res or {}).get("instructionReports") or [{}])[0]
        new_id = str(report.get("betId") or "")
        if str(report.get("status")).upper() != "SUCCESS" or not new_id or new_id == str(bet_id):
            raise RuntimeError("REPLACE_REJECTED")
        self.operation_log.append(("REPLACE", bet_id, str(new_price), new_id))
        self._snapshot()
        if self._take("LOST_REPLACE_RESPONSE"):
            raise LostResponse("replace accepted but response lost")
        return self.order(new_id)

    def cancel_all_remaining(self, bet_id: str) -> SimOrder:
        current = self.order(bet_id)
        res = self.broker.cancel_orders(market_id=current.market_id, instructions=[{"betId": bet_id}])
        report = ((res or {}).get("instructionReports") or [{}])[0]
        self.operation_log.append(("CANCEL", bet_id, str(current.remaining)))
        if str(report.get("status")).upper() != "SUCCESS":
            raise RuntimeError("CANCEL_REJECTED")
        self._snapshot()
        return self.order(bet_id)


# ---------------------------------------------------------------------------- prova del book in SIM
class SimulationQuoteProvider:
    """Prezzo dello stub passivo dal book del broker SIM.

    In simulazione l'exchange E' il broker: la sua assenza di controparte e' la prova che nulla puo' incrociare,
    quindi lo stub si appoggia al prezzo target (nessun replace, stessa identita'). Con controparte: se il target
    non incrocia resta il target, altrimenti il primo tick oltre il best. Mai gli stub storici BACK@1.01/LAY@1000.
    """

    def __init__(self, broker: Any) -> None:
        self.broker = broker

    def __call__(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        side = str(payload.get("bet_type") or payload.get("side") or "").upper()
        target = _dec(payload["price"])
        read = getattr(self.broker, "best_counterparty_price", None)
        best = read(str(payload["market_id"]), int(payload["selection_id"]), side) if callable(read) else None
        if best is None:
            return {"passive_stub_price": target, "best_counterparty_price": None}
        best_d = _dec(best)
        crosses = (side == "BACK" and best_d >= target) or (side == "LAY" and best_d <= target)
        stub = passive_price_beyond(float(best_d), side) if crosses else target
        return {"passive_stub_price": stub, "best_counterparty_price": best_d}


# ---------------------------------------------------------------------------- esecutore nativo per l'engine
class NativeSimMicrostakeExecutor(NativeMicrostakeExecutor):
    """Esecutore sul broker reale: uno store illeggibile DOPO un'operazione exchange e' AMBIGUOUS, mai FAILED.
    Snapshot FINALE del broker non persistito => la saga NON e' ACTIVE: al riavvio il broker ricaricherebbe uno
    stato precedente all'ultima operazione (stub intero o vecchio betId) e `recover()` su ACTIVE non lo
    riconcilierebbe. La saga viene retrocessa ad AMBIGUOUS con riserva tenuta (rilievo Codex #512 su 034af59)."""

    def __init__(self, port: SimulationExchangePort, store: DbSagaStore, *, quote_provider: Callable,
                 transient_exposure_cap: Any) -> None:
        self.port = port
        self.store = store
        coordinator = MicrostakeCoordinator(port, store, MicrostakePolicy())
        super().__init__(coordinator, quote_provider=quote_provider, transient_exposure_cap=transient_exposure_cap)

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        before = len(self.port.operation_log)
        snapshot_failures_before = self.port.snapshot_failures
        try:
            out = super().execute(payload)
            if len(self.port.operation_log) > before:
                # dichiarato, mai silenzioso: la fonte di reload del broker e' aggiornata oppure no
                out["snapshot_persisted"] = self.port.snapshot_failures == snapshot_failures_before
                if out.get("status") == "SUCCESS" and out.get("phase") == Phase.ACTIVE.value \
                        and not self.port.last_snapshot_persisted:
                    out = self._demote_unpersisted_active(payload, out)
            return out
        except SagaStoreError as exc:
            if len(self.port.operation_log) > before:
                return {"status": "AMBIGUOUS", "order_unknown": True, "microstake": True,
                        "error": f"saga_store_unreadable_after_exchange_call:{exc}"}
            out = self._blocked("saga_store_unreadable")
            out["pre_send"] = True          # nessuna operazione exchange: l'intento dedupe si rilascia
            return out
        except Exception as exc:
            # Rilievo Codex r4311806874 (#512 su 3d3f5cc): un errore ordinario del DB (es. sqlite3.OperationalError)
            # DOPO un'operazione exchange non e' un fallimento: lo stub puo' essere vivo. AMBIGUOUS con riserva
            # trattenuta (mai rilasciata qui), intento dedupe consumato, riconciliazione. Prima dell'invio resta
            # l'eccezione originale: nulla e' partito.
            if len(self.port.operation_log) > before:
                ref = str(payload.get("customer_ref"))
                logger.error("Microstake %s: errore di persistenza dopo l'invio, saga AMBIGUOUS (riconciliazione)", ref)
                return {"status": "AMBIGUOUS", "order_unknown": True, "microstake": True,
                        "reservation_held": self.port.has_reservation(ref),
                        "error": f"saga_store_write_failed_after_exchange_call:{type(exc).__name__}"}
            raise

    def _demote_unpersisted_active(self, payload: Dict[str, Any], out: Dict[str, Any]) -> Dict[str, Any]:
        """ACTIVE con snapshot finale non persistito => AMBIGUOUS persistito, riserva ripristinata, risultato
        AMBIGUOUS (`order_unknown`, mai `instructionReports`): riconciliazione, mai retry."""
        ref = str(payload.get("customer_ref"))
        saga = self.store.load(ref) or {}
        self.port.hold_reservation(ref, _dec(saga.get("transient_exposure", "0")))
        self.store.save(ref, phase=Phase.AMBIGUOUS, reservation_held=True, reason="snapshot_not_persisted")
        logger.error("Microstake %s: snapshot finale del broker SIM non persistito, saga AMBIGUOUS (riconciliazione)", ref)
        demoted = {k: v for k, v in out.items() if k != "instructionReports"}
        demoted.update(status="AMBIGUOUS", phase=Phase.AMBIGUOUS.value, reservation_held=True, order_unknown=True,
                       error="snapshot_not_persisted")
        return demoted

    def pending_same_position(self, market_id: Any, selection_id: Any, side: Any) -> List[str]:
        """Ref delle saghe non concluse sulla stessa posizione (mercato, selezione, lato).
        Store illeggibile => SagaStoreError (il chiamante tratta l'errore come 'pendente')."""
        want = (str(market_id), str(selection_id), str(side or "").upper())
        out: List[str] = []
        for ref in self.store.pending_refs():
            req = (self.store.load(ref) or {}).get("request") or {}
            key = (str(req.get("market_id")), str(req.get("selection_id")), str(req.get("side") or "").upper())
            if key == want:
                out.append(ref)
        return out

    def _recomputed_stub_exposure(self, saved: Dict[str, Any]) -> Decimal:
        """Esposizione dello stub ricalcolata da lato e prezzo persistiti; ignota => Decimal('Infinity')."""
        try:
            side = Side(str(((saved.get("request") or {}).get("side")) or "").upper())
            price = Decimal(str(saved.get("stub_price")))
            stub = Decimal(str(getattr(self.coordinator.policy, "exchange_stub_size", "2.00")))
            if not price.is_finite() or price <= 1:
                raise ValueError
            value = order_exposure(side, stub, price)
            if value.is_finite() and value > 0:
                return value
        except (InvalidOperation, ValueError, TypeError, KeyError):
            pass
        return Decimal("Infinity")

    def recover_pending(self) -> Dict[str, str]:
        """Riconciliazione all'avvio: ogni saga non conclusa viene ripresa dalla sua identita' persistita.
        Prima si persiste lo snapshot del broker: se la fonte di reload non si riesce ad allineare allo stato in
        memoria, nessuna saga viene promossa (restano AMBIGUOUS/pendenti; un crash successivo ricaricherebbe uno
        stato vecchio sotto una saga ACTIVE)."""
        out: Dict[str, str] = {}
        pending = self.store.pending_refs()
        # Riserve transitorie ripristinate PRIMA di snapshot e recovery (rilievo Codex r4308704071): dopo un
        # riavvio la porta parte senza riserve, ma ogni saga pendente che le tiene (`reservation_held`, assente
        # = tenuta) copre uno stub che puo' essere sull'exchange; senza di esse un altro microstake supererebbe
        # il cap. Il coordinatore le rilascia solo quando la saga diventa terminale.
        for ref in pending:
            saved = self.store.load(ref) or {}
            if saved.get("reservation_held") is False:
                continue
            try:
                exposure = Decimal(str(saved.get("transient_exposure")))
            except (InvalidOperation, ValueError, TypeError):
                exposure = None
            if exposure is None or not exposure.is_finite() or exposure <= 0:
                # Importo transitorio ignoto (store corrotto): mai il solo stub, la liability di uno stub LAY puo'
                # superarlo (Grok su 9ab753a). Si ricalcola dal lato e dal prezzo dello stub persistiti; se anche
                # quelli mancano, riserva illimitata: nessun altro microstake finche' la saga non e' riconciliata.
                exposure = self._recomputed_stub_exposure(saved)
            self.port.hold_reservation(ref, exposure)
        if pending and not self.port._snapshot():
            return {ref: Phase.AMBIGUOUS.value for ref in pending}
        for ref in pending:
            try:
                out[ref] = self.coordinator.recover(ref).phase.value
            except Exception as exc:  # noqa: BLE001 - riportato, mai nascosto
                out[ref] = f"ERROR:{type(exc).__name__}"
        return out


def build_native_microstake_executor(broker: Any, db: Any, transient_cap_provider: Callable[[Dict[str, Any]], Any]
                                     ) -> NativeSimMicrostakeExecutor:
    port = SimulationExchangePort(broker)
    return NativeSimMicrostakeExecutor(port, DbSagaStore(db), quote_provider=SimulationQuoteProvider(broker),
                                       transient_exposure_cap=transient_cap_provider)
