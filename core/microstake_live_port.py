"""Adapter LIVE del Microstake nativo (PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01, decisione owner 24/10/2026).

Stessa saga e stesso esecutore della SIM (`core.microstake_safe`, `core.microstake_sim_port`), sopra il
`BetfairClient` reale. Solo operazioni pubbliche Betfair con la loro semantica:

- `placeOrders` dello stub al minimo exchange (`core.stake_precision.EXCHANGE_MIN_ORDER`, riferimento owner 1 EUR) a
  un prezzo che non puo' abbinarsi (BACK a 1000, LAY a 1.01), con `customerOrderRef` = identita' della saga;
- `cancelOrders` con `sizeReduction` (stub ridotto esattamente allo stake richiesto, mai ripetuto);
- `replaceOrders` al prezzo richiesto: nuovo betId con lo stesso residuo;
- `listCurrentOrders` per leggere l'ordine e riconciliare, `listMarketBook` per la prova del book.

L'esecuzione diretta sotto il minimo non e' supportata dall'exchange: per questo esiste la saga. Lo stake eseguito e'
quello richiesto o nulla (mai un importo diverso); ogni esito ignoto dopo un invio e' AMBIGUOUS con riserva tenuta e
riconciliazione, mai un nuovo invio. Le saghe LIVE stanno in uno store separato (`microstake.sagas.live`): una saga
SIM non viene mai cercata sull'exchange reale, ne' il contrario.

LIVE resta NO-GO: questo adapter e' raggiungibile solo dal ramo LIVE dell'engine, dopo `is_live_allowed()`.
"""
from __future__ import annotations

import logging
import threading
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from core.microstake_safe import LostResponse, MicrostakePolicy, Side, SimOrder
from core.microstake_sim_port import DbSagaStore, NativeSimMicrostakeExecutor, _dec

logger = logging.getLogger(__name__)

D = Decimal
LIVE_SAGA_SETTINGS_KEY = "microstake.sagas.live"
#: Prezzi Betfair estremi: uno stub BACK a 1000 o LAY a 1.01 non incrocia un book normale (la prova del book lo
#: verifica comunque prima dell'invio).
STUB_PRICE = {"BACK": D("1000"), "LAY": D("1.01")}
#: Rifiuti PRE-invio del `BetfairClient` (validazione degli argomenti o nessuna sessione/circuito aperto):
#: l'unica prova che nulla e' partito. Ogni altro errore dopo la chiamata e' un esito ignoto.
PRE_SEND_REJECTIONS = frozenset({
    "INVALID_MARKET_ID", "INVALID_SELECTION_ID", "INVALID_PRICE", "INVALID_SIZE", "INVALID_SIDE",
    "INVALID_CUSTOMER_ORDER_REF", "INVALID_BET_ID", "INVALID_SIZE_REDUCTION", "NOT_AUTHENTICATED",
    "CIRCUIT_BREAKER_OPEN", "DUPLICATE_CUSTOMER_REF",
})
_DEAD_STATUSES = frozenset({"EXECUTION_COMPLETE"})
#: Esiti di listClearedOrders senza esposizione residua (ordine sparito dagli ordini correnti senza abbinato), e
#: SETTLED (abbinato e regolato). Interrogati in quest'ordine solo quando un betId non e' fra gli ordini correnti.
_CLEARED_STATUSES = ("CANCELLED", "LAPSED", "VOIDED", "SETTLED")


def _report_for(result: Any, bet_id: str) -> Optional[Dict[str, Any]]:
    """Report dell'istruzione che riguarda `bet_id` (CancelInstructionReport: `instruction.betId`; o `betId`).
    Un solo report senza identita' e' accettato come quello dell'unica istruzione inviata; un report che nomina un
    ALTRO betId, o piu' report ambigui, => None (risposta contraddittoria: nessuna prova)."""
    reports = (result or {}).get("instructionReports") if isinstance(result, dict) else None
    if not isinstance(reports, list) or not reports:
        return None
    named = []
    for rep in reports:
        if not isinstance(rep, dict):
            return None
        rid = str((rep.get("instruction") or {}).get("betId") or rep.get("betId") or "")
        named.append((rid, rep))
    exact = [rep for rid, rep in named if rid == str(bet_id)]
    if len(exact) == 1:
        return exact[0]
    if len(named) == 1 and not named[0][0]:
        return named[0][1]
    return None


def _first_report(result: Any) -> Dict[str, Any]:
    reports = (result or {}).get("instructionReports") if isinstance(result, dict) else None
    return reports[0] if isinstance(reports, list) and reports and isinstance(reports[0], dict) else {}


class LiveExchangePort:
    """Interfaccia exchange della saga sopra il `BetfairClient` reale (stessi metodi della porta SIM)."""

    is_live_port = True
    hide_reconciliation = False
    # L'exchange reale e' la fonte autorevole: non c'e' uno snapshot locale da cui ricaricare.
    last_snapshot_persisted = True
    snapshot_failures = 0

    def __init__(self, client: Any) -> None:
        for name in ("place_bet", "reduce_order", "cancel_orders", "replace_orders", "get_current_orders",
                     "list_cleared_orders"):
            if not callable(getattr(client, name, None)):
                raise TypeError(f"client senza {name}")
        self.client = client
        self.operation_log: List[tuple] = []
        self._lock = threading.RLock()
        self._reservations: Dict[str, Decimal] = {}

    def _snapshot(self) -> bool:
        return True

    # -- lettura ---------------------------------------------------------------------------------------
    @staticmethod
    def _to_order(row: Dict[str, Any]) -> SimOrder:
        """Riga di listCurrentOrders => ordine della saga. `size` = abbinato + residuo (la parte cancellata non e'
        piu' esposizione); `remaining` = `sizeRemaining` di Betfair, autorevole."""
        ps = row.get("priceSize") or {}
        # Campi OBBLIGATORI: un `sizeRemaining` mancante letto come zero farebbe sparire un residuo vivo (rollback
        # dichiarato con l'ordine eseguibile). Riga incompleta => ValueError, nessuna prova (audit PF-561 §6).
        if row.get("sizeRemaining") is None or row.get("sizeMatched") is None:
            raise ValueError("CURRENT_ORDER_INCOMPLETE: sizeRemaining/sizeMatched mancanti")
        matched = _dec(row.get("sizeMatched"))
        remaining = _dec(row.get("sizeRemaining"))
        if matched < 0 or remaining < 0:
            raise ValueError("ordine con importi negativi")
        return SimOrder(
            bet_id=str(row.get("betId")), market_id=str(row.get("marketId")),
            selection_id=int(row.get("selectionId")), side=Side(str(row.get("side")).upper()),
            price=_dec(ps.get("price", 0)), size=matched + remaining, matched=matched, remaining=remaining,
            customer_ref=str(row.get("customerOrderRef") or ""),
        )

    def _rows(self, market_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return list(self.client.get_current_orders([market_id] if market_id else None) or [])

    def order(self, bet_id: str) -> SimOrder:
        """Ordine per betId: dagli ordini correnti o, se non c'e' piu', dal suo esito in listClearedOrders.

        Un ordine cancellato/lapsato/annullato senza abbinato sparisce dagli ordini correnti: senza questa lettura
        la saga non saprebbe distinguerlo da un ordine non (ancora) visibile e resterebbe pendente per sempre.
        Lo si restituisce con residuo zero; abbinato = somma delle quote SETTLED (zero se nessuna: nessuna esposizione).
        Assente anche li' => KeyError (identita' non dimostrata: il chiamante resta fail-closed)."""
        for row in self._rows():
            if str(row.get("betId")) == str(bet_id):
                return self._to_order(row)
        # TUTTI gli esiti: lo stesso betId puo' comparire CANCELLED (quota cancellata) e SETTLED (quota abbinata,
        # rilievo GPT-6.1 Sol su #561). L'abbinato e' la somma dei SETTLED, mai lo zero del primo esito trovato.
        found, matched = None, D("0")
        for status in _CLEARED_STATUSES:
            for row in self.client.list_cleared_orders(bet_status=status, bet_ids=[str(bet_id)]) or []:
                if str(row.get("betId")) != str(bet_id):
                    continue
                found = found or row
                if status == "SETTLED":
                    matched += _dec(row.get("sizeSettled", 0))
        if found is not None:
            return SimOrder(
                bet_id=str(bet_id), market_id=str(found.get("marketId")),
                selection_id=int(found.get("selectionId")), side=Side(str(found.get("side")).upper()),
                price=_dec(found.get("priceRequested") or found.get("priceMatched") or 0), size=matched,
                matched=matched, remaining=D("0"), customer_ref=str(found.get("customerOrderRef") or ""),
            )
        raise KeyError(bet_id)

    @staticmethod
    def _dead(order: SimOrder, row: Dict[str, Any]) -> bool:
        """Ordine concluso senza alcun abbinamento: non e' piu' una posizione (es. stub sostituito dal replace)."""
        return str(row.get("status") or "").upper() in _DEAD_STATUSES and order.matched == 0 and order.remaining == 0

    def find_by_ref(self, customer_ref: str) -> Optional[SimOrder]:
        """Ordine vivo con questo `customerOrderRef`. Piu' di uno => errore (identita' non dimostrabile)."""
        if self.hide_reconciliation:
            return None
        live = []
        for row in self._rows():
            if str(row.get("customerOrderRef") or "") != customer_ref:
                continue
            order = self._to_order(row)
            if not self._dead(order, row):
                live.append(order)
        if len(live) > 1:
            raise RuntimeError("duplicate customer reference")
        return live[0] if live else None

    # -- riserva transitoria (stessa semantica della porta SIM) ----------------------------------------
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
        with self._lock:
            self._reservations[customer_ref] = exposure

    # -- operazioni exchange ---------------------------------------------------------------------------
    def place(self, *, market_id: str, selection_id: int, side: Side, size: Decimal, price: Decimal,
              customer_ref: str) -> SimOrder:
        # Lettura PRIMA dell'invio: un ordine con lo stesso ref esiste gia' => nessun secondo invio. Una lettura
        # fallita qui e' pre-invio (l'eccezione esce, nulla e' partito).
        try:
            existing = self.find_by_ref(customer_ref)
        except RuntimeError as exc:
            if "duplicate customer reference" in str(exc):
                # Piu' ordini vivi con questo ref: esistono ordini, mai un "fallimento senza ordine" (review P3).
                raise RuntimeError("DUPLICATE_CUSTOMER_REF") from exc
            raise
        if existing is not None:
            raise RuntimeError("DUPLICATE_CUSTOMER_REF")
        try:
            res = self.client.place_bet(market_id=str(market_id), selection_id=int(selection_id), side=side.value,
                                        price=float(price), size=float(size), customer_ref=customer_ref,
                                        customer_order_ref=customer_ref)
        except RuntimeError as exc:
            if str(exc) in PRE_SEND_REJECTIONS:
                raise
            self.operation_log.append(("PLACE", "", str(size), str(price), customer_ref))
            raise LostResponse(f"place outcome unknown: {exc}") from exc
        except Exception as exc:
            self.operation_log.append(("PLACE", "", str(size), str(price), customer_ref))
            raise LostResponse(f"place outcome unknown: {type(exc).__name__}") from exc
        if not isinstance(res, dict) or res.get("ok") is not True:
            error = str((res or {}).get("error") or "PLACE_REJECTED") if isinstance(res, dict) else "NON_DICT"
            if not isinstance(res, dict) or res.get("order_unknown") is not False:
                # Esito non dimostrato (timeout, DUPLICATE_TRANSACTION, risposta illeggibile): l'ordine puo' esistere.
                self.operation_log.append(("PLACE", "", str(size), str(price), customer_ref))
                raise LostResponse(f"place outcome unknown: {error}")
            if error in PRE_SEND_REJECTIONS:      # NOT_AUTHENTICATED / CIRCUIT_BREAKER_OPEN: mai partito
                raise RuntimeError(error)
            # Rifiuto esplicito di Betfair (fondi, importo, mercato): nessun ordine creato. Registrato come invio
            # (come la porta SIM): l'intento dedupe resta consumato, nessun nuovo invio automatico.
            self.operation_log.append(("PLACE", "", str(size), str(price), customer_ref))
            raise RuntimeError(f"PLACE_REJECTED:{error}")
        report = _first_report(res.get("result"))
        bet_id = str(report.get("betId") or "")
        self.operation_log.append(("PLACE", bet_id, str(size), str(price), customer_ref))
        if not bet_id:
            raise LostResponse("accepted without betId")
        try:
            return self.order(bet_id)
        except Exception as exc:
            # Ordine ACCETTATO (betId noto) ma non rileggibile: esito da riconciliare, mai un fallimento. L'identita'
            # viaggia con l'eccezione e la saga la persiste (rilievo Codex P2 su f1594fb, come per il replacement).
            raise LostResponse(f"accepted but post-accept read failed: {type(exc).__name__}", bet_id=bet_id) from exc

    def cancel_reduction(self, bet_id: str, reduction: Decimal) -> SimOrder:
        current = self.order(bet_id)
        try:
            res = self.client.reduce_order(market_id=current.market_id, bet_id=bet_id, size_reduction=float(reduction))
        except Exception as exc:
            self.operation_log.append(("CANCEL", bet_id, str(reduction)))
            raise LostResponse(f"cancel outcome unknown: {type(exc).__name__}") from exc
        self.operation_log.append(("CANCEL", bet_id, str(reduction)))
        report = _report_for(res, bet_id)
        status = str((report or {}).get("status") or "").upper()
        if status == "FAILURE":
            raise RuntimeError(f"INVALID_SIZE_REDUCTION:{(report or {}).get('errorCode') or ''}")
        if status != "SUCCESS":
            # TIMEOUT, report assente o contraddittorio (PROCESSED_WITH_ERRORS senza esito per questo betId): la
            # riduzione puo' essere avvenuta o no. Mai ripetuta: esito ignoto, decide la riconciliazione.
            raise LostResponse(f"reduction outcome unknown: status={status or 'missing'}")
        try:
            return self.order(bet_id)
        except Exception as exc:
            raise LostResponse(f"cancel accepted but post-cancel read failed: {type(exc).__name__}") from exc

    def replace(self, bet_id: str, new_price: Decimal) -> SimOrder:
        current = self.order(bet_id)
        if current.matched > 0:
            raise RuntimeError("MATCHED_STUB_CANNOT_REPLACE")
        try:
            res = self.client.replace_orders(market_id=current.market_id, bet_id=bet_id, new_price=float(new_price))
        except Exception as exc:
            self.operation_log.append(("REPLACE", bet_id, str(new_price), ""))
            raise LostResponse(f"replace outcome unknown: {type(exc).__name__}") from exc
        report = _first_report(res)
        new_id = str(report.get("betId") or "")
        status = str(report.get("status") or (res or {}).get("status") or "").upper()
        if status == "FAILURE":
            # Rifiuto ESPLICITO: nessun nuovo ordine (Betfair non annulla la cancellazione gia' fatta: il rollback
            # trova lo stub a residuo zero e non chiama un'altra cancellazione).
            self.operation_log.append(("REPLACE_REJECTED", bet_id, str(new_price), ""))
            raise RuntimeError("REPLACE_REJECTED")
        if status != "SUCCESS" or not new_id or new_id == str(bet_id):
            # TIMEOUT, status mancante o SUCCESS senza nuovo betId: il nuovo ordine puo' esistere. Mai un rollback
            # come se non ci fosse: esito ignoto, riconciliazione (rilievo della review indipendente, P2).
            self.operation_log.append(("REPLACE", bet_id, str(new_price), new_id))
            # betId del nuovo ordine noto anche in TIMEOUT: viaggia con l'eccezione e si persiste (review P3).
            raise LostResponse(f"replace outcome unknown: status={status or 'missing'}",
                               bet_id=new_id if new_id and new_id != str(bet_id) else "")
        self.operation_log.append(("REPLACE", bet_id, str(new_price), new_id))
        try:
            return self.order(new_id)
        except Exception as exc:
            # Replace ACCETTATO con betId noto: l'identita' viaggia con l'eccezione e la saga la persiste.
            raise LostResponse(f"replace accepted but post-replace read failed: {type(exc).__name__}",
                               bet_id=new_id) from exc

    def cancel_all_remaining(self, bet_id: str) -> SimOrder:
        """Cancellazione del residuo per il rollback. La PROVA e' la rilettura dell'ordine, non la risposta:
        `BetfairClient.cancel_orders` torna ok=True anche con l'istruzione fallita (PROCESSED_WITH_ERRORS, report
        FAILURE, TIMEOUT), mandato PF-561-LIVE-MICROSTAKE-P1-HARDENING-01. Residuo ancora positivo =>
        CANCEL_NOT_CONFIRMED; rilettura impossibile => esito ignoto. Mai un secondo invio cieco: se la rilettura
        mostra residuo zero (anche dopo una risposta persa) non si cancella di nuovo."""
        current = self.order(bet_id)
        if current.remaining == 0:
            return current                      # niente da cancellare (es. replace che ha gia' cancellato lo stub)
        try:
            res = self.client.cancel_orders(market_id=current.market_id, bet_ids=[bet_id])
        except Exception as exc:                # esito ignoto: decide la rilettura
            res = {"ok": False, "error": type(exc).__name__}
        self.operation_log.append(("CANCEL", bet_id, str(current.remaining)))
        report = _report_for((res or {}).get("result") if isinstance(res, dict) else None, bet_id)
        try:
            after = self.order(bet_id)          # dagli ordini correnti o dal suo esito (listClearedOrders)
        except Exception as exc:
            raise LostResponse(f"cancel outcome unknown: post-cancel read failed: {type(exc).__name__}") from exc
        if after.remaining > 0:
            status = str((report or {}).get("status") or (res or {}).get("error") or "missing").upper()
            raise RuntimeError(f"CANCEL_NOT_CONFIRMED:{status}:{after.remaining}")
        return after


class LiveQuoteProvider:
    """Prova del book dal `listMarketBook` reale: mercato OPEN, selezione ACTIVE, prezzo dello stub che non incrocia.

    Qualunque errore di lettura => nessuna prova (dizionario vuoto => BLOCKED prima dell'invio): mai un'eccezione
    che l'engine leggerebbe come esito ignoto di un ordine mai partito."""

    def __init__(self, client: Any) -> None:
        if not callable(getattr(client, "get_market_book", None)):
            raise TypeError("client senza get_market_book")
        self.client = client

    def __call__(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            side = str(payload.get("bet_type") or payload.get("side") or "").upper()
            stub = STUB_PRICE[side]
            book = self.client.get_market_book(str(payload["market_id"]), include_prices=True)
            if not isinstance(book, dict) or str(book.get("status") or "").upper() != "OPEN":
                return {}
            runner = next((r for r in book.get("runners") or []
                           if str(r.get("selectionId")) == str(payload["selection_id"])), None)
            if not isinstance(runner, dict) or str(runner.get("status") or "").upper() != "ACTIVE":
                return {}
            # BACK si abbina contro i prezzi `availableToBack` (il migliore e' il piu' alto), LAY contro
            # `availableToLay` (il migliore e' il piu' basso).
            ladder = runner.get("availableToBack" if side == "BACK" else "availableToLay") or []
            prices = [_dec(level.get("price")) for level in ladder if isinstance(level, dict)]
            best = (max(prices) if side == "BACK" else min(prices)) if prices else None
            return {"passive_stub_price": stub, "best_counterparty_price": best}
        except Exception as exc:  # noqa: BLE001 - nessuna prova, mai un esito ignoto
            logger.warning("Microstake LIVE: prova del book non disponibile (%s)", type(exc).__name__)
            return {}


class NativeLiveMicrostakeExecutor(NativeSimMicrostakeExecutor):
    """Esecutore LIVE: stessa logica dell'esecutore SIM (store illeggibile dopo un invio => AMBIGUOUS, recovery
    all'avvio, saghe pendenti sulla stessa posizione), policy `live_adapter` sopra la porta LIVE."""

    def __init__(self, port: LiveExchangePort, store: DbSagaStore, *, quote_provider: Callable,
                 transient_exposure_cap: Any) -> None:
        if getattr(port, "is_live_port", False) is not True:
            raise TypeError("porta non LIVE")
        super().__init__(port, store, quote_provider=quote_provider, transient_exposure_cap=transient_exposure_cap,
                         policy=MicrostakePolicy(simulation_only=False, live_adapter=True))
        # Nuova sessione Betfair => nuovo client => nuovo esecutore: le riserve delle saghe pendenti (stub che
        # possono essere vivi) si ripristinano SUBITO, non solo al recovery d'avvio (review F2 su #561). Store
        # illeggibile => eccezione: l'engine non costruisce l'esecutore (fail-closed).
        self.restore_reservations(self.store.pending_refs())

    @property
    def client(self) -> Any:
        return self.port.client


def build_live_microstake_executor(client: Any, db: Any, transient_cap_provider: Callable[[Dict[str, Any]], Any]
                                   ) -> NativeLiveMicrostakeExecutor:
    return NativeLiveMicrostakeExecutor(LiveExchangePort(client), DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY),
                                        quote_provider=LiveQuoteProvider(client),
                                        transient_exposure_cap=transient_cap_provider)
