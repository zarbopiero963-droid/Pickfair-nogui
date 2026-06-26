"""Routing del cashout mirror (Fase 2.1-B, design A3 + identità DB I1).

Orchestra la chiusura delle posizioni del bot quando arriva un segnale
``CASHOUT`` / ``CASHOUT_ALL``:

1. carica gli ordini **del bot** dal suo DB (identità I1: il ``betId`` di un
   ordine corrente è "del bot" se sta nei record del bot — il ``customerOrderRef``
   NON è inviato a Betfair sul piazzamento, quindi non è affidabile); i record DB
   danno anche la mappa ``market_id → event_name`` per restringere un ``CASHOUT``
   singolo alla **sua partita**;
2. legge gli ordini correnti live (``list_current_orders``) per lo stato abbinato;
3. ricostruisce le posizioni (``cashout_resolver`` — solo bot, lato singolo,
   miste saltate);
4. per ogni posizione da chiudere: gate tradabilità OPEN (solo OPEN esplicito),
   prezzo live al lato di chiusura, **cancella il resting** non abbinato e ne
   verifica la conferma, costruisce e pubblica un ``REQ_EXECUTE_CASHOUT``.

**Best-effort / fail-closed**: l'errore (o un mercato non OPEN, o un prezzo
mancante, o un cancel non confermato) su UNA posizione non aborta il batch; un
errore di lettura (DB ordini bot o ordini correnti) pubblica ``CASHOUT_FAILED`` e
aborta senza chiudere alla cieca. Gli I/O sono iniettati così il router è
testabile senza broker e **non tocca file forbidden**.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from cashout_resolver import build_cashout_request, reconstruct_open_positions

logger = logging.getLogger(__name__)

REQ_EXECUTE_CASHOUT = "REQ_EXECUTE_CASHOUT"
CASHOUT_FAILED = "CASHOUT_FAILED"
_TRADABLE_STATUS = "OPEN"


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _bet_id(row: Dict[str, Any]) -> str:
    return str(row.get("betId") or row.get("bet_id") or "").strip()


def _market_is_open(book: Dict[str, Any]) -> bool:
    """Tradabile solo se lo stato del mercato e' esplicitamente OPEN.

    Stato assente o diverso da OPEN => non tradabile (skip fail-closed): un book
    parziale o malformato non deve passare il gate e pubblicare un cashout.
    """
    status = str(book.get("status") or "").strip().upper()
    if not status:
        market_def = book.get("marketDefinition") or {}
        status = str(market_def.get("status") or "").strip().upper()
    return status == _TRADABLE_STATUS


def _closing_price(book: Dict[str, Any], selection_id: Any, side: str) -> Optional[float]:
    """Miglior prezzo al lato di CHIUSURA: una posizione BACK si chiude LAYando
    (``availableToLay``), una LAY backando (``availableToBack``)."""
    try:
        sel = int(selection_id)
    except (TypeError, ValueError):
        return None
    ladder_key = "availableToLay" if str(side).upper() == "BACK" else "availableToBack"
    for runner in book.get("runners") or []:
        try:
            if int(runner.get("selectionId")) != sel:
                continue
        except (TypeError, ValueError):
            continue
        ladder = runner.get(ladder_key) or (runner.get("ex") or {}).get(ladder_key) or []
        for level in ladder:
            price = level.get("price") if isinstance(level, dict) else None
            try:
                price_f = float(price)
            except (TypeError, ValueError):
                continue
            if price_f > 1.0:
                return price_f
    return None


class CashoutRouter:
    """Orchestratore del cashout: dipendenze iniettate (testabile, zero forbidden)."""

    def __init__(
        self,
        *,
        fetch_current_orders: Callable[[], Any],
        fetch_bot_orders: Callable[[], Any],
        fetch_market_book: Callable[[str], Optional[Dict[str, Any]]],
        cancel_orders: Callable[[str, List[str]], Any],
        publish: Callable[[str, Dict[str, Any]], None],
        commission_pct: float = 4.5,
        source: str = "TELEGRAM",
    ) -> None:
        self.fetch_current_orders = fetch_current_orders
        self.fetch_bot_orders = fetch_bot_orders
        self.fetch_market_book = fetch_market_book
        self.cancel_orders = cancel_orders
        self.publish = publish
        self.commission_pct = float(commission_pct)
        self.source = source

    def route(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Gestisce un segnale CASHOUT/CASHOUT_ALL. Ritorna un sommario per
        log/test: ``{published, positions, skipped, reason?}``."""
        signal = dict(signal or {})
        signal_type = str(signal.get("signal_type") or "").strip().upper()
        if signal_type not in {"CASHOUT", "CASHOUT_ALL"}:
            return self._summary(0, 0, reason="not_cashout")

        # 1. Ordini del bot dal DB (identità I1) → set bet_id + mappa market→event.
        bot = self._load_bot_orders()
        if bot is None:  # lettura DB fallita: CASHOUT_FAILED già pubblicato (fail-closed).
            return self._summary(0, 0, reason="bot_orders_error")
        bot_bet_ids, market_event = bot
        if not bot_bet_ids:
            logger.info("[cashout_router] nessun ordine del bot: niente da chiudere")
            return self._summary(0, 0, reason="no_bot_orders")

        # 2. Ordini correnti live (fail-closed: in LIVE solleva su sessione invalida).
        current_orders = self._load_current_orders()
        if current_orders is None:  # CASHOUT_FAILED già pubblicato.
            return self._summary(0, 0, reason="current_orders_error")

        def _is_bot_order(row: Dict[str, Any]) -> bool:
            return _bet_id(row) in bot_bet_ids

        # 3. Posizioni nette del bot (solo lato singolo; miste saltate dal resolver).
        positions = reconstruct_open_positions(current_orders, is_bot_order=_is_bot_order)

        # 4. CASHOUT singolo → ristretto alla sua partita (event_name).
        if signal_type == "CASHOUT":
            positions = self._restrict_to_event(signal, positions, market_event)
            if positions is None:  # senza event_name non si chiude nulla (fail-closed).
                return self._summary(0, 0, reason="no_event_name")

        published, skipped = self._close_positions(positions, current_orders, _is_bot_order)
        return self._summary(published, skipped)

    # ------------------------------------------------------------------
    @staticmethod
    def _summary(published: int, skipped: int, reason: Optional[str] = None) -> Dict[str, Any]:
        """Sommario uniforme per log/test: ``{published, positions, skipped, reason?}``."""
        out = {"published": published, "positions": published + skipped, "skipped": skipped}
        if reason:
            out["reason"] = reason
        return out

    def _load_bot_orders(self):
        """``(bot_bet_ids, market_event)`` dal DB del bot, o ``None`` se la lettura
        fallisce.

        Fail-closed: una lettura DB che solleva (es. SQLite lockato) pubblica
        ``CASHOUT_FAILED`` e aborta, così l'operatore riceve la notifica invece di
        un'eccezione non gestita nel bus.
        """
        try:
            bot_orders = list(self.fetch_bot_orders() or [])
        except Exception as exc:  # noqa: BLE001 - fail-closed: non si chiude alla cieca
            logger.warning("[cashout_router] lettura ordini bot fallita, abort: %s", exc)
            self.publish(CASHOUT_FAILED, {"reason": f"fetch_bot_orders:{exc}",
                                          "status": "ERROR", "source": self.source})
            return None
        bot_bet_ids = {_bet_id(o) for o in bot_orders if _bet_id(o)}
        market_event = {
            str(o.get("market_id") or o.get("marketId") or ""): _norm(o.get("event_name"))
            for o in bot_orders
            if (o.get("market_id") or o.get("marketId"))
        }
        return bot_bet_ids, market_event

    def _load_current_orders(self):
        """Ordini correnti live, o ``None`` se la fetch fallisce (pubblica
        ``CASHOUT_FAILED``, fail-closed)."""
        try:
            return list(self.fetch_current_orders() or [])
        except Exception as exc:  # noqa: BLE001 - fail-closed: non si chiude alla cieca
            logger.warning("[cashout_router] list_current_orders fallita, abort: %s", exc)
            self.publish(CASHOUT_FAILED, {"reason": f"list_current_orders:{exc}",
                                          "status": "ERROR", "source": self.source})
            return None

    @staticmethod
    def _restrict_to_event(signal, positions, market_event):
        """Filtra le posizioni alla partita del segnale (``event_name``).

        Ritorna ``None`` se manca ``event_name`` (fail-closed: meglio niente che la
        partita sbagliata).
        """
        target = _norm(signal.get("event_name"))
        if not target:
            logger.warning("[cashout_router] CASHOUT singolo senza event_name: skip (fail-closed)")
            return None
        return [p for p in positions if market_event.get(str(p["market_id"])) == target]

    # ------------------------------------------------------------------
    def _close_positions(self, positions, current_orders, is_bot_order):
        """Chiude ogni posizione in modo best-effort: un'eccezione su UNA
        posizione (es. ``fetch_market_book`` che solleva) viene assorbita e la
        posizione saltata, senza abortire il batch (contratto best-effort)."""
        published = 0
        skipped = 0
        for pos in positions:
            try:
                closed = self._close_position(pos, current_orders, is_bot_order)
            except Exception as exc:  # noqa: BLE001 - best-effort: un errore su una posizione non aborta il batch
                logger.warning("[cashout_router] chiusura posizione %s fallita, skip best-effort: %s",
                               pos.get("market_id"), exc)
                closed = False
            if closed:
                published += 1
            else:
                skipped += 1
        return published, skipped

    def _close_position(self, pos: Dict[str, Any], current_orders, is_bot_order) -> bool:
        """Chiude una singola posizione (best-effort: ritorna False senza
        abortire il batch se non chiudibile)."""
        market_id = str(pos.get("market_id") or "")
        book = self.fetch_market_book(market_id)
        if not book:
            logger.warning("[cashout_router] book assente per %s: skip", market_id)
            return False
        if not _market_is_open(book):
            logger.warning("[cashout_router] mercato %s non OPEN (SUSPENDED?): skip best-effort", market_id)
            return False
        price = _closing_price(book, pos.get("selection_id"), str(pos.get("side") or ""))
        if price is None:
            logger.warning("[cashout_router] prezzo di chiusura assente per %s/%s: skip",
                           market_id, pos.get("selection_id"))
            return False

        # Cancella il resting non abbinato del bot sulla selezione PRIMA di chiudere:
        # se si abbinasse dopo, riaprirebbe la posizione appena chiusa.
        if float(pos.get("resting_remainder") or 0.0) > 0.0:
            resting_ids = self._resting_bet_ids(current_orders, is_bot_order, market_id, pos.get("selection_id"))
            if resting_ids and not self._cancel_confirmed(market_id, resting_ids):
                return False

        req = build_cashout_request(
            pos, current_price=price, commission=self.commission_pct, source=self.source
        )
        if req is None:
            logger.warning("[cashout_router] green-up non calcolabile per %s/%s: skip",
                           market_id, pos.get("selection_id"))
            return False
        self.publish(REQ_EXECUTE_CASHOUT, req)
        return True

    def _cancel_confirmed(self, market_id: str, resting_ids: List[str]) -> bool:
        """``True`` solo se il cancel del resting e' CONFERMATO.

        Il callable iniettato ``cancel_orders`` deve ritornare un valore truthy a
        conferma (es. ``True`` o la lista dei bet_id cancellati) e falsy/raise in
        caso di rifiuto o errore. Un resting non cancellato che si abbina DOPO
        l'hedge riaprirebbe o sovra-copre la posizione appena chiusa: meglio
        saltare (fail-closed) che chiudere alla cieca. Nota: ``BetfairClient`` e
        ``SimulationBroker`` riportano il fallimento del cancel nel dict di
        risultato (non solo via eccezione), quindi non basta intercettare il raise.
        """
        try:
            result = self.cancel_orders(market_id, resting_ids)
        except Exception as exc:  # noqa: BLE001 - cancel fallito => non si chiude (fail-closed)
            logger.warning("[cashout_router] cancel resting fallito su %s: skip (fail-closed): %s",
                           market_id, exc)
            return False
        if not result:
            logger.warning("[cashout_router] cancel resting NON confermato su %s (%r): skip (fail-closed)",
                           market_id, result)
            return False
        return True

    @staticmethod
    def _resting_bet_ids(current_orders, is_bot_order, market_id: str, selection_id: Any) -> List[str]:
        """bet_id degli ordini del bot con resting (>0) su questa market+selection."""
        try:
            sel = int(selection_id)
        except (TypeError, ValueError):
            return []
        out: List[str] = []
        for row in current_orders or []:
            if not isinstance(row, dict) or not is_bot_order(row):
                continue
            if str(row.get("marketId") or row.get("market_id") or "") != market_id:
                continue
            try:
                if int(row.get("selectionId", row.get("selection_id"))) != sel:
                    continue
            except (TypeError, ValueError):
                continue
            try:
                remaining = float(row.get("sizeRemaining") or 0.0)
            except (TypeError, ValueError):
                remaining = 0.0
            bid = _bet_id(row)
            if remaining > 0.0 and bid:
                out.append(bid)
        return out
