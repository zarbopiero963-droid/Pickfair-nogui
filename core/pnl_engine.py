from __future__ import annotations

import logging
import math
import threading
from typing import Any, Dict, Optional

from trading_config import enforce_betfair_italy_commission_pct
from core.position_ledger import PositionLedger

logger = logging.getLogger(__name__)


class MarketNetRealizedSettlementAggregator:
    def __init__(self, *, commission_pct: float, context: str):
        self._commission_pct = float(commission_pct or 0.0)
        self._context = str(context or "market_net_realized")
        self._ledger: Dict[str, Dict[str, float]] = {}

    @property
    def ledger(self) -> Dict[str, Dict[str, float]]:
        return self._ledger

    @ledger.setter
    def ledger(self, value: Dict[str, Dict[str, float]]) -> None:
        self._ledger = value if isinstance(value, dict) else {}

    def apply(self, *, market_id: str, gross_pnl: float) -> dict[str, float | str]:
        market_key = str(market_id or "").strip()
        if not market_key:
            raise ValueError("market_id is required for realized settlement")

        commission_pct = enforce_betfair_italy_commission_pct(
            self._commission_pct,
            context=self._context,
        )
        ledger_row = self._ledger.setdefault(
            market_key,
            {"gross": 0.0, "commission": 0.0},
        )
        previous_market_gross = float(ledger_row.get("gross", 0.0))
        previous_market_commission = float(ledger_row.get("commission", 0.0))
        market_gross_after = previous_market_gross + float(gross_pnl or 0.0)
        desired_market_commission = 0.0
        if market_gross_after > 0.0 and commission_pct > 0.0:
            desired_market_commission = market_gross_after * (commission_pct / 100.0)

        commission_delta = desired_market_commission - previous_market_commission
        net_pnl = float(gross_pnl or 0.0) - commission_delta
        ledger_row["gross"] = market_gross_after
        ledger_row["commission"] = desired_market_commission
        return {
            "gross_pnl": float(gross_pnl or 0.0),
            "commission_amount": float(commission_delta),
            "net_pnl": float(net_pnl),
            "commission_pct": float(commission_pct),
            "market_net_gross": float(market_gross_after),
            "market_commission_amount_total": float(desired_market_commission),
            "settlement_basis": "market_net_realized",
        }


# Stati del runner che il settlement stile exchange sa regolare per una bet
# semplice. Gli altri (ACTIVE, HIDDEN, PLACED dei mercati each-way, stati
# sconosciuti) non hanno un esito certo: errore, mai un lordo di ripiego.
_ESITI_RUNNER_REGOLABILI = {
    "WINNER": "WIN",
    "LOSER": "LOSE",
    "REMOVED": "VOID",
    "REMOVED_VACANT": "VOID",
}


def _numero_finito(valore: Any, nome: str) -> float:
    if isinstance(valore, bool) or not isinstance(valore, (int, float)):
        raise ValueError(f"SETTLEMENT_INVALID_{nome.upper()}: {valore!r}")
    numero = float(valore)
    if not math.isfinite(numero):
        raise ValueError(f"SETTLEMENT_INVALID_{nome.upper()}: {valore!r}")
    return numero


def exchange_settled_gross_pnl(*, side: Any, price: Any, size: Any, runner_status: Any) -> float:
    """Lordo di una bet abbinata al settlement, semantica exchange.

    Fonte unica del calcolo (PR03 #461): la usa il broker simulato per
    regolare un mercato CHIUSO; in LIVE il lordo lo riporta Betfair.

    - BACK: il runner vince => ``size x (price - 1)``; perde => ``-size``.
    - LAY: il runner vince => ``-size x (price - 1)`` (la liability);
      perde => ``+size``.
    - Runner rimosso (``REMOVED`` / ``REMOVED_VACANT``): bet annullata, 0.

    ``price`` e ``size`` sono quota media e importo ABBINATI. Fail-closed:
    lato fuori BACK/LAY, quota <= 1, importo <= 0, valori non numerici o non
    finiti (bool compreso) o stato del runner senza esito certo =>
    ``ValueError``.
    """
    lato = side.strip().upper() if isinstance(side, str) else ""
    if lato not in {"BACK", "LAY"}:
        raise ValueError(f"SETTLEMENT_INVALID_SIDE: {side!r}")
    stato = runner_status.strip().upper() if isinstance(runner_status, str) else ""
    esito = _ESITI_RUNNER_REGOLABILI.get(stato)
    if esito is None:
        raise ValueError(f"SETTLEMENT_RUNNER_STATUS_UNSUPPORTED: {runner_status!r}")
    quota = _numero_finito(price, "price")
    importo = _numero_finito(size, "size")
    if quota <= 1.0:
        raise ValueError(f"SETTLEMENT_INVALID_PRICE: {price!r}")
    if importo <= 0.0:
        raise ValueError(f"SETTLEMENT_INVALID_SIZE: {size!r}")
    if esito == "VOID":
        return 0.0
    vince = esito == "WIN"
    if lato == "BACK":
        return importo * (quota - 1.0) if vince else -importo
    return -importo * (quota - 1.0) if vince else importo


class PnLEngine:
    """
    PnL Engine completo.

    - tracking posizioni (QUICK_BET_FILLED / QUICK_BET_PARTIAL)
    - mark-to-market su MARKET_BOOK_UPDATE
    - settlement realizzato da report cleared orders
      (``apply_cleared_market_settlement``)
    - publisher unico di RUNTIME_CLOSE_POSITION

    Auto-close mark-to-market (soglie ±% su stima di prezzo): **disattivo di
    default** (``auto_close_enabled=False``). La chiusura a soglia NON piazza
    alcun ordine reale: realizzerebbe PnL contabile su una posizione ancora
    viva su Betfair. Va armata esplicitamente e consapevolmente — coerente col
    contratto del RuntimeController («NON chiude automaticamente le
    posizioni»). Il percorso di settlement REALE (cleared orders) non passa da
    questo flag: un mercato settlato da Betfair è realizzato per definizione.
    """

    def __init__(
        self,
        bus=None,
        commission_pct: float = 4.5,
        auto_close_enabled: bool = False,
    ):
        self.bus = bus
        self.auto_close_enabled = bool(auto_close_enabled)
        # Serializza OGNI mutazione dello stato condiviso (_positions,
        # _position_ledgers, aggregatore market-net): i fill/market update
        # arrivano dai worker dell'EventBus, i settlement cleared dal thread
        # del poller — senza lock due thread corromperebbero i ledger.
        # RLock perche' _on_market (sotto lock) chiama _close.
        self._state_lock = threading.RLock()
        # Idempotenza interna del settlement cleared: un mercato realizzato
        # via cleared orders non puo' essere ri-applicato dal motore, quale
        # che sia il chiamante (difesa in profondita' oltre il dedupe del
        # poller e il checkpoint durevole del consumer).
        self._applied_cleared_markets: set[str] = set()
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._position_ledgers: Dict[str, PositionLedger] = {}
        self.commission = float(commission_pct) / 100.0
        self._market_net_realized_aggregator = MarketNetRealizedSettlementAggregator(
            commission_pct=(self.commission * 100.0),
            context="core_pnl_engine_realized_settlement",
        )
        # Settlement del broker simulato (PR03 #461): aggregatore e dedupe
        # propri, cosi' il netto di mercato SIM non si mescola con i
        # settlement LIVE dello stesso mercato nello stesso processo.
        self._sim_market_net_realized_aggregator = MarketNetRealizedSettlementAggregator(
            commission_pct=(self.commission * 100.0),
            context="core_pnl_engine_sim_settlement",
        )
        # Applicazione contabile e consegna separate (GPT-6 Astra sulla #485):
        # un settlement applicato ma non consegnato (publish fallito) resta qui
        # e il ritento ripubblica lo stesso payload, senza riapplicarlo.
        self._pending_simulated_settlements: Dict[str, Dict[str, Any]] = {}
        self._delivered_simulated_settlements: set[str] = set()

        if self.bus:
            self.bus.subscribe("QUICK_BET_FILLED", self._on_filled)
            self.bus.subscribe("QUICK_BET_PARTIAL", self._on_filled)
            self.bus.subscribe("MARKET_BOOK_UPDATE", self._on_market)

    # =========================================================
    # POSITION TRACKING
    # =========================================================
    def _on_filled(self, payload):
        event_key = str(payload.get("event_key") or "")
        if not event_key:
            return
        matched_price = payload.get("avg_price_matched")
        if matched_price is None:
            matched_price = payload.get("matched_price")
        if matched_price is None:
            matched_price = payload.get("price")

        matched_size = payload.get("matched_size")
        if matched_size is None:
            matched_size = payload.get("stake")

        market_id = str(payload.get("market_id") or "")
        selection_id = int(payload.get("selection_id") or 0)
        side = str(payload.get("bet_type", payload.get("side", "BACK"))).upper()
        price = float(matched_price or 0.0)
        size = float(matched_size or 0.0)
        if not market_id or selection_id < 0 or price <= 1.0 or size <= 0.0:
            return

        with self._state_lock:
            ledger = self._position_ledgers.get(event_key)
            if ledger is None:
                ledger = PositionLedger(market_id=market_id, runner_id=selection_id)
                self._position_ledgers[event_key] = ledger

            fill_id = str(
                payload.get("fill_id")
                or payload.get("match_id")
                or payload.get("bet_id")
                or payload.get("customer_ref")
                or event_key
            )
            applied = ledger.apply_fill(
                fill_id=fill_id,
                side=side,
                price=price,
                size=size,
            )
            snap = applied["snapshot"]
            self._positions[event_key] = {
                "event_key": event_key,
                "market_id": market_id,
                "selection_id": selection_id,
                "side": str(snap.open_side or side),
                "price": float(snap.avg_entry_price or price),
                "stake": float(snap.open_size or 0.0),
                "table_id": payload.get("table_id"),
                "batch_id": payload.get("batch_id"),
            }

    # =========================================================
    # MARKET UPDATE
    # =========================================================
    def _on_market(self, market_book):
        # Gate di sicurezza: la chiusura a soglia su stima mark-to-market NON
        # piazza ordini reali => realizzerebbe PnL contabile fantasma su una
        # posizione ancora aperta su Betfair. Dormiente salvo arming esplicito.
        if not self.auto_close_enabled:
            return

        market_id = str(market_book.get("marketId") or "")

        with self._state_lock:
            for pos in list(self._positions.values()):
                if pos["market_id"] != market_id:
                    continue

                settlement = self._calc_settlement(pos, market_book)
                pnl = float(settlement["net_pnl"])

                # 🎯 LOGICA USCITA
                if pnl >= pos["stake"] * 0.03 or pnl <= -pos["stake"] * 0.05:
                    self._close(pos, settlement)

    # =========================================================
    # PNL CALC
    # =========================================================
    def _calc(self, pos, market_book):
        return float(self._calc_settlement(pos, market_book)["net_pnl"])

    def _calc_settlement(self, pos, market_book):
        ledger = self._position_ledgers.get(str(pos.get("event_key") or ""))
        current_snapshot = ledger.snapshot() if ledger is not None else None
        use_legacy_pos = (
            current_snapshot is None
            or current_snapshot.open_side not in {"BACK", "LAY"}
            or current_snapshot.open_size <= 0.0
        )

        sel = int(pos["selection_id"])
        side = str(pos.get("side") or "BACK").upper() if use_legacy_pos else current_snapshot.open_side
        entry = float(pos.get("price") or 0.0)
        stake = float(pos.get("stake") or 0.0)

        for r in market_book.get("runners", []):
            if int(r.get("selectionId")) != sel:
                continue

            ex = r.get("ex", {})
            back = (ex.get("availableToBack") or [{}])[0].get("price")
            lay = (ex.get("availableToLay") or [{}])[0].get("price")

            if not back or not lay:
                return {
                    "gross_pnl": 0.0,
                    "commission_amount": 0.0,
                    "net_pnl": 0.0,
                    "commission_pct": float(self.commission * 100.0),
                    "settlement_source": "core_pnl_engine",
                    "settlement_kind": "mark_to_market_estimate",
                }

            close_price = float(lay if side == "BACK" else back)
            if use_legacy_pos:
                if side == "BACK":
                    gross_pnl = (entry - close_price) * stake
                else:
                    gross_pnl = (close_price - entry) * stake
            else:
                if close_price > 1.0:
                    mtm = ledger.mark_to_market(mark_price=close_price)
                    gross_pnl = float(mtm.unrealized_pnl)
                else:
                    avg_entry = float(current_snapshot.avg_entry_price)
                    open_size = float(current_snapshot.open_size)
                    if side == "BACK":
                        gross_pnl = (avg_entry - close_price) * open_size
                    else:
                        gross_pnl = (close_price - avg_entry) * open_size

            # 💰 commissione applicata solo su profitto positivo
            commission_amount = self._commission_amount(gross_pnl)
            pnl_net = gross_pnl - commission_amount

            return {
                "gross_pnl": float(gross_pnl),
                "commission_amount": float(commission_amount),
                "net_pnl": float(pnl_net),
                "commission_pct": float(self.commission * 100.0),
                "settlement_source": "core_pnl_engine",
                "settlement_kind": "mark_to_market_estimate",
                "close_price": close_price,
            }

        return {
            "gross_pnl": 0.0,
            "commission_amount": 0.0,
            "net_pnl": 0.0,
            "commission_pct": float(self.commission * 100.0),
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "mark_to_market_estimate",
        }

    def _commission_amount(self, gross_pnl: float) -> float:
        gross_pnl = float(gross_pnl or 0.0)
        if gross_pnl <= 0.0:
            return 0.0
        return gross_pnl * float(self.commission)

    def _apply_realized_market_net_commission(self, *, market_id: str, gross_pnl: float) -> dict[str, float | str]:
        return self._market_net_realized_aggregator.apply(market_id=market_id, gross_pnl=gross_pnl)

    # =========================================================
    # CLOSE
    # =========================================================
    def _close(self, pos, settlement):
        with self._state_lock:
            self._close_locked(pos, settlement)

    def _close_locked(self, pos, settlement):
        settlement = dict(settlement or {})
        event_key = str(pos.get("event_key") or "")
        market_id = str(pos.get("market_id") or "").strip()
        gross_pnl = float(settlement.get("gross_pnl", settlement.get("net_pnl", 0.0)) or 0.0)
        ledger = self._position_ledgers.get(event_key)
        close_price = float(settlement.get("close_price") or 0.0)
        if ledger is not None:
            snap = ledger.snapshot()
            if snap.open_side in {"BACK", "LAY"} and snap.open_size > 0.0 and close_price > 1.0:
                close_side = "LAY" if snap.open_side == "BACK" else "BACK"
                close_fill = ledger.apply_fill(
                    fill_id=f"close:{event_key}",
                    side=close_side,
                    price=close_price,
                    size=float(snap.open_size),
                )
                gross_pnl = float(close_fill.get("realized_delta") or gross_pnl)
        realized = self._apply_realized_market_net_commission(market_id=market_id, gross_pnl=gross_pnl)
        net_pnl = float(realized["net_pnl"])
        commission_amount = float(realized["commission_amount"])
        commission_pct = float(realized["commission_pct"])
        settlement_source = str(
            settlement.get("settlement_source")
            or settlement.get("source")
            or "core_pnl_engine"
        )
        settlement_kind = "realized_settlement"
        payload = {
            "event_key": event_key,
            "market_id": market_id,
            "table_id": pos["table_id"],
            "batch_id": pos["batch_id"],
            # legacy alias (net pnl) kept for compatibility
            "pnl": net_pnl,
            "gross_pnl": gross_pnl,
            "commission_amount": commission_amount,
            "net_pnl": net_pnl,
            "commission_pct": commission_pct,
            "market_net_gross": float(realized["market_net_gross"]),
            "market_commission_amount_total": float(realized["market_commission_amount_total"]),
            "settlement_basis": str(realized["settlement_basis"]),
            "settlement_source": settlement_source,
            "settlement_kind": settlement_kind,
        }

        logger.info(f"[PnL] Close {event_key} pnl={net_pnl:.2f}")

        if self.bus:
            self.bus.publish("RUNTIME_CLOSE_POSITION", payload)

        self._positions.pop(event_key, None)
        self._position_ledgers.pop(event_key, None)

    # =========================================================
    # SETTLEMENT REALE (cleared orders)
    # =========================================================
    def apply_cleared_market_settlement(
        self,
        *,
        market_id: str,
        gross_pnl: float,
        source: str = "betfair_cleared_orders",
        settled_date: str = "",
        reported_commission: Optional[float] = None,
        settlement_ref: str = "",
    ) -> Dict[str, Any]:
        """Realizza un settlement reale di Betfair sul mercato indicato.

        Ingresso del ciclo di chiusura reale (poller listClearedOrders).
        Granularita' PER-BET quando ``settlement_ref`` (il betId del bot) e'
        valorizzato: piu' bet dello stesso mercato si applicano in sequenza e
        l'aggregatore ricalcola il market-net a ogni passo (stessa meccanica
        dei close multi-leg); l'idempotenza e' per (mercato, ref). Senza ref
        la granularita' resta il mercato intero. ``gross_pnl`` è il ``profit``
        GROSS del report (pre-commissione — la commissione Betfair è
        riportata a parte). La
        commissione applicata qui è quella di POLICY (market-net, aliquota
        Italia) via aggregatore: è l'unica forma che il contratto settlement
        del RuntimeController accetta (``COMMISSION_AMOUNT_POLICY_MISMATCH``
        altrimenti); l'eventuale commissione riportata da Betfair viaggia nel
        payload come campo osservabilità (``betfair_reported_commission``),
        mai come base contabile. Il saldo VERO resta il bankroll sync
        post-settlement (get_account_funds).

        Fail-closed: ``market_id`` vuoto o ``gross_pnl`` non finito/non
        numerico => raise, nessun payload parziale. Idempotente NEL MOTORE:
        lo stesso mercato non si realizza due volte
        (``CLEARED_SETTLEMENT_DUPLICATE``), qualunque sia il chiamante.
        Thread-safe: mutazioni serializzate con i fill/market update del bus
        (``_state_lock``). Identità deterministica PER-BET:
        ``event_key = "cleared:<market_id>:<settlement_ref>"`` (con ref =
        betId; senza ref degrada a ``cleared:<market_id>``) — stessa chiave
        che il consumer usa per il checkpoint durevole => dedupe
        ricostruibile al riavvio.
        Le posizioni tracked del mercato vengono rimosse dal tracking
        (il mercato non esiste più); non essendoci table_id, il rilascio
        tavoli resta al percorso cashout/reset come oggi.
        """
        market_key = str(market_id or "").strip()
        if not market_key:
            raise ValueError(
                "CLEARED_SETTLEMENT_INVALID_MARKET: market_id is required"
            )
        try:
            gross = float(gross_pnl)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"CLEARED_SETTLEMENT_INVALID_GROSS: profit non numerico per "
                f"market {market_key}: {gross_pnl!r}"
            ) from exc
        if not math.isfinite(gross):
            raise ValueError(
                f"CLEARED_SETTLEMENT_INVALID_GROSS: profit non finito per "
                f"market {market_key}: {gross_pnl!r}"
            )
        reported_commission_f: Optional[float] = None
        if reported_commission is not None:
            # Convertita PRIMA di mutare l'aggregatore: un valore malformato
            # deve fallire senza lasciare stato parziale nel ledger.
            try:
                reported_commission_f = float(reported_commission)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"CLEARED_SETTLEMENT_INVALID_COMMISSION: commissione "
                    f"riportata non numerica per market {market_key}: "
                    f"{reported_commission!r}"
                ) from exc

        ref = str(settlement_ref or "").strip()
        dedupe_key = f"{market_key}:{ref}" if ref else market_key
        with self._state_lock:
            # Idempotenza nel MOTORE (non solo nel poller): lo stesso
            # settlement (mercato, o singola bet del mercato quando ref e'
            # valorizzato) non si ri-applica MAI — un secondo apply
            # raddoppierebbe realized e commissione nel daily-loss. Il guard
            # precede la mutazione dell'aggregatore: il raise non lascia
            # stato parziale.
            if dedupe_key in self._applied_cleared_markets:
                raise ValueError(
                    f"CLEARED_SETTLEMENT_DUPLICATE: settlement {dedupe_key} "
                    "gia' realizzato da cleared orders"
                )

            realized = self._apply_realized_market_net_commission(
                market_id=market_key, gross_pnl=gross
            )
            self._applied_cleared_markets.add(dedupe_key)
            net_pnl = float(realized["net_pnl"])
            event_key = (
                f"cleared:{market_key}:{ref}" if ref else f"cleared:{market_key}"
            )

            cleared_positions = [
                key
                for key, pos in list(self._positions.items())
                if str(pos.get("market_id") or "") == market_key
            ]
            for key in cleared_positions:
                self._positions.pop(key, None)
                self._position_ledgers.pop(key, None)

            payload: Dict[str, Any] = {
                "event_key": event_key,
                "market_id": market_key,
                "table_id": None,
                "batch_id": "",
                # legacy alias (net pnl) kept for compatibility
                "pnl": net_pnl,
                "gross_pnl": gross,
                "commission_amount": float(realized["commission_amount"]),
                "net_pnl": net_pnl,
                "commission_pct": float(realized["commission_pct"]),
                "market_net_gross": float(realized["market_net_gross"]),
                "market_commission_amount_total": float(
                    realized["market_commission_amount_total"]
                ),
                "settlement_basis": str(realized["settlement_basis"]),
                "settlement_source": str(source or "betfair_cleared_orders"),
                "settlement_kind": "realized_settlement",
                "settled_date": str(settled_date or ""),
                "settlement_ref": ref,
                "cleared_positions": list(cleared_positions),
            }
            if reported_commission_f is not None:
                payload["betfair_reported_commission"] = reported_commission_f

            logger.info(
                "[PnL] Cleared settlement %s ref=%s gross=%.2f net=%.2f positions=%d",
                market_key,
                ref or "-",
                gross,
                net_pnl,
                len(cleared_positions),
            )

        # Publish FUORI dalla sezione critica: il bus e' enqueue-only, ma un
        # subscriber sincrono (bus di test / futuri wiring) non deve mai
        # rientrare nel motore con il lock ancora tenuto (lock-ordering).
        if self.bus:
            self.bus.publish("RUNTIME_CLOSE_POSITION", payload)

        return payload

    # =========================================================
    # SETTLEMENT SIMULATO (broker SIM, PR03 #461)
    # =========================================================
    @staticmethod
    def simulated_settlement_key(market_id: Any, settlement_ref: Any) -> str:
        """Chiave di un settlement SIM: e' l'``event_key`` del payload, quindi
        la stessa ``settlement_key`` con cui il consumer scrive il checkpoint
        durevole. Il poller deduplica con questa, prima di emettere."""
        return f"sim:{str(market_id or '').strip()}:{str(settlement_ref or '').strip()}"

    def apply_simulated_market_settlement(
        self,
        *,
        market_id: str,
        gross_pnl: float,
        settlement_ref: str,
        settled_date: str = "",
    ) -> Dict[str, Any]:
        """Realizza il settlement di un mercato CHIUSO nel broker simulato.

        Il broker ha gia' aggregato le selezioni: ``gross_pnl`` e' il lordo
        dell'INTERO mercato, quindi la commissione di policy si applica una
        volta sola sul netto e il mercato si chiude con UN evento (mai una
        chiusura per gamba). Stesso payload canonico del settlement LIVE,
        con ``settlement_source="simulation_broker"``.

        Fail-closed: mercato o riferimento vuoti, lordo non numerico o non
        finito (bool compreso) => raise senza stato ne' publish. Idempotente
        nel motore sulla chiave ``sim:<market_id>:<settlement_ref>``, oltre
        alla dedupe del poller e al checkpoint durevole del consumer:

        - la commissione e il netto si calcolano UNA volta, alla prima
          chiamata;
        - se il publish fallisce il settlement resta applicato ma non
          consegnato: la chiamata successiva ripubblica lo stesso payload
          (stesso lordo obbligatorio, altrimenti ``SIM_SETTLEMENT_CONFLICT``);
        - dopo una consegna riuscita ogni ripetizione solleva
          ``SIM_SETTLEMENT_DUPLICATE``.
        """
        market_key = str(market_id or "").strip()
        if not market_key:
            raise ValueError("SIM_SETTLEMENT_INVALID_MARKET: market_id is required")
        ref = str(settlement_ref or "").strip()
        if not ref:
            raise ValueError(
                f"SIM_SETTLEMENT_INVALID_REF: settlement_ref is required for market {market_key}"
            )
        if isinstance(gross_pnl, bool) or not isinstance(gross_pnl, (int, float)):
            raise ValueError(
                f"SIM_SETTLEMENT_INVALID_GROSS: lordo non numerico per market "
                f"{market_key}: {gross_pnl!r}"
            )
        gross = float(gross_pnl)
        if not math.isfinite(gross):
            raise ValueError(
                f"SIM_SETTLEMENT_INVALID_GROSS: lordo non finito per market "
                f"{market_key}: {gross_pnl!r}"
            )

        event_key = self.simulated_settlement_key(market_key, ref)
        with self._state_lock:
            if event_key in self._delivered_simulated_settlements:
                raise ValueError(
                    f"SIM_SETTLEMENT_DUPLICATE: settlement {event_key} gia' consegnato"
                )
            payload = self._pending_simulated_settlements.get(event_key)
            if payload is not None:
                if payload["gross_pnl"] != gross:
                    raise ValueError(
                        f"SIM_SETTLEMENT_CONFLICT: {event_key} gia' applicato con lordo "
                        f"{payload['gross_pnl']!r}, ritento con {gross!r}"
                    )
            else:
                payload = self._apply_simulated_settlement_locked(
                    market_key=market_key,
                    ref=ref,
                    event_key=event_key,
                    gross=gross,
                    settled_date=settled_date,
                )
                self._pending_simulated_settlements[event_key] = payload

        # Publish fuori dalla sezione critica, come per il settlement LIVE.
        # Consegnato solo se il publish non solleva: altrimenti resta pendente
        # e il prossimo giro del poller lo ripubblica.
        if self.bus:
            self.bus.publish("RUNTIME_CLOSE_POSITION", dict(payload))
        with self._state_lock:
            self._pending_simulated_settlements.pop(event_key, None)
            self._delivered_simulated_settlements.add(event_key)
        return dict(payload)

    def _apply_simulated_settlement_locked(
        self, *, market_key: str, ref: str, event_key: str, gross: float, settled_date: str
    ) -> Dict[str, Any]:
        """Parte contabile del settlement SIM, sotto ``_state_lock``: commissione
        di policy sul netto del mercato, posizioni tracked rimosse, payload."""
        realized = self._sim_market_net_realized_aggregator.apply(
            market_id=market_key, gross_pnl=gross
        )
        net_pnl = float(realized["net_pnl"])

        cleared_positions = [
            key
            for key, pos in list(self._positions.items())
            if str(pos.get("market_id") or "") == market_key
        ]
        for key in cleared_positions:
            self._positions.pop(key, None)
            self._position_ledgers.pop(key, None)

        logger.info(
            "[PnL] SIM settlement %s ref=%s gross=%.2f net=%.2f positions=%d",
            market_key,
            ref,
            gross,
            net_pnl,
            len(cleared_positions),
        )
        return {
            "event_key": event_key,
            "market_id": market_key,
            "table_id": None,
            "batch_id": "",
            # legacy alias (net pnl) kept for compatibility
            "pnl": net_pnl,
            "gross_pnl": gross,
            "commission_amount": float(realized["commission_amount"]),
            "net_pnl": net_pnl,
            "commission_pct": float(realized["commission_pct"]),
            "market_net_gross": float(realized["market_net_gross"]),
            "market_commission_amount_total": float(
                realized["market_commission_amount_total"]
            ),
            "settlement_basis": str(realized["settlement_basis"]),
            "settlement_source": "simulation_broker",
            "settlement_kind": "realized_settlement",
            "settled_date": str(settled_date or ""),
            "settlement_ref": ref,
            "cleared_positions": list(cleared_positions),
        }

    # =========================================================
    # STATUS
    # =========================================================
    def snapshot(self):
        with self._state_lock:
            return self._snapshot_locked()

    def _snapshot_locked(self):
        return {
            "open_positions": len(self._positions),
            "positions": [
                {
                    **dict(position),
                    "ledger": self._position_ledgers[key].snapshot().__dict__
                    if key in self._position_ledgers
                    else {},
                }
                for key, position in self._positions.items()
            ],
        }
