"""Executor del cashout (Fase 2.1-A, B5).

Consuma ``CMD_EXECUTE_CASHOUT`` (già pubblicato da ``RiskMiddleware`` su
``REQ_EXECUTE_CASHOUT``), piazza il bet di green-up tramite ``OrderRouter`` —
il seam unico sim/live consentito dai guardrail no-bypass — e pubblica
``CASHOUT_SUCCESS`` / ``CASHOUT_FAILED``.

Il componente è **dormiente** finché qualcuno non emette
``REQ_EXECUTE_CASHOUT`` (routing del segnale CASHOUT/CASHOUT_ALL = Fase 2.1-B,
oppure la UI). Da solo non innesca alcun bet autonomo.

Fail-closed real-money:
- le invarianti real-money hard girano SEMPRE (anche col ``SafetyLayer``, che
  non copre side allow-list, finitezza NaN/Inf né presenza di ``green_up``):
  campi richiesti (incl. ``green_up``), ``side∈{BACK,LAY}``, ``price>1`` finito,
  ``stake>0`` finito; payload invalido ⇒ ``CASHOUT_FAILED`` ``REJECTED``,
  nessun piazzamento;
- il lato (``side``) è quello calcolato a monte dalla matematica di green-up e
  **non viene mai defaultato**: un side mancante ⇒ rigetto (defaultare a LAY
  potrebbe piazzare un ordine reale che *aumenta* l'esposizione);
- esito incerto (``order_unknown`` da timeout/rete) ⇒ ``CASHOUT_FAILED``
  ``status=AMBIGUOUS``, **mai** retry (un retry creerebbe un secondo bet reale:
  decide la riconciliazione);
- ordine piazzato ma **non** abbinato (``matched<=0``) ⇒ ``CASHOUT_FAILED``
  ``status=UNMATCHED``: il green-up non è bloccato (il ``bet_id`` è comunque
  riportato per non lasciare l'ordine orfano);
- solo ``placed and matched>0`` ⇒ ``CASHOUT_SUCCESS`` con ``green_up``;
- #536: con ``authority_check`` (cablato dagli entrypoint sul runtime) l'autorita' esclusiva del runtime e'
  verificata subito prima del piazzamento: persa, illeggibile o controllo in errore ⇒ ``CASHOUT_FAILED``
  ``REJECTED``, nessun hedge. Il bus e' asincrono: la verifica a monte, al routing del segnale, non basta.

NOTA (Fase 2.1-B, requisiti di attivazione sicura): finché 2.1-A è dormiente
nessun ordine reale parte, ma prima di attivare il routing la 2.1-B deve:
(a) aggiungere il consumer di ``CASHOUT_FAILED`` con notifica operatore per
AMBIGUOUS/UNMATCHED/ERROR (oggi solo ``CASHOUT_SUCCESS`` è consumato da
``telegram_sender``); (b) uniformare lo shape di ``CASHOUT_FAILED`` (dict) anche
in ``RiskMiddleware`` (oggi pubblica una stringa); (c) gestire il **lifecycle
dell'ordine non abbinato/ambiguo**: un LIMIT accettato ma non subito abbinato
resta vivo e può abbinarsi *dopo* l'UNMATCHED (hedge non tracciato) — va
cancellato oppure persistito/accodato per la riconciliazione (il ``bet_id`` è
già nel payload ``CASHOUT_FAILED``).
"""
from __future__ import annotations

import contextlib
import json
import logging
import math
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional

from core import hedge_ref as _hedge
from core.trading_constants import (
    CASHOUT_FAILED,
    CASHOUT_SUCCESS,
    CMD_EXECUTE_CASHOUT,
)

logger = logging.getLogger(__name__)


class CashoutExecutor:
    """Piazza il bet di green-up per il cashout e pubblica l'esito sul bus."""

    _VALID_SIDES = {"BACK", "LAY"}

    def __init__(self, bus: Any, order_router: Any, safety_layer: Optional[Any] = None,
                 authority_check: Optional[Callable[[], str]] = None,
                 authority_operation: Optional[Callable[[], Any]] = None,
                 ledger: Optional[Any] = None,
                 fetch_current_orders: Optional[Callable[[], Any]] = None) -> None:
        self.bus = bus
        # F-05 (CSH-06): ledger `orders` del bot (Database). Con il ledger l'hedge e' scritto PRIMA dell'invio
        # (write-ahead) e aggiornato con l'esito; senza (doppi di test) il comportamento resta quello di prima.
        self.ledger = ledger
        # controllo d'idempotenza + write-ahead atomici rispetto ai worker concorrenti del bus
        self._ledger_lock = threading.Lock()
        # F-05 (Sol su 59c7eca): ordini correnti dell'exchange (``BetfairService.list_current_orders``) per sapere se
        # un hedge precedente sulla stessa selezione e' ancora abbinabile. Assente o in errore => nessun nuovo hedge
        # finche' ne esiste uno precedente non chiuso (fail-closed).
        self.fetch_current_orders = fetch_current_orders
        self.order_router = order_router
        self.safety_layer = safety_layer
        self.authority_check = authority_check
        # #536 (Grok su 1542b5d): l'hedge e' un'operazione in volo; lo STOP la attende prima di sciogliere il fence
        self.authority_operation = authority_operation

    def _authority_block(self) -> str:
        """#536: '' se il runtime ha l'autorita' esclusiva (o non ne ha da verificare); altrimenti il motivo."""
        check = self.authority_check
        if check is None:
            return ""
        try:
            return str(check() or "")
        except Exception:  # noqa: BLE001 - autorita' non dimostrabile => nessun hedge
            logger.exception("[CashoutExecutor] verifica dell'autorita' del runtime fallita")
            return "runtime_authority_check_failed"

    def wire(self) -> None:
        """Sottoscrive il consumer di ``CMD_EXECUTE_CASHOUT`` al bus (dopo il recupero delle righe hedge in volo)."""
        self.recover_pending_hedges()
        self.bus.subscribe(CMD_EXECUTE_CASHOUT, self.on_cmd_execute_cashout)

    def recover_pending_hedges(self) -> int:
        """F-05: righe ``HEDGE_PENDING`` di un processo precedente (crash fra il write-ahead e l'esito) => l'hedge
        PUO' essere sull'exchange: ``HEDGE_AMBIGUOUS`` (esito ignoto, mai retry, riconciliazione manuale). Da quel
        momento nessun nuovo hedge sulla stessa selezione (vedi ``_hedge_block``). Ritorna quante righe ha marcato."""
        if self.ledger is None:
            return 0
        try:
            rows = self._hedge_rows()
        except Exception:  # noqa: BLE001 - ledger illeggibile: i comandi successivi saranno rifiutati comunque
            logger.critical("[CashoutExecutor] righe hedge non leggibili all'avvio", exc_info=True)
            return 0
        marked = 0
        for row in rows:
            if row["status"] != _hedge.HEDGE_PENDING:
                continue
            try:
                self.ledger.update_order(row["id"], {"status": _hedge.HEDGE_AMBIGUOUS, "updated_at": time.time(),
                                                     "ambiguity_reason": "restart_before_outcome"})
                marked += 1
            except Exception:  # noqa: BLE001 - resta PENDING: per _hedge_block e' comunque un esito incerto
                logger.critical("[CashoutExecutor] hedge %s non marcabile AMBIGUOUS", row["customer_ref"], exc_info=True)
        if marked:
            logger.critical("[CashoutExecutor] %d hedge con esito ignoto dopo il riavvio: riconciliazione richiesta",
                            marked)
        return marked

    def _hedge_rows(self) -> list:
        rows = self.ledger._execute(
            "SELECT id, customer_ref, status, payload_json, response_json FROM orders WHERE customer_ref LIKE 'hedge:%'",
            fetch=True, commit=False) or []
        out = []
        for raw in rows:
            row = dict(raw)
            try:
                payload = json.loads(row.get("payload_json") or "{}")
            except (TypeError, ValueError):
                payload = {}
            try:
                response = json.loads(row.get("response_json") or "{}")
            except (TypeError, ValueError):
                response = {}
            out.append({"id": str(row.get("id")), "customer_ref": str(row.get("customer_ref") or ""),
                        "status": str(row.get("status") or ""), "payload": payload if isinstance(payload, dict) else {},
                        "response": response if isinstance(response, dict) else {}})
        return out

    def _hedge_block(self, payload: Dict[str, Any]) -> str:
        """Idempotenza dell'hedge (F-05). '' se si puo' procedere, altrimenti il motivo del rifiuto:
        - stessa ``cashout_request_id`` gia' con un hedge non fallito => duplicato, mai un secondo hedge;
        - un hedge con esito incerto (PENDING/AMBIGUOUS) sulla stessa selezione => nessun nuovo hedge finche' non e'
          riconciliato (esito ignoto: mai retry);
        - un hedge accettato (UNMATCHED/PLACED) sulla stessa selezione ancora abbinabile sull'exchange
          (``sizeRemaining > 0``) => nessun nuovo hedge finche' la sua cancellazione non e' confermata (PF-EXEC-01;
          Sol su 59c7eca: il router pubblica il nuovo hedge prima del flatten best-effort del resting); vivacita' non
          verificabile => rifiuto;
        - ledger illeggibile => rifiuto (fail-closed)."""
        try:
            rows = self._hedge_rows()
        except Exception:  # noqa: BLE001
            logger.exception("[CashoutExecutor] ledger hedge illeggibile: nessun hedge")
            return "ledger_read_failed"
        request_id = self._request_identity(payload).get("cashout_request_id")
        market, selection = str(payload.get("market_id", "")), str(payload.get("selection_id", ""))
        for row in rows:
            p, status = row["payload"], row["status"]
            if request_id and p.get("cashout_request_id") == request_id and status != _hedge.HEDGE_FAILED:
                return "duplicate_cashout_request"
            if (status in (_hedge.HEDGE_PENDING, _hedge.HEDGE_AMBIGUOUS)
                    and str(p.get("market_id", "")) == market and str(p.get("selection_id", "")) == selection):
                return "hedge_outcome_uncertain"
        accepted = [row for row in rows
                    if row["status"] in (_hedge.HEDGE_UNMATCHED, _hedge.HEDGE_PLACED)
                    and str(row["payload"].get("market_id", "")) == market
                    and str(row["payload"].get("selection_id", "")) == selection]
        return self._previous_hedge_block(accepted) if accepted else ""

    def _previous_hedge_block(self, accepted: list) -> str:
        """'' se nessuno degli hedge accettati sulla selezione e' ancora abbinabile sull'exchange."""
        bet_ids = {str(row["response"].get("bet_id") or "").strip() for row in accepted}
        if "" in bet_ids or self.fetch_current_orders is None:
            return "hedge_liveness_unverifiable"        # hedge senza betId o nessuna fonte: non dimostrabile chiuso
        try:
            current = list(self.fetch_current_orders() or [])
        except Exception:  # noqa: BLE001 - ordini correnti illeggibili => nessun nuovo hedge (fail-closed)
            logger.exception("[CashoutExecutor] ordini correnti illeggibili: vivacita' dell'hedge precedente ignota")
            return "hedge_liveness_unverifiable"
        for order in current:
            if not isinstance(order, dict):
                continue
            bid = str(order.get("betId") or order.get("bet_id") or "").strip()
            if bid not in bet_ids:
                continue
            raw = order.get("sizeRemaining", order.get("size_remaining"))
            try:
                remaining = float(raw)
            except (TypeError, ValueError):
                return "previous_hedge_still_live"      # residuo illeggibile: vivo fino a prova contraria
            if not math.isfinite(remaining) or remaining > 0.0:
                return "previous_hedge_still_live"
        return ""

    def on_cmd_execute_cashout(self, payload: Dict[str, Any]) -> None:
        """Esegue un singolo comando di cashout, fail-closed end-to-end."""
        if not isinstance(payload, dict):
            self._fail("payload_non_dict", status="REJECTED", payload={})
            return

        # Fail-closed: le invarianti real-money hard girano SEMPRE (anche col
        # SafetyLayer iniettato, che NON copre side allow-list, finitezza
        # NaN/Inf né presenza di green_up). Se c'è il SafetyLayer si aggiunge
        # il suo schema. Il broad ``except`` è voluto: qualunque errore di
        # validazione diventa un REJECTED loggato e pubblicato, mai propagato a
        # crashare l'handler del bus.
        try:
            self._enforce_hard_invariants(payload)
            if self.safety_layer is not None:
                self.safety_layer.validate_cashout_request(payload)
        except Exception as exc:  # noqa: BLE001 - fail-closed intenzionale
            self._fail(f"validation:{exc}", status="REJECTED", payload=payload)
            return

        try:
            place_payload = self._build_place_payload(payload)
        except KeyError as exc:
            self._fail(f"campo_mancante:{exc}", status="REJECTED", payload=payload)
            return

        # Cattura il mode SIM/LIVE PRIMA del place: l'OrderRouter sceglie il
        # broker via get_client() all'inizio di place(), quindi catturarlo qui
        # (nessun I/O tra questa riga e get_client) riflette il broker effettivo.
        # Rileggerlo dopo place() esporrebbe a uno switch SIM/LIVE in volo (la
        # finestra della chiamata di rete) → flag sim errato (Codex P2).
        sim = self._is_simulation()
        operation = self.authority_operation
        stack = contextlib.ExitStack()
        try:
            stack.enter_context(operation() if callable(operation) else contextlib.nullcontext())
        except Exception:  # noqa: BLE001 - operazione non registrabile (Grok su 9a80162) => nessun hedge
            logger.exception("[CashoutExecutor] operazione in volo non registrabile")
            self._fail("runtime_authority_operation_failed", status="REJECTED", payload=payload)
            return
        with stack:
            authority_block = self._authority_block()     # #536: subito prima dell'invio dell'hedge
            if authority_block:
                self._fail(authority_block, status="REJECTED", payload=payload)
                return
            ledger_id = None
            if self.ledger is not None:
                with self._ledger_lock:
                    block = self._hedge_block(payload)
                    if block:
                        self._fail(block, status="REJECTED", payload=payload)
                        return
                    place_payload["customer_ref"] = _hedge.new_hedge_ref()
                    try:
                        ledger_id = self._ledger_write_ahead(place_payload, payload)
                    except Exception as exc:  # noqa: BLE001 - hedge non registrabile => nessun hedge (fail-closed)
                        logger.exception("[CashoutExecutor] hedge non registrabile nel ledger: nessun invio")
                        self._fail(f"ledger_write_failed:{exc}", status="REJECTED", payload=payload)
                        return
            try:
                result = self.order_router.place(place_payload)
            except Exception as exc:  # noqa: BLE001 - esito ignoto => fail-closed
                self._ledger_outcome(ledger_id, _hedge.HEDGE_AMBIGUOUS, None, f"place_exception:{exc}")
                self._fail(f"place_exception:{exc}", status="ERROR", payload=payload)
                return

            result = result if isinstance(result, dict) else {}
            self._ledger_outcome(ledger_id, self._ledger_status(result), result, None)
            self._handle_result(result, payload, sim)

    @classmethod
    def _enforce_hard_invariants(cls, payload: Dict[str, Any]) -> None:
        """Invarianti real-money applicate SEMPRE, anche col SafetyLayer.

        Lo schema del SafetyLayer verifica i bound price/stake e i tipi, ma NON
        la side allow-list (un ``side`` arbitrario il client live lo coerciva
        in BACK; dalla DECISIONE-426 P13 lo rifiuta con ``INVALID_SIDE``, ma
        qui si ferma prima del router, come REJECTED e non come ERROR), NON la
        finitezza (NaN/Inf passano i confronti ``<=``) e NON è
        garantita la presenza di ``green_up`` (un cashout senza green_up
        riporterebbe un P&L falso). Difesa in profondità, indipendente
        dall'injection.
        """
        for field in ("market_id", "selection_id", "side", "price", "stake", "green_up"):
            if payload.get(field) in (None, ""):
                raise ValueError(f"campo_mancante:{field}")
        if str(payload.get("side", "")).upper() not in cls._VALID_SIDES:
            raise ValueError("side_invalido")
        price = cls._as_float(payload.get("price"))
        stake = cls._as_float(payload.get("stake"))
        green_up = cls._as_float(payload.get("green_up"))
        if not (math.isfinite(price) and math.isfinite(stake) and math.isfinite(green_up)):
            raise ValueError("valore_non_finito")
        if price <= 1.0:
            raise ValueError("price<=1")
        if stake <= 0.0:
            raise ValueError("stake<=0")

    @staticmethod
    def _build_place_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Costruisce il payload per ``OrderRouter.place``.

        ``side`` è il lato di green-up calcolato a monte: si usa ``payload["side"]``
        (KeyError se assente) — mai un default, che potrebbe aumentare l'esposizione.
        """
        return {
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "bet_type": str(payload["side"]).upper(),
            "price": payload["price"],
            "stake": payload["stake"],
            "event_key": str(payload.get("event_key", "")),
            "customer_ref": str(payload.get("source", "") or ""),
            "event_name": str(payload.get("event_name", "")),
            "market_name": str(payload.get("market_name", "")),
            "runner_name": str(payload.get("runner_name", "")),
        }

    def _ledger_write_ahead(self, place_payload: Dict[str, Any], payload: Dict[str, Any]) -> str:
        """Riga `orders` dell'hedge PRIMA dell'invio. Nessun ``event_key``/``table_id``/``batch_id``: l'hedge non e'
        un ingresso e non occupa tavoli. Stato ``HEDGE_PENDING``, fuori dalle letture degli ordini aperti."""
        request = {k: v for k, v in place_payload.items() if k not in ("event_key", "table_id", "batch_id")}
        request.update({"role": _hedge.HEDGE_ROLE, "order_origin": _hedge.HEDGE_ORIGIN,
                        "green_up": self._as_float(payload.get("green_up")),
                        **self._request_identity(payload)})
        now = time.time()
        return self.ledger.insert_order({"customer_ref": place_payload["customer_ref"],
                                         "correlation_id": str(uuid.uuid4()), "status": _hedge.HEDGE_PENDING,
                                         "payload": request, "created_at": now, "updated_at": now})

    def _ledger_outcome(self, ledger_id: Optional[str], status: str, result: Optional[Dict[str, Any]],
                        error: Optional[str]) -> None:
        """Esito dell'hedge sulla sua riga. Un aggiornamento fallito NON annulla l'esito (l'hedge e' gia' sull'exchange):
        la riga resta ``HEDGE_PENDING`` e lo si segnala come CRITICAL."""
        if ledger_id is None:
            return
        update: Dict[str, Any] = {"status": status, "updated_at": time.time()}
        if result is not None:
            update["response"] = result
        if error:
            update["last_error"] = error
        if status == _hedge.HEDGE_AMBIGUOUS:
            update["ambiguity_reason"] = error or "order_unknown"
        try:
            self.ledger.update_order(ledger_id, update)
        except Exception:  # noqa: BLE001 - l'esito resta valido; la riga write-ahead resta come traccia
            logger.critical("[CashoutExecutor] esito dell'hedge non scritto nel ledger (riga %s resta %s)",
                            ledger_id, _hedge.HEDGE_PENDING, exc_info=True)

    def _ledger_status(self, result: Dict[str, Any]) -> str:
        # stessa classificazione di _handle_result (ignoto => ambiguo, mai fallito)
        if bool(result.get("order_unknown")):
            return _hedge.HEDGE_AMBIGUOUS
        if not result.get("placed"):
            return _hedge.HEDGE_FAILED
        return _hedge.HEDGE_PLACED if self._as_float(result.get("matched")) > 0.0 else _hedge.HEDGE_UNMATCHED

    def _handle_result(self, result: Dict[str, Any], payload: Dict[str, Any],
                       sim: bool = False) -> None:
        """Interpreta l'esito normalizzato del router e pubblica success/failed.

        ``sim`` è il mode catturato al momento del piazzamento (non rieletto qui),
        usato per il sim-broadcast guard del ``CASHOUT_SUCCESS``.
        """
        bet_id = result.get("bet_id")
        matched = self._as_float(result.get("matched"))

        if result.get("order_unknown"):
            # Esito incerto: l'ordine PUO' esistere ⇒ AMBIGUO, mai retry.
            self._fail("order_unknown", status="AMBIGUOUS", payload=payload,
                       bet_id=bet_id, matched=matched)
            return

        if not result.get("placed"):
            self._fail(result.get("error") or "not_placed", status="FAILURE",
                       payload=payload, bet_id=bet_id)
            return

        if matched <= 0.0:
            # Ordine a riposo non abbinato: green-up NON bloccato.
            self._fail("unmatched", status="UNMATCHED", payload=payload, bet_id=bet_id)
            return

        # Match parziale (verifica owner pre-merge #522): il residuo dell'hedge resta a riposo e puo' abbinarsi
        # dopo. `partial` e' True anche quando lo stake richiesto non e' leggibile (fail-closed): il
        # CashoutResidualHandler ne tenta il cancel e la barriera d'ammissione resta fino all'esito.
        requested = self._as_float(payload.get("stake"))
        residual = max(0.0, requested - matched) if requested > 0.0 else None
        partial = residual is None or residual > 1e-9
        success = {
            "green_up": self._as_float(payload.get("green_up")),
            "matched": matched,
            "requested": requested,
            "residual": residual,
            "partial": partial,
            "status": "DONE",
            "bet_id": bet_id,
            "market_id": str(payload.get("market_id", "")),
            "selection_id": payload.get("selection_id"),
            # Flag per il sim-broadcast guard: un cashout eseguito in simulazione
            # NON deve fare broadcast del MASTER_CASHOUT ai follower reali
            # (telegram_sender._on_cashout_success lo filtra). Catturato al
            # piazzamento (non rieletto qui) per evitare uno switch in volo.
            "sim": bool(sim),
            **self._request_identity(payload),
        }
        logger.info("[CashoutExecutor] CASHOUT_SUCCESS matched=%s bet_id=%s", matched, bet_id)
        self.bus.publish(CASHOUT_SUCCESS, success)

    def _fail(self, reason: Any, *, status: str, payload: Dict[str, Any],
              bet_id: Any = None, matched: Any = 0.0) -> None:
        logger.warning("[CashoutExecutor] CASHOUT_FAILED reason=%s status=%s", reason, status)
        self.bus.publish(CASHOUT_FAILED, {
            "reason": str(reason),
            "status": str(status),
            "bet_id": bet_id,
            "matched": self._as_float(matched),
            "market_id": str(payload.get("market_id", "")) if isinstance(payload, dict) else "",
            "selection_id": payload.get("selection_id") if isinstance(payload, dict) else None,
            **self._request_identity(payload),
        })

    @staticmethod
    def _request_identity(payload: Any) -> Dict[str, Any]:
        """``cashout_request_id`` della richiesta, riportato nell'esito (solo se presente e stringa)."""
        raw = payload.get("cashout_request_id") if isinstance(payload, dict) else None
        return {"cashout_request_id": raw} if isinstance(raw, str) and raw else {}

    def _is_simulation(self) -> bool:
        """True se il broker attivo è la simulazione (via ``OrderRouter.service``).

        Difensivo: se non determinabile ⇒ False (trattato come live). Usato solo
        per il flag ``sim`` del ``CASHOUT_SUCCESS`` (sim-broadcast guard).
        """
        try:
            svc = getattr(self.order_router, "service", None)
            fn = getattr(svc, "is_simulation_mode", None)
            return bool(fn()) if callable(fn) else False
        except Exception:  # noqa: BLE001 - best-effort, mai crash sul publish
            return False

    @staticmethod
    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
