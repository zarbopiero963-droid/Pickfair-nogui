from __future__ import annotations

import inspect
import math
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

from core.stake_precision import EXCHANGE_MIN_ORDER

#: Minimo del singolo ordine sull'exchange (EUR), dalla fonte unica `core.stake_precision` (riferimento owner 1 EUR,
#: PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01). Sotto questa soglia il router non chiama MAI il broker
#: direttamente: passa dal Microstake nativo (PF-MICROSTAKE-NATIVE-INTEGRATION-02) oppure fallisce chiuso.
EXCHANGE_MINIMUM_STAKE = float(EXCHANGE_MIN_ORDER)


class OrderRouter:
    """
    Router unico di piazzamento ordini: seleziona il broker attivo
    (simulation vs live) via ``betfair_service.get_client()`` e normalizza
    sia l'input sia l'output, così che i chiamatori (es. l'executor del
    cashout) non debbano conoscere le differenze di firma e di forma di
    ritorno fra ``BetfairClient.place_bet`` e ``SimulationBroker.place_bet``.

    Output deterministico
    ``{ok, placed, order_unknown, status, matched, bet_id, error, raw}``:

    - ``placed``: il broker ha **accettato** l'ordine e questo **esiste**
      (anche se non ancora abbinato). Quando ``placed`` è ``True`` il
      ``bet_id`` è valorizzato: il chiamante DEVE registrarlo, altrimenti
      un ordine reale resta orfano e un retry può piazzarne un duplicato.
    - ``order_unknown``: esito **incerto** dopo l'invio (timeout/rete): la
      richiesta può aver raggiunto l'exchange ma la risposta è andata persa.
      Va trattato come AMBIGUO (riconciliazione), MAI come fallimento da
      ritentare — un retry creerebbe un secondo ordine reale. Ha precedenza
      su ``placed``/``ok``.
    - ``ok``: ``placed and matched > 0`` — l'ordine è stato (almeno
      parzialmente) **abbinato**. È il gate "operazione efficace" su cui
      l'executor del cashout decide il green-up. Un LIMIT non abbinato è
      ``placed=True, ok=False, matched=0`` su **entrambi** i broker (parità
      SIM/LIVE: il sim crea comunque l'ordine, il live lo lascia a riposo).

    Contratto fail-closed (allineato a guardrails/*/core.order_router.json):
    - ``place`` è l'unica authority di piazzamento;
    - un campo richiesto mancante solleva ``KeyError`` (mai successo
      silenzioso su input invalido);
    - gli errori downstream del broker si propagano (non mascherati);
    - shape di output sempre identico.

    Perché il filtro dei kwargs: ``BetfairClient.place_bet`` accetta i cinque
    campi core keyword-only **più ``customer_ref``** (dalla #452/#PR-C: finisce
    sul wire come ``customerRef`` top-level, chiave di de-dup Betfair a 60s),
    mentre ``SimulationBroker.place_bet`` accetta anche i campi di
    audit/reconciliation (``event_key``/``table_id``/``batch_id``/
    ``event_name``/``market_name``/``runner_name``). Inoltrare i kwargs extra al
    client live solleverebbe ``TypeError``: il router passa a ciascun broker solo
    i parametri che la sua firma dichiara, senza perdere metadata per il sim.

    Questa frase diceva il falso fino alla PR02/#461 — dichiarava il client live
    limitato ai soli cinque core, cosa non più vera dalla #452. Il contratto wire
    effettivo sta in ``guardrails/contracts/core.trading_engine.json``.
    """

    #: Campi richiesti nel payload (fail-closed: assenza => KeyError).
    REQUIRED_FIELDS: Tuple[str, ...] = ("market_id", "selection_id", "bet_type", "price", "stake")

    def __init__(self, betfair_service, microstake_executor_provider: Optional[Callable[[], Any]] = None):
        self.service = betfair_service
        # Microstake nativo (decisione owner PF-MICROSTAKE-NATIVE-AUTO-01): non e'
        # una flag ma l'esecutore dell'engine, risolto lazy a ogni invio perche'
        # il broker attivo (SIM/LIVE) si decide al momento del place. Assente
        # (es. LIVE senza adapter) => un hedge sotto il minimo fallisce chiuso.
        self._microstake_executor_provider = microstake_executor_provider

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Piazza un ordine sul broker attivo e ritorna l'esito normalizzato."""
        # Fail-closed sull'input: ogni campo richiesto deve esserci.
        for field in self.REQUIRED_FIELDS:
            if field not in payload:
                raise KeyError(field)

        if self._below_exchange_minimum(payload.get("stake")):
            return self._place_native_microstake(payload)

        # Tutti i kwargs che un broker POTREBBE accettare; _filter_kwargs
        # taglia quelli fuori firma del broker attivo (no TypeError sul live,
        # nessun metadata audit perso sul sim).
        full_kwargs = {
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "side": payload["bet_type"],
            "price": payload["price"],
            "size": payload["stake"],
            "customer_ref": payload.get("customer_ref", ""),
            "event_key": payload.get("event_key", ""),
            "table_id": payload.get("table_id"),
            "batch_id": payload.get("batch_id", ""),
            "event_name": payload.get("event_name", ""),
            "market_name": payload.get("market_name", ""),
            "runner_name": payload.get("runner_name", ""),
        }

        client = self.service.get_client()
        call_kwargs = self._filter_kwargs(client.place_bet, full_kwargs)
        # Eventuali eccezioni del broker si propagano: fail-closed, mai
        # mascherate come piazzamento riuscito.
        raw = client.place_bet(**call_kwargs)
        return self._normalize(raw)

    @staticmethod
    def _below_exchange_minimum(raw_stake: Any) -> bool:
        """True solo per uno stake numerico finito in (0, minimo exchange).

        Tutto il resto (non numerico, NaN/Inf, <= 0) resta al broker, che lo
        rifiuta come sempre: qui non si valida, si instrada.
        """
        if raw_stake is None or isinstance(raw_stake, bool):
            return False
        try:
            stake = float(raw_stake)
        except (TypeError, ValueError):
            return False
        return math.isfinite(stake) and 0.0 < stake < EXCHANGE_MINIMUM_STAKE

    def _place_native_microstake(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Importo sotto il minimo: solo il Microstake nativo, mai il broker diretto.

        Il ``customer_ref`` del cashout e' la ``source`` del comando, non e' unico:
        la saga ha bisogno di un'identita' propria, quindi se ne genera una per
        questo invio (``raw["microstake_ref"]``). Il cashout non ritenta mai un
        esito AMBIGUO (vedi ``CashoutExecutor``), percio' la perdita
        dell'idempotenza per-ref sul retry non apre a un secondo hedge.
        """
        # Stesso contratto pre-invio del broker (DECISIONE-426 P13/P15): un lato
        # assente o non BACK/LAY si rifiuta PRIMA di qualunque esecutore.
        side = payload.get("bet_type")
        if not isinstance(side, str) or side.strip().upper() not in {"BACK", "LAY"}:
            raise RuntimeError("INVALID_SIDE")
        provider = self._microstake_executor_provider
        executor = provider() if callable(provider) else None
        execute = getattr(executor, "execute", None)
        if not callable(execute):
            return self._result(
                ok=False, placed=False, order_unknown=False, status="FAILURE",
                matched=0.0, bet_id=None, error="MICROSTAKE_EXECUTOR_UNAVAILABLE", raw=None,
            )
        # Mitigazione fail-closed (rilievo Codex #512): il ref della saga e' generato per
        # chiamata, quindi una saga AMBIGUOUS sulla stessa posizione non verrebbe ritrovata e
        # un secondo hedge partirebbe. Finche' esiste una saga pendente su (mercato,
        # selezione, lato) nessun nuovo hedge nativo: riconciliazione prima, mai un duplicato.
        pending_fn = getattr(executor, "pending_same_position", None)
        if callable(pending_fn):
            try:
                pending = list(pending_fn(payload["market_id"], payload["selection_id"], side.strip().upper()) or [])
            except Exception:
                pending = ["<store_unreadable>"]
            if pending:
                return self._result(
                    ok=False, placed=False, order_unknown=False, status="FAILURE",
                    matched=0.0, bet_id=None, error="MICROSTAKE_PENDING_SAME_POSITION",
                    raw={"pending_refs": pending},
                )
        base_ref = str(payload.get("customer_ref") or "cashout").strip() or "cashout"
        ref = f"{base_ref[:16]}-ms-{uuid.uuid4().hex[:12]}"
        native_payload = {
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "side": side.strip().upper(),
            "bet_type": side.strip().upper(),
            "price": payload["price"],
            "stake": payload["stake"],
            "size": payload["stake"],
            "customer_ref": ref,
            # PFC-008: hedge del cashout = uscita protettiva; il cap temporaneo non la blocca per percentuali
            # mancanti o cassa non verificata (resta il limite dei fondi disponibili).
            "protective": True,
        }
        raw = execute(native_payload)
        return self._normalize_native(raw, ref)

    @classmethod
    def _normalize_native(cls, raw: Any, ref: str) -> Dict[str, Any]:
        """Esito dell'esecutore nativo nello shape del router (AMBIGUOUS mai mascherato)."""
        if not isinstance(raw, dict):
            return cls._result(ok=False, placed=False, order_unknown=False, status="UNKNOWN",
                               matched=0.0, bet_id=None, error="NON_DICT_RESPONSE", raw=raw)
        raw = dict(raw, microstake_ref=ref)
        status = str(raw.get("status") or "").upper()
        if raw.get("order_unknown") is True or status == "AMBIGUOUS":
            # Il matched NOTO (stub abbinato, race match/cancel) resta visibile all'operatore
            # (rilievo Codex #512): AMBIGUOUS non azzera un'esposizione gia' dichiarata.
            try:
                known = float(raw.get("matched") or 0.0)
            except (TypeError, ValueError):
                known = 0.0
            if not math.isfinite(known) or known < 0.0:
                known = 0.0
            return cls._result(ok=False, placed=False, order_unknown=True, status="AMBIGUOUS",
                               matched=known, bet_id=None, error=raw.get("error"), raw=raw)
        matched, bet_id = cls._sum_reports(raw.get("instructionReports"))
        placed = status == "SUCCESS" and bet_id is not None
        return cls._result(
            ok=placed and matched > 0.0, placed=placed, order_unknown=False,
            status=status or "UNKNOWN", matched=matched, bet_id=bet_id,
            error=None if placed else (raw.get("error") or "BET_REJECTED"), raw=raw,
        )

    @staticmethod
    def _filter_kwargs(fn: Callable[..., Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Tiene solo i kwargs dichiarati dalla firma di ``fn`` (o tutti se ha **kwargs)."""
        try:
            params = inspect.signature(fn).parameters
        except (TypeError, ValueError):
            # Firma non ispezionabile: inoltra tutto (il broker validerà).
            return dict(kwargs)
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return dict(kwargs)
        return {k: v for k, v in kwargs.items() if k in params}

    @classmethod
    def _normalize(cls, raw: Any) -> Dict[str, Any]:
        """Riduce la risposta del broker (live o sim) allo shape deterministico."""
        if not isinstance(raw, dict):
            return cls._result(
                ok=False, placed=False, order_unknown=False, status="UNKNOWN",
                matched=0.0, bet_id=None, error="NON_DICT_RESPONSE", raw=raw,
            )
        # La forma BetfairClient ha la chiave "ok"; il SimulationBroker
        # ritorna la forma Betfair raw {"status", "instructionReports"}.
        if "ok" in raw:
            return cls._normalize_live(raw)
        return cls._normalize_sim(raw)

    @classmethod
    def _normalize_live(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizza ``BetfairClient.place_bet``: {"ok", "result"/"error", "order_unknown"}."""
        placed = bool(raw.get("ok"))
        order_unknown = bool(raw.get("order_unknown"))
        result = raw.get("result")
        matched, bet_id = 0.0, None
        if isinstance(result, dict):
            matched, bet_id = cls._sum_reports(result.get("instructionReports"))
        if order_unknown:
            # Esito incerto: né placed né failed. Riconciliazione, non retry.
            status = "AMBIGUOUS"
        elif isinstance(result, dict) and result.get("status"):
            status = str(result.get("status")).upper()
        else:
            status = "SUCCESS" if placed else "FAILURE"
        return cls._result(
            ok=placed and matched > 0.0,
            placed=placed,
            order_unknown=order_unknown,
            status=status,
            matched=matched,
            bet_id=bet_id,
            error=raw.get("error"),
            raw=raw,
        )

    @classmethod
    def _normalize_sim(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizza la forma Betfair raw del ``SimulationBroker``: {"status", "instructionReports"}."""
        status = str(raw.get("status") or "").upper()
        reports = raw.get("instructionReports") or []
        matched, bet_id = cls._sum_reports(reports)
        # L'ordine sim è CREATO e PERSISTITO anche se non abbinato: "placed"
        # segnala che esiste (registra il bet_id => niente orphan/retry
        # duplicato), mentre "ok" richiede l'abbinamento.
        placed = status == "SUCCESS" and bool(reports)
        if placed and raw.get("snapshot_persisted") is False:
            # #545: ordine creato in memoria ma snapshot di reload NON scritto: un riavvio lo perderebbe. Esito
            # incerto come un ordine live senza risposta: ne' riuscito ne' fallito, riconciliazione e mai retry.
            return cls._result(
                ok=False, placed=True, order_unknown=True, status="AMBIGUOUS", matched=matched,
                bet_id=bet_id, error="SIM_SNAPSHOT_NOT_PERSISTED", raw=raw)
        return cls._result(
            ok=placed and matched > 0.0,
            placed=placed,
            order_unknown=False,  # il sim è in-process: nessuna ambiguità di rete.
            status=status or "UNKNOWN",
            matched=matched,
            bet_id=bet_id,
            error=None if placed else "BET_REJECTED",
            raw=raw,
        )

    @staticmethod
    def _result(*, ok: bool, placed: bool, order_unknown: bool, status: str,
                matched: float, bet_id: Any, error: Any, raw: Any) -> Dict[str, Any]:
        """Costruisce lo shape di output canonico del router."""
        return {
            "ok": ok,
            "placed": placed,
            "order_unknown": order_unknown,
            "status": status,
            "matched": matched,
            "bet_id": bet_id,
            "error": error,
            "raw": raw,
        }

    @staticmethod
    def _sum_reports(reports: Any) -> Tuple[float, Any]:
        """Somma ``sizeMatched`` e raccoglie il primo ``betId`` dagli instruction report."""
        matched = 0.0
        bet_id = None
        for report in reports or []:
            if not isinstance(report, dict):
                continue
            try:
                matched += float(report.get("sizeMatched") or 0.0)
            except (TypeError, ValueError):
                pass
            if bet_id is None and report.get("betId"):
                bet_id = str(report.get("betId"))
        return matched, bet_id
