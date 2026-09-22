from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, Tuple


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

    def __init__(self, betfair_service):
        self.service = betfair_service

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Piazza un ordine sul broker attivo e ritorna l'esito normalizzato."""
        # Fail-closed sull'input: ogni campo richiesto deve esserci.
        for field in self.REQUIRED_FIELDS:
            if field not in payload:
                raise KeyError(field)

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
