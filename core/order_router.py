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

    Contratto (allineato a guardrails/*/core.order_router.json):
    - ``place`` è l'unica authority di piazzamento;
    - fail-closed sull'input: un campo richiesto mancante solleva
      ``KeyError`` (mai successo silenzioso su input invalido);
    - gli errori downstream del broker si propagano (non vengono
      mascherati come successo);
    - output deterministico: sempre il medesimo shape normalizzato
      ``{ok, status, matched, bet_id, error, raw}``.

    Perché il filtro dei kwargs: ``BetfairClient.place_bet`` accetta solo
    ``(market_id, selection_id, side, price, size)`` keyword-only, mentre
    ``SimulationBroker.place_bet`` accetta anche ``customer_ref``/``event_key``/
    ``table_id``/``batch_id``. Inoltrare i kwargs extra al client live
    solleverebbe ``TypeError``: il router passa a ciascun broker solo i
    parametri che la sua firma dichiara.
    """

    #: Campi richiesti nel payload (fail-closed: assenza => KeyError).
    REQUIRED_FIELDS: Tuple[str, ...] = ("market_id", "selection_id", "bet_type", "price", "stake")

    def __init__(self, betfair_service):
        self.service = betfair_service

    def place(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Fail-closed sull'input: i campi richiesti devono esserci. L'accesso
        # via [] solleva KeyError sul campo mancante (no successo silenzioso).
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
        }

        client = self.service.get_client()
        call_kwargs = self._filter_kwargs(client.place_bet, full_kwargs)
        # Eventuali eccezioni del broker si propagano: fail-closed, mai
        # mascherate come piazzamento riuscito.
        raw = client.place_bet(**call_kwargs)
        return self._normalize(raw)

    @staticmethod
    def _filter_kwargs(fn: Callable[..., Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
        try:
            params = inspect.signature(fn).parameters
        except (TypeError, ValueError):
            # Firma non ispezionabile: inoltra tutto (il broker validerà).
            return dict(kwargs)
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return dict(kwargs)
        return {k: v for k, v in kwargs.items() if k in params}

    @staticmethod
    def _normalize(raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            return {
                "ok": False,
                "status": "UNKNOWN",
                "matched": 0.0,
                "bet_id": None,
                "error": "NON_DICT_RESPONSE",
                "raw": raw,
            }

        # Forma BetfairClient.place_bet: {"ok": bool, "result"/"error": ...}.
        if "ok" in raw:
            ok = bool(raw.get("ok"))
            result = raw.get("result")
            status = "SUCCESS" if ok else "FAILURE"
            matched, bet_id = 0.0, None
            if isinstance(result, dict):
                status = str(result.get("status") or status).upper()
                matched, bet_id = OrderRouter._sum_reports(result.get("instructionReports"))
            return {
                "ok": ok,
                "status": status,
                "matched": matched,
                "bet_id": bet_id,
                "error": raw.get("error"),
                "raw": raw,
            }

        # Forma Betfair raw (SimulationBroker): {"status", "instructionReports"}.
        status = str(raw.get("status") or "").upper()
        reports = raw.get("instructionReports") or []
        matched, bet_id = OrderRouter._sum_reports(reports)
        report_ok = bool(reports) and all(
            str(r.get("status") or "").upper() in {"SUCCESS", "PLACED"} for r in reports
        )
        ok = status == "SUCCESS" and report_ok
        return {
            "ok": ok,
            "status": status or "UNKNOWN",
            "matched": matched,
            "bet_id": bet_id,
            "error": None if ok else "BET_NOT_FULLY_PLACED",
            "raw": raw,
        }

    @staticmethod
    def _sum_reports(reports: Any) -> Tuple[float, Any]:
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
