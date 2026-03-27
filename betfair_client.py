from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import requests


logger = logging.getLogger(__name__)


class BetfairClient:
    """
    Client Betfair compatibile con:
    - login/logout
    - get_account_funds
    - place_bet / place_orders
    - list_current_orders / cancel_orders
    - resolver Telegram:
        - list_live_soccer_events
        - list_event_markets
        - get_market_book
    - cashout / green-up helper corretto
    """

    IDENTITY_URL = "https://identitysso.betfair.it/api/certlogin"
    BETTING_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
    ACCOUNT_URL = "https://api.betfair.com/exchange/account/json-rpc/v1"

    SOCCER_EVENT_TYPE_ID = "1"

    def __init__(
        self,
        *,
        username: str,
        app_key: str,
        cert_pem: str,
        key_pem: str,
        session: Optional[requests.Session] = None,
        timeout: float = 20.0,
    ):
        self.username = str(username or "").strip()
        self.app_key = str(app_key or "").strip()
        self.cert_pem = str(cert_pem or "").strip()
        self.key_pem = str(key_pem or "").strip()
        self.timeout = float(timeout or 20.0)

        self.session = session or requests.Session()
        self.session_token = ""
        self.session_expiry = ""
        self.connected = False

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================
    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return int(default)
            return int(float(value))
        except Exception:
            return int(default)

    def _safe_side(self, value: Any) -> str:
        side = str(value or "BACK").upper().strip()
        return side if side in {"BACK", "LAY"} else "BACK"

    def _cert_tuple(self):
        if not self.cert_pem or not self.key_pem:
            raise RuntimeError("Certificato o key mancanti")

        if not os.path.exists(self.cert_pem):
            raise RuntimeError(f"File certificato non trovato: {self.cert_pem}")

        if not os.path.exists(self.key_pem):
            raise RuntimeError(f"File private key non trovato: {self.key_pem}")

        return (self.cert_pem, self.key_pem)

    def _headers(self, with_session: bool = True) -> Dict[str, str]:
        headers = {
            "X-Application": self.app_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if with_session and self.session_token:
            headers["X-Authentication"] = self.session_token
        return headers

    def _post_jsonrpc(self, url: str, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.connected or not self.session_token:
            raise RuntimeError("Betfair client non autenticato")

        payload = [
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 1,
            }
        ]

        response = self.session.post(
            url,
            headers=self._headers(with_session=True),
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list) or not data:
            raise RuntimeError("Risposta JSON-RPC non valida")

        item = data[0]
        if "error" in item:
            raise RuntimeError(str(item["error"]))

        return item.get("result") or {}

    # =========================================================
    # SESSION
    # =========================================================
    def login(self, password: str) -> Dict[str, Any]:
        if not self.username or not self.app_key:
            raise RuntimeError("Username o app_key mancanti")

        if not password:
            raise RuntimeError("Password mancante")

        response = self.session.post(
            self.IDENTITY_URL,
            headers={
                "X-Application": self.app_key,
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={
                "username": self.username,
                "password": password,
            },
            cert=self._cert_tuple(),
            timeout=self.timeout,
        )
        response.raise_for_status()

        data = response.json()
        status = str(data.get("loginStatus") or "").upper()
        if status != "SUCCESS":
            raise RuntimeError(f"Login Betfair fallito: {status or data}")

        self.session_token = str(data.get("sessionToken") or "")
        self.session_expiry = str(data.get("sessionExpiryTime") or "")
        self.connected = bool(self.session_token)

        return {
            "session_token": self.session_token,
            "expiry": self.session_expiry,
            "connected": self.connected,
            "simulated": False,
        }

    def logout(self) -> None:
        self.session_token = ""
        self.session_expiry = ""
        self.connected = False

    # =========================================================
    # ACCOUNT
    # =========================================================
    def get_account_funds(self) -> Dict[str, float]:
        result = self._post_jsonrpc(
            self.ACCOUNT_URL,
            "AccountAPING/v1.0/getAccountFunds",
            {"wallet": "ITALIAN"},
        )

        available = self._safe_float(result.get("availableToBetBalance"), 0.0)
        exposure = self._safe_float(result.get("exposure"), 0.0)
        retained = self._safe_float(result.get("retainedCommission"), 0.0)

        total = available + max(0.0, exposure) - max(0.0, retained)

        return {
            "available": float(available),
            "exposure": float(exposure),
            "total": float(total),
            "simulated": False,
        }

    # =========================================================
    # CASHOUT / GREEN-UP
    # =========================================================
    def calculate_cashout(
        self,
        original_stake: float,
        original_odds: float,
        current_odds: float,
        side: str = "BACK",
    ) -> Dict[str, float]:
        """
        Calcola il cashout (green-up) con profitto/perdita quanto più possibile
        uniforme su tutti gli esiti.

        - side="BACK": posizione iniziale BACK, hedge con LAY
        - side="LAY":  posizione iniziale LAY, hedge con BACK
        """
        original_stake = self._safe_float(original_stake, 0.0)
        original_odds = self._safe_float(original_odds, 0.0)
        current_odds = self._safe_float(current_odds, 0.0)
        side_mode = self._safe_side(side)

        if original_stake <= 0.0 or original_odds <= 1.0 or current_odds <= 1.0:
            return {
                "cashout_stake": 0.0,
                "profit_if_win": 0.0,
                "profit_if_lose": 0.0,
                "guaranteed_profit": 0.0,
                "side_to_place": "LAY" if side_mode == "BACK" else "BACK",
            }

        # Formula universale green-up
        cashout_stake = round((original_stake * original_odds) / current_odds, 2)

        if side_mode == "BACK":
            profit_if_win = (original_stake * (original_odds - 1.0)) - (
                cashout_stake * (current_odds - 1.0)
            )
            profit_if_lose = cashout_stake - original_stake
            side_to_place = "LAY"
        else:
            profit_if_win = (cashout_stake * (current_odds - 1.0)) - (
                original_stake * (original_odds - 1.0)
            )
            profit_if_lose = original_stake - cashout_stake
            side_to_place = "BACK"

        profit_if_win = round(profit_if_win, 2)
        profit_if_lose = round(profit_if_lose, 2)
        guaranteed_profit = round(min(profit_if_win, profit_if_lose), 2)

        return {
            "cashout_stake": cashout_stake,
            "profit_if_win": profit_if_win,
            "profit_if_lose": profit_if_lose,
            "guaranteed_profit": guaranteed_profit,
            "side_to_place": side_to_place,
        }

    # =========================================================
    # ORDER HELPERS
    # =========================================================
    def _build_instruction(
        self,
        *,
        selection_id: int,
        side: str,
        price: float,
        size: float,
    ) -> Dict[str, Any]:
        return {
            "selectionId": int(selection_id),
            "side": self._safe_side(side),
            "orderType": "LIMIT",
            "limitOrder": {
                "size": float(size),
                "price": float(price),
                "persistenceType": "LAPSE",
            },
        }

    # =========================================================
    # ORDERS
    # =========================================================
    def place_bet(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        customer_ref: str = "",
        event_key: str = "",
        table_id: Any = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
        runner_name: str = "",
    ) -> Dict[str, Any]:
        _ = event_key, table_id, batch_id, event_name, market_name, runner_name

        params = {
            "marketId": str(market_id),
            "instructions": [
                self._build_instruction(
                    selection_id=int(selection_id),
                    side=side,
                    price=float(price),
                    size=float(size),
                )
            ],
            "customerRef": str(customer_ref or ""),
        }

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/placeOrders",
            params,
        )

        return {
            "status": str(result.get("status") or ""),
            "marketId": str(result.get("marketId") or market_id),
            "instructionReports": result.get("instructionReports") or [],
            "simulated": False,
        }

    def place_orders(
        self,
        *,
        market_id: str,
        instructions: List[Dict[str, Any]],
        customer_ref: str = "",
        event_key: str = "",
        table_id: Any = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
    ) -> Dict[str, Any]:
        _ = event_key, table_id, batch_id, event_name, market_name

        built_instructions = []
        for item in instructions or []:
            built_instructions.append(
                self._build_instruction(
                    selection_id=self._safe_int(item.get("selection_id", item.get("selectionId"))),
                    side=item.get("side") or item.get("bet_type") or item.get("action") or "BACK",
                    price=self._safe_float(item.get("price")),
                    size=self._safe_float(item.get("size", item.get("stake"))),
                )
            )

        params = {
            "marketId": str(market_id),
            "instructions": built_instructions,
            "customerRef": str(customer_ref or ""),
        }

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/placeOrders",
            params,
        )

        return {
            "status": str(result.get("status") or ""),
            "marketId": str(result.get("marketId") or market_id),
            "instructionReports": result.get("instructionReports") or [],
            "simulated": False,
        }

    def list_current_orders(
        self,
        market_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if market_ids:
            params["marketIds"] = [str(x) for x in market_ids]

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listCurrentOrders",
            params,
        )

        return {
            "currentOrders": result.get("currentOrders") or [],
            "moreAvailable": bool(result.get("moreAvailable", False)),
            "simulated": False,
        }

    def cancel_orders(
        self,
        *,
        market_id: Optional[str] = None,
        instructions: Optional[List[Dict[str, Any]]] = None,
        customer_ref: str = "",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "customerRef": str(customer_ref or ""),
        }

        if market_id:
            params["marketId"] = str(market_id)

        if instructions:
            mapped = []
            for item in instructions:
                entry: Dict[str, Any] = {}
                bet_id = item.get("betId") or item.get("bet_id")
                size_reduction = item.get("sizeReduction") or item.get("size_reduction")

                if bet_id:
                    entry["betId"] = str(bet_id)
                if size_reduction not in (None, ""):
                    entry["sizeReduction"] = float(size_reduction)

                if entry:
                    mapped.append(entry)

            if mapped:
                params["instructions"] = mapped

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/cancelOrders",
            params,
        )

        return {
            "status": str(result.get("status") or ""),
            "instructionReports": result.get("instructionReports") or [],
            "simulated": False,
        }

    # =========================================================
    # TELEGRAM RESOLVER SUPPORT
    # =========================================================
    def list_live_soccer_events(self) -> List[Dict[str, Any]]:
        params = {
            "filter": {
                "eventTypeIds": [self.SOCCER_EVENT_TYPE_ID],
                "inPlayOnly": True,
            },
            "marketProjection": ["EVENT"],
            "maxResults": "200",
        }

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketCatalogue",
            params,
        )

        events: List[Dict[str, Any]] = []
        seen = set()

        for item in result or []:
            event = item.get("event") or {}
            event_id = str(event.get("id") or "").strip()
            event_name = str(event.get("name") or "").strip()

            if not event_id or not event_name:
                continue

            key = (event_id, event_name)
            if key in seen:
                continue
            seen.add(key)

            events.append(
                {
                    "event_id": event_id,
                    "event_name": event_name,
                }
            )

        return events

    def list_event_markets(self, event_id: Any) -> List[Dict[str, Any]]:
        params = {
            "filter": {
                "eventIds": [str(event_id)],
                "eventTypeIds": [self.SOCCER_EVENT_TYPE_ID],
                "inPlayOnly": True,
            },
            "marketProjection": ["EVENT", "RUNNER_DESCRIPTION"],
            "maxResults": "200",
        }

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketCatalogue",
            params,
        )

        markets: List[Dict[str, Any]] = []
        for item in result or []:
            markets.append(
                {
                    "market_id": str(item.get("marketId") or ""),
                    "market_name": str(item.get("marketName") or ""),
                    "event_id": str((item.get("event") or {}).get("id") or ""),
                    "event_name": str((item.get("event") or {}).get("name") or ""),
                    "runners": item.get("runners") or [],
                }
            )

        return markets

    def get_market_book(self, market_id: str) -> Optional[Dict[str, Any]]:
        params = {
            "marketIds": [str(market_id)],
            "priceProjection": {
                "priceData": ["EX_BEST_OFFERS"]
            },
            "orderProjection": "ALL",
            "matchProjection": "ROLLED_UP_BY_AVG_PRICE",
        }

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketBook",
            params,
        )

        if not result:
            return None

        raw = result[0]

        runners: List[Dict[str, Any]] = []
        for r in raw.get("runners") or []:
            ex = r.get("ex") or {}
            runners.append(
                {
                    "selectionId": self._safe_int(r.get("selectionId")),
                    "runnerName": "",
                    "status": str(r.get("status") or ""),
                    "ex": {
                        "availableToBack": ex.get("availableToBack") or [],
                        "availableToLay": ex.get("availableToLay") or [],
                    },
                }
            )

        runner_names = self._get_runner_names_for_market(market_id)

        for rr in runners:
            sel_id = rr.get("selectionId")
            rr["runnerName"] = runner_names.get(sel_id, "")

        return {
            "marketId": str(raw.get("marketId") or market_id),
            "status": str(raw.get("status") or ""),
            "inplay": bool(raw.get("inplay", False)),
            "runners": runners,
        }

    def _get_runner_names_for_market(self, market_id: str) -> Dict[int, str]:
        try:
            params = {
                "filter": {
                    "marketIds": [str(market_id)],
                },
                "marketProjection": ["RUNNER_DESCRIPTION"],
                "maxResults": "1",
            }

            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/listMarketCatalogue",
                params,
            )

            if not result:
                return {}

            market = result[0]
            mapping: Dict[int, str] = {}
            for runner in market.get("runners") or []:
                sel_id = self._safe_int(runner.get("selectionId"))
                name = str(runner.get("runnerName") or "")
                if sel_id:
                    mapping[sel_id] = name
            return mapping
        except Exception:
            logger.exception("Errore recupero runner names market_id=%s", market_id)
            return {}

    # =========================================================
    # OPTIONAL COMPAT HELPERS
    # =========================================================
    def list_soccer_events(self, live_only: bool = True) -> List[Dict[str, Any]]:
        if live_only:
            return self.list_live_soccer_events()

        params = {
            "filter": {
                "eventTypeIds": [self.SOCCER_EVENT_TYPE_ID],
            },
            "marketProjection": ["EVENT"],
            "maxResults": "200",
        }

        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketCatalogue",
            params,
        )

        events: List[Dict[str, Any]] = []
        seen = set()

        for item in result or []:
            event = item.get("event") or {}
            event_id = str(event.get("id") or "").strip()
            event_name = str(event.get("name") or "").strip()

            if not event_id or not event_name:
                continue

            key = (event_id, event_name)
            if key in seen:
                continue
            seen.add(key)

            events.append(
                {
                    "event_id": event_id,
                    "event_name": event_name,
                }
            )

        return events

    def list_markets_for_event(self, event_id: Any) -> List[Dict[str, Any]]:
        return self.list_event_markets(event_id=event_id)

    def list_market_book(self, market_id: str) -> Optional[Dict[str, Any]]:
        return self.get_market_book(market_id=market_id)

    # =========================================================
    # STATUS
    # =========================================================
    def status(self) -> Dict[str, Any]:
        return {
            "connected": bool(self.connected and self.session_token),
            "session_token_present": bool(self.session_token),
            "expiry": self.session_expiry,
            "simulated": False,
        }