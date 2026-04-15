from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from requests.exceptions import HTTPError, RequestException, Timeout

from circuit_breaker import CircuitBreaker
from core.type_helpers import safe_float, safe_int, safe_side

logger = logging.getLogger(__name__)


class BetfairClient:
    IDENTITY_URL = "https://identitysso.betfair.it/api/certlogin"
    BETTING_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
    ACCOUNT_URL = "https://api.betfair.com/exchange/account/json-rpc/v1"

    SOCCER_EVENT_TYPE_ID = "1"

    # =========================================================
    # INIT
    # =========================================================
    def __init__(
        self,
        *,
        username: str,
        app_key: str,
        cert_pem: str,
        key_pem: str,
        session: Optional[requests.Session] = None,
        timeout: float = 20.0,
        max_retries: int = 2,
    ):
        self.username = str(username or "").strip()
        self.app_key = str(app_key or "").strip()
        self.cert_pem = str(cert_pem or "").strip()
        self.key_pem = str(key_pem or "").strip()

        self.timeout = float(timeout or 20.0)
        self.max_retries = max(0, int(max_retries))

        self.session = session or requests.Session()

        self.session_token = ""
        self.session_expiry = ""
        self.connected = False

        # Circuit breaker guards all JSON-RPC calls to Betfair API.
        # SESSION_EXPIRED does NOT trip the breaker; only network/HTTP failures do.
        self._api_breaker = CircuitBreaker(max_failures=5, reset_timeout=60.0)
        self._io_stats: Dict[str, Any] = {
            "last_operation": "",
            "last_latency_ms": 0.0,
            "last_status": "UNKNOWN",
            "total_calls": 0,
            "slow_calls": 0,
            "degraded_calls": 0,
            "unavailable_calls": 0,
            "last_error": "",
            "last_call_at": 0.0,
        }

    def _record_io(self, *, operation: str, started_at: float, status: str, error: str = "") -> None:
        elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
        status_up = str(status or "UNKNOWN").strip().upper()
        self._io_stats["last_operation"] = str(operation)
        self._io_stats["last_latency_ms"] = round(elapsed_ms, 3)
        self._io_stats["last_status"] = status_up
        self._io_stats["last_error"] = str(error or "")
        self._io_stats["last_call_at"] = time.time()
        self._io_stats["total_calls"] = int(self._io_stats.get("total_calls", 0) or 0) + 1
        if status_up == "SLOW":
            self._io_stats["slow_calls"] = int(self._io_stats.get("slow_calls", 0) or 0) + 1
        if status_up == "DEGRADED":
            self._io_stats["degraded_calls"] = int(self._io_stats.get("degraded_calls", 0) or 0) + 1
        if status_up == "UNAVAILABLE":
            self._io_stats["unavailable_calls"] = int(self._io_stats.get("unavailable_calls", 0) or 0) + 1

    def io_snapshot(self) -> Dict[str, Any]:
        return dict(self._io_stats)

    # =========================================================
    # SAFE UTILS
    # =========================================================
    def _safe_float(self, v: Any, d: float = 0.0) -> float:
        return safe_float(v, d)

    def _safe_int(self, v: Any, d: int = 0) -> int:
        return safe_int(v, d)

    def _safe_side(self, v: Any) -> str:
        return safe_side(v)

    def _cert_tuple(self) -> tuple[str, str]:
        if not os.path.exists(self.cert_pem):
            raise RuntimeError("CERT_MISSING")
        if not os.path.exists(self.key_pem):
            raise RuntimeError("KEY_MISSING")
        return (self.cert_pem, self.key_pem)

    def _headers(self) -> Dict[str, str]:
        headers = {
            "X-Application": self.app_key,
            "Content-Type": "application/json",
        }
        if self.session_token:
            headers["X-Authentication"] = self.session_token
        return headers

    def _parse_json(self, response: Any, err_code: str) -> Any:
        try:
            return response.json()
        except Exception as exc:
            raise RuntimeError(err_code) from exc

    # =========================================================
    # ERROR CLASSIFICATION
    # =========================================================
    def _classify_error(self, error: str) -> str:
        e = str(error).upper()

        if "TIMEOUT" in e:
            return "TRANSIENT"

        if "NETWORK_ERROR" in e:
            return "TRANSIENT"

        if "HTTP_5" in e:
            return "TRANSIENT"

        if "SESSION_EXPIRED" in e:
            return "PERMANENT"

        if "INVALID_JSON" in e:
            return "PERMANENT"

        if "INVALID_JSON_RPC" in e:
            return "PERMANENT"

        if "API_ERROR" in e:
            return "PERMANENT"

        return "UNKNOWN"

    # =========================================================
    # CORE JSON-RPC
    # =========================================================
    def _post_jsonrpc(self, url: str, method: str, params: Dict[str, Any]) -> Any:
        started_at = time.monotonic()
        if not self.session_token:
            self._record_io(operation=method, started_at=started_at, status="UNAVAILABLE", error="NOT_AUTHENTICATED")
            raise RuntimeError("NOT_AUTHENTICATED")

        if self._api_breaker.is_open():
            self._record_io(operation=method, started_at=started_at, status="UNAVAILABLE", error="CIRCUIT_BREAKER_OPEN")
            raise RuntimeError("CIRCUIT_BREAKER_OPEN")

        payload = [{
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }]

        last_error: Optional[str] = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(
                    url,
                    headers=self._headers(),
                    data=json.dumps(payload),
                    timeout=self.timeout,
                )

                response.raise_for_status()

                data = self._parse_json(response, "INVALID_JSON")

                if not isinstance(data, list) or not data:
                    raise RuntimeError("INVALID_JSON_RPC")

                item = data[0]

                if "error" in item:
                    err = str(item["error"])

                    if "INVALID_SESSION" in err or "NO_SESSION" in err:
                        self.connected = False
                        self.session_token = ""
                        self.session_expiry = ""
                        raise RuntimeError("SESSION_EXPIRED")

                    raise RuntimeError(f"API_ERROR: {err}")

                result = item.get("result") or {}
                self._api_breaker.record_success()
                elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
                status = "SLOW" if elapsed_ms >= max(2000.0, self.timeout * 1000.0 * 0.8) else "SUCCESS"
                self._record_io(operation=method, started_at=started_at, status=status)
                return result

            except Timeout:
                last_error = "TIMEOUT"
                logger.warning("timeout attempt=%s method=%s", attempt, method)

            except HTTPError as exc:
                code = getattr(exc.response, "status_code", "UNKNOWN")
                last_error = f"HTTP_{code}"
                logger.warning("http error attempt=%s method=%s code=%s", attempt, method, code)

            except RequestException as exc:
                last_error = f"NETWORK_ERROR: {exc}"
                logger.warning("network error attempt=%s method=%s error=%s", attempt, method, exc)

            except RuntimeError:
                raise

            except Exception as exc:
                last_error = f"UNKNOWN_ERROR: {exc}"
                logger.warning("unknown error attempt=%s method=%s error=%s", attempt, method, exc)

        err = RuntimeError(f"REQUEST_FAILED: {last_error}")
        self._api_breaker.record_failure(err)
        failure_status = "DEGRADED" if (last_error or "").startswith(("TIMEOUT", "HTTP_5", "NETWORK_ERROR")) else "UNAVAILABLE"
        self._record_io(operation=method, started_at=started_at, status=failure_status, error=str(last_error or "REQUEST_FAILED"))
        raise err

    # =========================================================
    # LOGIN / LOGOUT
    # =========================================================
    def login(self, password: str) -> Dict[str, Any]:
        started_at = time.monotonic()
        try:
            response = self.session.post(
                self.IDENTITY_URL,
                headers={
                    "X-Application": self.app_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"username": self.username, "password": password},
                cert=self._cert_tuple(),
                timeout=self.timeout,
            )

            response.raise_for_status()

            data = self._parse_json(response, "INVALID_LOGIN_JSON")

            if str(data.get("loginStatus")) != "SUCCESS":
                raise RuntimeError(f"LOGIN_FAILED: {data}")

            self.session_token = str(data.get("sessionToken") or "")
            self.session_expiry = str(data.get("sessionExpiryTime") or "")
            self.connected = bool(self.session_token)
            elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
            status = "SLOW" if elapsed_ms >= max(2000.0, self.timeout * 1000.0 * 0.8) else "SUCCESS"
            self._record_io(operation="login", started_at=started_at, status=status)

            return {
                "connected": self.connected,
                "session_token": bool(self.session_token),
                "expiry": self.session_expiry,
            }

        except Timeout as exc:
            self._record_io(operation="login", started_at=started_at, status="DEGRADED", error="LOGIN_TIMEOUT")
            raise RuntimeError("LOGIN_TIMEOUT") from exc

        except HTTPError as exc:
            self._record_io(operation="login", started_at=started_at, status="DEGRADED", error=f"LOGIN_HTTP_ERROR:{exc}")
            raise RuntimeError(f"LOGIN_HTTP_ERROR: {exc}") from exc

        except RequestException as exc:
            self._record_io(operation="login", started_at=started_at, status="DEGRADED", error=f"LOGIN_NETWORK_ERROR:{exc}")
            raise RuntimeError(f"LOGIN_NETWORK_ERROR: {exc}") from exc

    def logout(self) -> Dict[str, Any]:
        self.session_token = ""
        self.session_expiry = ""
        self.connected = False
        return {
            "ok": True,
            "logged_out": True,
        }

    # =========================================================
    # ACCOUNT
    # =========================================================
    def get_account_funds(self) -> Dict[str, Any]:
        result = self._post_jsonrpc(
            self.ACCOUNT_URL,
            "AccountAPING/v1.0/getAccountFunds",
            {},
        )

        if not isinstance(result, dict):
            raise RuntimeError("INVALID_ACCOUNT_FUNDS")

        return {
            "available": self._safe_float(result.get("availableToBetBalance"), 0.0),
            "exposure": self._safe_float(result.get("exposure"), 0.0),
            "retained_commission": self._safe_float(result.get("retainedCommission"), 0.0),
            "exposure_limit": self._safe_float(result.get("exposureLimit"), 0.0),
            "discount_rate": self._safe_float(result.get("discountRate"), 0.0),
            "points_balance": self._safe_float(result.get("pointsBalance"), 0.0),
        }

    # =========================================================
    # CASHOUT
    # =========================================================
    def calculate_cashout(
        self,
        original_stake: Any,
        original_odds: Any,
        current_odds: Any,
        side: str = "BACK",
    ) -> Dict[str, Any]:
        original_stake_f = self._safe_float(original_stake, 0.0)
        original_odds_f = self._safe_float(original_odds, 0.0)
        current_odds_f = self._safe_float(current_odds, 0.0)
        safe_side = self._safe_side(side)

        default_side = "LAY" if safe_side == "BACK" else "BACK"

        if original_stake_f <= 0.0 or original_odds_f <= 1.0 or current_odds_f <= 1.0:
            return {
                "cashout_stake": 0.0,
                "profit_if_win": 0.0,
                "profit_if_lose": 0.0,
                "side_to_place": default_side,
            }

        cashout = round((original_stake_f * original_odds_f) / current_odds_f, 2)

        if safe_side == "BACK":
            profit_if_win = (
                original_stake_f * (original_odds_f - 1.0)
                - cashout * (current_odds_f - 1.0)
            )
            profit_if_lose = cashout - original_stake_f
            side_to_place = "LAY"
        else:
            profit_if_win = (
                cashout * (current_odds_f - 1.0)
                - original_stake_f * (original_odds_f - 1.0)
            )
            profit_if_lose = original_stake_f - cashout
            side_to_place = "BACK"

        return {
            "cashout_stake": cashout,
            "profit_if_win": round(profit_if_win, 2),
            "profit_if_lose": round(profit_if_lose, 2),
            "side_to_place": side_to_place,
        }

    # =========================================================
    # MARKET BOOK
    # =========================================================
    def get_market_book(self, market_id: str) -> Optional[Dict[str, Any]]:
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketBook",
            {"marketIds": [market_id]},
        )

        if not result:
            return None

        try:
            book = result[0]
        except Exception:
            return None

        runners = book.get("runners") or []
        for runner in runners:
            ex = runner.get("ex") or {}
            runner["availableToBack"] = ex.get("availableToBack") or []
            runner["availableToLay"] = ex.get("availableToLay") or []

        return book

    # =========================================================
    # ORDERS
    # =========================================================
    def place_bet(
        self,
        *,
        market_id: Any,
        selection_id: Any,
        side: Any,
        price: Any,
        size: Any,
    ) -> Dict[str, Any]:
        market_id_s = str(market_id or "").strip()
        if not market_id_s:
            raise RuntimeError("INVALID_MARKET_ID")

        try:
            selection_id_i = int(selection_id)
        except Exception as exc:
            raise RuntimeError("INVALID_SELECTION_ID") from exc
        if selection_id_i <= 0:
            raise RuntimeError("INVALID_SELECTION_ID")

        try:
            price_f = float(price)
        except Exception as exc:
            raise RuntimeError("INVALID_PRICE") from exc
        if price_f <= 1.0:
            raise RuntimeError("INVALID_PRICE")

        try:
            size_f = float(size)
        except Exception as exc:
            raise RuntimeError("INVALID_SIZE") from exc
        if size_f <= 0.0:
            raise RuntimeError("INVALID_SIZE")

        try:
            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/placeOrders",
                {
                    "marketId": market_id_s,
                    "instructions": [{
                        "selectionId": selection_id_i,
                        "side": self._safe_side(side),
                        "orderType": "LIMIT",
                        "limitOrder": {
                            "size": size_f,
                            "price": price_f,
                            "persistenceType": "LAPSE",
                        },
                    }],
                },
            )

            status = str(result.get("status") or "").upper()
            reports = result.get("instructionReports") or []

            if status != "SUCCESS":
                raise RuntimeError(f"BET_FAILED: {status}")

            if not reports:
                raise RuntimeError("BET_NO_REPORT")

            for report in reports:
                if str(report.get("status") or "").upper() not in {"SUCCESS", "PLACED"}:
                    raise RuntimeError(f"BET_REJECTED: {report}")

            return {
                "ok": True,
                "result": result,
            }

        except RuntimeError as exc:
            error_text = str(exc)
            return {
                "ok": False,
                "error": error_text,
                "classification": self._classify_error(error_text),
                "order_unknown": "TIMEOUT" in error_text.upper(),
            }

    # =========================================================
    # ORDERS – CANCEL
    # =========================================================
    def cancel_orders(
        self,
        *,
        market_id: Any,
        bet_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Cancel orders on a Betfair market via the cancelOrders API.

        When bet_ids is empty or None, cancels ALL unmatched orders on the
        market (the standard emergency-stop / flatten behaviour).
        Returns a result dict; never raises on API-level failure (logs and
        returns ok=False so callers can record the error without crashing).
        """
        market_id_s = str(market_id or "").strip()
        if not market_id_s:
            raise RuntimeError("INVALID_MARKET_ID")

        # Empty instructions list → cancel all active orders on the market.
        # Non-empty → cancel only the listed bet IDs.
        instructions: List[Dict[str, Any]] = (
            [{"betId": str(bid)} for bid in bet_ids if bid]
            if bet_ids else []
        )

        try:
            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/cancelOrders",
                {
                    "marketId": market_id_s,
                    "instructions": instructions,
                },
            )

            status = str(result.get("status") or "").upper()
            reports = result.get("instructionReports") or []

            if status == "FAILURE":
                err = str(result.get("errorCode") or "UNKNOWN")
                raise RuntimeError(f"CANCEL_FAILED: {err}")

            return {
                "ok": True,
                "market_id": market_id_s,
                "status": status or "SUCCESS",
                "cancelled_count": len(reports),
                "result": result,
            }

        except RuntimeError as exc:
            error_text = str(exc)
            return {
                "ok": False,
                "market_id": market_id_s,
                "error": error_text,
                "classification": self._classify_error(error_text),
            }

    # =========================================================
    # STATUS
    # =========================================================
    def status(self) -> Dict[str, Any]:
        return {
            "connected": bool(self.session_token),
            "expiry": self.session_expiry,
        }
