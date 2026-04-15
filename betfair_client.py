from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import HTTPError, RequestException, Timeout

from circuit_breaker import CircuitBreaker
from core.session_manager import SessionManager
from core.type_helpers import safe_float, safe_int, safe_side

logger = logging.getLogger(__name__)


class BetfairClient:
    IDENTITY_URL = "https://identitysso.betfair.it/api/certlogin"
    KEEPALIVE_URL = "https://identitysso.betfair.it/api/keepAlive"
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
        self._last_login_password = ""

        # Circuit breaker guards all JSON-RPC calls to Betfair API.
        # SESSION_EXPIRED does NOT trip the breaker; only network/HTTP failures do.
        self._api_breaker = CircuitBreaker(max_failures=5, reset_timeout=60.0)
        self.session_manager = SessionManager(
            login_func=self._login_for_session_manager,
            keepalive_func=self._keepalive_for_session_manager,
            clock=time.time,
        )

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
        headers.update(self.session_manager.get_auth_headers())
        return headers

    def _sync_session_manager_from_legacy_fields(self) -> None:
        manager_token = self.session_manager.get_session_token()
        if self.session_token and not manager_token:
            self.session_manager.mark_logged_in(self.session_token)

    def _normalize_auth_error_code(self, error_payload: Any) -> str:
        if isinstance(error_payload, dict):
            code = str(error_payload.get("errorCode") or error_payload.get("code") or "").upper()
            if code:
                return code
            data = str(error_payload)
        else:
            data = str(error_payload or "")
        upper = data.upper()
        if "NO_SESSION" in upper:
            return "NO_SESSION"
        if "INVALID_SESSION" in upper:
            return "INVALID_SESSION"
        if "TEMPORARY_BAN_TOO_MANY_REQUESTS" in upper:
            return "TEMPORARY_BAN_TOO_MANY_REQUESTS"
        if "INPUT_VALIDATION_ERROR" in upper:
            return "INPUT_VALIDATION_ERROR"
        if "INTERNAL_ERROR" in upper:
            return "INTERNAL_ERROR"
        return "UNKNOWN_AUTH_ERROR"

    def _attempt_controlled_recovery(self) -> bool:
        if not self.session_manager.can_attempt_login():
            return False
        state_before = self.session_manager.snapshot().get("state")
        self.session_manager.tick()
        self.session_token = self.session_manager.get_session_token()
        self.connected = bool(self.session_token)
        if self.connected:
            self.session_expiry = str(self.session_manager.snapshot().get("session_expires_at") or "")
        return bool(self.session_manager.has_valid_session()) and self.session_manager.snapshot().get("state") != state_before

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
        self._sync_session_manager_from_legacy_fields()
        self.session_manager.tick()
        if not self.session_manager.has_valid_session():
            if not self._attempt_controlled_recovery():
                raise RuntimeError("NOT_AUTHENTICATED")

        if self._api_breaker.is_open():
            raise RuntimeError("CIRCUIT_BREAKER_OPEN")

        payload = [{
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }]

        last_error: Optional[str] = None
        recovered = False

        for attempt in range(self.max_retries + 2):
            try:
                self.session_token = self.session_manager.get_session_token()
                self.connected = bool(self.session_token)
                if not self.session_token:
                    raise RuntimeError("NOT_AUTHENTICATED")
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
                    err_payload = item.get("error")
                    err = str(err_payload)
                    auth_code = self._normalize_auth_error_code(err_payload)
                    self.session_manager.on_api_auth_error(auth_code, raw=err_payload)
                    self.session_token = self.session_manager.get_session_token()
                    self.connected = bool(self.session_token)
                    if auth_code in {"NO_SESSION", "INVALID_SESSION"}:
                        self.session_expiry = ""
                        if not recovered and self._attempt_controlled_recovery():
                            recovered = True
                            continue
                        raise RuntimeError("SESSION_EXPIRED")
                    if auth_code == "TEMPORARY_BAN_TOO_MANY_REQUESTS":
                        raise RuntimeError("AUTH_THROTTLED")
                    raise RuntimeError(f"API_ERROR: {err}")

                result = item.get("result") or {}
                self.session_manager.on_api_success(raw=result)
                self._api_breaker.record_success()
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
        raise err

    # =========================================================
    # LOGIN / LOGOUT
    # =========================================================
    def login(self, password: str) -> Dict[str, Any]:
        self._last_login_password = str(password or "")
        data = self._perform_login(self._last_login_password)
        self.session_manager.on_login_result(data)
        self.session_token = self.session_manager.get_session_token()
        self.connected = bool(self.session_token)
        self.session_expiry = str(self.session_manager.snapshot().get("session_expires_at") or "")
        return {
            "connected": self.connected,
            "session_token": bool(self.session_token),
            "expiry": self.session_expiry,
        }

    def _perform_login(self, password: str) -> Dict[str, Any]:
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

            return data

        except Timeout as exc:
            raise RuntimeError("LOGIN_TIMEOUT") from exc

        except HTTPError as exc:
            raise RuntimeError(f"LOGIN_HTTP_ERROR: {exc}") from exc

        except RequestException as exc:
            raise RuntimeError(f"LOGIN_NETWORK_ERROR: {exc}") from exc

    def _login_for_session_manager(self) -> Dict[str, Any]:
        if not self._last_login_password:
            return {"loginStatus": "FAIL", "errorCode": "INPUT_VALIDATION_ERROR"}
        try:
            return self._perform_login(self._last_login_password)
        except RuntimeError as exc:
            message = str(exc)
            code = self._normalize_auth_error_code(message)
            if "TEMPORARY_BAN_TOO_MANY_REQUESTS" in message.upper():
                code = "TEMPORARY_BAN_TOO_MANY_REQUESTS"
            return {"loginStatus": "FAIL", "errorCode": code, "detail": message}

    def _keepalive_for_session_manager(self) -> Dict[str, Any]:
        token = self.session_manager.get_session_token()
        if not token:
            return {"status": "FAIL", "errorCode": "NO_SESSION"}
        try:
            response = self.session.post(
                self.KEEPALIVE_URL,
                headers={
                    "X-Application": self.app_key,
                    "X-Authentication": token,
                    "Accept": "application/json",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = self._parse_json(response, "INVALID_KEEPALIVE_JSON")
            keepalive_status = str(data.get("status") or data.get("keepAliveStatus") or "").upper()
            if keepalive_status == "SUCCESS":
                return {"status": "SUCCESS"}
            code = self._normalize_auth_error_code(data)
            return {"status": "FAIL", "errorCode": code, "detail": data}
        except Exception as exc:
            code = self._normalize_auth_error_code(str(exc))
            return {"status": "FAIL", "errorCode": code, "detail": str(exc)}

    def logout(self) -> Dict[str, Any]:
        self.session_token = ""
        self.session_expiry = ""
        self.connected = False
        self.session_manager.mark_session_expired("LOGOUT")
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
        self.session_token = self.session_manager.get_session_token() or self.session_token
        return {
            "connected": bool(self.session_manager.get_session_token() or self.session_token),
            "expiry": self.session_expiry,
        }
