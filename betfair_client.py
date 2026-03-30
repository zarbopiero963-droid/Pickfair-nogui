from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

import requests
from requests.exceptions import RequestException, Timeout, HTTPError


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
        self.max_retries = int(max_retries)

        self.session = session or requests.Session()

        self.session_token = ""
        self.session_expiry = ""
        self.connected = False

    # =========================================================
    # SAFE UTILS
    # =========================================================
    def _safe_float(self, v, d=0.0):
        try:
            return float(v)
        except Exception:
            return float(d)

    def _safe_int(self, v, d=0):
        try:
            return int(float(v))
        except Exception:
            return int(d)

    def _safe_side(self, v):
        s = str(v or "BACK").upper()
        return s if s in {"BACK", "LAY"} else "BACK"

    def _cert_tuple(self):
        if not os.path.exists(self.cert_pem):
            raise RuntimeError("CERT_MISSING")
        if not os.path.exists(self.key_pem):
            raise RuntimeError("KEY_MISSING")
        return (self.cert_pem, self.key_pem)

    def _headers(self):
        h = {
            "X-Application": self.app_key,
            "Content-Type": "application/json",
        }
        if self.session_token:
            h["X-Authentication"] = self.session_token
        return h

    def _parse_json(self, response, err_code: str):
        try:
            return response.json()
        except Exception:
            raise RuntimeError(err_code)

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

        if "API_ERROR" in e:
            return "PERMANENT"

        return "UNKNOWN"

    # =========================================================
    # CORE JSON-RPC
    # =========================================================
    def _post_jsonrpc(self, url: str, method: str, params: Dict[str, Any]):
        if not self.session_token:
            raise RuntimeError("NOT_AUTHENTICATED")

        payload = [{
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }]

        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                r = self.session.post(
                    url,
                    headers=self._headers(),
                    data=json.dumps(payload),
                    timeout=self.timeout
                )

                r.raise_for_status()

                data = self._parse_json(r, "INVALID_JSON")

                if not isinstance(data, list) or not data:
                    raise RuntimeError("INVALID_JSON_RPC")

                item = data[0]

                if "error" in item:
                    err = str(item["error"])

                    if "INVALID_SESSION" in err or "NO_SESSION" in err:
                        self.connected = False
                        self.session_token = ""
                        raise RuntimeError("SESSION_EXPIRED")

                    raise RuntimeError(f"API_ERROR: {err}")

                return item.get("result") or {}

            except Timeout:
                last_error = "TIMEOUT"
                logger.warning("timeout attempt=%s", attempt)

            except HTTPError as e:
                code = getattr(e.response, "status_code", "UNKNOWN")
                last_error = f"HTTP_{code}"
                logger.warning("http error attempt=%s code=%s", attempt, code)

            except RequestException as e:
                last_error = f"NETWORK_ERROR: {e}"
                logger.warning("network error attempt=%s error=%s", attempt, e)

            except RuntimeError:
                raise

            except Exception as e:
                last_error = f"UNKNOWN_ERROR: {e}"
                logger.warning("unknown error attempt=%s error=%s", attempt, e)

        raise RuntimeError(f"REQUEST_FAILED: {last_error}")

    # =========================================================
    # LOGIN
    # =========================================================
    def login(self, password: str) -> Dict[str, Any]:
        try:
            r = self.session.post(
                self.IDENTITY_URL,
                headers={
                    "X-Application": self.app_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"username": self.username, "password": password},
                cert=self._cert_tuple(),
                timeout=self.timeout
            )

            r.raise_for_status()

            data = self._parse_json(r, "INVALID_LOGIN_JSON")

            if str(data.get("loginStatus")) != "SUCCESS":
                raise RuntimeError(f"LOGIN_FAILED: {data}")

            self.session_token = data.get("sessionToken", "")
            self.session_expiry = data.get("sessionExpiryTime", "")
            self.connected = True

            return {
                "connected": True,
                "session_token": bool(self.session_token),
                "expiry": self.session_expiry,
            }

        except Timeout:
            raise RuntimeError("LOGIN_TIMEOUT")

        except HTTPError as e:
            raise RuntimeError(f"LOGIN_HTTP_ERROR: {e}")

        except RequestException as e:
            raise RuntimeError(f"LOGIN_NETWORK_ERROR: {e}")

    # =========================================================
    # LOGOUT (FIX GUARDRAIL)
    # =========================================================
    def logout(self) -> Dict[str, Any]:
        self.session_token = ""
        self.session_expiry = ""
        self.connected = False
        return {
            "ok": True,
            "logged_out": True,
        }

    # =========================================================
    # MARKET BOOK (SAFE)
    # =========================================================
    def get_market_book(self, market_id: str):
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketBook",
            {"marketIds": [market_id]}
        )

        if not result:
            return None

        try:
            book = result[0]
        except Exception:
            return None

        # 🔴 HARDEN: quote mancanti
        runners = book.get("runners") or []
        for r in runners:
            ex = r.get("ex") or {}
            r["availableToBack"] = ex.get("availableToBack") or []
            r["availableToLay"] = ex.get("availableToLay") or []

        return book

    # =========================================================
    # PLACE BET (ENTERPRISE SAFE)
    # =========================================================
    def place_bet(self, *, market_id, selection_id, side, price, size):
        try:
            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/placeOrders",
                {
                    "marketId": market_id,
                    "instructions": [{
                        "selectionId": int(selection_id),
                        "side": self._safe_side(side),
                        "orderType": "LIMIT",
                        "limitOrder": {
                            "size": float(size),
                            "price": float(price),
                            "persistenceType": "LAPSE"
                        }
                    }]
                }
            )

            status = str(result.get("status") or "").upper()
            reports = result.get("instructionReports") or []

            if status != "SUCCESS":
                raise RuntimeError(f"BET_FAILED: {status}")

            if not reports:
                raise RuntimeError("BET_NO_REPORT")

            for r in reports:
                if str(r.get("status")).upper() not in {"SUCCESS", "PLACED"}:
                    raise RuntimeError(f"BET_REJECTED: {r}")

            return {
                "ok": True,
                "result": result
            }

        except RuntimeError as e:
            classification = self._classify_error(str(e))

            return {
                "ok": False,
                "error": str(e),
                "classification": classification,
                "order_unknown": "TIMEOUT" in str(e)
            }

    # =========================================================
    # STATUS
    # =========================================================
    def status(self):
        return {
            "connected": bool(self.session_token),
            "expiry": self.session_expiry,
        }