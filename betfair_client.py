from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

import requests
from requests.exceptions import RequestException, Timeout


logger = logging.getLogger(__name__)


class BetfairClient:
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
            raise RuntimeError("cert file missing")
        if not os.path.exists(self.key_pem):
            raise RuntimeError("key file missing")
        return (self.cert_pem, self.key_pem)

    def _headers(self):
        h = {
            "X-Application": self.app_key,
            "Content-Type": "application/json",
        }
        if self.session_token:
            h["X-Authentication"] = self.session_token
        return h

    # =========================================================
    # CORE HTTP (RETRY + ERROR CLASSIFICATION)
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

                try:
                    data = r.json()
                except Exception:
                    raise RuntimeError("INVALID_JSON")

                if not isinstance(data, list) or not data:
                    raise RuntimeError("INVALID_JSON_RPC")

                item = data[0]

                # 🔴 SESSION EXPIRED (CRITICO)
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

            except RequestException as e:
                last_error = f"NETWORK_ERROR: {e}"
                logger.warning("network error attempt=%s error=%s", attempt, e)

            except RuntimeError:
                raise  # non nascondere errori logici

            except Exception as e:
                last_error = f"UNKNOWN_ERROR: {e}"
                logger.warning("generic error attempt=%s error=%s", attempt, e)

        raise RuntimeError(f"REQUEST_FAILED: {last_error}")

    # =========================================================
    # LOGIN
    # =========================================================
    def login(self, password: str):
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

        try:
            data = r.json()
        except Exception:
            raise RuntimeError("INVALID_LOGIN_JSON")

        if str(data.get("loginStatus")) != "SUCCESS":
            raise RuntimeError(f"LOGIN_FAILED: {data}")

        self.session_token = data.get("sessionToken", "")
        self.session_expiry = data.get("sessionExpiryTime", "")
        self.connected = True

        return {"connected": True}

    # =========================================================
    # CASHOUT (CORRETTO)
    # =========================================================
    def calculate_cashout(self, stake, odds, current_odds, side="BACK"):
        stake = self._safe_float(stake)
        odds = self._safe_float(odds)
        current_odds = self._safe_float(current_odds)

        if odds <= 1 or current_odds <= 1:
            return {"cashout_stake": 0.0}

        cashout = round((stake * odds) / current_odds, 2)

        if side == "BACK":
            profit_win = stake * (odds - 1) - cashout * (current_odds - 1)
            profit_lose = cashout - stake
            side_to_place = "LAY"
        else:
            profit_win = cashout * (current_odds - 1) - stake * (odds - 1)
            profit_lose = stake - cashout
            side_to_place = "BACK"

        return {
            "cashout_stake": cashout,
            "profit_if_win": round(profit_win, 2),
            "profit_if_lose": round(profit_lose, 2),
            "side_to_place": side_to_place,
        }

    # =========================================================
    # MARKET BOOK
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
            return result[0]
        except Exception:
            return None

    # =========================================================
    # ORDER (CRITICO → VALIDAZIONE)
    # =========================================================
    def place_bet(self, *, market_id, selection_id, side, price, size):
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

        # 🔴 VALIDAZIONE REALE
        status = str(result.get("status") or "").upper()
        reports = result.get("instructionReports") or []

        if status != "SUCCESS":
            raise RuntimeError(f"BET_FAILED: {status}")

        if not reports:
            raise RuntimeError("BET_NO_REPORT")

        for r in reports:
            if str(r.get("status")).upper() not in {"SUCCESS", "PLACED"}:
                raise RuntimeError(f"BET_REJECTED: {r}")

        return result

    # =========================================================
    def status(self):
        return {
            "connected": bool(self.session_token),
            "expiry": self.session_expiry,
        }