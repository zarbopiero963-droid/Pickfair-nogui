from __future__ import annotations

from typing import Any, Callable, Optional


class SessionManager:
    LOGGED_OUT = "LOGGED_OUT"
    LOGGING_IN = "LOGGING_IN"
    ACTIVE = "ACTIVE"
    KEEPALIVE_DUE = "KEEPALIVE_DUE"
    REFRESHING = "REFRESHING"
    EXPIRED = "EXPIRED"
    DEGRADED = "DEGRADED"
    LOCKED_OUT = "LOCKED_OUT"

    _ALLOWED_TRANSITIONS = {
        (LOGGED_OUT, LOGGING_IN),
        (LOGGING_IN, ACTIVE),
        (LOGGING_IN, DEGRADED),
        (LOGGING_IN, LOCKED_OUT),
        (ACTIVE, KEEPALIVE_DUE),
        (KEEPALIVE_DUE, REFRESHING),
        (REFRESHING, ACTIVE),
        (REFRESHING, EXPIRED),
        (REFRESHING, DEGRADED),
        (ACTIVE, EXPIRED),
        (ACTIVE, DEGRADED),
        (DEGRADED, LOGGING_IN),
        (DEGRADED, LOCKED_OUT),
        (DEGRADED, ACTIVE),
        (EXPIRED, LOGGING_IN),
        (LOCKED_OUT, LOGGED_OUT),
        (LOCKED_OUT, LOGGING_IN),
    }

    _AUTH_CATEGORY_BY_CODE = {
        "NO_SESSION": "AUTH_EXPIRED",
        "INVALID_SESSION": "AUTH_INVALID",
        "TEMPORARY_BAN_TOO_MANY_REQUESTS": "AUTH_THROTTLED",
        "INPUT_VALIDATION_ERROR": "AUTH_INTERNAL",
        "INTERNAL_ERROR": "AUTH_INTERNAL",
        "MALFORMED_AUTH_RESPONSE": "AUTH_MALFORMED",
        "MISSING_ERROR_FIELD": "AUTH_MALFORMED",
        "MISSING_STATUS_FIELD": "AUTH_MALFORMED",
        "UNKNOWN_AUTH_ERROR": "AUTH_UNKNOWN",
    }

    def __init__(
        self,
        login_func: Callable[[], Any],
        keepalive_func: Callable[[], Any],
        clock: Callable[[], float],
        keepalive_interval_sec: int = 600,
        session_ttl_sec: int = 1200,
        login_backoff_sec: int = 60,
        lockout_sec: int = 1200,
    ) -> None:
        self.login_func = login_func
        self.keepalive_func = keepalive_func
        self.clock = clock
        self.keepalive_interval_sec = int(keepalive_interval_sec)
        self.session_ttl_sec = int(session_ttl_sec)
        self.login_backoff_sec = int(login_backoff_sec)
        self.lockout_sec = int(lockout_sec)

        self.state = self.LOGGED_OUT
        self._token: str = ""
        self.logged_in_at: Optional[float] = None
        self.last_keepalive_at: Optional[float] = None
        self.session_expires_at: Optional[float] = None
        self.consecutive_keepalive_failures = 0
        self.consecutive_login_failures = 0
        self.last_login_attempt_at: Optional[float] = None
        self.last_keepalive_attempt_at: Optional[float] = None
        self.locked_out_until: Optional[float] = None
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[str] = None
        self.last_error_category: Optional[str] = None
        self.last_error_detail: Any = None
        self._last_tick_login_attempt_at: Optional[float] = None
        self._last_keepalive_window_attempted: Optional[int] = None

    def _now(self, now: Optional[float] = None) -> float:
        return float(self.clock() if now is None else now)

    def _enter_state(self, target: str) -> None:
        if self.state == target:
            return
        transition = (self.state, target)
        if transition not in self._ALLOWED_TRANSITIONS:
            raise RuntimeError(f"ILLEGAL_SESSION_TRANSITION: {self.state}->{target}")
        self.state = target

    def _set_error(self, code: str, detail: Any = None, raw: Any = None) -> None:
        normalized = self._normalize_auth_error(code=code, raw=raw, detail=detail)
        self.last_error_code = normalized["code"]
        self.last_error_category = normalized["category"]
        self.last_error = normalized["message"]
        self.last_error_detail = normalized["detail"]

    def _is_expired(self, now: Optional[float] = None) -> bool:
        ts = self._now(now)
        return bool(self.session_expires_at is not None and ts >= float(self.session_expires_at))

    def _current_keepalive_window(self, now: float) -> Optional[int]:
        if self.logged_in_at is None:
            return None
        elapsed = now - float(self.logged_in_at)
        if elapsed < self.keepalive_interval_sec:
            return None
        return int(elapsed // self.keepalive_interval_sec)

    def _normalize_auth_error(self, code: Any = None, raw: Any = None, detail: Any = None) -> dict[str, Any]:
        upper_code = str(code or "").strip().upper()
        if not upper_code:
            upper_code = "UNKNOWN_AUTH_ERROR"
        category = self._AUTH_CATEGORY_BY_CODE.get(upper_code, "AUTH_UNKNOWN")
        return {
            "code": upper_code,
            "category": category,
            "message": f"{category}:{upper_code}",
            "detail": detail if detail is not None else raw,
        }

    def tick(self, now: Optional[float] = None) -> str:
        ts = self._now(now)
        if self.state == self.LOCKED_OUT and self.locked_out_until is not None and ts >= float(self.locked_out_until):
            self._enter_state(self.LOGGED_OUT)

        if self._is_expired(ts) and self.state in {self.ACTIVE, self.KEEPALIVE_DUE, self.REFRESHING}:
            self.mark_session_expired(reason="SESSION_TTL_ELAPSED", now=ts)

        if self.has_valid_session(ts) and self.should_keepalive(ts):
            self._enter_state(self.KEEPALIVE_DUE)
            self._enter_state(self.REFRESHING)
            self.last_keepalive_attempt_at = ts
            window = self._current_keepalive_window(ts)
            if window is not None:
                self._last_keepalive_window_attempted = window
            result = self.keepalive_func()
            self.on_keepalive_result(result, now=ts)
            return self.state

        if not self.has_valid_session(ts) and self.can_attempt_login(ts):
            if self._last_tick_login_attempt_at == ts:
                return self.state
            self._last_tick_login_attempt_at = ts
            if self.state in {self.LOGGED_OUT, self.DEGRADED, self.EXPIRED, self.LOCKED_OUT}:
                self._enter_state(self.LOGGING_IN)
            self.last_login_attempt_at = ts
            result = self.login_func()
            self.on_login_result(result, now=ts)
            return self.state

        return self.state

    def has_valid_session(self, now: Optional[float] = None) -> bool:
        ts = self._now(now)
        if self.state in {self.EXPIRED, self.LOCKED_OUT}:
            return False
        if not self._token:
            return False
        if self._is_expired(ts):
            return False
        return True

    def get_session_token(self, now: Optional[float] = None) -> str:
        return self._token if self.has_valid_session(now) else ""

    def get_auth_headers(self, now: Optional[float] = None) -> dict[str, str]:
        token = self.get_session_token(now)
        return {"X-Authentication": token} if token else {}

    def should_keepalive(self, now: Optional[float] = None) -> bool:
        ts = self._now(now)
        if not self.has_valid_session(ts):
            return False
        if self.state in {self.LOCKED_OUT, self.LOGGED_OUT, self.EXPIRED}:
            return False
        last_touch = self.last_keepalive_at if self.last_keepalive_at is not None else self.logged_in_at
        if last_touch is None:
            return False
        if ts < float(last_touch) + self.keepalive_interval_sec:
            return False
        window = self._current_keepalive_window(ts)
        if window is None:
            return False
        if self._last_keepalive_window_attempted == window:
            return False
        return True

    def can_attempt_login(self, now: Optional[float] = None) -> bool:
        ts = self._now(now)
        if self.state == self.LOCKED_OUT and self.locked_out_until is not None and ts < float(self.locked_out_until):
            return False
        if self.last_login_attempt_at is None:
            return True
        return ts >= float(self.last_login_attempt_at) + self.login_backoff_sec

    def mark_logged_in(self, token: Any, now: Optional[float] = None) -> None:
        ts = self._now(now)
        token_s = str(token or "").strip()
        if not token_s:
            self._set_error(code="MALFORMED_AUTH_RESPONSE", detail={"reason": "missing_token"})
            if self.state == self.LOGGING_IN:
                self._enter_state(self.DEGRADED)
            return
        self._token = token_s
        self.logged_in_at = ts
        self.last_keepalive_at = ts
        self.session_expires_at = ts + self.session_ttl_sec
        self.consecutive_login_failures = 0
        self.last_error = None
        self.last_error_code = None
        self.last_error_category = None
        self.last_error_detail = None
        if self.state in {self.LOGGED_OUT, self.EXPIRED, self.DEGRADED, self.LOCKED_OUT}:
            self._enter_state(self.LOGGING_IN)
        if self.state != self.ACTIVE:
            self._enter_state(self.ACTIVE)

    def mark_session_expired(self, reason: Any, now: Optional[float] = None) -> None:
        _ = self._now(now)
        self._token = ""
        self.session_expires_at = None
        self._set_error(code="NO_SESSION", detail={"reason": str(reason or "SESSION_EXPIRED")})
        if self.state in {self.ACTIVE, self.REFRESHING, self.KEEPALIVE_DUE}:
            self._enter_state(self.EXPIRED)

    def on_keepalive_result(self, result: Any, now: Optional[float] = None) -> None:
        ts = self._now(now)
        payload = result if isinstance(result, dict) else {"raw": result}
        status = str(payload.get("status") or payload.get("keepAliveStatus") or "").upper()
        error_code = str(payload.get("errorCode") or payload.get("code") or "").upper()
        if status == "SUCCESS" and error_code in {"", "OK"}:
            self.last_keepalive_at = ts
            self.session_expires_at = ts + self.session_ttl_sec
            self.consecutive_keepalive_failures = 0
            self.last_error = None
            self.last_error_code = None
            self.last_error_category = None
            self.last_error_detail = None
            self._enter_state(self.ACTIVE)
            return

        self.consecutive_keepalive_failures += 1
        if error_code in {"NO_SESSION", "INVALID_SESSION"}:
            self._set_error(code=error_code, detail=payload)
            self._token = ""
            self.session_expires_at = None
            self._enter_state(self.EXPIRED)
            return

        self._set_error(code=error_code or "INTERNAL_ERROR", detail=payload)
        self._enter_state(self.DEGRADED)

    def on_login_result(self, result: Any, now: Optional[float] = None) -> None:
        ts = self._now(now)
        payload = result if isinstance(result, dict) else {"raw": result}
        login_status = str(payload.get("loginStatus") or payload.get("status") or "").upper()
        token = str(payload.get("sessionToken") or payload.get("token") or "").strip()
        error_code = str(payload.get("errorCode") or payload.get("code") or "").upper()
        if login_status == "SUCCESS" and token:
            self._token = token
            self.logged_in_at = ts
            self.last_keepalive_at = ts
            self.session_expires_at = ts + self.session_ttl_sec
            self.consecutive_login_failures = 0
            self.locked_out_until = None
            self.last_error = None
            self.last_error_code = None
            self.last_error_category = None
            self.last_error_detail = None
            self._enter_state(self.ACTIVE)
            return

        self.consecutive_login_failures += 1
        if error_code == "TEMPORARY_BAN_TOO_MANY_REQUESTS":
            self._set_error(code=error_code, detail=payload)
            self.locked_out_until = ts + self.lockout_sec
            if self.has_valid_session(ts):
                if self.state != self.ACTIVE:
                    self._enter_state(self.DEGRADED)
                else:
                    self._enter_state(self.DEGRADED)
            else:
                self._token = ""
                self.session_expires_at = None
                self._enter_state(self.LOCKED_OUT)
            return

        self._set_error(code=error_code or "INTERNAL_ERROR", detail=payload)
        self._enter_state(self.DEGRADED)

    def on_api_success(self, raw: Any = None, now: Optional[float] = None) -> None:
        _ = self._now(now)
        _ = raw

    def on_api_auth_error(self, error_code: Any, raw: Any = None, now: Optional[float] = None) -> None:
        ts = self._now(now)
        normalized = self._normalize_auth_error(code=error_code, raw=raw)
        code = normalized["code"]
        self.last_error = normalized["message"]
        self.last_error_code = code
        self.last_error_category = normalized["category"]
        self.last_error_detail = normalized["detail"]

        if code in {"NO_SESSION", "INVALID_SESSION"}:
            self._token = ""
            self.session_expires_at = None
            if self.state in {self.ACTIVE, self.REFRESHING, self.KEEPALIVE_DUE}:
                self._enter_state(self.EXPIRED)
            elif self.state not in {self.EXPIRED, self.LOCKED_OUT}:
                self._enter_state(self.DEGRADED)
            return
        if code == "TEMPORARY_BAN_TOO_MANY_REQUESTS":
            self.locked_out_until = ts + self.lockout_sec
            if self.has_valid_session(ts):
                self._enter_state(self.DEGRADED)
            else:
                self._token = ""
                self.session_expires_at = None
                if self.state in {self.DEGRADED, self.LOGGING_IN}:
                    self._enter_state(self.LOCKED_OUT)
                else:
                    self.state = self.LOCKED_OUT
            return
        if self.state == self.ACTIVE:
            self._enter_state(self.DEGRADED)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "has_token": bool(self._token),
            "token_present": bool(self._token),
            "logged_in_at": self.logged_in_at,
            "last_keepalive_at": self.last_keepalive_at,
            "session_expires_at": self.session_expires_at,
            "keepalive_interval_sec": self.keepalive_interval_sec,
            "session_ttl_sec": self.session_ttl_sec,
            "consecutive_keepalive_failures": self.consecutive_keepalive_failures,
            "consecutive_login_failures": self.consecutive_login_failures,
            "last_login_attempt_at": self.last_login_attempt_at,
            "last_keepalive_attempt_at": self.last_keepalive_attempt_at,
            "locked_out_until": self.locked_out_until,
            "last_error": self.last_error,
            "last_error_code": self.last_error_code,
            "last_error_category": self.last_error_category,
            "last_error_detail": self.last_error_detail,
        }
