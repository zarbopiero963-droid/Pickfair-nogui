from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


@dataclass
class ResolvedTelegramBet:
    event_name: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    minute: int
    signal_type: str
    market_family: str
    target_line: float
    runner_name: str
    bet_type: str
    market_id: str
    selection_id: int
    price: float
    market_name: str
    confidence: float

    def to_order_payload(self, stake: float, simulation_mode: bool = False) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "selection_id": self.selection_id,
            "bet_type": self.bet_type,
            "price": float(self.price),
            "stake": float(stake),
            "event_name": self.event_name,
            "market_name": self.market_name,
            "runner_name": self.runner_name,
            "simulation_mode": bool(simulation_mode),
            "source": "TELEGRAM",
            "signal_type": self.signal_type,
            "target_line": self.target_line,
            "resolver_confidence": self.confidence,
        }


class TelegramBetResolver:
    """
    Traduce un segnale Telegram nel bet concreto da piazzare su Betfair.

    Flusso:
    1. estrae squadre / score / minuto / tipo segnale
    2. calcola linea target (es. over successivo)
    3. cerca l'evento corretto su Betfair
    4. trova il mercato Over/Under X.5
    5. trova il runner "Over X.5"
    6. prende la miglior quota disponibile
    """

    OVER_FAMILY = "OVER_UNDER"

    SIGNAL_OVER_NEXT = {
        "NEXT_GOL",
        "NEXT_GOAL",
        "NEXT_GOL_2T",
        "NEXT_GOAL_2T",
        "GOL_2_TEMPO",
        "GOL_SECONDO_TEMPO",
        "OVER_SUCCESSIVO",
    }

    SIGNAL_ZERO_ZERO_BREAK = {
        "NON_TERMINA_0_0",
        "NO_0_0",
    }

    EXPLICIT_OVER_RE = re.compile(r"\bOVER\s*(\d+(?:[.,]\d+)?)", re.IGNORECASE)
    SCORE_RE = re.compile(r"(\d+)\s*[-–]\s*(\d+)")
    MINUTE_RE = re.compile(r"(\d+)\s*m\b", re.IGNORECASE)
    VS_RE = re.compile(r"([A-Za-z0-9 .'\-]+?)\s+v\s+([A-Za-z0-9 .'\-]+)", re.IGNORECASE)

    def __init__(self, client_getter):
        """
        client_getter deve restituire il broker/client attivo.
        In live: BetfairClient
        In sim:  broker simulato (ma per trovare mercati serve il client dati live o una cache equivalente)

        Per il resolver mercato/evento servono almeno:
        - list_soccer_events() oppure list_market_catalogue(...)
        - get_market_book(...)
        """
        self.client_getter = client_getter

    # =========================================================
    # PUBLIC API
    # =========================================================
    def resolve(
        self,
        signal: Dict[str, Any],
        *,
        aggressive_best_price: bool = True,
    ) -> Optional[ResolvedTelegramBet]:
        signal = dict(signal or {})

        parsed = self._parse_signal(signal)
        if not parsed:
            return None

        home_team = parsed["home_team"]
        away_team = parsed["away_team"]
        home_score = parsed["home_score"]
        away_score = parsed["away_score"]
        minute = parsed["minute"]
        signal_type = parsed["signal_type"]

        target_line = self._resolve_target_line(
            signal_type=signal_type,
            home_score=home_score,
            away_score=away_score,
            explicit_line=parsed.get("explicit_line"),
        )
        if target_line is None:
            logger.warning("[TelegramBetResolver] Nessuna target_line risolta per segnale=%s", signal_type)
            return None

        event_match = self._resolve_event(home_team, away_team)
        if not event_match:
            logger.warning(
                "[TelegramBetResolver] Evento non trovato per %s v %s",
                home_team,
                away_team,
            )
            return None

        market_match = self._resolve_over_under_market(
            event_id=event_match["event_id"],
            target_line=target_line,
        )
        if not market_match:
            logger.warning(
                "[TelegramBetResolver] Mercato over/under %.1f non trovato per evento=%s",
                target_line,
                event_match["event_name"],
            )
            return None

        runner_match = self._resolve_runner(
            market_id=market_match["market_id"],
            market_book=market_match["market_book"],
            target_line=target_line,
            aggressive_best_price=aggressive_best_price,
        )
        if not runner_match:
            logger.warning(
                "[TelegramBetResolver] Runner Over %.1f non trovato in market_id=%s",
                target_line,
                market_match["market_id"],
            )
            return None

        return ResolvedTelegramBet(
            event_name=event_match["event_name"],
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
            minute=minute,
            signal_type=signal_type,
            market_family=self.OVER_FAMILY,
            target_line=target_line,
            runner_name=runner_match["runner_name"],
            bet_type="BACK",
            market_id=market_match["market_id"],
            selection_id=int(runner_match["selection_id"]),
            price=float(runner_match["price"]),
            market_name=market_match["market_name"],
            confidence=float(event_match["confidence"]),
        )

    # =========================================================
    # PARSING TELEGRAM SIGNAL
    # =========================================================
    def _parse_signal(self, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = self._build_text_blob(signal)
        if not text.strip():
            return None

        home_team, away_team = self._extract_teams(signal, text)
        if not home_team or not away_team:
            return None

        home_score, away_score = self._extract_score(signal, text)
        minute = self._extract_minute(signal, text)
        signal_type = self._extract_signal_type(signal, text)
        explicit_line = self._extract_explicit_over_line(signal, text)

        return {
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "minute": minute,
            "signal_type": signal_type,
            "explicit_line": explicit_line,
        }

    def _build_text_blob(self, signal: Dict[str, Any]) -> str:
        parts = [
            str(signal.get("raw_text") or ""),
            str(signal.get("message") or ""),
            str(signal.get("text") or ""),
            str(signal.get("event_name") or signal.get("event") or signal.get("match") or ""),
            str(signal.get("selection") or ""),
            str(signal.get("market_name") or signal.get("market") or ""),
            str(signal.get("signal_name") or signal.get("signal_type") or ""),
        ]
        return "\n".join([p for p in parts if p]).strip()

    def _extract_teams(self, signal: Dict[str, Any], text: str) -> Tuple[str, str]:
        event_name = str(signal.get("event_name") or signal.get("event") or signal.get("match") or "").strip()
        source = event_name if event_name else text

        m = self.VS_RE.search(source)
        if m:
            return m.group(1).strip("* ").strip(), m.group(2).strip("* ").strip()

        if "🆚" in source:
            after = source.split("🆚", 1)[1]
            m2 = self.VS_RE.search(after)
            if m2:
                return m2.group(1).strip("* ").strip(), m2.group(2).strip("* ").strip()

        return "", ""

    def _extract_score(self, signal: Dict[str, Any], text: str) -> Tuple[int, int]:
        hs = signal.get("home_score")
        aw = signal.get("away_score")
        if hs not in (None, "") and aw not in (None, ""):
            return self._safe_int(hs), self._safe_int(aw)

        m = self.SCORE_RE.search(text)
        if m:
            return self._safe_int(m.group(1)), self._safe_int(m.group(2))

        return 0, 0

    def _extract_minute(self, signal: Dict[str, Any], text: str) -> int:
        minute = signal.get("minute") or signal.get("time_minute")
        if minute not in (None, ""):
            return self._safe_int(minute)

        m = self.MINUTE_RE.search(text)
        if m:
            return self._safe_int(m.group(1))
        return 0

    def _extract_signal_type(self, signal: Dict[str, Any], text: str) -> str:
        explicit = str(signal.get("signal_type") or signal.get("signal_name") or "").strip().upper()
        if explicit:
            return explicit

        upper = self._normalize_text(text).upper()

        if "OVER SUCCESSIVO" in upper:
            return "OVER_SUCCESSIVO"
        if "NEXT GOL 2 TEMPO" in upper or "NEXT GOAL 2 TEMPO" in upper:
            return "NEXT_GOL_2T"
        if "NEXT GOL" in upper or "NEXT GOAL" in upper:
            return "NEXT_GOL"
        if "GOL 2 TEMPO" in upper:
            return "GOL_2_TEMPO"
        if "GOL SECONDO TEMPO" in upper:
            return "GOL_SECONDO_TEMPO"
        if "NON TERMINA 0-0" in upper:
            return "NON_TERMINA_0_0"

        m = self.EXPLICIT_OVER_RE.search(upper)
        if m:
            return "EXPLICIT_OVER"

        return "UNKNOWN"

    def _extract_explicit_over_line(self, signal: Dict[str, Any], text: str) -> Optional[float]:
        raw = signal.get("target_line")
        if raw not in (None, ""):
            return self._safe_float(raw)

        m = self.EXPLICIT_OVER_RE.search(text)
        if not m:
            return None
        return self._safe_float(m.group(1).replace(",", "."))

    def _resolve_target_line(
        self,
        *,
        signal_type: str,
        home_score: int,
        away_score: int,
        explicit_line: Optional[float],
    ) -> Optional[float]:
        total_goals = int(home_score) + int(away_score)

        if signal_type == "EXPLICIT_OVER":
            return explicit_line

        if signal_type in self.SIGNAL_OVER_NEXT:
            return float(total_goals) + 0.5

        if signal_type in self.SIGNAL_ZERO_ZERO_BREAK:
            if total_goals == 0:
                return 0.5
            return None

        return None

    # =========================================================
    # BETFAIR EVENT RESOLUTION
    # =========================================================
    def _resolve_event(self, home_team: str, away_team: str) -> Optional[Dict[str, Any]]:
        client = self._client()
        if client is None:
            return None

        events = self._list_live_soccer_events(client)
        if not events:
            return None

        target = f"{home_team} v {away_team}"
        target_norm = self._normalize_name(target)

        best = None
        best_score = 0.0

        for event in events:
            event_name = str(event.get("event_name") or event.get("event") or event.get("name") or "").strip()
            if not event_name:
                continue

            score = self._similarity(target_norm, self._normalize_name(event_name))
            if score > best_score:
                best_score = score
                best = event

        if not best:
            return None

        if best_score < 0.55:
            logger.warning("[TelegramBetResolver] Matching evento troppo debole: %.3f", best_score)
            return None

        return {
            "event_id": best.get("event_id"),
            "event_name": best.get("event_name") or best.get("name"),
            "confidence": best_score,
        }

    def _list_live_soccer_events(self, client) -> List[Dict[str, Any]]:
        """
        Compatibile con diversi client.
        Restituisce:
        [
            {"event_id": "...", "event_name": "Team A v Team B"},
            ...
        ]
        """
        try:
            if hasattr(client, "list_live_soccer_events"):
                return client.list_live_soccer_events() or []
        except Exception:
            logger.exception("[TelegramBetResolver] list_live_soccer_events fallita")

        try:
            if hasattr(client, "list_soccer_events"):
                return client.list_soccer_events(live_only=True) or []
        except Exception:
            logger.exception("[TelegramBetResolver] list_soccer_events fallita")

        try:
            if hasattr(client, "search_events"):
                return client.search_events(sport="SOCCER", live_only=True) or []
        except Exception:
            logger.exception("[TelegramBetResolver] search_events fallita")

        return []

    # =========================================================
    # MARKET RESOLUTION
    # =========================================================
    def _resolve_over_under_market(self, *, event_id: Any, target_line: float) -> Optional[Dict[str, Any]]:
        client = self._client()
        if client is None:
            return None

        markets = self._list_event_markets(client, event_id)
        if not markets:
            return None

        wanted_market_name = self._wanted_market_name(target_line)

        best_market = None
        for market in markets:
            market_name = str(market.get("market_name") or market.get("name") or "").strip()
            if self._normalize_name(market_name) == self._normalize_name(wanted_market_name):
                best_market = market
                break

        if not best_market:
            return None

        market_id = str(best_market.get("market_id") or best_market.get("marketId") or "").strip()
        if not market_id:
            return None

        market_book = self._get_market_book(client, market_id)
        if not market_book:
            return None

        return {
            "market_id": market_id,
            "market_name": str(best_market.get("market_name") or best_market.get("name") or wanted_market_name),
            "market_book": market_book,
        }

    def _list_event_markets(self, client, event_id: Any) -> List[Dict[str, Any]]:
        try:
            if hasattr(client, "list_event_markets"):
                return client.list_event_markets(event_id=event_id) or []
        except Exception:
            logger.exception("[TelegramBetResolver] list_event_markets fallita")

        try:
            if hasattr(client, "list_markets_for_event"):
                return client.list_markets_for_event(event_id=event_id) or []
        except Exception:
            logger.exception("[TelegramBetResolver] list_markets_for_event fallita")

        return []

    def _get_market_book(self, client, market_id: str) -> Optional[Dict[str, Any]]:
        try:
            if hasattr(client, "get_market_book"):
                return client.get_market_book(market_id) or None
        except Exception:
            logger.exception("[TelegramBetResolver] get_market_book fallita")

        try:
            if hasattr(client, "list_market_book"):
                return client.list_market_book(market_id=market_id) or None
        except Exception:
            logger.exception("[TelegramBetResolver] list_market_book fallita")

        return None

    def _wanted_market_name(self, target_line: float) -> str:
        return f"Over/Under {target_line:.1f} Goals"

    # =========================================================
    # RUNNER RESOLUTION
    # =========================================================
    def _resolve_runner(
        self,
        *,
        market_id: str,
        market_book: Dict[str, Any],
        target_line: float,
        aggressive_best_price: bool,
    ) -> Optional[Dict[str, Any]]:
        runners = market_book.get("runners") or []
        wanted_runner_name = f"Over {target_line:.1f}"

        best = None
        for runner in runners:
            runner_name = str(
                runner.get("runnerName")
                or runner.get("runner_name")
                or runner.get("name")
                or ""
            ).strip()

            if self._normalize_name(runner_name) != self._normalize_name(wanted_runner_name):
                continue

            selection_id = runner.get("selectionId") or runner.get("selection_id")
            if selection_id in (None, ""):
                continue

            ex = runner.get("ex") or {}
            backs = ex.get("availableToBack") or []
            lays = ex.get("availableToLay") or []

            price = 0.0
            if aggressive_best_price:
                # BACK aggressivo: prendo miglior lay disponibile
                if lays:
                    price = self._safe_float(lays[0].get("price"), 0.0)
                elif backs:
                    price = self._safe_float(backs[0].get("price"), 0.0)
            else:
                # BACK passivo: prendo miglior back
                if backs:
                    price = self._safe_float(backs[0].get("price"), 0.0)
                elif lays:
                    price = self._safe_float(lays[0].get("price"), 0.0)

            if price <= 1.0:
                continue

            best = {
                "selection_id": int(selection_id),
                "runner_name": runner_name,
                "price": price,
            }
            break

        return best

    # =========================================================
    # UTILS
    # =========================================================
    def _client(self):
        if callable(self.client_getter):
            try:
                return self.client_getter()
            except Exception:
                logger.exception("[TelegramBetResolver] client_getter fallita")
        return None

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return int(default)
            return int(float(value))
        except Exception:
            return int(default)

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return float(default)
            return float(str(value).replace(",", "."))
        except Exception:
            return float(default)

    def _normalize_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _normalize_name(self, text: str) -> str:
        text = self._normalize_text(text).lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r"[^a-z0-9 ]+", " ", text)
        text = re.sub(r"\bfc\b|\bclub\b|\bcalcio\b|\bsad\b", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _similarity(self, a: str, b: str) -> float:
        return float(SequenceMatcher(None, a, b).ratio())