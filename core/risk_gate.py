"""Il risk gate reale che H-04 pretende.

Contesto, perche' non e' ovvio dal nome del file.

`TradingEngine._risk_gate()` chiama `check(payload)` sull'oggetto passato come
`risk_middleware=`. Fino alla PR precedente NESSUNA classe del repo
implementava quel contratto: l'unica con un `check()` compatibile era il
segnaposto interno all'engine, che approva tutto. `core/risk_middleware.py` NON
e' un sostituto — e' un sottoscrittore di eventi sul bus e non espone `check()`.
Questo modulo colma quel vuoto.

I limiti NON sono scritti qui: vengono letti da `trading_config`, cioe' dai
valori gia' configurati dall'owner, e una costante MANCANTE fa NEGARE invece di
ripiegare su un numero di comodo (un typo nel config disarmerebbe i limiti in
silenzio).

Tre trappole che l'ordine e la forma dei controlli devono rispettare:

H-13 — `isfinite` PRIMA di ogni soglia. In Python un NaN supera ogni confronto:
    nan < 0.10  -> False        nan > 10000 -> False        nan <= 0 -> False
Uno stake NaN attraverserebbe quindi TUTTI i limiti senza toccarne uno.

H-14 — l'esposizione di una LAY non e' lo stake. Bancare 600 a quota 21 mette a
rischio 600*(21-1) = 12.000 euro, venti volte lo stake.

Campo opzionale ASSENTE e campo opzionale INVALIDO non sono la stessa cosa.
Confonderli e' un fail-open: un `book_pct` a NaN verrebbe trattato come "non
fornito" e aggirerebbe la soglia invece di farla scattare.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, Optional, Tuple

import trading_config

logger = logging.getLogger(__name__)

BACK = "BACK"
LAY = "LAY"
_SIDES = frozenset({BACK, LAY})

# Verdetto gia' pronto, oppure None se il controllo e' passato.
_Verdict = Optional[Dict[str, Any]]


class MissingRiskConfig(LookupError):
    """Una costante di rischio attesa non esiste in trading_config."""


class RiskGate:
    """Gate fail-closed sul percorso dell'ordine.

    Implementa il contratto che l'engine si aspetta:
        check(payload) -> {"allowed": bool, "reason": str | None, "payload": dict}

    NON espone `is_wired()`: e' il segnale con cui l'engine distingue un gate
    reale dal segnaposto, quindi la sua assenza qui e' voluta.
    """

    def __init__(self, config: Any = trading_config) -> None:
        self.config = config

    # -- contratto engine ------------------------------------------------
    def is_ready(self) -> bool:
        return True

    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self._check(payload)
        except MissingRiskConfig as exc:
            # Un limite che non si riesce a leggere non e' un limite assente:
            # e' un gate che non sa cosa applicare, quindi nega.
            logger.error("Costante di rischio mancante (%s) -> DENY", exc)
            return self._deny(payload, "RISK_CONFIG_MISSING")
        except Exception:
            logger.exception("RiskGate ha sollevato -> DENY")
            return self._deny(payload, "RISK_GATE_INTERNAL_ERROR")

    # -- helper ----------------------------------------------------------
    @staticmethod
    def _deny(payload: Any, reason: str) -> Dict[str, Any]:
        return {"allowed": False, "reason": reason, "payload": payload}

    @staticmethod
    def _allow(payload: Any) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}

    def _limit(self, name: str) -> float:
        """Legge un limite dal config. Assente o non numerico -> NEGA.

        Niente `getattr(cfg, name, default)`: un default silenzioso
        maschererebbe una costante rinominata o cancellata dietro un numero
        scritto qui dentro, e i limiti risulterebbero applicati quando invece
        non lo sono piu'.
        """
        if not hasattr(self.config, name):
            raise MissingRiskConfig(name)
        value = self._finite(getattr(self.config, name))
        if value is None:
            raise MissingRiskConfig(f"{name} non e' un numero finito")
        return value

    def _flag(self, name: str) -> bool:
        """Legge un interruttore dal config. Assente o non bool -> NEGA.

        Stessa regola di `_limit`, e per lo stesso motivo. Lasciare questi due
        su `getattr(..., default)` era un'incoerenza pericolosa: il default di
        LIQUIDITY_GUARD_ENABLED era False mentre il valore configurato e' True,
        quindi una costante rinominata avrebbe DISARMATO la guardia di
        liquidita' in silenzio, proprio mentre gli altri limiti erano protetti.
        """
        if not hasattr(self.config, name):
            raise MissingRiskConfig(name)
        value = getattr(self.config, name)
        if value is not True and value is not False:
            raise MissingRiskConfig(f"{name} non e' un bool ({value!r})")
        return value

    @staticmethod
    def _finite(raw: Any) -> Optional[float]:
        """float finito, oppure None per QUALUNQUE altra cosa.

        `bool` e' escluso di proposito: e' sottoclasse di `int`, quindi `True`
        diventerebbe uno stake da un euro.
        """
        if raw is None or isinstance(raw, bool):
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) else None

    def _required(self, payload: Dict[str, Any], key: str,
                  reason: str) -> Tuple[Optional[float], _Verdict]:
        """Campo obbligatorio: assente, non numerico o non finito -> nega."""
        value = self._finite(payload.get(key))
        if value is None:
            return None, self._deny(payload, reason)
        return value, None

    def _optional(self, payload: Dict[str, Any], key: str,
                  reason: str) -> Tuple[Optional[float], _Verdict]:
        """Campo opzionale, con la distinzione che conta.

        ASSENTE  -> (None, None)  : il controllo si salta, e' legittimo.
        PRESENTE ma non numerico o non finito -> nega.
        Trattare l'invalido come assente sarebbe un fail-open: basterebbe un
        NaN per aggirare la soglia.
        """
        if key not in payload:
            return None, None
        value = self._finite(payload[key])
        if value is None:
            return None, self._deny(payload, reason)
        return value, None

    # -- singoli controlli ------------------------------------------------
    def _check_stake(self, payload: Dict[str, Any]) -> Tuple[Optional[float], _Verdict]:
        stake, denial = self._required(payload, "stake", "RISK_STAKE_NOT_FINITE")
        if stake is None:
            return None, denial
        if stake <= 0:
            return None, self._deny(payload, "RISK_STAKE_NOT_POSITIVE")
        if stake < self._limit("MIN_STAKE"):
            return None, self._deny(payload, "RISK_BELOW_MIN_STAKE")
        return stake, None

    def _check_price(self, payload: Dict[str, Any]) -> Tuple[Optional[float], _Verdict]:
        price, denial = self._required(payload, "price", "RISK_PRICE_NOT_FINITE")
        if price is None:
            return None, denial
        if price < self._limit("MIN_PRICE"):
            return None, self._deny(payload, "RISK_PRICE_BELOW_MIN")
        return price, None

    def _check_money(self, payload: Dict[str, Any], side: str,
                     stake: float, price: float) -> _Verdict:
        """Vincita ed esposizione, ognuna misurata per il lato giusto.

        BACK: si vince stake*(price-1) e si rischia lo stake.
        LAY : si vince lo stake e si rischia la liability stake*(price-1).

        `MAX_WIN` dice "vincita massima", e su quella va confrontata la vincita
        (era il difetto: confrontarla con il rischio lasciava passare una BACK
        da 500 a quota 100, che vince 49.500).
        La stessa costante e' usata anche come tetto all'esposizione: e' l'unico
        limite in euro assoluti disponibile nel config, e senza di esso H-14
        resterebbe scoperto, perche' una LAY con stake piccolo e quota alta ha
        vincita bassa ma liability enorme. Scelta CONSERVATIVA e dichiarata: se
        serve un tetto d'esposizione diverso va aggiunta una costante sua.
        """
        payout = stake * (price - 1.0)
        win = payout if side == BACK else stake
        exposure = stake if side == BACK else payout

        if not math.isfinite(win) or not math.isfinite(exposure):
            # stake e price sono finiti, ma il prodotto puo' traboccare.
            return self._deny(payload, "RISK_EXPOSURE_NOT_FINITE")

        max_win = self._limit("MAX_WIN")
        if win > max_win:
            return self._deny(payload, "RISK_MAX_WIN_EXCEEDED")
        if exposure > max_win:
            return self._deny(payload, "RISK_MAX_EXPOSURE_EXCEEDED")
        return None

    def _check_book(self, payload: Dict[str, Any]) -> _Verdict:
        book, denial = self._optional(payload, "book_pct", "RISK_BOOK_NOT_FINITE")
        if denial is not None:
            return denial
        if book is not None and book >= self._limit("BOOK_BLOCK"):
            return self._deny(payload, "RISK_BOOK_OVER_BLOCK")
        return None

    def _check_liquidity(self, payload: Dict[str, Any], stake: float) -> _Verdict:
        if not self._flag("LIQUIDITY_GUARD_ENABLED"):
            return None

        liquidity, denial = self._optional(
            payload, "available_liquidity", "RISK_LIQUIDITY_NOT_FINITE")
        if denial is not None:
            # Dato corrotto: nega SEMPRE, anche in modalita' solo-avviso.
            # `LIQUIDITY_WARNING_ONLY` dice "accetto mercati sottili", non
            # "accetto numeri illeggibili".
            return denial
        if liquidity is None:
            return None

        required = max(stake * self._limit("LIQUIDITY_MULTIPLIER"),
                       self._limit("MIN_LIQUIDITY_ABSOLUTE"))
        if liquidity >= required:
            return None

        if self._flag("LIQUIDITY_WARNING_ONLY"):
            # Scelta dell'owner (#383): qui si avvisa e basta.
            logger.warning(
                "Liquidita' %.2f sotto la richiesta %.2f: AVVISO "
                "(LIQUIDITY_WARNING_ONLY attivo, nessun blocco).",
                liquidity, required)
            return None
        return self._deny(payload, "RISK_LIQUIDITY_INSUFFICIENT")

    # -- orchestrazione ---------------------------------------------------
    def _check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return self._deny(payload, "RISK_PAYLOAD_NOT_DICT")

        stake, denial = self._check_stake(payload)
        if denial is not None:
            return denial
        price, denial = self._check_price(payload)
        if denial is not None:
            return denial
        if stake is None or price is None:
            # Invariante interna: un controllo obbligatorio ha restituito
            # "nessun valore" SENZA rifiutare. Oggi non e' raggiungibile, ma un
            # `assert` qui sarebbe peggio che inutile: sotto `python -O` sparisce,
            # e proprio sul percorso del denaro la guardia scomparirebbe in
            # silenzio lasciando proseguire con None. Si nega, e si dice perche'.
            logger.error("Invariante RiskGate violata: stake=%r price=%r -> DENY",
                         stake, price)
            return self._deny(payload, "RISK_INTERNAL_INVARIANT")

        side = str(payload.get("bet_type") or "").strip().upper()
        if side not in _SIDES:
            return self._deny(payload, "RISK_BET_TYPE_INVALID")

        for denial in (self._check_money(payload, side, stake, price),
                       self._check_book(payload),
                       self._check_liquidity(payload, stake)):
            if denial is not None:
                return denial

        return self._allow(payload)
