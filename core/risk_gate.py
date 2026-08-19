"""Il risk gate reale che H-04 pretende.

Contesto, perche' non e' ovvio dal nome del file.

`TradingEngine._risk_gate()` chiama `check(payload)` sull'oggetto passato come
`risk_middleware=`. Fino alla PR precedente NESSUNA classe del repo
implementava quel contratto: l'unica con un `check()` compatibile era il
segnaposto interno all'engine, che approva tutto. `core/risk_middleware.py` NON
e' un sostituto — e' un sottoscrittore di eventi sul bus e non espone `check()`.
Questo modulo colma quel vuoto.

I limiti NON sono inventati qui: vengono tutti da `trading_config`, cioe' dai
valori gia' configurati dall'owner. Questo modulo li APPLICA sul percorso
dell'ordine, dove finora nessuno li applicava.

Due trappole numeriche che l'ordine dei controlli deve rispettare:

H-13 — `isfinite` PRIMA di ogni soglia. In Python un NaN supera ogni confronto:
    nan < 0.10  -> False        nan > 10000 -> False        nan <= 0 -> False
Uno stake NaN attraverserebbe quindi TUTTI i limiti senza toccarne uno. Va
respinto prima, non "anche".

H-14 — l'esposizione di una LAY non e' lo stake. Bancare 50 euro a quota 21
mette a rischio 50*(21-1) = 1000 euro, venti volte lo stake. Un gate che
misura la LAY come `stake` sbaglia, e sbaglia nel verso pericoloso.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, Optional

import trading_config

logger = logging.getLogger(__name__)

BACK = "BACK"
LAY = "LAY"
_SIDES = frozenset({BACK, LAY})


class RiskGate:
    """Gate fail-closed sul percorso dell'ordine.

    Implementa il contratto che l'engine si aspetta:
        check(payload) -> {"allowed": bool, "reason": str | None, "payload": dict}

    NON espone `is_wired()`: e' il segnale con cui l'engine distingue un gate
    reale dal segnaposto, quindi la sua assenza qui e' voluta e la readiness
    passera' da DEGRADED a cablato.
    """

    def __init__(self, config: Any = trading_config) -> None:
        self.config = config

    # -- contratto engine ------------------------------------------------
    def is_ready(self) -> bool:
        return True

    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self._check(payload)
        except Exception:
            # Un gate che esplode non e' un gate che approva. L'engine sarebbe
            # comunque fail-closed sull'eccezione, ma non deleghiamo a lui la
            # nostra stessa robustezza.
            logger.exception("RiskGate ha sollevato -> DENY")
            return self._deny(payload, "RISK_GATE_INTERNAL_ERROR")

    # -- helper ----------------------------------------------------------
    @staticmethod
    def _deny(payload: Any, reason: str) -> Dict[str, Any]:
        return {"allowed": False, "reason": reason, "payload": payload}

    @staticmethod
    def _allow(payload: Any) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}

    @staticmethod
    def _number(raw: Any) -> Optional[float]:
        """float finito, oppure None.

        None copre insieme tre casi che devono tutti negare: campo assente,
        campo non convertibile, campo convertibile ma non finito (nan/inf).
        Restituire None invece di sollevare tiene il chiamante lineare.
        """
        if raw is None or isinstance(raw, bool):
            # bool e' sottoclasse di int: True diventerebbe 1.0 e passerebbe
            # per uno stake da un euro. Non e' un numero, e' un flag.
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) else None

    # -- logica ----------------------------------------------------------
    def _check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return self._deny(payload, "RISK_PAYLOAD_NOT_DICT")

        cfg = self.config

        # --- STAKE ------------------------------------------------------
        # Assente = negato. Un gate che non vede il denaro non puo' approvarlo:
        # il payload dell'engine non ha uno schema garantito per questi campi.
        stake = self._number(payload.get("stake"))
        if stake is None:
            return self._deny(payload, "RISK_STAKE_NOT_FINITE")
        if stake <= 0:
            return self._deny(payload, "RISK_STAKE_NOT_POSITIVE")
        min_stake = float(getattr(cfg, "MIN_STAKE", 0.10))
        if stake < min_stake:
            return self._deny(payload, "RISK_BELOW_MIN_STAKE")

        # --- PRICE ------------------------------------------------------
        price = self._number(payload.get("price"))
        if price is None:
            return self._deny(payload, "RISK_PRICE_NOT_FINITE")
        min_price = float(getattr(cfg, "MIN_PRICE", 1.02))
        if price < min_price:
            return self._deny(payload, "RISK_PRICE_BELOW_MIN")

        # --- SIDE -------------------------------------------------------
        side = str(payload.get("bet_type") or "").strip().upper()
        if side not in _SIDES:
            return self._deny(payload, "RISK_BET_TYPE_INVALID")

        # --- ESPOSIZIONE (H-14) -----------------------------------------
        # BACK: si rischia lo stake. LAY: si rischia la liability.
        exposure = stake if side == BACK else stake * (price - 1.0)
        if not math.isfinite(exposure):
            # stake e price sono finiti, ma il prodotto puo' traboccare.
            return self._deny(payload, "RISK_EXPOSURE_NOT_FINITE")
        max_win = float(getattr(cfg, "MAX_WIN", 10000.0))
        if exposure > max_win:
            return self._deny(payload, "RISK_MAX_WIN_EXCEEDED")

        # --- BOOK -------------------------------------------------------
        # Controllato SOLO se il payload lo porta. L'engine non lo calcola, e
        # negare ogni ordine perche' il dato non c'e' bloccherebbe tutto.
        # Limite dichiarato, non nascosto: senza `book_pct` l'over-round non e'
        # verificato da questo gate.
        book = self._number(payload.get("book_pct"))
        if book is not None:
            book_block = float(getattr(cfg, "BOOK_BLOCK", 110.0))
            if book >= book_block:
                return self._deny(payload, "RISK_BOOK_OVER_BLOCK")

        # --- LIQUIDITA' -------------------------------------------------
        # Rispetta la scelta dell'owner: `LIQUIDITY_WARNING_ONLY` e' True per
        # decisione esplicita (#383), quindi qui si AVVISA e non si blocca.
        # Togliere quella spunta dalla GUI arma il blocco reale.
        if bool(getattr(cfg, "LIQUIDITY_GUARD_ENABLED", True)):
            liquidity = self._number(payload.get("available_liquidity"))
            if liquidity is not None:
                multiplier = float(getattr(cfg, "LIQUIDITY_MULTIPLIER", 3.0))
                floor = float(getattr(cfg, "MIN_LIQUIDITY_ABSOLUTE", 50.0))
                required = max(stake * multiplier, floor)
                if liquidity < required:
                    if bool(getattr(cfg, "LIQUIDITY_WARNING_ONLY", True)):
                        logger.warning(
                            "Liquidita' %.2f sotto la richiesta %.2f: AVVISO "
                            "(LIQUIDITY_WARNING_ONLY attivo, nessun blocco).",
                            liquidity, required,
                        )
                    else:
                        return self._deny(payload, "RISK_LIQUIDITY_INSUFFICIENT")

        return self._allow(payload)
