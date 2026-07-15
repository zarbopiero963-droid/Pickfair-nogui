from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from typing import Any, Dict, List, Optional

import trading_config

try:
    from dutching import calculate_dutching
except ImportError:
    # compat legacy guardrail
    from dutching import calculate_dutching_stakes as _calculate_dutching_stakes

    def calculate_dutching(selections, total_stake, commission=4.5):
        odds = [float(s["price"]) for s in selections]
        commission_value = float(commission if commission is not None else 4.5)
        res = _calculate_dutching_stakes(
            odds,
            float(total_stake),
            commission=commission_value,
            commission_aware=True,
        )
        stakes = res.get("stakes", []) or []
        profits = res.get("profits", []) or []
        net_profits = res.get("net_profits", []) or []
        avg_profit = float(res.get("avg_profit", 0.0) or 0.0)
        avg_net_profit = float(res.get("avg_net_profit", avg_profit) or avg_profit)
        book_pct = float(res.get("book_pct", 0.0) or 0.0)

        results = []
        for idx, selection in enumerate(selections):
            side = str(selection.get("side") or selection.get("effectiveType") or "BACK").upper()
            item = {
                "selectionId": int(selection["selectionId"]),
                "price": float(selection["price"]),
                "stake": float(stakes[idx]) if idx < len(stakes) else 0.0,
                "side": side,
                "runnerName": selection.get("runnerName", ""),
                "profitIfWins": float(profits[idx]) if idx < len(profits) else 0.0,
                "profitIfWinsNet": (
                    float(net_profits[idx]) if idx < len(net_profits) else 0.0
                ),
            }
            if side == "LAY":
                item["liability"] = round(
                    float(item["stake"]) * max(0.0, float(item["price"]) - 1.0),
                    2,
                )
            results.append(item)

        return results, avg_profit, book_pct, avg_net_profit


logger = logging.getLogger(__name__)


class DutchingController:
    """
    Controller headless per dutching, con contract stabile.

    API pubbliche:
    - validate(payload)
    - preview(payload)
    - precheck(payload)
    - submit_dutching(payload, dry_run=False, preflight=False)
    - execute(payload)  # alias compatibile
    - manual_bet(payload)
    - check_duplicate(payload)
    """

    def __init__(self, bus, runtime_controller):
        self.bus = bus
        self.runtime = runtime_controller
        self._recent_batches: Dict[str, float] = {}
        self._batch_ttl_seconds = 6 * 60 * 60

    # =========================================================
    # HELPERS
    # =========================================================
    def _ok(self, **kwargs) -> Dict[str, Any]:
        out = {"ok": True}
        out.update(kwargs)
        return out

    def _fail(self, error: str, **kwargs) -> Dict[str, Any]:
        out = {"ok": False, "error": str(error)}
        out.update(kwargs)
        return out

    def _safe_publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if self.bus is None or not hasattr(self.bus, "publish"):
            return
        self.bus.publish(event_name, payload)

    def _publish_audit(self, event_name: str, payload: Dict[str, Any]) -> None:
        try:
            self._safe_publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish audit event %s", event_name)

    def _cleanup_batches(self) -> None:
        now = time.time()
        expired = [
            batch_id
            for batch_id, ts in self._recent_batches.items()
            if now - ts > self._batch_ttl_seconds
        ]
        for batch_id in expired:
            self._recent_batches.pop(batch_id, None)

    def _build_batch_id(self, payload: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
        normalized = {
            "market_id": str(payload.get("market_id") or ""),
            "event_name": str(payload.get("event_name") or ""),
            "market_name": str(payload.get("market_name") or ""),
            "simulation_mode": bool(payload.get("simulation_mode", False)),
            "legs": [
                {
                    "selectionId": int(item["selectionId"]),
                    "price": float(item["price"]),
                    "stake": float(item["stake"]),
                    "side": str(item.get("side", "BACK")).upper(),
                }
                for item in results
            ],
        }
        raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _build_event_key(self, payload: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
        market_id = str(payload.get("market_id") or "")
        event_name = str(payload.get("event_name") or "")
        market_name = str(payload.get("market_name") or "")
        selection_part = ",".join(
            str(int(item["selectionId"]))
            for item in sorted(results, key=lambda x: int(x["selectionId"]))
        )
        base = f"dutching|{market_id}|{event_name}|{market_name}|{selection_part}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _duplication_guard(self):
        return getattr(self.runtime, "duplication_guard", None)

    def _table_manager(self):
        return getattr(self.runtime, "table_manager", None)

    def _config(self):
        return getattr(self.runtime, "config", None)

    @staticmethod
    def _book_threshold(config, attr: str, fallback: float) -> float:
        """Soglia book% dalla config (editabile da GUI) con fallback FAIL-SAFE.

        Se il valore in config e' assente / non numerico / non finito / <= 0, si
        ricade sulla costante `trading_config` (soglia di sicurezza): un backend
        rotto o un valore corrotto NON deve disattivare il gate ne' spostarlo su
        un valore assurdo.
        """
        raw = getattr(config, attr, None)
        if raw is None:
            return float(fallback)
        try:
            val = float(raw)
            if math.isfinite(val) and val > 0.0:
                return val
        except (TypeError, ValueError):
            pass
        return float(fallback)

    def _book_block_threshold(self, config) -> float:
        return self._book_threshold(config, "book_block", trading_config.BOOK_BLOCK)

    def _book_warning_threshold(self, config) -> float:
        return self._book_threshold(config, "book_warning", trading_config.BOOK_WARNING)

    # -- Liquidity guard (PR2b) -------------------------------------------
    @staticmethod
    def _min_liquidity_absolute(config) -> float:
        """Floor assoluto di liquidita' dalla config, fallback FAIL-SAFE.

        A differenza del multiplier (> 0), qui `0.0` e' LEGITTIMO (= nessun floor
        assoluto). Si ricade sulla costante solo se il valore e' assente / non
        numerico / non finito / negativo.
        """
        raw = getattr(config, "min_liquidity_absolute", None)
        if raw is None:
            return float(trading_config.MIN_LIQUIDITY_ABSOLUTE)
        try:
            val = float(raw)
            if math.isfinite(val) and val >= 0.0:
                return val
        except (TypeError, ValueError):
            pass
        return float(trading_config.MIN_LIQUIDITY_ABSOLUTE)

    @staticmethod
    def _liquidity_warning_only(config) -> bool:
        """True => guard in sola OSSERVAZIONE (avviso, mai blocco); False => BLOCCO.

        Default OPT-IN (#383): config assente ricade su
        trading_config.LIQUIDITY_WARNING_ONLY (True), cosi' il rilascio del blocco
        NON inizia a bloccare a sorpresa; l'owner lo arma dalla GUI (warning_only=False).
        """
        raw = getattr(config, "liquidity_warning_only", None)
        if raw is None:
            return bool(trading_config.LIQUIDITY_WARNING_ONLY)
        if isinstance(raw, str):
            # Parsing robusto: "False"/"0"/"" NON devono valere True (bool("0") e'
            # True). Un blocco armato salvato come stringa deve restare armato.
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        return bool(raw)

    @staticmethod
    def _min_price(config) -> float:
        """Quota minima di strategia (floor editabile) con fallback FAIL-SAFE.

        Clampata a >= 1.01 (minimo Betfair inviolabile): config assente / non
        numerica / non finita / < 1.01 ricade su max(1.01, trading_config.MIN_PRICE).
        NB: il floor hard `price <= 1.01` in validate() resta INDIPENDENTE e
        immune-da-config; questo e' un floor di strategia SOPRA di esso.
        """
        raw = getattr(config, "min_price", None)
        if raw is not None:
            try:
                val = float(raw)
                if math.isfinite(val) and val >= 1.01:
                    return val
            except (TypeError, ValueError):
                pass
        return max(1.01, float(trading_config.MIN_PRICE))

    # -- Max Win cap (G5) --------------------------------------------------
    @staticmethod
    def _max_win(config) -> float:
        """Cap vincita/payout per gamba dalla config (editabile GUI), fail-safe.

        Riusa `_book_threshold` (numerico finito > 0, fallback FAIL-SAFE): un
        backend rotto o un valore corrotto NON deve disattivare il cap ne'
        spostarlo su un valore assurdo. Fallback = trading_config.MAX_WIN.
        """
        return DutchingController._book_threshold(config, "max_win", trading_config.MAX_WIN)

    @staticmethod
    def _profit_epsilon(config) -> float:
        """Tolleranza (in €) sulla varianza di profitto NETTO tra gli esiti (G5).

        Importo ASSOLUTO in euro (come `max_win`), non una percentuale: riusa
        `_book_threshold` (numerico finito > 0, fallback FAIL-SAFE) => un backend
        rotto o un valore corrotto NON disattiva l'avviso ne' lo sposta su un
        valore assurdo. Fallback = trading_config.PROFIT_EPSILON (0.50). Warning-
        only: questa soglia NON blocca mai.
        """
        return DutchingController._book_threshold(config, "profit_epsilon", trading_config.PROFIT_EPSILON)

    @staticmethod
    def _profit_epsilon_enabled(config) -> bool:
        """True => avviso profit_epsilon ATTIVO (default). False solo se l'owner lo
        disattiva dalla GUI.

        Default OPT-OUT (``True`` quando la config e' assente/None): l'avviso e'
        NON-bloccante (zero rischio), quindi resta attivo di default e la checkbox
        fa da interruttore 'muto'. Parsing stringa robusto: 'false'/'0'/'no'/'off'/''
        disattivano; qualunque altro valore (o un bool) segue il proprio truthiness.
        """
        raw = getattr(config, "profit_epsilon_enabled", None)
        if raw is None:
            return True
        if isinstance(raw, str):
            return raw.strip().lower() not in {"0", "false", "no", "off", ""}
        return bool(raw)

    @staticmethod
    def _profit_spread_net(results: List[Dict[str, Any]]):
        """Spread del profitto NETTO tra gli esiti = max - min su ``profitIfWinsNet``.

        FAIL-OPEN, coerente con `_min_net_profit`: ritorna ``None`` (=> nessun
        avviso) se i profitti netti non sono TUTTI presenti/validi (uno per esito)
        o se c'e' meno di un esito comparabile. Il controller usa sempre il calcolo
        equalizzato (``calculate_dutching`` default ``equalize=True``, mai
        sovrascritto), quindi lo spread misurato e' quello POST-equalizzazione, come
        richiesto (avviso solo con equalize attivo).
        """
        net_values: List[float] = []
        for item in results or []:
            if not isinstance(item, dict) or "profitIfWinsNet" not in item:
                continue
            raw = item.get("profitIfWinsNet")
            # Presente ma None => netto assente per l'esito: NON contarlo (sotto, il
            # guard sulla lunghezza attiva il FAIL-OPEN). Coercirlo a 0.0 gonfierebbe
            # lo spread con un profitto fittizio => avviso spurio (Fable/Greptile P2).
            if raw is None:
                continue
            try:
                val = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(val):
                continue
            net_values.append(val)
        if len(net_values) < 2 or len(net_values) != len(results or []):
            return None
        return max(net_values) - min(net_values)

    @staticmethod
    def _max_win_warning_only(config) -> bool:
        """True => cap in sola OSSERVAZIONE (avviso, mai blocco); False => BLOCCO.

        Default OPT-IN (#383-style): config assente => True, cosi' il rilascio del
        cap NON inizia a bloccare a sorpresa; l'owner lo arma dalla GUI
        (warning_only=False). Parsing stringa robusto: "False"/"0"/"" NON valgono
        True (un blocco armato salvato come stringa deve restare armato).
        """
        raw = getattr(config, "max_win_warning_only", None)
        if raw is None:
            return True
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        return bool(raw)

    @staticmethod
    def _leg_potential_win(item: Dict[str, Any]) -> float:
        """Importo massimo di UNA gamba confrontato con MAX_WIN.

        BACK: payout lordo = stake * quota (importo restituito se la selezione
        vince). LAY: LIABILITY = stake * (quota - 1), cioe' la perdita/esposizione
        reale se la selezione vince (decisione owner: sul LAY il cap protegge dal
        RISCHIO, non dal piccolo backer-stake incassato). Usa il campo `liability`
        precomputato se presente (coerente con _compute_order_exposure), altrimenti
        lo ricalcola. Fail-safe su campi mancanti/non numerici => 0.0 (nessun falso
        blocco).
        """
        side = str(item.get("side", "BACK")).upper()
        try:
            stake = float(item.get("stake", 0.0) or 0.0)
            price = float(item.get("price", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if side == "LAY":
            # Liability = rischio reale del LAY. La baseline e' SEMPRE ricalcolata da
            # stake/price (stake*(quota-1)); il campo `liability` precomputato si usa
            # solo se e' un float positivo finito, e comunque si prende il MAX col
            # ricalcolo. Cosi' un precomputato assente / None / 0 / non-numerico /
            # non-finito / sottostimato NON puo' mai abbassare il rischio sotto il
            # valore reale (fail-CLOSED: il gate money-management non si aggira con
            # una liability stale/corrotta — rilievo GPT-5.6 Terra + Fable 5 su #393).
            recomputed = max(0.0, stake * max(0.0, price - 1.0))
            raw_liability = item.get("liability")
            if raw_liability is not None:
                try:
                    liability = float(raw_liability)
                    if math.isfinite(liability) and liability > 0.0:
                        return max(recomputed, liability)
                except (TypeError, ValueError):
                    pass
            return recomputed
        return max(0.0, stake * price)

    @staticmethod
    def _max_win_breaches(results: List[Dict[str, Any]], max_win: float) -> List[Dict[str, Any]]:
        """Gambe la cui vincita potenziale supera il cap (per-gamba, non somma).

        FAIL-SAFE: un `selectionId` non numerico NON deve sollevare (romperebbe il
        gate money-management nel precheck) => degrada a 0 nel report.
        """
        breaches: List[Dict[str, Any]] = []
        for item in results:
            win = DutchingController._leg_potential_win(item)
            if win > max_win + 1e-9:
                try:
                    sel = int(item.get("selectionId", 0) or 0)
                except (TypeError, ValueError):
                    sel = 0
                breaches.append(
                    {
                        "selectionId": sel,
                        "potential_win": round(win, 2),
                        "cap": round(max_win, 2),
                    }
                )
        return breaches

    # -- Max Stake % warning (G5) -----------------------------------------
    @staticmethod
    def _max_stake_pct(config) -> float:
        """Soglia WARNING stake come FRAZIONE del bankroll (0.30 = 30%).

        Il campo config `max_stake_pct` e' in scala PERCENTUALE (0-100, come gli
        altri `max_*_pct`); qui lo si converte in frazione. Fallback FAIL-SAFE a
        trading_config.MAX_STAKE_PCT (gia' una frazione, 0.30) se assente / non
        numerico / non finito / <= 0 / FUORI dal contratto 0-100. Warning-only:
        questa soglia NON blocca mai.

        Upper-bound 0-100 anche a RUNTIME (non solo in GUI): un valore > 100 (es.
        da edit diretto del DB o config legacy) diventerebbe una frazione > 1
        (200 => 2.0 = 200% del bankroll), soglia irraggiungibile che indebolisce
        silenziosamente l'avviso. Fuori contratto => FAIL-SAFE costante, coerente
        col rifiuto > 100 lato GUI e con la gestione degli altri valori invalidi.
        """
        raw = getattr(config, "max_stake_pct", None)
        if raw is not None:
            try:
                pct = float(raw)
                if math.isfinite(pct) and 0.0 < pct <= 100.0:
                    return pct / 100.0
            except (TypeError, ValueError):
                pass
        return float(trading_config.MAX_STAKE_PCT)

    def _market_book(self, market_id):
        """Book di mercato dalla CACHE (market_tracker), SENZA I/O.

        Nessuna chiamata di rete nel path di submit (niente snapshot sincrono che
        stallerebbe il thread o restituirebbe ladder vuoti da cold-cache). Se il
        book non e' in cache => None => il gate NON blocca (fail-open).
        """
        tracker = getattr(self.runtime, "market_tracker", None)
        if tracker is not None and hasattr(tracker, "get_market"):
            try:
                book = tracker.get_market(market_id)
                if isinstance(book, dict) and book.get("runners"):
                    return book
            except Exception:
                logger.exception("liquidity guard: market_tracker.get_market fallita")
        return None

    @staticmethod
    def _liquidity_crosses(side, order_price, book_price) -> bool:
        """Un livello del book e' eseguibile alla quota dell'ordine (book REALE Betfair).

        Sul book reale un BACK a order_price viene riempito dai livelli di
        availableToBack a quota >= order_price (quote uguali o migliori per il
        backer) => eseguibile se book_price >= order_price. Un LAY a order_price dai
        livelli di availableToLay a quota <= order_price => eseguibile se book_price
        <= order_price. NB: direzione INVERTITA rispetto al mirror del matcher
        interno del simulatore (#383): il market_tracker usa i campi Betfair-standard.
        """
        if str(side).upper() == "BACK":
            return book_price >= order_price
        return book_price <= order_price

    @classmethod
    def _sum_executable_liquidity(cls, ladder, side, order_price) -> float:
        """Somma le size dei soli livelli eseguibili alla quota dell'ordine."""
        total = 0.0
        for level in ladder or []:
            try:
                lvl_price = float((level or {}).get("price", 0.0) or 0.0)
                lvl_size = float((level or {}).get("size", 0.0) or 0.0)
            except (TypeError, ValueError, AttributeError):
                continue
            if lvl_price <= 0.0 or lvl_size <= 0.0:
                continue
            if cls._liquidity_crosses(side, order_price, lvl_price):
                total += lvl_size
        return total

    @classmethod
    def _available_liquidity(cls, book, selection_id, side, price):
        """Liquidita' ESEGUIBILE per una gamba dal book REALE gia' recuperato.

        Lato ladder Betfair-standard (#383, deciso dall'owner): un BACK consuma
        availableToBack, un LAY consuma availableToLay (e' la liquidita' che l'ordine
        matcha davvero sul mercato reale; coerente con direct_best_price/
        betfair_client). Filtrata per prezzo eseguibile (_liquidity_crosses). Le
        `size` del book sono backer-stake, omogenee con lo `stake` della gamba (per
        il LAY lo stake e' backer-stake, la liability e' un campo separato). Ritorna
        None se book/selezione/ladder non disponibili (osservazione ignota).
        """
        if not isinstance(book, dict):
            return None
        runners = book.get("runners")
        if not isinstance(runners, list):
            return None
        try:
            target = int(selection_id)
            order_price = float(price)
        except (TypeError, ValueError):
            return None
        ladder_key = "availableToBack" if str(side).upper() == "BACK" else "availableToLay"
        for runner in runners:
            if not isinstance(runner, dict):
                continue
            sid = runner.get("selectionId")
            try:
                if sid is None or int(sid) != target:
                    continue
            except (TypeError, ValueError):
                continue
            ladder = (runner.get("ex") or {}).get(ladder_key)
            if ladder is None:
                return None  # ladder assente => ignoto
            return cls._sum_executable_liquidity(ladder, side, order_price)
        return None  # selezione non trovata

    @classmethod
    def _leg_liquidity_shortfall(cls, book, item, multiplier, min_abs):
        """Shortfall di liquidita' per una gamba, o None se ok/ignota.

        required = stake * multiplier: la size da matchare sul lato opposto e' lo
        STAKE (le size del book sono backer-stake), NON la liability. Confronto
        omogeneo (size vs size).
        """
        try:
            selection_id = int(item.get("selectionId"))
            stake = float(item.get("stake", 0.0) or 0.0)
        except (TypeError, ValueError):
            return None
        if stake <= 0.0:
            return None
        side = str(item.get("side") or "BACK").upper()
        try:
            price = float(item.get("price", 1.0) or 1.0)
        except (TypeError, ValueError):
            price = 1.0
        available = cls._available_liquidity(book, selection_id, side, price)
        if available is None:
            return None  # dato mancante => nessuna osservazione
        required = float(stake) * float(multiplier)
        if available < min_abs or available < required:
            return {
                "selectionId": selection_id,
                "available": round(float(available), 2),
                "required": round(float(required), 2),
                "min_absolute": round(float(min_abs), 2),
            }
        return None

    def _evaluate_liquidity_guard(self, config, payload, results) -> Dict[str, Any]:
        """Calcola lo shortfall di liquidita' eseguibile per gamba sul book REALE.

        Ritorna {"warning": bool, "shortfall": [...]}. Il precheck usa lo shortfall
        per BLOCCARE (se `liquidity_warning_only=False`) oppure solo segnalare (flag
        `liquidity_warning`/`liquidity_shortfall`) in modalita' avviso. Lato ladder
        Betfair-standard (#383): BACK -> availableToBack, LAY -> availableToLay,
        filtrato per prezzo eseguibile. `guard_enabled=False` esplicito disattiva
        del tutto (nessuna osservazione ne' blocco). Il book e' recuperato UNA sola
        volta (solo cache, nessun I/O) e riusato per le gambe; book assente =>
        fail-open (nessuno shortfall, quindi nessun blocco).
        """
        out: Dict[str, Any] = {"warning": False, "shortfall": []}
        if not bool(getattr(config, "liquidity_guard_enabled", True)):
            return out
        multiplier = self._book_threshold(config, "liquidity_multiplier", trading_config.LIQUIDITY_MULTIPLIER)
        min_abs = self._min_liquidity_absolute(config)
        book = self._market_book((payload or {}).get("market_id"))
        if book is None:
            return out  # nessun book in cache => nessuna osservazione (fail-open)
        shortfall = [
            leg
            for leg in (
                self._leg_liquidity_shortfall(book, item, multiplier, min_abs)
                for item in (results or [])
            )
            if leg is not None
        ]
        if shortfall:
            out["warning"] = True
            out["shortfall"] = shortfall
        return out

    def _mode(self):
        return getattr(self.runtime, "mode", None)

    def _risk_desk(self):
        return getattr(self.runtime, "risk_desk", None)

    def _batch_manager(self):
        return getattr(self.runtime, "dutching_batch_manager", None)

    def _table_total_exposure(self) -> float:
        table_manager = self._table_manager()
        if table_manager and hasattr(table_manager, "total_exposure"):
            try:
                return float(table_manager.total_exposure() or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _event_current_exposure(self, event_key: str) -> float:
        table_manager = self._table_manager()
        if table_manager and hasattr(table_manager, "find_by_event_key"):
            try:
                table = table_manager.find_by_event_key(event_key)
                if table:
                    return float(getattr(table, "current_exposure", 0.0) or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _bankroll_current(self) -> float:
        risk_desk = self._risk_desk()
        if risk_desk:
            return float(getattr(risk_desk, "bankroll_current", 0.0) or 0.0)
        return 0.0

    def _compute_order_exposure(self, item: Dict[str, Any]) -> float:
        side = str(item.get("side", "BACK")).upper()
        stake = float(item.get("stake", 0.0) or 0.0)
        price = float(item.get("price", 0.0) or 0.0)

        if side == "LAY":
            if "liability" in item:
                return max(0.0, float(item.get("liability", 0.0) or 0.0))
            return max(0.0, stake * max(0.0, price - 1.0))

        return max(0.0, stake)

    def _compute_batch_exposure(self, results: List[Dict[str, Any]]) -> float:
        return sum(self._compute_order_exposure(item) for item in results)

    def _allocate_table(
        self,
        event_key: str,
        batch_exposure: float,
        meta: Dict[str, Any],
    ) -> Optional[int]:
        table_manager = self._table_manager()
        config = self._config()

        if table_manager is None:
            return None

        allow_recovery = bool(getattr(config, "allow_recovery", True)) if config else True

        table = None
        if hasattr(table_manager, "allocate"):
            table = table_manager.allocate(event_key=event_key, allow_recovery=allow_recovery)

        if table is None:
            return None

        if hasattr(table_manager, "activate"):
            table_manager.activate(
                table_id=table.table_id,
                event_key=event_key,
                exposure=float(batch_exposure),
                market_id=str(meta.get("market_id") or ""),
                selection_id=None,
                meta=meta,
            )

        return int(table.table_id)

    def _release_table_and_key(self, table_id: Optional[int], event_key: str) -> None:
        duplication_guard = self._duplication_guard()
        table_manager = self._table_manager()

        if duplication_guard and event_key:
            try:
                duplication_guard.release(event_key)
            except Exception:
                logger.exception("Errore release duplication key")

        if table_manager and table_id:
            try:
                if hasattr(table_manager, "force_unlock"):
                    table_manager.force_unlock(int(table_id))
            except Exception:
                logger.exception("Errore force_unlock table")

    def _runtime_active(self) -> bool:
        mode = self._mode()
        return bool(mode and str(getattr(mode, "value", mode)) == "ACTIVE")

    def _bus_available(self) -> bool:
        return self.bus is not None and hasattr(self.bus, "publish")

    def _normalize_side(self, value: Any) -> str:
        side = str(value or "BACK").upper().strip()
        return side if side in {"BACK", "LAY"} else "BACK"

    def _resolve_selection_side(self, selection: Dict[str, Any]) -> str:
        raw = (selection or {}).get("side") or (selection or {}).get("effectiveType") or "BACK"
        return self._normalize_side(raw)

    def _resolve_commission_pct(self, payload: Dict[str, Any]) -> float:
        if "commission" in payload:
            try:
                return max(0.0, float(payload.get("commission", 4.5) or 0.0))
            except Exception:
                return 4.5
        return 4.5

    def _calculate_dutching_with_commission(
        self, payload: Dict[str, Any]
    ) -> tuple[List[Dict[str, Any]], float, float, float]:
        normalized_selections: List[Dict[str, Any]] = []
        for selection in list(payload.get("selections") or []):
            item = dict(selection or {})
            item["side"] = self._resolve_selection_side(item)
            normalized_selections.append(item)

        commission_pct = self._resolve_commission_pct(payload)
        try:
            calc_out = calculate_dutching(
                normalized_selections,
                float(payload["total_stake"]),
                commission=commission_pct,
            )
        except TypeError:
            # Compat path for test doubles/legacy callables without commission argument.
            calc_out = calculate_dutching(
                normalized_selections,
                float(payload["total_stake"]),
            )

        if isinstance(calc_out, tuple) and len(calc_out) >= 4:
            results, avg_profit, book_pct, avg_net_profit = calc_out[:4]
        elif isinstance(calc_out, tuple) and len(calc_out) == 3:
            results, avg_profit, book_pct = calc_out
            avg_net_profit = float(avg_profit)
        else:
            raise ValueError("Formato output calculate_dutching non valido")

        return (
            results,
            float(avg_profit),
            float(book_pct),
            float(avg_net_profit),
        )

    def _dutching_model(self, results: List[Dict[str, Any]]) -> str:
        sides = {
            self._normalize_side(item.get("side", "BACK"))
            for item in (results or [])
            if isinstance(item, dict)
        }
        if len(sides) == 1:
            side = next(iter(sides))
            return f"{side}_EQUAL_PROFIT_FIXED_TOTAL_STAKE"
        return "UNSPECIFIED"

    def _lay_liability_metrics(self, results: List[Dict[str, Any]]) -> Dict[str, float]:
        liabilities: List[float] = []
        for item in results or []:
            side = self._normalize_side((item or {}).get("side", "BACK"))
            if side != "LAY":
                continue
            try:
                liabilities.append(max(0.0, float((item or {}).get("liability", 0.0) or 0.0)))
            except Exception:
                continue
        if not liabilities:
            return {
                "lay_total_liability": 0.0,
                "lay_worst_case_liability": 0.0,
            }
        return {
            "lay_total_liability": float(sum(liabilities)),
            "lay_worst_case_liability": float(max(liabilities)),
        }

    def _min_net_profit(self, results: List[Dict[str, Any]], avg_net_profit: float) -> float:
        net_values: List[float] = []
        for item in results:
            if "profitIfWinsNet" not in item:
                continue
            try:
                net_values.append(float(item.get("profitIfWinsNet", 0.0) or 0.0))
            except Exception:
                continue
        if net_values and len(net_values) == len(results):
            return min(net_values)
        return float(avg_net_profit)

    def _batch_manager_create(
        self,
        batch_id: str,
        event_key: str,
        payload: Dict[str, Any],
        orders: List[Dict[str, Any]],
    ) -> None:
        batch_manager = self._batch_manager()
        if batch_manager is None:
            return

        if hasattr(batch_manager, "create_batch"):
            batch_manager.create_batch(
                batch_id=batch_id,
                event_key=event_key,
                market_id=str(payload.get("market_id") or ""),
                legs=[
                    {
                        "selectionId": int(o["selection_id"]),
                        "price": float(o["price"]),
                        "stake": float(o["stake"]),
                        "side": str(o["bet_type"]).upper(),
                    }
                    for o in orders
                ],
            )

    def _batch_manager_mark_failed(self, batch_id: str, error: str) -> None:
        batch_manager = self._batch_manager()
        if batch_manager is None:
            return

        if hasattr(batch_manager, "mark_batch_failed"):
            batch_manager.mark_batch_failed(batch_id=batch_id, error=error)
            return

        if hasattr(batch_manager, "fail_batch"):
            batch_manager.fail_batch(batch_id=batch_id, error=error)

    # =========================================================
    # VALIDAZIONE
    # =========================================================
    def validate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(payload, dict):
                return self._fail("Payload non valido")

            market_id = payload.get("market_id")
            selections = payload.get("selections", [])
            total_stake = float(payload.get("total_stake", 0) or 0)

            if not market_id:
                return self._fail("market_id mancante")

            if not isinstance(selections, list) or not selections:
                return self._fail("Nessuna selezione")

            seen_selection_ids = set()
            # Floor di strategia editabile (default 1.02), SOPRA il minimo Betfair
            # 1.01 hard-coded piu' sotto. Fail-safe: mai < 1.01.
            min_price = self._min_price(self._config())

            for idx, selection in enumerate(selections, start=1):
                if not isinstance(selection, dict):
                    return self._fail(f"Selezione #{idx} non valida")

                if "selectionId" not in selection:
                    return self._fail(f"selectionId mancante alla selezione #{idx}")

                if "price" not in selection:
                    return self._fail(f"price mancante alla selezione #{idx}")

                try:
                    selection_id = int(selection["selectionId"])
                except Exception:
                    return self._fail(f"selectionId non valido alla selezione #{idx}")

                if selection_id in seen_selection_ids:
                    return self._fail(f"selectionId duplicato: {selection_id}")
                seen_selection_ids.add(selection_id)

                try:
                    price = float(selection["price"])
                except Exception:
                    return self._fail(f"price non valido alla selezione #{idx}")

                if price <= 1.01:
                    return self._fail(f"Quota non valida alla selezione #{idx}: {price}")

                if price < min_price:
                    return self._fail(
                        f"Quota {price} sotto il minimo configurato ({min_price}) alla selezione #{idx}"
                    )

                if "side" in selection:
                    side = self._resolve_selection_side(selection)
                    if side not in {"BACK", "LAY"}:
                        return self._fail(f"side non valido alla selezione #{idx}: {side}")

            if total_stake <= 0:
                return self._fail("total_stake non valido")

            return self._ok()
        except Exception as exc:
            return self._fail(str(exc))

    # =========================================================
    # PREVIEW / DRY RUN
    # =========================================================
    def preview(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            validation = self.validate(payload)
            if not validation["ok"]:
                return validation

            results, avg_profit, book_pct, avg_net_profit = (
                self._calculate_dutching_with_commission(payload)
            )

            if not isinstance(results, list):
                return self._fail("Risultato dutching non valido")

            event_key = self._build_event_key(payload, results)
            batch_id = self._build_batch_id(payload, results)
            batch_exposure = self._compute_batch_exposure(results)
            min_net_profit = self._min_net_profit(results, avg_net_profit)

            return self._ok(
                dry_run=True,
                preflight=False,
                results=results,
                avg_profit=float(avg_profit),
                avg_profit_net=float(avg_net_profit),
                avg_profit_semantics="gross",
                book_pct=float(book_pct),
                event_key=event_key,
                batch_id=batch_id,
                batch_exposure=round(batch_exposure, 2),
                commission_pct=self._resolve_commission_pct(payload),
                profitable_net=bool(float(min_net_profit) > 0.0),
                dutching_model=self._dutching_model(results),
                **self._lay_liability_metrics(results),
            )
        except Exception as exc:
            logger.exception("Errore preview dutching")
            return self._fail(str(exc))

    # =========================================================
    # PRECHECK RISCHIO / DUPLICATI
    # =========================================================
    def precheck(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        validation = self.validate(payload)
        if not validation["ok"]:
            return validation

        if not self._runtime_active():
            return self._fail("Runtime non attivo")

        try:
            results, avg_profit, book_pct, avg_net_profit = (
                self._calculate_dutching_with_commission(payload)
            )
        except Exception as exc:
            logger.exception("Errore calculate_dutching in precheck")
            return self._fail(str(exc))

        if not results:
            return self._fail("Dutching vuoto")

        event_key = self._build_event_key(payload, results)
        batch_id = self._build_batch_id(payload, results)
        batch_exposure = self._compute_batch_exposure(results)
        min_net_profit = self._min_net_profit(results, avg_net_profit)

        self._cleanup_batches()
        if batch_id in self._recent_batches:
            return self._fail(
                "Batch già inviato (idempotency guard)",
                batch_id=batch_id,
            )

        duplication_guard = self._duplication_guard()
        config = self._config()
        bankroll = self._bankroll_current()
        current_total_exposure = self._table_total_exposure()
        event_current_exposure = self._event_current_exposure(event_key)

        # Book % guard (PR2a): blocca il submit se il book totale del dutching
        # (over-round) raggiunge/supera la soglia di blocco configurata. Gate
        # reale sul percorso di piazzamento, eseguito PRIMA di ogni side-effect
        # (duplication acquire). Fonte-dato: RoserpinaConfig (editabile da GUI),
        # con fallback fail-safe a trading_config.BOOK_BLOCK.
        book_block = self._book_block_threshold(config)
        if float(book_pct) >= book_block:
            return self._fail(
                f"Book troppo alto: {float(book_pct):.2f}% >= soglia di blocco {book_block:.2f}%",
                book_pct=round(float(book_pct), 2),
                book_block=round(book_block, 2),
            )

        # Liquidity guard (#383) — BLOCCO reale (opt-in): calcola lo shortfall di
        # liquidita' eseguibile sul book REALE (lato Betfair-standard: BACK ->
        # availableToBack, LAY -> availableToLay) e, se il guard NON e' in sola
        # osservazione (liquidity_warning_only=False), BLOCCA il submit PRIMA di
        # ogni side-effect (duplication acquire), come il book% gate. In modalita'
        # avviso (default opt-in) segnala soltanto (liquidity_warning/shortfall nel
        # risultato). FAIL-OPEN su dato mancante (cache fredda => nessun blocco).
        liquidity = self._evaluate_liquidity_guard(config, payload, results)
        if liquidity.get("shortfall") and not self._liquidity_warning_only(config):
            # Segnale di blocco AUTOREVOLE: ok=False. NON si aggiunge
            # liquidity_warning (che nel path _ok indica la sola-osservazione):
            # metterlo su un fail e' ambiguo per i consumer money-management
            # (potrebbero leggerlo come "avviso procedibile"). Il blocco espone lo
            # shortfall per diagnostica; ok=False prevale sempre.
            return self._fail(
                "Liquidità insufficiente sul book per una o più gambe",
                liquidity_shortfall=liquidity.get("shortfall", []),
            )

        # MAX_WIN cap (G5) — enforce-first opt-in: blocca il submit se l'importo
        # potenziale (BACK: payout stake*quota; LAY: liability stake*(quota-1),
        # cioe' il RISCHIO) di UNA gamba supera il cap configurato, PRIMA di ogni
        # side-effect (duplication acquire), come il
        # book%/liquidity gate. Per-gamba (esiti dutching mutuamente esclusivi =>
        # si valuta la MAX, non la somma). In modalita' avviso (default opt-in) solo
        # segnalazione (max_win_warning/max_win_breaches nel risultato, sotto).
        # FAIL-SAFE: config rotta ricade su trading_config.MAX_WIN (cap sempre armato).
        max_win = self._max_win(config)
        max_win_breaches = self._max_win_breaches(results, max_win)
        if max_win_breaches and not self._max_win_warning_only(config):
            return self._fail(
                f"Vincita potenziale oltre il cap {max_win:.2f}€ su una o più gambe",
                max_win=round(max_win, 2),
                max_win_breaches=max_win_breaches,
            )

        if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
            try:
                if not duplication_guard.acquire(event_key):
                    return self._fail("Duplicato bloccato", event_key=event_key)
            except Exception:
                logger.exception("Errore duplication_guard.acquire")

        # MAX_STAKE_PCT (G5) — WARNING opt-in NON-bloccante: segnala se l'esposizione
        # reale di QUESTA operazione (batch_exposure = BACK stake / LAY liability)
        # supera una frazione del bankroll (default 30%). Distinto dai gate cumulativi
        # (max_total_exposure e' il BLOCCO cumulativo al 35%): qui si misura la sola
        # operazione, e non si BLOCCA mai. FAIL-OPEN: bankroll <= 0 => nessun warning.
        max_stake_pct = self._max_stake_pct(config)
        stake_pct_ratio = (batch_exposure / bankroll) if bankroll > 0 else 0.0
        stake_pct_warning = bool(
            bankroll > 0 and batch_exposure > bankroll * max_stake_pct + 1e-9
        )

        # PROFIT_EPSILON (G5) — WARNING opt-in NON-bloccante: segnala se la varianza
        # di profitto NETTO tra gli esiti equalizzati (max - min su profitIfWinsNet)
        # supera la tolleranza configurata (default €0.50). Uno spread residuo alto
        # dopo l'equalizzazione tradisce la premessa "profitto garantito uguale"
        # (l'operatore vince sensibilmente di piu' su certi esiti). Non BLOCCA MAI.
        # FAIL-OPEN: netti incompleti / < 2 esiti => nessun avviso. FAIL-SAFE: config
        # rotta => trading_config.PROFIT_EPSILON. Il controller usa sempre equalize=ON
        # (calculate_dutching default), quindi lo spread e' quello post-equalizzazione.
        profit_epsilon_enabled = self._profit_epsilon_enabled(config)
        profit_epsilon = self._profit_epsilon(config)
        profit_spread = self._profit_spread_net(results)
        # L'avviso scatta SOLO se abilitato dalla GUI (default on): disattivandolo
        # l'owner silenzia l'avviso senza toccare la soglia. profit_spread resta
        # comunque esposto (informativo, non-bloccante).
        profit_epsilon_warning = bool(
            profit_epsilon_enabled and profit_spread is not None and profit_spread > profit_epsilon + 1e-9
        )

        if bankroll > 0 and config is not None:
            max_total_exposure = bankroll * (
                float(getattr(config, "max_total_exposure_pct", 35.0)) / 100.0
            )
            max_event_exposure = bankroll * (
                float(getattr(config, "max_event_exposure_pct", 18.0)) / 100.0
            )
            max_single_bet = bankroll * (
                float(getattr(config, "max_single_bet_pct", 18.0)) / 100.0
            )

            if current_total_exposure + batch_exposure > max_total_exposure + 1e-9:
                return self._fail(
                    "Esposizione globale oltre limite",
                    batch_exposure=round(batch_exposure, 2),
                    current_total_exposure=round(current_total_exposure, 2),
                    max_total_exposure=round(max_total_exposure, 2),
                )

            if event_current_exposure + batch_exposure > max_event_exposure + 1e-9:
                return self._fail(
                    "Esposizione evento oltre limite",
                    batch_exposure=round(batch_exposure, 2),
                    event_current_exposure=round(event_current_exposure, 2),
                    max_event_exposure=round(max_event_exposure, 2),
                )

            too_large = [
                {
                    "selectionId": int(item["selectionId"]),
                    "stake": round(float(item["stake"]), 2),
                    "limit": round(max_single_bet, 2),
                }
                for item in results
                if self._compute_order_exposure(item) > max_single_bet + 1e-9
            ]
            if too_large:
                return self._fail(
                    "Una o più gambe superano max_single_bet",
                    violations=too_large,
                )

        return self._ok(
            preflight=True,
            dry_run=False,
            results=results,
            avg_profit=float(avg_profit),
            avg_profit_net=float(avg_net_profit),
            avg_profit_semantics="gross",
            book_pct=float(book_pct),
            book_warning=round(self._book_warning_threshold(config), 2),
            book_warning_exceeded=bool(float(book_pct) >= self._book_warning_threshold(config)),
            liquidity_warning=bool(liquidity.get("warning")),
            liquidity_shortfall=liquidity.get("shortfall", []),
            max_win=round(max_win, 2),
            max_win_warning=bool(max_win_breaches),
            max_win_breaches=max_win_breaches,
            max_stake_pct=round(max_stake_pct, 4),
            stake_pct_warning=stake_pct_warning,
            stake_pct_ratio=round(stake_pct_ratio, 4),
            profit_epsilon=round(profit_epsilon, 2),
            profit_epsilon_enabled=profit_epsilon_enabled,
            profit_spread=(round(float(profit_spread), 2) if profit_spread is not None else None),
            profit_epsilon_warning=profit_epsilon_warning,
            event_key=event_key,
            batch_id=batch_id,
            batch_exposure=float(batch_exposure),
            commission_pct=self._resolve_commission_pct(payload),
            profitable_net=bool(float(min_net_profit) > 0.0),
            dutching_model=self._dutching_model(results),
            **self._lay_liability_metrics(results),
        )

    # =========================================================
    # API FINALE
    # =========================================================
    def submit_dutching(
        self,
        payload: Dict[str, Any],
        dry_run: bool = False,
        preflight: bool = False,
    ) -> Dict[str, Any]:
        """
        API finale pubblica stabile.

        Path:
        - dry_run=True  -> preview
        - preflight=True -> precheck
        - default -> execute reale
        """
        if dry_run:
            out = self.preview(payload)
            out.setdefault("dry_run", True)
            out.setdefault("preflight", False)
            return out

        if preflight:
            out = self.precheck(payload)
            out.setdefault("dry_run", False)
            out.setdefault("preflight", True)
            return out

        return self._execute_impl(payload)

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.submit_dutching(payload, dry_run=False, preflight=False)

    # =========================================================
    # EXECUTE
    # =========================================================
    def _execute_impl(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        pre = self.precheck(payload)
        if not pre["ok"]:
            self._publish_audit(
                "DUTCHING_BATCH_REJECTED",
                {
                    "payload": payload,
                    "reason": pre["error"],
                },
            )
            pre.setdefault("dry_run", False)
            pre.setdefault("preflight", False)
            return pre

        if not self._bus_available():
            return self._fail(
                "EventBus non disponibile",
                dry_run=False,
                preflight=False,
                batch_id=pre.get("batch_id"),
                event_key=pre.get("event_key"),
            )

        results: List[Dict[str, Any]] = pre["results"]
        avg_profit = pre["avg_profit"]
        book_pct = pre["book_pct"]
        event_key = pre["event_key"]
        batch_id = pre["batch_id"]
        batch_exposure = float(pre["batch_exposure"] or 0.0)

        duplication_guard = self._duplication_guard()
        table_id = payload.get("table_id")
        allocated_here = False

        if not table_id:
            table_id = self._allocate_table(
                event_key=event_key,
                batch_exposure=batch_exposure,
                meta={
                    "market_id": payload.get("market_id"),
                    "event_name": payload.get("event_name", ""),
                    "market_name": payload.get("market_name", ""),
                    "type": "dutching_batch",
                    "batch_id": batch_id,
                },
            )
            allocated_here = table_id is not None

        if table_id is None and self._table_manager() is not None:
            msg = "Nessun tavolo disponibile per batch dutching"
            self._publish_audit("DUTCHING_BATCH_REJECTED", {"payload": payload, "reason": msg})
            return self._fail(
                msg,
                dry_run=False,
                preflight=False,
                batch_id=batch_id,
                event_key=event_key,
            )

        orders = []
        published_orders = []
        batch_created = False

        try:
            for idx, item in enumerate(results, start=1):
                order = {
                    "market_id": str(payload["market_id"]),
                    "selection_id": int(item["selectionId"]),
                    "bet_type": str(item.get("side", "BACK")).upper(),
                    "price": float(item["price"]),
                    "stake": float(item["stake"]),
                    "event_name": payload.get("event_name", ""),
                    "market_name": payload.get("market_name", ""),
                    "runner_name": item.get("runnerName", ""),
                    "simulation_mode": bool(payload.get("simulation_mode", False)),
                    "table_id": table_id,
                    "event_key": event_key,
                    "batch_id": batch_id,
                    "batch_size": len(results),
                    "batch_leg_index": idx,
                    "batch_avg_profit": float(avg_profit),
                    "batch_book_pct": float(book_pct),
                    "batch_exposure": float(batch_exposure),
                }
                orders.append(order)

            self._batch_manager_create(batch_id, event_key, payload, orders)
            batch_created = True

            self._publish_audit(
                "DUTCHING_BATCH_APPROVED",
                {
                    "batch_id": batch_id,
                    "event_key": event_key,
                    "table_id": table_id,
                    "count": len(orders),
                    "avg_profit": avg_profit,
                    "book_pct": book_pct,
                    "batch_exposure": round(batch_exposure, 2),
                    "payload": payload,
                },
            )

            for order in orders:
                self.bus.publish("CMD_QUICK_BET", order)
                published_orders.append(order)

            self._recent_batches[batch_id] = time.time()

            return self._ok(
                dry_run=False,
                preflight=False,
                status="SUBMITTED",
                batch_id=batch_id,
                event_key=event_key,
                table_id=table_id,
                orders=orders,
                published_count=len(published_orders),
                count=len(orders),
                avg_profit=float(avg_profit),
                book_pct=float(book_pct),
                batch_exposure=round(batch_exposure, 2),
            )

        except Exception as exc:
            logger.exception("Errore execute dutching batch")

            self._publish_audit(
                "DUTCHING_BATCH_PARTIAL_FAILURE",
                {
                    "batch_id": batch_id,
                    "event_key": event_key,
                    "table_id": table_id,
                    "published_count": len(published_orders),
                    "total_count": len(orders),
                    "error": str(exc),
                },
            )

            if batch_created:
                self._batch_manager_mark_failed(batch_id=batch_id, error=str(exc))

            if allocated_here:
                self._release_table_and_key(table_id, event_key)
            elif duplication_guard:
                try:
                    duplication_guard.release(event_key)
                except Exception:
                    logger.exception("Errore release duplication key after failure")

            return self._fail(
                str(exc),
                dry_run=False,
                preflight=False,
                batch_id=batch_id,
                event_key=event_key,
                table_id=table_id,
                published_count=len(published_orders),
                total_count=len(orders),
            )

    # =========================================================
    # MANUAL BET
    # =========================================================
    def manual_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            required = ["market_id", "selection_id", "price", "stake"]
            for key in required:
                if key not in payload:
                    return self._fail(f"{key} mancante")

            if not self._runtime_active():
                return self._fail("Runtime non attivo")

            if not self._bus_available():
                return self._fail("EventBus non disponibile")

            market_id = str(payload["market_id"])
            selection_id = int(payload["selection_id"])
            price = float(payload["price"])
            stake = float(payload["stake"])

            if price <= 1.01:
                return self._fail("Quota non valida")
            if stake <= 0:
                return self._fail("Stake non valido")

            event_key = str(payload.get("event_key") or f"manual_{market_id}_{selection_id}")
            duplication_guard = self._duplication_guard()
            config = self._config()

            # MAX_WIN cap (G5) anche sul bet manuale (chiude la via non-gated,
            # anti fat-finger), PRIMA di ogni side-effect. Importo potenziale: BACK
            # = payout stake*quota; LAY = liability stake*(quota-1) (il RISCHIO reale).
            # Enforce-first opt-in: in modalita' avviso (default) non blocca.
            # FAIL-SAFE su trading_config.MAX_WIN.
            side = str(self._normalize_side(payload.get("bet_type", "BACK"))).upper()
            potential_win = self._leg_potential_win({"side": side, "stake": stake, "price": price})
            max_win = self._max_win(config)
            max_win_exceeded = potential_win > max_win + 1e-9
            if max_win_exceeded and not self._max_win_warning_only(config):
                return self._fail(
                    f"Vincita potenziale {potential_win:.2f}€ oltre il cap {max_win:.2f}€",
                    max_win=round(max_win, 2),
                    potential_win=round(potential_win, 2),
                )
            if max_win_exceeded:
                # Modalita' avviso: NON blocca ma rende OSSERVABILE lo sforamento
                # (log + flag nel risultato _ok, come precheck), cosi' l'operatore
                # vede il superamento del cap anche prima di armare il blocco.
                logger.warning(
                    "MAX_WIN warning (manual_bet): vincita potenziale %.2f€ oltre il cap %.2f€ (sel %s)",
                    potential_win,
                    max_win,
                    selection_id,
                )

            if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
                if not duplication_guard.acquire(event_key):
                    return self._fail("Duplicato bloccato")

            bankroll = self._bankroll_current()
            if bankroll > 0 and config is not None:
                exposure = stake
                current_total_exposure = self._table_total_exposure()
                max_total_exposure = bankroll * (
                    float(getattr(config, "max_total_exposure_pct", 35.0)) / 100.0
                )
                max_single_bet = bankroll * (
                    float(getattr(config, "max_single_bet_pct", 18.0)) / 100.0
                )

                if exposure > max_single_bet + 1e-9:
                    return self._fail("Stake oltre max_single_bet")

                if current_total_exposure + exposure > max_total_exposure + 1e-9:
                    return self._fail("Esposizione globale oltre limite")

            order = {
                "market_id": market_id,
                "selection_id": selection_id,
                "bet_type": self._normalize_side(payload.get("bet_type", "BACK")),
                "price": price,
                "stake": stake,
                "event_name": payload.get("event_name", ""),
                "market_name": payload.get("market_name", ""),
                "runner_name": payload.get("runner_name", ""),
                "simulation_mode": bool(payload.get("simulation_mode", False)),
                "table_id": payload.get("table_id"),
                "event_key": event_key,
            }

            try:
                self.bus.publish("CMD_QUICK_BET", order)
            except Exception:
                if duplication_guard:
                    duplication_guard.release(event_key)
                raise

            self._publish_audit("MANUAL_BET_APPROVED", {"order": order})
            return self._ok(
                order=order,
                max_win=round(max_win, 2),
                max_win_warning=bool(max_win_exceeded),
                potential_win=round(potential_win, 2),
            )

        except Exception as exc:
            logger.exception("Errore manual_bet")
            return self._fail(str(exc))

    # =========================================================
    # SOFT CHECK
    # =========================================================
    def check_duplicate(self, payload: Dict[str, Any]) -> bool:
        try:
            pre = self.preview(payload)
            if not pre.get("ok"):
                return False
            event_key = pre.get("event_key", "")
            duplication_guard = self._duplication_guard()
            if duplication_guard and event_key:
                return bool(duplication_guard.is_duplicate(event_key))
            return False
        except Exception:
            return False
