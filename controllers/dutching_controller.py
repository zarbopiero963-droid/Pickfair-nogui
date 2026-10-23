from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional

import trading_config
from core import liquidity
from core import validators
from core.order_identity import (
    derive_customer_ref,
    new_operation_customer_ref,
    normalize_upstream_customer_ref,
)

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
            side = _selection_side(selection)
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

# PFC-008 (M019): cap percentuali obbligatori nei controlli di esposizione del Dutching.
_DUTCHING_PCT_FIELDS = ("max_single_bet_pct", "max_total_exposure_pct", "max_event_exposure_pct")


def _side(value: Any) -> str:
    """#523: lato BACK/LAY esplicito, con lo stesso contratto dei segnali (`validators.signal_side`).
    Assente o non valido => ValueError("INVALID_SIDE"): un ordine non diventa mai BACK per default."""
    return validators.signal_side({"side": value})


def _selection_side(selection: Dict[str, Any]) -> str:
    """#523 / #529 ciclo A5: lato di una selezione con la regola unica `validators.leg_side` (la stessa del
    RiskMiddleware): ogni campo di lato presente (`side`, `effectiveType`, `bet_type`) deve essere BACK/LAY,
    campi discordanti o vuoti => INVALID_SIDE. Il controller non ha un lato di lotto: nessun default."""
    return validators.leg_side(selection)


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
        self._bus_attached = False
        self._attach_lock = threading.Lock()

    # =========================================================
    # CORSIA BUS (piano «dutching agganciato»)
    # =========================================================
    def attach_bus_consumer(self) -> "DutchingController":
        """Aggancia l'esecutore della corsia dutching:
        ``CMD_PLACE_DUTCHING`` -> ``submit_dutching``.

        Chiamata ESPLICITA dagli entrypoint, non in ``__init__``: i
        costruttori esistenti (test, tooling, usi diretti dell'API) non
        devono guadagnare sottoscrizioni bus in silenzio. Ritorna ``self``
        per il wiring in una riga. Bus assente => solleva (fail-closed:
        una corsia che sembra agganciata ma non lo e' sarebbe peggio).
        """
        if self.bus is None or not hasattr(self.bus, "subscribe"):
            raise RuntimeError(
                "DutchingController senza bus: corsia CMD_PLACE_DUTCHING "
                "non agganciabile"
            )
        # Idempotente e ATOMICO (R1+R2 #442, GPT-5.6+Fable): due attach —
        # anche CONCORRENTI — non devono mai produrre due handler (= gambe
        # piazzate due volte). Sull'app reale il doppio attach non avviene
        # (build() e' guardato da _built e ogni build crea un EventBus
        # nuovo), ma il contratto lo garantisce comunque, qualunque sia il
        # chiamante: guardia e subscribe stanno nella stessa sezione
        # critica (niente TOCTOU).
        with self._attach_lock:
            if self._bus_attached:
                return self
            self.bus.subscribe(
                "CMD_PLACE_DUTCHING", self._handle_cmd_place_dutching
            )
            self._bus_attached = True
        return self

    def _handle_cmd_place_dutching(self, payload: Dict[str, Any]) -> None:
        """Consumer bus della corsia dutching. Non solleva MAI nel worker.

        Ogni esito — incluso il rigetto del precheck — e' gia' un dict
        auditato da ``submit_dutching`` (``DUTCHING_BATCH_REJECTED`` /
        lifecycle del batch); un payload non-dict degrada a ``{}`` e viene
        rigettato dal precheck (fail-closed, nessun ordine parziale: il
        fallimento avviene PRIMA della publish delle gambe).
        """
        try:
            esito = self.submit_dutching(self._payload_da_cmd(payload))
            if not esito.get("ok"):
                logger.warning(
                    "[DutchingController] CMD_PLACE_DUTCHING rigettato: %s",
                    esito.get("error"),
                )
        except Exception:
            logger.exception(
                "[DutchingController] Errore inatteso su CMD_PLACE_DUTCHING"
            )

    @staticmethod
    def _payload_da_cmd(payload: Any) -> Dict[str, Any]:
        """Adatta il payload ``CMD_PLACE_DUTCHING`` (shape del RiskMiddleware:
        gambe in ``results``) al contratto di ``submit_dutching`` (gambe in
        ``selections``).

        Decisione di sicurezza: gli ``stake`` per-gamba eventualmente
        presenti sul wire vengono IGNORATI — la fonte di verita' degli
        stake e' il RICALCOLO del controller (equal-profit + gates
        Roserpina) a partire da ``total_stake``, mai numeri precomputati
        arrivati dal bus. Payload non-dict => ``{}`` (rigettato dal
        validate, fail-closed).
        """
        if not isinstance(payload, dict):
            return {}
        # NESSUN passthrough (R1 #442, Fable): anche un payload gia' in
        # shape `selections` viene ri-normalizzato alla whitelist — cosi'
        # uno `stake` per-gamba sul wire non raggiunge MAI il controller,
        # da qualunque ramo arrivi.
        gambe = payload.get("selections") or payload.get("results") or []
        adattato = {
            k: v for k, v in payload.items() if k not in ("results", "selections")
        }
        adattato["selections"] = [
            dict(
                {
                    "selectionId": r.get("selectionId"),
                    "price": r.get("price"),
                    "runnerName": r.get("runnerName", ""),
                },
                # #523 / #529 ciclo A5: solo i campi di lato PRESENTI (anche vuoti) viaggiano, cosi' il
                # controller distingue «assente» da «presente ma vuoto» (validators.leg_side).
                **{k: r[k] for k in validators.LEG_SIDE_FIELDS if k in r},
            )
            for r in gambe
            if isinstance(r, dict)
        ]
        return adattato

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
                    "side": _side(item.get("side")),
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
    def _absolute_cap_reason(self, orders: List[Dict[str, Any]]) -> str:
        """Cap assoluti dal RuntimeController (stessa logica dei segnali). Senza runtime: nessun cap da applicare
        qui (la config Roserpina vive nel runtime). Errore del controllo => rifiuto (fail-closed)."""
        checker = getattr(getattr(self, "runtime", None), "absolute_cap_reason_for_orders", None)
        if not callable(checker):
            return ""
        try:
            reason = checker(orders)
        except Exception:
            logger.exception("Controllo cap assoluti fallito: ordine bloccato")
            return "cap_assoluti_non_verificabili"
        if reason is None or isinstance(reason, str):
            return reason or ""
        logger.error("Controllo cap assoluti ha restituito %r invece di str: ordine bloccato", reason)
        return "cap_assoluti_non_verificabili"

    @staticmethod
    def _max_win(config) -> Optional[float]:
        """Cap vincita/payout per gamba dalla config (editabile GUI), OPZIONALE.

        Rettifica owner 18/10/2026 (chiarimento definitivo 19/10): None/assente = non attivo, nessun
        default numerico. Presente ma non numerico, non finito o <= 0 => ``ValueError``: il chiamante
        blocca l'ordine (fail-closed), mai "cap disattivato" per un valore corrotto.
        """
        raw = getattr(config, "max_win", None)
        if raw is None:
            return None
        if isinstance(raw, bool):
            raise ValueError("max_win non valido")
        try:
            val = float(raw)
        except (TypeError, ValueError):
            raise ValueError("max_win non valido") from None
        if not math.isfinite(val) or val <= 0.0:
            raise ValueError("max_win non valido")
        return val

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
        side = _side(item.get("side"))
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
            # una liability stale/corrotta — rilievo GPT-5.6 Sol + Fable 5 su #393).
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
        book non e' in cache => None: in sola osservazione nessun blocco; con il guard
        armato liquidita' IGNOTA, blocco LIQUIDITY_UNKNOWN (ciclo 11 #512).
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
        """Delega a ``core.liquidity`` (PF-ALL-F030: una sola formula per dutching e ingressi singoli)."""
        return liquidity.liquidity_crosses(side, order_price, book_price)

    @classmethod
    def _sum_executable_liquidity(cls, ladder, side, order_price) -> float:
        return liquidity.sum_executable_liquidity(ladder, side, order_price)

    @classmethod
    def _available_liquidity(cls, book, selection_id, side, price):
        """Liquidita' ESEGUIBILE per una gamba dal book REALE (lato Betfair-standard #383); None se ignota."""
        return liquidity.executable_liquidity(book, selection_id, side, price)

    @classmethod
    def _leg_liquidity_shortfall(cls, book, item, multiplier, min_abs, *, warning_only=True):
        """Shortfall di liquidita' per una gamba, o None se ok (o ignota in sola osservazione).

        required = stake * multiplier: la size da matchare sul lato opposto e' lo
        STAKE (le size del book sono backer-stake), NON la liability. Confronto
        omogeneo (size vs size).
        Liquidita' IGNOTA (ladder malformata, selezione assente dal book): in
        modalita' blocco (`warning_only=False`) e' uno shortfall esplicito
        `LIQUIDITY_UNKNOWN` con `available=None` (fail-closed: un dato mancante
        non vale "liquidita' sufficiente"; rilievo Codex #512 su 034af59); in sola
        osservazione resta nessuna osservazione, come prima.
        """
        try:
            selection_id = int(item.get("selectionId"))
            stake = float(item.get("stake", 0.0) or 0.0)
        except (TypeError, ValueError):
            return None
        if stake <= 0.0:
            return None
        side = _side(item.get("side"))
        try:
            price = float(item.get("price", 1.0) or 1.0)
        except (TypeError, ValueError):
            price = 1.0
        available = cls._available_liquidity(book, selection_id, side, price)
        required = float(stake) * float(multiplier)
        if available is None:
            if warning_only:
                return None  # dato mancante => nessuna osservazione (sola osservazione)
            return {
                "selectionId": selection_id,
                "available": None,
                "required": round(float(required), 2),
                "min_absolute": round(float(min_abs), 2),
                "reason": "LIQUIDITY_UNKNOWN",
            }
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
        in sola osservazione nessuno shortfall, con il guard in blocco shortfall
        `LIQUIDITY_UNKNOWN` per ogni gamba (fail-closed, rilievo Codex r4309039358).
        """
        out: Dict[str, Any] = {"warning": False, "shortfall": []}
        if not bool(getattr(config, "liquidity_guard_enabled", True)):
            return out
        multiplier = self._book_threshold(config, "liquidity_multiplier", trading_config.LIQUIDITY_MULTIPLIER)
        min_abs = self._min_liquidity_absolute(config)
        book = self._market_book((payload or {}).get("market_id"))
        warning_only = self._liquidity_warning_only(config)
        if book is None:
            if warning_only:
                return out  # sola osservazione: nessun book in cache => nessuna osservazione
            # Guard armato in blocco: un book assente e' liquidita' IGNOTA per ogni gamba, mai "sufficiente"
            # (rilievo Codex r4309039358; stesso contratto del percorso singolo LIQUIDITY_BOOK_UNAVAILABLE).
            unknown = []
            for item in (results or []):
                try:
                    selection_id = int(item.get("selectionId"))
                    stake = float(item.get("stake", 0.0) or 0.0)
                except (TypeError, ValueError, AttributeError):
                    continue
                if stake <= 0.0:
                    continue
                unknown.append({"selectionId": selection_id, "available": None,
                                "required": round(float(stake) * float(multiplier), 2),
                                "min_absolute": round(float(min_abs), 2), "reason": "LIQUIDITY_UNKNOWN"})
            if unknown:
                out["warning"] = True
                out["shortfall"] = unknown
            return out
        shortfall = [
            leg
            for leg in (
                self._leg_liquidity_shortfall(book, item, multiplier, min_abs, warning_only=warning_only)
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

    def _cap_base(self, bankroll: float) -> Optional[float]:
        """PFC-008: base dei cap percentuali = cassa verificata del RiskDesk (stessa regola del MM); None = BLOCKED."""
        risk_desk = self._risk_desk()
        return validators.verified_cap_base(getattr(risk_desk, "cash_equity_current", None), bankroll)

    def _risk_caps_failure(self, config: Any, bankroll: float) -> Optional[Dict[str, Any]]:
        """PFC-008: percentuali obbligatorie e cassa verificata prima di ogni controllo di esposizione.

        Ritorna il risultato di rifiuto, oppure None se i cap si possono calcolare."""
        pct_reason = validators.risk_pcts_reason(config, _DUTCHING_PCT_FIELDS)
        if pct_reason:
            return self._fail("Limite di rischio % non configurato o non valido", reason=pct_reason)
        if self._cap_base(bankroll) is None:
            risk_desk = self._risk_desk()
            return self._fail("Cassa non verificata: nessun nuovo ingresso", reason="cassa_non_verificata",
                              cash_reason=str(getattr(risk_desk, "cash_equity_reason", "") or "CASH_NOT_READ"))
        return None

    def _compute_order_exposure(self, item: Dict[str, Any]) -> float:
        side = _side(item.get("side"))
        stake = float(item.get("stake", 0.0) or 0.0)
        price = float(item.get("price", 0.0) or 0.0)

        if side == "LAY":
            if "liability" in item:
                return max(0.0, float(item.get("liability", 0.0) or 0.0))
            return max(0.0, stake * max(0.0, price - 1.0))

        return max(0.0, stake)

    @staticmethod
    def _invalid_leg_stake(results: List[Dict[str, Any]]) -> Optional[str]:
        """#529 ciclo A3 (decisione owner): lo stake CALCOLATO di ogni gamba passa dal validatore finanziario
        condiviso (`validators.order_stake`, lo stesso della normalizzazione dell'engine). Il riparto puo'
        produrre stake nulli o negativi (es. totale 0.05 su quote 2/3/50 => 0.10/0.10/-0.15): una sola gamba
        non valida rifiuta l'intero lotto prima di chiavi, tavoli, esposizione e pubblicazione."""
        for idx, item in enumerate(results):
            try:
                validators.order_stake((item or {}).get("stake"))
            except (ValueError, TypeError, AttributeError):        # gamba malformata => rifiuto, mai eccezione
                return f"stake non valido alla gamba #{idx} (selezione {(item or {}).get('selectionId')}): INVALID_SIZE"
        return None

    def _compute_batch_exposure(self, results: List[Dict[str, Any]]) -> float:
        return sum(self._compute_order_exposure(item) for item in results)

    def _allocate_table(
        self,
        event_key: str,
        batch_exposure: float,
        meta: Dict[str, Any],
        selection_id: Optional[int] = None,
    ) -> Optional[int]:
        table_manager = self._table_manager()
        config = self._config()

        if table_manager is None:
            return None

        # PFC-008 (M019): recovery solo se configurato esplicitamente.
        allow_recovery = getattr(config, "allow_recovery", False) is True

        table = None
        if hasattr(table_manager, "allocate"):
            table = table_manager.allocate(event_key=event_key, allow_recovery=allow_recovery)

        if table is None:
            return None

        if hasattr(table_manager, "activate"):
            try:
                table_manager.activate(
                    table_id=table.table_id,
                    event_key=event_key,
                    exposure=float(batch_exposure),
                    market_id=str(meta.get("market_id") or ""),
                    selection_id=selection_id,      # bet manuale: selezione singola (lotto Dutching: None)
                    meta=meta,
                )
            except Exception:
                # #523: tavolo preso ma non attivato => liberato, l'errore risale al chiamante (rollback)
                if hasattr(table_manager, "force_unlock"):
                    try:
                        table_manager.force_unlock(table.table_id)
                    except Exception:
                        logger.exception("Errore force_unlock dopo activate fallita")
                raise

        return int(table.table_id)

    #: Lock di riserva se il runtime non espone il proprio (double nei test); l'ammissione resta comunque
    #: rifiutata senza `admit_controller_entries`.
    _fallback_entry_lock = threading.RLock()

    def _admission_lock(self):
        """#523 (rilievo Codex #529): cap, tavolo e fence di un ingresso si decidono sotto lo stesso lock
        d'ammissione dei segnali (RLock del runtime), cosi' due ingressi concorrenti non leggono la stessa
        esposizione e non riservano lo stesso tavolo. La pubblicazione avviene fuori dal lock."""
        lock = getattr(self.runtime, "_risk_admission_lock", None)
        if lock is None or not hasattr(lock, "acquire"):
            lock = DutchingController._fallback_entry_lock
        return lock

    @staticmethod
    def _acquire_pinned(duplication_guard, event_key: str) -> bool:
        """Acquisisce la chiave fissata (nessuna scadenza per TTL); guardia senza `pin` => acquisizione semplice."""
        try:
            return duplication_guard.acquire(event_key, pin=True)
        except TypeError:
            return duplication_guard.acquire(event_key)

    def _locked_entry(self, body, payload: Dict[str, Any]) -> Dict[str, Any]:
        lock = self._admission_lock()
        lock.acquire()
        held = [True]

        def unlock():
            if held[0]:
                held[0] = False
                lock.release()

        try:
            return body(payload, unlock)
        finally:
            unlock()

    def _admit_entries(self, orders: List[Dict[str, Any]], batch_id: str = "") -> str:
        """#523: ammissione delle gambe Dutching e dell'ordine manuale tramite il RuntimeController: barriera
        cashout e fence d'ammissione per ogni ordine, registrati sotto un'unica presa del lock (lotto atomico).
        "" se ammesso, altrimenti il motivo. Runtime senza la barriera => rifiuto (fail-closed)."""
        admit = getattr(self.runtime, "admit_controller_entries", None)
        if not callable(admit):
            return "barriera_cashout_non_disponibile"
        try:
            reason = admit(orders, batch_id=batch_id)
        except Exception:
            logger.exception("Errore ammissione ingressi controller")
            return "ammissione_non_verificabile"
        if not isinstance(reason, str):
            return "ammissione_non_verificabile"
        return reason

    def _withdraw_entries(self, orders: List[Dict[str, Any]]) -> None:
        """Pubblicazione fallita dopo l'ammissione: registrazioni di fence e lotto scartate."""
        withdraw = getattr(self.runtime, "withdraw_controller_entries", None)
        if callable(withdraw):
            try:
                withdraw(orders)
            except Exception:
                logger.exception("Errore ritiro ingressi controller")

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
        return _side(value)

    def _resolve_selection_side(self, selection: Dict[str, Any]) -> str:
        """`side` o il suo alias `effectiveType`: entrambi presenti e diversi => INVALID_SIDE (rilievo Codex)."""
        selection = selection or {}
        return _selection_side(selection or {})

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
            self._normalize_side(item.get("side"))
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
            side = self._normalize_side((item or {}).get("side"))
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
                        # PR26-a: l'identita' della gamba arriva anche nel
                        # registro durevole (chiave piu' forte del reconcile).
                        "customer_ref": str(o.get("customer_ref") or ""),
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
            # #529 ciclo A2 (rilievo Codex r4326488228): identificativi validati con i validatori condivisi (gli
            # stessi della normalizzazione dell'engine) prima di calcolo e acquisizioni: una gamba che l'engine
            # rifiuterebbe blocca l'intero lotto, mai un piazzamento parziale.
            try:
                validators.order_market_id(market_id)
            except ValueError:
                return self._fail("market_id non valido: INVALID_MARKET_ID")

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
                    selection_id = validators.order_selection_id(selection["selectionId"])
                except ValueError:
                    return self._fail(f"selectionId non valido alla selezione #{idx}")

                if selection_id in seen_selection_ids:
                    return self._fail(f"selectionId duplicato: {selection_id}")
                seen_selection_ids.add(selection_id)

                try:
                    price = validators.order_price(selection["price"])
                except ValueError:
                    return self._fail(f"price non valido alla selezione #{idx}")

                if price <= 1.01:
                    return self._fail(f"Quota non valida alla selezione #{idx}: {price}")

                if price < min_price:
                    return self._fail(
                        f"Quota {price} sotto il minimo configurato ({min_price}) alla selezione #{idx}"
                    )

                # #523: lato obbligatorio; assente o non valido => lotto rifiutato per intero, nessuna gamba
                try:
                    self._resolve_selection_side(selection)
                except ValueError:
                    return self._fail(f"side non valido alla selezione #{idx}: INVALID_SIDE")

            try:
                validators.order_stake(total_stake)        # finito e > 0 (NaN/inf rifiutati)
            except ValueError:
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
            invalid_stake = self._invalid_leg_stake(results)
            if invalid_stake:
                return self._fail(invalid_stake)

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

        invalid_stake = self._invalid_leg_stake(results)
        if invalid_stake:
            return self._fail(invalid_stake)

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
        # risultato). Book assente (cache fredda) o liquidita' IGNOTA di una gamba: in sola
        # osservazione nessuna osservazione; con il guard armato blocco LIQUIDITY_UNKNOWN
        # (cicli 8 e 11 #512).
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
        # Opzionale (rettifica owner 18-19/10/2026): non configurato => nessun controllo; configurato ma
        # non valido => blocco esplicito (fail-closed).
        try:
            max_win = self._max_win(config)
        except ValueError:
            return self._fail("Max Win configurato ma non valido: nessun ordine")
        max_win_breaches = [] if max_win is None else self._max_win_breaches(results, max_win)
        if max_win_breaches and not self._max_win_warning_only(config):
            return self._fail(
                f"Vincita potenziale oltre il cap {max_win:.2f}€ su una o più gambe",
                max_win=round(max_win, 2),
                max_win_breaches=max_win_breaches,
            )

        # Cap monetari assoluti opzionali configurati (PR28-d, rilievo Codex #522): valgono anche per il batch
        # Dutching, PRIMA di ogni side-effect. Nessun cap configurato => nessun limite.
        cap_reason = self._absolute_cap_reason([
            {
                "market_id": payload.get("market_id"),
                "selection_id": item.get("selectionId"),
                "bet_type": _side(item.get("side")),
                "price": item.get("price"),
                "stake": item.get("stake"),
                "event_name": payload.get("event_name", ""),
                "event_id": payload.get("event_id"),
            }
            for item in results
        ])
        if cap_reason:
            return self._fail(f"Cap assoluto: {cap_reason}", cap_reason=cap_reason)

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

        # PROFIT_EPSILON (G5) — WARNING NON-bloccante, ATTIVO DI DEFAULT (opt-out via
        # checkbox profit_epsilon_enabled): segnala se la varianza di profitto NETTO
        # tra gli esiti equalizzati (max - min su profitIfWinsNet) supera la tolleranza
        # configurata (default €0.50). Uno spread residuo alto dopo l'equalizzazione
        # tradisce la premessa "profitto garantito uguale" (l'operatore vince
        # sensibilmente di piu' su certi esiti). Non BLOCCA MAI. FAIL-OPEN: netti
        # incompleti / < 2 esiti => nessun avviso. FAIL-SAFE: config rotta =>
        # trading_config.PROFIT_EPSILON. Il controller usa sempre equalize=ON
        # (calculate_dutching default), quindi lo spread e' quello post-equalizzazione.
        profit_epsilon_enabled = self._profit_epsilon_enabled(config)
        profit_epsilon = self._profit_epsilon(config)
        raw_spread = self._profit_spread_net(results)
        # GUARD MONEY-MANAGEMENT: il confronto usa lo spread ESATTO (raw). Il valore
        # ESPOSTO (profit_spread) e' lo STESSO spread usato per la decisione, ripulito
        # solo dal rumore float (round a 4 decimali) — cosi' display e decisione non si
        # contraddicono MAI (niente "0.50 mostrato con avviso attivo" per un raw 0.504:
        # si mostra 0.504) e non si nasconde uno sbilancio sub-centesimo (GPT-5.6 Sol).
        # In produzione profitIfWinsNet e' gia' cent-preciso (dutching _round_step),
        # quindi profit_spread esce naturalmente a 2 decimali. L'avviso scatta SOLO se
        # abilitato dalla GUI (default on): disattivarlo lo silenzia senza toccare la soglia.
        profit_spread = round(float(raw_spread), 4) if raw_spread is not None else None
        profit_epsilon_warning = bool(
            profit_epsilon_enabled and raw_spread is not None and raw_spread > profit_epsilon + 1e-9
        )

        # PFC-008 (M019): percentuale assente o non valida, o cassa non verificata => blocco esplicito.
        # Prima un default di codice sostituiva la percentuale, un NaN rendeva sempre falso il confronto
        # e con bankroll <= 0 o config assente i controlli di esposizione venivano saltati (fail-open).
        failure = self._risk_caps_failure(config, bankroll)
        if failure is not None:
            return failure
        cap_base = self._cap_base(bankroll)
        max_total_exposure = cap_base * (float(config.max_total_exposure_pct) / 100.0)
        max_event_exposure = cap_base * (float(config.max_event_exposure_pct) / 100.0)
        max_single_bet = cap_base * (float(config.max_single_bet_pct) / 100.0)

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

        # #529 ciclo A2 (rilievo Codex r4326488216): il precheck e' PRIVO di effetti permanenti: controlla soltanto
        # (in lettura) che la chiave non sia gia' attiva. L'acquisizione, fissata, avviene nella fase controllata
        # di ammissione (`_execute_reserved`, sotto il lock d'ammissione). Errore della guardia => rifiuto.
        if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
            try:
                is_duplicate = getattr(duplication_guard, "is_duplicate", None)
                # controllo informativo: il gate vero e' l'acquisizione all'ammissione (sempre eseguita)
                if callable(is_duplicate) and is_duplicate(event_key):
                    return self._fail("Duplicato bloccato", event_key=event_key)
            except Exception:
                logger.exception("Errore duplication_guard.is_duplicate")
                return self._fail("Anti-duplicato non disponibile: nessun ordine", event_key=event_key)

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
            max_win=None if max_win is None else round(max_win, 2),
            max_win_warning=bool(max_win_breaches),
            max_win_breaches=max_win_breaches,
            max_stake_pct=round(max_stake_pct, 4),
            stake_pct_warning=stake_pct_warning,
            stake_pct_ratio=round(stake_pct_ratio, 4),
            profit_epsilon=round(profit_epsilon, 2),
            profit_epsilon_enabled=profit_epsilon_enabled,
            profit_spread=profit_spread,
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
            out = self.precheck(payload)              # senza effetti: nessuna chiave riservata
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
        return self._locked_entry(self._execute_reserved, payload)

    def _execute_reserved(self, payload: Dict[str, Any], unlock) -> Dict[str, Any]:
        """Controlli, tavolo e ammissione sotto il lock d'ammissione; `unlock()` prima di pubblicare."""
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
        # Fase controllata di ammissione (sotto il lock): la chiave anti-duplicato del lotto si acquisisce qui,
        # FISSATA per la durata del lotto trattenuto (rilievo Codex r4325985989). Si rilascia nei rifiuti solo se
        # acquisita da QUESTO lotto: con l'anti-duplicato disattivato la chiave non e' sua.
        owned_key = ""
        if duplication_guard and bool(getattr(self._config(), "anti_duplication_enabled", True)):
            rejected = {"dry_run": False, "preflight": False, "batch_id": batch_id, "event_key": event_key}
            try:
                if not self._acquire_pinned(duplication_guard, event_key):
                    self._publish_audit("DUTCHING_BATCH_REJECTED", {"payload": payload, "reason": "duplicate"})
                    return self._fail("Duplicato bloccato", **rejected)
            except Exception:
                # anti-duplicato non verificabile => nessun ordine (mai fail-open)
                logger.exception("Errore duplication_guard.acquire")
                return self._fail("Anti-duplicato non disponibile: nessun ordine", **rejected)
            owned_key = event_key
        # #523 (rilievo Codex #529): il tavolo e' sempre riservato qui con l'esposizione del lotto; un
        # `table_id` del chiamante non viene usato (non riserverebbe nulla e non entrerebbe nelle misure).
        if payload.get("table_id"):
            logger.warning("table_id %r del chiamante ignorato: tavolo riservato dal controller",
                           payload.get("table_id"))
        try:
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
        except Exception as exc:
            # #523 (rilievo Codex #529): nessun ordine pubblicato => chiave anti-duplicato rilasciata
            logger.exception("Errore riserva tavolo batch dutching")
            self._release_table_and_key(None, owned_key)
            return self._fail(f"Riserva tavolo fallita: {exc}", dry_run=False, preflight=False,
                              batch_id=batch_id, event_key=event_key)
        allocated_here = table_id is not None

        if table_id is None and self._table_manager() is not None:
            msg = "Nessun tavolo disponibile per batch dutching"
            self._release_table_and_key(None, owned_key)   # nessun ordine: chiave del lotto rilasciata
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
        admitted = False

        # PR26-a: identita' delle gambe dagli INPUT della richiesta, non dagli
        # stake calcolati (con quote uguali calculate_dutching assegna il
        # centesimo di arrotondamento in base all'ordine delle selezioni). Il
        # batch e' mercato + stake totale + elenco ORDINATO delle gambe; una
        # gamba e' la sua chiave, mai la posizione nella lista del chiamante
        # (selectionId duplicati sono gia' rifiutati in validazione; due chiavi
        # identiche darebbero comunque lo stesso ref: doppione bloccato).
        def _leg_key(item):
            return (
                int(item["selectionId"]),
                _side(item.get("side")),
                float(item["price"]),
            )

        leg_batch_identity = {
            "market_id": str(payload.get("market_id") or ""),
            "event_name": str(payload.get("event_name") or ""),
            "market_name": str(payload.get("market_name") or ""),
            "simulation_mode": bool(payload.get("simulation_mode", False)),
            "total_stake": float(payload.get("total_stake") or 0.0),
            "legs": sorted(_leg_key(item) for item in results),
        }

        try:
            for idx, item in enumerate(results, start=1):
                leg_key = _leg_key(item)
                order = {
                    "market_id": str(payload["market_id"]),
                    "selection_id": int(item["selectionId"]),
                    "bet_type": _side(item.get("side")),
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
                    # PR26-a: una identita' per gamba (vedi leg_batch_identity):
                    # stesso batch ricalcolato, anche con le selezioni in
                    # ordine diverso = stessi ref; gambe distinte = ref distinti.
                    "customer_ref": derive_customer_ref(
                        "dut",
                        {
                            "batch": leg_batch_identity,
                            "leg": list(leg_key),
                        },
                    ),
                    "batch_avg_profit": float(avg_profit),
                    "batch_book_pct": float(book_pct),
                    "batch_exposure": float(batch_exposure),
                }
                orders.append(order)

            # #523: barriera cashout e fence d'ammissione per TUTTE le gambe prima di pubblicarne una sola:
            # con un cashout attivo nel loro ambito il lotto e' rifiutato per intero.
            admission = self._admit_entries(orders, batch_id=batch_id)
            if admission:
                self._publish_audit("DUTCHING_BATCH_REJECTED", {"payload": payload, "reason": admission})
                self._release_table_and_key(table_id if allocated_here else None, owned_key)
                return self._fail(
                    f"Lotto non ammesso: {admission}",
                    dry_run=False,
                    preflight=False,
                    batch_id=batch_id,
                    event_key=event_key,
                    reason=admission,
                )
            admitted = True
            unlock()                     # ingresso riservato: la pubblicazione non trattiene il lock

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
            if admitted:
                # gambe non pubblicate: le loro registrazioni non devono restare in memoria
                self._withdraw_entries([o for o in orders if o not in published_orders])

            if published_orders:
                # #523/#529: gambe gia' pubblicate possono essere sull'exchange. Tavolo e chiave del lotto
                # restano trattenuti fino a settlement e riconciliazione (fail-closed; esito del lotto: PR30).
                pass
            else:
                self._release_table_and_key(table_id if allocated_here else None, owned_key)

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
        return self._locked_entry(self._manual_bet_reserved, payload)

    def _manual_bet_reserved(self, payload: Dict[str, Any], unlock) -> Dict[str, Any]:
        """Cap, dedupe, tavolo e ammissione sotto il lock d'ammissione; `unlock()` prima di pubblicare."""
        try:
            required = ["market_id", "selection_id", "price", "stake"]
            for key in required:
                if key not in payload:
                    return self._fail(f"{key} mancante")

            # #523: lato obbligatorio (bet_type / side / action), stesso contratto dei segnali: mai BACK per
            # default, alias in conflitto rifiutati.
            try:
                side = validators.signal_side(payload)
            except ValueError:
                return self._fail("side non valido: INVALID_SIDE")

            if not self._runtime_active():
                return self._fail("Runtime non attivo")

            if not self._bus_available():
                return self._fail("EventBus non disponibile")

            # #529 (rilievo Codex r4325986004): identificativi validati PRIMA di ogni acquisizione (dedupe, tavolo):
            # un ordine che l'engine rifiuterebbe in normalizzazione non deve occupare un tavolo.
            try:
                market_id = validators.order_market_id(payload["market_id"])
                selection_id = validators.order_selection_id(payload["selection_id"])
                price = validators.order_price(payload["price"])
                stake = validators.order_stake(payload["stake"])
            except ValueError as exc:
                return self._fail(f"Ordine non valido: {exc}")

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
            # Opzionale (rettifica owner 18-19/10/2026): non configurato => nessun controllo.
            potential_win = self._leg_potential_win({"side": side, "stake": stake, "price": price})
            try:
                max_win = self._max_win(config)
            except ValueError:
                return self._fail("Max Win configurato ma non valido: nessun ordine")
            max_win_exceeded = max_win is not None and potential_win > max_win + 1e-9
            if max_win_exceeded and not self._max_win_warning_only(config):
                return self._fail(
                    f"Vincita potenziale {potential_win:.2f}€ oltre il cap {max_win:.2f}€",
                    max_win=round(max_win, 2),
                    potential_win=round(potential_win, 2),
                )
            # Cap monetari assoluti opzionali configurati (PR28-d, rilievo Codex #522), PRIMA del dedupe.
            cap_reason = self._absolute_cap_reason([{
                "market_id": market_id, "selection_id": selection_id, "bet_type": side,
                "price": price, "stake": stake, "event_name": payload.get("event_name", ""),
                "event_id": payload.get("event_id"),
            }])
            if cap_reason:
                return self._fail(f"Cap assoluto: {cap_reason}", cap_reason=cap_reason)
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

            # #523: rischio reale dell'ordine (BACK = stake, LAY = liability), lo stesso usato per tavoli e cap.
            exposure = validators.order_exposure(side, stake, price)

            bankroll = self._bankroll_current()
            failure = self._risk_caps_failure(config, bankroll)   # PFC-008, come nel precheck
            if failure is not None:
                return failure
            current_total_exposure = self._table_total_exposure()
            cap_base = self._cap_base(bankroll)
            max_total_exposure = cap_base * (float(config.max_total_exposure_pct) / 100.0)
            max_single_bet = cap_base * (float(config.max_single_bet_pct) / 100.0)

            if exposure > max_single_bet + 1e-9:
                return self._fail("Stake oltre max_single_bet")

            if current_total_exposure + exposure > max_total_exposure + 1e-9:
                return self._fail("Esposizione globale oltre limite")

            guarded = bool(duplication_guard) and bool(getattr(config, "anti_duplication_enabled", True))
            if guarded:
                try:
                    acquired = duplication_guard.acquire(event_key)
                except Exception:
                    logger.exception("Errore duplication_guard.acquire (manual_bet)")
                    return self._fail("Anti-duplicato non disponibile: nessun ordine")
                if not acquired:
                    return self._fail("Duplicato bloccato")

            # #523: l'ordine manuale occupa un tavolo con il suo rischio reale, cosi' entra nelle misure di
            # mercato, evento e totale come ogni altro ingresso, e viene liberato dagli esiti terminali. Il
            # tavolo e' sempre riservato qui: un `table_id` del chiamante non riserverebbe nulla (rilievo Codex).
            if payload.get("table_id"):
                logger.warning("table_id %r del chiamante ignorato: tavolo riservato dal controller",
                               payload.get("table_id"))
            try:
                table_id = self._allocate_table(
                    event_key=event_key,
                    batch_exposure=exposure,
                    meta={
                        "market_id": market_id,
                        "event_name": payload.get("event_name", ""),
                        "event_id": payload.get("event_id"),
                        "market_name": payload.get("market_name", ""),
                        "runner_name": payload.get("runner_name", ""),
                        "bet_type": side,
                        "price": price,
                        "type": "manual_bet",
                    },
                    selection_id=selection_id,   # rilievo Codex #529: settlement e SL/TP riconoscono la posizione
                )
            except Exception as exc:
                # nessun ordine pubblicato => chiave anti-duplicato rilasciata, retry sicuro (rilievo Codex)
                logger.exception("Errore riserva tavolo (manual_bet)")
                if guarded:
                    duplication_guard.release(event_key)
                return self._fail(f"Riserva tavolo fallita: {exc}")
            allocated_here = table_id is not None
            if table_id is None and self._table_manager() is not None:
                if guarded:
                    duplication_guard.release(event_key)
                return self._fail("Nessun tavolo disponibile per il bet manuale")

            order = {
                "market_id": market_id,
                "selection_id": selection_id,
                "bet_type": side,
                "price": price,
                "stake": stake,
                "event_name": payload.get("event_name", ""),
                "market_name": payload.get("market_name", ""),
                "runner_name": payload.get("runner_name", ""),
                "simulation_mode": bool(payload.get("simulation_mode", False)),
                "table_id": table_id,
                "event_key": event_key,
                # PR26-a: ref a monte preservato; altrimenti ogni manual_bet e'
                # un intento NUOVO (due click identici = due bet): ref di
                # operazione generato qui una volta, riusato dai retry a valle.
                "customer_ref": (
                    normalize_upstream_customer_ref(payload.get("customer_ref"))
                    or new_operation_customer_ref("man")
                ),
            }

            def _undo():
                if allocated_here:
                    self._release_table_and_key(table_id, event_key if guarded else "")
                elif guarded:
                    duplication_guard.release(event_key)

            # #523: barriera cashout e fence d'ammissione, come per i segnali.
            reason = self._admit_entries([order])
            if reason:
                _undo()
                return self._fail(f"Ingresso non ammesso: {reason}", reason=reason)
            unlock()                     # ingresso riservato: la pubblicazione non trattiene il lock

            try:
                self.bus.publish("CMD_QUICK_BET", order)
            except Exception:
                self._withdraw_entries([order])
                _undo()
                raise

            self._publish_audit("MANUAL_BET_APPROVED", {"order": order})
            return self._ok(
                order=order,
                max_win=None if max_win is None else round(max_win, 2),
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
