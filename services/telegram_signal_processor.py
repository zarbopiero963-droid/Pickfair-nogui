from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional

from parsers import motore as parser_motore


class TelegramSignalProcessor:
    """
    Processore / normalizzatore segnali Telegram.

    Responsabilità:
    - normalizzare action / side
    - leggere price / odds
    - leggere market_id / selection_id quando già presenti
    - costruire payload runtime pronto per il bus

    Nota importante:
    - se market_id / selection_id NON esistono, questo processore da solo
      non può costruire un payload eseguibile
    - in quel caso deve intervenire TelegramBetResolver
    """

    # =========================================================
    # NORMALIZZAZIONE BASE
    # =========================================================
    def normalize_action(self, signal: Dict[str, Any]) -> str:
        action = (
            signal.get("action")
            or signal.get("side")
            or signal.get("bet_type")
            or "BACK"
        )
        action = str(action).upper().strip()
        if action not in ("BACK", "LAY"):
            action = "BACK"
        return action

    def parse_price(self, signal: Dict[str, Any]) -> Optional[float]:
        raw = (
            signal.get("price")
            or signal.get("odds")
            or signal.get("master_price")
            or signal.get("quote")
        )
        if raw in (None, ""):
            return None
        try:
            return float(str(raw).replace(",", "."))
        except Exception:
            return None

    def parse_selection_id(self, signal: Dict[str, Any]) -> Optional[int]:
        raw = signal.get("selection_id", signal.get("selectionId"))
        try:
            if raw in (None, ""):
                return None
            return int(raw)
        except Exception:
            return None

    def parse_market_id(self, signal: Dict[str, Any]) -> Optional[str]:
        raw = signal.get("market_id", signal.get("marketId"))
        if raw in (None, ""):
            return None
        try:
            value = str(raw).strip()
            return value or None
        except Exception:
            return None

    # =========================================================
    # NAMES / LABELS
    # =========================================================
    def parse_event_name(self, signal: Dict[str, Any]) -> str:
        return str(
            signal.get("match")
            or signal.get("event")
            or signal.get("event_name")
            or "Segnale Telegram"
        )

    def parse_market_name(self, signal: Dict[str, Any]) -> str:
        return str(
            signal.get("market")
            or signal.get("market_name")
            or signal.get("market_type")
            or "Scommessa da Segnale"
        )

    def parse_market_type(self, signal: Dict[str, Any]) -> str:
        return str(signal.get("market_type") or "MATCH_ODDS")

    def parse_selection_name(self, signal: Dict[str, Any], selection_id: Optional[int]) -> str:
        return str(
            signal.get("selection")
            or signal.get("selection_name")
            or signal.get("runner_name")
            or signal.get("runnerName")
            or selection_id
            or "Unknown"
        )

    # =========================================================
    # SCORE / MINUTE HELPERS
    # =========================================================
    def parse_home_score(self, signal: Dict[str, Any]) -> Optional[int]:
        raw = signal.get("home_score")
        if raw in (None, ""):
            return None
        try:
            return int(raw)
        except Exception:
            return None

    def parse_away_score(self, signal: Dict[str, Any]) -> Optional[int]:
        raw = signal.get("away_score")
        if raw in (None, ""):
            return None
        try:
            return int(raw)
        except Exception:
            return None

    def parse_minute(self, signal: Dict[str, Any]) -> Optional[int]:
        raw = signal.get("minute") or signal.get("time_minute")
        if raw in (None, ""):
            return None
        try:
            return int(raw)
        except Exception:
            return None

    # =========================================================
    # DIRECT PAYLOAD BUILD
    # =========================================================

    # ------------------------------------------------------------------
    # Parser Personalizzati (H-08)
    # ------------------------------------------------------------------
    # Qui prima c'era:
    #
    #     from core.custom_parser_engine import CustomParserEngine
    #     ...
    #     except ImportError:
    #         pass
    #
    # `CustomParserEngine` non e' mai esistita in questo repository:
    # l'`ImportError` scattava a ogni messaggio e veniva ingoiato, quindi i
    # Parser Personalizzati non hanno MAI girato sul percorso vivo. Rilievo
    # H-08 dell'audit #335.
    #
    # Il vecchio blocco conteneva anche un fail-open sul percorso dei soldi:
    #
    #     signal["action"] = parsed.get("action", "BACK")
    #
    # cioe' un parser che non estraeva la direzione produceva comunque una
    # PUNTA. La direzione di una scommessa non e' un campo su cui mettere un
    # default: `parsers.campi.normalizza_azione` restituisce "" e
    # `parsers.motore` rifiuta con `DIREZIONE_ASSENTE`.

    def __init__(self) -> None:
        # Dichiarati qui e non al primo uso (rilievo DeepSource): chi legge la
        # classe deve poter vedere lo stato che un'istanza puo' avere.
        #
        # `_cache_parser` e' UNA tupla, non tre attributi (rilievo Fugu +
        # Fable): valorizzarli uno per uno lasciava una finestra in cui un
        # altro thread vedeva parser nuovi con value-map vecchie. Una singola
        # assegnazione di riferimento e' atomica sotto il GIL, quindi chi
        # legge vede o tutto il vecchio o tutto il nuovo, mai un misto.
        self._cache_parser = None   # None = da caricare | (definizioni, registro, dizionario_ok)
        # Serializza caricamento E assegnazione (rilievo GPT-5.6 Sol + Fable).
        # L'assegnazione di una tupla e' atomica, ma atomica non vuol dire
        # ORDINATA: un caricamento lento partito prima poteva concludersi DOPO
        # una ricarica e sovrascriverla, lasciando i parser vecchi in memoria a
        # tempo indeterminato — "last writer wins" sul percorso dei segnali.
        # Il lock e' tenuto anche durante l'I/O: e' una lettura di pochi file
        # che avviene una volta all'avvio e quando l'utente salva, non a ogni
        # messaggio, quindi non e' sul cammino caldo.
        self._lucchetto_cache = threading.Lock()

    def _parser_personalizzati(self):
        """Definizioni e value-map, caricate una volta sola.

        Rileggere la cartella a ogni messaggio sarebbe I/O per messaggio su un
        percorso che deve reggere una raffica. `ricarica_parser()` esiste per
        quando l'utente ne salva uno nuovo dalla GUI.
        """
        cache = self._cache_parser
        if cache is not None:
            return cache[0], cache[1]

        with self._lucchetto_cache:
            # Ricontrollo dentro il lock: mentre aspettavamo, un altro thread
            # (o una `ricarica_parser`) puo' aver gia' popolato la cache. Senza
            # questo, ricaricheremmo sopra uno stato piu' fresco del nostro.
            cache = self._cache_parser
            if cache is not None:
                return cache[0], cache[1]
            try:
                definizioni = parser_motore.carica_parser()
                registro, dizionario_ok = parser_motore.registro_value_map()
            except Exception:
                logging.getLogger(__name__).exception(
                    "Caricamento dei Parser Personalizzati fallito: nessun arricchimento")
                definizioni, registro, dizionario_ok = [], {}, False
            cache = (definizioni, registro, dizionario_ok)
            self._cache_parser = cache
        return cache[0], cache[1]

    @property
    def dizionario_disponibile(self) -> bool:
        """True se le value-map derivate dal dizionario sono caricate."""
        self._parser_personalizzati()
        return bool(self._cache_parser[2])

    def ricarica_parser(self) -> int:
        """Rilegge i parser dal disco e restituisce quanti ne ha caricati.

        Da chiamare quando l'utente salva o cancella un parser, altrimenti la
        modifica non avrebbe effetto fino al riavvio.

        Non azzera la cache prima di ricaricare (rilievo Fugu + Fable): fra
        l'azzeramento e la ricarica un messaggio in arrivo avrebbe visto
        `None` e ricaricato per conto suo. Qui si costruisce lo stato nuovo e
        poi lo si sostituisce in un colpo solo; chi sta leggendo continua a
        vedere quello vecchio, che e' valido, fino allo scambio.
        """
        with self._lucchetto_cache:
            try:
                definizioni = parser_motore.carica_parser()
                registro, dizionario_ok = parser_motore.registro_value_map()
            except Exception:
                logging.getLogger(__name__).exception(
                    "Ricarica dei Parser Personalizzati fallita: resta lo stato precedente")
                precedente = self._cache_parser
                return len(precedente[0]) if precedente else 0
            self._cache_parser = (definizioni, registro, dizionario_ok)
            return len(definizioni)

    @staticmethod
    def _campo_da_riempire(signal: Dict[str, Any], chiave: str) -> bool:
        """True se il campo e' assente o vuoto — NON semplicemente falsy.

        Rilievo Fugu: `if not signal.get(chiave)` trattava come "da riempire"
        anche valori Betfair legittimi che in Python sono falsy. `handicap`
        vale `0` o `"0"` in quasi ogni mercato senza handicap: il parser lo
        avrebbe sovrascritto, e un handicap diverso e' una LINEA diversa,
        cioe' una scommessa diversa da quella che il segnale chiedeva.
        """
        if chiave not in signal:
            return True
        valore = signal[chiave]
        if valore is None:
            return True
        return isinstance(valore, str) and not valore.strip()

    def _arricchisci_con_parser_personalizzati(self, signal: Dict[str, Any], raw_text: str) -> None:
        """Applica i parser dell'owner e riempie i campi assenti o vuoti.

        **Non sovrascrive un valore gia' presente.** Se il segnale arriva con
        un `market_id` suo, quello e' piu' autorevole di un'estrazione da
        testo libero, e lasciare che il parser lo cambi significherebbe
        spostare di nascosto la scommessa su un altro mercato.

        Non solleva mai: un guasto qui deve valere "nessun arricchimento", non
        "il bot non risponde". Ma a differenza di prima **viene registrato**,
        perche' il silenzio e' cio' che ha tenuto H-08 nascosto.
        """
        log = logging.getLogger(__name__)
        try:
            definizioni, registro = self._parser_personalizzati()
            if not definizioni:
                return
            esito = parser_motore.estrai(raw_text, parser=definizioni, registro=registro)
        except Exception:
            log.exception("Parser Personalizzati: errore, nessun arricchimento")
            return

        if not esito.ok:
            log.debug("Parser Personalizzati: nessuna estrazione (%s)", esito.motivo)
            return

        riempiti = 0
        for chiave, valore in esito.campi.items():
            if self._campo_da_riempire(signal, chiave):
                signal[chiave] = valore
                riempiti += 1
        log.info("Parser Personalizzati: '%s' ha riempito %d campi su %d estratti",
                 esito.parser, riempiti, len(esito.campi))

    def normalize_ingestion_signal(self, signal: Any) -> Dict[str, Any]:
        """
        Telegram ingestion boundary.

        Restituisce sempre un contratto deterministico:
        {
          "ok": bool,
          "error_code": str|None,
          "error_reason": str|None,
          "normalized_signal": dict,
        }
        """
        # Integrazione Custom Parser Engine (#290, #305)
        raw_text = ""
        if isinstance(signal, dict):
            raw_text = signal.get("raw_text") or signal.get("message") or signal.get("text") or ""
        elif isinstance(signal, str):
            raw_text = signal
            signal = {"text": raw_text}

        if raw_text:
            self._arricchisci_con_parser_personalizzati(signal, raw_text)

        if not isinstance(signal, dict):
            return {
                "ok": False,
                "error_code": "SIGNAL_NOT_DICT",
                "error_reason": "telegram signal must be a dict payload",
                "normalized_signal": {},
            }

        raw = dict(signal)
        copy_meta = raw.get("copy_meta")
        pattern_meta = raw.get("pattern_meta")

        if copy_meta is not None and not isinstance(copy_meta, dict):
            return {
                "ok": False,
                "error_code": "COPY_META_NOT_DICT",
                "error_reason": "copy_meta must be a dict when provided",
                "normalized_signal": {},
            }
        if pattern_meta is not None and not isinstance(pattern_meta, dict):
            return {
                "ok": False,
                "error_code": "PATTERN_META_NOT_DICT",
                "error_reason": "pattern_meta must be a dict when provided",
                "normalized_signal": {},
            }
        if isinstance(copy_meta, dict) and isinstance(pattern_meta, dict):
            return {
                "ok": False,
                "error_code": "COPY_PATTERN_MUTUALLY_EXCLUSIVE",
                "error_reason": "copy_meta and pattern_meta cannot coexist",
                "normalized_signal": {},
            }

        selection_id = self.parse_selection_id(raw)
        market_id = self.parse_market_id(raw)
        action = self.normalize_action(raw)
        price = self.parse_price(raw)

        normalized: Dict[str, Any] = {
            "boundary_stage": "telegram_ingestion_normalized_v1",
            "market_id": market_id,
            "selection_id": selection_id,
            "bet_type": action,
            "action": action,
            "price": price,
            "event_name": self.parse_event_name(raw),
            "market_name": self.parse_market_name(raw),
            "market_type": self.parse_market_type(raw),
            "selection": self.parse_selection_name(raw, selection_id),
            "signal_type": str(raw.get("signal_type") or raw.get("signal_name") or ""),
            "minute": self.parse_minute(raw),
            "home_score": self.parse_home_score(raw),
            "away_score": self.parse_away_score(raw),
            "raw_text": raw.get("raw_text") or raw.get("message") or raw.get("text") or "",
            "raw_signal": raw,
        }

        if isinstance(copy_meta, dict):
            normalized["copy_meta"] = dict(copy_meta)
            normalized["order_origin"] = str(raw.get("order_origin") or "COPY").upper()
        elif isinstance(pattern_meta, dict):
            normalized["pattern_meta"] = dict(pattern_meta)
            normalized["order_origin"] = str(raw.get("order_origin") or "PATTERN").upper()
        elif raw.get("order_origin"):
            normalized["order_origin"] = str(raw.get("order_origin")).strip()

        return {
            "ok": True,
            "error_code": None,
            "error_reason": None,
            "normalized_signal": normalized,
        }

    def build_runtime_signal(
        self,
        signal: Dict[str, Any],
        stake: float,
        simulation_mode: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Costruisce payload runtime SOLO se il segnale possiede già:
        - market_id
        - selection_id
        - price/odds

        Se mancano, restituisce None e deve intervenire TelegramBetResolver.
        """
        action = self.normalize_action(signal)
        selection_id = self.parse_selection_id(signal)
        market_id = self.parse_market_id(signal)
        original_price = self.parse_price(signal)

        if selection_id is None or not market_id or original_price is None:
            return None

        selection_name = self.parse_selection_name(signal, selection_id)
        event_name = self.parse_event_name(signal)
        market_name = self.parse_market_name(signal)
        market_type = self.parse_market_type(signal)

        return {
            "market_id": market_id,
            "market_type": market_type,
            "event_name": event_name,
            "event": event_name,
            "market_name": market_name,
            "market": market_name,
            "selection_id": int(selection_id),
            "selectionId": int(selection_id),
            "runner_name": selection_name,
            "runnerName": selection_name,
            "selection": selection_name,
            "bet_type": action,
            "action": action,
            "price": float(original_price),
            "odds": float(original_price),
            "master_price": float(original_price),
            "stake": float(stake),
            "simulation_mode": bool(simulation_mode),
            "source": "TELEGRAM",
            "forced_execution": True,
            "signal_type": str(signal.get("signal_type") or signal.get("signal_name") or ""),
            "minute": self.parse_minute(signal),
            "home_score": self.parse_home_score(signal),
            "away_score": self.parse_away_score(signal),
            "raw_text": signal.get("raw_text") or signal.get("message") or signal.get("text") or "",
        }

    # =========================================================
    # LIGHT NORMALIZED SNAPSHOT
    # =========================================================
    def normalize_signal_snapshot(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Utile per debug/UI/log.
        Non garantisce che il segnale sia già eseguibile.
        """
        selection_id = self.parse_selection_id(signal)
        market_id = self.parse_market_id(signal)
        price = self.parse_price(signal)

        return {
            "event_name": self.parse_event_name(signal),
            "market_name": self.parse_market_name(signal),
            "market_type": self.parse_market_type(signal),
            "selection_name": self.parse_selection_name(signal, selection_id),
            "market_id": market_id,
            "selection_id": selection_id,
            "action": self.normalize_action(signal),
            "price": price,
            "minute": self.parse_minute(signal),
            "home_score": self.parse_home_score(signal),
            "away_score": self.parse_away_score(signal),
            "signal_type": str(signal.get("signal_type") or signal.get("signal_name") or ""),
            "has_direct_bet_ids": bool(market_id and selection_id is not None),
        }
