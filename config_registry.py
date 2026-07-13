"""Config & Go-Live Console — registro di configurazione (fonte-dato UNICA).

Epic #359 / PR-A. Questo modulo NON ha UI e NON modifica nessuna logica: e' la
fonte-dato che GUI (`mini_gui`) e headless (`--config-report`/`--preflight`)
consumeranno senza duplicare l'enumerazione dei parametri.

Due prodotti:
- `ConfigRegistry.entries()`: enumera ogni parametro configurabile come
  `ConfigEntry{chiave, label, valore, valido?, richiesto_per_LIVE?, sorgente,
  rimedio, is_secret}`. I segreti (app_key, certificate, private_key, password)
  sono **mascherati**: si espone presenza/validita', mai il valore.
- `readiness_report()`: checklist ✅/❌ dei prerequisiti LIVE, **riusando**
  `RuntimeController.evaluate_live_readiness` (autorita' unica, #350). NON
  reimplementa la logica del gate: la espone soltanto (fail-closed invariato).

Vincoli (epic #359): sola lettura, nessun segreto in chiaro, nessun gate
indebolito.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional

import trading_config

logger = logging.getLogger(__name__)


# Rimedio per ogni blocker LIVE. Fonte unica allineata a
# `evaluate_live_readiness` (#350) e a `headless_main._BLOCKER_REMEDIATION`
# (che in PR-B verra' deduplicato importando da qui).
BLOCKER_REMEDIATION: dict[str, tuple[str, str]] = {
    "INVALID_EXECUTION_MODE": (
        "execution_mode non valido",
        "Usa SIMULATION o LIVE.",
    ),
    "LIVE_NOT_ENABLED": (
        "live_enabled e' False",
        "Avvia con --live-enabled oppure imposta live_enabled=True nel DB.",
    ),
    "LIVE_READINESS_FLAG_NOT_OK": (
        "Il flag live_readiness_ok non e' confermato",
        "Conferma la readiness LIVE (live_readiness_ok=True nel DB) dopo la checklist go-live.",
    ),
    "LIVE_KEY_SOURCE_UNSAFE": (
        "La chiave segreta non proviene da una sorgente sicura",
        "Usa PICKFAIR_SECRET_KEY (env) o un file chiave valido (~/.pickfair/db.key).",
    ),
    "LIVE_HARD_STOP_CONFIG_MISSING": (
        "Config hard-stop giornaliero mancante",
        "Imposta i campi hard-stop (max_daily_loss, max_drawdown_hard_stop_pct, max_open_exposure).",
    ),
    "LIVE_HARD_STOP_CONFIG_INVALID": (
        "Config hard-stop giornaliero non valida",
        "Correggi i valori hard-stop (numerici, finiti e > 0; pct <= 100).",
    ),
    "KILL_SWITCH_ACTIVE": (
        "Kill switch attivo",
        "Disattiva il kill switch (flag DB / --kill-switch-off).",
    ),
    "SAFE_MODE_BLOCKING": (
        "Safe mode sta bloccando il LIVE",
        "Rimuovi la condizione di safe mode (kill switch / emergenza).",
    ),
    "LIVE_DEPENDENCY_MISSING": (
        "Dipendenza LIVE assente (es. betfair_service non connettibile)",
        "Verifica che betfair_service sia presente e connettibile (credenziali/cert).",
    ),
    "CONTRADICTORY_STATE": (
        "Stato contraddittorio (LIVE ma simulation_mode/live_enabled incoerenti)",
        "Allinea execution_mode / live_enabled / simulation_mode.",
    ),
    "RUNTIME_NOT_INITIALIZED": (
        "Runtime non inizializzato",
        "Verifica il build del runtime (config/table_manager/...).",
    ),
    "RUNTIME_HALF_STARTED": (
        "Runtime avviato a meta' (servizi non connessi)",
        "Riavvia e verifica la connessione betfair/telegram.",
    ),
    "STARTUP_FAILED": (
        "Avvio fallito (last_error valorizzato)",
        "Controlla il log di avvio e correggi la causa del fallimento.",
    ),
    "READINESS_SIGNAL_UNKNOWN": (
        "Segnale di readiness sconosciuto (mode runtime non riconosciuto)",
        "Verifica lo stato del runtime; riavvia se necessario.",
    ),
}


# Prerequisiti LIVE nell'ordine di presentazione: (chiave, label, blocker_code).
# Ogni voce e' ✅ se il suo blocker_code NON e' tra i blockers di
# evaluate_live_readiness, ❌ (con rimedio) altrimenti.
_LIVE_PREREQUISITES: tuple[tuple[str, str, str], ...] = (
    ("execution_mode_valid", "Execution mode valido", "INVALID_EXECUTION_MODE"),
    ("live_enabled", "Live abilitato (live_enabled)", "LIVE_NOT_ENABLED"),
    ("live_readiness_ok", "Readiness confermata (live_readiness_ok)", "LIVE_READINESS_FLAG_NOT_OK"),
    ("key_source_safe", "Sorgente chiave segreta sicura", "LIVE_KEY_SOURCE_UNSAFE"),
    ("hard_stop_present", "Hard-stop giornaliero presente", "LIVE_HARD_STOP_CONFIG_MISSING"),
    ("hard_stop_valid", "Hard-stop giornaliero valido", "LIVE_HARD_STOP_CONFIG_INVALID"),
    ("kill_switch_off", "Kill switch disattivo", "KILL_SWITCH_ACTIVE"),
    ("safe_mode_off", "Safe mode non bloccante", "SAFE_MODE_BLOCKING"),
    ("betfair_dependency", "Dipendenza Betfair presente", "LIVE_DEPENDENCY_MISSING"),
    ("runtime_initialized", "Runtime inizializzato", "RUNTIME_NOT_INITIALIZED"),
    ("runtime_not_half_started", "Runtime non half-started", "RUNTIME_HALF_STARTED"),
    ("no_contradictory_state", "Nessuno stato contraddittorio", "CONTRADICTORY_STATE"),
    ("readiness_signal_known", "Segnale readiness noto", "READINESS_SIGNAL_UNKNOWN"),
    ("startup_ok", "Nessun errore di avvio", "STARTUP_FAILED"),
)


# Placeholder mascheramento.
_MASK_SET = "(impostato)"
_MASK_UNSET = "(non impostato)"


@dataclass(frozen=True)
class ConfigEntry:
    """Un parametro configurabile e i suoi metadati (per GUI/headless)."""

    key: str
    label: str
    value: Any
    valid: bool
    required_for_live: bool
    source: str  # "DB" | "code" | "runtime"
    remedy: str = ""
    is_secret: bool = False


@dataclass(frozen=True)
class ReadinessItem:
    """Una voce della checklist go-live (✅/❌)."""

    key: str
    label: str
    ok: bool
    blocker: str = ""
    remedy: str = ""


def _mask_secret(raw: Any) -> tuple[Any, bool]:
    """(valore_mascherato, valido) per un segreto: mai il valore reale."""
    present = bool(str(raw or "").strip())
    return (_MASK_SET if present else _MASK_UNSET), present


def readiness_report(
    runtime,
    *,
    execution_mode: Optional[str] = None,
    live_enabled: Optional[bool] = None,
    live_readiness_ok: Optional[bool] = None,
) -> dict:
    """Checklist ✅/❌ dei prerequisiti LIVE, riusando `evaluate_live_readiness`.

    NON reimplementa ne' modifica la logica del gate (#350): chiama l'autorita'
    unica e mappa i suoi `blockers` sulla checklist ordinata dei prerequisiti,
    ciascuno con il rimedio da `BLOCKER_REMEDIATION`. Ritorna `ready`/`level`
    grezzi del gate piu' la lista `items` (ReadinessItem) e i `details`.
    """
    res = runtime.evaluate_live_readiness(
        execution_mode=execution_mode,
        live_enabled=live_enabled,
        live_readiness_ok=live_readiness_ok,
    )
    blockers = set(res.get("blockers") or [])
    items: list[ReadinessItem] = []
    mapped_codes = {code for _, _, code in _LIVE_PREREQUISITES}
    for key, label, code in _LIVE_PREREQUISITES:
        ok = code not in blockers
        remedy = "" if ok else BLOCKER_REMEDIATION.get(code, ("", ""))[1]
        items.append(
            ReadinessItem(key=key, label=label, ok=ok, blocker="" if ok else code, remedy=remedy)
        )
    # Fail-closed: ogni blocker emesso dal gate ma NON mappato tra i prerequisiti
    # noti diventa comunque un item ❌ esplicito. Senza questo, un blocker nuovo
    # introdotto dal gate darebbe una checklist tutta ✅ pur con ready=False
    # (fail-open nella presentazione: la GUI/headless mostrerebbe "tutto ok").
    for code in sorted(blockers - mapped_codes):
        desc, remedy = BLOCKER_REMEDIATION.get(
            code, (code, "Blocker non catalogato: consulta il deploy gate/log.")
        )
        items.append(
            ReadinessItem(key=f"blocker.{code}", label=desc or code, ok=False, blocker=code, remedy=remedy)
        )
    return {
        "ready": bool(res.get("ready", False)),
        "level": str(res.get("level", "NOT_READY")),
        "blockers": sorted(blockers),
        "items": items,
        "details": res.get("details", {}),
    }


class ConfigRegistry:
    """Fonte-dato UNICA dei parametri configurabili.

    Sola lettura: legge da `settings_service` (DB) e da `trading_config`
    (costanti). NON scrive, NON modifica i gate. I segreti sono mascherati.
    `runtime` (opzionale) serve solo a leggere i campi hard-stop gia' caricati
    in `runtime.config` e per `readiness_report`.
    """

    def __init__(self, settings_service, runtime=None):
        self._settings = settings_service
        self._runtime = runtime

    # -- API pubblica ------------------------------------------------------

    def entries(self) -> list[ConfigEntry]:
        """Tutti i parametri enumerati, per dominio, nell'ordine di display."""
        out: list[ConfigEntry] = []
        out.extend(self._execution_entries())
        out.extend(self._betfair_entries())
        out.extend(self._trading_entries())
        out.extend(self._telegram_entries())
        out.extend(self._simulation_entries())
        return out

    def readiness(
        self,
        *,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
    ) -> dict:
        """Checklist go-live via il runtime associato (richiede `runtime`)."""
        if self._runtime is None:
            raise ValueError("ConfigRegistry.readiness richiede un runtime associato")
        return readiness_report(
            self._runtime,
            execution_mode=execution_mode,
            live_enabled=live_enabled,
            live_readiness_ok=live_readiness_ok,
        )

    # -- Enumerazione per dominio -----------------------------------------

    def _read(self, name: str, default: Any = None) -> tuple[Any, bool]:
        """(valore, letto_ok). `letto_ok=False` se il loader manca o SOLLEVA.

        L'errore di lettura NON viene mascherato da un default plausibile e viene
        loggato: cosi' i campi safety-critical possono marcarsi non-validi invece
        di mostrare un valore falso (es. "SIMULATION valido" mentre il DB e' giu'
        e il sistema e' in LIVE). Rilievo Fable 5 / Codacy.
        """
        loader = getattr(self._settings, name, None)
        if not callable(loader):
            return default, False
        try:
            return loader(), True
        except Exception as exc:
            # Sicurezza: NON loggare traceback (exc_info) ne' il messaggio
            # dell'eccezione. I loader dei segreti (betfair_config, password)
            # potrebbero includere credenziali o materiale di decrittazione nel
            # messaggio/traceback. Si logga solo nome loader + tipo eccezione,
            # sufficiente per la diagnosi senza rischio di leak. Rilievo GPT-5.6 Terra.
            logger.warning("ConfigRegistry: lettura di '%s' fallita (%s)", name, type(exc).__name__)
            return default, False

    def _call(self, name: str, default: Any = None) -> Any:
        """Come `_read` ma ritorna solo il valore (per campi non safety-critical,
        dove un errore di lettura si riflette gia' in value/valid)."""
        value, _ok = self._read(name, default)
        return value

    _READ_ERROR_VALUE = "(errore lettura)"
    _READ_ERROR_REMEDY = "Errore di lettura della configurazione: verifica il DB/settings service."

    def _safety_entry(self, key: str, label: str, value: Any, read_ok: bool, *, valid: bool, remedy: str) -> ConfigEntry:
        """ConfigEntry per un campo safety-critical (required_for_live).

        Su errore di lettura (`read_ok=False`) forza il fail-closed: valore
        "(errore lettura)", non valido, con rimedio dedicato — cosi' un backend
        rotto non appare mai come un valore reale valido (rilievo Fable/Codacy).
        """
        if not read_ok:
            return ConfigEntry(
                key=key,
                label=label,
                value=self._READ_ERROR_VALUE,
                valid=False,
                required_for_live=True,
                source="DB",
                remedy=self._READ_ERROR_REMEDY,
            )
        return ConfigEntry(
            key=key, label=label, value=value, valid=valid, required_for_live=True, source="DB", remedy=remedy
        )

    def _execution_entries(self) -> list[ConfigEntry]:
        mode_raw, mode_ok = self._read("load_execution_mode", "SIMULATION")
        le_raw, le_ok = self._read("load_live_enabled", False)
        lro_raw, lro_ok = self._read("load_live_readiness_ok", False)
        level_raw, level_ok = self._read("load_live_readiness_level", "UNKNOWN")
        level = str(level_raw or "UNKNOWN") if level_ok else self._READ_ERROR_VALUE
        ks_raw, ks_ok = self._read("load_kill_switch", False)

        mode = str(mode_raw or "SIMULATION")
        mode_valid = mode in {"SIMULATION", "LIVE"}
        live_enabled = bool(le_raw)
        live_readiness_ok = bool(lro_raw)
        kill_switch = bool(ks_raw)

        return [
            self._safety_entry(
                "execution_mode", "Modalita' di esecuzione", mode, mode_ok,
                valid=mode_valid,
                remedy="" if mode_valid else BLOCKER_REMEDIATION["INVALID_EXECUTION_MODE"][1],
            ),
            self._safety_entry(
                "live_enabled", "Live abilitato", live_enabled, le_ok,
                valid=True,
                remedy="" if live_enabled else BLOCKER_REMEDIATION["LIVE_NOT_ENABLED"][1],
            ),
            self._safety_entry(
                "live_readiness_ok", "Readiness LIVE confermata", live_readiness_ok, lro_ok,
                valid=True,
                remedy="" if live_readiness_ok else BLOCKER_REMEDIATION["LIVE_READINESS_FLAG_NOT_OK"][1],
            ),
            ConfigEntry(
                key="live_readiness_level",
                label="Livello readiness",
                value=level,
                valid=level_ok,
                required_for_live=False,
                source="DB",
            ),
            self._safety_entry(
                "kill_switch", "Kill switch", kill_switch, ks_ok,
                valid=not kill_switch,
                remedy="" if not kill_switch else BLOCKER_REMEDIATION["KILL_SWITCH_ACTIVE"][1],
            ),
        ]

    def _betfair_entries(self) -> list[ConfigEntry]:
        # None-safe: se il loader manca/fallisce, un namespace vuoto evita
        # AttributeError e le entry risultano "(non impostato)" (rilievo Codacy).
        cfg = self._call("load_betfair_config") or SimpleNamespace()
        password = self._call("load_password", "")
        entries: list[ConfigEntry] = []
        username = str(getattr(cfg, "username", "") or "")
        entries.append(
            ConfigEntry(
                key="betfair.username",
                label="Betfair username",
                value=username or _MASK_UNSET,
                valid=bool(username),
                required_for_live=True,
                source="DB",
                remedy="" if username else "Imposta lo username Betfair.",
            )
        )
        for attr, label in (
            ("app_key", "Betfair app key"),
            ("certificate", "Certificato Betfair"),
            ("private_key", "Chiave privata Betfair"),
        ):
            masked, present = _mask_secret(getattr(cfg, attr, ""))
            entries.append(
                ConfigEntry(
                    key=f"betfair.{attr}",
                    label=label,
                    value=masked,
                    valid=present,
                    required_for_live=True,
                    source="DB",
                    remedy="" if present else f"Imposta {label.lower()}.",
                    is_secret=True,
                )
            )
        masked_pwd, pwd_present = _mask_secret(password)
        entries.append(
            ConfigEntry(
                key="betfair.password",
                label="Password Betfair",
                value=masked_pwd,
                valid=pwd_present,
                required_for_live=True,
                source="DB",
                remedy="" if pwd_present else "Imposta la password Betfair.",
                is_secret=True,
            )
        )
        return entries

    def _trading_entries(self) -> list[ConfigEntry]:
        entries: list[ConfigEntry] = []
        for const, label in (
            ("MIN_STAKE", "Stake minimo (floor software)"),
            ("MAX_WIN", "Vincita massima"),
            ("MAX_STAKE_PCT", "Max % stake su balance"),
            ("MAX_SPREAD_TICKS", "Spread massimo (tick)"),
        ):
            value = getattr(trading_config, const, None)
            entries.append(
                ConfigEntry(
                    key=f"trading.{const}",
                    label=label,
                    value=value,
                    valid=value is not None,
                    required_for_live=False,
                    source="code",
                )
            )
        # Hard-stop giornaliero: campi su runtime.config. La validazione DEVE
        # combaciare con `_validate_live_hard_stop_config` del gate (rilievo
        # Greptile/Codacy): numerico, finito, > 0, e per la % drawdown <= 100.
        # Altrimenti il registry direbbe "valido" mentre il gate blocca LIVE.
        # Rimedio: MISSING se il campo e' assente, INVALID se presente ma non
        # valido.
        cfg = getattr(self._runtime, "config", None)
        for attr, label in (
            ("max_daily_loss", "Hard-stop: perdita giornaliera max"),
            ("max_drawdown_hard_stop_pct", "Hard-stop: drawdown max %"),
            ("max_open_exposure", "Hard-stop: esposizione aperta max"),
        ):
            raw = getattr(cfg, attr, None) if cfg is not None else None
            if raw is None:
                valid = False
                remedy = BLOCKER_REMEDIATION["LIVE_HARD_STOP_CONFIG_MISSING"][1]
            else:
                try:
                    parsed = float(raw)
                    valid = (
                        math.isfinite(parsed)
                        and parsed > 0.0
                        and not (attr == "max_drawdown_hard_stop_pct" and parsed > 100.0)
                    )
                except (TypeError, ValueError):
                    valid = False
                remedy = "" if valid else BLOCKER_REMEDIATION["LIVE_HARD_STOP_CONFIG_INVALID"][1]
            entries.append(
                ConfigEntry(
                    key=f"hard_stop.{attr}",
                    label=label,
                    value=raw,
                    valid=valid,
                    required_for_live=True,
                    source="runtime",
                    remedy=remedy,
                )
            )
        return entries

    def _telegram_entries(self) -> list[ConfigEntry]:
        enabled = bool(self._call("load_telegram_alerts_enabled", False))
        chat_id = str(self._call("load_telegram_alert_chat_id", "") or "")
        severity = str(self._call("load_telegram_alert_min_severity", "WARNING") or "WARNING")
        cooldown = self._call("load_telegram_alert_cooldown_sec", 300)
        return [
            ConfigEntry(
                key="telegram.alerts_enabled",
                label="Telegram alert abilitati",
                value=enabled,
                valid=True,
                required_for_live=False,
                source="DB",
            ),
            ConfigEntry(
                key="telegram.alert_chat_id",
                label="Telegram chat id alert",
                value=chat_id or _MASK_UNSET,
                valid=(not enabled) or bool(chat_id),
                required_for_live=False,
                source="DB",
                remedy="" if (not enabled or chat_id) else "Imposta la chat id Telegram per gli alert.",
            ),
            ConfigEntry(
                key="telegram.alert_min_severity",
                label="Severita' minima alert",
                value=severity,
                valid=True,
                required_for_live=False,
                source="DB",
            ),
            ConfigEntry(
                key="telegram.alert_cooldown_sec",
                label="Cooldown alert (s)",
                value=cooldown,
                valid=True,
                required_for_live=False,
                source="DB",
            ),
        ]

    def _simulation_entries(self) -> list[ConfigEntry]:
        cfg = self._call("load_simulation_config", {}) or {}
        fields = (
            ("starting_balance", "Bankroll iniziale simulazione"),
            ("commission_pct", "Commissione simulazione %"),
            ("partial_fill_enabled", "Partial fill simulazione"),
            ("consume_liquidity", "Consumo liquidita' simulazione"),
            ("persist_state", "Persistenza stato simulazione"),
        )
        return [
            ConfigEntry(
                key=f"simulation.{name}",
                label=label,
                value=cfg.get(name),
                valid=cfg.get(name) is not None,
                required_for_live=False,
                source="DB",
            )
            for name, label in fields
        ]
