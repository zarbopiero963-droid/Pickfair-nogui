"""PR-13: controller delle impostazioni avanzate (logica pura, testabile in CI).

Espone alla GUI le impostazioni oggi modificabili solo a mano in `config.json`,
così l'utente non deve editare il file per i toggle critici (in primis `dry_run`).
Niente widget customtkinter: solo opzioni per i menu, lettura dei valori correnti
dalla config e **validazione + merge** robusti, sullo stesso modello del controller
del Parser Personalizzato (CP-06, `parser_builder`).

Impostazioni gestite (tutte già presenti in `config_store.DEFAULTS`):

- `recognition_mode`  (ID_ONLY / NAME_ONLY / BOTH)        — riusa `recognition`
- `queue_mode`        (OVERWRITE_LAST / APPEND_ACTIVE / …) — riusa `signal_queue`
- `dry_run`           (bool, simulazione)                  — riusa `safety_guard`
- `max_per_day`       (intero > 0)                         — riusa `safety_guard`
- `xtrader_notification_chat_id` (str, chat conferme XTrader)
- `confirmation_timeout`  (intero > 0, secondi)            — collegato al runtime (PR-17b)
- `confirmation_keywords` / `rejection_keywords` (liste)   — parole conferma/rifiuto XTrader

Il merge parte SEMPRE da una copia della config caricata e tocca solo queste
chiavi: ogni altra impostazione (token, chat, sorgenti, parser, ecc.) è preservata.
"""

import copy

from . import autostart, bridge_mode, config_store, recognition, safety_guard, signal_queue

# Default del timeout conferme: fonte unica = config_store.DEFAULTS.
DEFAULT_CONFIRMATION_TIMEOUT = config_store.DEFAULTS["confirmation_timeout"]

# Le chiavi gestite da questo controller (per documentazione/test).
MANAGED_KEYS = (
    "recognition_mode",
    "queue_mode",
    "dry_run",
    "bridge_mode",
    "max_per_day",
    "xtrader_notification_chat_id",
    "confirmation_timeout",
    "confirmation_keywords",
    "rejection_keywords",
    "auto_start_listener",
)


def _keyword_list(value) -> list:
    """Normalizza le keyword a lista di stringhe non vuote. Accetta sia la **stringa
    CSV** dal campo GUI (`"piazzata, ok"`) sia una **lista** dalla config. Vuoto → []."""
    if isinstance(value, str):
        parts = value.split(",")
    elif isinstance(value, (list, tuple)):
        parts = value
    else:
        parts = []
    return [str(p).strip() for p in parts if str(p).strip()]


# ── opzioni per i menu a tendina della GUI ─────────────────────────────────
def recognition_mode_options() -> list:
    """Modalità di riconoscimento XTrader ammesse (ID_ONLY/NAME_ONLY/BOTH)."""
    return list(recognition.VALID_MODES)


def queue_mode_options() -> list:
    """Modalità della coda dei segnali attivi ammesse."""
    return list(signal_queue.MODES)


def bridge_mode_options() -> list:
    """Etichette del selettore «Modalità bridge» (#311 §3.1): Simulazione Bridge /
    Collaudo XTrader / Reale, nell'ordine di `bridge_mode.VALID_MODES`."""
    return bridge_mode.mode_options()


# ── lettura dei valori correnti (per popolare i widget) ────────────────────
def current_values(cfg: dict) -> dict:
    """Valori correnti normalizzati per i widget, ricavati dalla config con gli
    stessi default sicuri usati a runtime (così la GUI mostra ciò che il bridge
    userebbe davvero, non un valore grezzo fuorviante)."""
    cfg = cfg if isinstance(cfg, dict) else {}
    # Uppercase/strip prima di normalizzare: recognition.normalize_mode è
    # case-sensitive, quindi un "both" salvato a mano ricadrebbe sul default.
    rec = str(cfg.get("recognition_mode", recognition.DEFAULT_MODE) or "").strip().upper()
    qm = str(cfg.get("queue_mode", "") or "").strip().upper()
    return {
        "recognition_mode": recognition.normalize_mode(rec),
        "queue_mode": signal_queue.normalize_mode(qm),
        "dry_run": safety_guard.is_dry_run(cfg),
        # Modalità nominata (#311 §3.1): etichetta della tendina per il modo EFFETTIVO
        # (mode_from_cfg: dry_run autoritativo, config incoerente → Simulazione).
        "bridge_mode": bridge_mode.label_for(bridge_mode.mode_from_cfg(cfg)),
        "max_per_day": _coerce_int_display(cfg.get("max_per_day"), safety_guard.DEFAULT_MAX_PER_DAY),
        # Tetto righe attive simultanee (#136 p5): intero >= 1, default dai DEFAULTS.
        "max_active_signals": _coerce_int_display(
            cfg.get("max_active_signals"), config_store.DEFAULTS["max_active_signals"]),
        "xtrader_notification_chat_id": str(cfg.get("xtrader_notification_chat_id", "") or "").strip(),
        "confirmation_timeout": _coerce_int_display(
            cfg.get("confirmation_timeout"), DEFAULT_CONFIRMATION_TIMEOUT),
        # Keyword come stringa CSV per il campo di testo della GUI ("kw1, kw2").
        "confirmation_keywords": ", ".join(_keyword_list(cfg.get("confirmation_keywords"))),
        "rejection_keywords": ", ".join(_keyword_list(cfg.get("rejection_keywords"))),
        # Coerente col runtime: stessa logica fail-closed di autostart (un valore
        # malformato/None NON deve mostrare il toggle come attivo).
        "auto_start_listener": autostart.is_enabled(cfg),
        # Privacy log: default OFF (solo truthy esplicito mostra il toggle come attivo).
        # Helper fail-closed unico: None/`null`/vuoto → False.
        "debug_message_payload": _as_bool_optin(cfg.get("debug_message_payload")),
    }


def _coerce_int_display(value, default: int) -> int:
    """Intero > 0 per la visualizzazione: un valore non valido/assente in config
    ricade sul default, così il campo mostra sempre un numero sensato. Rifiuta i
    bool (un `True`/`False` da JSON non è un conteggio).

    NON tronca: un numero non intero (es. `1.5`) o `<= 0` ricade sul default invece
    di diventare un limite valido diverso (`1`), allineandosi a come il
    `DailyLimiter` runtime tratta i valori malformati (finding Codex P2)."""
    if isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if not f.is_integer() or f <= 0:
        return default
    return int(f)


# ── validazione + merge ────────────────────────────────────────────────────
def apply_advanced(cfg: dict, form: dict) -> tuple:
    """Valida i valori del form e li fonde su una COPIA della config.

    `form` accetta valori grezzi (stringhe dai widget o bool dai checkbox):

    - `recognition_mode` / `queue_mode`: devono stare tra le opzioni ammesse;
    - `require_price` / `dry_run`: bool (o stringa truthy/falsey);
    - `max_per_day`: intero > 0;
    - `xtrader_notification_chat_id`: stringa (vuota = conferme disattivate).

    Ritorna `(nuova_cfg, errori)`. Se `errori` non è vuoto, `nuova_cfg` è la config
    di partenza **invariata** (nessun merge parziale: o tutto valido, o niente)."""
    base = copy.deepcopy(cfg) if isinstance(cfg, dict) else {}
    errors = []
    updates = {}

    rec = str(form.get("recognition_mode", "") or "").strip().upper()
    if rec not in recognition.VALID_MODES:
        errors.append(
            f"Modalità riconoscimento non valida {rec!r}; ammesse: "
            f"{', '.join(recognition.VALID_MODES)}.")
    else:
        updates["recognition_mode"] = rec

    qm = str(form.get("queue_mode", "") or "").strip().upper()
    if qm not in signal_queue.MODES:
        errors.append(
            f"Modalità coda non valida {qm!r}; ammesse: {', '.join(signal_queue.MODES)}.")
    else:
        updates["queue_mode"] = qm

    # Modalità bridge (#311 §3.1): il form nuovo manda `bridge_mode` (etichetta della
    # tendina o nome canonico) e `dry_run` viene DERIVATO (SIMULAZIONE ⇔ True) così i
    # due non possono divergere. Valore sconosciuto → errore, nessun merge (mai
    # indovinare una modalità). Retro-compat: un form legacy SENZA `bridge_mode`
    # (test/chiamanti headless) continua a governare il solo `dry_run` come prima —
    # `mode_from_cfg` a valle resta coerente (dry_run autoritativo).
    raw_mode = form.get("bridge_mode")
    if raw_mode is None:
        updates["dry_run"] = _as_bool(form.get("dry_run", True))
        # Coerenza IMMEDIATA anche sul path legacy (Fable #349): senza questo, un form
        # solo-dry_run può persistere la coppia incoerente `dry_run=false` +
        # `bridge_mode:"SIMULAZIONE"` fino alla ricoercion. Ri-derivo qui, stessa
        # regola di `mode_from_cfg` (dry_run autoritativo, COLLAUDO dichiarato preservato).
        merged = dict(base)
        merged.update(updates)
        updates["bridge_mode"] = bridge_mode.mode_from_cfg(merged)
    else:
        mode = bridge_mode.mode_for_form_value(raw_mode)
        if mode is None:
            errors.append(
                f"Modalità bridge non valida {str(raw_mode)!r}; ammesse: "
                f"{', '.join(bridge_mode.VALID_MODES)}.")
        else:
            updates["bridge_mode"] = mode
            updates["dry_run"] = (mode == bridge_mode.SIMULAZIONE)
    # Avvio automatico del listener: default sicuro False (parte solo con START).
    # FAIL-CLOSED al salvataggio (audit #259 C2): la stessa coercizione allowlist del
    # runtime (`autostart.coerce_enabled`), NON `as_bool` — con la denylist un valore
    # malformato («flase») veniva persistito come `True` bool genuino, scavalcando
    # l'allowlist che il runtime applica solo in lettura.
    updates["auto_start_listener"] = autostart.coerce_enabled(
        form.get("auto_start_listener", False))
    # Privacy log: default sicuro False (payload NON loggato in chiaro; opt-in di debug).
    # Helper fail-closed unico: None/vuoto → False.
    updates["debug_message_payload"] = _as_bool_optin(form.get("debug_message_payload"))

    max_day, err = _parse_positive_int(form.get("max_per_day"), "Limite giornaliero")
    if err:
        errors.append(err)
    else:
        updates["max_per_day"] = max_day

    # Tetto righe attive simultanee (#136 p5): intero >= 1. Validato solo se il form lo
    # porta (la GUI lo include sempre); assente → preserva il valore esistente.
    if "max_active_signals" in form:
        max_act, err = _parse_positive_int(form.get("max_active_signals"), "Max segnali attivi")
        if err:
            errors.append(err)
        else:
            updates["max_active_signals"] = max_act

    # La chat notifiche è testo libero: stringa vuota = conferme disattivate.
    updates["xtrader_notification_chat_id"] = str(
        form.get("xtrader_notification_chat_id", "") or "").strip()

    timeout, err = _parse_positive_int(
        form.get("confirmation_timeout"), "Timeout conferme XTrader")
    if err:
        errors.append(err)
    else:
        updates["confirmation_timeout"] = timeout

    # Keyword: il campo GUI è una stringa CSV → lista di parole non vuote (vuoto = []
    # → a runtime normalize_keywords ricade sui default del modulo). Testo libero,
    # nessun errore bloccante.
    updates["confirmation_keywords"] = _keyword_list(form.get("confirmation_keywords"))
    updates["rejection_keywords"] = _keyword_list(form.get("rejection_keywords"))

    if errors:
        return base, errors
    base.update(updates)
    return base, []


# Coercizione robusta a bool: fonte unica condivisa (config_store), per non avere
# versioni divergenti dello stesso helper (feedback Sourcery).
_as_bool = config_store.as_bool
# Variante fail-closed per i flag opt-in di privacy/sicurezza (None/vuoto → False).
_as_bool_optin = config_store.as_bool_optin


def _parse_positive_int(value, label: str):
    """Parser generico di un intero > 0 → `(intero, None)` oppure `(None, messaggio)`.

    Autonomo (non riusa `parse_timeout`, che è semanticamente legato all'auto-clear):
    vuoto, non numerico, decimale o `<= 0` sono errori — questi campi (limite/giorno,
    timeout conferme) non hanno un default "vuoto" sensato in input."""
    s = str(value if value is not None else "").strip()
    try:
        n = int(s)
    except ValueError:
        return None, f"{label}: deve essere un intero maggiore di 0."
    if n <= 0:
        return None, f"{label}: deve essere un intero maggiore di 0."
    return n, None
