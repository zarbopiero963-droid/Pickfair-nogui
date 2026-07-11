"""A3: profili di impostazioni con nome (salva/carica più setup riusabili).

Un "profilo" è uno snapshot della configurazione salvato con un nome in
`<config_dir>/profiles/<nome>.json`, così l'utente può tenere più setup
(es. "Prematch", "Live", "Test") e ripristinarli senza editare `config.json` a mano.

SICUREZZA — niente segreti nei profili: `bot_token` (la credenziale Telegram) NON
viene mai scritto in un profilo (`SECRET_KEYS`). Caricando un profilo il token
attuale resta intatto (`apply_profile`). I file profilo vivono nella cartella utente
persistente (`%APPDATA%\\XTraderBridge\\profiles`), MAI nel repository.

Funzioni pure su `dict` + I/O su file (scrittura atomica), sul modello provato di
`custom_parser`. NESSUNA GUI e NESSUN auto-apply al runtime: applicare un profilo
alla config viva è uno step esplicito del chiamante (`apply_profile` +
`config_store.save_config`), con la stessa validazione di sempre.
"""

import json
import os

from . import atomic_io, config_store, validators

# Chiavi MAI salvate in un profilo: la credenziale Telegram e il sentinel di stato del
# suo storage (`bot_token_storage`). Un profilo deve poter essere ripristinato/condiviso
# senza trascinarsi dietro il token né lo stato del keyring (accoppiato alla macchina);
# entrambi vivono solo nella config principale e `apply_profile` li preserva.
SECRET_KEYS = ("bot_token", "bot_token_storage")


def profiles_dir() -> str:
    """Cartella persistente dei profili: `<config_dir>/profiles/`.

    Riusa `config_store.config_dir()` (`%APPDATA%\\XTraderBridge` su Windows,
    `~/.config/XTraderBridge` altrove): posizione scrivibile e persistente, fuori dal
    repo e dalla cartella temporanea read-only dell'EXE."""
    return os.path.join(config_store.config_dir(), "profiles")


def _safe_filename(name: str) -> str:
    """Nome file sicuro dal nome profilo (nucleo condiviso `validators`): solo
    alfanumerici, '-', '_' e spazi (poi spazi → '_'); path traversal e NOMI DEVICE
    RISERVATI Windows gestiti. Ritorna ``""`` se il nome si pulisce a vuoto, così
    `save_profile` lo RIFIUTA (diverso da `custom_parser`, che ripiega su un default):
    fallback per-dominio voluto, ma il nucleo di sanitizzazione è unico (anti-drift)."""
    return validators.safe_filename_core(name)


def profile_path(name: str, dir_path: str = None) -> str:
    base = dir_path if dir_path is not None else profiles_dir()
    return os.path.join(base, _safe_filename(name) + ".json")


def _strip_secrets(cfg: dict) -> dict:
    """Copia della config senza le chiavi segrete (`SECRET_KEYS`) né i marker SOLO-IN-RAM
    (`config_store.RAM_ONLY_KEYS`, es. `_post_corruption`/`_token_load_incomplete`).

    I marker RAM-only sono flag interni che `load_config` può mettere nella config viva: non
    devono mai finire nel JSON di un profilo (Codex #256). Caricati in un secondo momento su uno
    stato diverso del token falserebbero la logica clear/preserve (es. far preservare/resuscitare
    un token che la config corrente — sentinel "none" — teneva cancellato). Usato sia in scrittura
    (`save_profile`) sia in lettura/merge (`apply_profile`), così anche un profilo VECCHIO che già
    contenesse il flag viene ripulito al caricamento."""
    drop = set(SECRET_KEYS) | set(config_store.RAM_ONLY_KEYS)
    return {k: v for k, v in (cfg or {}).items() if k not in drop}


def _clean_name_or_raise(name: str) -> str:
    """Nome profilo non vuoto dopo la sanitizzazione, o `ValueError`. Fonte unica usata
    da save/load così un nome vuoto/non valido NON viene mai mappato sul file `.json`
    (che colpirebbe un file non voluto, finding Sourcery)."""
    clean = str(name or "").strip()
    if not _safe_filename(clean):
        raise ValueError("Nome profilo non valido (vuoto dopo la sanitizzazione).")
    return clean


def _read_profile_name(path: str):
    """Nome reale salvato dentro il file profilo, o None se illeggibile/corrotto."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    if isinstance(data, dict):
        return str(data.get("name", "")) or None
    return None


def ensure_valid_new_name(name: str, dir_path: str = None) -> str:
    """Valida il nome profilo SENZA scrivere nulla: ritorna il nome pulito o solleva
    `ValueError` se è vuoto/non valido oppure se **collide** col filename di un profilo
    *diverso* (es. "Live!" vs "Live"). Fonte unica usata sia da `save_profile` sia dalla
    GUI per validare PRIMA di persistere il form (così un nome cattivo non commette mai
    impostazioni safety-critical, finding Codex)."""
    clean = _clean_name_or_raise(name)
    path = profile_path(clean, dir_path)
    if os.path.exists(path):
        existing = _read_profile_name(path)
        if existing is not None and existing != clean:
            raise ValueError(
                f"Il nome {clean!r} collide con il profilo {existing!r} "
                f"(stesso file {os.path.basename(path)}): scegli un nome diverso."
            )
    return clean


def save_profile(name: str, cfg: dict, dir_path: str = None) -> str:
    """Salva la config (SENZA segreti) come profilo `<dir>/<nome>.json`.

    Rifiuta un nome che si riduce a vuoto dopo la sanitizzazione o che collide col
    filename di un profilo diverso (`ensure_valid_new_name`): sovrascrivere è consentito
    solo per lo *stesso* profilo (update). Scrittura atomica (tmp + fsync + rename): un
    crash a metà non lascia un JSON parziale e non distrugge il profilo esistente."""
    base = dir_path if dir_path is not None else profiles_dir()
    clean_name = ensure_valid_new_name(name, base)
    os.makedirs(base, exist_ok=True)
    path = profile_path(clean_name, base)
    payload = json.dumps(
        {"name": clean_name, "config": _strip_secrets(cfg)},
        ensure_ascii=False, indent=2,
    )
    atomic_io.atomic_write_text(path, payload, prefix=".profile_", suffix=".json")
    return path


def load_profile(name: str, dir_path: str = None) -> dict:
    """Config (SENZA segreti) salvata nel profilo `name`.

    Solleva `ValueError` se il nome è vuoto/non valido o il file è corrotto/non nel
    formato atteso, `FileNotFoundError` se il profilo non esiste."""
    path = profile_path(_clean_name_or_raise(name), dir_path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Profilo non trovato: {name!r}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or not isinstance(data.get("config"), dict):
        raise ValueError(f"Profilo corrotto: {os.path.basename(path)}")
    # Difesa in profondità: anche se un file fosse stato editato a mano per inserire un
    # bot_token, NON lo restituiamo (i profili non portano mai segreti).
    return _strip_secrets(data["config"])


def list_profiles(dir_path: str = None) -> list:
    """Nomi dei profili salvati, ordinati case-insensitive.

    Legge il `name` dentro ogni file (non il filename sanitizzato), così l'elenco
    mostra il nome reale. Ignora i temporanei `.profile_*` e i file
    corrotti/senza nome (non devono apparire come "fantasmi")."""
    base = dir_path if dir_path is not None else profiles_dir()
    if not os.path.isdir(base):
        return []
    names = []
    for f in os.listdir(base):
        if not f.endswith(".json") or f.startswith("."):
            continue
        nm = _read_profile_name(os.path.join(base, f))
        if nm:
            names.append(nm)
    return sorted(names, key=str.lower)


def delete_profile(name: str, dir_path: str = None) -> bool:
    """Elimina il file del profilo. True se rimosso, False se non esisteva (o nome non
    valido: non si tenta mai di rimuovere un `.json` derivato da nome vuoto)."""
    if not _safe_filename(str(name or "").strip()):
        return False
    try:
        os.remove(profile_path(name, dir_path))
        return True
    except FileNotFoundError:
        return False


def apply_profile(current_cfg: dict, profile_cfg: dict) -> dict:
    """Fonde un profilo sulla config viva PRESERVANDO i segreti correnti.

    Ritorna una NUOVA `dict` = `current_cfg` con sopra le chiavi del profilo, MENO i
    segreti (`SECRET_KEYS`): così caricare un profilo non sovrascrive mai il
    `bot_token` attuale (il profilo non lo contiene, e per sicurezza lo togliamo
    comunque qui). Funzione pura: non scrive su disco né tocca il runtime."""
    merged = dict(current_cfg) if isinstance(current_cfg, dict) else {}
    merged.update(_strip_secrets(profile_cfg))
    return merged
