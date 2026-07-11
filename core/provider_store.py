"""Anagrafica Provider: lista di nomi provider salvati dall'utente, riutilizzabili nel
Parser Personalizzato (colonna `Provider`) tramite menu a tendina, così non si digita
il nome a mano ogni volta (ed è coerente col filtro Provider dell'azione XTrader).

Logica PURA su un `dict` di config (chiave `providers`): nessuna GUI, nessun I/O — la
persistenza è del chiamante (`config_store.save_config`), come per `parser_manager`.
Funzioni immutabili: ritornano una COPIA della config, non mutano l'originale.
"""


def provider_names(cfg: dict) -> list:
    """Nomi provider salvati (config `providers`), ripuliti, deduplicati e ordinati
    (case-insensitive). Valori vuoti/non-stringa scartati. Lista per i menu a tendina."""
    raw = (cfg or {}).get("providers", [])
    if not isinstance(raw, (list, tuple)):
        return []
    seen = set()
    names = []
    for v in raw:
        s = str(v if v is not None else "").strip()
        if s and s.casefold() not in seen:
            seen.add(s.casefold())
            names.append(s)
    return sorted(names, key=str.casefold)


def add_provider(cfg: dict, name: str) -> dict:
    """Copia di `cfg` con `name` aggiunto all'anagrafica (ripulito; nessun duplicato,
    confronto case-insensitive). Un nome vuoto è ignorato (config invariata)."""
    out = dict(cfg or {})
    s = str(name or "").strip()
    current = [v for v in (str(x or "").strip() for x in out.get("providers", []) or []) if v]
    if s and s.casefold() not in {c.casefold() for c in current}:
        current.append(s)
    out["providers"] = current
    return out


def remove_provider(cfg: dict, name: str) -> dict:
    """Copia di `cfg` senza `name` (confronto case-insensitive)."""
    out = dict(cfg or {})
    s = str(name or "").strip().casefold()
    out["providers"] = [v for v in (str(x or "").strip() for x in out.get("providers", []) or [])
                        if v and v.casefold() != s]
    return out
