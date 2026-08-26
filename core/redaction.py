"""Predicato UNICO di rilevamento chiavi sensibili per la redazione dei segreti.

Fonte unica condivisa da `telegram_sanitizer` (log/alert Telegram) e
`observability.sanitizers` (snapshot + bundle diagnostici). Prima esistevano due
predicati `_is_sensitive_key` indipendenti con set di chiavi divergenti: un
segreto oscurato su un path poteva trapelare sull'altro (es. `bot_token` in un
bundle diagnostico condivisibile, `app_key`/`ssoid`/`session_string` in un log
Telegram).

Strategia = UNIONE di ciò che i due predicati storici coprivano, **fail-closed**
(sovra-redige piuttosto che lasciar trapelare un segreto). La chiave è prima
NORMALIZZATA inserendo `_` ai confini camelCase (`sessionToken` -> `session_token`)
e minuscolando, così le chiavi camelCase delle API Betfair/Telethon ricadono
nelle stesse strategie della forma snake_case; poi:
- match esatto sull'unione dei nomi-chiave;
- match sul suffisso dopo l'ultimo punto (dot-notation, es. `telegram.apiHash`);
- combo a ≥2 frammenti sensibili in una chiave multi-parte (es. `api_token`).

Dipende solo dalla stdlib (`re`): nessun import di runtime, nessun ciclo.
(Nome del modulo `redaction` e non `secret_redaction`: `.gitignore` ha la regola
`secret*` che escluderebbe dal commit un file con quel prefisso.)
"""

from __future__ import annotations

import re

# Unione dei nomi-chiave esatti storicamente coperti dai due sanitizer.
# (telegram_sanitizer.TELEGRAM_SENSITIVE_KEYS ∪ observability.SENSITIVE_KEYS)
SENSITIVE_KEYS_EXACT: frozenset = frozenset({
    # storicamente da telegram_sanitizer
    "token", "auth_token", "access_token", "bearer", "user_session",
    "session", "session_token", "api_key", "secret", "password",
    "authorization", "auth", "refresh_token", "bot_token", "client_secret",
    "private_key", "api_secret", "authorization_header",
    # storicamente da observability.sanitizers
    "passwd", "session_string", "app_key", "certificate", "cert",
    "cookie", "telegram_token", "api_hash", "api_id", "ssoid",
})

# Frammenti sensibili: ≥2 in una chiave multi-parte ⇒ sensibile (da telegram_sanitizer).
SENSITIVE_KEY_FRAGMENTS: frozenset = frozenset({
    "token", "secret", "password", "auth", "bearer", "key", "session", "api",
})

# Suffisso dopo l'ultimo punto (dot-notation): l'intera unione esatta vale come
# suffisso (superset dei suffissi storici dell'osservabilità).
SENSITIVE_KEY_SUFFIXES: frozenset = SENSITIVE_KEYS_EXACT

_SPLIT = re.compile(r"[\s_.-]+")

# Confine camelCase / PascalCase: separa `botToken` -> `bot_Token`,
# `appKey` -> `app_Key`, `APIKey` -> `API_Key`. Necessario perché le API
# Betfair/Telethon usano chiavi camelCase: senza normalizzazione un segreto come
# `sessionToken`/`appKey` non matcherebbe né esatto, né suffisso, né combo, e
# trapelerebbe su ENTRAMBI i path (fail-open, contro il contratto fail-closed).
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _normalize(key: str) -> str:
    """Inserisce '_' ai confini camelCase, poi minuscolo.

    `botToken`/`appKey`/`sessionString` -> `bot_token`/`app_key`/`session_string`,
    così un segreto in camelCase ricade nell'unione esatta/suffisso/combo esattamente
    come la forma snake_case. No-op su chiavi già snake_case o tutto-minuscolo/maiuscolo.
    """
    return _CAMEL_BOUNDARY.sub("_", str(key or "")).lower()


def is_sensitive_key(key: str) -> bool:
    """True se la chiave va oscurata (unione delle strategie storiche, fail-closed)."""
    norm = _normalize(key)
    if norm in SENSITIVE_KEYS_EXACT:
        return True
    # dot-notation: suffisso dopo l'ultimo punto (es. "telegram.apiHash" -> "api_hash")
    if norm.rsplit(".", 1)[-1] in SENSITIVE_KEY_SUFFIXES:
        return True
    # combo: ≥2 frammenti sensibili in una chiave multi-parte
    parts = [p for p in _SPLIT.split(norm) if p]
    if len(parts) > 1 and sum(1 for p in parts if p in SENSITIVE_KEY_FRAGMENTS) >= 2:
        return True
    return False
