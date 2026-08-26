"""Predicato UNICO di rilevamento chiavi sensibili per la redazione dei segreti.

Fonte unica condivisa da `telegram_sanitizer` (log/alert Telegram) e
`observability.sanitizers` (snapshot + bundle diagnostici). Prima esistevano due
predicati `_is_sensitive_key` indipendenti con set di chiavi divergenti: un
segreto oscurato su un path poteva trapelare sull'altro (es. `bot_token` in un
bundle diagnostico condivisibile, `app_key`/`ssoid`/`session_string` in un log
Telegram).

Strategia = UNIONE di ciò che i due predicati storici coprivano, **fail-closed**
(sovra-redige piuttosto che lasciar trapelare un segreto):
- match esatto sull'unione dei nomi-chiave;
- match sul suffisso dopo l'ultimo punto (dot-notation, es. `telegram.api_hash`);
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


def is_sensitive_key(key: str) -> bool:
    """True se la chiave va oscurata (unione delle strategie storiche, fail-closed)."""
    key_l = str(key or "").lower()
    if key_l in SENSITIVE_KEYS_EXACT:
        return True
    # dot-notation: suffisso dopo l'ultimo punto (es. "telegram.api_hash" -> "api_hash")
    if key_l.rsplit(".", 1)[-1] in SENSITIVE_KEY_SUFFIXES:
        return True
    # combo: ≥2 frammenti sensibili in una chiave multi-parte
    parts = [p for p in _SPLIT.split(key_l) if p]
    if len(parts) > 1 and sum(1 for p in parts if p in SENSITIVE_KEY_FRAGMENTS) >= 2:
        return True
    return False
