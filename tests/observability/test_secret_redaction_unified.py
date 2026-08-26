"""M1 — redazione segreti UNIFICATA su entrambi i path di log.

Prima di questo fix esistevano DUE predicati `_is_sensitive_key` indipendenti,
con set di chiavi divergenti:
- `telegram_sanitizer._is_sensitive_key` (path log/alert Telegram)
- `observability.sanitizers._is_sensitive_key` (snapshot + bundle diagnostici)

La divergenza faceva trapelare credenziali reali su UN path mentre l'altro le
oscurava. Esempi materiali (BLOCK di questo test):
- `bot_token` (token del bot Telegram) NON era oscurato dal path osservabilità
  → sarebbe finito in chiaro in un bundle diagnostico condivisibile;
- `app_key` (App Key Betfair) NON era oscurato dal path Telegram.

Questo test PASSA solo con il predicato condiviso (`core.redaction`): ogni
segreto è oscurato su ENTRAMBI i path; i campi non-segreti restano intatti su
entrambi (nessuna sovra-redazione). Copre anche le chiavi in camelCase (tipiche
delle API Betfair/Telethon), che il predicato normalizza prima del match.
"""

import pytest

from observability.sanitizers import sanitize_value
from telegram_sanitizer import sanitize_telegram_payload

_SENTINEL = "SUPERSECRET_VALUE_12345"

# Unione dei segreti che DEVONO essere oscurati su entrambi i path.
CRITICAL_SECRET_KEYS = [
    # coperti storicamente da telegram_sanitizer ma NON da observability:
    "bot_token", "auth_token", "access_token", "bearer", "session",
    "auth", "refresh_token", "client_secret", "api_secret",
    "authorization_header", "user_session",
    # coperti storicamente da observability ma NON da telegram_sanitizer:
    "app_key", "ssoid", "session_string", "api_hash", "api_id",
    "telegram_token", "passwd", "certificate", "cert", "cookie",
    # coperti da entrambi (non devono regredire):
    "password", "secret", "token", "session_token", "private_key",
    "authorization", "api_key",
]

# Campi NON segreti: non devono MAI essere oscurati (parità col comportamento
# esistente, nessuna sovra-redazione su chiavi legittime).
NON_SECRET_KEYS = [
    "username", "market_id", "selection_id", "stake", "price",
    "event_name", "phone_number", "chat_id", "alerts_enabled",
    "simulation_mode", "execution_mode", "runner_name", "market_name",
]

# Segreti in camelCase: le API Betfair/Telethon usano chiavi come `appKey`,
# `sessionToken`, `botToken`. Il predicato normalizza il camelCase prima del
# match (es. `botToken` -> `bot_token`), quindi NON devono trapelare su nessun
# path. BLOCK: pre-fix `_SPLIT` non separava il camelCase -> segreto in chiaro.
CRITICAL_SECRET_KEYS_CAMEL = [
    "botToken", "appKey", "sessionString", "apiKey", "sessionToken",
    "accessToken", "refreshToken", "clientSecret", "userSession", "apiSecret",
    "authToken",
]

# camelCase legittimo (non segreto): NON deve essere sovra-redatto dopo la
# normalizzazione (es. `marketId` -> `market_id`, `apiVersion` -> `api_version`).
NON_SECRET_KEYS_CAMEL = [
    "marketId", "selectionId", "eventName", "runnerName", "apiVersion",
]

# Segreti tutto-maiuscolo ATTACCATI (senza separatori): il camelCase-split non li
# tocca (nessuna transizione minuscola->maiuscola); la forma collassata li cattura.
# BLOCK: pre-fix restavano un unico frammento e trapelavano su entrambi i path.
CRITICAL_SECRET_KEYS_UPPER = [
    "APIKEY", "SESSIONTOKEN", "BOTTOKEN", "APPKEY", "ACCESSTOKEN",
    "CLIENTSECRET", "REFRESHTOKEN", "PRIVATEKEY", "APISECRET", "SESSIONSTRING",
]

# Tutto-maiuscolo legittimo (non segreto): NON deve essere sovra-redatto dalla
# forma collassata (es. `MARKETID` -> `marketid`, non nell'unione).
NON_SECRET_KEYS_UPPER = [
    "MARKETID", "SELECTIONID", "EVENTNAME", "STAKE", "PRICE",
]


def _redacted(sanitizer, key):
    out = sanitizer({key: _SENTINEL})
    # marker-agnostico: conta solo che il valore segreto non compaia in chiaro.
    return out.get(key) != _SENTINEL and _SENTINEL not in str(out)


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", CRITICAL_SECRET_KEYS)
def test_secret_redacted_on_telegram_path(key):
    assert _redacted(sanitize_telegram_payload, key), (
        f"{key!r} trapela sul path Telegram"
    )


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", CRITICAL_SECRET_KEYS)
def test_secret_redacted_on_observability_path(key):
    assert _redacted(sanitize_value, key), (
        f"{key!r} trapela sul path osservabilità"
    )


@pytest.mark.unit
@pytest.mark.guardrail
def test_bot_token_redacted_on_observability_path():
    # BLOCK: pre-fix observability NON oscurava bot_token → leak in bundle diagnostico.
    out = sanitize_value({"bot_token": "123456:ABCDEF"})
    assert out["bot_token"] != "123456:ABCDEF"


@pytest.mark.unit
@pytest.mark.guardrail
def test_app_key_redacted_on_telegram_path():
    # BLOCK: pre-fix telegram_sanitizer NON oscurava app_key (App Key Betfair).
    out = sanitize_telegram_payload({"app_key": "BF_APP_KEY"})
    assert out["app_key"] != "BF_APP_KEY"


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", NON_SECRET_KEYS)
def test_non_secret_not_redacted_both_paths(key):
    assert sanitize_value({key: _SENTINEL})[key] == _SENTINEL, (
        f"{key!r} sovra-redatto sul path osservabilità"
    )
    assert sanitize_telegram_payload({key: _SENTINEL})[key] == _SENTINEL, (
        f"{key!r} sovra-redatto sul path Telegram"
    )


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", CRITICAL_SECRET_KEYS + NON_SECRET_KEYS)
def test_both_paths_agree(key):
    # I due path devono prendere la STESSA decisione su ogni chiave (no divergenza).
    tg = _redacted(sanitize_telegram_payload, key)
    obs = _redacted(sanitize_value, key)
    assert tg == obs, f"divergenza sul path per {key!r}: telegram={tg} observability={obs}"


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", CRITICAL_SECRET_KEYS_CAMEL)
def test_camelcase_secret_redacted_both_paths(key):
    # BLOCK: pre-fix il camelCase non veniva separato -> segreto Betfair/Telegram
    # in chiaro su entrambi i path nonostante l'unificazione.
    assert _redacted(sanitize_telegram_payload, key), (
        f"{key!r} (camelCase) trapela sul path Telegram"
    )
    assert _redacted(sanitize_value, key), (
        f"{key!r} (camelCase) trapela sul path osservabilità"
    )


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", NON_SECRET_KEYS_CAMEL)
def test_camelcase_non_secret_not_redacted_both_paths(key):
    # La normalizzazione camelCase non deve sovra-redigere chiavi legittime.
    assert sanitize_value({key: _SENTINEL})[key] == _SENTINEL, (
        f"{key!r} (camelCase) sovra-redatto sul path osservabilità"
    )
    assert sanitize_telegram_payload({key: _SENTINEL})[key] == _SENTINEL, (
        f"{key!r} (camelCase) sovra-redatto sul path Telegram"
    )


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", CRITICAL_SECRET_KEYS_UPPER)
def test_uppercase_secret_redacted_both_paths(key):
    # BLOCK: pre-fix le chiavi tutto-maiuscolo attaccate restavano un unico
    # frammento -> segreto in chiaro su entrambi i path.
    assert _redacted(sanitize_telegram_payload, key), (
        f"{key!r} (ALL-CAPS) trapela sul path Telegram"
    )
    assert _redacted(sanitize_value, key), (
        f"{key!r} (ALL-CAPS) trapela sul path osservabilità"
    )


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", NON_SECRET_KEYS_UPPER)
def test_uppercase_non_secret_not_redacted_both_paths(key):
    # La forma collassata non deve sovra-redigere chiavi legittime tutto-maiuscolo.
    assert sanitize_value({key: _SENTINEL})[key] == _SENTINEL, (
        f"{key!r} (ALL-CAPS) sovra-redatto sul path osservabilità"
    )
    assert sanitize_telegram_payload({key: _SENTINEL})[key] == _SENTINEL, (
        f"{key!r} (ALL-CAPS) sovra-redatto sul path Telegram"
    )
