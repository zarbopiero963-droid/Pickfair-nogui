"""MCP-01j: `GET /v1/config` su un RuntimeController REALE.

Sola lettura dell'allowlist di ``RoserpinaConfig``; credenziali Betfair/Telegram, certificati e
token non devono mai comparire (nemmeno come nome); valori assenti o illeggibili mai convertiti
in zero; nessun effetto su DB, bus o configurazione.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from control_api import views
from control_api.views import CONFIG_KEYS, OPTIONAL_CONFIG_KEYS, config_view
from core.runtime_controller import RuntimeController
from core.system_state import RiskProfile, RoserpinaConfig

SECRETS = {
    "app_key": "APPKEY-SECRET-1",
    "password": "PASSWORD-SECRET-2",
    "certificate": "-----BEGIN CERTIFICATE-----CERT-SECRET-3",
    "private_key": "-----BEGIN PRIVATE KEY-----PK-SECRET-4",
    "session_token": "SESSION-SECRET-5",
}
TELEGRAM = {"api_id": "777", "api_hash": "HASH-SECRET-6", "session_string": "SS-SECRET-7",
            "bot_token": "123:BOT-SECRET-8"}


def settings(view: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {s["key"]: s for s in view["settings"]}


def test_keys_match_the_contract_and_the_core_dataclass() -> None:
    from pickfair_control_api.contract import load_descriptor

    semantics = load_descriptor()["semantics"]["config"]
    assert semantics["keys"] == CONFIG_KEYS
    assert set(semantics["optional_keys"]) == OPTIONAL_CONFIG_KEYS
    fields = {f.name: f for f in dataclasses.fields(RoserpinaConfig)}
    assert set(CONFIG_KEYS) <= set(fields)  # nessuna chiave inventata
    optional = {n for n, f in fields.items() if "Optional" in str(f.type)}
    assert optional == OPTIONAL_CONFIG_KEYS


def test_real_runtime_values_are_reported_exactly(runtime: RuntimeController) -> None:
    view = config_view(runtime)
    assert view["unavailable"] == [] and view["consistency"] == "SINGLE_REFERENCE_READ"
    assert [s["key"] for s in view["settings"]] == list(CONFIG_KEYS)
    s = settings(view)
    cfg = runtime.config
    assert s["table_count"] == {"key": "table_count", "kind": "INT", "state": "SET",
                                "value": cfg.table_count}
    assert s["risk_profile"]["value"] == cfg.risk_profile.value
    assert s["defense_drawdown_pct"]["value"] == "7.5"
    assert s["expansion_multiplier"]["value"] == "1.1"
    assert s["allow_recovery"]["value"] is False
    # percentuali obbligatorie non configurate sul DB nuovo: NOT_SET, mai 0
    assert s["max_single_bet_pct"] == {"key": "max_single_bet_pct", "kind": "DECIMAL",
                                       "state": "NOT_SET", "value": None}
    assert view["unlisted"] == [] and view["redacted"] == 0


def test_reload_config_is_seen_and_never_written(runtime: RuntimeController) -> None:
    runtime.settings_service.save_settings({"roserpina.max_single_bet_pct": "4.25",
                                            "roserpina.table_count": "3"})
    runtime.reload_config()
    s = settings(config_view(runtime))
    assert s["max_single_bet_pct"]["value"] == "4.25" and s["table_count"]["value"] == 3


def test_invalid_values_are_invalid_never_zero(runtime: RuntimeController) -> None:
    cfg = dataclasses.replace(runtime.config)
    cfg.max_daily_loss = float("nan")
    cfg.commission_pct = None  # campo non opzionale assente: INVALID, non NOT_SET
    cfg.book_block = True      # bool non è un numero
    cfg.table_count = 3.0      # float non è un intero
    cfg.allow_recovery = 1     # int non è un bool
    cfg.risk_profile = "YOLO"
    cfg.min_price = 1e20       # fuori dalla scala del contratto
    runtime.config = cfg
    s = settings(config_view(runtime))
    for key in ("max_daily_loss", "commission_pct", "book_block", "table_count",
                "allow_recovery", "risk_profile", "min_price"):
        assert s[key]["state"] == "INVALID" and s[key]["value"] is None, key


def test_enum_as_plain_string_and_negative_values(runtime: RuntimeController) -> None:
    cfg = dataclasses.replace(runtime.config, risk_profile=RiskProfile.AGGRESSIVE,
                              max_recovery_chase_abs=-1.0)
    runtime.config = cfg
    s = settings(config_view(runtime))
    assert s["risk_profile"]["value"] == "AGGRESSIVE"
    assert s["max_recovery_chase_abs"]["value"] == "-1"  # opt-out del core: riportato com'è


def test_missing_field_is_invalid(runtime: RuntimeController) -> None:
    del runtime.config.__dict__["min_stake"]
    del runtime.config.__dict__["max_win"]  # opzionale MANCANTE: illeggibile, non «non configurato»
    s = settings(config_view(runtime))
    assert s["min_stake"]["state"] == "INVALID" and s["max_win"]["state"] == "INVALID"


def test_bool_is_never_an_int(runtime: RuntimeController) -> None:
    runtime.config.table_count = True
    runtime.config.max_recovery_tables = False
    s = settings(config_view(runtime))
    assert s["table_count"]["state"] == "INVALID" and s["max_recovery_tables"]["state"] == "INVALID"


def test_config_absent_or_unreadable(runtime: RuntimeController) -> None:
    runtime.config = None
    view = config_view(runtime)
    assert view["settings"] is None and view["unavailable"] == ["settings"]
    del runtime.__dict__["config"]
    assert config_view(runtime)["unavailable"] == ["settings"]


def test_credentials_never_leak(runtime: RuntimeController) -> None:
    runtime.db.save_credentials(username="mario", app_key=SECRETS["app_key"],
                                certificate=SECRETS["certificate"],
                                private_key=SECRETS["private_key"])
    runtime.db.save_password(SECRETS["password"])
    runtime.db.save_session(SECRETS["session_token"])
    runtime.db.save_telegram_settings(TELEGRAM)
    runtime.reload_config()
    text = json.dumps(config_view(runtime))
    for secret in [*SECRETS.values(), *TELEGRAM.values(), "mario"]:
        assert secret not in text
    # nomi delle credenziali: nessuno (le chiavi legittime come max_session_loss restano)
    for name in ("app_key", "password", "certificate", "private_key", "session_token",
                 "telegram", "username", "api_hash", "api_id", "bot_token", "session_string"):
        assert name not in text


def test_unknown_fields_are_named_never_valued_and_secrets_not_even_named(
        runtime: RuntimeController) -> None:
    cfg = runtime.config
    extra = {
        "stop_loss_pct": 25.0,               # campo nuovo (es. F-02): solo il nome
        "betfair_app_key": "K-SECRET",       # sensibile: solo contato
        "ApiToken": "T-SECRET",              # camelCase sensibile
        "pin_code": "1234",                  # frammento del contratto
        "Weird Name": "W-SECRET",            # non canonico
        "_private_cache": "P-SECRET",        # privato: ignorato
    }
    cfg.__dict__.update(extra)
    view = config_view(runtime)
    assert view["unlisted"] == ["stop_loss_pct"]
    assert view["redacted"] == 4
    text = json.dumps(view)
    for value in ("K-SECRET", "T-SECRET", "1234", "W-SECRET", "P-SECRET"):
        assert value not in text
    assert all(s["value"] != "25" for s in view["settings"])  # valore del campo nuovo mai esposto
    for name in ("betfair_app_key", "ApiToken", "pin_code", "Weird", "_private_cache"):
        assert name not in text


def test_unlisted_is_capped(runtime: RuntimeController) -> None:
    runtime.config.__dict__.update({f"future_field_{i:03d}": i for i in range(70)})
    view = config_view(runtime)
    assert len(view["unlisted"]) == views.MAX_UNLISTED and view["redacted"] == 6


def test_reads_leave_db_and_config_untouched(runtime: RuntimeController, tmp_path: Path) -> None:
    def digest() -> str:
        h = hashlib.sha256()
        for p in sorted(tmp_path.glob("pickfair.db*")):
            if not p.name.endswith("-shm"):
                h.update(p.name.encode() + p.read_bytes())
        conn = sqlite3.connect(f"file:{tmp_path / 'pickfair.db'}?mode=ro", uri=True)
        try:
            for line in conn.iterdump():
                h.update(line.encode())
        finally:
            conn.close()
        return h.hexdigest()

    before, cfg_before, cfg_ref = digest(), dataclasses.asdict(runtime.config), runtime.config
    published = list(runtime.bus.published)
    for _ in range(20):
        config_view(runtime)
    assert digest() == before
    assert runtime.config is cfg_ref and dataclasses.asdict(runtime.config) == cfg_before
    assert runtime.bus.published == published


def test_view_passes_the_server_contract(runtime: RuntimeController) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    validate(config_view(runtime), response_schema("config"))
    runtime.config.max_daily_loss = float("inf")
    runtime.config.__dict__["stop_loss_pct"] = 1.0
    validate(config_view(runtime), response_schema("config"))
    runtime.config = None
    validate(config_view(runtime), response_schema("config"))


@pytest.mark.parametrize("name", ["auth_mode", "session_id", "api_url", "certfile", "cookie_jar",
                                  "secret_sauce", "private_note", "hash_seed", "key_id",
                                  "refreshToken", "x_credentials"])
def test_sensitive_names_never_listed(runtime: RuntimeController, name: str) -> None:
    runtime.config.__dict__[name] = "value"
    view = config_view(runtime)
    assert name not in view["unlisted"] and view["redacted"] == 1
