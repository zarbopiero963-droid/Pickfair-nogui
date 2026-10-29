"""F-33 — Livello 1 (configurazioni): per ogni chiave ATTIVA con persistenza nelle settings, round-trip reale
save -> SQLite -> load (SettingsService + Database su file) con i valori rappresentativi dell'inventario; per le 42
chiavi Roserpina anche l'effetto sul RoserpinaConfig caricato. I valori INVALIDI documentano il comportamento del
loader: dove oggi un valore illeggibile diventa un default silenzioso (NF-16/NF-17, scheda F-18) il test e' xfail
strict con riferimento: diventera' verde quando F-18 rende il loader fail-closed.

Dati: tools/combi/inventory.json (190 chiavi, 79 con round-trip nelle settings).
"""
import json
import math
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from database import Database  # noqa: E402
from services.settings_service import SettingsService  # noqa: E402
from tools.combi.generator import load_inventory  # noqa: E402

pytestmark = [pytest.mark.fullscope, pytest.mark.integration]

INV = load_inventory()
ROUNDTRIP = [p for p in INV["parameters"] if p["l1_mode"] == "settings_roundtrip"]
ROSERPINA = [p for p in ROUNDTRIP if p["roserpina42"]]
LEGACY = [p for p in INV["parameters"] if p["test_class"] == "LEGACY"]

# le quattro percentuali obbligatorie (PFC-008) servono sempre, altrimenti il loader non costruisce un config valido
REQUIRED = {"roserpina.target_profit_cycle_pct": 3.0, "roserpina.max_single_bet_pct": 18.0,
            "roserpina.max_total_exposure_pct": 35.0, "roserpina.max_event_exposure_pct": 18.0}


def _svc(tmp_path):
    db = Database(str(tmp_path / "l1.db"))
    return SettingsService(db), db


def _as_stored(value):
    """Come il DB restituisce il valore: str(value) per tutto, '' per None."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value)) if False else str(value)
    return str(value)


def _same(stored, value):
    if value is None:
        return stored in ("", None)
    if isinstance(value, bool):
        return str(stored) in {str(value), str(int(value)), "True" if value else "False"}
    if isinstance(value, (int, float)):
        try:
            return math.isclose(float(stored), float(value), rel_tol=0, abs_tol=1e-12)
        except (TypeError, ValueError):
            return False
    if isinstance(value, list):
        try:
            return json.loads(stored) == value or stored == str(value)
        except Exception:  # noqa: BLE001
            return stored == str(value)
    return str(stored) == str(value)


@pytest.mark.parametrize("param", ROUNDTRIP, ids=[p["key"] for p in ROUNDTRIP])
def test_valid_values_roundtrip_through_sqlite(tmp_path, param):
    svc, _db = _svc(tmp_path)
    key = param["key"]
    for value in param["domain"]["valid"]:
        svc.save_settings({key: value})
        stored = svc.get_all_settings().get(key)
        assert _same(stored, value), f"{key}: salvato {value!r}, riletto {stored!r}"
        # riapertura del DB: il valore sopravvive alla chiusura (restart)
        svc2 = SettingsService(Database(str(tmp_path / "l1.db")))
        assert _same(svc2.get_all_settings().get(key), value), f"{key}: perso alla riapertura"


def _config_attr(key):
    return key.split(".", 1)[1]


@pytest.mark.parametrize("param", ROSERPINA, ids=[p["key"] for p in ROSERPINA])
def test_roserpina_key_reaches_loaded_config(tmp_path, param):
    """Il valore salvato e' quello che RoserpinaConfig legge (tre strati: GUI/service -> DB -> runtime config)."""
    svc, _db = _svc(tmp_path)
    key = param["key"]
    attr = _config_attr(key)
    for value in param["domain"]["valid"]:
        payload = dict(REQUIRED)
        payload[key] = value
        svc.save_settings(payload)
        cfg = svc.load_roserpina_config()
        got = getattr(cfg, attr)
        if hasattr(got, "value"):
            got = got.value
        if value is None:
            assert got is None or (isinstance(got, float) and math.isnan(got)) is False and got in (None,), f"{key}: None -> {got!r}"
        elif isinstance(value, bool):
            assert bool(got) == value, f"{key}: {value!r} -> {got!r}"
        elif isinstance(value, (int, float)):
            assert math.isclose(float(got), float(value), abs_tol=1e-9), f"{key}: {value!r} -> {got!r}"
        else:
            assert str(got) == str(value), f"{key}: {value!r} -> {got!r}"


def _invalid_cases():
    out = []
    for p in ROSERPINA:
        kind = p["domain"].get("kind")
        if kind in ("pct", "money_optional", "multiplier", "float", "int"):
            for bad in ("nan", "inf", "abc"):
                out.append(pytest.param(p, bad, id=f"{p['key']}[{bad}]"))
    return out


@pytest.mark.parametrize("param,bad", _invalid_cases())
def test_invalid_numeric_value_is_not_silently_replaced_by_a_default(tmp_path, param, bad):
    """Contratto atteso (F-18, CAP-02, NEW-01): un valore illeggibile NON diventa un default silenzioso.
    Oggi il loader si comporta in due modi: le percentuali obbligatorie e i limiti opzionali restituiscono NaN
    (fail-closed a valle, OK); i parametri con default di codice (_f) ricadono sul default in silenzio (NF-16)."""
    svc, _db = _svc(tmp_path)
    key = param["key"]
    attr = _config_attr(key)
    payload = dict(REQUIRED)
    payload[key] = bad
    svc.save_settings(payload)
    cfg = svc.load_roserpina_config()
    got = getattr(cfg, attr)
    if got is None or (isinstance(got, float) and math.isnan(got)):
        return                                          # fail-closed: il validatore a valle nega (contratto atteso)
    if isinstance(got, float) and math.isinf(got):
        pytest.xfail(f"F-18 (NF-16): {key}={bad!r} caricato come {got!r}: infinito accettato dal loader (fail-open)")
    import re
    m_num = re.search(r"(-?\d+(?:\.\d+)?)", str(param["default"]))
    default_num = float(m_num.group(1)) if m_num else None
    if default_num is not None and isinstance(got, (int, float)) and math.isclose(float(got), default_num, abs_tol=1e-9):
        pytest.xfail(f"F-18 (NF-16): {key}={bad!r} caricato come default silenzioso {got!r} (fail-open del loader)")
    pytest.fail(f"{key}={bad!r} -> {got!r}: esito inatteso (ne' fail-closed ne' default noto)")


@pytest.mark.parametrize("param", LEGACY, ids=[p["key"] for p in LEGACY])
def test_legacy_key_present_in_settings_does_not_break_loading(tmp_path, param):
    """Chiavi LEGACY (51): la loro presenza nel DB non rompe il caricamento dei config attivi."""
    svc, _db = _svc(tmp_path)
    key = param["key"].split(":", 1)[-1]
    payload = dict(REQUIRED)
    payload[key] = "legacy-value"
    svc.save_settings(payload)
    cfg = svc.load_roserpina_config()
    sim = svc.load_simulation_config()
    assert cfg is not None and isinstance(sim, dict)
