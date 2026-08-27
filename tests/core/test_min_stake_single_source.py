"""#320-D2 — MIN_STAKE fonte unica.

`trading_config.MIN_STAKE` (floor software dello stake) deve essere la SOLA
sorgente del minimo. Prima di questa PR il literal `0.10` era duplicato in tre
punti che potevano DIVERGERE se `MIN_STAKE` cambiasse:

- `core/system_state.RoserpinaConfig.min_stake` (default dataclass, hardcoded 0.10)
- `services/setting_service.load_roserpina_config` (default di load, hardcoded 0.10)
- `core/money_management._calculate ... _safe_float(min_stake, 0.10)` (fallback)

Ora tutti e tre riferiscono `trading_config.MIN_STAKE`. Poiche' oggi
`MIN_STAKE == 0.10 ==` il vecchio literal, non c'e' cambiamento di comportamento
al valore corrente (guardie sotto); la differenza si manifesta se `MIN_STAKE`
cambia — ed e' li' che i test red-first mordono: sul vecchio codice il valore
resta inchiodato a 0.10 mentre `trading_config.MIN_STAKE` e' stato cambiato.

Testabilita': i due path runtime (load + fallback money management) leggono
`trading_config.MIN_STAKE` come ATTRIBUTO a call-time, quindi `monkeypatch`
li intercetta. Il default della dataclass e' catturato a import-time (modello
"cambia config e riavvia"): la sua guardia e' l'uguaglianza con la sorgente.
"""

from __future__ import annotations

import math

import pytest

import trading_config
from core.system_state import RoserpinaConfig
from core.money_management import RoserpinaMoneyManagement


class _InMemoryDB:
    def __init__(self, initial=None):
        self._s = dict(initial or {})

    def get_settings(self):
        return dict(self._s)

    def save_settings(self, payload):
        self._s.update(dict(payload or {}))


# ---------------------------------------------------------------------------
# Guardia: default dataclass cablato alla sorgente (non un literal divergente)
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_dataclass_default_wired_to_trading_config():
    assert RoserpinaConfig().min_stake == trading_config.MIN_STAKE


# ---------------------------------------------------------------------------
# BLOCK (red-first): il default di LOAD segue trading_config.MIN_STAKE
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_load_default_follows_trading_config(monkeypatch):
    from services.setting_service import SettingsService

    monkeypatch.setattr(trading_config, "MIN_STAKE", 0.25)
    # DB senza `roserpina.min_stake` => si usa il default di load.
    cfg = SettingsService(_InMemoryDB({})).load_roserpina_config()
    # NEW: legge trading_config.MIN_STAKE (0.25). OLD: hardcoded 0.10 => FALLISCE.
    assert cfg.min_stake == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# BLOCK (red-first): il FALLBACK del money management segue trading_config
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_money_management_fallback_follows_trading_config(monkeypatch):
    monkeypatch.setattr(trading_config, "MIN_STAKE", 2.0)
    cfg = RoserpinaConfig(
        target_profit_cycle_pct=0.1,   # target_profit = 1000*0.1/100 = 1.0 €
        max_single_bet_pct=50.0,       # max_single = 500 (non morde)
        max_total_exposure_pct=50.0,
        max_event_exposure_pct=50.0,
    )
    cfg.min_stake = float("nan")       # invalido => scatta il fallback

    mm = RoserpinaMoneyManagement(cfg)
    dec = mm.calculate(
        signal={"price": 2.0},
        bankroll_current=1000.0,
        equity_peak=1000.0,            # niente drawdown => desk NORMAL (x1.0)
        current_total_exposure=0.0,
        event_current_exposure=0.0,
        table={"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
    )
    # base_stake=1.0 ; il clamp lo alza al floor = fallback = trading_config.MIN_STAKE.
    # NEW: fallback 2.0 => recommended 2.0. OLD: fallback 0.10 => recommended 1.0 => FALLISCE.
    assert dec.approved is True
    assert dec.recommended_stake == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Guardia: al valore reale (0.10) nessun cambiamento di comportamento
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_real_min_stake_value_unchanged():
    assert trading_config.MIN_STAKE == pytest.approx(0.10)
    assert RoserpinaConfig().min_stake == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# Fail-closed: sorgente MIN_STAKE corrotta => lo stake resta finito/positivo
# (rilievo GPT-5.6 Sol #451). Il floor puo' solo INDEBOLIRSI (direzione sicura:
# stake piu' piccoli), MAI produrre uno stake non-finito, negativo o gonfiato.
# _safe_float neutralizza NaN/inf; un floor <= 0 non morde uno stake positivo
# (un `recommended <= 0` e' gia' bloccato a monte, e il clamp non ABBASSA mai lo
# stake). Questi test GUARDIA fissano l'invariante: falliscono se una regressione
# futura togliesse la neutralizzazione del non-finito o lasciasse passare un
# floor che gonfia/corrompe lo stake.
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
@pytest.mark.parametrize("corrupt_source", [float("nan"), float("inf"), 0.0, -5.0])
def test_stake_floor_failsafe_on_corrupt_source(monkeypatch, corrupt_source):
    monkeypatch.setattr(trading_config, "MIN_STAKE", corrupt_source)
    cfg = RoserpinaConfig(
        target_profit_cycle_pct=0.1,   # stake calcolato piccolo (~1.0€): il floor e' rilevante
        max_single_bet_pct=50.0,
        max_total_exposure_pct=50.0,
        max_event_exposure_pct=50.0,
    )
    cfg.min_stake = float("nan")       # config invalido => si ricade sulla fonte (corrotta)

    mm = RoserpinaMoneyManagement(cfg)
    dec = mm.calculate(
        signal={"price": 2.0},
        bankroll_current=1000.0,
        equity_peak=1000.0,
        current_total_exposure=0.0,
        event_current_exposure=0.0,
        table={"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
    )
    # Nessuno stake non-finito / negativo / bloccato per colpa del floor corrotto.
    assert dec.approved is True
    assert math.isfinite(dec.recommended_stake)
    assert dec.recommended_stake > 0.0
