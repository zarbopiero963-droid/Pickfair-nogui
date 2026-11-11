"""Gate A — batch A, componente risk (core.risk_gate.RiskGate).

Validazione fail-closed della config dei limiti: chiave obbligatoria assente o
invalida => NEGA (RISK_CONFIG_MISSING), anche quando il payload non attiva il
ramo che la usa. Facoltativo disattivato (MAX_WIN=None) != obbligatorio assente.
"""
from __future__ import annotations

from typing import Any, Dict

import pytest

from core.risk_gate import RiskGate

_LIMITS = ("MIN_STAKE", "MIN_PRICE", "MAX_WIN", "BOOK_BLOCK",
           "LIQUIDITY_MULTIPLIER", "MIN_LIQUIDITY_ABSOLUTE")
_FLAGS = ("LIQUIDITY_GUARD_ENABLED", "LIQUIDITY_WARNING_ONLY")


def _config(**over: Any) -> Any:
    """Config valida con tutti i limiti; `over` sostituisce, `_ABSENT` rimuove."""
    valori: Dict[str, Any] = {
        "MIN_STAKE": 0.10,
        "MIN_PRICE": 1.02,
        "MAX_WIN": None,                 # facoltativo dichiarato ma non attivo
        "BOOK_BLOCK": 110.0,
        "LIQUIDITY_MULTIPLIER": 3.0,
        "MIN_LIQUIDITY_ABSOLUTE": 50.0,
        "LIQUIDITY_GUARD_ENABLED": False,  # ramo liquidita' NON usato
        "LIQUIDITY_WARNING_ONLY": True,
    }
    valori.update(over)

    class _Cfg:
        pass

    cfg = _Cfg()
    for nome, valore in valori.items():
        if valore is not _ABSENT:
            setattr(cfg, nome, valore)
    return cfg


_ABSENT = object()


def _payload() -> Dict[str, Any]:
    # Nessun book_pct, nessuna available_liquidity: i rami opzionali non scattano.
    return {"customer_ref": "GA-RISK", "correlation_id": "GA-CID", "market_id": "1.234",
            "selection_id": 47999, "bet_type": "BACK", "price": 2.0, "stake": 10.0}


def test_baseline_config_valida_approva():
    """Controllo di sanita': senza difetti di config lo stesso payload passa."""
    r = RiskGate(config=_config()).check(_payload())
    assert r["allowed"] is True and r["reason"] is None


# ---------------------------------------------------------------------------
# TRG-628-B-0762 — tutti i sei limiti e i due flag validati anche su ramo non usato
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("nome", _LIMITS + _FLAGS)
@pytest.mark.parametrize("difetto", ["assente", "invalido"])
def test_trg_628_b_0762(nome: str, difetto: str):
    """ID: TRG-628-B-0762 / PF-B1-RISK-CONFIG

    Con un payload che non attiva book, liquidita' ne' cap (guardia liquidita'
    spenta, MAX_WIN non attivo), ogni chiave dei sei limiti e dei due flag,
    se assente o invalida, fa negare l'ordine con RISK_CONFIG_MISSING.
    """
    cattivo = _ABSENT if difetto == "assente" else float("nan")
    if difetto == "invalido" and nome == "MAX_WIN":
        cattivo = float("inf")  # None e' "non attivo"; un non finito e' invalido
    r = RiskGate(config=_config(**{nome: cattivo})).check(_payload())
    assert r["allowed"] is False, f"{nome} {difetto} non ha negato"
    assert r["reason"] == "RISK_CONFIG_MISSING"


# ---------------------------------------------------------------------------
# Foglie per singola chiave: presente, numerica, finita, non bool
# ---------------------------------------------------------------------------
_CATTIVI = [_ABSENT, float("nan"), float("inf"), float("-inf"), "abc", None, True, False, [1.0]]
_IDS = ["assente", "nan", "inf", "-inf", "testo", "None", "True", "False", "lista"]


def _assert_nega(nome: str, valore: Any) -> None:
    r = RiskGate(config=_config(**{nome: valore})).check(_payload())
    assert r["allowed"] is False, f"{nome}={valore!r} accettato"
    assert r["reason"] == "RISK_CONFIG_MISSING"


@pytest.mark.parametrize("valore", _CATTIVI, ids=_IDS)
def test_trg_628_b_0899(valore: Any):
    """ID: TRG-628-B-0899 / PF-B1-RISK-CONFIG-MIN_PRICE"""
    _assert_nega("MIN_PRICE", valore)


@pytest.mark.parametrize("valore", _CATTIVI, ids=_IDS)
def test_trg_628_b_0901(valore: Any):
    """ID: TRG-628-B-0901 / PF-B1-RISK-CONFIG-BOOK_BLOCK"""
    _assert_nega("BOOK_BLOCK", valore)


@pytest.mark.parametrize("valore", _CATTIVI, ids=_IDS)
def test_trg_628_b_0902(valore: Any):
    """ID: TRG-628-B-0902 / PF-B1-RISK-CONFIG-LIQUIDITY_MULTIPLIER"""
    _assert_nega("LIQUIDITY_MULTIPLIER", valore)


@pytest.mark.parametrize("valore", _CATTIVI, ids=_IDS)
def test_trg_628_b_0903(valore: Any):
    """ID: TRG-628-B-0903 / PF-B1-RISK-CONFIG-MIN_LIQUIDITY_ABSOLUTE"""
    _assert_nega("MIN_LIQUIDITY_ABSOLUTE", valore)
