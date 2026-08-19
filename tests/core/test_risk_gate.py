"""H-13 / H-14 — il risk gate reale applica i limiti gia' configurati.

Ogni test BLOCK fissa un percorso che, senza il gate, portava denaro fino al
broker senza che nessun limite lo guardasse. I valori attesi vengono da
`trading_config`: se l'owner li cambia lì, questi test seguono.
"""
from __future__ import annotations

import math
from typing import Any, Dict

import pytest

import trading_config
from core.risk_gate import RiskGate
from core.trading_engine import TradingEngine


GATE = RiskGate()


def payload(**over: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "customer_ref": "CUST-1",
        "correlation_id": "CID-1",
        "market_id": "1.234",
        "selection_id": 47999,
        "bet_type": "BACK",
        "price": 2.00,
        "stake": 10.00,
    }
    base.update(over)
    return base


def verdict(**over: Any) -> Dict[str, Any]:
    return GATE.check(payload(**over))


# ---------------------------------------------------------------------------
# PASS: un ordine dentro i limiti passa
# ---------------------------------------------------------------------------
def test_pass_ordine_regolare() -> None:
    r = verdict()
    assert r["allowed"] is True
    assert r["reason"] is None
    assert r["payload"]["stake"] == 10.00, "il payload va restituito intatto"


def test_pass_lay_dentro_il_cap() -> None:
    # liability = 10 * (5-1) = 40, ben sotto MAX_WIN
    assert verdict(bet_type="LAY", price=5.00)["allowed"] is True


def test_pass_side_case_insensitive() -> None:
    assert verdict(bet_type="back")["allowed"] is True
    assert verdict(bet_type=" Lay ")["allowed"] is True


# ---------------------------------------------------------------------------
# BLOCK H-13: isfinite PRIMA delle soglie
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad", [float("nan"), float("inf"), float("-inf")],
    ids=["nan", "inf", "-inf"],
)
def test_block_stake_non_finito(bad: float) -> None:
    """nan supera OGNI confronto di soglia: va respinto prima di confrontarlo.

    Controprova del perche' l'ordine conta: `nan < MIN_STAKE` e' False,
    `nan > MAX_WIN` e' False, `nan <= 0` e' False. Un gate che mettesse
    isfinite in fondo lascerebbe passare il NaN indisturbato.
    """
    assert not math.isfinite(bad)
    r = verdict(stake=bad)
    assert r["allowed"] is False
    assert r["reason"] == "RISK_STAKE_NOT_FINITE"


@pytest.mark.parametrize("bad", [float("nan"), float("inf")], ids=["nan", "inf"])
def test_block_price_non_finito(bad: float) -> None:
    r = verdict(price=bad)
    assert r["allowed"] is False
    assert r["reason"] == "RISK_PRICE_NOT_FINITE"


def test_block_nan_supererebbe_ogni_soglia() -> None:
    """Fissa per iscritto il motivo per cui H-13 esiste."""
    nan = float("nan")
    assert (nan < trading_config.MIN_STAKE) is False
    assert (nan > trading_config.MAX_WIN) is False
    assert (nan <= 0) is False


@pytest.mark.parametrize(
    "bad", [None, "", "abc", [], {}, object()],
    ids=["None", "vuota", "testo", "lista", "dict", "oggetto"],
)
def test_block_stake_non_numerico(bad: Any) -> None:
    assert verdict(stake=bad)["reason"] == "RISK_STAKE_NOT_FINITE"


def test_block_stake_assente() -> None:
    """Un gate che non vede il denaro non puo' approvarlo."""
    p = payload()
    del p["stake"]
    assert GATE.check(p)["reason"] == "RISK_STAKE_NOT_FINITE"


def test_block_bool_non_e_uno_stake() -> None:
    """`True` e' int in Python e diventerebbe uno stake da 1 euro."""
    assert verdict(stake=True)["reason"] == "RISK_STAKE_NOT_FINITE"


# ---------------------------------------------------------------------------
# BLOCK: soglie di stake e quota, dai valori configurati
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bad", [0.0, -1.0, -0.01], ids=["zero", "negativo", "negativo_piccolo"])
def test_block_stake_non_positivo(bad: float) -> None:
    assert verdict(stake=bad)["reason"] == "RISK_STAKE_NOT_POSITIVE"


def test_block_sotto_min_stake() -> None:
    assert trading_config.MIN_STAKE == 0.10
    assert verdict(stake=0.09)["reason"] == "RISK_BELOW_MIN_STAKE"
    assert verdict(stake=0.10)["allowed"] is True, "il limite e' incluso"


def test_block_quota_sotto_il_minimo() -> None:
    assert trading_config.MIN_PRICE == 1.02
    assert verdict(price=1.01)["reason"] == "RISK_PRICE_BELOW_MIN"
    assert verdict(price=1.02)["allowed"] is True, "il limite e' incluso"


@pytest.mark.parametrize(
    "bad", [None, "", "PUNTA", "back ma non proprio", 1, "LAYY"],
    ids=["None", "vuota", "italiano", "frase", "numero", "typo"],
)
def test_block_bet_type_invalido(bad: Any) -> None:
    assert verdict(bet_type=bad)["reason"] == "RISK_BET_TYPE_INVALID"


# ---------------------------------------------------------------------------
# BLOCK H-14: la LAY si misura come liability, non come stake
# ---------------------------------------------------------------------------
def test_block_lay_oltre_il_cap_mentre_la_back_passa() -> None:
    """Stesso stake, stessa quota: la BACK passa, la LAY no.

    E' la dimostrazione di H-14. Con stake 600 a quota 21 la BACK rischia 600,
    la LAY rischia 600*20 = 12.000, oltre MAX_WIN di 10.000. Un gate che
    misurasse la LAY come stake le farebbe passare entrambe.
    """
    assert trading_config.MAX_WIN == 10000.0
    stake, price = 600.0, 21.0
    assert stake <= trading_config.MAX_WIN
    assert stake * (price - 1) > trading_config.MAX_WIN

    assert verdict(bet_type="BACK", stake=stake, price=price)["allowed"] is True
    r = verdict(bet_type="LAY", stake=stake, price=price)
    assert r["allowed"] is False
    assert r["reason"] == "RISK_MAX_WIN_EXCEEDED"


def test_block_back_oltre_max_win() -> None:
    assert verdict(stake=10_001.0)["reason"] == "RISK_MAX_WIN_EXCEEDED"


def test_pass_lay_esattamente_al_cap() -> None:
    # liability = 10000 * (2-1) = 10000, esattamente MAX_WIN: ammesso
    assert verdict(bet_type="LAY", stake=10_000.0, price=2.0)["allowed"] is True


def test_block_esposizione_che_trabocca() -> None:
    """stake e price finiti, il prodotto no."""
    huge = 1e308
    assert math.isfinite(huge)
    r = verdict(bet_type="LAY", stake=huge, price=huge)
    assert r["allowed"] is False
    assert r["reason"] in {"RISK_MAX_WIN_EXCEEDED", "RISK_EXPOSURE_NOT_FINITE"}


# ---------------------------------------------------------------------------
# BOOK e LIQUIDITA': controllati solo se il payload li porta
# ---------------------------------------------------------------------------
def test_block_book_oltre_soglia() -> None:
    assert trading_config.BOOK_BLOCK == 110.0
    assert verdict(book_pct=110.0)["reason"] == "RISK_BOOK_OVER_BLOCK"
    assert verdict(book_pct=109.9)["allowed"] is True


def test_pass_book_assente_non_e_verificato() -> None:
    """Limite DICHIARATO: senza `book_pct` l'over-round non e' controllato qui.

    L'engine non lo calcola. Negare ogni ordine per un dato che non arriva mai
    bloccherebbe tutto; il punto e' che sia scritto, non che sia nascosto.
    """
    p = payload()
    assert "book_pct" not in p
    assert GATE.check(p)["allowed"] is True


class _Cfg:
    """Config finta per esercitare i rami che dipendono dai flag owner."""
    MIN_STAKE = 0.10
    MIN_PRICE = 1.02
    MAX_WIN = 10000.0
    BOOK_BLOCK = 110.0
    LIQUIDITY_GUARD_ENABLED = True
    LIQUIDITY_MULTIPLIER = 3.0
    MIN_LIQUIDITY_ABSOLUTE = 50.0
    LIQUIDITY_WARNING_ONLY = True


def test_pass_liquidita_scarsa_solo_avviso_come_vuole_owner() -> None:
    """`LIQUIDITY_WARNING_ONLY` e' True per decisione esplicita (#383).

    Il gate rispetta quella scelta invece di scavalcarla: avvisa e lascia
    passare.
    """
    assert trading_config.LIQUIDITY_WARNING_ONLY is True
    gate = RiskGate(config=_Cfg())
    assert gate.check(payload(available_liquidity=1.0))["allowed"] is True


def test_block_liquidita_scarsa_quando_owner_arma_il_blocco() -> None:
    cfg = _Cfg()
    cfg.LIQUIDITY_WARNING_ONLY = False
    gate = RiskGate(config=cfg)
    r = gate.check(payload(available_liquidity=1.0))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_LIQUIDITY_INSUFFICIENT"


def test_pass_liquidita_sufficiente() -> None:
    cfg = _Cfg()
    cfg.LIQUIDITY_WARNING_ONLY = False
    gate = RiskGate(config=cfg)
    # richiesta = max(10*3, 50) = 50
    assert gate.check(payload(available_liquidity=50.0))["allowed"] is True
    assert gate.check(payload(available_liquidity=49.9))["allowed"] is False


# ---------------------------------------------------------------------------
# Contratto con l'engine
# ---------------------------------------------------------------------------
def test_block_payload_non_dict() -> None:
    assert GATE.check("non sono un dict")["reason"] == "RISK_PAYLOAD_NOT_DICT"  # type: ignore[arg-type]


def test_block_gate_che_esplode_nega() -> None:
    class _Boom:
        def __getattr__(self, _n: str) -> Any: raise RuntimeError("boom")
    r = RiskGate(config=_Boom()).check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_GATE_INTERNAL_ERROR"


def test_block_verdetto_sempre_booleano_vero() -> None:
    """L'engine rifiuta i verdetti non booleani: il gate deve parlare la sua
    lingua, altrimenti verrebbe negato per RISK_GATE_NON_BOOLEAN_VERDICT."""
    for r in (verdict(), verdict(stake=0.01), verdict(bet_type="X")):
        assert r["allowed"] is True or r["allowed"] is False


def test_pass_gate_reale_risulta_cablato_nella_readiness() -> None:
    """Non espone `is_wired()`: e' cosi' che l'engine lo riconosce come reale."""
    assert not hasattr(RiskGate(), "is_wired")

    class _Bus:
        def subscribe(self, *_a: Any, **_k: Any) -> None: return None
    class _Db:
        def insert_order(self, *_a: Any, **_k: Any) -> int: return 1
        def update_order(self, *_a: Any, **_k: Any) -> None: return None
        def get_order(self, *_a: Any, **_k: Any) -> Dict[str, Any]: return {}
        def order_exists_inflight(self, **_k: Any) -> bool: return False
        def insert_audit_event(self, *_a: Any, **_k: Any) -> None: return None

    engine = TradingEngine(bus=_Bus(), db=_Db(), client_getter=lambda: object(),
                           executor=None, risk_middleware=RiskGate())
    assert engine.risk_gate_wired is True
    assert engine.readiness()["health"]["risk_middleware"]["wired"] is True


def test_block_engine_usa_davvero_il_gate() -> None:
    """Il verdetto del gate arriva all'engine senza essere riscritto."""
    class _Bus:
        def subscribe(self, *_a: Any, **_k: Any) -> None: return None
    class _Db:
        def insert_order(self, *_a: Any, **_k: Any) -> int: return 1
        def update_order(self, *_a: Any, **_k: Any) -> None: return None
        def get_order(self, *_a: Any, **_k: Any) -> Dict[str, Any]: return {}
        def order_exists_inflight(self, **_k: Any) -> bool: return False
        def insert_audit_event(self, *_a: Any, **_k: Any) -> None: return None

    engine = TradingEngine(bus=_Bus(), db=_Db(), client_getter=lambda: object(),
                           executor=None, risk_middleware=RiskGate())
    assert engine._risk_gate(payload())["allowed"] is True
    denied = engine._risk_gate(payload(stake=0.01))
    assert denied["allowed"] is False
    assert denied["reason"] == "RISK_BELOW_MIN_STAKE"
