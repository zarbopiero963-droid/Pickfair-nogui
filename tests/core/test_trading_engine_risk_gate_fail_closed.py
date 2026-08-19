"""H-04 / R2 — il risk gate deve essere FAIL-CLOSED.

Prima di questa PR ogni percorso anomalo del gate finiva sullo stesso
``return {"allowed": True}``: `check` assente, non callable, risultato non-dict,
dict senza ``allowed``, eccezione sollevata. Un gate ROTTO lasciava passare
denaro ed era indistinguibile da un gate che aveva approvato davvero.

Ogni test qui sotto BLOCCA quel comportamento: se qualcuno rimette un
fail-open su uno di questi percorsi, il test diventa rosso.
"""
from __future__ import annotations

from typing import Any, Dict

import pytest

from core.trading_engine import TradingEngine, _NullRiskMiddleware


class _Bus:
    def subscribe(self, *_a: Any, **_kw: Any) -> None: return None
    def publish(self, *_a: Any, **_kw: Any) -> None: return None
    def emit(self, *_a: Any, **_kw: Any) -> None: return None


class _Db:
    def insert_order(self, *_a: Any, **_kw: Any) -> int: return 1
    def update_order(self, *_a: Any, **_kw: Any) -> None: return None
    def get_order(self, *_a: Any, **_kw: Any) -> Dict[str, Any]: return {}
    def order_exists_inflight(self, **_kw: Any) -> bool: return False
    def insert_audit_event(self, *_a: Any, **_kw: Any) -> None: return None


def _engine(risk_middleware: Any = None) -> TradingEngine:
    return TradingEngine(
        bus=_Bus(), db=_Db(), client_getter=lambda: object(), executor=None,
        risk_middleware=risk_middleware,
    )


REQUEST: Dict[str, Any] = {"selection_id": 1, "size": 10.0, "price": 2.0}


# --------------------------------------------------------------------------
# BLOCK: ogni forma di gate rotto deve NEGARE
# --------------------------------------------------------------------------
class _NoCheck:
    """Espone is_ready ma NON check: e' il caso di core/risk_middleware.py."""
    def is_ready(self) -> bool: return True


class _CheckNotCallable:
    check = "non sono una funzione"
    def is_ready(self) -> bool: return True


class _Raises:
    def check(self, _payload: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("gate esploso")
    def is_ready(self) -> bool: return True


class _ReturnsNonDict:
    def check(self, _payload: Dict[str, Any]) -> Any: return True
    def is_ready(self) -> bool: return True


class _ReturnsNone:
    def check(self, _payload: Dict[str, Any]) -> Any: return None
    def is_ready(self) -> bool: return True


class _DictWithoutVerdict:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"reason": "ho dimenticato allowed", "payload": payload}
    def is_ready(self) -> bool: return True


@pytest.mark.parametrize(
    "middleware, expected_reason",
    [
        (_NoCheck(), "RISK_GATE_MISSING_CHECK"),
        (_CheckNotCallable(), "RISK_GATE_MISSING_CHECK"),
        (_Raises(), "RISK_GATE_RAISED"),
        (_ReturnsNonDict(), "RISK_GATE_BAD_RESULT_TYPE"),
        (_ReturnsNone(), "RISK_GATE_BAD_RESULT_TYPE"),
        (_DictWithoutVerdict(), "RISK_GATE_NO_VERDICT"),
    ],
    ids=["no_check", "check_non_callable", "raises", "non_dict", "none", "no_allowed_key"],
)
def test_block_gate_rotto_nega_sempre(middleware: Any, expected_reason: str) -> None:
    result = _engine(middleware)._risk_gate(REQUEST)

    assert result["allowed"] is False, (
        "fail-open reintrodotto: un gate rotto ha lasciato passare l'ordine"
    )
    assert result["reason"] == expected_reason, (
        "il motivo del rifiuto deve essere distinguibile, non generico"
    )
    assert result["payload"] == REQUEST, "il payload va restituito intatto al chiamante"


# --------------------------------------------------------------------------
# PASS: un gate sano continua a decidere lui
# --------------------------------------------------------------------------
class _Allows:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}
    def is_ready(self) -> bool: return True


class _Denies:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": False, "reason": "MAX_EXPOSURE", "payload": payload}
    def is_ready(self) -> bool: return True


class _Mutates:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": {**payload, "size": 5.0}}
    def is_ready(self) -> bool: return True


def test_pass_gate_che_approva_viene_rispettato() -> None:
    assert _engine(_Allows())._risk_gate(REQUEST)["allowed"] is True


def test_pass_gate_che_nega_conserva_il_suo_motivo() -> None:
    result = _engine(_Denies())._risk_gate(REQUEST)
    assert result["allowed"] is False
    assert result["reason"] == "MAX_EXPOSURE", "il motivo del gate non va sovrascritto"


def test_pass_gate_puo_riscrivere_il_payload() -> None:
    assert _engine(_Mutates())._risk_gate(REQUEST)["payload"]["size"] == 5.0


# --------------------------------------------------------------------------
# BLOCK: un gate assente non deve sembrare un gate funzionante
# --------------------------------------------------------------------------
def test_block_segnaposto_si_dichiara_non_cablato() -> None:
    engine = _engine()
    assert isinstance(engine.risk_middleware, _NullRiskMiddleware)
    assert engine.risk_gate_wired is False, (
        "senza gate reale l'engine deve saperlo, non crederlo cablato"
    )


def test_block_readiness_espone_il_gate_mancante() -> None:
    health = _engine().readiness()["health"]["risk_middleware"]
    assert health["wired"] is False
    assert health["reason"] == "risk_gate_not_wired", (
        "la readiness deve dire PERCHE', non solo che qualcosa e' degradato"
    )


def test_pass_gate_reale_risulta_cablato() -> None:
    engine = _engine(_Allows())
    assert engine.risk_gate_wired is True
    assert engine.readiness()["health"]["risk_middleware"]["wired"] is True


def test_known_gap_il_segnaposto_approva_e_lo_dichiara() -> None:
    """LACUNA NOTA, non un comportamento voluto.

    Finche' nessun gate reale esiste nel repo, il segnaposto APPROVA: e' il
    fail-open residuo di H-04, che questa PR rende visibile ma non chiude.
    Il test lo fissa per iscritto perche' non venga scambiato per sicurezza, e
    perche' `gate: UNWIRED` permetta all'audit di distinguere questo permesso da
    quello di un gate vero. Quando il gate reale arrivera', questo test va
    SOSTITUITO da uno che pretende il rifiuto.
    """
    decision = _NullRiskMiddleware().check(REQUEST)
    assert decision["gate"] == "UNWIRED", "il permesso del segnaposto deve essere riconoscibile"
    assert decision["allowed"] is True, "lacuna nota: senza gate reale si approva ancora"


# --------------------------------------------------------------------------
# BLOCK: il verdetto deve essere un bool VERO, non qualcosa di truthy
# --------------------------------------------------------------------------
class _Truthy:
    """Un gate che risponde con qualcosa di truthy al posto di True."""
    def __init__(self, verdict: Any) -> None: self._v = verdict
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": self._v, "reason": None, "payload": payload}
    def is_ready(self) -> bool: return True


class _AlwaysTrue:
    def __bool__(self) -> bool: return True


@pytest.mark.parametrize(
    "verdict",
    ["false", "no", "False", "0", 1, -1, 3.14, [0], {"x": 1}, _AlwaysTrue()],
    ids=["str_false", "str_no", "str_False", "str_zero", "int_1", "int_neg",
         "float", "lista", "dict", "oggetto_con_bool"],
)
def test_block_verdetto_non_booleano_viene_negato(verdict: Any) -> None:
    """Il chiamante fa `bool(risk_result.get("allowed", False))`.

    bool("false") e' True: un gate che rispondesse la stringa "false"
    AUTORIZZEREBBE l'ordine. Tutti questi valori sono truthy o comunque non
    booleani e devono essere rifiutati PRIMA di arrivare al chiamante.
    """
    result = _engine(_Truthy(verdict))._risk_gate(REQUEST)

    assert result["allowed"] is False, (
        f"allowed={verdict!r} e' passato: bool() lo avrebbe reso un'autorizzazione"
    )
    assert result["reason"] == "RISK_GATE_NON_BOOLEAN_VERDICT"


@pytest.mark.parametrize("verdict", [0, 0.0, "", [], None], ids=["0", "0.0", "vuota", "lista", "None"])
def test_block_verdetto_falsy_ma_non_bool_viene_comunque_negato(verdict: Any) -> None:
    """Anche i falsy non-bool vanno rifiutati con motivo esplicito: un gate che
    non parla il contratto e' rotto, non 'implicitamente prudente'."""
    result = _engine(_Truthy(verdict))._risk_gate(REQUEST)
    assert result["allowed"] is False
    assert result["reason"] == "RISK_GATE_NON_BOOLEAN_VERDICT"


def test_pass_i_due_booleani_veri_passano() -> None:
    assert _engine(_Truthy(True))._risk_gate(REQUEST)["allowed"] is True
    assert _engine(_Truthy(False))._risk_gate(REQUEST)["allowed"] is False


# --------------------------------------------------------------------------
# BLOCK: lo stato di cablatura non deve restare indietro
# --------------------------------------------------------------------------
def test_block_wired_segue_la_sostituzione_a_runtime() -> None:
    """Se il gate viene sostituito dopo la costruzione, `risk_gate_wired` e la
    readiness devono dire la verita' sul gate ATTUALE, non su quello iniziale."""
    engine = _engine()
    assert engine.risk_gate_wired is False

    engine.risk_middleware = _Allows()
    assert engine.risk_gate_wired is True, "valore congelato in __init__: direbbe il falso"

    engine.risk_middleware = _NullRiskMiddleware()
    assert engine.risk_gate_wired is False


class _WiredExplodes:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}
    def is_ready(self) -> bool: return True
    def is_wired(self) -> bool: raise RuntimeError("boom")


def test_block_is_wired_che_solleva_vale_come_non_cablato() -> None:
    """Nel dubbio si dichiara il gate assente: e' il verso prudente."""
    assert _engine(_WiredExplodes()).risk_gate_wired is False


class _WiredReturns:
    def __init__(self, v: Any) -> None: self._v = v
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}
    def is_ready(self) -> bool: return True
    def is_wired(self) -> Any: return self._v


@pytest.mark.parametrize(
    "value", [None, 0, "", [], {}, "si", 1, "True"],
    ids=["None", "zero", "vuota", "lista", "dict", "str_si", "int_1", "str_True"],
)
def test_block_is_wired_non_booleano_vale_come_non_cablato(value: Any) -> None:
    """`is not False` avrebbe contato come CABLATI tutti questi valori.

    Un gate con `is_wired()` difettoso sarebbe cosi' risultato operativo nella
    readiness, che e' il contrario della prudenza dichiarata. Si accetta solo il
    singleton `True`.
    """
    assert _engine(_WiredReturns(value)).risk_gate_wired is False, (
        f"is_wired()={value!r} e' stato contato come gate cablato"
    )


def test_pass_is_wired_true_vale_come_cablato() -> None:
    assert _engine(_WiredReturns(True)).risk_gate_wired is True


def test_block_property_senza_setter_non_rompe_nessuno() -> None:
    """`risk_gate_wired` e' una property in sola lettura: chi provasse ad
    assegnarla otterrebbe AttributeError. Nessun codice lo fa (verificato con
    grep sull'intero repo), ma il test lo fissa perche' resti vero."""
    engine = _engine()
    with pytest.raises(AttributeError):
        engine.risk_gate_wired = True  # type: ignore[misc]
