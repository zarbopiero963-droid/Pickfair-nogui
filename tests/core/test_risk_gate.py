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
from core.risk_gate import (
    _REQUIRED_FLAGS,
    _REQUIRED_LIMITS,
    RiskGate,
    RoserpinaRiskLimits,
)
from core.system_state import RoserpinaConfig
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
    lim = trading_config.MIN_STAKE
    assert verdict(stake=lim / 2)["reason"] == "RISK_BELOW_MIN_STAKE"
    assert verdict(stake=lim)["allowed"] is True, "il limite e' incluso"


def test_block_quota_sotto_il_minimo() -> None:
    lim = trading_config.MIN_PRICE
    assert verdict(price=lim - 0.01)["reason"] == "RISK_PRICE_BELOW_MIN"
    assert verdict(price=lim)["allowed"] is True, "il limite e' incluso"


@pytest.mark.parametrize(
    "bad", [None, "", "PUNTA", "back ma non proprio", 1, "LAYY"],
    ids=["None", "vuota", "italiano", "frase", "numero", "typo"],
)
def test_block_bet_type_invalido(bad: Any) -> None:
    assert verdict(bet_type=bad)["reason"] == "RISK_BET_TYPE_INVALID"


# ---------------------------------------------------------------------------
# BLOCK H-14: la LAY si misura come liability, non come stake
# ---------------------------------------------------------------------------
def test_block_stesso_ordine_motivo_diverso_per_lato() -> None:
    """H-14 reso osservabile: stessi numeri, rifiuto per ragioni opposte.

    Con un solo tetto per vincita ed esposizione i due lati sono simmetrici
    nell'ESITO — quel che cambia e' DOVE sta il denaro a rischio, e il motivo
    lo dice:
      BACK 100 @ 500 -> vincita  49.900 oltre il cap -> MAX_WIN
      LAY  100 @ 500 -> vincita     100 sotto il cap,
                        ma liability 49.900          -> MAX_EXPOSURE
    Un gate che misurasse la LAY come stake vedrebbe solo 100 e la farebbe
    passare: e' esattamente il buco di H-14.
    """
    cap = trading_config.MAX_WIN
    stake, price = 100.0, 500.0
    payout = stake * (price - 1)
    assert stake <= cap < payout, "i numeri del test devono davvero stare a cavallo del cap"

    back = verdict(bet_type="BACK", stake=stake, price=price)
    lay = verdict(bet_type="LAY", stake=stake, price=price)
    assert back["reason"] == "RISK_MAX_WIN_EXCEEDED"
    assert lay["reason"] == "RISK_MAX_EXPOSURE_EXCEEDED"


def test_block_back_oltre_max_win() -> None:
    # BACK: vincita = stake*(price-1). Con quota 2.0 basta superare il cap.
    r = verdict(stake=trading_config.MAX_WIN + 1, price=2.0)
    assert r["reason"] == "RISK_MAX_WIN_EXCEEDED"


def test_pass_lay_esattamente_al_cap() -> None:
    # LAY: vincita = stake, liability = stake*(price-1). A quota 2.0 sono pari
    # al cap: entrambi ammessi perche' il confronto e' stretto.
    cap = trading_config.MAX_WIN
    assert verdict(bet_type="LAY", stake=cap, price=2.0)["allowed"] is True


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
    lim = trading_config.BOOK_BLOCK
    assert verdict(book_pct=lim)["reason"] == "RISK_BOOK_OVER_BLOCK"
    assert verdict(book_pct=lim - 0.1)["allowed"] is True


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
    r = GATE.check("non sono un dict")  # type: ignore[arg-type]
    assert r["reason"] == "RISK_PAYLOAD_NOT_DICT"


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


# ---------------------------------------------------------------------------
# BLOCK: campo opzionale PRESENTE ma invalido != campo ASSENTE
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad", [float("nan"), float("inf"), "abc", None, [], True],
    ids=["nan", "inf", "testo", "None", "lista", "bool"],
)
def test_block_book_presente_ma_invalido(bad: Any) -> None:
    """Un `book_pct` illeggibile aggirava BOOK_BLOCK.

    Trattare "presente ma invalido" come "assente" e' un fail-open: bastava un
    NaN per saltare la soglia invece di farla scattare.
    """
    r = verdict(book_pct=bad)
    assert r["allowed"] is False, f"book_pct={bad!r} ha aggirato la soglia"
    assert r["reason"] == "RISK_BOOK_NOT_FINITE"


@pytest.mark.parametrize(
    "bad", [float("nan"), float("inf"), "abc", None],
    ids=["nan", "inf", "testo", "None"],
)
def test_block_liquidita_presente_ma_invalida(bad: Any) -> None:
    """Vale anche in modalita' solo-avviso.

    `LIQUIDITY_WARNING_ONLY` dice "accetto mercati sottili", non "accetto
    numeri illeggibili": un dato corrotto non e' una liquidita' bassa.
    """
    for warning_only in (True, False):
        cfg = _Cfg()
        cfg.LIQUIDITY_WARNING_ONLY = warning_only
        r = RiskGate(config=cfg).check(payload(available_liquidity=bad))
        assert r["allowed"] is False, f"warning_only={warning_only}, valore {bad!r}"
        assert r["reason"] == "RISK_LIQUIDITY_NOT_FINITE"


# ---------------------------------------------------------------------------
# BLOCK: MAX_WIN misura la VINCITA, non il rischio
# ---------------------------------------------------------------------------
def test_block_back_che_puo_vincere_oltre_il_cap() -> None:
    """`MAX_WIN` dice "vincita massima": va confrontata con la vincita.

    Il difetto era confrontarla col RISCHIO. Una BACK da 500 a quota 100
    rischia solo 500 e passava, ma puo' vincere 49.500.
    """
    cap = trading_config.MAX_WIN
    stake, price = 500.0, 100.0
    assert stake <= cap, "il rischio della BACK sta sotto il tetto"
    assert stake * (price - 1) > cap, "ma la vincita lo supera"

    r = verdict(bet_type="BACK", stake=stake, price=price)
    assert r["allowed"] is False
    assert r["reason"] == "RISK_MAX_WIN_EXCEEDED"


def test_block_lay_con_vincita_bassa_ma_liability_enorme() -> None:
    """Il verso opposto: senza il tetto d'esposizione H-14 resterebbe scoperto.

    Una LAY con stake piccolo e quota alta ha vincita bassa (= stake) ma
    liability enorme. Controllare solo la vincita la lascerebbe passare.
    """
    cap = trading_config.MAX_WIN
    stake, price = 60.0, 900.0
    assert stake <= cap, "la vincita della LAY e' bassa"
    assert stake * (price - 1) > cap, "la liability no"

    r = verdict(bet_type="LAY", stake=stake, price=price)
    assert r["allowed"] is False
    assert r["reason"] == "RISK_MAX_EXPOSURE_EXCEEDED"


# ---------------------------------------------------------------------------
# BLOCK: nessun default silenzioso sui limiti
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "missing",
    ["MIN_STAKE", "MIN_PRICE", "MAX_WIN", "BOOK_BLOCK",
     "LIQUIDITY_MULTIPLIER", "MIN_LIQUIDITY_ABSOLUTE"],
)
def test_block_costante_di_rischio_mancante_nega(missing: str) -> None:
    """Un typo in trading_config disarmerebbe il limite in silenzio.

    Con `getattr(cfg, name, default)` il gate avrebbe applicato un numero
    scritto dentro se stesso, facendo credere che il limite fosse attivo.
    """
    r = RiskGate(config=_cfg_senza(missing)).check(payload(book_pct=100.0))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


@pytest.mark.parametrize("bad", [float("nan"), "abc", None], ids=["nan", "testo", "None"])
def test_block_costante_non_numerica_nega(bad: Any) -> None:
    cfg = _Cfg()
    cfg.MIN_STAKE = bad
    r = RiskGate(config=cfg).check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


# ---------------------------------------------------------------------------
# BLOCK: nemmeno gli INTERRUTTORI hanno un default silenzioso
# ---------------------------------------------------------------------------
def _cfg_senza(name: str) -> Any:
    """Copia di _Cfg priva di UNA costante, senza toccare la classe condivisa."""
    class _Partial:
        pass
    part = _Partial()
    for attr in vars(_Cfg):
        if attr.startswith("__") or attr == name:
            continue
        setattr(part, attr, getattr(_Cfg, attr))
    return part


def test_block_guardia_liquidita_assente_nega() -> None:
    """Il difetto che questo test blocca era peggiore di un default qualsiasi.

    Il default scritto nel gate era False mentre il valore realmente
    configurato e' True: una costante rinominata avrebbe DISARMATO la guardia
    di liquidita' in silenzio, e il gate avrebbe continuato a sembrare intero.
    """
    assert trading_config.LIQUIDITY_GUARD_ENABLED is True, (
        "il valore configurato e' l'opposto del vecchio default: e' questo che "
        "rendeva il fallback pericoloso"
    )
    r = RiskGate(config=_cfg_senza("LIQUIDITY_GUARD_ENABLED")).check(
        payload(available_liquidity=1.0))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_block_modalita_avviso_assente_nega() -> None:
    """Anche il flag che ALLENTA il controllo va letto, non presunto.

    Presumerlo True aprirebbe il passaggio a mercati sottili senza che l'owner
    lo abbia mai deciso; presumerlo False bloccherebbe una scelta che invece ha
    preso (#383). Non si presume: se manca, si nega.
    """
    r = RiskGate(config=_cfg_senza("LIQUIDITY_WARNING_ONLY")).check(
        payload(available_liquidity=1.0))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


@pytest.mark.parametrize(
    "bad", [1, 0, "True", "false", "", None, 1.0, [], object()],
    ids=["int_1", "int_0", "str_True", "str_false", "vuota", "None", "float",
         "lista", "oggetto"],
)
@pytest.mark.parametrize(
    "flag", ["LIQUIDITY_GUARD_ENABLED", "LIQUIDITY_WARNING_ONLY"],
)
def test_block_interruttore_non_booleano_nega(flag: str, bad: Any) -> None:
    """Un interruttore va letto come bool VERO, non come qualcosa di truthy.

    `if not cfg.LIQUIDITY_GUARD_ENABLED` su una stringa "false" lascerebbe la
    guardia ATTIVA per caso, e su `0` la spegnerebbe senza che nessuno lo abbia
    scritto: in entrambi i casi il comportamento non e' quello configurato.
    """
    cfg = _Cfg()
    setattr(cfg, flag, bad)
    r = RiskGate(config=cfg).check(payload(available_liquidity=1.0))
    assert r["allowed"] is False, f"{flag}={bad!r} e' stato interpretato come un bool"
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_pass_i_due_booleani_veri_restano_leggibili() -> None:
    cfg = _Cfg()
    cfg.LIQUIDITY_GUARD_ENABLED = False
    # guardia spenta: la liquidita' ridicola non viene nemmeno guardata
    assert RiskGate(config=cfg).check(payload(available_liquidity=1.0))["allowed"] is True


@pytest.mark.parametrize(
    "rotta",
    ["BOOK_BLOCK", "LIQUIDITY_MULTIPLIER", "MIN_LIQUIDITY_ABSOLUTE",
     "LIQUIDITY_WARNING_ONLY"],
)
def test_block_config_rotta_emerge_al_primo_ordine_non_al_peggiore(rotta: str) -> None:
    """Le letture pigre nascondono il guasto fino al momento sbagliato.

    Queste quattro costanti servono solo su certi rami: `BOOK_BLOCK` se il
    payload porta un book_pct, le altre tre quando la liquidita' e' gia' sotto
    soglia. Lette pigramente, un config rotto resterebbe invisibile per tutta la
    sessione e verrebbe fuori sul mercato sottile — ordini negati in blocco
    proprio quando servono. Il payload qui NON tocca nessuno di quei rami:
    deve negare lo stesso, perche' il config viene letto tutto in blocco.
    """
    p = payload()
    assert "book_pct" not in p and "available_liquidity" not in p, (
        "il test perde senso se il payload attiva il ramo che usa la costante"
    )
    r = RiskGate(config=_cfg_senza(rotta)).check(p)
    assert r["allowed"] is False, f"{rotta} rotta e' passata inosservata"
    assert r["reason"] == "RISK_CONFIG_MISSING"


# ---------------------------------------------------------------------------
# BLOCK: l'invariante interna nega, non prosegue
# ---------------------------------------------------------------------------
class _GateRotto(RiskGate):
    """Simula una regressione futura: un controllo che non nega e non decide."""

    def _check_stake(self, payload: Dict[str, Any]) -> Any:
        return None, None


def test_block_invariante_interna_nega_invece_di_proseguire() -> None:
    """Qui c'era un `assert`, e sotto `python -O` sarebbe sparito.

    Con l'assert, un controllo obbligatorio che restituisse "nessun valore"
    senza rifiutare avrebbe fatto proseguire il gate con stake=None: il
    confronto successivo sarebbe esploso (rifiuto per errore interno) oppure,
    peggio, sarebbe stato saltato. Ora quel caso ha un rifiuto suo, con un
    motivo che lo rende riconoscibile nell'audit.

    Config esplicito, non quello reale: con la validazione in blocco un
    trading_config incompleto darebbe RISK_CONFIG_MISSING e il test parlerebbe
    dell'ambiente invece che dell'invariante.
    """
    r = _GateRotto(config=_Cfg()).check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_INTERNAL_INVARIANT"


def test_block_invariante_regge_anche_senza_assert_attivi() -> None:
    """Controprova esplicita: il rifiuto non dipende dagli assert.

    `-O` disattiva gli assert; se la guardia fosse ancora un assert questo
    sottoprocesso approverebbe l'ordine.
    """
    import os
    import subprocess
    import sys

    # Il gate rotto e' ridefinito QUI DENTRO di proposito: importare
    # `tests.core.test_risk_gate` legherebbe il test alla directory da cui
    # pytest e' stato lanciato e all'esistenza di `tests/__init__.py` (che non
    # c'e'). Riprodotto: lanciando pytest da tests/ il sottoprocesso moriva con
    # ModuleNotFoundError. Cosi' serve solo che `core` sia importabile.
    # Il config viaggia con il codice, generato da _Cfg cosi' resta una sola
    # fonte: senza, `G()` caricherebbe il trading_config reale e — ora che la
    # validazione e' in blocco — un config incompleto nell'ambiente darebbe
    # RISK_CONFIG_MISSING. Il test parlerebbe dell'ambiente, non dell'invariante.
    #
    # L'elenco delle costanti viene da `core.risk_gate`, non da `vars(_Cfg)`:
    # cosi' `getattr` segue anche l'ereditarieta' e il config generato copre per
    # costruzione esattamente cio' che il gate pretende. Il tipo e' controllato
    # qui perche' un valore senza `repr` valido come literal genererebbe codice
    # rotto nel sottoprocesso, e il test fallirebbe per il motivo sbagliato.
    righe = []
    for nome in _REQUIRED_LIMITS + _REQUIRED_FLAGS:
        valore = getattr(_Cfg, nome)
        assert isinstance(valore, (int, float, bool)), (
            f"_Cfg.{nome} = {valore!r} non e' un literal: il config del "
            f"sottoprocesso non si puo' generare cosi'"
        )
        # inf e nan passano isinstance ma repr() ne fa 'inf'/'nan', che in
        # Python non sono literal: nel sottoprocesso darebbero NameError.
        assert isinstance(valore, bool) or math.isfinite(valore), (
            f"_Cfg.{nome} = {valore!r}: repr() non produce un literal valido"
        )
        righe.append(f"    {nome} = {valore!r}\n")
    assert righe, (
        "elenchi di costanti vuoti: `class Cfg:` senza corpo darebbe SyntaxError "
        "nel sottoprocesso, mascherando l'invariante che questo test verifica"
    )
    cfg_src = "class Cfg:\n" + "".join(righe)
    code = (
        "from core.risk_gate import RiskGate\n"
        + cfg_src
        + "class G(RiskGate):\n"
        "    def _check_stake(self, payload):\n"
        "        return None, None\n"
        "r = G(config=Cfg()).check({'bet_type': 'BACK', 'price': 2.0, 'stake': 10.0})\n"
        "print(r['allowed'], r['reason'])\n"
    )
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [root] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
    # check=False esplicito: l'uscita non nulla la vogliamo VEDERE nell'assert
    # sotto, con lo stderr del sottoprocesso in chiaro, non come CalledProcessError.
    out = subprocess.run([sys.executable, "-O", "-c", code], check=False,
                         capture_output=True, text=True, cwd=root, env=env)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False RISK_INTERNAL_INVARIANT", out.stdout


# ---------------------------------------------------------------------------
# BLOCK: il config REALE deve soddisfare il contratto del gate
# ---------------------------------------------------------------------------
def test_block_il_trading_config_reale_soddisfa_il_contratto_del_gate() -> None:
    """La contropartita della lettura in blocco, resa automatica.

    Leggere tutto il config a ogni ordine ha un prezzo dichiarato: se
    `trading_config` perdesse anche UNA delle costanti attese, ogni ordine
    verrebbe negato con RISK_CONFIG_MISSING — blocco totale del trading, non
    una degradazione parziale. E' il verso giusto (meglio fermi che senza
    limiti), ma va sorvegliato, e sorvegliarlo "prima del deploy" e' una
    raccomandazione che nessuno esegue.
    Questo test lo esegue a ogni commit: se qualcuno rinomina o cancella una
    costante di rischio, diventa rosso qui invece che in produzione.
    """
    mancanti = [n for n in _REQUIRED_LIMITS + _REQUIRED_FLAGS
                if not hasattr(trading_config, n)]
    assert not mancanti, (
        f"trading_config non espone {mancanti}: con la validazione in blocco "
        f"il gate negherebbe OGNI ordine"
    )
    # non basta che esistano: devono essere leggibili come il gate pretende
    RiskGate()._validate_config()
    assert GATE.check(payload())["allowed"] is True, (
        "col config reale un ordine regolare deve passare"
    )


# ---------------------------------------------------------------------------
# Tripwire: i limiti configurati OGGI
# ---------------------------------------------------------------------------
def test_tripwire_limiti_configurati_oggi() -> None:
    """Test DELIBERATAMENTE legato ai valori correnti.

    Non e' un test di comportamento — quelli sopra derivano dal config e lo
    seguono. Questo esiste per FARE RUMORE se qualcuno cambia un limite di
    rischio: su un sistema che muove denaro, accorgersene e' il punto.
    Se il cambio e' voluto, si aggiorna questa riga nella stessa PR.
    """
    assert trading_config.MIN_STAKE == 0.10
    assert trading_config.MIN_PRICE == 1.02
    assert trading_config.MAX_WIN == 10000.0
    assert trading_config.BOOK_BLOCK == 110.0
    assert trading_config.LIQUIDITY_MULTIPLIER == 3.0
    assert trading_config.MIN_LIQUIDITY_ABSOLUTE == 50.0
    assert trading_config.LIQUIDITY_WARNING_ONLY is True


# ---------------------------------------------------------------------------
# RoserpinaRiskLimits — la vista owner-config del gate
# (piano: PR «RiskGate ↔ config Roserpina»)
# ---------------------------------------------------------------------------
class _SettingsRoserpina:
    """SettingsService minimo: la vista chiede solo load_roserpina_config().

    ``cfg`` puo' essere una RoserpinaConfig (restituita), una Exception
    (sollevata: settings illeggibili) o un oggetto qualunque (per simulare
    snapshot con campi mancanti). Riassegnabile tra un check e l'altro per
    simulare il salvataggio dell'owner a bot acceso.
    """

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self.loads = 0

    def load_roserpina_config(self) -> Any:
        self.loads += 1
        if isinstance(self.cfg, Exception):
            raise self.cfg
        return self.cfg


def _gate_roserpina(cfg: Any) -> "tuple[RiskGate, _SettingsRoserpina]":
    servizio = _SettingsRoserpina(cfg)
    return RiskGate(config=RoserpinaRiskLimits(servizio)), servizio


def test_roserpina_vista_applica_i_limiti_salvati_dall_owner() -> None:
    """Il min_price della tab Roserpina NEGA sotto soglia; le costanti no."""
    gate, _ = _gate_roserpina(RoserpinaConfig(min_price=1.50))
    r = gate.check(payload(price=1.30))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_PRICE_BELOW_MIN"
    assert gate.check(payload(price=1.60))["allowed"] is True


def test_roserpina_vista_refresh_a_ogni_check_senza_riavvio() -> None:
    """L'owner salva a bot acceso: il check DOPO applica il valore nuovo."""
    gate, servizio = _gate_roserpina(RoserpinaConfig())
    assert gate.check(payload(price=1.30))["allowed"] is True  # default 1.02
    servizio.cfg = RoserpinaConfig(min_price=1.50)  # "salvataggio" owner
    r = gate.check(payload(price=1.30))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_PRICE_BELOW_MIN"
    assert servizio.loads >= 2, "la vista deve ricaricare a ogni check"


def test_roserpina_vista_default_vergini_identici_alle_costanti() -> None:
    """Zero delta a settings vergini: stessi verdetti del modulo costanti."""
    gate, _ = _gate_roserpina(RoserpinaConfig())
    assert gate.check(payload())["allowed"] is True
    assert gate.check(payload(price=1.01))["reason"] == "RISK_PRICE_BELOW_MIN"
    assert gate.check(payload(stake=0.05))["reason"] == "RISK_BELOW_MIN_STAKE"
    assert (
        gate.check(payload(stake=600.0, price=21.0, bet_type="LAY"))["reason"]
        == "RISK_MAX_EXPOSURE_EXCEEDED"
    )  # H-14: liability 12.000 > max_win 10.000


def test_roserpina_vista_campo_mancante_nega_fail_closed() -> None:
    """Snapshot senza un campo atteso => il gate NEGA, mai default silenziosi."""
    class _SenzaMaxWin:
        min_stake = 0.10
        min_price = 1.02
        book_block = 110.0
        liquidity_guard_enabled = True
        liquidity_multiplier = 3.0
        min_liquidity_absolute = 50.0
        liquidity_warning_only = True
        # max_win MANCANTE di proposito

    gate, _ = _gate_roserpina(_SenzaMaxWin())
    r = gate.check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_roserpina_vista_settings_illeggibili_nega() -> None:
    """load_roserpina_config solleva (db giu') => DENY, mai limiti di comodo."""
    gate, _ = _gate_roserpina(RuntimeError("SETTINGS_UNAVAILABLE"))
    r = gate.check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_roserpina_vista_snapshot_none_nega() -> None:
    """Loader che restituisce None => vista senza limiti => DENY."""
    gate, _ = _gate_roserpina(None)
    r = gate.check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_roserpina_vista_valore_non_finito_nega() -> None:
    """Un NaN scritto nel store attraversa il loader (_f non lo filtra): il
    gate lo intercetta come limite illeggibile e NEGA (H-13 sul config)."""
    gate, _ = _gate_roserpina(RoserpinaConfig(min_price=float("nan")))
    r = gate.check(payload())
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_roserpina_vista_flag_non_bool_nega() -> None:
    """Interruttore non-bool ("yes") => DENY: mai coercizioni di comodo."""
    gate, _ = _gate_roserpina(RoserpinaConfig(liquidity_warning_only="yes"))
    r = gate.check(payload(available_liquidity=1.0))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_CONFIG_MISSING"


def test_roserpina_max_win_resta_blocco_hard_sul_gate() -> None:
    """`max_win_warning_only` governa SOLO il precheck dutching: sul gate del
    percorso ordine il cap resta un BLOCCO (H-14), qualunque sia il flag —
    declassarlo ad avviso qui sarebbe fail-open sull'esposizione."""
    gate, _ = _gate_roserpina(
        RoserpinaConfig(max_win=100.0, max_win_warning_only=True)
    )
    r = gate.check(payload(stake=60.0, price=3.0))  # win 120 > 100
    assert r["allowed"] is False
    assert r["reason"] == "RISK_MAX_WIN_EXCEEDED"


def test_roserpina_vista_liquidita_owner_applicata() -> None:
    """Soglie di liquidita' owner (multiplier 5x, warning_only=False armato
    dalla GUI) applicate al gate: 40 < max(10*5, 50) => blocco reale."""
    gate, _ = _gate_roserpina(
        RoserpinaConfig(liquidity_multiplier=5.0, liquidity_warning_only=False)
    )
    r = gate.check(payload(available_liquidity=40.0))
    assert r["allowed"] is False
    assert r["reason"] == "RISK_LIQUIDITY_INSUFFICIENT"


def test_roserpina_vista_snapshot_isolato_per_thread() -> None:
    """R1 su #441 (GPT-5.6+Fable convergenti, fondato): con lo snapshot
    condiviso sull'istanza, un refresh() eseguito da un check CONCORRENTE
    su un altro thread sostituiva lo snapshot A META' del check corrente:
    letture strappate tra limiti (mix di config mai salvate insieme
    dall'owner). Lo snapshot e' per-THREAD: il refresh del thread B non
    tocca quello che il thread corrente sta leggendo."""
    import threading as _threading

    servizio = _SettingsRoserpina(RoserpinaConfig(min_price=1.02))
    vista = RoserpinaRiskLimits(servizio)
    vista.refresh()  # snapshot del thread corrente: min_price 1.02

    servizio.cfg = RoserpinaConfig(min_price=9.99)  # "salvataggio" owner
    thread_b = _threading.Thread(target=vista.refresh)
    thread_b.start()
    thread_b.join()

    # Il thread corrente, a meta' del SUO check, deve ancora vedere 1.02.
    assert vista.MIN_PRICE == 1.02
    # Il refresh successivo del thread corrente vede il valore nuovo.
    vista.refresh()
    assert vista.MIN_PRICE == 9.99
