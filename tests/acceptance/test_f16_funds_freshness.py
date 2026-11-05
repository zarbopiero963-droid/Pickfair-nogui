"""F-16 (#586, FUNDS-01) — freschezza della conferma dei fondi per un nuovo ingresso.

Decisione owner PF-LIVE-FUNDS-THRESHOLD-01 (#426 c.6162806642, 14/10/2026): «Approvo la soglia di 30 secondi per
il saldo LIVE». Una conferma dei fondi autorizza un nuovo ingresso solo se la sua eta' e' <= 30 s, misurata con
orologio monotono dal momento dell'acquisizione; oltre, o se la validita' non e' dimostrata, nessun nuovo ingresso.
Test obbligatorio T-F01 (c.6162826424): 29,999 s ammessa / 30,001 s rifiutata.

Su `main` `ada4076` il gate dei fondi d'ingresso (`RuntimeController._entry_funds_reason`) usava come eta' massima
`TableManager.PLACED_TOMBSTONE_TTL_S` (120 s, il TTL delle tracce delle riserve rilasciate) e saltava del tutto il
controllo con `read_at=None`. Percorsi coperti: ingresso da segnale, ingresso del DutchingController
(`controller_entry_funds_reason`), auto-next dopo il settlement (snapshot del conto).
"""
from __future__ import annotations

import time as _time

import pytest

import core.runtime_controller as rc_mod
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager
from tests.acceptance.test_issue437_pr27 import _rifiuti
from tests.acceptance.test_issue461_pr28e_funds_gate import _auto_next, _chain, _con_fondi
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

FONDI_OK = {"available": 500.0, "exposure": 0.0}


class _Orologio:
    """`time.monotonic` del modulo runtime_controller sotto controllo del test (orologio monotono simulato)."""

    def __init__(self, ora: float) -> None:
        self.ora = ora

    def monotonic(self) -> float:
        return self.ora


def _rc_minimo() -> RuntimeController:
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = TableManager(table_count=2)
    return rc


@pytest.fixture
def orologio(monkeypatch):
    o = _Orologio(1000.0)
    monkeypatch.setattr(rc_mod.time, "monotonic", o.monotonic)
    return o


# ------------------------------------------------------------------ T-F01: confine esatto, orologio monotono
@pytest.mark.parametrize("eta,ammessa", [(0.0, True), (29.999, True), (30.0, True), (30.001, False),
                                          (31.0, False), (119.0, False)])
def test_t_f01_eta_della_conferma_fondi(orologio, eta: float, ammessa: bool) -> None:
    rc = _rc_minimo()

    motivo = rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=orologio.ora - eta)

    if ammessa:
        assert motivo == "", f"conferma di {eta}s rifiutata: {motivo!r}"
    else:
        assert motivo == "fondi_non_verificabili:FUNDS_STALE", f"conferma di {eta}s ammessa: {motivo!r}"


def test_conferma_senza_istante_di_lettura_non_e_valida(orologio) -> None:
    """`read_at` assente: la validita' non e' dimostrata (PF-LIVE-FUNDS-POLICY-01 regola 5) => nessun ingresso."""
    rc = _rc_minimo()

    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=None) == "fondi_non_verificabili:FUNDS_STALE"


@pytest.mark.parametrize("read_at", [float("nan"), float("inf"), "abc", True])
def test_istante_di_lettura_non_valido_e_rifiutato(orologio, read_at) -> None:
    rc = _rc_minimo()

    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=read_at) == "fondi_non_verificabili:FUNDS_STALE"


def test_booleano_non_e_un_istante_anche_se_sembrerebbe_fresco(orologio) -> None:
    """`True` vale 1.0: con l'orologio monotono a 10 s sembrerebbe una lettura di 9 s. Non e' un istante: rifiuto."""
    rc = _rc_minimo()
    orologio.ora = 10.0

    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=True) == "fondi_non_verificabili:FUNDS_STALE"
    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=1.0) == ""      # lo stesso valore come numero: ammesso


def test_lettura_nel_futuro_dell_orologio_monotono_e_rifiutata(orologio) -> None:
    """Un `read_at` successivo all'ora monotona non viene da questo orologio: eta' non dimostrabile."""
    rc = _rc_minimo()

    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=orologio.ora + 5.0) == \
        "fondi_non_verificabili:FUNDS_STALE"


def test_orologio_di_sistema_spostato_non_cambia_l_esito(orologio, monkeypatch) -> None:
    """T-F01: «orologio di sistema spostato senza effetto». Solo l'orologio monotono decide."""
    rc = _rc_minimo()
    monkeypatch.setattr(rc_mod.time, "time", lambda: 0.0)
    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=orologio.ora - 29.0) == ""
    monkeypatch.setattr(rc_mod.time, "time", lambda: 9.9e9)
    assert rc._entry_funds_reason(dict(FONDI_OK), 10.0, read_at=orologio.ora - 31.0) == \
        "fondi_non_verificabili:FUNDS_STALE"


def test_soglia_owner_e_dentro_il_ttl_delle_tracce_delle_riserve() -> None:
    """30 s e' la soglia owner; deve restare <= al TTL delle tracce (altrimenti le riserve non sarebbero confrontabili)."""
    assert RuntimeController.LIVE_FUNDS_MAX_AGE_S == 30.0
    assert RuntimeController.LIVE_FUNDS_MAX_AGE_S <= TableManager.PLACED_TOMBSTONE_TTL_S


# ------------------------------------------------------------------ DutchingController (controller_entry_funds_reason)
def test_controller_lettura_senza_read_at_e_rifiutata(orologio) -> None:
    rc = _rc_minimo()

    assert rc.controller_entry_funds_reason({"funds": dict(FONDI_OK)}, extra_exposure=5.0) == \
        "fondi_non_verificabili:FUNDS_STALE"


def test_controller_lettura_di_31s_e_rifiutata_e_di_29s_ammessa(orologio) -> None:
    rc = _rc_minimo()

    assert rc.controller_entry_funds_reason({"funds": dict(FONDI_OK), "read_at": orologio.ora - 31.0},
                                            extra_exposure=5.0) == "fondi_non_verificabili:FUNDS_STALE"
    assert rc.controller_entry_funds_reason({"funds": dict(FONDI_OK), "read_at": orologio.ora - 29.0},
                                            extra_exposure=5.0) == ""


# ------------------------------------------------------------------ ingresso da segnale (catena runtime reale)
def _lettura_invecchiata(c, secondi: float) -> None:
    lettura = c.rc._read_funds_for_entry

    def _vecchia():
        out = lettura()
        out["read_at"] = _time.monotonic() - secondi        # es. get_account_funds lento o attesa sul lock
        return out

    c.rc._read_funds_for_entry = _vecchia


def test_segnale_con_conferma_fondi_di_60s_e_rifiutato(tmp_path) -> None:
    c = _chain(tmp_path, stake=10.0, anti_duplication=True)
    _lettura_invecchiata(c, 60.0)
    _con_fondi(c, 500.0)

    c.rc._on_signal_received(_signal())

    assert "fondi_non_verificabili:FUNDS_STALE" in _rifiuti(c), _rifiuti(c)
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.rc.duplication_guard._active == {}, "rifiuto per fondi: chiave rilasciata, segnale ritentabile"


def test_segnale_con_conferma_fondi_fresca_parte(tmp_path) -> None:
    c = _chain(tmp_path, stake=10.0, anti_duplication=True)
    _lettura_invecchiata(c, 5.0)
    _con_fondi(c, 500.0)

    c.rc._on_signal_received(_signal())

    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, _rifiuti(c)


# ------------------------------------------------------------------ auto-next (snapshot del settlement)
def _auto_next_con_snapshot_vecchio(monkeypatch, secondi: float):
    originale = RuntimeController._record_account_snapshot

    def _registra(self, funds, *, source, read_at=None):
        if read_at is not None:
            read_at = float(read_at) - secondi              # settlement elaborato `secondi` dopo la lettura
        return originale(self, funds, source=source, read_at=read_at)

    monkeypatch.setattr(RuntimeController, "_record_account_snapshot", _registra)
    return _auto_next({"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY"},
                      funds={"available": 150.0, "exposure": 0.0})


def test_auto_next_con_snapshot_fondi_di_60s_e_rifiutato(monkeypatch) -> None:
    _rc, bus, esito = _auto_next_con_snapshot_vecchio(monkeypatch, 60.0)

    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == [], esito
    assert esito["reason"] == "fondi_non_verificabili:FUNDS_STALE", esito


def test_auto_next_con_snapshot_fondi_fresco_parte(monkeypatch) -> None:
    _rc, bus, esito = _auto_next_con_snapshot_vecchio(monkeypatch, 5.0)

    assert len([e for e in bus.events if e[0] == "CMD_QUICK_BET"]) == 1, esito


# ------------------------------------------------------------------ manual bet e lotto Dutching (DutchingController reale)
def _catena_controller_con_lettura(tmp_path, secondi: float):
    from tests.acceptance.test_issue437_pr27 import _catena as _catena_dc

    c = _catena_dc(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "total": 1000.0,
                                                      "simulated": True}
    _lettura_invecchiata(c, secondi)
    return c


@pytest.mark.parametrize("ingresso", ["manual_bet", "submit_dutching"])
def test_controller_ingresso_con_lettura_di_31s_rifiutato_senza_effetti(tmp_path, ingresso: str) -> None:
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _dutching, _manual, _no_side_effects

    c = _catena_controller_con_lettura(tmp_path, 31.0)
    dc = _dutching(c)
    res = dc.manual_bet(_manual()) if ingresso == "manual_bet" else dc.submit_dutching(_batch())

    assert res["ok"] is False and res["reason"] == "fondi_non_verificabili:FUNDS_STALE", res
    _no_side_effects(c)


@pytest.mark.parametrize("ingresso", ["manual_bet", "submit_dutching"])
def test_controller_ingresso_con_lettura_di_29s_parte(tmp_path, ingresso: str) -> None:
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _dutching, _manual

    c = _catena_controller_con_lettura(tmp_path, 29.0)
    dc = _dutching(c)
    res = dc.manual_bet(_manual(stake=4.0)) if ingresso == "manual_bet" else dc.submit_dutching(
        _batch(total_stake=10.0))

    assert res["ok"] is True, res


def test_accessore_unico_della_soglia_dei_fondi(monkeypatch) -> None:
    """Richiesta Claude C (MCP-01k): la Control API legge la soglia effettiva da un solo accessore, la stessa che decide."""
    import time as _time

    from core.runtime_controller import RuntimeController
    from core.table_manager import TableManager

    assert RuntimeController.funds_max_age_s() == 30.0
    monkeypatch.setattr(TableManager, "PLACED_TOMBSTONE_TTL_S", 12.0)
    assert RuntimeController.funds_max_age_s() == 12.0
    now = _time.monotonic()
    assert RuntimeController._funds_confirmation_fresh(now - 11.0) is True
    assert RuntimeController._funds_confirmation_fresh(now - 13.0) is False
