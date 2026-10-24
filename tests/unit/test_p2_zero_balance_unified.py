"""Phase 2 (PF-POST-556, decisione owner): avvio, `get_status` e settlement applicano la lettura del conto con lo
STESSO contratto (`RuntimeController._apply_funds_reading`).

Prima:
- l'avvio allineava il bankroll al disponibile anche a 0 (conto con €0 disponibili e €100 impegnati => bankroll 0:
  falso drawdown, possibile LOCKDOWN);
- `get_status` lasciava il bankroll ma invalidava la cassa (CASH_READING_MISMATCH);
- il settlement lasciava tutto, salvo azzerare il bankroll se lo zero era marcato `ok`/`authoritative`/
  `balance_confirmed`.

Ora, per tutte e tre:
- A, disponibile 0 con impegnato: BALANCE_ZERO_COMMITTED, bankroll invariato, posizioni intatte, nuove entrate
  bloccate dai fondi freschi;
- B, disponibile 0 e impegnato 0: ACCOUNT_EMPTY_VERIFIED, idem, nessun falso drawdown/LOCKDOWN;
- C, lettura non valida: FUNDS_UNVERIFIABLE, nessun valore inventato, cassa non verificata, entrate bloccate;
- D, fondi tornati: la prima lettura d'ingresso valida riallinea bankroll e cassa (anche headless, senza
  get_status), senza doppi conteggi.

Catena reale (`_catena`): RuntimeController, TradingEngine, SimulationBroker, money management vero.
"""
from __future__ import annotations

import threading
import time

import pytest

from core.runtime_controller import RuntimeController
from tests.acceptance.test_issue437_pr27 import _catena
from tests.acceptance.test_issue523_dutching_manual_bet import _dutching, _manual
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]

A = {"available": 0.0, "exposure": 100.0, "total": 100.0, "simulated": True}    # €0 disponibili, €100 impegnati
B = {"available": 0.0, "exposure": 0.0, "total": 0.0, "simulated": True}        # patrimonio zero verificato
C_ERRORE = {"available": 0.0, "exposure": 0.0, "error": "FILL_LEDGER_INCONSISTENT", "simulated": True}
C_CAMPO = {"available": 50.0, "simulated": True}                                # `exposure` mancante
VALIDO = {"available": 1000.0, "exposure": 0.0, "total": 1000.0, "simulated": True}

STATO = {"A": RuntimeController.ACCOUNT_ZERO_COMMITTED, "B": RuntimeController.ACCOUNT_EMPTY_VERIFIED,
         "C_ERRORE": RuntimeController.ACCOUNT_UNVERIFIABLE, "C_CAMPO": RuntimeController.ACCOUNT_UNVERIFIABLE}
FONDI = {"A": A, "B": B, "C_ERRORE": C_ERRORE, "C_CAMPO": C_CAMPO}

_seq = iter(range(10 ** 6))


def _conto(c, fondi):
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)


def _settlement(c):
    n = next(_seq)
    return c.rc._sync_bankroll_post_settlement({"event_key": f"evt-p2-{n}", "batch_id": f"b-p2-{n}",
                                                "correlation_id": f"corr-p2-{n}", "gross_pnl": -1.0})


def _applica(c, percorso, fondi):
    _conto(c, fondi)
    if percorso == "avvio":
        c.rc._applica_fondi_avvio(dict(fondi), read_at=time.monotonic())
    elif percorso == "stato":
        c.rc.get_status()
    else:
        _settlement(c)


def _fotografia(c):
    desk = c.rc.risk_desk
    return {"bankroll": float(desk.bankroll_current), "picco": float(desk.equity_peak),
            "stato": c.rc._account_state, "mode": str(getattr(c.rc.mode, "value", c.rc.mode))}


# ----------------------------------------------------------------------------- A / B: lo zero non azzera
@pytest.mark.parametrize("caso", ["A", "B"])
@pytest.mark.parametrize("percorso", ["avvio", "stato", "settlement"])
def test_zero_verificato_non_azzera_il_bankroll_e_ha_uno_stato_esplicito(tmp_path, percorso, caso):
    c = _catena(tmp_path)
    _applica(c, percorso, FONDI[caso])
    foto = _fotografia(c)
    assert foto["bankroll"] == 1000.0 and foto["picco"] == 1000.0, foto     # prima all'avvio: bankroll 0
    assert foto["stato"] == STATO[caso], foto
    assert foto["mode"] != "LOCKDOWN", foto
    # PFC-008: la cassa vecchia non resta in uso, con lo stato come motivo esplicito
    assert c.rc.risk_desk.cash_equity_current is None and c.rc.risk_desk.cash_equity_reason == STATO[caso]


@pytest.mark.parametrize("caso", ["A", "B", "C_ERRORE", "C_CAMPO"])
def test_avvio_stato_e_settlement_danno_lo_stesso_esito(tmp_path, caso):
    esiti = {}
    for percorso in ("avvio", "stato", "settlement"):
        c = _catena(tmp_path / percorso)
        _applica(c, percorso, FONDI[caso])
        esiti[percorso] = (_fotografia(c), c.rc.risk_desk.cash_equity_current is None)
    assert esiti["avvio"] == esiti["stato"] == esiti["settlement"], esiti


def test_settlement_con_zero_marcato_confermato_non_azzera_il_bankroll(tmp_path):
    """Prima `ok`/`authoritative`/`balance_confirmed` con disponibile 0 allineavano il bankroll a 0."""
    for marcatura in ("ok", "authoritative", "balance_confirmed"):
        c = _catena(tmp_path / marcatura)
        _conto(c, dict(A, **{marcatura: True}))
        esito = _settlement(c)
        assert float(c.rc.risk_desk.bankroll_current) == 1000.0, (marcatura, esito)
        assert esito["account_state"] == RuntimeController.ACCOUNT_ZERO_COMMITTED
        assert esito["bankroll_sync_status"] == "SYNC_FAILED_BALANCE_UNAVAILABLE"     # auto-next resta saltato


@pytest.mark.parametrize("caso", ["A", "B"])
def test_avvio_a_zero_non_apre_la_baseline_del_ciclo_su_zero(tmp_path, caso):
    c = _catena(tmp_path)
    salvati = []
    c.rc.db.save_settings = lambda d: salvati.append(dict(d))
    c.rc._applica_fondi_avvio(dict(FONDI[caso]), read_at=time.monotonic())
    assert not any(RuntimeController._EXPANSION_KEY in d for d in salvati)


@pytest.mark.parametrize("caso", ["A", "B"])
def test_con_zero_verificato_le_nuove_entrate_si_fermano(tmp_path, caso):
    c = _catena(tmp_path)
    _applica(c, "avvio", FONDI[caso])
    c.rc._on_signal_received(_signal())
    assert c.bus.payloads("CMD_QUICK_BET") == [] and c.broker.state.orders == {}
    res = _dutching(c).manual_bet(_manual(stake=4.0))
    assert res["ok"] is False, res


def test_con_zero_verificato_le_posizioni_restano(tmp_path):
    """Caso A dopo un ingresso: il tavolo e la sua esposizione non vengono liberati da una lettura a zero."""
    c = _catena(tmp_path)
    _conto(c, VALIDO)
    c.rc._on_signal_received(_signal())
    occupati = [(t.table_id, t.current_exposure) for t in c.rc.table_manager.active_tables()]
    assert occupati
    for percorso in ("stato", "settlement"):
        _applica(c, percorso, A)
        assert [(t.table_id, t.current_exposure) for t in c.rc.table_manager.active_tables()] == occupati
        assert float(c.rc.risk_desk.bankroll_current) == 1000.0


# ----------------------------------------------------------------------------- C: non verificabile
@pytest.mark.parametrize("caso", ["C_ERRORE", "C_CAMPO"])
@pytest.mark.parametrize("percorso", ["avvio", "stato", "settlement"])
def test_lettura_non_valida_non_inventa_valori_e_blocca(tmp_path, percorso, caso):
    c = _catena(tmp_path)
    _applica(c, percorso, FONDI[caso])
    assert float(c.rc.risk_desk.bankroll_current) == 1000.0
    assert c.rc.risk_desk.cash_equity_current is None                       # cassa non verificata
    assert c.rc._account_state == RuntimeController.ACCOUNT_UNVERIFIABLE
    c.rc._on_signal_received(_signal())
    assert c.bus.payloads("CMD_QUICK_BET") == []


def test_settlement_senza_exposure_non_sincronizza_il_bankroll(tmp_path):
    """Prima il settlement allineava il bankroll al solo `available` anche senza `exposure` (avvio e get_status
    lo trattavano gia' come non verificabile)."""
    c = _catena(tmp_path)
    _conto(c, C_CAMPO)
    esito = _settlement(c)
    assert esito["bankroll_sync_status"] == "SYNC_FAILED_BALANCE_UNAVAILABLE", esito
    assert esito["reason"] == "BALANCE_FIELD_MISSING"
    assert float(c.rc.risk_desk.bankroll_current) == 1000.0


# ----------------------------------------------------------------------------- D: i fondi tornano
@pytest.mark.parametrize("caso", ["A", "B", "C_ERRORE"])
def test_quando_i_fondi_tornano_il_segnale_riparte_senza_get_status(tmp_path, caso):
    """Headless: nessuna lettura di stato. La lettura fresca dell'ingresso riallinea bankroll e cassa."""
    c = _catena(tmp_path)
    _applica(c, "avvio", FONDI[caso])
    _conto(c, VALIDO)                                         # i fondi tornano (stesso capitale: nessun drawdown)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert c.rc._account_state == RuntimeController.ACCOUNT_FUNDS_VERIFIED
    assert c.rc.risk_desk.cash_equity_current is not None


def test_quando_i_fondi_tornano_la_bet_manuale_riparte(tmp_path):
    c = _catena(tmp_path)
    _applica(c, "avvio", B)
    _conto(c, VALIDO)
    assert _dutching(c).manual_bet(_manual(stake=4.0))["ok"] is True


def test_ripresa_senza_doppi_conteggi(tmp_path):
    """Fondi tornati: il bankroll e' il disponibile letto, non disponibile + bankroll registrato o impegnato."""
    c = _catena(tmp_path)
    _applica(c, "settlement", A)
    _applica(c, "stato", dict(VALIDO, available=300.0, exposure=100.0, total=400.0))
    assert float(c.rc.risk_desk.bankroll_current) == 300.0
    assert c.rc.risk_desk.cash_equity_current == 400.0


def test_ripresa_all_ingresso_non_cambia_nulla_con_stato_gia_verificato(tmp_path):
    c = _catena(tmp_path)
    _applica(c, "stato", VALIDO)
    prima = (float(c.rc.risk_desk.bankroll_current), c.rc.risk_desk.cash_basis())
    _conto(c, dict(VALIDO, available=700.0, total=700.0))
    c.rc._read_funds_for_entry()
    assert (float(c.rc.risk_desk.bankroll_current), c.rc.risk_desk.cash_basis()) == prima


# ----------------------------------------------------------------------------- ordine delle letture
def test_una_lettura_a_zero_piu_vecchia_non_sovrascrive_quella_valida(tmp_path):
    c = _catena(tmp_path)
    vecchia = time.monotonic()
    _conto(c, dict(VALIDO, available=500.0, total=500.0))
    c.rc.get_status()                                        # lettura piu' recente: 500
    esito = c.rc._apply_funds_reading(dict(A), source="settlement", read_at=vecchia)
    assert esito["applied"] is False
    assert c.rc._account_state == RuntimeController.ACCOUNT_FUNDS_VERIFIED
    assert float(c.rc.risk_desk.bankroll_current) == 500.0


def test_letture_concorrenti_zero_e_valide_finiscono_coerenti(tmp_path):
    """get_status (valido) e settlement (zero) in parallelo: alla fine stato, bankroll e cassa vengono dalla
    STESSA lettura (nessuna combinazione mezza vecchia e mezza nuova)."""
    for giro in range(20):
        c = _catena(tmp_path / str(giro))
        letture = iter([dict(VALIDO, available=600.0, total=600.0), dict(A)] * 4)
        lock = threading.Lock()

        def leggi():
            with lock:
                return next(letture)
        c.rc.betfair_service.get_account_funds = leggi
        partenza = threading.Barrier(2)

        def stato():
            partenza.wait()
            c.rc.get_status()

        def settlement():
            partenza.wait()
            _settlement(c)
        threads = [threading.Thread(target=f) for f in (stato, settlement)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        bankroll = float(c.rc.risk_desk.bankroll_current)
        assert bankroll in (600.0, 1000.0), bankroll
        if c.rc._account_state == RuntimeController.ACCOUNT_FUNDS_VERIFIED:
            assert bankroll == 600.0 and c.rc.risk_desk.cash_equity_current == 600.0
        else:
            assert c.rc._account_state == RuntimeController.ACCOUNT_ZERO_COMMITTED


def test_get_status_espone_lo_stato_del_conto(tmp_path):
    c = _catena(tmp_path)
    _conto(c, A)
    data = c.rc.get_status()
    assert data["account_state"] == RuntimeController.ACCOUNT_ZERO_COMMITTED
    assert data["account_state_source"] == "status"


# ----------------------------------------------------------------------------- review indipendente sul diff Phase 2
def test_lettura_che_solleva_in_get_status_e_non_verificabile_e_poi_riprende(tmp_path):
    """P2: un'eccezione nella lettura di get_status invalidava solo la cassa e lasciava lo stato FUNDS_VERIFIED: la
    ripresa all'ingresso non scattava e il segnale restava bloccato fino a un'altra lettura di stato."""
    c = _catena(tmp_path)
    _conto(c, VALIDO)
    c.rc.get_status()

    def guasto():
        raise RuntimeError("rete")
    c.rc.betfair_service.get_account_funds = guasto
    with pytest.raises(RuntimeError):
        c.rc.get_status()
    assert c.rc._account_state == RuntimeController.ACCOUNT_UNVERIFIABLE
    _conto(c, VALIDO)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1


def test_lettura_che_solleva_al_settlement_e_non_verificabile(tmp_path):
    c = _catena(tmp_path)
    _conto(c, VALIDO)
    c.rc.get_status()

    def guasto():
        raise RuntimeError("rete")
    c.rc.betfair_service.get_account_funds = guasto
    esito = _settlement(c)
    assert esito["account_state"] == RuntimeController.ACCOUNT_UNVERIFIABLE
    assert c.rc.risk_desk.cash_equity_current is None


def test_cassa_invalidata_fuori_da_una_lettura_riprende_all_ingresso(tmp_path):
    """P2: dopo il P&L applicato (o snapshot/ripristino) la cassa non e' verificata anche con stato FUNDS_VERIFIED:
    la prima lettura d'ingresso valida la riallinea."""
    c = _catena(tmp_path)
    _conto(c, VALIDO)
    c.rc.get_status()
    c.rc.risk_desk.apply_closed_pnl(0.0)
    assert c.rc.risk_desk.cash_equity_current is None
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1


def test_settlement_con_disponibile_non_numerico_segue_il_contratto(tmp_path):
    c = _catena(tmp_path)
    _conto(c, VALIDO)
    c.rc.get_status()
    _conto(c, {"available": None, "exposure": 0.0, "simulated": True})
    esito = _settlement(c)
    assert esito["account_state"] == RuntimeController.ACCOUNT_UNVERIFIABLE
    assert c.rc.risk_desk.cash_equity_current is None


def test_avvio_a_zero_con_impegnato_apre_la_baseline_alla_ripresa(tmp_path):
    """P3: con l'avvio in caso A la baseline del ciclo (F032) non si apriva mai nella sessione. Ora si apre alla
    prima lettura verificata della ripresa, se nessuna baseline e' gia' registrata."""
    c = _catena(tmp_path)
    c.rc._applica_fondi_avvio(dict(A), read_at=time.monotonic())
    assert c.rc._expansion_baseline is None
    _conto(c, VALIDO)
    c.rc._on_signal_received(_signal())
    assert c.rc._expansion_baseline is not None


def test_nessuna_lettura_pre_ammissione_a_runtime_non_attivo(tmp_path):
    """P3: la lettura del conto prima dell'ammissione partiva anche a runtime fermo (rete, ripresa di sessione LIVE)
    per un segnale che i gate avrebbero comunque rifiutato."""
    from core.system_state import RuntimeMode

    c = _catena(tmp_path)
    _conto(c, A)
    c.rc.get_status()
    letture = []
    c.rc.betfair_service.get_account_funds = lambda: letture.append(1) or dict(VALIDO)
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._on_signal_received(_signal())
    assert letture == []
