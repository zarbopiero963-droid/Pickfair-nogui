"""M154 / M154-bis (owner, single-account) — runtime: un solo conto Betfair per installazione.

Identita' = SOLO `client.authenticated_identity()` (mai `client.username` ne' la configurazione). Identita' autorizzata
`betfair_authorized_identity` = hash dello username configurato, associata UNA volta alla prima connessione LIVE con
un'identita' attestata uguale, salvata e riletta, mai riscritta (F4). Identita' mancante, diversa o non associabile =>
nessun ingresso LIVE, nessuna lettura/cancellazione/certificazione nello sweep, nessun reset di uno stop con obblighi
LIVE. CX-N1: lo sweep certifica solo l'ambito attestato dal client che ha letto, se non e' cambiato nel frattempo.
Banco: catena reale del runtime (`_banco` di PR-04a), conti LIVE finti con attestazione esplicita."""
from __future__ import annotations

import json
import threading
import types

import pytest

from core import financial_scope
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from tests.d1.test_pr04a_financial_scope import (
    CONTO_A,
    CONTO_B,
    _attendi,
    _banco,
    _ordine_bot,
    _riserva,
    _segna_riga_live,
    _settle_tavolo,
)

pytestmark = [pytest.mark.integration, pytest.mark.safety]
IDENTITA_REALE = True        # nessun adattatore dei client finti legacy (tests/conftest.py): identita' solo attestata

RC = RuntimeController
KEY = RC._AUTHORIZED_IDENTITY_KEY


class _Conto:
    """Conto LIVE finto: `username` di configurazione e attestazione del login separati (None = nessun login)."""

    def __init__(self, username, attestato="__uguale__", ordini=None, cancella=True):
        self.username = username
        self.attestato = username if attestato == "__uguale__" else attestato
        self.ordini, self.cancella = list(ordini or []), cancella
        self.letture, self.cancellazioni = 0, []
        self.durante_lettura = None

    def authenticated_identity(self):
        return self.attestato

    def get_current_orders(self, market_ids=None):
        self.letture += 1
        if self.durante_lettura:
            azione, self.durante_lettura = self.durante_lettura, None
            azione()
        return [dict(o) for o in self.ordini]

    def cancel_orders(self, market_id, bet_ids=None, **kw):
        self.cancellazioni.append((market_id, bet_ids))
        if self.cancella:
            self.ordini = [o for o in self.ordini if o.get("marketId") != market_id]
            return {"ok": True, "instructionReports": [{"betId": b} for b in (bet_ids or [])]}
        return {"ok": False, "status": "FAILURE"}


def _configura(c, username):
    if username is None:
        c.rc.betfair_service.settings_service = None
        return
    c.rc.betfair_service.settings_service = types.SimpleNamespace(
        load_betfair_config=lambda *a, **k: types.SimpleNamespace(username=username))


def _connetti(c, conto):
    c.rc.betfair_service.client = conto
    c.rc.betfair_service.get_live_client = lambda: conto


def _hash(u):
    return RC._identity_hash(u)


def _ambito(u):
    return financial_scope.runtime_scope(simulated=False, live_account=_hash(u))


def _autorizzata(c):
    return (c.rc.db.get_settings() or {}).get(KEY)


def _registro(c):
    raw = (c.rc.db.get_settings() or {}).get(RC._LIVE_ACCOUNTS_KEY) or ""
    data = json.loads(raw) if raw else {}
    return {k: v.get("state") for k, v in (data.get("accounts") or {}).items()}


def _banco_semplice(tmp_path, nome, configurato=CONTO_A, conto=None):
    """Catena reale del runtime, nessuna associazione ancora: configurazione e conto connesso scelti dal test."""
    c = _banco(tmp_path, nome, max_daily_loss=30.0)
    c.rc._live_recovery_interval_s = 60.0
    if configurato is not None:
        _configura(c, configurato)
    _connetti(c, conto or _Conto(CONTO_A))
    return c


def _banco_live(tmp_path, nome, configurato=CONTO_A, conto=None):
    """Ordine LIVE del bot piazzato dal conto A attestato e configurato (prima associazione), poi runtime in SIM con il
    conto scelto dal test connesso. Riaperto sullo stesso db: l'identita' autorizzata e' quella salvata."""
    c = _banco(tmp_path, nome, max_daily_loss=30.0)
    c.rc._live_recovery_interval_s = 60.0
    tavolo = None
    if not (c.rc.db.get_settings() or {}).get(KEY):
        _configura(c, CONTO_A)
        a = _Conto(CONTO_A)
        _connetti(c, a)
        c.rc.set_simulation_mode(False)
        fondi = {"available": 1000.0, "exposure": 0.0, "ok": True, "simulated": False}
        c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
        assert c.rc._apply_funds_reading(dict(fondi), source="m154")["applied"]
        c.rc.mode = RuntimeMode.ACTIVE
        tavolo = _riserva(c, "1.971")
        _segna_riga_live(c, tavolo.meta["customer_ref"])
        c.rc.set_simulation_mode(True)
    _configura(c, configurato)
    _connetti(c, conto or _Conto(CONTO_A))
    c.rc._route_cashout_signal = lambda segnale: None
    c.rc.mode = RuntimeMode.ACTIVE
    return c, tavolo


def _stop_su_a(tmp_path, nome, cancella=False):
    """ES da superamento LIVE di A (ordine del bot aperto, cancellazione rifiutata): A PENDING, associato."""
    c, tavolo = _banco_live(tmp_path, nome)
    conto_a = _Conto(CONTO_A, ordini=[_ordine_bot(tavolo)], cancella=cancella)
    _connetti(c, conto_a)
    _settle_tavolo(c, tavolo, -40.0)
    assert c.rc._emergency_stopped is True
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RC._live_recovery_active(c.rc))
    return c, tavolo, conto_a


def _sweep(c):
    c.rc._emergency_reconcile_pending = True
    try:
        return RC._reconcile_restored_emergency(c.rc, trigger="m154")
    finally:
        c.rc._emergency_reconcile_pending = False


# ============================================================================ identita' attestata, mai username
def test_m154_il_runtime_legge_solo_l_identita_attestata(tmp_path):
    c = _banco_semplice(tmp_path, "id1.db")
    _connetti(c, _Conto(CONTO_A, attestato=None))
    assert RC._live_account_identity(c.rc) == "", "identita' dallo username senza login (ripiego vietato)"
    _connetti(c, _Conto(CONTO_B, attestato=CONTO_A))
    assert RC._live_account_identity(c.rc) == _hash(CONTO_A), "username manomesso usato come identita'"
    _connetti(c, types.SimpleNamespace(username=CONTO_A))
    assert RC._live_account_identity(c.rc) == "", "client senza attestazione: identita' dallo username"


def test_m154_ambito_contabile_senza_attestazione_e_l_autorizzato_mai_la_configurazione(tmp_path):
    c = _banco_semplice(tmp_path, "id2.db")
    _connetti(c, _Conto(CONTO_A, attestato=None))
    assert c.rc._financial_scope_for(False) == financial_scope.runtime_scope(simulated=False, live_account="")
    _connetti(c, _Conto(CONTO_A))
    assert RC._live_identity_check(c.rc) == (_ambito(CONTO_A), "")
    _connetti(c, _Conto(CONTO_A, attestato=None))
    assert c.rc._financial_scope_for(False) == _ambito(CONTO_A), "sessione in riconnessione: perdita fuori dal conto"


# ============================================================================ prima associazione (migrazione)
def test_m154_prima_associazione_solo_con_attestata_uguale_alla_configurata(tmp_path):
    c = _banco_semplice(tmp_path, "as1.db")
    assert _autorizzata(c) in (None, "")
    assert RC._live_identity_block_reason(c.rc) == ""
    assert _autorizzata(c) == _hash(CONTO_A)


@pytest.mark.parametrize("caso,configurato,conto,motivo", [
    ("senza_configurazione", None, _Conto(CONTO_A), "username_configurato_assente"),
    ("attestata_diversa", CONTO_A, _Conto(CONTO_B), "identita_diversa_dalla_configurata"),
    ("senza_login", CONTO_A, _Conto(CONTO_A, attestato=None), "identita_non_attestata"),
    ("username_manomesso", CONTO_A, _Conto(CONTO_A, attestato=CONTO_B), "identita_diversa_dalla_configurata"),
])
def test_m154_nessuna_associazione_e_nessun_ingresso_live(tmp_path, caso, configurato, conto, motivo):
    c = _banco_semplice(tmp_path, f"as2_{caso}.db", configurato=configurato, conto=conto)
    assert RC._live_identity_block_reason(c.rc) == f"live_identity:{motivo}"
    assert _autorizzata(c) in (None, ""), "associazione da una sola delle due prove"


def test_m154_registro_con_un_conto_reale_diverso_nessuna_associazione(tmp_path):
    c = _banco_semplice(tmp_path, "as3.db")
    RC._bind_live_account(c.rc, _ambito(CONTO_B), {"key": "m154-b", "kind": "daily"})
    assert RC._live_identity_block_reason(c.rc) == "live_identity:registro_con_altri_conti"
    assert _autorizzata(c) in (None, "")


def test_m154_registro_con_la_sola_sentinella_o_lo_stesso_conto_associa(tmp_path):
    c = _banco_semplice(tmp_path, "as4.db")
    RC._bind_live_account(c.rc, RC._LIVE_ACCOUNT_UNIDENTIFIED, {"key": "m154-u", "kind": "manual"})
    RC._bind_live_account(c.rc, _ambito(CONTO_A), {"key": "m154-a", "kind": "daily"})
    assert RC._live_identity_block_reason(c.rc) == ""
    assert _autorizzata(c) == _hash(CONTO_A)


@pytest.mark.parametrize("guasto", ["falso", "eccezione", "rilettura_diversa"])
def test_m154_associazione_non_durevole_resta_bloccata(tmp_path, monkeypatch, guasto):
    c = _banco_semplice(tmp_path, f"as5_{guasto}.db")
    reale_save, reale_get = c.rc.db.save_settings, c.rc.db.get_settings

    def save(data, *a, **k):
        if KEY in data:
            if guasto == "falso":
                return False
            if guasto == "eccezione":
                raise OSError("disco pieno")
        return reale_save(data, *a, **k)

    def get(*a, **k):
        out = dict(reale_get(*a, **k) or {})
        if guasto == "rilettura_diversa" and KEY in out:
            out[KEY] = _hash(CONTO_B)
        return out
    monkeypatch.setattr(c.rc.db, "save_settings", save)
    monkeypatch.setattr(c.rc.db, "get_settings", get)
    assert RC._live_identity_block_reason(c.rc).startswith("live_identity:")


def test_m154_chiave_illeggibile_mai_riscritta_e_lettura_fallita_blocca(tmp_path, monkeypatch):
    c = _banco_semplice(tmp_path, "as6.db")
    c.rc.db.save_settings({KEY: "non-un-hash"})
    assert RC._live_identity_block_reason(c.rc) == "live_identity:identita_autorizzata_illeggibile"
    assert _autorizzata(c) == "non-un-hash", "chiave corrotta riscritta"
    monkeypatch.setattr(c.rc.db, "get_settings", lambda *a, **k: (_ for _ in ()).throw(OSError("db")))
    assert RC._live_identity_block_reason(c.rc) == "live_identity:identita_autorizzata_illeggibile"


# ============================================================================ F4: mai riscritta, anche fra riavvii
def test_m154_f4_altro_account_bloccato_anche_dopo_riavvii_ripetuti(tmp_path):
    c = _banco_semplice(tmp_path, "f4.db")
    assert RC._live_identity_block_reason(c.rc) == ""
    for giro in range(4):
        r = _banco_semplice(tmp_path, "f4.db", configurato=CONTO_B if giro % 2 else CONTO_A,
                            conto=_Conto(CONTO_B))
        assert RC._live_identity_block_reason(r.rc) == "live_identity:identita_non_autorizzata", giro
        assert _autorizzata(r) == _hash(CONTO_A), "identita' autorizzata riscritta"
    r = _banco_semplice(tmp_path, "f4.db", conto=_Conto(CONTO_A))
    assert RC._live_identity_block_reason(r.rc) == ""


def test_m154_ingresso_live_al_punto_d_invio_rifiutato_senza_identita(tmp_path):
    c = _banco_semplice(tmp_path, "inv.db")
    c.rc.set_simulation_mode(False)
    c.rc.mode = RuntimeMode.ACTIVE
    assert RC._live_identity_block_reason(c.rc) == ""
    _connetti(c, _Conto(CONTO_A, attestato=None))
    motivo = c.rc.drawdown_entry_block_reason()
    assert motivo == "live_identity:identita_non_attestata", motivo
    _connetti(c, _Conto(CONTO_B))
    assert c.rc.drawdown_entry_block_reason() == "live_identity:identita_non_autorizzata"
    c.rc.set_simulation_mode(True)
    assert not str(c.rc.drawdown_entry_block_reason()).startswith("live_identity:"), "la SIM dipende dall'identita'"


def test_m154_ingresso_live_senza_alcun_client_rifiutato(tmp_path):
    c = _banco_semplice(tmp_path, "nocl.db")
    c.rc.betfair_service.client = None
    c.rc.set_simulation_mode(False)
    c.rc.mode = RuntimeMode.ACTIVE
    assert RC._live_identity_block_reason(c.rc) == "live_identity:identita_non_attestata"
    assert c.rc.drawdown_entry_block_reason() == "live_identity:identita_non_attestata"


# ============================================================================ sweep, certificazione, reset
def test_m154_sweep_con_identita_diversa_nessuna_lettura_cancellazione_o_certificazione(tmp_path):
    c, _t, conto_a = _stop_su_a(tmp_path, "sw1.db")
    estraneo = _Conto(CONTO_B, ordini=[{"betId": "X", "marketId": "1.971", "status": "EXECUTABLE",
                                       "sizeRemaining": 2.0}])
    _connetti(c, estraneo)
    esito = _sweep(c)
    assert esito["verification"] == "ACCOUNT_MISMATCH" and esito["account_mismatch"]["error"] == \
        "identita_non_autorizzata"
    assert estraneo.letture == 0 and estraneo.cancellazioni == []
    assert _registro(c).get(_ambito(CONTO_A)) == "PENDING" and _ambito(CONTO_B) not in _registro(c)


def test_m154_sweep_senza_login_nessuna_lettura(tmp_path):
    c, _t, conto_a = _stop_su_a(tmp_path, "sw2.db")
    anonimo = _Conto(CONTO_A, attestato=None, ordini=[])
    _connetti(c, anonimo)
    esito = _sweep(c)
    assert esito["account_mismatch"]["error"] == "identita_non_attestata" and anonimo.letture == 0
    assert c.rc.reset_emergency()["emergency_reset"] is False


def test_m154_cx_n1_cambio_di_identita_durante_lo_sweep_nessuna_certificazione(tmp_path):
    c, _t, conto_a = _stop_su_a(tmp_path, "n1.db")
    conto_a.ordini, conto_a.cancella = [], True
    b = _Conto(CONTO_B)
    conto_a.durante_lettura = lambda: _connetti(c, b)
    esito = _sweep(c)
    assert esito["verification"] == "IDENTITY_CHANGED" and esito["identity_changed"] is True
    assert _registro(c).get(_ambito(CONTO_A)) == "PENDING" and _registro(c).get(_ambito(CONTO_B)) is None
    assert c.rc.reset_emergency()["emergency_reset"] is False


def test_m154_cx_n1_relogin_su_un_altro_conto_dello_stesso_client_durante_lo_sweep(tmp_path):
    c, _t, conto_a = _stop_su_a(tmp_path, "n1b.db")
    conto_a.ordini, conto_a.cancella = [], True

    def relogin():
        conto_a.attestato = CONTO_B
    conto_a.durante_lettura = relogin
    esito = _sweep(c)
    assert esito["identity_changed"] is True and _registro(c).get(_ambito(CONTO_A)) == "PENDING"


def test_m154_username_manomesso_durante_lo_sweep_non_conta(tmp_path):
    c, _t, conto_a = _stop_su_a(tmp_path, "n1c.db")
    conto_a.ordini, conto_a.cancella = [], True

    def manomette():
        conto_a.username = CONTO_B
    conto_a.durante_lettura = manomette
    esito = _sweep(c)
    assert esito["verification"] == "VERIFIED" and _registro(c).get(_ambito(CONTO_A)) == "VERIFIED"
    assert _ambito(CONTO_B) not in _registro(c)


def test_m154_reset_dell_es_richiede_l_identita_autorizzata_connessa(tmp_path):
    c, _t, conto_a = _stop_su_a(tmp_path, "rs.db")
    conto_a.ordini, conto_a.cancella = [], True
    _sweep(c)
    assert _registro(c).get(_ambito(CONTO_A)) == "VERIFIED"
    for estraneo in (_Conto(CONTO_A, attestato=None), _Conto(CONTO_B)):
        _connetti(c, estraneo)
        esito = c.rc.reset_emergency()
        assert esito["emergency_reset"] is False and esito["reason"].startswith("identita_live_non_verificata:"), esito
        assert c.rc._emergency_stopped is True
    _connetti(c, conto_a)
    assert c.rc.reset_emergency()["emergency_reset"] is True


def test_m154_reset_senza_obblighi_live_non_richiede_identita(tmp_path):
    c = _banco_semplice(tmp_path, "rs2.db", configurato=None, conto=_Conto(CONTO_A, attestato=None))
    c.rc.set_simulation_mode(True)
    c.rc.emergency_stop("m154: ES manuale solo SIM")
    assert c.rc.reset_emergency()["emergency_reset"] is True


def test_m154_associazione_concorrente_di_due_identita_una_sola_vince_e_non_si_riscrive(tmp_path):
    c = _banco_semplice(tmp_path, "cc.db")
    esiti, barriera = [], threading.Barrier(8)

    def prova(conto):
        barriera.wait()
        esiti.append(RC._live_identity_check(c.rc, conto))
    fili = [threading.Thread(target=prova, args=(_Conto(CONTO_A if i % 2 else CONTO_B),)) for i in range(8)]
    for f in fili:
        f.start()
    for f in fili:
        f.join(10)
    assert _autorizzata(c) == _hash(CONTO_A)
    assert all(err in ("", "identita_non_autorizzata", "identita_diversa_dalla_configurata")
               for _s, err in esiti), esiti
    assert not any(s == _ambito(CONTO_B) and err == "" for s, err in esiti), "B autorizzato"


# ============================================================================ sostituti single-account (M154-bis)
# I quattro test di PR-04a che codificano un SUCCESSO con un secondo conto (c.7004520954 §4) restano invariati finche'
# #638 non si chiude (decisione owner: nulla si rimuove prima), e girano con l'adattatore legacy di tests/conftest.py.
# Qui la stessa proprieta' sull'unico conto, con l'identita' reale, piu' la variante avversaria «identita' estranea».
from tests.d1.test_pr04a_financial_scope import (  # noqa: E402
    _g_banco,
    _m147_banco,
    _ordine_b,
    _reset_confermato,
    _settle,
)


def _unico(c, ordini=None, cancella=True):
    _configura(c, CONTO_A)
    a = _Conto(CONTO_A, ordini=ordini, cancella=cancella)
    _connetti(c, a)
    return a


def _riconcilia(c):
    c.rc._emergency_reconcile_pending = True
    RC._reconcile_restored_emergency(c.rc, trigger="m154_sostituto")


def _fine(c):
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RC._live_recovery_active(c.rc))


def test_m154_sostituto_m146_h2_reset_solo_dopo_la_riconciliazione_reale_dell_unico_conto(tmp_path):
    """Sostituisce la parte finale di test_m146_h2 (B riconciliato => reset): stop di sessione dell'unico conto A con un
    ordine del bot non cancellabile => esposizione non verificata, reset confermato rifiutato; ordine chiuso e A
    riconciliato => VERIFICATA e reset."""
    c = _g_banco(tmp_path, "s146.db")
    a = _unico(c, ordini=[_ordine_b()], cancella=False)
    _settle(c, -40.0, event_key="cleared:1.9681:1")
    assert (RC._live_bound_accounts(c.rc) or {}).get(_ambito(CONTO_A)) == "PENDING"
    _riconcilia(c)
    assert c.rc.get_status()["live_exposure"]["state"] != "VERIFICATA"
    assert _reset_confermato(c.rc)["risk_stop_reset"] is False and c.rc._risk_stop_reason
    a.ordini, a.cancella = [], True
    _riconcilia(c)
    assert c.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    assert _reset_confermato(c.rc)["risk_stop_reset"] is True
    _fine(c)


def test_m154_sostituto_m146_h2_avversaria_identita_estranea(tmp_path):
    c = _g_banco(tmp_path, "s146b.db")
    a = _unico(c, ordini=[_ordine_b()], cancella=False)
    _settle(c, -40.0, event_key="cleared:1.9681:1")
    a.ordini, a.cancella = [], True
    b = _Conto(CONTO_B)
    _connetti(c, b)
    _riconcilia(c)
    assert b.letture == 0 and b.cancellazioni == []
    assert (RC._live_bound_accounts(c.rc) or {}).get(_ambito(CONTO_A)) == "PENDING"
    assert _ambito(CONTO_B) not in (RC._live_bound_accounts(c.rc) or {})
    assert _reset_confermato(c.rc)["risk_stop_reset"] is False
    _fine(c)


def test_m154_sostituto_m147_reset_emergency_ammesso_con_l_unico_conto_verificato(tmp_path):
    """Sostituisce test_m147_reset_emergency_ammesso_solo_con_tutti_i_conti_verificati: A irrisolto => reset rifiutato;
    A riconciliato => reset accettato (nessun blocco permanente)."""
    c = _m147_banco(tmp_path, "s147.db")
    a = _unico(c, ordini=[_ordine_b()], cancella=False)
    c.rc.emergency_stop("m154: ES dell'unico conto", live_exposure=True, scope=_ambito(CONTO_A))
    _riconcilia(c)
    assert c.rc.reset_emergency()["emergency_reset"] is False
    a.ordini, a.cancella = [], True
    _riconcilia(c)
    assert c.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    assert c.rc.reset_emergency()["emergency_reset"] is True and not c.rc._emergency_stopped
    _fine(c)


def test_m154_sostituto_m147_avversaria_identita_estranea_non_toglie_l_es(tmp_path):
    c = _m147_banco(tmp_path, "s147b.db")
    a = _unico(c)
    c.rc.emergency_stop("m154: ES dell'unico conto", live_exposure=True, scope=_ambito(CONTO_A))
    _riconcilia(c)
    assert (RC._live_bound_accounts(c.rc) or {}).get(_ambito(CONTO_A)) == "VERIFIED"
    _connetti(c, _Conto(CONTO_B))
    assert c.rc.reset_emergency()["emergency_reset"] is False and c.rc._emergency_stopped
    _connetti(c, a)
    assert c.rc.reset_emergency()["emergency_reset"] is True
    _fine(c)


def test_m154_sostituto_m151bis_k2_obbligo_in_sospeso_dell_unico_conto(tmp_path):
    """Sostituisce test_m151bis_k2 (obbligo di B in sospeso): binding dell'UNICO conto con il registro non rileggibile
    => in sospeso (memoria e giornale), reset rifiutato; riconciliazione => entra nel registro, il sospeso sparisce."""
    import os
    c = _m147_banco(tmp_path, "s151.db")
    a = _unico(c, ordini=[_ordine_b()], cancella=False)
    c.rc.emergency_stop("m154: ES dell'unico conto", live_exposure=True, scope=_ambito(CONTO_A))
    vero = c.rc.db.get_settings

    def lettura(*x, **k):
        raise OSError("M154: registro non leggibile")
    c.rc.db.get_settings = lettura
    try:
        RC._bind_live_account(c.rc, _ambito(CONTO_A), breach={"key": "daily|m154|2026-11-19", "kind": "daily"})
    finally:
        c.rc.db.get_settings = vero
    giornale = str(tmp_path / "s151.db") + RC._SAFETY_JOURNAL_SUFFIX
    assert _ambito(CONTO_A) in json.loads(json.load(open(giornale))[RC._PENDING_BINDINGS_KEY])
    esito = c.rc.reset_emergency()
    assert esito["emergency_reset"] is False and _ambito(CONTO_A) in esito["accounts_pending"]
    _riconcilia(c)
    assert not c.rc.__dict__.get("_pending_live_bindings") and not os.path.exists(giornale)
    assert c.rc.reset_emergency()["emergency_reset"] is False, "ordine del bot ancora aperto"
    a.ordini, a.cancella = [], True
    _riconcilia(c)
    assert c.rc.reset_emergency()["emergency_reset"] is True
    _fine(c)


def test_m154_sostituto_m152_l2_nessun_blocco_permanente_sull_unico_conto(tmp_path):
    """Sostituisce test_m152_l2_nessun_blocco_permanente (B riconciliato): stop globale con l'unico conto PENDING =>
    bloccato; A riconciliato davvero => nessun blocco. Variante avversaria: con B connesso resta bloccato."""
    c = _m147_banco(tmp_path, "s152.db")
    assert RC._live_reset_blocker(c.rc) == ""
    a = _unico(c)
    c.rc._risk_stop_reason = "session_loss_breached:loss=40.00:limit=30.0"
    __import__("core.risk_stop", fromlist=["persist_risk_stop"]).persist_risk_stop(c.rc.db, c.rc._risk_stop_reason)
    RC._set_live_verify_only(c.rc, True)
    RC._bind_live_account(c.rc, _ambito(CONTO_A))
    assert RC._live_reset_blocker(c.rc) == "esposizione_live_irrisolta"
    _connetti(c, _Conto(CONTO_B))
    _riconcilia(c)
    assert RC._live_reset_blocker(c.rc) == "esposizione_live_irrisolta"
    _connetti(c, a)
    _riconcilia(c)
    assert (RC._live_bound_accounts(c.rc) or {}).get(_ambito(CONTO_A)) == "VERIFIED"
    assert RC._live_reset_blocker(c.rc) == ""
    _connetti(c, _Conto(CONTO_B))
    assert RC._live_reset_blocker(c.rc).startswith("identita_live_non_verificata:")
    _fine(c)
