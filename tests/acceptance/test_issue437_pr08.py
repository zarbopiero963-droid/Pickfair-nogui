"""PR-20 — catalogo Betfair locale e resolver deterministico, fail-closed.

Ogni test usa il ``Database`` reale in memoria, il ``DeterministicResolver`` reale e
il ``CatalogSyncService`` reale con un client di listing finto e offline. I casi
BLOCK riproducono i difetti del codice precedente: «il primo che capita» su nomi
ambigui, catalogo vecchio usato come fresco, caratteri jolly nel testo del
provider, mercati chiusi ancora risolvibili, due sync intrecciati, metadati
illeggibili che fermano l'avvio.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from database import Database
from services import catalog_sync_service as sync_mod
from services.catalog_sync_service import CatalogSyncService
from services.deterministic_resolver import (
    AMBIGUOUS,
    CATALOG_STALE,
    INVALID_INPUT,
    NOT_FOUND,
    OK,
    DeterministicResolver,
    normalize_name,
)

_NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
_SYNC = "2026-11-11T11:00:00.000000"   # un'ora prima di _NOW
_MAX_AGE = 6 * 3600.0


def _resolver(db, max_age=_MAX_AGE, now=_NOW):
    return DeterministicResolver(db, max_catalog_age_sec=max_age, now=lambda: now)


def _catalog(db, sync_id=_SYNC):
    db.upsert_bf_event("E1", "Manchester City v Real Madrid", "C1", "Champions League", "1", "2026-11-11T21:00:00", sync_id)
    db.upsert_bf_market("M1", "E1", "Correct Score", "CORRECT_SCORE", 1000.0, "2026-11-11T21:00:00", sync_id)
    db.upsert_bf_runner("M1", "11", "1 - 1", 0, 1, sync_id)
    db.upsert_bf_runner("M1", "21", "2 - 1", 0, 2, sync_id)
    db.update_sync_meta(last_sync_at=sync_id, last_sync_id=sync_id, events=1, markets=1, runners=2)


@pytest.fixture
def db():
    database = Database(db_path=":memory:")
    pid = database.save_provider("TipsterPro")
    database.save_name_alias(pid, "Manchester City", "man city", "UK")
    database.save_market_alias(pid, "risultato esatto", "CORRECT_SCORE", market_name="Correct Score")
    _catalog(database)
    return database, pid


# ── PASS: il percorso valido resta risolto ──────────────────────────────

def test_percorso_valido_evento_mercato_runner(db):
    database, pid = db
    resolver = _resolver(database)

    event = resolver.resolve_event_strict(pid, "  MAN   City ")
    assert (event.ok, event.reason, event.value) == (True, OK, "E1")
    assert resolver.resolve_event_strict(pid, "Real Madrid").value == "E1"
    assert resolver.resolve_event_strict(pid, "manchester city v real madrid").value == "E1"

    market = resolver.resolve_market_strict(pid, "E1", "Risultato  Esatto")
    assert market.ok and market.value["market_id"] == "M1" and market.value["market_type"] == "CORRECT_SCORE"

    runner = resolver.resolve_runner_strict("M1", "2 - 1")
    assert (runner.ok, runner.value) == (True, "21")


def test_normalizzazione_accenti_maiuscole_spazi(db):
    database, pid = db
    database.upsert_bf_event("E2", "Atlético Madrid v Málaga", "C2", "La Liga", "1", "", _SYNC)
    assert normalize_name("  ATLÉTICO   Madrid ") == "atletico madrid"
    assert normalize_name(None) == "" and normalize_name(12) == ""
    assert _resolver(database).resolve_event_strict(pid, "atletico madrid").value == "E2"
    assert _resolver(database).resolve_event_strict(pid, "MALAGA").value == "E2"


# ── BLOCK: ambiguita' = rifiuto, mai il primo candidato ─────────────────

def test_due_eventi_per_la_stessa_squadra_sono_ambigui(db):
    database, pid = db
    database.upsert_bf_event("E9", "Arsenal v Manchester City", "C3", "Premier League", "1", "", _SYNC)
    resolver = _resolver(database)

    result = resolver.resolve_event_strict(pid, "Man City")
    assert (result.ok, result.reason, result.value) == (False, AMBIGUOUS, None)
    assert result.candidates == ("E1", "E9")
    assert resolver.resolve_event(pid, "Man City") is None
    # il nome completo distingue ancora i due eventi
    assert resolver.resolve_event(pid, "Arsenal v Manchester City") == "E9"


def test_alias_che_collidono_dopo_la_normalizzazione_sono_ambigui(db):
    database, pid = db
    database.save_name_alias(pid, "Real Madrid", "MAN  CITY", "ES")
    result = _resolver(database).resolve_event_strict(pid, "man city")
    assert (result.ok, result.reason) == (False, AMBIGUOUS)


def test_due_mercati_dello_stesso_tipo_sono_ambigui(db):
    database, pid = db
    database.save_market_alias(pid, "over", "OVER_UNDER_25")
    database.upsert_bf_market("M2", "E1", "Over/Under 2.5 Goals", "OVER_UNDER_25", 1.0, "", _SYNC)
    database.upsert_bf_market("M3", "E1", "Over/Under 2.5 Goals (alt)", "OVER_UNDER_25", 1.0, "", _SYNC)
    resolver = _resolver(database)

    result = resolver.resolve_market_strict(pid, "E1", "over")
    assert (result.ok, result.reason, result.candidates) == (False, AMBIGUOUS, ("M2", "M3"))
    assert resolver.resolve_market(pid, "E1", "over") is None


def test_nome_mercato_dell_alias_disambigua(db):
    database, pid = db
    database.upsert_bf_market("M4", "E1", "Correct Score 2", "CORRECT_SCORE", 1.0, "", _SYNC)
    result = _resolver(database).resolve_market_strict(pid, "E1", "risultato esatto")
    assert result.ok and result.value["market_id"] == "M1"


def test_alias_mercato_in_conflitto_sono_ambigui(db):
    database, pid = db
    database.save_market_alias(pid, "Risultato Esatto", "MATCH_ODDS")
    result = _resolver(database).resolve_market_strict(pid, "E1", "risultato esatto")
    assert (result.ok, result.reason) == (False, AMBIGUOUS)


def test_due_runner_con_lo_stesso_nome_sono_ambigui(db):
    database, _ = db
    database.upsert_bf_runner("M1", "99", "2 - 1", 0, 9, _SYNC)
    resolver = _resolver(database)
    result = resolver.resolve_runner_strict("M1", "2 - 1")
    assert (result.ok, result.reason, result.candidates) == (False, AMBIGUOUS, ("21", "99"))
    assert resolver.resolve_runner("M1", "2 - 1") is None


# ── BLOCK: il testo del provider non e' un pattern ──────────────────────

@pytest.mark.parametrize("testo", ["%", "_", "man%", "% v %", "manchester cit_", "%city%"])
def test_caratteri_jolly_non_risolvono_nulla(db, testo):
    database, pid = db
    resolver = _resolver(database)
    assert resolver.resolve_event_strict(pid, testo).reason == NOT_FOUND
    assert resolver.resolve_runner_strict("M1", testo).reason == NOT_FOUND


@pytest.mark.parametrize("valore", [None, "", "   ", 12, 1.5, b"man city", ["man city"]])
def test_input_malformato_rifiutato(db, valore):
    database, pid = db
    resolver = _resolver(database)
    assert resolver.resolve_event_strict(pid, valore).reason == INVALID_INPUT
    assert resolver.resolve_market_strict(pid, "E1", valore).reason == INVALID_INPUT
    assert resolver.resolve_market_strict(pid, valore, "risultato esatto").reason == INVALID_INPUT
    assert resolver.resolve_runner_strict("M1", valore).reason == INVALID_INPUT
    assert resolver.resolve_runner_strict(valore, "1 - 1").reason == INVALID_INPUT
    assert resolver.resolve_event(pid, valore) is None


@pytest.mark.parametrize("provider", [None, "1", 1.0, True])
def test_provider_malformato_rifiutato(db, provider):
    database, _ = db
    resolver = _resolver(database)
    assert resolver.resolve_event_strict(provider, "man city").reason == INVALID_INPUT
    assert resolver.resolve_market_strict(provider, "E1", "risultato esatto").reason == INVALID_INPUT


def test_sconosciuti_non_inventano_nulla(db):
    database, pid = db
    resolver = _resolver(database)
    assert resolver.resolve_event_strict(pid, "Squadra Inesistente").reason == NOT_FOUND
    assert resolver.resolve_event_strict(pid, "city").reason == NOT_FOUND      # niente sottostringhe
    assert resolver.resolve_market_strict(pid, "E1", "frase mai vista").reason == NOT_FOUND
    assert resolver.resolve_market_strict(pid, "E404", "risultato esatto").reason == NOT_FOUND
    assert resolver.resolve_runner_strict("M1", "9 - 9").reason == NOT_FOUND
    assert resolver.resolve_runner_strict("M404", "1 - 1").reason == NOT_FOUND


# ── BLOCK: catalogo di eta' ignota o vecchio ────────────────────────────

def test_catalogo_senza_metadati_di_sync_rifiutato():
    database = Database(db_path=":memory:")
    pid = database.save_provider("P")
    database.upsert_bf_event("E1", "Roma v Lazio", "", "", "1", "", _SYNC)
    resolver = _resolver(database)
    assert resolver.catalog_block_reason() == CATALOG_STALE
    assert resolver.resolve_event_strict(pid, "Roma").reason == CATALOG_STALE
    assert resolver.resolve_event(pid, "Roma") is None


@pytest.mark.parametrize("last_sync_at", ["", "ieri", "2026-13-45T00:00:00", None])
def test_metadati_illeggibili_rifiutati(db, last_sync_at):
    database, pid = db
    database._execute("UPDATE sync_meta SET last_sync_at = ? WHERE id = 1", (last_sync_at,))
    resolver = _resolver(database)
    assert resolver.resolve_event_strict(pid, "Man City").reason == CATALOG_STALE
    assert resolver.resolve_market_strict(pid, "E1", "risultato esatto").reason == CATALOG_STALE
    assert resolver.resolve_runner_strict("M1", "1 - 1").reason == CATALOG_STALE


def test_soglia_di_eta_del_catalogo(db):
    database, pid = db
    sync = datetime(2026, 11, 11, 11, 0, 0, tzinfo=timezone.utc)
    al_limite = _resolver(database, now=sync + timedelta(seconds=_MAX_AGE))
    oltre = _resolver(database, now=sync + timedelta(seconds=_MAX_AGE + 1))
    assert al_limite.resolve_event_strict(pid, "Man City").ok
    assert oltre.resolve_event_strict(pid, "Man City").reason == CATALOG_STALE
    assert oltre.resolve_runner("M1", "1 - 1") is None


def test_sync_nel_futuro_oltre_tolleranza_rifiutato(db):
    database, pid = db
    sync = datetime(2026, 11, 11, 11, 0, 0, tzinfo=timezone.utc)
    assert _resolver(database, now=sync - timedelta(seconds=60)).resolve_event_strict(pid, "Man City").ok
    assert _resolver(database, now=sync - timedelta(seconds=61)).resolve_event_strict(pid, "Man City").reason == CATALOG_STALE


def test_orologio_senza_fuso_letto_come_utc(db):
    database, pid = db
    resolver = _resolver(database, now=datetime(2026, 11, 11, 12, 0, 0))
    assert resolver.resolve_event_strict(pid, "Man City").ok


@pytest.mark.parametrize("eta", [0, -1, None, "3600", float("nan"), float("inf"), True])
def test_soglia_di_eta_non_valida_rifiutata_alla_costruzione(eta):
    with pytest.raises(ValueError):
        DeterministicResolver(Database(db_path=":memory:"), max_catalog_age_sec=eta)


def test_soglia_di_eta_obbligatoria():
    with pytest.raises(TypeError):
        DeterministicResolver(Database(db_path=":memory:"))   # nessun default implicito


# ── Catalogo: upsert e pulizia ──────────────────────────────────────────

def test_upsert_aggiorna_i_campi_mutabili(db):
    database, _ = db
    nuovo = "2026-11-11T11:30:00.000000"
    database.upsert_bf_event("E1", "Manchester City v Real Madrid", "C1", "Champions League", "7", "2026-11-12T20:00:00", nuovo)
    database.upsert_bf_market("M1", "E1", "Correct Score (new)", "CORRECT_SCORE_2", 5.0, "2026-11-12T20:00:00", nuovo)
    database.upsert_bf_runner("M1", "11", "1 - 1", 1.5, 7, nuovo)

    ev = dict(database._execute("SELECT * FROM bf_events WHERE event_id='E1'", fetchone=True, commit=False))
    mk = dict(database._execute("SELECT * FROM bf_markets WHERE market_id='M1'", fetchone=True, commit=False))
    rn = dict(database._execute("SELECT * FROM bf_runners WHERE selection_id='11'", fetchone=True, commit=False))
    assert (ev["open_date"], ev["event_type_id"], ev["last_seen"]) == ("2026-11-12T20:00:00", "7", nuovo)
    assert (mk["market_name"], mk["market_type"], mk["open_date"], mk["total_matched"]) == (
        "Correct Score (new)", "CORRECT_SCORE_2", "2026-11-12T20:00:00", 5.0)
    assert (rn["handicap"], rn["sort_priority"], rn["last_seen"]) == (1.5, 7, nuovo)


def test_mercato_spostato_su_un_altro_evento(db):
    database, _ = db
    database.upsert_bf_event("E2", "Roma v Lazio", "", "", "1", "", _SYNC)
    database.upsert_bf_market("M1", "E2", "Correct Score", "CORRECT_SCORE", 1.0, "", _SYNC)
    row = database._execute("SELECT event_id FROM bf_markets WHERE market_id='M1'", fetchone=True, commit=False)
    assert row["event_id"] == "E2"


def test_pulizia_rimuove_mercati_e_runner_non_rivisti_di_un_evento_presente(db):
    database, pid = db
    nuovo = "2026-11-11T11:30:00.000000"
    # il nuovo sync rivede l'evento, un mercato nuovo e UN solo runner del vecchio mercato
    database.upsert_bf_event("E1", "Manchester City v Real Madrid", "C1", "Champions League", "1", "", nuovo)
    database.upsert_bf_market("M1", "E1", "Correct Score", "CORRECT_SCORE", 1.0, "", nuovo)
    database.upsert_bf_runner("M1", "11", "1 - 1", 0, 1, nuovo)
    database.upsert_bf_market("M5", "E1", "Match Odds", "MATCH_ODDS", 1.0, "", _SYNC)   # mercato chiuso: non rivisto
    database.upsert_bf_runner("M5", "1", "Manchester City", 0, 1, _SYNC)

    database.cleanup_stale_bf_data(nuovo)
    database.update_sync_meta(last_sync_at=nuovo, last_sync_id=nuovo, events=1, markets=1, runners=1)

    markets = [r["market_id"] for r in database._execute("SELECT market_id FROM bf_markets", fetch=True, commit=False)]
    runners = [(r["market_id"], r["selection_id"]) for r in database._execute(
        "SELECT market_id, selection_id FROM bf_runners", fetch=True, commit=False)]
    assert markets == ["M1"]
    assert runners == [("M1", "11")]
    # il runner ritirato non e' piu' risolvibile
    assert _resolver(database).resolve_runner_strict("M1", "2 - 1").reason == NOT_FOUND
    assert _resolver(database).resolve_runner_strict("M1", "1 - 1").value == "11"


def test_pulizia_rimuove_eventi_non_rivisti_con_i_loro_figli(db):
    database, _ = db
    nuovo = "2026-11-11T11:30:00.000000"
    database.upsert_bf_event("E2", "Roma v Lazio", "", "", "1", "", nuovo)
    database.cleanup_stale_bf_data(nuovo)
    for table, attesi in (("bf_events", 1), ("bf_markets", 0), ("bf_runners", 0)):
        n = database._execute(f"SELECT COUNT(*) AS n FROM {table}", fetchone=True, commit=False)["n"]
        assert n == attesi, table


def test_pulizia_atomica_su_errore(db, monkeypatch):
    database, _ = db
    originale = database._execute

    def _fallisce_sugli_eventi(sql, *a, **kw):
        if "DELETE FROM bf_events" in sql:
            raise RuntimeError("disco pieno")
        return originale(sql, *a, **kw)

    monkeypatch.setattr(database, "_execute", _fallisce_sugli_eventi)
    with pytest.raises(RuntimeError):
        database.cleanup_stale_bf_data("2026-11-11T11:30:00.000000")
    monkeypatch.undo()
    # rollback: runner e mercati cancellati prima dell'errore sono ancora li'
    assert database._execute("SELECT COUNT(*) AS n FROM bf_runners", fetchone=True, commit=False)["n"] == 2
    assert database._execute("SELECT COUNT(*) AS n FROM bf_markets", fetchone=True, commit=False)["n"] == 1


@pytest.mark.parametrize("sync_id", [None, "", "   ", 5])
def test_pulizia_senza_sync_id_rifiutata(db, sync_id):
    database, _ = db
    with pytest.raises(ValueError):
        database.cleanup_stale_bf_data(sync_id)
    assert database._execute("SELECT COUNT(*) AS n FROM bf_events", fetchone=True, commit=False)["n"] == 1


# ── Sync: esito, concorrenza, metadati, troncamento ─────────────────────

def _evento(eid, nome):
    return {"event": {"id": eid, "name": nome, "openDate": "2026-11-12T18:00:00.000Z"}, "marketCount": 1}


def _mercato(mid, eid, nome="Match Odds", tipo="MATCH_ODDS", runners=((1, "Home"), (2, "Away"))):
    return {
        "marketId": mid, "marketName": nome, "totalMatched": 10.0,
        "marketStartTime": "2026-11-12T18:00:00.000Z",
        "description": {"marketType": tipo}, "event": {"id": eid},
        "runners": [{"selectionId": s, "runnerName": n, "handicap": 0.0, "sortPriority": i}
                    for i, (s, n) in enumerate(runners, 1)],
    }


class _Client:
    def __init__(self, events, markets):
        self.events = events
        self.markets = markets          # event_id -> lista di mercati
        self.calls = []

    def list_events(self, event_type_ids, *, in_play_only=False):
        return self.events

    def list_market_catalogue(self, event_type_ids, event_ids=None, *, market_type_codes=None, max_results=200):
        self.calls.append(list(event_ids or []))
        out = []
        for eid in event_ids or []:
            out.extend(self.markets.get(eid, []))
        return out[:max_results]


def test_sync_completo_riporta_esito_e_ripulisce(db):
    database, pid = db
    client = _Client([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]})
    esito = CatalogSyncService(database, client).run_sync(force=True)

    assert esito == {"status": sync_mod.COMPLETED, "reason": "", "events": 1, "markets": 1, "runners": 2}
    assert [r["event_id"] for r in database._execute("SELECT event_id FROM bf_events", fetch=True, commit=False)] == ["E7"]
    meta = database.get_sync_meta()
    assert (meta["events_count"], meta["markets_count"], meta["runners_count"]) == (1, 1, 2)
    assert "." in meta["last_sync_at"] and meta["last_sync_at"] == meta["last_sync_id"]
    # catalogo appena sincronizzato: risolvibile con l'orologio reale
    resolver = DeterministicResolver(database, max_catalog_age_sec=60)
    assert resolver.resolve_event(pid, "Roma") == "E7"
    assert resolver.resolve_event(pid, "Man City") is None        # evento non rivisto: rimosso


@pytest.mark.parametrize("events,markets,reason", [
    ([], {}, "nessun_evento"),
    ([_evento("E7", "Roma v Lazio")], {}, "zero_mercati"),
    ([{"event": {}}], {}, "nessun_evento_valido"),
])
def test_sync_incompleto_non_tocca_catalogo_ne_metadati(db, events, markets, reason):
    database, pid = db
    esito = CatalogSyncService(database, _Client(events, markets)).run_sync(force=True)
    assert (esito["status"], esito["reason"]) == (sync_mod.INCOMPLETE, reason)
    assert database.get_sync_meta()["last_sync_id"] == _SYNC
    assert _resolver(database).resolve_runner("M1", "2 - 1") == "21"


def test_sync_fallito_riporta_failed_e_preserva(db):
    database, _ = db

    class _Rotto(_Client):
        def list_market_catalogue(self, *a, **kw):
            raise RuntimeError("upstream")

    esito = CatalogSyncService(database, _Rotto([_evento("E7", "Roma v Lazio")], {})).run_sync(force=True)
    assert (esito["status"], esito["reason"]) == (sync_mod.FAILED, "RuntimeError")
    assert database.get_sync_meta()["last_sync_id"] == _SYNC
    assert _resolver(database).resolve_runner("M1", "2 - 1") == "21"


def test_batch_al_limite_viene_bisecato_e_il_sync_si_completa(db):
    database, _ = db
    events = [_evento(f"E{i}", f"Casa{i} v Ospite{i}") for i in range(4)]
    markets = {f"E{i}": [_mercato(f"M{i}_{k}", f"E{i}", runners=()) for k in range(60)] for i in range(4)}
    client = _Client(events, markets)          # 240 mercati in un solo chunk: oltre il limite 200
    esito = CatalogSyncService(database, client).run_sync(force=True)

    assert (esito["status"], esito["events"], esito["markets"]) == (sync_mod.COMPLETED, 4, 240)
    assert client.calls == [["E0", "E1", "E2", "E3"], ["E0", "E1"], ["E2", "E3"]]
    assert database._execute("SELECT COUNT(*) AS n FROM bf_markets", fetchone=True, commit=False)["n"] == 240


def test_evento_singolo_al_limite_resta_troncato_e_preserva(db):
    database, _ = db
    markets = {"E7": [_mercato(f"M7_{k}", "E7", runners=()) for k in range(200)]}
    esito = CatalogSyncService(database, _Client([_evento("E7", "Roma v Lazio")], markets)).run_sync(force=True)
    assert (esito["status"], esito["reason"]) == (sync_mod.INCOMPLETE, "batch_troncato")
    assert database.get_sync_meta()["last_sync_id"] == _SYNC
    assert _resolver(database).resolve_runner("M1", "2 - 1") == "21"     # nulla cancellato


@pytest.mark.parametrize("last_sync_at", ["", "ieri", None])
def test_metadati_illeggibili_non_fermano_il_sync(db, last_sync_at):
    database, _ = db
    database._execute("UPDATE sync_meta SET last_sync_at = ? WHERE id = 1", (last_sync_at,))
    client = _Client([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]})
    esito = CatalogSyncService(database, client).run_sync(force=False)
    assert esito["status"] == sync_mod.COMPLETED


def test_catalogo_recente_salta_il_sync_senza_force(db, monkeypatch):
    database, _ = db
    client = _Client([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]})
    monkeypatch.setattr(sync_mod, "_utc_now", lambda: datetime(2026, 11, 11, 22, 59, 59))
    assert CatalogSyncService(database, client).run_sync()["status"] == sync_mod.SKIPPED_FRESH
    assert client.calls == []
    monkeypatch.setattr(sync_mod, "_utc_now", lambda: datetime(2026, 11, 11, 23, 0, 0))    # 12 ore esatte
    assert CatalogSyncService(database, client).run_sync()["status"] == sync_mod.COMPLETED


def test_sync_nel_futuro_non_vale_come_recente(db, monkeypatch):
    database, _ = db
    client = _Client([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]})
    monkeypatch.setattr(sync_mod, "_utc_now", lambda: datetime(2026, 11, 11, 10, 0, 0))    # prima dell'ultimo sync
    assert CatalogSyncService(database, client).run_sync()["status"] == sync_mod.COMPLETED


def test_due_sync_contemporanei_il_secondo_e_rifiutato(tmp_path):
    database = Database(db_path=str(tmp_path / "catalogo.db"))   # su file: condiviso fra i thread
    _catalog(database)
    dentro, via = threading.Event(), threading.Event()

    class _Lento(_Client):
        def list_events(self, *a, **kw):
            dentro.set()
            assert via.wait(10)
            return self.events

    lento = CatalogSyncService(database, _Lento([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]}))
    esiti = {}
    t = threading.Thread(target=lambda: esiti.setdefault("primo", lento.run_sync(force=True)))
    t.start()
    try:
        assert dentro.wait(10)
        altro = _Client([_evento("E8", "Milan v Inter")], {"E8": [_mercato("M8", "E8")]})
        secondo = CatalogSyncService(database, altro).run_sync(force=True)   # altra istanza, stesso DB
        assert (secondo["status"], secondo["reason"]) == (sync_mod.SKIPPED_BUSY, "sync_in_corso")
        assert altro.calls == []
    finally:
        via.set()
        t.join(10)
    assert esiti["primo"]["status"] == sync_mod.COMPLETED
    # il lock e' rilasciato: un nuovo sync parte
    assert CatalogSyncService(database, altro).run_sync(force=True)["status"] == sync_mod.COMPLETED


def test_lock_rilasciato_dopo_un_errore(db):
    database, _ = db

    class _Rotto(_Client):
        def list_events(self, *a, **kw):
            raise RuntimeError("giu'")

    svc = CatalogSyncService(database, _Rotto([], {}))
    assert svc.run_sync(force=True)["status"] == sync_mod.FAILED
    assert svc.is_syncing is False
    ok = _Client([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]})
    assert CatalogSyncService(database, ok).run_sync(force=True)["status"] == sync_mod.COMPLETED


def test_due_sync_consecutivi_hanno_id_distinti_e_confrontabili(db, monkeypatch):
    database, _ = db
    client = _Client([_evento("E7", "Roma v Lazio")], {"E7": [_mercato("M7", "E7")]})
    monkeypatch.setattr(sync_mod, "_utc_now", lambda: datetime(2026, 11, 12, 0, 0, 0))     # microsecondi a zero
    CatalogSyncService(database, client).run_sync(force=True)
    assert database.get_sync_meta()["last_sync_id"] == "2026-11-12T00:00:00.000000"
