"""M153 (owner M155, commit 2) — SafetyStateOwner: unico proprietario dello stato di sicurezza.

P4 guardia AST (con mutanti di sensibilita'), P1 precondizione e scrittura nella stessa sezione critica, P7 rimozione del
marker dell'ES dal settlement mai sopra un ES dichiarato dopo (C13 al confine interno), P8 proprietario non inizializzato
= blocco, giornale come istantanea scritta solo dal proprietario (CX-M1, C02, formato precedente), reset dell'ES che tocca
solo l'ES (CX-M2, C12, C04), pending-stop giornaliero tolto dal settlement che l'ha armato (nessun blocco perpetuo),
tracciatore dell'ordine dei lock. Banco: catena reale del runtime (`_catena_su` di D-1), db SQLite reale in tmp_path."""
from __future__ import annotations

import ast
import json
import os
import pathlib
import threading
import time

import pytest

import core.atomic_io as atomic_io
from core import safety_state
from core.runtime_controller import RuntimeController
from core.safety_state import SafetyStateOwner
from tests.d1.test_d1_drawdown_hard_stop import _catena_su

pytestmark = [pytest.mark.safety, pytest.mark.concurrency]

RC = RuntimeController
ROOT = pathlib.Path(__file__).resolve().parents[2]


# ============================================================================ P4: guardia AST
_SCOPED_DIRS = ("core", "services", "controllers", "ai", "ui_panels")
_SCOPED_ROOT_FILES = ("headless_main.py", "mini_gui.py", "betfair_client.py", "database.py")


def _db_key_names() -> dict:
    """Costanti di RuntimeController che valgono una chiave di sicurezza del db (es. _LIVE_ACCOUNTS_KEY)."""
    return {name: getattr(RuntimeController, name) for name in dir(RuntimeController)
            if isinstance(getattr(RuntimeController, name, None), str)
            and getattr(RuntimeController, name) in safety_state.DB_KEYS}


def _keys_of(node: ast.AST, consts: dict) -> set:
    keys = set()
    if isinstance(node, ast.Dict):
        for k in node.keys:
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.add(k.value)
            elif isinstance(k, ast.Attribute) and k.attr in consts:
                keys.add(consts[k.attr])
    return keys


def _via_proprietario(func: ast.AST) -> bool:
    """`RuntimeController._owner(x).persist_risk_stop(...)` oppure `owner.persist_risk_stop(...)`."""
    base = getattr(func, "value", None)
    if isinstance(base, ast.Name) and base.id == "owner":
        return True
    return isinstance(base, ast.Call) and getattr(base.func, "attr", "") == "_owner"


def violazioni(source: str, filename: str) -> list:
    """Scritture dello stato di sicurezza fuori dal proprietario: assegnazione/`setattr`/`__dict__` dei campi posseduti,
    `save_settings` di chiavi di sicurezza, `persist_risk_stop` diretto, scrittura o rimozione del giornale."""
    consts = _db_key_names()
    out = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for sub in ast.walk(target):
                    if isinstance(sub, ast.Attribute) and sub.attr in safety_state.FIELDS \
                            and isinstance(sub.ctx, ast.Store):
                        out.append(f"{filename}:{line} assegnazione di {sub.attr}")
                    if isinstance(sub, ast.Subscript) and isinstance(sub.slice, ast.Constant) \
                            and sub.slice.value in safety_state.FIELDS and isinstance(sub.ctx, ast.Store):
                        out.append(f"{filename}:{line} __dict__[{sub.slice.value!r}]")
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            first = node.args[0] if node.args else None
            if name == "setattr" and len(node.args) > 1 and isinstance(node.args[1], ast.Constant) \
                    and node.args[1].value in safety_state.FIELDS:
                out.append(f"{filename}:{line} setattr di {node.args[1].value}")
            if name in ("setdefault", "pop", "update") and isinstance(first, ast.Constant) \
                    and first.value in safety_state.FIELDS:
                out.append(f"{filename}:{line} {name} di {first.value}")
            if name == "update" and first is not None and _keys_of(first, {}) & safety_state.FIELDS:
                out.append(f"{filename}:{line} __dict__.update dei campi posseduti")
            if name == "save_settings" and first is not None and _keys_of(first, consts) & safety_state.DB_KEYS:
                out.append(f"{filename}:{line} save_settings di {sorted(_keys_of(first, consts) & safety_state.DB_KEYS)}")
            if name == "persist_risk_stop" and not _via_proprietario(func):
                out.append(f"{filename}:{line} persist_risk_stop fuori dal proprietario")
            if name in ("atomic_write_text", "remove", "unlink") and any(
                    isinstance(s, ast.Constant) and isinstance(s.value, str) and "safety_pending" in s.value
                    or isinstance(s, ast.Attribute) and s.attr in ("_safety_journal_path", "_SAFETY_JOURNAL_SUFFIX")
                    or isinstance(s, ast.Name) and s.id in ("path",) and filename.endswith("runtime_controller.py")
                    for a in node.args for s in ast.walk(a)):
                out.append(f"{filename}:{line} {name} sul giornale/marker fuori dal proprietario")
    return out


def _sorgenti():
    files = [p for d in _SCOPED_DIRS for p in (ROOT / d).rglob("*.py")]
    files += [ROOT / f for f in _SCOPED_ROOT_FILES if (ROOT / f).exists()]
    for path in sorted(files):
        rel = str(path.relative_to(ROOT))
        if rel == "core/safety_state.py":
            continue
        source = path.read_text(encoding="utf-8")
        if rel == "core/risk_stop.py":
            # il modulo dello stop di rischio scrive i propri marker SOLO in `persist_risk_stop`, chiamato dal proprietario
            tree = ast.parse(source)
            for fn in [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name != "persist_risk_stop"]:
                yield rel + "::" + fn.name, ast.get_source_segment(source, fn)
            continue
        yield rel, source


def test_p4_nessuna_scrittura_dello_stato_di_sicurezza_fuori_dal_proprietario():
    trovate = []
    for rel, source in _sorgenti():
        trovate.extend(violazioni(source, rel))
    assert not trovate, "\n".join(trovate[:30])


_MUTANTI = {
    "assegnazione": "    self._emergency_stopped = False\n",
    "assegnazione_rc": "    rc._risk_stop_reason = ''\n",
    "setattr": "    setattr(self, '_session_loss_pending_global', False)\n",
    "dict_store": "    self.__dict__['_daily_loss_pending_stop'] = False\n",
    "dict_pop": "    self.__dict__.pop('_pending_live_bindings', None)\n",
    "save_chiave": "    self.db.save_settings({'emergency_stopped': '0'})\n",
    "save_costante": "    self.db.save_settings({RuntimeController._LIVE_ACCOUNTS_KEY: ''})\n",
    "persist_diretto": "    stop_rules.persist_risk_stop(self.db, '')\n",
    "giornale": "    os.remove(RuntimeController._safety_journal_path(self))\n",
    "augassign": "    self._emergency_reconcile_pending |= True\n",
}


@pytest.mark.parametrize("mutante", sorted(_MUTANTI))
def test_p4_la_guardia_scatta_su_ogni_mutante(mutante):
    """Sensibilita': ogni forma di scrittura diretta aggiunta al runtime fa fallire la guardia."""
    source = (ROOT / "core/runtime_controller.py").read_text(encoding="utf-8")
    assert not violazioni(source, "core/runtime_controller.py")
    mutato = source + "\n\ndef _mutante_m153(self):\n" + _MUTANTI[mutante]
    assert violazioni(mutato, "core/runtime_controller.py"), f"guardia cieca al mutante {mutante}"


def test_p4_transizione_con_campo_non_dichiarato_rifiutata(tmp_path):
    c = _catena_su(tmp_path, "p4.db")
    with pytest.raises(ValueError):
        RC._owner(c.rc).apply(c.rc, "reset_es", _risk_stop_reason="")
    with pytest.raises(ValueError):
        RC._owner(c.rc).apply(c.rc, "transizione_inventata", _emergency_stopped=False)


# ============================================================================ banco
def _banco(tmp_path, nome="m153.db"):
    c = _catena_su(tmp_path, nome, max_daily_loss=30.0)
    c.rc.set_simulation_mode(True)
    return c


def _giornale(c):
    path = RC._safety_journal_path(c.rc)
    return json.load(open(path)) if os.path.exists(path) else None


def _db(c, key):
    return (c.rc.db.get_settings() or {}).get(key)


class _Pausa:
    def __init__(self):
        self.dentro, self.via = threading.Event(), threading.Event()

    def attraversa(self):
        self.dentro.set()
        assert self.via.wait(5), "pausa mai rilasciata"


# ============================================================================ CX-M1 / C02: giornale
def test_cx_m1_due_scrittori_del_giornale_nessuna_scrittura_persa(tmp_path, monkeypatch):
    """ES non durevole (giornale) || obbligo LIVE in sospeso (giornale), con il primo scrittore fermo DENTRO la scrittura:
    il secondo attende il proprietario e scrive l'istantanea completa; nessuna chiave persa. Prima (cef002ba): il secondo
    leggeva il giornale vecchio e lo riscriveva senza l'ES (M1)."""
    c = _banco(tmp_path, "m1.db")
    pausa, reale = _Pausa(), atomic_io.atomic_write_text
    primo = {"tid": None}

    def scrivi(path, text, *a, **k):
        if str(path).endswith(".safety_pending.json") and threading.get_ident() == primo["tid"] \
                and not pausa.dentro.is_set():
            pausa.attraversa()
        return reale(path, text, *a, **k)
    monkeypatch.setattr(atomic_io, "atomic_write_text", scrivi)

    def binding():
        primo["tid"] = threading.get_ident()
        RC._journal_safety_state(c.rc, {RC._PENDING_BINDINGS_KEY: json.dumps({"betfair|LIVE|betfair:b|EUR": []})})
    t = threading.Thread(target=binding)
    t.start()
    assert pausa.dentro.wait(5)
    es = threading.Thread(target=RC._journal_safety_state, args=(c.rc, {"emergency_stopped": "1"}))
    es.start()
    time.sleep(0.2)
    assert es.is_alive(), "il secondo scrittore non ha atteso il proprietario"
    pausa.via.set()
    t.join(5)
    es.join(5)
    stato = _giornale(c)
    assert stato.get("emergency_stopped") == "1" and RC._PENDING_BINDINGS_KEY in stato, stato
    assert stato.get(safety_state.JOURNAL_SEQ_KEY), "istantanea senza seq"


def test_c02_rimozione_del_giornale_non_cancella_un_es_riscritto(tmp_path, monkeypatch):
    """settle degli obblighi (rimozione della loro chiave) || ES non durevole: la rimozione e la riscrittura dell'ES sono
    serializzate dal proprietario; l'ES resta nel giornale. Prima: `os.remove` dopo la riscrittura dell'ES (C02)."""
    c = _banco(tmp_path, "c02.db")
    RC._journal_safety_state(c.rc, {RC._PENDING_BINDINGS_KEY: json.dumps({"x": []})})
    pausa, reale = _Pausa(), os.remove
    primo = {"tid": None}

    def rimuovi(path, *a, **k):
        if str(path).endswith(".safety_pending.json") and threading.get_ident() == primo["tid"] \
                and not pausa.dentro.is_set():
            pausa.attraversa()
        return reale(path, *a, **k)
    monkeypatch.setattr(os, "remove", rimuovi)

    def settle():
        primo["tid"] = threading.get_ident()
        RC._drop_safety_journal(c.rc, [RC._PENDING_BINDINGS_KEY])
    t = threading.Thread(target=settle)
    t.start()
    assert pausa.dentro.wait(5)
    es = threading.Thread(target=RC._journal_safety_state, args=(c.rc, {"emergency_stopped": "1"}))
    es.start()
    time.sleep(0.2)
    pausa.via.set()
    t.join(5)
    es.join(5)
    stato = _giornale(c)
    assert stato and stato.get("emergency_stopped") == "1", f"ES perso dal giornale: {stato}"


def test_giornale_illeggibile_mai_riscritto(tmp_path):
    c = _banco(tmp_path, "l1.db")
    path = RC._safety_journal_path(c.rc)
    pathlib.Path(path).write_text("{non json", encoding="utf-8")
    assert RC._journal_safety_state(c.rc, {"emergency_stopped": "1"}) is False
    assert pathlib.Path(path).read_text(encoding="utf-8") == "{non json"
    assert RC._drop_safety_journal(c.rc, ["emergency_stopped"]) is False and os.path.exists(path)


def test_giornale_formato_precedente_con_es_recuperato_e_migrato_solo_dopo_il_salvataggio_durevole(tmp_path,
                                                                                                    monkeypatch):
    """Giornale M151/M152 (chiavi in piano, nessuna `journal_seq`) con l'ES attivo e db pulito: all'avvio l'ES e' attivo.
    Con il db che NON salva, il giornale resta identico (nessuna migrazione); con il db che salva, l'ES e' durevole sul
    db e il giornale non ne ha piu' copia."""
    c = _banco(tmp_path, "old.db")
    path = RC._safety_journal_path(c.rc)
    vecchio = {"emergency_stopped": "1", "emergency_stopped_at": "2026-11-18T10:00:00", "emergency_reason": "VECCHIO",
               "emergency_settlement_source": "SIM", RC._HEDGE_GUARD_KEY: ""}
    pathlib.Path(path).write_text(json.dumps(vecchio, sort_keys=True), encoding="utf-8")
    import database as dbmod
    reale = dbmod.Database.save_settings

    def save_ko(self, data, *a, **k):
        if "emergency_stopped" in (data or {}):
            return False
        return reale(self, data, *a, **k)
    monkeypatch.setattr(dbmod.Database, "save_settings", save_ko)
    r = _catena_su(tmp_path, "old.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped is True and r.rc._emergency_reason == "VECCHIO"
    assert json.load(open(path)) == vecchio, "giornale precedente riscritto senza salvataggio durevole"
    monkeypatch.undo()
    s = _catena_su(tmp_path, "old.db", max_daily_loss=30.0)
    assert s.rc._emergency_stopped is True
    assert str(_db(s, "emergency_stopped")) == "1"
    assert not os.path.exists(path) or "emergency_stopped" not in json.load(open(path))


# ============================================================================ CX-M2, C12, C04: reset solo dell'ES
def test_c04_c12_reset_senza_es_esito_esatto_e_nessun_effetto(tmp_path):
    c = _banco(tmp_path, "c04.db")
    owner = RC._owner(c.rc)
    owner.apply(c.rc, "session_barrier", _session_loss_pending_global=True)
    owner.apply(c.rc, "daily_pending_stop", _daily_loss_pending_stop=True)
    c.rc.db.save_settings({"emergency_stopped": "1", "emergency_reason": "DAILY_LOSS_BREACH_PENDING:40"})
    assert c.rc.reset_emergency() == {"emergency_reset": True, "persist_error": ""}
    assert c.rc._session_loss_pending_global is True, "CX-M2: reset ha tolto la barriera C3"
    assert c.rc._daily_loss_pending_stop is True, "C12: reset ha tolto il pending-stop giornaliero"
    assert str(_db(c, "emergency_stopped")) == "1", "C12: reset senza ES ha cancellato il marker pre-sync"


def test_cx_m2_reset_con_es_tocca_solo_l_es(tmp_path):
    c = _banco(tmp_path, "m2.db")
    c.rc.emergency_stop("m153: ES manuale SIM")
    owner = RC._owner(c.rc)
    owner.apply(c.rc, "session_barrier", _session_loss_pending_global=True)
    owner.apply(c.rc, "daily_pending_stop", _daily_loss_pending_stop=True)
    assert c.rc.reset_emergency()["emergency_reset"] is True and not c.rc._emergency_stopped
    assert c.rc._session_loss_pending_global is True and c.rc._daily_loss_pending_stop is True


def test_pending_stop_giornaliero_tolto_dal_settlement_quando_subentra_l_es(tmp_path):
    """S7: superamento giornaliero globale nel settlement => ES; il pending-stop si toglie nel settlement (non resta un
    blocco perpetuo dopo il reset dell'ES)."""
    from tests.d1.test_pr04a_financial_scope import _settle
    c = _catena_su(tmp_path, "pend.db", max_daily_loss=30.0)
    c.rc.set_simulation_mode(False)
    c.rc.betfair_service.client = None
    _settle(c, -40.0)
    assert c.rc._emergency_stopped is True
    assert c.rc._daily_loss_pending_stop is False, "pending-stop rimasto armato con l'ES attivo"


# ============================================================================ P1
def test_p1_reset_dell_es_attende_il_proprietario(tmp_path):
    c = _banco(tmp_path, "p1.db")
    c.rc.emergency_stop("m153 P1")
    owner = RC._owner(c.rc)
    esito = {}
    owner.lock.acquire()
    try:
        t = threading.Thread(target=lambda: esito.update(r=c.rc.reset_emergency()))
        t.start()
        time.sleep(0.2)
        assert t.is_alive() and not esito, "reset eseguito mentre un'altra transizione tiene il proprietario"
        RC._bind_live_account(c.rc, "betfair|LIVE|betfair:aaaaaaaaaaaaaaaaaaaaaaaa|EUR", {"key": "p1", "kind": "x"})
    finally:
        owner.lock.release()
    t.join(5)
    assert esito["r"]["emergency_reset"] is False, "obbligo entrato prima del reset ignorato"


def test_p1_reset_dello_stop_ricontrolla_l_obbligo_nella_sezione_della_scrittura(tmp_path, monkeypatch):
    c = _banco(tmp_path, "p1r.db")
    RC._owner(c.rc).apply(c.rc, "set_risk_stop", _risk_stop_reason="exposure_stop_reached:m153")
    reale = RC._live_reset_blocker
    chiamate = {"n": 0}

    def blocker(self):
        chiamate["n"] += 1
        if chiamate["n"] == 1:
            # un obbligo LIVE entra DOPO il primo controllo, prima della scrittura
            RC._bind_live_account(self, "betfair|LIVE|betfair:bbbbbbbbbbbbbbbbbbbbbbbb|EUR", {"key": "p1r", "kind": "x"})
            return ""
        return reale(self)
    monkeypatch.setattr(RC, "_live_reset_blocker", blocker)
    esito = c.rc.reset_risk_stop()
    assert esito == {"risk_stop_reset": False, "reason": "esposizione_live_irrisolta"}, esito
    assert c.rc._risk_stop_reason


# ============================================================================ P7 (C13, confine interno)
def test_p7_rimozione_del_marker_mai_sopra_un_es_dichiarato_dopo_l_apertura(tmp_path):
    c = _banco(tmp_path, "p7.db")
    seq = RC._owner(c.rc).es_seq
    c.rc._persist_db_emergency_marker(active=True, reason="DAILY_LOSS_BREACH_PENDING:40")
    c.rc.emergency_stop("m153 P7: ES manuale")
    assert c.rc._persist_db_emergency_marker(active=False, es_seq=seq) == ""
    assert str(_db(c, "emergency_stopped")) == "1", "P7: '0' scritto sopra un ES manuale"
    c.rc.reset_emergency()
    c.rc._persist_db_emergency_marker(active=False, es_seq=seq)
    assert str(_db(c, "emergency_stopped")) == "0"          # il reset lo ha gia' tolto; nessuna scrittura nuova
    seq2 = RC._owner(c.rc).es_seq
    c.rc._persist_db_emergency_marker(active=True, reason="DAILY_LOSS_BREACH_PENDING:40")
    c.rc._persist_db_emergency_marker(active=False, es_seq=None)
    assert str(_db(c, "emergency_stopped")) == "1", "rimozione senza es_seq scritta"
    c.rc._persist_db_emergency_marker(active=False, es_seq=seq2)
    assert str(_db(c, "emergency_stopped")) == "0", "nessun ES dopo l'apertura: la rimozione deve scriversi"


def test_p7_rimozione_concorrente_con_es_manuale_mai_zero_con_es_attivo(tmp_path):
    c = _banco(tmp_path, "p7c.db")
    for giro in range(20):
        seq = RC._owner(c.rc).es_seq
        c.rc._persist_db_emergency_marker(active=True, reason=f"DAILY_LOSS_BREACH_PENDING:{giro}")
        barriera = threading.Barrier(2)
        t1 = threading.Thread(target=lambda: (barriera.wait(), c.rc._persist_db_emergency_marker(
            active=False, es_seq=seq)))
        t2 = threading.Thread(target=lambda: (barriera.wait(), c.rc.emergency_stop(f"m153 P7 {giro}")))
        t1.start(), t2.start()
        t1.join(5), t2.join(5)
        assert c.rc._emergency_stopped is True
        assert str(_db(c, "emergency_stopped")) == "1", f"giro {giro}: '0' sul db con ES attivo"
        assert c.rc.reset_emergency()["emergency_reset"] is True


# ============================================================================ P8
def test_p8_proprietario_non_inizializzato_blocca_ingressi_reset_e_sweep(tmp_path):
    c = _banco(tmp_path, "p8.db")
    c.rc.emergency_stop("m153 P8")
    SafetyStateOwner.install(c.rc).apply(c.rc, "init", _emergency_stopped=True)
    assert c.rc._loss_block_reason() == "stato_di_sicurezza_non_inizializzato"
    assert c.rc.drawdown_entry_block_reason() == "stato_di_sicurezza_non_inizializzato"
    assert c.rc.reset_emergency()["emergency_reset"] is False and c.rc._emergency_stopped
    RC._owner(c.rc).apply(c.rc, "set_risk_stop", _risk_stop_reason="exposure_stop_reached:p8")
    assert c.rc.reset_risk_stop()["reason"] == "stato_di_sicurezza_non_inizializzato"
    RC._owner(c.rc).apply(c.rc, "reconcile_pending", _emergency_reconcile_pending=True)
    assert RC._reconcile_restored_emergency(c.rc, trigger="p8") is None
    RC._owner(c.rc).mark_initialized()
    assert c.rc._loss_block_reason() != "stato_di_sicurezza_non_inizializzato"
    RC._owner(c.rc).apply(c.rc, "reconcile_pending", _emergency_reconcile_pending=False)


def test_p8_sweep_chiesto_durante_la_ricostruzione_all_avvio_rifiutato(tmp_path, monkeypatch):
    visti = []
    reale = RC._restore_breached_live_scope_stop

    def durante(self):
        visti.append((RC._owner(self).initialized,
                      RC._reconcile_restored_emergency(self, trigger="durante_ricostruzione")))
        return reale(self)
    c = _banco(tmp_path, "p8b.db")
    c.rc.emergency_stop("m153 P8 avvio")
    monkeypatch.setattr(RC, "_restore_breached_live_scope_stop", durante)
    r = _catena_su(tmp_path, "p8b.db", max_daily_loss=30.0)
    assert visti and visti[0] == (False, None), visti
    assert RC._owner(r.rc).initialized is True
    RC._owner(r.rc).apply(r.rc, "reconcile_pending", _emergency_reconcile_pending=False)


# ============================================================================ tracciatore dei lock
def test_tracciatore_registra_un_lock_preso_tenendo_il_proprietario(tmp_path):
    c = _banco(tmp_path, "lk.db")
    prima = len(safety_state.VIOLATIONS)
    with c.rc._risk_stop_lock:
        with RC._owner(c.rc).lock:
            pass
    assert len(safety_state.VIOLATIONS) == prima, "ordine ammesso (stop di rischio -> proprietario) segnalato"
    with RC._owner(c.rc).lock:
        with c.rc._risk_stop_lock:
            pass
    nuove = safety_state.VIOLATIONS[prima:]
    del safety_state.VIOLATIONS[prima:]
    assert nuove and "risk_stop" in nuove[0], "inversione (proprietario -> stop di rischio) non registrata"


def test_un_solo_dominio_per_i_lock_storici(tmp_path):
    c = _banco(tmp_path, "dom.db")
    owner = RC._owner(c.rc)
    assert c.rc.__dict__["_safety_state_lock"] is owner.lock
    assert c.rc.__dict__["_live_accounts_lock"] is owner.lock


def test_cache_del_registro_mai_riscritta_con_una_lettura_vecchia(tmp_path, monkeypatch):
    """Corsa trovata sull'oracolo cx_l2_protezioni sotto carico: un lettore rilegge il registro dal disco (PENDING) e, dopo
    che un altro thread ha scritto VERIFIED, rimetteva in cache il valore vecchio. Lettura e cache sono ora nella stessa
    sezione critica: lo scrittore attende il lettore e la cache coincide sempre con il disco."""
    c = _banco(tmp_path, "cache.db")
    scope = "betfair|LIVE|betfair:aaaaaaaaaaaaaaaaaaaaaaaa|EUR"
    RC._save_live_bound_accounts(c.rc, {scope: "PENDING"})
    RC._owner(c.rc).apply(c.rc, "registry", _emergency_live_accounts=None)
    pausa, reale = _Pausa(), c.rc.db.get_settings
    lettore = {"tid": None}

    def get(*a, **k):
        out = reale(*a, **k)
        if threading.get_ident() == lettore["tid"] and not pausa.dentro.is_set():
            pausa.attraversa()                    # il lettore ha in mano il registro vecchio
        return out
    monkeypatch.setattr(c.rc.db, "get_settings", get)

    def leggi():
        lettore["tid"] = threading.get_ident()
        RC._live_bound_accounts(c.rc)
    t = threading.Thread(target=leggi)
    t.start()
    assert pausa.dentro.wait(5)
    w = threading.Thread(target=RC._save_live_bound_accounts, args=(c.rc, {scope: "VERIFIED"}))
    w.start()
    time.sleep(0.2)
    pausa.via.set()
    t.join(5)
    w.join(5)
    assert RC._live_bound_accounts(c.rc) == {scope: "VERIFIED"}, "cache riscritta con una lettura vecchia"


# ============================================================================ C13: prova AST (B), confine interno
_C13_SCRITTORI_AMMESSI = {"_reset_emergency_locked", "_write_db_emergency_marker", "_emergency_state_payload"}


def test_c13_nessun_percorso_scrive_emergency_stopped_zero_fuori_da_reset_e_ritiro_controllato():
    """C13 «NON RAGGIUNGIBILE AI CONFINI su cef002ba; verifica alternativa»: prova strutturale. Nel runtime una mappa con
    `"emergency_stopped"` che puo' valere "0" esiste SOLO in: `_reset_emergency_locked` (reset_es, sotto il proprietario,
    solo con ES attivo), `_write_db_emergency_marker` (ritiro del marker pre-sync, chiamato solo sotto il proprietario
    con il controllo di `es_seq`) e `_emergency_state_payload` (usata solo da `_persist_emergency_state`, che non scrive
    mai senza un ES attivo). Nessun altro percorso scrive quella chiave."""
    import inspect
    source = (ROOT / "core/runtime_controller.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    trovati = set()
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        for node in ast.walk(fn):
            if not isinstance(node, ast.Dict):
                continue
            for k, v in zip(node.keys, node.values):
                if isinstance(k, ast.Constant) and k.value == "emergency_stopped" and any(
                        isinstance(x, ast.Constant) and x.value == "0" for x in ast.walk(v)):
                    trovati.add(fn.name)
    assert trovati == _C13_SCRITTORI_AMMESSI, trovati
    ritiro = inspect.getsource(RC._persist_db_emergency_marker)
    assert "owner.es_seq != es_seq" in ritiro and "with owner.lock" in ritiro
    persist = inspect.getsource(RC._persist_emergency_state)
    assert "if not self._emergency_stopped" in persist


def test_c13_persist_dello_stato_mai_zero_senza_es(tmp_path):
    c = _banco(tmp_path, "c13p.db")
    c.rc.db.save_settings({"emergency_stopped": "1", "emergency_reason": "DAILY_LOSS_BREACH_PENDING:40"})
    assert c.rc._emergency_stopped is False
    assert c.rc._persist_emergency_state() == ""
    assert str(_db(c, "emergency_stopped")) == "1", "persist dello stato con ES spento ha scritto '0'"
