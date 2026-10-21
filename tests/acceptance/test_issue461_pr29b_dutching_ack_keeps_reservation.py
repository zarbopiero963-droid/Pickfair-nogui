"""#461 PR29-b — un lotto Dutching a mercato non libera la riserva (PF-C29-01.17).

Contratto PF-C29-01.17 (#506 c.6123640661): un lotto le cui gambe sono PLACED (accettate, non abbinate) mantiene
l'intera riserva; il rilascio avviene solo su esito terminale certo e privo di rischio di OGNI gamba. Oracolo: lotto
di 2 gambe PLACED con sizeMatched=0 -> esposizione del tavolo invariata, tavolo ACTIVE; gambe CANCELLED con
sizeCancelled=size -> rilascio una volta sola. Prima di PR29-b `recompute_batch_status` classificava PLACED/MATCHED
come EXECUTED (terminale) e la riconciliazione liberava tavolo e chiave con le gambe ancora a mercato.
Componenti reali: ReconciliationEngine, DutchingBatchManager, Database (SQLite), TableManager, DuplicationGuard.
"""
from __future__ import annotations

from core.duplication_guard import DuplicationGuard
from core.dutching_batch_manager import DutchingBatchManager
from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine
from core.table_manager import TableManager
from database import Database

EVENT_KEY = "1.777:DUTCH"


class _DB(Database):
    """Database reale + sink del registro decisioni (la riconciliazione e' fail-closed sull'audit)."""

    def persist_decision_log(self, batch_id, entries):
        self.__dict__.setdefault("decisioni", []).extend(entries)


class _Client:
    def __init__(self):
        self.orders = []

    def get_current_orders(self, market_ids=None):
        return [dict(o) for o in self.orders if not market_ids or o["marketId"] in market_ids]


def _ordine(ref, sel, *, stato="EXECUTABLE", abbinato=0.0, residuo=5.0, annullato=0.0):
    return {"customerOrderRef": ref, "betId": f"BET-{ref}", "marketId": "1.777", "selectionId": sel,
            "status": stato, "sizeMatched": abbinato, "sizeRemaining": residuo, "sizeCancelled": annullato}


def _lotto(tmp_path):
    db = _DB(str(tmp_path / "pr29b.db"))
    bm, tm, guard, client = DutchingBatchManager(db), TableManager(table_count=3), DuplicationGuard(), _Client()
    # Come `DutchingController._allocate_table`: il meta del tavolo porta l'identita' del lotto.
    tm.activate(table_id=1, event_key=EVENT_KEY, exposure=10.0, market_id="1.777", selection_id=11,
                meta={"type": "dutching_batch", "batch_id": "B-29B"})
    assert guard.acquire(EVENT_KEY)
    bm.create_batch(batch_id="B-29B", event_key=EVENT_KEY, market_id="1.777", table_id=1, total_legs=2,
                    batch_exposure=10.0,
                    legs=[{"leg_index": 0, "customer_ref": "REF-A", "market_id": "1.777", "selection_id": 11,
                           "side": "BACK", "price": 2.0, "stake": 5.0, "liability": 5.0, "status": "SUBMITTED"},
                          {"leg_index": 1, "customer_ref": "REF-B", "market_id": "1.777", "selection_id": 12,
                           "side": "BACK", "price": 2.0, "stake": 5.0, "liability": 5.0, "status": "SUBMITTED"}])
    for leg in bm.get_batch_legs("B-29B"):                          # inviate al trasporto, come in produzione
        bm.update_leg_submission(batch_id="B-29B", leg_index=leg["leg_index"], customer_ref=leg["customer_ref"])
    engine = ReconciliationEngine(db=db, batch_manager=bm, client_getter=lambda: client, table_manager=tm,
                                  duplication_guard=guard,
                                  config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0,
                                                        transient_retry_base_delay=0.0))
    return engine, bm, tm, guard, client


def _tavolo(tm):
    return tm.get_table(1)


def test_gambe_a_mercato_non_abbinate_mantengono_l_intera_riserva(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11), _ordine("REF-B", 12)]
    engine.reconcile_batch("B-29B")
    assert {leg["status"] for leg in bm.get_batch_legs("B-29B")} == {"PLACED"}
    t = _tavolo(tm)
    assert t.current_event_key == EVENT_KEY and t.current_exposure == 10.0, "riserva liberata con gambe a mercato"
    assert EVENT_KEY in guard._active


def test_gambe_abbinate_mantengono_la_riserva_fino_al_settlement(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11, stato="EXECUTION_COMPLETE", abbinato=5.0, residuo=0.0),
                     _ordine("REF-B", 12, stato="EXECUTION_COMPLETE", abbinato=5.0, residuo=0.0)]
    engine.reconcile_batch("B-29B")
    t = _tavolo(tm)
    assert t.current_event_key == EVENT_KEY and t.current_exposure == 10.0, "posizione abbinata uscita dai cap"
    assert EVENT_KEY in guard._active


def test_gambe_annullate_per_intero_rilasciano_una_volta(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11, stato="CANCELLED", residuo=0.0, annullato=5.0),
                     _ordine("REF-B", 12, stato="CANCELLED", residuo=0.0, annullato=5.0)]
    engine.reconcile_batch("B-29B")
    assert _tavolo(tm).current_event_key == ""
    assert EVENT_KEY not in guard._active
    tm.activate(table_id=1, event_key="1.888:ALTRO", exposure=4.0, market_id="1.888", selection_id=1, meta={})
    engine.reconcile_batch("B-29B")                                 # lotto gia' terminale: nessun secondo rilascio
    assert _tavolo(tm).current_event_key == "1.888:ALTRO"


def _annullate(client):
    client.orders = [_ordine("REF-A", 11, stato="CANCELLED", residuo=0.0, annullato=5.0),
                     _ordine("REF-B", 12, stato="CANCELLED", residuo=0.0, annullato=5.0)]


def test_rilascio_del_lotto_non_colpisce_un_tavolo_riassegnato_a_un_altra_riserva(tmp_path):
    # Il lotto diventa terminale senza rischio, ma nel frattempo il suo tavolo e' stato liberato e riassegnato a
    # un'altra riserva (altro evento): il rilascio del lotto non tocca ne' quel tavolo ne' la sua chiave.
    engine, bm, tm, guard, client = _lotto(tmp_path)
    tm.force_unlock(1)
    guard.release(EVENT_KEY)
    tm.activate(table_id=1, event_key="1.888:ALTRO", exposure=4.0, market_id="1.888", selection_id=1, meta={})
    assert guard.acquire("1.888:ALTRO")
    _annullate(client)
    engine.reconcile_batch("B-29B")
    t = _tavolo(tm)
    assert t.current_event_key == "1.888:ALTRO" and t.current_exposure == 4.0, "liberata la riserva di un altro"
    assert "1.888:ALTRO" in guard._active


def test_settlement_del_lotto_non_colpisce_un_tavolo_riassegnato(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    tm.force_unlock(1)
    tm.activate(table_id=1, event_key="1.888:ALTRO", exposure=4.0, market_id="1.888", selection_id=1, meta={})
    engine.finalize_terminal_batch("B-29B", status="EXECUTED", pnl=3.0)
    assert _tavolo(tm).current_event_key == "1.888:ALTRO"


def test_gamba_parzialmente_abbinata_mantiene_la_riserva(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11, abbinato=2.0, residuo=3.0),
                     _ordine("REF-B", 12, stato="CANCELLED", residuo=0.0, annullato=5.0)]
    engine.reconcile_batch("B-29B")
    t = _tavolo(tm)
    assert t.current_event_key == EVENT_KEY and t.current_exposure == 10.0
    assert EVENT_KEY in guard._active


def test_esito_incerto_di_una_gamba_mantiene_la_riserva(tmp_path):
    # Una gamba non visibile sull'exchange (esito ignoto) e una annullata: nessun rilascio (fail-closed).
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-B", 12, stato="CANCELLED", residuo=0.0, annullato=5.0)]
    engine.reconcile_batch("B-29B")
    assert _tavolo(tm).current_event_key == EVENT_KEY
    assert EVENT_KEY in guard._active


def test_riavvio_non_rilascia_due_volte_un_lotto_gia_chiuso(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    _annullate(client)
    engine.reconcile_batch("B-29B")
    assert _tavolo(tm).current_event_key == ""
    tm.activate(table_id=1, event_key=EVENT_KEY, exposure=7.0, market_id="1.777", selection_id=11, meta={})
    assert guard.acquire(EVENT_KEY)                                # nuova riserva sullo stesso evento
    riavviato = ReconciliationEngine(db=engine.db, batch_manager=DutchingBatchManager(engine.db),
                                     client_getter=lambda: client, table_manager=tm, duplication_guard=guard,
                                     config=engine.cfg)
    riavviato.reconcile_all_open_batches()
    riavviato.reconcile_batch("B-29B")
    assert _tavolo(tm).current_exposure == 7.0 and EVENT_KEY in guard._active


def test_riconciliazioni_concorrenti_rilasciano_una_volta(tmp_path):
    import threading
    engine, bm, tm, guard, client = _lotto(tmp_path)
    _annullate(client)
    rilasci = []
    originale = bm.release_runtime_artifacts

    def conta(**kw):
        rilasci.append(kw["batch_id"])
        return originale(**kw)

    bm.release_runtime_artifacts = conta
    fili = [threading.Thread(target=engine.reconcile_batch, args=("B-29B",)) for _ in range(4)]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    assert rilasci == ["B-29B"]


def test_rilascio_del_lotto_non_libera_la_chiave_di_una_nuova_riserva_sullo_stesso_evento(tmp_path):
    # Il tavolo del lotto e' gia' libero (reset) e la chiave dell'evento appartiene ora a una nuova riserva su un
    # altro tavolo: la chiusura tardiva del lotto non libera ne' quel tavolo ne' la sua chiave anti-duplicato.
    engine, bm, tm, guard, client = _lotto(tmp_path)
    tm.force_unlock(1)
    tm.activate(table_id=2, event_key=EVENT_KEY, exposure=6.0, market_id="1.777", selection_id=11, meta={})
    _annullate(client)
    engine.reconcile_batch("B-29B")
    assert tm.get_table(2).current_event_key == EVENT_KEY and tm.get_table(2).current_exposure == 6.0
    assert EVENT_KEY in guard._active, "liberata la chiave anti-duplicato della nuova riserva"


def test_chiave_liberata_se_il_tavolo_del_lotto_era_gia_libero_e_nessuno_porta_l_evento(tmp_path):
    # Review anticipata (P2): tavolo del lotto gia' liberato da un reset e non riassegnato; il controller ha preso la
    # chiave dell'evento appuntata (mai scaduta per TTL). Il lotto chiude senza rischio: nessuna riserva porta piu'
    # l'evento, la chiave va liberata (altrimenti l'evento resta bloccato fino al riavvio).
    engine, bm, tm, guard, client = _lotto(tmp_path)
    guard.acquire(EVENT_KEY, pin=True)
    tm.force_unlock(1)
    _annullate(client)
    engine.reconcile_batch("B-29B")
    assert EVENT_KEY not in guard._active, "chiave dell'evento trattenuta per sempre"


def test_settlement_su_tavolo_gia_libero_registra_la_perdita(tmp_path):
    # Review anticipata (P2): il P&L del lotto su un tavolo gia' libero (non riassegnato) resta contabilizzato nel
    # recovery del tavolo, come prima; solo un tavolo riassegnato a un'altra riserva non si tocca.
    engine, bm, tm, guard, client = _lotto(tmp_path)
    tm.force_unlock(1)
    engine.finalize_terminal_batch("B-29B", status="EXECUTED", pnl=-5.0)
    t = tm.get_table(1)
    assert t.loss_amount == 5.0 and t.in_recovery, "perdita del lotto non contabilizzata"



def test_riuso_del_tavolo_per_lo_stesso_evento_non_e_rilasciato_dal_vecchio_lotto(tmp_path):
    # Review Sol su #534: dopo un reset il tavolo e' riusato per lo STESSO evento da una nuova riserva (altro
    # intento). La chiusura tardiva del vecchio lotto non la libera: l'identita' e' il batch_id, non l'event_key.
    engine, bm, tm, guard, client = _lotto(tmp_path)
    tm.force_unlock(1)
    tm.activate(table_id=1, event_key=EVENT_KEY, exposure=6.0, market_id="1.777", selection_id=11,
                meta={"customer_ref": "sig-nuovo-intento"})
    _annullate(client)
    engine.reconcile_batch("B-29B")
    t = _tavolo(tm)
    assert t.current_event_key == EVENT_KEY and t.current_exposure == 6.0, "liberata la nuova riserva sullo stesso evento"
    assert EVENT_KEY in guard._active


def test_perdita_del_lotto_su_tavolo_riassegnato_entra_nel_recovery_senza_liberarlo(tmp_path):
    # Review Sol su #534: tavolo riassegnato prima del settlement negativo del vecchio lotto. La perdita entra nel
    # recovery del tavolo, la riserva corrente resta intatta.
    engine, bm, tm, guard, client = _lotto(tmp_path)
    tm.force_unlock(1)
    tm.activate(table_id=1, event_key="1.888:ALTRO", exposure=4.0, market_id="1.888", selection_id=1, meta={})
    engine.finalize_terminal_batch("B-29B", status="EXECUTED", pnl=-5.0)
    t = _tavolo(tm)
    assert t.current_event_key == "1.888:ALTRO" and t.current_exposure == 4.0
    assert t.loss_amount == 5.0 and t.in_recovery, "perdita del vecchio lotto non contabilizzata"
