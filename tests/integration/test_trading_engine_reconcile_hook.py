import pytest

from core.trading_engine import TradingEngine


class DummyReconcile:
    def __init__(self):
        self.called = False

    def enqueue(self, **kwargs):
        self.called = True


class TimeoutExecutor:
    def submit(self, *_):
        raise TimeoutError("timeout")


class DummyBus:
    def subscribe(self, *_):
        pass

    def publish(self, *_):
        pass


class DummyDB:
    def insert_order(self, payload):
        return "OID1"

    def update_order(self, *_):
        pass


def test_reconcile_called_on_ambiguous():
    reconcile = DummyReconcile()

    engine = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=TimeoutExecutor(),
        reconciliation_engine=reconcile,
    )

    engine._runtime_state = "READY"

    engine.submit_quick_bet(
        {
            "customer_ref": "C1",
            "price": 2.0,
        }
    )

    assert reconcile.called is True

# =========================================================================
# Risoluzione LIVE del motore di riconciliazione (piano #437 punto 4,
# PR «iniezione ReconciliationEngine»). Il runtime RICOSTRUISCE il motore a
# ogni start(): un'iniezione congelata al build andrebbe stantia, quindi
# l'engine risolve il motore CORRENTE dal runtime a ogni enqueue — stesso
# pattern della risoluzione lazy del betfair_client (#443).
# =========================================================================

class _Runtime:
    def __init__(self, motore):
        self.reconciliation_engine = motore


def _engine_ambiguo(reconciliation_engine=None):
    return TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=TimeoutExecutor(),
        reconciliation_engine=reconciliation_engine,
    )


def test_enqueue_arriva_al_motore_del_runtime_senza_iniezione_esplicita():
    """REPRO del gap: engine costruito col default (_NullReconciliationEngine),
    motore vero disponibile SOLO sul runtime — l'ambiguita' deve arrivargli."""
    motore = DummyReconcile()
    engine = _engine_ambiguo()
    engine.runtime_controller = _Runtime(motore)
    engine._runtime_state = "READY"

    engine.submit_quick_bet({"customer_ref": "C1", "price": 2.0})

    assert motore.called is True, (
        "submission ambigua persa: l'enqueue non raggiunge il motore del runtime"
    )


def test_iniezione_esplicita_vince_sul_motore_del_runtime():
    """Lock di contratto: i test/iniezioni esplicite continuano a ricevere
    l'enqueue anche quando il runtime espone un altro motore."""
    esplicito = DummyReconcile()
    dal_runtime = DummyReconcile()
    engine = _engine_ambiguo(reconciliation_engine=esplicito)
    engine.runtime_controller = _Runtime(dal_runtime)
    engine._runtime_state = "READY"

    engine.submit_quick_bet({"customer_ref": "C1", "price": 2.0})

    assert esplicito.called is True
    assert dal_runtime.called is False


def test_risoluzione_live_segue_il_motore_ricostruito():
    """start() ricostruisce il motore: l'engine deve seguire il NUOVO oggetto,
    non un riferimento catturato una volta."""
    primo = DummyReconcile()
    secondo = DummyReconcile()
    engine = _engine_ambiguo()
    runtime = _Runtime(primo)
    engine.runtime_controller = runtime
    engine._runtime_state = "READY"

    engine.submit_quick_bet({"customer_ref": "C1", "price": 2.0})
    assert primo.called is True

    runtime.reconciliation_engine = secondo  # rebuild simulato (nuovo start)
    engine.submit_quick_bet({"customer_ref": "C2", "price": 2.0})
    assert secondo.called is True, (
        "risoluzione congelata: dopo il rebuild l'enqueue va al motore vecchio"
    )


def test_runtime_senza_motore_fallback_null_senza_crash():
    """Runtime presente ma senza motore (o assente del tutto): la submission
    ambigua completa comunque il lifecycle, nessun crash nuovo."""
    engine = _engine_ambiguo()
    engine.runtime_controller = object()  # nessun attributo reconciliation_engine
    engine._runtime_state = "READY"

    result = engine.submit_quick_bet({"customer_ref": "C1", "price": 2.0})
    assert isinstance(result, dict)

    engine2 = _engine_ambiguo()
    engine2._runtime_state = "READY"
    result2 = engine2.submit_quick_bet({"customer_ref": "C2", "price": 2.0})
    assert isinstance(result2, dict)


def test_enqueue_che_solleva_non_rompe_il_lifecycle_ambiguo():
    """Motore rotto (enqueue solleva): l'ambiguita' deve comunque completare
    il proprio lifecycle — mai un ordine lasciato a meta' per colpa
    dell'intake di riconciliazione."""

    class _MotoreRotto:
        def __init__(self):
            self.called = False

        def enqueue(self, **_kw):
            self.called = True
            raise RuntimeError("INTAKE_BROKEN")

    rotto = _MotoreRotto()
    engine = _engine_ambiguo()
    engine.runtime_controller = _Runtime(rotto)
    engine._runtime_state = "READY"

    result = engine.submit_quick_bet({"customer_ref": "C1", "price": 2.0})

    assert rotto.called is True
    assert isinstance(result, dict), (
        "l'eccezione dell'intake ha interrotto il lifecycle dell'ordine ambiguo"
    )
