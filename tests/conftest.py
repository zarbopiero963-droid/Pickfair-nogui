import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _TestRuntimeBox:
    """BB-1G: runtime di test con una scatola nera sana, per i test che esercitano ordini, avvii e riprese senza
    riguardare la scatola nera. Accetta solo i tipi del vocabolario reale (un tipo inventato resta un errore) e non
    scrive nulla. Il comportamento vero del gate (assente, non runtime, guasta, uscite protettive) e' verificato con la
    `BlackBox` reale nei test marcati `real_blackbox` e in `tests/blackbox`, `tests/packaging`."""

    def __init__(self):
        self.events = []

    def is_runtime(self):
        return True

    def record(self, event_type, **fields):
        from core import event_journal
        if event_type not in event_journal.EVENT_TYPES:
            raise ValueError(f"tipo evento sconosciuto: {event_type!r}")
        self.events.append((event_type, fields))
        return uuid.uuid4().hex

    def end_session(self, reason="STOP", **data):
        return self.record("SHUTDOWN", reason_code=reason)

    def close(self):
        pass


# Moduli che gestiscono da se' la scatola nera del processo: la fixture non deve fingere un runtime installato.
_OWN_BLACKBOX_DIRS = ("blackbox", "packaging")


@pytest.fixture(autouse=True)
def _bb1g_test_runtime(request, monkeypatch):
    path = Path(str(request.node.fspath))
    if request.node.get_closest_marker("real_blackbox") or any(part in _OWN_BLACKBOX_DIRS for part in path.parts[-3:-1]):
        yield None
        return
    from core import blackbox_runtime
    if blackbox_runtime.get() is not None:      # un test che ha installato una scatola nera vera la usa
        yield None
        return
    box = _TestRuntimeBox()
    monkeypatch.setattr(blackbox_runtime, "_box", box)
    yield box


# M154-bis (owner, single-account): il runtime legge l'identita' LIVE SOLO da `client.authenticated_identity()` (login
# Betfair riuscito), la confronta con l'identita' autorizzata (associata alla prima connessione LIVE solo se coincide con
# lo username CONFIGURATO) e senza di essa nega ingressi, letture, cancellazioni, certificazioni e reset LIVE. I client
# finti dei test esistenti hanno il solo `username` (o nessuna identita'), e i banchi non hanno configurazione Betfair:
# senza adattamento ogni loro percorso LIVE sarebbe fail-closed per il requisito nuovo, non per cio' che verificano.
# Adattatore di test DICHIARATO (stesso schema di BB-1G sopra), nessuna asserzione cambiata:
#   (1) client LEGACY = senza `authenticated_identity` che restituisca str/None (oggetti con il solo username, spie
#       senza identita', Mock): identita' = hash del suo `username`; senza username, quella del client connesso (la
#       regola precedente); per lui il controllo d'identita' e' quello di prima (nessun gate nuovo);
#   (2) banco SENZA username configurato: vale come configurato lo username del client connesso (prima associazione).
# Il comportamento vero (attestazione obbligatoria, configurazione obbligatoria, nessun ripiego, F4, CX-N1) e'
# verificato dai moduli con `IDENTITA_REALE = True` (tests/unit/test_m154_identita_attestata.py,
# tests/d1/test_m154_single_account.py), dove l'adattatore non si applica, e dalla matrice M153 di Claude D. I client
# che dichiarano `authenticated_identity` (str/None) non sono mai adattati: seguono le regole reali.
# MonkeyPatch privato: un `monkeypatch.undo()` dentro un test non toglie l'adattatore a meta' test.
def _m154_attestazione_dichiarata(client):
    attesta = getattr(client, "authenticated_identity", None)
    if client is None or not callable(attesta):
        return False
    try:
        valore = attesta()
    except Exception:
        return True
    return valore is None or isinstance(valore, str)


@pytest.fixture(autouse=True)
def _m154_client_finti_legacy(request):
    if getattr(request.module, "IDENTITA_REALE", False):
        yield
        return
    try:
        from core import financial_scope
        from core.runtime_controller import RuntimeController as RC
    except Exception:              # moduli che non caricano il runtime
        yield
        return
    reale_identita = RC._live_account_identity
    reale_configurato = RC._configured_betfair_username
    reale_controllo = RC._live_identity_check

    def connesso(self):
        return getattr(getattr(self, "betfair_service", None), "client", None)

    def identita(self, client=RC._NO_CLIENT):
        c = connesso(self) if client is RC._NO_CLIENT else client
        if _m154_attestazione_dichiarata(c):
            return reale_identita(self, c)
        username = getattr(c, "username", None)
        if isinstance(username, str) and username.strip():
            return RC._identity_hash(username)
        principale = connesso(self)
        if principale is not None and principale is not c:
            return identita(self, principale)
        return ""

    def configurato(self):
        valore = reale_configurato(self)
        if valore:
            return valore
        username = getattr(connesso(self), "username", None)
        return username if isinstance(username, str) else ""

    def controllo(self, client=RC._NO_CLIENT, associate=True):
        c = connesso(self) if client is RC._NO_CLIENT else client
        if _m154_attestazione_dichiarata(c):
            return reale_controllo(self, client, associate)
        legacy = identita(self, c)
        return (financial_scope.runtime_scope(simulated=False, live_account=legacy) if legacy else ""), ""

    mp = pytest.MonkeyPatch()
    mp.setattr(RC, "_live_account_identity", identita)
    mp.setattr(RC, "_configured_betfair_username", configurato)
    mp.setattr(RC, "_live_identity_check", controllo)
    try:
        yield
    finally:
        mp.undo()


# M153: tracciatore dell'ordine dei lock dello stato di sicurezza attivo in tutta la suite. Un lock tracciato acquisito
# mentre si tiene il lock FOGLIA del SafetyStateOwner (rischio di deadlock) fa fallire il test che lo produce.
@pytest.fixture(autouse=True)
def _m153_ordine_dei_lock():
    try:
        from core import safety_state
    except Exception:
        yield
        return
    safety_state.LOCK_ORDER_CHECK = True
    prima = len(safety_state.VIOLATIONS)
    yield
    nuove = safety_state.VIOLATIONS[prima:]
    del safety_state.VIOLATIONS[prima:]
    assert not nuove, f"M153: ordine dei lock violato: {nuove[:3]}"
