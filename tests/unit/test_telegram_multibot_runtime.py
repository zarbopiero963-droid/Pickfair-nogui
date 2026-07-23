"""PR-5b (epica #374) — orchestratore N-bot `TelegramMultiBotRuntime`.

Compone N `TelegramBotApiRuntime` indipendenti presentando la superficie
duck-typed attesa da `TelegramService.self.listener`, con semantica AGGREGATA
fail-closed. Qui i child sono FAKE (nessun transport/rete): si verifica solo la
logica di aggregazione (state/running/_runtime_thread/start/stop/status).
"""
from __future__ import annotations

import pytest

from recovery.telegram_autoheal import TelegramAutohealPolicy
from telegram_bot_runtime import (
    TelegramMultiBotRuntime,
    _child_thread_alive,
    _latest_iso_ts,
)

pytestmark = pytest.mark.unit


class _FakeThread:
    def __init__(self, alive: bool):
        self._alive = alive

    def is_alive(self) -> bool:
        return self._alive


class _FakeChild:
    """Child runtime fake: espone la superficie che l'orchestratore consuma."""

    def __init__(self, *, state="CONNECTED", running=True, thread_alive=True,
                 handlers=1, chats=1, start_res=None, stop_res=None, last_error="",
                 intentional_stop=False, restart_heals_to=None, restart_stop_ok=True,
                 raise_on=None, restart_malformed=None):
        self._state = state
        self._running = running
        self._thread = _FakeThread(thread_alive) if thread_alive is not None else None
        self._handlers = handlers
        self._chats = chats
        self._start_res = start_res if start_res is not None else {"started": True}
        self._stop_res = stop_res if stop_res is not None else {"stopped": True}
        self._last_error = last_error
        self.intentional_stop = intentional_stop
        self.start_called = 0
        self.stop_called = 0
        self.restart_called = 0
        # Se impostato, dopo restart() lo stato diventa `restart_heals_to`
        # (simula heal riuscito -> CONNECTED, o fallito -> resta FAILED se None).
        self._restart_heals_to = restart_heals_to
        # False => restart() simula uno stop NON riuscito (thread ancora vivo):
        # nessun ciclo del transport, `restarted=False`, `stop.stopped=False`.
        self._restart_stop_ok = restart_stop_ok
        # "runtime_snapshot"/"restart" => solleva su quel metodo (isolamento eccezioni).
        self._raise_on = raise_on
        # Se impostato, restart() ritorna QUESTO dict (esito malformato): p.es.
        # {"stop": "notadict", "start": True} => `stop`/`start` non-dict. Il parsing
        # difensivo dell'orchestratore NON deve sollevare né lasciare la guardia stuck.
        self._restart_malformed = restart_malformed

    def runtime_snapshot(self):
        if self._raise_on == "runtime_snapshot":
            raise RuntimeError("boom_snapshot")
        return {
            "state": self._state,
            "intentional_stop": self.intentional_stop,
            "last_error": self._last_error,
            "running": self._running,
            "handlers_registered": self._handlers,
            "last_successful_message_ts": None,
        }

    def restart(self):
        if self._raise_on == "restart":
            raise RuntimeError("boom_restart")
        self.restart_called += 1
        if self._restart_malformed is not None:
            # Esito NON conforme (stop/start non-dict): l'orchestratore deve
            # trattarlo come restart_deferred, non sollevare.
            return self._restart_malformed
        if not self._restart_stop_ok:
            # stop NON riuscito (thread ancora vivo): nessun ciclo reale del transport.
            return {"restarted": False,
                    "stop": {"stopped": False, "error": "bot_transport_thread_still_alive"},
                    "start": {}}
        self.stop_called += 1
        self.start_called += 1
        if self._restart_heals_to is not None:
            self._state = self._restart_heals_to
            self._running = self._restart_heals_to == "CONNECTED"
            self._handlers = 1 if self._restart_heals_to == "CONNECTED" else 0
        started = self._state == "CONNECTED"
        return {"restarted": started, "stop": {"stopped": True}, "start": {"started": started}}

    def force_state(self, state):
        """Helper test: forza lo stato (simula una nuova degradazione runtime)."""
        self._state = state
        self._running = state == "CONNECTED"
        self._handlers = 1 if state == "CONNECTED" else 0

    @property
    def state(self):
        return self._state

    @property
    def running(self):
        return self._running

    @property
    def _runtime_thread(self):
        return self._thread

    def start(self):
        self.start_called += 1
        return self._start_res

    def stop(self):
        self.stop_called += 1
        # Dopo uno stop "riuscito" il thread muore (per i fake che lo simulano).
        if self._stop_res.get("stopped"):
            self._thread = None
        return self._stop_res

    def status(self):
        return {
            "state": self._state,
            "running": self._running,
            "handlers_registered": self._handlers,
            "active_network_resources": self._handlers,
            "monitored_chat_count": self._chats,
            "listener_started": self._running,
            "last_error": self._last_error,
            "last_successful_message_ts": None,
        }


def test_requires_at_least_one_runtime():
    with pytest.raises(ValueError):
        TelegramMultiBotRuntime([])


def test_all_connected_aggregates_connected_and_sums():
    a = _FakeChild(handlers=1, chats=2)
    b = _FakeChild(handlers=1, chats=3)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "CONNECTED"
    assert rt.running is True
    st = rt.status()
    assert st["handlers_registered"] == 2
    assert st["monitored_chat_count"] == 5
    assert st["expected_handlers"] == 2
    assert st["bot_count"] == 2
    assert st["healthy_bot_count"] == 2


def test_one_child_failed_aggregates_failed():
    # BLOCK fail-closed: un solo bot degradato => l'intero runtime è FAILED (in
    # PR-5b l'autoheal service-level fa restart-all; per-bot in PR-5c).
    a = _FakeChild(state="CONNECTED", handlers=1)
    b = _FakeChild(state="FAILED", running=False, handlers=0)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "FAILED"
    assert rt.running is False
    assert rt.status()["healthy_bot_count"] == 1


def test_all_stopped_and_all_created_aggregate():
    stopped = TelegramMultiBotRuntime([_FakeChild(state="STOPPED", running=False),
                                       _FakeChild(state="STOPPED", running=False)])
    assert stopped.state == "STOPPED"
    created = TelegramMultiBotRuntime([_FakeChild(state="CREATED", running=False),
                                       _FakeChild(state="CREATED", running=False)])
    assert created.state == "CREATED"


def test_runtime_thread_any_alive():
    dead = _FakeChild(thread_alive=False)
    alive = _FakeChild(thread_alive=True)
    rt = TelegramMultiBotRuntime([dead, alive])
    assert rt._runtime_thread.is_alive() is True
    both_dead = TelegramMultiBotRuntime([_FakeChild(thread_alive=False),
                                         _FakeChild(thread_alive=False)])
    assert both_dead._runtime_thread.is_alive() is False


def test_start_all_ok():
    a = _FakeChild(start_res={"started": True})
    b = _FakeChild(start_res={"started": True})
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.start()
    assert out == {"started": True}
    assert a.start_called == 1 and b.start_called == 1


def test_start_partial_failure_is_failclosed_and_does_not_stop_started():
    # BLOCK: se un bot non parte, l'aggregato è started=False con errori; i child
    # già avviati NON vengono fermati qui (thread visibile => guard blocca retry).
    a = _FakeChild(start_res={"started": True})
    b = _FakeChild(start_res={"started": False, "error": "boom_b"})
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.start()
    assert out["started"] is False
    assert "boom_b" in out["error"]
    assert a.stop_called == 0  # il child avviato NON è stato fermato


def test_partial_start_then_stop_recovers_not_wedged():
    # Evidence (Fugu full-range): dopo un avvio PARZIALE (un child fallisce, thread
    # morto) i child avviati restano fermabili => stop() ritorna stopped=True => un
    # restart può ripartire pulito (il service NON resta wedged). Il caso fail-closed
    # (thread superstite) è l'anti-409 intenzionale e transitorio (il thread poi muore).
    a = _FakeChild(start_res={"started": True}, thread_alive=True, stop_res={"stopped": True})
    b = _FakeChild(start_res={"started": False, "error": "boom"}, thread_alive=False)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.start()["started"] is False       # avvio parziale
    assert rt.stop()["stopped"] is True          # fermabile => non wedged, restart possibile


def test_partial_start_keeps_anti_409_guard_active_no_restart_storm():
    # Evidence (Fugu full-range #2): dopo un avvio PARZIALE (un child vivo, uno
    # fallito) l'aggregato è FAILED ma il child avviato resta VIVO => il proxy
    # _runtime_thread è any-alive => True. Il service legge questo come
    # `previous_runtime_still_alive` e BLOCCA il restart-all (anti-409): niente
    # secondo getUpdates sullo stesso token, niente loop di 409/doppio consumo. Il
    # child sano continua a servire; la gestione per-bot dello stato FAILED è PR-5c.
    a = _FakeChild(start_res={"started": True}, thread_alive=True, state="CONNECTED")
    b = _FakeChild(start_res={"started": False, "error": "boom"}, thread_alive=False,
                   state="FAILED", running=False, handlers=0)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.start()["started"] is False          # avvio parziale => aggregato non-started
    assert rt.state == "FAILED"                     # un child FAILED => aggregato FAILED
    assert rt._runtime_thread.is_alive() is True    # anti-409 attivo => restart-all BLOCCATO


def test_stop_all_dead_is_stopped():
    a = _FakeChild(thread_alive=True, stop_res={"stopped": True})
    b = _FakeChild(thread_alive=True, stop_res={"stopped": True})
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.stop()
    assert out == {"stopped": True}
    assert rt.intentional_stop is True


def test_stop_failclosed_if_any_child_thread_survives():
    # BLOCK anti-409: se un thread child sopravvive allo stop, l'aggregato NON è
    # stopped (un restart aprirebbe un secondo getUpdates sullo stesso token).
    a = _FakeChild(thread_alive=True, stop_res={"stopped": True})

    class _Survivor(_FakeChild):
        def stop(self):
            self.stop_called += 1
            return {"stopped": False, "error": "bot_transport_thread_still_alive"}

    b = _Survivor(thread_alive=True)
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.stop()
    assert out["stopped"] is False
    assert "multibot_thread_still_alive" in out["error"]
    # intentional_stop PRESERVATO anche con thread zombie superstite (rilievo Fugu
    # Ultra): stop() è intento operatore => niente restart-all automatico (che
    # 409-erebbe sullo zombie). Coerente col ramo "nessun thread vivo".
    assert rt.intentional_stop is True


def test_runtime_snapshot_carries_expected_handlers():
    rt = TelegramMultiBotRuntime([_FakeChild(handlers=1), _FakeChild(handlers=1)])
    snap = rt.runtime_snapshot()
    assert snap["handlers_registered"] == 2
    assert snap["expected_handlers"] == 2


def test_mix_connecting_only_during_start_failed_when_persistent():
    # BLOCK (convergente GPT-5.6 Terra / Fable 5 / Fugu Ultra): un mix senza FAILED è
    # CONNECTING solo DURANTE start() (transitorio, no restart-storm); FUORI dalla
    # finestra di start() un mix che non converge (child giù senza flag FAILED) è
    # una DEGRADAZIONE PERSISTENTE => FAILED, così l'autoheal riavvia (niente
    # mascheramento indefinito che sopprime l'autoheal e perde segnali).
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="CREATED", running=False)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "FAILED"          # persistente (fuori start)
    rt._starting = True
    assert rt.state == "CONNECTING"      # transitorio (durante start)


def test_persistent_connected_stopped_mix_is_failed():
    # BLOCK (Fable): un child in STOPPED non intenzionale (thread morto) mentre altri
    # CONNECTED => FAILED (fuori start), NON CONNECTING indefinito.
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="STOPPED", running=False)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "FAILED"


def test_state_is_connecting_during_actual_start_fanout():
    # BLOCK: durante il fan-out REALE di start(), lo stato osservato mentre un child
    # è già CONNECTED e un altro ancora CREATED deve essere CONNECTING (_starting).
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="CREATED", running=False)
    rt = TelegramMultiBotRuntime([a, b])
    observed = {}
    orig_b_start = b.start

    def _b_start():
        observed["state"] = rt.state  # a=CONNECTED, b=CREATED, _starting=True
        return orig_b_start()

    b.start = _b_start
    rt.start()
    assert observed["state"] == "CONNECTING"
    assert rt.state == "FAILED"  # dopo start(): _starting False, mix persistente


def test_stop_preserves_intentional_stop_on_operator_stop():
    # BLOCK (Fable 5): stop() è invocato per intento OPERATORE. Anche se un child
    # riporta stopped=False senza thread vivi, intentional_stop DEVE restare True: se
    # venisse azzerato, l'autoheal service-level riavvierebbe il listener DOPO uno
    # shutdown voluto (ripresa consumo segnali/piazzamenti contro l'intento operatore
    # = rischio safety). Senza thread vivi non c'è orfano né consumo => preservare è
    # sicuro. [Supera Greptile P2, che azzerava il flag e riabilitava il restart.]
    class _DeadButFailedStop(_FakeChild):
        def stop(self):
            self.stop_called += 1
            self._thread = None  # thread morto
            return {"stopped": False, "error": "boom"}

    a = _FakeChild(thread_alive=True, stop_res={"stopped": True})
    b = _DeadButFailedStop(thread_alive=False)
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.stop()
    assert out["stopped"] is False
    assert rt.intentional_stop is True  # PRESERVATO => niente restart contro l'operatore


def test_start_clears_intentional_stop_so_restart_is_not_suppressed():
    # BLOCK (rilievo Fugu Ultra): preservare intentional_stop in stop() non deve
    # incastrare un restart. `start()` azzera SEMPRE intentional_stop: la sequenza di
    # restart (stop→start), sia da operatore sia dall'autoheal service-level, riparte
    # pulita. Così il flag preservato non lascia il listener morto per sempre.
    a = _FakeChild(thread_alive=True, stop_res={"stopped": True})
    b = _FakeChild(thread_alive=False, stop_res={"stopped": False, "error": "boom"})
    rt = TelegramMultiBotRuntime([a, b])
    rt.stop()
    assert rt.intentional_stop is True  # preservato dopo stop fallito
    rt.start()
    assert rt.intentional_stop is False  # start ripulisce => restart NON soppresso


def test_any_failed_child_still_aggregates_failed():
    # Un child DEGRADATO (FAILED) domina comunque => FAILED (fail-closed reale).
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="FAILED", running=False, handlers=0)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "FAILED"


def test_any_alive_thread_join_fans_out_to_children():
    # BLOCK (Fable): il proxy _runtime_thread deve esporre join() (fan-out sui thread
    # child), non solo is_alive(): un chiamante che tratta il proxy come Thread reale
    # (shutdown/autoheal) non deve prendere AttributeError e saltare l'anti-409.
    joined = {"a": 0, "b": 0}

    class _JoinThread:
        def __init__(self, key):
            self._key = key

        def is_alive(self):
            return True

        def join(self, timeout=None):
            joined[self._key] += 1

    a = _FakeChild(thread_alive=True)
    a._thread = _JoinThread("a")
    b = _FakeChild(thread_alive=True)
    b._thread = _JoinThread("b")
    rt = TelegramMultiBotRuntime([a, b])
    rt._runtime_thread.join(timeout=0.1)
    assert joined == {"a": 1, "b": 1}


def test_latest_iso_ts_is_chronological_across_timezone_offsets():
    # BLOCK (Fable): _latest_iso_ts confronta le datetime UTC, NON le stringhe. Con
    # offset diversi, `13:00+05:00` (=08:00Z) è PRIMA di `12:00+00:00`, ma il max
    # lessicografico sceglierebbe erroneamente `13:00+05:00`.
    earlier = "2026-01-01T13:00:00+05:00"   # 08:00Z
    later = "2026-01-01T12:00:00+00:00"     # 12:00Z (più recente)
    assert _latest_iso_ts([earlier, later]) == later
    assert _latest_iso_ts([later, earlier]) == later
    # Fail-safe: valori non parsabili ignorati; nessuno valido => None.
    assert _latest_iso_ts([None, "not-a-date"]) is None


def test_child_thread_alive_failclosed_on_exception():
    # Helper: se is_alive() solleva => fail-closed (True: non nascondere l'orfano).
    class _BoomThread:
        def is_alive(self):
            raise RuntimeError("boom")

    class _Child:
        _runtime_thread = _BoomThread()

    assert _child_thread_alive(_Child()) is True


# ============================ PR-5c: autoheal PER-BOT ============================

def test_perbot_autoheal_restarts_only_failed_child():
    # BLOCK PR-5c: con un bot sano (A) e uno FAILED (B), l'autoheal per-bot riavvia
    # SOLO B; A NON viene toccato (niente restart-ALL). Prima di PR-5c non esiste
    # `run_perbot_autoheal_once` e il recovery era il restart-ALL del service.
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="FAILED", running=False, handlers=0, restart_heals_to="CONNECTED")
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    assert out["healed"] == 1
    assert a.restart_called == 0   # bot sano MAI toccato
    assert b.restart_called == 1   # solo il bot giù riavviato
    assert rt.state == "CONNECTED"  # B guarito => aggregato torna CONNECTED


def test_perbot_autoheal_all_healthy_is_noop():
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="CONNECTED")
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    assert out["healed"] == 0 and out["locked_out"] == 0
    assert a.restart_called == 0 and b.restart_called == 0


def test_perbot_autoheal_budget_exhaustion_locks_out_only_that_bot():
    # BLOCK PR-5c: un bot che resta FAILED dopo i restart esaurisce il budget
    # per-child (max 3 nella finestra) => lockout del SOLO bot (niente restart storm),
    # senza mai toccare il bot sano.
    # Clock iniettato: così run_perbot_autoheal_once E status() (che legge il lockout)
    # condividono lo STESSO tempo — altrimenti status() userebbe il clock reale.
    clock = {"t": 0.0}
    policy = TelegramAutohealPolicy(clock=lambda: clock["t"])
    a = _FakeChild(state="CONNECTED")
    b = _FakeChild(state="FAILED", running=False, handlers=0)  # non guarisce mai
    rt = TelegramMultiBotRuntime([a, b], autoheal_policy=policy)
    # 3 restart, avanzando oltre il cooldown (20s) e dentro la finestra (300s)
    for t in (0.0, 25.0, 50.0):
        clock["t"] = t
        rt.run_perbot_autoheal_once()
    assert b.restart_called == 3
    # 4° ciclo: budget esaurito => lockout, nessun ulteriore restart
    clock["t"] = 75.0
    out = rt.run_perbot_autoheal_once()
    assert out["locked_out"] == 1
    assert b.restart_called == 3
    assert rt.locked_out_bot_count() == 1
    assert rt.status()["locked_out_bot_count"] == 1
    assert a.restart_called == 0


def test_perbot_autoheal_recovered_child_resets_budget():
    # Un bot che si riavvia con successo (torna CONNECTED) AZZERA il proprio budget:
    # una degradazione SUCCESSIVA riparte con budget fresco, non subito in lockout.
    b = _FakeChild(state="FAILED", running=False, handlers=0, restart_heals_to="CONNECTED")
    rt = TelegramMultiBotRuntime([b])
    rt.run_perbot_autoheal_once(now_ts=0.0)    # restart => guarisce (CONNECTED)
    assert b.restart_called == 1 and b.state == "CONNECTED"
    rt.run_perbot_autoheal_once(now_ts=25.0)   # sano => reset budget
    # nuova degradazione: deve TENTARE il restart (budget fresco), non essere in lockout
    b.force_state("FAILED")
    b._restart_heals_to = None
    rt.run_perbot_autoheal_once(now_ts=50.0)
    assert b.restart_called == 2                 # tentativo reale (budget resettato)
    assert rt.locked_out_bot_count(now_ts=50.0) == 0  # NON in lockout


def test_perbot_autoheal_skips_intentionally_stopped_child():
    # Un bot STOPPED intenzionalmente NON deve essere riavviato dall'autoheal.
    b = _FakeChild(state="STOPPED", running=False, handlers=0, intentional_stop=True)
    rt = TelegramMultiBotRuntime([b])
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    assert out["healed"] == 0
    assert b.restart_called == 0


def test_perbot_autoheal_orphan_deferred_then_stuck_lockout():
    # BLOCK (Fable/Fugu/Greptile + GPT): uno stop che NON cicla il transport (thread
    # vivo) è DEFERRED e NON consuma il budget restart (un wind-down transitorio non
    # deve far lockout). MA un thread PERMANENTEMENTE bloccato non deve generare retry
    # infiniti (rilievo GPT/Fable): dopo N deferral CONSECUTIVI => lockout "stuck".
    b = _FakeChild(state="FAILED", running=False, handlers=0, restart_stop_ok=False)
    rt = TelegramMultiBotRuntime([b])
    # primi cicli: deferred, nessun lockout (finestra di wind-down)
    for t in (0.0, 25.0):
        out = rt.run_perbot_autoheal_once(now_ts=t)
        assert out["restart_failed"] == 1 and out["healed"] == 0
        assert out["locked_out"] == 0
    # 3° ciclo: deferral consecutivi >= max_restarts_in_window => lockout stuck
    out = rt.run_perbot_autoheal_once(now_ts=50.0)
    assert out["locked_out"] == 1
    assert rt.locked_out_bot_count(now_ts=50.0) == 1
    assert rt.status()["perbot_restart_total"] == 0   # nessun restart REALE contato


def test_perbot_autoheal_failing_child_does_not_abort_healthy_siblings():
    # BLOCK (CodeRabbit critical): un'eccezione su un child (runtime_snapshot o
    # restart) NON deve abortire il ciclo per gli altri — «un singolo bot giù non
    # butta giù gli altri». Il child che solleva è registrato come action=error.
    boom_snap = _FakeChild(state="FAILED", running=False, handlers=0, raise_on="runtime_snapshot")
    boom_restart = _FakeChild(state="FAILED", running=False, handlers=0, raise_on="restart")
    good = _FakeChild(state="FAILED", running=False, handlers=0, restart_heals_to="CONNECTED")
    rt = TelegramMultiBotRuntime([boom_snap, boom_restart, good])
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    assert out["errors"] == 2                 # i due child che sollevano
    assert out["healed"] == 1                 # il bot sano è stato comunque curato
    assert good.restart_called == 1


def test_perbot_autoheal_healed_counts_only_successful_restart():
    # BLOCK (CodeRabbit/Greptile/Fugu): `healed` conta SOLO i restart REALMENTE
    # riusciti, non i tentativi. Un bot che si riavvia ma resta FAILED è restart_failed.
    fails = _FakeChild(state="FAILED", running=False, handlers=0)                  # non guarisce
    heals = _FakeChild(state="FAILED", running=False, handlers=0, restart_heals_to="CONNECTED")
    rt = TelegramMultiBotRuntime([fails, heals])
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    assert out["healed"] == 1          # solo `heals`
    assert out["restart_failed"] == 1  # `fails` conteggiato come tentativo non riuscito


def test_perbot_autoheal_restart_in_progress_guard_skips_concurrent_restart():
    # BLOCK (GPT/Fable/Fugu): con restart() fuori dal lock, un ciclo che trova
    # `_child_restart_in_progress` già True per quel child NON deve avviare un secondo
    # restart in parallelo (doppio getUpdates/orphan, budget incoerente).
    b = _FakeChild(state="FAILED", running=False, handlers=0, restart_heals_to="CONNECTED")
    rt = TelegramMultiBotRuntime([b])
    rt._child_restart_in_progress[0] = True  # simula un restart già in corso
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    assert any(a["action"] == "restart_in_progress" for a in out["actions"])
    assert b.restart_called == 0             # NESSUN secondo restart


def test_perbot_autoheal_stuck_lockout_suppresses_next_cycle_no_restart():
    # BLOCK (Fugu/Fable): dopo il lockout "stuck" un bot NON deve essere riavviato al
    # ciclo successivo (policy => SUPPRESS_RESTART finché il lockout è attivo): niente
    # restart storm su un thread permanentemente bloccato.
    b = _FakeChild(state="FAILED", running=False, handlers=0, restart_stop_ok=False)
    rt = TelegramMultiBotRuntime([b])
    for t in (0.0, 25.0, 50.0):   # 3 deferral consecutivi => stuck lockout al 3°
        rt.run_perbot_autoheal_once(now_ts=t)
    assert rt.locked_out_bot_count(now_ts=50.0) == 1
    calls_before = b.restart_called
    out = rt.run_perbot_autoheal_once(now_ts=60.0)   # entro il lockout
    assert b.restart_called == calls_before          # SUPPRESS: nessun nuovo restart
    assert out["healed"] == 0


def test_perbot_autoheal_malformed_restart_result_does_not_wedge_guard():
    # BLOCK (GPT-5.6 Terra, round-5): se restart() ritorna un esito NON conforme
    # (`stop`/`start` truthy ma non-dict), il parsing NON deve sollevare fuori dal
    # percorso di reset: la guardia `_child_restart_in_progress[i]` va SEMPRE azzerata
    # (`finally`), altrimenti il child resta escluso dall'autoheal per sempre.
    b = _FakeChild(state="FAILED", running=False, handlers=0,
                   restart_malformed={"restarted": True, "stop": "notadict", "start": True})
    rt = TelegramMultiBotRuntime([b])
    out = rt.run_perbot_autoheal_once(now_ts=1000.0)
    # Esito malformato => trattato come deferral (stop non provato), NON eccezione
    # (nessuna action "error", che sarebbe la firma della guardia stuck su pre-fix).
    assert b.restart_called == 1
    assert out["healed"] == 0
    assert out["errors"] == 0
    assert any(a["action"] == "restart_deferred" for a in out["actions"])
    # Guardia azzerata: il child NON è escluso permanentemente.
    assert rt._child_restart_in_progress[0] is False
    # Ciclo successivo: il child viene di nuovo valutato/riavviato (non "restart_in_progress").
    out2 = rt.run_perbot_autoheal_once(now_ts=1001.0)
    assert b.restart_called == 2
    assert not any(a["action"] == "restart_in_progress" for a in out2["actions"])
