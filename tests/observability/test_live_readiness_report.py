from observability.runtime_probe import RuntimeProbe
from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig


class _Ready:
    def is_ready(self):
        return True


class _NotReady:
    def is_ready(self):
        return False


class _TradingReady:
    def readiness(self):
        return {"state": "READY", "health": {"lag_ms": 1}}


class _TradingUnknown:
    def readiness(self):
        return {"state": "READY", "health": None}


class _BetfairConnected:
    connected = True


class _BetfairDisconnected:
    connected = False


class _SafeModeActive:
    def is_enabled(self):
        return True


class _SafeModeInactive:
    def is_enabled(self):
        return False


class _GateBus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _GateDb:
    def __init__(self, key_source="unknown"):
        class _Cipher:
            def __init__(self, src):
                self.key_source = src

        self._cipher = _Cipher(key_source)

    def _execute(self, *_args, **_kwargs):
        return None


class _GateSettings:
    def __init__(self, *, strict_live_key_source_required=False, config=None):
        self._strict = strict_live_key_source_required
        self._config = config

    def load_roserpina_config(self):
        if self._config is not None:
            return self._config
        return RoserpinaConfig(
            table_count=1,
            max_daily_loss=100.0,
            max_drawdown_hard_stop_pct=20.0,
            max_open_exposure=200.0,
        )

    def load_live_readiness_ok(self):
        return True

    def load_strict_live_key_source_required(self):
        return self._strict


class _GateBetfair:
    def set_simulation_mode(self, _enabled):
        return None

    def connect(self, **_kwargs):
        return {"ok": True}

    def get_account_funds(self):
        return {"available": 100.0}

    def status(self):
        return {"connected": True}


class _GateTelegram:
    def status(self):
        return {"connected": True}



def test_runtime_probe_live_readiness_report_shape_is_stable():
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    report = probe.get_live_readiness_report()

    assert set(report.keys()) == {"ready", "level", "blockers", "details"}
    assert set(report["details"].keys()) == {"degraded", "components", "unknown_components"}
    assert isinstance(report["blockers"], list)
    assert isinstance(report["details"]["degraded"], list)
    assert isinstance(report["details"]["components"], dict)


def test_ready_degraded_not_ready_are_distinguishable_in_report_contract():
    ready_probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )
    degraded_probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairDisconnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )
    not_ready_probe = RuntimeProbe(
        db=None,
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    assert ready_probe.get_live_readiness_report()["level"] == "READY"
    assert degraded_probe.get_live_readiness_report()["level"] == "DEGRADED"
    assert not_ready_probe.get_live_readiness_report()["level"] == "NOT_READY"


def test_unknown_state_is_not_reported_as_ready_and_fails_closed():
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingUnknown(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    report = probe.get_live_readiness_report()

    assert report["ready"] is False
    assert report["level"] == "NOT_READY"
    assert "trading_engine" in report["details"]["unknown_components"]
    assert any(item["code"] == "READINESS_SIGNAL_UNKNOWN" for item in report["blockers"])


def test_missing_dependency_and_kill_switch_have_expected_blockers():
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=None,
        safe_mode=_SafeModeActive(),
        shutdown_manager=_Ready(),
    )

    report = probe.get_live_readiness_report()
    blocker_codes = {item["code"] for item in report["blockers"]}
    degraded_codes = {item["code"] for item in report["details"]["degraded"]}

    assert report["ready"] is False
    assert report["level"] == "NOT_READY"
    assert "LIVE_DEPENDENCY_MISSING" in blocker_codes
    assert "SAFE_MODE_BLOCKING" in degraded_codes


def test_runtime_controller_readiness_exposes_strict_key_source_truth():
    rc = RuntimeController(
        bus=_GateBus(),
        db=_GateDb(key_source="unknown"),
        settings_service=_GateSettings(strict_live_key_source_required=True),
        betfair_service=_GateBetfair(),
        telegram_service=_GateTelegram(),
        safe_mode=_SafeModeInactive(),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    key_source_state = readiness["details"]["key_source_state"]
    assert key_source_state["key_source"] == "unknown"
    assert key_source_state["strict_live_key_source_required"] is True
    assert key_source_state["configured_strict_live_key_source_required"] is True
    assert key_source_state["passed"] is False
    assert "LIVE_KEY_SOURCE_UNSAFE" in readiness["blockers"]


def test_runtime_controller_readiness_exposes_hard_stop_config_state_and_blockers():
    rc = RuntimeController(
        bus=_GateBus(),
        db=_GateDb(key_source="env"),
        settings_service=_GateSettings(
            strict_live_key_source_required=False,
            config=RoserpinaConfig(
                table_count=1,
                max_daily_loss=None,
                max_drawdown_hard_stop_pct=0.0,
                max_open_exposure=200.0,
            ),
        ),
        betfair_service=_GateBetfair(),
        telegram_service=_GateTelegram(),
        safe_mode=_SafeModeInactive(),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )
    state = readiness["details"]["hard_stop_config_state"]

    assert state["required_fields"] == [
        "max_daily_loss",
        "max_drawdown_hard_stop_pct",
        "max_open_exposure",
    ]
    assert "max_daily_loss" in state["missing_fields"]
    assert "max_drawdown_hard_stop_pct" in state["invalid_fields"]
    assert "LIVE_HARD_STOP_CONFIG_MISSING" in readiness["blockers"]
    assert "LIVE_HARD_STOP_CONFIG_INVALID" in readiness["blockers"]


def test_runtime_controller_readiness_surfaces_non_finite_hard_stop_invalid_state():
    rc = RuntimeController(
        bus=_GateBus(),
        db=_GateDb(key_source="env"),
        settings_service=_GateSettings(
            strict_live_key_source_required=False,
            config=RoserpinaConfig(
                table_count=1,
                max_daily_loss=float("inf"),
                max_drawdown_hard_stop_pct=20.0,
                max_open_exposure=200.0,
            ),
        ),
        betfair_service=_GateBetfair(),
        telegram_service=_GateTelegram(),
        safe_mode=_SafeModeInactive(),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )
    state = readiness["details"]["hard_stop_config_state"]

    assert "max_daily_loss" in state["invalid_fields"]
    assert "LIVE_HARD_STOP_CONFIG_INVALID" in readiness["blockers"]


def _no_checker_probe():
    # db/runtime_controller/shutdown_manager senza is_ready -> 'no-checker'
    # UNKNOWN (con fallback_status=READY). Sono presenti ma non espongono
    # un'interfaccia di readiness.
    return RuntimeProbe(
        db=object(),
        trading_engine=_TradingReady(),
        runtime_controller=object(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=object(),
    )


def test_no_checker_components_do_not_block_live_readiness_at_boot():
    # #361 B-1: al boot i componenti 'no-checker' non devono bloccare (erano
    # blocker spuri -> deadlock LIVE al boot).
    report = _no_checker_probe().get_live_readiness_report(tolerate_pending_connection=True)

    assert report["ready"] is True
    assert report["level"] == "READY"
    assert report["blockers"] == []


def test_no_checker_components_do_not_block_live_readiness_at_runtime():
    # #361 B-1 (GPT-5.6 Sol + Fable 5, BLOCK): il rilassamento 'no-checker'
    # NON e' phase-aware. A runtime (default: watchdog / is_live_allowed /
    # _on_signal_received) i componenti strutturalmente senza checker
    # (database/runtime_controller/shutdown_manager, che non hanno is_ready)
    # NON devono bloccare: renderli UNKNOWN=blocker disabiliterebbe LIVE in
    # modo permanente dopo un avvio riuscito. Differiscono da 'disconnected'
    # (B-2), che invece a runtime resta bloccante (vedi test sotto).
    report = _no_checker_probe().get_live_readiness_report()

    assert report["ready"] is True
    assert report["level"] == "READY"
    assert report["blockers"] == []


def test_ready_without_health_still_fails_closed_in_both_phases():
    # #361 B-1 (BLOCK): il rilassamento e' ristretto a reason 'no-checker'.
    # Un UNKNOWN diverso (trading_engine 'ready_without_health') resta
    # fail-closed sia al boot sia a runtime.
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingUnknown(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    for report in (
        probe.get_live_readiness_report(),
        probe.get_live_readiness_report(tolerate_pending_connection=True),
    ):
        assert report["ready"] is False
        assert report["level"] == "NOT_READY"
        assert "trading_engine" in report["details"]["unknown_components"]


def test_boot_gate_tolerates_disconnected_but_runtime_does_not():
    # #361 B-2: al boot betfair 'disconnected' e' atteso (connette in start());
    # a runtime (default) la disconnessione resta DEGRADED (monitoraggio intatto).
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairDisconnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    # Default (watchdog/runtime): BLOCK -> disconnessione ancora segnalata.
    assert probe.get_live_readiness_report()["level"] == "DEGRADED"

    # Boot gate: tollera la connessione pendente -> READY.
    boot = probe.get_live_readiness_report(tolerate_pending_connection=True)
    assert boot["ready"] is True
    assert boot["level"] == "READY"


def test_boot_gate_does_not_tolerate_real_degraded():
    # #361: il rilassamento vale SOLO per 'disconnected'. Un DEGRADED reale
    # (is_ready False -> 'unhealthy') resta bloccante anche al boot.
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_NotReady(),
    )

    boot = probe.get_live_readiness_report(tolerate_pending_connection=True)

    assert boot["ready"] is False
    assert boot["level"] == "DEGRADED"


def test_boot_does_not_promote_mixed_degradation_or_non_allowlisted():
    # #361 (CodeRabbit Major): il rilassamento 'disconnected' vale SOLO per la
    # allowlist pending-connection (betfair) e SOLO senza altri degradi reali.
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    # betfair 'disconnected' MA con streaming auth degradato -> NON promosso.
    mixed = {
        "status": "DEGRADED",
        "reason": "disconnected",
        "details": {"streaming_feed": {"auth_degraded": True}},
    }
    assert (
        probe._normalize_component_status(
            "betfair_service", mixed, tolerate_pending_connection=True
        )
        == "DEGRADED"
    )

    # betfair 'disconnected' MA con external_io degradato -> NON promosso.
    io_bad = {
        "status": "DEGRADED",
        "reason": "disconnected",
        "details": {"external_io": {"last_status": "UNAVAILABLE"}},
    }
    assert (
        probe._normalize_component_status(
            "betfair_service", io_bad, tolerate_pending_connection=True
        )
        == "DEGRADED"
    )

    # pura disconnessione di betfair -> promossa a READY (solo al boot).
    clean = {"status": "DEGRADED", "reason": "disconnected", "details": {}}
    assert (
        probe._normalize_component_status(
            "betfair_service", clean, tolerate_pending_connection=True
        )
        == "READY"
    )

    # componente FUORI allowlist (es. database) 'disconnected' -> mai promosso.
    other = {"status": "DEGRADED", "reason": "disconnected", "details": {}}
    assert (
        probe._normalize_component_status(
            "database", other, tolerate_pending_connection=True
        )
        == "DEGRADED"
    )


# --- Regressione a livello controller: split boot vs runtime del deploy gate ---


def _make_gate_rc():
    return RuntimeController(
        bus=_GateBus(),
        db=_GateDb(key_source="env"),
        settings_service=_GateSettings(),
        betfair_service=_GateBetfair(),
        telegram_service=_GateTelegram(),
        safe_mode=_SafeModeInactive(),
    )


class _ProbeBootAware:
    """Ritorna READY solo se invocata col kwarg tolerate_pending_connection=True;
    a strict (runtime) segnala NOT_READY (fail-closed)."""

    def __init__(self):
        self.calls = []

    def get_live_readiness_report(self, *, tolerate_pending_connection=False):
        self.calls.append(tolerate_pending_connection)
        if tolerate_pending_connection:
            return {"ready": True, "level": "READY", "blockers": []}
        return {
            "ready": False,
            "level": "NOT_READY",
            "blockers": [{"name": "betfair_service", "status": "NOT_READY",
                          "reason": "disconnected", "code": "LIVE_PROBE_NOT_READY"}],
        }


class _ProbeNoKwarg:
    """Getter privo del kwarg: il boot NON deve passarlo (niente TypeError)."""

    @staticmethod
    def get_live_readiness_report():
        return {"ready": True, "level": "READY", "blockers": []}


class _ProbeInternalTypeError:
    """Solleva TypeError DENTRO l'implementazione quando invocata col kwarg
    (boot). Il vecchio codice (except TypeError -> retry senza kwarg) lo
    mascherava degradando a strict e restituendo READY: fail-open. Il fix con
    inspect.signature NON deve mascherarlo -> probe_report_exception."""

    def __init__(self):
        self.plain_called = False

    def get_live_readiness_report(self, *, tolerate_pending_connection=False):
        if tolerate_pending_connection:
            raise TypeError("internal boom (non e' un mismatch di firma)")
        self.plain_called = True
        return {"ready": True, "level": "READY", "blockers": []}


def test_controller_boot_flag_threads_probe_tolerance():
    # #361 (CodeRabbit CRITICAL): boot=True tollera pending-connection; boot
    # assente (runtime: is_live_allowed / _on_signal_received) resta strict.
    rc = _make_gate_rc()
    probe = _ProbeBootAware()
    rc.runtime_probe = probe

    ok_boot, _reason_boot, _r1 = rc._get_probe_live_readiness_report(boot=True)
    ok_runtime, _reason_runtime, _r2 = rc._get_probe_live_readiness_report(boot=False)

    assert ok_boot is True
    assert ok_runtime is False  # runtime NON tollera la disconnessione: fail-closed
    assert probe.calls == [True, False]


def test_controller_runtime_default_is_strict():
    # BLOCK: il default di _get_probe_live_readiness_report NON deve tollerare.
    rc = _make_gate_rc()
    rc.runtime_probe = _ProbeBootAware()

    ok_default, _reason, _r = rc._get_probe_live_readiness_report()

    assert ok_default is False


def test_controller_boot_does_not_pass_kwarg_to_getter_without_it():
    # #361: se il getter non accetta il kwarg, boot=True non deve passarlo
    # (inspect.signature) -> nessun TypeError spurio, probe letto correttamente.
    rc = _make_gate_rc()
    rc.runtime_probe = _ProbeNoKwarg()

    ok, _reason, _r = rc._get_probe_live_readiness_report(boot=True)

    assert ok is True


def test_controller_internal_typeerror_is_not_masked_as_strict():
    # #361 (Fugu/Fable/CodeRabbit/Sourcery BLOCK): un TypeError sollevato DENTRO
    # il getter col kwarg NON deve essere degradato a una chiamata strict (che
    # nel vecchio codice restituiva READY = fail-open). Deve fallire fail-closed.
    rc = _make_gate_rc()
    probe = _ProbeInternalTypeError()
    rc.runtime_probe = probe

    ok, reason, _r = rc._get_probe_live_readiness_report(boot=True)

    assert ok is False
    assert reason == "probe_report_exception"
    assert probe.plain_called is False  # non ha ritentato in strict silenziosamente


def _probe_ok(status):
    payload = (status.get("details") or {}).get("readiness_payload") or {}
    return bool(payload.get("probe_ok"))


def test_get_deploy_gate_status_propagates_boot_to_probe():
    # #361 (Fugu Ultra BLOCK): il flag boot deve propagarsi da
    # get_deploy_gate_status fino al probe. boot=True -> tolerate=True (GO);
    # default -> strict (NO-GO). Prova la catena, non solo l'helper interno.
    rc = _make_gate_rc()
    probe = _ProbeBootAware()
    rc.runtime_probe = probe

    boot_status = rc.get_deploy_gate_status(
        execution_mode="LIVE", live_enabled=True, live_readiness_ok=True, boot=True
    )
    runtime_status = rc.get_deploy_gate_status(
        execution_mode="LIVE", live_enabled=True, live_readiness_ok=True
    )

    assert probe.calls == [True, False]
    assert _probe_ok(boot_status) is True       # boot: pending-connection tollerato
    assert _probe_ok(runtime_status) is False   # runtime: strict, fail-closed


def test_enforce_deploy_gate_accepts_and_propagates_boot():
    # #361 (Fugu Ultra BLOCK): enforce_deploy_gate (la variante con side effect,
    # usata da start() e dall'headless) deve ACCETTARE boot (niente TypeError)
    # e propagarlo a get_deploy_gate_status -> probe.
    rc = _make_gate_rc()
    probe = _ProbeBootAware()
    rc.runtime_probe = probe

    boot_status = rc.enforce_deploy_gate(
        execution_mode="LIVE", live_enabled=True, live_readiness_ok=True, boot=True
    )
    runtime_status = rc.enforce_deploy_gate(
        execution_mode="LIVE", live_enabled=True, live_readiness_ok=True
    )

    assert probe.calls == [True, False]
    assert _probe_ok(boot_status) is True
    assert _probe_ok(runtime_status) is False


def _real_components_probe():
    # #363: Database e ShutdownManager REALI esposti al probe.
    from database import Database
    from shutdown_manager import ShutdownManager

    db = Database(":memory:")
    sm = ShutdownManager()
    probe = RuntimeProbe(
        db=db,
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=sm,
    )
    return probe, sm


def test_real_db_and_shutdown_manager_are_not_no_checker_when_healthy():
    # #363: sani -> READY e NON piu' tra i componenti 'no-checker' (unknown).
    probe, _sm = _real_components_probe()

    healthy = probe.get_live_readiness_report()

    assert healthy["ready"] is True
    assert healthy["level"] == "READY"
    assert "database" not in healthy["details"]["unknown_components"]
    assert "shutdown_manager" not in healthy["details"]["unknown_components"]


def test_shutting_down_manager_blocks_live_even_at_boot():
    # #363 (BLOCK, chiude la superficie fail-open GPT/Fugu): uno shutdown in
    # corso -> is_ready False -> DEGRADED -> blocca LIVE ANCHE al boot. Senza il
    # checker il manager sarebbe 'no-checker' -> B-1 -> READY (fail-open).
    probe, sm = _real_components_probe()

    sm.register("noop", lambda: None)
    sm.shutdown()
    blocked_boot = probe.get_live_readiness_report(tolerate_pending_connection=True)

    assert blocked_boot["ready"] is False
    assert blocked_boot["level"] == "DEGRADED"
    assert any(d["name"] == "shutdown_manager" for d in blocked_boot["details"]["degraded"])
