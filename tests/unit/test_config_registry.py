"""Test del ConfigRegistry / readiness_report (epic #359 PR-A).

Verifica la fonte-dato unica: enumerazione parametri con metadati, mascheramento
dei segreti (mai in chiaro), e la checklist go-live che riusa
`evaluate_live_readiness` (non reimplementa la logica del gate).
"""

from types import SimpleNamespace

import pytest

from config_registry import ConfigRegistry, readiness_report


class _FakeSettings:
    """Settings service minimale con i loader usati dal registry."""

    def __init__(
        self,
        *,
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
        kill_switch=False,
        username="user",
        app_key="APPKEY",
        certificate="CERT",
        private_key="PK",
        password="PWD",
        telegram_enabled=False,
    ):
        self._em = execution_mode
        self._le = live_enabled
        self._lro = live_readiness_ok
        self._ks = kill_switch
        self._bf = SimpleNamespace(
            username=username, app_key=app_key, certificate=certificate, private_key=private_key
        )
        self._pwd = password
        self._tg = telegram_enabled

    def load_execution_mode(self):
        return self._em

    def load_live_enabled(self):
        return self._le

    def load_live_readiness_ok(self):
        return self._lro

    def load_live_readiness_level(self):
        return "READY"

    def load_kill_switch(self):
        return self._ks

    def load_betfair_config(self):
        return self._bf

    def load_password(self):
        return self._pwd

    def load_telegram_alerts_enabled(self):
        return self._tg

    def load_telegram_alert_chat_id(self):
        return ""

    def load_telegram_alert_min_severity(self):
        return "WARNING"

    def load_telegram_alert_cooldown_sec(self):
        return 300

    def load_simulation_config(self):
        return {
            "starting_balance": 1000.0,
            "commission_pct": 4.5,
            "partial_fill_enabled": True,
            "consume_liquidity": True,
            "persist_state": True,
        }


class _FakeRuntime:
    """Runtime minimale: registra le chiamate a evaluate_live_readiness."""

    def __init__(self, *, blockers, ready, config=None):
        self.calls = []
        self._blockers = list(blockers)
        self._ready = ready
        self.config = config

    def evaluate_live_readiness(self, *, execution_mode=None, live_enabled=None, live_readiness_ok=None):
        self.calls.append((execution_mode, live_enabled, live_readiness_ok))
        return {
            "ready": self._ready,
            "level": "READY" if self._ready else "NOT_READY",
            "blockers": list(self._blockers),
            "details": {"execution_state": {"execution_mode": execution_mode}},
        }


# ---- readiness_report -------------------------------------------------------


@pytest.mark.unit
def test_readiness_report_all_ok_when_no_blockers():
    rt = _FakeRuntime(blockers=[], ready=True)
    rep = readiness_report(rt, execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert rep["ready"] is True
    assert rep["items"] and all(it.ok for it in rep["items"])
    # I kwargs devono essere propagati all'autorita' del gate (non ignorati).
    assert rt.calls[-1] == ("LIVE", True, True)


@pytest.mark.unit
def test_readiness_report_maps_blockers_to_items_with_remedy():
    # BLOCK: readiness_report DERIVA la checklist dai blockers del gate; se
    # ignorasse i blockers (es. hardcodando ok=True) questo test fallirebbe.
    rt = _FakeRuntime(blockers=["LIVE_NOT_ENABLED", "KILL_SWITCH_ACTIVE"], ready=False)
    rep = readiness_report(rt, execution_mode="LIVE")

    assert rep["ready"] is False
    failed = {it.blocker: it for it in rep["items"] if not it.ok}
    assert "LIVE_NOT_ENABLED" in failed
    assert "KILL_SWITCH_ACTIVE" in failed
    assert "--live-enabled" in failed["LIVE_NOT_ENABLED"].remedy
    # I prerequisiti non bloccati restano ✅.
    assert any(it.key == "live_readiness_ok" and it.ok for it in rep["items"])


# ---- ConfigRegistry.entries: mascheramento segreti (BLOCK sicurezza) --------


@pytest.mark.unit
def test_secrets_never_appear_in_plaintext():
    reg = ConfigRegistry(
        _FakeSettings(app_key="SUPER_SECRET_APP", private_key="PRIVKEY123", certificate="CERTDATA", password="PWD_XYZ")
    )
    entries = reg.entries()
    secret_entries = [e for e in entries if e.is_secret]

    assert secret_entries, "attese entry segrete (app_key/certificate/private_key/password)"
    for e in secret_entries:
        assert e.value in ("(impostato)", "(non impostato)")

    # BLOCK: nessun valore reale di segreto deve comparire in ALCUNA entry.
    blob = " ".join(f"{e.key}={e.value}" for e in entries)
    for secret in ("SUPER_SECRET_APP", "PRIVKEY123", "CERTDATA", "PWD_XYZ"):
        assert secret not in blob

    # La presenza resta rilevata: segreto impostato -> valid True.
    app = next(e for e in entries if e.key == "betfair.app_key")
    assert app.valid is True and app.is_secret is True


@pytest.mark.unit
def test_missing_secret_marked_unset_and_invalid():
    reg = ConfigRegistry(_FakeSettings(app_key=""))
    app = next(e for e in reg.entries() if e.key == "betfair.app_key")
    assert app.value == "(non impostato)"
    assert app.valid is False
    assert app.remedy  # rimedio presente


# ---- ConfigRegistry.entries: metadati esecuzione / hard-stop ----------------


@pytest.mark.unit
def test_execution_entries_flags_and_remedy():
    reg = ConfigRegistry(_FakeSettings(execution_mode="LIVE", live_enabled=False))
    entries = {e.key: e for e in reg.entries()}

    assert entries["execution_mode"].value == "LIVE"
    assert entries["execution_mode"].required_for_live is True
    le = entries["live_enabled"]
    assert le.value is False
    assert le.required_for_live is True
    assert "--live-enabled" in le.remedy  # rimedio quando live non abilitato


@pytest.mark.unit
def test_hard_stop_entries_from_runtime_config():
    good = SimpleNamespace(max_daily_loss=50.0, max_drawdown_hard_stop_pct=20.0, max_open_exposure=100.0)
    reg = ConfigRegistry(_FakeSettings(), runtime=_FakeRuntime(blockers=[], ready=True, config=good))
    entries = {e.key: e for e in reg.entries()}
    hs = entries["hard_stop.max_daily_loss"]
    assert hs.valid is True and hs.required_for_live is True

    bad = SimpleNamespace(max_daily_loss=0.0, max_drawdown_hard_stop_pct=None, max_open_exposure=-1)
    reg2 = ConfigRegistry(_FakeSettings(), runtime=_FakeRuntime(blockers=[], ready=True, config=bad))
    e2 = {e.key: e for e in reg2.entries()}
    assert e2["hard_stop.max_daily_loss"].valid is False
    assert e2["hard_stop.max_daily_loss"].remedy


@pytest.mark.unit
def test_entries_cover_all_domains():
    reg = ConfigRegistry(_FakeSettings(), runtime=_FakeRuntime(blockers=[], ready=True, config=SimpleNamespace()))
    keys = {e.key for e in reg.entries()}
    # almeno un parametro per ciascun dominio enumerato
    assert "execution_mode" in keys
    assert any(k.startswith("betfair.") for k in keys)
    assert any(k.startswith("trading.") for k in keys)
    assert any(k.startswith("telegram.") for k in keys)
    assert any(k.startswith("simulation.") for k in keys)
    assert any(k.startswith("hard_stop.") for k in keys)


@pytest.mark.unit
def test_readiness_requires_runtime():
    reg = ConfigRegistry(_FakeSettings(), runtime=None)
    with pytest.raises(ValueError):
        reg.readiness(execution_mode="LIVE")
