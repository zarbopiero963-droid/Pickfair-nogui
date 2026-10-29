"""bk_observe.py (#562): osservabilità Buildkite in sola lettura, verdetti mai più ottimisti dei dati."""
from __future__ import annotations

import ast
import copy
from pathlib import Path

import pytest

from scripts.ci import bk_observe as bo

SHA = "185078e42f30c9e9f29ad2858b73fd27fb6ff70a"
INTENT = "p585-t3-185078e42f30"
SRC = Path(bo.__file__)


def _step(key, state="passed", start="2026-10-29T09:41:30Z", end="2026-10-29T09:42:00Z"):
    return {"id": f"job-{key}", "type": "script", "step_key": key, "state": state, "exit_status": 0,
            "started_at": start, "finished_at": end}


def _build(n_steps=49, state="passed", number=31, commit=SHA, intent=INTENT, finished="2026-10-29T11:00:00Z"):
    jobs = [_step("pf-full-suite-launch")] + [_step(f"s{i}") for i in range(n_steps)] + [{"type": "waiter"}]
    return {"number": number, "state": state, "commit": commit, "branch": "b", "source": "api",
            "meta_data": {"pf_intent": intent, "pf_pr": "585"}, "created_at": "2026-10-29T09:41:27Z",
            "started_at": "2026-10-29T09:41:28Z", "finished_at": finished, "jobs": jobs}


class FakeApi:
    def __init__(self, build, listed=None, whoami=None):
        self.build, self.listed, self.calls = build, listed, []
        self.whoami = whoami or {"uuid": "u", "description": "Claude", "scopes": ["read_builds"], "expires_at": None}

    def __call__(self, path, params=None, raw=False):
        self.calls.append((path, params, raw))
        if path == bo.WHOAMI_PATH:
            return self.whoami
        if path.endswith("/builds"):
            return self.listed if self.listed is not None else [self.build]
        if path.endswith("/log"):
            return "riga1\nriga2\nriga3"
        if path.endswith("/artifacts"):
            return [{"job_id": "j", "state": "finished", "path": "bk-out/unit.xml", "file_size": 1, "sha1sum": "ab"}]
        return copy.deepcopy(self.build)


def _report(api, *extra):
    return bo.main(["report", "--build", "31", "--sha", SHA, "--intent", INTENT, "--expect-steps", "49", *extra],
                   get=api)


def test_complete_build_on_exact_sha_and_intent_passes(capsys):
    assert _report(FakeApi(_build())) == 0
    out = capsys.readouterr().out
    assert "VERDETTO_BUILD = PASS" in out and "step (escluso il launcher): 49" in out and SHA in out


def test_running_build_is_pending_never_pass(capsys):
    b = _build(state="running", finished=None)
    b["jobs"][5]["state"] = "running"
    assert _report(FakeApi(b)) == 3
    assert "VERDETTO_BUILD = PENDING" in capsys.readouterr().out


@pytest.mark.parametrize("mutate, reason", [
    (lambda b: b.update(commit="0" * 40), "SHA diverso"),
    (lambda b: b["meta_data"].update(pf_intent="p585-t2-9af362b47bdb"), "pf_intent diverso"),
    (lambda b: b["jobs"][3].update(state="failed", exit_status=1), "step falliti"),
    (lambda b: b["jobs"][3].update(state="canceled"), "NOT RUN"),
    (lambda b: b["jobs"][3].update(state="skipped"), "NOT RUN"),
    (lambda b: b.update(finished_at="2026-10-29T11:41:29Z"), "oltre il budget"),
    (lambda b: b["jobs"].pop(4), "step: 48 != attesi 49"),
    (lambda b: b.update(state="failed"), ""),
])
def test_any_mismatch_fails(mutate, reason, capsys):
    b = _build()
    mutate(b)
    assert _report(FakeApi(b)) == 1
    out = capsys.readouterr().out
    assert "VERDETTO_BUILD = FAIL" in out and reason in out


def test_a_running_build_with_a_failed_step_is_already_fail(capsys):
    b = _build(state="failing", finished=None)
    b["jobs"][2].update(state="failed", exit_status=1)
    assert _report(FakeApi(b)) == 1


def test_duplicate_build_for_the_same_sha_or_intent_fails(capsys):
    b = _build()
    dup = _build(number=32)
    assert _report(FakeApi(b, listed=[b, dup])) == 1
    assert "build duplicate per lo stesso SHA/intento: [32]" in capsys.readouterr().out


def test_lookup_uses_exact_sha_and_intent_filters():
    api = FakeApi(_build())
    _report(api)
    params = [p for path, p, _ in api.calls if path.endswith("/builds")]
    assert {"per_page": 100, "commit": SHA} in params
    assert {"per_page": 100, "meta_data[pf_intent]": INTENT} in params


def test_unreadable_answer_is_unknown(capsys):
    def broken(path, params=None, raw=False):
        raise bo.Unknown("GET x: URLError")
    assert _report(broken) == 4
    assert "VERDETTO_BUILD = UNKNOWN" in capsys.readouterr().out


def test_build_without_job_list_is_unknown(capsys):
    b = _build()
    del b["jobs"]
    assert _report(FakeApi(b)) == 4


def test_finished_build_without_timestamps_is_unknown(capsys):
    b = _build(finished=None)
    assert _report(FakeApi(b)) == 4


def test_invalid_sha_is_refused_before_any_lookup(capsys):
    api = FakeApi(_build())
    assert bo.main(["find", "--sha", "185078e4"], get=api) == 2
    assert api.calls == []


def test_whoami_flags_any_write_scope(capsys):
    assert bo.main(["whoami"], get=FakeApi(_build())) == 0
    api = FakeApi(_build(), whoami={"uuid": "u", "scopes": ["read_builds", "write_builds"]})
    assert bo.main(["whoami"], get=api) == 2
    assert "write_builds" in capsys.readouterr().err


def test_log_and_artifacts_are_read_only_lookups(capsys):
    api = FakeApi(_build())
    assert bo.main(["log", "--build", "31", "--step", "s3", "--tail", "2"], get=api) == 0
    assert capsys.readouterr().out.strip().splitlines() == ["riga2", "riga3"]
    assert bo.main(["artifacts", "--build", "31"], get=api) == 0
    assert "bk-out/unit.xml" in capsys.readouterr().out
    assert bo.main(["log", "--build", "31", "--step", "assente"], get=api) == 2


def test_source_is_get_only_with_fixed_host_and_no_secret_output():
    tree = ast.parse(SRC.read_text())
    methods = [kw.value.value for node in ast.walk(tree) if isinstance(node, ast.Call)
               for kw in node.keywords if kw.arg == "method"]
    assert methods == ["GET"]
    assert bo.API == "https://api.buildkite.com/v2" and bo.PIPELINE == "pf-dedicated-ci"
    text = SRC.read_text()
    assert "bkua_" not in text and "print(bearer" not in text and "print(os.environ" not in text


def test_http_get_never_prints_the_token(monkeypatch, capsys):
    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"[]"

    def fake_open(req, timeout):
        seen["method"], seen["auth"] = req.get_method(), req.get_header("Authorization")
        return Resp()

    monkeypatch.setenv("BUILDKITE_API_TOKEN", "fake-observability-credential-123")
    monkeypatch.setattr(bo.urllib.request, "urlopen", fake_open)
    assert bo.http_get("/x") == []
    assert seen == {"method": "GET", "auth": "Bearer fake-observability-credential-123"}
    out = capsys.readouterr()
    assert "fake-observability-credential" not in out.out + out.err


def test_whoami_path_is_the_buildkite_access_token_endpoint():
    assert bo.WHOAMI_PATH == "/access-" + "token"
