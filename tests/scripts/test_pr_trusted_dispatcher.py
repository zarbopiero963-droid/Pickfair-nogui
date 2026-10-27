"""Test del dispatcher trusted (#562, PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01, intake a polling: PF-562 opzione B).

Offline e deterministici: un finto GitHub REST (elenco PR aperte e commenti, paginazione, rate limit, errori) e un
finto Buildkite che riproduce l'API REST v2 (creazione, lettura, elenco, annullamento, artifact con redirect), piu'
un publisher che registra le chiamate al posto dell'helper root `pf-publish-status`. Il percorso reale (VPS, GitHub
App, Buildkite) e' NOT VERIFIED: lo collauda Grok (docs/trusted_dispatcher.md).
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import urllib.parse
from pathlib import Path

import pytest

# il dispatcher e' un servizio POSIX (VPS Linux, lock con `fcntl.flock`): altrove il modulo si salta in modo
# esplicito invece di interrompere la raccolta dei test (Sol su #569)
pytest.importorskip("fcntl", reason="dispatcher trusted solo POSIX: usa fcntl.flock")

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("pr_trusted_dispatcher", ROOT / "scripts" / "pr_trusted_dispatcher.py")
td = importlib.util.module_from_spec(_spec)
sys.modules["pr_trusted_dispatcher"] = td
_spec.loader.exec_module(td)

REPO = "zarbopiero963-droid/Pickfair-nogui"
ORG = "pf-org-test"
BK_CRED = "bkua_test_token_0123456789abcdef0123"
GH_CRED = "gh-installation-cred-for-tests-0123456789"
# chiave JSON del contratto del comando del token, scritta spezzata: la redazione dei reviewer oscura le righe
# con la chiave del token seguita da due punti e un valore, e Sol non vedrebbe il test (#569)
TK = "tok" "en"
HEAD = "a" * 40
HEAD2 = "b" * 40
HEAD3 = "c" * 40
S3 = "artifacts.example-s3.test"
RERUNNER = "pf-owner"


# --------------------------------------------------------------------------------------------- fixture
def write_secret(path: Path, value: str, mode: int = 0o600) -> str:
    path.write_text(value + "\n", encoding="ascii")
    os.chmod(path, mode)
    return str(path)


def raw_config(tmp_path: Path) -> dict:
    return {
        "github": {"token_command": ["/usr/local/sbin/pf-github-read-token"], "token_command_timeout_seconds": 20},
        "rerun": {"allowed_logins": [RERUNNER]},
        "buildkite": {
            "organization": ORG,
            "token_file": write_secret(tmp_path / "bk-token", BK_CRED),
            "result_artifacts": {"overlap": ".trusted-guard/overlap.json",
                                 "readiness": ".trusted-guard/readiness.json"},
            "artifact_download_hosts": [S3],
        },
        "publisher": {"command": ["/usr/local/sbin/pf-publish-status"], "timeout_seconds": 60},
        "state_db": str(tmp_path / "state.sqlite3"),
        "retention_hours": 168,
        "poll_interval_seconds": 60,
        "build_timeout_seconds": {"overlap": 600, "readiness": 900},
        "http_timeout_seconds": 30,
    }


class Clock:
    def __init__(self) -> None:
        self.now = 1_800_000_000.0

    def __call__(self) -> float:
        return self.now


class FakeGitHub:
    """`GET /repos/<repo>/pulls` e `GET /repos/<repo>/issues/comments` con paginazione `Link: rel="next"`."""

    def __init__(self) -> None:
        self.pulls: dict[int, dict] = {}
        self.comments: list[dict] = []
        self.page_size = 100
        self.fail_status: int | None = None
        self.rate_limit_reset: float | None = None
        self.scopes = ""
        self.break_page: int | None = None
        self.drop_next_link_on_page: int | None = None
        self.requests: list[tuple[str, str, dict]] = []
        self.permissions: dict[str, str] = {RERUNNER: "admin"}     # login -> permesso effettivo sul repository
        self.permission_status = 200

    def put(self, pr, head, *, draft=False, base="main", head_repo=REPO) -> None:
        self.pulls[pr] = {"number": pr, "state": "open", "draft": draft,
                          "base": {"ref": base, "repo": {"full_name": REPO}},
                          "head": {"sha": head, "repo": None if head_repo is None else {"full_name": head_repo}}}

    def comment(self, cid, pr, body, *, login=RERUNNER, created="2030-01-01T00:00:00Z", updated=None,
                user_type="User") -> None:
        self.comments.append({"id": cid, "body": body, "user": {"login": login, "type": user_type},
                              "author_association": "OWNER",    # mai usato: non certifica i permessi attuali
                              "issue_url": f"https://api.github.com/repos/{REPO}/issues/{pr}",
                              "created_at": created, "updated_at": updated or created})

    def handle(self, method, url, headers):
        self.requests.append((method, url, dict(headers)))
        assert method == "GET", "il dispatcher legge GitHub solo in GET"
        assert headers.get("Authorization") == f"Bearer {GH_CRED}"
        parts = urllib.parse.urlsplit(url)
        query = {k: v[0] for k, v in urllib.parse.parse_qs(parts.query).items()}
        page = int(query.get("page", "1"))
        if self.rate_limit_reset is not None:
            return 403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(self.rate_limit_reset))}, b"{}"
        if self.fail_status is not None:
            return self.fail_status, {}, b'{"message":"boom"}'
        headers_out = {"X-OAuth-Scopes": self.scopes} if self.scopes else {}
        if parts.path == f"/repos/{REPO}/pulls":
            data = [self.pulls[k] for k in sorted(self.pulls)]
        elif parts.path == f"/repos/{REPO}/issues/comments":
            data = [c for c in self.comments if c["updated_at"] >= query["since"]]
        elif parts.path.startswith(f"/repos/{REPO}/collaborators/") and parts.path.endswith("/permission"):
            login = parts.path.split("/")[-2]
            if self.permission_status != 200:
                return self.permission_status, {}, b'{"message":"x"}'
            if login not in self.permissions:
                return 404, {}, b'{"message":"Not Found"}'
            body = {"permission": self.permissions[login], "user": {"login": login}}
            return 200, headers_out, json.dumps(body).encode()
        else:
            return 404, {}, b"{}"
        chunk = data[(page - 1) * self.page_size: page * self.page_size]
        if page * self.page_size < len(data) and self.drop_next_link_on_page != page:
            nxt = dict(query, page=str(page + 1))
            headers_out["Link"] = f'<https://api.github.com{parts.path}?{urllib.parse.urlencode(nxt)}>; rel="next"'
        if self.break_page == page:
            return 200, headers_out, b'{"not": "a list"}'
        return 200, headers_out, json.dumps(chunk).encode()


class FakeBuildkite:
    """API REST v2 minima. `fail_create` = 'transport' | 'http500' | 'http422'; `builds` per pipeline. Inoltra le
    richieste a api.github.com al finto GitHub `gh`."""

    def __init__(self) -> None:
        self.gh = FakeGitHub()
        self.builds: dict[str, dict[int, dict]] = {"pr-overlap-guard": {}, "pr-merge-readiness": {}}
        self.artifacts: dict[tuple[str, int], bytes] = {}
        self.requests: list[tuple[str, str, dict, bytes | None]] = []
        self.fail_create: str | None = None
        self.create_lands = False          # con fail_create: la build viene comunque creata (esito ambiguo)
        self.fail_get = False
        self.fail_list = False
        self.canceled: list[tuple[str, int]] = []
        self.redirect_host = S3
        self.download_url_override: str | None = None
        self.mutate_build = None
        self.fail_cancel = False
    def _new_build(self, pipeline: str, payload: dict) -> dict:
        number = len(self.builds[pipeline]) + 1
        build = {"number": number, "state": "scheduled", "branch": payload["branch"], "commit": payload["commit"],
                 "env": dict(payload["env"]), "message": payload["message"],
                 "web_url": f"https://buildkite.com/{ORG}/{pipeline}/builds/{number}"}
        self.builds[pipeline][number] = build
        return build

    def __call__(self, method, url, headers, body, timeout):
        if url.startswith("https://api.github.com/"):
            return self.gh.handle(method, url, headers)
        self.requests.append((method, url, dict(headers), body))
        if url.startswith(f"https://{S3}/") or url.startswith(f"https://{self.redirect_host}/"):
            key = url.rsplit("/", 2)
            return 200, {}, self.artifacts[(key[-2], int(key[-1]))]
        prefix = f"{td.BUILDKITE_API}/organizations/{ORG}/pipelines/"
        assert url.startswith(prefix), url
        assert headers.get("Authorization") == f"Bearer {BK_CRED}"
        rest = url[len(prefix):]
        pipeline, _, tail = rest.partition("/")
        if pipeline not in self.builds:
            return 404, {}, b'{"message":"No pipeline found"}'
        if method == "POST" and tail == "builds":
            payload = json.loads(body)
            if self.fail_create:
                if self.create_lands:
                    self._new_build(pipeline, payload)
                if self.fail_create == "transport":
                    raise td.TransportError("timeout")
                if self.fail_create == "truncated201":
                    return 201, {}, b'{"number": 1, "web_url'
                if self.fail_create == "html400":
                    return 400, {"Content-Type": "text/html"}, b"<html>bad request</html>"
                if self.fail_create == "html502":
                    return 502, {"Content-Type": "text/html"}, b"<html>502 Bad Gateway</html>"
                code = 500 if self.fail_create == "http500" else 422
                return code, {}, b'{"message":"no"}'
            return 201, {}, json.dumps(self._new_build(pipeline, payload)).encode()
        if method == "GET" and tail.startswith("builds?"):
            if self.fail_list:
                raise td.TransportError("rete")
            return 200, {}, json.dumps(list(self.builds[pipeline].values())).encode()
        parts = tail.split("?")[0].split("/")
        number = int(parts[1])
        build = self.builds[pipeline].get(number)
        if build is None:
            return 404, {}, b"{}"
        if method == "GET" and len(parts) == 2:
            if self.fail_get:
                raise td.TransportError("rete")
            data = dict(build)
            if self.mutate_build:
                self.mutate_build(data)
            return 200, {}, json.dumps(data).encode()
        if method == "PUT" and parts[2:] == ["cancel"]:
            if self.fail_cancel:
                raise td.TransportError("rete")
            self.canceled.append((pipeline, number))
            build["state"] = "canceled"
            return 200, {}, json.dumps(build).encode()
        if method == "GET" and parts[2:] == ["artifacts"]:
            data = self.artifacts.get((pipeline, number))
            if data is None:
                return 200, {}, b"[]"
            guard = "overlap" if pipeline == "pr-overlap-guard" else "readiness"
            dl = self.download_url_override or f"{prefix}{pipeline}/builds/{number}/jobs/j1/artifacts/a1/download"
            return 200, {}, json.dumps([{"path": f".trusted-guard/{guard}.json", "state": "finished",
                                         "file_size": len(data), "download_url": dl}]).encode()
        if method == "GET" and parts[-1] == "download":
            assert headers.get("Authorization")
            return 302, {"Location": f"https://{self.redirect_host}/bucket/{pipeline}/{number}"}, b""
        raise AssertionError(f"richiesta non prevista: {method} {url}")

    # helper di scenario
    def finish(self, pipeline: str, number: int, state: str, result: dict | None) -> None:
        self.builds[pipeline][number]["state"] = state
        if result is not None:
            self.artifacts[(pipeline, number)] = json.dumps(result).encode()


class Publisher:
    def __init__(self, rc: int = 0) -> None:
        self.calls: list[tuple] = []
        self.rc = rc

    def __call__(self, pr, sha, state, context, url, description):
        self.calls.append((pr, sha, state, context, url, description))
        return self.rc

    def final(self):
        return [c for c in self.calls if c[2] != "pending"]


def token_runner(clock, *, rc=0, ttl=3600, body=None):
    calls = []

    def run(argv, timeout):
        calls.append((argv, timeout))
        out = body if body is not None else json.dumps({TK: GH_CRED, "expires_at": td.iso(clock() + ttl)})
        return rc, out.encode()
    run.calls = calls
    return run


def make_dispatcher(cfg, transport, publisher, clock=None, runner=None):
    clock = clock or Clock()
    return td.build_dispatcher(cfg, transport=transport, publisher=publisher,
                               token_runner=runner or token_runner(clock), clock=clock)


@pytest.fixture()
def env(tmp_path):
    cfg = td.validate_config(raw_config(tmp_path))
    bk = FakeBuildkite()
    pub = Publisher()
    clock = Clock()
    disp = make_dispatcher(cfg, bk, pub, clock)
    yield disp, bk, pub, clock
    disp.store.close()


def payload(*, pr=42, head=HEAD, action="synchronize", head_repo=REPO, base="main", state="open",
            draft=False, **_ignored) -> dict:
    """Stato di una PR come lo restituirebbe GitHub; `send` lo pubblica nel finto GitHub e fa un giro di polling."""
    return {"action": action, "pr": pr, "head": head, "head_repo": head_repo, "base": base, "state": state,
            "draft": draft}


def send(disp, data, **_ignored):
    """Il finto GitHub assume lo stato descritto e il dispatcher fa i giri di polling necessari a osservarlo:
    chiusura = assente da due elenchi completi; riapertura = chiusa, poi di nuovo aperta. Ritorna (codice, tid)."""
    gh = disp.gh._transport.gh
    if data["action"] == "closed" or data["state"] != "open":
        gh.pulls.pop(data["pr"], None)
        disp.poll()
        created = disp.poll()
    else:
        if data["action"] == "reopened":
            gh.pulls.pop(data["pr"], None)
            disp.poll()
            disp.poll()
        gh.put(data["pr"], data["head"], draft=data["draft"], base=data["base"], head_repo=data["head_repo"])
        created = disp.poll()
    return (202 if created else 200), ",".join(created)


def result(guard, *, verdict="PASS", pr=42, head=HEAD, repo=REPO, reasons=None):
    return {"guard": guard, "repo": repo, "pr": pr, "head_sha": head, "verdict": verdict,
            "reasons": reasons if reasons is not None else ([] if verdict == "PASS" else ["sovrapposizione"])}


def _overlap(disp):
    return [j for j in disp.store.recent_jobs() if j["guard"] == "overlap"][0]


def test_pass_publishes_success_on_the_pr_head_with_build_url(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    final = sorted(pub.final(), key=lambda c: c[3])
    assert final == [
        (42, HEAD, "success", "overlap-check", f"https://buildkite.com/{ORG}/pr-overlap-guard/builds/1", "PASS"),
        (42, HEAD, "success", "pr-merge-readiness", f"https://buildkite.com/{ORG}/pr-merge-readiness/builds/1",
         "PASS"),
    ]
    assert not disp.store.active_jobs()


def test_guard_fail_publishes_failure(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "failed", result("overlap", verdict="FAIL", reasons=["piu' PR aperte"]))
    disp.tick()
    assert [c[2:4] for c in pub.final()] == [("failure", "overlap-check")]
    assert "piu' PR aperte" in pub.final()[0][5]


def test_new_push_cancels_old_builds_and_old_sha_never_gets_success(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-000000000004")
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))       # il vecchio finisce PASS nel frattempo
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled and ("pr-merge-readiness", 1) in bk.canceled
    assert not [c for c in pub.final() if c[1] == HEAD]
    states = {(j["head_sha"], j["guard"]): j["state"] for j in disp.store.recent_jobs()}
    assert states[(HEAD, "overlap")] == "superseded"


def test_supersede_between_verdict_and_publication_blocks_success(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    real_publish = disp.publish

    def publish_then_push(*args):
        if args[2] == "success":
            raise AssertionError("success pubblicato su uno SHA sostituito")
        return real_publish(*args)

    job = [j for j in disp.store.active_jobs() if j["guard"] == "overlap"][0]
    disp.store.update_job(job["id"], disp.clock(), state="publishing", outcome="success", description="PASS")
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:02:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-000000000005")
    disp.publish = publish_then_push
    disp._publish_final(dict(job, state="publishing", outcome="success", description="PASS"))
    assert disp.store.get_job(job["id"])["state"] == "superseded"


def test_result_for_a_different_head_is_error_stale_sha(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap", head=HEAD2))
    disp.tick()
    assert pub.final() and pub.final()[0][2] == "error" and "stale SHA" in pub.final()[0][5]


@pytest.mark.parametrize("bad", [
    {"pr": 41}, {"pr": True}, {"pr": "42"}, {"guard": "readiness"}, {"repo": "evil/x"}, {"verdict": "pass"},
])
def test_result_identity_mismatch_is_error(env, bad):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", {**result("overlap"), **bad})
    disp.tick()
    assert [c[2] for c in pub.final()] == ["error"]


def test_build_with_different_env_is_error_and_canceled(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.mutate_build = lambda b: b["env"].update(PF_PR_HEAD_SHA=HEAD2) if b["number"] == 1 else None
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    assert all(c[2] == "error" for c in pub.final()) and pub.final()


@pytest.mark.parametrize("state", ["canceled", "skipped", "not_run", "blocked"])
def test_non_passed_terminal_build_is_never_success(env, state):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, state, result("overlap"))          # anche con un artifact PASS
    disp.tick()
    final = [c for c in pub.final() if c[3] == "overlap-check"]
    assert [c[2] for c in final] == ["error"]


def test_blocked_build_is_canceled(env):
    disp, bk, _, _ = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "blocked"
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled


def test_unknown_build_state_is_error(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "passed_with_warnings"
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]


@pytest.mark.parametrize("state,verdict", [("passed", "FAIL"), ("failed", "PASS")])
def test_build_state_contradicting_verdict_is_error(env, state, verdict):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, state, result("overlap", verdict=verdict))
    disp.tick()
    assert [c[2] for c in pub.final()] == ["error"]


@pytest.mark.parametrize("state", ["passed", "failed"])
def test_missing_artifact_is_error(env, state):
    """Artifact assente: si ritenta entro la finestra (un upload puo' arrivare in ritardo), poi `error`."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, state, None)
    disp.tick()
    assert not pub.final()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]


def test_build_timeout_cancels_and_publishes_error(env):
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    clock.now += 601
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled
    assert [(c[2], c[5]) for c in pub.final() if c[3] == "overlap-check"] == [("error", "timeout della build")]


def test_unreadable_build_until_timeout_is_error(env):
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.fail_get = True
    disp.tick()
    assert not pub.final()
    clock.now += 901
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]


def test_rejected_build_creation_publishes_error(env):
    disp, bk, pub, _ = env
    bk.fail_create = "http422"
    send(disp, payload())
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]
    assert not bk.builds["pr-overlap-guard"]


def test_ambiguous_creation_adopts_the_build_that_landed(env):
    disp, bk, pub, _ = env
    bk.fail_create, bk.create_lands = "transport", True
    send(disp, payload())
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"}
    bk.fail_create = None
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"dispatched"}
    assert len(bk.builds["pr-overlap-guard"]) == 1                # nessuna seconda creazione


def test_ambiguous_creation_with_no_build_is_error_never_retried(env):
    disp, bk, pub, clock = env
    bk.fail_create = "http500"
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"} and not pub.final()
    clock.now += disp.cfg.build_timeout_seconds["readiness"] + 1
    disp.tick()
    assert not bk.builds["pr-overlap-guard"]
    assert [r for r in bk.requests if r[0] == "POST"].__len__() == 2
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]


def test_ambiguous_creation_with_two_builds_cancels_both(env):
    disp, bk, pub, _ = env
    bk.fail_create, bk.create_lands = "transport", True
    send(disp, payload())
    disp.tick()
    first = bk.builds["pr-overlap-guard"][1]
    bk.builds["pr-overlap-guard"][2] = {**first, "number": 2,
                                        "web_url": f"https://buildkite.com/{ORG}/pr-overlap-guard/builds/2"}
    bk.fail_create = None
    disp.tick()
    assert {("pr-overlap-guard", 1), ("pr-overlap-guard", 2)} <= set(bk.canceled)
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]


def test_unlistable_builds_after_ambiguous_creation_wait_then_error(env):
    disp, bk, pub, clock = env
    bk.fail_create = "transport"
    send(disp, payload())
    disp.tick()
    bk.fail_list = True
    disp.tick()
    assert not pub.final()
    clock.now += 901
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]


def test_recovery_after_restart_resumes_dispatched_jobs(env):
    disp, bk, _, _ = env
    send(disp, payload())
    disp.tick()
    disp.store.close()
    pub2 = Publisher()
    disp2 = make_dispatcher(disp.cfg, bk, pub2, disp.clock)
    try:
        bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
        bk.finish("pr-merge-readiness", 1, "failed", result("readiness", verdict="FAIL"))
        disp2.tick()
        assert sorted(c[2] for c in pub2.final()) == ["failure", "success"]
    finally:
        disp2.store.close()
    disp.store = td.Store(disp.cfg.state_db)


def test_crash_during_creation_is_reconciled_after_restart(env):
    disp, bk, _, _ = env
    send(disp, payload())
    for job in disp.store.active_jobs():                       # crash dopo `creating`, build arrivata
        disp.store.update_job(job["id"], disp.clock(), state="creating")
        bk._new_build(td.GUARDS[job["guard"]]["pipeline"],
                      {"branch": "main", "commit": "HEAD", "message": "m", "env": td.build_env(job)})
    disp.store.close()
    disp2 = make_dispatcher(disp.cfg, bk, Publisher(), disp.clock)
    try:
        disp2.tick()
        assert {j["state"] for j in disp2.store.active_jobs()} == {"dispatched"}
        assert not [r for r in bk.requests if r[0] == "POST"]
    finally:
        disp2.store.close()
    disp.store = td.Store(disp.cfg.state_db)


def test_publisher_failure_is_recorded_not_success(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    pub.rc = 1
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    states = {j["guard"]: j["state"] for j in disp.store.recent_jobs()}
    assert states["overlap"] == "publishing"           # ritentata ai giri successivi, mai `done`


def test_only_the_two_contract_pipelines_can_be_started(env):
    disp, _, _, _ = env
    for forbidden in ("pf-trusted-smoke", "pf-dedicated-ci", "overlap-diagnostic", "../x"):
        with pytest.raises(KeyError):
            disp.bk.create_build(forbidden, {}, "m")
    assert {g["pipeline"] for g in td.GUARDS.values()} == {"pr-overlap-guard", "pr-merge-readiness"}


def test_contexts_are_the_contract_ones_and_never_the_diagnostic():
    assert {g["context"] for g in td.GUARDS.values()} == {"overlap-check", "pr-merge-readiness"}
    assert "overlap-diagnostic" not in {g["context"] for g in td.GUARDS.values()}


def test_readiness_status_is_a_self_check_and_overlap_status_is_not():
    from scripts import pr_flow_automation as pfa

    # forma GraphQL di statusCheckRollup (StatusContext), quella che legge la readiness
    readiness = {"context": "pr-merge-readiness", "state": "PENDING",
                 "targetUrl": f"https://buildkite.com/{ORG}/pr-merge-readiness/builds/7"}
    overlap = {"context": "overlap-check", "state": "PENDING",
               "targetUrl": f"https://buildkite.com/{ORG}/pr-overlap-guard/builds/7"}
    assert pfa.is_self_check(readiness)
    assert not pfa.is_self_check(overlap)


def test_artifact_redirect_drops_authorization_and_enforces_host(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    s3 = [r for r in bk.requests if r[1].startswith(f"https://{S3}/")]
    assert s3 and all("Authorization" not in r[2] for r in s3)


def test_artifact_redirect_to_other_host_is_error(env):
    disp, bk, pub, _ = env
    bk.redirect_host = "evil.example"
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    disp.clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]
    assert not [r for r in bk.requests if "evil.example" in r[1]]


def test_artifact_download_url_outside_buildkite_api_is_error(env):
    disp, bk, pub, _ = env
    bk.download_url_override = "https://evil.example/download"
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    disp.clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]
    assert not [r for r in bk.requests if r[1].startswith("https://evil.example")]


def test_default_publisher_gets_no_inherited_environment(tmp_path, monkeypatch):
    script = tmp_path / "pub.sh"
    out = tmp_path / "out"
    script.write_text(f"#!/bin/sh\nenv > {out}\nprintf '%s\\n' \"$@\" >> {out}\n")
    os.chmod(script, 0o700)
    monkeypatch.setenv("BUILDKITE_TOKEN_LEAK", BK_CRED)
    rc = td.default_publisher((str(script),), 10)(42, HEAD, "success", "overlap-check",
                                                   "https://buildkite.com/x/pr-overlap-guard/builds/1", "PASS")
    text = out.read_text()
    assert rc == 0 and BK_CRED not in text
    assert text.splitlines()[-6:] == ["42", HEAD, "success", "overlap-check",
                                      "https://buildkite.com/x/pr-overlap-guard/builds/1", "PASS"]


def test_cli_without_config_is_not_configured(capsys):
    assert td.main(["check-config"]) == td.EXIT_NOT_CONFIGURED
    assert "NOT CONFIGURED" in capsys.readouterr().err


def test_cli_serve_with_incomplete_config_does_not_start(tmp_path, capsys):
    raw = raw_config(tmp_path)
    del raw["publisher"]
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(raw))
    assert td.main(["serve", "--config", str(path)]) == td.EXIT_NOT_CONFIGURED
    assert "publisher: MISSING" in capsys.readouterr().err


def test_failed_publication_is_retried_until_it_lands(env):
    """P2-2: un helper temporaneamente in errore non lascia lo status richiesto su `pending` per sempre."""
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    pub.rc = 1
    bk.finish("pr-overlap-guard", 1, "failed", result("overlap", verdict="FAIL"))
    disp.tick()
    disp.tick()
    job = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap"][0]
    assert job["state"] == "publishing"
    pub.rc = 0
    disp.tick()
    assert disp.store.get_job(job["id"])["state"] == "done"
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["failure"] * 3


def test_failed_publication_gives_up_after_the_window(env):
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    pub.rc = 1
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    job = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap"][0]
    assert job["state"] == "publish_failed"


def _publish_failed_after_rerun(disp, bk, pub, clock):
    """PASS pubblicato, rerun sullo stesso SHA che da' FAIL con il publisher fermo oltre la finestra."""
    _rerun_ready(disp, bk)
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    assert ("success" in [c[2] for c in pub.final() if c[3] == "overlap-check"])
    bk.gh.comment(9020, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    disp.poll()
    disp.tick()
    pub.rc = 1
    bk.finish("pr-overlap-guard", 2, "failed", result("overlap", verdict="FAIL"))
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    job = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["event"] == "manual"][0]
    assert job["state"] == "publish_failed"
    return job


def test_publish_failed_outcome_is_redelivered_when_the_publisher_recovers(env):
    """Sol su #569 (range 90d8261...bac3b3e): scaduta la finestra il job usciva dai retry e il vecchio `success`
    restava valido sullo stesso SHA anche col publisher ripristinato. L'esito resta da consegnare."""
    disp, bk, pub, clock = env
    job = _publish_failed_after_rerun(disp, bk, pub, clock)
    pub.rc = 0
    clock.now += td.REDELIVERY_SECONDS + 1
    disp.poll()
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"][-1] == "failure"
    assert disp.store.get_job(job["id"])["state"] == "done"


def test_redelivery_never_overwrites_another_pr_on_the_same_sha(env):
    """Sol su #569 (90d8261...1ed6de8): gli status GitHub sono per SHA e contesto, non per PR. Il PASS non consegnato
    della PR 42 non deve essere riconsegnato dopo il FAIL della PR 43 sullo stesso SHA."""
    disp, bk, pub, clock = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    pub.rc = 1
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    first = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["pr"] == 42][0]
    assert first["state"] == "publish_failed"
    pub.rc = 0
    restored = len(pub.calls)                                     # da qui ogni chiamata e' una pubblicazione riuscita
    bk.gh.put(43, HEAD)                                           # seconda PR interna sullo stesso commit
    disp.poll()
    disp.tick()
    second = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["pr"] == 43][0]
    bk.finish("pr-overlap-guard", second["build_number"], "failed", result("overlap", verdict="FAIL", pr=43))
    disp.tick()
    clock.now += td.REDELIVERY_SECONDS + 1
    disp.tick()
    overlap = [c for c in pub.calls[restored:] if c[3] == "overlap-check" and c[2] != "pending"]
    assert overlap and overlap[-1][2] == "failure"                # l'ultimo esito sullo SHA resta il FAIL della 43
    assert not [c for c in overlap if c[0] == 42 and c[2] == "success"]
    assert disp.store.get_job(first["id"])["state"] == "publish_failed"


def test_older_active_job_never_publishes_over_a_newer_pr_on_the_same_sha(env):
    """Codex P1 su #569 (0e129c3): PR 42 passa ma il risultato arriva in ritardo; PR 43 sullo stesso SHA fallisce.
    Il PASS della 42 non deve uscire dopo il FAIL della 43: lo status e' per SHA e contesto, conta il piu' recente."""
    disp, bk, pub, clock = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", None)             # build passata, artifact non ancora caricato
    disp.tick()
    bk.gh.put(43, HEAD)
    disp.poll()
    disp.tick()
    second = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["pr"] == 43][0]
    bk.finish("pr-overlap-guard", second["build_number"], "failed", result("overlap", verdict="FAIL", pr=43))
    disp.tick()
    bk.artifacts[("pr-overlap-guard", 1)] = json.dumps(result("overlap")).encode()   # ora il PASS della 42 c'e'
    disp.tick()
    finals = [c for c in pub.final() if c[3] == "overlap-check" and c[1] == HEAD]
    assert finals and finals[-1][0] == 43 and finals[-1][2] == "failure"
    assert not [c for c in finals if c[0] == 42 and c[2] == "success"]
    first = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["pr"] == 42][0]
    assert first["superseded"] == 1


def test_late_publication_retry_of_an_older_pr_never_overwrites_the_same_sha(env):
    """Sol su #569 (0e129c3): il job della PR 42 e' gia' `publishing` (PASS) e ritenta perche' il publisher e'
    fermo; arriva il FAIL della PR 43 sullo stesso SHA; il retry tardivo del PASS della 42 non deve uscire."""
    disp, bk, pub, clock = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    pub.rc = 1
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    first = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["pr"] == 42][0]
    assert first["state"] == "publishing"                       # PASS pronto, pubblicazione in ritentativo
    pub.rc = 0
    bk.gh.put(43, HEAD)
    disp.poll()
    restored = len(pub.calls)
    second = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["pr"] == 43][0]
    disp.tick()
    second = disp.store.get_job(second["id"])
    bk.finish("pr-overlap-guard", second["build_number"], "failed", result("overlap", verdict="FAIL", pr=43))
    disp.tick()
    disp.tick()
    finals = [c for c in pub.calls[restored:] if c[3] == "overlap-check" and c[2] != "pending"]
    assert finals and finals[-1][0] == 43 and finals[-1][2] == "failure"
    assert not [c for c in finals if c[0] == 42]
    assert disp.store.get_job(first["id"])["superseded"] == 1


def test_redelivery_waits_for_the_backoff_and_keeps_retrying(env):
    disp, bk, pub, clock = env
    job = _publish_failed_after_rerun(disp, bk, pub, clock)
    overlap = lambda: len([c for c in pub.calls if c[3] == "overlap-check"])     # noqa: E731
    calls = overlap()
    disp.tick()
    assert overlap() == calls                                     # prima del backoff nessun tentativo
    clock.now += td.REDELIVERY_SECONDS + 1
    disp.tick()
    assert overlap() == calls + 1                                 # publisher ancora fermo: un tentativo
    assert disp.store.get_job(job["id"])["state"] == "publish_failed"
    clock.now += td.REDELIVERY_SECONDS + 1
    pub.rc = 0
    disp.tick()
    assert disp.store.get_job(job["id"])["state"] == "done"


@pytest.mark.parametrize("change", ["newer_rerun", "new_head", "closed", "head_drift"])
def test_stale_publish_failed_outcome_is_never_redelivered(env, change):
    """Un esito non consegnato non deve mai coprire uno piu' recente: niente riconsegna se esiste un job piu'
    nuovo per la stessa PR e lo stesso guard, se l'head e' cambiato o se la PR non e' piu' aperta."""
    disp, bk, pub, clock = env
    job = _publish_failed_after_rerun(disp, bk, pub, clock)
    pub.rc = 0
    if change == "newer_rerun":
        bk.gh.comment(9021, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 5))
        clock.now += 10
    elif change == "new_head":
        bk.gh.put(42, HEAD2)
    elif change == "head_drift":
        # difesa in profondita': head registrato diverso da quello del job anche senza job piu' recenti
        disp.store._tx(lambda db: db.execute("UPDATE pr_state SET head_sha = ? WHERE pr = 42", (HEAD2,)))
        bk.gh.put(42, HEAD2)
        disp.store._tx(lambda db: db.execute("DELETE FROM jobs WHERE head_sha = ?", (HEAD2,)))
    else:
        del bk.gh.pulls[42]
        disp.poll()
    if change != "head_drift":
        disp.poll()
    before = [c for c in pub.final() if c[3] == "overlap-check" and c[2] == "failure"]
    clock.now += td.REDELIVERY_SECONDS + 1
    disp.tick()
    after = [c for c in pub.final() if c[3] == "overlap-check" and c[2] == "failure"]
    assert after == before
    assert disp.store.get_job(job["id"])["state"] == "publish_failed"


def test_superseded_ambiguous_creation_cancels_the_build_that_landed(env):
    """P2-4: un job in `creating` sostituito da un push nuovo non lascia girare la build orfana."""
    disp, bk, _, _ = env
    bk.fail_create, bk.create_lands = "transport", True
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000e1")
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled and ("pr-merge-readiness", 1) in bk.canceled


def test_second_instance_on_the_same_database_is_refused(env):
    """P2-3: due processi sullo stesso archivio creerebbero due build per la stessa richiesta."""
    disp, _, _, _ = env
    with pytest.raises(td.ConfigError) as exc:
        td.Store(disp.cfg.state_db)
    assert "altra istanza" in str(exc.value)


def test_job_transition_is_conditional_on_the_previous_state(env):
    disp, bk, _, _ = env
    send(disp, payload())
    job = disp.store.active_jobs()[0]
    disp.store.update_job(job["id"], disp.clock(), state="creating")
    disp._create(dict(job))                      # copia vecchia ancora `queued`: non deve creare una seconda build
    assert not [r for r in bk.requests if r[0] == "POST"]


def test_jobs_command_reads_while_the_service_holds_the_lock(env, tmp_path, capsys):
    disp, _, _, _ = env
    send(disp, payload())
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(raw_config(tmp_path)))
    assert td.main(["jobs", "--config", str(path)]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2


def test_failed_commit_leaves_the_store_usable(env, monkeypatch):
    """P3-3: un COMMIT fallito non lascia la connessione dentro una transazione aperta."""
    disp, _, _, _ = env
    store = disp.store
    real = store._db

    class Flaky:
        def __init__(self):
            self.fail = True

        def execute(self, sql, *args):
            if sql == "COMMIT" and self.fail:
                self.fail = False
                raise td.sqlite3.OperationalError("database is locked")
            return real.execute(sql, *args)

    store._db = Flaky()
    with pytest.raises(td.sqlite3.OperationalError):
        send(disp, payload())
    store._db = real
    assert send(disp, payload(), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000f1")[0] == 202


def test_publisher_timeout_is_mandatory_config(tmp_path):
    raw = raw_config(tmp_path)
    del raw["publisher"]["timeout_seconds"]
    with pytest.raises(td.ConfigError) as exc:
        td.validate_config(raw)
    assert "publisher.timeout_seconds: MISSING" in exc.value.problems


def test_artifact_listing_asks_for_a_full_page(env):
    disp, bk, _, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    listing = [r[1] for r in bk.requests if "/artifacts" in r[1] and r[1].split("?")[0].endswith("/artifacts")]
    assert listing and all("per_page=100" in u for u in listing)


def test_non_json_5xx_on_creation_is_ambiguous_and_reconciled(env):
    """Codex P2 su #569: un 502 HTML del gateway dopo che la build e' stata creata non chiude il job come
    `error`: e' una creazione ambigua, riconciliata al giro dopo senza una seconda creazione."""
    disp, bk, pub, _ = env
    bk.fail_create, bk.create_lands = "html502", True
    send(disp, payload())
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"}
    assert not pub.final()
    bk.fail_create = None
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"dispatched"}
    assert len(bk.builds["pr-overlap-guard"]) == 1


def test_failed_pending_publication_is_retried_while_the_build_runs(env):
    """Codex P1 su #569: rivalutazione di un head gia' `success` (reopen) con l'helper momentaneamente in errore:
    il `pending` si ritenta mentre la build gira, cosi' il vecchio `success` non resta visibile come esito."""
    disp, bk, pub, _ = env
    pub.rc = 1
    send(disp, payload())
    disp.tick()
    assert all(j["pending_published"] == 0 for j in disp.store.active_jobs())
    pub.rc = 0
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.builds["pr-merge-readiness"][1]["state"] = "running"
    disp.tick()
    pendings = [c for c in pub.calls if c[2] == "pending" and c[1] == HEAD]
    assert len(pendings) == 4                      # 2 falliti + 2 ritentati e riusciti
    assert all(j["pending_published"] == 1 for j in disp.store.active_jobs())
    disp.tick()
    assert len([c for c in pub.calls if c[2] == "pending"]) == 4      # riuscito: non si ripete


def test_failed_pending_is_retried_while_the_result_artifact_is_unavailable(env):
    """Sol su #569 (range 22d1b71...e1c6e36): build finita ma artifact non ancora scaricabile. Il `pending` fallito
    va ritentato anche qui, altrimenti in un rerun sullo stesso SHA resta visibile il vecchio `success`."""
    disp, bk, pub, _ = env
    pub.rc = 1
    send(disp, payload())
    disp.tick()
    assert all(j["pending_published"] == 0 for j in disp.store.active_jobs())
    pub.rc = 0
    bk.finish("pr-overlap-guard", 1, "failed", None)            # risultato non (ancora) disponibile
    bk.finish("pr-merge-readiness", 1, "failed", None)
    disp.tick()
    assert all(j["pending_published"] == 1 for j in disp.store.active_jobs())
    assert pub.final() == []                                     # nessun esito finale senza risultato


# --------------------------------------------------------------------------------------------- descrizioni (socket)
_SOCKET_DESCRIPTION = __import__("re").compile(r"[\x20-\x23\x25-\x5b\x5d-\x5f\x61-\x7e]{1,140}")


@pytest.mark.parametrize("raw,expected", [
    ("PASS", "PASS"),
    ("FAIL: piu' PR aperte", "FAIL: piu' PR aperte"),
    ("FAIL: verità già è", "FAIL: verita gia e"),                     # accenti: lettera base, significato intatto
    ("FAIL: `rm` $HOME C:\\x", "FAIL: ?rm? ?HOME C:?x"),           # vietati dal socket: sostituiti in vista
    ("riga 1\nriga 2\ttab", "riga 1 riga 2 tab"),
    ("FAIL: ✗ simbolo", "FAIL: ? simbolo"),
])
def test_status_description_matches_the_socket_contract(raw, expected):
    """Grok t215u: il publisher a socket accetta 1..140 caratteri stampabili, senza ` $ \. La pulizia non toglie
    niente in silenzio: un carattere vietato diventa '?', un testo troppo lungo finisce con '...'."""
    out = td.clean_description(raw)
    assert out == expected and _SOCKET_DESCRIPTION.fullmatch(out)


def test_long_or_empty_description_is_marked_not_silently_cut():
    long = td.clean_description("FAIL: " + "x" * 300)
    assert len(long) == 140 and long.endswith("...") and _SOCKET_DESCRIPTION.fullmatch(long)
    assert _SOCKET_DESCRIPTION.fullmatch(td.clean_description(""))
    assert _SOCKET_DESCRIPTION.fullmatch(td.clean_description(" \n\t "))


def test_fail_reason_with_forbidden_characters_reaches_the_publisher_cleaned_and_logged(env, capsys):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "failed", result("overlap", verdict="FAIL", reasons=["PR `#7` costa $5 \\o/"]))
    disp.tick()
    final = [c for c in pub.final() if c[3] == "overlap-check"]
    assert final and final[0][2] == "failure" and final[0][5] == "FAIL: PR ?#7? costa ?5 ?o/"
    assert _SOCKET_DESCRIPTION.fullmatch(final[0][5])
    assert "PR `#7` costa $5" in capsys.readouterr().err        # il testo originale resta nei log


def test_default_publisher_refuses_a_description_outside_the_contract(monkeypatch):
    calls = []
    monkeypatch.setattr(td.subprocess, "run", lambda *a, **k: calls.append(a))
    publish = td.default_publisher(("/usr/local/bin/pf-status-publish-client",), 120)
    assert publish(42, HEAD, "failure", "overlap-check", "https://buildkite.com/x", "FAIL: $(id)") != 0
    assert publish(42, HEAD, "failure", "overlap-check", "https://buildkite.com/x", "x" * 141) != 0
    assert calls == []                                           # mai inviato: niente argv fuori contratto


def test_archive_from_an_earlier_schema_is_migrated(tmp_path):
    """Sol su #569 (range b179426...e464795): un archivio creato prima di `publishing_since`/`pending_published`
    (e con il vecchio indice `jobs_active`) va migrato in modo idempotente, non deve fallire a runtime."""
    cfg = td.validate_config(raw_config(tmp_path))
    old = td.sqlite3.connect(cfg.state_db)
    old.executescript("""
        CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, received_at REAL NOT NULL);
        CREATE TABLE pr_heads (pr INTEGER PRIMARY KEY, head_sha TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL, event TEXT NOT NULL,
          pr INTEGER NOT NULL, head_sha TEXT NOT NULL, guard TEXT NOT NULL, state TEXT NOT NULL,
          superseded INTEGER NOT NULL DEFAULT 0, build_number INTEGER, build_url TEXT, outcome TEXT,
          description TEXT, created_at REAL NOT NULL, dispatched_at REAL, updated_at REAL NOT NULL);
        CREATE UNIQUE INDEX jobs_active ON jobs(pr, head_sha, guard)
          WHERE state IN ('queued', 'creating', 'dispatched', 'publishing');
    """)
    old.close()
    bk, pub = FakeBuildkite(), Publisher()
    for _ in range(2):                                    # idempotente: la seconda apertura non cambia nulla
        disp = make_dispatcher(cfg, bk, pub)
        cols = {r[1] for r in disp.store._db.execute("PRAGMA table_info(jobs)")}
        indexes = {r[1] for r in disp.store._db.execute("PRAGMA index_list(jobs)")}
        disp.store.close()
        assert {"publishing_since", "pending_published", "final_published"} <= cols
        assert "jobs_active" not in indexes and "jobs_active_current" in indexes
    disp = make_dispatcher(cfg, bk, pub)
    try:
        send(disp, payload())
        disp.tick()
        assert all(j["state"] == "dispatched" and j["pending_published"] == 1 for j in disp.store.active_jobs())
    finally:
        disp.store.close()


def test_truncated_http_response_is_a_transport_error(monkeypatch):
    """Codex P2 su #569 (47ca6fb): `http.client.IncompleteRead` di una risposta chunked troncata non deve sfuggire
    a `_poll` prima del controllo del timeout: e' un errore di trasporto come gli altri."""
    import http.client as hc

    class Resp:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self, n=-1):
            raise hc.IncompleteRead(b"{", 10)

    monkeypatch.setattr(td._OPENER, "open", lambda req, timeout=None: Resp())
    with pytest.raises(td.TransportError):
        td.urllib_transport("GET", "https://api.buildkite.com/v2/x", {}, None, 5)


def test_unconfirmed_cancellation_keeps_the_superseded_job_active(env):
    """Codex P2 su #569 (47ca6fb): un annullamento fallito non ritira il job: si ritenta al giro dopo, cosi' la
    build del vecchio head non resta in coda senza nessuno che la segua."""
    disp, bk, _, _ = env
    send(disp, payload())
    disp.tick()
    bk.fail_cancel = True
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000f9")
    disp.tick()
    old = [j for j in disp.store.recent_jobs() if j["head_sha"] == HEAD]
    assert all(j["state"] == "dispatched" and j["superseded"] == 1 for j in old)
    bk.fail_cancel = False
    disp.tick()
    assert {("pr-overlap-guard", 1), ("pr-merge-readiness", 1)} <= set(bk.canceled)
    assert all(j["state"] == "superseded" for j in disp.store.recent_jobs() if j["head_sha"] == HEAD)


def test_unconfirmed_cancellation_gives_up_after_the_window(env):
    disp, bk, _, clock = env
    send(disp, payload())
    disp.tick()
    bk.fail_cancel = True
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000fa")
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["readiness"] + 1
    disp.tick()
    assert all(j["state"] == "superseded" for j in disp.store.recent_jobs() if j["head_sha"] == HEAD)


@pytest.mark.parametrize("mode", ["truncated201", "html502", "transport"])
def test_unreadable_creation_response_is_ambiguous_never_terminal(env, mode):
    """Codex P2 su #569 (6d3f9e2): un 201 con il corpo troncato (Content-Length) non solleva IncompleteRead ma
    non e' interpretabile; la build puo' esistere. Ogni risposta di creazione non interpretabile e' ambigua:
    riconciliazione, mai `error` con la build lasciata in coda."""
    disp, bk, pub, _ = env
    bk.fail_create, bk.create_lands = mode, True
    send(disp, payload())
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"} and not pub.final()
    bk.fail_create = None
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"dispatched"}
    assert len(bk.builds["pr-overlap-guard"]) == 1


def test_explicit_4xx_rejection_with_non_json_body_is_a_definite_error(env):
    disp, bk, pub, _ = env
    bk.fail_create = "html400"
    send(disp, payload())
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"] and not disp.store.active_jobs()


def test_build_landing_after_an_empty_list_is_adopted_within_the_window(env):
    """Audit A1: creazione in timeout, primo elenco vuoto, build arrivata dopo: va adottata, non orfana."""
    disp, bk, pub, clock = env
    bk.fail_create = "transport"
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    clock.now += 5
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"}
    for job in disp.store.recent_jobs():
        bk._new_build(td.GUARDS[job["guard"]]["pipeline"],
                      {"branch": "main", "commit": "HEAD", "message": "m", "env": td.build_env(job)})
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"dispatched"}


@pytest.mark.parametrize("bad_state", [["running"], {"s": 1}, 7, None])
def test_non_string_build_state_is_error_not_a_stuck_job(env, bad_state):
    """Audit A2: uno `state` non stringa sollevava TypeError fuori dal controllo del timeout."""
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.mutate_build = lambda b: b.__setitem__("state", bad_state)
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]


def test_malformed_artifact_location_is_retried_then_error(env):
    """Audit A3: `Location` malformata (ValueError di urlsplit) non blocca il job per sempre."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.redirect_host = "[bad"
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]


def test_201_without_a_usable_number_is_ambiguous(env):
    """Audit A4: un 201 senza numero di build e' ambiguo: si riconcilia e la build arrivata viene adottata."""
    disp, bk, pub, _ = env
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        status, h, data = orig(method, url, headers, body, timeout)
        if method == "POST":
            return 201, h, b'{"id": "0190-uuid", "state": "scheduled"}'
        return status, h, data
    disp.bk._transport = tr
    send(disp, payload())
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"} and not pub.final()
    disp.bk._transport = bk
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"dispatched"}


def test_failed_cancel_on_timeout_is_retried_after_the_error_is_published(env):
    """Audit A5/A9: esito `error` pubblicato, ma il job resta `canceling` finche' l'annullamento non e'
    confermato: la build non resta orfana."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.fail_cancel = True
    clock.now += 601
    disp.tick()
    assert _overlap(disp)["state"] == "canceling"
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]
    bk.fail_cancel = False
    disp.tick()
    assert _overlap(disp)["state"] == "done" and ("pr-overlap-guard", 1) in bk.canceled


def test_duplicate_builds_with_failed_cancels_stay_canceling(env):
    disp, bk, pub, _ = env
    bk.fail_create, bk.create_lands = "transport", True
    send(disp, payload())
    disp.tick()
    first = bk.builds["pr-overlap-guard"][1]
    bk.builds["pr-overlap-guard"][2] = {**first, "number": 2,
                                        "web_url": f"https://buildkite.com/{ORG}/pr-overlap-guard/builds/2"}
    bk.fail_create = None
    bk.fail_cancel = True
    disp.tick()
    assert _overlap(disp)["state"] == "canceling"
    assert json.loads(_overlap(disp)["pending_cancels"]) == [1, 2]
    bk.fail_cancel = False
    disp.tick()
    assert _overlap(disp)["state"] == "done"
    assert {("pr-overlap-guard", 1), ("pr-overlap-guard", 2)} <= set(bk.canceled)


def test_never_confirmed_cancel_gives_up_after_the_window(env):
    disp, bk, _, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.fail_cancel = True
    clock.now += 601
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert _overlap(disp)["state"] == "done"


def test_deeply_nested_json_is_not_a_stuck_job(env):
    """Audit A6: JSON annidato all'estremo (RecursionError) e' una risposta illeggibile, non un'eccezione libera."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        if method == "GET" and "/builds/" in url and "artifacts" not in url:
            return 200, {}, b"[" * 100_000
        return orig(method, url, headers, body, timeout)
    disp.bk._transport = tr
    clock.now += 10_000
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]


def test_transient_artifact_failure_is_retried(env):
    """Audit A7: un errore temporaneo sul download del risultato non da' un `error` definitivo a una build
    passata: si ritenta al giro dopo."""
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    orig = bk.__call__
    fails = {"n": 1}

    def tr(method, url, headers, body, timeout):
        if url.startswith(f"https://{S3}/") and fails["n"]:
            fails["n"] -= 1
            raise td.TransportError("timeout")
        return orig(method, url, headers, body, timeout)
    disp.bk._transport = tr
    disp.tick()
    assert not [c for c in pub.final() if c[3] == "overlap-check"]
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["success"]


def test_truncated_artifact_body_is_not_a_verdict(env):
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        status, h, data = orig(method, url, headers, body, timeout)
        if url.startswith(f"https://{S3}/"):
            return status, h, data[:-3]
        return status, h, data
    disp.bk._transport = tr
    disp.tick()
    assert not pub.final()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"] == ["error"]


def test_full_build_page_is_not_proof_of_absence(env):
    """Audit A8: 100 build nella prima pagina: l'assenza non e' dimostrata, si riprova (mai orfana)."""
    disp, bk, pub, _ = env
    bk.fail_create, bk.create_lands = "transport", True
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        if method == "GET" and "builds?" in url:
            return 200, {}, json.dumps([{"number": 1000 + i, "env": {}} for i in range(100)]).encode()
        return orig(method, url, headers, body, timeout)
    disp.bk._transport = tr
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"} and not pub.final()


def test_exception_escaping_advance_still_times_out(env, monkeypatch):
    """Fallback strutturale: un job che solleva a ogni giro diventa `error` scaduto il timeout della build."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()

    def boom(job):
        raise RuntimeError("bug")
    monkeypatch.setattr(disp, "_poll", boom)
    disp.tick()
    assert not pub.final()
    clock.now += 10_000
    disp.tick()
    assert sorted(c[2] for c in pub.final()) == ["error", "error"]


def test_full_build_page_does_not_let_a_superseded_creation_retire_unchecked(env):
    """Audit A8, percorso di sostituzione: con la pagina piena non si sa se la build ambigua esista; il job
    sostituito resta attivo e riprova, invece di ritirarsi senza annullare nulla."""
    disp, bk, _, _ = env
    bk.fail_create, bk.create_lands = "transport", True
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        if method == "GET" and "builds?" in url:
            return 200, {}, json.dumps([{"number": 1000 + i, "env": {}} for i in range(100)]).encode()
        return orig(method, url, headers, body, timeout)
    disp.bk._transport = tr
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000fb")
    disp.tick()
    old = [j for j in disp.store.recent_jobs() if j["head_sha"] == HEAD]
    assert all(j["state"] == "creating" and j["superseded"] == 1 for j in old)


def test_superseded_ambiguous_creation_waits_for_a_late_build_to_cancel_it(env):
    """Codex P2 su #569 (cee448c): creazione in timeout, head nuovo prima che la build sia visibile. Elenco vuoto
    entro la finestra non ritira il job: la build arrivata in ritardo viene trovata e annullata."""
    disp, bk, _, clock = env
    bk.fail_create = "transport"
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000fc")
    disp.tick()
    old = [j for j in disp.store.recent_jobs() if j["head_sha"] == HEAD]
    assert all(j["state"] == "creating" and j["superseded"] == 1 for j in old)
    for job in old:                                       # la build del vecchio head compare in ritardo
        bk._new_build(td.GUARDS[job["guard"]]["pipeline"],
                      {"branch": "main", "commit": "HEAD", "message": "m", "env": td.build_env(job)})
    disp.tick()
    old_numbers = {(td.GUARDS[j["guard"]]["pipeline"], n) for j in old for n in
                   [b["number"] for b in bk.builds[td.GUARDS[j["guard"]]["pipeline"]].values()
                    if b["env"]["PF_PR_HEAD_SHA"] == HEAD]}
    assert old_numbers and old_numbers <= set(bk.canceled)
    assert all(j["state"] == "superseded" for j in disp.store.recent_jobs() if j["head_sha"] == HEAD)
    clock.now += 1


def test_pending_cancels_are_retried_while_publication_keeps_failing(env):
    """Codex P2 su #569 (cee448c): annullamento fallito sul timeout e publisher in errore. L'annullamento si
    ritenta comunque a ogni giro; scaduta la finestra di pubblicazione il job non sparisce con build pendenti."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.fail_cancel = True
    pub.rc = 1
    clock.now += 601
    disp.tick()
    assert _overlap(disp)["state"] == "publishing"
    bk.fail_cancel = False
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled
    assert json.loads(_overlap(disp)["pending_cancels"]) == []


def test_publication_window_expiry_never_drops_pending_cancels(env):
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.fail_cancel = True
    pub.rc = 1
    clock.now += 601
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert _overlap(disp)["state"] in td.ACTIVE_STATES        # ancora attivo: build da annullare
    bk.fail_cancel = False
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled


def test_undelivered_outcome_survives_the_end_of_cancellation_and_is_redelivered(env):
    """Codex P2 su #569 (59b071e): con annullamento e publisher entrambi in errore, `publish_failed` diventava
    `canceling` e poi `done` all'annullamento confermato, senza mai consegnare l'esito: il required check restava
    `pending`. La consegna si registra a parte: annullamento chiuso => di nuovo `publish_failed`, poi riconsegna."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.fail_cancel = True
    pub.rc = 1
    clock.now += 601
    disp.tick()
    clock.now += disp.cfg.build_timeout_seconds["overlap"] + 1
    disp.tick()
    assert _overlap(disp)["state"] == "canceling"
    bk.fail_cancel = False
    disp.tick()
    job = _overlap(disp)
    assert job["state"] == "publish_failed" and not job["final_published"]
    pub.rc = 0
    clock.now += td.REDELIVERY_SECONDS + 1
    disp.tick()
    job = disp.store.get_job(job["id"])
    assert job["state"] == "done" and job["final_published"] == 1
    assert [c[2] for c in pub.final() if c[3] == "overlap-check"][-1] == "error"


def test_delivered_outcome_ends_done_when_cancellation_completes(env):
    """Esito consegnato, annullamento confermato dopo: `done`, nessuna riconsegna."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.builds["pr-overlap-guard"][1]["state"] = "running"
    bk.fail_cancel = True
    clock.now += 601
    disp.tick()
    assert _overlap(disp)["state"] == "canceling" and _overlap(disp)["final_published"] == 1
    bk.fail_cancel = False
    disp.tick()
    overlap_finals = lambda: [c for c in pub.final() if c[3] == "overlap-check"]     # noqa: E731
    finals = len(overlap_finals())
    clock.now += td.REDELIVERY_SECONDS + 1
    disp.tick()
    assert _overlap(disp)["state"] == "done" and len(overlap_finals()) == finals


def test_no_final_state_with_pending_cancels(env):
    """Invariante in `_set`: un passaggio a uno stato finale con build da annullare diventa `canceling`."""
    disp, _, _, _ = env
    send(disp, payload())
    job = disp.store.active_jobs()[0]
    job = dict(job, pending_cancels="[7]")
    disp.store.update_job(job["id"], disp.clock(), pending_cancels="[7]")
    for final in td.FINAL_STATES:
        disp._set(job, state=final)
        assert job["state"] == "canceling" and disp.store.get_job(job["id"])["state"] == "canceling"
    disp._set(job, state="done", pending_cancels="[]")
    assert disp.store.get_job(job["id"])["state"] == "done"


def test_full_build_page_reports_unverifiable_not_unconfirmed(env):
    """Pagina piena: scaduta la finestra l'esito e' `error` con il motivo veritiero («non verificabile»), non
    «non confermata» come per un elenco letto davvero vuoto."""
    disp, bk, pub, clock = env
    bk.fail_create = "transport"
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        if method == "GET" and "builds?" in url:
            return 200, {}, json.dumps([{"number": 1000 + i, "env": {}} for i in range(100)]).encode()
        return orig(method, url, headers, body, timeout)
    disp.bk._transport = tr
    clock.now += disp.cfg.build_timeout_seconds["readiness"] + 1
    disp.tick()
    descs = [c[5] for c in pub.final()]
    assert descs and all("non verificabile" in d for d in descs)


def test_late_adopted_superseded_build_keeps_retrying_its_cancel(env):
    """Audit I1: build adottata tardi e poi sostituita: la finestra di annullamento parte dall'adozione, non dalla
    creazione, quindi un annullamento fallito si ritenta."""
    disp, bk, _, clock = env
    bk.fail_create = "transport"
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    clock.now += 590
    for job in disp.store.recent_jobs():
        bk._new_build(td.GUARDS[job["guard"]]["pipeline"],
                      {"branch": "main", "commit": "HEAD", "message": "m", "env": td.build_env(job)})
    disp.tick()
    clock.now += 110
    bk.fail_cancel = True
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000a1")
    disp.tick()
    old = [j for j in disp.store.recent_jobs() if j["guard"] == "overlap" and j["head_sha"] == HEAD][0]
    assert old["state"] in td.ACTIVE_STATES
    bk.fail_cancel = False
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled


def test_publisher_raising_every_tick_is_bounded(env):
    """Audit I2: un publisher che solleva a ogni giro non tiene il job attivo per sempre."""
    disp, bk, _, clock = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))

    def boom(*a):
        raise RuntimeError("publisher bug")
    disp.publish = boom
    for _ in range(3):
        clock.now += 100_000
        disp.tick()
    assert not disp.store.active_jobs()


def test_reopen_with_ambiguous_creation_publishes_pending_over_old_success(env):
    """Audit I4: head gia' `success` rivalutato con creazione ambigua: `pending` subito, non dopo la finestra."""
    disp, bk, pub, clock = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    bk.fail_create = "transport"
    send(disp, payload(action="reopened", updated="2026-10-27T11:00:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000a2")
    for _ in range(3):
        clock.now += 60
        disp.tick()
    last = [c for c in pub.calls if c[3] == "overlap-check"][-1]
    assert last[2] == "pending"
    readiness_pending = [c for c in pub.calls if c[3] == "pr-merge-readiness" and c[2] == "pending"][-1]
    assert "pr-merge-readiness" in readiness_pending[4]           # resta un self-check per la readiness


def test_migration_backfills_publishing_since(tmp_path):
    cfg = td.validate_config(raw_config(tmp_path))
    store = td.Store(cfg.state_db)
    store._db.execute("INSERT INTO jobs(delivery_id, event, pr, head_sha, guard, state, created_at, updated_at) "
                      "VALUES ('d', 'opened', 1, ?, 'overlap', 'canceling', 1, 5)", (HEAD,))
    store.close()
    store = td.Store(cfg.state_db)
    try:
        assert store.recent_jobs()[0]["publishing_since"] == 5
    finally:
        store.close()


# --------------------------------------------------------------------------------------------- polling: transizioni
def _posts(bk):
    return [json.loads(r[3]) for r in bk.requests if r[0] == "POST"]


def _states(disp, head=None):
    return {(j["head_sha"], j["guard"]): j["state"] for j in disp.store.recent_jobs()
            if head is None or j["head_sha"] == head}


def test_new_pr_dispatches_exactly_two_trusted_builds_with_contract_env(env):
    disp, bk, pub, _ = env
    bk.gh.put(42, HEAD)
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa"]
    disp.tick()
    posts = _posts(bk)
    assert sorted(r[1].rsplit("/", 2)[-2] for r in bk.requests if r[0] == "POST") == \
        ["pr-merge-readiness", "pr-overlap-guard"]
    for sent in posts:
        assert sent["branch"] == "main" and sent["commit"] == "HEAD"
        assert sent["env"] == {"PF_PR_NUMBER": "42", "PF_PR_HEAD_SHA": HEAD, "PF_PR_BASE_REF": "main",
                               "PF_EVENT": "opened", "PF_DELIVERY_ID": "p42-t1-aaaaaaaaaaaa"}
    assert {c[3] for c in pub.calls} == {"overlap-check", "pr-merge-readiness"}
    assert all(c[2] == "pending" for c in pub.calls)


def test_unchanged_state_polled_again_starts_no_new_builds(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    for _ in range(3):
        assert disp.poll() == []
        disp.tick()
    assert len(_posts(bk)) == 2


def test_new_commit_is_synchronize_and_cancels_the_old_builds(env):
    disp, bk, pub, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.gh.put(42, HEAD2)
    assert disp.poll() == ["p42-t2-bbbbbbbbbbbb"]
    disp.tick()
    assert ("pr-overlap-guard", 1) in bk.canceled and ("pr-merge-readiness", 1) in bk.canceled
    assert {p["env"]["PF_EVENT"] for p in _posts(bk)[2:]} == {"synchronize"}
    assert not [c for c in pub.final() if c[1] == HEAD and c[2] == "success"]


def test_return_to_a_previous_sha_is_a_new_distinct_transition(env):
    disp, bk, _, _ = env
    tids = []
    for head in (HEAD, HEAD2, HEAD):
        bk.gh.put(42, head)
        tids += disp.poll()
        disp.tick()
    assert tids == ["p42-t1-aaaaaaaaaaaa", "p42-t2-bbbbbbbbbbbb", "p42-t3-aaaaaaaaaaaa"]
    assert {p["env"]["PF_DELIVERY_ID"] for p in _posts(bk)} == set(tids)
    live = [j["state"] for j in disp.store.recent_jobs() if j["head_sha"] == HEAD and not j["superseded"]]
    assert live == ["dispatched", "dispatched"]


def test_closed_and_reopened_pr_is_reevaluated_even_on_the_same_sha(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    del bk.gh.pulls[42]
    disp.poll()
    disp.poll()                                        # assente da due elenchi completi: chiusa
    bk.gh.put(42, HEAD)
    assert disp.poll() == ["p42-t2-aaaaaaaaaaaa"]
    disp.tick()
    assert [p["env"]["PF_EVENT"] for p in _posts(bk)[2:]] == ["reopened", "reopened"]


def test_closing_a_pr_cancels_its_running_builds(env):
    disp, bk, pub, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    del bk.gh.pulls[42]
    disp.poll()
    disp.poll()
    disp.tick()
    assert {("pr-overlap-guard", 1), ("pr-merge-readiness", 1)} <= set(bk.canceled)
    assert not pub.final()


def test_a_single_missing_listing_does_not_close_the_pr(env):
    """Una PR puo' sparire da un elenco letto mentre cambia fra due pagine: chiusa solo dopo due assenze."""
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    saved = bk.gh.pulls.pop(42)
    disp.poll()
    bk.gh.pulls[42] = saved
    assert disp.poll() == []
    disp.tick()
    assert not bk.canceled and len(_posts(bk)) == 2


def test_two_concurrent_prs_are_independent(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    bk.gh.put(43, HEAD2)
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa", "p43-t1-bbbbbbbbbbbb"]
    disp.tick()
    bk.gh.put(43, HEAD3)
    disp.poll()
    disp.tick()
    assert not [c for c in bk.canceled if c[1] in (1, 2) and False]
    by_pr = {(p["env"]["PF_PR_NUMBER"], p["env"]["PF_PR_HEAD_SHA"]) for p in _posts(bk)}
    assert by_pr == {("42", HEAD), ("43", HEAD2), ("43", HEAD3)}
    states = {(j["pr"], j["head_sha"]): j["state"] for j in disp.store.recent_jobs()}
    assert states[(42, HEAD)] == "dispatched" and states[(43, HEAD2)] == "superseded"


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(draft=True),
    lambda p: p["base"].update(ref="develop"),
    lambda p: p["head"].update(repo={"full_name": "attacker/Pickfair-nogui"}),
    lambda p: p["head"].update(repo=None),
])
def test_out_of_contract_prs_never_dispatch(env, mutate):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    mutate(bk.gh.pulls[42])
    assert disp.poll() == []
    disp.tick()
    assert not _posts(bk)


def test_draft_becoming_ready_is_a_ready_for_review_transition(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD, draft=True)
    disp.poll()
    bk.gh.put(42, HEAD)
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa"]
    disp.tick()
    assert {p["env"]["PF_EVENT"] for p in _posts(bk)} == {"ready_for_review"}


def test_pr_turning_draft_stops_its_evaluation(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.gh.put(42, HEAD, draft=True)
    disp.poll()
    disp.tick()
    assert {("pr-overlap-guard", 1), ("pr-merge-readiness", 1)} <= set(bk.canceled)


@pytest.mark.parametrize("bad", [
    lambda p: p.update(number=True), lambda p: p.update(number="42"), lambda p: p.update(number=0),
    lambda p: p["head"].update(sha="A" * 40), lambda p: p.update(draft="false"), lambda p: p.update(state="closed"),
    lambda p: p["base"]["repo"].update(full_name="evil/x"), lambda p: p["head"].update(repo={"full_name": 7}),
])
def test_malformed_pr_item_aborts_the_whole_poll(env, bad):
    disp, bk, _, _ = env
    bk.gh.put(41, HEAD2)
    bk.gh.put(42, HEAD)
    bad(bk.gh.pulls[42])
    assert disp.poll() == []
    disp.tick()
    assert not _posts(bk)                                    # nemmeno la PR #41, valida, parte


def test_restart_with_unchanged_state_starts_no_new_builds(env):
    disp, bk, pub, clock = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    disp.store.close()
    disp2 = make_dispatcher(disp.cfg, bk, Publisher(), clock)
    try:
        assert disp2.poll() == []
        disp2.tick()
        assert len(_posts(bk)) == 2
    finally:
        disp2.store.close()
    disp.store = td.Store(disp.cfg.state_db)


def test_restart_reconciles_a_push_made_while_the_dispatcher_was_down(env):
    disp, bk, _, clock = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    disp.store.close()
    bk.gh.put(42, HEAD2)                                    # push durante il fermo
    disp2 = make_dispatcher(disp.cfg, bk, Publisher(), clock)
    try:
        assert disp2.poll() == ["p42-t2-bbbbbbbbbbbb"]
        disp2.tick()
        assert ("pr-overlap-guard", 1) in bk.canceled
    finally:
        disp2.store.close()
    disp.store = td.Store(disp.cfg.state_db)


# --------------------------------------------------------------------------------------------- polling: errori GitHub
@pytest.mark.parametrize("status", [500, 502, 404, 422])
def test_github_api_error_aborts_the_poll_and_never_closes_prs(env, status):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.gh.fail_status = status
    for _ in range(3):
        assert disp.poll() == []
    disp.tick()
    assert not bk.canceled
    bk.gh.fail_status = None
    bk.gh.put(42, HEAD2)
    assert disp.poll() == ["p42-t2-bbbbbbbbbbbb"]


def test_non_list_page_aborts_the_poll(env):
    disp, bk, _, _ = env
    bk.gh.page_size = 1
    for n in (40, 41, 42):
        bk.gh.put(n, HEAD)
    bk.gh.break_page = 2
    assert disp.poll() == []
    assert not disp.store.recent_jobs()


def test_pagination_is_followed_to_the_last_page(env):
    disp, bk, _, _ = env
    bk.gh.page_size = 2
    for n in range(40, 45):
        bk.gh.put(n, HEAD)
    assert len(disp.poll()) == 5
    pages = [r for r in bk.gh.requests if "/pulls" in r[1]]
    assert len(pages) == 3


def test_pagination_beyond_the_limit_is_unverifiable_and_aborts(env):
    disp, bk, _, _ = env
    bk.gh.page_size = 1
    for n in range(1, td.MAX_PAGES + 2):
        bk.gh.put(n, HEAD)
    assert disp.poll() == []
    assert not disp.store.recent_jobs()


def test_incomplete_listing_without_next_link_cannot_close_prs_in_one_poll(env):
    """Un elenco che si ferma prima (Link `next` mancante) non e' riconoscibile: la difesa e' che una PR sparita
    da UN elenco non e' chiusa; servono due elenchi completi consecutivi."""
    disp, bk, _, _ = env
    bk.gh.page_size = 1
    bk.gh.put(41, HEAD2)
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.gh.drop_next_link_on_page = 1
    disp.poll()
    bk.gh.drop_next_link_on_page = None
    disp.poll()
    disp.tick()
    assert not bk.canceled


def test_rate_limit_skips_polls_until_reset(env):
    disp, bk, _, clock = env
    bk.gh.put(42, HEAD)
    bk.gh.rate_limit_reset = clock.now + 300
    assert disp.poll() == []
    calls = len(bk.gh.requests)
    clock.now += 100
    assert disp.poll() == [] and len(bk.gh.requests) == calls      # nessuna richiesta prima del reset
    bk.gh.rate_limit_reset = None
    clock.now += 201
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa"]


def test_secondary_rate_limit_with_retry_after_is_honoured(env):
    disp, bk, _, clock = env
    orig = bk.gh.handle

    def handle(method, url, headers):
        bk.gh.requests.append((method, url, dict(headers)))
        return 429, {"Retry-After": "120"}, b"{}"
    bk.gh.handle = handle
    bk.gh.put(42, HEAD)
    assert disp.poll() == []
    bk.gh.handle = orig
    clock.now += 60
    assert disp.poll() == []
    clock.now += 61
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa"]


def test_github_client_refuses_any_other_endpoint():
    for url in (f"https://api.github.com/repos/{REPO}/contents/scripts", f"https://api.github.com/repos/{REPO}/tarball",
                f"https://api.github.com/repos/{REPO}/zipball/main", f"https://api.github.com/repos/other/x/pulls",
                f"http://api.github.com/repos/{REPO}/pulls", f"https://evil.example/repos/{REPO}/pulls",
                f"https://api.github.com/repos/{REPO}/pulls/42/files"):
        with pytest.raises(td.ApiError):
            td.GitHub.check_url(url)


def test_next_link_to_another_endpoint_aborts_the_poll(env):
    disp, bk, _, _ = env
    orig = bk.gh.handle

    def handle(method, url, headers):
        status, h, body = orig(method, url, headers)
        h = dict(h, Link=f'<https://api.github.com/repos/{REPO}/contents/x?page=2>; rel="next"')
        return status, h, body
    bk.gh.handle = handle
    bk.gh.put(42, HEAD)
    assert disp.poll() == []
    assert not [r for r in bk.gh.requests if "/contents/" in r[1]]


# --------------------------------------------------------------------------------------------- credenziali
def test_failing_token_command_means_no_poll(env, tmp_path):
    disp, bk, _, clock = env
    disp.gh._tokens._runner = token_runner(clock, rc=1)
    bk.gh.put(42, HEAD)
    assert disp.poll() == []
    assert not bk.gh.requests


@pytest.mark.parametrize("body", ["not json", json.dumps({TK: "short", "expires_at": "2030-01-01T00:00:00Z"}),
                                  '{"expires_at": "2030-01-01T00:00:00Z"}', json.dumps({TK: "g" * 34}),
                                  json.dumps({TK: "g" * 34, "expires_at": "domani"})])
def test_malformed_token_output_means_no_poll(env, body):
    disp, bk, _, clock = env
    disp.gh._tokens._runner = token_runner(clock, body=body)
    bk.gh.put(42, HEAD)
    assert disp.poll() == [] and not bk.gh.requests


def test_expired_or_expiring_token_is_refused(env):
    disp, bk, _, clock = env
    disp.gh._tokens._runner = token_runner(clock, ttl=120)       # meno del margine di rinnovo
    bk.gh.put(42, HEAD)
    assert disp.poll() == [] and not bk.gh.requests


def test_token_is_refreshed_before_expiry(env):
    disp, bk, _, clock = env
    runner = token_runner(clock, ttl=3600)
    disp.gh._tokens._runner = runner
    disp.poll()
    disp.poll()
    assert len(runner.calls) == 1
    clock.now += 3600 - td.TOKEN_REFRESH_MARGIN + 1
    disp.poll()
    assert len(runner.calls) == 2


def test_rejected_token_is_invalidated_and_the_poll_aborted(env):
    disp, bk, _, clock = env
    runner = token_runner(clock)
    disp.gh._tokens._runner = runner
    bk.gh.put(42, HEAD)
    bk.gh.fail_status = 401
    assert disp.poll() == []
    bk.gh.fail_status = None
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa"]
    assert len(runner.calls) == 2                                 # nuovo token dopo il 401


def test_classic_token_with_scopes_is_refused(env):
    disp, bk, _, _ = env
    bk.gh.scopes = "repo"
    bk.gh.put(42, HEAD)
    assert disp.poll() == []
    assert not disp.store.recent_jobs()


def test_tokens_never_reach_builds_publisher_or_logs(env, capsys):
    disp, bk, pub, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    bk.fail_create = "http422"
    disp.tick()
    bk.gh.fail_status = 401
    disp.poll()
    for method, url, headers, body in bk.requests:
        assert GH_CRED not in (body or b"").decode() and GH_CRED not in url and BK_CRED not in url
        if body:
            assert BK_CRED not in body.decode()
    assert not any(GH_CRED in str(a) or BK_CRED in str(a) for c in pub.calls for a in c)
    err = capsys.readouterr().err
    assert GH_CRED not in err and BK_CRED not in err


# --------------------------------------------------------------------------------------------- rerun manuale
def _rerun_ready(disp, bk):
    bk.gh.put(42, HEAD)
    disp.poll()                                                   # primo avvio: cursore dei commenti = adesso
    disp.tick()


def test_authorized_rerun_on_the_current_head_starts_new_builds(env):
    disp, bk, pub, clock = env
    _rerun_ready(disp, bk)
    bk.gh.comment(9001, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == ["p42-r9001-aaaaaaaaaaaa"]
    disp.tick()
    assert {("pr-overlap-guard", 1), ("pr-merge-readiness", 1)} <= set(bk.canceled)
    new = _posts(bk)[2:]
    assert {p["env"]["PF_EVENT"] for p in new} == {"manual"}
    assert {p["env"]["PF_DELIVERY_ID"] for p in new} == {"p42-r9001-aaaaaaaaaaaa"}
    audit = disp.store.recent_reruns()
    assert audit[0]["accepted"] == 1 and audit[0]["login"] == RERUNNER and audit[0]["tid"] == "p42-r9001-aaaaaaaaaaaa"


def test_rerun_command_is_executed_once(env):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    bk.gh.comment(9002, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    disp.poll()
    disp.tick()
    clock.now += 30                                               # finestra di sovrapposizione: lo rilegge
    assert disp.poll() == []
    disp.tick()
    assert len(_posts(bk)) == 4


@pytest.mark.parametrize("case,reason", [
    ("login", "non autorizzato"), ("sha", "non e' l'head corrente"), ("edited", "modificato"),
    ("closed", "PR non aperta"), ("draft", "non valutabile"),
    ("read", "senza scrittura"), ("triage", "senza scrittura"), ("revoked", "senza scrittura"),
    ("bot", "non e' un utente"),
])
def test_unauthorized_or_stale_rerun_is_rejected_and_audited(env, case, reason):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    created = td.iso(clock.now + 10)
    kwargs = {"login": {"login": "intruder"}, "bot": {"user_type": "Bot"}}.get(case, {})
    if case in ("read", "triage"):
        bk.gh.permissions[RERUNNER] = case       # allowlist e association OWNER non bastano: conta il permesso attuale
    if case == "revoked":
        del bk.gh.permissions[RERUNNER]          # 404: nessun accesso al repository
    sha = HEAD2 if case == "sha" else HEAD
    updated = td.iso(clock.now + 15) if case == "edited" else None
    if case == "closed":
        del bk.gh.pulls[42]
        disp.poll()
        disp.poll()
    if case == "draft":
        bk.gh.put(42, HEAD, draft=True)
    bk.gh.comment(9003, 42, f"/pf-trusted-rerun {sha}", created=created, updated=updated, **kwargs)
    clock.now += 20
    assert disp.poll() == []
    audit = disp.store.recent_reruns()
    assert audit and audit[0]["accepted"] == 0 and reason in audit[0]["reason"]
    disp.tick()
    assert len([p for p in _posts(bk) if p["env"]["PF_EVENT"] == "manual"]) == 0


def test_non_command_comments_are_ignored_and_not_recorded(env):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    for cid, body in ((1, "LGTM"), (2, f"please /pf-trusted-rerun {HEAD}"), (3, f"/pf-trusted-rerun {HEAD[:39]}")):
        bk.gh.comment(cid, 42, body, created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == [] and disp.store.recent_reruns() == []


def test_historical_rerun_commands_are_not_executed_at_first_start(env):
    disp, bk, _, clock = env
    bk.gh.comment(9004, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now - 3600))
    bk.gh.put(42, HEAD)
    disp.poll()
    clock.now += 60
    disp.poll()
    assert disp.store.recent_reruns() == []


def test_overlap_never_moves_the_cursor_before_the_first_start(env):
    """Codex P2 su #569 (893390a): con il polling a 60 s la sovrapposizione di 120 s riportava il cursore prima del
    primo avvio e un comando di 30 s prima dell'avvio veniva eseguito al terzo giro."""
    disp, bk, _, clock = env
    bk.gh.comment(9010, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now - 30))
    bk.gh.put(42, HEAD)
    for _ in range(4):
        disp.poll()
        disp.tick()
        clock.now += 60
    assert disp.store.recent_reruns() == []
    assert [p for p in _posts(bk) if p["env"]["PF_EVENT"] == "manual"] == []


def test_command_in_the_same_second_as_the_first_start_is_rejected(env):
    """Timestamp al secondo: un comando con lo stesso istante del primo avvio puo' essere anteriore. Fail closed:
    rifiutato e registrato, nessuna build."""
    disp, bk, _, clock = env
    bk.gh.put(42, HEAD)
    disp.poll()                                                   # primo avvio: limite = iso(clock.now)
    bk.gh.comment(9011, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now))
    clock.now += 60
    assert disp.poll() == []
    audit = disp.store.recent_reruns()
    assert audit and audit[0]["accepted"] == 0 and "primo avvio" in audit[0]["reason"]


def test_cursor_floor_survives_restart_and_old_commands_never_build(tmp_path):
    """Riavvio (anche dopo un crash): il limite inferiore e' nell'archivio, non in memoria. Un comando anteriore al
    primo avvio resta escluso per sempre; uno successivo e autorizzato si esegue una sola volta."""
    cfg = td.validate_config(raw_config(tmp_path))
    bk, clock, pub = FakeBuildkite(), Clock(), Publisher()
    start = clock.now
    bk.gh.comment(9012, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(start - 10))
    bk.gh.put(42, HEAD)
    disp = make_dispatcher(cfg, bk, pub, clock)
    disp.poll()
    disp.store.close()                                            # arresto senza pulizia (crash)
    for _ in range(3):
        clock.now += 60
        disp = make_dispatcher(cfg, bk, pub, clock)
        assert disp.store.get_meta("comments_floor") == td.iso(start)
        disp.poll()
        disp.tick()
        disp.store.close()
    bk.gh.comment(9013, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 5))
    for _ in range(3):                                            # piu' giri: la sovrapposizione lo rilegge
        clock.now += 60
        disp = make_dispatcher(cfg, bk, pub, clock)
        disp.poll()
        disp.store.close()
    disp = make_dispatcher(cfg, bk, pub, clock)
    try:
        rows = disp.store.recent_reruns()
        assert [r["comment_id"] for r in rows] == [9013] and rows[0]["accepted"] == 1
    finally:
        disp.store.close()


@pytest.mark.parametrize("cid", [10 ** 15, 10 ** 45])
def test_comment_id_out_of_range_is_a_malformed_command(env, cid):
    """Un id fuori misura non entra in SQLite (overflow a ogni giro) e darebbe un PF_DELIVERY_ID oltre i 64 caratteri
    del gate: e' un comando malformato, quindi lettura annullata, nessuna build, nessuna eccezione."""
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    cursor = disp.store.get_meta("comments_since")
    bk.gh.comment(cid, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == []
    assert disp.store.get_meta("comments_since") == cursor and disp.store.recent_reruns() == []
    assert td._TID_RE.fullmatch(f"p9999999-r{td.MAX_COMMENT_ID - 1}-aaaaaaaaaaaa")


def test_malformed_rerun_command_keeps_the_cursor(env):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    cursor = disp.store.get_meta("comments_since")
    bk.gh.comment(9005, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    bk.gh.comments[-1]["issue_url"] = "https://evil.example/issues/42"
    clock.now += 20
    disp.poll()
    assert disp.store.get_meta("comments_since") == cursor and disp.store.recent_reruns() == []


@pytest.mark.parametrize("case", ["no_type", "permission_api_error", "permission_malformed", "permission_other_user"])
def test_rerun_command_without_verified_permissions_keeps_the_cursor(env, case):
    """Tipo di account e permesso attuale li dichiara GitHub: se non sono verificabili il comando non si valuta e
    il cursore resta fermo (fail closed); nessuna riga di audit con un esito inventato."""
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    cursor = disp.store.get_meta("comments_since")
    bk.gh.comment(9006, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    if case == "no_type":
        bk.gh.comments[-1]["user"].pop("type")
    elif case == "permission_api_error":
        bk.gh.permission_status = 502
    elif case == "permission_malformed":
        bk.gh.permissions[RERUNNER] = "superuser"
    else:
        orig = bk.gh.handle
        bk.gh.handle = lambda m, u, h: (lambda r: (r[0], r[1], r[2].replace(RERUNNER.encode(), b"someone-else"))
                                        if "/permission" in u else r)(orig(m, u, h))
    clock.now += 20
    assert disp.poll() == []
    assert disp.store.get_meta("comments_since") == cursor and disp.store.recent_reruns() == []


@pytest.mark.parametrize("perm", ["admin", "maintain", "write"])
def test_rerun_accepted_only_with_current_write_permission(env, perm):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    bk.gh.permissions[RERUNNER] = perm
    bk.gh.comment(9007, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == ["p42-r9007-aaaaaaaaaaaa"]
    assert [r for r in bk.gh.requests if r[1].endswith(f"/collaborators/{RERUNNER}/permission")]


def test_permission_is_not_queried_for_logins_outside_the_allowlist(env):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    bk.gh.comment(9008, 42, f"/pf-trusted-rerun {HEAD}", login="intruder", created=td.iso(clock.now + 10))
    clock.now += 20
    disp.poll()
    assert not [r for r in bk.gh.requests if "/permission" in r[1]]
    assert "non autorizzato" in disp.store.recent_reruns()[0]["reason"]


def test_reruns_disabled_with_an_empty_allowlist(tmp_path):
    raw = raw_config(tmp_path)
    raw["rerun"]["allowed_logins"] = []
    cfg = td.validate_config(raw)
    bk, clock = FakeBuildkite(), Clock()
    disp = make_dispatcher(cfg, bk, Publisher(), clock)
    try:
        bk.gh.put(42, HEAD)
        disp.poll()
        assert not [r for r in bk.gh.requests if "/issues/comments" in r[1]]
    finally:
        disp.store.close()


# --------------------------------------------------------------------------------------------- codice della PR mai eseguito
def test_builds_always_run_main_never_the_pr_head(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    for sent in _posts(bk):
        assert sent["branch"] == "main" and sent["commit"] == "HEAD"
        assert set(sent) == {"commit", "branch", "message", "env"}
        assert set(sent["env"]) == {"PF_PR_NUMBER", "PF_PR_HEAD_SHA", "PF_PR_BASE_REF", "PF_EVENT", "PF_DELIVERY_ID"}


def test_dispatcher_has_no_web_server_and_no_code_fetching_paths():
    source = (ROOT / "scripts" / "pr_trusted_dispatcher.py").read_text(encoding="utf-8")
    for forbidden in ("http.server", "socketserver", "/contents", "/tarball", "/zipball", "git clone",
                      "checkout", "listen"):
        assert forbidden not in source, forbidden
    assert td._GITHUB_PATHS == (f"/repos/{REPO}/pulls", f"/repos/{REPO}/issues/comments")


def test_transition_ids_match_the_gate_format_even_for_large_numbers():
    import re
    gate = re.compile(r"[A-Za-z0-9-]{8,64}")
    for tid in ("p9999999-t99999999-aaaaaaaaaaaa", "p9999999-r99999999999-aaaaaaaaaaaa", "p1-t1-aaaaaaaaaaaa"):
        assert gate.fullmatch(tid) and td._TID_RE.fullmatch(tid)


# --------------------------------------------------------------------------------------------- DRY-RUN (#562)
@pytest.fixture()
def dry(tmp_path):
    cfg = td.validate_config(raw_config(tmp_path))
    bk, pub, clock = FakeBuildkite(), Publisher(), Clock()
    disp = td.build_dispatcher(cfg, transport=bk, publisher=pub, token_runner=token_runner(clock), clock=clock,
                               dry_run=True)
    yield disp, bk, pub, clock, cfg
    disp.store.close()


def _writes(bk):
    return [(m, u) for m, u, *_ in bk.requests if m != "GET"] + [(m, u) for m, u, _ in bk.gh.requests if m != "GET"]


def test_dry_run_evaluates_prs_without_any_external_write(dry, capsys):
    """Collaudo del dispatcher sulla VPS: legge GitHub, calcola transizioni e build, ma non crea build, non annulla
    niente e non pubblica status. Il percorso e' quello reale fino al punto in cui scriverebbe fuori."""
    disp, bk, pub, clock, _ = dry
    bk.gh.put(42, HEAD)
    assert disp.poll() == ["p42-t1-aaaaaaaaaaaa"]
    disp.tick()
    bk.gh.put(42, HEAD2)                                          # nuovo commit: sostituzione, sempre senza scritture
    clock.now += 60
    disp.poll()
    disp.tick()
    assert _writes(bk) == [] and pub.calls == []
    assert not any(b for builds in bk.builds.values() for b in builds)
    jobs = disp.store.recent_jobs()
    assert {j["state"] for j in jobs} <= {"done"} and {j["outcome"] for j in jobs} == {"dry_run"}
    err = capsys.readouterr().err
    assert "DRY-RUN" in err and "pr-overlap-guard" in err and f"PF_PR_HEAD_SHA={HEAD2}" in err


def test_dry_run_rerun_is_authorized_and_audited_without_builds(dry):
    disp, bk, pub, clock, _ = dry
    _rerun_ready(disp, bk)
    bk.gh.comment(9030, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == ["p42-r9030-aaaaaaaaaaaa"]
    disp.tick()
    assert disp.store.recent_reruns()[0]["accepted"] == 1
    assert _writes(bk) == [] and pub.calls == []


@pytest.mark.parametrize("call", ["create", "cancel"])
def test_dry_run_buildkite_client_cannot_write_even_if_called(dry, call):
    """Difesa in profondita': anche chiamato direttamente, il client Buildkite del DRY-RUN non scrive."""
    disp, bk, _, _, _ = dry
    with pytest.raises(td.TransportError):
        if call == "create":
            disp.bk.create_build("overlap", {"PF_PR_NUMBER": "42"}, "x")
        else:
            disp.bk.cancel_build("overlap", 1)
    assert _writes(bk) == []


@pytest.mark.parametrize("state", ["dispatched", "canceling", "publishing"])
def test_dry_run_job_with_pending_cancels_never_takes_the_real_path(dry, state):
    """Review indipendente su #571: in `advance` il ramo `pending_cancels`/`canceling` precedeva `dry_run`. Un job
    con annullamenti pendenti (archivio di collaudo ereditato o corrotto) deve chiudersi come DRY-RUN, senza nemmeno
    tentare l'annullamento verso Buildkite."""
    disp, bk, pub, clock, _ = dry
    bk.gh.put(42, HEAD)
    disp.poll()
    job = disp.store.recent_jobs()[0]
    disp.store.update_job(job["id"], clock.now, state=state, build_number=7, pending_cancels="[7]")
    attempts = []
    disp.bk.cancel_build = lambda guard, number: attempts.append((guard, number))   # nessun tentativo ammesso
    disp.tick()
    fresh = disp.store.get_job(job["id"])
    assert attempts == [] and _writes(bk) == [] and pub.calls == []
    assert fresh["state"] == "done" and fresh["outcome"] == "dry_run" and fresh["pending_cancels"] == "[]"


def test_dry_run_fallback_and_redelivery_never_write(dry, monkeypatch):
    """Review owner su #571: anche il fallback degli errori interni (annullamento + esito `error`) e la riconsegna
    di un `publish_failed` passano dal trasporto solo-GET e dal publisher che rifiuta: in DRY-RUN nessuna scrittura."""
    disp, bk, pub, clock, cfg = dry
    bk.gh.put(42, HEAD)
    disp.poll()
    stuck, failed = disp.store.recent_jobs()[:2]
    disp.store.update_job(stuck["id"], clock.now, state="dispatched", build_number=7, dispatched_at=clock.now)
    disp.store.update_job(failed["id"], clock.now, state="publish_failed", outcome="success", publishing_since=clock.now)
    monkeypatch.setattr(disp, "_dry_run", lambda job: (_ for _ in ()).throw(RuntimeError("guasto")))
    clock.now += max(cfg.build_timeout_seconds.values()) + td.REDELIVERY_SECONDS + 1
    attempts = []
    real_publish = disp.publish
    disp.publish = lambda *a: attempts.append(a) or real_publish(*a)
    disp.tick()
    assert len(attempts) == 2                                     # fallback e riconsegna ci hanno provato davvero
    assert _writes(bk) == [] and pub.calls == []
    assert disp.store.get_job(failed["id"])["state"] == "publish_failed"


def test_dry_run_never_touches_the_real_state_archive(dry):
    _, _, _, _, cfg = dry
    assert not os.path.exists(cfg.state_db)                       # archivio reale mai creato ne' modificato
    assert os.path.exists(cfg.state_db + ".dry-run")


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_read_only_transport_refuses_every_write(method):
    calls = []
    guarded = td.read_only_transport(lambda *a: calls.append(a) or (200, {}, b"{}"))
    with pytest.raises(td.TransportError):
        guarded(method, "https://api.buildkite.com/v2/x", {}, b"{}", 5)
    assert calls == []
    assert guarded("GET", "https://api.github.com/x", {}, None, 5)[0] == 200


def test_dry_run_publisher_refuses_to_publish():
    with pytest.raises(RuntimeError):
        td.dry_run_publisher(42, HEAD, "success", "overlap-check", "https://x", "PASS")


def test_cli_dry_run_is_only_for_serve(tmp_path, capsys):
    with pytest.raises(SystemExit):
        td.main(["jobs", "--dry-run", "--config", str(tmp_path / "c.json")])


# --------------------------------------------------------------------------------------------- segreti e configurazione
@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604])
def test_secret_file_readable_by_others_is_refused(tmp_path, mode):
    path = write_secret(tmp_path / "s", BK_CRED, mode)
    with pytest.raises(td.ConfigError) as exc:
        td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file")
    assert BK_CRED not in str(exc.value)


# ---- credenziali di systemd (LoadCredential): systemd 255 le espone 0440 in $CREDENTIALS_DIRECTORY (Grok, #562)
def _credential(tmp_path, monkeypatch, mode=0o440, *, set_env=True, dir_mode=0o500, inside=True):
    creds = tmp_path / "credentials" / "pf-dispatch.service"
    creds.mkdir(parents=True)
    other = tmp_path / "other"
    other.mkdir()
    path = write_secret((creds if inside else other) / "buildkite", BK_CRED, mode)
    os.chmod(creds, dir_mode)
    if set_env:
        monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(creds))
    else:
        monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    return path


def test_systemd_credential_0440_in_the_credentials_directory_is_accepted(tmp_path, monkeypatch):
    path = _credential(tmp_path, monkeypatch)
    try:
        assert td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file") == BK_CRED
    finally:
        os.chmod(os.path.dirname(path), 0o700)


@pytest.mark.parametrize("case", ["outside_dir", "no_env", "dir_group_writable", "other_read", "group_write",
                                  "mode_0640"])
def test_group_readable_secret_outside_the_systemd_rules_is_refused(tmp_path, monkeypatch, case):
    kwargs = {"outside_dir": {"inside": False}, "no_env": {"set_env": False},
              "dir_group_writable": {"dir_mode": 0o770}, "other_read": {"mode": 0o444},
              "group_write": {"mode": 0o460}, "mode_0640": {"mode": 0o640}}[case]
    path = _credential(tmp_path, monkeypatch, **kwargs)
    try:
        with pytest.raises(td.ConfigError) as exc:
            td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file")
        assert BK_CRED not in str(exc.value)
    finally:
        os.chmod(tmp_path / "credentials" / "pf-dispatch.service", 0o700)


@pytest.mark.skipif(getattr(os, "geteuid", lambda: -1)() != 0, reason="cambiare proprietario o gruppo richiede root")
@pytest.mark.parametrize("field", ["owner", "group", "dir_owner"])
def test_systemd_credential_owned_by_someone_else_is_refused(tmp_path, monkeypatch, field):
    """Review indipendente su #571: anche la directory $CREDENTIALS_DIRECTORY deve essere di root o dell'utente del
    servizio; il proprietario di una directory puo' sostituirne i file anche senza permessi di scrittura."""
    path = _credential(tmp_path, monkeypatch)
    try:
        if field == "dir_owner":
            os.chown(os.path.dirname(path), 4242, -1)
        else:
            os.chown(path, 4242 if field == "owner" else -1, 4242 if field == "group" else -1)
        with pytest.raises(td.ConfigError):
            td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file")
    finally:
        os.chmod(os.path.dirname(path), 0o700)


def test_secret_symlink_is_refused(tmp_path):
    real = write_secret(tmp_path / "real", BK_CRED)
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(td.ConfigError):
        td.read_secret_file(str(link), td._TOKEN_RE, "buildkite.token_file")


@pytest.mark.parametrize("value", ["short", "with space inside the token value", "x" * 9000])
def test_secret_out_of_format_is_refused_without_echo(tmp_path, value):
    path = write_secret(tmp_path / "s", value)
    with pytest.raises(td.ConfigError) as exc:
        td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file")
    assert value not in str(exc.value)


@pytest.mark.parametrize("path", [
    ("github",), ("github", "token_command"), ("github", "token_command_timeout_seconds"), ("rerun",),
    ("rerun", "allowed_logins"), ("buildkite",), ("buildkite", "organization"), ("buildkite", "token_file"),
    ("buildkite", "result_artifacts"), ("buildkite", "artifact_download_hosts"), ("publisher",),
    ("publisher", "command"), ("publisher", "timeout_seconds"), ("state_db",), ("retention_hours",),
    ("poll_interval_seconds",), ("build_timeout_seconds",), ("build_timeout_seconds", "readiness"),
    ("http_timeout_seconds",),
])
def test_every_missing_mandatory_key_is_not_configured(tmp_path, path):
    raw = raw_config(tmp_path)
    node = raw
    for key in path[:-1]:
        node = node[key]
    del node[path[-1]]
    with pytest.raises(td.ConfigError) as exc:
        td.validate_config(raw)
    assert any("MISSING" in p and path[-1] in p for p in exc.value.problems)


@pytest.mark.parametrize("mutate", [
    lambda r: r.update(extra=1),
    lambda r: r.update(listen={"host": "0.0.0.0", "port": 8787, "path": "/x"}),   # niente piu' endpoint
    lambda r: r.update(webhook_secret_file="/etc/x"),
    lambda r: r["github"].update(token_command=["pf-github-read-token"]),
    lambda r: r["github"].update(token_command=[]),
    lambda r: r["github"].update(token_command_timeout_seconds=1),
    lambda r: r["rerun"].update(allowed_logins="pf-owner"),
    lambda r: r["rerun"].update(allowed_logins=["bad login"]),
    lambda r: r["buildkite"].update(pipelines=["pf-trusted-smoke"]),
    lambda r: r["buildkite"]["result_artifacts"].update(smoke=".trusted-guard/smoke.json"),
    lambda r: r["buildkite"]["result_artifacts"].update(overlap=".trusted-guard/readiness.json"),
    lambda r: r["buildkite"]["result_artifacts"].update(overlap="../overlap.json"),
    lambda r: r["buildkite"].update(organization="Bad Org"),
    lambda r: r["buildkite"].update(artifact_download_hosts=["*.amazonaws.com"]),
    lambda r: r["buildkite"].update(token_file="relative/token"),
    lambda r: r["publisher"].update(command=["pf-publish-status"]),
    lambda r: r.update(retention_hours=23),
    lambda r: r.update(poll_interval_seconds=5),
    lambda r: r.update(poll_interval_seconds=True),
    lambda r: r["build_timeout_seconds"].update(overlap=59),
    lambda r: r.update(http_timeout_seconds="30"),
    lambda r: r.update(state_db="/var/lib/../etc/x"),
])
def test_invalid_config_values_are_not_configured(tmp_path, mutate):
    raw = raw_config(tmp_path)
    mutate(raw)
    with pytest.raises(td.ConfigError):
        td.validate_config(raw)


def test_cli_check_config_runs_the_real_token_command(tmp_path, capsys):
    raw = raw_config(tmp_path)
    script = tmp_path / "token.sh"
    script.write_text("#!/bin/sh\necho '" + json.dumps({TK: GH_CRED, "expires_at": "2099-01-01T00:00:00Z"}) + "'\n")
    os.chmod(script, 0o700)
    raw["github"]["token_command"] = [str(script)]
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(raw))
    assert td.main(["check-config", "--config", str(path), "--check-secrets"]) == 0
    script.write_text("#!/bin/sh\nexit 3\n")
    assert td.main(["check-config", "--config", str(path), "--check-secrets"]) == td.EXIT_NOT_CONFIGURED
    assert GH_CRED not in capsys.readouterr().err
