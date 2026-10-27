"""Test del dispatcher trusted (#562, PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01).

Offline e deterministici: un finto Buildkite che riproduce l'API REST v2 (creazione, lettura, elenco, annullamento,
artifact con redirect) e un publisher che registra le chiamate al posto dell'helper root `pf-publish-status`.
Il percorso reale (VPS, webhook GitHub, Buildkite, App) e' NOT VERIFIED: lo collauda Grok (docs/trusted_dispatcher.md).
"""
from __future__ import annotations

import hashlib
import hmac
import http.client
import importlib.util
import json
import os
import sys
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("pr_trusted_dispatcher", ROOT / "scripts" / "pr_trusted_dispatcher.py")
td = importlib.util.module_from_spec(_spec)
sys.modules["pr_trusted_dispatcher"] = td
_spec.loader.exec_module(td)

REPO = "zarbopiero963-droid/Pickfair-nogui"
ORG = "pf-org-test"
SECRET = "webhook-secret-for-tests-0123456789"
TOKEN = "bkua_test_token_0123456789abcdef0123"
HEAD = "a" * 40
HEAD2 = "b" * 40
S3 = "artifacts.example-s3.test"


# --------------------------------------------------------------------------------------------- fixture
def write_secret(path: Path, value: str, mode: int = 0o600) -> str:
    path.write_text(value + "\n", encoding="ascii")
    os.chmod(path, mode)
    return str(path)


def raw_config(tmp_path: Path) -> dict:
    return {
        "listen": {"host": "127.0.0.1", "port": 8787, "path": "/github/webhook"},
        "webhook_secret_file": write_secret(tmp_path / "hmac", SECRET),
        "buildkite": {
            "organization": ORG,
            "token_file": write_secret(tmp_path / "bk-token", TOKEN),
            "result_artifacts": {"overlap": ".trusted-guard/overlap.json",
                                 "readiness": ".trusted-guard/readiness.json"},
            "artifact_download_hosts": [S3],
        },
        "publisher": {"command": ["/usr/local/sbin/pf-publish-status"], "timeout_seconds": 60},
        "state_db": str(tmp_path / "state.sqlite3"),
        "delivery_retention_hours": 168,
        "poll_interval_seconds": 5,
        "build_timeout_seconds": {"overlap": 600, "readiness": 900},
        "http_timeout_seconds": 30,
    }


class Clock:
    def __init__(self) -> None:
        self.now = 1_800_000_000.0

    def __call__(self) -> float:
        return self.now


class FakeBuildkite:
    """API REST v2 minima. `fail_create` = 'transport' | 'http500' | 'http422'; `builds` per pipeline."""

    def __init__(self) -> None:
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
        self.requests.append((method, url, dict(headers), body))
        if url.startswith(f"https://{S3}/") or url.startswith(f"https://{self.redirect_host}/"):
            key = url.rsplit("/", 2)
            return 200, {}, self.artifacts[(key[-2], int(key[-1]))]
        prefix = f"{td.BUILDKITE_API}/organizations/{ORG}/pipelines/"
        assert url.startswith(prefix), url
        assert headers.get("Authorization") == f"Bearer {TOKEN}"
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


@pytest.fixture()
def env(tmp_path):
    cfg = td.validate_config(raw_config(tmp_path))
    bk = FakeBuildkite()
    pub = Publisher()
    clock = Clock()
    disp = td.build_dispatcher(cfg, transport=bk, publisher=pub)
    disp.clock = clock
    yield disp, bk, pub, clock
    disp.store.close()


def payload(*, pr=42, head=HEAD, action="synchronize", repo=REPO, head_repo=REPO, base="main", state="open",
            draft=False, updated="2026-10-27T10:00:00Z") -> dict:
    return {
        "action": action, "number": pr,
        "repository": {"full_name": repo},
        "pull_request": {
            "number": pr, "state": state, "draft": draft, "updated_at": updated,
            "base": {"ref": base, "repo": {"full_name": repo}},
            "head": {"sha": head, "repo": None if head_repo is None else {"full_name": head_repo}},
        },
    }


def signed(body: bytes, *, delivery="0f3c2a10-aaaa-bbbb-cccc-000000000001", event="pull_request",
           secret=SECRET) -> dict:
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {"X-Hub-Signature-256": f"sha256={sig}", "X-GitHub-Event": event, "X-GitHub-Delivery": delivery}


def send(disp, data, **kw):
    body = json.dumps(data).encode()
    return disp.accept(signed(body, **kw), body)


def result(guard, *, verdict="PASS", pr=42, head=HEAD, repo=REPO, reasons=None):
    return {"guard": guard, "repo": repo, "pr": pr, "head_sha": head, "verdict": verdict,
            "reasons": reasons if reasons is not None else ([] if verdict == "PASS" else ["sovrapposizione"])}


# --------------------------------------------------------------------------------------------- evento valido
def test_valid_event_dispatches_exactly_two_trusted_builds_with_contract_env(env):
    disp, bk, pub, _ = env
    code, _ = send(disp, payload())
    assert code == 202
    disp.tick()
    posts = [r for r in bk.requests if r[0] == "POST"]
    assert sorted(r[1].rsplit("/", 2)[-2] for r in posts) == ["pr-merge-readiness", "pr-overlap-guard"]
    for _, _, _, body in posts:
        sent = json.loads(body)
        assert sent["branch"] == "main" and sent["commit"] == "HEAD"
        assert sent["env"] == {"PF_PR_NUMBER": "42", "PF_PR_HEAD_SHA": HEAD, "PF_PR_BASE_REF": "main",
                               "PF_EVENT": "synchronize", "PF_DELIVERY_ID": "0f3c2a10-aaaa-bbbb-cccc-000000000001"}
    assert {c[3] for c in pub.calls} == {"overlap-check", "pr-merge-readiness"}
    assert all(c[2] == "pending" for c in pub.calls)


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


# --------------------------------------------------------------------------------------------- evento falsificato
@pytest.mark.parametrize("mangle", [
    lambda h, b: {k: v for k, v in h.items() if k != "X-Hub-Signature-256"},
    lambda h, b: {**h, "X-Hub-Signature-256": "sha1=" + "0" * 40},
    lambda h, b: {**h, "X-Hub-Signature-256": h["X-Hub-Signature-256"].upper()},
    lambda h, b: {**h, "X-Hub-Signature-256": "sha256=" + "0" * 64},
    lambda h, b: signed(b, secret="un-altro-secret-di-attaccante-000"),
])
def test_forged_or_unsigned_event_is_rejected_before_parsing(env, mangle):
    disp, bk, pub, _ = env
    body = json.dumps(payload()).encode()
    code, _ = disp.accept(mangle(signed(body), body), body)
    assert code == 401
    disp.tick()
    assert not bk.requests and not pub.calls and not disp.store.recent_jobs()


def test_signature_over_a_different_body_is_rejected(env):
    disp, bk, _, _ = env
    good = json.dumps(payload()).encode()
    evil = json.dumps(payload(head=HEAD2)).encode()
    assert disp.accept(signed(good), evil)[0] == 401
    assert not disp.store.recent_jobs()


def test_garbage_body_with_valid_signature_is_400(env):
    disp, _, _, _ = env
    body = b"\xff\xfenot json"
    assert disp.accept(signed(body), body)[0] == 400


# --------------------------------------------------------------------------------------------- dati PR
@pytest.mark.parametrize("data,code", [
    ({k: v for k, v in payload().items() if k != "pull_request"}, 422),            # PR mancante
    (payload(pr=0), 422),
    (payload(pr=True), 422),                                                        # bool non e' un numero
    (payload(pr="42"), 422),
    (payload(pr=10_000_000), 422),
    (payload(head="A" * 40), 422),
    (payload(head="a" * 39), 422),
    (payload(repo="evil/Pickfair-nogui"), 422),                                     # repo sbagliato
    (payload(updated="ieri"), 422),
    (payload(draft="false"), 422),
    (payload(state="merged"), 422),
    (payload(head_repo="attacker/Pickfair-nogui"), 200),                            # fork: scartato
    (payload(head_repo=None), 200),                                                 # repo cancellato
    (payload(base="develop"), 200),                                                 # base non main
    (payload(draft=True), 200),
    (payload(state="closed"), 200),
    (payload(action="closed"), 200),
    (payload(action="edited"), 200),
    (payload(action="labeled"), 200),
])
def test_invalid_or_out_of_contract_pr_data_never_dispatches(env, data, code):
    disp, bk, _, _ = env
    assert send(disp, data)[0] == code
    disp.tick()
    assert not bk.requests and not disp.store.recent_jobs()


def test_payload_number_disagreeing_with_pr_number_is_rejected(env):
    disp, _, _, _ = env
    data = payload()
    data["number"] = 43
    assert send(disp, data)[0] == 422


@pytest.mark.parametrize("event,code", [("push", 200), ("pull_request_review", 200), (None, 200), ("ping", 200)])
def test_other_events_never_dispatch(env, event, code):
    disp, bk, _, _ = env
    body = json.dumps(payload()).encode()
    headers = signed(body)
    if event is None:
        headers.pop("X-GitHub-Event")
    else:
        headers["X-GitHub-Event"] = event
    assert disp.accept(headers, body)[0] == code
    disp.tick()
    assert not bk.requests


@pytest.mark.parametrize("delivery", ["", "short", "x;rm -rf /", "a" * 65, "ü" * 10])
def test_malformed_delivery_id_is_rejected(env, delivery):
    disp, _, _, _ = env
    assert send(disp, payload(), delivery=delivery)[0] == 422


# --------------------------------------------------------------------------------------------- replay / duplicati / ordine
def test_replayed_delivery_is_ignored(env):
    disp, bk, _, _ = env
    assert send(disp, payload())[0] == 202
    assert send(disp, payload())[0] == 200
    disp.tick()
    assert len([r for r in bk.requests if r[0] == "POST"]) == 2


def test_replay_is_remembered_across_restart(env, tmp_path):
    disp, _, _, _ = env
    send(disp, payload())
    disp.store.close()
    disp2 = td.build_dispatcher(disp.cfg, transport=FakeBuildkite(), publisher=Publisher())
    try:
        assert send(disp2, payload())[0] == 200
    finally:
        disp2.store.close()
    disp.store = td.Store(disp.cfg.state_db)


def test_duplicate_head_with_new_delivery_does_not_start_second_builds(env):
    disp, bk, _, _ = env
    send(disp, payload(action="synchronize"))
    disp.tick()
    code, text = send(disp, payload(action="reopened"), delivery="0f3c2a10-aaaa-bbbb-cccc-000000000002")
    assert code == 202 and "gia' in corso" in text
    disp.tick()
    assert len([r for r in bk.requests if r[0] == "POST"]) == 2


def test_same_head_after_completion_runs_again(env):
    disp, bk, _, _ = env
    send(disp, payload())
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    send(disp, payload(action="reopened", updated="2026-10-27T10:05:00Z"),
         delivery="0f3c2a10-aaaa-bbbb-cccc-000000000003")
    disp.tick()
    assert len([r for r in bk.requests if r[0] == "POST"]) == 4


def test_concurrent_identical_deliveries_record_one_event(env):
    disp, _, _, _ = env
    results: list[int] = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(send(disp, payload())[0])

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [200] * 7 + [202]
    assert len(disp.store.recent_jobs()) == 2


def test_out_of_order_older_event_does_not_supersede_newer_head(env):
    disp, bk, _, _ = env
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:10:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-00000000000b")
    code, text = send(disp, payload(head=HEAD, updated="2026-10-27T10:00:00Z"))
    assert code == 200 and "piu' vecchio" in text
    disp.tick()
    assert {json.loads(r[3])["env"]["PF_PR_HEAD_SHA"] for r in bk.requests if r[0] == "POST"} == {HEAD2}


# --------------------------------------------------------------------------------------------- head cambiato / stale SHA
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


# --------------------------------------------------------------------------------------------- esiti della build
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


# --------------------------------------------------------------------------------------------- errori API e ripresa
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
    disp2 = td.build_dispatcher(disp.cfg, transport=bk, publisher=pub2)
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
    disp2 = td.build_dispatcher(disp.cfg, transport=bk, publisher=Publisher())
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


# --------------------------------------------------------------------------------------------- pipeline / contesti
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


# --------------------------------------------------------------------------------------------- segreti
def test_build_env_never_contains_secrets(env):
    disp, bk, pub, _ = env
    send(disp, payload())
    disp.tick()
    for method, url, headers, body in bk.requests:
        if body:
            assert TOKEN not in body.decode() and SECRET not in body.decode()
        assert TOKEN not in url
    for call in pub.calls:
        assert not any(TOKEN in str(a) or SECRET in str(a) for a in call)


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


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604])
def test_secret_file_readable_by_others_is_refused(tmp_path, mode):
    path = write_secret(tmp_path / "s", SECRET, mode)
    with pytest.raises(td.ConfigError) as exc:
        td.read_secret_file(path, td._SECRET_RE, "webhook_secret_file")
    assert SECRET not in str(exc.value)


def test_secret_symlink_is_refused(tmp_path):
    real = write_secret(tmp_path / "real", SECRET)
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(td.ConfigError):
        td.read_secret_file(str(link), td._SECRET_RE, "webhook_secret_file")


@pytest.mark.parametrize("value", ["short", "with space inside the secret value", "x" * 5000])
def test_secret_out_of_format_is_refused_without_echo(tmp_path, value):
    path = write_secret(tmp_path / "s", value)
    with pytest.raises(td.ConfigError) as exc:
        td.read_secret_file(path, td._SECRET_RE, "webhook_secret_file")
    assert value not in str(exc.value)


def test_default_publisher_gets_no_inherited_environment(tmp_path, monkeypatch):
    script = tmp_path / "pub.sh"
    out = tmp_path / "out"
    script.write_text(f"#!/bin/sh\nenv > {out}\nprintf '%s\\n' \"$@\" >> {out}\n")
    os.chmod(script, 0o700)
    monkeypatch.setenv("BUILDKITE_TOKEN_LEAK", TOKEN)
    rc = td.default_publisher((str(script),), 10)(42, HEAD, "success", "overlap-check",
                                                   "https://buildkite.com/x/pr-overlap-guard/builds/1", "PASS")
    text = out.read_text()
    assert rc == 0 and TOKEN not in text
    assert text.splitlines()[-6:] == ["42", HEAD, "success", "overlap-check",
                                      "https://buildkite.com/x/pr-overlap-guard/builds/1", "PASS"]


def test_tokens_never_logged(env, capsys):
    disp, bk, pub, clock = env
    bk.fail_create = "http422"
    send(disp, payload())
    disp.tick()
    err = capsys.readouterr().err
    assert TOKEN not in err and SECRET not in err


# --------------------------------------------------------------------------------------------- configurazione
@pytest.mark.parametrize("path", [
    ("listen",), ("listen", "host"), ("listen", "port"), ("listen", "path"), ("webhook_secret_file",),
    ("buildkite",), ("buildkite", "organization"), ("buildkite", "token_file"), ("buildkite", "result_artifacts"),
    ("buildkite", "artifact_download_hosts"), ("publisher",), ("publisher", "command"), ("state_db",),
    ("delivery_retention_hours",), ("poll_interval_seconds",), ("build_timeout_seconds",),
    ("build_timeout_seconds", "readiness"), ("http_timeout_seconds",),
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
    lambda r: r["buildkite"].update(pipelines=["pf-trusted-smoke"]),
    lambda r: r["buildkite"]["result_artifacts"].update(smoke=".trusted-guard/smoke.json"),
    lambda r: r["buildkite"]["result_artifacts"].update(overlap=".trusted-guard/readiness.json"),
    lambda r: r["buildkite"]["result_artifacts"].update(overlap="../overlap.json"),
    lambda r: r["buildkite"]["result_artifacts"].update(overlap="/abs/overlap.json"),
    lambda r: r["buildkite"].update(organization="Bad Org"),
    lambda r: r["buildkite"].update(artifact_download_hosts=[]),
    lambda r: r["buildkite"].update(artifact_download_hosts=["*.amazonaws.com"]),
    lambda r: r["buildkite"].update(token_file="relative/token"),
    lambda r: r["listen"].update(port=True),
    lambda r: r["listen"].update(port=0),
    lambda r: r["listen"].update(path="no-slash"),
    lambda r: r["publisher"].update(command=["pf-publish-status"]),
    lambda r: r["publisher"].update(command=[]),
    lambda r: r.update(delivery_retention_hours=23),
    lambda r: r.update(delivery_retention_hours=168.0),
    lambda r: r.update(poll_interval_seconds=0),
    lambda r: r["build_timeout_seconds"].update(overlap=59),
    lambda r: r.update(http_timeout_seconds="30"),
    lambda r: r.update(state_db="/var/lib/../etc/x"),
])
def test_invalid_config_values_are_not_configured(tmp_path, mutate):
    raw = raw_config(tmp_path)
    mutate(raw)
    with pytest.raises(td.ConfigError):
        td.validate_config(raw)


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


def test_cli_check_config_with_secrets(tmp_path, capsys):
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(raw_config(tmp_path)))
    assert td.main(["check-config", "--config", str(path), "--check-secrets"]) == 0
    os.chmod(tmp_path / "bk-token", 0o644)
    assert td.main(["check-config", "--config", str(path), "--check-secrets"]) == td.EXIT_NOT_CONFIGURED


def test_retention_purges_old_deliveries_only_after_the_window(env):
    disp, _, _, clock = env
    send(disp, payload(action="opened", state="open"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000aa")
    clock.now += 167 * 3600
    disp.tick()
    assert send(disp, payload(), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000aa")[0] == 200   # ancora replay
    clock.now += 2 * 3600
    disp.store.purge(clock.now, 168)
    with disp.store._lock:
        left = disp.store._db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    assert left == 0


# --------------------------------------------------------------------------------------------- server HTTP reale
def test_http_server_end_to_end(env):
    disp, bk, _, _ = env
    server = ThreadingHTTPServer(("127.0.0.1", 0), td.make_handler(disp))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        body = json.dumps(payload()).encode()

        def post(path, data, headers):
            req = urllib.request.Request(base + path, data=data, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 - loopback di test
                    return resp.status
            except urllib.error.HTTPError as exc:
                return exc.code

        assert post("/github/webhook", body, signed(body)) == 202
        assert post("/github/webhook", body, {**signed(body), "X-Hub-Signature-256": "sha256=" + "0" * 64}) == 401
        assert post("/other", body, signed(body)) == 404
        conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        conn.putrequest("POST", "/github/webhook")
        conn.putheader("Content-Length", str(td.MAX_BODY_BYTES + 1))
        conn.endheaders()
        assert conn.getresponse().status == 413
        conn.close()
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(base + "/github/webhook", timeout=5)  # noqa: S310
        assert exc.value.code == 405
    finally:
        server.shutdown()
        server.server_close()
    assert len(disp.store.recent_jobs()) == 2


# --------------------------------------------------------------------------------------------- rilievi della review indipendente
def test_force_push_back_to_an_earlier_head_dispatches_that_head(env):
    """P2-1: A -> B -> di nuovo A prima del tick. I job vecchi di A, gia' sostituiti, non devono occupare il posto
    dei job nuovi di A: l'head corrente resterebbe senza build e con `pending` per sempre."""
    disp, bk, pub, _ = env
    send(disp, payload(head=HEAD, updated="2026-10-27T10:00:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000c1")
    disp.tick()
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:01:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000c2")
    code, _ = send(disp, payload(head=HEAD, updated="2026-10-27T10:02:00Z"),
                   delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000c3")
    assert code == 202
    disp.tick()
    disp.tick()
    active = disp.store.active_jobs()
    assert {(j["head_sha"], j["guard"]) for j in active} == {(HEAD, "overlap"), (HEAD, "readiness")}
    assert all(j["state"] == "dispatched" for j in active)
    bk.finish("pr-overlap-guard", 2, "passed", result("overlap"))
    disp.tick()
    assert (42, HEAD, "success", "overlap-check") in [c[:4] for c in pub.final()]


def test_events_with_the_same_updated_at_do_not_supersede_each_other(env):
    """P3-1: due push nello stesso secondo consegnati in ordine inverso. Con lo stesso `updated_at` l'ordine non e'
    noto: entrambi gli head restano in corsa e decide il ricontrollo dell'helper, mai l'ultimo arrivato."""
    disp, bk, _, _ = env
    send(disp, payload(head=HEAD2, updated="2026-10-27T10:00:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000d1")
    send(disp, payload(head=HEAD, updated="2026-10-27T10:00:00Z"), delivery="0f3c2a10-aaaa-bbbb-cccc-0000000000d2")
    disp.tick()
    heads = {json.loads(r[3])["env"]["PF_PR_HEAD_SHA"] for r in bk.requests if r[0] == "POST"}
    assert heads == {HEAD, HEAD2}
    assert not any(j["superseded"] for j in disp.store.recent_jobs())


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


@pytest.mark.parametrize("value", ["²", "+5", "-1", "1e3", "123456789"])
def test_non_ascii_or_oversized_content_length_is_refused(env, value):
    """P3-2: `'²'.isdigit()` e' vero e `int()` solleva: il gestore non deve morire senza risposta."""
    disp, _, _, _ = env
    server = ThreadingHTTPServer(("127.0.0.1", 0), td.make_handler(disp))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        conn.putrequest("POST", "/github/webhook", skip_accept_encoding=True)
        conn.putheader("Content-Length", value)    # http.client codifica in latin-1: arriva '\u00b2' com'e'
        conn.endheaders()
        assert conn.getresponse().status in (411, 413)
        conn.close()
    finally:
        server.shutdown()
        server.server_close()


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


# --------------------------------------------------------------------------------------------- rilievi Codex su #569
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
        disp = td.build_dispatcher(cfg, transport=bk, publisher=pub)
        cols = {r[1] for r in disp.store._db.execute("PRAGMA table_info(jobs)")}
        indexes = {r[1] for r in disp.store._db.execute("PRAGMA index_list(jobs)")}
        disp.store.close()
        assert {"publishing_since", "pending_published"} <= cols
        assert "jobs_active" not in indexes and "jobs_active_current" in indexes
    disp = td.build_dispatcher(cfg, transport=bk, publisher=pub)
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



# --------------------------------------------------------------------------------------------- audit indipendente (liveness)
def _overlap(disp):
    return [j for j in disp.store.recent_jobs() if j["guard"] == "overlap"][0]


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


# --------------------------------------------------------------------------------------------- Codex su cee448c
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
