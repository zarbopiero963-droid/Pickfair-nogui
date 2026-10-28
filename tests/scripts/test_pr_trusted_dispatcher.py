"""Test del dispatcher trusted (#562, PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01, intake a polling: PF-562 opzione B).

Offline e deterministici: un finto GitHub REST (elenco PR aperte e commenti, paginazione, rate limit, errori) e un
finto Buildkite che riproduce l'API REST v2 (creazione, lettura, elenco, annullamento, artifact con redirect), piu'
un publisher che registra le chiamate al posto dell'helper root `pf-publish-status`. Il percorso reale (VPS, GitHub
App, Buildkite) e' NOT VERIFIED: lo collauda Grok (docs/trusted_dispatcher.md).
"""
from __future__ import annotations

import errno
import importlib.util
import json
import os
import struct
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
REPO_ID = 1189713160                             # id numerico del repository fidato (API GitHub)
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
        "readiness_wait": {"enabled": False, "max_seconds": 7200, "settle_polls": 2, "min_delay_seconds": 120},
        "full_suite": {"enabled": False, "build_timeout_seconds": 10800},
    }


class Clock:
    def __init__(self) -> None:
        self.now = 1_800_000_000.0

    def __call__(self) -> float:
        return self.now


class FakeGitHub:
    """`GET /repos/<repo>/pulls` e `GET /repos/<repo>/issues/comments` con paginazione `Link: rel="next"`. Come
    l'API reale, il link `next` usa la forma `/repositories/<id numerico>/...` (#562, rollback T252 di #576);
    `next_link_hook(url)` permette di manometterlo."""

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
        self.check_runs: dict[str, list] = {}          # sha -> check run (attesa della readiness)
        self.statuses: dict[str, list] = {}            # sha -> status, dal piu' recente
        self.checks_status = 200
        self.total_count_delta = 0
        self.next_link_hook = None                     # url -> url: link `next` manomesso
        self.link_style = "repositories"               # "repos" = forma canonica, mai usata da GitHub nei Link

    def _next_link(self, path, query, page):
        nxt = dict(query, page=str(page + 1))
        if self.link_style == "repositories":
            path = path.replace(f"/repos/{REPO}/", f"/repositories/{REPO_ID}/", 1)
        url = f"https://api.github.com{path}?{urllib.parse.urlencode(nxt)}"
        return f'<{self.next_link_hook(url) if self.next_link_hook else url}>; rel="next"'

    def put(self, pr, head, *, draft=False, base="main", head_repo=REPO, ref="feature/pr-branch") -> None:
        self.pulls[pr] = {"number": pr, "state": "open", "draft": draft,
                          "base": {"ref": base, "repo": {"full_name": REPO}},
                          "head": {"sha": head, "ref": ref,
                                   "repo": None if head_repo is None else {"full_name": head_repo}}}

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
        assert parts.path.startswith(f"/repos/{REPO}/"), "il client richiede solo l'URL canonico, mai il link ricevuto"
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
        elif parts.path.startswith(f"/repos/{REPO}/commits/"):
            sha, kind = parts.path.split("/")[-2:]
            if self.checks_status != 200:
                return self.checks_status, {}, b'{"message":"x"}'
            data = self.check_runs.get(sha, []) if kind == "check-runs" else self.statuses.get(sha, [])
            chunk = data[(page - 1) * self.page_size: page * self.page_size]
            if page * self.page_size < len(data):
                headers_out["Link"] = self._next_link(parts.path, query, page)
            body = ({"total_count": len(data) + self.total_count_delta, "check_runs": chunk}
                    if kind == "check-runs" else chunk)
            return 200, headers_out, json.dumps(body).encode()
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
            headers_out["Link"] = self._next_link(parts.path, query, page)
        if self.break_page == page:
            return 200, headers_out, b'{"not": "a list"}'
        return 200, headers_out, json.dumps(chunk).encode()


class FakeBuildkite:
    """API REST v2 minima. `fail_create` = 'transport' | 'http500' | 'http422'; `builds` per pipeline. Inoltra le
    richieste a api.github.com al finto GitHub `gh`."""

    def __init__(self) -> None:
        self.gh = FakeGitHub()
        self.builds: dict[str, dict[int, dict]] = {"pr-overlap-guard": {}, "pr-merge-readiness": {},
                                                    "pf-dedicated-ci": {}}
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
        self.fail_list_intent = False      # elenco per metadato illeggibile (#562 P0)
        self.full_page_intent = False      # elenco per metadato a pagina piena: assenza non verificabile
        self.on_create = None              # hook(pipeline, payload) prima della risposta: creazione concorrente
        self.clock = None                  # con un Clock: `created_at` sulle build e filtro `created_from` reale
    def _new_build(self, pipeline: str, payload: dict) -> dict:
        number = len(self.builds[pipeline]) + 1
        build = {"number": number, "state": "scheduled", "branch": payload["branch"], "commit": payload["commit"],
                 "env": dict(payload["env"]), "message": payload["message"],
                 "meta_data": dict(payload.get("meta_data") or {}),
                 **({"created_at": td.iso(self.clock.now)} if self.clock else {}),
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
            if self.on_create:
                self.on_create(pipeline, payload)
            return 201, {}, json.dumps(self._new_build(pipeline, payload)).encode()
        if method == "GET" and tail.startswith("builds?"):
            query = dict(urllib.parse.parse_qsl(tail.partition("?")[2]))
            intent = query.get("meta_data[pf_intent]")
            if intent is not None:
                # l'API reale filtra per metadato: solo le build con quel `pf_intent`, a qualunque data
                if self.fail_list_intent:
                    raise td.TransportError("rete")
                if self.full_page_intent:
                    return 200, {}, json.dumps([{"number": n} for n in range(1, 101)]).encode()
                hits = [b for b in self.builds[pipeline].values() if b.get("meta_data", {}).get("pf_intent") == intent]
                return 200, {}, json.dumps(hits).encode()
            if self.fail_list:
                raise td.TransportError("rete")
            since = query.get("created_from", "")
            return 200, {}, json.dumps([b for b in self.builds[pipeline].values()
                                        if b.get("created_at", since) >= since]).encode()
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


def test_full_window_page_is_not_proof_of_absence_even_with_an_empty_intent_listing(env):
    """Audit A8 con #562 P0: l'elenco per intento vuoto non compensa una finestra a pagina piena (una build creata
    senza metadato, o non ancora indicizzata, puo' stare nella pagina dopo): scaduta la finestra l'esito resta
    «non verificabile», mai «non confermata» come per un'assenza dimostrata."""
    disp, bk, pub, clock = env
    bk.fail_create = "transport"
    send(disp, payload())
    disp.tick()
    bk.fail_create = None
    orig = bk.__call__

    def tr(method, url, headers, body, timeout):
        if method == "GET" and "builds?" in url and "pf_intent" in urllib.parse.unquote(url):
            return 200, {}, b"[]"
        if method == "GET" and "builds?" in url:
            return 200, {}, json.dumps([{"number": 1000 + i, "env": {}} for i in range(100)]).encode()
        return orig(method, url, headers, body, timeout)
    disp.bk._transport = tr
    disp.tick()
    assert {j["state"] for j in disp.store.active_jobs()} == {"creating"} and not pub.final()
    clock.now += disp.cfg.build_timeout_seconds["readiness"] + 1
    disp.tick()
    descs = [c[5] for c in pub.final()]
    assert descs and all("non verificabile" in d for d in descs)


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
        assert set(sent) == {"commit", "branch", "message", "env", "meta_data"}
        # identita' stabile dell'intento (#562 P0): solo il tid, mai altri dati
        assert sent["meta_data"] == {"pf_intent": sent["env"]["PF_DELIVERY_ID"]}
        assert set(sent["env"]) == {"PF_PR_NUMBER", "PF_PR_HEAD_SHA", "PF_PR_BASE_REF", "PF_EVENT", "PF_DELIVERY_ID"}


def test_dispatcher_has_no_web_server_and_no_code_fetching_paths():
    source = (ROOT / "scripts" / "pr_trusted_dispatcher.py").read_text(encoding="utf-8")
    for forbidden in ("http.server", "socketserver", "/contents", "/tarball", "/zipball", "git clone",
                      "checkout"):
        assert forbidden not in source, forbidden
    # nessun socket in ascolto: la chiamata, non la parola (lo step `w2-telegram-listener` della full-suite la
    # contiene come nome di un controllo, #562 PR-A)
    import re
    for call in (r"\.listen\s*\(", r"\.bind\s*\(", r"\.accept\s*\("):
        assert not re.search(call, source), call
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


# ---- D1 (Grok, #562 c.6450558425): systemd 255 espone la credenziale 0440 root:root nella directory 0550 root:root;
# il servizio (pf-dispatch, uid/gid 983) la legge tramite una voce ACL, non tramite il gruppo
SERVICE_ID = 983
ACL_XATTR = "system.posix_acl_access"


def _acl(user=SERVICE_ID, group=None):
    """ACL POSIX come la scrive systemd: proprietario r, voce nominativa per l'utente del servizio, mask r, altri 0."""
    none = 0xFFFFFFFF
    entries = [(0x01, 4, none)] + ([(0x02, 4, user)] if user is not None else []) + [(0x04, 4, none)]
    entries += ([(0x08, 4, group)] if group is not None else []) + [(0x10, 4, none), (0x20, 0, none)]
    return struct.pack("<I", 2) + b"".join(struct.pack("<HHI", *e) for e in entries)


def _as_systemd_service(tmp_path, monkeypatch, *, acl=None, file_ids=(0, 0), dir_ids=(0, 0), link=False):
    """Il caso della VPS visto dal processo del servizio. Il file e' davvero del processo di test (che lo deve poter
    leggere) e porta una vera ACL; `stat` e `fstat` riportano le identita' di systemd (root:root di default) e il
    processo ha le identita' di pf-dispatch. Gira anche senza root."""
    path = _credential(tmp_path, monkeypatch, dir_mode=0o700)
    if link:
        target = write_secret(tmp_path / "other" / "real", BK_CRED, 0o440)
        os.remove(path)
        os.symlink(target, path)
    try:
        os.setxattr(path if not link else target, ACL_XATTR, _acl() if acl is None else acl)
    except OSError as exc:
        pytest.skip(f"ACL POSIX non disponibili su questo filesystem ({exc.errno})")
    os.chmod(os.path.dirname(path), 0o500)
    real_uid, real_fstat, real_stat = os.geteuid(), os.fstat, os.stat
    cdir = os.path.realpath(os.path.dirname(path))

    def seen(st, ids):
        if st.st_uid != real_uid:
            return st
        fields = list(st[:10])
        fields[4], fields[5] = ids
        return os.stat_result(fields)

    monkeypatch.setattr(os, "geteuid", lambda: SERVICE_ID)
    monkeypatch.setattr(os, "getegid", lambda: SERVICE_ID)
    monkeypatch.setattr(os, "fstat", lambda fd: seen(real_fstat(fd), file_ids))
    monkeypatch.setattr(os, "stat", lambda p, *a, **k: seen(real_stat(p, *a, **k),
                                                              dir_ids if os.path.realpath(p) == cdir else file_ids))
    return path


def _restore_dir(tmp_path):
    os.chmod(tmp_path / "credentials" / "pf-dispatch.service", 0o700)


def test_d1_systemd_255_root_credential_readable_via_acl_is_accepted(tmp_path, monkeypatch):
    """RED su c26d928: 0440 root:root con la voce ACL dell'utente del servizio era rifiutato (gruppo != egid)."""
    path = _as_systemd_service(tmp_path, monkeypatch)
    try:
        assert td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file") == BK_CRED
    finally:
        _restore_dir(tmp_path)


@pytest.mark.parametrize("case", ["acl_foreign_user", "acl_foreign_group", "file_foreign_group",
                                  "file_foreign_owner", "dir_foreign_owner", "symlink"])
def test_d1_systemd_credential_with_foreign_access_is_refused(tmp_path, monkeypatch, case):
    """Il caso root:root e' ammesso solo se nessun altro puo' leggere: voci ACL di utenti o gruppi estranei, gruppo o
    proprietario estranei del file, directory di un altro utente o link simbolico => rifiutato, senza il token."""
    kwargs = {"acl_foreign_user": {"acl": _acl(user=4242)},
              "acl_foreign_group": {"acl": _acl(group=4242)},
              "file_foreign_group": {"file_ids": (0, 4242)},
              "file_foreign_owner": {"file_ids": (4242, 0)},
              "dir_foreign_owner": {"dir_ids": (4242, 0)},
              "symlink": {"link": True}}[case]
    path = _as_systemd_service(tmp_path, monkeypatch, **kwargs)
    try:
        with pytest.raises(td.ConfigError) as exc:
            td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file")
        assert BK_CRED not in str(exc.value)
    finally:
        _restore_dir(tmp_path)


def test_d1_unreadable_acl_is_refused(tmp_path, monkeypatch):
    """Errore nella lettura della ACL (diverso da «nessuna ACL») => rifiutato: mai un'accettazione per difetto."""
    path = _as_systemd_service(tmp_path, monkeypatch)

    def broken(*_a, **_k):
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(os, "getxattr", broken)
    try:
        with pytest.raises(td.ConfigError):
            td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file")
    finally:
        _restore_dir(tmp_path)


@pytest.mark.parametrize("acl, ok", [
    (_acl(), True), (_acl(user=None), True), (_acl(user=0, group=0), True), (_acl(group=SERVICE_ID), True),
    (_acl(user=4242), False), (_acl(group=4242), False),
    (struct.pack("<I", 2) + struct.pack("<HHI", 0x20, 4, 0xFFFFFFFF), False),     # altri in lettura
    (struct.pack("<I", 1) + _acl()[4:], False),                                    # versione sconosciuta
    (_acl()[:-3], False), (b"", False),                                            # troncata, vuota
    (struct.pack("<I", 2) + struct.pack("<HHI", 0x40, 4, 0), False),               # voce sconosciuta
])
def test_credential_acl_admits_only_root_and_the_service(acl, ok):
    assert td._credential_acl_ok(acl, SERVICE_ID, SERVICE_ID) is ok


@pytest.mark.parametrize("mode", [0o400, 0o600])
def test_private_secret_still_accepted_without_systemd(tmp_path, monkeypatch, mode):
    """Regressione: 0400/0600 restano validi ovunque, anche senza $CREDENTIALS_DIRECTORY."""
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    path = write_secret(tmp_path / "s", BK_CRED, mode)
    assert td.read_secret_file(path, td._TOKEN_RE, "buildkite.token_file") == BK_CRED


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
    ("http_timeout_seconds",), ("readiness_wait",), ("readiness_wait", "enabled"), ("readiness_wait", "max_seconds"),
    ("readiness_wait", "settle_polls"), ("readiness_wait", "min_delay_seconds"),
    ("http_timeout_seconds",), ("full_suite",), ("full_suite", "enabled"), ("full_suite", "build_timeout_seconds"),
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
    lambda r: r["readiness_wait"].update(enabled="false"),
    lambda r: r["readiness_wait"].update(max_seconds=599),
    lambda r: r["readiness_wait"].update(max_seconds=21601),
    lambda r: r["readiness_wait"].update(settle_polls=0),
    lambda r: r["readiness_wait"].update(settle_polls=True),
    lambda r: r["readiness_wait"].update(min_delay_seconds=-1),
    lambda r: r["readiness_wait"].update(extra=1),
    lambda r: r["full_suite"].update(enabled="true"),
    lambda r: r["full_suite"].update(enabled=1),
    lambda r: r["full_suite"].update(build_timeout_seconds=599),
    lambda r: r["full_suite"].update(build_timeout_seconds=21601),
    lambda r: r["full_suite"].update(pipeline="pickfair-nogui"),
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


# --------------------------------------------------------------------------------------------- readiness non bloccante
# (#562 opzione A, OWNER GO): la build della readiness parte solo quando gli altri check dell'head sono chiusi.
def _run(name, status="completed", conclusion="success"):
    return {"name": name, "status": status, "conclusion": conclusion if status == "completed" else None}


def _st(context, state):
    return {"context": context, "state": state}


@pytest.fixture()
def wait_env(tmp_path):
    raw = raw_config(tmp_path)
    raw["readiness_wait"]["enabled"] = True
    cfg = td.validate_config(raw)
    bk = FakeBuildkite()
    pub = Publisher()
    clock = Clock()
    disp = make_dispatcher(cfg, bk, pub, clock)
    yield disp, bk, pub, clock
    disp.store.close()


def _readiness(disp):
    return [j for j in disp.store.recent_jobs() if j["guard"] == "readiness"][0]


def _open_checks(gh, sha=HEAD):
    gh.check_runs[sha] = [_run("unit"), _run("tests", "in_progress"), _run("GPT-6.1 Sol push-range review", "queued")]
    gh.statuses[sha] = [_st("overlap-check", "pending")]


def _close_checks(gh, sha=HEAD, *, failed=False):
    gh.check_runs[sha] = [_run("unit"), _run("tests", conclusion="failure" if failed else "success"),
                          _run("GPT-6.1 Sol push-range review")]
    gh.statuses[sha] = [_st("overlap-check", "success"), _st("overlap-check", "pending")]   # dal piu' recente


def _tick(disp, clock, seconds=60):
    clock.now += seconds
    disp.tick()


def test_readiness_waits_for_open_checks_without_any_build(wait_env):
    disp, bk, pub, clock = wait_env
    _open_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(5):
        _tick(disp, clock)
    assert not bk.builds["pr-merge-readiness"]                      # nessun agent occupato durante l'attesa
    assert len(bk.builds["pr-overlap-guard"]) == 1                  # l'overlap parte subito
    waiting = [c for c in pub.calls if c[3] == "pr-merge-readiness"]
    assert waiting == [(42, HEAD, "pending", "pr-merge-readiness", f"https://buildkite.com/{ORG}/pr-merge-readiness",
                        "in attesa degli altri check")]
    assert _readiness(disp)["state"] == "queued"


def test_readiness_starts_after_checks_are_closed_and_stable(wait_env):
    disp, bk, pub, clock = wait_env
    _open_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 130)                                         # oltre min_delay, ma check aperti
    _close_checks(bk.gh)
    _tick(disp, clock)                                              # 1 giro stabile su 2
    assert not bk.builds["pr-merge-readiness"]
    _tick(disp, clock)                                              # 2 giri stabili: via libera
    assert len(bk.builds["pr-merge-readiness"]) == 1
    job = _readiness(disp)
    assert job["state"] == "dispatched" and job["released_at"] == clock.now and job["gate_seen"] == 4
    # una volta avviata non si rilegge piu' lo stato dei check
    n = len([r for r in bk.gh.requests if "/commits/" in r[1]])
    _tick(disp, clock)
    assert len([r for r in bk.gh.requests if "/commits/" in r[1]]) == n


def test_readiness_never_waits_for_its_own_self_checks(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.check_runs[HEAD] = [_run("unit"), _run("Merge readiness", "in_progress")]
    bk.gh.statuses[HEAD] = [_st("pr-merge-readiness", "pending"), _st("overlap-check", "success")]
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(4):
        _tick(disp, clock, 60)
    assert len(bk.builds["pr-merge-readiness"]) == 1


def test_a_failed_check_releases_the_readiness_which_judges_it(wait_env):
    # l'attesa guarda solo la chiusura dei check: l'esito lo decide la readiness, quindi mai un PASS dal gate
    disp, bk, pub, clock = wait_env
    _close_checks(bk.gh, failed=True)
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(4):
        _tick(disp, clock, 60)
    assert len(bk.builds["pr-merge-readiness"]) == 1
    assert not [c for c in pub.final() if c[3] == "pr-merge-readiness"]


@pytest.mark.parametrize("still_open", ["status", "run_queued", "run_in_progress", "no_checks"])
def test_any_open_check_or_no_check_at_all_keeps_waiting(wait_env, still_open):
    disp, bk, pub, clock = wait_env
    _close_checks(bk.gh)
    if still_open == "status":
        bk.gh.statuses[HEAD] = [_st("full-suite", "pending")] + bk.gh.statuses[HEAD]
    elif still_open == "run_queued":
        bk.gh.check_runs[HEAD].append(_run("e2e", "queued"))
    elif still_open == "run_in_progress":
        bk.gh.check_runs[HEAD].append(_run("mutation", "in_progress"))
    else:
        bk.gh.check_runs[HEAD], bk.gh.statuses[HEAD] = [], []
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(10):
        _tick(disp, clock, 60)
    assert not bk.builds["pr-merge-readiness"]


def test_a_growing_rollup_resets_the_stability_count(wait_env):
    disp, bk, pub, clock = wait_env
    _close_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 130)                                         # 1 giro stabile
    bk.gh.check_runs[HEAD].append(_run("late-check"))               # un check nuovo, gia' chiuso
    _tick(disp, clock)                                              # conteggio cambiato: si riparte da 1
    assert not bk.builds["pr-merge-readiness"]
    _tick(disp, clock)
    assert len(bk.builds["pr-merge-readiness"]) == 1


def test_min_delay_is_respected_even_if_everything_is_closed(wait_env):
    disp, bk, pub, clock = wait_env
    _close_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(3):
        _tick(disp, clock, 30)                                      # 90 s < 120 s
    assert not bk.builds["pr-merge-readiness"]
    _tick(disp, clock, 40)
    _tick(disp, clock, 30)
    assert len(bk.builds["pr-merge-readiness"]) == 1


def test_wait_beyond_the_maximum_is_error_and_never_a_build(wait_env):
    disp, bk, pub, clock = wait_env
    _open_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 3600)
    _tick(disp, clock, 3601)                                        # 7201 s
    assert not bk.builds["pr-merge-readiness"]
    (final,) = [c for c in pub.final() if c[3] == "pr-merge-readiness"]
    assert final[2] == "error" and "attesa dei check oltre 7200 s" in final[5]
    assert _readiness(disp)["state"] == "done"


def test_unreadable_checks_keep_waiting_and_end_in_error_at_the_maximum(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.checks_status = 500
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(20):
        _tick(disp, clock, 300)
    assert not bk.builds["pr-merge-readiness"]
    _tick(disp, clock, 1300)
    (final,) = [c for c in pub.final() if c[3] == "pr-merge-readiness"]
    assert final[2] == "error"


def test_incomplete_check_run_listing_is_not_a_release(wait_env):
    disp, bk, pub, clock = wait_env
    _close_checks(bk.gh)
    bk.gh.total_count_delta = 1                                     # total_count > check run ricevuti
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(6):
        _tick(disp, clock, 60)
    assert not bk.builds["pr-merge-readiness"]


def test_new_head_drops_the_old_wait_and_waits_on_the_new_sha(wait_env):
    disp, bk, pub, clock = wait_env
    _open_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 130)
    _close_checks(bk.gh)                                            # il vecchio head si chiude...
    _open_checks(bk.gh, HEAD2)
    bk.gh.put(42, HEAD2)                                            # ...ma arriva un head nuovo
    disp.poll()
    for _ in range(4):
        _tick(disp, clock)
    assert not bk.builds["pr-merge-readiness"]                      # nessuna readiness per il vecchio head
    old = [j for j in disp.store.recent_jobs() if j["guard"] == "readiness" and j["head_sha"] == HEAD][0]
    assert old["state"] == "superseded"
    _close_checks(bk.gh, HEAD2)
    _tick(disp, clock)
    _tick(disp, clock)
    (build,) = bk.builds["pr-merge-readiness"].values()
    assert build["env"]["PF_PR_HEAD_SHA"] == HEAD2


def test_wait_state_survives_a_restart_without_duplicates(wait_env):
    disp, bk, pub, clock = wait_env
    _close_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 130)                                         # 1 giro stabile salvato
    assert _readiness(disp)["gate_stable"] == 1
    disp.store.close()
    disp.store = td.Store(disp.cfg.state_db)                        # riavvio
    _tick(disp, clock)
    _tick(disp, clock)
    disp.poll()
    _tick(disp, clock)
    assert len(bk.builds["pr-merge-readiness"]) == 1


def test_released_readiness_build_window_starts_at_the_release(wait_env):
    # dopo un'attesa lunga, una creazione con esito ignoto non e' gia' fuori finestra (si riconcilia)
    disp, bk, pub, clock = wait_env
    _open_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 3000)
    _close_checks(bk.gh)
    bk.fail_create, bk.create_lands = "transport", True
    _tick(disp, clock)
    _tick(disp, clock)
    assert _readiness(disp)["state"] == "creating"
    bk.fail_create, bk.fail_list = None, True                       # elenco illeggibile: si riprova nella finestra
    _tick(disp, clock)
    assert _readiness(disp)["state"] == "creating"                  # finestra dal via libera, non dall'attesa
    bk.fail_list = False
    _tick(disp, clock)
    job = _readiness(disp)
    assert job["state"] == "dispatched" and job["build_number"] == 1
    assert not [c for c in pub.final() if c[3] == "pr-merge-readiness"]


def test_readiness_without_wait_is_unchanged(env):
    disp, bk, _, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    assert len(bk.builds["pr-merge-readiness"]) == 1                # opzione A spenta: avvio immediato
    assert not [r for r in bk.gh.requests if "/commits/" in r[1]]


def test_checks_gate_latest_status_wins_and_malformed_data_is_refused():
    pending, seen = td.checks_gate([_run("a")], [_st("x", "success"), _st("x", "pending")])
    assert pending == [] and seen == 2
    pending, _ = td.checks_gate([], [_st("x", "pending"), _st("x", "success")])
    assert pending == ["x"]
    for runs, statuses in ([{"name": 1, "status": "completed"}], []), ([], [{"context": "x", "state": "weird"}]), \
            ("x", []), ([], None), ([{"name": "a"}], []):
        with pytest.raises(td.ApiError):
            td.checks_gate(runs, statuses)


def test_commit_check_paths_are_allowed_only_for_a_full_sha():
    td.GitHub.check_url(f"https://api.github.com/repos/{REPO}/commits/{HEAD}/check-runs?filter=latest")
    td.GitHub.check_url(f"https://api.github.com/repos/{REPO}/commits/{HEAD}/statuses?per_page=100")
    for bad in (f"/repos/{REPO}/commits/main/check-runs", f"/repos/{REPO}/commits/{HEAD}/comments",
                f"/repos/{REPO}/commits/{HEAD}", f"/repos/other/repo/commits/{HEAD}/statuses",
                f"/repos/{REPO}/commits/{HEAD.upper()}/statuses"):
        with pytest.raises(td.ApiError):
            td.GitHub.check_url(f"https://api.github.com{bad}")


# ------------------------------------------------------------------ paginazione reale `/repositories/<id>/` (#562 F1)
# Rollback T252 di #576: con 107 check run il link `next` di GitHub ha la forma `/repositories/<id>/...` e il
# dispatcher lo rifiutava, quindi la readiness non partiva mai. Il link vale solo come segnale di pagina successiva:
# e' validato in modo stretto e la pagina si chiede all'URL canonico `/repos/<repo>/...` ricostruito dal client.
def _many_runs(n, status="completed"):
    return [_run(f"job-{i:03d}", status) for i in range(n)]


def test_readiness_starts_with_107_check_runs_over_two_real_pages(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.check_runs[HEAD] = _many_runs(107)
    bk.gh.statuses[HEAD] = [_st("overlap-check", "success")]
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 130)
    _tick(disp, clock)
    assert len(bk.builds["pr-merge-readiness"]) == 1
    assert _readiness(disp)["gate_seen"] == 108
    pages = [urllib.parse.urlsplit(u) for _, u, _ in bk.gh.requests if u.split("?")[0].endswith("/check-runs")]
    assert {p.path for p in pages} == {f"/repos/{REPO}/commits/{HEAD}/check-runs"}
    assert {dict(urllib.parse.parse_qsl(p.query)).get("page") for p in pages} == {None, "2"}


def test_one_open_check_on_the_second_page_keeps_the_readiness_waiting(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.check_runs[HEAD] = _many_runs(106) + [_run("ultimo", "in_progress")]
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(4):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"] and _readiness(disp)["state"] == "queued"


def test_statuses_are_read_over_several_real_pages(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.page_size = 2
    bk.gh.check_runs[HEAD] = [_run("unit")]
    bk.gh.statuses[HEAD] = [_st("a", "success"), _st("b", "success"), _st("c", "success"), _st("d", "pending")]
    assert len(disp.gh.statuses(HEAD)) == 4
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(3):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"]                      # `d` pending sulla seconda pagina
    bk.gh.statuses[HEAD][3] = _st("d", "success")
    _tick(disp, clock)
    _tick(disp, clock)
    assert len(bk.builds["pr-merge-readiness"]) == 1


def test_pulls_and_comments_follow_real_repositories_links(env):
    disp, bk, _, clock = env
    bk.gh.page_size = 2
    for n in range(40, 45):
        bk.gh.put(n, HEAD)
    assert len(disp.gh.open_pulls()) == 5
    for cid in range(1, 6):
        bk.gh.comment(cid, 40, "ciao")
    assert [c["id"] for c in disp.gh.comments_since("2000-01-01T00:00:00Z")] == [1, 2, 3, 4, 5]


def _swap(old, new):
    return lambda url: url.replace(old, new, 1)


BAD_NEXT_LINKS = {
    "host esterno": _swap("https://api.github.com/", "https://evil.example/"),
    "host simile": _swap("https://api.github.com/", "https://api.github.com.evil.example/"),
    "http in chiaro": _swap("https://", "http://"),
    "credenziali nell'URL": _swap("https://api.github.com/", "https://x:y@api.github.com/"),
    "porta": _swap("https://api.github.com/", "https://api.github.com:8443/"),
    "id di un altro repository": _swap(f"/repositories/{REPO_ID}/", "/repositories/1189713161/"),
    "id non numerico": _swap(f"/repositories/{REPO_ID}/", "/repositories/abc/"),
    "altro repository per nome": _swap(f"/repositories/{REPO_ID}/", "/repos/other/repo/"),
    "altro commit": _swap(f"/commits/{HEAD}/", f"/commits/{HEAD2}/"),
    "altro endpoint": _swap("/check-runs?", "/statuses?"),
    "path con risalita": _swap(f"/commits/{HEAD}/", f"/commits/{HEAD}/../{HEAD2}/"),
    "pagina saltata": _swap("page=2", "page=3"),
    "pagina all'indietro": _swap("page=2", "page=1"),
    "pagina non numerica": _swap("page=2", "page=2x"),
    "parametro cambiato": _swap("filter=latest", "filter=all"),
    "parametro in piu'": _swap("page=2", "page=2&head_sha=x"),
    "parametro mancante": _swap("filter=latest&", ""),
    "parametro duplicato": _swap("page=2", "page=2&page=2"),
    "frammento": lambda url: url + "#x",
}


@pytest.mark.parametrize("name", sorted(BAD_NEXT_LINKS))
def test_tampered_next_link_is_refused(env, name):
    disp, bk, _, _ = env
    bk.gh.check_runs[HEAD] = _many_runs(107)
    bk.gh.next_link_hook = BAD_NEXT_LINKS[name]
    with pytest.raises(td.ApiError):
        disp.gh.check_runs(HEAD)
    assert all(urllib.parse.urlsplit(u).netloc == "api.github.com" for _, u, _ in bk.gh.requests)
    assert len(bk.gh.requests) == 1                                 # la seconda pagina non e' mai richiesta


def test_canonical_repos_link_is_still_accepted(env):
    disp, bk, _, _ = env
    bk.gh.link_style = "repos"
    bk.gh.check_runs[HEAD] = _many_runs(107)
    assert len(disp.gh.check_runs(HEAD)) == 107


def test_tampered_next_link_keeps_the_readiness_waiting_and_never_passes(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.check_runs[HEAD] = _many_runs(107)
    bk.gh.next_link_hook = BAD_NEXT_LINKS["id di un altro repository"]
    bk.gh.put(42, HEAD)
    disp.poll()
    for _ in range(4):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"] and _readiness(disp)["state"] == "queued"
    assert not [c for c in pub.final() if c[3] == "pr-merge-readiness"]


def test_total_count_inconsistent_over_two_pages_is_refused(env):
    disp, bk, _, _ = env
    bk.gh.check_runs[HEAD] = _many_runs(107)
    for delta in (1, -1):
        bk.gh.total_count_delta = delta
        with pytest.raises(td.ApiError):
            disp.gh.check_runs(HEAD)


def test_next_link_without_a_valid_target_is_refused(env):
    disp, bk, _, _ = env
    bk.gh.check_runs[HEAD] = _many_runs(107)
    bk.gh.next_link_hook = lambda url: "https://api.github.com/"     # rel="next" presente, destinazione non valida
    with pytest.raises(td.ApiError):
        disp.gh.check_runs(HEAD)


def test_paginated_wait_survives_restart_and_a_new_head(wait_env):
    disp, bk, pub, clock = wait_env
    bk.gh.check_runs[HEAD] = _many_runs(107)
    bk.gh.check_runs[HEAD2] = _many_runs(107)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock, 130)                                         # 1 giro stabile su 2, salvato
    assert _readiness(disp)["gate_stable"] == 1
    disp.store.close()
    disp.store = td.Store(disp.cfg.state_db)                        # riavvio
    bk.gh.put(42, HEAD2)                                            # head nuovo prima del via libera
    disp.poll()
    for _ in range(4):
        _tick(disp, clock, 130)
    builds = bk.builds["pr-merge-readiness"].values()
    assert [b["env"]["PF_PR_HEAD_SHA"] for b in builds] == [HEAD2]      # mai una readiness per l'head vecchio
    old = [j for j in disp.store.recent_jobs() if j["guard"] == "readiness" and j["head_sha"] == HEAD]
    assert old and all(j["state"] == "superseded" for j in old)


# --------------------------------------------------------------------------------------------- full-suite (#562 PR-A)
_gen_spec = importlib.util.spec_from_file_location("pr_full_suite_pipeline",
                                                   ROOT / "scripts" / "pr_full_suite_pipeline.py")
fsp = importlib.util.module_from_spec(_gen_spec)
sys.modules["pr_full_suite_pipeline"] = fsp             # le dataclass del modulo lo cercano in sys.modules
_gen_spec.loader.exec_module(fsp)

SUITE_PIPE = "pf-dedicated-ci"


@pytest.fixture()
def suite_env(tmp_path):
    raw = raw_config(tmp_path)
    raw["full_suite"]["enabled"] = True
    raw["readiness_wait"].update(enabled=True, max_seconds=10800)   # obbligatoria con la full-suite (#562 P0)
    cfg = td.validate_config(raw)
    bk = FakeBuildkite()
    pub = Publisher()
    clock = Clock()
    disp = make_dispatcher(cfg, bk, pub, clock)
    yield disp, bk, pub, clock
    disp.store.close()




def test_readiness_wait_shorter_than_the_suite_is_refused(tmp_path):
    """La readiness attende anche lo status `full-suite`: un'attesa massima piu' corta del timeout della suite la
    manderebbe in `error` mentre la suite gira ancora. Con entrambe attive la config lo rifiuta."""
    raw = raw_config(tmp_path)
    raw["readiness_wait"].update(enabled=True, max_seconds=7200)
    raw["full_suite"].update(enabled=True, build_timeout_seconds=7201)
    with pytest.raises(td.ConfigError) as exc:
        td.validate_config(raw)
    assert any("readiness_wait.max_seconds" in p and "full_suite" in p for p in exc.value.problems)
    raw["full_suite"]["build_timeout_seconds"] = 7200
    assert td.validate_config(raw).full_suite_enabled
    # full-suite senza attesa della readiness: rifiutata, la readiness partirebbe insieme alla suite (#562 P0)
    raw["full_suite"]["build_timeout_seconds"] = 10800
    raw["readiness_wait"]["enabled"] = False
    with pytest.raises(td.ConfigError) as exc:
        td.validate_config(raw)
    assert "readiness_wait.enabled: obbligatoria con full_suite.enabled" in exc.value.problems
    # attesa senza full-suite: nessun vincolo sul massimo
    raw["readiness_wait"].update(enabled=True, max_seconds=600)
    raw["full_suite"]["enabled"] = False
    assert not td.validate_config(raw).full_suite_enabled


def _suite_jobs(keys=None, state="passed", **overrides):
    """Job script di una build della full-suite (API REST v2): uno per step atteso, piu' lo step di avvio."""
    jobs = [{"type": "script", "step_key": None, "state": "passed", "name": "pipeline upload da main"}]
    for key in sorted(td.FULL_SUITE_STEP_KEYS if keys is None else keys):
        jobs.append({"type": "script", "step_key": key, "state": overrides.get(key, state)})
    return jobs


def _finish_suite(bk, number, state, jobs):
    bk.builds[SUITE_PIPE][number]["state"] = state
    bk.builds[SUITE_PIPE][number]["jobs"] = jobs


def _suite_final(pub):
    return [c for c in pub.final() if c[3] == "full-suite"]


def test_full_suite_step_keys_are_exactly_the_generator_steps():
    # il dispatcher (trusted) e il generatore di main devono concordare: una suite che perde uno step non e' PASS
    assert td.FULL_SUITE_STEP_KEYS == {j.key for j in fsp.JOBS}
    assert len(td.FULL_SUITE_STEP_KEYS) == len(fsp.JOBS)


def test_full_suite_is_off_unless_enabled(env):
    disp, bk, _, _ = env
    assert disp.cfg.targets == ("overlap", "readiness") and "full-suite" not in disp.cfg.build_timeout_seconds
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    assert not bk.builds[SUITE_PIPE] and {j["guard"] for j in disp.store.recent_jobs()} == {"overlap", "readiness"}


def test_full_suite_build_runs_the_exact_pr_head_on_its_branch(suite_env):
    disp, bk, pub, _ = suite_env
    assert disp.cfg.targets == ("overlap", "readiness", "full-suite")
    assert disp.cfg.build_timeout_seconds["full-suite"] == 10800
    bk.gh.put(42, HEAD, ref="feature/x")
    disp.poll()
    disp.tick()
    sent = {url.split("/pipelines/")[1].split("/")[0]: json.loads(body) for m, url, _, body in bk.requests
            if m == "POST"}
    assert sent[SUITE_PIPE]["commit"] == HEAD and sent[SUITE_PIPE]["branch"] == "feature/x"
    assert set(sent[SUITE_PIPE]) == {"commit", "branch", "message", "env", "meta_data"}
    assert sent[SUITE_PIPE]["meta_data"] == {"pf_intent": sent[SUITE_PIPE]["env"]["PF_DELIVERY_ID"]}   # #562 P0
    assert set(sent[SUITE_PIPE]["env"]) == {"PF_PR_NUMBER", "PF_PR_HEAD_SHA", "PF_PR_BASE_REF", "PF_EVENT",
                                            "PF_DELIVERY_ID"}
    # i guard trusted restano su main / HEAD, mai sul codice della PR; la readiness attende la suite (#562 P0)
    assert sent["pr-overlap-guard"]["commit"] == "HEAD" and sent["pr-overlap-guard"]["branch"] == "main"
    assert "pr-merge-readiness" not in sent
    pending = [c for c in pub.calls if c[3] == "full-suite"]
    assert pending == [(42, HEAD, "pending", "full-suite", f"https://buildkite.com/{ORG}/{SUITE_PIPE}/builds/1",
                        "build in corso")]


def test_full_suite_success_needs_every_expected_step_passed(suite_env):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    assert _suite_final(pub) == [(42, HEAD, "success", "full-suite", f"https://buildkite.com/{ORG}/{SUITE_PIPE}/builds/1",
                                  f"PASS ({len(td.FULL_SUITE_STEP_KEYS) + 1} step)")]
    # nessun artifact chiesto alla lane non fidata: l'esito e' lo stato della build
    assert not [u for m, u, _, _ in bk.requests if SUITE_PIPE in u and "/artifacts" in u]


@pytest.mark.parametrize("case", ["missing_step", "soft_failed", "step_not_passed", "no_jobs", "jobs_not_list",
                                  "only_bootstrap", "extra_soft_failed", "extra_failed", "expected_broken"])
def test_full_suite_passed_build_that_is_not_complete_is_error_never_success(suite_env, case):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    keys = sorted(td.FULL_SUITE_STEP_KEYS)
    jobs = {
        "missing_step": _suite_jobs(keys[1:]),
        "soft_failed": [dict(j, soft_failed=True) if j["step_key"] == keys[0] else j for j in _suite_jobs()],
        "step_not_passed": _suite_jobs(**{keys[0]: "broken"}),
        "no_jobs": None,
        "jobs_not_list": {"x": 1},
        "only_bootstrap": _suite_jobs([]),
        "extra_soft_failed": _suite_jobs() + [{"type": "script", "step_key": "x", "state": "passed",
                                               "soft_failed": True}],
        "extra_failed": _suite_jobs() + [{"type": "script", "step_key": "x", "state": "failed"}],
        "expected_broken": _suite_jobs(**{keys[0]: "broken"}),
    }[case]
    _finish_suite(bk, 1, "passed", jobs)
    if jobs is None:
        del bk.builds[SUITE_PIPE][1]["jobs"]
    disp.tick()
    (final,) = _suite_final(pub)
    assert final[2] == "error", final


def test_full_suite_tolerates_conditional_scenario_steps_not_run(suite_env):
    # pf-dedicated-ci #12 (W1): gli scenari S0-S9 con `if: build.env("SCN") == ...` risultano `broken` in una build passed
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    extra = [{"type": "script", "step_key": f"s{i}", "state": "broken"} for i in range(10)]
    _finish_suite(bk, 1, "passed", _suite_jobs() + extra + [{"type": "script", "step_key": "y", "state": "skipped"}])
    disp.tick()
    assert _suite_final(pub)[0][2] == "success"


def test_full_suite_failed_build_is_failure_naming_the_failed_step(suite_env):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    _finish_suite(bk, 1, "failed", _suite_jobs(**{"w1-pr-check": "failed", "w1-mutation": "failed"}))
    disp.tick()
    (final,) = _suite_final(pub)
    assert final[2] == "failure" and final[5] == "FAIL: w1-mutation (+1)"


def test_full_suite_retried_step_counts_only_the_last_attempt(suite_env):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    jobs = _suite_jobs() + [{"type": "script", "step_key": "w1-unit", "state": "failed", "retried": True}]
    _finish_suite(bk, 1, "passed", jobs)
    disp.tick()
    assert _suite_final(pub)[0][2] == "success"


@pytest.mark.parametrize("state", ["canceled", "blocked", "skipped", "not_run"])
def test_full_suite_non_verdict_states_are_error(suite_env, state):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    _finish_suite(bk, 1, state, _suite_jobs())
    disp.tick()
    assert _suite_final(pub)[0][2] == "error"


@pytest.mark.parametrize("field,value", [("commit", HEAD2), ("branch", "main"), ("branch", "other")])
def test_full_suite_build_on_another_commit_or_branch_is_refused(suite_env, field, value):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.builds[SUITE_PIPE][1][field] = value
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    (final,) = _suite_final(pub)
    assert final[2] == "error" and (SUITE_PIPE, 1) in bk.canceled


def test_decide_suite_outcome_refuses_a_stale_commit_even_if_passed():
    job = {"head_sha": HEAD}
    build = {"state": "passed", "commit": HEAD2, "jobs": _suite_jobs()}
    assert td.decide_suite_outcome(build, job)[0] == "error"
    assert td.decide_suite_outcome(dict(build, commit=HEAD), job)[0] == "success"


@pytest.mark.parametrize("ref", [None, "", "-rf", "a..b", "a//b", "x/", "x.lock", "a b", "é", "x" * 201])
def test_full_suite_without_a_valid_head_ref_creates_no_build(suite_env, ref):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD, ref=ref)
    disp.poll()
    disp.tick()
    assert not bk.builds[SUITE_PIPE]
    (final,) = _suite_final(pub)
    assert final[2] == "error" and "branch" in final[5]
    # l'overlap non dipende dal ref; la readiness attende la suite dell'intento e non parte su una suite in errore
    assert len(bk.builds["pr-overlap-guard"]) == 1 and not bk.builds["pr-merge-readiness"]


@pytest.mark.parametrize("ref", ["main2", "feature/x", "ccr-764b0316-9r1a1d", "a_b.c-d/e"])
def test_clean_ref_keeps_ordinary_branch_names(ref):
    assert td.clean_ref(ref) == ref


def test_full_suite_build_is_cancelled_by_a_new_head(suite_env):
    disp, bk, pub, _ = suite_env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.gh.put(42, HEAD2)
    disp.poll()
    disp.tick()
    assert (SUITE_PIPE, 1) in bk.canceled
    assert not [c for c in pub.final() if c[1] == HEAD]                # nessun esito sul vecchio head
    assert bk.builds[SUITE_PIPE][2]["commit"] == HEAD2


def test_full_suite_ambiguous_creation_is_recovered_by_commit(suite_env):
    disp, bk, pub, _ = suite_env
    bk.fail_create, bk.create_lands = "transport", True
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.fail_create = None
    disp.tick()
    lists = [u for m, u, _, _ in bk.requests if m == "GET" and f"/{SUITE_PIPE}/builds?" in u]
    assert lists and all(f"commit={HEAD}" in u and "branch=" not in u for u in lists)
    suite = [j for j in disp.store.recent_jobs() if j["guard"] == "full-suite"][0]
    assert suite["state"] == "dispatched" and suite["build_number"] == 1


def test_full_suite_rerun_uses_the_saved_head_ref(suite_env):
    disp, bk, pub, clock = suite_env
    bk.gh.put(42, HEAD, ref="feature/rerun")
    disp.poll()
    disp.poll()                                    # primo avvio dei rerun: fissa il limite inferiore
    clock.now += 10
    bk.gh.comment(7001, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now))
    created = disp.poll()
    assert any(t.startswith("p42-r7001-") for t in created)
    disp.tick()
    posts = [json.loads(b) for m, u, _, b in bk.requests if m == "POST" and f"/{SUITE_PIPE}/" in u]
    assert posts[-1]["branch"] == "feature/rerun" and posts[-1]["commit"] == HEAD


def test_store_migration_adds_wait_and_head_ref_columns_to_an_old_archive(tmp_path):
    import sqlite3
    path = str(tmp_path / "old.sqlite3")
    db = sqlite3.connect(path)
    db.executescript("""
CREATE TABLE pr_state (pr INTEGER PRIMARY KEY, head_sha TEXT NOT NULL, eligible INTEGER NOT NULL,
  open INTEGER NOT NULL, missing INTEGER NOT NULL DEFAULT 0, seq INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL);
CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL, event TEXT NOT NULL,
  pr INTEGER NOT NULL, head_sha TEXT NOT NULL, guard TEXT NOT NULL, state TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0, build_number INTEGER, build_url TEXT, outcome TEXT, description TEXT,
  created_at REAL NOT NULL, dispatched_at REAL, updated_at REAL NOT NULL);
INSERT INTO pr_state VALUES (42, 'aaaa', 1, 1, 0, 3, 1.0);
""")
    db.commit()
    db.close()
    store = td.Store(path, targets=("overlap", "readiness", "full-suite"))
    try:
        assert store._db.execute("SELECT head_ref, seq FROM pr_state WHERE pr = 42").fetchone()[:] == ("", 3)
        cols = {r[1] for r in store._db.execute("PRAGMA table_info(jobs)")}
        assert {"gate_seen", "gate_stable", "released_at", "head_ref"} <= cols
    finally:
        store.close()


def test_store_refuses_unknown_targets(tmp_path):
    with pytest.raises(ValueError):
        td.Store(str(tmp_path / "s.sqlite3"), targets=("overlap", "pf-trusted-smoke"))
    with pytest.raises(ValueError):
        td.Store(str(tmp_path / "s2.sqlite3"), targets=())


# --- #562 P0: readiness prematura. La readiness parte e passa solo con la full-suite DELLO STESSO INTENTO conclusa,
# esito pubblicato dal dispatcher e lo stesso esito visibile sull'head; l'assenza dello status non e' mai successo.

def _visible(bk, pub, sha=HEAD, drop=()):
    """Status dell'head come li vedrebbe GitHub: quelli pubblicati dal dispatcher, dal piu' recente, senza `drop`."""
    bk.gh.statuses[sha] = [_st(c[3], c[2]) for c in reversed(pub.calls) if c[1] == sha and c[3] not in drop]


def _suite_wait_start(suite_env):
    disp, bk, pub, clock = suite_env
    bk.gh.check_runs[HEAD] = [_run("unit")]
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    return disp, bk, pub, clock


def _ticks(disp, clock, bk, pub, n=4, seconds=130, drop=()):
    for _ in range(n):
        _visible(bk, pub, drop=drop)
        _tick(disp, clock, seconds)


def test_readiness_never_starts_while_the_suite_status_is_missing_on_github(suite_env):
    """Check della suite non ancora creato o API in ritardo: tutti gli altri check chiusi non bastano."""
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _ticks(disp, clock, bk, pub, drop=("full-suite",))
    assert not bk.builds["pr-merge-readiness"] and _readiness(disp)["state"] == "queued"


def test_readiness_waits_for_the_running_suite_then_passes_on_the_exact_head(suite_env):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _ticks(disp, clock, bk, pub)                                    # suite `pending` visibile
    assert not bk.builds["pr-merge-readiness"]
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    assert _suite_final(pub)[0][2] == "success"
    _ticks(disp, clock, bk, pub, n=2, seconds=60)
    assert len(bk.builds["pr-merge-readiness"]) == 1
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    final = [c for c in pub.final() if c[3] == "pr-merge-readiness"]
    assert [(c[1], c[2]) for c in final] == [(HEAD, "success")]


def test_suite_concluded_locally_but_not_visible_keeps_the_readiness_waiting(suite_env):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    bk.gh.statuses[HEAD] = [_st("overlap-check", "success"), _st("full-suite", "pending")]    # lettura vecchia
    for _ in range(4):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"]
    bk.gh.statuses[HEAD] = [_st("overlap-check", "success")]                                 # status assente
    for _ in range(4):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"]


def test_suite_success_published_by_someone_else_is_not_evidence(suite_env):
    """Status `full-suite: success` sull'head mentre la build dell'intento gira: non e' prova, si attende."""
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    bk.gh.statuses[HEAD] = [_st("full-suite", "success"), _st("overlap-check", "success")]
    for _ in range(4):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"]


def test_remote_suite_status_disagreeing_with_the_local_outcome_keeps_waiting(suite_env):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    bk.gh.statuses[HEAD] = [_st("full-suite", "failure"), _st("full-suite", "success"), _st("overlap-check", "success")]
    for _ in range(4):
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"]


@pytest.mark.parametrize("state,expected", [("failed", "failure"), ("canceled", "error")])
def test_suite_not_passed_makes_the_readiness_negative_without_a_build(suite_env, state, expected):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _finish_suite(bk, 1, state, _suite_jobs(state="failed" if state == "failed" else "passed"))
    disp.tick()
    assert _suite_final(pub)[0][2] == expected
    _ticks(disp, clock, bk, pub, n=3, seconds=60)
    assert not bk.builds["pr-merge-readiness"]
    final = [c for c in pub.final() if c[3] == "pr-merge-readiness"]
    assert [c[2] for c in final] == [expected] and "full-suite" in final[0][5]


def test_suite_still_running_at_the_wait_limit_is_error_never_pass(suite_env):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _ticks(disp, clock, bk, pub, n=2)
    clock.now += disp.cfg.readiness_wait_max_seconds + 1
    disp.tick()
    final = [c for c in pub.final() if c[3] == "pr-merge-readiness"]
    assert [c[2] for c in final] == ["error"] and not bk.builds["pr-merge-readiness"]


def test_old_head_suite_success_does_not_release_the_new_head_readiness(suite_env):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()                                                     # suite di HEAD: success
    bk.gh.check_runs[HEAD2] = [_run("unit")]
    bk.gh.put(42, HEAD2)
    disp.poll()
    disp.tick()
    bk.finish("pr-overlap-guard", 2, "passed", result("overlap", head=HEAD2))
    disp.tick()
    for _ in range(4):
        _visible(bk, pub, sha=HEAD)
        _visible(bk, pub, sha=HEAD2)
        _tick(disp, clock, 130)
    assert not bk.builds["pr-merge-readiness"]                      # ne' l'head vecchio ne' il nuovo
    new = [j for j in disp.store.recent_jobs() if j["guard"] == "readiness" and j["head_sha"] == HEAD2]
    assert [j["state"] for j in new] == ["queued"]


def test_restart_during_the_wait_keeps_the_suite_requirement(suite_env, tmp_path):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _ticks(disp, clock, bk, pub, n=2, drop=("full-suite",))
    disp.store.close()
    disp2 = make_dispatcher(disp.cfg, bk, pub, clock)              # stesso archivio su disco
    for _ in range(4):
        _visible(bk, pub, drop=("full-suite",))
        _tick(disp2, clock, 130)
    assert not bk.builds["pr-merge-readiness"]
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp2.tick()
    _ticks(disp2, clock, bk, pub, n=2, seconds=60)
    assert len(bk.builds["pr-merge-readiness"]) == 1
    disp2.store.close()
    disp.store = disp2.store                                        # chiusura idempotente del fixture


def test_readiness_success_without_a_passed_suite_is_published_as_error(suite_env):
    """Ultima difesa: anche se un esito `success` della readiness arrivasse senza la suite dell'intento superata
    (archivio ripristinato, suite rilanciata), lo status pubblicato e' `error`, mai `success`."""
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    _ticks(disp, clock, bk, pub, n=3)
    assert len(bk.builds["pr-merge-readiness"]) == 1
    suite = [j for j in disp.store.recent_jobs() if j["guard"] == "full-suite"][0]
    disp.store.update_job(suite["id"], clock.now, outcome="error")
    bk.finish("pr-merge-readiness", 1, "passed", result("readiness"))
    disp.tick()
    final = [c for c in pub.final() if c[3] == "pr-merge-readiness"]
    assert [c[2] for c in final] == ["error"] and "full-suite" in final[0][5]


def test_suite_verdict_without_the_intent_suite_job_waits(suite_env):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    job = _readiness(disp)
    assert disp._suite_verdict({**job, "delivery_id": "p42-t99-" + HEAD[:12]}, []) == (
        "wait", "full-suite dell'intento non registrata")


@pytest.mark.parametrize("fields,reason", [
    ({"superseded": 1}, "full-suite dell'intento sostituita"),
    ({"state": "publish_failed", "final_published": 0}, "full-suite in corso"),
    ({"outcome": "dry_run"}, "full-suite con esito dry_run"),
])
def test_suite_verdict_waits_on_a_suite_that_is_not_a_published_verdict(suite_env, fields, reason):
    disp, bk, pub, clock = _suite_wait_start(suite_env)
    _finish_suite(bk, 1, "passed", _suite_jobs())
    disp.tick()
    suite = [j for j in disp.store.recent_jobs() if j["guard"] == "full-suite"][0]
    assert disp._suite_verdict(_readiness(disp), [_st("full-suite", "success")]) == (
        "success", "full-suite success sull'head")
    with disp.store._lock:                       # stato forzato: campi che `update_job` non espone
        disp.store._db.execute(f"UPDATE jobs SET {', '.join(f'{k} = ?' for k in fields)} WHERE id = ?",
                               (*fields.values(), suite["id"]))
    assert disp._suite_verdict(_readiness(disp), [_st("full-suite", "success")]) == ("wait", reason)


@pytest.mark.parametrize("value", ["true", 1, 0, None])
def test_full_suite_enabled_must_be_an_explicit_boolean_even_with_the_wait_on(tmp_path, value):
    """Il booleano esplicito e' il suo controllo, non un effetto collaterale dell'obbligo di attesa (#562 P0)."""
    raw = raw_config(tmp_path)
    raw["readiness_wait"].update(enabled=True, max_seconds=10800)
    raw["full_suite"]["enabled"] = value
    with pytest.raises(td.ConfigError) as exc:
        td.validate_config(raw)
    assert "full_suite.enabled: booleano true/false" in exc.value.problems


# --------------------------------------------------------------- recovery da snapshot: deduplicazione remota (P0)
# Rollback T252 (#562 c.6471475567): ripristinato uno snapshot vecchio, il dispatcher ha riletto il commento di rerun
# di #575 e le transizioni di #577 come nuovi (assenti dall'archivio) e ha creato build doppie. L'archivio locale non
# prova che un intento non sia stato eseguito: prima di creare si cercano le build remote con lo stesso `pf_intent`.
import sqlite3 as _sqlite3

RERUN_TID = "p42-r9101-aaaaaaaaaaaa"


def _snapshot(disp, path):
    dst = _sqlite3.connect(str(path))
    disp.store._db.backup(dst)
    dst.close()


def _restore(disp, path):
    """Ripristino di uno snapshot come nel rollback T252: l'archivio torna allo stato salvato."""
    disp.store.close()
    src, dst = _sqlite3.connect(str(path)), _sqlite3.connect(disp.cfg.state_db)
    src.backup(dst)
    dst.close()
    src.close()
    disp.store = td.Store(disp.cfg.state_db)


def _consume_rerun(disp, bk, clock, cid=9101, finish=True):
    bk.gh.comment(cid, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == [f"p42-r{cid}-aaaaaaaaaaaa"]
    disp.tick()
    if finish:
        bk.finish("pr-overlap-guard", 2, "passed", result("overlap"))
        bk.finish("pr-merge-readiness", 2, "passed", result("readiness"))
        disp.tick()


def _intent_jobs(disp, tid=RERUN_TID):
    return [j for j in disp.store.recent_jobs() if j["delivery_id"] == tid]


@pytest.fixture()
def snap_env(env, tmp_path):
    disp, bk, pub, clock = env
    bk.clock = clock
    _rerun_ready(disp, bk)
    snap = tmp_path / "snapshot.sqlite3"
    _snapshot(disp, snap)                                          # snapshot PRIMA del rerun
    return disp, bk, pub, clock, snap


def test_restored_snapshot_does_not_replay_a_consumed_and_finished_rerun(snap_env):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock)
    posts, finals = len(_posts(bk)), len(pub.final())
    clock.now += 600                                               # fuori dalla finestra temporale del nuovo job
    _restore(disp, snap)
    for _ in range(3):                                             # polling ripetuto dopo il ripristino
        disp.poll()
        disp.tick()
    assert len(_posts(bk)) == posts, "build doppia dopo il ripristino dello snapshot"
    assert len(pub.final()) == finals, "esito vecchio ripubblicato"
    jobs = _intent_jobs(disp)
    assert jobs and all(j["outcome"] == "recovered" and j["build_number"] == 2 for j in jobs)


def test_restored_snapshot_adopts_a_rerun_build_still_running(snap_env):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock, finish=False)
    posts = len(_posts(bk))
    clock.now += 600
    _restore(disp, snap)
    disp.poll()
    disp.tick()
    assert len(_posts(bk)) == posts
    assert {j["state"] for j in _intent_jobs(disp)} == {"dispatched"}
    assert {j["build_number"] for j in _intent_jobs(disp)} == {2}
    bk.finish("pr-overlap-guard", 2, "passed", result("overlap"))
    bk.finish("pr-merge-readiness", 2, "passed", result("readiness"))
    disp.tick()
    assert len(_posts(bk)) == posts and {j["state"] for j in _intent_jobs(disp)} == {"done"}


@pytest.mark.parametrize("flag", ["fail_list_intent", "full_page_intent"])
def test_unverifiable_remote_state_blocks_new_builds_then_errors(snap_env, flag):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock)
    posts = len(_posts(bk))
    _restore(disp, snap)
    setattr(bk, flag, True)
    disp.poll()
    for _ in range(3):
        disp.tick()
    assert len(_posts(bk)) == posts and {j["state"] for j in _intent_jobs(disp)} == {"queued"}
    clock.now += 1000                                              # oltre la finestra della build (900 s)
    disp.tick()
    assert len(_posts(bk)) == posts, "creata una build con lo stato remoto incerto"
    errors = [c for c in pub.final() if "RECOVERY_UNCERTAIN" in c[5]]
    assert {c[3] for c in errors} == {"overlap-check", "pr-merge-readiness"}
    assert all(c[2] == "error" for c in errors)


def test_new_legit_rerun_after_restore_runs_exactly_once(snap_env):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock)
    _restore(disp, snap)
    disp.poll()
    disp.tick()
    posts = len(_posts(bk))
    bk.gh.comment(9102, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == ["p42-r9102-aaaaaaaaaaaa"]
    disp.tick()
    clock.now += 30
    disp.poll()
    disp.tick()
    new = _posts(bk)[posts:]
    assert len(new) == 2 and {p["env"]["PF_DELIVERY_ID"] for p in new} == {"p42-r9102-aaaaaaaaaaaa"}


def test_normal_restart_keeps_reruns_deduplicated_without_remote_effects(snap_env):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock)
    posts = len(_posts(bk))
    disp.store.close()
    disp.store = td.Store(disp.cfg.state_db)                       # riavvio: archivio intatto
    for _ in range(3):
        clock.now += 30
        disp.poll()
        disp.tick()
    assert len(_posts(bk)) == posts
    assert disp.store.recent_reruns()[0]["comment_id"] == 9101 and len(disp.store.recent_reruns()) == 1


def test_crash_between_build_creation_and_local_record_then_restore(snap_env):
    """La build e' creata ma la risposta si perde (crash/timeout) e poi l'archivio torna allo snapshot."""
    disp, bk, pub, clock, snap = snap_env
    bk.fail_create, bk.create_lands = "transport", True
    bk.gh.comment(9101, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    disp.poll()
    disp.tick()
    landed = len(bk.builds["pr-overlap-guard"]) + len(bk.builds["pr-merge-readiness"])
    bk.fail_create, bk.create_lands = None, False
    _restore(disp, snap)
    disp.poll()
    disp.tick()
    assert len(bk.builds["pr-overlap-guard"]) + len(bk.builds["pr-merge-readiness"]) == landed
    assert {j["build_number"] for j in _intent_jobs(disp)} == {2}


def test_two_dispatchers_racing_on_the_same_intent_keep_one_build(snap_env, tmp_path):
    disp, bk, pub, clock, snap = snap_env
    raced = []

    def concurrent(pipeline, payload):
        # l'altra istanza crea la build dello stesso intento un attimo prima di noi
        if not raced and payload["env"]["PF_DELIVERY_ID"] == RERUN_TID:
            raced.append(bk._new_build(pipeline, payload)["number"])
    bk.on_create = concurrent
    _consume_rerun(disp, bk, clock, finish=False)
    for pipe in ("pr-overlap-guard", "pr-merge-readiness"):
        live = [n for n, b in bk.builds[pipe].items() if b["meta_data"].get("pf_intent") == RERUN_TID
                and (pipe, n) not in bk.canceled]
        assert len(live) == 1, (pipe, live)
    assert {j["build_number"] for j in _intent_jobs(disp)} <= {2, 3}


def test_second_instance_with_an_old_archive_adopts_instead_of_creating(snap_env, tmp_path):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock, finish=False)
    posts = len(_posts(bk))
    raw = raw_config(tmp_path)
    raw["state_db"] = str(tmp_path / "other.sqlite3")
    other = make_dispatcher(td.validate_config(raw), bk, pub, clock)
    try:
        _restore(other, snap)                                      # seconda istanza partita da una copia vecchia
        other.poll()
        other.tick()
        assert len(_posts(bk)) == posts
        assert {j["build_number"] for j in _intent_jobs(other)} == {2}
    finally:
        other.store.close()


def test_head_change_during_recovery_invalidates_the_old_rerun_and_dedups_the_transition(snap_env):
    disp, bk, pub, clock, snap = snap_env
    _consume_rerun(disp, bk, clock)
    bk.gh.put(42, HEAD2)                                           # push dopo lo snapshot
    disp.poll()
    disp.tick()
    posts = len(_posts(bk))
    _restore(disp, snap)
    disp.poll()
    disp.tick()
    assert len(_posts(bk)) == posts, "transizione o rerun rigiocati dopo il ripristino"
    audit = {r["comment_id"]: r for r in disp.store.recent_reruns()}
    assert audit[9101]["accepted"] == 0 and "head corrente" in audit[9101]["reason"]


def test_transitions_replayed_from_a_restored_snapshot_create_no_builds(env, tmp_path):
    """Il caso #577 di T252: transizioni gia' eseguite rigiocate dall'archivio ripristinato."""
    disp, bk, pub, clock = env
    bk.clock = clock
    snap = tmp_path / "snap.sqlite3"
    _snapshot(disp, snap)
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    posts = len(_posts(bk))
    clock.now += 600
    _restore(disp, snap)
    disp.poll()
    disp.tick()
    assert len(_posts(bk)) == posts


def test_rerun_older_than_the_maximum_age_is_rejected_by_its_github_timestamp(env):
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    disp.store.set_meta("comments_floor", td.iso(clock.now - td.RERUN_MAX_AGE_SECONDS - 100))
    disp.store.set_meta("comments_since", td.iso(clock.now - td.RERUN_MAX_AGE_SECONDS - 100))
    bk.gh.comment(9103, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now - td.RERUN_MAX_AGE_SECONDS - 10))
    assert disp.poll() == []
    audit = disp.store.recent_reruns()[0]
    assert audit["accepted"] == 0 and "piu' vecchio" in audit["reason"]


def test_rerun_on_a_pr_missing_from_the_open_listing_is_rejected(env):
    """Il caso #575 di T252: PR gia' mergiata ma ancora `open` nell'archivio ripristinato (serve una seconda assenza
    per chiuderla). Un rerun su una PR assente dall'ultimo elenco non e' mai eseguito."""
    disp, bk, _, clock = env
    _rerun_ready(disp, bk)
    posts = len(_posts(bk))
    bk.gh.pulls.pop(42)
    bk.gh.comment(9104, 42, f"/pf-trusted-rerun {HEAD}", created=td.iso(clock.now + 10))
    clock.now += 20
    assert disp.poll() == []
    disp.tick()
    audit = disp.store.recent_reruns()[0]
    assert audit["accepted"] == 0 and "assente dall'ultimo elenco" in audit["reason"]
    assert len(_posts(bk)) == posts


# --- #562 P1: una readiness di uno SHA superato non ritarda i controlli dell'head corrente.

def test_obsolete_readiness_build_is_cancelled_in_the_same_round_that_starts_the_new_overlap(env):
    """Push nuovo mentre la readiness del vecchio head gira: nello STESSO giro la build vecchia e' annullata (agent
    trusted liberato) e l'overlap del nuovo head e' creato; nessun esito sul vecchio head."""
    disp, bk, pub, _ = env
    bk.gh.put(42, HEAD)
    disp.poll()
    disp.tick()
    bk.finish("pr-overlap-guard", 1, "passed", result("overlap"))
    disp.tick()
    assert len(bk.builds["pr-merge-readiness"]) == 1 and not bk.canceled
    bk.gh.put(42, HEAD2)
    disp.poll()
    disp.tick()
    assert ("pr-merge-readiness", 1) in bk.canceled
    assert len(bk.builds["pr-overlap-guard"]) == 2 and len(bk.builds["pr-merge-readiness"]) == 2
    assert bk.builds["pr-overlap-guard"][2]["env"]["PF_PR_HEAD_SHA"] == HEAD2
    assert not [c for c in pub.final() if c[1] == HEAD and c[3] == "pr-merge-readiness"]


def test_waiting_obsolete_readiness_holds_no_agent_and_starts_nothing(wait_env):
    """Readiness in attesa (opzione A) del vecchio head: nessuna build da annullare, nessuna build creata dopo la
    sostituzione; l'overlap del nuovo head parte subito."""
    disp, bk, pub, clock = wait_env
    _open_checks(bk.gh)
    bk.gh.put(42, HEAD)
    disp.poll()
    _tick(disp, clock)
    bk.gh.put(42, HEAD2)
    disp.poll()
    _tick(disp, clock)
    assert not bk.builds["pr-merge-readiness"] and bk.canceled == [("pr-overlap-guard", 1)]   # solo l'overlap vecchio
    assert [b["env"]["PF_PR_HEAD_SHA"] for b in bk.builds["pr-overlap-guard"].values()] == [HEAD, HEAD2]
    old = [j for j in disp.store.recent_jobs() if j["guard"] == "readiness" and j["head_sha"] == HEAD]
    assert [j["state"] for j in old] == ["superseded"]


def test_full_suite_intent_lookup_is_scoped_to_the_pr_commit_not_main(suite_env):
    """#562 P0 + PR-A: la build della suite e' sul branch della PR, quindi la ricerca per intento va per commit
    dell'head (come `list_builds`); su `main` non la troverebbe mai e la deduplicazione remota sarebbe cieca."""
    disp, bk, pub, clock = suite_env
    bk.fail_create, bk.create_lands = "transport", True
    bk.gh.put(42, HEAD, ref="feature/x")
    disp.poll()
    disp.tick()
    bk.fail_create = None
    disp.tick()
    queries = [urllib.parse.parse_qs(urllib.parse.urlsplit(u).query) for m, u, _, _ in bk.requests
               if m == "GET" and f"/pipelines/{SUITE_PIPE}/builds?" in u and "pf_intent" in urllib.parse.unquote(u)]
    assert queries and all(q.get("commit") == [HEAD] and "branch" not in q for q in queries)
    suite = [j for j in disp.store.recent_jobs() if j["guard"] == "full-suite"][0]
    assert suite["state"] == "dispatched" and len(bk.builds[SUITE_PIPE]) == 1
