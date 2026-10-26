"""Lane Buildkite trusted (issue #562): `scripts/ci/pr_overlap_guard.sh` e `scripts/ci/pr_merge_readiness.sh`.

Il codice reale (`scripts/ci/trusted_pr_guard.py`) gira contro un'API GitHub finta: solo il trasporto HTTP e il
lancio di `scripts/pr_merge_readiness.py` sono sostituiti. Nessuna rete, nessun token reale. Ogni caso negativo
deve finire FAIL con exit 1: un dato mancante, incoerente o non verificabile non e' mai PASS.
"""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import trusted_pr_guard as g  # noqa: E402

REPO = g.TRUSTED_REPO
HEAD = "a" * 40
BASE = "b" * 40
TOKEN = "github_pat_" + "T0k3n" * 8


# --------------------------------------------------------------------------------------------- API finta
class FakeGitHub:
    """GitHub REST minimo con stato: PR aperte, file per PR, compare della base, header degli scope."""

    def __init__(self):
        self.prs = {561: self.pr(561)}
        self.files = {561: ["core/microstake_safe.py", "docs/microstake_native.md"]}
        self.compare_status = "ahead"
        self.scopes = None                    # token fine-grained: nessun header X-OAuth-Scopes
        self.fail = {}                        # path -> status HTTP
        self.calls = []
        self.page_size = 100
        self.after_first_read = None          # callback: modifica lo stato dopo la prima lettura della PR

    @staticmethod
    def pr(number, *, head=HEAD, base=BASE, state="open", head_repo=REPO, base_repo=REPO, ref="main",
           changed=None):
        return {"number": number, "state": state, "draft": False, "url": f"{g.API_ROOT}/repos/{REPO}/pulls/{number}",
                "head": {"sha": head, "repo": {"full_name": head_repo} if head_repo else None},
                "base": {"sha": base, "ref": ref, "repo": {"full_name": base_repo}},
                "changed_files": changed}

    def __call__(self, url, headers):
        assert url.startswith(g.API_ROOT + "/")
        assert headers["Authorization"] == f"Bearer {TOKEN}"
        path = url[len(g.API_ROOT):]
        self.calls.append(path)
        bare = path.split("?")[0]
        if bare in self.fail:
            return self.fail[bare], {}, b""
        hdr = {}
        if bare == f"/repos/{REPO}":
            if self.scopes is not None:
                hdr["X-OAuth-Scopes"] = self.scopes
            return 200, hdr, json.dumps({"full_name": REPO}).encode()
        if bare == f"/repos/{REPO}/pulls" and "state=open" in path:
            data = [p for p in self.prs.values() if p["state"] == "open"]
            return 200, hdr, json.dumps(data).encode()
        if bare.startswith(f"/repos/{REPO}/compare/"):
            return 200, hdr, json.dumps({"status": self.compare_status}).encode()
        parts = bare.split("/")
        if len(parts) >= 6 and parts[4] == "pulls":
            n = int(parts[5])
            if n not in self.prs:
                return 404, {}, b""
            if len(parts) == 6:
                body = dict(self.prs[n])
                if body["changed_files"] is None:
                    body["changed_files"] = len(self.files.get(n, []))
                out = 200, hdr, json.dumps(body).encode()
                if self.after_first_read:
                    cb, self.after_first_read = self.after_first_read, None
                    cb(self)
                return out
            if parts[6] == "files":
                page = int((path.split("page=")[-1]) if "&page=" in path else 1)
                names = self.files.get(n, [])
                chunk = names[(page - 1) * self.page_size: page * self.page_size]
                if page * self.page_size < len(names):
                    hdr["Link"] = f'<{g.API_ROOT}{bare}?per_page=100&page={page + 1}>; rel="next"'
                return 200, hdr, json.dumps([{"filename": f} for f in chunk]).encode()
        return 404, {}, b""


def env_for(tmp_path, **over):
    token_file = tmp_path / "github-token"
    if not token_file.exists():
        token_file.write_text(TOKEN + "\n")
        token_file.chmod(0o600)
    env = {"PF_PR_NUMBER": "561", "PF_PR_HEAD_SHA": HEAD, "PF_GITHUB_TOKEN_FILE": str(token_file),
           "PF_RESULT_DIR": str(tmp_path / "out"), "BUILDKITE_REPO": f"https://github.com/{REPO}.git",
           "BUILDKITE_BRANCH": "main", "BUILDKITE_PULL_REQUEST": "false"}
    env.update(over)
    return {k: v for k, v in env.items() if v is not None}


def clean_decision(**over):
    d = {"can_merge": True, "headRefOid": HEAD, "is_open": True, "is_draft": False, "blockers": [], "pending": [],
         "reasons": [], "ai_review_gate_status": "PASS"}
    d.update(over)
    return d


def runner_returning(decision, rc=0, seen=None):
    def run(repo_root, pr, wait, token, output):
        if seen is not None:
            seen.update(pr=pr, wait=wait, token=token)
        if decision is not None:
            Path(output).write_text(json.dumps(decision))
        return rc
    return run


def result(tmp_path, guard):
    return json.loads((tmp_path / "out" / f"{guard}.json").read_text())


def run(guard, tmp_path, fake, env=None, argv=None, decision=None, rc=0, seen=None):
    return g.main(guard, argv or [], env if env is not None else env_for(tmp_path), transport=fake,
                  readiness_runner=runner_returning(decision if decision is not None else clean_decision(), rc, seen),
                  repo_root=ROOT)


# --------------------------------------------------------------------------------------------- positivi
def test_overlap_passes_with_one_open_pr_and_records_the_verified_shas(tmp_path):
    fake = FakeGitHub()
    assert run("overlap", tmp_path, fake) == 0
    res = result(tmp_path, "overlap")
    assert res["verdict"] == "PASS" and res["head_sha"] == HEAD and res["base_sha"] == BASE and res["pr"] == 561


def test_readiness_passes_only_with_a_clean_decision_on_the_expected_head(tmp_path):
    fake, seen = FakeGitHub(), {}
    assert run("readiness", tmp_path, fake, seen=seen) == 0
    assert seen == {"pr": 561, "wait": g.READINESS_WAIT_DEFAULT, "token": TOKEN}
    assert result(tmp_path, "readiness")["verdict"] == "PASS"


def test_ssh_repository_url_is_accepted():
    ctx = g.parse_environment({"PF_PR_NUMBER": "7", "PF_PR_HEAD_SHA": HEAD,
                               "BUILDKITE_REPO": f"git@github.com:{REPO}.git"})
    assert ctx == {"pr": 7, "head_sha": HEAD, "base_ref": "main"}


def test_credentials_directory_is_the_default_token_source(tmp_path):
    (tmp_path / "github-token").write_text(TOKEN)
    (tmp_path / "github-token").chmod(0o400)
    assert g.load_token({"CREDENTIALS_DIRECTORY": str(tmp_path)}) == TOKEN


# --------------------------------------------------------------------------------------------- ambiente / argomenti
@pytest.mark.parametrize("value", [None, "", "0", "0561", "-1", "1e3", " 561", "561 ", "561;id", "５６１", "12345678",
                                   "561\n", "0x231"])
def test_invalid_pr_number_is_refused(tmp_path, value):
    fake = FakeGitHub()
    assert run("overlap", tmp_path, fake, env=env_for(tmp_path, PF_PR_NUMBER=value)) == 1
    assert fake.calls == [], "nessuna chiamata API con un numero PR non valido"


@pytest.mark.parametrize("value", [None, "", "A" * 40, "a" * 39, "a" * 41, "g" * 40, HEAD + "\n"])
def test_invalid_head_sha_is_refused(tmp_path, value):
    fake = FakeGitHub()
    assert run("readiness", tmp_path, fake, env=env_for(tmp_path, PF_PR_HEAD_SHA=value)) == 1
    assert fake.calls == []


@pytest.mark.parametrize("repo", ["https://github.com/zarbopiero963-droid/Pickfair-nogui-evil.git",
                                  "https://github.com/attacker/Pickfair-nogui.git",
                                  "https://gitlab.com/zarbopiero963-droid/Pickfair-nogui.git",
                                  "https://github.com.evil.io/zarbopiero963-droid/Pickfair-nogui.git", ""])
def test_foreign_buildkite_repository_is_refused(tmp_path, repo):
    assert run("overlap", tmp_path, FakeGitHub(), env=env_for(tmp_path, BUILDKITE_REPO=repo)) == 1


@pytest.mark.parametrize("over", [{"BUILDKITE_PULL_REQUEST": "561"}, {"BUILDKITE_BRANCH": "feature"},
                                  {"BUILDKITE_BRANCH": "refs/pull/561/head"}, {"PF_PR_BASE_REF": "../main"}])
def test_untrusted_buildkite_context_is_refused(tmp_path, over):
    fake = FakeGitHub()
    assert run("overlap", tmp_path, fake, env=env_for(tmp_path, **over)) == 1
    assert fake.calls == []


def test_command_line_arguments_are_refused(tmp_path):
    fake = FakeGitHub()
    assert run("readiness", tmp_path, fake, argv=["--pr", "999"]) == 1
    assert fake.calls == [] and "nessun argomento" in result(tmp_path, "readiness")["reasons"][0]


@pytest.mark.parametrize("wait", ["-1", "abc", "6601", "99999", "1.5"])
def test_invalid_readiness_wait_is_refused(tmp_path, wait):
    assert run("readiness", tmp_path, FakeGitHub(), env=env_for(tmp_path, PF_READINESS_WAIT_SECONDS=wait)) == 1


# --------------------------------------------------------------------------------------------- token
def test_missing_token_is_refused(tmp_path):
    env = env_for(tmp_path)
    env.pop("PF_GITHUB_TOKEN_FILE")
    assert run("overlap", tmp_path, FakeGitHub(), env=env) == 1


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o660])
def test_token_file_readable_by_others_is_refused(tmp_path, mode):
    env = env_for(tmp_path)
    Path(env["PF_GITHUB_TOKEN_FILE"]).chmod(mode)
    fake = FakeGitHub()
    assert run("overlap", tmp_path, fake, env=env) == 1
    assert fake.calls == []


@pytest.mark.parametrize("content", ["", "   \n", "token with spaces", "x" * 10, "ghp_" + "é" * 30])
def test_malformed_token_is_refused(tmp_path, content):
    env = env_for(tmp_path)
    p = Path(env["PF_GITHUB_TOKEN_FILE"])
    p.write_text(content, encoding="utf-8")
    p.chmod(0o600)
    assert run("overlap", tmp_path, FakeGitHub(), env=env) == 1


def test_token_file_that_is_not_a_regular_file_is_refused(tmp_path):
    d = tmp_path / "dir"
    d.mkdir(mode=0o700)
    assert run("overlap", tmp_path, FakeGitHub(), env=env_for(tmp_path, PF_GITHUB_TOKEN_FILE=str(d))) == 1


@pytest.mark.parametrize("scopes", ["repo", "public_repo", "repo, workflow", "admin:org"])
def test_classic_token_with_scopes_is_refused(tmp_path, scopes):
    fake = FakeGitHub()
    fake.scopes = scopes
    assert run("overlap", tmp_path, fake) == 1
    assert "sola lettura" in result(tmp_path, "overlap")["reasons"][0]


def test_classic_token_without_scopes_is_accepted(tmp_path):
    fake = FakeGitHub()
    fake.scopes = ""
    assert run("overlap", tmp_path, fake) == 0


def test_token_never_appears_in_output_or_result(tmp_path, capsys):
    fake = FakeGitHub()
    leak = clean_decision(can_merge=False, reasons=[f"errore con {TOKEN}"])
    assert run("readiness", tmp_path, fake, decision=leak) == 1
    out = capsys.readouterr()
    text = out.out + out.err + (tmp_path / "out" / "readiness.json").read_text()
    assert TOKEN not in text and "***" in text


# --------------------------------------------------------------------------------------------- provenienza PR
@pytest.mark.parametrize("change,needle", [
    ({"state": "closed"}, "non aperta"),
    ({"head": HEAD.replace("a", "c")}, "head della PR cambiato"),
    ({"head_repo": "attacker/Pickfair-nogui"}, "fork"),
    ({"head_repo": None}, "fork"),
    ({"base_repo": "attacker/Pickfair-nogui"}, "repository base"),
    ({"ref": "release"}, "branch base"),
    ({"base": "not-a-sha"}, "base SHA"),
])
def test_incoherent_pr_provenance_is_refused(tmp_path, change, needle):
    fake = FakeGitHub()
    fake.prs[561] = FakeGitHub.pr(561, **change)
    assert run("overlap", tmp_path, fake) == 1
    assert needle in result(tmp_path, "overlap")["reasons"][0]


@pytest.mark.parametrize("status", ["diverged", "behind", None])
def test_base_sha_outside_main_history_is_refused(tmp_path, status):
    fake = FakeGitHub()
    fake.compare_status = status
    assert run("readiness", tmp_path, fake) == 1


def test_api_returning_another_pr_number_is_refused(tmp_path):
    fake = FakeGitHub()
    fake.prs[561] = dict(FakeGitHub.pr(561), number=562)
    assert run("overlap", tmp_path, fake) == 1


@pytest.mark.parametrize("guard", ["overlap", "readiness"])
def test_head_pushed_during_the_evaluation_is_fail(tmp_path, guard):
    fake = FakeGitHub()

    def push(f):
        f.prs[561] = FakeGitHub.pr(561, head="d" * 40)
    fake.after_first_read = push
    assert run(guard, tmp_path, fake) == 1
    assert "cambiati durante la valutazione" in result(tmp_path, guard)["reasons"][0]


@pytest.mark.parametrize("path", [f"/repos/{REPO}/pulls/561", f"/repos/{REPO}", f"/repos/{REPO}/compare/{BASE}...main"])
@pytest.mark.parametrize("status", [401, 403, 404, 500, 502])
def test_api_errors_are_fail_never_pass(tmp_path, path, status):
    fake = FakeGitHub()
    fake.fail[path] = status
    assert run("readiness", tmp_path, fake) == 1


def test_network_error_is_fail(tmp_path):
    def broken(url, headers):
        raise TimeoutError("timeout")
    assert g.main("overlap", [], env_for(tmp_path), transport=broken) == 1
    assert "non raggiungibile" in result(tmp_path, "overlap")["reasons"][0]


def test_non_json_answer_is_fail(tmp_path):
    def garbage(url, headers):
        return 200, {}, b"<html>proxy</html>"
    assert g.main("overlap", [], env_for(tmp_path), transport=garbage) == 1


def test_urls_outside_the_api_host_are_never_requested():
    gh = g.GitHub(TOKEN, transport=lambda url, headers: pytest.fail("chiamata non attesa"))
    with pytest.raises(g.GuardError):
        gh._get("https://evil.example/repos/x")


def test_pagination_link_to_another_host_is_refused(tmp_path):
    def transport(url, headers):
        return 200, {"Link": '<https://evil.example/steal?page=2>; rel="next"'}, b"[]"
    with pytest.raises(g.GuardError):
        g.GitHub(TOKEN, transport).get_all("/repos/x/pulls")


def test_redirects_are_never_followed():
    handler = g._NoRedirect()
    assert handler.redirect_request(None, None, 302, "Found", {}, "https://evil.example/") is None


# --------------------------------------------------------------------------------------------- overlap
def test_a_second_open_pr_is_fail_on_every_open_pr(tmp_path):
    fake = FakeGitHub()
    fake.prs[570] = FakeGitHub.pr(570, head="e" * 40)
    fake.files[570] = ["README.md"]
    assert run("overlap", tmp_path, fake) == 1
    assert "una sola PR aperta" in result(tmp_path, "overlap")["reasons"][0]


def test_critical_overlap_is_reported(tmp_path):
    fake = FakeGitHub()
    fake.prs[570] = FakeGitHub.pr(570, head="e" * 40)
    fake.files[561].append("core/trading_engine.py")
    fake.files[570] = ["core/trading_engine.py"]
    assert run("overlap", tmp_path, fake) == 1
    reasons = result(tmp_path, "overlap")["reasons"]
    assert any("sovrapposizione critica con #570: core/trading_engine.py" in r for r in reasons)


def test_unreadable_files_of_another_pr_are_fail_not_skipped(tmp_path):
    """Regressione dello step inline del workflow (`except Exception: continue`): una PR illeggibile era saltata."""
    fake = FakeGitHub()
    fake.prs[570] = FakeGitHub.pr(570, head="e" * 40)
    fake.fail[f"/repos/{REPO}/pulls/570/files"] = 500
    assert run("overlap", tmp_path, fake) == 1


def test_files_are_read_across_all_pages(tmp_path):
    fake = FakeGitHub()
    fake.page_size = 2
    fake.files[561] = [f"docs/f{i}.md" for i in range(5)] + ["core/trading_engine.py"]
    assert g.pr_files(g.GitHub(TOKEN, fake), 561, 6) == set(fake.files[561])


@pytest.mark.parametrize("expected", [7, 3000, None, -1, True])
def test_incomplete_or_unverifiable_file_list_is_fail(tmp_path, expected):
    fake = FakeGitHub()
    with pytest.raises(g.GuardError):
        g.pr_files(g.GitHub(TOKEN, fake), 561, expected)


def test_current_pr_missing_from_the_open_list_is_fail(tmp_path):
    fake = FakeGitHub()
    orig = fake.__call__

    def transport(url, headers):
        if "state=open" in url:
            return 200, {}, b"[]"
        return orig(url, headers)
    assert g.main("overlap", [], env_for(tmp_path), transport=transport) == 1


# --------------------------------------------------------------------------------------------- readiness
@pytest.mark.parametrize("over,needle", [
    ({"can_merge": False}, "can_merge"),
    ({"can_merge": "true"}, "can_merge"),
    ({"headRefOid": "d" * 40}, "head diverso"),
    ({"is_draft": True}, "bozza"),
    ({"blockers": [{"name": "tests"}]}, "blockers"),
    ({"pending": [{"name": "tests"}]}, "pending"),
    ({"pending": None}, "pending"),
    ({"ai_review_gate_status": "STOP_OWNER"}, "gate review"),
    ({"ai_review_gate_status": "UNKNOWN"}, "gate review"),
    ({"ai_review_gate_status": None}, "gate review"),
    ({"ai_review_gate_status": "RERUN_REVIEW_ONCE"}, "gate review"),
    ({"reasons": ["2 active unresolved review thread(s)"]}, "unresolved"),
])
def test_readiness_is_fail_unless_everything_is_clean(tmp_path, over, needle):
    assert run("readiness", tmp_path, FakeGitHub(), decision=clean_decision(**over)) == 1
    assert any(needle in r for r in result(tmp_path, "readiness")["reasons"])


def test_readiness_nonzero_exit_is_fail_even_with_a_clean_decision(tmp_path):
    assert run("readiness", tmp_path, FakeGitHub(), rc=1) == 1


def test_readiness_without_a_decision_file_is_fail(tmp_path):
    fake = FakeGitHub()
    assert g.main("readiness", [], env_for(tmp_path), transport=fake,
                  readiness_runner=runner_returning(None), repo_root=ROOT) == 1


def test_readiness_runner_crash_is_fail(tmp_path):
    def boom(*a):
        raise subprocess.TimeoutExpired("x", 1)
    assert g.main("readiness", [], env_for(tmp_path), transport=FakeGitHub(), readiness_runner=boom,
                  repo_root=ROOT) == 1


def test_readiness_wait_is_passed_through(tmp_path):
    seen = {}
    assert run("readiness", tmp_path, FakeGitHub(), env=env_for(tmp_path, PF_READINESS_WAIT_SECONDS="6600"),
               seen=seen) == 0
    assert seen["wait"] == 6600


def test_default_runner_calls_the_existing_readiness_with_a_minimal_environment(tmp_path, monkeypatch):
    captured = {}

    def fake_run(cmd, cwd, env, stdout, stderr, timeout, check):
        captured.update(cmd=cmd, cwd=cwd, env=env, timeout=timeout)
        Path(cmd[cmd.index("--output") + 1]).write_text(json.dumps(clean_decision()))
        return subprocess.CompletedProcess(cmd, 0, stdout=f"token {TOKEN}".encode())
    monkeypatch.setattr(g.subprocess, "run", fake_run)
    monkeypatch.setenv("GITHUB_TOKEN", "leaky-parent-token")
    out = tmp_path / "d.json"
    assert g.default_run_readiness(ROOT, 561, 420, TOKEN, out) == 0
    cmd = captured["cmd"]
    assert cmd[2] == str(ROOT / "scripts" / "pr_merge_readiness.py")
    assert cmd[3:] == ["--repo", REPO, "--pr", "561", "--ignore-safe-autofix", "--wait-pending-seconds", "420",
                       "--poll-seconds", "15", "--output", str(out)]
    assert captured["env"]["GH_TOKEN"] == TOKEN and "GITHUB_TOKEN" not in captured["env"]
    assert captured["timeout"] == 420 + g.READINESS_MARGIN_SECONDS
    assert 420 + g.READINESS_MARGIN_SECONDS + 60 <= 600, "il default deve stare nel timeout di 10 minuti della pipeline"


# --------------------------------------------------------------------------------------------- wrapper shell
@pytest.mark.parametrize("script", ["pr_overlap_guard.sh", "pr_merge_readiness.sh"])
def test_wrappers_are_executable_and_refuse_arguments(script):
    path = ROOT / "scripts" / "ci" / script
    assert os.stat(path).st_mode & stat.S_IXUSR
    proc = subprocess.run([str(path), "--pr", "1"], capture_output=True, text=True, env={"PATH": os.environ["PATH"]})
    assert proc.returncode == 1 and "nessun argomento" in proc.stderr


@pytest.mark.parametrize("script,guard", [("pr_overlap_guard.sh", "overlap"), ("pr_merge_readiness.sh", "readiness")])
def test_wrappers_fail_closed_without_the_controlled_environment(tmp_path, script, guard):
    proc = subprocess.run([str(ROOT / "scripts" / "ci" / script)], capture_output=True, text=True,
                          env={"PATH": os.environ["PATH"], "PF_RESULT_DIR": str(tmp_path)})
    assert proc.returncode == 1 and "PF_PR_NUMBER" in proc.stdout
    assert json.loads((tmp_path / f"{guard}.json").read_text())["verdict"] == "FAIL"


def test_wrappers_ignore_python_environment_injection(tmp_path):
    """`python3 -I`: PYTHONPATH/PYTHONSTARTUP del job non caricano codice nel controllo trusted."""
    evil = tmp_path / "evil"
    evil.mkdir()
    (evil / "json.py").write_text("raise SystemExit('injected')\n")
    proc = subprocess.run([str(ROOT / "scripts" / "ci" / "pr_overlap_guard.sh")], capture_output=True, text=True,
                          env={"PATH": os.environ["PATH"], "PYTHONPATH": str(evil), "PF_RESULT_DIR": str(tmp_path)})
    assert "injected" not in proc.stdout + proc.stderr and proc.returncode == 1


def test_unknown_guard_is_fail(tmp_path):
    assert g.main("deploy", [], env_for(tmp_path), transport=FakeGitHub()) == 1


# --------------------------------------------------------------------------------------------- contratto con la lane
def test_trusted_readiness_status_published_on_the_pr_is_its_own_self_check():
    """Owner (sequenza #562, punto 5): nessuna dipendenza circolare. La readiness esistente ignora come self-check
    lo status che la lane pubblica sulla PR SOLO se la sua URL e' la build della pipeline `pr-merge-readiness` (o il
    nome e' «merge readiness»). Uno status con un'altra identita' verrebbe atteso o bloccherebbe se stesso."""
    sys.path.insert(0, str(ROOT / "scripts"))
    import pr_flow_automation as fa

    build = {"context": "buildkite/pr-merge-readiness",
             "targetUrl": "https://buildkite.com/zarbopiero963-droid/pr-merge-readiness/builds/12"}
    assert fa.is_self_check(build)
    assert not fa.is_self_check({"context": "buildkite/pr-overlap-guard",
                                 "targetUrl": "https://buildkite.com/zarbopiero963-droid/pr-overlap-guard/builds/3"}), \
        "l'overlap guard e' un check reale: la readiness lo deve attendere e richiedere verde"
    assert not fa.is_self_check({"context": "trusted/readiness", "targetUrl": "https://example.invalid/x"})


def test_guards_only_issue_get_requests():
    """Owner (sequenza #562, punto 6): read-only, nessuna pubblicazione diretta di status."""
    src = (ROOT / "scripts" / "ci" / "trusted_pr_guard.py").read_text(encoding="utf-8")
    assert 'method="GET"' in src
    for verb in ("POST", "PATCH", "PUT", "DELETE"):
        assert f'"{verb}"' not in src and f"'{verb}'" not in src
    assert "/statuses" not in src and "/check-runs" not in src
