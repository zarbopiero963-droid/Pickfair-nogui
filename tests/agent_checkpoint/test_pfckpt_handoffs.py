"""pfckpt `handoffs`: consegna di una patch da un agente (Claude C, senza PR proprie) a un altro (Claude B) che la
integra su main. Git REALE, offline (repository bare locali).

Regole (decisione owner): source_sha risolvibile e durevole come code.sha; digest della patch inclusa = patch_sha256;
DELIVERED non e' mai «integrato»; INTEGRATED_VERIFIED solo se verify RICALCOLA che patch applicata su base_sha da' il
tree di integration_sha, altrimenti va dichiarato INTEGRATED_NOT_PROVEN.
"""
import hashlib
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "tools", "agent_checkpoint"))
import pfckpt  # noqa: E402
from test_pfckpt import BR, CODE_BR, D, _cfg, agent, expect, next_state, raw_commit, sh, world  # noqa: E402,F401

C_BRANCH = "claude-c/delivery"
DURABLE = ("main", C_BRANCH)


def _code(world):
    path = str(world["tmp"] / "codework")
    if not os.path.exists(path):
        subprocess.run(["git", "clone", "-q", "--no-local", "--branch", "main", world["url"], path], check=True)
        _cfg(path)
    return path


def deliver(world):
    """Claude C: commit sul proprio branch e patch base..source. Ritorna (base, source, patch_bytes)."""
    c = _code(world)
    base = sh(c, "rev-parse", "HEAD")
    sh(c, "checkout", "-q", "-b", "c")
    with open(os.path.join(c, "app.py"), "a") as fh:
        fh.write("print('patch di C')\n")
    with open(os.path.join(c, "nuovo.py"), "w") as fh:
        fh.write("Y = 2\n")
    sh(c, "add", "-A"); sh(c, "commit", "-q", "-m", "lavoro di C")
    source = sh(c, "rev-parse", "HEAD")
    sh(c, "push", "-q", "origin", f"HEAD:refs/heads/{C_BRANCH}")
    patch = subprocess.run(["git", "-C", c, "diff", "--binary", base, source], capture_output=True, check=True).stdout
    return base, source, patch


def integrate_on_main(world, base, patch, extra=None):
    """Claude B: applica la patch su main (con `extra`, una modifica in piu') e pubblica. Ritorna lo SHA."""
    c = _code(world)
    sh(c, "fetch", "-q", "origin")
    sh(c, "checkout", "-q", "-B", "b", base)
    subprocess.run(["git", "-C", c, "apply", "--index", "-"], input=patch, check=True)
    if extra:
        with open(os.path.join(c, extra), "w") as fh:
            fh.write("aggiunta di B\n")
        sh(c, "add", "-A")
    sh(c, "commit", "-q", "-m", "integrazione patch di C")
    sha = sh(c, "rev-parse", "HEAD")
    sh(c, "push", "-q", "origin", "HEAD:refs/heads/main")
    return sha


def record(source, patch, status="DELIVERED", **extra):
    return {"checkpoint_repo": "zarbopiero963-droid/Pickfair-nogui",
            "source_repo": "zarbopiero963-droid/Pickfair-nogui", "source_branch": C_BRANCH, "source_sha": source,
            "patch_sha256": hashlib.sha256(patch).hexdigest(), "target_repo": "zarbopiero963-droid/Pickfair-nogui",
            "handoff_ref": "https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/1#issuecomment-1",
            "status": status, **extra}


def put_patch(repo, patch, name="handoff/c.patch"):
    path = os.path.join(repo, D, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(patch)
    return name


def b_agent(world, name="b"):
    a = agent(world, name)
    sh(a, "fetch", "-q", "origin", "+refs/heads/*:refs/remotes/origin/*")
    return a


def commit_handoff(world, name, handoffs, patch=None):
    a = b_agent(world, name)
    if patch is not None:
        put_patch(a, patch)
    next_state(a, world, handoffs=handoffs)
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt con handoff")
    return a


# ================================================================== consegna
def test_delivery_on_durable_branch_is_saved_and_never_integrated(world):
    base, source, patch = deliver(world)
    a = b_agent(world, "a")
    art = put_patch(a, patch)
    exp = next_state(a, world, handoffs=[record(source, patch, artifact=art)])
    res = pfckpt.save(a, BR, "origin", exp, "consegna di C", D, agent="claude-b", durable_branches=DURABLE,
                      release=True)
    assert res["status"] == "DURABLE"
    assert {r["label"]: r["via"] for r in res["durability"]}["handoff[0]"] == f"BRANCH:{C_BRANCH}"
    assert res["handoffs"] == [{"index": 0, "status": "DELIVERED", "source_reachable": True, "handoff_proven": False,
                                "verified": False}]
    integrate_on_main(world, base, patch)                         # B integra, ma il record resta DELIVERED:
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["handoffs"][0]["status"] == "DELIVERED"
    assert rep["handoffs"][0]["verified"] is False, "nessuna inferenza automatica da patch a main"


def test_delivery_from_a_non_durable_branch_is_refused_at_save(world):
    _base, source, patch = deliver(world)
    a = b_agent(world, "a")
    exp = next_state(a, world, handoffs=[record(source, patch)])
    err = expect(2, pfckpt.save, a, BR, "origin", exp, "consegna", D, agent="claude-b")   # solo main durevole
    assert err.reason == "CODE_SHA_NOT_DURABLE"
    assert [r["label"] for r in err.detail["not_durable"]] == ["handoff[0]"]


# ================================================================== integrazione
def test_integration_with_equal_tree_is_verified(world, capsys):
    base, source, patch = deliver(world)
    integ = integrate_on_main(world, base, patch)
    a = b_agent(world, "a")
    pfile = str(world["tmp"] / "c.patch")
    open(pfile, "wb").write(patch)
    rc = pfckpt.main(["handoff-verify", "--repo", a, "--source", source, "--patch", pfile, "--base", base,
                      "--integration", integ])
    rec = json.loads(capsys.readouterr().out)
    assert rc == 0 and rec["status"] == "INTEGRATED_VERIFIED" and rec["applied_tree"] == rec["integration_tree"]
    assert rec["patch_sha256"] == hashlib.sha256(patch).hexdigest()
    art = put_patch(a, patch)
    exp = next_state(a, world, handoffs=[record(source, patch, "INTEGRATED_VERIFIED", artifact=art, base_sha=base,
                                                integration_sha=integ, integration_tree=rec["integration_tree"])])
    res = pfckpt.save(a, BR, "origin", exp, "integrata", D, agent="claude-b", durable_branches=DURABLE)
    assert res["handoffs"][0]["verified"] is True


def test_integration_with_one_extra_change_is_not_proven_and_a_verified_claim_is_invalid(world, capsys):
    base, source, patch = deliver(world)
    integ = integrate_on_main(world, base, patch, extra="extra.py")
    a = b_agent(world, "probe")
    pfile = str(world["tmp"] / "c.patch")
    open(pfile, "wb").write(patch)
    rc = pfckpt.main(["handoff-verify", "--repo", a, "--source", source, "--patch", pfile, "--base", base,
                      "--integration", integ])
    assert rc == 2 and json.loads(capsys.readouterr().out)["status"] == "INTEGRATED_NOT_PROVEN"
    honest = commit_handoff(world, "honest", [record(source, patch, "INTEGRATED_NOT_PROVEN", artifact="handoff/c.patch",
                                                     base_sha=base, integration_sha=integ)], patch)
    assert pfckpt.verify(honest, "HEAD", D)["handoffs"][0]["verified"] is False
    liar = commit_handoff(world, "liar", [record(source, patch, "INTEGRATED_VERIFIED", artifact="handoff/c.patch",
                                                 base_sha=base, integration_sha=integ)], patch)
    err = expect(2, pfckpt.verify, liar, "HEAD", D)
    assert any("INTEGRATED_VERIFIED non provato" in p for p in err.detail["problems"]), err.detail["problems"]
    assert not set(err.detail["problems"]) <= set(err.detail["reference_problems"]), "integrita', non riferimento"
    sh(liar, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "LATEST_CHECKPOINT_CORRUPT"


def test_verified_without_the_patch_in_the_checkpoint_is_not_proven(world):
    base, source, patch = deliver(world)
    integ = integrate_on_main(world, base, patch)
    a = commit_handoff(world, "a", [record(source, patch, "INTEGRATED_VERIFIED", base_sha=base, integration_sha=integ)])
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("INTEGRATED_VERIFIED non provato" in p for p in err.detail["problems"])


def test_patch_digest_mismatch_is_invalid(world):
    _base, source, patch = deliver(world)
    rec = record(source, patch, artifact="handoff/c.patch")
    a = commit_handoff(world, "a", [rec], patch + b"\n# alterata\n")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("patch_sha256" in p for p in err.detail["problems"]), err.detail["problems"]


@pytest.mark.parametrize("field,value", [("status", "INTEGRATED"), ("source_sha", "HEAD"), ("patch_sha256", "x"),
                                         ("handoff_ref", ""), ("integration_sha", "main")])
def test_malformed_handoff_record_is_invalid(world, field, value):
    _base, source, patch = deliver(world)
    rec = record(source, patch)
    rec[field] = value
    a = commit_handoff(world, "a", [rec])
    expect(2, pfckpt.verify, a, "HEAD", D)


# ================================================================== branch sorgente cancellato
def test_source_branch_deleted_before_reconciliation_is_needs_reconciliation(world):
    _base, source, patch = deliver(world)
    a = b_agent(world, "a")
    art = put_patch(a, patch)
    exp = next_state(a, world, handoffs=[record(source, patch, artifact=art)])
    pfckpt.save(a, BR, "origin", exp, "consegna", D, agent="claude-b", durable_branches=DURABLE, release=True)
    sh(world["url"], "update-ref", "-d", f"refs/heads/{C_BRANCH}")
    sh(world["url"], "reflog", "expire", "--expire=now", "--all")
    sh(world["url"], "gc", "-q", "--prune=now")
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION"
    assert any("handoff[0]" in p for p in err.detail["problems"])
