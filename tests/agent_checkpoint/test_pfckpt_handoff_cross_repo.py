"""PF-MCP-CROSS-REPO-HANDOFF-01: handoff fra repository diversi (git REALE, offline).

La catena di Claude C sta nel bare A (`pickfair-mcp-`). Base, commit di handoff pubblicato da Claude B e integrazione
stanno nel bare B (`Pickfair-nogui`). Ogni riferimento si risolve nel PROPRIO repository (--repo-path NOME=PERCORSO),
mai nel repository della catena per ripiego. Repository non disponibile => NEEDS_RECONCILIATION, mai READY.
"""
import hashlib
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "tools", "agent_checkpoint"))
import pfckpt  # noqa: E402
from test_pfckpt import BR, CODE_BR, D, _cfg, agent, expect, next_state, raw_commit, sh, world  # noqa: E402,F401

CHAIN = "zarbopiero963-droid/pickfair-mcp-"
TARGET = "zarbopiero963-droid/Pickfair-nogui"
HBRANCH = "handoff/claude-c-1"


def _git(*args, **kw):
    return subprocess.run(["git", *args], check=True, capture_output=True, **kw)


@pytest.fixture
def target(world):
    """Bare B con main (base), commit di handoff su HBRANCH e integrazione su main; clone fresco per --repo-path."""
    t = world["tmp"]
    bare = str(t / (TARGET + ".git"))                 # percorso del remoto = owner/name, come su GitHub
    os.makedirs(os.path.dirname(bare), exist_ok=True)
    _git("init", "-q", "--bare", bare)
    w = str(t / "target_work")
    _git("init", "-q", w); _cfg(w)
    open(os.path.join(w, "app.py"), "w").write("print('nogui')\n")
    sh(w, "add", "-A"); sh(w, "commit", "-q", "-m", "base")
    sh(w, "remote", "add", "origin", bare)
    sh(w, "push", "-q", "origin", "HEAD:refs/heads/main")
    base = sh(w, "rev-parse", "HEAD")
    # la patch di C (prodotta fuori dal target: qui un diff su una copia di lavoro)
    sh(w, "checkout", "-q", "-b", "scratch")
    open(os.path.join(w, "app.py"), "a").write("print('patch di C')\n")
    open(os.path.join(w, "mcp_bridge.py"), "w").write("BRIDGE = 1\n")
    sh(w, "add", "-A")
    patch = _git("-C", w, "diff", "--cached", "--binary", base).stdout
    sh(w, "reset", "-q", "--hard", base); sh(w, "checkout", "-q", "main"); sh(w, "branch", "-q", "-D", "scratch")
    return {"bare": bare, "work": w, "base": base, "patch": patch, "tmp": t}


def publish_handoff(tg, extra=None):
    """Claude B applica la patch su base, su HBRANCH (con `extra`: una modifica in piu'), e pubblica."""
    w = tg["work"]
    sh(w, "checkout", "-q", "-B", "h", tg["base"])
    _git("-C", w, "apply", "--index", "-", input=tg["patch"])
    if extra:
        open(os.path.join(w, extra), "w").write("non nella patch\n")
        sh(w, "add", "-A")
    sh(w, "commit", "-q", "-m", "handoff patch di C")
    sh(w, "push", "-q", "-f", "origin", f"HEAD:refs/heads/{HBRANCH}")
    return sh(w, "rev-parse", "HEAD"), sh(w, "rev-parse", "HEAD^{tree}")


def integrate(tg, extra=None):
    w = tg["work"]
    sh(w, "fetch", "-q", "origin")
    sh(w, "checkout", "-q", "-B", "i", "origin/main")
    _git("-C", w, "apply", "--index", "-", input=tg["patch"])
    if extra:
        open(os.path.join(w, extra), "w").write("aggiunta in integrazione\n")
        sh(w, "add", "-A")
    sh(w, "commit", "-q", "-m", "squash handoff")
    sh(w, "push", "-q", "origin", "HEAD:refs/heads/main")
    return sh(w, "rev-parse", "HEAD"), sh(w, "rev-parse", "HEAD^{tree}")


def target_clone(tg, name="target_clone"):
    path = str(tg["tmp"] / name)
    _git("clone", "-q", "--no-local", tg["bare"], path)
    return path


def rec(world, tg, status, **kw):
    r = {"checkpoint_repo": CHAIN, "source_repo": CHAIN, "source_sha": world["code"], "target_repo": TARGET,
         "base_sha": tg["base"], "patch_sha256": hashlib.sha256(tg["patch"]).hexdigest(),
         "handoff_ref": "https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/1#issuecomment-2",
         "artifact": "handoff/c.patch", "status": status}
    r.update(kw)
    return r


def chain_ckpt(world, tg, name, handoffs, push=False):
    a = agent(world, name)
    os.makedirs(os.path.join(a, D, "handoff"), exist_ok=True)
    open(os.path.join(a, D, "handoff", "c.patch"), "wb").write(tg["patch"])
    next_state(a, world, repository=CHAIN, handoffs=handoffs)
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt C con handoff")
    if push:
        sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    return a


def _only_reference_problems(err):
    return set(err.detail["problems"]) <= set(err.detail["reference_problems"])


# ================================================================== repository del target assente
def test_target_repo_absent_is_needs_reconciliation_and_present_is_ready(world, target):
    hsha, htree = publish_handoff(target)
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r], push=True)
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert _only_reference_problems(err) and any("non disponibile" in p for p in err.detail["problems"])
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "f1"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION"
    paths = {TARGET: target_clone(target)}
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "f2"), D, CODE_BR, repo_paths=paths)
    assert rep["status"] == "READY" and rep["handoffs"][0]["handoff_proven"] is True
    assert rep["handoffs"][0]["verified"] is False, "DURABLE non e' integrato"


def test_never_resolved_in_the_chain_repo_as_fallback(world, target):
    """Anche se gli oggetti del target esistono nel repository della catena, senza --repo-path non si risolvono li'."""
    hsha, htree = publish_handoff(target)
    sh(target["work"], "push", "-q", world["url"], f"{hsha}:refs/heads/copia-nella-catena")
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r])
    sh(a, "fetch", "-q", "origin", "+refs/heads/*:refs/remotes/origin/*")
    assert pfckpt.resolve_commit(a, hsha) == hsha, "l'oggetto c'e' nella catena..."
    err = expect(2, pfckpt.verify, a, "HEAD", D)                 # ...ma non vale come prova
    assert _only_reference_problems(err)


# ================================================================== handoff pubblicato e tree uguale
def test_published_handoff_with_equal_tree_is_durable_then_integrated_verified(world, target):
    hsha, htree = publish_handoff(target)
    paths = {TARGET: target_clone(target, "t1")}
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = agent(world, "c")
    os.makedirs(os.path.join(a, D, "handoff"), exist_ok=True)
    open(os.path.join(a, D, "handoff", "c.patch"), "wb").write(target["patch"])
    exp = next_state(a, world, repository=CHAIN, handoffs=[r])
    res = pfckpt.save(a, BR, "origin", exp, "handoff DURABLE", D, agent="claude-c", repo_paths=paths)
    assert res["status"] == "DURABLE" and res["handoffs"][0]["handoff_proven"] is True
    assert {x["label"]: x["via"] for x in res["durability"]}["handoff[0]"] == "BRANCH:main"   # sorgente nella catena
    isha, itree = integrate(target)
    assert itree == htree
    paths = {TARGET: target_clone(target, "t2")}
    r2 = dict(r, status="INTEGRATED_VERIFIED", integration_sha=isha, integration_tree=itree)
    exp2 = next_state(a, world, handoffs=[r2])
    res2 = pfckpt.save(a, BR, "origin", exp2, "handoff integrato", D, agent="claude-c", repo_paths=paths)
    assert res2["status"] == "DURABLE" and res2["handoffs"][0]["verified"] is True


def test_cli_verify_accepts_repo_path_and_rejects_malformed(world, target, capsys):
    hsha, htree = publish_handoff(target)
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r])
    assert pfckpt.main(["verify", "--repo", a, "--dir", D]) == 2
    capsys.readouterr()
    assert pfckpt.main(["verify", "--repo", a, "--dir", D, "--repo-path", f"{TARGET}={target_clone(target)}"]) == 0
    capsys.readouterr()
    assert pfckpt.main(["verify", "--repo", a, "--dir", D, "--repo-path", "senza-percorso"]) == 5


# ================================================================== una modifica in piu'
def test_extra_change_in_handoff_declared_durable_is_invalid(world, target):
    hsha, htree = publish_handoff(target, extra="extra.py")
    paths = {TARGET: target_clone(target)}
    for st in ("DURABLE", "INTEGRATED_VERIFIED"):
        r = rec(world, target, st, handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree,
                integration_sha=hsha, integration_tree=htree)
        a = chain_ckpt(world, target, f"c_{st}", [r])
        err = expect(2, pfckpt.verify, a, "HEAD", D, repo_paths=paths)
        assert any(f"{st} non provato" in p and "patch su base_sha" in p for p in err.detail["problems"])
        assert not _only_reference_problems(err), "integrita', non riferimento"


def test_extra_change_in_integration_declared_verified_is_invalid_not_proven_is_accepted(world, target):
    hsha, htree = publish_handoff(target)
    isha, itree = integrate(target, extra="extra.py")
    paths = {TARGET: target_clone(target)}
    base = dict(handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree, integration_sha=isha)
    liar = chain_ckpt(world, target, "liar", [rec(world, target, "INTEGRATED_VERIFIED", **base)], push=True)
    err = expect(2, pfckpt.verify, liar, "HEAD", D, repo_paths=paths)
    assert any("tree(integration_sha) != handoff_tree" in p for p in err.detail["problems"])
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "f"), D, CODE_BR, repo_paths=paths)
    assert err.reason == "LATEST_CHECKPOINT_CORRUPT"
    honest = chain_ckpt(world, target, "honest", [rec(world, target, "INTEGRATED_NOT_PROVEN", **base)])
    assert pfckpt.verify(honest, "HEAD", D, repo_paths=paths)["handoffs"][0]["verified"] is False


# ================================================================== sorgente non pubblicata
def test_unpublished_source_is_delivered_only_unless_the_handoff_commit_proves_it(world, target):
    unpub = {"source_published": False}
    a = chain_ckpt(world, target, "d", [{k: v for k, v in rec(world, target, "DELIVERED", **unpub).items()
                                         if k != "source_sha"}])
    rep = pfckpt.verify(a, "HEAD", D)                             # nessun repository extra serve per DELIVERED
    assert rep["handoffs"][0] == {"index": 0, "status": "DELIVERED", "source_reachable": False,
                                  "handoff_proven": False, "verified": False}
    for st in ("DURABLE", "INTEGRATED_VERIFIED"):                 # la sorgente non e' mai la prova
        r = {k: v for k, v in rec(world, target, st, **unpub).items() if k != "source_sha"}
        b = chain_ckpt(world, target, f"x_{st}", [r])
        err = expect(2, pfckpt.verify, b, "HEAD", D, repo_paths={TARGET: target_clone(target, f"t_{st}")})
        assert any(f"{st} non provato" in p for p in err.detail["problems"])
    hsha, htree = publish_handoff(target)                         # la prova e' il commit di handoff pubblicato
    r = {k: v for k, v in rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha,
                              handoff_tree=htree, **unpub).items() if k != "source_sha"}
    c = chain_ckpt(world, target, "ok", [r])
    assert pfckpt.verify(c, "HEAD", D, repo_paths={TARGET: target_clone(target)})["handoffs"][0]["handoff_proven"]


def test_published_source_without_sha_and_wrong_checkpoint_repo_are_invalid(world, target):
    r = {k: v for k, v in rec(world, target, "DELIVERED").items() if k != "source_sha"}
    expect(2, pfckpt.verify, chain_ckpt(world, target, "a", [r]), "HEAD", D)
    r = rec(world, target, "DELIVERED", checkpoint_repo="altro/repo")
    expect(2, pfckpt.verify, chain_ckpt(world, target, "b", [r]), "HEAD", D)


def test_handoff_branch_deleted_before_reconciliation_is_needs_reconciliation(world, target):
    hsha, htree = publish_handoff(target)
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r], push=True)
    sh(target["bare"], "update-ref", "-d", f"refs/heads/{HBRANCH}")
    sh(target["bare"], "reflog", "expire", "--expire=now", "--all")
    sh(target["bare"], "gc", "-q", "--prune=now")
    paths = {TARGET: target_clone(target)}
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "f"), D, CODE_BR, repo_paths=paths)
    assert err.reason == "NEEDS_RECONCILIATION"
    assert pfckpt.remote_head(a, "origin", BR) is not None


# ================================================================== DURABLE senza artefatto: digest canonico
def canonical_patch(tg, hsha):
    """`git diff --binary --full-index base handoff` nel target: la forma che la catena di C registra."""
    return _git("-C", tg["work"], "diff", "--binary", "--full-index", tg["base"], hsha).stdout


def rec_no_artifact(world, tg, status, digest, **kw):
    r = {k: v for k, v in rec(world, tg, status, **kw).items() if k != "artifact"}
    r["patch_sha256"] = digest
    return r


def test_durable_without_artifact_is_proven_by_the_canonical_digest(world, target):
    hsha, htree = publish_handoff(target)
    digest = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    r = rec_no_artifact(world, target, "DURABLE", digest, handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r], push=True)
    paths = {TARGET: target_clone(target)}
    rep = pfckpt.verify(a, "HEAD", D, repo_paths=paths)
    assert rep["handoffs"][0]["handoff_proven"] is True and rep["handoffs"][0]["verified"] is False
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "f"), D, CODE_BR, repo_paths=paths)
    assert rep["status"] == "READY"
    # INTEGRATED_VERIFIED senza artefatto, attraverso il commit di handoff pubblicato
    isha, itree = integrate(target)
    paths = {TARGET: target_clone(target, "t2")}
    r2 = dict(r, status="INTEGRATED_VERIFIED", integration_sha=isha, integration_tree=itree)
    b = chain_ckpt(world, target, "c2", [r2])
    assert pfckpt.verify(b, "HEAD", D, repo_paths=paths)["handoffs"][0]["verified"] is True


def test_canonical_digest_ignores_local_diff_config(world, target):
    hsha, htree = publish_handoff(target)
    digest = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    clone = target_clone(target)
    for k, v in (("diff.noprefix", "true"), ("diff.mnemonicPrefix", "true"), ("diff.algorithm", "patience"),
                 ("color.diff", "always"), ("diff.renames", "false")):
        sh(clone, "config", k, v)
    r = rec_no_artifact(world, target, "DURABLE", digest, handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r])
    assert pfckpt.verify(a, "HEAD", D, repo_paths={TARGET: clone})["handoffs"][0]["handoff_proven"] is True


def test_durable_without_artifact_blocks_on_wrong_digest_form_or_extra_change(world, target):
    hsha, htree = publish_handoff(target)
    good = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    short = hashlib.sha256(target["patch"]).hexdigest()           # forma non canonica (indici abbreviati)
    assert short != good
    paths = {TARGET: target_clone(target, "t1")}
    for name, digest in (("short", short), ("zero", "0" * 64)):
        r = rec_no_artifact(world, target, "DURABLE", digest, handoff_branch=HBRANCH, handoff_sha=hsha,
                            handoff_tree=htree)
        err = expect(2, pfckpt.verify, chain_ckpt(world, target, f"c_{name}", [r]), "HEAD", D, repo_paths=paths)
        assert any("DURABLE non provato" in p and "digest canonico" in p for p in err.detail["problems"])
        assert not _only_reference_problems(err), "integrita', non riferimento"
    # commit di handoff con una modifica in piu' rispetto alla patch consegnata: il digest non coincide
    hsha2, htree2 = publish_handoff(target, extra="extra.py")
    r = rec_no_artifact(world, target, "DURABLE", good, handoff_branch=HBRANCH, handoff_sha=hsha2, handoff_tree=htree2)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c_extra", [r]), "HEAD", D,
                 repo_paths={TARGET: target_clone(target, "t2")})
    assert any("digest canonico" in p for p in err.detail["problems"])


def test_declared_but_corrupt_artifact_never_falls_back_to_the_digest(world, target):
    hsha, htree = publish_handoff(target)
    good = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree, patch_sha256=good)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D,
                 repo_paths={TARGET: target_clone(target)})
    assert any("patch non inclusa o non integra" in p for p in err.detail["problems"])


# ================================================================== PF-CKPT-CROSS-REPO-PROOF-02: casi negativi
def _durable_no_artifact(world, target, name, **over):
    hsha, htree = publish_handoff(target)
    digest = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    kw = dict(handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    kw.update(over)
    return chain_ckpt(world, target, name, [rec_no_artifact(world, target, "DURABLE", digest, **kw)], push=True)


def test_missing_handoff_commit_is_never_ready(world, target):
    a = _durable_no_artifact(world, target, "c", handoff_sha="1" * 40)
    paths = {TARGET: target_clone(target)}
    err = expect(2, pfckpt.verify, a, "HEAD", D, repo_paths=paths)
    assert any("handoff_sha irrisolto" in p for p in err.detail["problems"])
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "f"), D, CODE_BR, repo_paths=paths)
    assert err.reason == "NEEDS_RECONCILIATION"


def test_wrong_repository_is_never_ready(world, target):
    a = _durable_no_artifact(world, target, "c")
    other = str(target["tmp"] / "other")                          # repository estraneo, senza gli oggetti del target
    _git("init", "-q", other); _cfg(other)
    open(os.path.join(other, "x"), "w").write("x\n")
    sh(other, "add", "-A"); sh(other, "commit", "-q", "-m", "altro")
    err = expect(2, pfckpt.verify, a, "HEAD", D, repo_paths={TARGET: other})
    assert _only_reference_problems(err)
    err = expect(2, pfckpt.verify, a, "HEAD", D, repo_paths={"altro/repo": target_clone(target)})
    assert any("non disponibile" in p for p in err.detail["problems"])
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "f"), D, CODE_BR,
                 repo_paths={TARGET: other})
    assert err.reason == "NEEDS_RECONCILIATION"


def test_base_not_on_the_handoff_branch_is_never_ready(world, target):
    w = target["work"]                                            # un base pubblicato altrove, non sul branch di handoff
    sh(w, "checkout", "-q", "-B", "side", target["base"])
    open(os.path.join(w, "side.py"), "w").write("s\n")
    sh(w, "add", "-A"); sh(w, "commit", "-q", "-m", "side")
    sh(w, "push", "-q", "origin", "HEAD:refs/heads/side")
    side = sh(w, "rev-parse", "HEAD")
    a = _durable_no_artifact(world, target, "c", base_sha=side)
    err = expect(2, pfckpt.verify, a, "HEAD", D, repo_paths={TARGET: target_clone(target)})
    assert any("base_sha non raggiungibile" in p for p in err.detail["problems"])


def test_handoff_branch_deleted_without_alternative_ref_no_artifact(world, target):
    _durable_no_artifact(world, target, "c")
    sh(target["bare"], "update-ref", "-d", f"refs/heads/{HBRANCH}")
    sh(target["bare"], "reflog", "expire", "--expire=now", "--all")
    sh(target["bare"], "gc", "-q", "--prune=now")
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "f"), D, CODE_BR,
                 repo_paths={TARGET: target_clone(target)})
    assert err.reason == "NEEDS_RECONCILIATION"


def test_canonical_digest_ignores_context_and_order_config(world, target):
    """Codex #615: diff.context, diff.interHunkContext e diff.orderFile del clone non cambiano il digest canonico."""
    hsha, htree = publish_handoff(target)
    digest = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    clone = target_clone(target)
    order = os.path.join(str(target["tmp"]), "order.txt")
    open(order, "w").write("mcp_bridge.py\napp.py\n")
    for k, v in (("diff.context", "0"), ("diff.interHunkContext", "7"), ("diff.orderFile", order),
                 ("diff.suppressBlankEmpty", "true")):
        sh(clone, "config", k, v)
    r = rec_no_artifact(world, target, "DURABLE", digest, handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = chain_ckpt(world, target, "c", [r])
    assert pfckpt.verify(a, "HEAD", D, repo_paths={TARGET: clone})["handoffs"][0]["handoff_proven"] is True


def test_gitlink_change_hidden_by_ignore_submodules_config_is_still_in_the_canonical_digest(world, target):
    """Sol #615 (aaa3e8db): con diff.ignoreSubmodules=all nel clone, un gitlink aggiunto nel commit di handoff non
    deve sparire dal digest canonico: il digest della sola patch non prova un handoff con una modifica in piu'."""
    hsha, _ = publish_handoff(target)
    clean = hashlib.sha256(canonical_patch(target, hsha)).hexdigest()
    w = target["work"]
    sh(w, "checkout", "-q", "-B", "h2", hsha)
    _git("-C", w, "update-index", "--add", "--cacheinfo", f"160000,{target['base']},vendor/sub")
    sh(w, "commit", "-q", "-m", "gitlink in piu'")
    sh(w, "push", "-q", "-f", "origin", f"HEAD:refs/heads/{HBRANCH}")
    h2, t2 = sh(w, "rev-parse", "HEAD"), sh(w, "rev-parse", "HEAD^{tree}")
    clone = target_clone(target)
    sh(clone, "config", "diff.ignoreSubmodules", "all")
    r = rec_no_artifact(world, target, "DURABLE", clean, handoff_branch=HBRANCH, handoff_sha=h2, handoff_tree=t2)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert any("digest canonico" in p for p in err.detail["problems"])


def test_cross_repo_integration_commit_must_be_published_on_target_main(world, target):
    """Sol #615 (319b9b13): un commit di integrazione presente solo nel clone locale del target (mai pubblicato)
    non rende INTEGRATED_VERIFIED: un clone nuovo perderebbe la prova."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    _cfg(clone)                                                  # identita' git locale: il runner CI non ne ha una globale
    sh(clone, "checkout", "-q", "-B", "locale", hsha)
    sh(clone, "commit", "-q", "--allow-empty", "-m", "integrazione locale, mai pubblicata")
    local_integ = sh(clone, "rev-parse", "HEAD")
    r = rec(world, target, "INTEGRATED_VERIFIED", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree,
            integration_sha=local_integ, integration_tree=htree)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert _only_reference_problems(err) and any("main" in p for p in err.detail["problems"])


# ================================================================== quinto giro di Sol su #615 (c0ec9571)
def _local_only_commit(clone, start="origin/main", tag="mai pubblicato"):
    _cfg(clone)
    sh(clone, "checkout", "-q", "-B", "solo-locale", start)
    sh(clone, "commit", "-q", "--allow-empty", "-m", tag)          # messaggio distinto: commit mai identici fra cloni
    return sh(clone, "rev-parse", "HEAD")


def test_external_published_source_must_be_reachable_from_a_published_ref(world, target):
    """Sol #615 (c0ec9571): per un repository esterno `source_published: true` richiede che source_sha sia
    raggiungibile da un ref pubblicato, non solo presente nel clone locale."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    local = _local_only_commit(clone)
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree,
            source_repo=TARGET, source_sha=local)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert _only_reference_problems(err) and any("source_sha" in p for p in err.detail["problems"])


def test_external_base_of_a_verified_handoff_must_be_published(world, target):
    """Sol #615 (c0ec9571): forma INTEGRATED_VERIFIED senza commit di handoff: anche base_sha deve essere
    raggiungibile da un ref pubblicato del target."""
    clone = target_clone(target)
    base_local = _local_only_commit(clone)
    _git("-C", clone, "apply", "--index", "-", input=target["patch"])
    sh(clone, "commit", "-q", "-m", "integrazione")
    sh(clone, "push", "-q", "origin", "HEAD:refs/heads/main")       # integrazione pubblicata, base solo per ascendenza
    sh(clone, "fetch", "-q", "origin")
    isha = sh(clone, "rev-parse", "HEAD")
    r = rec(world, target, "INTEGRATED_VERIFIED", base_sha=base_local, integration_sha=isha)
    rep_ok = pfckpt.verify(chain_ckpt(world, target, "ok", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert rep_ok["handoffs"][0]["verified"] is True, "base antenata di main pubblicato: ammessa"
    other = target_clone(target, "t_other")
    lone = _local_only_commit(other, target["base"], "solo nel clone t_other")         # stesso tree della base: la prova del tree regge
    r2 = rec(world, target, "INTEGRATED_VERIFIED", base_sha=lone, integration_sha=isha)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "bad", [r2]), "HEAD", D, repo_paths={TARGET: other})
    assert _only_reference_problems(err) and any("base_sha" in p for p in err.detail["problems"])



# ================================================================== Sol #615 (8e40302e): remoto del target, ref freschi
def test_integration_only_on_a_fork_remote_is_not_published_on_target_main(world, target):
    """Un commit su `fork/main` (altro remote dello stesso clone) non e' sul main di target_repo."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    _cfg(clone)
    sh(clone, "checkout", "-q", "-B", "fk", target["base"])
    _git("-C", clone, "apply", "--index", "-", input=target["patch"])
    sh(clone, "commit", "-q", "-m", "integrazione solo nel fork")
    fsha = sh(clone, "rev-parse", "HEAD")
    fork = str(target["tmp"] / "fork.git")
    _git("init", "-q", "--bare", fork)
    sh(clone, "remote", "add", "fork", fork)
    sh(clone, "push", "-q", "fork", "HEAD:refs/heads/main")
    sh(clone, "fetch", "-q", "fork")
    assert sh(clone, "rev-parse", "HEAD^{tree}") == htree
    r = rec(world, target, "INTEGRATED_VERIFIED", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree,
            integration_sha=fsha, integration_tree=htree)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert _only_reference_problems(err)
    assert any("integration_sha non pubblicato su main" in p for p in err.detail["problems"])


def test_repo_path_whose_origin_is_a_fork_is_not_the_target(world, target):
    """Un clone il cui origin e' un fork (stessi oggetti, altro repository) non vale come target_repo."""
    a = _durable_no_artifact(world, target, "c")
    fork = str(target["tmp"] / "evil" / "Pickfair-nogui.git")
    _git("clone", "-q", "--bare", "--no-local", target["bare"], fork)
    fclone = str(target["tmp"] / "fork_clone")
    _git("clone", "-q", "--no-local", fork, fclone)
    assert pfckpt.verify(a, "HEAD", D, repo_paths={TARGET: target_clone(target)})["handoffs"][0]["handoff_proven"]
    err = expect(2, pfckpt.verify, a, "HEAD", D, repo_paths={TARGET: fclone})
    assert _only_reference_problems(err) and any("origin del percorso" in p for p in err.detail["problems"])


def test_save_never_proves_publication_on_a_stale_remote_tracking_ref(world, target):
    """Branch di handoff cancellato sul remoto dopo il clone: il remote-tracking locale e' obsoleto. `save` lo
    aggiorna prima di verificare e non pubblica nulla."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    assert pfckpt._published_on(clone, hsha, HBRANCH)
    sh(target["bare"], "update-ref", "-d", f"refs/heads/{HBRANCH}")      # l'oggetto resta, il branch no
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = agent(world, "c")
    os.makedirs(os.path.join(a, D, "handoff"), exist_ok=True)
    open(os.path.join(a, D, "handoff", "c.patch"), "wb").write(target["patch"])
    exp = next_state(a, world, repository=CHAIN, handoffs=[r])
    expect(2, pfckpt.save, a, BR, "origin", exp, "handoff su ref obsoleto", D, agent="claude-c",
           repo_paths={TARGET: clone})
    assert pfckpt.remote_head(a, "origin", BR) == exp, "nessun checkpoint pubblicato"
    assert not pfckpt._published_on(clone, hsha, HBRANCH), "remote-tracking potato"


# ================================================================== Sol #615 (a2814322): ref esatto, pull freschi, recover
def test_handoff_branch_with_revision_syntax_is_never_a_proof(world, target):
    """`handoff_branch` = `<branch>@{1}`: il reflog locale non e' un ref pubblicato."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    sh(target["work"], "push", "-q", "-f", "origin", f"{target['base']}:refs/heads/{HBRANCH}")   # handoff tolto
    sh(clone, "fetch", "-q", "origin")
    assert sh(clone, "rev-parse", f"refs/remotes/origin/{HBRANCH}@{{1}}") == hsha, "il reflog lo ricorda ancora"
    r = rec(world, target, "DURABLE", handoff_branch=f"{HBRANCH}@{{1}}", handoff_sha=hsha, handoff_tree=htree)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert any("nome di branch valido" in p for p in err.detail["problems"])
    assert not pfckpt._published_on(clone, hsha, f"{HBRANCH}@{{1}}")


def test_save_realigns_a_stale_pull_ref_before_using_it_as_proof(world, target):
    """`origin/pull/7` locale fermo a una testa vecchia (la PR e' stata force-pushata): `save` lo riallinea a
    refs/pull/7/head e la sorgente rimasta solo li' non vale piu' come pubblicata."""
    clone = target_clone(target)
    old = _local_only_commit(clone, tag="vecchia testa della PR 7")
    sh(clone, "update-ref", "refs/remotes/origin/pull/7", old)
    sh(target["bare"], "update-ref", "refs/pull/7/head", target["base"])                   # testa attuale della PR
    assert pfckpt._published_anywhere(clone, old)
    r = rec(world, target, "DELIVERED", source_repo=TARGET, source_sha=old)
    a = agent(world, "c")
    os.makedirs(os.path.join(a, D, "handoff"), exist_ok=True)
    open(os.path.join(a, D, "handoff", "c.patch"), "wb").write(target["patch"])
    exp = next_state(a, world, repository=CHAIN, handoffs=[r])
    expect(2, pfckpt.save, a, BR, "origin", exp, "sorgente su pull obsoleto", D, agent="claude-c",
           repo_paths={TARGET: clone})
    assert pfckpt.remote_head(a, "origin", BR) == exp, "nessun checkpoint pubblicato"
    assert sh(clone, "rev-parse", "refs/remotes/origin/pull/7") == target["base"], "pull riallineato"


def test_recover_skips_a_head_with_a_malformed_tests_executed(world):
    """HEAD corrotto con `tests_executed: 1`: niente TypeError, si recupera il checkpoint valido precedente."""
    a = agent(world, "a")
    exp = next_state(a, world)
    good = pfckpt.save(a, BR, "origin", exp, "buono", D, agent="claude")["commit"]
    next_state(a, world, tests_executed=1)
    pfckpt.write_manifest(a, D)
    raw_commit(a, "rotto")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    rep = pfckpt.recover(world["url"], BR, str(world["tmp"] / "rec"), D)
    assert rep["status"] == "RECOVERED" and rep["commit"] == good
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "boot"), D, CODE_BR)
    assert err.reason != "TypeError"


# ================================================================== Grok #615 (a2814322): stderr redatto, identita' prima del fetch
def test_refresh_error_never_carries_credentials_from_git_stderr(world, target, monkeypatch):
    clone = target_clone(target)
    real = pfckpt.git
    # valore composto a runtime e senza forma di token: la redazione dei segreti dei workflow di review non lo
    # altera nel diff (Sol/Grok vedevano il fixture gia' redatto e lo segnalavano come test rotto)
    secret = "valore" + "-riservato-" + "42"
    stderr = ("fatal: unable to access 'https://utente:" + secret + "@host/o/r/'\n").encode()

    def fake(repo, *args, **kw):
        if args and args[0] in ("fetch", "ls-remote"):
            return subprocess.CompletedProcess(args, 128, b"", stderr)
        return real(repo, *args, **kw)

    monkeypatch.setattr(pfckpt, "git", fake)
    err = expect(5, pfckpt.refresh_published_refs, clone, "origin")
    assert err.reason == "PUBLISHED_REFS_REFRESH_FAILED"
    assert secret not in repr(err.detail)
    assert "https://[REDACTED]@host/o/r/" in err.detail["stderr"]


def test_save_never_fetches_a_repo_path_that_is_not_the_declared_repository(world, target):
    """`--repo-path` il cui origin non e' target_repo: nessun fetch (il remoto estraneo non viene contattato), il
    checkpoint e' rifiutato come riferimento, non come errore di rete."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    sh(clone, "remote", "set-url", "origin", str(target["tmp"] / "inesistente" / "estraneo.git"))
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = agent(world, "c")
    os.makedirs(os.path.join(a, D, "handoff"), exist_ok=True)
    open(os.path.join(a, D, "handoff", "c.patch"), "wb").write(target["patch"])
    exp = next_state(a, world, repository=CHAIN, handoffs=[r])
    err = expect(2, pfckpt.save, a, BR, "origin", exp, "repo-path estraneo", D, agent="claude-c",
                 repo_paths={TARGET: clone})
    assert any("origin del percorso" in p for p in err.detail["problems"])
    assert pfckpt.remote_head(a, "origin", BR) == exp


def test_recover_never_accepts_a_handoff_proven_by_a_stale_repo_path(world, target):
    """Sol #615 (139fdff5): dopo la cancellazione del branch di handoff, `recover` con un `--repo-path` obsoleto non
    prende per valido il checkpoint che lo cita: aggiorna il clone e torna al checkpoint valido precedente."""
    hsha, htree = publish_handoff(target)
    clone = target_clone(target)
    r = rec(world, target, "DURABLE", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree)
    a = agent(world, "c")
    os.makedirs(os.path.join(a, D, "handoff"), exist_ok=True)
    open(os.path.join(a, D, "handoff", "c.patch"), "wb").write(target["patch"])
    exp = next_state(a, world, repository=CHAIN, handoffs=[r])
    res = pfckpt.save(a, BR, "origin", exp, "handoff DURABLE", D, agent="claude-c", repo_paths={TARGET: clone})
    sh(target["bare"], "update-ref", "-d", f"refs/heads/{HBRANCH}")
    assert pfckpt._published_on(clone, hsha, HBRANCH), "il tracking del clone e' ora obsoleto"
    rep = pfckpt.recover(world["url"], BR, str(world["tmp"] / "rec"), D, repo_paths={TARGET: clone})
    assert rep["commit"] != res["commit"], "il checkpoint con l'handoff non piu' pubblicato non e' recuperato"


def test_recover_skips_a_head_with_a_numeric_original_sha(world):
    """Sol #615 (2f176231): `original_sha` numero JSON di 40 cifre supera la regex ma non e' una stringa: il
    checkpoint e' invalido (mai AttributeError) e recover torna al checkpoint valido precedente."""
    a = agent(world, "a")
    exp = next_state(a, world)
    good = pfckpt.save(a, BR, "origin", exp, "buono", D, agent="claude")["commit"]
    n40 = int("1234567890" * 4)
    integ = {"original_sha": n40, "original_tree": "a" * 40, "integration_sha": world["code"],
             "integration_tree": "b" * 40, "equivalence": "TREE_EQUAL"}
    next_state(a, world, code={"branch": "main", "sha": str(n40), "integration": integ})
    pfckpt.write_manifest(a, D)
    raw_commit(a, "rotto: original_sha numerico")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert err.reason == "CHECKPOINT_INVALID"
    rep = pfckpt.recover(world["url"], BR, str(world["tmp"] / "rec"), D)
    assert rep["status"] == "RECOVERED" and rep["commit"] == good


def test_local_git_replace_never_fakes_an_integrated_verified_handoff(world, target):
    """Sol #615 (5b8b24ff): `refs/replace/<integration_sha>` locale che punta al commit di handoff nasconde le modifiche
    in piu' dell'integrazione su main. pfckpt ignora i replacement: la prova resta quella del tree pubblicato."""
    hsha, htree = publish_handoff(target)
    isha, itree = integrate(target, extra="extra_non_nella_patch.py")
    assert itree != htree
    clone = target_clone(target)
    sh(clone, "replace", isha, hsha)
    assert sh(clone, "rev-parse", f"{isha}^{{tree}}") == htree, "git, di default, vede il tree sostituito"
    r = rec(world, target, "INTEGRATED_VERIFIED", handoff_branch=HBRANCH, handoff_sha=hsha, handoff_tree=htree,
            integration_sha=isha)
    err = expect(2, pfckpt.verify, chain_ckpt(world, target, "c", [r]), "HEAD", D, repo_paths={TARGET: clone})
    assert any("tree(integration_sha) != handoff_tree" in p for p in err.detail["problems"])
    assert pfckpt.tree_of(clone, isha) == itree
