"""PR #512 ciclo 8 — rilievo Codex su 034af59 (pfckpt.verify): ogni `tests_executed[].sha` con status PASS/FAIL deve
risolversi a un commit reale (stessa lookup di `code.sha`, `git cat-file -e <sha>^{commit}`), altrimenti il
checkpoint e' invalido ("test sha unresolved"). Git reale, nessun mock."""
import json
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "tools", "agent_checkpoint"))
import pfckpt  # noqa: E402
from test_pfckpt import D, agent, expect, next_state, world  # noqa: E402,F401


def _checkpoint_with_tests(a, world, tests):
    exp = next_state(a, world, tests_executed=tests)
    pfckpt.write_manifest(a, D)
    return exp


@pytest.mark.parametrize("sha", ["deadbeefcafe", "0" * 40, "abcdef1"])
def test_unresolvable_test_sha_invalidates_the_checkpoint(world, sha):
    a = agent(world, "a")
    _checkpoint_with_tests(a, world, [{"id": "t1", "status": "PASS", "sha": sha}])
    from test_pfckpt import raw_commit
    raw_commit(a, "ckpt con sha di test inventato")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("t1" in p and "test sha unresolved" in p for p in err.detail["problems"]), err.detail["problems"]


def test_declared_unpublished_tree_evidence_is_accepted_only_with_an_explicit_note(world):
    """Ciclo 9 (#512): un'evidenza su un albero scratch mai pubblicato e' ammessa SOLO se dichiarata
    (`unpublished_tree: true` + `sha_note`); senza dichiarazione lo SHA non risolvibile resta un errore."""
    from test_pfckpt import raw_commit
    a = agent(world, "a")
    _checkpoint_with_tests(a, world, [{"id": "scratch", "status": "FAIL", "sha": "f63cf3a5ece48558fef9682ff126a6c437976186",
                                       "unpublished_tree": True,
                                       "sha_note": "albero scratch locale mai pubblicato, dichiarato"}])
    raw_commit(a, "ckpt con albero scratch dichiarato")
    assert pfckpt.verify(a, "HEAD", D)["seq"] == 2
    b = agent(world, "b")
    _checkpoint_with_tests(b, world, [{"id": "scratch", "status": "FAIL", "sha": "f63cf3a5ece48558fef9682ff126a6c437976186",
                                       "unpublished_tree": True}])          # flag senza nota: non basta
    raw_commit(b, "ckpt con albero scratch non motivato")
    err = expect(2, pfckpt.verify, b, "HEAD", D)
    assert any("scratch" in p and "test sha unresolved" in p for p in err.detail["problems"])


@pytest.mark.parametrize("bad_sha", ["", "non-uno-sha", "F63CF3A", "f63cf3", "zz63cf3a5ece"])
def test_c9_review_declared_unpublished_tree_still_needs_a_well_formed_sha(world, bad_sha):
    """Riesame indipendente c9: la dichiarazione esonera dalla RISOLUZIONE dello SHA, mai dalla sua forma."""
    from test_pfckpt import raw_commit
    a = agent(world, "a")
    _checkpoint_with_tests(a, world, [{"id": "scratch", "status": "FAIL", "sha": bad_sha, "unpublished_tree": True,
                                       "sha_note": "albero scratch locale mai pubblicato, dichiarato"}])
    raw_commit(a, "ckpt con sha malformato")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("scratch" in p and "test sha unresolved" in p for p in err.detail["problems"])


@pytest.mark.parametrize("moving_ref", ["HEAD", "main", "HEAD~1"])
def test_c9_review2_a_moving_ref_is_never_accepted_as_test_sha(world, moving_ref):
    """Secondo riesame c9: anche una voce NON dichiarata deve portare uno SHA, mai un riferimento mobile
    che si risolve oggi a un commit e domani a un altro."""
    from test_pfckpt import raw_commit
    a = agent(world, "a")
    _checkpoint_with_tests(a, world, [{"id": "mobile", "status": "PASS", "sha": moving_ref}])
    raw_commit(a, "ckpt con riferimento mobile")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("mobile" in p and "test sha unresolved" in p for p in err.detail["problems"])


def test_real_test_sha_resolves_via_the_code_repo_lookup(world):
    a = agent(world, "a")
    _checkpoint_with_tests(a, world, [{"id": "t1", "status": "PASS", "sha": world["code"]},
                                      {"id": "t2", "status": "FAIL", "sha": world["code"][:12]},
                                      {"id": "t3", "status": "BLOCKED"}])
    from test_pfckpt import raw_commit
    raw_commit(a, "ckpt con sha reali")
    rep = pfckpt.verify(a, "HEAD", D)
    assert rep["seq"] == 2



@pytest.mark.parametrize("payload", ["[]", "\"testo\"", "42", "null"])
def test_c10_non_object_state_is_a_structured_invalid_checkpoint(world, payload):
    """Codex r4308704074 (P2): STATE.json JSON valido ma non oggetto => CheckpointError(2), mai un
    AttributeError; `last_valid_checkpoint` salta il commit corrotto e trova il precedente."""
    from test_pfckpt import raw_commit
    a = agent(world, "a")
    good = pfckpt.out(a, "rev-parse", "HEAD")
    with open(os.path.join(a, D, "STATE.json"), "w") as fh:
        fh.write(payload)
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt con STATE non oggetto")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert err.reason == "STATE_JSON_INVALID", err.reason
    assert pfckpt.last_valid_checkpoint(a, D) == good
