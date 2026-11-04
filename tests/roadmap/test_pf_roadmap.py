"""PF-GOV-PERSISTENT-ROADMAP-01: registro minimo. Stati derivati dai soli fatti GitHub (fixture offline)."""
import copy
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "roadmap"))
import pf_roadmap as R  # noqa: E402

REG = json.loads((ROOT / "docs" / "roadmap" / "registry.json").read_text(encoding="utf-8"))
MERGE = "a" * 40
HEAD = "b" * 40


def fake(prs=None, branches=None, compare="ahead", open_prs=()):
    prs, branches = prs or {}, branches or {}

    def fetch(path):
        if path.startswith(f"/repos/{REG['repo']}/pulls?"):
            return list(open_prs)
        if "/pulls/" in path:
            n = int(path.rsplit("/", 1)[1])
            return prs.get(n)
        if "/compare/" in path:
            return {"status": compare}
        if "/branches/" in path:
            name = path.split("/branches/", 1)[1].replace("%2F", "/")
            return {"commit": {"sha": branches[name]}} if name in branches else None
        if "/check-runs" in path:
            return {"check_runs": []}
        raise AssertionError(path)
    return fetch


def pr(n, merged=True, state="closed"):
    return {"number": n, "state": state, "merged_at": "2026-11-01T00:00:00Z" if merged else None,
            "merge_commit_sha": MERGE, "head": {"sha": HEAD, "ref": f"b{n}"}, "base": {"sha": "c" * 40},
            "title": "t", "mergeable_state": "clean"}


def card(snap, cid):
    return next(c for c in snap["cards"] if c["id"] == cid)


def test_registry_has_exactly_the_33_cards_with_sources():
    assert R.validate(REG) == []
    assert [c["id"] for c in REG["cards"]] == R.CARD_IDS


@pytest.mark.parametrize("mutate,frag", [
    (lambda r: r["cards"].pop(), "CARD_IDS"),
    (lambda r: r["cards"][1].update(id="F-01"), "CARD_IDS"),
    (lambda r: r["cards"][0].update(source="https://example.com"), "SOURCE_NOT_A_586_COMMENT"),
    (lambda r: r["cards"][0].update(depends_on=["F-99"]), "BAD_DEPENDENCY"),
    (lambda r: r["cards"][0].update(certifications=[{"level": "1", "sha": "x", "evidence": "y"}]),
     "CERTIFICATION_WITHOUT_VALID_EVIDENCE"),
])
def test_validate_flags_traceability_problems(mutate, frag):
    r = copy.deepcopy(REG); mutate(r)
    assert any(frag in p for p in R.validate(r))


def test_merged_is_never_certified_without_evidence():
    snap = R.derive(REG, fetch=fake(prs={n: pr(n) for n in (589, 593, 594, 596, 597, 599, 600, 601)}))
    f01 = card(snap, "F-01")
    assert (f01["integration"], f01["certification"], f01["overall"]) == ("MERGED", "CERTIFICATION_PENDING", "MERGED")
    assert snap["counts"].get("CERTIFIED", 0) == 0 and snap["total"] == 33 == sum(snap["counts"].values())


def test_merge_not_on_main_is_flagged():
    snap = R.derive(REG, fetch=fake(prs={n: pr(n) for n in (589, 593, 594, 596, 597, 599, 600, 601)}, compare="diverged"))
    assert card(snap, "F-01")["integration"] == "MERGED_NOT_ON_MAIN"


def test_partial_card_is_not_merged_and_blocked_cards_stay_blocked():
    snap = R.derive(REG, fetch=fake(prs={n: pr(n) for n in (589, 593, 594, 596, 597, 599, 600, 601)},
                                   branches={"keep/f13b-order-identity-cashout-20261103": HEAD}))
    f13 = card(snap, "F-13")
    assert f13["partial"] and f13["integration"] != "MERGED" and f13["overall"] == "BRANCH_READY"
    assert card(snap, "F-31")["overall"] == "BLOCKED"


def test_missing_link_is_a_traceability_problem():
    snap = R.derive(REG, fetch=fake(prs={}))
    assert any("F-01:MISSING_LINK:596" == p for p in snap["traceability_problems"])


def test_open_pr_without_card_trailer_is_reported():
    op = dict(pr(700, merged=False, state="open"), body="nessun trailer")
    snap = R.derive(REG, fetch=fake(open_prs=[op]))
    assert [p["pr"] for p in snap["unlinked_open_prs"]] == [700]
    op["body"] = "Roadmap-Card: F-14"
    assert R.derive(REG, fetch=fake(open_prs=[op]))["unlinked_open_prs"] == []


def test_transitions_are_idempotent_and_record_from_to():
    f = fake(prs={n: pr(n) for n in (589, 593, 594, 596, 597, 599, 600, 601)})
    a, b = R.derive(REG, fetch=f), R.derive(REG, fetch=f)
    assert R.transitions(a, b) == []
    t = R.transitions(None, a)
    x = next(t for t in t if t["card"] == "F-01" and t["dimension"] == "integration")
    assert (x["from"], x["to"], x["sha"]) == ("UNRECORDED", "MERGED", MERGE)


def test_github_error_is_never_a_state():
    def boom(path):
        raise R.GitHubError("down")
    with pytest.raises(R.GitHubError):
        R.derive(REG, fetch=boom)


def test_snapshot_round_trips_through_the_comment():
    snap = R.derive(REG, fetch=fake())
    assert R.snapshot_from_comment(R.comment_body(snap)) == json.loads(json.dumps(snap, sort_keys=True))
