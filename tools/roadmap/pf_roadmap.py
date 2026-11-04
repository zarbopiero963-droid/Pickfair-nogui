#!/usr/bin/env python3
"""PF-GOV-PERSISTENT-ROADMAP-01: registro persistente delle 33 schede della Roadmap V2 (F-01..F-33).

Due parti, separate di proposito:
  - docs/roadmap/registry.json: campi STATICI (nome, priorita', dipendenze, fonte, collegamenti a PR e branch,
    blocchi, riconciliazioni, certificazioni). Cambia solo con una PR.
  - questo strumento: deriva gli STATI dai fatti di GitHub (PR, merge SHA, branch, check run), mai da un commento
    di un agente o di un reviewer AI. Ricalcolo completo e deterministico => idempotente; le transizioni sono la
    differenza fra lo snapshot precedente e quello nuovo.

Tre dimensioni distinte per scheda (una PR mergiata non e' una scheda certificata):
  development   NOT_STARTED | BRANCH_READY | BLOCKED
  integration   NOT_STARTED | PR_OPEN | CI_PENDING | MERGE_READY | MERGED | BLOCKED
  certification NOT_STARTED | CERTIFICATION_PENDING | CERTIFIED | BLOCKED
e uno stato complessivo (per la dashboard, ogni scheda contata una sola volta).

Comandi:
  validate   [--registry P]                              controlli di completezza del registro (offline)
  derive     [--registry P] [--out snapshot.json]        stati da GitHub (GET pubbliche, token opzionale)
  render     --snapshot S                                dashboard Markdown
  transitions --old A --new B                            transizioni (stato precedente, nuovo, motivo, evidenza, SHA)
  publish    --snapshot S --issue N [--dry-run]          aggiorna il commento-registro (marker) e appende le
                                                          transizioni su un commento separato; richiede GITHUB_TOKEN
Esiti: exit 0 ok, 1 problemi di registro/tracciabilita', 2 GitHub non leggibile (mai uno stato inventato).
"""
import argparse
import datetime
import json
import os
import re
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"
MARKER = "<!-- pf-roadmap-registry:v1 -->"
TRANSITIONS_MARKER = "<!-- pf-roadmap-transitions:v1 -->"
CARD_IDS = [f"F-{n:02d}" for n in range(1, 34)]
CARD_TRAILER = re.compile(r"(?im)^Roadmap-Card:[ \t]*(F-\d{2}[a-z]?|none\b.*)$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SOURCE_RE = re.compile(r"^https://github\.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-\d+$")
EVIDENCE_RE = re.compile(r"^https://github\.com/zarbopiero963-droid/Pickfair-nogui/(issues|pull)/\d+#issuecomment-\d+$")
ORDER = ["NOT_STARTED", "BRANCH_READY", "PR_OPEN", "CI_PENDING", "MERGE_READY", "MERGED", "CERTIFICATION_PENDING",
         "CERTIFIED"]


class GitHubError(Exception):
    pass


def gh_get(path, fetch=None, token=None):
    """GET su API GitHub; 404 => None (oggetto assente). Ogni altro errore => GitHubError (mai uno stato inventato)."""
    if fetch:
        return fetch(path)
    hdr = {"Accept": "application/vnd.github+json", "User-Agent": "pf-roadmap"}
    if token:
        hdr["Authorization"] = f"Bearer {token}"
    try:
        with urllib.request.urlopen(urllib.request.Request(API + path, headers=hdr), timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise GitHubError(f"HTTP_{e.code}:{path}") from e
    except Exception as e:  # noqa: BLE001
        raise GitHubError(f"{type(e).__name__}:{path}") from e


# ---- validate -------------------------------------------------------------------------------------------------------
def validate(reg):
    """Problemi di TRACCIABILITA' del registro (non difetti del codice)."""
    p = []
    if reg.get("schema") != "pf-roadmap-registry/1":
        p.append("SCHEMA")
    cards = reg.get("cards")
    if not isinstance(cards, list):
        return p + ["CARDS_NOT_LIST"]
    ids = [c.get("id") for c in cards]
    if ids != CARD_IDS:
        missing = sorted(set(CARD_IDS) - set(ids))
        dup = sorted({i for i in ids if ids.count(i) > 1})
        extra = sorted(set(ids) - set(CARD_IDS), key=str)
        p.append(f"CARD_IDS(missing={missing},duplicated={dup},unknown={extra},order_ok={sorted(ids, key=str) == ids})")
    for c in cards:
        cid = c.get("id")
        for k in ("name", "priority", "source"):
            if not c.get(k):
                p.append(f"{cid}:MISSING_{k.upper()}")
        if c.get("source") and not SOURCE_RE.match(c["source"]):
            p.append(f"{cid}:SOURCE_NOT_A_586_COMMENT")
        for d in c.get("depends_on", []):
            if d not in CARD_IDS or d == cid:
                p.append(f"{cid}:BAD_DEPENDENCY:{d}")
        links = c.get("links") or {}
        parts = links.get("parts") or {}
        for part, pl in list(parts.items()) + [(cid, links)]:
            if not re.match(rf"^{re.escape(cid)}[a-z]?$", part):
                p.append(f"{cid}:BAD_PART_ID:{part}")
            for n in pl.get("prs", []):
                if not isinstance(n, int) or n <= 0:
                    p.append(f"{cid}:BAD_PR:{n}")
            for b in pl.get("branches", []):
                if not isinstance(b, str) or not b or b.startswith("-") or ".." in b:
                    p.append(f"{cid}:BAD_BRANCH:{b}")
        for cert in c.get("certifications", []):
            if not (isinstance(cert, dict) and SHA_RE.match(str(cert.get("sha", "")))
                    and EVIDENCE_RE.match(str(cert.get("evidence", ""))) and cert.get("level")):
                p.append(f"{cid}:CERTIFICATION_WITHOUT_VALID_EVIDENCE")
    return p


# ---- derive ---------------------------------------------------------------------------------------------------------
def _pr_facts(repo, n, fetch, token):
    pr = gh_get(f"/repos/{repo}/pulls/{n}", fetch, token)
    if pr is None:
        return {"pr": n, "missing": True}
    f = {"pr": n, "state": pr.get("state"), "merged": bool(pr.get("merged_at")), "merged_at": pr.get("merged_at"),
         "merge_sha": pr.get("merge_commit_sha") if pr.get("merged_at") else None, "head_sha": pr["head"]["sha"],
         "base_sha": pr["base"]["sha"], "branch": pr["head"]["ref"], "title": pr.get("title", ""),
         "mergeable_state": pr.get("mergeable_state")}
    if f["merged"]:   # MERGED solo se il merge SHA e' davvero nella storia di main
        cmp_ = gh_get(f"/repos/{repo}/compare/{f['merge_sha']}...main", fetch, token)
        f["merge_on_main"] = bool(cmp_) and cmp_.get("status") in ("ahead", "identical")
    elif f["state"] == "open":
        runs = []
        for page in range(1, 11):
            b = gh_get(f"/repos/{repo}/commits/{f['head_sha']}/check-runs?filter=latest&per_page=100&page={page}",
                       fetch, token) or {}
            rows = b.get("check_runs", [])
            runs += rows
            if len(rows) < 100:
                break
        f["checks_pending"] = sum(1 for r in runs if r.get("status") != "completed")
        f["checks_failing"] = sorted({r.get("name") for r in runs if r.get("status") == "completed"
                                      and r.get("conclusion") in ("failure", "timed_out", "action_required",
                                                                  "cancelled", "startup_failure")})
        pre = [r for r in runs if r.get("name") == "pf-betfair-certification"
               and (r.get("app") or {}).get("id") == 5579027]
        f["prelive"] = pre[0].get("conclusion") if len(pre) == 1 else ("MISSING" if not pre else "DUPLICATED")
    return f


def _branch_facts(repo, name, fetch, token):
    b = gh_get(f"/repos/{repo}/branches/{urllib.request.quote(name, safe='')}", fetch, token)
    return {"branch": name, "exists": b is not None, "sha": (b or {}).get("commit", {}).get("sha")}


def _integration(prs, branches):
    merged = [p for p in prs if p.get("merged")]
    if merged:
        return ("MERGED", merged[-1]) if all(p.get("merge_on_main") for p in merged) else ("MERGED_NOT_ON_MAIN",
                                                                                          merged[-1])
    open_ = [p for p in prs if p.get("state") == "open"]
    if open_:
        p = open_[-1]
        if p.get("checks_pending"):
            return "CI_PENDING", p
        if p.get("mergeable_state") == "clean" and not p.get("checks_failing"):
            return "MERGE_READY", p
        return "PR_OPEN", p
    return "NOT_STARTED", None


def derive_card(card, facts):
    """Stati della scheda dai soli fatti GitHub raccolti. Nessun PASS da commenti."""
    links = card.get("links") or {}
    units = list((links.get("parts") or {}).items()) or [(card["id"], links)]
    parts = []
    for uid, ul in units:
        prs = [facts["prs"][n] for n in ul.get("prs", [])]
        brs = [facts["branches"][b] for b in ul.get("branches", [])]
        integ, ref = _integration(prs, brs)
        has_branch = any(b.get("exists") for b in brs) or bool(prs and not all(p.get("missing") for p in prs))
        dev = "BRANCH_READY" if has_branch else "NOT_STARTED"
        parts.append({"unit": uid, "development": dev, "integration": integ,
                      "ref": {k: ref.get(k) for k in ("pr", "head_sha", "merge_sha", "branch", "prelive",
                                                       "checks_failing") if ref and ref.get(k) is not None},
                      "branches": [{"branch": b["branch"], "sha": b["sha"], "exists": b["exists"]} for b in brs],
                      "missing_links": [p["pr"] for p in prs if p.get("missing")]
                      + [b["branch"] for b in brs if not b.get("exists")]})
    dev = "BRANCH_READY" if any(p["development"] == "BRANCH_READY" for p in parts) else "NOT_STARTED"
    integ_all = [p["integration"] for p in parts]
    if all(i == "MERGED" for i in integ_all):
        integ = "MERGED"
    elif any(i == "MERGED_NOT_ON_MAIN" for i in integ_all):
        integ = "MERGED_NOT_ON_MAIN"
    else:   # parziale: lo stato della parte meno avanzata non mergiata
        rest = [i for i in integ_all if i != "MERGED"]
        integ = min(rest, key=lambda s: ORDER.index(s) if s in ORDER else -1)
    certs = card.get("certifications") or []
    if integ == "MERGED":
        cert = "CERTIFIED" if certs else "CERTIFICATION_PENDING"
    else:
        cert = "NOT_STARTED"
    blocked = card.get("blocked_by")
    if blocked and integ == "NOT_STARTED":
        dev, integ, cert = "BLOCKED", "BLOCKED", "BLOCKED"
    if cert == "CERTIFIED":
        overall = "CERTIFIED"
    elif blocked and integ == "BLOCKED":
        overall = "BLOCKED"
    elif integ == "MERGED":
        overall = "MERGED"
    elif integ in ("PR_OPEN", "CI_PENDING", "MERGE_READY", "MERGED_NOT_ON_MAIN"):
        overall = integ
    else:
        overall = dev
    partial = len(parts) > 1 and "MERGED" in integ_all and integ != "MERGED"
    return {"id": card["id"], "name": card["name"], "priority": card["priority"], "development": dev,
            "integration": integ, "certification": cert, "overall": overall, "partial": partial,
            "blocked_by": blocked, "needs_reconciliation": card.get("needs_reconciliation"), "parts": parts,
            "next_action": next_action(card, overall, parts)}


def next_action(card, overall, parts):
    if overall == "BLOCKED":
        return f"attende: {card.get('blocked_by')}"
    if overall == "NOT_STARTED":
        return "avvio sviluppo (verificare dipendenze: " + (", ".join(card.get("depends_on") or []) or "nessuna") + ")"
    if overall == "BRANCH_READY":
        return "riallineare il branch a main, test, poi PR quando la corsia e' libera"
    if overall in ("PR_OPEN", "CI_PENDING"):
        return "PR aperta: CI, review, PRE-LIVE e gate fino a merge readiness"
    if overall == "MERGE_READY":
        return "decisione di merge secondo policy (owner o delega)"
    if overall == "MERGED_NOT_ON_MAIN":
        return "VERIFICARE: merge SHA non trovato nella storia di main"
    if overall == "MERGED":
        return "certificazione: evidenza firmata (PRE-LIVE / livello V2) da registrare in 'certifications'"
    return "nessuna"


def collect(reg, fetch=None, token=None):
    repo = reg["repo"]
    prs, branches = {}, {}
    for c in reg["cards"]:
        links = c.get("links") or {}
        for ul in [links] + list((links.get("parts") or {}).values()):
            for n in ul.get("prs", []):
                prs.setdefault(n, None)
            for b in ul.get("branches", []):
                branches.setdefault(b, None)
    for n in prs:
        prs[n] = _pr_facts(repo, n, fetch, token)
    for b in branches:
        branches[b] = _branch_facts(repo, b, fetch, token)
    # PR aperte non collegate a nessuna scheda e senza trailer `Roadmap-Card:`: problema di tracciabilita'
    open_prs = gh_get(f"/repos/{repo}/pulls?state=open&per_page=100", fetch, token) or []
    linked = set(prs)
    unlinked, open_info = [], []
    for p in open_prs:
        m = CARD_TRAILER.search(p.get("body") or "")
        open_info.append({"pr": p["number"], "head_sha": p["head"]["sha"],
                          "prelive": _pr_facts(repo, p["number"], fetch, token).get("prelive", "UNKNOWN")})
        if p["number"] not in linked and not m:
            unlinked.append({"pr": p["number"], "title": p.get("title", ""), "head_sha": p["head"]["sha"]})
    return {"prs": prs, "branches": branches, "unlinked_open_prs": unlinked, "open_prs": open_info}


def derive(reg, fetch=None, token=None, now=None):
    facts = collect(reg, fetch, token)
    cards = [derive_card(c, facts) for c in reg["cards"]]
    counts = {}
    for c in cards:
        counts[c["overall"]] = counts.get(c["overall"], 0) + 1
    return {"schema": "pf-roadmap-snapshot/1", "generated_at": (now or datetime.datetime.now(datetime.timezone.utc)
                                                               ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total": len(cards), "counts": dict(sorted(counts.items())), "cards": cards,
            "unlinked_open_prs": facts["unlinked_open_prs"], "open_prs": facts["open_prs"],
            "traceability_problems": validate(reg) + [f"{c['id']}:MISSING_LINK:{m}" for c in cards
                                                      for p in c["parts"] for m in p["missing_links"]]}


# ---- transitions ----------------------------------------------------------------------------------------------------
def transitions(old, new, actor="pf-roadmap", now=None):
    """Differenza fra due snapshot: una voce per ogni dimensione cambiata. Stessi stati => nessuna voce (idempotente)."""
    prev = {c["id"]: c for c in (old or {}).get("cards", [])}
    ts = new.get("generated_at") or now
    out = []
    for c in new["cards"]:
        o = prev.get(c["id"], {})
        for dim in ("development", "integration", "certification", "overall"):
            if o.get(dim) != c[dim]:
                ref = next((p["ref"] for p in c["parts"] if p.get("ref")), {})
                out.append({"card": c["id"], "dimension": dim, "from": o.get(dim, "UNRECORDED"), "to": c[dim],
                            "reason": f"derivato da GitHub ({dim})", "evidence": ref,
                            "sha": ref.get("merge_sha") or ref.get("head_sha"), "actor": actor, "at": ts})
    return out


# ---- render ---------------------------------------------------------------------------------------------------------
def render(snap):
    c = snap["counts"]
    lines = [MARKER, "## Roadmap V2: registro delle 33 schede (generato, non modificare a mano)", "",
             f"Generato da `tools/roadmap/pf_roadmap.py` il {snap['generated_at']} dai soli fatti di GitHub (PR, merge "
             "SHA, branch, check). MERGED non significa CERTIFIED.", "",
             f"**Totale: {snap['total']}** · " + " · ".join(f"{k}: {v}" for k, v in c.items()), "",
             "**P0/P1 non ancora mergiate:** " + (", ".join(
                 x["id"] for x in snap["cards"] if re.match(r"^P[01]\b", x["priority"]) and x["integration"] != "MERGED")
                 or "nessuna") + " · **Certificate:** " + str(c.get("CERTIFIED", 0)) + " · **PRE-LIVE sulle PR "
             "aperte:** " + (", ".join(f"#{p['pr']} {p.get('prelive', 'UNKNOWN')}" for p in snap.get("open_prs", []))
                             or "nessuna PR aperta"), "",
             "| Scheda | Nome | Prio | Sviluppo | Integrazione | Certificazione | Riferimento | Prossima azione |",
             "|---|---|---|---|---|---|---|---|"]
    for x in snap["cards"]:
        refs = []
        for p in x["parts"]:
            r = p["ref"]
            if r.get("pr"):
                refs.append(f"{p['unit']} #{r['pr']} " + (f"merge `{r['merge_sha'][:8]}`" if r.get("merge_sha")
                                                          else f"head `{(r.get('head_sha') or '')[:8]}`"))
            for b in p["branches"]:
                if b["exists"]:
                    refs.append(f"{p['unit']} `{b['branch']}`@`{(b['sha'] or '')[:8]}`")
        flags = (" ⚠ parziale" if x["partial"] else "") + (" ⚠ da riconciliare" if x["needs_reconciliation"] else "")
        lines.append(f"| {x['id']}{flags} | {x['name']} | {x['priority']} | {x['development']} | {x['integration']} | "
                     f"{x['certification']} | {'<br>'.join(refs) or '—'} | {x['next_action']} |")
    if snap["unlinked_open_prs"]:
        lines += ["", "**PR aperte senza scheda** (aggiungere `Roadmap-Card: F-xx` o `Roadmap-Card: none <motivo>` nel "
                  "body):"] + [f"- #{p['pr']} {p['title']}" for p in snap["unlinked_open_prs"]]
    if snap["traceability_problems"]:
        lines += ["", "**Problemi di tracciabilità** (non difetti del codice):"] + [
            f"- `{p}`" for p in snap["traceability_problems"]]
    return "\n".join(lines) + "\n"


# ---- publish --------------------------------------------------------------------------------------------------------
def _request(method, path, token, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method, headers={
        "Accept": "application/vnd.github+json", "User-Agent": "pf-roadmap", "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def find_marker_comment(repo, issue, marker, fetch, token, author=None):
    hits = []
    for page in range(1, 51):
        rows = gh_get(f"/repos/{repo}/issues/{issue}/comments?per_page=100&page={page}", fetch, token) or []
        hits += [c for c in rows if (c.get("body") or "").startswith(marker)
                 and (author is None or (c.get("user") or {}).get("login") == author)]
        if len(rows) < 100:
            break
    return hits


def snapshot_from_comment(body):
    m = re.search(r"<!-- snapshot:([A-Za-z0-9+/=]+) -->", body or "")
    if not m:
        return None
    import base64
    import zlib
    return json.loads(zlib.decompress(base64.b64decode(m.group(1))))


def comment_body(snap):
    import base64
    import zlib
    blob = base64.b64encode(zlib.compress(json.dumps(snap, sort_keys=True, separators=(",", ":")).encode(), 9)).decode()
    return render(snap) + f"\n<!-- snapshot:{blob} -->\n"


def publish(snap, repo, issue, token, author=None, fetch=None, dry_run=False, request=None):
    """Un solo commento-registro (aggiornato) + commenti di transizione append-only. Ricalcolo identico => nessuna
    scrittura. Piu' commenti-registro => si aggiorna il piu' vecchio e lo si segnala (mai duplicare)."""
    request = request or (lambda m, p, b=None: _request(m, p, token, b))
    hits = find_marker_comment(repo, issue, MARKER, fetch, token, author)
    old = snapshot_from_comment(hits[0]["body"]) if hits else None
    tr = transitions(old, snap)
    actions = []
    if old is not None and not tr and old.get("unlinked_open_prs") == snap.get("unlinked_open_prs") \
            and old.get("traceability_problems") == snap.get("traceability_problems"):
        return {"changed": False, "actions": [], "duplicates": max(0, len(hits) - 1)}
    body = comment_body(snap)
    if dry_run:
        return {"changed": True, "transitions": tr, "actions": ["DRY_RUN"], "duplicates": max(0, len(hits) - 1)}
    if hits:
        request("PATCH", f"/repos/{repo}/issues/comments/{hits[0]['id']}", {"body": body})
        actions.append(f"updated:{hits[0]['id']}")
    else:
        r = request("POST", f"/repos/{repo}/issues/{issue}/comments", {"body": body})
        actions.append(f"created:{r.get('id')}")
    if tr:
        text = TRANSITIONS_MARKER + "\n### Transizioni registro Roadmap V2\n\n| Scheda | Dimensione | Da | A | SHA | Evidenza | Attore | Quando |\n|---|---|---|---|---|---|---|---|\n" + "\n".join(
            f"| {t['card']} | {t['dimension']} | {t['from']} | {t['to']} | `{(t['sha'] or '—')[:12]}` | "
            f"{('#' + str(t['evidence']['pr'])) if t['evidence'].get('pr') else '—'} | {t['actor']} | {t['at']} |"
            for t in tr) + "\n"
        r = request("POST", f"/repos/{repo}/issues/{issue}/comments", {"body": text})
        actions.append(f"transitions:{r.get('id')}:{len(tr)}")
    return {"changed": True, "transitions": tr, "actions": actions, "duplicates": max(0, len(hits) - 1)}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["validate", "derive", "render", "transitions", "publish"])
    ap.add_argument("--registry", default="docs/roadmap/registry.json")
    ap.add_argument("--snapshot")
    ap.add_argument("--out")
    ap.add_argument("--old")
    ap.add_argument("--new")
    ap.add_argument("--issue", type=int)
    ap.add_argument("--author")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if a.cmd == "validate":
        p = validate(json.load(open(a.registry, encoding="utf-8")))
        print(json.dumps({"problems": p}, ensure_ascii=False, indent=1))
        return 1 if p else 0
    if a.cmd == "derive":
        try:
            snap = derive(json.load(open(a.registry, encoding="utf-8")), token=token)
        except GitHubError as e:
            print(json.dumps({"error": "INFRA_ERROR", "detail": str(e)}))
            return 2
        out = json.dumps(snap, ensure_ascii=False, indent=1)
        if a.out:
            open(a.out, "w", encoding="utf-8").write(out)
        else:
            print(out)
        return 1 if snap["traceability_problems"] else 0
    if a.cmd == "render":
        sys.stdout.write(render(json.load(open(a.snapshot, encoding="utf-8"))))
        return 0
    if a.cmd == "transitions":
        print(json.dumps(transitions(json.load(open(a.old)), json.load(open(a.new))), ensure_ascii=False, indent=1))
        return 0
    if a.cmd == "publish":
        if not token and not a.dry_run:
            print("GITHUB_TOKEN assente: pubblicazione impossibile (PENDING_PUBLISH)", file=sys.stderr)
            return 2
        snap = json.load(open(a.snapshot, encoding="utf-8"))
        try:
            res = publish(snap, json.load(open(a.registry))["repo"], a.issue, token, a.author, dry_run=a.dry_run)
        except (GitHubError, urllib.error.URLError) as e:
            print(json.dumps({"error": "PENDING_PUBLISH", "detail": str(e)}))
            return 2
        print(json.dumps(res, ensure_ascii=False, indent=1, default=str))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
