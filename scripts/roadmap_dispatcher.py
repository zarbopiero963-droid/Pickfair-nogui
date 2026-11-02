#!/usr/bin/env python3
"""PF-DISPATCHER-01 — coda di lavoro della Roadmap V2 per Claude B (#586, mandato owner PF-CLAUDE-AUTO-DEV-01).

Sceglie la PROSSIMA scheda da sviluppare e ne registra l'avanzamento, senza mai aprire, modificare o mergiare una PR.
Integra gli strumenti esistenti, non li sostituisce:
- le schede e le label di stato le pubblica il registrar della VPS (Grok): BACKLOG, READY_FOR_CLAUDE, IN_PROGRESS,
  PR_OPEN, CERTIFICATION_RUNNING, BLOCKED, MERGED, VERIFIED, ``roadmap-v2``;
- la certificazione resta il check ``pf-betfair-certification`` di Pickfair Dedicated;
- il merge resta ai gate esistenti (``scripts/pr_autonomy_policy.py``) o all'owner. **CI verde non e' un merge.**

Fonti (sola lettura, ``GET``):
1. le issue con label ``roadmap-v2``: id dalla marcatura ``<!-- pf-card:id=<ID> ... -->`` nel corpo (o dal titolo
   ``[<ID>] ...``), stato dalla label di stato, priorita' dalla label ``P0..P3`` o dalla marcatura, dipendenze dalla
   marcatura ``deps=<ID>,<ID>``;
2. il registro su #586: il commento piu' recente con ``<!-- pf-roadmap-registry:v1 -->`` seguito da un blocco JSON
   ``{"cards": [{"id", "title", "priority", "status", "deps", "order"}]}``;
3. gli eventi di avanzamento su #586: ``<!-- pf-dispatch:v1 {json} -->`` (vedi ``Event``).
Le fonti 2 e 3 contano solo se scritte da un login in ``trusted_logins`` (gli agenti pubblicano come owner, #472).

Decisione (``plan``), fail-closed in quest'ordine:
1. una lavorazione attiva (ultimo evento non terminale) => RESUME di quella, mai una seconda assegnazione;
2. una PR ``[TASK:`` aperta, o una PR di governance in ``blocking_prs`` (es. #602), che non e' della lavorazione
   attiva => NONE ``ONE_PR_AT_A_TIME``;
3. una scheda BLOCKED su cui un'owner decision e' aperta ferma la coda => NONE ``QUEUE_BLOCKED``;
4. fra le schede READY_FOR_CLAUDE con tutte le dipendenze MERGED/VERIFIED: P0 > P1 > P2 > P3, poi ``order``, poi id.
   Nessuna => NONE ``NOTHING_READY``.
Lo stesso stato riletto da' la stessa decisione (idempotenza); dopo un riavvio lo stato si ricostruisce SOLO dagli
eventi GitHub, quindi una scheda gia' assegnata non viene assegnata di nuovo.

Scrittura (solo con ``--live`` E ``PF_DISPATCHER_LIVE=1``): un solo ``POST`` di commento evento su #586. Il client
rifiuta qualsiasi altra scrittura (PR, merge, label, contenuti) prima della richiesta. Default: dry-run.
Solo libreria standard.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

REPO = "zarbopiero963-droid/Pickfair-nogui"
ROADMAP_ISSUE = 586
ROADMAP_LABEL = "roadmap-v2"
STATUSES = ("BACKLOG", "READY_FOR_CLAUDE", "IN_PROGRESS", "PR_OPEN", "CERTIFICATION_RUNNING", "BLOCKED", "MERGED",
            "VERIFIED")
DONE = {"MERGED", "VERIFIED"}
PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
UNKNOWN_PRIORITY = 9
REGISTRY_MARKER = "<!-- pf-roadmap-registry:v1 -->"
_EVENT_RE = re.compile(r"<!--\s*pf-dispatch:v1\s+(\{.*?\})\s*-->", re.S)
_CARD_RE = re.compile(r"<!--\s*pf-card:([^>]*)-->")
_ID_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Za-z0-9]+)+$")
_TITLE_ID_RE = re.compile(r"^\[([A-Z][A-Z0-9]*(?:-[A-Za-z0-9]+)+)\]")

# stati della lavorazione (eventi pf-dispatch)
ACTIVE_STATES = ("ASSIGNED", "PR_OPEN", "CERTIFICATION_RUNNING", "MERGED", "OWNER_REQUIRED", "BLOCKED")
TERMINAL_STATES = ("VERIFIED", "RELEASED")
EVENT_STATES = ACTIVE_STATES + TERMINAL_STATES
# transizioni ammesse (idempotenti: ripetere lo stesso stato non e' un errore)
TRANSITIONS = {
    "ASSIGNED": {"PR_OPEN", "BLOCKED", "OWNER_REQUIRED", "RELEASED"},
    "PR_OPEN": {"CERTIFICATION_RUNNING", "BLOCKED", "OWNER_REQUIRED", "MERGED", "RELEASED"},
    "CERTIFICATION_RUNNING": {"PR_OPEN", "BLOCKED", "OWNER_REQUIRED", "MERGED"},
    "OWNER_REQUIRED": {"PR_OPEN", "CERTIFICATION_RUNNING", "BLOCKED", "MERGED", "RELEASED"},
    "BLOCKED": {"PR_OPEN", "CERTIFICATION_RUNNING", "OWNER_REQUIRED", "RELEASED"},
    "MERGED": {"VERIFIED", "BLOCKED"},
}


class DispatcherError(RuntimeError):
    """Evidenza assente, ambigua o contraddittoria: nessuna decisione (fail-closed)."""


@dataclass(frozen=True)
class Card:
    id: str
    title: str = ""
    priority: str = ""
    status: str = "BACKLOG"
    deps: Tuple[str, ...] = ()
    order: int = 10_000
    issue: Optional[int] = None

    def rank(self) -> Tuple[int, int, str]:
        return PRIORITY_RANK.get(self.priority, UNKNOWN_PRIORITY), self.order, self.id


@dataclass(frozen=True)
class Event:
    """Avanzamento di una lavorazione: ``{"assignment": "<card>#<n>", "card", "state", "pr", "sha", "reason"}``."""

    assignment: str
    card: str
    state: str
    pr: Optional[int] = None
    sha: str = ""
    reason: str = ""
    comment_id: int = 0


@dataclass
class Decision:
    action: str                    # SELECT | RESUME | NONE
    reason: str
    card: Optional[str] = None
    assignment: Optional[str] = None
    state: Optional[str] = None
    pr: Optional[int] = None
    detail: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v not in (None, {}, "")} | {"action": self.action}


# ── parsing ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def _valid_id(value: Any) -> bool:
    return isinstance(value, str) and bool(_ID_RE.fullmatch(value))


def _kv(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for token in text.split():
        if "=" in token:
            k, v = token.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _deps(raw: Any) -> Tuple[str, ...]:
    items = raw if isinstance(raw, list) else [d for d in str(raw or "").split(",")]
    deps = tuple(sorted({str(d).strip() for d in items if str(d).strip()}))
    for d in deps:
        if not _valid_id(d):
            raise DispatcherError(f"DEPENDENCY_MALFORMED:{d}")
    return deps


def card_from_issue(issue: Mapping[str, Any]) -> Optional[Card]:
    """Scheda da una issue ``roadmap-v2``; ``None`` se l'issue non e' una scheda (nessun id). Dati contraddittori
    (due label di stato, id malformato) => ``DispatcherError``."""
    body = str(issue.get("body") or "")
    labels = [str((lb or {}).get("name") if isinstance(lb, dict) else lb) for lb in issue.get("labels") or []]
    meta: Dict[str, str] = {}
    for m in _CARD_RE.finditer(body):
        meta.update(_kv(m.group(1)))
    card_id = meta.get("id")
    if not card_id:
        t = _TITLE_ID_RE.match(str(issue.get("title") or ""))
        card_id = t.group(1) if t else None
    if not card_id:
        return None
    if not _valid_id(card_id):
        raise DispatcherError(f"CARD_ID_MALFORMED:{card_id}")
    statuses = [lb for lb in labels if lb in STATUSES]
    if len(statuses) > 1:
        raise DispatcherError(f"CARD_STATUS_AMBIGUOUS:{card_id}:{','.join(sorted(statuses))}")
    if str(issue.get("state")) == "closed" and not statuses:
        statuses = ["VERIFIED"] if "VERIFIED" in labels else ["BACKLOG"]
    prios = [lb for lb in labels if lb in PRIORITY_RANK]
    priority = meta.get("priority") or (prios[0] if len(prios) == 1 else "")
    order = meta.get("order", "")
    return Card(id=card_id, title=str(issue.get("title") or ""), priority=priority,
                status=statuses[0] if statuses else "BACKLOG", deps=_deps(meta.get("deps")),
                order=int(order) if order.isdigit() else 10_000, issue=int(issue["number"]))


def cards_from_registry(body: str) -> List[Card]:
    """Schede dal commento registro su #586 (blocco JSON dopo ``REGISTRY_MARKER``)."""
    if REGISTRY_MARKER not in body:
        return []
    tail = body.split(REGISTRY_MARKER, 1)[1]
    m = re.search(r"```json\s*(\{.*?\})\s*```", tail, re.S)
    if not m:
        raise DispatcherError("REGISTRY_MALFORMED:no-json")
    try:
        data = json.loads(m.group(1))
    except ValueError as exc:
        raise DispatcherError(f"REGISTRY_MALFORMED:{exc}") from None
    out = []
    for raw in data.get("cards") or []:
        if not isinstance(raw, dict) or not _valid_id(raw.get("id")):
            raise DispatcherError(f"REGISTRY_CARD_MALFORMED:{raw!r}"[:120])
        status = raw.get("status", "BACKLOG")
        if status not in STATUSES:
            raise DispatcherError(f"REGISTRY_STATUS_UNKNOWN:{raw['id']}:{status}")
        order = raw.get("order", 10_000)
        out.append(Card(id=raw["id"], title=str(raw.get("title") or ""), priority=str(raw.get("priority") or ""),
                        status=status, deps=_deps(raw.get("deps")), order=int(order) if isinstance(order, int) else
                        10_000, issue=raw.get("issue") if isinstance(raw.get("issue"), int) else None))
    return out


def merge_cards(registry: Iterable[Card], issues: Iterable[Card]) -> Dict[str, Card]:
    """Unione delle due fonti. La issue (label del registrar) prevale sul registro per stato e priorita'; due issue
    per la stessa scheda sono un'ambiguita' (fail-closed)."""
    out: Dict[str, Card] = {c.id: c for c in registry}
    seen: Dict[str, int] = {}
    for c in issues:
        if c.id in seen:
            raise DispatcherError(f"CARD_DUPLICATED:{c.id}:#{seen[c.id]}:#{c.issue}")
        seen[c.id] = c.issue or 0
        base = out.get(c.id)
        out[c.id] = Card(id=c.id, title=c.title or (base.title if base else ""),
                         priority=c.priority or (base.priority if base else ""), status=c.status,
                         deps=c.deps or (base.deps if base else ()), order=c.order if c.order != 10_000 else
                         (base.order if base else 10_000), issue=c.issue)
    return out


def events_from_comments(comments: Iterable[Mapping[str, Any]], trusted_logins: Sequence[str]) -> List[Event]:
    """Eventi ``pf-dispatch`` in ordine di commento. Marcatura malformata da un login fidato => fail-closed; da un
    login non fidato => ignorata (chiunque puo' commentare)."""
    out: List[Event] = []
    for c in comments:
        login = str(((c.get("user") or {}).get("login")) or "")
        for m in _EVENT_RE.finditer(str(c.get("body") or "")):
            if login not in trusted_logins:
                continue
            try:
                raw = json.loads(m.group(1))
            except ValueError:
                raise DispatcherError(f"EVENT_MALFORMED:comment {c.get('id')}") from None
            if (not isinstance(raw, dict) or raw.get("state") not in EVENT_STATES or not _valid_id(raw.get("card"))
                    or not re.fullmatch(r"[A-Z][A-Za-z0-9-]*#[1-9][0-9]*", str(raw.get("assignment") or ""))
                    or not str(raw["assignment"]).startswith(raw["card"] + "#")):
                raise DispatcherError(f"EVENT_MALFORMED:comment {c.get('id')}")
            pr = raw.get("pr")
            out.append(Event(assignment=raw["assignment"], card=raw["card"], state=raw["state"],
                             pr=pr if isinstance(pr, int) and not isinstance(pr, bool) else None,
                             sha=str(raw.get("sha") or ""), reason=str(raw.get("reason") or ""),
                             comment_id=int(c.get("id") or 0)))
    return out


# ── stato delle lavorazioni ─────────────────────────────────────────────────────────────────────────────────────────

def assignments(events: Sequence[Event]) -> Dict[str, List[Event]]:
    """Eventi per lavorazione; transizione non ammessa => fail-closed (registro manomesso o agenti concorrenti)."""
    out: Dict[str, List[Event]] = {}
    for e in events:
        hist = out.setdefault(e.assignment, [])
        if hist:
            prev = hist[-1].state
            if e.state != prev and e.state not in TRANSITIONS.get(prev, set()):
                raise DispatcherError(f"TRANSITION_INVALID:{e.assignment}:{prev}->{e.state}")
            if e.card != hist[0].card:
                raise DispatcherError(f"ASSIGNMENT_CARD_MISMATCH:{e.assignment}")
        elif e.state != "ASSIGNED":
            raise DispatcherError(f"TRANSITION_INVALID:{e.assignment}:<none>->{e.state}")
        hist.append(e)
    return out


def active_assignment(events: Sequence[Event]) -> Optional[List[Event]]:
    """La lavorazione non terminale, se c'e'. Due lavorazioni attive insieme => fail-closed."""
    active = [h for h in assignments(events).values() if h[-1].state not in TERMINAL_STATES]
    if len(active) > 1:
        raise DispatcherError("TWO_ACTIVE_ASSIGNMENTS:" + ",".join(sorted(h[0].assignment for h in active)))
    return active[0] if active else None


def next_assignment_id(card: str, events: Sequence[Event]) -> str:
    n = 1 + len({e.assignment for e in events if e.card == card})
    return f"{card}#{n}"


def effective_status(card: Card, events: Sequence[Event]) -> str:
    """Stato della scheda tenendo conto degli eventi: una lavorazione MERGED/VERIFIED soddisfa le dipendenze anche
    prima che il registrar aggiorni la label."""
    done = {e.card for e in events if e.state in ("MERGED", "VERIFIED")}
    return "MERGED" if card.status not in DONE and card.id in done else card.status


# ── decisione ───────────────────────────────────────────────────────────────────────────────────────────────────────

def task_prs(open_prs: Iterable[Mapping[str, Any]], blocking_prs: Iterable[int] = ()) -> List[int]:
    """PR aperte che occupano la coda: titolo con ``[TASK:`` oppure numero in ``blocking_prs`` (es. #602)."""
    blocking = set(blocking_prs)
    out = []
    for pr in open_prs:
        if str(pr.get("state", "open")) != "open":
            continue
        if "[TASK:" in str(pr.get("title") or "") or int(pr.get("number") or 0) in blocking:
            out.append(int(pr["number"]))
    return sorted(out)


def plan(cards: Mapping[str, Card], events: Sequence[Event], open_prs: Sequence[Mapping[str, Any]],
         blocking_prs: Sequence[int] = ()) -> Decision:
    active = active_assignment(events)
    prs = task_prs(open_prs, blocking_prs)
    if active:
        last = active[-1]
        # una PR aperta che non e' quella registrata della lavorazione (anche una PR appena aperta e non ancora
        # registrata con PR_OPEN) ferma tutto: si registra prima il collegamento, poi si riprende
        foreign = [n for n in prs if n != last.pr]
        if foreign:
            return Decision("NONE", "ONE_PR_AT_A_TIME", card=last.card, assignment=last.assignment,
                            state=last.state, pr=last.pr, detail={"open_prs": foreign})
        return Decision("RESUME", f"ACTIVE_{last.state}", card=last.card, assignment=last.assignment,
                        state=last.state, pr=last.pr)
    if prs:
        return Decision("NONE", "ONE_PR_AT_A_TIME", detail={"open_prs": prs})
    blocked = sorted(c.id for c in cards.values() if c.status == "BLOCKED")
    status = {cid: effective_status(c, events) for cid, c in cards.items()}
    ready, waiting = [], {}
    for c in cards.values():
        if status[c.id] != "READY_FOR_CLAUDE":
            continue
        missing = [d for d in c.deps if status.get(d) not in DONE]
        if missing:
            waiting[c.id] = missing
            continue
        ready.append(c)
    if not ready:
        return Decision("NONE", "NOTHING_READY", detail={"waiting_on_deps": waiting, "blocked": blocked})
    chosen = sorted(ready, key=Card.rank)[0]
    return Decision("SELECT", "READY", card=chosen.id, assignment=next_assignment_id(chosen.id, events),
                    state="ASSIGNED", detail={"priority": chosen.priority, "issue": chosen.issue,
                                              "waiting_on_deps": waiting, "blocked": blocked})


# ── classificazione degli esiti (mai un errore convertito in PASS) ──────────────────────────────────────────────────

OUTCOMES = ("PASS", "APP_FAILURE", "INFRA_FAILURE", "MISSING_EVIDENCE", "REVIEW_BLOCKERS", "OWNER_APPROVAL",
            "LIVE_NOT_RUN", "PENDING")
_INFRA_HINTS = ("i/o timeout", "timed out", "connection reset", "502", "503", "504", "rate limit",
                "runner lost", "no space left", "dial tcp", "temporarily unavailable")


def classify_check(check: Mapping[str, Any]) -> str:
    """Esito di un check della PR. ``success`` e' PASS; tutto cio' che non e' concluso e' PENDING; il LIVE neutral e'
    LIVE_NOT_RUN (mai un PASS del LIVE); un fallimento con traccia infrastrutturale e' INFRA_FAILURE; gli altri
    fallimenti sono APP_FAILURE; ``action_required`` e' OWNER_APPROVAL; uno stato sconosciuto MISSING_EVIDENCE."""
    status, concl = check.get("status"), check.get("conclusion")
    title = " ".join(str(check.get(k) or "") for k in ("name", "title", "summary")).lower()
    if status != "completed":
        return "PENDING"
    if concl == "success":
        return "PASS"
    if concl == "neutral" and "live" in title:
        return "LIVE_NOT_RUN"
    if concl == "action_required":
        return "OWNER_APPROVAL"
    if concl in ("failure", "timed_out", "cancelled", "startup_failure"):
        return "INFRA_FAILURE" if concl in ("timed_out", "startup_failure") or any(h in title for h in _INFRA_HINTS) \
            else "APP_FAILURE"
    if concl == "skipped":
        return "PASS" if check.get("allow_skip") else "MISSING_EVIDENCE"
    return "MISSING_EVIDENCE"


def pr_gate(checks: Sequence[Mapping[str, Any]], review_blockers: int, owner_required: bool,
            required: Sequence[str] = ("pf-betfair-certification",)) -> str:
    """Esito complessivo per decidere il prossimo passo della lavorazione. Precedenza fail-closed: OWNER_APPROVAL,
    REVIEW_BLOCKERS, APP_FAILURE, INFRA_FAILURE, MISSING_EVIDENCE, PENDING; PASS solo con tutto PASS (LIVE_NOT_RUN
    ammesso solo per il check LIVE) e ogni check richiesto presente."""
    if owner_required:
        return "OWNER_APPROVAL"
    if review_blockers:
        return "REVIEW_BLOCKERS"
    names = {str(c.get("name")) for c in checks}
    outcomes = [classify_check(c) for c in checks]
    for o in ("OWNER_APPROVAL", "APP_FAILURE", "INFRA_FAILURE", "MISSING_EVIDENCE", "PENDING"):
        if o in outcomes:
            return o
    if not checks or any(r not in names for r in required):
        return "MISSING_EVIDENCE"
    return "PASS"


# ── lease locale del worker (un solo esecutore per lavorazione, anche dopo un riavvio) ─────────────────────────────

class LeaseHeld(RuntimeError):
    pass


class LocalLease:
    """Lease su file con ``fcntl.flock`` e scadenza. Il file contiene ``{assignment, holder, fencing, expires}``.
    Un holder diverso con lease non scaduto => ``LeaseHeld``; un lease scaduto si riprende con fencing+1 (lo scrittore
    vecchio, se torna, perde: ``check`` confronta il fencing)."""

    def __init__(self, path: str, clock: Callable[[], float] = time.time):
        self.path, self.clock = path, clock

    def _locked(self, fn):
        os.makedirs(os.path.dirname(self.path) or ".", mode=0o700, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.read(fd, 65536)
            try:
                cur = json.loads(raw.decode("utf-8")) if raw.strip() else {}
            except ValueError:
                raise DispatcherError("LEASE_CORRUPTED") from None
            new = fn(cur)
            if new is not None:
                os.lseek(fd, 0, os.SEEK_SET)
                os.ftruncate(fd, 0)
                os.write(fd, json.dumps(new, sort_keys=True).encode("utf-8"))
                os.fsync(fd)
            return new if new is not None else cur
        finally:
            os.close(fd)

    def acquire(self, assignment: str, holder: str, ttl: float) -> Dict[str, Any]:
        now = self.clock()

        def take(cur):
            if cur and cur.get("expires", 0) > now and (cur.get("holder") != holder
                                                        or cur.get("assignment") != assignment):
                raise LeaseHeld(f"{cur.get('holder')}:{cur.get('assignment')}")
            fencing = int(cur.get("fencing", 0)) + (0 if cur.get("holder") == holder and
                                                    cur.get("assignment") == assignment else 1)
            return {"assignment": assignment, "holder": holder, "fencing": fencing, "expires": now + ttl}
        return self._locked(take)

    def check(self, assignment: str, holder: str, fencing: int) -> bool:
        cur = self._locked(lambda c: None)
        return (cur.get("assignment") == assignment and cur.get("holder") == holder
                and int(cur.get("fencing", -1)) == fencing and cur.get("expires", 0) > self.clock())

    def release(self, assignment: str, holder: str) -> None:
        def drop(cur):
            if cur.get("assignment") == assignment and cur.get("holder") == holder:
                return dict(cur, expires=0)
            raise LeaseHeld(f"{cur.get('holder')}:{cur.get('assignment')}")
        self._locked(drop)


# ── GitHub (letture; una sola scrittura ammessa) ────────────────────────────────────────────────────────────────────

class WriteRefused(RuntimeError):
    pass


class GitHub:
    """Client minimo. ``GET`` solo sotto ``/repos/<repo>/``; l'unica scrittura e' ``POST`` di un commento su
    ``issues/<ROADMAP_ISSUE>/comments``. Tutto il resto (PR, merge, label, contenuti, altri repo) e' rifiutato prima
    della richiesta."""

    def __init__(self, repo: str = REPO, token: str = "", http: Optional[Callable[..., Tuple[int, bytes]]] = None,
                 roadmap_issue: int = ROADMAP_ISSUE):
        self.repo, self.token, self.issue = repo, token, roadmap_issue
        self.http = http or self._urllib

    def _urllib(self, method: str, url: str, body: Optional[bytes] = None) -> Tuple[int, bytes]:
        import urllib.request
        req = urllib.request.Request(url, data=body, method=method, headers={"Accept": "application/vnd.github+json"})
        if self.token:
            req.add_unredirected_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - solo api.github.com
            return resp.status, resp.read()

    def check_request(self, method: str, path: str) -> None:
        if not path.startswith(f"/repos/{self.repo}/") or ".." in path:
            raise WriteRefused(f"PATH_REFUSED:{path}")
        if method == "GET":
            return
        if method == "POST" and path == f"/repos/{self.repo}/issues/{self.issue}/comments":
            return
        raise WriteRefused(f"WRITE_REFUSED:{method} {path}")

    def request(self, method: str, path: str, payload: Optional[Mapping[str, Any]] = None) -> Any:
        self.check_request(method, path)
        status, body = self.http(method, f"https://api.github.com{path}",
                                 json.dumps(payload).encode("utf-8") if payload is not None else None)
        if status not in (200, 201):
            raise DispatcherError(f"GITHUB_HTTP_{status}:{path}")
        return json.loads(body.decode("utf-8"))

    def get_all(self, path: str, max_pages: int = 30) -> List[Any]:
        sep = "&" if "?" in path else "?"
        out: List[Any] = []
        for page in range(1, max_pages + 1):
            rows = self.request("GET", f"{path}{sep}per_page=100&page={page}")
            if not isinstance(rows, list):
                raise DispatcherError(f"GITHUB_NOT_A_LIST:{path}")
            out += rows
            if len(rows) < 100:
                return out
        raise DispatcherError(f"GITHUB_TOO_MANY_PAGES:{path}")

    def snapshot(self, trusted_logins: Sequence[str]) -> Tuple[Dict[str, Card], List[Event], List[Dict[str, Any]]]:
        comments = self.get_all(f"/repos/{self.repo}/issues/{self.issue}/comments")
        registry: List[Card] = []
        for c in comments:            # vale il registro piu' recente di un login fidato
            if str(((c.get("user") or {}).get("login")) or "") in trusted_logins and REGISTRY_MARKER in str(
                    c.get("body") or ""):
                registry = cards_from_registry(str(c["body"]))
        issues = [i for i in self.get_all(f"/repos/{self.repo}/issues?state=all&labels={ROADMAP_LABEL}")
                  if "pull_request" not in i]
        issue_cards = [c for c in (card_from_issue(i) for i in issues) if c is not None]
        events = events_from_comments(comments, trusted_logins)
        open_prs = self.get_all(f"/repos/{self.repo}/pulls?state=open")
        return merge_cards(registry, issue_cards), events, open_prs

    def post_event(self, event: Mapping[str, Any]) -> Any:
        body = (f"<!-- pf-dispatch:v1 {json.dumps(event, sort_keys=True, separators=(',', ':'))} -->\n"
                f"**PF-DISPATCHER-01** `{event['assignment']}` → **{event['state']}**"
                + (f" (PR #{event['pr']})" if event.get("pr") else "")
                + (f": {event['reason']}" if event.get("reason") else "")
                + "\n\n---\n_Generated by [Claude Code](https://claude.ai/code)_")
        return self.request("POST", f"/repos/{self.repo}/issues/{self.issue}/comments", {"body": body})


def event_for(decision: Decision) -> Dict[str, Any]:
    if decision.action != "SELECT":
        raise DispatcherError(f"NOTHING_TO_ASSIGN:{decision.reason}")
    return {"assignment": decision.assignment, "card": decision.card, "state": "ASSIGNED", "reason": "dispatcher"}


def fingerprint(cards: Mapping[str, Card], events: Sequence[Event], open_prs: Sequence[Mapping[str, Any]]) -> str:
    """Impronta dello stato osservato: due giri con la stessa impronta danno la stessa decisione."""
    data = {"cards": sorted([c.__dict__ for c in cards.values()], key=lambda c: c["id"]),
            "events": [e.__dict__ for e in events], "prs": sorted(int(p.get("number") or 0) for p in open_prs)}
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=list).encode()).hexdigest()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="PF-DISPATCHER-01 — coda Roadmap V2 (default: dry-run)")
    ap.add_argument("cmd", choices=["plan"])
    ap.add_argument("--repo", default=REPO)
    ap.add_argument("--issue", type=int, default=ROADMAP_ISSUE)
    ap.add_argument("--trusted-login", action="append", default=[])
    ap.add_argument("--blocking-pr", action="append", type=int, default=[])
    ap.add_argument("--live", action="store_true", help="pubblica l'evento ASSIGNED (richiede PF_DISPATCHER_LIVE=1)")
    args = ap.parse_args(argv)
    trusted = args.trusted_login or [args.repo.split("/", 1)[0]]
    gh = GitHub(args.repo, os.environ.get("GH_TOKEN", ""), roadmap_issue=args.issue)
    try:
        cards, events, prs = gh.snapshot(trusted)
        decision = plan(cards, events, prs, args.blocking_pr)
    except (DispatcherError, WriteRefused) as exc:
        print(json.dumps({"action": "NONE", "reason": f"FAIL_CLOSED:{exc}"}))
        return 2
    out = decision.to_dict() | {"fingerprint": fingerprint(cards, events, prs), "dry_run": not args.live}
    if args.live and decision.action == "SELECT":
        if os.environ.get("PF_DISPATCHER_LIVE") != "1":
            print(json.dumps(out | {"reason": "FAIL_CLOSED:LIVE_NOT_ENABLED"}))
            return 2
        gh.post_event(event_for(decision))
        out["published"] = True
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
