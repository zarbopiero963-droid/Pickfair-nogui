"""Runner della campagna Betfair READ-ONLY (PF-BETFAIR-E2E-01, livello B).

Esercita, attraverso il `BetfairClient` di produzione con la ``ReadOnlyTransport`` come sessione, le letture su cui
Pickfair basa le decisioni: fondi, eventi, catalogo mercati, book con prezzi (stato mercato/selezioni, ladder BACK/LAY),
ordini correnti e cleared. Ogni passo registra esito, forma della risposta e i campi da cui dipende il codice.

Sicurezza: nessuna operazione mutante puo' partire (guardia sul trasporto); il report non contiene token, app key,
username, importi o id del conto (solo presenza/validita' dei campi e hash SHA-256 troncato degli id di mercato), cosi'
e' pubblicabile come evidenza. ``mode`` distingue REAL_READ_ONLY (sessione reale) da OFFLINE_FIXTURE (sessione finta).

Uso (solo sul canale autorizzato; credenziali fornite dall'operatore in PF_BF_USERNAME, PF_BF_APP_KEY,
PF_BF_CERT_PEM, PF_BF_KEY_PEM, PF_BF_PASSWORD; assenti => BLOCKED_ACCESS, exit 2)::

    python -c "from tools.bf_readonly.campaign import main; raise SystemExit(main(['--out', 'report.json']))"
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from typing import Any, Callable, Dict, List, Optional

from tools.bf_readonly.guard import ReadOnlyTransport

SOCCER = "1"


def _h(value: Any) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]


def _finite(value: Any) -> bool:
    try:
        return not isinstance(value, bool) and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _step(name: str, fn: Callable[[], Any], summarize: Callable[[Any], Dict[str, Any]]) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        out = fn()
        rec = {"op": name, "ok": True, **summarize(out)}
    except Exception as exc:                                   # noqa: BLE001 - l'esito va nel report
        rec = {"op": name, "ok": False, "error": type(exc).__name__,
               "error_code": str(exc).split(":", 1)[0][:60]}
    rec["elapsed_ms"] = round((time.monotonic() - started) * 1000.0, 1)
    return rec


def _funds_summary(out: Any) -> Dict[str, Any]:
    d = out if isinstance(out, dict) else {}
    return {"fields": sorted(k for k in d if k in ("available", "exposure", "retained_commission",
                                                   "exposure_limit", "discount_rate", "points_balance",
                                                   "wallet", "error")),
            "available_finite": _finite(d.get("available")), "exposure_finite": _finite(d.get("exposure")),
            "error_present": bool(d.get("error"))}


def _book_summary(book: Any, market_id: str) -> Dict[str, Any]:
    if not isinstance(book, dict):
        return {"market": _h(market_id), "book_present": False}
    runners = book.get("runners") if isinstance(book.get("runners"), list) else []
    ladders_ok = all(isinstance((r.get("ex") or {}).get("availableToBack"), list)
                     and isinstance((r.get("ex") or {}).get("availableToLay"), list)
                     for r in runners if isinstance(r, dict))
    return {"market": _h(market_id), "book_present": True, "market_id_matches": str(book.get("marketId")) == market_id,
            "status": str(book.get("status") or ""), "inplay": bool(book.get("inplay")),
            "runner_count": len(runners),
            "runner_statuses": sorted({str(r.get("status") or "") for r in runners if isinstance(r, dict)}),
            "ladders_present": bool(runners) and ladders_ok,
            "fields": sorted(k for k in book if isinstance(k, str))[:30]}


STATEMENT_PAGE = 100            # recordCount massimo per pagina di getAccountStatement
STATEMENT_MAX_PAGES = 200       # guardia contro paginazioni infinite (fail-closed: troncato => errore)


def export_account_statement(client: Any, *, from_iso: str, to_iso: str) -> Dict[str, Any]:
    """Esporta l'estratto conto (``getAccountStatement``, sola lettura) nell'intervallo, tutte le pagine.

    Il risultato contiene IMPORTI: e' l'evidenza privata da passare a ``tools/equity_peak_reconstruction`` (F-11), mai
    al report pubblicabile. Paginazione oltre il limite o risposta malformata => eccezione (mai un estratto parziale
    presentato come completo)."""
    items: List[Dict[str, Any]] = []
    for page in range(STATEMENT_MAX_PAGES):
        result = client._post_jsonrpc(client.ACCOUNT_URL, "AccountAPING/v1.0/getAccountStatement", {
            "fromRecord": page * STATEMENT_PAGE, "recordCount": STATEMENT_PAGE,
            "itemDateRange": {"from": from_iso, "to": to_iso}, "includeItem": "ALL",
        })
        if not isinstance(result, dict) or not isinstance(result.get("accountStatement"), list):
            raise RuntimeError("STATEMENT_MALFORMED")
        items.extend(result["accountStatement"])
        if not result.get("moreAvailable"):
            return {"accountStatement": items, "itemDateRange": {"from": from_iso, "to": to_iso}}
    raise RuntimeError("STATEMENT_TRUNCATED: pagination cap exceeded")


def run_campaign(client: Any, transport: ReadOnlyTransport, *, mode: str, max_markets: int = 3,
                 commit_sha: str = "") -> Dict[str, Any]:
    """Esegue le letture; mai un'eccezione. Il report e' FAIL se la guardia ha registrato una violazione."""
    steps: List[Dict[str, Any]] = []
    steps.append(_step("getAccountFunds", client.get_account_funds, _funds_summary))
    events_rec = _step("listEvents", lambda: client.list_events([SOCCER]),
                       lambda out: {"count": len(out) if isinstance(out, list) else None})
    steps.append(events_rec)
    market_ids: List[str] = []

    def _catalogue() -> Any:
        return client.list_market_catalogue([SOCCER], max_results=max(1, int(max_markets)))

    def _cat_summary(out: Any) -> Dict[str, Any]:
        rows = out if isinstance(out, list) else []
        for row in rows[:max_markets]:
            mid = str((row or {}).get("marketId") or "")
            if mid:
                market_ids.append(mid)
        return {"count": len(rows), "with_runners": sum(1 for r in rows if (r or {}).get("runners"))}

    steps.append(_step("listMarketCatalogue", _catalogue, _cat_summary))
    for mid in market_ids:
        steps.append(_step("listMarketBook", lambda m=mid: client.get_market_book(m, include_prices=True),
                           lambda out, m=mid: _book_summary(out, m)))
    steps.append(_step("listCurrentOrders", client.get_current_orders,
                       lambda out: {"count": len(out) if isinstance(out, list) else None}))
    steps.append(_step("listClearedOrders", lambda: client.list_cleared_orders(bet_status="SETTLED"),
                       lambda out: {"count": len(out) if isinstance(out, list) else None}))
    violations = list(transport.violations)
    return {"campaign": "PF-BETFAIR-E2E-01/B", "mode": mode, "commit_sha": commit_sha,
            "verdict": "FAIL" if violations or not all(s["ok"] for s in steps) else "PASS",
            "read_only_violations": violations, "methods_sent": list(transport.methods_sent),
            "steps": steps}


def main(argv: Optional[List[str]] = None) -> int:      # pragma: no cover - solo sul canale autorizzato
    import argparse
    import subprocess

    import betfair_client

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-markets", type=int, default=3)
    args = ap.parse_args(argv)
    import os

    import requests
    # Credenziali fornite dall'operatore del canale autorizzato (mai lette da altre fonti, mai scritte nel report).
    env = {k: os.environ.get(k, "") for k in ("PF_BF_USERNAME", "PF_BF_APP_KEY", "PF_BF_CERT_PEM", "PF_BF_KEY_PEM",
                                              "PF_BF_PASSWORD")}
    if not all(env.values()):
        print("BLOCKED_ACCESS: credenziali del canale read-only non configurate")
        return 2
    transport = ReadOnlyTransport(requests.Session())
    client = betfair_client.BetfairClient(username=env["PF_BF_USERNAME"], app_key=env["PF_BF_APP_KEY"],
                                          cert_pem=env["PF_BF_CERT_PEM"], key_pem=env["PF_BF_KEY_PEM"],
                                          session=transport)
    login = client.login(password=env["PF_BF_PASSWORD"])
    if not (isinstance(login, dict) and login.get("ok")):
        print("BLOCKED_ACCESS: login read-only non riuscito")
        return 2
    sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    report = run_campaign(client, transport, mode="REAL_READ_ONLY", max_markets=args.max_markets, commit_sha=sha)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
    return 0 if report["verdict"] == "PASS" else 1
