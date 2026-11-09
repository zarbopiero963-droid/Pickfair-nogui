"""Oracolo di completezza finanziaria della scatola nera (PF-BLACKBOX-01, parte di Claude D).

Confronta il registro (letto SOLO tramite il contratto di lettura §4-bis) con la verita' dell'exchange (ordini e
settlement) e ritorna gli anelli mancanti. Ogni ordine della campagna deve avere la catena:

  1. ``ORDER_PLACE_REQUEST`` (ATTEMPT, critico: durevole PRIMA dell'invio)
  2. esito exchange: ``ORDER_PLACE_RESPONSE`` (RESULT; VERIFIED / REJECTED / FAILED / UNKNOWN)
  3. abbinamento: ``ORDER_MATCHED`` (OBSERVATION; ``values_after.size_matched`` = sizeMatched dell'exchange)
  4. hedge/cashout: l'hedge risale per causation a ``CASHOUT_REQUEST``; la richiesta ha un esito
  5. riconciliazione: ogni esito UNKNOWN o tentativo senza esito ha ``RECONCILIATION_RESOLVED`` successivo
  6. P&L: ``SETTLEMENT`` per mercato regolato, ``values_after.net_pnl`` = netto dell'exchange

Piu' i controlli strutturali: ``seq`` contiguo e unico, catena di hash, causation verso eventi precedenti, protezioni
SL/TP/trailing con un esito di chiusura, nessun segreto nei record, ordini usciti in modalita' degradata coperti da un
``BLACKBOX_GAP`` dichiarato. Nessun anello viene dedotto o inventato: mancante = problema.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, Iterable, List, Optional

from tests.blackbox_completeness.fake_blackbox import GENESIS, chain_hash

# Vocabolario: tipi gia' in core.event_journal.EVENT_CATEGORIES (ORDER/RISK/BETFAIR) piu' i NUOVI proposti a Claude B.
REQUEST = "ORDER_PLACE_REQUEST"
RESPONSE = "ORDER_PLACE_RESPONSE"
MATCHED = "ORDER_MATCHED"
CANCEL_REQUEST, CANCEL_RESPONSE = "ORDER_CANCEL_REQUEST", "ORDER_CANCEL_RESPONSE"
CASHOUT_REQUEST, CASHOUT_EXECUTED = "CASHOUT_REQUEST", "CASHOUT_EXECUTED"
SETTLEMENT = "SETTLEMENT"
NEW_TYPES = {                                              # assenti oggi da EVENT_CATEGORIES
    "RECONCILIATION_RESOLVED": "ORDER",                   # esito di riconciliazione PER ordine (oggi solo RUN/DIFF)
    "SL_TRIGGERED": "RISK", "TP_TRIGGERED": "RISK", "TRAILING_TRIGGERED": "RISK",
    "SL_TP_CLOSE_ROUTED": "ORDER", "SL_TP_CLOSE_NOT_EXECUTED": "ORDER", "SL_TP_CLOSE_UNCERTAIN": "ORDER",
    "SL_TP_CLOSE_REJECTED": "ORDER", "SL_TP_CLOSE_SKIPPED": "ORDER",
    "PROTECTION_WAITING": "RISK", "PROTECTION_RESUMED": "RISK",        # meta.sl_tp_wait_reason (D-C4-02)
    "PROTECTION_LATCH_RELEASED": "RISK",                                # D-C4-01
    "FUNDS_RESERVED": "RISK", "FUNDS_RELEASED": "RISK",
    "BLACKBOX_DEGRADED": "SYSTEM", "BLACKBOX_RECOVERED": "SYSTEM", "BLACKBOX_GAP": "SYSTEM",
    "BLACKBOX_UNAVAILABLE": "SYSTEM", "UNCLEAN_TERMINATION": "SESSION",
}
RESULT_TYPES = {RESPONSE, "RECONCILIATION_RESOLVED"}
TRIGGERS = {"SL_TRIGGERED", "TP_TRIGGERED", "TRAILING_TRIGGERED"}
CLOSE_OUTCOMES = {"SL_TP_CLOSE_ROUTED", "SL_TP_CLOSE_NOT_EXECUTED", "SL_TP_CLOSE_UNCERTAIN",
                  "SL_TP_CLOSE_REJECTED", "SL_TP_CLOSE_SKIPPED"}
CENT = Decimal("0.01")


def _q(x: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)
    except Exception:  # noqa: BLE001
        return None


def _bet(o: Dict[str, Any]) -> str:
    return str(o.get("betId") or o.get("bet_id") or "")


def _ref(o: Dict[str, Any]) -> str:
    return str(o.get("customerOrderRef") or o.get("order_ref") or o.get("ref") or "")


def _ts(value: Any) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def check(records: List[Dict[str, Any]], exchange_orders: Iterable[Dict[str, Any]] = (),
          settlements: Iterable[Dict[str, Any]] = (), secrets: Iterable[str] = (),
          hash_fn=None) -> List[str]:
    """``exchange_orders`` = STORICO completo degli ordini sull'exchange (anche annullati/decaduti, es. lo stub del
    microstake); ``settlements`` = netti per mercato dell'exchange; ``secrets`` = valori che non devono comparire;
    ``hash_fn(prev_hash, record)`` = verifica della catena (default: quella della scatola finta)."""
    hash_fn = hash_fn or chain_hash
    problems: List[str] = []
    recs = sorted(records, key=lambda r: r.get("seq") or 0)
    by_id = {r["event_id"]: r for r in recs if r.get("event_id")}

    # --- struttura: seq, catena, causation
    seqs = [r.get("seq") for r in recs]
    if len(set(seqs)) != len(seqs):
        problems.append("SEQ_DUPLICATE")
    if recs and seqs != list(range(seqs[0], seqs[0] + len(seqs))):
        problems.append("SEQ_GAP")
    prev = GENESIS if recs and recs[0].get("seq") == 1 else (recs[0].get("prev_hash") if recs else GENESIS)
    for r in recs:
        if r.get("prev_hash", prev) != prev or hash_fn(prev, r) != r.get("hash"):
            problems.append(f"CHAIN_BROKEN:{r.get('seq')}")
            break
        prev = r["hash"]
    for r in recs:
        cid = r.get("causation_id")
        if cid and (cid not in by_id or by_id[cid]["seq"] >= r["seq"]):
            problems.append(f"CAUSATION_DANGLING:{r.get('seq')}")

    def root_types(r):
        seen, cur = [], r
        while cur is not None and len(seen) < 50:
            seen.append(cur)
            cur = by_id.get(cur.get("causation_id") or "")
        return seen

    attempts = [r for r in recs if r.get("type") == REQUEST]
    results = [r for r in recs if r.get("type") in RESULT_TYPES]
    for a in attempts:
        if a.get("phase") != "ATTEMPT" or not a.get("critical"):
            problems.append(f"REQUEST_NOT_DURABLE:{a.get('seq')}")

    # --- 1/2/5: ogni tentativo ha un esito; UNKNOWN o senza esito => riconciliazione
    for a in attempts:
        res = [r for r in results if r.get("causation_id") == a["event_id"]]
        rec = [r for r in recs if r.get("type") == "RECONCILIATION_RESOLVED"
               and r.get("order_ref") == a.get("order_ref") and r["seq"] > a["seq"]]
        if not res and not rec:
            problems.append(f"MISSING_RESULT:{a.get('order_ref')}")
            continue
        if any(r.get("type") == RESPONSE and r.get("outcome") == "UNKNOWN" for r in res) and not rec:
            problems.append(f"MISSING_RECONCILIATION:{a.get('order_ref')}")

    # --- disco pieno (semantica BB-1, Claude B pfab-b-20261108-blackbox-oracle-ack-001): due casi soltanto.
    # Caso 1, buffer recuperato: la richiesta dell'uscita e' presente con data.buffered=true E una BLACKBOX_GAP
    # (refs noti) ne dichiara il customerOrderRef. Un ingresso bufferizzato non e' ammesso (ingressi rifiutati).
    # Caso 2, buffer perso: un'uscita SENZA richiesta e' valida solo se cade nell'intervallo di una BLACKBOX_GAP con
    # outcome UNKNOWN E ha una RECONCILIATION_RESOLVED (phase OBSERVATION) per il suo bet.
    gaps = [r for r in recs if r.get("type") == "BLACKBOX_GAP"]
    known_gap_refs = set()
    for g in gaps:
        if g.get("outcome") != "UNKNOWN":
            known_gap_refs.update((g.get("data") or {}).get("refs") or [])
    for a in attempts:
        if (a.get("data") or {}).get("buffered"):
            if (a.get("data") or {}).get("purpose") == "ENTRY":
                problems.append(f"ENTRY_DURING_OUTAGE:{a.get('order_ref')}")
            elif a.get("order_ref") not in known_gap_refs:
                problems.append(f"BUFFERED_WITHOUT_GAP:{a.get('order_ref')}")

    def in_unknown_gap(order):
        ts = _ts(order.get("placedDate"))
        for g in gaps:
            d = g.get("data") or {}
            lo, hi = _ts(d.get("from_ts")), _ts(d.get("to_ts"))
            if g.get("outcome") == "UNKNOWN" and lo and hi and ts and lo <= ts <= hi:
                return True
        return False

    # --- ogni ordine sull'exchange: richiesta durevole prima, esito con il suo bet_id
    exch = list(exchange_orders)
    exch_bets = {_bet(o) for o in exch}
    for o in exch:
        bet, ref = _bet(o), _ref(o)
        res = [r for r in results if r.get("bet_id") == bet]
        if not any(a.get("order_ref") == ref for a in attempts):
            observed = any(r.get("type") == "RECONCILIATION_RESOLVED" and r.get("bet_id") == bet
                           and r.get("phase") == "OBSERVATION" for r in recs)
            if ref.startswith("hedge:") and in_unknown_gap(o) and observed:
                continue                                    # caso 2: buffer perso, lacuna UNKNOWN + riconciliazione
            problems.append(f"MISSING_REQUEST:{bet}")
            continue
        if not res:
            problems.append(f"MISSING_RESULT_FOR_BET:{bet}")
            continue
        chain_ok = False
        for r in res:
            for anc in root_types(r)[1:]:
                if anc.get("type") == REQUEST and anc["seq"] < r["seq"]:
                    chain_ok = True
            if r.get("type") == "RECONCILIATION_RESOLVED" and any(
                    a.get("order_ref") == ref and a["seq"] < r["seq"] for a in attempts):
                chain_ok = True
        if not chain_ok:
            problems.append(f"MISSING_REQUEST:{bet}")
        # 3: abbinamento
        sm = _q(o.get("sizeMatched") or 0)
        if sm and sm > 0:
            obs = [r for r in recs if r.get("type") == MATCHED and r.get("bet_id") == bet]
            if not obs:
                problems.append(f"MATCH_MISSING:{bet}")
            elif _q(((obs[-1].get("values_after") or {}).get("size_matched"))) != sm:
                problems.append(f"MATCH_MISMATCH:{bet}")
        # 4: hedge con causa
        if ref.startswith("hedge:"):
            req = [anc for r in res for anc in root_types(r) if anc.get("type") == CASHOUT_REQUEST]
            if not req:
                problems.append(f"HEDGE_WITHOUT_CAUSE:{bet}")

    # nessuna conferma senza ordine reale
    for r in results:
        if r.get("outcome") == "VERIFIED" and r.get("bet_id") and r.get("bet_id") not in exch_bets:
            problems.append(f"PHANTOM_CONFIRMATION:{r.get('bet_id')}")

    # 4: richieste di cashout con esito; protezioni con esito di chiusura
    for c in (r for r in recs if r.get("type") == CASHOUT_REQUEST):
        if not any(r.get("causation_id") == c["event_id"] and r.get("phase") == "RESULT" for r in recs):
            problems.append(f"CASHOUT_WITHOUT_RESULT:{c.get('seq')}")
    for t in (r for r in recs if r.get("type") in TRIGGERS):
        if not any(r.get("causation_id") == t["event_id"] and r.get("type") in CLOSE_OUTCOMES for r in recs):
            problems.append(f"TRIGGER_WITHOUT_CLOSE_OUTCOME:{t.get('seq')}")

    # 6: P&L
    for s in settlements:
        m = str(s.get("market_id"))
        rows = [r for r in recs if r.get("type") == SETTLEMENT and str(r.get("market_id")) == m]
        if not rows:
            problems.append(f"SETTLEMENT_MISSING:{m}")
        elif _q((rows[-1].get("values_after") or {}).get("net_pnl")) != _q(s.get("net_pnl")):
            problems.append(f"SETTLEMENT_MISMATCH:{m}")

    # segreti
    blob = json.dumps(recs, default=str)
    for secret in secrets:
        if secret and secret in blob:
            problems.append("SECRET_LEAK")
            break
    return problems
