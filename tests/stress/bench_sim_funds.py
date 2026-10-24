"""#543: benchmark riproducibile del ricalcolo fondi del SimulationBroker su sessioni lunghe.

Carico deterministico: ``ordini`` piazzati a turno su ``mercati`` mercati aperti (book a 2 runner, vincitore
unico). Ogni mercato ha poca liquidita' al primo livello: i primi BACK si abbinano (anche in parte, quindi ci
sono fill), poi gli ordini restano a riposo; i LAY sotto la miglior quota restano a riposo. Saldo ampio: nessun
rifiuto per fondi, quindi si misura il ricalcolo, non il rifiuto.

Stampa i tempi (totale dei piazzamenti, ultimo ``place_bet``, ``get_account_funds`` finale) e un'impronta
contabile (fondi finali, abbinato, numero di fill, stati degli ordini) che deve essere identica prima e dopo
un'ottimizzazione.

Uso:  python tests/stress/bench_sim_funds.py 1000 50
      python tests/stress/bench_sim_funds.py 2000 100 --json

(Non e' un test raccolto da pytest: misura tempi, che dipendono dalla macchina.)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from simulation_broker import SimulationBroker  # noqa: E402


def _book(market_id: str) -> dict:
    return {
        "marketId": market_id, "status": "OPEN", "numberOfWinners": 1,
        "runners": [
            {"selectionId": 1, "status": "ACTIVE",
             "ex": {"availableToBack": [{"price": 2.0, "size": 7.0}],
                    "availableToLay": [{"price": 2.1, "size": 7.0}]}},
            {"selectionId": 2, "status": "ACTIVE",
             "ex": {"availableToBack": [{"price": 2.0, "size": 7.0}],
                    "availableToLay": [{"price": 2.1, "size": 7.0}]}},
        ],
    }


def esegui(ordini: int, mercati: int) -> dict:
    broker = SimulationBroker(starting_balance=1e9, commission_pct=4.5)
    ids = [f"1.{100000 + m}" for m in range(mercati)]
    for market_id in ids:
        broker.update_market_book(market_id, _book(market_id))
    ultimo = 0.0
    inizio = time.perf_counter()
    for i in range(ordini):
        market_id = ids[i % mercati]
        lato = "BACK" if i % 2 == 0 else "LAY"
        t0 = time.perf_counter()
        risposta = broker.place_bet(market_id=market_id, selection_id=1 + (i // mercati) % 2, side=lato,
                                    price=2.0, size=2.0 + (i % 3), customer_ref=f"bench-{i}")
        ultimo = time.perf_counter() - t0
        if risposta.get("status") != "SUCCESS":
            raise SystemExit(f"ordine {i} rifiutato: {risposta}")
    totale = time.perf_counter() - inizio
    t0 = time.perf_counter()
    fondi = broker.get_account_funds()
    t_fondi = time.perf_counter() - t0

    stati: dict = {}
    for order in broker.state.orders.values():
        stati[order.status] = stati.get(order.status, 0) + 1
    impronta = {
        "available": round(fondi["available"], 9), "exposure": round(fondi["exposure"], 9),
        "total": round(fondi["total"], 9), "reserved_unmatched": round(fondi["reserved_unmatched"], 9),
        "matched": round(sum(o.matched_size for o in broker.state.orders.values()), 9),
        "fills": len(broker.state.fills), "stati": dict(sorted(stati.items())),
    }
    return {
        "ordini": ordini, "mercati": mercati,
        "totale_s": round(totale, 3), "ultimo_place_bet_ms": round(ultimo * 1000, 2),
        "get_account_funds_ms": round(t_fondi * 1000, 2),
        "impronta": impronta,
        "impronta_sha": hashlib.sha256(json.dumps(impronta, sort_keys=True).encode()).hexdigest()[:16],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("ordini", type=int)
    parser.add_argument("mercati", type=int)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    esito = esegui(args.ordini, args.mercati)
    if args.json:
        print(json.dumps(esito, sort_keys=True))
        return
    print(f"{esito['ordini']} ordini / {esito['mercati']} mercati: totale {esito['totale_s']} s, "
          f"ultimo place_bet {esito['ultimo_place_bet_ms']} ms, get_account_funds {esito['get_account_funds_ms']} ms, "
          f"impronta {esito['impronta_sha']} {esito['impronta']}")


if __name__ == "__main__":
    main()
