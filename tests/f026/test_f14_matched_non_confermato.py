"""F-14 / MCP-01k (aggiunta owner al NIT-3, 05/11/2026): il `matched` NON confermato di un esito AMBIGUOUS non entra
MAI in P&L, esposizione accertata, riserve o disponibilita' di cassa, ne' prima ne' dopo restart e riconciliazione.

Runtime reale (HeadlessApp in sottoprocesso, SQLite, Money Management e poller del settlement reali), broker SIM come
confine dell'exchange. Modalita' `mixed_diag`: l'exchange esegue DAVVERO l'ordine (BACK abbinato per intero), ma al bot
torna un esito misto (PROCESSED_WITH_ERRORS, report SUCCESS con il betId reale) con `sizeMatched` DIAGNOSTICO 999 e
`averagePriceMatched` 9.9, entrambi falsi. RED-sensibile: se quei valori entrassero in uno dei quattro calcoli, gli
importi qui sotto cambierebbero di ordini di grandezza (999 x 8.9 di vincita, o 999 di liability). Quote SINTETICHE.
"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from tests.f026.test_f026_financial import START, account_ok, describe, q, run

pytestmark = pytest.mark.integration

M = "1.741"
COMM = D("0.045")
DIAG = D("999")


def _row(s):
    rows = [o for o in s["orders"] if o["market_id"] == M]
    assert len(rows) == 1, s["orders"]
    return rows[0]


def _exchange_matched(s):
    """Quantita' CONFERMATA dall'exchange (ordini correnti), l'unica fonte ammessa."""
    return sum(D(str(o["matched"] or 0)) for o in s["exch_orders"] if o["market_id"] == M and o["side"] == "BACK")


# Cosa conferma Betfair (exchange SIM) per lo stesso ordine, mentre il bot riceve sempre il report diagnostico 999:
#  pieno    = abbinato per intero (importo confermato diverso dal diagnostico);
#  parziale = abbinato solo 1 EUR (book con 1 EUR di contropartita): importo confermato diverso e minore dello stake;
#  nessuno  = nessun abbinamento (book peggiore del prezzo): l'ordine decade alla chiusura del mercato.
SCENARI = {"pieno": [], "parziale": [{"op": "mode", "m": M, "mode": "partial"}],
           "nessuno": [{"op": "mode", "m": M, "mode": "nomatch"}]}


@pytest.fixture(scope="module", params=list(SCENARI), ids=list(SCENARI))
def corsa(request):
    r1 = run(SCENARI[request.param] + [{"op": "mode", "m": M, "mode": "mixed_diag"},
                                        {"op": "signal", "m": M, "price": 2.0}, {"op": "crash_now"}])
    r2 = run([{"op": "wait", "s": 1}, {"op": "settle", "m": M, "win": True}], home=r1["_home"])
    return r1, r2


def test_prima_della_riconciliazione_valori_pre_ordine(corsa) -> None:
    r1, _r2 = corsa
    s = r1["snaps"][1]                                   # dopo il segnale, prima del crash
    row = _row(s)
    stake = D(str(row["stake"]))
    assert row["status"] == "AMBIGUOUS", describe(s)
    assert D(str(row["matched"])) == DIAG                # il report conservato porta il dato diagnostico...
    # ...ma nessun calcolo finanziario lo usa:
    # 1. P&L: nessun realizzato
    assert q(s["realized"]) == q(0), describe(s)
    # 2. esposizione accertata: la riserva della RICHIESTA (stake del BACK), non 999
    tavolo = [t for t in s["tables"] if t["market_id"] == M]
    assert len(tavolo) == 1 and q(tavolo[0]["exposure"]) == q(stake), describe(s)
    # 3. riserva trattenuta (tavolo occupato, nessun rilascio su esito ignoto)
    assert "1.741" in {t["market_id"] for t in s["tables"]}
    # 4. cassa: i fondi sono quelli dell'exchange (100 - stake impegnato davvero); il bankroll del MM resta il valore
    #    pre-ordine fino al settlement (nessun matched contabilizzato), mai ridotto o aumentato dal dato diagnostico
    assert q(s["funds"]["available"]) == q(START - stake), describe(s)
    assert q(s["bankroll"]) == q(START), describe(s)
    # equita' di cassa (disponibile + impegnato) invariata: lo stake si sposta fra le due voci, nessun importo inventato
    assert q(s["account"]["cash_equity"]) == q(START) == q(s["funds"]["total"]), describe(s)
    assert _exchange_matched(s) <= stake < DIAG          # quantita' confermata dall'exchange, mai il 999


def test_dopo_restart_e_settlement_contano_solo_i_valori_confermati(corsa) -> None:
    r1, r2 = corsa
    stake = D(str(_row(r1["snaps"][1])["stake"]))
    confermato = _exchange_matched(r1["snaps"][1])

    riavvio = r2["snaps"][0]                             # dopo il riavvio, prima del settlement
    tavolo = [t for t in riavvio["tables"] if t["market_id"] == M]
    assert len(tavolo) == 1 and q(tavolo[0]["exposure"]) == q(stake), describe(riavvio)
    assert q(riavvio["realized"]) == q(0) and q(riavvio["funds"]["available"]) == q(START - stake)

    finale = r2["snaps"][-1]                             # settlement: vince la selezione puntata
    atteso = confermato * (D("2.0") - 1) * (1 - COMM)    # dal SOLO abbinato confermato, prezzo richiesto 2.0
    assert q(finale["realized"]) == q(atteso), describe(finale)
    assert q(finale["funds"]["total"]) == q(START + atteso), describe(finale)
    tavoli = {t["market_id"] for t in finale["tables"] if t["market_id"]}
    if confermato > 0:
        assert M not in tavoli, "riserva non rilasciata al settlement di un abbinato confermato"
    else:
        # Nessun abbinamento: nessun settlement della bet, l'ordine decade. Un esito AMBIGUOUS non si libera mai
        # "come annullato" senza prova (F028_04): la riserva resta trattenuta finche' la riconciliazione non la chiude.
        assert M in tavoli, "esito ignoto liberato senza prova"
        # nessun evento di conto dopo la decadenza: la lettura resta quella (prudente) di prima, mai gonfiata
        assert q(finale["bankroll"]) <= q(finale["funds"]["available"]), describe(finale)
        return
    account_ok(finale)
    # mai il dato diagnostico: con 999 @ 9.9 il realizzato sarebbe ~8.4k, con 999 @ 2.0 ~954
    assert abs(D(str(finale["realized"]))) < DIAG
