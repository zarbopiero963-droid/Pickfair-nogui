# Financial Impact Gate (PF-FINANCIAL-IMPACT-GATE-01)

Procedura riutilizzabile per ogni PR che tocca cassa, Money Management, esposizione, ordini o uscite
protettive. Mandato owner del 23/10/2026 su #547. Non e' un workflow e non aggiunge costi: e' un audit
che l'agente esegue **una sola volta**, sull'ultimo head, prima di dichiarare la PR pronta.

Scopo: una correzione finanziaria non deve risolvere un difetto isolato lasciando vulnerabili le operazioni
che dipendono dallo stesso dato (stessa cassa, stessa esposizione, stesso stato dell'ordine).

## Quando si applica

La PR modifica almeno uno fra: `core/money_management.py`, `core/risk_desk.py`, `core/validators.py`,
`core/runtime_controller.py` (ammissione, fondi, settlement, auto-next), `core/trading_engine.py`,
`core/table_manager.py`, `core/order_router.py`, `controllers/dutching_controller.py`, cashout/hedge,
`order_manager.py`, `betfair_client`/`betfair_market_api`, config dei limiti.

## Le otto aree

| # | Area | Domanda da chiudere |
|---|------|---------------------|
| 1 | Cassa | La base dei cap viene da una lettura coerente, fresca, con timestamp, applicata in modo atomico? Una lettura arrivata tardi puo' riscrivere dati piu' recenti? |
| 2 | Money Management | Quando la cassa cambia, **tutti** i limiti configurati (singolo, totale, evento, tetti assoluti) vengono rivalutati? Nessun limite predefinito introdotto? |
| 3 | Esposizione | BACK = stake, LAY = liability; pending, unmatched, parziali, esposizione esterna: contati una sola volta, mai sottostimati? |
| 4 | Percorsi d'ingresso | Segnale, Dutching (precheck e manuale), auto-next, Microstake: passano tutti dagli stessi gate (percentuali valide, cassa verificata, cap, fondi)? |
| 5 | Stati ordine | ACK, rifiuto, timeout, AMBIGUOUS, recovery, idempotenza: ogni stato lascia l'esposizione e le riserve coerenti? AMBIGUOUS mai ritentato? |
| 6 | Uscite protettive | Hedge/cashout consentiti secondo le decisioni owner (#548), limitati ai fondi disponibili, non spoofabili da un ingresso? |
| 7 | Concorrenza | Un ordine diverso degli eventi (ACK durante una lettura, rilascio fra due letture, letture fuori ordine) puo' far sottostimare l'esposizione o usare dati superati? |
| 8 | Parita' SIM/LIVE | Stessi contratti e stessi percorsi? Cio' che richiede Betfair reale si marca NON VERIFICATO, mai PASS. |

## Regole

- Per ogni area: funzioni coinvolte (file:riga), test che la coprono, lacune, esito **PASS / FAIL / NON VERIFICATO**.
- **FAIL solo con riproduzione**: test RED sul head prima della correzione, GREEN dopo. Un rischio senza
  riproduzione si scrive come "teorico, non riprodotto" e non giustifica una patch.
- Si correggono nella stessa PR solo i difetti concreti introdotti o aggravati dalla PR; i preesistenti
  indipendenti diventano issue (con prova), non allargano la PR.
- Nessuna nuova regola economica, nessun cap monetario fisso di default; fail-closed e idempotenza invariati.
- Un solo giro: si verificano le modifiche e i loro effetti diretti, poi si conclude. Dopo una correzione si
  ripetono solo le verifiche pertinenti, poi suite completa, mutation, CI e review sul commit finale.

## Consegna

Commento sulla PR con la tabella:

| Area | Rischio | Test | Esito | Eventuale correzione |
|------|---------|------|-------|----------------------|

seguito, se tutto e' conforme e i gate sono verdi, da **PRONTA PER MERGE MANUALE OWNER** (o dallo stato reale).
Il merge resta all'owner dove la policy lo prevede; LIVE resta NO-GO finche' l'owner non lo autorizza.
