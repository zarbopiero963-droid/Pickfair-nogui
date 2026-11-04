# Negoziabilità del mercato all'ingresso (F-23, #586 NF-11/STR-10/SUP-04)

Decisione owner **PF-EXEC-05** (#426 c.6235676826): mercato sospeso, feed o disconnessione = nessun ingresso, nessun
riprezzamento, nessun retry, riconciliazione.

## Regola

Un **nuovo ingresso** è ammesso solo se una lettura **fresca** del book del suo mercato, fatta all'ammissione, dice
che il mercato è negoziabile (`core.liquidity.market_tradable_reason`):

| Condizione | Motivo |
|---|---|
| book assente, illeggibile, senza `runners`, errore o timeout di lettura | `mercato_non_verificabile:BOOK_UNAVAILABLE` |
| `marketId` del book diverso dal mercato richiesto | `mercato_non_verificabile:BOOK_MISMATCH` |
| nessuna selezione valida nell'ingresso | `mercato_non_verificabile:SELECTION_MISSING` |
| stato del mercato diverso da `OPEN` (SUSPENDED, CLOSED, INACTIVE, assente) | `mercato_non_negoziabile:<stato>` |
| selezione assente dal book o non `ACTIVE` (REMOVED, WINNER, …, assente) | `selezione_non_negoziabile:<id>:<stato>` |

Nessuna soglia d'età nuova: la lettura è contemporanea all'ammissione (non si usa la cache del `market_tracker`, né i
dati dello stream). La verifica è indipendente dal Liquidity Guard: con il guard spento lo stato del mercato si
controlla comunque; con il guard acceso, sullo stesso book, restano i motivi di liquidità F030 (prima nel RiskGate).

## Percorsi

- **Segnale e auto-next**: `RuntimeController._read_entry_market` legge il book una sola volta fuori dal lock
  d'ammissione (stessa finestra di rete dei fondi), ne deriva liquidità F030 e negoziabilità; la richiesta porta
  `market_tradability_required` e `market_not_tradable_reason`, e il RiskGate nega con `RISK_MARKET_NOT_TRADABLE`
  (riga ordine `DENIED`, nessun invio, tavolo e chiave rilasciati come per ogni esito certo).
- **Bet manuale e lotto Dutching**: `DutchingController` legge il book fuori dal lock
  (`read_market_for_controller_entry`) e, sotto il lock, rifiuta **l'intero lotto** se una sola selezione non è
  negoziabile (`controller_market_tradable_reason`): mai gambe negate una per una (Dutching parziale, PF-EXEC-02).
  Risorse rilasciate, nessun ordine.
- **Uscite protettive** (hedge del cashout): non portano la richiesta di verifica, non vengono mai ritardate
  (PF-EXEC-03).

## Limiti dichiarati

- **Corsa fra verifica e invio**: un mercato sospeso fra la lettura e il `placeOrders` è rifiutato da Betfair
  (`FAILURE`, esito certo): nessun ordine, risorse rilasciate. L'exchange resta autorevole. **Parità SIM**: il
  `SimulationBroker` ora rifiuta allo stesso modo (`MARKET_SUSPENDED`, `MARKET_NOT_OPEN_FOR_BETTING`, istruzione
  `RUNNER_REMOVED`, batch rifiutato intero); prima accettava — e abbinava — ordini su un mercato sospeso. Provato sul
  runtime vero con un mercato OPEN alla verifica e SUSPENDED all'invio (`suspend_on_place` nell'harness F026).
  Senza book caricato il broker non conosce lo stato (invariato), ma l'ammissione F-23 nega già l'ingresso.
- **SIM**: il book del broker simulato esiste solo se caricato (stream/fallback in LIVE, dataset di replay, feed F-31).
  Senza book, in SIM nessun ingresso è ammesso (come già con il Liquidity Guard attivo di default).
