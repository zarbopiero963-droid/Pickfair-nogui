# D10 parte 2 — 80 comportamenti (2/6)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

- [ ] ✅ **D10-BA013** — `cancelOrders`: senza `instructions` cancella **tutti** gli unmatched del mercato; senza marketId tutti gli unmatched dell'account; cancel-all concorrenti rifiutati (ERROR_IN_MATCHER). Solo LIMIT cancellabili; `sizeReduction` = cancel parziale; report con `sizeCancelled`; nessun report per istruzioni a livello account. Max 60 istruzioni. · S-A03, S-A05, S-A06 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA014** — `BET_TAKEN_OR_LAPSED` su cancel/modify **non distingue** abbinato da lapsed/cancellato; può arrivare anche su placeOrders a un evento admin o con marketVersion superata. Un utente riporta `listCurrentOrders` non ancora aggiornato subito dopo. · S-A05, S-A16 · OFFICIAL + COMMUNITY
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA015** — `listCurrentOrders` riporta per bet: sizeMatched, sizeRemaining, sizeLapsed, sizeCancelled, sizeVoided, averagePriceMatched (frammenti void esclusi, aggiustato per riduzioni), customerOrderRef, customerStrategyRef; max 1000 record/pagina; 250 betId+marketId per richiesta; `BY_MATCH_TIME` per tracciare match. · S-A06, S-A08 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA016** — `updateOrders` cambia solo campi che non toccano l'esposizione (persistenza); `NO_CHANGE`/`NO_ACTION_REQUIRED` se invariato. · S-A04, S-A05 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA017** — Errori di ammissione rilevanti: INSUFFICIENT_FUNDS, INVALID_ACCOUNT_STATE, INVALID_WALLET_STATUS, LOSS_LIMIT_EXCEEDED, MARKET_SUSPENDED, MARKET_NOT_OPEN_FOR_BETTING, PERMISSION_DENIED (delayed key), REJECTED_BY_REGULATOR (IT, >50 istruzioni), INVALID_BET_SIZE, INVALID_ODDS (fuori tick), INVALID_BACK_LAY_COMBINATION, RUNNER_REMOVED, BET_LAPSED_PRICE_IMPROVEMENT_TOO_LARGE. TOO_MANY_REQUESTS oltre 1000 istruzioni/s. Codici transport DSC-0019 Timeout = HTTP 504. · S-A05, S-A07, S-A09 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA018** — Bet delay: secondo la community place e replace subiscono il delay in-play, il cancel no; mercato sospeso durante il delay ⇒ lapse. · S-A17 · COMMUNITY/HISTORICAL — da confermare in Famiglia C
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA019** — Una ripetizione con stesso `customerRef` ha restituito `FAILURE/DUPLICATE_TRANSACTION` mentre l'utente vedeva la seconda bet abbinata (2013, nessuna risposta staff; possibile confusione con la prima bet). · S-A14 · COMMUNITY_REPORTED/HISTORICAL, UNVERIFIED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA020** — Italia: max 50 istruzioni per placeOrders; place con back e lay nello stesso ordine rifiutato; vincita potenziale max 10.000 € (stake incluso); regole stake minimo (vedi D09-A1). "Bet to payout" e "Min Bet Payout" **non** abilitati per .it. · S-A01, S-A10 · OFFICIAL (Italia)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB001** — `AccountFundsResponse` = availableToBetBalance, exposure, retainedCommission, exposureLimit, discountRate, pointsBalance (+ wallet). **Nessun timestamp, nessuna valuta, nessun numero di versione**: la freschezza non è dimostrabile dalla risposta. La valuta va letta da `getAccountDetails.currencyCode`. · S-B01, S-B02 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB002** — Il segno di `exposure` non è documentato. La community lo riporta come valore negativo. · S-B02 + community · UNVERIFIED (D09-B1)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB003** — `INSUFFICIENT_FUNDS` (esecuzione e istruzione) = superato "exposure limit **or** available to bet limit". Altri rifiuti account: INVALID_ACCOUNT_STATE, INVALID_WALLET_STATUS, LOSS_LIMIT_EXCEEDED (limite autoimposto). · S-A05 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB004** — Betfair trattiene in exposure il **worst case per mercato**: puntate su esiti opposti dello stesso mercato liberano disponibilità; il denaro passa da disponibile a exposure al piazzamento. · S-B06 (AU) · OFFICIAL per AU; per IT **non confermato**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB005** — Liability: BACK = stake; LAY = stake·(prezzo−1). Per BSP la liability è un parametro esplicito (`liability`, `bspLiability`). · S-A06, S-B06 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

