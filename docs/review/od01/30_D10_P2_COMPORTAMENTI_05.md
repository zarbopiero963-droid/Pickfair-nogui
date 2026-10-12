# D10 parte 2 — 80 comportamenti (5/6)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

- [ ] ✅ **D10-BE003** — `commission`: disponibile **solo** ai livelli EXCHANGE/EVENT_TYPE/EVENT/**MARKET**, non per bet o side. `groupBy=MARKET` esclude LAPSED/CANCELLED. `profit` = P&L della riga, in valuta del conto. · OFFICIAL + STAFF (= BF-FORUM-04, chiuso)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE004** — Un mercato liquidato in più momenti: il rollup per mercato riporta **sempre la posizione complessiva** di tutti i settlement, anche filtrando per settledDateRange. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE005** — Resettlement: l'account statement mostra le voci originali marcate **RESULT_ERR**, poi lo storno della commissione (COMMISSION_REVERSAL/RESULT_FIX), lo storno della vincita (RESULT_FIX) e il nuovo esito (es. RESULT_LOST). · OFFICIAL (= BF-FORUM-08 confermato)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE006** — Ritardo cleared dopo la chiusura: da 2 a più di 10 minuti (2014), con latenza quando molti mercati si liquidano insieme. Lo staff dichiarava miglioramenti in corso. Nessuno SLA attuale. · STAFF/HISTORICAL (= BF-FORUM-07)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE007** — Calcio NEXT_GOAL: settlement su marketId diversi dall'originale (storico 2015). · STAFF/HISTORICAL; fuori dai mercati attivi se l'owner limita i tipi (da confermare)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE008** — `listMarketProfitAndLoss` solo per mercati OPEN e ODDS; `netOfCommission` alla tariffa corrente. Per i CLOSED si usa listClearedOrders. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE009** — Cashout: **non esiste un'operazione API di cashout**. Si fa con ordini BACK/LAY di hedge soggetti a tutte le regole di Famiglia A (parziali, lapse, sospensione, delay, INSUFFICIENT_FUNDS…). · OFFICIAL by omission (nessuna operazione nella lista Betting API)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF01** — Login non interattivo (bot) · `https://identitysso-cert.betfair.it/api/certlogin` con certificato client · identitysso-cert.betfair.com · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF02** — Login interattivo API / desktop · `identitysso.betfair.it/api/login`; `/view/login?product=<AppKey>&url=https://www.betfair.it` · .com · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF03** — Errori specifici di login · `ITALIAN_CONTRACT_ACCEPTANCE_REQUIRED`, `ITALIAN_PROFILING_ACCEPTANCE_REQUIRED` (vanno accettati sul sito) · — · CONFIRMED ⇒ fail-closed, nessuna entrata
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF04** — Durata della sessione · **20 minuti**, keepAlive obbligatorio entro il timeout · 12 h (24 h UK/IE) · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF05** — Limite di login · 100/min, poi ban di 20 min, le sessioni esistenti restano valide · uguale · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF06** — Endpoint Betting/Account · stessi host `api.betfair.com/exchange/{betting,account}` con sessione .it; restituiscono i mercati visibili su .it · uguale · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF07** — Stream · `stream-api.betfair.com:443`, disponibile per IT · uguale · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF08** — App key LIVE · richiesta "For My Personal Betting"; prima test con la delayed key; KYC completo; **nessun costo di attivazione** per IT; read-only con la live key non ammesso · possibile fee per altre giurisdizioni · CONFIRMED (solo uso personale)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF09** — Istruzioni per placeOrders · **max 50** (oltre: REJECTED_BY_REGULATOR / fallimento) · 200 · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF10** — Back e lay nella stessa placeOrders · **rifiutato** · ammesso · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF11** — Vincita potenziale massima · 10.000 € stake incluso (DM 111/2006 art. 12 c.4) · — · CONFIRMED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF12** — Stake minimo · Testo ufficiale corrente: back min 2,00 €, incrementi 0,50 €, lay con back equivalente ≥ 0,50 €. **Owner:** Small Bet attivo, 0,10 € LIVE eseguito · Min Bet Size EUR 1 + Min Bet Payout (non .it) · **CONTRADICTED → ACCOUNT_SPECIFIC; prevale l'owner** (D09-F1)
  - [ ] ❌ RESPINGI · NOTA: 

