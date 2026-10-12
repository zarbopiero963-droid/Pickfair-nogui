# D10 parte 2 — 80 comportamenti (4/6)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

- [ ] ✅ **D10-BC009** — Order stream `closed=true` alla chiusura del mercato. Lo staff (2016) dice che lo stream può essere **in anticipo** rispetto a listMarketBook/listClearedOrders; il settlement si legge da listClearedOrders. · STAFF/HISTORICAL (ripreso in E)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC010** — Limiti: errori `MAX_CONNECTION_LIMIT_EXCEEDED`, `TOO_MANY_REQUESTS` (connessioni troppo frequenti), `SUBSCRIPTION_LIMIT_EXCEEDED` (default **200 mercati**, unico errore che non chiude la connessione); `connectionsAvailable` sull'autenticazione. Senza marketIds la sottoscrizione copre tutti i mercati. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC011** — MarketDefinition: status, inPlay, betDelay, betDelayModels (PASSIVE/DYNAMIC), version, marketTime, bspReconciled. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC012** — Dati ritardati con chiave live se l'account non è mai stato finanziato o ha un override; `isMarketDataDelayed` true visto da utenti con chiave live (account-specific). · OFFICIAL di supporto + ACCOUNT_SPECIFIC (= BF-17)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC013** — Un ordine REST non visibile subito nello stream: segnalazione community (S-C03) con causa contestata. In ufficiale c'è solo BF2-C006 (nessun ordine garantito) e BF2-C004 (503). · COMMUNITY + OFFICIAL indiretto
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD001** — **Unica dedup lato Betfair:** `customerRef` per richiesta, **60 s**. Nessuna idempotenza durevole, nessuna dedup per `customerOrderRef` (unicità non validata), nessuna dedup tra sessioni documentata oltre a questo. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD002** — Identità recuperabili dopo un crash: `betId` (se ricevuto); `customerOrderRef`/`customerStrategyRef` (filtri su listCurrentOrders e listClearedOrders, `rfo`/`rfs` in stream); `sourceIdKey` (**app key** che ha piazzato l'ordine) su CurrentOrderSummary e ClearedOrderSummary (`includeSourceId`). Il `customerRef` **non** è recuperabile. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD003** — `listCurrentOrders`: max 1000 per pagina, `moreAvailable`; "underlying data is highly time-dependent" ⇒ **paginazione non consistente** (snapshot non garantito). · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD004** — `listClearedOrders`: default ultimi **90 giorni**, `settledDateRange` consigliato; LAPSED/CANCELLED solo con `groupBy=BET`; mercato con settlement multipli ⇒ il rollup di mercato include sempre tutte le liquidazioni. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD005** — Una sessione può essere usata da più thread/chiamate. Sessione IT: **20 minuti** senza keepAlive; login limitato a 100/min, oltre: ban di 20 min (`TEMPORARY_BAN_TOO_MANY_REQUESTS`), le sessioni esistenti restano valide. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD006** — **Nessun** lock distribuito, lease, fencing o riserva atomica dei fondi offerti da Betfair. Due processi sullo stesso account competono senza coordinamento; l'unico arbitro è il rifiuto al piazzamento (INSUFFICIENT_FUNDS, ecc.). · OFFICIAL by omission
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BD007** — Ordini manuali (sito/app) e di altri software sono visibili in listCurrentOrders e nello stream ordini dell'account, distinguibili solo per `sourceIdKey`/`rfo`/`rfs` o per betId noti. · OFFICIAL (campi) + inferenza documentata
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE001** — MarketStatus: INACTIVE, OPEN, SUSPENDED, CLOSED. L'enum dice che CLOSED = "has been settled", ma `MARKET_NOT_OPEN_FOR_BETTING` cita "closed awaiting settlement" e lo staff riporta cleared in ritardo dopo CLOSED. **CLOSED non è una prova di settlement finale** (D09-E1). · OFFICIAL (contraddittorio) + STAFF
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BE002** — BetStatus (cleared): SETTLED, VOIDED (matched annullata **prima, durante o dopo** il settlement), LAPSED (unmatched cancellata da Betfair), CANCELLED (unmatched cancellata dal cliente). LAPSED/CANCELLED solo con `groupBy=BET`. "LAPSE" esiste anche come PersistenceType, "CANCELLED"/"LAPSED" non sono MarketStatus. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

