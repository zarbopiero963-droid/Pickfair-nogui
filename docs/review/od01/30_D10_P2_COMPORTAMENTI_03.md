# D10 parte 2 — 80 comportamenti (3/6)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

- [ ] ✅ **D10-BB006** — Una bet unmatched può essere abbinata in qualsiasi momento finché resta nel mercato; ripetere l'invio crea esposizione multipla. · S-B08 · OFFICIAL (help)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB007** — Il calcolo dell'esposizione Betfair è stato rifatto: oggi è "near instantaneous". Prima, i calcoli concorrenti causavano `ERROR_IN_MATCHER`. · S-B07 · STAFF/HISTORICAL. Non garantisce che `getAccountFunds` letto subito dopo una bet sia aggiornato.
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB008** — `listMarketProfitAndLoss`: P&L per esito sui soli mercati **OPEN** e **ODDS**, gli altri tipi sono ignorati in silenzio; `netOfCommission` usa la commissione corrente del mercato; per i mercati CLOSED serve `listClearedOrders`. BSP e settled sono esclusi di default. · S-B04 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB009** — Gli endpoint Account sono separati da quelli Betting. Per l'Italia sono gli stessi host `api.betfair.com/exchange/account`, con sessione .it. · S-A10, S-B09 · OFFICIAL + COMMUNITY
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB010** — Nessuna API offre una **riserva** o un lock dei fondi lato client: tra il risk check e `placeOrders` il saldo può cambiare (altri processi, ordini manuali, match). L'unico arbitro è il rifiuto `INSUFFICIENT_FUNDS` al momento del piazzamento. · assenza in S-A01/S-B01 (gap documentato) · OFFICIAL by omission: nessuna garanzia
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BB011** — Italia: vincita potenziale max 10.000 € stake incluso; lay vincolato al back equivalente. · S-A10 · OFFICIAL (IT)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC001** — Ogni sottoscrizione restituisce un **SUB_IMAGE** iniziale. Un SUB_IMAGE può arrivare anche durante la sottoscrizione e allora **sostituisce l'intera cache**. `img=true` (market) / `fullImage=true` (order) sostituiscono l'elemento e non sono delta. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC002** — `initialClk`/`clk` vanno memorizzati: initialClk sull'immagine, clk su ogni messaggio non segmentato o su SEG_END. Il re-subscribe funziona **solo con criteri identici** e produce RESUB_DELTA; alcuni mercati possono tornare con img=true. Un clock non valido ⇒ `INVALID_CLOCK`. Lo staff dice che i clock non sono riutilizzabili da un'altra connessione (S-C02). · OFFICIAL + STAFF
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC003** — `heartbeatMs` 500–5000: se non ci sono cambi arriva `ct=HEARTBEAT`. Nessun messaggio per **2× heartbeat** ⇒ considerare la connessione persa e riconnettersi con back-off. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC004** — `status=503` = dati stream **non affidabili** ("not all bets and market changes will be reflected"). Il client **non** deve disconnettersi; al recupero arrivano i dati aggiornati. Lo status è per sottoscrizione, su heartbeat **e su change message**. `status` null = aggiornato. · OFFICIAL (= BF-14)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC005** — `con=true` = più cambi conflati (client lento, conflateMs > 0, ciclo lento). Con chiave delayed o account delay, `conflateMs` = **180000**. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC006** — **Order stream e market stream sono prodotti da sistemi indipendenti: nessuna garanzia d'ordine fra loro.** · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC007** — Order stream: ordini inviati **per intero** a ogni cambio, il transient verso EXECUTION_COMPLETE è inviato ma non compare nell'immagine iniziale. Campi: id, p, s, bsp, side, status E/EC, pt L/P/MOC, ot L/MOC/LOC, pd, md, cd, ld, **sm, sr, sl, sc, sv**, avp, lsrc, rfo, rfs; matched ladder mb/ml in delta. `fullImage` vuoto ⇒ nessuna posizione, rimuovere dalla cache. · OFFICIAL (= BF-15)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BC008** — `lsrc` (lapse reason): MKT_UNKNOWN, MKT_INVALID, RNR_UNKNOWN, **TIME_ELAPSED** (troppo tempo in coda), PRICE_INVALID, **MKT_SUSPENDED**, **MKT_VERSION**, SMALL_STAKE, PRICE_IMP_TOO_LARGE, ecc. Null se non lapsed o lapsed per altro motivo. · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

