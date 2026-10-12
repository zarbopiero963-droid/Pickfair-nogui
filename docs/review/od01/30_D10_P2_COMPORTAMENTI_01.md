# D10 parte 2 — 80 comportamenti (1/6)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

# BF2-D10 — Pacchetto di revisione owner — parte 2/3
Stato: **REVIEW_PENDING**. Istruzioni nella parte 1.

## Parte 5 — 80 comportamenti API
_Formato: affermazione · fonti (ID registro, Parte 6) · classe._

- [ ] ✅ **D10-BA001** — `placeOrders` "in normal circumstances" è **atomico**; se la "Best Execution" è disattivata può restituire `PROCESSED_WITH_ERRORS` (alcune bet piazzate, altre no). Per replace/cancel/update `PROCESSED_WITH_ERRORS` = ordine accettato ma almeno un'azione (anche tutte) in errore. · S-A01, S-A05 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA002** — `customerRef`: max 32 char, alfabeto limitato, **finestra di de-dup 60 s**; errore `DUPLICATE_TRANSACTION`. Non è un'identità d'ordine e non va in Order Stream. · S-A01, S-A05, S-A12 · OFFICIAL + STAFF
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA003** — `customerOrderRef` per istruzione: max 32, **nessuna validazione di unicità**, interrogabile con `listCurrentOrders(customerOrderRefs)`, visibile come `rfo` in stream. `customerStrategyRef` max 15, `rfs` in stream, filtro su listCurrentOrders. · S-A06, S-A08, S-A12 · OFFICIAL + STAFF
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA004** — Esecuzione `TIMEOUT` (report o istruzione) = "status of the bet is unknown": controllare `listCurrentOrders` prima di piazzare altro; timeout dopo 5 s ma l'ordine può comparire fino a **15 s**; dopo, le bet non processate sono lapsed. Stesso testo per `TIMEOUT_ERROR` (APINGException). · S-A05, S-A07 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA005** — `BET_IN_PROGRESS` (istruzione) = nessun risultato dal matcher entro il timeout di sistema; `ERROR_IN_MATCHER` = matcher non sano, anche per cancel-all concorrenti. La documentazione **non** dice se l'ordine sia stato eseguito. · S-A05 · OFFICIAL (esito non definito)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA006** — `async=true`: `orderStatus=PENDING`, `betId` e `placedDate` null; tracciamento solo via `customerOrderRef` (API o stream). · S-A01, S-A05, S-A06 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA007** — `OrderStatus`: PENDING, EXECUTABLE (residuo unmatched), EXECUTION_COMPLETE (nessun residuo), EXPIRED (FOK ucciso). `orderStatus` è assente se l'istruzione fallisce e di default per MOC/LOC. · S-A05, S-A06 · OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA008** — FOK: `timeInForce` prevale su `persistenceType`; senza `minFillSize` match intero o niente, con `minFillSize` match ≥ min e residuo cancellato; il prezzo è il limite del **VWAP**, non di ogni frammento. Esempio ufficiale: `SUCCESS` con `sizeMatched=0` (kill). Errori dedicati TIME_IN_FORCE_CONFLICT, UNEXPECTED_PERSISTENCE_TYPE, INVALID_ORDER_TYPE, UNEXPECTED/INVALID_MIN_FILL_SIZE. · S-A01, S-A05, S-A06 · OFFICIAL (= BF-12)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA009** — `marketVersion`: se la versione corrente supera quella inviata, la bet è lapsed. Betfair accetta versioni ≥ ultima **variazione materiale** (sotto sospensione: rimozione runner, in-play, lapse/void per goal). Esempio ufficiale: top `FAILURE/BET_ACTION_ERROR`, istruzione `BET_TAKEN_OR_LAPSED` **con betId** e `orderStatus=EXECUTION_COMPLETE`. · S-A01, S-A05 · OFFICIAL (= BF-11, rafforzato)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA010** — PersistenceType: LAPSE (cancellato a in-play se unmatched), PERSIST (resta in-play, non cancellato da eventi materiali), MARKET_ON_CLOSE (all'SP). `INVALID_PERSISTENCE_TYPE` se non ammesso dal mercato o con BetDelayModel PASSIVE. · S-A05 · OFFICIAL (= BF-13)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA011** — `replaceOrders` = cancel bulk **poi** place bulk; i nuovi ordini sono atomici fra loro, ma **la cancellazione non viene annullata** se il place fallisce (`CANCELLED_NOT_PLACED`). Limite 60 istruzioni; `async` non per MOC/LOC. Il replace crea un **nuovo betId** sul solo residuo ("remaining stake"). · S-A02, S-A05, S-A06, S-A18 · OFFICIAL (= BF-18, BF-FORUM-09)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BA012** — Su replace di un ordine parzialmente abbinato il `customerOrderRef` risulta copiato sul nuovo betId. · S-A13 · COMMUNITY_REPORTED (non garantito)
  - [ ] ❌ RESPINGI · NOTA: 

