# D10 parte 1 — finding, errata, contraddizioni, customerOrderRef (2/2)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

- [ ] ✅ **D10-K08** — `D09-B2` Netting Betfair IT → **APERTA ⇒ vale OWNER-EXPOSURE-01**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K09** — `D09-B3` Latenza dei fondi → **APERTA ⇒ TTL, stale ⇒ no LIVE**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K10** — `D09-C1` Causa BF-FORUM-02 → **APERTA (community contestata) ⇒ nessun requisito**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K11** — `D09-C3` Connessioni stream massime → **APERTA ⇒ MAX_CONNECTION_LIMIT_EXCEEDED = DEGRADED**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K12** — `D09-D1` Dedup customerRef tra sessioni/app → **APERTA ⇒ assumere nessun dedup**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K13** — `D09-E2` SLA del cleared → **APERTA ⇒ PENDING senza timeout di rilascio**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K14** — `D09-E3` NEXT_GOAL → **STORICA ⇒ N/A finché il mercato non è abilitato**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K15** — `D09-F3` Differenze IT in-play/delay/settlement → **NON DOCUMENTATA ⇒ regole globali**
  - [ ] ❌ RESPINGI · NOTA: 


## Parte 4 — customerOrderRef (decisione owner #426/6125270694)

- [ ] ✅ **D10-I01** — Lunghezza ≤ 32 caratteri — OFFICIAL ("limited to 32 characters")
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I02** — Nessuna validazione di unicità lato Betfair — OFFICIAL ⇒ unicità a carico di Pickfair
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I03** — Stringa vuota = null — OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I04** — Charset NON documentato ⇒ UNVERIFIED, usare `[A-Za-z0-9-_]`
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I05** — Visibile in listCurrentOrders/listClearedOrders (filtro `customerOrderRefs`) e nello stream come `rfo` — OFFICIAL
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I06** — Scope/retention tra sessioni e app key ⇒ UNVERIFIED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I07** — Conservazione su replace ⇒ UNVERIFIED (solo community)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I08** — Nessuna idempotenza: mai prova d'inesistenza né dedup permanente
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I09** — Identità: intent_id (interno, persistito per primo) ≠ customerOrderRef (correlazione, persistito prima dell'invio, derivato da intent_id+tentativo) ≠ customerRef (dedup 60 s) ≠ betId (Betfair) ≠ customerStrategyRef (solo filtro)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-I10** — Re-invio solo con nuovo intent_id e nuovo ref dopo ABSENT_PROBABLE, mai automatico, mai stake aumentato
  - [ ] ❌ RESPINGI · NOTA: 

