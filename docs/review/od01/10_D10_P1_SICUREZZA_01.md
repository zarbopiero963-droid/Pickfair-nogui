# D10 parte 1 — finding, errata, contraddizioni, customerOrderRef (1/2)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

# BF2-D10 — Pacchetto di revisione per l'owner (revisore umano distinto, decisione 15:46)

**Stato: REVIEW_PENDING** finché l'owner non risponde. Niente qui è REVIEWED.
Baseline: main `028cee9` · #505 `618884e` (non toccata). Fonti: checkpoint A–F e lotti L01–L10 su #506.

**Come rispondere:** spunta ✅ per approvare; spunta ❌ e scrivi dopo "NOTA:" per respingere. Un item senza spunte resta PENDING.
Ordine: prima ciò che pesa su sicurezza e Gate A (Parti 1–4), poi il dettaglio (Parti 5–6).
Conteggi: 5 finding · 10 errori · 15 contraddizioni · 10 customerOrderRef · 80 comportamenti · 59 righe fonti.

## Parte 1 — Finding nuovi (priorità massima: sicurezza e Gate A)

- [ ] ✅ **D10-F01** — **BF2-A-F01** (P1 candidato, PR30): `BET_IN_PROGRESS`/`ERROR_IN_MATCHER` trattati come rifiuto certo. `betfair_client.py:1686-1691` solleva `BET_FAILED`; i marker `order_unknown` in `1724-1730` non li includono. Regola: UNKNOWN_OUTCOME, retry bloccato.
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-F02** — **BF2-R-F01** (P2, PR30/31): `core/reconciliation_engine.py:714-719` e `986-998` trattano `customerRef` e `customerOrderRef` come alias. La doc placeOrders dice che customerRef non persiste nella risposta né nello stream.
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-F03** — **BF2-C-F01** (P2, scheda Stream/PR31): `services/streaming_feed.py:264-270` scarta l'intero messaggio con status=503; la doc dice 503 = latenza, dati validi.
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-F04** — **BF2-C-F02** (P2, scheda Stream/PR31): `services/streaming_feed.py:341-352` non legge `img`; la doc dice di sostituire la cache.
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-F05** — **BF2-E-F04** (P2 candidato, PR19/31): `core/runtime_controller.py:780-784` chiede solo `bet_status="SETTLED"`: un VOIDED dopo il settlement non viene letto. (Correlato: E-F01 dup BF-FORUM-08, chiave `cleared:{market}:{bet}` a `:840` invariata dopo un resettlement.)
  - [ ] ❌ RESPINGI · NOTA: 


## Parte 2 — Errori del dossier già corretti (L01)

- [ ] ✅ **D10-E01** — Fonti: 59 righe, 9 alias ⇒ ≈50 distinte (non 58)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E02** — TS-F09 duplica TS-B09 ⇒ 67 specifiche distinte
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E03** — TS-D01…D13 esplicitati singolarmente
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E04** — TS-A14 esplicitato (era solo nella riga T17)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E05** — TS-B08: oracle 'senza netting (O-B3)' sostituito da OWNER-EXPOSURE-01
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E06** — TS-E08: premessa 'poller MARKET' errata (poller a livello BET)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E07** — TS-E02: commission non disponibile a livello BET ⇒ oracle solo con query MARKET separata
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E08** — Riferimento commissione corretto: `pnl_engine.py:67-78` (non 45-50)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E09** — 68 test normalizzati in tabella (tipo/PR/priorità = proposte)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-E10** — Aggiunto finding mancante BF2-R-F01
  - [ ] ❌ RESPINGI · NOTA: 


## Parte 3 — Contraddizioni (regola prudente)

- [ ] ✅ **D10-K01** — `D09-A1/F1` Stake minimo IT: doc 2 € vs owner 0,10 € LIVE → **RISOLTA (ACCOUNT_SPECIFIC): niente minimo 2 €, stake mai aumentato, INVALID_BET_SIZE fail-closed**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K02** — `D09-E1` CLOSED = settled? → **RISOLTA: CLOSED ≠ settlement, nessun rilascio su CLOSED**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K03** — `D09-A2` Eco del customerRef → **RISOLTA: customerRef non torna, non usarlo per correlare**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K04** — `D09-A3/F2` Esecuzione dopo BET_IN_PROGRESS/ERROR_IN_MATCHER/REGULATOR_IS_NOT_AVAILABLE → **APERTA ⇒ UNKNOWN_OUTCOME**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K05** — `D09-A4/D2` customerOrderRef conservato dal replace → **APERTA (solo community) ⇒ correlare con intent_id e betId**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K06** — `D09-A5/C2` Bet delay sul replace → **APERTA ⇒ assumere delay, UNKNOWN fino a conferma**
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-K07** — `D09-B1` Segno di exposure → **APERTA ⇒ fonte di verità = riserva interna**
  - [ ] ❌ RESPINGI · NOTA: 

