# Errata D03 lotto 1 — ciclo di correzione 1/5 (2/2)

> Correzioni di errori **dimostrati** segnalati da Sol/Grok sulla PR #508. Prevalgono sulle versioni nei file 20_*/21_*. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO.

### DIFF-PF-BF-17-SCHEMA — CORRETTO (Grok: codice MISSING ma conformità affermata)
- Requisito: (codice MISSING: requisito per l'implementazione futura, nessuna conformità attuale affermata) updateOrders: parametri e output usati da Pickfair corrispondono alla firma ufficiale versionata (2024-06-19); un campo non documentato non viene inviato
- R1: «updateOrders(marketId, instructions ≤60, customerRef): Update non-exposure changing fields» — https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687485 (updateOrders, versione 2024-06-19, testo locale sha256 2bba25a659844ac8…)
- Oracle: input _payload updateOrders costruito da Pickfair_ → atteso _solo parametri della firma ufficiale 2024-06-19; tipi coerenti (Decimal per importi)_ · negativo _campo non in firma (es. betTargetType su .it) o importo bool/NaN ⇒ rifiuto locale prima dell'invio (updateOrders)_ · basis: Fonte B ufficiale Betfair citata in R1 (non il codice sotto esame)
- R5: MISSING (cercati: updateOrders, customerOrderRef, def ) · R6 NOT_RUN · R7 Nuova scheda updateOrders (#461 r.25), PR30/31
- Stato: SEMANTIC_RESOLVED_CANDIDATE · REVIEW_PENDING

### DIFF-PF-BF-11-SCHEMA — CORRETTO (stessa incoerenza (codice MISSING))
- Requisito: (codice MISSING: requisito per l'implementazione futura, nessuna conformità attuale affermata) listMarketProfitAndLoss: parametri e output usati da Pickfair corrispondono alla firma ufficiale versionata (2025-09-02); un campo non documentato non viene inviato
- R1: «listMarketProfitAndLoss(marketIds, includeSettledBets, includeBspBets, netOfCommission)» — https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687667 (listMarketProfitAndLoss, versione 2025-09-02, testo locale sha256 29a8e3730652b6ff…)
- Oracle: input _payload listMarketProfitAndLoss costruito da Pickfair_ → atteso _solo parametri della firma ufficiale 2025-09-02; tipi coerenti (Decimal per importi)_ · negativo _campo non in firma (es. betTargetType su .it) o importo bool/NaN ⇒ rifiuto locale prima dell'invio (listMarketProfitAndLoss)_ · basis: Fonte B ufficiale Betfair citata in R1 (non il codice sotto esame)
- R5: MISSING (cercati: listMarketProfitAndLoss, customerOrderRef, def ) · R6 NOT_RUN · R7 PR29 (solo controllo incrociato)
- Stato: SEMANTIC_RESOLVED_CANDIDATE · REVIEW_PENDING

### DIFF-PF-BF-S02-SCHEMA — CORRETTO (Grok (2a9d5ad3d0c6): codice MISSING ma conformità affermata)
- Requisito: (codice MISSING: requisito per l'implementazione futura, nessuna conformità attuale affermata) Order Stream: parametri e output usati da Pickfair corrispondono alla firma ufficiale versionata (2026-05-11); un campo non documentato non viene inviato
- R1: «OrderSubscriptionMessage / OrderChangeMessage (op=ocm, oc[].orc[].uo[] con id, rfo, rfs, sm, sr, sl, sc, sv)» — https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687396 (Order Stream, versione 2026-05-11, testo locale sha256 c5ad13989e6c1422…)
- Oracle: input _payload Order Stream costruito da Pickfair_ → atteso _solo parametri della firma ufficiale 2026-05-11; tipi coerenti (Decimal per importi)_ · negativo _campo non in firma (es. betTargetType su .it) o importo bool/NaN ⇒ rifiuto locale prima dell'invio (Order Stream)_ · basis: Fonte B ufficiale Betfair citata in R1 (non il codice sotto esame)
- R5: MISSING (cercati: Order Stream, customerOrderRef, def ) · R6 NOT_RUN · R7 Nuova scheda Order Stream (#461 r.22) + PR31
- Stato: SEMANTIC_RESOLVED_CANDIDATE · REVIEW_PENDING
