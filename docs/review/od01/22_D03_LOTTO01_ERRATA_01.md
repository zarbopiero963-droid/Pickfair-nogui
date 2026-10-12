# Errata D03 lotto 1 — ciclo di correzione 1/5 (1/2)

> Correzioni di errori **dimostrati** segnalati da Sol/Grok sulla PR #508. Prevalgono sulle versioni nei file 20_*/21_*. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO.

### DIFF-PF-BF-16-STATE — CORRETTO (Sol (range 01e05fab357d): mancava matched(new))
- Requisito: replaceOrders: Esposizione = matched(old) + matched(new) + remaining(new); nuovo betId correlato tramite intent_id
- R1: «logically a bulk cancel followed by a bulk place» — https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687487 (replaceOrders, versione 2024-06-04, testo locale sha256 a29a83118a8289c9…)
- Oracle: input _replace su ordine con sm 0,04, nuovo prezzo; il nuovo ordine da 0,06 viene abbinato subito_ → atteso _vecchio betId: matched 0,04; nuovo betId: matched 0,06, remaining 0; esposizione riservata 0,10_ · negativo _riservare solo matched(old)+remaining(new)=0,04 o perdere il matched del vecchio betId_ · basis: Fonte B ufficiale Betfair citata in R1 (non il codice sotto esame)
- R5: LINKED betfair_client.py:1830-1866 · R6 NOT_RUN · R7 PR30
- Stato: SEMANTIC_RESOLVED_CANDIDATE · REVIEW_PENDING

### DIFF-PF-BF-09-ERROR — CORRETTO (Grok (2a9d5ad3d0c6): oracle ERROR e STATE invertiti)
- Requisito: listMarketBook: Errore/timeout ⇒ dati di mercato non affidabili ⇒ NO_ENTRY
- R1: «Separate requests should be made for OPEN & CLOSED markets» — https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687510 (listMarketBook, versione 2024-06-04, testo locale sha256 69e914843a79bc9f…)
- Oracle: input _listMarketBook in timeout prima di un'entrata_ → atteso _NO_ENTRY con motivo MARKET_DATA_UNRELIABLE_ · negativo _usare la cache stream STALE per decidere_ · basis: Fonte B ufficiale Betfair citata in R1 (non il codice sotto esame)
- R5: LINKED betfair_client.py:1472-1500 · R6 NOT_RUN · R7 PR31 / scheda Stream
- Stato: SEMANTIC_RESOLVED_CANDIDATE · REVIEW_PENDING

### DIFF-PF-BF-09-STATE — CORRETTO (Grok (2a9d5ad3d0c6): oracle ERROR e STATE invertiti)
- Requisito: listMarketBook: Mercato assente nella risposta mista ⇒ non dedurre CLOSED né settlement
- R1: «Requests that include both OPEN & CLOSED markets will only return those markets that are OPEN» — https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687510 (listMarketBook, versione 2024-06-04, testo locale sha256 69e914843a79bc9f…)
- Oracle: input _richiesta con un mercato OPEN e uno CLOSED_ → atteso _solo l'OPEN ritorna; l'altro resta stato precedente, nessun rilascio_ · negativo _dedurre che il mercato mancante è liquidato_ · basis: Fonte B ufficiale Betfair citata in R1 (non il codice sotto esame)
- R5: LINKED betfair_client.py:1472-1500 · R6 NOT_RUN · R7 PR31 / scheda Stream
- Stato: SEMANTIC_RESOLVED_CANDIDATE · REVIEW_PENDING
