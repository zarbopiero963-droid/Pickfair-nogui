# Verdetti motivati — 10 issue NEEDS_REVIEW (tutte restano APERTE)

Base main `a09d8d6`. Nessuna decisione owner precedente sovrascritta. "Verdetto" = proposta tecnica; decide l'owner.

| # | Verdetto proposto | Motivo | Cosa serve davvero all'owner |
|---|---|---|---|
| #274 | **KEEP** (istruzione owner «NON chiudere», 20/08) | Tracker storico consolidato in #453 il 13/09, ma i dettagli unici (11 test TOP30 Parte 2, 6 follow-up cashout, proof operazionali) sono in #453 solo per richiamo, non elencati | Nulla ora. Prima di un'eventuale futura chiusura: trasferire l'elenco puntuale in #453 |
| #374 | **KEEP** finché D-9/D-11/D-12 non sono registrate | Tracker storico; 3 decisioni (D-9 stake dal messaggio, D-11 tastiere Bot API da Pickfair-MarketAPI, D-12 CS_OTHER su 3 gambe) non trovate in #426/#453/#461 | **Decisione 1**: D-9, D-11, D-12 sono ancora vigenti? (Se sì le registro in #426 e #461; D-12 coincide con il requisito CS_OTHER del consolidamento) |
| #398 | **KEEP in sospeso** (rinviata da #426 P11) | La campagna sulle 69 PR chiuse presuppone i reviewer a label sospesi da P41: non eseguibile come scritta, ma P11 la rinvia, non la annulla | **Decisione 2**: abbandonarla (not planned) o mantenerla con budget Sol/Grok? |
| #454 | **KEEP** (§C) | Audit Grok: tutti i Top10 hanno destinazione in #453/#461, nessun requisito orfano; ma è fonte raggruppata (§C) | Nessuna: resta come fonte finché vale §C |
| #455 | **KEEP** (§C; ora unica fonte dopo la chiusura di #456) | Contiene la decisione owner «Quant escluso» (c.5446020778) e la mappa X01–X04 | Nessuna |
| #457 | **KEEP** (contiene P0 aperti) | P0-1 esposizione proiettata, P0-2 barriera STOP, feed→rischio→ordine, AMBIGUOUS: tracciati in PR28/PR29/PR37/PR11/PR30/PR31 ma non chiusi | Nessuna: si rivaluta quando quelle schede sono INTEGRATED_VERIFIED |
| #514 | **KEEP** come registro delle divergenze residue | Molte decisioni del 18/10 ora ratificate/implementate (#522, #610); restano non tracciate altrove: PF-C22-01 (EXPANSION su bankroll_start), freshness Delayed/Live, migrazione pre-v9 | **Decisione 3**: PF-C22-01 resta OWNER DECISION REQUIRED (già in #586 sez. C) — confermare dove registrarla |
| #565 | **KEEP** fino a prova sul repo | Ruleset con overlap-check legato alla GitHub App 15368 configurato; accettazione del check autentico ricavabile in lettura dalle PR #593–#610, ma il test negativo (check omonimo da altra app rifiutato) non è eseguito | **Decisione 4**: basta l'evidenza documentale + lettura delle PR, o serve il test negativo? |
| #578 | **KEEP**, corpo da riallineare | Dopo la rettifica t257 i backup automatici non sono più richiesti; restano attività vere: kit recovery, pulizia VPS MatchPilot, revoca vecchio PAT (owner) | **Decisione 5**: riscrivere il corpo con le sole attività residue (azione documentale, nessun intervento VPS)? |
| #605 | **KEEP** | Scheda automatica del registrar (label BACKLOG/pf-registrar): esito NOT_RUN/EXTERNAL_PENDING, non un FAIL. La gestisce il registrar | Nessuna urgente; la chiusura spetta al registrar quando lo scenario passa |
