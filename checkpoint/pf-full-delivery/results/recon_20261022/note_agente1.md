# note_agente1 — inventario #461 / #426 (22/10/2026, sola lettura)

## Copertura delle fonti
- #461: corpo (53.812 car., aggiornato 2026-10-21T21:45Z) + 224 commenti (8 pagine da 30, pagina 9 vuota). 130 commenti sono blocchi base64 B1/B1-R2 (6109611019…6109875732, 11/10), non roadmap.
- #426: corpo + 132 commenti (5 pagine da 30, pagina 6 vuota).
- Numero schede trovate: **60 numerate PR01–PR60** (conteggio dichiarato "60" nel corpo #461; verificato 60/60 intestazioni in cards1–6 da D03 c.6087228239) + **5 inserti owner mergiati** (PR04-bis/ter/quater, PR26-0/PR-F001, PR26-a) + sotto-PR di PR28 (b/c/d/e + Microstake #512) e PR29 (a/b/c) + 4 "NUOVE SCHEDE NECESSARIE" senza numero (Order Stream, REST discovery, Account, updateOrders) + voci di sequenza non-scheda (PF-API-0/1/commands, MCP-01a). Totale oggetti in schede_461.json: 71.

## Distribuzione stato dichiarato (60 schede numerate)
- MERGED: 6 (PR01, PR02, PR03, PR04, PR26, PR27)
- PARZIALE: 3 (PR28 IMPLEMENTED_WITH_NOTE; PR29 in corso con 29-a/b/c merged; PR50 solo per P16 orchestratori via #527)
- NON INIZIATO / PENDING: 42
- SOSPESO (Telegram/Copy/Mirror, OWN-004): 6 (PR18, PR21, PR39–PR42)
- DEFERRED_FIRST_LIVE: 3 (PR56–PR58)
- Inserti: 5 MERGED. Nuove schede: 4 NON CREATE.
- Nessuna scheda dichiarata FAILED/BLOCCATO come stato PR; blocchi dichiarati come etichette trasversali: BLOCKS_LIVE (PR30, G7 #532, .runtime.lock), BLOCKS_DUTCHING_ACTIVATION (PR09/PR30/PR31), BLOCKED_ENV (PR33, #351 installati), Gate A #506 NON PASS, LIVE NO-GO.

## Fonte delle "60 schede"
- Tabella definitoria: **corpo #461**, sezione "Roadmap — 60 schede operative da riconciliare col codice corrente" (marker `PR-BY-PR-437-PHASE0-2026-09-14-REV2`), riga finale "Conteggio delle schede: 60".
- Schede individuali: commenti **5664139028** (PR01–15), **5664141515** (PR16–30), **5664144043** (PR31–45), **5664146928** (PR46–60), tutti 2026-09-14T12:44Z, rivisti 01/10.
- Addenda PF-DOC con stato requisito (VIGENTE/SUSPENDED/DEFERRED_FIRST_LIVE) e ID PF-C: **6085511432, 6085511970, 6085512485, 6085513127, 6085513826, 6085514414** (2026-10-09T17:02Z).
- Matrice 60+3 inserti: **6087228239** (2026-10-09T18:51Z).
- Sequenza MCP corrente (36 voci, PR26-a→…→PF-API commands): corpo #461 §REGOLE VIGENTI.

## Stati ambigui / contraddittori fra commenti
1. **PR28**: scheda "ombrello" con 7 PR collegate (#502, #503, #504, #512, #505→#522, #531). Ultimo stato: IMPLEMENTED_WITH_NOTE (c.6304343807) ma "manca verbale formale verifica PR28", PF-C28-01..03 NOT VERIFIED (crash drill PR34), PF-CAP-13/18/07/15 aperti, P06 per-strategia DEFERRED, G7 #532 aperto. Né MERGED né NON INIZIATO: classificata PARZIALE.
2. **PR29**: tre sotto-PR mergiate (29-a 2f25576, 29-b 409bcf9, 29-c 152613b), ma 29-f (doppio conteggio gate fondi / G7), T-F03/T-F06 con due processi reali, e il nuovo rischio `.runtime.lock` (c.6335409948) restano aperti; la copertura dei 18 figli PF-C29-01.01–.18 non è riepilogata in un unico verbale. Classificata PARZIALE/IN CORSO.
3. **PR30**: ultimo stato esplicito "non MODIFIED" (c.6278391354) dopo #526, ma accumula ~12 requisiti trasferiti (PF-EXEC-02, R1–R4, batch manager, manual_bet, customerOrderRef, D-B PAUSED, riconciliazione automatica AMBIGUOUS BLOCKS_LIVE). Nessun verbale definisce se D-B (PAUSED al riavvio, preparato come lotto locale su #512 c.6248587325) sia entrato in #512 o sia rimasto in #516–#521.
4. **PR50**: P16 (5792003414) assegnava gli orchestratori CI a PR50; risolti da #525/#527 (e694e39) fuori dalla scheda. La scheda non è stata aggiornata: PARZIALE per inferenza.
5. **PR59 dipendenza da PR58**: il corpo #461 (tabella REGOLE VIGENTI) dice "PR58 → PR59 totale → PR60; Caso B non autorizzato, punto owner aperto", ma OWN-003 (c.6068305820, 08/10) ha scelto Caso B (updater non blocca primo MCP LIVE). Il corpo non è stato riallineato.
6. **PR26-a (#498) e PR28-c (#504)**: dichiarate MERGED (c.6309349391) ma nessun commento letto riporta lo SHA di merge; PR04-quater (#490) idem (solo "#490/#492/#493 MERGIATE").
7. **PR23/PR24/PR25 identità**: rinominate il 06/10 (c.6025961736); gli addenda PF-DOC del 09/10 sono coerenti, le schede originali del 14/09 contengono i titoli precedenti.
8. **Schede fuori sequenza MCP** (PR05, PR06, PR07, PR10, PR13, PR14, PR48–PR60): requisito VIGENTE ma non nella sequenza autorizzata 07/10: lo stato "NON INIZIATO" non dice quando/se vengono attivate; PR05 esplicitamente "non è la prossima attività automatica MCP" pur avendo Phase 0 parziale e falso successo riprodotto.
9. **Drawdown/cap €10 e €1**: P37/P06/Q5 fissano €10 assoluti; la rettifica 18/10 (6249983066, 6272356144) toglie ogni cap fisso di default; 6141902809 mantiene €10 come tetto vincolante della campagna #351. Le schede PR28-d/#522 applicano la rettifica; la lettura "limite di campagna vs default prodotto" va dichiarata esplicitamente nella riconciliazione.
10. **TTL saldo LIVE**: OD-PF-FUNDS-02 (6128186028) = 10 s; PF-LIVE-FUNDS-THRESHOLD-01 (6162806642) = 30 s "chiude la parte aperta di POLICY-01". Due valori registrati, nessuna rettifica esplicita del primo.
11. **max_stake_abs assente**: t80u (6143637861) = blocco; decisione definitiva 19/10 (6272356144) = opzionale/None = non attivo. Divergenza di semantica dell'assenza.
12. **Fix loop 10 cicli**: PF-QA-AUTONOMOUS-CONTINUATION-01 (6263595478) rende il cap una soglia di diagnosi con prosecuzione autonoma; CLAUDE.md vigente e #530 descrivono "undicesimo ciclo si ferma: NEEDS_MANUAL, deroga owner per numero preciso". Testo di policy e decisione canonica divergono.
13. **Merge autonomo dei workflow**: PF-AUTONOMOUS-MERGE-20261020 (6286967995) autorizza l'agente a mergiare anche workflow CI; CLAUDE.md/AGENTS.md ancora "merge owner" (PR di allineamento prevista, non trovata mergiata nei commenti letti; #530 è stata comunque mergiata manualmente dall'owner).
14. **Order Stream e altre "NUOVE SCHEDE NECESSARIE"** (c.6043517977, 08/10): mai numerate; il denominatore "60" non le include; D03 le ignora. Da decidere se entrano nel conteggio.
15. **PF-POST-PR29C-GLOBAL-RECONCILIATION-01**: registrata PENDING — DO NOT EXECUTE (6322399988) con 4 condizioni; dopo c.6335409948 (merge #535) restano da verificare CI post-merge e checkpoint seq 58 prima dell'attivazione dichiarata "fase read-only autorizzata separatamente".

## Comment id delle fonti principali
- #461: corpo; 5664139028, 5664141515, 5664144043, 5664146928 (schede); 5932610970 (direttiva perimetro, F1–F15, X05); 6025961736; 6043517977; 6049998287/6053160870/6053161124/6053164400 (PR26-a); 6063071704 (parità OS); 6063940461; 6064163784 (PR26); 6065369529 (PR27); 6066020561/6067529638/6067971158/6068101049/6068114207/6068307679 (PR28, strategy, Caso B); 6081025215/6081036640/6084075352; 6085509692 + cards 6085511432…6085514414; 6087195988; 6087228239; 6089253292 (Gate A/B); 6124158750; 6125598240 (BF2); 6162829180 (fondi LIVE → PR29); 6167132117/6170842441 (PR-F001); 6177955933/6179223908 (#511); 6210967565; 6225689722; 6235813994/6235867968 (OD12 mapping); 6240949335/6242326211; 6250409725; 6255182700; 6265079321/6270750771/6272363335/6275842942/6278391354; 6284059630; 6288782631; 6292754293/6294043578/6294445741/6295132960/6296247545 (trasferimenti a PR30/31); 6298118212; 6300328359; 6304343807 (PR28-e); 6309349391 (stato schede 21/10); 6311697332/6313767433; 6315249711 (29-a); 6316751984/6317204134 (29-b); 6320051493 (29-b chiusa, Phase 0 29-c); 6327246613 (29-c pronta).
- #426: corpo; 5654359134, 5655799793, 5655887855, 5656005416 (P01–P12); 5791172352…5905778715 (P13–P32); 5932618681; 5934306152/5934505951/5934562905 (P33–P40, P35); 6025277738/6025960661/6026322035 (MCP); 6035270922 (P41); 6043516714; 6067971776/6068101392/6068113541/6068305820; 6084260665; 6085502813; 6087860670; 6110336882; 6112703848; 6124156631; 6125270694; 6126847863/6126895636/6127910031; 6128186028/6128717447/6129211258/6129318628/6129518925/6129715382/6130388766/6131285661/6131667643/6131708446/6131980558; 6139406637/6141136157/6141450045/6141902809/6143637861/6145958246/6146717266; 6162788177/6162806642/6162826424/6164133115; 6210870965; 6235508131/6235560755/6235676826/6235863600; 6240947984/6242324827/6243156939/6247974757/6249825590/6249927744/6249983066/6250308098/6250353189/6250405396/6250526960; 6255213376/6255410532; 6262487915/6263572358/6263595478/6265078210/6265278160/6267898378/6270750139/6272356144/6272360473/6275843696/6278392552/6284074145; 6286967995/6288782964/6298122733/6300329609/6304341575; 6322399988/6322621696/6324994877; **6335409948** (rischio .runtime.lock).

## Limiti di questo inventario
- Nessun accesso a PR GitHub/codice: gli SHA sono quelli citati nei commenti; PR #498, #504, #490 senza SHA di merge citato.
- I 130 commenti base64 (B1) non sono stati decodificati.
- Il commento 5932610970 è stato letto solo per intestazioni.
- Altri file presenti in `recon/` (cert_351.json, finding_453.json, gate_a_506.json, pr_merged.json) non sono stati prodotti né toccati da questo agente.
