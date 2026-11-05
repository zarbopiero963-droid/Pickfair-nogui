# G7: campagna finanziaria composta (PF-UNIVERSAL-360-TESTING)

Questo è l'elenco dei requisiti che una campagna G7 deve provare. Gli ID `G7-Rnn` sono stabili: le campagne li citano, mai il contrario.

Stato al 05/11/2026: **nessun requisito ha ancora un'evidenza certificativa**. Ogni riga è `NOT_TESTED` finché non esiste un'esecuzione su uno SHA pubblicato con un esito verificabile.

**Ruoli**
- Claude D scrive gli scenari eseguibili: harness F-026 su HeadlessApp reale, SQLite, MM e poller del settlement reali, exchange SIM, seed fissi, oracolo, dump.
- Claude B definisce i gate, verifica in modo indipendente e registra.

## Regole di evidenza

1. **Codice reale.** HeadlessApp reale; l'unico confine simulato è l'exchange SIM, dichiarato tale. Una prova SIM non è mai una prova di un ordine reale su Betfair.
2. **Ogni esito cita:**
   - lo SHA dell'albero su cui ha girato;
   - il seed;
   - il file e il nome del test;
   - il conteggio PASS/FAIL;
   - il dump dell'oracolo.
3. **Certificativo solo su uno SHA pubblicato:** l'head di una PR aperta o main. Un albero locale vale come sviluppo (`unpublished_tree`), mai come certificazione.
4. **Ogni requisito ha un test BLOCK:** un mutante che rompe il requisito deve far fallire la campagna. Va annotato quale mutante viene ucciso.
5. **Stati ammessi:** `PASS`, `FAIL`, `NOT_TESTED`, `UNSUPPORTED`, `INFRA_ERROR`. Un requisito senza esecuzione non è mai PASS.

## Requisiti

| ID | Requisito | Oracolo / prova richiesta |
|---|---|---|
| G7-R01 | Un ordine con esito ignoto (risposta persa, timeout, esito misto) diventa AMBIGUOUS e non viene mai reinviato | 1 solo `placeOrders` per chiave idempotente, anche dopo il riavvio; riga AMBIGUOUS persistita |
| G7-R02 | AMBIGUOUS non libera tavolo, riserva o chiave anti-duplicato prima della riconciliazione | riserva trattenuta = stake della richiesta; tavolo occupato; un secondo segnale identico viene rifiutato |
| G7-R03 | Gli importi non confermati (report diagnostico) non entrano mai in P&L, esposizione, riserve, cassa o bankroll | valori = exchange (`listCurrentOrders` / settlement); assenza dei valori diagnostici in ogni grandezza |
| G7-R04 | Crash in un punto finanziariamente critico (prima o dopo l'invio, prima o dopo il commit, durante l'hedge) seguito da riavvio | punto di crash scelto dal seed; dopo il riavvio la riconciliazione precede qualsiasi nuovo ordine |
| G7-R05 | Al riavvio gli ordini Betfair vengono recuperati e attribuiti per identità (`customerOrderRef` / `betId`), mai per importo | ogni ordine dell'exchange è attribuito oppure segnalato come orfano; nessun ordine orfano viene ripiazzato |
| G7-R06 | La riconciliazione chiude AMBIGUOUS solo con una prova dell'exchange (matched, cancelled, lapsed, settled) | transizione AMBIGUOUS → stato finale solo con il dato dell'exchange citato |
| G7-R07 | Il bankroll del MM resta coerente con i fondi reali dell'exchange in ogni fase | bankroll ≤ disponibile dell'exchange; valore esatto atteso per fase |
| G7-R08 | Il settlement usa solo l'abbinato e il prezzo medio confermati, al netto della commissione | realizzato = Σ abbinato × (prezzo medio − 1) × (1 − commissione), oppure −stake per BACK perso |
| G7-R09 | Abbinamento parziale + SL/TP: il residuo viene cancellato prima dell'hedge; un solo hedge, sull'abbinato confermato | ≤ 1 hedge per mercato; hedge = abbinato confermato; sotto il minimo exchange si usa il Microstake nativo |
| G7-R10 | Un hedge con risposta persa non viene mai ripetuto, né allo scatto ripetuto né dopo il riavvio | 1 solo invio dell'hedge per chiave |
| G7-R11 | Cancel con esito ignoto ⇒ saga AMBIGUOUS; la riserva resta trattenuta finché il cancel non è confermato | test F-14 e saga terminale #495 |
| G7-R12 | Fondi vecchi o non verificabili ⇒ nessun ingresso, nemmeno in auto-next (F-16) | rifiuto `FUNDS_STALE`; nessun `placeOrders` |
| G7-R13 | Multitavolo: le invarianti valgono con più tavoli contemporanei, senza contaminazione fra mercati | ≤ 1 chiusura e ≤ 1 hedge per mercato; report = oracolo; 0 ordini non riconciliati |
| G7-R14 | Riproducibilità: lo stesso seed sullo stesso SHA dà lo stesso esito | ripetizione dello stesso seed con esito e dump identici |

## Mappa delle campagne

Da compilare da parte di Claude D. Claude B verifica ogni riga.

| Campagna | SHA | Seed | Requisiti coperti | Esito | Verifica Claude B |
|---|---|---|---|---|---|
| — | — | — | — | NOT_TESTED | — |

## Come entra nel catalogo

Quando il codice che la campagna prova sarà su main (F-14, F-16, F-02/F-04/F-03), si aprirà una PR che aggiunge la campagna `financial-recovery` a `tests/catalog/pf-test-catalog.json`, con ambiente `SIM` o `SANDBOX_EPHEMERAL` e i limiti di risorse. Fino ad allora le esecuzioni restano di sviluppo.
