# PF-UNIVERSAL-360-TESTING: roadmap del collaudo universale

Direttiva owner del 05/11/2026, requisito permanente. Ogni PR di Pickfair deve poter essere sottoposta a una campagna a 360°, avviabile senza SSH né ConnectBot. Nessuna parte di Pickfair può restare non testabile senza una motivazione tecnica documentata.

Questo documento registra lo stato **verificato** al 05/11/2026, dopo il merge della #606 (`911562ad`). Non è una certificazione: ogni riga dice cosa esiste, cosa manca e cosa blocca.

## 1. Classi di ambiente

| Classe | Cosa prova | Cosa NON prova | Stato |
|---|---|---|---|
| A. OFFLINE | codice, contratti, invarianti, mutazioni | integrazione con un exchange | operativo (CI GitHub + VPS) |
| B. SIM | runtime Pickfair con exchange simulato e denaro fittizio: ordini, matching parziale, AMBIGUOUS, crash/restart, settlement | che Betfair accetti o abbini davvero | operativo per le suite esistenti (`simulation_broker`, harness F-026) |
| C. BETFAIR READ-ONLY | API reali in lettura (mercati, quote, liquidità, saldo, connettività) tramite il gateway VPS `pf-bfgw` | qualunque operazione su ordini: place, cancel, settlement controllato | parziale: solo la suite `bf-readonly-api` nel PRE-LIVE |
| D. LIVE con denaro reale | — | — | **NON AUTORIZZATO** |

Regola: un evento che Betfair non permette di riprodurre in sola lettura (ordine, cancel, settlement) si prova in SIM, con simulatore dichiarato. Una prova SIM non viene mai presentata come operazione reale.

## 2. Cosa funziona già

| Componente | Stato verificato | Fonte |
|---|---|---|
| Catalogo versionato | 45 suite (OFFLINE 31, SIM 7, SANDBOX_EPHEMERAL 5, REPLAY_RECORDED 2), campagna `all` con 44 suite; argv fisso, risorse e timeout | `tests/catalog/pf-test-catalog.json` (#606) |
| Richiesta firmata da GitHub | `pf-test-request.yml`, solo da main, attestazione Sigstore, schema chiuso, idempotenza persistente | `scripts/pf_test_dispatch.py` (#606) |
| Stato per suite e per PR | `status`: solo check run dell'app fidata, solo lo SHA esatto, solo il catalogo vigente di main | #606 |
| Corsia development | `dev:tests/...` esegue i test nuovi della PR, mai certificativi | #606 |
| PRE-LIVE automatico per ogni head | 7 suite VPS firmate (bf-readonly-api, full-regression, idempotency-crash-restart, microstake-recovery, multi-table, reconciliation, sim-replay-microstake) + 4 suite di evidenza GitHub | `auto.py` VPS, check `pf-betfair-certification` |
| Teardown verificato | verificatore GitHub sul doppio firmato Ed25519 | `tools/teardown_evidence/` (#608) |
| Evidenze firmate | decisione Ed25519 per ogni PRE-LIVE, manifest per run | VPS |

## 3. Cosa manca

| # | Gap | Effetto | Bloccante per | Prossimo passo |
|---|---|---|---|---|
| G1 | **Il protocollo #606 non è collegato alla VPS.** certd (`fea7137a`) ha `ingestion: github` disattivato (`GITHUB_INGESTION_NOT_ENABLED_IN_T177`) e usa campi di richiesta e nomi di run diversi | nessuna suite del catalogo si avvia da GitHub; `status` non mostra PASS (fail-closed) | **tutto il resto** | kit di adattamento di certd verso `pf_test_dispatch` di main con test di compatibilità e sicurezza, in dry-run, KS intatti → **GO owner** installazione |
| G2 | **`external_id` legato al catalogo.** certd deve pubblicare `<catalog_sha256>:<idempotency_key>` | senza, ogni PASS viene ignorato | G1 | parte del kit G1 |
| G3 | **Scenari per ID e seed.** Oggi l'unità minima è la suite; non esiste `scenario:<id>@seed` | non si può riprodurre un singolo scenario | campagne mirate (F-14, F-04) | estensione dello schema del catalogo con `scenarios` e `seed`, poi del runner |
| G4 | **Baseline main vs head.** Il PRE-LIVE confronta i fallimenti con la base solo in `full-regression` | per le altre suite non c'è un diff di esito | verdetto per PR | stato «nuovo fallimento / preesistente / risolto» per suite |
| G5 | **Betfair read-only esteso.** Oggi una sola suite: mancano streaming, recovery della connessione e conti in sola lettura come suite separate | copertura C parziale | — (non bloccante per SIM) | catalogo di suite C sul gateway `pf-bfgw`, senza credenziali agli agenti |
| G6 | **Inventario Gate A vuoto.** `behaviour_coverage.json` ha 3 voci di inventario e 0 comportamenti mappati | la copertura non è dimostrabile | dichiarazione di copertura | import dell'inventario (bundle Grok verificato, riconciliazione 44↔61 fatta in locale) → mappa comportamento → suite → COVERED/PARTIAL/NOT_COVERED |
| G7 | **Campagne finanziarie composte.** Manca una campagna che concateni in un'unica esecuzione: ordine incerto → crash → restart → recupero ordini → riconciliazione → bankroll | — | collaudo F-14/F-04 a 360° | campagna `financial-recovery` sulle suite SIM esistenti (harness F-026, `recovery`, `reconciliation`) |
| G8 | **GUI Windows, Control API, MCP, packaging.** Esiste solo `packaging` OFFLINE; nessuna esecuzione su Windows né della GUI sotto Xvfb nel catalogo | non testati in automatico | — | suite dichiarate `UNSUPPORTED` con motivo finché non c'è un runner; Xvfb sulla VPS come prima estensione |
| G9 | **Stress e durata.** `stress` esiste, ma senza campagna di durata | — | — | campagna `soak` con limiti di tempo dal catalogo |
| G10 | **Altri exchange (BetFlag, SX Bet).** Nessun adapter; la #588 resta ricerca | — | — | schema del catalogo per «capacità del provider» quando ci sarà un GO separato |

Gli stati usati nei verdetti sono PASS / FAIL / NOT_TESTED / UNSUPPORTED / INFRA_ERROR. Una suite assente, non implementata o non verificabile non è mai PASS.

## 4. Sequenza proposta

Una PR alla volta, merge sempre dell'owner, LIVE OFF. Il lavoro non interrompe la corsia finanziaria (F-14 → F-16 → F-11 → …).

1. **G1 + G2:** kit certd ← `pf_test_dispatch` (fuori dal repository pubblico), test di compatibilità e sicurezza, prova E2E in dry-run. **GO owner** per installazione, revoca del NO-GO t177, uscita dal dry-run, kill switch.
2. **G7:** campagna `financial-recovery` con le suite esistenti. Solo catalogo, nessun codice nuovo di runtime.
3. **G6:** import dell'inventario Gate A e mappa di copertura.
4. **G3 + G4:** scenari per ID e seed, poi baseline main contro head.
5. **G5, G8, G9:** estensioni read-only, GUI/Windows, durata.
6. **G10:** solo dopo un GO separato sulla #588.

## 5. Obiettivo operativo

Quando G1–G4 saranno chiusi e autorizzati, la richiesta

> «Claude, testa completamente la PR F-14 su Pickfair, includendo tutti gli scenari Betfair compatibili e tutte le regressioni.»

si tradurrà in:
- una richiesta firmata `campaign:all` + `campaign:financial-recovery` sull'head esatto;
- esecuzione sulla VPS;
- check run per suite, legati a SHA e catalogo;
- evidenze firmate;
- verdetto per PR, con lo stato di copertura e i difetti riproducibili per ID e seed.
