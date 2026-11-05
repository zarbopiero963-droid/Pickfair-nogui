# Test automation universale: richieste firmate alla VPS (PF-VPS-UNIVERSAL-TEST-AUTOMATION-01)

Riferimenti: #586, mandato owner PF-VPS-UNIVERSAL-TEST-AUTOMATION-01, che sostituisce PF-VPS-CERTIFICATION-SELF-SERVICE-01.

Codice:
- `scripts/pf_test_dispatch.py`;
- `tests/catalog/pf-test-catalog.json`;
- `tests/catalog/behaviour_coverage.json`;
- `.github/workflows/pf-test-request.yml`.

Test: `tests/scripts/test_pf_test_dispatch.py`.

Addendum PF-UNIVERSAL-TEST-AUTONOMY-365-01: due corsie, **certification** (catalogo di main) e **development**
(test nuovi scritti nella PR, §4-bis).

**Stato: dry-run.** L'esecuzione reale richiede i gate del §11 del mandato e il GO owner, più una PR a merge owner che
cambi `PF_TEST_DISPATCH_MODE` e `MODES`.

## 1. Obiettivo e confini

Claude B sceglie una suite del catalogo, oppure una campagna, per lo SHA dell'head di una PR, e ne riceve il
verdetto, senza dipendere da una sessione di Grok.

Claude B **non** ottiene:
- SSH, root o una shell remota;
- segreti, la Live App Key o il gateway transazionale;
- comandi arbitrari.

Grok resta l'amministratore della VPS e il responsabile dell'esecutore.

## 2. Architettura (modello a pull)

```
Claude B ──workflow_dispatch──▶ pf-test-request.yml (solo main)
                                 │  build: richiedente, permessi, PR, head, catalogo di main
                                 │  firma Sigstore (attest-build-provenance) + artefatto pf-test-request
                                 ▼
VPS (orchestratore esistente) ──preleva gli artefatti dei run del workflow
                                 │  gh attestation verify --signer-workflow …/pf-test-request.yml@refs/heads/main
                                 │  verify_request (stato ATTUALE della PR + catalogo di main) → Ledger.admit
                                 │  esegue in sandbox SOLO runner+args del catalogo
                                 ▼
GitHub ◀── check run Pickfair Dedicated sull'head esatto (pf-test/<id>; certificazioni: nomi storici)
Claude B ── pf_test_dispatch.py status --target … --sha …  → QUEUED…INFRA_ERROR
```

- **Nessun endpoint in ingresso sulla VPS.** La VPS legge GitHub con un token in sola lettura che resta suo.
- **Nessun segreto su GitHub.** Il workflow ha solo `contents`/`pull-requests: read` e `id-token`/`attestations: write`, e non usa nessun `secrets.*`.
- **Fiducia mai dal codice della PR.** Il workflow gira solo su `refs/heads/main` (`if:` del job). Il catalogo è quello di main, la firma è legata al workflow di main e la VPS rilegge da sé PR e catalogo.

## 3. Catalogo (`tests/catalog/pf-test-catalog.json`)

Ogni suite contiene:
- `id` stabile;
- `component`, `behaviour` e `category` (unit, integration, end-to-end, regression, property-based, mutation, chaos, crash-restart, reconciliation, performance, security, compatibility, financial-invariants, multi-table, strategy, contract, pre-live…);
- `runner` e `args` fissi;
- `environment`, `risk`, `timeout_seconds` e `resources`;
- `datasets`, `evidence`, `requires` e `criteria` (PASS, FAIL, BLOCKED).

`validate_catalog` applica queste regole:
- **Runner ammessi:**
  - `pytest`, solo con percorsi esistenti sotto `tests/`;
  - `python`, solo con gli script in allowlist (`scripts/run_mutation_guardrails.py`);
  - `orchestrator`, solo con i job `PRELIVE_CERTIFY` e `SIM_MULTITABLE_CERTIFY`, che la VPS mappa sul proprio orchestratore.
- **Argomenti:** solo caratteri `[A-Za-z0-9_./-]`. Sono vietati `..`, i percorsi assoluti, i flag e qualunque metacarattere di shell. Ogni percorso deve essere già canonico: nessun segmento `.` o vuoto, nessuna barra finale o doppia (`tests/.` e `tests/./chaos` sfuggirebbero al controllo di isolamento; rilievo di Sol su #606). Vale anche per i percorsi `dev:`.
- **Ambienti:** `OFFLINE`, `SIM`, `REPLAY_RECORDED` (book registrati: nessuna credenziale nel job), `SANDBOX_EPHEMERAL` (processi, disco e rete effimeri; mai il DB operativo). Le categorie chaos e crash-restart sono ammesse **solo** in `SANDBOX_EPHEMERAL`, qualunque sia il rischio dichiarato (correzione dopo il rilievo di Grok su #606). Inoltre una suite fuori dalla sandbox non può eseguire percorsi che il catalogo isola, né aggregandoli né prendendone una parte (`AGGREGATE_INCLUDES_ISOLATED`, rilievo di Sol su #606): per questo `full-regression` (`pytest tests`, che comprende chaos, recovery e stress) gira in `SANDBOX_EPHEMERAL`.
- **Limiti:** timeout fino a 7200 s, CPU fino a 4, memoria fino a 8192 MB. Lo schema delle suite è chiuso.
- **Completezza:** un test impone che ogni cartella di `tests/` che contiene test, e ogni modulo `tests/test_*.py`, sia raggiunta da una suite. Una funzionalità nuova senza voce nel catalogo fa fallire la CI.
- **Campagna `all`:** comprende tutte le suite tranne `full-regression`, che è ridondante.

## 4. Richiesta (`pf-test-request/1`)

**Campi:** `schema`, `target`, `suites`, `catalog_sha256`, `catalog_version`, `repo`, `pr`, `head_sha`, `base_sha`,
`requested_by`, `run_id`, `run_attempt`, `created_at`, `mode`, `idempotency_key`. Nessun altro campo è ammesso.

**`build` (workflow) rifiuta:**
- un richiedente fuori da `TRUSTED_REQUESTERS` o con permesso diverso da `admin`/`write`;
- PR chiusa, PR da fork, SHA diverso dall'head attuale, SHA malformato;
- target non presente nel catalogo;
- modalità diversa da `dry-run`;
- catalogo invalido.

Il numero di PR e l'attore sono validati **prima** di comporre qualsiasi URL.

**`verify_request` (VPS, dopo la firma) rifiuta:**
- schema non chiuso;
- repository, richiedente, modalità, PR o SHA non validi;
- `run_id` diverso da quello del certificato Sigstore;
- impronta del catalogo diversa da quella di main;
- suite che non corrispondono al target;
- chiave d'idempotenza alterata;
- richiesta più vecchia di 24 h o datata nel futuro;
- PR riletta diversa, chiusa, fork o con head cambiato.

**`Ledger.admit` (VPS):** lettura, controllo e scrittura avvengono sotto un **lock esclusivo interprocesso** (`flock` su `<registro>.lock`). Due consumer concorrenti non ammettono mai la stessa richiesta e non perdono le ammissioni l'uno dell'altro. Se il lock non è disponibile la richiesta viene rifiutata con `LEDGER_LOCK_UNAVAILABLE` (rilievo di Sol e Grok su #606). Una richiesta già ammessa (`target`, catalogo, repository, PR, SHA) viene rifiutata anche dopo un riavvio. La scrittura è atomica e durevole: `fsync` del file temporaneo, `os.replace` e poi `fsync` della directory (rilievo di Sol su #606); se la sincronizzazione fallisce l'ammissione non viene dichiarata. Un registro illeggibile produce un rifiuto: non si riparte mai da vuoto.

## 4-bis. Corsia development: test nuovi, codice non fidato

Target: `dev:tests/<percorso>[,tests/<percorso>…]`, con al massimo 20 percorsi tutti diversi.

**Percorsi ammessi:**
- solo file `.py` o cartelle sotto `tests/`;
- vietati `..`, `//`, i flag (quindi niente `-p`, cioè nessun plugin pytest), gli spazi e i metacaratteri;
- `build` verifica che ogni percorso **esista nel codice della PR allo SHA** (`contents?ref=<sha>`): un percorso assente, o un'API che non risponde, porta al rifiuto.

Il ciclo è questo:
1. scrivo il test nella PR;
2. lo chiedo sulla corsia development e ottengo RED;
3. correggo, pusho e lo richiedo, ottenendo GREEN;
4. la regressione pertinente passa sulla corsia certification;
5. aggiungo la voce al catalogo nella stessa PR: il test di completezza la impone per ogni cartella nuova;
6. dopo review e merge la suite è ufficiale.

**Sandbox di sviluppo (`DEV_SANDBOX`, lato VPS):**
- solo `pytest` sui percorsi richiesti;
- **rete assente** e **nessun segreto**;
- filesystem in sola lettura, salvo la directory di lavoro effimera;
- 2 CPU, 2048 MB, 1800 s;
- niente Docker socket e nessun privilegio host;
- se l'isolamento manca, l'esito è `BLOCKED`.

**Idempotenza:** ogni dispatch è un'esecuzione nuova, perché la chiave include il `run_id`. Si può quindi ripetere un test fallito, mentre lo stesso run non viene mai eseguito due volte. La certificazione resta una sola esecuzione per (target, catalogo, PR, SHA).

**Separazione dalla certificazione:**
- i check run hanno il nome `pf-dev-test/<chiave>`, distinto da qualunque suite;
- `dev_status` restituisce sempre `certification: false`;
- l'esito non conta per campagne, gate o certificazioni;
- una richiesta dev non può dichiararsi `certification` né portarsi dietro suite, e una richiesta di certificazione non può contenere `dev_paths`.

## 5. Stati

Gli stati sono: `QUEUED`, `RUNNING`, `PASS`, `FAIL`, `BLOCKED`, `TIMEOUT`, `CANCELLED`, `INFRA_ERROR`, oltre a `NOT_REQUESTED`.
- I check run si leggono **tutti**: `filter=all` e tutte le pagine con `page=N` esplicito (non i link `next`), fino a un massimo di 20 pagine. Oltre il limite, o con una forma inattesa, la lettura non è verificabile: mai un elenco parziale. Rilievi di Codex e Sol su #606.
- Contano solo i check run dell'app `pickfair-dedicated` sullo SHA esatto. Se un run è ancora aperto, conta quello.
- Un titolo che inizia con `INFRA_ERROR`, o una conclusione sconosciuta (anche `neutral`), dà `INFRA_ERROR`: mai `PASS` e mai `FAIL` del codice.
- Una campagna è `PASS` solo se **tutte** le sue suite sono `PASS`.

## 6. Contratto lato VPS (Grok)

Il codice di questa PR copre solo il lato GitHub e la libreria di verifica condivisa. La VPS deve:
1. prelevare gli artefatti `pf-test-request` dei run conclusi di `pf-test-request.yml`;
2. verificare la firma con `gh attestation verify --signer-workflow <repo>/.github/workflows/pf-test-request.yml@refs/heads/main --source-ref refs/heads/main --cert-oidc-issuer https://token.actions.githubusercontent.com`;
3. chiamare `verify_request` con il `run_id` del certificato, la PR riletta e il catalogo letto da main;
4. registrare la richiesta con `Ledger.admit`;
5. eseguire **solo** `runner` e `args` del catalogo in una sandbox con queste caratteristiche:
   - nessun segreto, niente Docker socket, nessun privilegio host;
   - filesystem di sola lettura, tranne la directory di lavoro effimera;
   - rete disabilitata, salvo il recorder autorizzato per `REPLAY_RECORDED`;
   - limiti di CPU, RAM, I/O e tempo presi dal catalogo;
   - se l'isolamento non è disponibile, `BLOCKED`;
6. gestire coda persistente, lock, concorrenza e recovery dopo crash con l'orchestratore esistente;
7. pubblicare i check run e le evidenze sanitizzate (log senza segreti, JUnit, decisione firmata);
8. tenere un kill switch del servizio di test, separato da quello di trading.

## 7. Matrice dei test (offline, in questa PR)

| Area | Esempi di casi negativi (rifiuto atteso) |
|---|---|
| Autorizzazione | attore estraneo; permesso `read` |
| SHA e PR | head cambiato; SHA malformato; PR chiusa; fork; PR riletta diversa |
| Catalogo | argv con `;`, `|`, flag, `..`, percorsi non canonici (`tests/.`, `tests/./chaos`, barra finale), percorsi assoluti, segreti host; runner `bash`; script non in allowlist; job orchestratore `LIVE_ORDER`; ambiente `PRODUCTION`; timeout e risorse oltre i limiti; campi extra; id duplicati; campagna con suite inesistenti; chaos in `OFFLINE` |
| Richiesta | target non registrato o con metacaratteri; modalità diversa da dry-run; campi extra (`command`); suite o target alterati dopo la firma; artefatto di un altro run; catalogo diverso da main; scaduta o nel futuro |
| Idempotenza e crash | duplicato dopo riavvio; registro corrotto; crash durante la scrittura (registro precedente intatto, nessun file temporaneo) |
| Stati | check run di un'altra app o di un altro SHA; `INFRA_ERROR` con conclusione success; conclusione sconosciuta; run aperto con id più basso; campagna con uno stato non PASS |
| Workflow | solo `workflow_dispatch`, solo main; permessi minimi; nessun `${{ }}` negli script; nessun `secrets.*`; nessun checkout del codice della PR |

| Corsia development | percorsi fuori da `tests/`, `..`, assoluti, flag o plugin, metacaratteri, duplicati, più di 20; percorso assente allo SHA o verifica non disponibile; dev travestita da certificazione; certificazione con `dev_paths`; esito dev usato per una suite; replay dello stesso run |

**Prove:** 114 test; 28 mutazioni sui controlli di sicurezza, tutte uccise.

## 8. Limiti residui (NOT_RUN o da fare)

- **Lato VPS non implementato qui:** esecutore, sandbox (anche `DEV_SANDBOX`), coda, kill switch e pubblicazione degli esiti sono di Grok. Sono quindi NOT_RUN la prova E2E richiesta → esecuzione → evidenza e il test di accettazione dell'addendum §7 (test nuovo, RED, correzione, GREEN, catalogo, certificazione ufficiale). Va eseguita dopo il merge, quando la VPS preleva le richieste.
- **Inventario Gate A (1.062 comportamenti):** non è nel repository. `tests/catalog/behaviour_coverage.json` resta `NOT_IMPORTED` finché non viene importato dalla sua fonte, senza inventare voci.
- **`TRUSTED_REQUESTERS`:** è l'account owner. Le sessioni Claude B agiscono con quell'identità GitHub, quindi il controllo non distingue Claude B dall'owner. L'autorizzazione è comunque limitata a quello che il catalogo consente.
- **Nomi dei check:** il nome `pf-sim-multitable-certification` e il prefisso `pf-test/` vanno confermati da Grok.

## 9. Rollback

- **Lato GitHub:** disattivare il workflow `PF test request` dalla UI, oppure rimuovere il file. La VPS non trova più richieste nuove e nient'altro dipende da questo workflow.
- **Lato VPS:** il kill switch del servizio di test ferma il prelievo. Le richieste nel ledger restano registrate e non vengono rieseguite.
