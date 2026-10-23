# CI senza duplicati, check `full-suite` e copertura attestata delle review (PFC-002)

Roadmap #461, scheda **PFC-002**: M156 (deduplica CI), M130 (raccolta/workflow e
policy reviewer), M159 (copertura review completa e attestata). La PR modifica
workflow e `scripts/pr_*`: **merge manuale dell'owner**. Non tocca codice di
trading, money management, Betfair o configurazione runtime.

## 1. Cosa non andava (misurato su #535, #537 e #538)

- **La suite completa girava 17 volte per ogni push**, sempre sullo stesso albero
  `refs/pull/N/merge`:
  - 10 volte come backstop di `_module-ultra-check` nelle lane chiamate da
    `ci-master-gate`;
  - 3 volte con `ci-dynamic-intelligent` (annidato nel master e da solo);
  - 3 volte con `merge-simulation-hard` (da solo e nei due orchestratori);
  - 1 volta in `pr-check`.
- **Una ventina di workflow partivano due volte**, una su `push` e una su
  `pull_request`: il `push` non aveva filtro di branch e scattava anche sul
  branch della PR.
- **I run degli SHA superati non si cancellavano**: 162 minuti-job persi solo
  su #535.
- **Nessun check verificava le proprie condizioni.**
  - `pytest -q` verde significava solo exit code 0: non distingueva una raccolta
    parziale, test deselezionati o test raccolti e mai eseguiti.
  - Merge Readiness accettava `SKIPPED` come OK per ogni check, inclusi quelli
    che contano. Un job saltato da `if:` diventa `SKIPPED`, e GitHub lo conta
    come required soddisfatto.
- **Le review di Sol e Grok troncavano il diff in silenzio.**
  - Ogni file oltre 7000 caratteri (Sol) o 5000 (Grok) veniva tagliato.
  - Oltre 24 000 o 18 000 caratteri di prompt i file venivano saltati per intero.
  - Il job finiva comunque verde. Un segreto mancante o un errore del provider
    davano a loro volta un job verde.
  - Su #538 tre file sono rimasti parzialmente senza review indipendente. È
    servita l'eccezione owner PF-PFC001-REVIEW-EXCEPTION-01.

## 2. Topologia dopo la PFC-002

| Esecuzione | Prima (per push) | Dopo |
|---|---|---|
| Suite completa (`pytest` dell'intera raccolta) | 17 | **1**, nel check `full-suite` |
| `mutation-guardrails` | fino a 3 (master, master→dynamic, dynamic) | **1** (`ci-master-gate`) |
| `ci-dynamic-intelligent` | 2 (da solo e annidato) | **1** (da solo, solo `detect`: diagnostica) |
| Lane `live-gate` | 2 su una PR che tocca i suoi file (path diretti + `ci-master-gate`) | **1** (path diretti; il suo test gira comunque in `full-suite`) |
| Lane `dutching` / `pnl-engine` | 2 su una PR che tocca i loro file (path diretti + detector) | **1** (path diretti: per `dutching` allargati a `controllers/dutching_controller.py` e `dutching_state.py`; per `pnl-engine` già comprensivi dei path del detector. `test_i_path_del_detector_restano_coperti_dalla_lane_diretta` lo vincola) |
| `merge-simulation-hard` | 3, ognuno con la suite completa | **1**, senza pytest: merge con il main più recente, marker, JSON dei guardrail |
| Workflow con `push` + `pull_request` | 2 run per push sul branch della PR | 1 (`push` solo su `main`) |
| Run di SHA superati su una PR | proseguivano fino alla fine | cancellati (`full-suite`, `ci-master-gate`, `ci-dynamic-intelligent`, `merge-simulation-hard`) |

Le lane di modulo (`_module-ultra-check`) mantengono:
- esistenza del modulo e dei file guardrail;
- validazione JSON;
- `guardrail_runner.py`;
- test mirati.

Perde solo il backstop. I test mirati non si perdono: girano anche dentro
`full-suite`.

**Su `main` non si cancella mai nulla.** Ogni merge ha la sua suite completa:
`full-suite` gira su `push` a `main` con un gruppo di concorrenza per SHA, perché
GitHub scarterebbe un run in attesa nello stesso gruppo anche con
`cancel-in-progress: false`.

**Equivalenza della copertura.** Su `pull_request` tutti i workflow fanno
checkout di `refs/pull/N/merge`, cioè del merge della PR con la base. Le 17
esecuzioni erano quindi la stessa suite sullo stesso albero, e una basta.

L'unica differenza era `merge-simulation-hard`, che rifaceva il merge con il
main più recente e poi eseguiva pytest. Ora quel merge lo fa `full-suite` stessa.
Su `pull_request` parte dall'**head della PR**, non da `refs/pull/N/merge`, e
fonde la base corrente della PR, di norma `main` (step *Merge the current base
branch into the PR head*; il nome del branch arriva dall'evento come variabile
`PR_BASE_REF`, mai interpolato nello script). Un
conflitto rende il check rosso. Le dipendenze si installano **dopo** il merge,
così valgono quelle dell'albero provato. La suite prova quindi l'albero che il
merge produrrà davvero, anche se main si è mosso dopo l'evento (rilievi Grok 4.7
e GPT-6.1 Sol su #539). `merge-simulation-hard` resta per conflitti, marker e JSON dei guardrail.

**Effetto collaterale dichiarato.** Alcuni workflow avevano filtri `paths` sul
`push` più larghi di quelli sul `pull_request`. Per esempio
`reconciliation-engine` scattava anche su `tests/reconciliation/*`. Una PR che
tocca solo quei test non avvia più quella lane. I test girano comunque, e una
volta sola, in `full-suite`.

## 3. Il check `full-suite`

File: `.github/workflows/full-suite.yml`. Job unico, nome stabile
`full-suite`, adatto a un required status check:

- parte su ogni `pull_request` (opened, synchronize, reopened,
  ready_for_review, edited) **senza filtri di path o branch**, e su `push` a
  `main`. `edited` copre il **cambio di base**: l'head resta lo stesso, quindi
  senza un nuovo run il verde calcolato sulla base vecchia resterebbe attaccato
  allo SHA (rilievo Codex su #539). Gira anche su titolo o descrizione
  modificati: un run in più, mai un verde non verificato. Per la stessa ragione
  `edited` è anche fra i trigger dei due revisori e di Merge Readiness;
- **nessun `if:`** sul job e **nessun `workflow_dispatch`**: un dispatch sul
  branch della PR farebbe girare la versione del file presente nella PR e
  attaccherebbe un verde al suo head;
- esegue `python scripts/ci/full_suite_gate.py run`, che lancia pytest con il
  plugin `scripts/ci/full_suite_plugin.py` e **giudica il report**, non
  l'exit code.

Il gate fallisce se una qualsiasi di queste condizioni non regge:

| Condizione | Contratto |
|---|---|
| exit code di pytest | 0 |
| errori di raccolta | 0 |
| test deselezionati | 0, e nessun argomento fra `-k`, `-m`, `--deselect`, `-x`, `--lf`, `--sw`… |
| test raccolti | almeno `min_collected` (8916, baseline CI di #538) |
| test raccolti senza esito | 0 |
| `failed` / `error` / `xpassed` | 0 |
| `skipped` | al massimo `max_skipped` (15) |
| `xfailed` | al massimo `max_xfailed` (5) |

Il contratto sta in `scripts/ci/full_suite_contract.json`. Abbassare una soglia
è una modifica visibile nel diff e, come tutto `scripts/ci`, passa dall'owner.
Prima di lanciare pytest il gate cancella il report precedente, così un report
vecchio non può valere per il run nuovo.

L'evidenza `full-suite-evidence.json` contiene:
- SHA del run e head della PR, più il main fuso e l'albero effettivamente provato;
- evento, run e tentativo;
- conteggi, durata ed esito.

Si carica come artifact `full-suite-evidence-<sha>-a<attempt>` ed è riportata
nel riepilogo del job.

La suite gira ancora in serie: xdist entra con PFC-003. Il marker `serial` di
PFC-001 resta pronto per quel passo.

## 4. Copertura attestata delle review (M159)

Modulo: `scripts/ci/review_coverage.py`, senza dipendenze esterne. Lo usano i
workflow di Sol e Grok.

### 4.1 Segmentazione deterministica, niente troncamento

- `build_patch_payload` non taglia più nulla: redige i segreti, neutralizza i
  fence e passa i file a `plan_review`.
- Ogni patch si divide ai confini degli hunk (`@@`). Un hunk più lungo del tetto
  si divide per righe, una riga più lunga per caratteri. La concatenazione dei
  segmenti è il patch originale, e ogni segmento ha id (`F003S02`) e sha256.
- I segmenti si impacchettano in **lotti** entro `MAX_TOTAL_PATCH_CHARS`, nel
  testo esatto che finisce nel prompt, e con al massimo 40 segmenti per lotto:
  con molti file piccoli il riscontro non starebbe nella risposta corta chiesta
  al modello (rilievo Codex su #539). Un lotto corrisponde a una chiamata al
  modello.
- I lotti girano **tutti insieme**, uno per worker, quindi il job dura quanto una
  chiamata e il timeout del job resta valido. Con meno worker che lotti le
  chiamate andrebbero in più ondate (rilievo Codex su #539).
- `MAX_PATCH_PER_FILE_CHARS` (7000 Sol / 5000 Grok) diventa la dimensione del
  segmento. `MAX_TOTAL_PATCH_CHARS` (24 000 / 18 000) diventa quella del lotto.
- **Tetto di spesa:** `MAX_REVIEW_CALLS` (8). Se un range richiede più lotti, non
  si chiama nessun modello e l'attestazione è incompleta: non si paga una review
  che resterebbe incompleta. Le strade sono dividere la PR o alzare il tetto, ed
  è una decisione owner.

### 4.2 Riscontro del modello

Il prompt di ogni lotto elenca i propri segmenti e chiede come ultima riga
`SEGMENTI ESAMINATI: <id>, …`. Un lotto conta come revisionato solo se:
- la risposta è completa, cioè non troncata (`finish_reason=length`), non vuota,
  non un errore del provider;
- l'ultima riga di riscontro cita **tutti** i suoi segmenti.

La riga di riscontro non conta nel limite di 150 parole. Vale solo l'**ultima
riga non vuota** della risposta. Un riscontro seguito da un
rifiuto, da un errore o da altro testo non chiude la review (rilievo Codex su
#539).

**Un solo commento leggibile dal gate.** Con più lotti, il commento pubblica una
sola coppia `## Bloccanti` / `## Verdetto finale` (`aggregate_reviews`).
- I bloccanti di tutti i lotti sono etichettati `[lotto i/n]`.
- Un lotto senza una sezione Bloccanti unica, o senza un Verdetto finale unico e
  non vuoto, conta esso stesso come bloccante (rilievo GPT-6.1 Sol su #539). La
  riga di riscontro non conta come contenuto di nessuna delle due sezioni.
- Il testo integrale dei lotti resta nel dettaglio, con i titoli abbassati a
  `####`.

Il lettore delle review del gate (`pr_autonomy_policy._section`) tratta le
sezioni duplicate come UNKNOWN (rilievo Codex su #539).

Un commento oltre 64 000 caratteri verrebbe tagliato: la copertura risulta
incompleta e il `done_marker` non viene scritto.

Anche un range **senza righe di testo cambiate** (soli rename, file venuti dal
branch base) pubblica le due sezioni canoniche: «Nessun bloccante evidente» se
la copertura è completa, l'elenco dei motivi se non lo è (tetto di lotti). Prima
il commento portava il `done_marker` senza sezioni e il gate lo leggeva UNKNOWN
(rilievo Codex su #539).

**Un bloccante rende rosso il check, e resta aperto finché l'owner non lo
chiude.** I bloccanti aggregati entrano nell'attestazione (`blocking_findings`).
Prima bastava la copertura completa, e Merge Readiness leggeva solo il SUCCESS
del check anche con `## Bloccanti` piena (rilievo Codex su #539).

Decisione owner D1–D4 su #539, dopo un audit indipendente che aveva riprodotto
due falsi verdi:
- **D1:** il rerun di un run con un lotto troncato cancellava il bloccante
  trovato da un altro lotto;
- **D2:** un commit vuoto o un merge di main dopo un head con bloccanti dava una
  review «pulita» e il check verde.

Regole in vigore (`review_coverage.py`: `attestazioni_con_bloccanti`,
`bloccanti_aperti`, `triage_dell_owner`):
1. La verifica raccoglie i bloccanti di **tutte** le attestazioni ufficiali
   della PR e del revisore: ogni range, ogni tentativo, anche quelle
   **incomplete** e quelle sui commit tolti da un force-push (il run è della PR
   per `pull_requests` o per il branch). Un bloccante resta aperto attraverso
   rerun, commit vuoti, merge di main, modifiche di titolo/descrizione, cambio
   di base e modifiche non pertinenti.

   **PR storiche collegate** (decisioni owner sul terzo audit e deroga 11/11 su
   #539, `pr_collegate`). Contano anche i bloccanti delle PR **non mergiate**
   collegate a questa, in modo transitivo (al massimo 10, oltre: rosso):
   - per **branch** (`/pulls?state=closed&head=<owner>:<branch>`): nuova PR
     sullo stesso branch, anche dopo force-push e con commit riscritti da un
     rebase;
   - per **commit in comune** (`/commits/<sha>/pulls`, che elenca anche le PR
     chiuse): nuova PR su un branch diverso che riusa almeno un commit.

   Di ogni PR collegata si leggono i run del **suo** branch solo nella
   finestra in cui era aperta (`created=<apertura>..<chiusura>`): cosi' si
   ritrovano anche i run sui commit tolti dal force-push, e un branch riusato
   in un'altra epoca non e' letto (ne' riempie il tetto di pagine). Contano le
   attestazioni con il numero di una PR collegata o che terminano su un commit
   di questa. Le PR mergiate non trasmettono bloccanti (sono passate dal merge
   owner). Il triage dell'owner vale anche se scritto sulla PR storica che ha
   trovato il bloccante.

   | Scenario | Esito | Test |
   |---|---|---|
   | PR bloccata, force-push, chiusura, nuova PR sullo stesso branch (stessi commit o rebase) | rosso | `test_s1_*` |
   | Stessa sequenza su un branch diverso con un commit in comune | rosso | `test_s2_*` |
   | Cambio di base | rosso | `test_s4_*` |
   | Rerun con attestazione incompleta | rosso | `test_d1_*` (unità ed e2e) |
   | Bloccante precedente ancora aperto (commit vuoto, merge di main) | rosso | `test_d2_*` |
   | Falso positivo con triage owner, anche sulla PR storica | verde; triage da app: rosso | `test_s7_*`, `test_triage_*` |
   | Nuova PR indipendente (altro branch, nessun commit in comune) | verde, nessuna PR collegata | `test_s8_*` |
   | PR storica mergiata sullo stesso branch, run di un'altra epoca, branch riusato a lungo | verde | `test_pr_storica_mergiata_*`, `test_run_della_pr_storica_fuori_*`, `test_branch_riusato_*` |

   **Scenario NON coperto, per costruzione.** Una PR nuova su un branch
   **diverso** con commit **tutti riscritti** (cherry-pick, riscrittura) non ha
   alcun legame verificabile con la PR precedente: per il codice è
   indistinguibile da una PR indipendente (scenario `s8`), che non deve
   ereditare niente. Distinguerle richiederebbe un sistema strutturalmente
   diverso (un registro dei bloccanti di repository, o un confronto del
   contenuto), fuori da questa PR. Riceve comunque la review **completa** dal
   branch base, che può non ripetere il bloccante.

   **Divieto di governance.** L'agente non chiude, ricrea o riapre una PR, né
   riscrive i commit su un altro branch, per azzerare i bloccanti di una
   review. È l'unica copertura dello scenario qui sopra, e va riportata anche
   in AGENTS.md (file dell'owner, modifica a suo carico).
2. Cio' che impedisce di leggere tutte le attestazioni è rosso, non uno scarto
   silenzioso:
   - un **run della PR ancora in corso** (anche legato alla PR solo dal
     branch, dopo un force-push) si aspetta (fino a `--wait-seconds`) anche
     quando la catena è già completa, e se è ancora in corso il check è
     rosso. L'attestazione di questo stesso run conta per i bloccanti anche se
     incompleta e anche se il suo artifact non è ancora elencato. Un run
     soltanto **accodato sullo stesso head** non si aspetta: è nella stessa
     concurrency, parte dopo e vedrà tutto (quarto audit su #539); Con un range dal base (force-push, reopen) la catena si chiude con
     la sola attestazione locale, e i bloccanti del run in corso andavano persi
     (secondo audit su #539);
   - un artifact scaduto o illeggibile diventa un bloccante con il suo id,
     triagiabile dall'owner (senza id la PR resterebbe rossa per sempre);
   - uno storico dei run oltre 10 pagine dalla creazione della PR è rosso senza
     id: rimedio, una PR nuova.
3. **Nessuna chiusura automatica.** Una review successiva pulita che non cita
   il bloccante non prova che sia stato corretto, e il modello non è un
   verificatore dimostrabile offline. Per decisione owner, senza una chiusura
   automatica dimostrabile vale la regola fail-closed.
4. **Si chiude solo con il triage dell'owner**, per id. Ogni bloccante ha un
   id di 12 caratteri (`blocker_id`: sha256 di revisore, range e testo, con
   l'etichetta del lotto), stampato nel motivo del check rosso («bloccante
   aperto `<id>`»). L'id resta lo stesso fra i rerun dello stesso range (il
   piano dei lotti è deterministico), e cambia da un range all'altro e da un
   lotto all'altro: un triage non copre le ripetizioni dello stesso testo in
   altri push o in altri lotti, compreso il bloccante sintetico «lotto non
   leggibile» (secondo e quarto audit su #539). Un esito non registrato e un artifact
   illeggibile hanno anch'essi un id. Il marcatore «lotto non leggibile» vale
   solo per una risposta **completa** ma malformata: un errore del provider o
   un troncamento rendono già incompleta la copertura, e non lasciano un
   bloccante da triagiare dopo il rerun (terzo audit su #539). I bloccanti reali
   estratti da un testo parziale restano. Un solo commento può triagiare più
   bloccanti, con una motivazione comune. L'owner pubblica sulla PR, **dalla UI di GitHub**, un
   commento con il marker e una motivazione di almeno 20 caratteri:

   ```
   <!-- pfc-triage-bloccante:gpt56sol:<id> -->
   Corretto in <sha>: <cosa e perche'>   (oppure: falso positivo, perche' ...)
   ```

   Il revisore è `gpt56sol` o `grok46`. Poi si rilancia il job del revisore:
   la dedup non ripaga la review, la verifica rilegge i triage.
5. **L'agente non può assolversi da solo.** Vale solo un commento:
   - dell'owner (login e `author_association` OWNER);
   - **senza** `performed_via_github_app`. L'agente scrive con l'account
     dell'owner ma sempre tramite l'app Claude (verificato sui commenti di
     #539), quindi i suoi commenti non contano;
   - **mai modificato**: un'app potrebbe modificare un commento dell'owner
     senza cambiarne l'autore. Per correggere un triage se ne pubblica uno
     nuovo.

   Un revisore indipendente abilitato diverso dall'owner non c'è ancora: oggi
   il triage è solo dell'owner.

**Rischi residui della persistenza** (secondo audit indipendente su #539), non
chiusi dal codice:
- **Cancellazione di prove.** Chi ha `actions: write` sull'account dell'owner
  (quindi anche l'agente) può cancellare un artifact o un run via API: il
  bloccante sparisce senza lasciare traccia, e dopo un force-push la catena si
  ricostruisce. Lo vieta la policy dell'agente (azione distruttiva); il codice
  non lo può rilevare.
- **Token diversi dall'app.** `performed_via_github_app` è valorizzato per le
  GitHub App. Un token personale o di una OAuth App (per esempio `gh auth
  login`) sull'account dell'owner pubblicherebbe un commento senza app, che
  varrebbe come triage. La garanzia regge finché l'agente usa solo l'app
  Claude: non va dato all'agente un token personale dell'owner.
- **Tentativi precedenti di un run.** I test assumono che
  `/runs/{id}/artifacts` elenchi gli artifact di tutti i tentativi (i nomi
  portano `-a<tentativo>` per questo). Va misurato sulla prima PR dopo il
  merge.
- **Rerun di un run vecchio dopo il verde.** Se un run di un push precedente
  viene rilanciato dopo che il check del head è già verde e trova un
  bloccante, il check del head non si ricalcola da solo: lo ricalcola il push
  o il rerun successivo.
- **Attestazioni della versione precedente** (senza il campo dei bloccanti):
  non bloccano, sono annotate nelle `notes`. Riguarda solo le PR aperte a
  cavallo del merge.
- **Costo per l'owner.** Ogni bloccante, anche corretto, chiede un triage; una
  riformulazione del modello a ogni push genera un id nuovo.
- **Rossi senza id** (probabilità bassa): «storico dei run oltre 10 pagine» e
  «oltre 100 run sul commit» non si possono triagiare; rimedio, una PR nuova
  con commit nuovi (review completa).
- **Owner = login del proprietario del repository.** Vale per questo
  repository personale; in un repository di un'organizzazione nessun triage
  passerebbe e andrebbe definito chi lo autorizza.
- **Da misurare su GitHub reale:** se `updated_at` di un commento può cambiare
  senza modifica del testo (invaliderebbe il triage: fail-closed), e se un
  check rosso di un run superato resta nel riepilogo dei check accanto al
  verde dello stesso nome.

**`edited` senza cambio di base** (titolo, descrizione): nessuna chiamata al
modello e nessuna attestazione nuova; la verifica legge quelle già caricate. Con
il cambio di base (`changes.base.ref.from` presente) si rivede l'intera PR
contro la base nuova: le attestazioni vecchie partono da commit che non sono
della base nuova e non fanno catena.

### 4.3 Attestazione

Per ogni range il workflow scrive un JSON, schema `pickfair-review-coverage/v1`,
con:
- revisore, modello, PR e range;
- run, tentativo e file workflow;
- segmenti con sha256, lotti ed esito per lotto;
- segmenti revisionati e non revisionati;
- file binari, file rinominati senza modifiche, file di testo senza patch;
- `complete` e i motivi di incompletezza;
- `blocking_findings`: i bloccanti della review aggregata (lista vuota = pulita).

Il JSON si carica come artifact del run
(`review-coverage-<reviewer>-<base12>-<head12>-a<attempt>`). Un artifact non si
modifica: si può solo cancellare, e allora manca, quindi fail-closed.

Rendono l'attestazione **incompleta**:
- un lotto non revisionato;
- un file di testo senza patch (GitHub non lo fornisce oltre una certa
  dimensione);
- un file **binario**: un eseguibile, un database o un archivio non li ha visti
  nessuno, e AGENTS.md vieta i binari salvo richiesta. Il motivo lo nomina, così
  l'owner sa cosa decidere (rilievo Codex su #539);
- la Compare API al tetto di 300 file;
- il tetto di lotti superato;
- il **commento della review non pubblicato** (403, 5xx): una review che nessuno
  può leggere non copre niente (rilievo Codex su #539).

Restano neutri solo i rename senza modifiche. Il `done_marker` del commento, che
evita di ripagare lo stesso range, si scrive **solo** con attestazione completa.

La dedup salta la review solo se esiste anche l'**artifact ufficiale** del range.
Con il marker presente ma l'artifact mancante (upload fallito, scaduto,
cancellato) la review si rifà: altrimenti il check resterebbe rosso per sempre su
quel range (rilievo Codex su #539).

### 4.4 Verifica dell'intera PR: il verdetto del check

L'ultimo step del job (`Verify review coverage of the whole PR`, `if: always()`)
esegue `review_coverage.py verify`. Il check del revisore è verde **solo se**
le attestazioni complete formano una **catena di range** che parte da un commit
del branch base e arriva al head corrente, **e** nessuna attestazione della PR
ha un bloccante non triagiato dall'owner (§4.2), **e** la review sul head ha
registrato il proprio esito (`blocking_findings` assente = rosso). Le
attestazioni di una versione precedente del workflow, senza il campo, non
bloccano ma sono annotate nelle `notes` della verifica.

Ogni riga del diff finale è stata scritta da una push, e quella push è un
anello della catena.

Contano solo le attestazioni prodotte dai **run ufficiali**:
- il file workflow del revisore;
- evento `pull_request_target`, quindi codice del branch base e mai della PR;
- run associato alla PR: head del run fra i commit della PR, oppure la PR fra le
  `pull_requests` del run;
- artifact che nomina lo stesso run e un head che è commit della PR.

**Push ravvicinati.** I run per SHA possono sovrapporsi. Se la catena non è
ancora completa e ci sono run ufficiali della stessa PR ancora in corso, la
verifica aspetta fino a 10 minuti (`--wait-seconds 600`) e rilegge. Scaduta
l'attesa vale lo stato reale. Per questo i timeout dei job salgono a 108 minuti
(Sol) e 45 (Grok).

Sono quindi scartati:
- un workflow aggiunto dalla PR;
- un evento `pull_request`;
- un artifact con lo stesso nome caricato da un altro run;
- un commento forgiato.

Il modulo del verificatore arriva da un checkout **sparse del branch base**
(`persist-credentials: false`): la PR non può cambiare né come si segmenta né
come si verifica.

Il job è rosso, senza alcuna chiamata al modello, nei casi seguenti:
- PR in draft;
- PR da fork;
- segreto mancante;
- errore delle API GitHub.

Il job non ha più un `if:` a livello di job, perché uno skip varrebbe come
required soddisfatto.

**Rimedi quando la catena è rotta.**
- *Push senza review completa* (per esempio provider in timeout): si rilancia
  quel run (re-run del job). Il nuovo tentativo produce l'attestazione per lo
  stesso range, e la dedup evita di ripagare i range già completi.
- *Force-push o rebase*: il workflow lo riconosce da sé. Se il `before` del push
  non è antenato del nuovo head (Compare API `status` diverso da
  `ahead`/`identical`), rivede l'intera PR `base...head` invece del range della
  push (rilievo Codex su #539).
- *Artifact scaduti*: la conservazione è di 90 giorni. Riaprire la PR produce
  una review completa nuova.

**Su questa stessa PR** i workflow `pull_request_target` girano dal branch base,
cioè con la versione **precedente**. La nuova copertura si applica dalle PR
successive al merge. La PFC-002 stessa è revisionata con la vecchia logica, e
il gate ne dichiara la copertura effettiva.

## 5. Merge Readiness: check obbligatori

`scripts/ci/required_checks.json` elenca i check che devono essere
**SUCCESS sull'head**:

- `full-suite`
- `mutation-guardrails / mutation`
- `GPT-6.1 Sol push-range review`
- `Grok 4.7 push-range review`

Come li giudica `scripts/pr_flow_automation.py`:
- **assente** → pending, quindi il gate aspetta, e a budget scaduto è rosso;
- `SKIPPED`/`NEUTRAL` → bloccante;
- `FAILURE`/`CANCELLED`/`TIMED_OUT` → bloccante, come prima;
- vale anche per `report`, che può annunciare «Merge allowed», e non solo per
  il gate CI (rilievo Codex su #539);
- SHA diverso dall'head (quando il rollup lo riporta) → bloccante;
- due esecuzioni con lo stesso nome valgono solo se sono **entrambe** SUCCESS;
- manifest illeggibile o vuoto → gate rosso.

Per i check non obbligatori `SKIPPED` resta accettabile: le lane di modulo si
saltano da sole quando il diff non le riguarda.

Restano invariati: thread irrisolti, `mergeable`, `mergeStateStatus`, draft,
nessun check osservato e stabilità del rollup.

## 6. Enforcement GitHub: stato e azioni owner

**Stato verificato il 23/10/2026** (API pubblica, ruleset 25459867
`PICKFAIR-MAIN-PROTECTION`):
- `deletion`, `non_fast_forward`, PR obbligatoria con 0 approvazioni, thread
  risolti e solo squash;
- **nessuna** regola `required_status_checks`;
- `bypass_actors` vuoto.

Finché il ruleset non richiede i check, **nessun check blocca davvero il
merge**: lo blocca solo la disciplina owner/agente. Quindi:

> **IMPLEMENTATION READY / ENFORCEMENT PENDING OWNER.** Il merge automatico resta
> vietato.

**Azione owner, dopo il merge della PFC-002** (D4: prima i check nuovi non
esistono su `main`, e il check di Sol su `main` non ha ancora lo step di
verifica, quindi oggi è verde senza nessuna attestazione). Prima di attivarlo,
verificare su una PR di prova che i nuovi check girino e che un check mancante
o fallito blocchi:

1. *Settings → Rules → Rulesets → PICKFAIR-MAIN-PROTECTION → Add rule →
   Require status checks to pass*.
2. Aggiungi questi check, ognuno con sorgente **GitHub Actions**: così un
   commit status scritto con un altro token non lo soddisfa.
   - `full-suite`
   - `mutation-guardrails / mutation`
   - `GPT-6.1 Sol push-range review`
   - `Grok 4.7 push-range review`
   - `Merge readiness`
3. Spunta **Require branches to be up to date before merging**: i check devono
   riguardare il merge con il main attuale. Con una PR alla volta il costo è
   nullo.
4. Lascia vuoti i **bypass actors**.

Equivalente via API (token owner con permesso Administration):
`PUT /repos/zarbopiero963-droid/Pickfair-nogui/rulesets/25459867` con le regole
attuali più:

```json
{"type": "required_status_checks", "parameters": {
  "strict_required_status_checks_policy": true,
  "do_not_enforce_on_create": false,
  "required_status_checks": [
    {"context": "full-suite", "integration_id": 15368},
    {"context": "mutation-guardrails / mutation", "integration_id": 15368},
    {"context": "GPT-6.1 Sol push-range review", "integration_id": 15368},
    {"context": "Grok 4.7 push-range review", "integration_id": 15368},
    {"context": "Merge readiness", "integration_id": 15368}]}}
```

`15368` è l'id della GitHub App «GitHub Actions».

**Limiti che il ruleset da solo NON chiude.** Vanno dichiarati: a causa loro il
merge automatico resta vietato anche dopo l'azione qui sopra.

1. **Una PR può modificare i propri check `pull_request`.** `full-suite`,
   `ci-master-gate` (mutation) e Merge Readiness girano dalla versione della
   PR. Una PR che li indebolisce fa girare su di sé il check già indebolito.
   - Un ruleset di repository personale non può fissare il file del workflow:
     *Require workflows* esiste solo a livello di organizzazione.
   - Mitigazione in vigore: ogni modifica a `.github/workflows/`,
     `scripts/pr_*` e (da questa PR) tutto `scripts/ci/` è
     `MANUAL_AUTHORITY_PATTERNS`, quindi `manual-review-required` e merge
     manuale owner.
   - Su main solo `scripts/ci/check_ci_quarantine.py` lo era.
     `full_suite_gate.py`, `review_coverage.py`, il contratto e il manifest
     sono entrati in `OWNER_MANUAL_MERGE_FILES` e nei pattern, nella copia
     identica di Sol e Grok.
   - L'elenco in prosa di CLAUDE.md (file dell'owner) non è toccato:
     aggiornarlo è un'azione owner.
   - I check dei reviewer invece girano da `pull_request_target`, cioè dalla
     base, e la PR non li può cambiare.
2. **Il nome di un check non identifica il workflow** (D3, rischio tracciato).
   - Neutralizzato in parte: Merge Readiness conta un check obbligatorio solo
     se è un **CheckRun**. Uno status commit con lo stesso nome (basta un token
     con `statuses: write`) o una voce senza tipo non lo sostituiscono, e se ci
     sono bloccano.
   - Resta aperto: un altro job chiamato `full-suite` in un workflow aggiunto
     dalla PR è anch'esso un CheckRun di GitHub Actions. Merge Readiness esige
     che **tutte** le esecuzioni con quel nome siano SUCCESS, ma non può
     impedire una copia verde se l'originale viene rimosso nello stesso diff.
     Il rollup non dice da quale workflow viene un CheckRun, e il ruleset fissa
     solo l'app (`integration_id` 15368). Lo coprono il merge owner sui workflow
     e `manual-review-required`.
3. **Identità dell'agente.** L'agente lavora con l'account dell'owner. GitHub
   non permette di approvare la propria PR, quindi «review del code owner
   obbligatoria» bloccherebbe ogni PR, senza aggiungere un controllo
   indipendente. Un enforcement indipendente completo richiede un'identità
   separata per l'agente (GitHub App o utente macchina). Allora l'owner può
   diventare reviewer obbligatorio per `.github/workflows/**`, `scripts/pr_*`,
   `scripts/ci/**` e i file di policy. È una decisione owner (scheda
   «identità dell'agente» della roadmap).

## 7. Tempi CI

| Misura | Prima | Dopo | Tipo |
|---|---|---|---|
| Suite completa in CI | 711–753 s per esecuzione (#538 `pr-check` 711 s; main `99250c5` 752,65 s), ×17 per push | 1 esecuzione per push | prima [M]; dopo [M] sul head della PFC-002, nel report della PR |
| Minuti-job per push | 342–391 (#535/#537) | ~70 | prima [M]; dopo [S], da confermare con la PR |
| Dal push all'ultimo check | 25–30 minuti | ~14–16 minuti, limitato da `full-suite` più Merge Readiness | prima [M]; dopo [S] |
| Review AI | 1 chiamata per reviewer e push, troncata | 1–8 chiamate parallele per reviewer e push, solo se il range supera il budget di un lotto | regola |
