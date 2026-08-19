# CLAUDE.md

## REGOLA PRINCIPALE

Prima di lavorare su questo repository, leggi e segui AGENTS.md: contiene le
policy complete (invarianti di safety, sequenza operativa, template Phase 0 /
micro-audit / test / hard verify, formati di risposta). Questo file aggiunge
le regole operative per-dominio e i puntatori alle spec — non duplica AGENTS.md.

Questo repository è **Pickfair (no-GUI)**: un bot di trading Betfair headless.
La catena di rischio è: Telegram → parser → validazione → dutching/money
management → ordine Betfair con **denaro reale** → riconciliazione → database.
Una modifica sbagliata può piazzare un ordine errato, duplicare una puntata,
rigiocare un segnale stantio o indebolire un gate di sicurezza. Tratta ordini,
stake, dedupe, riconciliazione e teardown come codice di produzione trading.

Il merge è gated: vedi la sezione AUTO-MERGE (autorizzazione owner). Fuori
dalle condizioni gated il merge resta manuale dell'owner.

## QUANDO USARE QUESTE REGOLE

Usa il flusso completo (AUTO PR FLOW + AGENTS.md) per qualsiasi task che:

- modifica `headless_main`, `mini_gui`, `ui_panels/`, runtime o teardown;
- modifica parser Telegram, listener, filtro chat, token;
- modifica `betfair_client`, `betfair_market_api`, `order_manager`,
  `market_validator`, cashout;
- modifica `dutching*`, `stake_mode`, money management, limiti;
- modifica `database`, `database_schema`, persistenza stato;
- modifica config (`config.json`, `trading_config`, `config_registry`);
- modifica il motore AI (`ai/ai_pattern_engine`, `ai/ai_guardrail`,
  `ai/wom_engine`);
- modifica build/packaging (`packaging/`, `pickfair.spec`,
  `install_linux.sh`, `*.bat`), dipendenze o workflow;
- richiede commit, push, PR, resolve thread o valutazione merge;
- corregge review comments, check rossi, Codacy, DeepSource, CodeRabbit,
  Sourcery, Gitar o GitHub Actions.

Per domande, spiegazioni o analisi read-only non serve il flusso.

## LE CINQUE REGOLE ANTI-REGRESSIONE — OBBLIGATORIE

Valgono per **ogni** PR che tocca codice, in aggiunta al resto di questo file
e ad AGENTS.md. Gli esempi qui sotto sono di questo repository e sono stati
**misurati**, non ipotizzati.

### 1. Test fail-first, sempre

Prima il test che riproduce il difetto, **verificato rosso sul codice
attuale**; poi la patch. Un test scritto dopo la patch dimostra solo che la
patch fa quello che fa, non che il difetto è chiuso. Nel report va scritto
l'esito del test **prima** della correzione, non solo dopo.

### 2. Cerca la CLASSE, non il sito

Prima di chiudere una PR, `grep` del pattern corretto su tutto il repository.

**Esempio reale.** `core/trading_engine.py` sostituisce in silenzio un
`_NullRiskMiddleware` che approva tutto quando il risk gate non è cablato.
Correggere quello non basta: la stessa forma è su `_NullReconciliationEngine`
(enqueue no-op, `is_ready()` sempre `True`) e su `_NullStateRecovery`
(`recover()` sempre `ok`). **Tre siti, una sola classe di difetto.** Chi si
ferma al primo chiude la PR col difetto ancora vivo.

### 2-bis. Cerca i CONSUMATORI, non solo i siti

Il grep della regola 2 trova i posti che hanno lo stesso difetto. **Non trova
i posti che si fidavano del comportamento che stai cambiando.**

Ogni volta che cambi un valore di ritorno, dove una funzione scrive, o una
promessa del docstring (*non solleva* / *è best-effort* / *è idempotente*):

- `grep` di **chi chiama** quella funzione — non del pattern del difetto;
- per ciascun chiamante, leggi cosa fa del risultato: lo converte? si fida che
  non sollevi? si aspetta il dato in quella posizione?
- il test fail-first va scritto **sul chiamante**, non solo sulla funzione.

**Esempio reale.** `list_runner_book` restituisce `[]` sia quando non ci sono
dati sia quando il mercato è chiuso. La funzione è corretta e i suoi test
passano. Il chiamante però non distingue *«ritenta al polling successivo»* da
*«disattiva la regola»*, e una regola su mercato chiuso interrogherebbe
Betfair per sempre. Il difetto è invisibile testando la funzione da sola.

### 3. Fonte unica dove esiste

Se una correzione va scritta in due posti, il posto giusto è **zero**: va
estratta in una fonte unica **prima** di correggere. Due copie corrette oggi
sono due copie divergenti domani.

**Esempio reale.** Le mappe nomi/mercati esistono due volte: come profili su
config (`core/name_mapping_store.py`, `core/market_mapping_store.py` — usati
da 10 moduli) e come tabelle DB (`name_aliases`, `market_aliases` — presenti
nella DDL, senza CRUD). Nessuna delle due è sbagliata; averle entrambe lo è.
Vedi la decisione registrata su #290.

### 4. Una PR aperta alla volta — **dimostrata, non dichiarata**

PR che toccano lo stesso modulo in parallelo si conflittano, e il merge
risolto a mano è dove nascono i difetti nuovi.

A differenza delle altre quattro non è ispezionabile dal diff, ma è
**verificabile**. Nell'hard verify va riportato l'**elenco effettivo** delle PR
aperte al momento del controllo (tool MCP GitHub o API), non un PASS asserito.
Elenco vuoto o contenente solo questa PR ⇒ PASS; qualsiasi altra PR aperta ⇒
si dichiara quale e perché non è in conflitto, oppure FAIL.

Elenco non ottenibile ⇒ **UNKNOWN con il motivo, mai PASS**. UNKNOWN blocca il
DONE esattamente come un FAIL.

### 5. Non toccare ciò che è dichiarato sano — **se è vincolato da test**

Le aree da non toccare senza task esplicito sono il contratto degli ordini
Betfair (`customerRef`/`customerOrderRef`, single-shot su `placeOrders`,
tassonomia `order_unknown`), il filtro delle chat e i gate di money management.

**Ma «dichiarato sano» significa qualcosa solo se ci sono test eseguibili che
diventano rossi quando lo tocchi.** Una regola che vieta di toccare una cosa,
appoggiata a una prova che non esiste, è il meccanismo con cui un difetto
sopravvive per mesi.

**Esempio reale.** La tab Telegram della GUI dichiara *«configurazione
persistita, non ancora attiva a runtime»*. È falso dalla PR-5a: il path Bot API
è cablato in `TelegramService` e si attiva da solo quando `api_id`/`api_hash`
sono vuoti (`services/telegram_service.py:386`). L'affermazione è rimasta in
piedi perché nessuno l'aveva misurata.

Se un task sembra richiedere di toccare un'area dichiarata sana, **fermati e
chiedi** invece di procedere.

## LA PATCH È CODICE NUOVO — NON INTRODURRE DIFETTI CORREGGENDO

Le cinque regole qui sopra si applicano al difetto che stai chiudendo. **Si
applicano anche alla tua correzione**, che è codice nuovo scritto in fretta,
sotto la pressione di un ciclo di review già aperto, ed è il posto dove i
difetti nuovi nascono più spesso.

**Il pattern che si ripete**, e da cui viene questa regola: si analizza **un**
percorso di uscita, si scrive il fix per quello, e si generalizza agli altri
**senza eseguirli**. Il fix è corretto sul ramo guardato e sbagliato sugli
altri, e nessun test lo intercetta perché i test coprono il ramo che si stava
guardando.

Casi misurati su questo repository e sui suoi fratelli:

- una validazione degli host aggiunta senza derivare l'host di identity ⇒ login
  su un dominio e keepAlive su un altro, sessione mai rinnovata;
- un fix del circuit breaker che rendeva 429 e 408 né ritentabili né contati;
- un guard HALF_OPEN aggiunto senza controllo di proprietà, e poi col token non
  invalidato fra i cicli — **due giri per lo stesso guard**;
- una condizione di troncamento corretta come whitelist in un workflow e
  lasciata nella forma debole negli altri due: **regola 2 non applicata alla
  propria patch**.

Tutti e quattro sono difetti *della correzione*, non del difetto originale.

### Cosa è obbligatorio prima di dichiarare chiusa una patch

- **Enumera i percorsi di uscita** della funzione che stai toccando — successo,
  ogni errore, timeout, valore assente, tipo inatteso — e **eseguili**. Non
  dedurre il comportamento di un ramo da quello di un altro: se non l'hai
  eseguito, non lo sai.
- **Applica la regola 2 alla tua stessa patch**: se hai corretto una forma in
  un punto, `grep` della stessa forma altrove. Il caso della whitelist nasce
  esattamente da qui.
- **Applica la regola 2-bis alla tua stessa patch**: se la correzione cambia un
  valore di ritorno o una promessa, i chiamanti vanno riletti.
- **Audit dei call-site** della funzione corretta, non solo dei suoi test.

### Il segnale d'allarme

Se una PR supera i tre giri di patch e ogni giro produce un rilievo nuovo sulla
**stessa area**, non stai convergendo: stai inseguendo la tua stessa
correzione. Fermati, enumera i percorsi, ed esegui — invece di scrivere il
quinto fix. Se dopo l'enumerazione i rilievi continuano a cambiare forma,
**dichiara lo stato all'owner e chiedi**, non spendere un altro giro.


## MISURATO O RIFERITO — DICHIARA SEMPRE QUALE DEI DUE

Quando scrivi in una doc, in un report o in un commento che qualcosa **è**
in un certo modo, devi poter dire **come lo sai**. Le due categorie ammesse:

| Etichetta | Significato |
|---|---|
| **misurato** | ho eseguito il comando / letto il log / ispezionato il file, e riporto l'evidenza |
| **riferito** | me l'ha detto qualcuno, o l'ho letto in una doc che a sua volta non è misurata |

Un'affermazione **riferita** non diventa vera perché è scritta in un file di
questo repository. Non scrivere che un Secret esiste prima di aver visto il
log che lo usa; non scrivere che un comportamento è verificato prima di averlo
eseguito; non scrivere che un componente è attivo perché lo dice l'interfaccia.

E una regola scritta può contenere, **nella stessa frase, l'affermazione e la
sua smentita**: quando scrivi «solo X», controlla che il codice dica *solo X* e
non *diverso da Y* — sono due cose opposte e si somigliano.

## LAVORO IN BACKGROUND — CICLO AUTONOMO E VERDETTO OBBLIGATORIO

L'agente porta avanti l'intero ciclo PR **in autonomia, senza fermarsi ad
aspettare istruzioni intermedie**: push → attesa check settled → lettura e
triage review → patch strette → gate finale a label → hard verify. Niente
domande all'owner per i passi che le policy già coprono; ci si ferma solo per
i need-manual reali (decisioni owner, ambiguità, rischio).

Il ciclo termina SEMPRE in uno di questi due esiti — mai in attesa passiva:

1. **Tutte le condizioni AUTO-MERGE soddisfatte E PR non safety-critical**
   => l'agente **esegue il merge da solo** e riporta lo SHA di merge.
2. **Qualsiasi altra situazione** (PR safety-critical, bloccante reale,
   Fugu/Fable in usage-quota, condizione gated mancante) => l'agente **NON
   resta in silenzio**: dichiara esplicitamente all'owner **"PRONTA PER
   MERGE"** (o lo stato reale: NEEDS_MANUAL / CHECKS_PENDING / FAILED) con
   il motivo preciso per cui il merge resta manuale.

Un ciclo che finisce senza merge eseguito né verdetto esplicito consegnato
all'owner è un ciclo incompleto.

**Il verdetto vale anche senza PR.** La regola sopra presuppone una PR aperta,
ma il caso più frequente è un altro: lavoro completo, branch pushato, PR non
ancora aperta — perché l'owner ha chiesto di aprirla lui, o perché il flusso si
è fermato prima. Anche lì il ciclo **non può chiudersi in silenzio**: va
consegnato lo stesso verdetto, riferito al **branch** invece che alla PR, e
deve dire almeno:

- cosa è stato verificato e **come** (comandi eseguiti, esito, evidenza) —
  distinguendo misurato da riferito;
- cosa **non** è stato verificato, e perché;
- quali gate di questo file sono soddisfatti, quali no e quali sono N/A con
  motivo — inclusi il gate design handoff e il gate Xvfb;
- lo stato esplicito: **PRONTA PER MERGE** (una volta aperta la PR e passati i
  check) / **NEEDS_MANUAL** / **PARTIAL** / **NOT DONE**, con il motivo preciso.

Consegnare il branch dicendo solo «fatto, dimmi se apro la PR» è un ciclo
incompleto: manca il verdetto, che è la parte che serve all'owner per decidere.


## AUTO PR FLOW (OBBLIGATORIO)

Segui la macchina a stati definita in docs/auto_pr_flow_spec.md
per QUALSIASI task che:

- modifica codice destinato a una PR (con o senza TASK_MARKER)
- tocca scripts/pr_*.py o i relativi test
- richiede commit, push, resolve thread o valutazione merge readiness

PRIMA di iniziare un task di questo tipo: leggi per intero
docs/auto_pr_flow_spec.md. Non procedere a memoria.

Se il task ha TASK_MARKER [TASK: ...]: applica anche la
validazione formale del task spec (step 1 INIT). Marker
mancante o spec malformato => fermati fail-closed.

REGOLE NON NEGOZIABILI (valgono sempre):

- Fail-closed: evidence mancante, ambigua o contraddittoria
  => AUTO_PR_FLOW_STATUS=NEEDS_MANUAL. Non inventare, non forzare.
- UNA SOLA PR aperta / UN SOLO task attivo alla volta (allineato ad
  AGENTS.md «Core rules»: "Only one active task/one open pull request
  is allowed at a time"). Se esiste già una PR aperta: lavoro NON
  correlato => fermati (BLOCKED), non aprire una seconda PR; lavoro di
  fix sulla PR aperta => continua sullo STESSO branch. Mai lavorare
  direttamente su `main`, mai task in parallelo. Nuova PR (e nuovo
  branch) SOLO dopo che la precedente è merged/closed: per follow-up si
  ristabilisce il branch designato dal `main` aggiornato.
- Lavora SOLO sul current head della PR. Head mismatch => NEEDS_MANUAL.
- Matrix Phase 0 obbligatoria prima di ogni patch safety-critical.
  Phase 0 assente o NEEDS_MANUAL => non patchare.
- Patch strette: solo bug reali, riproducibili, current-head,
  o required blockers. Mai "fix everything".
- Rispetta files_allowed / files_forbidden. Di default MAI toccare:
  .github/workflows/*, core/*, services/*, secrets, runtime
  trading, Betfair, Telegram live — SALVO che il task spec li
  includa esplicitamente in files_allowed. Violazione => FAILED.
- Apertura PR new-task: registra SEMPRE la task key in
  `.guardrails/allowed_scope.json` (oggetto `tasks`, con i suoi
  `files` allowed e `max_files`) NELLO STESSO PR. Il marker
  `[TASK: <key>]` deve combaciare con una chiave registrata: il check
  `guard` valida presenza E registrazione — marker presente ma chiave
  NON registrata => "Unknown TASK tag" => guard FAILED. La PR può
  includere `.guardrails/allowed_scope.json` tra i propri `files`
  (non è file critico): si auto-registra e il guard passa sullo stesso
  head, perché il guard legge il registro dal merge-ref della PR. Vale
  per ogni nuova PR, inclusi i task ad-hoc dell'owner.
- Micro-audit post-fix obbligatorio PRIMA di test/commit/push.
- NESSUN push, resolve, rerun o merge di default. Ogni azione
  esterna richiede la flag esplicita (AUTO_PUSH_ENABLED,
  AUTO_RESOLVE_ENABLED, AUTO_RERUN_ENABLED) E tutti i gate passati.
  Eccezione: la richiesta esplicita dell'owner (prompt diretto,
  handoff, riparazione della PR corrente) vale come autorizzazione
  al push/resolve sulla PR in lavorazione; le flag restano
  obbligatorie per l'automazione non presidiata.
- AUTO-MERGE: l'owner ha autorizzato l'auto-merge GATED dell'agente (vedi
  sezione AUTO-MERGE). Consentito SOLO a verde totale + able-to-merge + zero
  bloccanti + nessun need-manual, ed ESCLUSE le PR safety-critical (che restano
  merge manuale dell'owner). Fuori da queste condizioni il merge resta manuale.
- DeepSource è advisory di default: patcha solo se è required
  failing current-head o dimostra bug reale/safety/fail-open.
- Check-completion gate: le decisioni FINALI (READY_TO_MERGE,
  "implementato"/"done", evidence-resolve, resolve definitivo dei
  thread) si prendono solo quando TUTTI i check current-head sono
  SETTLED — non basta il solo Codacy finito. Rafforza
  auto_pr_flow_spec §10/§13/§14: dove la spec gatta sul singolo Codacy,
  qui si richiede l'intero rollup settled. Sono NON settled: PENDING,
  QUEUED, IN_PROGRESS, WAITING, REQUESTED, EXPECTED, UNKNOWN, null/empty.
  Leggi i rilievi review/inline/corpi **dopo** che i check finiscono (i
  bot — CodeRabbit/Codacy/DeepSource/Sourcery/Gitar — pubblicano spesso
  solo a check completato). Dopo OGNI push ripeti il ciclo: push =>
  attendi fine check => rileggi check+annotazioni+commenti+inline+thread
  => triage => eventuale patch. Lo status intermedio di monitoraggio è
  ammesso, ma non vale come giudizio finale.
- Docs nello stesso PR: ogni aggiunta/modifica/rimozione di codice
  (funzione, classe, modulo, comportamento, chiave config, gate,
  contratto, voce di roadmap) aggiorna la documentazione corrispondente
  nello STESSO PR (README, docs/ di dominio, ops/, docstring): le docs
  non devono mai restare disallineate dal codice. Vincolo di scope: se la
  doc da aggiornare è FUORI da files_allowed, NON forzare lo scope
  (niente commit fuori allowlist) => richiedi NEEDS_MANUAL o l'estensione
  esplicita dell'allowlist; l'obbligo di aggiornare la doc vale solo
  entro files_allowed. Se davvero non serve, scrivilo come nota.
  Micro-audit e Hard-Verify includono il check "docs aggiornate per il
  cambiamento: PASS/FAIL" (definito in hard_verify_spec §12-bis).
- A ogni check-in della PR leggi e fai triage dei thread inline
  attivi, non-outdated e non-risolti (review comments e review threads,
  inclusi i bot: CodeRabbit, Sourcery, Gitar, DeepSource, Codacy).
  Classifica ogni rilievo: `PATCH_REQUIRED` (bug reale current-head =>
  patch stretta), `EVIDENCE_RESOLVE` (già coperto/outdated => rispondi
  con prova), `SKIP` (falso positivo / duplicato / fuori scope =>
  motiva), `NEEDS_MANUAL` (ambiguo, rischioso o decisione owner).
  Bloccanti = `PATCH_REQUIRED` e `NEEDS_MANUAL` irrisolti: NON dichiarare
  il lavoro completo finché restano. Risolvere un thread è azione gated:
  serve AUTO_RESOLVE_ENABLED o il mandato esplicito dell'owner, più
  current-head e validation/evidence (vedi auto_pr_flow_spec §11/§13).
- A ogni check-in della PR leggi **anche** i corpi delle review e i
  commenti di conversazione, non solo i thread inline: i rilievi
  "outside diff range" (es. CodeRabbit) vivono solo nel corpo della
  review e non arrivano né come thread né via webhook.
- Riporta sempre lo stato finale: READY_TO_MERGE, NEEDS_MANUAL,
  FAILED, CHECKS_PENDING o PATCH_REQUIRED_LOOP_STOPPED, con REASON.

NON serve il flusso per: domande, spiegazioni, analisi read-only,
lavoro che non tocca codice PR.

## HARD VERIFY (OBBLIGATORIO)

Prima di dichiarare un task/PR "implementato" o "pronto per il merge",
applica la verifica a strati definita in docs/hard_verify_spec.md:
contratto del task, current-head, static audit nei file autoritativi,
test PASS e BLOCK, py_compile, pytest mirato, wiring nel flusso finale,
scope pulito, fail-closed, docs aggiornate per il cambiamento
(hard_verify_spec §12-bis). Il report finale **DEVE** includere una delle
etichette: MISSING, PARTIAL, IMPLEMENTED_WITH_NOTE, FULLY_IMPLEMENTED,
MERGED_BUT_NOT_FULLY_AUTOMATED. "PR merged" da sola non è prova di
implementazione.

## AI PR REVIEW — 4 REVIEWER + GATE LABEL (OBBLIGATORIO)

Ogni PR è coperta da quattro workflow di review AI (GitHub Actions con API
key nei Secret del repo) più CodeRabbit. Dettaglio operativo e postura di
sicurezza in `docs/ai_audit_workflows.md`.

- **GPT-5.6 Terra** e **GLM 5.2**: girano a OGNI push della PR. Review MIRATA e
  corta: solo `## Bloccanti` + `## Verdetto finale` (tetti di output alti →
  non troncano; si pagano solo i token generati).
- **Fugu Ultra** e **Claude Fable 5** (reviewer forti, costosi): partono da
  soli SOLO su push che tocca file **core o critici** di Pickfair — `core/`,
  `services/`, `controllers/`, i moduli root (`headless_main`, `mini_gui`,
  `betfair_client`, `betfair_market_api`, `order_manager`, `dutching`,
  `database`, `database_schema`, `trading_config`), dipendenze, workflow,
  config/segreti, o le aree safety (money management, dutching, safety_layer,
  reconciliation, runtime, catalog) — OPPURE con la label finale. Su push di
  soli docs/test i due job partono ma NON spendono (costo zero).

**Gate finale a label (obbligatorio pre-merge, da RIPETERE fino a pulito).**
Far partire le review finali via label `final-fugu-review` e `final-fable-review`
(già create dall'owner) è **OBBLIGATORIO** prima di dichiarare pronta QUALSIASI
PR — anche se non ha toccato file core/critici (quindi i forti non sono partiti
da soli) e anche se i workflow "final review" sono già girati sul push. Con i
tool MCP GitHub: **rimuovi e riaggiungi** le due label alla PR (GitHub non emette
un nuovo evento `labeled` se la label è già presente). Requisiti prima di
lanciarle: lavoro completo, check locali tentati, branch pushato, PR non draft.

**Ripeti il lancio finché Fugu/Fable non tornano SENZA bloccanti (decisione
owner).** Ogni volta che il head cambia (un fix, un allineamento) ri-lancia le
due label sul nuovo head stabile e attendi il loro esito full-range. Il gate è
soddisfatto SOLO quando ENTRAMBI tornano senza bloccanti reali. Un falso positivo
persistente NON è un bloccante reale (vedi nota diff-only): trattalo con evidenza,
non ciclare all'infinito — se dopo il lancio full-range resta solo un falso
positivo strutturale, dichiara pronto documentandolo. Se Fugu/Fable sono in
usage-quota (il workflow parte ma il modello non risponde) vale il carve-out
della sezione AUTO-MERGE: auto-merge BLOCCATO, decide l'owner.

**Nota diff-only / push-range vs full-range (appreso su #393).** I reviewer sono
diff-only (no checkout, no esecuzione). Le review **per-push** (auto su ogni push:
GPT/GLM sempre; Fugu/Fable su file core) vedono SOLO l'ultimo commit del range,
quindi possono dare falsi positivi su import/coerenza/«codice assente» quando il
codice citato sta in commit precedenti. Le review **a label** girano invece
sull'INTERA range della PR (`base…head`) e vedono tutto il diff, quindi risolvono
quei falsi positivi. Per il verdetto finale conta la **full-range a label** di
Fugu/Fable, non le per-push. Non inseguire con un commit un falso positivo
push-range: ri-lancia le label e leggi la full-range.

**Timing: i gate forti sono l'ULTIMO passo pre-merge.** Fai scattare le due
label quando la PR è stabile e in teoria pronta al merge: i reviewer per-push
(GPT-5.6 Terra, GLM 5.2) hanno COMPLETATO e i loro rilievi reali sono stati trattati
(patch o evidenza in-thread). CodeRabbit NON è un gate d'attesa: se ha completato
tratta i suoi rilievi reali; se è in rate-limit/usage-quota è assente e NON lo si
aspetta; se è «processing» sta ancora revisionando: non lo si aspetta come gate
vincolante, ma i suoi rilievi reali — se arrivano prima di finalizzare — si
trattano, altrimenti post-merge tracking. Così Fugu Ultra e Fable 5 revisionano un head
STABILE e non si sprecano su versioni che cambieranno ancora (ogni push ai forti
costa). Sequenza: lavoro completo → push → GPT/GLM finiti e finding trattati
(CodeRabbit solo se disponibile) → head stabile → fai partire `final-fugu-review`
+ `final-fable-review` → attendi l'esito **full-range** → se restano bloccanti
reali: fixa, ri-pusha e **RI-LANCIA le label**, ripeti finché entrambi tornano
puliti → merge secondo la sezione AUTO-MERGE.

**L'agente non vede mai le API key**: aggiunge solo la label; i secret restano
nei GitHub Secrets e Actions resta read-only sul codice (diff-only, niente
checkout né esecuzione del codice PR, redazione segreti).

**UN CHECK VERDE NON È PROVA DI REVIEW.** Un workflow di review esce **verde**
anche quando non ha chiamato il modello: Secret assente, credito esaurito
(HTTP 402), dedup su `done_marker` già presente. Nessun errore, nessun check
rosso, e una PR con quattro spunte e zero righe lette. Per dichiarare che un
reviewer ha coperto un head devi **leggere il log e trovare la riga d'uso
token**. Se compare un `::notice` «non configurato», o il job è uscito dalla
dedup, quel reviewer non ha revisionato: dichiaralo, non tacerlo.

Corollario già costato caro su questo repository (#374, PR #8): la condizione
«due giri full-range consecutivi sullo stesso head» è **impossibile per
costruzione**, perché al secondo lancio la dedup fa uscire il job senza
interrogare il modello. Il check è verde perché il job è uscito pulito, non
perché qualcuno abbia revisionato.

**Le due meccaniche delle label che non sono deducibili dai log.**

- **Il gate si arma solo con la SUA label.** Su un evento `labeled` un workflow
  si arma solo se la label **appena aggiunta** è la propria — non basta che sia
  presente nell'elenco. Aggiungere `manual-review-required` a una PR che ha già
  le label finali **non** rilancia i due reviewer forti.
- **Una alla volta, mai in una sola chiamata.** Aggiungere entrambe le label
  insieme emette un evento `labeled` per ciascuna; i job gatano su
  `github.event.label.name`, l'evento della label che non è la propria viene
  rifiutato dalla condizione e — col gruppo di concorrenza della PR — i job
  buoni finiscono **skipped**. Il sintomo è «ho messo le label e i reviewer non
  partono», e non è deducibile dai log. Rimuovi e riaggiungi **una alla volta**,
  con una pausa fra le due.

**I tre modi in cui un reviewer non vede il codice.** Prima di trattare un
bloccante come reale, stabilisci in quale dei tre stati si trova.

1. **File non inviato.** Ogni review stampa in fondo l'elenco dei file che il
   workflow non le ha mandato. Se il file citato è lì, quella review non poteva
   verificarlo — e questo è **tutto** ciò che l'omissione dimostra: non prova
   che il difetto non esista.
2. **File inviato TRONCATO.** È il caso peggiore, perché non si nota: il
   modello riceve codice vero ma incompleto e conclude su ciò che manca. Un
   bloccante nella forma «non verificabile dal diff» è tipicamente questo.
3. **Review troncata in USCITA.** Il modello si interrompe a metà della propria
   review. Una review interrotta **non è una review completa**: nessuna delle
   sue omissioni prova niente, non ha finito di guardare.

Regola di lettura degli stati 1 e 2: **non si patcha e non si archivia sulla
fiducia, si verifica** — ispezione diretta del file, i test che lo vincolano, i
suoi chiamanti. Verifica conferma ⇒ il difetto è reale e va corretto, chiunque
l'abbia visto. Verifica smentisce ⇒ si risponde nel thread con quell'evidenza
(comando eseguito, righe lette, esito), **non con un commit**. Verifica
impossibile ⇒ si dichiara il limite e decide l'owner.

Quando un reviewer etichetta i propri rilievi, un `[INSUFFICIENT_CONTEXT]` non
è un difetto: è una **richiesta di verifica**, e va trattata come sopra.
L'etichetta dice da dove viene il dubbio, non che il dubbio sia infondato.

**Se una review segnala bloccanti** (bug, security, rischi Betfair/dutching/
money management, gestione segreti, rischi workflow o `manual-review-required`):
NON dichiarare la PR pronta e NON auto-mergiare. Lascia la PR aperta e scrivi:
`AUTO-MERGE DISABILITATO: questa PR richiede merge manuale dell'owner`. In
presenza di bloccanti l'auto-merge è VIETATO (fail-closed): si auto-mergia solo
a verde totale senza bloccanti e nei limiti della sezione AUTO-MERGE.

**Reviewer da aspettare / non aspettare.** La copertura di default su OGNI PR è:
i 4 workflow API (GPT-5.6 Terra, GLM 5.2, Fugu Ultra, Fable 5) + CodeRabbit. Codex,
Sourcery **e CodeRabbit** NON sono un gate d'attesa: se pubblicano usage-limit /
rate-limit / usage-quota, trattali come ASSENTI (non pending) — non aspettarli,
non contarli nel check-completion gate, non bloccare il DONE su di loro; annota
solo che non hanno revisionato. Decisione owner: **qualunque** reviewer in
rate-limit/usage-quota è assente da SUBITO, nessuna attesa e nessun cap-timer
(vedi «Skip per indisponibilità»). L'unico gate finale vincolante sono i due
reviewer forti a label (Fugu Ultra + Fable 5): vedi «Gate finale a label».

**Fail-closed preservato (nota anti-regressione).** Declassare ad «assente» i
reviewer ADVISORY non disponibili (CodeRabbit/Codex/Sourcery) NON indebolisce il
fail-closed: NON sono check CI richiesti e NON sostituiscono i gate vincolanti. I
gate VINCOLANTI restano SEMPRE attivi e non si saltano mai — (a) i check CI
current-head SETTLED e (b) i due reviewer forti a label Fugu Ultra + Fable 5
(full-range, ripetuti fino a esito pulito). I rilievi tardivi dei reviewer
advertiti come assenti sono coperti dal tracciamento post-merge (Issue + fix PR).
Se un reviewer VINCOLANTE (Fugu/Fable a label) è in usage-quota, NON si dichiara
DONE saltandolo: vale il carve-out AUTO-MERGE (auto-merge BLOCCATO, decide
l'owner). L'owner può inoltre sempre mergiare a mano (override umano).

**Finestra review event-driven (non a timer).** I quattro reviewer sincroni
rispondono in ~1 min. **CodeRabbit NON è un gate d'attesa**: se ha già COMPLETATO
leggi e tratta i suoi rilievi reali (inline + corpo review); se è in rate-limit /
usage-quota, trattalo come ASSENTE (nessuna attesa, nessun cap-timer) e demanda al
tracciamento post-merge. Se è «processing» sta ancora revisionando: non lo si
aspetta come gate vincolante, ma non è 'assente' — se completa prima che finalizzi
tratta i suoi rilievi, altrimenti post-merge; non restare in stallo su di lui. Il
verdetto dell'AGENTE (pronto / DONE) NON dipende da CodeRabbit: dipende dai check
CI settled e dai gate forti a label (Fugu/Fable). L'owner può mergiare a mano in
qualsiasi momento.

**Parsimonia push (costo API + minuti CI).** Ogni push che aggiorna il head
paga i modelli (GPT/GLM sempre; Fugu/Fable su push core/critici). Accorpa i fix
di review in UN push per giro; non pushare per cleanup cosmetici o per
rincorrere falsi positivi da diff-per-push (rispondi in-thread con evidenza,
mai con un commit).

**Tracciamento post-merge + sweep ultime 5 PR.** Poiché non si aspetta una
finestra a timer, i commenti-bot possono arrivare dopo il merge: se un evento
review atterra su una PR chiusa, rileggila e per ogni finding reale/azionabile
apri una Issue (numero PR, head SHA, file:riga, bot, severità P1/P2/nitpick,
link al commento) e una fix PR dedicata dal main aggiornato (Phase 0 +
micro-audit + test hard PASS/BLOCK; niente riuso/stack della PR mergiata). In
Phase 0 di ogni task ispeziona le ultime 5 PR mergiate per finding AI mai
indirizzati, deduplicando su Issue esistenti (aperte e chiuse).

**Skip per indisponibilità (usage-quota / rate-limit) — vale per TUTTI i
reviewer.** Un reviewer che non può revisionare NON è un gate e NON è "pending":
trattalo come ASSENTE e prosegui (annota che non ha revisionato).
- **Codex**: usage-limit => assente, saltato.
- **Sourcery**: rate-limit => assente, saltato.
- **CodeRabbit**: rate-limit / usage-quota => assente, saltato (come Codex/
  Sourcery); non aspettarlo, nessun cap-timer, demanda al tracciamento post-merge.
  «processing» = sta revisionando: NON è 'assente' ma NON è un gate d'attesa
  vincolante (il DONE poggia su check CI settled + Fugu/Fable a label); se completa
  in tempo tratta i rilievi reali, altrimenti post-merge. Se ha già completato,
  tratta i rilievi reali.
- **I 4 workflow API** (GPT-5.6 Terra, GLM 5.2, Fugu Ultra, Fable 5): se un giro
  riporta usage-quota / rate-limit del provider, quel reviewer è assente per quel
  push => non aspettarlo, non contarlo nel check-completion gate, non bloccare il
  DONE su di lui.

**Lettura obbligatoria dei rilievi.** A ogni check-in leggi SIA i commenti inline
(review comments su file:riga) SIA i corpi delle review (review bodies) SIA i
commenti di conversazione della PR — i rilievi "outside diff range" vivono solo
nel corpo della review. Non fermarti ai soli nomi/stato dei check.

**Cosa patchare: SOLO bug logici reali.** Patcha solo bug logici/comportamentali
reali, regressioni e rischi safety (Betfair, dutching, money management, gestione
segreti, race/idempotenza, fail-open). NON inseguire rilievi estetici/cosmetici
(stile, naming, formattazione, preferenze) né falsi positivi da diff-per-push: a
quelli rispondi in-thread con evidenza, MAI con un commit.

**Ogni fix verificato con test hard.** Un fix che nasce da un rilievo review deve
avere un test hard che lo copre: scrivi PRIMA il test che riproduce il bug
(fallisce sul vecchio codice), poi la patch che lo fa passare (PASS + BLOCK).
`py_compile` + `pytest` mirato eseguiti davvero, esito osservato. Niente DONE se
il fix non è coperto.

**Rispondi nel thread ("risolti").** Per ogni rilievo indirizzato commenta nel
thread GitHub `Fatto in commit <SHA>` con evidenza (comando test: PASS, file:riga
modificato). Per i rilievi saltati: `Skipped / già coperto` col motivo (outdated
/ duplicato / cosmetico / fuori scope) e evidenza. Marcare il thread "resolved" è
azione gated: current-head + tutti i check settled + evidenza (vedi
auto_pr_flow_spec §11/§13).

## AUTO-MERGE (autorizzato dall'owner — GATED)

L'owner ha autorizzato l'agente a eseguire il merge automatico della PR in
lavorazione, ma SOLO in modo gated. Questo AGGIORNA/SUPERA i precedenti
"AUTO_MERGE_ENABLED=false sempre" e i generici "non mergiare": alle condizioni
qui sotto l'agente PUÒ mergiare; fuori da esse il merge resta manuale dell'owner.

**Condizioni per auto-mergiare (TUTTE obbligatorie, fail-closed):**
1. Tutti i check current-head SETTLED e verdi (check-completion gate passato).
2. Zero bloccanti dai 4 reviewer AI (GPT-5.6 Terra, GLM 5.2, Fugu Ultra,
   Fable 5) e — se ha completato la review — da CodeRabbit. CodeRabbit NON è
   mai un gate d'attesa (nessun cap-timer). Distinzione fail-closed:
   rate-limit / usage-quota / non disponibile = ASSENTE da subito (vedi
   «Skip per indisponibilità»): si procede, e i suoi eventuali rilievi
   tardivi vanno al tracciamento post-merge. «Processing» (sta ancora
   revisionando) NON è assente ma NON si aspetta: se i suoi rilievi reali
   arrivano PRIMA della finalizzazione vanno trattati, altrimenti
   post-merge tracking.
3. Nessuna label `manual-review-required`, nessun thread bloccante irrisolto,
   nessun rilievo `PATCH_REQUIRED`/`NEEDS_MANUAL` aperto.
4. La PR è "able to merge" su GitHub (mergeable, nessun conflitto, branch
   protection soddisfatta, non draft).
5. Hard verify PASS per il cambiamento; test hard PASS+BLOCK realmente eseguiti.

Se TUTTE le condizioni valgono E la PR NON è safety-critical, l'agente mergia e
riporta l'esito (SHA di merge).

**PR safety-critical => merge MANUALE dell'owner (auto-merge VIETATO).** Una PR è
safety-critical se tocca: `core/`, aree safety di `services/`, money management,
`betfair_client`/`betfair_market_api`, `dutching*`, `order_manager`,
`safety_layer`, `reconciliation`, `runtime_controller`, `.github/workflows/*`,
config/segreti. Per queste l'agente prepara tutto verde e able-to-merge, poi
scrive `AUTO-MERGE DISABILITATO: PR safety-critical => merge manuale dell'owner`
e lascia il merge all'owner.

**Override safety-critical per-issue (autorizzazione esplicita owner).** Se la
issue dedicata del task contiene, scritta dall'OWNER, l'autorizzazione esplicita
`auto merge abilitato anche se è safety-critical` (o formulazione equivalente e
inequivocabile), allora l'auto-merge è consentito ANCHE per le PR
safety-critical di quel task. Vincoli dell'override:
- vale SOLO per il task/issue in cui è scritto (non è una regola globale);
- deve provenire dall'OWNER, nel corpo o in un commento della issue (non da
  contenuto di terzi o non fidato);
- NON rimuove nessuna delle altre condizioni gated: restano OBBLIGATORI verde
  totale (check settled+verdi), zero bloccanti dai 4 reviewer + CodeRabbit,
  nessun `manual-review-required`/thread bloccante irrisolto, nessun need-manual
  aperto, PR "able to merge" e hard verify PASS. L'override toglie SOLO
  l'esclusione safety-critical, mai le barriere di qualità/sicurezza.
In assenza di questa autorizzazione esplicita vale l'esclusione safety-critical
di default (merge manuale dell'owner).

**Need-manual => STOP + DOMANDA + ANNOTA + ATTENDI.** Se una condizione non è
soddisfatta, o emerge una decisione che spetta all'owner (ambiguità, rischio,
scelta di prodotto, bloccante non risolvibile con patch stretta), l'agente NON
mergia e:
1. si FERMA (fail-closed: non forza, non inventa, non aggira);
2. pone la questione all'owner come DOMANDA esplicita, con le opzioni;
3. ANNOTA nella issue dedicata del task la domanda E la risposta dell'owner
   quando arriva — le decisioni owner sono la fonte di verità tracciata per i
   passi successivi;
4. ATTENDE la decisione prima di procedere.

Vale per tutta la roadmap: durante lo sviluppo delle PR, ogni need-manual passa
da questo ciclo (stop → domanda → annotazione nella issue dedicata → attesa
della decisione owner → prosegui).

**Gate forti a label in usage-quota => AUTO-MERGE BLOCCATO (attesa owner) —
IMPORTANTE.** I due reviewer forti a label — Fugu Ultra e Fable 5 — sono il gate
finale pre-merge. Se, dopo aver fatto partire le label, uno o entrambi NON
possono revisionare perché in usage-quota / crediti esauriti (il workflow parte
ma il modello non risponde), l'auto-merge è BLOCCATO: la review forte finale
richiesta non è avvenuta. In questo caso l'agente:
1. NON auto-mergia, nemmeno se tutto il resto è verde e able-to-merge, e nemmeno
   con l'override safety-critical attivo;
2. si FERMA e ATTENDE l'autorizzazione esplicita dell'owner a continuare;
3. ANNOTA nella issue dedicata che Fugu/Fable non hanno revisionato per quota e
   che l'auto-merge è in attesa della decisione owner.

Questa regola PREVALE sulla regola generale "skip per indisponibilità": quel
salto consente all'agente di non restare in stallo nel REPORT (annota il
reviewer come assente), ma NON autorizza l'auto-merge senza i gate forti finali.
Per MERGIARE in automatico servono Fugu Ultra e Fable 5 effettivamente eseguiti
e senza bloccanti; se sono in quota, il merge lo decide l'owner, mai l'agente.

## GENERAZIONE AUTOMATICA TEST HARD (OBBLIGATORIO)

La creazione di test hard veritieri NON è opzionale per NESSUNA modifica di
codice: è parte della patch (estende «Ogni fix verificato con test hard», che
vale per i fix da review, a OGNI cambiamento). Regole:

- Ogni funzione/ramo nuovo o modificato DEVE avere un test mirato che chiama
  il codice reale del progetto (niente `assert True`, niente mock-only,
  niente "dovrebbe passare", niente `|| true`).
- Il test deve FALLIRE se il bug torna (regressione bloccata), non solo
  passare (PASS + BLOCK, vedi hard_verify_spec §6).
- Per ogni fix da finding/review/bug: PRIMA il test che riproduce il problema
  (fallisce sul vecchio codice), POI la patch che lo fa passare.
- Test deterministici e offline: `simulation_broker`, stub, `tmp_path`; mai
  credenziali reali, mai chiamate live Betfair/Telegram nei test unitari.

Matrice di resilienza per aree safety-critical (scegli i casi pertinenti):

- **Ordini/dedupe**: segnale duplicato bloccato, dedupe persistente dopo
  restart, rate/daily limit rispettati, write failure con rollback di
  queue/dedupe/daily (segnale ritentabile in sicurezza), nessun ordine
  parziale da input malformato (fail-closed).
- **Riconciliazione/crash**: stato pendente su disco riconciliato all'avvio
  prima di qualsiasi nuovo ordine; crash tra write e commit non corrompe;
  ordini orfani gestiti, mai ri-piazzati alla cieca.
- **Dutching/money management**: distribuzione stake, arrotondamenti,
  virgola/punto, cap di liability, limiti giornalieri fail-closed; input
  NaN/inf/malformato => blocco.
- **Cashout/TTL unmatched**: cancel/cashout non lasciano stato orfano;
  TTL rispettato.
- **Config/DB**: config esistente intatta dopo save fallito, config corrotta
  in backup, default sicuri; modifiche `database_schema` con test di
  compatibilità e rollback su write multi-step.
- **Race runtime**: START fallito non lascia sessione attiva; STOP
  (`shutdown_manager`) teardown pulito; expiry/manual clear/process
  serializzati; nessun vecchio poller Telegram sopravvive a un nuovo epoch.
- **Motore AI**: `ai_guardrail` blocca nei casi previsti (errori consecutivi,
  volatilità, overtrade, dati insufficienti); auto-entry mai fail-open.

Limiti onesti: ciò che richiede ambiente live (Betfair reale, Telegram live,
GUI reale, AppImage/EXE) va scritto come smoke/manual checklist precisa e
marcato MANUAL_ONLY, mai dichiarato testato automaticamente.

## QUANDO TOCCHI IL PARSER TELEGRAM

Verifica almeno: messaggio valido dei formati supportati; messaggio
vuoto/non supportato => nessun segnale; quota con virgola e con punto;
campi obbligatori mancanti => segnale scartato, MAI ordine parziale;
chat non configurata => ignorata; nessun replay di messaggi vecchi senza
dedup. Il parser non inventa mai dati mancanti (mercato, selezione, quota,
stake). Token mai in log non redatto (`telegram_sanitizer`).

### Marcatori e codifica — specifica di questo repository

I marcatori del parser personalizzato (`start_after` / `end_before`) **non
stanno nel sorgente: arrivano dalla configurazione dell'utente**, e i messaggi
Telegram contengono emoji. Il confronto avviene quindi fra due stringhe di
**provenienza diversa** — file di config e payload di rete — e questo lo rende
più fragile, non meno.

Un confronto su emoji che fallisce **non solleva un errore**: restituisce
«non riconosciuto», e il segnale non produce mai un ordine senza che nessuno
se ne accorga.

- Ogni test su un parser con marcatore emoji deve confrontare i **codepoint**,
  non fidarsi dell'aspetto visivo: `🆚` e la stessa emoji con un variation
  selector si vedono identiche e **non** sono uguali.
- Oggi non esiste alcuna normalizzazione unicode nel percorso di parsing
  (verificato: nessun uso di `unicodedata` in `core/` e `services/`). Se
  introduci una normalizzazione, va applicata **a entrambi i lati** del
  confronto e coperta da un test che usa le due forme.
- I file sorgente sono UTF-8. Un marcatore non ASCII incollato in una config
  eredita la codifica di chi l'ha scritta: quando un parser «smette di
  funzionare senza motivo», questo è il primo posto da guardare.


## QUANDO TOCCHI BETFAIR CLIENT / ORDINI

Verifica almeno: idempotenza (stesso segnale => mai due ordini);
errori/timeout/risposte ambigue dell'API => fail-closed (blocco, non
retry cieco); riconciliazione all'avvio intatta; TTL unmatched e path di
cancel/cashout non indeboliti; nessuna credenziale hardcoded; `safe_mode`,
`circuit_breaker`, `auto_throttle` e `safety_layer` mai bypassati o
disattivati di default.

## QUANDO TOCCHI DUTCHING / MONEY MANAGEMENT

Safety-critical per definizione. Verifica almeno: calcolo stake con esempio
numerico verificato nei test; arrotondamenti e conversioni; cap di liability
e limiti giornalieri fail-closed; `stake_mode` retrocompatibile; nessun
cambiamento silenzioso di distribuzione/validazione prezzi. Documenta il
calcolo atteso nel PR body o nei test.

## QUANDO TOCCHI IL DATABASE

Modifiche a `database_schema` = breaking change: servono approvazione
esplicita del task, nota di migrazione/compatibilità e test. Mai drop o
riuso silenzioso di colonne/tabelle. Scritture del ciclo ordine (queue,
dedupe, daily, PnL) consistenti: write fallita => rollback dello stato
correlato, mai stato mezzo-applicato.

## QUANDO TOCCHI CONFIG / IMPOSTAZIONI

Verifica almeno: config esistente caricata correttamente; nuove chiavi con
default SICURI (mai live/real-money di default); compatibilità col
`config.json` esistente; save fallito non distrugge la config; config
corrotta va in backup; nessun segreto reale committato; nessun path locale
hardcoded.

## QUANDO TOCCHI LA MINI GUI

`headless_main` deve restare avviabile headless: la GUI è opzionale e non
può diventare dipendenza dura del runtime. Verifica (o descrivi il controllo
manuale): app avviabile; START/STOP funzionano; chiusura finestra fa
teardown pulito; indicatori di stato coerenti col runtime reale; nessun
controllo safety-rilevante rimosso o nascosto senza spiegazione. Ogni
modifica che tocca l'aspetto design/UI/UX attiva il GATE DESIGN HANDOFF
(sezione dedicata).

## QUANDO TOCCHI IL MOTORE AI (ai/)

`ai_pattern_engine` (Weight of Money), `wom_engine` e `ai_guardrail` sono
aree safety: l'auto-entry BACK/LAY resta gated dal guardrail. Verifica
almeno: nessun livello/soglia del guardrail indebolito di default; i
BlockReason esistenti continuano a bloccare; dati insufficienti o errori
consecutivi => blocco (mai fail-open); nessun ordine generato dall'AI senza
passare da money management e safety layer.

## QUANDO TOCCHI BUILD / PACKAGING

Verifica: workflow YAML valido; dipendenze coerenti coi requirements/lock;
`pickfair.spec` e `packaging/` (AppImage Linux) coerenti; `.bat` Windows non
rotti; artifact con nome chiaro; nessun segreto negli artifact; la build non
fa push o merge automatici. Se non hai eseguito la build reale scrivi
`Build not run in this environment` — mai dichiarare artifact generati se
non è vero.

## GATE DESIGN HANDOFF (OBBLIGATORIO PRIMA DI DICHIARARE PRONTA)

`docs/design/design_handoff.md` è la fonte unica di verità sull'aspetto
UI/UX della mini GUI e non deve mai restare disallineata dall'app reale.

- Prima di dichiarare una PR pronta/mergiabile (qualsiasi stato DONE/READY/
  PRONTA PER MERGE), se la modifica tocca l'aspetto design — finestre, tab,
  controlli/campi/pulsanti, stati o indicatori dinamici, flussi di conferma,
  palette colori e loro semantica di sicurezza, copy/microcopy della UI,
  information architecture, o le invarianti di sicurezza lato UI — DEVI
  aggiornare `docs/design/design_handoff.md` NELLO STESSO PR.
- L'aggiornamento deve essere veritiero e coerente col codice (label
  verbatim, stati/flussi corretti), come per il resto delle docs.
- Modifica puramente interna senza impatto design => dichiara N/A con
  motivazione scritta. Mai saltare in silenzio.
- È un gate, non un consiglio: PR che cambia il design con handoff stantio =
  PR incompleta, non dichiarabile pronta.
- Micro-audit e hard verify includono il check "design handoff aggiornato:
  PASS/FAIL/N/A" (vedi template in AGENTS.md).

## GATE XVFB — PROVA D'INTEGRAZIONE OBBLIGATORIA IN OGNI PR

**Non basta che i test hard passino: va dimostrato che il lavoro è davvero
integrato e funzionante nell'applicazione reale.** Un test unitario verde
prova che una funzione fa quello che fa; non prova che sia collegata a
qualcosa. Questo repository ha già quattro sottosistemi scritti e **mai
collegati** — il motore del parser (~3.150 righe che `telegram_listener.py:898`
non chiama), `stake_mode.py` importato da nessuno, `core/risk_middleware.py`
sostituito da un null object, `core/event_journal.py` scollegato. Ognuno ha i
suoi test verdi. Questo gate esiste perché non succeda una quinta volta.

### Cosa è obbligatorio

Ogni PR che tocca una superficie osservabile — GUI, tab, pannelli, flussi di
conferma, avvio/arresto, pipeline segnale→ordine, o qualunque cosa l'utente
possa vedere o premere — deve includere una **prova d'integrazione eseguita
sotto Xvfb**, con:

- **avvio reale dell'applicazione** nel display virtuale (non un import, non un
  mock): l'app deve partire davvero;
- **i passaggi che esercitano il cambiamento** — click sui controlli reali,
  scroll dove il contenuto eccede la finestra, cambio di tab, apertura delle
  finestre secondarie coinvolte;
- **screenshot** dei passaggi rilevanti, con lo stato prima e dopo quando la
  modifica cambia ciò che si vede;
- **zero eccezioni non gestite** durante il percorso, riportate come evidenza.

### Come si esegue — verificato in questo ambiente

Xvfb è disponibile e la GUI parte davvero: misurato il 2026-08-19, tutte e 12
le tab renderizzate e catturate. Il percorso che funziona:

1. `python3-tk` va installato e serve un interprete che ce l'abbia — non tutti
   gli interpreti presenti lo hanno; `customtkinter` **non è in
   `requirements.txt`** pur essendo indispensabile alla GUI, va installato a
   parte;
2. 🔴 **prima di avviare, neutralizza `config.json`**: svuota credenziali
   Betfair, disabilita Telegram e proxy. `main.py` e i service registrano
   sessioni all'avvio: far partire l'app con la config reale significa tentare
   un login vero con le credenziali del proprietario. Verifica che nessun
   valore originale sia rimasto **prima** di lanciare;
3. avvia `Xvfb` su un display dedicato, poi istanzia la GUI con
   `force_simulation=True` e pilotala dall'interno (cambio tab, scroll dei
   contenitori scrollabili, click) invece di simulare eventi dall'esterno:
   è deterministico e non dipende dai tempi di rendering;
4. cattura con uno strumento di screenshot sul display virtuale.

### Screenshot — regola segreti

Prima di allegare o committare uno screenshot, **oscura credenziali, token,
chat ID, percorsi locali e qualsiasi dato sensibile**. Uno screenshot è un
canale di fuga come un log. Se lo screenshot serve solo come evidenza nel
report della PR e non come documentazione, non va committato.

### Cosa dichiarare, e cosa non si può dimostrare così

Il gate copre l'integrazione nell'applicazione, **non** il comportamento
contro servizi reali. Betfair live, Telegram live e l'esecuzione di ordini
veri restano `MANUAL_ONLY` con checklist precisa: scriverlo, non spacciarlo.

Se il cambiamento è puramente interno e non ha alcuna superficie osservabile
(una funzione pura, una costante, una doc), dichiara **N/A con motivazione
scritta**. Mai saltare in silenzio.

Micro-audit e hard verify includono il check:

```
Prova d'integrazione Xvfb (avvio reale + click/scroll + screenshot):
- PASS / FAIL / N/A con motivazione scritta / MANUAL_ONLY con checklist
```

Una PR che cambia una superficie osservabile senza prova d'integrazione è
**incompleta** e non è dichiarabile pronta, esattamente come una PR senza
test hard.


## PRIORITÀ TECNICHE DEL REPOSITORY

Preserva sempre:

- Telegram legge solo messaggi validi e solo dalle chat configurate.
- Il parser non inventa dati mancanti.
- Un segnale valido produce UN SOLO ordine, quello giusto; mai duplicati.
- Money management e safety layer non si bypassano mai.
- La riconciliazione all'avvio precede qualsiasi nuovo ordine.
- Il database resta coerente anche su crash/write failure.
- START/STOP e chiusura non lasciano thread, poller o sessioni incoerenti.
- La config si salva e ricarica senza perdere dati; default sempre sicuri.
- Token e segreti non finiscono mai nel repository né nei log.
- Linux resta il target primario del runtime; la GUI resta opzionale.
- Il merge segue SOLO la policy gated della sezione AUTO-MERGE.

## REGOLA D'ORO

Non cercare di "fare tutto". Meglio una patch piccola, chiara e sicura che
una grande riscrittura. Il bot deve restare prevedibile:

Telegram corretto → parsing corretto → validazione → dutching corretto →
UN SOLO ordine, quello giusto → riconciliazione → database coerente.

Qualsiasi modifica che rompe questa catena va bloccata o approvata
esplicitamente dall'owner.
