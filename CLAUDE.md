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
- corregge review comments, check rossi, DeepSource, CodeRabbit,
  Sourcery, Gitar o GitHub Actions.

Per domande, spiegazioni o analisi read-only non serve il flusso.

## LAVORO IN BACKGROUND — CICLO AUTONOMO E VERDETTO OBBLIGATORIO

L'agente porta avanti l'intero ciclo PR **in autonomia, senza fermarsi ad
aspettare istruzioni intermedie**: push → attesa check settled → lettura e
triage review → patch strette → gate finale a label → hard verify. Niente
domande all'owner per i passi che le policy già coprono; ci si ferma solo per
i need-manual reali (decisioni owner, ambiguità, rischio).

Il ciclo termina SEMPRE in uno di questi due esiti — mai in attesa passiva:

1. **Tutte le condizioni AUTO-MERGE soddisfatte** => l'agente **esegue il
   merge da solo** — anche se la PR è safety-critical — riporta lo SHA di
   merge e **prosegue con la PR successiva**.
2. **Qualsiasi altra situazione** (bloccante reale, condizione gated
   mancante, e soprattutto **crediti esauriti su uno dei quattro reviewer
   pagati**) => l'agente **NON resta in silenzio**: dichiara esplicitamente
   all'owner **"PRONTA PER MERGE"** (o lo stato reale: NEEDS_MANUAL /
   CHECKS_PENDING / FAILED / CREDITI_ESAURITI) con il motivo preciso per cui
   non ha mergiato, e **ferma la coda** finché l'owner non dice di
   proseguire.

Un ciclo che finisce senza merge eseguito né verdetto esplicito consegnato
all'owner è un ciclo incompleto.

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
- AUTO-MERGE: l'owner ha autorizzato l'auto-merge GATED dell'agente, **anche
  per le PR safety-critical** (vedi sezione AUTO-MERGE). Consentito SOLO a verde
  totale + able-to-merge + zero bloccanti + nessun need-manual. Fuori da queste
  condizioni non si mergia. Unico stop assoluto: **crediti esauriti** su uno dei
  quattro reviewer pagati => non si mergia, si avvisa l'owner e si aspetta.
- DeepSource è advisory di default: patcha solo se è required
  failing current-head o dimostra bug reale/safety/fail-open.
- Check-completion gate: le decisioni FINALI (READY_TO_MERGE,
  "implementato"/"done", evidence-resolve, resolve definitivo dei
  thread) si prendono solo quando TUTTI i check current-head sono
  SETTLED. Rafforza
  auto_pr_flow_spec §10/§13/§14: dove la spec gatta su un singolo check,
  qui si richiede l'intero rollup settled. Sono NON settled: PENDING,
  QUEUED, IN_PROGRESS, WAITING, REQUESTED, EXPECTED, UNKNOWN, null/empty.
  Leggi i rilievi review/inline/corpi **dopo** che i check finiscono (i
  bot — CodeRabbit/DeepSource/Sourcery/Gitar — pubblicano spesso
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
  inclusi i bot: CodeRabbit, Sourcery, Gitar, DeepSource). Codacy è DISMESSO:
  nessuna integrazione, nessun gate, mai un blocker.
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

- **GPT-5.6 Sol** e **Grok 4.6**: girano a OGNI push della PR. Review MIRATA e
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

### Costo dei reviewer: NON troncare, ma non bruciare crediti

Regola d'ingresso, perché quasi tutti sbagliano qui: **`max_tokens` /
`MAX_OUTPUT_TOKENS` è un TETTO, non un addebito.** Si paga ciò che il modello
GENERA davvero, non il tetto che gli hai concesso. Conseguenze operative, da
non ri-litigare a ogni giro:

- **Alzare il tetto è GRATIS** e toglie il troncamento. Un tetto alto su una
  review che il prompt vincola a 150 parole non costa un token in più.
- **Abbassare il tetto NON fa risparmiare.** Non riduce quel che il modello
  genera: lo taglia a metà. Il risultato è una review troncata — cioè spesa
  piena e valore zero, il peggiore dei due mondi. Non è una leva di risparmio,
  è un modo di pagare per niente.

Quindi: se una review esce troncata **si alza il tetto**, non si accorcia il
prompt sperando che basti.

**Le leve che risparmiano davvero** (in ordine di resa):

1. **Non chiamare due volte lo stesso range.** Il `done_marker` per range è già
   cablato nei 4 workflow: non toglierlo e non "forzare" un ri-lancio per
   vedere se stavolta va meglio.
2. **Meno push, non push più piccoli.** Ogni push paga DUE chiamate (GPT-5.6 Sol
   + Grok 4.6). Accorpa i fix e pusha una volta sola quando il lavoro è
   completo: tre push da un fix ciascuno costano il triplo di un push da tre fix
   e producono la stessa review.
3. **Autorizzazione owner sulle due label** (sezione sotto): è la leva più
   grossa, perché Fugu e Fable sono i costosi.
4. **`reasoning_effort` basso dove il modello ragiona.** I token di
   ragionamento si pagano come output: su Grok 4.6 (default `high`) è impostato
   `low`, perché qui il reviewer deve produrre 150 parole, non pensare a lungo.

**Leve VIETATE, che sembrano risparmio e non lo sono:** abbassare i tetti di
output (vedi sopra); stringere `MAX_TOTAL_PATCH_CHARS` finché il reviewer
smette di vedere il codice (un reviewer che non vede è un check verde falso,
non un risparmio); disattivare un reviewer per "fare prima". Se il budget è il
problema, si riduce il NUMERO delle chiamate, mai la QUALITÀ della singola.

### Le due label si mettono SEMPRE — autorizzazione permanente dell'owner

**Decisione dell'owner, 16-09-2026, che SOSTITUISCE il vecchio «solo l'owner
autorizza, mai di iniziativa».** `final-fugu-review` e `final-fable-review` si
applicano a OGNI PR, sempre, senza chiedere.

Il motivo non è il costo, è la **leggibilità del gate**. Una PR dove le label
non compaiono non si distingue, guardandola su GitHub, da una dove il gate è
stato saltato: il gate dev'essere VISIBILE, non solo soddisfatto nei fatti. È
la stessa ambiguità che le PR #465-#468 hanno tolto di mezzo altrove.

Come si mettono: con i tool MCP GitHub, **una alla volta**, e se sono già
presenti si **rimuove e si riaggiunge** (GitHub non emette un nuovo evento
`labeled` per una label già presente). Il `manual-review-required` che i
workflow aggiungono da soli si conserva.

**Quanto costa: quasi sempre zero.** Il `done_marker` è per range: se i due
reviewer forti hanno già pubblicato su quel range — cosa che succede da sé
quando il push tocca file critici — i job ripartiti dall'evento label si
chiudono `success` senza chiamare il modello. Misurato sulla #468: label
applicate, **una sola review pubblicata per reviewer**, spesa aggiuntiva nulla.
Quando invece il range è nuovo, la spesa è quella del giro a label, ed è
preventivata: si paga il gate, non uno spreco.

Resta vero che **meno push costano meno**: ogni push paga i reviewer per-push, e
un push in più su file critici paga anche i due forti. Accorpare i fix resta la
leva vera. Misurato in una sessione: `$0.42` con un push (#468), `$1.35` con tre
(#467), `$1.81` con cinque (#466) — stesso ordine di lavoro.

**La partenza automatica sui file critici resta.** Quando un push tocca `core/`,
`services/`, `controllers/`, i moduli root, dipendenze, workflow, config/segreti
o le aree safety, Fugu e Fable partono da soli: non è iniziativa dell'agente, è
la rete di sicurezza. Non disattivarla e non aggirarla.

**Il gate resta da RIPETERE finché tornano puliti.** A ogni cambio di head serve
un giro nuovo: si rimettono le label e si aspetta l'esito full-range. Il gate è
soddisfatto solo quando ENTRAMBI tornano senza bloccanti reali. Un falso
positivo strutturale persistente non è un bloccante reale (vedi nota diff-only):
si tratta con evidenza e si documenta, non si cicla all'infinito.

**Ripeti il lancio finché Fugu/Fable non tornano SENZA bloccanti (decisione
owner).** Ogni volta che il head cambia (un fix, un allineamento) serve un nuovo
giro: dichiaralo, **fatti autorizzare** e ri-lancia le due label sul nuovo head
stabile, poi attendi il loro esito full-range. Il gate è
soddisfatto SOLO quando ENTRAMBI tornano senza bloccanti reali. Un falso positivo
persistente NON è un bloccante reale (vedi nota diff-only): trattalo con evidenza,
non ciclare all'infinito — se dopo il lancio full-range resta solo un falso
positivo strutturale, dichiara pronto documentandolo. Se Fugu/Fable sono in
usage-quota (il workflow parte ma il modello non risponde) vale la regola
CREDITI ESAURITI della sezione AUTO-MERGE: non si mergia, si avvisa l'owner che
servono crediti e si ferma la coda finché non dice di proseguire.

**Nota diff-only / push-range vs full-range (appreso su #393).** I reviewer sono
diff-only (no checkout, no esecuzione). Le review **per-push** (auto su ogni push:
GPT/Grok sempre; Fugu/Fable su file core) vedono SOLO l'ultimo commit del range,
quindi possono dare falsi positivi su import/coerenza/«codice assente» quando il
codice citato sta in commit precedenti. Le review **a label** girano invece
sull'INTERA range della PR (`base…head`) e vedono tutto il diff, quindi risolvono
quei falsi positivi. Per il verdetto finale conta la **full-range a label** di
Fugu/Fable, non le per-push. Non inseguire con un commit un falso positivo
push-range: ri-lancia le label e leggi la full-range.

**Timing: i gate forti sono l'ULTIMO passo pre-merge.** Fai scattare le due
label quando la PR è stabile e in teoria pronta al merge: i reviewer per-push
(GPT-5.6 Sol, Grok 4.6) hanno COMPLETATO e i loro rilievi reali sono stati trattati
(patch o evidenza in-thread). CodeRabbit NON è un gate d'attesa: se ha completato
tratta i suoi rilievi reali; se è in rate-limit/usage-quota è assente e NON lo si
aspetta; se è «processing» sta ancora revisionando: non lo si aspetta come gate
vincolante, ma i suoi rilievi reali — se arrivano prima di finalizzare — si
trattano, altrimenti post-merge tracking. Così Fugu Ultra e Fable 5 revisionano un head
STABILE e non si sprecano su versioni che cambieranno ancora (ogni push ai forti
costa). Sequenza: lavoro completo → push → GPT/Grok finiti e finding trattati
(CodeRabbit solo se disponibile) → head stabile → **consegna il verdetto di
merge-readiness all'owner e ATTENDI la sua autorizzazione** → solo dopo fai
partire `final-fugu-review` + `final-fable-review` → attendi l'esito
**full-range** → se restano bloccanti reali: fixa, ri-pusha, **ri-consegna il
verdetto e richiedi una NUOVA autorizzazione** prima di ri-lanciare le label,
ripeti finché entrambi tornano puliti → merge secondo la sezione AUTO-MERGE.

**L'agente non vede mai le API key**: aggiunge solo la label; i secret restano
nei GitHub Secrets e Actions resta read-only sul codice (diff-only, niente
checkout né esecuzione del codice PR, redazione segreti).

**Se una review segnala bloccanti** (bug, security, rischi Betfair/dutching/
money management, gestione segreti, rischi workflow o `manual-review-required`):
NON dichiarare la PR pronta e NON auto-mergiare. Lascia la PR aperta e scrivi:
quale bloccante resta aperto e perché. In presenza di bloccanti l'auto-merge
è VIETATO (fail-closed): si auto-mergia solo a verde totale senza bloccanti e
nei limiti della sezione AUTO-MERGE.

**Reviewer da aspettare / non aspettare.** La copertura di default su OGNI PR è:
i 4 workflow API (GPT-5.6 Sol, Grok 4.6, Fugu Ultra, Fable 5) + CodeRabbit. Codex,
Sourcery **e CodeRabbit** NON sono un gate d'attesa: se pubblicano usage-limit /
rate-limit / usage-quota, trattali come ASSENTI (non pending) — non aspettarli,
non contarli nel check-completion gate, non bloccare il DONE su di loro; annota
solo che non hanno revisionato. Decisione owner: un reviewer **advisory**
(CodeRabbit, Codex, Sourcery) in rate-limit/usage-quota è assente da SUBITO,
nessuna attesa e nessun cap-timer (vedi «Skip per indisponibilità»). Per i
**quattro reviewer pagati** «non aspettare» non vuol dire «mergiare lo stesso»:
non li si aspetta a timer, ma se uno di loro è a secco **sul head corrente** la
PR non si mergia — vale CREDITI ESAURITI. L'unico gate finale vincolante sono i
due reviewer forti a label (Fugu Ultra + Fable 5): vedi «Gate finale a label».

**Fail-closed preservato (nota anti-regressione).** Declassare ad «assente» i
reviewer ADVISORY non disponibili (CodeRabbit/Codex/Sourcery) NON indebolisce il
fail-closed: NON sono check CI richiesti e NON sostituiscono i gate vincolanti. I
gate VINCOLANTI restano SEMPRE attivi e non si saltano mai — (a) i check CI
current-head SETTLED e (b) i due reviewer forti a label Fugu Ultra + Fable 5
(full-range, ripetuti fino a esito pulito). I rilievi tardivi dei reviewer
advertiti come assenti sono coperti dal tracciamento post-merge (Issue + fix PR).
Se un reviewer PAGATO è in usage-quota sul head corrente — uno qualsiasi dei
quattro, non solo Fugu/Fable a label — NON si dichiara DONE saltandolo: vale la
regola CREDITI ESAURITI (non si mergia, si avvisa l'owner, si ferma la coda). L'owner può inoltre sempre mergiare a mano (override umano).

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
paga i modelli (GPT/Grok sempre; Fugu/Fable su push core/critici). Accorpa i fix
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
- **I 4 workflow API** (GPT-5.6 Sol, Grok 4.6, Fugu Ultra, Fable 5): NON sono
  declassabili ad assenti. Se un giro riporta usage-quota / rate-limit del
  provider, non lo si aspetta a timer e non lo si conta nel check-completion gate
  (il check chiude comunque), ma **il DONE resta bloccato**: vale CREDITI
  ESAURITI. Il criterio è il **head corrente**, non il singolo push: si mergia
  solo se tutti e quattro hanno prodotto una review reale del head che si sta
  mergiando (per Fugu e Fable, la full-range a label). Una quota su un push
  intermedio **non** blocca nulla se poi quel reviewer ha revisionato il head
  finale; una quota sul head finale blocca, anche se i push precedenti erano
  puliti.

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

**Decisione dell'owner, 16-09-2026.** L'agente esegue il merge da solo — **anche
delle PR safety-critical** — quando le condizioni gated qui sotto sono tutte
soddisfatte, e poi **prosegue da solo con la PR successiva**. Questo
AGGIORNA/SUPERA sia i precedenti "AUTO_MERGE_ENABLED=false sempre" sia
l'esclusione safety-critical che valeva fino alla #468.

Cosa cambia e cosa NON cambia, detto con precisione perché la differenza è
tutta qui: cade l'**esclusione per categoria di file**, non cade **nessun
gate di qualità**. Le cinque condizioni sotto restano tutte obbligatorie e
fail-closed. Il merge diventa automatico perché i gate sono verificati, non
perché si guarda meno.

**Condizioni per auto-mergiare (TUTTE obbligatorie, fail-closed):**
1. Tutti i check current-head SETTLED e verdi (check-completion gate passato).
2. Zero bloccanti dai 4 reviewer AI (GPT-5.6 Sol, Grok 4.6, Fugu Ultra,
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

Se TUTTE le condizioni valgono, l'agente mergia e riporta lo SHA di merge —
**senza distinzione fra PR safety-critical e non**. Subito dopo il merge
riparte con la PR successiva della coda, ristabilendo il branch designato dal
`main` aggiornato (resta valida la regola UNA SOLA PR aperta alla volta).

**Le PR safety-critical non sono più escluse, ma restano riconoscibili.** Una
PR è safety-critical se tocca `core/`, aree safety di `services/`, money
management, `betfair_client`/`betfair_market_api`, `dutching*`,
`order_manager`, `safety_layer`, `reconciliation`, `runtime_controller`,
`.github/workflows/*`, config/segreti. Su queste l'agente:

- lo DICHIARA nel verdetto (`PR safety-critical: sì — <file/aree>`), così
  l'owner sa sempre cosa è stato mergiato in autonomia;
- applica gli stessi gate, senza sconti: se anche uno solo non regge, non
  mergia e passa dal ciclo need-manual;
- non usa MAI l'urgenza o "tanto è verde" come sostituto di un gate mancante.

**UNICA ESCLUSIONE: ciò che definisce o applica i gate stessi.** Restano a
merge MANUALE dell'owner le PR che toccano uno di questi file. Sono due strati
della stessa cosa — la prosa che dichiara i gate, e il codice che li esegue.

I documenti che definiscono cosa l'agente può mergiare:

- `CLAUDE.md`
- `AGENTS.md`
- `docs/auto_pr_flow_spec.md`
- `docs/hard_verify_spec.md`

I workflow che SONO i gate su cui poggia la decisione di auto-merge:

- `.github/workflows/pr-review-openrouter-gpt56-sol.yml`
- `.github/workflows/pr-review-xai-grok46.yml`
- `.github/workflows/pr-review-openrouter-fugu-ultra.yml`
- `.github/workflows/pr-review-claude-fable5.yml`
- `.github/workflows/ci-quarantine-guard.yml`
- `.github/workflows/pr-guard.yml`
- `.github/workflows/pr-merge-readiness.yml`

Il motivo è strutturale, non di categoria di rischio. Se l'agente potesse
mergiare da solo una modifica a questi file, potrebbe **allargare
progressivamente la propria autorità**: ogni passo singolarmente gated, l'effetto
cumulativo senza limite. Un'autorizzazione che può riscrivere se stessa non è
più un'autorizzazione dell'owner.

Sui workflow-gate il meccanismo è lo stesso, ed è aggravato da come girano —
per due strade opposte che portano allo stesso punto.

I quattro reviewer e `ci-quarantine-guard` girano da `pull_request_target`, cioè
dal branch **base**: una PR che indebolisce un reviewer viene revisionata dalla
versione **vecchia** del workflow, quella che sta togliendo. Il controllo che
dovrebbe fermarla è esattamente quello che la PR rimuove, e se ne accorge solo
dal push successivo, quando è già su `main`.

`pr-guard` e `pr-merge-readiness` girano invece da `pull_request`, cioè con la
versione **della PR stessa**: una PR che li indebolisce fa girare su di sé il
guard già indebolito, e passa verde perché il controllo non c'è più.

Il primo caso non vede la modifica, il secondo la subisce. In nessuno dei due il
gate può accorgersi del proprio indebolimento, ed è questo che lo distingue da
una qualsiasi altra area safety-critical.

Gli altri workflow — build, packaging, test, chaos, lockfile — NON sono in
esclusione: non decidono se una PR può essere mergiata, la verificano soltanto.
L'agente li mergia da solo ai gate qui sopra.

Rilevato da GPT-5.6 Sol e Claude Fable 5 sulla #469 (i file-policy),
indipendentemente, e da Fugu Ultra sulla stessa PR (i workflow-gate). Decisione
dell'owner: esclusi i file-policy e i sette workflow-gate; tutto il resto —
`core/`, Betfair, dutching, `order_manager`, config/segreti, e il resto di
`.github/workflows/` — l'agente lo mergia da solo ai gate qui sopra.

Su queste PR l'agente prepara tutto verde e able-to-merge, lo dichiara, e lascia
il merge all'owner.

L'override per-issue che serviva a togliere l'esclusione caso per caso non
serve più ed è ritirato: l'autorizzazione è globale e sta qui.

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

**CREDITI ESAURITI => NON MERGIARE, FERMARSI E AVVISARE.** È la sola
eccezione all'autonomia, e vale per i **quattro reviewer che l'owner paga**:
GPT-5.6 Sol, Grok 4.6, Fugu Ultra, Fable 5. Se uno qualsiasi di loro non può
revisionare perché il provider risponde usage-quota / rate-limit / crediti
esauriti — il workflow parte ma il modello non risponde — allora quella PR
**non è stata revisionata**, e un merge senza review non è un merge
autorizzato. L'agente:

1. NON mergia, nemmeno se tutto il resto è verde e able-to-merge;
2. lo dice all'owner in chiaro: **quale** reviewer è a secco, **quale** PR è
   ferma, e che serve una ricarica di crediti;
3. ANNOTA nella issue dedicata che il reviewer non ha revisionato per quota;
4. ASPETTA. L'owner ricarica e scrive «prosegui»: solo allora l'agente
   rilancia il giro di review sul head corrente e, se torna pulito, mergia.

Nel frattempo l'agente **non apre la PR successiva**: la coda si ferma, perché
proseguire vorrebbe dire accumulare lavoro non revisionato dietro a una PR
ferma.

**Distinzione che NON va confusa.** Codex e CodeRabbit non sono crediti
dell'owner: sono terze parti a disposizione limitata (Codex in usage-limit
permanente, CodeRabbit fermo a `<10 stelle`). Restano **assenti da subito**, non
bloccano nulla e non fanno fermare la coda — altrimenti la coda non ripartirebbe
mai. La regola di stop qui sopra riguarda **solo** i quattro workflow API.

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

## GATE DI CONSEGNA (OBBLIGATORIO PRIMA DI DICHIARARE PRONTA)

Una PR non è consegnata quando il codice è verde: è consegnata quando l'owner
ha in mano ciò che gli serve per decidere. Cinque passi, in quest'ordine.
Nessuno saltabile in silenzio.

### 1. Giro a label sul range completo — PRIMA del verdetto

Se la PR ha PIÙ DI UN PUSH, le review automatiche hanno visto `push range`,
cioè i singoli delta, non il diff assemblato. Si controlla la riga `Range:`
nell'intestazione di ogni review: solo `Scope: current PR range` copre la PR
intera.

- più di un push => `final-fable-review` e `final-fugu-review`, e si attende
  l'esito PRIMA di dichiarare qualsiasi verdetto;
- un push solo, con `Range:` che copre `base...head` => le label sono una
  fotocopia a pagamento: si dichiara, e non si lanciano;
- le label costano: si chiedono all'owner, non si applicano d'iniziativa.

Violato su #427: tre push, verdetto «PRONTA PER MERGE» dichiarato senza il
giro a range completo. L'ha notato l'owner, non il processo.

Limite noto, da mettere in conto: i workflow di review girano su
`pull_request_target`, quindi partono dal branch BASE. Una PR non può alzare i
propri tetti di budget, e un file con patch oltre
`MAX_PATCH_PER_FILE_CHARS_ESCALATED` viene troncato dalla fine — dove di solito
sta il codice. Se succede, si dichiara quale parte NON è stata rivista invece di
lasciar credere che la review sia stata completa.

### 2. Verdetto esplicito

Come già prescritto in LAVORO IN BACKGROUND, con un vincolo in più: il verdetto
arriva DOPO il punto 1, mai prima.

### 3. Cosa è stato fatto — in italiano, per l'owner

Non il changelog del diff: cosa cambia per chi usa il programma, quale problema
concreto risolve ogni pezzo, e **cosa è peggiorato**. Le regressioni introdotte
dalla PR si dichiarano; non si lasciano scoprire.

### 4. Prova visiva sotto Xvfb — quando c'è una superficie visibile

Se la PR tocca `mini_gui`, `ui_panels/`, `core/*_gui.py`, `telegram_tab_ui` o
qualunque cosa l'utente veda, si consegna uno SCREENSHOT REALE, catturato
facendo girare l'app sotto Xvfb. Non una descrizione, non un mockup. Vale anche
— soprattutto — per mostrare un peggioramento.

Ambiente verificato il 2026-08-20: Xvfb è presente, ma `tkinter` e
`customtkinter` NON esistono sul python di default; stanno su
`/usr/bin/python3.12`, dove si installano con
`pip install --break-system-packages customtkinter pillow`.

```bash
Xvfb :99 -screen 0 1280x820x24 &
DISPLAY=:99 /usr/bin/python3.12 apri_la_finestra.py &
sleep 8 && DISPLAY=:99 import -window root schermata.png
```

Nessuna superficie visibile => si dichiara N/A con motivazione.

### 5. Costo totale della PR

```bash
python3 scripts/pr_costo_review.py commenti_pr.json
```

Somma i costi di TUTTE le review raggruppandoli per `Range:`, e segnala le
review che non riporta perché prive della riga di costo — un totale che tace su
ciò che non ha saputo leggere si legge come una buona notizia.

Serve a rendere visibile una cosa che altrimenti non si vede: **il numero di
push si paga**. Su #427, $2.58 in 14 review su 4 range, di cui $1.67 spesi
perché i push erano tre — $0.76 per i due giri sui delta, più $0.91 per il giro
a label che ha dovuto rifare tutto da capo. Preparare e spingere insieme costa
meno di correggere in pubblico.

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
