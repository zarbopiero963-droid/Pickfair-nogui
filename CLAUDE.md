# CLAUDE.md

## Percorso MCP — ingresso obbligatorio e policy corrente

Per qualsiasi lavoro MCP partire da **Pickfair-nogui #491** e leggere
**#491 → #489 → #461 → #426 → #453 → #351 quando serve collaudo → pickfair-mcp- #1**.
Applicare [il contratto operativo consolidato](docs/mcp_operational_contract.md):
gerarchia delle fonti, sequenza tecnica, ownership e tre milestone separate.
SIM/LIVE: vale la decisione owner SIM/LIVE del contratto consolidato (un solo
motore e un solo set di tool MCP; nessuna nuova modalità; mai auto-LIVE).
Una sola PR attiva **per repository**: una in Pickfair-nogui e una in
pickfair-mcp- possono coesistere; la serialità non è globale.
Le formulazioni di serialità sotto riguardano questo singolo repository.

Per reviewer e merge applicare la decisione owner vigente **#426/P41** e
successive decisioni pertinenti, inclusi #492/#493. Non riattivare workflow o
label sospese sulla base di istruzioni storiche. Una modifica presente in una
PR aperta non equivale a comportamento già presente sul main.
Questo rinvio non riscrive la policy reviewer storica: P41 prevale sui punti
espressamente sospesi. Resta il merge manuale owner per i file-policy esclusi.

## PR-flow — budget e triage owner (07/10/2026)

`MAX_FIX_LOOP_ITERATIONS_PER_PR = 5`: cinque cicli **patch → push** da
`PATCH_REQUIRED`, cumulativi per PR. Head, reviewer/provider, thread risolti,
CI verde, progresso, cluster o rerun non azzerano il contatore. Il sesto ciclo
si ferma **prima della patch/commit/push**: `NEEDS_MANUAL`,
`fix_loop_budget_exhausted`. Una deroga owner deve autorizzare quella PR e un
numero preciso di cicli aggiuntivi; non cambia il cap globale né il contatore.

Distinguere `CURRENT_DEFECT`, `GUARDRAIL_GAP` e `THEORETICAL_MUTATION` sulla
base delle prove sul current head, indipendentemente dal reviewer. Nuove
parafrasi teoriche non autorizzano automaticamente patch; ripetizioni
equivalenti senza nuovo difetto sono `REVIEW_CHURN` e possono fermare prima
del cap. `KNOWN_LIMITATION_ACCEPTED_BY_OWNER` richiede decisione owner
esplicita e tracciata; non significa `FIXED` e non copre bug safety/security,
real-money, fail-open o violazioni del contratto owner. Nessun resolve fittizio.

Specifica normativa: [auto_pr_flow_spec §11–13](docs/auto_pr_flow_spec.md).
Enforcement: `scripts/pr_fix_loop_policy.py` e gate `scripts/pr_*`; test di
policy in `tests/scripts/test_pr_fix_loop_policy.py`. Ledger persistente per
PR fuori dai checkout temporanei; stato assente/corrotto → arresto prudenziale.
Merge Readiness mantiene tutti i gate vigenti. Reviewer/merge: #426/P41 e
successive decisioni pertinenti; non riattivare workflow o label sospesi.

## Contratto di autonomia end-to-end (owner 08/10/2026)

Testo normativo: [docs/auto_pr_flow_spec.md §0](docs/auto_pr_flow_spec.md).
Classifier eseguibile (finding, review, label, full diff, decisione di merge,
PR successiva, parallelismo cross-repo): `scripts/pr_autonomy_policy.py`,
test in `tests/scripts/test_pr_autonomy_policy.py`. L'owner decide COSA
(prodotto, vincoli, denaro, modalità, decisioni aperte); l'agente decide COME
ed è il responsabile tecnico operativo:

> L’agente non deve comportarsi come un esecutore che chiede conferma per ogni scelta tecnica.
> Deve comportarsi come il responsabile tecnico operativo del progetto, entro le decisioni owner e la roadmap.
> Quando il contratto e il risultato atteso sono già determinati, l’agente deve scegliere autonomamente la soluzione tecnica più sicura, implementarla, provarla, triagiarla, mergiarla se consentito e continuare.
> L’owner non deve essere coinvolto per risolvere problemi che il contratto già rende deterministici.
> L’agente deve chiedere l’owner solo quando la risposta cambierebbe il prodotto, il rischio accettato, il denaro, l’infrastruttura, le credenziali o una decisione owner esistente.

Dove questa sezione e il testo più vecchio qui sotto divergono, prevalgono
questa sezione e la spec. Su un punto di sicurezza che il contratto NON decide
vale la regola più severa; dove decide esplicitamente (PR safety-critical
runtime/core in auto-merge con tutti i gate di §0.9, senza override per-issue)
sostituisce il testo più vecchio.

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
triage review → patch strette → gate di merge (spec §0.9) → hard verify →
merge se consentito → PR successiva (§0.14). Niente domande all'owner per i
passi che il contratto già rende deterministici; ci si ferma solo per le
condizioni STOP owner di spec §0.13.

Il ciclo termina SEMPRE in uno di questi due esiti — mai in attesa passiva:

1. **Tutte le condizioni AUTO-MERGE soddisfatte** => l'agente **esegue il
   merge da solo** — anche se la PR è safety-critical — riporta lo SHA di
   merge e **prosegue con la PR successiva**.
2. **Qualsiasi altra situazione** (bloccante reale, condizione gated
   mancante, file a merge owner, e soprattutto **crediti esauriti su Sol o
   Grok**) => l'agente **NON resta in silenzio**: dichiara esplicitamente
   all'owner **"PRONTA PER MERGE"** (o lo stato reale:
   READY_FOR_OWNER_MANUAL_MERGE / NEEDS_MANUAL /
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
- UNA SOLA PR aperta / UN SOLO task attivo alla volta PER REPOSITORY (allineato ad
  AGENTS.md «Core rules»: "Only one active task/one open pull request
  is allowed at a time"). Se esiste già una PR aperta: lavoro NON
  correlato => fermati (BLOCKED), non aprire una seconda PR; lavoro di
  fix sulla PR aperta => continua sullo STESSO branch. Mai lavorare
  direttamente su `main`, mai task in parallelo nello stesso repository. Nuova PR (e nuovo
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
  per le PR safety-critical runtime/core** (sezione AUTO-MERGE, spec §0.9).
  Consentito SOLO con tutte le condizioni di §0.9. Fuori da queste condizioni
  non si mergia. **Crediti esauriti** su Sol o Grok => non si mergia, si
  avvisa l'owner e si aspetta.
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

## AI PR REVIEW — REVIEWER ATTIVI (owner #426/P41)

Fonte normativa: `docs/auto_pr_flow_spec.md` §0.7–§0.8 (contratto di autonomia
end-to-end, 08/10/2026); controlli eseguibili in `scripts/pr_autonomy_policy.py`.
Dettaglio operativo e postura di sicurezza in `docs/ai_audit_workflows.md`.
Questa sezione SOSTITUISCE il vecchio testo operativo a cinque reviewer e a
giro di label.

- **Attivi:** **GPT-6.1 Sol** e **Grok 4.7**, a OGNI push. Review MIRATA e
  corta: solo `## Bloccanti` + `## Verdetto finale` (tetti di output alti →
  non troncano; si pagano solo i token generati).
- **Advisory:** Codex (e CodeRabbit/Sourcery quando presenti). L'assenza non è
  né PASS né bloccante; ogni rilievo reale si triagia con prova (§0.3–§0.6) e
  al merge non resta nessun thread irrisolto.
- **SOSPESI da P41** (workflow disattivati nella UI GitHub, file ancora su
  disco): Fugu Ultra, Claude Fable 5.1 e GPT-6 Astra. Non si aspettano e non si
  riattivano. Le loro label finali `final-fugu-review`, `final-fable-review` e
  `final-astra-review` NON si applicano finché vale la sospensione: la regola
  del 16-09-2026 «si mettono sempre» è sospesa da P41. Riattivarne uno richiede
  prima di allinearne la logica della label a spec §0.8 (portano ancora la
  vecchia regola larga).

**Una review conta solo se** è sul head corrente (o il suo range finisce sul
head), porta il marker di completamento (`gpt56sol-pr-review-done` /
`grok46-pr-review-done`), non ha bloccanti irrisolti ed è leggibile — non un
commento d'errore. Job verde senza marker = fallimento; schema/API incompleti
= UNKNOWN. Timeout Grok: un solo rerun; secondo timeout → STOP all'owner
(«PRONTA PER MERGE — Grok assente per timeout»). Quota/crediti su Sol o Grok →
CREDITI ESAURITI (sezione AUTO-MERGE).

**Label (spec §0.8).** «Safety-critical» (`CRITICAL_PATTERNS`, elencati nel
commento del reviewer) è INFORMATIVO: review e hard verify completi
obbligatori, ma da solo non blocca l'auto-merge. `manual-review-required` è un
bloccante REALE e Sol/Grok la applicano solo tramite
`MANUAL_AUTHORITY_PATTERNS`: file di autorità/governance, tutti i
`.github/workflows/`, manifest di dipendenze, materiale segreto, o diff
troncato dalla Compare API. L'agente la applica inoltre per decisione owner
necessaria, stop esplicito, fix loop esaurito, accettazione del rischio o dove
la policy lo richiede espressamente. Non si rimuove in modo opportunistico:
decide `manual_label_decision` sul diff completo, e solo se non è registrato
alcun motivo owner.

**Nota diff-only / push-range (appreso su #393).** I reviewer sono diff-only
(no checkout, no esecuzione) e Sol/Grok revisionano il range del push, quindi
possono dare falsi positivi su import/coerenza/«codice assente» quando il
codice citato sta in commit precedenti. Si risponde in-thread con la prova dal
diff completo della PR; non si insegue con un commit.

### Costo dei reviewer: NON troncare, ma non bruciare crediti

Regola d'ingresso, perché quasi tutti sbagliano qui: **`max_tokens` /
`MAX_OUTPUT_TOKENS` è un TETTO, non un addebito.** Si paga ciò che il modello
GENERA davvero, non il tetto che gli hai concesso. Alzare il tetto è gratis e
toglie il troncamento; abbassarlo non fa risparmiare e compra una review
troncata a prezzo pieno. Se una review esce troncata **si alza il tetto**, non
si accorcia il prompt sperando che basti.

**Le leve che risparmiano davvero:**

1. **Non chiamare due volte lo stesso range.** Il `done_marker` per range è
   cablato in ogni workflow di review: non toglierlo e non "forzare" un
   ri-lancio.
2. **Meno push, non push più piccoli.** Ogni push paga GPT-6.1 Sol + Grok 4.7:
   accorpa i fix e pusha una volta sola quando il lavoro è completo.
3. **`reasoning_effort`, dove il modello ragiona.** I token di ragionamento si
   pagano come output, quindi abbassarlo È una leva vera — ma **oggi non è in
   uso**: fra i reviewer non-Anthropic, Grok e i sospesi Fugu e Astra girano a
   `REVIEW_EFFORT: high` per l'esperimento dichiarato nei loro workflow, e Sol
   a `REVIEW_EFFORT: max`, il livello più alto che GPT-6.1 Sol offre (decisione
   dell'owner del 07-10-2026, verificato sui metadati OpenRouter del modello);
   Fable la manopola non ce l'ha (l'API Anthropic non la espone). Non scrivere
   qui un valore diverso da quello che i workflow hanno davvero:
   `tests/guardrails/test_policy_reviewer_consistency.py` confronta le due cose.

**Leve VIETATE, che sembrano risparmio e non lo sono:** abbassare i tetti di
output; stringere `MAX_TOTAL_PATCH_CHARS` finché il reviewer smette di vedere
il codice (un reviewer che non vede è un check verde falso, non un risparmio);
disattivare un reviewer attivo per "fare prima". Se il budget è il problema,
si riduce il NUMERO delle chiamate, mai la QUALITÀ della singola.

### Lavorare i rilievi

**Lettura obbligatoria.** A ogni check-in leggi SIA i commenti inline SIA i
corpi delle review SIA i commenti di conversazione della PR — i rilievi
"outside diff range" vivono solo nel corpo della review.

**Classifica ogni rilievo sui due assi di spec §0.3** (origine/disposizione e
severità) con prova sul current head, indipendentemente dal nome del
reviewer. Introdotto dalla PR → fix ora, test, niente merge finché aperto.
Preesistente → `DEFERRED_BY_POLICY` solo alle condizioni di §0.4, mai FIXED.
Teorico → §0.6. P0 → fail-closed (§0.6). NON inseguire rilievi cosmetici o
falsi positivi da push-range con commit: si risponde in-thread con evidenza.

**Ogni fix verificato con test hard.** PRIMA il test che riproduce il bug
(fallisce sul vecchio codice), poi la patch che lo fa passare (PASS + BLOCK).
`py_compile` + `pytest` mirato eseguiti davvero, esito osservato.

**Rispondi nel thread.** Per ogni rilievo indirizzato: `Fatto in commit <SHA>`
con evidenza. Per gli altri: la classificazione (STALE / DUPLICATE /
ALREADY_COVERED / THEORETICAL_MUTATION / DEFERRED_BY_POLICY / REVIEW_CHURN)
con la sua prova. Il resolve è gated: current-head + tutti i check settled +
evidenza (auto_pr_flow_spec §11/§13).

**Tracciamento post-merge + sweep ultime 5 PR.** I commenti-bot possono
arrivare dopo il merge: per ogni finding reale su una PR chiusa apri una Issue
(numero PR, head SHA, file:riga, bot, severità, link) e una fix PR dedicata dal
main aggiornato. In Phase 0 di ogni task ispeziona le ultime 5 PR mergiate per
finding AI mai indirizzati, deduplicando sulle Issue esistenti.

**L'agente non vede mai le API key**: i secret restano nei GitHub Secrets e
Actions resta read-only sul codice (diff-only, niente checkout né esecuzione
del codice PR, redazione segreti).

## AUTO-MERGE (autorizzato dall'owner — GATED)

**Decisione dell'owner, 16-09-2026.** L'agente esegue il merge da solo — **anche
delle PR safety-critical** — quando le condizioni gated qui sotto sono tutte
soddisfatte, e poi **prosegue da solo con la PR successiva**. Questo
AGGIORNA/SUPERA sia i precedenti "AUTO_MERGE_ENABLED=false sempre" sia
l'esclusione safety-critical che valeva fino alla #468.

**Condizioni per auto-mergiare (TUTTE obbligatorie, fail-closed — spec §0.9):**
1. Head stabile; scope valido; accettazione completa; suite richiesta PASS;
   hard verify PASS (test hard PASS+BLOCK realmente eseguiti).
2. Tutti i check current-head SETTLED e verdi (check-completion gate passato);
   Merge Readiness PASS, letta fail-closed (§0.11).
3. GPT-6.1 Sol e Grok 4.7 settled sul head corrente senza bloccanti (§0.7);
   Codex triagiato; zero thread irrisolti; nessun rilievo
   `PATCH_REQUIRED`/`NEEDS_MANUAL` aperto.
4. Nessun P0/P1 introdotto; nessun preesistente attivato o aggravato; nessuna
   decisione owner pertinente aperta; fix loop valido; nessuno stop manuale.
5. Nessuna label `manual-review-required`, nessun file a merge owner nel diff
   COMPLETO della PR, nessuna violazione di dipendenze (controllo full-diff di
   `pr-guard`, §0.10).
6. La PR è "able to merge" su GitHub (mergeable, nessun conflitto, branch
   protection soddisfatta, non draft).

Se TUTTE valgono: `AUTO_PR_FLOW_STATUS=READY_FOR_AUTO_MERGE`, l'agente mergia
e riporta lo SHA — **PR safety-critical runtime/core comprese** (decisioni
owner 16-09-2026 e P41). Subito dopo esegue la procedura PR successiva di spec
§0.14 (resta UNA SOLA PR aperta PER REPOSITORY). Se tutto vale ma la PR tocca
un file a merge owner: `READY_FOR_OWNER_MANUAL_MERGE` e mergia l'owner. Un head
cambiato invalida la valutazione. `scripts/pr_autonomy_policy.py
merge-decision` codifica queste condizioni.

**Le PR safety-critical non sono escluse, ma restano riconoscibili.** Una
PR è safety-critical se tocca `core/`, aree safety di `services/`, money
management, `betfair_client`/`betfair_market_api`, `dutching*`,
`order_manager`, `safety_layer`, `reconciliation`, `runtime_controller`,
config/segreti. Su queste l'agente:

- lo DICHIARA nel verdetto (`PR safety-critical: sì — <file/aree>`), così
  l'owner sa sempre cosa è stato mergiato in autonomia;
- applica gli stessi gate, senza sconti: se anche uno solo non regge, non
  mergia;
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
- `.github/workflows/pr-review-openrouter-gpt-astra.yml`
- `.github/workflows/ci-quarantine-guard.yml`
- `.github/workflows/pr-guard.yml`
- `.github/workflows/pr-merge-readiness.yml`
- `scripts/guardrail_check.py`
- `scripts/pr_autonomy_policy.py`
- `scripts/pr_fix_loop_policy.py`
- `scripts/pr_flow_automation.py`
- `scripts/pr_merge_readiness.py`
- `scripts/pr_automation_controller.py`
- `scripts/pr_clean_scope_rebuild.py`
- `scripts/pr_refresh_self_checks.py`
- `scripts/ci/check_ci_quarantine.py`
- (e, per fail-closed, ogni altro `scripts/pr_*.py`: il pattern della label li copre)

`scripts/guardrail_check.py` e' in lista per la stessa ragione dei workflow, e
ce l'ha messo GPT-5.6 Sol: «un gate indipendente e **non modificabile nello
scope delegato**». Il workflow `pr-guard` era gia' escluso, ma esegue lo script
dal checkout della PR: escludere il contenitore e lasciare fuori il contenuto
non avrebbe chiuso niente.

Il motivo è strutturale, non di categoria di rischio. Se l'agente potesse
mergiare da solo una modifica a questi file, potrebbe **allargare
progressivamente la propria autorità**: ogni passo singolarmente gated, l'effetto
cumulativo senza limite. Un'autorizzazione che può riscrivere se stessa non è
più un'autorizzazione dell'owner.

Sui workflow-gate il meccanismo è lo stesso, ed è aggravato da come girano —
per due strade opposte che portano allo stesso punto.

I cinque reviewer e `ci-quarantine-guard` girano da `pull_request_target`, cioè
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

I cinque `scripts/pr_*` qui sopra definiscono autorità di merge, readiness,
budget del fix loop e il classifier di questo contratto: stessa ragione
strutturale (owner, punto 19 del 08/10/2026). Dal 08/10/2026 anche OGNI ALTRO
workflow — build, packaging, test, chaos, lockfile — è a merge owner, per
fail-closed: i workflow girano con secret e permessi del repository, e
`MANUAL_AUTHORITY_PATTERNS` etichetta tutto `.github/workflows/`. Idem i
manifest di dipendenze (supply chain). Su questo punto supera il paragrafo
#469 qui sotto.

Rilevato da GPT-5.6 Sol e Claude Fable 5.1 sulla #469 (i file-policy),
indipendentemente, e da Fugu Ultra sulla stessa PR (i workflow-gate). Decisione
dell'owner: esclusi i file-policy e i workflow-gate — sette allora, otto da
quando c'è Astra; tutto il resto —
`core/`, Betfair, dutching, `order_manager`, config/segreti, e il resto di
`.github/workflows/` — l'agente lo mergia da solo ai gate qui sopra.

Su queste PR l'agente prepara tutto verde e able-to-merge, lo dichiara, e lascia
il merge all'owner.

**`.guardrails/allowed_scope.json` NON è in esclusione — e il perché va scritto,
perché tre reviewer su quattro hanno chiesto di metterlo.** Sol, Fugu e Fable,
indipendentemente, sulla #469. Il rischio che nominano è reale: l'agente
registra lì il proprio scope, quindi in linea di principio può allargarselo.

Non si chiude con l'esclusione, per due ragioni misurate. La prima: da sola non
servirebbe, perché i cap che contano stanno **nel codice** e non nel JSON —
`_path_conflict_safety_gate` limita comunque a `scripts/` + `tests/scripts/` e
nega `.github/workflows/`, `core/`, `secrets/`, `config/`. La seconda: **14 PR
su 14** lo toccano, perché questa
stessa policy impone di registrarci la task key nello stesso PR. Metterlo in
esclusione vorrebbe dire che nessuna PR viene mai auto-mergiata — una delega
che sembra concessa e non si applica mai è peggio del rischio che vorrebbe
chiudere, perché smette di essere verificabile.

Si chiude invece con un invariante **applicato dal check `guard`** — non con
una regola che l'agente applica a se stesso. La distinzione non e' formale: la
prima versione di questa sezione lo scriveva come prosa, e tutti e quattro i
reviewer pagati hanno bloccato sullo stesso punto («e' solo prosa», Grok;
«affidare questi vincoli alla sola dichiarazione dell'agente non chiude la
falla», Sol). Avevano ragione, e non in astratto: la #463 aveva toccato
`.github/workflows/pr-merge-readiness.yml` e `docs/auto_pr_flow_spec.md` fuori
dal proprio scope dichiarato, ed era stata mergiata senza che nessuno se ne
accorgesse.

L'invariante:

- i `files` della task key **coincidono con i file che la PR tocca davvero**, né
  più né meno. Una dichiarazione più larga del diff È un allargamento di scope:
  la PR diventa need-manual, anche se tutto il resto è verde;
- estendere i `files` della **propria** task key in corso d'opera è ammesso solo
  se dichiarato **esplicitamente** — nel verdetto all'owner E nella
  `description` della chiave. Mai in silenzio;
- modificare o rimuovere la entry di un'**altra** task key, o la sezione
  `default`, riscrive il registro di ciò che era stato concesso altrove: è
  sempre need-manual, mai auto-merge.

Con l'invariante, allargare la dichiarazione senza allargare il diff non serve a
niente, e allargare il diff è visibile nel diff — che ogni gate già guarda.

I tre punti sono verificati da `scripts/guardrail_check.py`
(`validate_declared_scope` e `validate_registry_untouched_elsewhere`), che
`pr-guard.yml` esegue a ogni push: il workflow gli passa anche
`allowed_scope_base.json`, la copia del registro dal branch base, cosi' il
confronto non dipende da cosa dice la PR di se stessa. Registro toccato ma
copia base assente => FAIL: un controllo che si puo' saltare in silenzio non e'
un controllo. `max_files` resta documentazione e non e' applicato: imposto
`files` == diff, il numero dei file e' gia' il numero del diff.


L'override per-issue che serviva a togliere l'esclusione caso per caso non
serve più ed è ritirato: l'autorizzazione è globale e sta qui.

**STOP owner => STOP + DOMANDA + ANNOTA + ATTENDI — solo per le condizioni minime.**
L'owner decide COSA, l'agente decide COME. L'agente si ferma e chiede SOLO per
le dieci condizioni di spec §0.13: nuova decisione di prodotto; spesa o
infrastruttura; credenziale o servizio; nuovo P0 non correggibile entro il
contratto deciso; cambio di contratto o accettazione del rischio; MANUAL via
MCP; fix loop esaurito; conflitto sostanziale fra fonti autorevoli;
autorizzazione o gate non verificabile; azione reale che richiede un comando
owner (soprattutto LIVE). Allora:
1. si FERMA (fail-closed: non forza, non inventa, non aggira);
2. pone la questione all'owner come DOMANDA esplicita, con le opzioni;
3. ANNOTA domanda e risposta nella issue dedicata del task / #426;
4. ATTENDE la decisione prima di procedere su QUELLA parte (una decisione
   aperta che tocca una sotto-parte blocca solo quella sotto-parte).

Un'incertezza puramente tecnica NON è uno stop: quando contratto e risultato
atteso sono determinati, l'agente sceglie la soluzione tecnica più sicura, la
prova, la triagia e prosegue. Un gate non verificabile non è un'incertezza
tecnica: è UNKNOWN/NEEDS_MANUAL (§0.11).

**CREDITI ESAURITI => NON MERGIARE, FERMARSI E AVVISARE.** Vale per i
**reviewer attivi che l'owner paga** (P41): GPT-6.1 Sol e Grok 4.7. Se uno dei
due non può
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
mai. La regola di stop qui sopra riguarda **solo** i workflow attivi di Sol e
Grok; i sospesi Fugu Ultra, Claude Fable 5.1 e GPT-6 Astra non si aspettano.

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

### 1. Copertura del diff completo — PRIMA del verdetto

Se la PR ha PIÙ DI UN PUSH, le review automatiche hanno visto `push range`,
cioè i singoli delta, non il diff assemblato. Si controlla la riga `Range:`
nell'intestazione di ogni review di Sol e Grok. Il giro a label sul range
completo dei reviewer forti è SOSPESO con loro (P41); la copertura del diff
assemblato la danno:

- il controllo full-diff di `pr-guard` (`scripts/pr_autonomy_policy.py
  full-diff`, spec §0.10) sul `base...head` reale;
- l'audit dell'agente sul diff completo (base, head, path vietati, scope,
  nuovi chiamanti, dipendenze, regressioni), dichiarato nel verdetto;
- una review di Sol e di Grok il cui range finisce sul head corrente.

Violato su #427: tre push, verdetto «PRONTA PER MERGE» dichiarato senza
guardare il diff assemblato. L'ha notato l'owner, non il processo.

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
