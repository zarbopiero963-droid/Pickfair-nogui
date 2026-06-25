# CLAUDE.md

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
- Lavora SOLO sul current head della PR. Head mismatch => NEEDS_MANUAL.
- Matrix Phase 0 obbligatoria prima di ogni patch safety-critical.
  Phase 0 assente o NEEDS_MANUAL => non patchare.
- Patch strette: solo bug reali, riproducibili, current-head,
  o required blockers. Mai "fix everything".
- Rispetta files_allowed / files_forbidden. Di default MAI toccare:
  .github/workflows/*, core/*, services/*, secrets, runtime
  trading, Betfair, Telegram live — SALVO che il task spec li
  includa esplicitamente in files_allowed. Violazione => FAILED.
- Micro-audit post-fix obbligatorio PRIMA di test/commit/push.
- NESSUN push, resolve, rerun o merge di default. Ogni azione
  esterna richiede la flag esplicita (AUTO_PUSH_ENABLED,
  AUTO_RESOLVE_ENABLED, AUTO_RERUN_ENABLED) E tutti i gate passati.
  Eccezione: la richiesta esplicita dell'owner (prompt diretto,
  handoff, riparazione della PR corrente) vale come autorizzazione
  al push/resolve sulla PR in lavorazione; le flag restano
  obbligatorie per l'automazione non presidiata.
- AUTO_MERGE_ENABLED=false sempre: il merge è manuale dell'owner.
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
