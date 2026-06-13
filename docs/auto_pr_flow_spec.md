# Auto PR Flow — Specifica

> Specifica del flusso automatico di gestione PR (orchestrator fail-closed).
> Implementazione di riferimento: `scripts/pr_automation_controller.py`,
> `scripts/pr_flow_automation.py`, `scripts/pr_merge_readiness.py`.

## 1. INIT — carica il task

All'inizio legge il task:

- task_key
- task_marker
- title
- branch
- files_allowed
- files_forbidden
- phase0_prompt
- patch_prompt
- acceptance_commands
- micro_audit_commands
- stop_conditions
- does_not_enable_live

Esempio:

```
TASK_KEY=claude_bug_pr8e_required_check_evidence_integration
TASK_MARKER=[TASK: claude_bug_pr8e_required_check_evidence_integration]
```

Capisce se il task è valido, se ha marker corretto, branch previsto, file ammessi e file vietati. Se manca il task key o il task spec è malformato, si ferma fail-closed. Il task spec minimo è richiesto dall'orchestrator.

Nota di scope: la validazione formale del task spec (questo step) si applica
ai task di automazione con TASK_MARKER. Il lavoro PR richiesto direttamente
dall'owner senza marker segue comunque il resto del flusso (preflight,
Matrix Phase 0 se safety-critical, micro-audit, gate su push/resolve) ma
senza validazione formale dello spec — coerente con CLAUDE.md e AGENTS.md,
che ammettono task da prompt diretto, commenti GitHub e handoff.

Output interno:

```
CURRENT_STATE=INIT
TASK_KEY=claude_bug_pr8e_required_check_evidence_integration
NEXT_ACTION=clean_branch_preflight
```

---

## 2. CLEAN_BRANCH_PREFLIGHT — controlla repo e branch

Poi controlla:

- git status --short
- branch corrente
- origin/main aggiornato
- PR head SHA
- local HEAD
- dirty worktree
- file non tracciati
- merge conflict
- branch sbagliato

Se trova worktree sporco non autorizzato:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=dirty_worktree
```

Se HEAD locale non coincide con PR head:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=current_head_mismatch
```

Questo serve perché il flusso lavora solo current-head, non su commenti vecchi o check stale. La policy impone current-head only e triage read-only prima di patchare.

---

## 3. PR8E evidence step — capisce branch protection e required checks

Questa è la PR nuova che stiamo per fare.

Il flusso leggerà:

- branch protection
- required status checks
- statusCheckRollup
- Codacy current-head
- DeepSource Python current-head
- review active
- unresolved_active
- headRefOid
- mergeStateStatus

E produrrà una evidence esplicita tipo:

```json
{
  "branch_protection_absent": true,
  "required_checks": [],
  "deepsource_required": false,
  "deepsource_required_current_head_check_failing": false,
  "codacy_success": true,
  "codacy_annotations_count": 0,
  "unresolved_active": 0,
  "current_head_sha": "..."
}
```

Questa evidence serve per automatizzare il ragionamento che abbiamo fatto a mano su PR256:

- branch protection assente
- => DeepSource Python non è required
- => Codacy verde
- => review attive 0
- => DeepSource FAILURE è advisory
- => non bloccare readiness solo per DeepSource

Se invece l'evidence è mancante, ambigua o contraddittoria:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=required_check_evidence_missing_or_ambiguous
```

Quindi il comportamento sarà:

PASS:

- branch_protection_absent=true
- required_checks=[]
- DeepSource failure non-required
- Codacy success
- review active=0
- => non bloccare

BLOCK:

- branch protection API errore
- required_checks malformati
- DeepSource required=true
- DeepSource required current-head failing=true
- Codacy failure
- review active > 0
- head mismatch
- => blocca fail-closed

---

## 4. MATRIX_PHASE_0 — ispeziona prima di patchare

Se il task tocca roba safety-critical, usa Matrix Phase 0. Questo vale per la PR8E perché tocca merge readiness, required checks, DeepSource required/non-required e fail-closed gating.

Matrix Phase 0 deve produrre:

```
PHASE_0_PREFLIGHT=PASS
```

oppure:

```
PHASE_0_PREFLIGHT=NEEDS_MANUAL
```

e deve includere file ispezionati, moduli autoritativi, dangerous gates, file ammessi, file vietati, patch plan, test matrix, stop conditions, risk level e next action. Se manca la Matrix Phase 0 o torna NEEDS_MANUAL, il flusso non patcha.

Per esempio su questo task capirà:

authoritative modules:

- scripts/pr_merge_readiness.py
- scripts/pr_flow_automation.py
- scripts/pr_automation_controller.py

dangerous gates:

- required checks
- DeepSource required/advisory
- branch protection absent
- Codacy current-head
- review active
- merge readiness
- PR flow guardrails

---

## 5. PATCH — patch stretta, solo se Phase 0 passa

Se:

- PHASE_0_PREFLIGHT=PASS
- AUTOMATION_MODE permette patch
- SAFE_AUTOFIX_ENABLED=true se richiesto
- files_allowed presenti
- files_forbidden rispettati

allora fa una patch stretta.

Per PR8E la patch dovrà essere solo su file tipo:

- scripts/pr_merge_readiness.py
- scripts/pr_flow_automation.py
- scripts/pr_automation_controller.py solo se serve wiring
- tests/scripts/test_pr_merge_readiness.py
- tests/scripts/test_pr_flow_automation.py
- tests/scripts/test_pr_automation_controller.py solo se serve

Non deve toccare:

- .github/workflows/*
- core/*
- services/*
- secrets
- runtime trading files
- Betfair
- Telegram live

Se prova a modificare un file vietato:

```
AUTO_PR_FLOW_STATUS=FAILED oppure NEEDS_MANUAL
REASON=forbidden_file_touched
```

Il patching non deve mai essere "fix everything". La policy dice di patchare solo bug reali, riproducibili, current-head, o required blockers.

---

## 6. Scope check / rollback

Dopo la patch guarda il diff:

```
git diff --name-only
```

Capisce:

- file ammessi modificati?
- file vietati toccati?
- workflow modificati?
- secret/env/credential toccati?
- core runtime toccato?
- live action introdotta?

Se trova violazione:

```
POST_FIX_AUDIT=FAIL
REASON=scope_violation
no commit
no push
```

Se è disponibile rollback/sandbox, ripristina i file fuori scope. Se non può garantire rollback sicuro:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=rollback_failed_or_scope_unclear
```

---

## 7. POST_FIX_MICRO_AUDIT

Prima dei test fa audit read-only della patch.

Controlla:

- solo file allowed
- nessun file forbidden
- nessun workflow edit non autorizzato
- nessun live action
- nessun auto-merge
- nessun auto-push default
- nessun broad suppression
- nessuna API key
- nessun segreto
- test mirati presenti
- fail-closed preservato

Se fallisce:

```
POST_FIX_AUDIT=FAIL
AUTO_PR_FLOW_STATUS=FAILED
REASON=post_fix_audit_failed
```

e non fa né validation, né commit, né push. L'orchestrator richiede che il micro-audit passi prima di test/commit/push.

---

## 8. TESTS — validation locale

Solo se:

```
POST_FIX_AUDIT=PASS
```

allora esegue i test.

Per PR8E saranno test tipo:

```bash
python3 -m py_compile scripts/pr_merge_readiness.py scripts/pr_flow_automation.py scripts/pr_automation_controller.py
python3 -m pytest -q tests/scripts/test_pr_merge_readiness.py -k "required or deepsource or branch_protection or readiness"
python3 -m pytest -q tests/scripts/test_pr_flow_automation.py -k "required or deepsource or readiness or guardrail"
python3 -m pytest -q tests/scripts/test_pr_automation_controller.py -k "deepsource or required_check or evidence"  # solo se controller toccato
git diff --check
```

Se un test fallisce:

```
AUTO_PR_FLOW_STATUS=FAILED
REASON=tests_failed
```

Se passa:

```
TESTS_STATUS=PASS
NEXT_ACTION=commit_push_or_report
```

---

## 9. COMMIT_PUSH_PR — solo con gate esplicito

Qui il flusso distingue bene tra "ho patchato localmente" e "posso mutare GitHub".

Per fare push servono:

- AUTOMATION_MODE=live oppure supervised autorizzato
- AUTO_PUSH_ENABLED=true
- Phase 0 PASS
- POST_FIX_AUDIT=PASS
- TESTS_STATUS=PASS
- current head atteso
- nessun file vietato
- task consente commit/push

Se AUTO_PUSH_ENABLED=false:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=push_disabled
NEXT_ACTION=human_review_and_push
```

Il design vieta push, resolve, rerun e merge di default; ogni azione esterna richiede flag specifica e gate passati.

---

## 10. CHECK_STATUS — legge PR dopo push

Dopo push o dopo PR aperta aggiornata, legge:

- bad checks
- pending checks
- cancelled checks
- Codacy status
- Codacy annotations_count
- DeepSource status
- Merge readiness
- PR flow guardrails
- review active
- unresolved_active
- headRefOid
- mergeable
- mergeStateStatus

Se Codacy è in progress:

```
AUTO_PR_FLOW_STATUS=CHECKS_PENDING
NEXT_ACTION=wait
```

Se Codacy è success con annotations 0:

```
CODACY=success
CODACY_ANNOTATIONS=0
NEXT_ACTION=review_triage
```

Se Codacy è action_required con annotations current-head in scope:

```
REVIEW_TRIAGE_RESULT=PATCH_REQUIRED
REASON=codacy_current_head_action_required
```

La policy dice di non patchare mentre Codacy/checks sono in progress salvo bug current-head riproducibile.

---

## 11. REVIEW_TRIAGE — classifica commenti e bot

Fonti OBBLIGATORIE del triage, a ogni check-in: oltre ai thread
inline vanno letti **anche** i corpi delle review e i commenti di
conversazione della PR. I rilievi "outside diff range" (es.
CodeRabbit) non compaiono come thread inline e non arrivano via
webhook: vivono solo nel corpo della review. Leggere solo i thread
=> triage incompleto.

Il flusso classifica ogni thread attivo:

- PATCH_REQUIRED
- EVIDENCE_RESOLVE
- SKIP
- NEEDS_MANUAL

Capisce:

- commento è stale?
- è su current head?
- è provider conosciuto?
- è bug reale?
- è safety/security?
- è fail-open?
- è refactor/style?
- è DeepSource advisory?
- è Codacy current-head?
- è già coperto da test?
- richiede file vietati?

DeepSource diventa speciale:

- DeepSource complexity/style/collapsible-if non-required => advisory
- DeepSource required current-head failing => PATCH_REQUIRED
- DeepSource fail-open/security claim => PATCH_REQUIRED
- DeepSource broad refactor => NEEDS_MANUAL

La policy DeepSource dice che DeepSource è advisory di default e va patchato solo se è required failing check current-head o dimostra bug reale/safety/fail-open/violazione contratto.

---

## 12. FIX_LOOP — ripete solo per blocker reali

Se trova:

```
REVIEW_TRIAGE_RESULT=PATCH_REQUIRED
```

entra nel fix loop.

Ma non fa una patch per ogni commento. Raggruppa per cluster:

- cluster: required-check evidence bug
- cluster: Codacy current-head annotation
- cluster: fail-open branch
- cluster: missing test reale

Poi riparte da:

- Matrix Phase 0
- patch
- micro-audit
- tests
- commit/push
- check status
- review triage

Se incontra stesso errore ripetuto, churn o retry budget esaurito:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=patch_required_loop_stopped
```

Retry budget: massimo 3 iterazioni del fix loop per PR. Il conteggio è
per-PR e cumulativo (non si resetta su progresso parziale), in linea con
il cap di 3 tentativi auto-fix per PR definito in AGENTS.md. Una nuova
iterazione si conta a ogni passaggio patch→push innescato da
PATCH_REQUIRED sulla stessa PR.

---

## 13. EVIDENCE_RESOLVE — risolve solo con prove

Se un thread è stale/advisory/già coperto:

```
REVIEW_TRIAGE_RESULT=EVIDENCE_RESOLVE
```

può rispondere e risolvere solo se:

- AUTO_RESOLVE_ENABLED=true
- current head combacia
- validation passata
- Codacy success
- annotations_count=0
- test/evidence coprono il commento
- nessun blocker attivo sullo stesso tema

Se AUTO_RESOLVE_ENABLED=false:

```
NEXT_ACTION=human_evidence_resolve
```

Quindi prepara testo e prove, ma non risolve automaticamente. Le regole vietano di risolvere thread "perché sembra risolto"; servono head SHA, validation e check/Codacy evidence.

---

## 14. Rerun readiness / guardrails

Rerunna readiness/guardrails solo quando:

- Codacy verde
- review active = 0
- pending = 0
- nessun blocker codice reale
- guard locale passa
- PR head corrente verificato
- AUTO_RERUN_ENABLED=true se serve rerun live

Non fa rerun infinito e non rerunna "a caso".

Dopo PR8E, readiness e guardrails dovrebbero capire:

- branch_protection_absent=true
- DeepSource non-required
- Codacy success
- review active=0
- => READY_TO_MERGE possibile anche se DeepSource Python è FAILURE advisory

---

## 15. READY_TO_MERGE

Il flusso dichiara:

```
AUTO_PR_FLOW_STATUS=READY_TO_MERGE
```

solo se:

- bad=[]
- pending=[]
- unresolved_active=0
- Codacy success
- Codacy annotations_count=0
- required checks non bloccanti
- DeepSource/Semgrep/security gates non blocking
- current head match
- PR non draft
- mergeStateStatus pulito o accettabile

L'orchestrator prevede READY_TO_MERGE solo con bad vuoti, pending vuoti, unresolved_active 0 e Codacy success.

Ma il merge resta manuale:

```
AUTO_MERGE_ENABLED=false
merge manuale owner
```

---

## 16. NEEDS_MANUAL

Si ferma con:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
```

quando trova:

- Phase 0 NEEDS_MANUAL
- branch protection evidence ambigua
- required-check evidence mancante
- file vietati necessari
- workflow edit richiesto ma non autorizzato
- review comment architetturale
- provider sconosciuto non classificabile
- Codacy/checks in progress senza bug riproducibile
- test failure non recuperabile
- scope troppo largo
- segreti/runtime/live Betfair/Telegram
- merge conflict unsafe

Questo è il comportamento corretto: quando non può dimostrare sicurezza, non inventa, non forza, non bypassa.

---

## 17. FAILED

Si ferma con:

```
AUTO_PR_FLOW_STATUS=FAILED
```

quando c'è un errore tecnico o una violazione certa:

- post-fix audit failed
- tests failed
- forbidden file touched
- patch outside allowlist
- state transition invalid
- Phase 0 bypass attempted
- micro-audit bypass attempted
- secret detected
- auto-merge/default push introdotto

---

## 18. Telegram passivo

Se abilitato:

```
TELEGRAM_NOTIFY_ENABLED=true
```

può inviare solo report passivi:

- READY_TO_MERGE
- NEEDS_MANUAL
- FAILED
- PATCH_REQUIRED_LOOP_STOPPED
- CHECKS_PENDING
- CODACY_ACTION_REQUIRED

Telegram non può:

- eseguire comandi
- approvare patch
- approvare merge
- cambiare automation mode
- fare trading
- chiamare listener live

Il design del router Telegram è passivo e disabled by default.

---

## In pratica, mentre lavora cosa "capisce"

Capisce queste cose:

1. Posso agire o sono disabled?
2. Il task è valido?
3. Sono sul current head giusto?
4. Il worktree è pulito?
5. Il task è safety-critical?
6. Serve Matrix Phase 0?
7. La Phase 0 autorizza patch o richiede manuale?
8. Quali file posso toccare?
9. Quali file sono vietati?
10. Codex ha toccato solo lo scope?
11. Il micro-audit è passato?
12. I test sono passati?
13. Posso fare push o devo fermarmi?
14. Codacy è verde?
15. Le review attive sono zero?
16. DeepSource è required o advisory?
17. Branch protection è assente o presente?
18. I required checks includono DeepSource Python?
19. Readiness può passare?
20. Devo patchare, evidence-resolve, skippare o fermarmi?
