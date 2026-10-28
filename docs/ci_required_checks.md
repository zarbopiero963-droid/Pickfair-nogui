# Required check di `main` e protezione dei merge (#562 PR-C2)

Stato: **piano preparato, nessuna modifica applicata.** Il ruleset lo cambia solo l'owner, fase per fase e dopo il
proprio GO. Questo documento dice cosa protegge oggi il merge, cosa manca, la configurazione proposta, come
arrivarci senza finestre di bypass e come tornare indietro.

Strumenti:
- [`ops/ci/required_checks.json`](../ops/ci/required_checks.json): il piano;
- [`scripts/ci/required_checks_plan.py`](../scripts/ci/required_checks_plan.py): verifica del piano, confronto con il
  ruleset reale, payload da applicare, valutazione di un head, matrice;
- `tests/scripts/test_required_checks_plan.py`: i test.

## 1. Inventario (28/10/2026)

Fonte: `GET /repos/zarbopiero963-droid/Pickfair-nogui/rulesets` e `…/rules/branches/main`. Copia del ruleset in
`tests/fixtures/ruleset_25459867_20261028.json`.

| Elemento | Valore |
|---|---|
| Ruleset attivo | `PICKFAIR-MAIN-PROTECTION` (id 25459867), su `~DEFAULT_BRANCH`, `enforcement: active` |
| Bypass | `bypass_actors: []`, `current_user_can_bypass: never` |
| Regole | `deletion`, `non_fast_forward`; `pull_request` (0 approvazioni, thread risolti obbligatori, solo squash); `required_status_checks` |
| Required check | **solo** `overlap-check` con `integration_id` 15368 (GitHub Actions), `strict: false` |
| Altri ruleset | `Piero` (14776122) e `Slack` (14960238), **disabilitati** |
| Branch protection classica | assente (Grok T252; l'API pubblica risponde 404) |

### Chi pubblica cosa

| Emittente | Id | Tipo | Cosa pubblica sull'head (#579, `8b088ea`) |
|---|---|---|---|
| GitHub Actions | 15368 | check run | 113 check run, fra cui `overlap-check` e `Merge readiness` |
| CodeRabbit | 347564 | check run | `CodeRabbit` (skipped, advisory) |
| `pickfair-trusted-status` (lane trusted) | 5492626 (Grok T252, da riverificare nella UI dell'App) | **commit status**, creator `pickfair-trusted-status[bot]` | `overlap-check`, `pr-merge-readiness`; con la PR-A `full-suite`; con la PR-C2 `pr-auto-merge` (mai required) |

Lo stesso nome `overlap-check` esiste come check run di Actions e come status della lane. GitHub li distingue solo con
`integration_id`, quindi ogni required check del piano è una coppia **contesto + app**.

### Il buco attuale

Visto su #576: lo status `pr-merge-readiness` della lane era `failure`, ma il merge è passato perché quel controllo non
è richiesto. Oggi `full-suite`, la readiness e l'overlap della lane non bloccano nulla. Se la lane si ferma o pubblica
`failure`, GitHub lascia mergiare comunque.

## 2. Matrice GitHub Actions ↔ Buildkite

Generata da `scripts/pr_full_suite_pipeline.py`, che è l'unica fonte. Il test `test_document_matrix_is_in_sync` la
confronta con l'output di `required_checks_plan.py matrix`.

<!-- matrix:begin (generata da `scripts/ci/required_checks_plan.py matrix`) -->
| Workflow Actions | Gruppo | Step Buildkite (`full-suite`) / motivo |
|---|---|---|
| `_module-ultra-check` | NOT_MIGRATED | workflow riusabile: eseguito tramite i moduli (`ultra_check`, `MODULES`) |
| `build-linux` | EXTRA (fuori PR) | `w4-build-linux-tests`, `w5-build-linux-appimage` |
| `build-windows-exe` | NOT_MIGRATED | build Windows: esclusa dal mandato #562 (solo workflow Linux) |
| `chaos-critical` | MAP (su PR) | `w3-chaos-critical` |
| `chaos-runtime` | EXTRA (fuori PR) | `w3-chaos-runtime` |
| `chaos-stateful` | EXTRA (fuori PR) | `w3-chaos-stateful` |
| `ci-dynamic-intelligent` | MAP (su PR) | `w4-ci-dynamic-detect`, `w2-backstop` |
| `ci-master-gate` | EXCLUDED | aggregatore: i workflow che chiama sono mappati singolarmente |
| `ci-quarantine-guard` | MAP (su PR) | `w1-quarantine` |
| `complete-task-on-merge` | EXCLUDED | contabilita' dei task alla chiusura della PR, non un controllo |
| `copy-engine` | MAP (su PR) | `w2-copy-engine` |
| `core-chaos` | EXTRA (fuori PR) | `w3-core-chaos` |
| `core-guardrails` | MAP (su PR) | `w1-core-guardrails` |
| `core-invariants` | MAP (su PR) | `w1-core-invariants` |
| `dutching` | MAP (su PR) | `w2-dutching` |
| `e2e` | MAP (su PR) | `w3-e2e` |
| `execution-guard` | MAP (su PR) | `w2-execution-guard` |
| `failure` | MAP (su PR) | `w3-failure` |
| `generate-linux-lockfile` | MAP (su PR) | `w5-lockfile-fresh` |
| `incident-snapshot` | MAP (su PR) | `w4-incident-snapshot` |
| `integration` | MAP (su PR) | `w3-integration` |
| `live-gate` | MAP (su PR) | `w4-live-gate` |
| `live-sim-parity` | MAP (su PR) | `w4-live-sim-parity` |
| `merge-simulation` | NOT_MIGRATED | variante manuale di `merge-simulation-hard`, gia' coperta da w4-merge-simulation |
| `merge-simulation-hard` | MAP (su PR) | `w4-merge-simulation` |
| `money-management` | MAP (su PR) | `w2-money-management` |
| `mutation-guardrails` | MAP (su PR) | `w1-mutation` |
| `net` | MAP (su PR) | `w3-net` |
| `noise-free-pr-gate` | MAP (su PR) | `w4-noise-free` |
| `observability-minimum` | MAP (su PR) | `w3-observability-minimum` |
| `observability-runtime` | MAP (su PR) | `w3-observability-runtime` |
| `observability-tests` | MAP (su PR) | `w3-observability-tests` |
| `order-manager` | MAP (su PR) | `w2-order-manager` |
| `pnl-engine` | MAP (su PR) | `w2-pnl-engine`, `w2-pnl-invariants` |
| `pr-check` | MAP (su PR) | `w1-pr-check` |
| `pr-guard` | EXCLUDED | metadati della PR (titolo, corpo, file) e registro dello scope: resta su Actions (check `guard`) |
| `pr-merge-readiness` | EXCLUDED | lane trusted (`pr-merge-readiness` del dispatcher) e `Merge readiness` di Actions |
| `pr-overlap-guard` | EXCLUDED | lane trusted (`overlap-check` del dispatcher) e required check di Actions |
| `pr-overlap-guard-diagnostic` | NOT_MIGRATED | diagnostica manuale della lane trusted (overlap-check), non della full-suite |
| `pr-review-claude-fable5` | EXCLUDED | reviewer sospeso (P41) |
| `pr-review-openrouter-fugu-ultra` | EXCLUDED | reviewer sospeso (P41) |
| `pr-review-openrouter-gpt-astra` | EXCLUDED | reviewer sospeso (P41) |
| `pr-review-openrouter-gpt56-sol` | EXCLUDED | review AI: `pull_request_target` con secret, resta su Actions |
| `pr-review-xai-grok46` | EXCLUDED | review AI: `pull_request_target` con secret, resta su Actions |
| `queue-on-pr-closed` | NOT_MIGRATED | recupero manuale della coda task: scrive sul repository, non e' un controllo |
| `rate-limiter` | MAP (su PR) | `w2-rate-limiter` |
| `readiness-gate` | MAP (su PR) | `w4-readiness-gate` |
| `reconciliation-engine` | MAP (su PR) | `w2-reconciliation-engine` |
| `recovery-guardrails` | MAP (su PR) | `w1-recovery-guardrails` |
| `repo-guardrails` | MAP (su PR) | `w1-repo-guardrails` |
| `repo-guardrails-extended` | MAP (su PR) | `w1-repo-guardrails-ext` |
| `risk-middleware` | MAP (su PR) | `w2-risk-middleware` |
| `rollback-gate` | MAP (su PR) | `w4-rollback-gate` |
| `runtime-controller` | MAP (su PR) | `w2-runtime-controller` |
| `session-manager` | MAP (su PR) | `w2-session-manager` |
| `simulation-broker` | MAP (su PR) | `w2-simulation-broker` |
| `smoke` | MAP (su PR) | `w3-smoke` |
| `smoke-core-fast` | MAP (su PR) | `w3-smoke-core-fast` |
| `stateful-integrity` | MAP (su PR) | `w2-stateful-integrity` |
| `telegram-listener` | MAP (su PR) | `w2-telegram-listener` |
| `telegram-live-integration` | EXTRA (fuori PR) | `w5-telegram-live-sim` |
| `telegram-test` | NOT_MIGRATED | invia un messaggio Telegram reale: vietato (nessun invio Telegram reale) |
| `trading-engine` | MAP (su PR) | `w2-trading-engine` |
| `trading-engine-hard-tests` | MAP (su PR) | `w2-trading-engine-hard` |
| `unit` | MAP (su PR) | `w1-unit` |
<!-- matrix:end -->

Limiti dichiarati: `WORKFLOW_LIMITS` e [ci_m2_migration.md](ci_m2_migration.md).

## 3. Configurazione proposta (opzione B approvata dall'owner)

**Decisione D-C2-1: opzione B (owner, 28/10/2026).** La build della readiness della lane pubblica due status distinti:

| Status | Significato | Required |
|---|---|---|
| `pr-merge-readiness` (5492626) | **readiness tecnica**: PASS solo se tutti i gate tecnici sono soddisfatti (check, review Sol/Grok, thread, head e base invariati) | **sì**, da F2 |
| `pr-auto-merge` (5492626) | **autorizzazione all'auto-merge dell'agente**: FAIL anche con un file a merge owner o la label `manual-review-required` (all'inizio o a fine attesa), se l'autorizzazione non è verificabile o se la readiness tecnica fallisce | **mai** (`never_required` nel piano, verificato da `validate`) |

Una PR a merge owner con i gate verdi ha `pr-merge-readiness` = success e `pr-auto-merge` = failure. I required check
sono soddisfatti, quindi l'owner la mergia a mano **senza bypass**. L'agente non mergia mai senza
`pr-auto-merge` = success sullo SHA esatto, e non ottiene privilegi in più: su GitHub agente e owner restano la stessa
identità, e il vincolo resta una regola dell'agente, ora con uno status verificabile.

Implementazione: `scripts/ci/trusted_pr_guard.py` (due verdetti) e `scripts/pr_trusted_dispatcher.py` (secondo status,
pending, ritentativi e self-check). Il dettaglio è in `docs/trusted_dispatcher.md` e in `docs/ci_trusted_lane.md`.

| Fase | Aggiunge | Toglie | `strict` | Precondizione | Decisione owner |
|---|---|---|---|---|---|
| F0, oggi | — | — | false | — | — |
| F1 | `overlap-check` (5492626), `full-suite` (5492626) | — | false | PR-A mergiata e full-suite certificata in runtime su una PR reale | GO all'applicazione |
| F2 | `pr-merge-readiness` (5492626) | — | **true** | F1 applicata; PR-C2 installata; almeno 2 PR reali coerenti con la `Merge readiness` di Actions | **D-C2-2** |
| F3 (facoltativa) | — | `overlap-check` (15368) | true | ritiro di Actions come lane primaria | **D-C2-3** |

Il payload esatto di ogni fase: `python3 scripts/ci/required_checks_plan.py payload --plan ops/ci/required_checks.json --phase N`.

**Proposta D-C2-2: `strict_required_status_checks_policy: true` da F2.**
- Obbliga il branch a essere aggiornato con `main` prima del merge. Così overlap, full-suite e readiness si riferiscono
  alla combinazione che verrà davvero mergiata, e non a una base vecchia.
- Con una PR alla volta il costo è un solo aggiornamento dopo ogni merge, che il flusso fa già (riallineamento dei branch
  dipendenti).
- Non si propone già in F1: finché la readiness non è obbligatoria, un aggiornamento forzato rifarebbe girare la
  full-suite senza aggiungere protezione.
- Rischio: un merge su `main` mentre una PR aspetta la rende di nuovo pending. È atteso: è proprio ciò che garantisce.

**Proposta D-C2-3: tenere `overlap-check` di Actions (15368) finché Actions resta disponibile.**
- Sulla stessa PR i due overlap sono indipendenti (Actions dal checkout del merge, la lane dal codice di `main`), quindi
  insieme sono una protezione in più e non un doppione inutile.
- Toglierlo (F3) ha senso solo se l'owner ritira Actions come lane primaria. In quel caso il piano rifiuta di toglierlo
  prima che `overlap-check` della lane sia obbligatorio.
- Costo di tenerlo: se Actions si ferma, i merge restano bloccati finché non torna. È fail-closed; il ripiego è il
  runbook di `ci_trusted_lane.md` §Emergenza.

## 4. Garanzie e semantica degli stati

- **Nessun falso PASS su un required check:**
  - GitHub accetta `success`, `skipped` e `neutral` per i check run. La lane pubblica **status**, che non hanno
    `skipped`.
  - `required_checks_plan.py evaluate` è più severo: vale solo `success` sullo SHA esatto. Mancante, pending, failure,
    error, skipped, neutral, un'altra app o un altro SHA bloccano.
- **SHA:** gli status e i check run sono legati al commit, e GitHub valuta quelli dell'head corrente. Con `strict` (F2)
  anche la base è quella aggiornata. Dispatcher e full-suite verificano lo SHA (`SHA mismatch`, `stale SHA`).
- **Stati della lane:** `pending` mentre gira, `success` o `failure` per l'esito, `error` quando l'esito non è
  verificabile (timeout, step mancanti, attesa oltre il limite). Per GitHub solo `success` sblocca.
- **Nessuna dipendenza circolare:**
  - ognuna delle due readiness esclude se stessa e l'altra (`READINESS_SELF_STATUS_CONTEXTS`,
    `_is_readiness_self_check`, `is_self_check`), ed entrambe **attendono** `full-suite`;
  - la full-suite non attende nessuna readiness;
  - lo verifica `test_no_readiness_waits_for_itself`.

## 5. Procedura di migrazione (per fase)

1. **Prima:** esportare il ruleset (`GET …/rulesets/25459867`) e salvarlo. È il punto di rollback.
2. `required_checks_plan.py diff --plan ops/ci/required_checks.json --ruleset <export>` deve dare OK. Se il ruleset
   reale non coincide con lo stato attuale del piano, ci si ferma.
3. Verificare la precondizione della fase. Per F1, l'ultima PR reale deve avere `full-suite` = `success` sullo SHA
   esatto, pubblicato da `pickfair-trusted-status`.
4. L'owner applica il payload della fase, aggiungendo prima di togliere, nella UI (Settings → Rules → Rulesets →
   PICKFAIR-MAIN-PROTECTION → Require status checks, con «source» = l'app indicata) oppure con l'API.
5. **Prova del blocco:** su una PR di prova, un required check in `failure` o mancante deve mostrare «Merging is
   blocked». Se GitHub non blocca, si fa subito il rollback.
6. Il piano si aggiorna: la fase applicata diventa lo stato `current`, e `diff` torna OK.

## 6. Rollback e recovery

- **Rollback:** riapplicare l'export salvato al punto 1, oppure il `payload` della fase precedente. Non tocca il codice.
- **Lane ferma con un required check della lane:** il merge resta **bloccato**, non bypassato. Ripristino con il
  dispatcher, eventualmente con un rerun (`/pf-trusted-rerun <sha>`), oppure con il runbook di
  [ci_trusted_lane.md](ci_trusted_lane.md) §Emergenza.
- Togliere temporaneamente un required check è **una decisione owner registrata**, mai un'azione silenziosa
  dell'agente.
- **App della lane revocata o ruotata:** l'`integration_id` resta quello dell'App. Una nuova App richiede di
  aggiornare il piano, e `diff` lo segnala.
