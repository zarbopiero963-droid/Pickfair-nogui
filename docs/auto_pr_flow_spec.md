# Auto PR Flow — Specifica

> **⚠️ CODACY È DISMESSO (integrazione rimossa).** Questo documento è la
> specifica ATTIVA: le istruzioni operative su Codacy sono state **rimosse**,
> non marcate come storiche (rilievo Codex P2 su #462 — un passo prescrittivo
> lasciato in pagina viene eseguito, qualunque disclaimer ci sia sopra).
>
> Non esistono più: l'API Codacy (nessuna chiamata di rete nel repo), il comando
> `codacy-task`, il segreto `CODACY_API_TOKEN`, lo step Codacy nel workflow di
> merge readiness.
>
> **Contratto attuale, in una riga:** il check residuo della GitHub App non
> ancora disinstallata è escluso da `split_checks`
> (`is_decommissioned_codacy_check`) e non è **né un blocker né un pending**,
> qualunque sia il suo stato. Non si aspetta, non si classifica, non si patcha
> per lui.
>
> **L'esenzione è per NOME ESATTO**, non per sottostringa e **mai sull'URL**:
> solo i nomi in `DECOMMISSIONED_CODACY_CHECK_NAMES` (oggi
> `codacy static code analysis`). Un predicato "il nome o l'URL contiene
> codacy" è un vettore **fail-open** sul gate di merge — una guardia sulla
> dismissione, un workflow rinominato, o un check con un semplice link a
> codacy.com sparirebbe dai blockers anche da FAILURE (rilievo convergente di
> Fugu Ultra, Claude Fable 5 e Codex su #462). Un check con un nome diverso NON
> è esente: blocca, che è la direzione sicura.
>
> **Una sola identità per due percorsi.** `pr_flow_automation`
> (`is_decommissioned_codacy_check`, usato dal gate CI) e
> `pr_automation_controller` (`is_codacy_check`, usato dalle decisioni del
> controller) condividono la **stessa** costante: il flow la importa dal
> controller. Non è pignoleria: al primo giro era stato stretto solo il
> predicato del flow e quello del controller era rimasto largo — stesso
> fail-open, altro ingresso. Un test pinna che i due siano lo stesso oggetto e
> diano lo stesso verdetto.
>
> **Nessun gate pretende più evidenza Codacy**, perché non può più esistere e
> pretenderla significa bloccare per sempre: né il micro-audit finale
> obbligatorio (`should_run_final_micro_audit` non guarda più
> `codacy_classification`), né la risoluzione con evidenza dei thread Codacy
> storici (`triage_review_thread_contract` / `should_resolve_review_thread`).
> L'evidenza decommissionata **non dichiara** `api_ok`: affermare che un'API
> inesistente ha risposto bene sarebbe evidenza fabbricata.
>
> Di conseguenza **nessun gate pretende più evidenza da Codacy**: né la catena
> "DeepSource advisory", né `can_auto_merge`, né `_report_ready_to_merge`.
> Pretenderla da un servizio dismesso non sarebbe fail-closed: sarebbe un blocco
> permanente, superabile solo **inventando** l'evidenza — cosa che AGENTS.md
> vieta. Tutti gli altri gate restano **fail-closed e invariati**.

> Specifica del flusso automatico di gestione PR (orchestrator fail-closed).
> Implementazione di riferimento: `scripts/pr_automation_controller.py`,
> `scripts/pr_flow_automation.py`, `scripts/pr_merge_readiness.py`.

## 0. Contratto di autonomia end-to-end (owner 08/10/2026) — NORMATIVO

Questa sezione è il contratto operativo stabile per l'intera roadmap Pickfair +
Control API + #495 + MCP. Prevale sulle sezioni seguenti dove sono meno
specifiche; dove una regola successiva è più severa su un punto di sicurezza
che questa sezione non decide, resta valida quella più severa (l'auto-merge
delle PR safety-critical runtime/core sotto §0.9 è deciso qui e sostituisce
l'override per-issue). Enforcement eseguibile:
`scripts/pr_autonomy_policy.py` (puro, solo stdlib, fail-closed); test:
`tests/scripts/test_pr_autonomy_policy.py`.

### 0.1 Principio

L'owner decide **COSA** (prodotto e vincoli); l'agente decide **COME** (tecnica).

> L’agente non deve comportarsi come un esecutore che chiede conferma per ogni scelta tecnica.
>
> Deve comportarsi come il responsabile tecnico operativo del progetto, entro le decisioni owner e la roadmap.
>
> Quando il contratto e il risultato atteso sono già determinati, l’agente deve scegliere autonomamente la soluzione tecnica più sicura, implementarla, provarla, triagiarla, mergiarla se consentito e continuare.
>
> L’owner non deve essere coinvolto per risolvere problemi che il contratto già rende deterministici.
>
> L’agente deve chiedere l’owner solo quando la risposta cambierebbe il prodotto, il rischio accettato, il denaro, l’infrastruttura, le credenziali o una decisione owner esistente.

### 0.2 Ruoli

**OWNER** — autorità su prodotto, vincoli, denaro, modalità operative e decisioni aperte.

**AGENTE** — autorità tecnica operativa. PUÒ: scegliere l'implementazione;
correggere difetti; refactor minimo; aggiungere test e guardrail; rinviare per
policy (§0.4); chiudere finding teorici per policy (§0.6); scegliere la PR
successiva determinabile (§0.14); lavorare cross-repo dove consentito (§0.12);
auto-mergiare le PR non-policy quando tutti i gate passano (§0.9).

NON PUÒ: cambiare una decisione owner; cambiare il significato di SIM/LIVE;
cambiare STOP/PAUSE/EMERGENCY; inventare MANUAL via MCP; cambiare i limiti di
denaro; attivare LIVE da solo; acquistare servizi; creare infrastruttura a
pagamento; introdurre nuove modalità; aggirare gate di safety/security;
lasciar cadere un requisito owner.

### 0.3 Tassonomia a due assi

Origine/disposizione e severità sono assi **separati**.

Origine: `CURRENT_DEFECT_INTRODUCED_BY_PR`, `CURRENT_DEFECT_PREEXISTING`,
`CURRENT_DEFECT_OUT_OF_SCOPE`, `GUARDRAIL_GAP`, `THEORETICAL_MUTATION`,
`STALE`, `DUPLICATE`, `ALREADY_COVERED`, `KNOWN_LIMITATION_ACCEPTED_BY_OWNER`,
`REVIEW_CHURN`.

Severità: `P0_SECURITY`, `P0_REAL_MONEY`, `P1_SAFETY`, `P1_ARCHITECTURE`,
`P2_COMPLETENESS`, `P3_DOCS`.

Preesistente **non** significa automaticamente rinviabile. Raccordo con il
ledger del fix loop (§11–§12, `scripts/pr_fix_loop_policy.py`, invariato):
introdotto dalla PR o preesistente attivato/aggravato → `CURRENT_DEFECT` →
`PATCH_REQUIRED` (consuma un ciclo); ogni altra disposizione non consuma cicli.

| Origine | Azione |
|---|---|
| introdotto dalla PR | fix ora → test → niente merge finché aperto; owner solo se il fix richiede una nuova decisione di prodotto |
| preesistente / fuori scope / gap non introdotto | `DEFERRED_BY_POLICY` solo alle condizioni §0.4; altrimenti blocker corrente o prove da completare |
| teorico | §0.6 |
| stale | provato sul current head → resolve |
| duplicate | link al finding canonico → resolve |
| already covered | comportamento o test esistente mostrato → resolve |
| limitazione owner | link all'accettazione owner; mai per P0/`P1_SAFETY`; non è FIXED |
| churn | nessuna patch artificiale; risposta con prova → resolve |

### 0.4 `DEFERRED_BY_POLICY`

Ammesso solo se TUTTE valgono: provato su base/main; non introdotto, non
aggravato, non rende raggiungibile un percorso irraggiungibile; non necessario
all'accettazione e non compromette il percorso consegnato; esiste una scheda
futura canonica con owner tecnico; blocker registrato, registrazione riuscita e
riletta; nessuna decisione owner lo vieta.

Procedura: prova base/head → classificazione → `finding_id` → scheda futura →
blocker → commento nel thread → `DEFERRED_BY_POLICY` → resolve del thread.
**Mai** marcarlo FIXED. Un P1 preesistente segue le stesse condizioni; se la
PR lo attiva diventa blocker corrente.

### 0.5 Blocker strutturati

`BLOCKS_NEXT_PR`, `BLOCKS_LIVE`, `BLOCKS_MCP_MUTATION`,
`BLOCKS_DUTCHING_ACTIVATION`, `BLOCKS_CASHOUT_ACTIVATION`,
`BLOCKS_SIM_CERTIFICATION`, `BLOCKS_FINAL_CERTIFICATION`,
`BLOCKS_DISTRIBUTION`, `BLOCKS_CONCURRENT_OPERATION`,
`BLOCKS_SECURITY_ACCEPTANCE`. Un rinvio senza almeno uno di questi non vale.

### 0.6 `THEORETICAL_MUTATION`, P0 e P1

Teorico solo se TUTTE: nessun chiamante reale; nessun wiring; nessuna config
che lo renda raggiungibile; nessun cambio di comportamento; nessun impatto
safety/security/denaro; prova sul current head; nessuna dipendenza imminente
che lo attivi. Allora: nessuna patch → commento con prova → resolve → non
bloccante. Prova incompleta → non teorico: si riclassifica, non si risolve.

`P0_SECURITY` / `P0_REAL_MONEY`: nessun auto-defer generico; fail-closed;
si ferma il percorso interessato; niente merge se raggiungibile dalla PR.
L'agente corregge da solo se il fix resta nel contratto deciso. Owner solo per
cambio di contratto, accettazione del rischio, nuova credenziale/infra/costo o
due semantiche di prodotto plausibili.

### 0.7 Reviewer e validità delle review (#426/P41)

Attivi: **GPT-6.1 Sol** e **Grok 4.7**, a ogni push. **Codex** è advisory:
la sua assenza non è né PASS né blocker; i suoi rilievi si triagiano tutti con
prova. **SOSPESI** da P41: Fugu Ultra, Claude Fable 5.1, GPT-6 Astra (workflow
disattivati nella UI, file presenti): non si attendono, non si riattivano.

Una review conta solo se: è sul current head o il suo range finisce sul head;
porta il proprio marker di completamento; non ha bloccanti irrisolti (la
sezione «Bloccanti» è pulita solo se è interamente «Nessun bloccante
[evidente]»); è leggibile (non un errore). Schema/API incompleti → UNKNOWN.
Timeout Grok (solo segnali di timeout veri): un solo rerun; secondo timeout →
STOP owner («PRONTA PER MERGE — Grok assente per timeout»). Quota/crediti
(incluso `credit_balance_exhausted`) → STOP (P41, CREDITI ESAURITI). Altro
errore del provider → PROVIDER_ERROR, review non valida. Prima del merge:
zero thread irrisolti.

### 0.8 Label: safety-critical informativo, manuale bloccante

- **Safety-critical** (`CRITICAL_PATTERNS` dei workflow): INFORMATIVO. Il
  commento del reviewer elenca i file; review e hard verify completi sono
  obbligatori; da solo non blocca l'auto-merge.
- **`manual-review-required`**: blocker REALE. Sol e Grok la applicano solo
  tramite `MANUAL_AUTHORITY_PATTERNS` (copia identica di
  `scripts/pr_autonomy_policy.py`): file di autorità/governance (§0.9), tutti
  i `.github/workflows/`, manifest di dipendenze, materiale segreto, o diff
  troncato dalla Compare API. L'agente la applica inoltre per: decisione owner
  necessaria; stop esplicito; fix loop esaurito; accettazione del rischio;
  policy che lo richiede espressamente.
- Non si applica più a ogni PR runtime/core. Non si rimuove in modo
  opportunistico: decide il classifier (`manual_label_decision`) sul diff
  completo, e solo se nessun motivo owner è registrato.
- Fugu/Fable/Astra, sospesi, conservano la vecchia regola larga: riattivarli
  richiede prima l'allineamento a questa sezione.

### 0.9 Merge

**Merge manuale owner** (almeno): `AGENTS.md`, `CLAUDE.md`,
`docs/auto_pr_flow_spec.md`, `docs/hard_verify_spec.md`, i workflow reviewer,
`pr-merge-readiness.yml`, `ci-quarantine-guard.yml`, `pr-guard.yml`,
`scripts/guardrail_check.py` e gli script che definiscono autorizzazione e
autorità di merge (`scripts/pr_autonomy_policy.py`,
`scripts/pr_fix_loop_policy.py`, `scripts/pr_flow_automation.py`,
`scripts/pr_merge_readiness.py`, `scripts/pr_automation_controller.py`,
`scripts/pr_clean_scope_rebuild.py`, `scripts/pr_refresh_self_checks.py` e ogni
altro `scripts/pr_*.py`), `scripts/ci/check_ci_quarantine.py`; inoltre, per
fail-closed, ogni altro workflow e i manifest di dipendenze. La label
`manual-review-required` non blocca: instrada qui. Il merge live
dell'automazione (`pr_automation_controller.can_auto_merge`) richiede, oltre ai
gate generici, `merge_decision(autonomy_merge_state) == READY_FOR_AUTO_MERGE`:
prove assenti o qualunque altro esito → niente merge automatico.
Esito: `READY_FOR_OWNER_MANUAL_MERGE`.

**Auto-merge runtime/core** consentito quando TUTTE: head stabile; scope
valido; accettazione completa; suite PASS; hard verify PASS; Sol e Grok
settled senza bloccanti; Codex triagiato; thread irrisolti = 0; Merge
Readiness PASS; nessun P0/P1 introdotto; nessun preesistente attivato o
aggravato; nessuna decisione owner pertinente aperta; fix loop valido; nessuno
stop manuale; nessun file a merge owner toccato; nessuna violazione di
dipendenze. Allora `AUTO_PR_FLOW_STATUS=READY_FOR_AUTO_MERGE` e l'agente
mergia. Head cambiato → la valutazione si invalida e si rifà.

Limite noto #472 (rischio accettato dall'owner): l'agente mergia con la stessa
identità GitHub che apre la PR, quindi l'esclusione dei file a merge owner è
applicata dalla procedura e dalla label, non da un'identità separata.

### 0.10 Controllo sul diff completo

Sul diff ASSEMBLATO della PR (base...head, non l'ultimo push): base, head,
diff completo, path vietati, scope, nuovi chiamanti, dipendenze, regressioni.
`pr-guard.yml` esegue `python -I scripts/pr_autonomy_policy.py full-diff`:
BLOCK su materiale segreto presente (per path e per contenuto con forma di
segreto in qualunque file; in `tests/` una chiave non blocca solo se la riga
si dichiara sintetica — `SYNTHETIC`/`fake`/`dummy`/`placeholder` — e porta
comunque a merge owner), input del guard nel diff, scope ≠ diff, nuove
dipendenze di produzione non dichiarate; riporta file a merge owner,
safety-critical, nuovi chiamanti del percorso denaro e dipendenze solo-test.
Il report contiene solo path e codici, mai righe del diff. Confine di fiducia:
il checker gira dal checkout della PR come `guardrail_check.py`; una PR che
modifica `scripts/` o i workflow è sempre a merge owner (§0.8), quindi non può
indebolire il proprio controllo e auto-mergiarsi.
Nessun nuovo servizio, nessun costo.

### 0.11 Readiness fail-closed

Struttura mancante, paginazione incompleta, schema inatteso o errori →
UNKNOWN/NEEDS_MANUAL, mai PASS. `fetch_all_review_threads` solleva su
`errors` GraphQL, livelli mancanti, `nodes` non lista, `pageInfo` assente,
`hasNextPage` senza `endCursor` o cursore ripetuto; readiness e report
riportano `review_threads_api_unavailable` e `can_merge=false`.

### 0.12 Roadmap, #495, Control API, MCP, #497

- Vale la roadmap autoritativa corrente (PR26 → … → MCP-07) nella catena
  [#491](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/491) →
  #489 → #461 → #426/#453 → #351 → pickfair-mcp- #1 e in
  [docs/mcp_operational_contract.md](mcp_operational_contract.md). Qui non si duplica.
- #495: le slice vanno nelle schede corrette; nessuna seconda roadmap; nessun
  bypass di PR26–31; nessuna mutazione MCP prima dell'authority; MCP non parla
  mai direttamente con Betfair.
- Control API: si procede in autonomia quando determinata. Non si cambiano:
  no-bypass, loopback in prima fase, auth/scopes, `request_id`,
  `operation_id`, handshake, SIM/LIVE.
- MCP: una PR per repository; due PR parallele si classificano
  `SAFE_PARALLEL` / `DEPENDENT` / `FORBIDDEN_PARALLEL`
  (`parallel_classification`). MCP è solo adapter: niente Betfair, niente DB
  Pickfair, niente credenziali Betfair, niente MM/risk/cashout/reconciliation
  duplicati (`full-diff --repo-kind mcp`).
- #497 resta DEFERRED: nessun servizio persistente, VPS, DB remoto o costo. Si
  estraggono solo controlli minimi autorizzati; una funzione che richiede
  l'intero #497 fallisce chiusa; nessuna persistenza finta.

### 0.13 Decisioni owner e STOP minimi

**Chiuse** (non si richiedono di nuovo; fonte tra parentesi): SIM Delayed +
SimulationBroker, LIVE reale, parità stesso motore, mai auto-LIVE
(mcp_operational_contract §SIM/LIVE, #426/6026322035); STOP/PAUSE/EMERGENCY/
RESUME (contratto §Contratti prodotto vigenti, P38); Telegram rinviabile non
rimosso; master/copy/follow non via MCP; UX per nome; una PR per repository
(#426/6026322035); superficie Betfair personale completa (#426/6043516714);
vendor/developer/subscription esclusi (#426/6043516714); P35 attesa/nessuna sostituzione; Void P39; solo calcio
P40; limiti di denaro P37/P02; Sol + Grok (P41); fix loop 5 (CLAUDE.md
§PR-flow, #496); limitazioni #471/#472 (registri di limite noto/rischio
accettato, #489); nessun costo senza owner (#497).

**Aperte** (mai inventarle): MANUAL via MCP; updater Caso B; nuove semantiche
di prodotto; costi/servizi/infrastruttura; limiti di denaro; modalità
operative. Se una decisione aperta tocca solo una sotto-parte, si blocca solo
quella sotto-parte.

**STOP owner** solo per: `NEW_PRODUCT_DECISION`, `SPEND_OR_INFRASTRUCTURE`,
`CREDENTIAL_OR_SERVICE`, `NEW_P0_NOT_FIXABLE_WITHIN_CONTRACT`,
`CONTRACT_CHANGE_OR_RISK_ACCEPTANCE`, `MANUAL_VIA_MCP`,
`FIX_LOOP_EXHAUSTED`, `SUBSTANTIVE_SOURCE_CONFLICT`,
`AUTHORIZATION_OR_GATE_NOT_VERIFIABLE`, `REAL_ACTION_REQUIRES_OWNER_COMMAND`
(soprattutto LIVE). Un'incertezza puramente tecnica non è uno STOP: l'agente
sceglie la soluzione più sicura, la prova e la documenta.

### 0.14 PR successiva dopo ogni merge

Verificare che il merge sia avvenuto davvero e il suo SHA; rileggere main;
nessuna PR aperta nello stesso repository; rileggere roadmap e nuove decisioni;
individuare la scheda successiva; verificarne le dipendenze; se nessuno STOP,
avviare Phase 0 in automatico (`next_pr_decision`). Una scheda già
soddisfatta riceve evidenza, non una PR artificiale.

### 0.15 Fix loop

Cap 5, cumulativo per PR, nessun reset (§12, invariato). `REVIEW_CHURN` non
riceve patch artificiali. Cap esaurito → STOP. Deroga owner solo per quella
PR e limitata; mai un cambio globale 5→6/7.

---

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
  "unresolved_active": 0,
  "current_head_sha": "..."
}
```

Questa evidence serve per automatizzare il ragionamento che abbiamo fatto a mano su PR256:

- branch protection assente
- => DeepSource Python non è required
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
- review active=0
- => non bloccare

BLOCK:

- branch protection API errore
- required_checks malformati
- DeepSource required=true
- DeepSource required current-head failing=true
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

Apertura di una PR new-task — registrazione task key OBBLIGATORIA:
prima/insieme all'apertura, la task key va registrata in
`.guardrails/allowed_scope.json` (oggetto `tasks`: `{ "<key>": { "files":
[...], "max_files": N, "allow_tests": bool } }`) nello STESSO PR. Il
check `guard` (`scripts/guardrail_check.py`) valida che il marker
`[TASK: <key>]` (da titolo/body/commit) sia una chiave REGISTRATA: se il
marker è presente ma la chiave non è nel registro, fallisce con
"Unknown TASK tag ... must be one of configured task keys". Il file
`.guardrails/allowed_scope.json` non è critico, quindi la PR può
includerlo nei propri `files` e auto-registrarsi: il guard legge il
registro dal merge-ref e passa sullo stesso head.

**Dalla #470 il guard non si ferma al marker: applica anche lo scope.**
Quattro vincoli, tutti fail-closed, tutti verificati contro la copia del
registro estratta dal branch base (`allowed_scope_base.json`, che il
workflow produce da `github.event.pull_request.base.sha`):

1. i `files` della task key devono **coincidere esattamente** col diff
   della PR — un file toccato e non dichiarato blocca, e blocca anche
   uno dichiarato e non toccato (scope riservato per dopo);
2. la task key deve avere una **entry nel registro**. Vale anche per il
   task sintetico `task_file_change` e per le famiglie-prefisso
   (`audit_`, `ci_`, …), che prima passavano senza registrazione:
   nessuno scope dichiarato = scope illimitato;
3. la entry del task deve differire da quella sul base **in un campo
   che porta autorizzazione** — `files` o `description` — o è nuova, o
   è aggiornata qui. Una entry identica significa riusare
   un'autorizzazione concessa a un lavoro diverso; e non basta che sia
   *diversa*: bumpare `max_files` o girare `allow_tests` la rende
   diversa senza concedere nulla di nuovo, e prima bastava a passare
   (P1 Codex, riprodotto su `exposure_total_clamp_m06` con `max_files`
   da 3 a 4). `files` è confrontato come insieme, così un riordino
   della lista non si spaccia per una modifica;
4. `default` e le entry di **altri** task non si toccano: riscrivono
   ciò che era stato concesso altrove, quindi è sempre need-manual.

Il workflow inoltre **rifiuta di partire** se il checkout contiene già
`pr_meta.json`, `pr_files_raw.json` o `allowed_scope_base.json`. Sono i
tre input che lo step di metadata genera dentro il checkout, cioè dentro
contenuto della PR: una PR che include uno di quei nomi come **symlink a
`scripts/guardrail_check.py`** fa seguire il link alla scrittura e
sostituisce il guard prima che venga eseguito — il JSON generato è un
dict display Python valido, quindi il guard "gira" uscendo 0 senza
produrre il report né validare nulla (P1 Codex, riprodotto: bypass
totale, check verde). `python -I` non difende da questo, perché isola
ciò che il guard importa e non impedisce che il guard venga rimpiazzato.

Se i `files` della PROPRIA chiave cambiano rispetto al base, deve
cambiare anche la `description`: l'estensione di scope va dichiarata.
Limite noto e dichiarato: il guard verifica che la `description` sia
cambiata, non che dica il vero, e per una chiave **nuova** il confronto
non avviene fra un push e l'altro (Issue #471).

`max_files` resta **non** applicato: imposto `files` == diff, è già il
numero del diff. `.github/workflows/*` non è bloccato in quanto tale —
va dichiarato come ogni altro file.

Se AUTO_PUSH_ENABLED=false:

```
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=push_disabled
NEXT_ACTION=human_review_and_push
```

Il design vieta push, resolve, rerun e merge di default; ogni azione esterna richiede flag specifica e gate passati.

---

## 9-bis. Quando il gate «Merge readiness» esprime il giudizio

Il gate gira **solo** su `pull_request` (piu' `workflow_dispatch` per la
rivalutazione manuale). E' l'unico evento il cui esito si attacca all'head della
PR: `check_run` e `workflow_run` rivalutavano nel contesto del branch di
default, quindi pubblicavano il verdetto su `main` — invisibile sulla PR, e con
una scia di run rosse su `main` (24 su 27, misurato sulla #463). Sono stati
rimossi: non si rimettono.

Poiche' la run parte ~20s dopo il push, quando i check dell'head sono ancora in
volo, il gate **aspetta** che siano settled prima di decidere:

| chiave | valore | significato |
|---|---|---|
| `--wait-pending-seconds` | `6600` | budget d'attesa; `0` = non aspetta (default dello script) |
| `--poll-seconds` | `15` | intervallo fra due letture |
| `timeout-minutes` (job) | `120` | rete di sicurezza se la suite si blocca |

Il budget e' tarato sul **reviewer piu' lento**, non sulla suite media: deve
coprire il `timeout-minutes` del job di review piu' lungo (oggi Sol, 105 minuti a
effort `max`) piu' il ritardo con cui quel job parte rispetto al gate. Era 900 s
fino al passaggio di Sol a GPT-6.1 Sol a `max` (decisione dell'owner del
07-10-2026), e gia' allora era sotto il job di Grok (20 minuti): una review
valida ma lenta lasciava il gate rosso. Lo verifica
`tests/guardrails/test_ai_review_timeout.py`, che legge il budget da
`pr-merge-readiness.yml` e i `timeout-minutes` dai cinque `pr-review-*.yml`.

**Fail-closed.** Scaduto il budget si giudica lo stato REALE: se i check non
sono finiti, `can_merge` resta falso e il gate FALLISCE. L'attesa serve a dare
un giudizio vero, non a fabbricare un verde.

**Anti-stallo.** Il self-check del gate non compare mai fra i `pending`
(`split_checks(..., ignore_self=True)`): senza quella esclusione l'attesa
sarebbe un deadlock — il gate aspetterebbe se stesso fino al timeout. E'
inchiodata da `tests/guardrails/test_merge_readiness_verde.py`.

**Quando si decide.** Non basta che `pending` sia vuoto: si esce dall'attesa
solo quando (a) nessun check e' pendente, (b) i check reali osservati sono piu'
di zero e (c) il loro numero NON e' cambiato fra due letture. La terza
condizione copre la finestra di registrazione: se un check veloce e' gia' verde
mentre gli altri non sono ancora comparsi nel rollup, `pending` e' vuoto e
`checks_seen` vale 1 — senza (c) il gate direbbe «pronta» con la suite ancora
da partire. Il criterio si auto-calibra: nessuna soglia da aggiornare quando si
aggiunge o si toglie un workflow.

La stabilita' si conta **solo a `pending` vuoto**: un conteggio fermo mentre la
suite gira dice che i check ci sono gia' tutti, non che il rollup sia completo.
Senza quel vincolo il contatore arrivava a N durante l'attesa e il gate usciva
nell'istante in cui l'ultimo pending diventava verde, senza mai osservare la
finestra DOPO. Costa due poll (~30s) su un gate che ne impiega ~300.

La stabilita' va verificata su **`INTERVALLI_STABILI_RICHIESTI` = 2** intervalli
di poll consecutivi, non su uno solo: con un intervallo, un ritardo di
registrazione piu' lungo di `--poll-seconds` basterebbe a far uscire il gate sul
plateau iniziale. **Limite dichiarato:** nessun valore di N elimina la finestra,
la stringe soltanto. Eliminarla davvero richiederebbe l'elenco dei check ATTESI,
che invecchierebbe a ogni workflow aggiunto o tolto — e un manifest stantio
produce falsi ROSSI sistematici, un danno peggiore del rischio che chiude.

**Fail-closed anche sul timeout.** Uscire dal ciclo per budget scaduto non e'
come uscirne perche' il rollup si e' stabilizzato: se la stabilita' non e' mai
stata confermata, il verdetto viene forzato a NON pronto con un reason
esplicito. Senza, un `pending` momentaneamente vuoto mentre i check continuano
a comparire produrrebbe un verde su suite incompleta — fail-open proprio nel
ramo che deve reggere quando le cose vanno male.

**`workflow_dispatch`: lanciarlo SUL BRANCH DELLA PR.** La run si attacca al ref
su cui viene lanciata: dal branch di default il verdetto finisce su `main`,
cioe' lo stesso difetto che questa modifica toglie per `check_run`/`workflow_run`.
Per una rivalutazione manuale che serva a qualcosa va scelto il branch della PR,
non `main`.

**Limite operativo dichiarato.** Tolti `check_run`/`workflow_run`, se il budget
scade con check ancora in volo il verdetto resta rosso sull'head e NON si
rivaluta da solo: serve `workflow_dispatch` (input `pr_number`, lanciato sul
branch della PR) o un nuovo push. E' una scelta, non una svista: rimettere quei trigger significherebbe
ripubblicare il verdetto su `main` invece che sull'head — il difetto che teneva
il gate rosso 24 volte su 27. Il rischio residuo e' limitato perche' il budget
(6600s) copre il caso peggiore del job di review piu' lungo, mentre la suite
osservata dura ~322s, e perche' un budget scaduto con check fermi e' un caso in
cui il rosso e' la risposta giusta.

**Il riepilogo legge solo chiavi che la decisione produce.** Il job summary
estrae i suoi dati con `jq` da `decision.json`. Una chiave sbagliata non rompe
nulla: `jq` restituisce `null` e la riga esce VUOTA — un check rosso escluso
sparisce dalla vista di chi legge, senza un solo errore. E' successo davvero
(`jq '.ignored | length'` contro `ignored_self_checks`, trovato da Claude Fable 5
sulla #462). Il contratto e' inchiodato da
`tests/scripts/test_pr_flow_automation.py`, che costruisce la decisione VERA col
codice di produzione e verifica ogni `.chiave` letta dal workflow.

Il guard legge il programma `jq` PER INTERO, righe di continuazione comprese. La
prima versione si fermava alla prima riga (`jq [^\n]*?'\.(chiave)`) e vedeva 7
chiavi su 10: i due programmi multi-riga del riepilogo erano coperti solo per la
prima chiave. Distingue inoltre il livello, perche' `jq` cambia documento quando
itera: in `.ignored_self_checks[]? | select(.state)` la prima chiave e' di primo
livello, `state` appartiene all'elemento — trattarle allo stesso modo darebbe
falsi rossi su `.name` e `.decommissioned`, che chiavi della decisione non sono.
**Limite dichiarato:** il guard verifica che la chiave ESISTA al livello giusto,
non che il suo tipo sia quello che il filtro si aspetta.

---

## 10. CHECK_STATUS — legge PR dopo push

Dopo push o dopo PR aperta aggiornata, legge:

- bad checks
- pending checks
- cancelled checks
- DeepSource status
- Merge readiness
- PR flow guardrails
- review active
- unresolved_active
- headRefOid
- mergeable
- mergeStateStatus

Se un check è in progress:

```
AUTO_PR_FLOW_STATUS=CHECKS_PENDING
NEXT_ACTION=wait
```

Quando tutti i check sono settled e verdi:

```
NEXT_ACTION=review_triage
```

**Codacy non entra in questa lettura.** Il servizio è dismesso: il check
residuo della GitHub App viene ESCLUSO da `split_checks`
(`is_decommissioned_codacy_check`, match per **nome esatto**) e non è né un
blocker né un pending. Non attenderlo, non classificarlo, non patchare per lui.

Per lo stesso motivo il planner di rerun passivo non ha più il blocker
`codacy_not_green`: `build_next_action_context` scrive sempre
`codacy.github_codacy_state`, quindi quel gate si sarebbe accodato per sempre
(Codacy ignorato dalla readiness ma ancora capace di impedire il rerun).

La policy dice di non patchare mentre i check sono in progress salvo bug
current-head riproducibile.

---

## 11. REVIEW_TRIAGE — classifica commenti e bot

Fonti OBBLIGATORIE del triage, a ogni check-in: oltre ai thread
inline vanno letti **anche** i corpi delle review e i commenti di
conversazione della PR. I rilievi "outside diff range" (es.
CodeRabbit) non compaiono come thread inline e non arrivano via
webhook: vivono solo nel corpo della review. Leggere solo i thread
=> triage incompleto.

La classificazione distingue la **classe del finding** dalla **decisione**.
Non dipende dal nome del reviewer (Sol/Grok/Codex/qualsiasi provider).
Leggere il current head, riprodurre il finding e confrontare il contratto owner.
Il testo del reviewer, il nome del provider o parole come “security/coverage”
non attestano da soli un difetto corrente.

| Classe | Current head errato? | Rischio materiale? | Default |
|---|---:|---:|---|
| `CURRENT_DEFECT` | sì | qualsiasi | `PATCH_REQUIRED` entro scope/budget |
| `GUARDRAIL_GAP` safety/contract-critical | no | alto, dimostrato | `PATCH_REQUIRED` entro scope/budget |
| `GUARDRAIL_GAP` non-critical | no | basso/medio | `NEEDS_MANUAL` / limite accettato dall'owner |
| `THEORETICAL_MUTATION` | no | non dimostrato | `SKIP` / `NEEDS_MANUAL`, nessuna patch automatica |
| stale/duplicate/already-covered | no | no | `EVIDENCE_RESOLVE` con prove e gate §13 |
| scope forbidden / decisione owner | n/a | n/a | `NEEDS_MANUAL` |

Questa tabella governa il **budget di patch**. La disposizione finale del
finding (rinvio, teorico, stale, P0, blocker strutturati) segue la tassonomia a
due assi di §0.3–§0.6: `THEORETICAL_MUTATION` con tutte le prove di §0.6 si
risolve con evidenza senza patch e senza owner.

`CURRENT_DEFECT`: bug presente, contratto autorevole contraddittorio, route
errata, requisito owner violato, test che dimostra regressione, fail-open o
rischio safety/security/real-money corrente. Fuori scope/budget o con una
scelta owner necessaria → `NEEDS_MANUAL`, mai un falso verde.

`GUARDRAIL_GAP`: contenuto corrente corretto ma un test/checker manca una
**regressione futura concreta**. Valutare rischio, safety/security/real-money,
criticità contrattuale, probabilità, copertura esistente, costo/churn e budget.
Solo una lacuna materialmente importante autorizza `PATCH_REQUIRED`.

`THEORETICAL_MUTATION`: contenuto corrente corretto, nuova parafrasi, sinonimo,
spelling/casing o variante grammaticale senza difetto/rischio corrente né
regressione introdotta. Non è automaticamente `PATCH_REQUIRED`.
Nessun checker regex/documentale deve riconoscere ogni possibile frase umana
prima che una PR possa terminare.

DeepSource rimane advisory salvo required failing current-head o prova di
un bug reale/safety/fail-open/violazione contratto. Il triage legacy può
segnalare parole sospette, ma non concede il permesso di patch: la prenotazione
richiede una valutazione verificata, strutturata e legata al current head.
Input `review_assessments[thread_id]`: `class`, `thread_id`,
`current_head_sha`, `current_head_correct` booleano e `evidence`; eventuali
`material`, `contract_critical`, rischi, `scope_forbidden`,
`owner_decision_required`. Sono risultati dell'ispezione dell'orchestratore,
**non campi da copiare dal payload/prosa di un reviewer**. Mancata prova → manuale.
Prima di usare una valutazione cached, verificare lo stato corrente del thread:
resolved, inactive o outdated non può produrre una nuova patch dalla vecchia
valutazione. Il classifier lo considera inattivo; non viene dichiarato FIXED
né risolto di nuovo automaticamente sulla sola cache.

---

## 12. FIX_LOOP — ripete solo per blocker reali

Se trova:

```
REVIEW_TRIAGE_RESULT=PATCH_REQUIRED
```

entra nel fix loop.

Ma non fa una patch per ogni commento. Raggruppa per cluster:

- cluster: required-check evidence bug
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

### Budget definitivo e persistenza

```text
MAX_FIX_LOOP_ITERATIONS_PER_PR = 5
```

Massimo cinque passaggi **patch → push** originati da `PATCH_REQUIRED` per PR.
Conteggio cumulativo: nuovo commit/head, finding risolti, reviewer/provider,
CI verde, progresso, cluster o rerun non lo azzerano. La pubblicazione iniziale
di un nuovo task non è un ciclo di riparazione. Un push di un cluster consuma
un ciclo; il retry di trasporto dello stesso push non ne consuma un secondo.

Prima della patch prenotare un ciclo nel ledger transazionale di automazione
`FixLoopLedger` (`scripts/pr_fix_loop_policy.py`). Non è il DB Pickfair.
Prima di consegnare un task di patch o modificare file, acquisire
`claim_patch_gate`: `reserved → working` con capability esclusiva conservata
nel contesto del worker. Un altro worker con la prenotazione originale non
può acquisire il ciclo. Il clean-rebuild acquisisce questa capability prima
di cambiare branch/restaurare file e la conserva fino al gate commit/push.
Identità `(repo, PR)`; storico iniziale attestato + righe `cycles` completate
costituiscono il contatore. Ogni nuova prenotazione è associata al branch PR
verificato dall'orchestratore (`pr_branch` nell'assessment).
Il push confronta la destinazione effettiva con quella immutabile
prima di ogni comando; la completion verifica lo stesso branch. Metadati del
branch mancanti/incoerenti richiedono verifica manuale, mai deduzione dal
branch di checkout o da prosa reviewer.
Prenotazioni incompiute occupano un posto e bloccano nuovi
cicli: errore di push, response lost o crash → recupero manuale, mai rimborso
/reset automatico. Dopo push confermato, `push_with_retry_once` registra
lo SHA del branch effettivamente inviato una sola volta. La prenotazione è legata
allo SHA dell'assessment: un contesto relativo a un altro head non autorizza
patch/commit/push. Il ledger registra `reserved → working → pushing → completed`:
`pushing` viene persistito **prima** dell'invio, quindi un response lost/crash
non rende riutilizzabile automaticamente la prenotazione. Soltanto un rifiuto
non-fast-forward confermato abilita l'unico retry di trasporto già previsto,
con lo stesso ciclo e nuove prove di audit/head; nessun nuovo slot. Il ciclo passa
a `retry_ready`, mai di nuovo a `reserved`: patch/commit/push ordinari non lo
possono acquisire, neppure durante l'audit del retry. Il retry acquisisce
atomicamente `retry_ready → pushing` dopo il suo audit completo. Prove retry
assenti, stale o respinte lasciano il ciclo non ripetibile automaticamente.
Il percorso clean-rebuild acquisisce il gate atomico prima del commit.
Ogni percorso di push richiede la capability `working` del worker; il salto
diretto `reserved → pushing` è vietato. Il clean-rebuild confronta l'head
effettivamente scaricato con quello dell'assessment, ripristina file dallo SHA
scaricato immutabile e usa una lease esplicita su quello SHA per il push.
Il fetch del clean rebuild usa refspec espliciti `refs/heads/` verso
`refs/remotes/origin/`, anche per main: un tag omonimo non può lasciare
obsoleto il riferimento confrontato prima della patch. Anche il retry del
push usa una lease esplicita sullo SHA dell'assessment; il fetch non cambia
il valore atteso. Se un collaboratore ha avanzato il remoto, il retry si
ferma in `NEEDS_MANUAL` senza sovrascriverlo; serve nuova verifica sul nuovo
head prima di qualsiasi ulteriore riparazione, mantenendo il budget cumulativo.
Entrambi i percorsi fissano lo SHA locale prima del push e usano quello stesso
SHA nel refspec e nella completion; un branch che avanza durante l'invio non
cambia l'evidenza registrata.
Anche il push diretto del clean-scope rebuild usa intent e completion comuni.
Reinvocare una completion identica è idempotente;
una completion nuova richiede lo stato `pushing`: non può dichiarare inviato
un ciclo ancora soltanto prenotato o in lavorazione.
riusare una prenotazione completata per una nuova patch è vietato.

Ledger in storage persistente dell'orchestratore **fuori dai checkout/artefatti
CI temporanei**. Non crearne uno vuoto a ogni run. Inizializzazione esplicita
soltanto per PR nuova senza repair push, oppure dopo ricostruzione verificata
dell'intera storia. `initialize` non sovrascrive una PR già registrata.
Stato assente/corrotto, identità incoerente o ciclo mancante → `NEEDS_MANUAL`.
Sono validati anche tipi/range del contatore, coerenza stage/head/claim e prove
dei grant owner. `reserved` non contiene un claim; tutti gli stati successivi,
incluso `completed`, richiedono la capability acquisita. Nessuna completion
retroattiva può fingere che un vecchio protocollo avesse una capability.
Per migrare uno storico di protocollo precedente: ricostruire e archiviare le
righe originali/prove nella storia attestata, mantenere il totale consumato e
ogni prenotazione incompiuta. Soltanto una migrazione esplicita verificata può
importare quelle vecchie completion nel contatore storico; niente conversione
automatica, reset, rimborso o claim inventato. Righe del protocollo corrente
e grant restano immutabili dopo completion.
Uno SQLite leggibile con righe invalide non è uno stato valido.
Errori dopo l'intent, inclusi response lost e risoluzione ref fallita, riportano
stop manuale. I branch locali sono risolti con `refs/heads/`, senza ambiguità
con tag omonimi.
Il vecchio JSON del controller e il conteggio per autore/messaggio dei commit
sono diagnostica, **non** il contatore autorevole.

`PR_FIX_LOOP_LEDGER` indica il medesimo ledger ai preflight/report del controller.
Il controller si ferma in `NEEDS_MANUAL` anche quando il ledger è assente o
il suo snapshot è respinto. Un semplice preflight/readiness a conteggio 5 non
richiede un override: il limite blocca la prenotazione del sesto repair.
Preflight/controller generali si fermano anche davanti a un ciclo incompiuto:
non pianificano altro lavoro o merge sulla sola leggibilità del ledger. Il
worker già titolare della capability usa i propri gate di fase, non quel
preflight generale, per completare il ciclo. `status.allowed` indica soltanto
la validità dello snapshot; non autorizza patch, push, resolve o merge.
L'autorizzazione `can_auto_merge` respinge sia gli stop manuali/budget ricevuti
sia snapshot incompleti/incompiuti. Ogni autorizzazione richiede identità PR completa
(`repo`/`pr` o nel descrittore `fix_loop`) e una sorgente ledger: rilegge
sempre il ledger autorevole prima di consentire il merge. Una
cache verde non può nascondere un ciclo incompiuto o un ledger assente.
Una nuova lettura verde non cancella uno stop manuale esplicito già nel
contesto. Restano richiesti tutti i normali gate di merge; count=5 con nessun
ciclo incompiuto non richiede una deroga per la sola readiness.
Il contesto di riparazione porta `fix_loop = {path, repo, pr, cycle_id}` ai gate
Phase 0/patch, autofix, post-fix audit/commit e push. Per un task già associato
a una PR, passare sempre l'identità PR (o `PR_NUMBER`): ometterla per fingere
una pubblicazione iniziale viola il contratto operativo. Tutte le riparazioni
passano dai gate; il vecchio launcher del supervisor, assente su main, non
viene dispatchato né riattivato perché privo di enforcement del nuovo budget.
La pianificazione passiva/dry-run non concede permessi di mutazione.

Al tentativo del sesto ciclo:

```text
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=fix_loop_budget_exhausted
```

Vietati nuova patch, commit, push, resolve che finga una correzione e merge
automatico del problema ancora aperto. Cinque cicli completati senza finding
residui non invalidano le prove né bypassano/alterano Merge Readiness.

### Anti-churn

Contratto/codice corretto + nuove mutation equivalenti nella stessa area senza
nuovo current defect = `REVIEW_CHURN`; niente altro fix-loop automatico.
Il ledger conserva osservazioni per area, anche tra run/head differenti.
Può fermarsi in `NEEDS_MANUAL` prima del quinto ciclo. A budget esaurito:

```text
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL
REASON=review_churn_or_fix_loop_budget_exhausted
```

Un nuovo difetto corrente dimostrato resta bloccante; non viene nascosto come
churn. Il cap non è un'autorizzazione a ignorare safety/security o contratti.

### Deroga owner bounded

L'owner della repository deve scrivere nella PR un commento dedicato contenente
soltanto questa riga esplicita (niente esempi citati o testo aggiuntivo):

```text
OWNER_FIX_LOOP_OVERRIDE PR=495 ADDITIONAL=1 AT_COUNT=5
```

L'ID commento viene letto via API GitHub autenticata; verificare autore owner,
repository, PR e URL. Con count=5, abilita esattamente il sesto ciclo;
ceiling=6. La settima iterazione richiede una **nuova** decisione puntuale.
Commento e ceiling restano registrati in `grants`; rigiocare l'ID non aggiunge
budget. Nessuna modifica del cap globale 5, nessun reset, nessuna trasferibilità
ad altra PR, nessun `IGNORE_FIX_LOOP_LIMIT=true`.

CLI operativa (`--ledger`, `--repo`, `--pr` obbligatori):

```bash
python scripts/pr_fix_loop_policy.py status --ledger /persistent/pr-flow/budget.sqlite --repo owner/repo --pr 495
python scripts/pr_fix_loop_policy.py reserve --ledger /persistent/pr-flow/budget.sqlite --repo owner/repo --pr 495 --cycle repair-1 --assessment-file /trusted/current-head-assessment.json
python scripts/pr_fix_loop_policy.py override --ledger /persistent/pr-flow/budget.sqlite --repo owner/repo --pr 495 --owner-comment-id 123456
```

`initialize --historical-count N --history-evidence TEXT` è bootstrap esplicito,
non parte del rerun automatico. `complete --cycle ID --head SHA` è recupero
esplicito dopo verifica del push; normalmente lo esegue il gate di push.

---

## 13. EVIDENCE_RESOLVE — risolve solo con prove

### Limite noto accettato: non è una correzione

`KNOWN_LIMITATION_ACCEPTED_BY_OWNER` significa limite tecnico reale, current
head corretto, assenza di current defect e scelta owner esplicita di non
ampliare la copertura. Non dichiarare `FIXED`, non inventare test/evidence.
Esempi: regex/checker incompleto ma sufficiente, mutation teoriche, advisory
non material, costo/churn sproporzionati.

L'owner deve annotare nella PR un commento dedicato con soltanto questa riga
(decisione legata a thread e head):

```text
KNOWN_LIMITATION_ACCEPTED_BY_OWNER PR=495 THREAD=PRRT_example HEAD=<current-head-sha>
```

Il planner legge `owner_decision_ids[thread_id]` dalla API autenticata e
verifica identità/URL/owner, classificazione verificata, head corrente,
validazione, test pertinenti e tutti i check settled/verdi. Richiede
`material is False`, `contract_critical is False` e ogni flag di rischio
protetto esplicitamente `False`:
un campo mancante è evidenza sconosciuta, mai assenza di rischio. Solo allora può
rispondere con il disposition e il link owner e risolvere formalmente il thread.
Le condizioni comuni di resolve restano identiche. Non si pretende che il
limite sia corretto o stale. L'assenza del reviewer da una allowlist non
trasforma la decisione owner in una correzione né determina la classe.

Sicurezza/credenziali/real-money/fail-open/corruzione dati/bypass risk gate,
violazioni owner e bug runtime dimostrati non possono essere silently accepted.
Default `NEEDS_MANUAL`; il percorso automatico di accepted limitation rifiuta
rischi protetti/material e current defect, anche con un commento owner generico.
Una vera decisione owner su questi rischi richiede gestione manuale esplicita.

La readiness legge ancora i thread reali: unresolved diventa 0 solo dopo la
risoluzione formale. Nessun ignore-all-review/ignore-Codex/ignore-unresolved,
force-green o bypass safety. Reviewer/merge restano #426/P41 e decisioni
successive; policy files richiedono merge manuale owner.


Se un thread è stale/advisory/già coperto:

```
REVIEW_TRIAGE_RESULT=EVIDENCE_RESOLVE
```

può rispondere e risolvere solo se:

- AUTO_RESOLVE_ENABLED=true
- current head combacia
- validation passata
- TUTTI i check current-head SETTLED: nessun check in
  PENDING/QUEUED/IN_PROGRESS/WAITING/REQUESTED/EXPECTED/UNKNOWN/null —
  i bot pubblicano rilievi solo a check completato, quindi il resolve
  definitivo aspetta l'intero rollup (vedi CLAUDE.md check-completion gate)
- test/evidence coprono il commento
- nessun blocker attivo sullo stesso tema

Se AUTO_RESOLVE_ENABLED=false:

```
NEXT_ACTION=human_evidence_resolve
```

Quindi prepara testo e prove, ma non risolve automaticamente. Le regole vietano di risolvere thread "perché sembra risolto"; servono head SHA, validation ed evidenza dai check.

---

## 14. Rerun readiness / guardrails

Rerunna readiness/guardrails solo quando:

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
- required checks non bloccanti
- DeepSource/Semgrep/security gates non blocking
- current head match
- PR non draft
- mergeStateStatus pulito o accettabile

L'orchestrator prevede READY_TO_MERGE solo con bad vuoti, pending vuoti e unresolved_active 0. (Il vincolo storico «Codacy success» non esiste più.)

READY_TO_MERGE della readiness è **una** delle condizioni di §0.9, non il
merge. L'esito finale è uno solo fra:

```
AUTO_PR_FLOW_STATUS=READY_FOR_AUTO_MERGE          # tutte le condizioni §0.9, nessun file a merge owner: l'agente mergia
AUTO_PR_FLOW_STATUS=READY_FOR_OWNER_MANUAL_MERGE  # tutte le condizioni, ma tocca file a merge owner: decide l'owner
```

L'automazione non presidiata non mergia da sola (`AUTO_MERGE_ENABLED=false`
resta il default del controller): il merge lo esegue l'agente dopo aver
verificato §0.9 sul current head.

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
- rilievo architetturale che richiede una decisione di prodotto (quelli
  tecnici li decide l'agente come `P1_ARCHITECTURE`, §0.3)
- provider sconosciuto non classificabile
- checks in progress senza bug riproducibile
- test failure non recuperabile
- scope troppo largo
- segreti/credenziali, o un'azione reale (LIVE) che richiede un comando owner
- merge conflict unsafe
- una delle condizioni STOP owner di §0.13

Lavoro su runtime/Betfair/Telegram dentro lo scope del task NON è di per sé
NEEDS_MANUAL: si applicano Phase 0, review e i gate di §0.9.

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
14. Le review attive sono zero?
15. DeepSource è required o advisory?
16. Branch protection è assente o presente?
17. I required checks includono DeepSource Python?
18. Readiness può passare?
20. Devo patchare, evidence-resolve, skippare o fermarmi?

### Metadati branch e scope verificati

Ogni gate di fase richiede un branch PR esplicito e coerente fra `pr_branch`,
`branch` e `headRefName` quando presenti. Nessun alias presente può essere
nullo o discordante. Anche le operazioni dirette del ledger di claim,
authorize, start-push e completion rifiutano branch mancante o errato.
Authorize/start-push richiedono anche lo SHA corrente attestato; ometterlo
non consente di saltare il confronto con l'head ispezionato.
La CLI completion richiede `--branch` corrispondente alla prenotazione.

Il triage applica il divieto default di `core/*` e `services/*`: soltanto una
lista `files_allowed` esplicita del task può autorizzare quei file. Un
`files_forbidden` esplicito resta prevalente; i gate di scope esistenti restano.
Un GUARDRAIL_GAP critico ma dichiarato non materiale si ferma per decisione
manuale; non viene etichettato noncritical né accettato automaticamente.

### Primo push e fallimenti dopo l'intent

Anche il primo push usa una lease esplicita sullo SHA remoto attestato nella
assessment. Prima dell'invio verifica che lo SHA attestato sia antenato dello
SHA locale fissato: la lease non autorizza riscritture non fast-forward. Un
avanzamento remoto, anche già incorporato localmente, richiede nuove prove;
fetch/merge non amplia la lease. Il solo retry confermato resta nello stesso ciclo.

Ogni path proposto in review richiede un `files_allowed` esplicito del task.
Scope mancante non autorizza patch: questo protegge ogni modulo prodotto,
configurazione e package senza indovinare nome, estensione o categoria.
I divieti espliciti continuano a prevalere.

Clean rebuild: dopo l'intent persistito, errore di commit/ref/push/completion
produce `AUTO_PR_FLOW_STATUS=NEEDS_MANUAL`, motivo tracciato, `next_action=needs_manual`
e uscita nonzero. Il ciclo resta incompiuto fino a verifica/recovery manuale;
nessun replay, refund o completion inventata. La stessa indicazione manuale
viene preservata quando un gate budget/ledger respinge la chiamata.
