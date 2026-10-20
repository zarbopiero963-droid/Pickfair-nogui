# Orchestratori CI (`ci-master-gate`, `ci-dynamic-intelligent`)

Stato al 20/10/2026, issue #525. Nessuno dei due è un required check di `main`
(su #526 la PR era `mergeable_state: clean` con entrambi rossi); la loro
configurazione in branch protection non è leggibile dall'integrazione
dell'agente (HTTP 403) e resta da verificare dall'owner.

## Cosa fanno

| Workflow | Trigger | Cosa chiama |
|---|---|---|
| `ci-dynamic-intelligent.yml` | `pull_request`, `workflow_dispatch`, `workflow_call` | `detect` (`scripts/ci_changed_modules.py origin/main`), poi sempre `noise-free-pr-gate`, `repo-guardrails-extended`, `merge-simulation-hard`; solo se `detect` le segnala toccate le 15 lane di modulo e `mutation-guardrails` (quest'ultima per diff su `guardrails/mutations/`, `scripts/run_mutation_guardrails.py`, `tests/`, `core/` o i moduli in radice elencati in `ci_changed_modules.py`) |
| `ci-master-gate.yml` | `pull_request`, `workflow_dispatch` | `ci-dynamic-intelligent` più tutte le lane di modulo **senza condizioni**, `live-gate`, `chaos-critical`, `mutation-guardrails`, `merge-simulation-hard` |

Le lane di modulo chiamano `_module-ultra-check.yml`: esistenza del modulo e
dei file guardrail, validazione JSON, `scripts/guardrail_runner.py`, test
mirati e infine la suite completa (`pytest -q`) come backstop.

Profondità massima del grafo: 4 livelli
(master → dynamic → modulo → `_module-ultra-check`).

## Il guasto (#525)

GitHub accetta `uses: ./.github/workflows/<file>.yml` solo se il file
richiamato dichiara `on: workflow_call`. `dutching.yml`, `pnl-engine.yml` e
`live-gate.yml` non lo dichiaravano: GitHub rifiutava **l'intero** workflow
chiamante all'avvio, con 0 job e nessun log. Risultato: 100 run su 100 falliti
per entrambi gli orchestratori; nessun job è mai partito.

Correzione: `workflow_call` aggiunto ai tre file, senza togliere i loro trigger
diretti (`push`/`pull_request` con filtro `paths`).
`tests/guardrails/test_orchestratori_ci_525.py` segue il grafo delle chiamate a
partire dagli orchestratori e fallisce se un workflow richiamato non dichiara
`workflow_call` o non esiste.

## Smart skip delle lane di modulo

Undici lane saltano `ultra-check` e passano verdi con un `skipped-report` se il
file del modulo non esiste. È legittimo solo per `session_manager.py`,
`rate_limiter.py` e `copy_engine.py`, assenti dal repository. Lo stesso test
fallisce se uno degli altri moduli sparisce (rename o cancellazione), così lo
skip non diventa un verde silenzioso; e fallisce se uno dei tre assenti
compare, perché da quel momento la sua lane deve girare davvero.

## Limite noto: `mutation-guardrails` non è un mutation test

`scripts/run_mutation_guardrails.py` legge le 120 mutazioni di
`guardrails/mutations/*.json`, per ciascuna scrive un blocco `_mutation_meta`
nel JSON, imposta `PICKFAIR_MUTATION_*` e rilancia l'intera suite. Nessun file
del prodotto o dei test legge quei valori: il codice non viene mutato. Ogni
mutazione lascia quindi la suite verde e risulta «sopravvissuta», il punteggio
sarebbe 0% contro una soglia dell'80%, e 120 esecuzioni della suite completa
non stanno nei 45 minuti del job, che viene interrotto (`cancelled`).

È rosso per costruzione, non un falso verde, ma non è una misura attendibile.
Finché l'owner non decide come trattarlo (vedi #525), `ci-master-gate`, che lo
chiama senza condizioni, resta non verde anche su un head sano;
`ci-dynamic-intelligent` resta non verde solo quando il diff tocca i path che
attivano la lane (ad esempio `tests/` o `core/`), mentre un diff di sola
documentazione o di soli workflow può chiudersi verde.

## Artifact `ultra-check-cache`

`_module-ultra-check.yml` carica `.pytest_cache` con un nome fisso, e nello
stesso run più lane chiamano quel workflow: `upload-artifact@v4` rifiuterebbe
il secondo caricamento con lo stesso nome. Oggi non succede: in tutti i job
`ultra-check` dei run qui sotto lo step scrive «No files were found with the
provided path: .pytest_cache. No artifacts will be uploaded», e l'unico
artifact del run è `changed-modules`. Il conflitto è latente: diventerebbe
reale solo se la suite tornasse a scrivere `.pytest_cache`. Il mutation
testing reale delle PR resta quello eseguito dall'agente sul codice mutato.

## Prove su run reali

| Prova | Run | Esito |
|---|---|---|
| Prima della correzione (head `c57f565`, PR #526) | 39332257841, 39332259533 | `failure` all'avvio, 0 job |
| Avvio dopo la correzione (`a6961c2`, `workflow_dispatch`) | 39375135892 (master, 57 job), 39375138250 (dynamic, 20 job) | job partiti e conclusi; tutti verdi tranne `mutation-guardrails` (timeout) |
| Fallimento iniettato (`e78adfb`, test `assert False`, poi rimosso) | 39379518628, 39379521105 | `failure`: 8 `ultra-check` e i `merge-simulation-hard` rossi sullo step pytest |
