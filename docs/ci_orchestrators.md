# Orchestratori CI (`ci-master-gate`, `ci-dynamic-intelligent`)

Stato al 20/10/2026, issue #525. Nessuno dei due è un required check di `main`
(su #526 la PR era `mergeable_state: clean` con entrambi rossi); la loro
configurazione in branch protection non è leggibile dall'integrazione
dell'agente (HTTP 403) e resta da verificare dall'owner.

## Cosa fanno

| Workflow | Trigger | Cosa chiama |
|---|---|---|
| `ci-dynamic-intelligent.yml` | `pull_request`, `workflow_dispatch`, `workflow_call` | `detect` (`scripts/ci_changed_modules.py origin/main`), poi sempre `noise-free-pr-gate`, `repo-guardrails-extended`, `merge-simulation-hard`; solo se `detect` le segnala toccate le 15 lane di modulo e `mutation-guardrails` (quest'ultima per diff su `guardrails/mutations/`, `scripts/run_mutation_guardrails.py`, `tests/`, `core/` o i moduli in radice elencati in `ci_changed_modules.py`) |
| `ci-master-gate.yml` | `pull_request`, `workflow_dispatch` | `ci-dynamic-intelligent`, più **senza condizioni** 11 lane di modulo (`trading-engine`, `order-manager`, `execution-guard`, `risk-middleware`, `runtime-controller`, `money-management`, `telegram-listener`, `copy-engine`, `simulation-broker`, `session-manager`, `rate-limiter`), `live-gate`, `chaos-critical`, `mutation-guardrails`, `merge-simulation-hard`. **`dutching` e `pnl-engine` non sono chiamate direttamente**: il master le raggiunge solo attraverso `ci-dynamic-intelligent`, quindi solo quando `detect` le segnala toccate; un run del master non le valida per diff fuori dai loro path |

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

## `mutation-guardrails`: mutation testing reale sul sorgente

**Il difetto (fino a `adcf208`).** Il vecchio `scripts/run_mutation_guardrails.py`
leggeva le 120 voci di `guardrails/mutations/*.json`. Per ciascuna scriveva un
blocco `_mutation_meta` nel JSON, impostava `PICKFAIR_MUTATION_*` e rilanciava
l'intera suite. Nessun file del prodotto o dei test leggeva quei valori, quindi
il codice non veniva mai mutato. Solo 19 bersagli su 120 corrispondevano a
funzioni esistenti. Ogni voce lasciava la suite verde e risultava
«sopravvissuta», con punteggio 0% contro una soglia dell'80%. Inoltre 120 suite
complete non stavano nei 45 minuti del job, che veniva interrotto
(`cancelled`): nei run 39386046315, 39386046149, 39387530346 e 39387530343
sulla #527. Il gate era rosso per costruzione, non un falso verde, ma non
misurava nulla.

**Il runner attuale.** Applica le mutazioni di `guardrails/mutation_catalog.json`:
31 voci, ciascuna con una riga intera del sorgente, la sua variante dannosa e i
test mirati che devono accorgersene. Le regole, verificate da
`tests/scripts/test_run_mutation_guardrails_reale.py`:

- ogni mutazione gira in una copia isolata dei file tracciati: il repository non
  viene mai toccato, e il file mutato viene ripristinato e ricontrollato con
  sha256 prima della mutazione successiva;
- la riga originale deve comparire esattamente una volta, altrimenti l'esito è
  `NOT_APPLICABLE`: se il codice cambia, il catalogo non può smettere in
  silenzio di colpirlo;
- se il sorgente mutato non compila, l'esito è `INVALID` e non conta come
  mutazione uccisa;
- i test mirati girano prima sul codice originale: se sono già rossi, l'esito è
  `INCONCLUSIVE`;
- pytest con rc 1 dà `KILLED`, rc 0 dà `SURVIVED`; qualunque altro rc
  (collezione fallita, nessun test, interruzione) o il timeout danno
  `INCONCLUSIVE`;
- le mutazioni equivalenti si dichiarano nel catalogo con la motivazione e non
  entrano nel punteggio.

Il punteggio è `KILLED / (KILLED + SURVIVED)`. Il gate fallisce (exit 1) nei
casi seguenti:
- nessuna mutazione valutabile;
- punteggio sotto `--fail-under 80`, soglia invariata;
- almeno un esito `INCONCLUSIVE`, `NOT_APPLICABLE` o `INVALID`;
- un'area di `required_areas` senza nessuna mutazione valutabile.

Senza mutazioni selezionate esce con 2. Le aree obbligatorie sono: `side`,
`lay_liability`, `exposure`, `dedupe`, `ambiguous`, `recovery`,
`reconciliation`, `cashout`, `funds`, `money_management`.

**Risultati.**
- Primo run: 24 mutazioni uccise su 31, punteggio 77,42%, gate FAIL.
- Sette sopravvissute senza nessun test che se ne accorgesse:
  - un `TimeoutError` non classificato come esito incerto;
  - un errore `ERROR_AMBIGUOUS` del client ignorato;
  - righe INFLIGHT non ricaricate dal DB al riavvio;
  - chiavi in volo non ripopolate nel motore;
  - FAILED/REJECTED dell'exchange mappato a UNKNOWN;
  - lato diverso da BACK/LAY accettato dall'order manager;
  - cap di esposizione che ignora l'esposizione già aperta.
- I test che le uccidono stanno in `tests/mutation/test_mutation_killers_525.py`.
- Run successivo: 31 su 31 uccise, punteggio 100%, gate PASS, in circa 40 s in
  locale contro gli oltre 45 minuti di prima.

I file `guardrails/mutations/*.json` restano specifiche per modulo, validate da
`scripts/guardrail_runner.py` e `scripts/validate_guardrails.py`, ma non sono
più la fonte delle mutazioni eseguite.

## Artifact `ultra-check-cache`

`_module-ultra-check.yml` carica `.pytest_cache` con un nome fisso, e nello
stesso run più lane chiamano quel workflow: `upload-artifact@v4` rifiuterebbe
il secondo caricamento con lo stesso nome. Oggi non succede: in tutti i job
`ultra-check` dei run qui sotto lo step scrive «No files were found with the
provided path: .pytest_cache. No artifacts will be uploaded», e l'unico
artifact del run è `changed-modules`. Il conflitto è latente: diventerebbe
reale solo se la suite tornasse a scrivere `.pytest_cache`.

## Prove su run reali

| Prova | Run | Esito |
|---|---|---|
| Prima della correzione (head `c57f565`, PR #526) | 39332257841, 39332259533 | `failure` all'avvio, 0 job |
| Avvio dopo la correzione (`a6961c2`, `workflow_dispatch`) | 39375135892 (master, 57 job), 39375138250 (dynamic, 20 job) | job partiti e conclusi; tutti verdi tranne `mutation-guardrails` (timeout) |
| Fallimento iniettato (`e78adfb`, test `assert False`, poi rimosso) | 39379518628, 39379521105 | `failure`: 8 `ultra-check` e i `merge-simulation-hard` rossi sullo step pytest |
