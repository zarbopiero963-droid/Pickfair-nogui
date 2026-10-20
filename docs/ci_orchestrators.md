# Orchestratori CI (`ci-master-gate`, `ci-dynamic-intelligent`)

Stato al 20/10/2026, issue #525. Nessuno dei due è un required check di `main`
(su #526 la PR era `mergeable_state: clean` con entrambi rossi); la loro
configurazione in branch protection non è leggibile dall'integrazione
dell'agente (HTTP 403) e resta da verificare dall'owner.

## Cosa fanno

| Workflow | Trigger | Cosa chiama |
|---|---|---|
| `ci-dynamic-intelligent.yml` | `pull_request`, `workflow_dispatch`, `workflow_call` | `detect` (`scripts/ci_changed_modules.py origin/main`), poi sempre `noise-free-pr-gate`, `repo-guardrails-extended`, `merge-simulation-hard`; solo se `detect` le segnala toccate le 15 lane di modulo e `mutation-guardrails` (quest'ultima per diff su `guardrails/mutations/`, `guardrails/mutation_catalog.json`, `scripts/run_mutation_guardrails.py`, `tests/`, `core/` o i file mutati dal catalogo e gli altri moduli in radice elencati in `ci_changed_modules.py`) |
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
65 voci (36 della #525, 29 della #523 su Dutching e bet manuale), ciascuna con una riga intera del sorgente, la sua variante dannosa e i
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
`reconciliation`, `cashout`, `funds`, `money_management`, `live_safety`
(kill switch, `live_enabled`, readiness, checklist del gate LIVE, quota del
cashout nel safety layer).

Il report di `mutation-guardrails.yml` si carica come `mutation-guardrails-report-<check_run_id>`: il master lancia la lane due volte nello stesso run (direttamente e tramite dynamic), e `upload-artifact@v4` rifiuta un secondo artifact con lo stesso nome. Ogni `file` e `tests` del catalogo deve essere un percorso relativo senza `..`, altrimenti il catalogo è rifiutato: la sandbox non si può scavalcare.

Al timeout il runner termina l'intero gruppo di processi del pytest, anche ciò
che i test hanno lanciato. All'avvio scrive un report FAIL, così un run
interrotto non lascia valido il report di un run precedente.
`scripts/guardrail_runner.py --run-mutations` considera riuscito il delegato
solo con exit 0 e `gate_passed: true`. Prima un exit diverso da zero veniva
tollerato se `total > 0`. Il default di `--mutation-timeout-sec` passa da 1 a
180 secondi.

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
- Run successivo: 31 su 31 uccise.
- Review indipendente: chiede di coprire anche il safety layer e il gate LIVE.
  Ho aggiunto 5 mutazioni `live_safety`; una sopravviveva (quota del cashout
  ≤ 1 accettata dal safety layer) ed è uccisa da un nuovo test nello stesso
  file.
- Run finale: 36 su 36 uccise, punteggio 100%, gate PASS, in circa 44 s in
  locale contro gli oltre 45 minuti di prima.
- #523: 29 mutazioni nuove su Dutching e bet manuale (lato senza default nel
  controller e nel middleware, alias di lato in conflitto, liability LAY del bet
  manuale, barriera cashout del lotto e runtime senza barriera, ammissione fuori dal
  lock, errore e chiave altrui dell'anti-duplicato, identificativo storico
  riammesso, gamba di un lotto registrato sovrascritta, gamba rilasciata dal
  proprio esito, AMBIGUOUS non marcato, lotto ritirato non tolto dal registro,
  gamba fuori registro dopo un riavvio, ref a monte "dut-" scambiato per una gamba,
  sweep F028 sul tavolo del lotto, chiave del lotto non fissata, identificativi del
  bet manuale non validati; ciclo A2: `market_id` e `selectionId` del Dutching non
  validati, `precheck` che fissa la chiave, gamba di un lotto invalidato piazzata,
  fence delle gambe sorelle non controllati; ciclo A3: stake calcolato della gamba
  non validato; ciclo A4: lato del lotto che prevale sul lato esplicito della gamba
  nel middleware, due varianti):
  29 su 29 uccise dai test di `tests/acceptance/test_issue523_dutching_manual_bet.py`
  (catalogo completo: 65 su 65).

**Gap tracciato, non escluso in silenzio.** Ogni report elenca in
`summary.spec_modules_without_mutations` i moduli che hanno una specifica in
`guardrails/mutations/` ma nessuna mutazione reale nel catalogo:
- non esistono nel repository: `copy_engine`, `rate_limiter`,
  `session_manager`;
- non ha importatori nel codice di produzione: `core.execution_guard`;
- senza mutazioni reali per ora: `core.order_router`, `core.risk_desk`,
  `pnl_engine`, `shutdown_manager` (`core.risk_middleware` ha una mutazione
  dalla #523: lato della gamba Dutching);
- Telegram, sospeso: `telegram_listener`, `telegram_sender`,
  `telegram_signal_processor`, `telegram_bet_resolver`.

Le aree finanziarie critiche richieste dall'owner (BACK/LAY, responsabilità,
esposizione, dedupe, AMBIGUOUS, recovery, riconciliazione, cashout) sono
coperte; per i moduli qui sopra la copertura resta da estendere.

Un limite noto del criterio: rc 1 indica qualunque test fallito, non solo
un'asserzione. La review indipendente ha controllato le uccisioni reali e
ognuna viene da un'asserzione sul contratto.

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
