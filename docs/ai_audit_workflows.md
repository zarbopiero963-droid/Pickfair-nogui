# AI PR-review workflows

Cinque workflow GitHub Actions eseguono una review AI automatica delle Pull
Request di Pickfair-nogui. Sono **reviewer opzionali e diff-only**: aiutano a
individuare bug, regressioni e problemi di sicurezza, ma **non sostituiscono il
controllo umano** e **non approvano né mergiano** nulla.

## I cinque reviewer

| Workflow | Modello | Provider | Quando chiama il modello (costo) |
|---|---|---|---|
| `pr-review-openrouter-gpt56-sol.yml` | GPT-5.6 Sol | OpenRouter `chat/completions` | ogni push della PR |
| `pr-review-xai-grok46.yml` | Grok 4.7 | xAI API | ogni push della PR |
| `pr-review-openrouter-fugu-ultra.yml` | Sakana Fugu Ultra | OpenRouter | solo su push che tocca file **core o critici** oppure con label `final-fugu-review` |
| `pr-review-claude-fable5.yml` | Claude Fable 5.1 | Anthropic Messages API | solo su push che tocca file **core o critici** oppure con label `final-fable-review` |
| `pr-review-openrouter-gpt-astra.yml` | OpenAI GPT-6 Astra | OpenRouter | solo su push che tocca file **core o critici** oppure con label `final-astra-review` |

I tre reviewer "forti" (Fugu Ultra, Fable 5.1, GPT-6 Astra) sono più costosi: il
gate di costo
nello script chiama il modello **solo** quando il push tocca i file **core o
critici** del progetto, oppure quando si aggiunge la label finale (gate pre-merge
sull'intera PR). Su push che toccano solo workflow/docs/test il job parte ma
**non spende**.

### File "core o critici" che attivano i reviewer forti
- **Core** (`CORE_TRIGGER_PATTERNS`): `core/`, `services/`, `controllers/` e i
  moduli di root (`headless_main.py`, `mini_gui.py`, `betfair_client.py`,
  `betfair_market_api.py`, `order_manager.py`, `dutching.py`, `database.py`,
  `database_schema.py`, `trading_config.py`), più `requirements*.txt/.in/.lock`
  e `pyproject.toml`.
- **Critici** (`CRITICAL_PATTERNS`): `.github/workflows/`, dipendenze,
  betfair/telegram/parser, money management, dutching, safety, reconciliation,
  runtime, order_manager, catalog, e i pattern segreti/credenziali
  (secret/token/auth/certlogin/app_key/config/`.env`/chiavi private).
- **File di governance** (dalla #475): `CLAUDE.md`, `AGENTS.md`,
  `docs/auto_pr_flow_spec.md`, `docs/hard_verify_spec.md` e
  `scripts/guardrail_check.py`. Sono cinque dei dodici file a merge manuale
  dell'owner e prima non erano coperti da nessun pattern: una PR che toccasse
  solo loro non riceveva l'etichetta e non faceva girare i reviewer forti.
  Ancorati con `^` perché sono file precisi, non famiglie — un pattern largo
  su `docs/` o `scripts/` farebbe spendere a ogni ritocco di README o roadmap.

## Postura di sicurezza (comune a tutti e 4)

- **Diff-only**: niente `checkout` del codice della PR, niente esecuzione di
  codice della PR. Il diff è recuperato via GitHub Compare API (range
  `base...head` della PR).
- **`pull_request_target` + guard PR interne**: i workflow girano su
  `pull_request_target`, quindi il file `.yml` **eseguito proviene dal branch
  base** (fidato): una PR interna che modifica il workflow **non** può
  esfiltrare i secret (una PR su `pull_request` avrebbe eseguito la versione
  dell'head, con i secret). Le PR da **fork** sono escluse dal guard di job
  `if: head.repo.full_name == github.repository`, così **non ricevono mai** i
  secret nonostante `pull_request_target`. Essendo diff-only (nessun checkout
  del codice PR), `pull_request_target` qui non introduce il rischio classico di
  esecuzione di codice non fidato.
  > Nota: poiché la versione eseguita è quella del branch base, un workflow
  > **nuovo** inizia a girare solo dopo che è stato **merge-ato** sul base; sulla
  > PR che lo introduce non parte da solo.
- **Redazione segreti**: chiavi private, API key (OpenAI/OpenRouter/xAI/GitHub/AWS),
  token Telegram e coppie `key=value` sensibili vengono redatte **prima**
  dell'invio al modello e **prima** della pubblicazione del commento — inclusi i
  nomi file e l'**output** del modello.
  Quando la chiave apre la riga (`.env`, YAML, INI, properties, `export`/`set`,
  liste, commenti con `#`, `//`, `;`, `!`, `--`, `/*`, `<!--`), un valore
  **non quotato** viene redatto fino a fine riga, spazi
  compresi: prima si fermava al primo spazio e il resto del segreto usciva
  (DECISIONE-426 P14, #479). Resta la regola generica, che si ferma al primo
  spazio, dove dopo il valore c'è codice e toglierlo al reviewer sarebbe un
  verde falso: chiave a metà riga (argomenti, confronti, firme) e valore con
  forma di codice a inizio istruzione (chiamate, indici, annotazioni,
  argomenti: comincia con `(`, `lambda`, `await`, o con un nome seguito subito
  da `(`/`[` o da `,`/`;` a fine riga; dopo `:` anche un tipo built-in come
  `str`, `int`, `bool` seguito da ` = ` o ` | `). Gli operatori `+ - * / %`, e
  ` = `/` | ` dopo un nome qualsiasi, non contano come codice, perché stanno
  nei segreti codificati (base64, URL) e nelle passphrase. `==`, `:=` e `=>`
  non sono separatori: confronti e predicati restano alla regola generica.
  Misurato sul repo intero: 18 righe su 219.588 redatte più di prima (commenti,
  esempi, percorsi di test con `/`), nessuna di codice di produzione. Residui
  dichiarati: valore non quotato con spazi a metà riga, o che comincia con una
  di quelle forme di codice (per esempio un tipo built-in seguito da ` = `);
  valori su righe successive (blocchi YAML `|`); tabelle senza separatore.
  Accettati dall'owner (DECISIONE-426 P22 e P23), perché su una riga sola sono
  indistinguibili da un valore: a inizio istruzione vengono redatti per intero
  un'annotazione con tipo custom, un'espressione che comincia con un letterale
  o una f-string, una fixture quotata e un'assegnazione temporanea della shell
  prima di un comando. Anche una riga dell'output del modello che comincia con
  una di queste chiavi seguita da `:` viene redatta per intero.
- **Il diff è trattato come contenuto non attendibile**: il prompt istruisce il
  modello a non seguire istruzioni contenute in codice/commenti/stringhe/nomi
  file (anti prompt-injection); i fence ```` ``` ```` nel diff vengono
  neutralizzati.
- **Aree sensibili → controllo manuale**: se il diff tocca aree critiche
  (`CRITICAL_PATTERNS`: workflow, dipendenze, betfair/telegram/parser,
  money management, dutching, safety, reconciliation, runtime, order manager,
  segreti/credenziali/certlogin/config…) o la Compare API tronca la lista
  (>= 300 file), il workflow applica la label `manual-review-required` e segnala
  di non usare auto-merge.
- **Reviewer opzionali**: se il secret del provider non è configurato, il job
  esce con successo (skip) e **non** fa fallire la PR. Eccezione: sul gate
  finale a label (Fugu/Fable/Astra) una review non eseguibile fallisce di proposito
  il job, così il check non risulta verde senza la review richiesta.

## Secret richiesti

Configurare in *Settings → Secrets and variables → Actions*:

| Secret (nome nel repo) | Provider | Usato da |
|---|---|---|
| `OPENROUTER_PICKFAIR` | OpenRouter | Fugu Ultra, GPT-5.6 Sol **e** GPT-6 Astra |
| `GROK_PICKFAIR` | xAI API | Grok 4.7 |
| `CLAUDE_PICKFAIR` | Anthropic API | Claude Fable 5.1 |

> `PICKFAIR_OPENAI` **non serve più**: dal 2026-09-14 GPT-5.6 Sol gira su
> OpenRouter con la chiave che già esisteva per Fugu Ultra. La chiave OpenAI
> diretta era esaurita (`credit_balance_exhausted`, HTTP 429 su ogni push) e il
> suo rosso non era un difetto del codice — era rumore sul gate. Il segreto può
> essere rimosso dal repo.

> I workflow leggono il segreto tramite questi nomi (`secrets.OPENROUTER_PICKFAIR`,
> `secrets.GROK_PICKFAIR`, `secrets.CLAUDE_PICKFAIR`) e lo espongono allo script
> con una env var interna (`OPENROUTER_API_KEY` / `XAI_API_KEY` /
> `ANTHROPIC_API_KEY`): il nome del segreto e quello della env interna sono
> volutamente distinti.

`GITHUB_TOKEN` è fornito automaticamente da Actions. Per far pubblicare i
commenti al bot serve *Settings → Actions → General → Workflow permissions →
Read and write permissions*.

## Copertura del diff: quello che il modello NON ha visto

Un reviewer diff-only riceve le patch dei file, non il repository. Alcuni file
restano fuori dal prompt: **binari**, file **senza patch** nella risposta della
Compare API, e quelli che **non entrano nel budget** (`MAX_TOTAL_PATCH_CHARS`).

Quell'elenco è sempre stato pubblicato, ma **solo nel commento**, nella sezione
*«File non inviati al modello»* in fondo: cioè lo leggeva l'umano, dopo. Il
modello vedeva un diff più corto e nessun avviso — e per due PR consecutive ne
ha dedotto che il codice mancasse:

| PR | File nel diff | File non inviato | Cosa ha concluso il reviewer |
|---|---|---|---|
| #465 | 15 | `tests/scripts/test_codex_gate_parser.py` | Fugu e Fable: «non è nel diff (14 file)» |
| #466 | 6 | `tests/testsuite/test_false_green_semantics.py` | Grok, come **bloccante**: «non è nel range […] fail-closed non shippato» |

In tutti e due i casi il file c'era. Un bloccante falso non è gratis: va
smentito con l'evidenza, e se arriva dai tre reviewer forti costa un altro giro
a label, a pagamento.

Per questo `blocco_copertura()` mette la stessa informazione **nel prompt**,
dove la legge il modello prima di concludere: quanti file ha il range, quanti
ne sono arrivati, quali no, e come vanno letti — *un file non inviato è
**non verificabile**, non assente*. La stessa regola vale dove nel diff compare
`[PATCH FILE TRONCATO PER BUDGET TOKEN]`: di quel file si vede solo l'inizio.

Il blocco vive a **colonna 0**, come il resto del prompt: a runtime YAML strippa
l'indentazione del blocco `run: |`, ma non quella scritta dentro una stringa
Python. Un rientro di quattro o più spazi è la sintassi con cui si scrive un
blocco di codice, e un avviso che chiede di essere letto come istruzione non
deve somigliare a un listato — perciò l'assenza di rientro è un'invariante
verificata, non una questione di gusto.

Il guard `tests/guardrails/test_ai_review_copertura_diff.py` estrae la funzione
dai workflow veri e verifica sia il contenuto del blocco sia che il valore sia
davvero interpolato in `user_prompt` — una funzione che esiste ma non arriva al
modello lascerebbe il difetto intatto con un test verde sopra.

## Costi e anti-doppia-review

- Ogni reviewer stima e riporta i token usati e un costo indicativo nel commento.
- Il tetto di output (`MAX_OUTPUT_TOKENS`) è un **ceiling** anti-costo, non il
  costo reale: si pagano solo i token effettivamente generati; i prompt sono
  volutamente corti.
- Un `done_marker` per range evita di ripagare la stessa review su re-run del
  workflow o togli/rimetti label. Un nuovo push = nuovo range = nuova review.
- **Attesa del provider.** La richiesta al modello non è in streaming: la
  risposta arriva tutta alla fine, dopo il ragionamento. Ogni reviewer fa tre
  tentativi, ciascuno con un limite di attesa, e dopo ogni tentativo fallito
  aspetta 2, 4 e poi 8 s. Il limite è di 100 s per Fable e per Sol, di 120 s
  per Fugu e per Astra, di 240 s per Grok. Quello di Grok era 100 s ed è salito
  con la DECISIONE-426 P24: a reasoning `high` Grok 4.7 rispondeva spesso oltre
  i 100 s. Sulla #478 non ha mai completato, sulla #483 ha completato 1
  tentativo su 8. Aspettare di più non costa, perché si pagano i token
  generati. Il caso peggiore deve stare dentro il `timeout-minutes` del job: tre
  tentativi, le attese fra i tentativi, le chiamate a GitHub al loro tetto di
  30 s (sei per workflow) e un minuto di avvio del runner. Per Grok fa 974 s, e
  il suo job ha 20 minuti. Altrimenti GitHub interrompe il job prima del
  commento d'errore, e il reviewer tace. Lo verifica
  `tests/guardrails/test_ai_review_timeout.py` per tutti e cinque i reviewer,
  contando le chiamate a GitHub dal file e pretendendo esattamente tre
  tentativi.

## Label

- `final-fugu-review` / `final-fable-review` / `final-astra-review`:
  attivano il gate finale del
  rispettivo reviewer forte sull'intera PR (pre-merge). **Si applicano a OGNI
  PR, sempre, senza chiedere** (decisione dell'owner del 16-09-2026): il
  motivo non è il costo ma la leggibilità del gate — una PR dove le label non
  compaiono non si distingue, guardandola su GitHub, da una dove il gate è
  stato saltato. Sono i tre reviewer costosi e ogni lancio è spesa, ma grazie
  al `done_marker` per range il giro a label costa quasi sempre zero quando i
  forti hanno già pubblicato su quel range (vedi CLAUDE.md / AGENTS.md, che
  restano autoritativi). Resta automatica la partenza sui push che toccano
  file **core o critici**: quella è la rete di sicurezza.
- `manual-review-required`: applicata automaticamente quando il diff tocca aree
  sensibili o la Compare API è troncata.

> Nota: questi reviewer sono un filtro tecnico avanzato. **Nessuno di questi
> workflow esegue auto-merge o auto-approve** — si limitano a commentare e a
> marcare `manual-review-required`. Il merge NON è deciso qui: l'automazione
> dell'agente può eseguire un **auto-merge gated** solo dopo che tutti i gate
> documentati sono passati (vedi la sezione «AUTO-MERGE» in `CLAUDE.md` /
> `AGENTS.md`).
>
> Aggiornato con la decisione dell'owner del 16-09-2026 (#469): le PR
> safety-critical **non sono più escluse** dall'auto-merge — restano
> riconoscibili e vanno dichiarate nel verdetto, ma si mergiano ai cinque gate
> come le altre, e l'override per-issue che serviva a toglierle caso per caso è
> ritirato. L'esclusione a merge manuale dell'owner vale ora per ciò che
> **definisce o applica i gate stessi**: i quattro file-policy e gli otto
> workflow-gate (i cinque `pr-review-*.yml`, `ci-quarantine-guard.yml`,
> `pr-guard.yml`, `pr-merge-readiness.yml`), più `scripts/guardrail_check.py`
> aggiunto dalla #470 — `pr-guard` era già escluso, ma esegue lo script dal
> checkout della PR: escludere il contenitore lasciando fuori il contenuto non
> chiudeva niente. La lista autoritativa sta nella
> sezione «AUTO-MERGE» di `CLAUDE.md` / `AGENTS.md`: se questa nota e quella
> sezione divergono, vale la sezione.
>
> Resta il blocco se un reviewer **pagato** è in usage-quota sul head corrente:
> uno qualsiasi dei cinque, non solo i tre forti a label.
