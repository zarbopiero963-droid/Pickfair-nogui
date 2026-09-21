# AI PR-review workflows

Quattro workflow GitHub Actions eseguono una review AI automatica delle Pull
Request di Pickfair-nogui. Sono **reviewer opzionali e diff-only**: aiutano a
individuare bug, regressioni e problemi di sicurezza, ma **non sostituiscono il
controllo umano** e **non approvano né mergiano** nulla.

## I quattro reviewer

| Workflow | Modello | Provider | Quando chiama il modello (costo) |
|---|---|---|---|
| `pr-review-openrouter-gpt56-sol.yml` | GPT-5.6 Sol | OpenRouter `chat/completions` | ogni push della PR |
| `pr-review-xai-grok46.yml` | Grok 4.6 | xAI API | ogni push della PR |
| `pr-review-openrouter-fugu-ultra.yml` | Sakana Fugu Ultra | OpenRouter | solo su push che tocca file **core o critici** oppure con label `final-fugu-review` |
| `pr-review-claude-fable5.yml` | Claude Fable 5 | Anthropic Messages API | solo su push che tocca file **core o critici** oppure con label `final-fable-review` |

I due reviewer "forti" (Fugu Ultra, Fable 5) sono più costosi: il gate di costo
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
  finale a label (Fugu/Fable) una review non eseguibile fallisce di proposito
  il job, così il check non risulta verde senza la review richiesta.

## Secret richiesti

Configurare in *Settings → Secrets and variables → Actions*:

| Secret (nome nel repo) | Provider | Usato da |
|---|---|---|
| `OPENROUTER_PICKFAIR` | OpenRouter | Fugu Ultra **e** GPT-5.6 Sol |
| `GROK_PICKFAIR` | xAI API | Grok 4.6 |
| `CLAUDE_PICKFAIR` | Anthropic API | Claude Fable 5 |

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
smentito con l'evidenza, e se arriva dai due reviewer forti costa un altro giro
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

## Label

- `final-fugu-review` / `final-fable-review`: attivano il gate finale del
  rispettivo reviewer forte sull'intera PR (pre-merge). **Le mette solo l'owner,
  o l'agente su sua autorizzazione esplicita, mai di iniziativa**: sono i due
  reviewer costosi e ogni lancio è spesa (vedi CLAUDE.md / AGENTS.md). Resta
  invece automatica — e non richiede autorizzazione — la partenza sui push che
  toccano file **core o critici**: quella è la rete di sicurezza.
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
> **definisce o applica i gate stessi**: i quattro file-policy e i sette
> workflow-gate (i quattro `pr-review-*.yml`, `ci-quarantine-guard.yml`,
> `pr-guard.yml`, `pr-merge-readiness.yml`), più `scripts/guardrail_check.py`
> aggiunto dalla #470 — `pr-guard` era già escluso, ma esegue lo script dal
> checkout della PR: escludere il contenitore lasciando fuori il contenuto non
> chiudeva niente. La lista autoritativa sta nella
> sezione «AUTO-MERGE» di `CLAUDE.md` / `AGENTS.md`: se questa nota e quella
> sezione divergono, vale la sezione.
>
> Resta il blocco se un reviewer **pagato** è in usage-quota sul head corrente:
> uno qualsiasi dei quattro, non solo i due forti a label.
