# AI PR-review workflows

Quattro workflow GitHub Actions eseguono una review AI automatica delle Pull
Request di Pickfair-nogui. Sono **reviewer opzionali e diff-only**: aiutano a
individuare bug, regressioni e problemi di sicurezza, ma **non sostituiscono il
controllo umano** e **non approvano né mergiano** nulla.

## I quattro reviewer

| Workflow | Modello | Provider | Quando chiama il modello (costo) |
|---|---|---|---|
| `pr-review-gpt55.yml` | GPT-5.5 | OpenAI Responses API | ogni push della PR |
| `pr-review-openrouter-glm52.yml` | GLM 5.2 | OpenRouter | ogni push della PR |
| `pr-review-openrouter-fugu-ultra.yml` | Sakana Fugu Ultra | OpenRouter | solo su push che tocca file **core** oppure con label `final-fugu-review` |
| `pr-review-claude-fable5.yml` | Claude Fable 5 | Anthropic Messages API | solo su push che tocca file **core** oppure con label `final-fable-review` |

I due reviewer "forti" (Fugu Ultra, Fable 5) sono più costosi: il gate di costo
nello script chiama il modello **solo** quando il push tocca i file core del
progetto, oppure quando si aggiunge la label finale (gate pre-merge sull'intera
PR). Su push che toccano solo workflow/docs/test il job parte ma **non spende**.

### File "core" che attivano i reviewer forti
`core/`, `services/`, `controllers/` e i moduli di root
(`headless_main.py`, `mini_gui.py`, `betfair_client.py`,
`betfair_market_api.py`, `order_manager.py`, `dutching.py`, `database.py`,
`database_schema.py`, `trading_config.py`), più `requirements*.txt/.in/.lock`
e `pyproject.toml`.

## Postura di sicurezza (comune a tutti e 4)

- **Diff-only**: niente `checkout` del codice della PR, niente esecuzione di
  codice della PR. Il diff è recuperato via GitHub Compare API.
- **Solo PR interne**: guard `head.repo.full_name == github.repository`. Su
  `pull_request` (non `pull_request_target`) le PR da fork non ricevono i
  secret.
- **Redazione segreti**: chiavi private, API key (OpenAI/OpenRouter/GitHub/AWS),
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

| Secret | Usato da |
|---|---|
| `OPENAI_API_KEY` | GPT-5.5 |
| `OPENROUTER_API_KEY` | GLM 5.2, Fugu Ultra |
| `ANTHROPIC_API_KEY` | Claude Fable 5 |

`GITHUB_TOKEN` è fornito automaticamente da Actions. Per far pubblicare i
commenti al bot serve *Settings → Actions → General → Workflow permissions →
Read and write permissions*.

## Costi e anti-doppia-review

- Ogni reviewer stima e riporta i token usati e un costo indicativo nel commento.
- Il tetto di output (`MAX_OUTPUT_TOKENS`) è un **ceiling** anti-costo, non il
  costo reale: si pagano solo i token effettivamente generati; i prompt sono
  volutamente corti.
- Un `done_marker` per range evita di ripagare la stessa review su re-run del
  workflow o togli/rimetti label. Un nuovo push = nuovo range = nuova review.

## Label

- `final-fugu-review` / `final-fable-review`: attivano il gate finale del
  rispettivo reviewer forte sull'intera PR (pre-merge).
- `manual-review-required`: applicata automaticamente quando il diff tocca aree
  sensibili o la Compare API è troncata.

> Nota: questi reviewer sono un filtro tecnico avanzato. Il merge resta
> **manuale** e umano; nessun workflow qui esegue auto-merge o auto-approve.
