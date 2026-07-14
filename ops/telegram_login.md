# Login Telegram userbot (headless / VPS) — #367

Pickfair legge i segnali dai canali Telegram tramite un **account utente
(userbot)** via Telethon: serve una `session_string` autenticata. Un bot NON
basta (non può leggere i messaggi di canali di cui non è admin).

## Il fix (#367)

`TelegramListener.request_code`/`sign_in` erano **stub**: `sign_in` ignorava il
codice ed emetteva un finto `AUTHORIZED`, quindi il runtime restava
`session_not_authorized`. Ora fanno un **login Telethon reale**:

- `request_code(phone)` → `client.send_code_request(phone)` e tiene vivo il
  client (su un loop dedicato) per lo step successivo.
- `sign_in(code, password_2fa=None)` → `client.sign_in(phone, code, ...)`,
  gestisce la **2FA** (`SessionPasswordNeededError` → `sign_in(password=...)`),
  gli errori (`invalid_code`, `expired_code`, `invalid_password`), e al successo
  **ritorna la `session_string`** (`client.session.save()`) impostando anche
  `self.session_string`.

La GUI aveva già un login funzionante in `controllers/telegram_controller.py`
(`send_code`/`verify_code`), ma è GUI-only: sul VPS headless non c'era modo di
autenticarsi. Questo comando colma quel buco.

## Uso sul VPS: `--telegram-login`

`api_id`/`api_hash` si prendono da <https://my.telegram.org> → *API development
tools*. Precedenza di risoluzione:

- **`api_id`** (non è un segreto): flag `--api-id` → env `TELEGRAM_API_ID` → DB.
- **`api_hash`** (è un **segreto**): env `TELEGRAM_API_HASH` → DB → altrimenti
  **chiesto con input nascosto** (`getpass`). **Non** si passa da riga di comando:
  un `--api-hash` sarebbe visibile in `ps`/`/proc` e nella shell history.

Le credenziali fornite da CLI/env — **incluso l'`api_hash` digitato all'input
nascosto** — sono **persistite nel DB (cifrate) solo dopo un login riuscito**
(così credenziali errate non sovrascrivono quelle valide). Il salvataggio **non**
è silenzioso: se fallisce il login è comunque avvenuto ma il comando esce con
codice `2` (vedi «Exit code» sotto), così lo script chiamante se ne accorge; i
login successivi potrebbero richiedere di nuovo le credenziali.

```bash
# api_id via flag, api_hash chiesto con input nascosto (consigliato)
python headless_main.py --telegram-login --api-id 1234567

# api_id + api_hash via env — leggi il segreto SENZA scriverlo (niente shell history)
export TELEGRAM_API_ID=1234567
read -rs TELEGRAM_API_HASH; export TELEGRAM_API_HASH   # incolla l'hash: non viene mostrato
python headless_main.py --telegram-login
# in alternativa: un file d'ambiente a permessi ristretti (systemd EnvironmentFile / `set -a; . ./tg.env`)

# credenziali già nel DB (da GUI o da un login precedente)
python headless_main.py --telegram-login
```

> Il comando **non** legge `config.json` (per non incoraggiare segreti in un file
> committato): usa flag/env/DB o l'input nascosto.

### ⚠️ Dove arriva il codice + chiave di cifratura

- Il codice di verifica arriva **in-app** nella chat di servizio **"Telegram"
  (contatto ufficiale, id 777000)**, **non** via SMS, se hai già una sessione
  Telegram attiva (app sul telefono / Telegram Web). Guarda lì.
- `telegram.api_id`/`api_hash`/`session_string` sono **cifrati at-rest**
  (elenco `_SECRET_FIELDS` in `database.py`). La chiave è **caricata** (non
  derivata) da `PICKFAIR_SECRET_KEY` o dal file `~/.pickfair/db.key`
  (`SecretCipher`). **Salva e fai il login con lo stesso utente e dalla stessa
  directory** (il `pickfair.db` è relativo alla cwd), **senza `sudo`** (cambia
  `HOME` → chiave diversa → decifratura fallita → campi "vuoti"). In alternativa
  fissa la stessa `PICKFAIR_SECRET_KEY` per entrambi i passaggi. Nei log un
  mismatch appare come `secret_cipher: decrypt failed`.
- Usa il numero in **formato internazionale** (`+39...`). Richieste ripetute
  troppo ravvicinate → `FloodWaitError`: attendi i secondi indicati e riprova una
  sola volta. Il comando **riconosce il FloodWait** e stampa un avviso esplicito
  («aspetta N secondi, NON rilanciare»): rilanciare peggiora il flood **e invalida
  i codici precedenti** (causa tipica del "codice non valido" dopo molti tentativi).
- **Digita SOLO le cifre** del codice (es. `12345`). Se incolli l'intero messaggio
  di servizio (`Login code: 12345`) va bene lo stesso: le **sole cifre** vengono
  estratte. Usa **sempre l'ULTIMO codice ricevuto** — ogni nuovo invio invalida i
  precedenti.

> **Nota (F-1a):** sanitizzazione del codice e gestione del FloodWait valgono su
> **entrambi** i percorsi di login, **headless e GUI**. La GUI
> (`controllers/telegram_controller.py`) usa un proprio client Telethon e **non**
> passa da `TelegramListener`, quindi l'hardening è applicato in due punti che
> condividono l'unico helper `telegram_listener.sanitize_login_code`:
> - **codice → sole cifre ASCII**, estratte dopo il marcatore `code`/`codice` se
>   presente (così `777000 Login code: 54321` → `54321`; le cifre non-ASCII sono
>   scartate perché Telegram le rifiuta);
> - **FloodWait**: headless `request_code` ritorna `retry_after` (secondi, **può
>   essere `None`** se Telethon non lo popola); la GUI (`send_code` **e**
>   `verify_code`) mostra «attendi N secondi, non rilanciare» — o «qualche
>   secondo» quando i secondi non sono noti. Il testo d'attesa è generato
>   dall'unico helper `telegram_listener.format_floodwait_wait_text` (distingue
>   `None` da `0` con `is None`). Su FloodWait il client Telethon locale della
>   GUI viene comunque disconnesso (nessun socket orfano).

Flusso interattivo (`HeadlessApp._telegram_login_flow`):

1. Chiede il **numero di telefono** (es. `+39...`) → invia il codice.
2. Chiede il **codice di verifica** ricevuto su Telegram (**solo le cifre**;
   incollare `Login code: 12345` è tollerato → tiene `12345`).
3. Se l'account ha la **2FA**, chiede la **password** (input nascosto).
4. Al successo il flusso **ritorna la `session_string`**; è il comando
   (`_run_telegram_login`, via `_telegram_persist_login`) a **salvarla nel DB**
   (`save_telegram_settings`, merge sui settings esistenti, `enabled=True`) e a
   uscire con codice `0`.

Exit code: `0` login ok **e** configurazione salvata; `2` login fallito,
credenziali mancanti/non valide, **oppure login riuscito ma persistenza fallita**
(in quest'ultimo caso l'autenticazione è avvenuta ma la `session_string` non è
stata scritta: rifai il login o salva da GUI). Il comando **non** avvia il
runtime/trading: costruisce solo il DB e fa il login.

Dopo il login, riavvia in modalità normale: il listener userà la
`session_string` salvata (in `_runtime_async`, `is_user_authorized()` sarà True).

## Verifica

I test (`tests/unit/test_telegram_login.py`) coprono con un client Telethon
fittizio: request_code→sign_in con `session_string`, flusso 2FA, codice/password
invalidi, `request_code` mancante, e il flusso CLI (happy/2FA/credenziali
mancanti/invio-codice-fallito). La verifica **end-to-end con l'API Telegram
reale** (telefono + codice reale) va fatta **sul VPS**.
