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

`api_id`/`api_hash` (da <https://my.telegram.org> → *API development tools*)
si passano con i flag CLI **oppure** via env, e vengono **salvati nel DB
(cifrati) al primo uso** — così i login successivi non li richiedono. Precedenza:
**flag CLI → env → DB**.

```bash
# opzione A — flag CLI (li salva nel DB cifrati per le volte successive)
python headless_main.py --telegram-login --api-id 1234567 --api-hash abcdef0123...

# opzione B — variabili d'ambiente
TELEGRAM_API_ID=1234567 TELEGRAM_API_HASH=abcdef0123... python headless_main.py --telegram-login

# opzione C — se già nel DB (via GUI o da un login precedente)
python headless_main.py --telegram-login
```

> Il comando **non** legge `config.json` (per non incoraggiare segreti in un file
> committato): usa i flag/env o il DB.

### ⚠️ Dove arriva il codice + chiave di cifratura
- Il codice di verifica arriva **in-app** nella chat di servizio **"Telegram"
  (contatto ufficiale, id 777000)**, **non** via SMS, se hai già una sessione
  Telegram attiva (app sul telefono / Telegram Web). Guarda lì.
- `api_id`/`api_hash`/`session_string` sono **cifrati at-rest**
  (`database.py` `_ENCRYPTED_KEYS`) con una chiave derivata da
  `PICKFAIR_SECRET_KEY` o da `~/.pickfair/db.key`. **Salva e fai il login con lo
  stesso utente e dalla stessa directory** (il `pickfair.db` è relativo alla cwd),
  **senza `sudo`** (cambia `HOME` → chiave diversa → decifratura fallita → campi
  "vuoti"). In alternativa fissa la stessa `PICKFAIR_SECRET_KEY` per entrambi.
  Nei log un mismatch appare come `secret_cipher: decrypt failed`.
- Usa il numero in **formato internazionale** (`+39...`). Richieste ripetute
  troppo ravvicinate → `FloodWaitError`: attendi i secondi indicati e riprova una
  sola volta.

Flusso interattivo (`HeadlessApp._telegram_login_flow`):

1. Chiede il **numero di telefono** (es. `+39...`) → invia il codice.
2. Chiede il **codice di verifica** ricevuto su Telegram.
3. Se l'account ha la **2FA**, chiede la **password** (input nascosto).
4. Al successo **salva la `session_string`** nel DB (`save_telegram_settings`,
   merge sui settings esistenti, `enabled=True`) ed esce con codice `0`.

Exit code: `0` login ok, `2` login fallito o credenziali mancanti. Il comando
**non** avvia il runtime/trading: costruisce solo il DB e fa il login.

Dopo il login, riavvia in modalità normale: il listener userà la
`session_string` salvata (in `_runtime_async`, `is_user_authorized()` sarà True).

## Verifica

I test (`tests/unit/test_telegram_login.py`) coprono con un client Telethon
fittizio: request_code→sign_in con `session_string`, flusso 2FA, codice/password
invalidi, `request_code` mancante, e il flusso CLI (happy/2FA/credenziali
mancanti/invio-codice-fallito). La verifica **end-to-end con l'API Telegram
reale** (telefono + codice reale) va fatta **sul VPS**.
