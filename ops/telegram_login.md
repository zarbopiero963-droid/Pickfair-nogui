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

Prerequisito: `telegram.api_id` e `telegram.api_hash` già configurati nel DB
(via GUI o direttamente nei settings).

```bash
python headless_main.py --telegram-login
```

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
