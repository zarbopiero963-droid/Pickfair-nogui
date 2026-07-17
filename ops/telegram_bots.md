# Telegram via Bot API — bot multipli e chat multiple

Migrazione (epica #374) dal modello **userbot Telethon** (`api_id`/`api_hash` +
session string, login telefono+codice) al modello **Bot API HTTP**: un
`bot_token` da BotFather + i `chat_id` delle chat **proprie** dove il bot è
admin/membro. Nessun `api_id`/`api_hash`, nessuna session string, nessun login.

## Requisiti operativi (chat proprie)

Un bot legge i messaggi **solo** nelle chat dove è stato aggiunto:

- **Canale:** aggiungi il bot come **amministratore** → riceve i post del canale.
- **Gruppo:** aggiungi il bot e disattiva la **privacy mode** (BotFather
  `/setprivacy` → Disable) **oppure** rendilo admin → riceve tutti i messaggi.
- Il bot **non** vede i messaggi inviati **prima** del suo ingresso (niente
  storico).
- Un `bot_token` può avere **un solo** consumer `getUpdates`/webhook attivo alla
  volta (non usare lo stesso token altrove in parallelo).

> I canali di **terzi** (provider a cui sei solo iscritto) NON sono leggibili da
> un bot: lì servirebbe l'userbot. Questa migrazione assume chat proprie.

## Stato (roadmap #374)

- **PR-1:** `telegram_bot_transport.py` — trasporto `getUpdates` long-poll di
  **un** bot. Isolato e **opt-in**, non agganciato al runtime.
- **PR-2:** `bot_token` cifrato a riposo (`telegram.bot_token` in `_SECRET_FIELDS`).
- **PR-3 (questa):** persistenza **multi-bot** nel DB (schema `telegram_bots` +
  `telegram_bot_chats`). SOLO dati/CRUD, **nessun wiring runtime**: il path
  single-bot (`telegram_chats` → `monitored_chat_ids`) resta invariato.
- Le PR successive aggiungono GUI, runtime multi-bot e il ritiro dell'userbot.

## Persistenza multi-bot (PR-3) — `telegram_bots` / `telegram_bot_chats`

Due tabelle **additive** (`CREATE TABLE IF NOT EXISTS`, `telegram_chats` legacy
intatta):

- `telegram_bots(id, label, bot_token, is_active, created_at, updated_at)` — un
  record per bot. Il **`bot_token` è cifrato a riposo** (`enc:v1:` via
  `SecretCipher`): la cifratura di una colonna reale **non** è automatica (l'hook
  `_SECRET_FIELDS` copre solo la tabella `settings`), quindi la fa **esplicita**
  il CRUD in `database.py`. Il plaintext legacy migra al primo `save`.
- `telegram_bot_chats(bot_id, chat_id, title, is_active)` — link table (PK
  composta `(bot_id, chat_id)`, `ON DELETE CASCADE`): ogni bot ha il **proprio**
  set di chat; lo stesso `chat_id` può essere monitorato da bot diversi.

CRUD (`database.py`): `save_telegram_bot(label, bot_token, *, is_active, bot_id)`
(create/update; `bot_token=None` in update **preserva** il token esistente),
`get_telegram_bots()` (decifra il token — **non loggarlo mai**),
`remove_telegram_bot(bot_id)` (elimina bot + sue chat, in transazione),
`set_telegram_bot_chats(bot_id, chats)` (swap scoped al bot),
`get_telegram_bot_chats(bot_id)`.

## `telegram_bot_transport.TelegramBotApiTransport`

Trasporto di un singolo bot che consegna i messaggi al contratto già esistente
`TelegramListener.handle_incoming(text, chat_id, message_date)` — da lì in giù
(parser, bus, trading) tutto è invariato.

Comportamento:

- **getUpdates long-poll** con gestione dell'`offset` (ack dei messaggi
  consumati). Riceve `message` (gruppi) e `channel_post` (canali admin).
- **Fail-open** sul singolo update malformato: viene saltato ma l'offset avanza
  comunque (un update rotto non incastra il loop).
- **Fail-safe** sul loop: un errore di rete non uccide il thread (backoff
  esponenziale, reset sul primo successo).
- **Fail-closed** sui messaggi: allow-list dei `chat_id` (difesa in profondità)
  e **data del messaggio obbligatoria e valida** (anti-replay), coerente con la
  guardia anti-stale di `handle_incoming` (Bot API fornisce `message.date`).
- **Nessun segreto nei log:** il `bot_token` compare nell'URL getUpdates e non
  viene **mai** loggato; ogni testo d'errore è redatto.
- Fetch HTTP **iniettabile** (default `urllib`) → i test sono deterministici e
  non toccano la rete.

Costruzione (esempio, non ancora wired in produzione):

```python
from telegram_bot_transport import TelegramBotApiTransport

transport = TelegramBotApiTransport(
    bot_token=IL_TUO_TOKEN_BOTFATHER,   # credenziale bot (da BotFather)
    chat_ids=[-1001234567890],          # chat proprie (bot admin/membro) — OBBLIGATORIA e non vuota
    on_message=listener.handle_incoming,  # stesso contratto dell'userbot
)
transport.start()   # avvia il long-poll in un thread dedicato
# ...
transport.stop()
```

Note di trasporto (PR-1):

- **allow-list obbligatoria:** `chat_ids` vuota → `ValueError` (fail-closed).
- **HTTPS obbligatorio:** `api_base` deve usare `https://` (il `bot_token` viaggia
  nell'URL getUpdates; su `http://` transiterebbe in chiaro e un MITM potrebbe
  iniettare update **falsi** nel pipeline di trading). `http://` è ammesso **solo**
  con l'override esplicito `allow_insecure_http=True`, riservato a test locali
  isolati — mai in produzione. Schema non-http(s) (`file://`, `ftp://`) → `ValueError`.
- **`ok=False` → backoff:** una risposta getUpdates non-ok solleva `BotApiPollError`
  e il loop applica backoff (niente polling stretto). Stesso trattamento per un
  batch non vuoto **senza `update_id` valido** (dati server malformati): si solleva
  per il backoff invece di reincastrarsi in hot-loop (anti-wedge).
- **nessun segreto nei log del loop:** su errore, `run()` logga tipo eccezione +
  messaggio **redatto** (mai `exc_info`/traceback, che potrebbe contenere l'URL col
  token non redatto).
- **offset in-memory:** in PR-1 l'offset non è persistito; al riavvio Telegram può
  riconsegnare gli update non-ack (fino a 24h) → il **replay è neutralizzato dalla
  guardia anti-stale** di `handle_incoming` (data del messaggio). La persistenza
  dell'offset è prevista in **PR-2** (schema config/DB).
- **token nei log:** non abilitare il debug di `http.client`/`urllib` in produzione
  (l'URL getUpdates contiene il token); il modulo non logga mai l'URL.

## Sicurezza

Il `bot_token` è una credenziale completa (chi lo possiede controlla il bot):
è **cifrato a riposo** nel DB — la chiave `telegram.bot_token` è in
`_SECRET_FIELDS` (`database.py`), quindi `save_telegram_settings` la scrive
cifrata (formato `enc:v1:…`) e `get_telegram_settings` la decifra in modo
trasparente. Non va mai committato né loggato (il trasporto lo redige, vedi sopra).

> **Migrazione dei token legacy (azione ops).** La cifratura scatta **in
> scrittura**: un `telegram.bot_token` già presente **in chiaro** (salvato prima
> di questa modifica) viene letto in passthrough e **resta in chiaro su disco
> finché non lo si ri-salva**. La sola lettura NON lo migra. Per cifrare un token
> legacy esistente: ri-salva le impostazioni Telegram una volta (dalla GUI o via
> `save_telegram_settings`) — al primo save la riga passa a `enc:v1:…`.
