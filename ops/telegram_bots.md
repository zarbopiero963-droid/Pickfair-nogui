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

- **PR-1 (questo modulo):** `telegram_bot_transport.py` — trasporto `getUpdates`
  long-poll di **un** bot. Isolato e **opt-in**: non è ancora agganciato al
  runtime, il path userbot resta invariato. Le PR successive aggiungono schema
  config multi-bot, GUI, runtime multi-bot e il ritiro dell'userbot.

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
    bot_token="123456:ABC...",          # da BotFather
    chat_ids=[-1001234567890],          # chat proprie (bot admin/membro)
    on_message=listener.handle_incoming,  # stesso contratto dell'userbot
)
transport.start()   # avvia il long-poll in un thread dedicato
# ...
transport.stop()
```

## Sicurezza

Il `bot_token` è una credenziale completa (chi lo possiede controlla il bot):
va **cifrato a riposo** nel DB (previsto in PR-2, oggi `_SECRET_FIELDS` non lo
copre) e mai committato/loggato.
