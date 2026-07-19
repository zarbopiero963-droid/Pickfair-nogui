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
- **PR-3:** persistenza **multi-bot** nel DB (schema `telegram_bots` +
  `telegram_bot_chats`). SOLO dati/CRUD, **nessun wiring runtime**: il path
  single-bot (`telegram_chats` → `monitored_chat_ids`) resta invariato.
- **PR-4:** **GUI** — gestione bot (aggiungi/seleziona/rimuovi bot con
  token mascherato) nella tab Telegram. Persiste via CRUD PR-3, **nessun wiring
  runtime**: i bot configurati non sono ancora ascoltati.
- **PR-4b (questa):** **GUI** — editor **chat per-bot** nella tab Telegram:
  assegna i `chat_id` al bot selezionato (scoped al `bot_id`). Persiste via CRUD
  PR-3 (`set/get_telegram_bot_chats`), **nessun wiring runtime**: le chat
  configurate non sono ancora ascoltate.
- **PR-5a (questa):** **wiring runtime** — primo step. Quando mancano le
  credenziali userbot (`api_id`/`api_hash`) ma è configurato **un** bot Bot API
  attivo con ≥1 chat attiva, `TelegramService.start()` avvia il transport HTTP
  `getUpdates` (PR-1) invece di Telethon: **gate rilassato** (niente
  `api_id`/`api_hash` sul path bot_token). Il path Telethon resta prioritario e
  invariato.
- Le PR successive aggiungono l'orchestrazione **N-bot**, l'autoheal per-bot e il
  ritiro dell'userbot.

## Wiring runtime Bot API (PR-5a) — `TelegramBotApiRuntime`

Modulo `telegram_bot_runtime.py`: `TelegramBotApiRuntime` è un **adapter
listener-compatibile** (`state`, `running`, `_runtime_thread`, `start`/`stop`/
`status`/`runtime_snapshot`) alimentato da `TelegramBotApiTransport` invece del
client Telethon. I messaggi ricevuti passano per `TelegramListener.handle_incoming`
(**riuso integrale**: allow-list, guardia anti-stale, parse, emit) verso gli stessi
callback `on_signal`/`on_status`: il listener è usato solo come **sink** (mai
avviato → nessun Telethon, nessun `api_id`/`api_hash`).

Selezione sorgente in `TelegramService.start()`:
- credenziali userbot presenti → **path Telethon** (invariato, prioritario);
- userbot assenti + **un** bot Bot API usable → **path Bot API single-bot** (PR-5a);
- userbot assenti + **più bot** Bot API usable → **orchestratore N-bot** (PR-5b,
  `TelegramMultiBotRuntime`): rimpiazza il precedente fail-closed
  `multi_bot_runtime_not_yet_supported`; i bot ingeriscono in **concorrenza**;
- niente di usable → **fail-closed** `Configurazione Telegram incompleta` (invariato).

### Orchestrazione N-bot (PR-5b)

`TelegramMultiBotRuntime` compone N `TelegramBotApiRuntime` indipendenti (uno per
bot usable), ognuno col **proprio** transport `getUpdates`, thread e **sink di
parsing PRIVATO** (mai condiviso: `handle_incoming` muta stato non sotto lock →
condividerlo tra N thread sarebbe una race). Presenta la stessa superficie
duck-typed attesa dal service, con semantica **aggregata fail-closed**:
- `state` **CONNECTED** solo se **tutti** i child sono CONNECTED. Un mix senza
  child FAILED distingue **transitorio** da **persistente**: **durante** il fan-out
  di `start()` (flag interno `_starting`) un mix CONNECTED+CREATED è normale ⇒
  **CONNECTING**, così un check autoheal concorrente non legge un `FAILED` spurio e
  non innesca un restart-all (409); **fuori** dalla finestra di `start()` un mix che
  non converge a all-CONNECTED (child rimasto CREATED/STOPPED per orphan/thread
  morto) è una **degradazione persistente** ⇒ **FAILED**. Qualsiasi child con flag
  FAILED ⇒ **FAILED** sempre (l'aggregato resta un segnale **onesto**: un bot è giù).
  Il **recovery** però NON è più restart-all: **PR-5c** aggiunge l'**autoheal
  PER-BOT** (vedi sotto) — l'aggregato torna CONNECTED quando il bot giù è guarito;
- `_runtime_thread` = proxy **any-alive** (un solo thread child vivo basta a far
  scattare il guard `previous_runtime_still_alive` → blocca un retry/409);
- `stop()` **fail-closed** se un **qualsiasi** thread child sopravvive (anti-409).
  `stop()` è invocato per **intento operatore** e imposta `intentional_stop=True`, che
  **resta True in TUTTI i rami** non-success (thread zombie superstite **o** stop
  figlio fallito senza thread vivi). Azzerarlo permetterebbe all'autoheal
  service-level di **riavviare** dopo uno shutdown voluto: con uno zombie vivo un
  restart-all aprirebbe un **secondo getUpdates** sullo stesso token (**409**/doppio
  consumo). Il fail-closed corretto è **non riavviare** dopo uno stop operatore; lo
  zombie è **SURFACED** (aggregato not-stopped + thread vivo ⇒ anti-409 guard) per
  intervento manuale / autoheal per-bot (**PR-5c**), mai per restart-all automatico.
  **Finestra di stop (nota, pre-esistente al transport, non introdotta da PR-5b):**
  la terminazione del thread è **cooperativa** (`run()` esce su `_stop.is_set()`; i
  thread Python non sono killabili a forza), quindi il long-poll `getUpdates`
  **in volo** può consegnare **un ultimo batch** prima che il loop noti lo stop —
  finestra **limitata** dal `long_poll_timeout`, non uno zombie indefinito. Vale
  identica per il single-bot (`telegram_bot_transport.py`, invariato in PR-5b). La
  terminazione per-bot più rapida (drop del batch post-stop / kill del consumo
  orfano) è tracciata per **PR-5c**;
- `handlers_registered`/`monitored_chat_count`/`active_network_resources` = **somma**
  sui child; `expected_handlers` = numero di bot.

**Bot attivo ma non-usable** (sole chat non numeriche `@canale`, risoluzione
`@username` rimandata): **SURFACED** in `status()` come `unusable_active_bot_count`
(visibile), **non** droppato in silenzio **né** bloccante per i bot usable — questo
rimpiazza il fail-closed di PR-5a preservandone l'intento (nessun drop silenzioso).

**Fan-in concorrente**: con N bot, N thread transport chiamano `_handle_signal`/
`_handle_status` in parallelo (prima single-writer). In `_handle_signal`
`_signal_fanin_lock` (un **RLock**, per rientro same-thread) serializza l'**intera**
sezione critica — `last_successful_message_ts` + `save_received_signal` +
`bus.publish` — così l'**ordine di enqueue sul bus == ordine di persistenza** (niente
finestra «A salva, B salva+pubblica, A pubblica»). Tenere `bus.publish` **dentro** il
lock è sicuro contro il deadlock perché **`EventBus.publish` è non bloccante**: fa
solo `enqueue` su una `Queue` (più un breve lock interno *leaf*) e **non** esegue i
subscriber inline — questi girano su un **worker pool asincrono** (`workers=4`).
Quindi nessuna lock-order inversion con il fan-in lock, e la consegna ai subscriber
avviene **fuori** dal lock (sui worker). Nota: la consegna era **già** concorrente e
non ordinata prima di PR-5b (4 worker), quindi i subscriber downstream sono **già**
tenuti a essere thread-safe; il lock qui garantisce solo l'ordine di *enqueue*.
`last_successful_message_ts` è **last-write-wins** (riflette il `received_at`
dell'ultimo messaggio; contratto `test_handle_signal_preserves_listener_received_at`):
un confronto stringa «monotòno» sarebbe stato fragile (offset ISO misti / `None`) e
avrebbe rischiato di bloccare `save/publish`. Questo campo è **solo di display**: la
**staleness detection** dell'invariant guard usa invece `last_message_processed_ts`
(wall-clock del *processing*, aggiornato a ogni segnale) — così un `received_at`
**backdated/malformato** del payload **non** può gonfiare `now − last` e innescare un
`STALE_RUNTIME` spurio → nessun restart/409. Nel `runtime_snapshot` il ts che alimenta
il guard è: ts del listener (receive-time, primario) → fallback `last_message_processed_ts`
del service, **mai** il `received_at` LWW. `_handle_status` non muta stato condiviso ⇒
pubblica **senza** lock.

**Invariant guard generalizzato**: `CONNECTED ⇒ handlers_registered ==
expected_handlers` (default `1` ⇒ backward-compatible single-bot/Telethon; `N` per
l'orchestratore), regola duplicati `> expected_handlers`. Così N handler sani con
aggregato CONNECTED **non** violano l'invariante.

**Autoheal PER-BOT (PR-5c)**: `TelegramMultiBotRuntime.run_perbot_autoheal_once()`
valuta **ogni child indipendentemente** e riavvia **solo** il bot non-sano (via
`TelegramBotApiRuntime.restart()` = `stop()`→`start()`, anti-409 preservato), **senza
toccare i bot sani** — sink/transport per-child sono isolati. Rimpiazza il restart-ALL
service-level: un singolo bot giù non butta più giù gli altri. Usa la **stessa**
`TelegramAutohealPolicy` del service (stateless), applicata con una **history
per-child** (`restart_timestamps` + `lockout_since`): budget di **3 restart nella
finestra di 300s**, cooldown 20s, poi **lockout per-bot di 300s** (fail-closed: niente
restart storm; il bot resta giù e **SURFACED** — `status()["locked_out_bot_count"]`,
`perbot_restart_total`). Un bot tornato **CONNECTED azzera** il proprio budget
(recovery pulito). Il service delega: `TelegramService.run_autoheal_once` chiama
`run_perbot_autoheal_once` quando il listener espone il metodo (multi-bot), **invece**
del restart-ALL — gate service-level (`intentional_stop` / grace) applicati prima di
delegare; il path single-bot/Telethon resta il restart aggregato invariato.

Conteggio e selezione derivano da **un'unica lettura** DB (`_select_bot_api_source`
→ `(active_count, usable)`): evita incoerenze tra il gate (bot attivi) e la
sorgente (bot usable). Gli **errori REALI del DB propagano** (nessun degrado a
`0`/`[]`, che riaprirebbe il drop silenzioso): `start()` li cattura e fa
**fail-closed** `telegram_bot_config_read_error`. Un DB **senza** supporto Bot API
(metodo `get_telegram_bots` assente, es. legacy) non è un errore di lettura ma
"nessuna sorgente bot" → `(0, [])` → fail-closed `Configurazione incompleta`.

Coerenza snapshot e anti-orfano su fallimento di `start()`: `handlers_registered`
è impostato **prima** di `listener.start()`. Se lo start **solleva** dopo aver
(parzialmente) avviato un thread, l'`except` fa **best-effort stop** del listener e
poi:
- thread **morto** ⇒ `listener=None` + `handlers_registered=0` (snapshot pulito e
  coerente: `FAILED` ⇒ 0 handler);
- thread **ancora vivo** ⇒ **tiene** il riferimento al listener e
  `handlers_registered=1` (residuo NON nascosto): il guard
  `previous_runtime_still_alive` lo vede e **blocca un retry**, evitando un secondo
  `getUpdates` (409) e segnali di betting duplicati.

Coerenza health/invariant: un transport **sano** conta come **1 handler**
(l'invariant guard richiede esattamente 1 handler quando `CONNECTED`). Lo stato
dell'adapter diventa **`FAILED`** (con `last_error`, `handlers_registered=0`) in
due casi di **ingestione morta**, così l'invariant guard e l'**autoheal esistenti**
reagiscono invece di restare `CONNECTED` in silenzio (fail-open):
- il thread `getUpdates` muore in modo non intenzionale (`bot_transport_thread_dead`);
- il thread è vivo ma i poll `getUpdates` falliscono in modo **permanente** (es.
  `bot_token` 401 o 409 Conflict): il transport conta i fallimenti consecutivi
  (`_consecutive_failures`, azzerato a ogni poll riuscito) e oltre la soglia
  (`_MAX_CONSECUTIVE_FAILURES=5`) l'adapter degrada a `FAILED`
  (`bot_transport_persistent_poll_failure`). Un `restart` con token ancora
  invalido rifallisce → lockout autoheal → il problema resta **visibile**.

La coppia `(thread_alive, _consecutive_failures)` è letta in modo **atomico** via
`health_snapshot()` sotto `_health_lock`; anche `start()` e `stop()` accedono a
`_thread` sotto lo stesso lock → nessuno snapshot *torn*. In più, l'INTERO
lifecycle `start()`/`stop()` è serializzato da un secondo lock dedicato
`_lifecycle_lock`: la sequenza di `stop()` (set-stop → lettura thread → `join`) è
**atomica** rispetto a `start()`, quindi un restart concorrente **non può**
rimpiazzare `_thread` e riavviare il polling mentre `stop()` fa `join` del thread
precedente (niente transport vivo dopo uno stop richiesto). Il `join` avviene
**dentro** `_lifecycle_lock` ma **fuori** da `_health_lock`: `run()` acquisisce
solo `_health_lock` (contatore fallimenti) e mai il lifecycle lock, quindi tenere
il lifecycle lock durante il `join` è sicuro (nessun deadlock).

L'idempotenza (`already_running`) è valutata **prima** della selezione sorgente: un
runtime già attivo non rivaluta il gate, così un cambio di config a runtime (2° bot
attivato, bot disattivato) non fa fallire/riavviare un runtime sano.

Solo `chat_id` **numerici** (es. `-100…`) sono ascoltabili via `getUpdates` (che
restituisce `chat.id` numerico): i `chat_id` non numerici (es. `@canale`) vengono
**scartati** in selezione (`_numeric_active_chat_ids`); un bot con sole chat non
numeriche risulta **non utilizzabile** → fail-closed. La risoluzione di
`@username` è rimandata.

Il `bot_transport_factory` è iniettabile (come il `client_factory` Telethon) per i
test headless. **Nessun effetto su money-management/ordini/Betfair/dutching/parsing.**
L'orchestrazione N-bot (**PR-5b**) e l'autoheal **per-bot** (**PR-5c**, vedi sezione
«Autoheal PER-BOT» sopra) sono ora implementati; il rilevamento del backoff permanente
con thread vivo è coperto dal contatore `_consecutive_failures` per-child.

## GUI — gestione bot (PR-4, tab Telegram)

Sezione **«Bot (Bot API)»** nella tab Telegram: etichetta + **bot_token
mascherato** (`show="*"`), checkbox Attivo, pulsanti Salva/Aggiorna e Rimuovi, e
un albero dei bot. La **logica** vive sul mixin `TelegramModule`
(`_save_telegram_bot_from_ui`, `_remove_selected_telegram_bot`,
`_load_selected_bot_into_editor`, `_refresh_telegram_bots_tree`) → testabile
headless; i widget in `telegram_tab_ui.py`.

Disciplina segreti (obbligatoria): il `bot_token` è mascherato in input, **mai
loggato**, e **in modifica NON viene ricaricato in chiaro** nell'entry — lasciarlo
vuoto in update significa «preserva il token esistente» (`save_telegram_bot(...,
bot_token=None, bot_id=...)`). Fail-closed: creazione bloccata senza etichetta o
senza token. **Nessun effetto runtime**: i bot sono persistiti ma non ancora
attivati (arriva nella PR di wiring).

## GUI — editor chat per-bot (PR-4b, tab Telegram)

Sotto-sezione **«Chat del bot selezionato»** dentro la sezione «Bot (Bot API)»:
entry `chat_id` + titolo (opzionale), pulsanti **Aggiungi Chat al Bot** /
**Rimuovi Chat dal Bot**, e un albero delle chat del bot. Le chat sono **scoped
al `bot_id` selezionato**: la vista segue sempre il bot corrente (aggiornata su
selezione/nuovo/salva/rimuovi bot). La **logica** vive sul mixin `TelegramModule`
(`_add_telegram_bot_chat_from_ui`, `_remove_telegram_bot_chat`,
`_refresh_telegram_bot_chats_tree`) → testabile headless; i widget in
`telegram_tab_ui.py`.

Fail-closed: l'aggiunta **pota prima una selezione stale** (bot rimosso altrove)
e richiede un bot selezionato **e** un `chat_id` non vuoto — così non si creano
chat orfane. Riaggiungere lo stesso `chat_id` **aggiorna il titolo** (upsert, via
reidratazione del set corrente + `set_telegram_bot_chats`, swap atomico scoped al
bot). **Nessun effetto runtime**: le chat sono persistite ma non ancora ascoltate.

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
(create/update; in update, passare `None` come token **preserva** quello
esistente; un `bot_id` inesistente solleva `ValueError`),
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
