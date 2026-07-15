from __future__ import annotations

import asyncio
import logging
import re
import threading
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
except ModuleNotFoundError:  # pragma: no cover - ambienti senza telethon
    TelegramClient = None
    events = None
    StringSession = None

logger = logging.getLogger(__name__)

# Prefissi delle righe statistiche del canale: la parola chiave NON deve
# cercare qui dentro (es. "📈Quota 0,5 HT Prematch:1.42" farebbe scattare
# la keyword "0,5 HT" su qualsiasi messaggio, mercato sbagliato).
_STATS_LINE_PREFIXES = ("📈", "🥅", "🎯", "📊")
_STATS_LINE_KEYWORDS = ("possesso palla",)


def _keyword_searchable_text(text: str) -> str:
    """Testo del messaggio senza le righe statistiche (per il match keyword)."""
    lines = []
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith(_STATS_LINE_PREFIXES):
            continue
        if any(stripped.lower().startswith(k) for k in _STATS_LINE_KEYWORDS):
            continue
        lines.append(line)
    return "\n".join(lines)


def sanitize_login_code(raw) -> str:
    """Estrae il codice di login di Telegram (SOLO cifre ASCII 0-9) dal testo.

    Fonte unica per headless (``TelegramListener.sign_in``) e GUI
    (``controllers/telegram_controller``). Robusto ai casi reali:
    - il messaggio di servizio 777000 è tipo ``Login code: 12345`` → ``12345``;
    - se il testo incollato contiene ALTRE cifre (es. l'id ``777000`` della chat),
      preferisce le cifre che seguono il marcatore ``code``/``codice`` per non
      concatenarle (``777000 Login code: 54321`` → ``54321``);
    - ``str.isdigit()`` NON è affidabile: tiene le cifre non-ASCII (arabo-indiane)
      che Telegram rifiuta, quindi si filtra esplicitamente su ``0-9``.
    La password 2FA NON passa mai da qui (non è un codice numerico).
    """
    text = str(raw or "")
    # Cattura SOLO la prima sequenza CONTIGUA di cifre dopo il marcatore: niente
    # `\s` nel gruppo, altrimenti "Login code: 54321\n777000" concatenerebbe le
    # cifre successive in "54321777000" (rilievo Fugu, regressione parser).
    # Marcatore "code"/"codice" delimitato: `\b` a SINISTRA (esclude il suffisso
    # "Barcode"/"encode") e lookahead negativo `(?![a-z_])` a DESTRA (esclude il
    # PREFISSO che continua il token — "codebase"/"codeword" con una lettera,
    # "code_foo" con un underscore: entrambi `\w` ma NON un vero marcatore). Il
    # `\b` iniziale da solo tornerebbe "999" su "codebase 999 codice 12345"
    # (rilievo CodeRabbit) e su "code_foo 999 codice12345" (rilievo GPT). NON si
    # usa `\b` a destra: fallirebbe col marcatore ATTACCATO alle cifre
    # ("code12345"), perché tra "e" e "1" (entrambi `\w`) non c'è word boundary →
    # nessun match → il fallback concatenerebbe TUTTE le cifre ("id 999
    # code12345" → "99912345", rilievo Fugu). Il lookahead accetta cifra/":"/
    # spazio dopo il marcatore ma rifiuta lettera/underscore, coprendo i casi.
    match = re.search(r"\bcod(?:e|ice)(?![a-z_])\D*([0-9]+)", text, re.IGNORECASE)
    segment = match.group(1) if match else text
    return "".join(ch for ch in segment if ch in "0123456789")


def format_floodwait_wait_text(seconds) -> str:
    """Testo d'attesa FloodWait leggibile, fonte unica per headless e GUI.

    Telethon a volte NON popola ``seconds`` (None): in quel caso torna un
    messaggio generico invece di ``"None secondi"``. Il controllo è ``is None``
    (non la falsyness): ``seconds == 0`` significa "riprova subito" e resta
    ``"0 secondi"``, non viene inghiottito come se mancasse (rilievo CodeRabbit).
    """
    if seconds is None:
        return "qualche secondo"
    return f"{seconds} secondi"


class TelegramListener:
    """
    Wrapper listener Telegram.

    Nota:
    questo file mantiene il motore parser già esistente, ma allinea:
    - fallback label/name dei pattern custom
    - callback registration
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_string: str | None = None,
        bot_token: str | None = None,
        db=None,
        client_factory=None,
        connect_timeout: float = 10.0,
        stop_timeout: float = 10.0,
        max_message_age_seconds: float = 300.0,
    ):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.session_string = session_string
        self.bot_token = bot_token
        self.db = db
        self._client_factory = client_factory
        self._connect_timeout = float(connect_timeout)
        self._stop_timeout = float(stop_timeout)
        # Guardia anti-stale: i messaggi piu' vecchi di questa soglia non
        # generano segnali (backlog post-reconnect su partita gia' cambiata).
        # Soglia <= 0 vietata: renderebbe stale OGNI messaggio reale,
        # silenziando il listener senza alcun errore visibile.
        max_age = float(max_message_age_seconds)
        if max_age <= 0:
            raise ValueError(
                f"max_message_age_seconds deve essere positivo, ricevuto {max_age}"
            )
        self.max_message_age_seconds = max_age

        self._state_lock = threading.Lock()
        self._runtime_thread: threading.Thread | None = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._client = None
        self._runtime_ready = threading.Event()
        self.runtime_handlers_registered = 0

        self.running = False
        self.monitored_chats: List[Any] = []
        self.state = "CREATED"
        self.last_error = ""
        self.intentional_stop = False
        self.reconnect_attempts = 0
        self.reconnect_in_progress = False
        self.last_successful_message_ts: str | None = None
        self.listener_started = False
        self.handlers_registered = 0
        self.active_network_resources = 0

        # Login interattivo userbot (request_code -> sign_in): il client Telethon
        # e la connessione devono restare vivi TRA le due chiamate, quindi
        # girano su un loop dedicato in un thread separato dal runtime.
        self._login_loop: asyncio.AbstractEventLoop | None = None
        self._login_thread: threading.Thread | None = None
        self._login_client = None
        self._login_phone: str | None = None
        self._login_code_hash: str | None = None
        self._login_awaiting_password = False

        self._callbacks = {
            "on_signal": None,
            "on_message": None,
            "on_status": None,
        }

    def _set_state(self, new_state: str) -> None:
        allowed_states = {"CREATED", "CONNECTING", "CONNECTED", "RECONNECTING", "STOPPED", "FAILED"}
        if new_state not in allowed_states:
            raise ValueError(f"Invalid Telegram listener state: {new_state}")
        prev_state = self.state
        self.state = new_state
        # Diagnostica: senza questa riga le transizioni (CONNECTED/RECONNECTING/
        # FAILED/STOPPED) erano invisibili nei log e un'anomalia runtime non
        # lasciava traccia. Solo il nome dello stato (nessun payload/segreto).
        if new_state != prev_state:
            logger.info("[TelegramListener] stato %s -> %s", prev_state, new_state)

    # =========================================================
    # EXTERNAL SETUP
    # =========================================================
    def set_database(self, db) -> None:
        self.db = db

    def set_monitored_chats(self, chats: List[Any]) -> None:
        processed_chats = []
        for c in (chats or []):
            try:
                processed_chats.append(int(c))
            except (ValueError, TypeError):
                processed_chats.append(str(c))
        self.monitored_chats = processed_chats

    def set_callbacks(self, on_signal=None, on_message=None, on_status=None) -> None:
        self._callbacks["on_signal"] = on_signal
        self._callbacks["on_message"] = on_message
        self._callbacks["on_status"] = on_status
        self.handlers_registered = sum(1 for cb in self._callbacks.values() if callable(cb))

    # =========================================================
    # LIFECYCLE
    # =========================================================
    def start(self, monitored_chats: Optional[List[Any]] = None):
        # Mutua esclusione login/runtime (rilievo Fugu/GPT/GLM/CodeRabbit): un
        # login interattivo pendente/abbandonato non deve convivere col runtime.
        # Cleanup PRIMA del guard already_running, cosi' vale su OGNI chiamata.
        # Nessuna race col login: request_code gira solo nel comando standalone
        # `--telegram-login` (il processo esce prima di start()) e la GUI usa
        # TelegramController, quindi request_code e start() non sono mai
        # concorrenti sulla stessa istanza; _cleanup_login e' comunque difensivo
        # (gestisce lo stato None, join/close idempotenti).
        self._cleanup_login()

        if self.running:
            return {"started": True, "reason": "already_running", "chat_count": len(self.monitored_chats)}

        if monitored_chats is not None:
            self.set_monitored_chats(monitored_chats)

        self.reconnect_in_progress = False
        self.last_error = ""

        # Preflight fail-closed: niente runtime finto. Se mancano i prerequisiti
        # il listener va in FAILED con errore esplicito, mai in finto LISTENING.
        # NOTA: intentional_stop si resetta solo DOPO i preflight: un runtime
        # precedente ancora vivo deve continuare a vedere lo stop richiesto.
        if TelegramClient is None or events is None:
            self.mark_failed("telethon_not_available")
            return {"started": False, "state": self.state, "error": self.last_error}
        if self._client_factory is None and not self.session_string and not self.bot_token:
            self.mark_failed("missing_session_string_or_bot_token")
            return {"started": False, "state": self.state, "error": self.last_error}
        # Fail-closed: senza chat monitorate Telethon ascolterebbe TUTTI i
        # dialoghi dell'account (chats=None = nessun filtro). Mai di default.
        if not self.monitored_chats:
            self.mark_failed("no_monitored_chats")
            return {"started": False, "state": self.state, "error": self.last_error}
        # Mai sovrascrivere un runtime precedente ancora vivo: due thread
        # condividerebbero client/loop e lo stato diventerebbe inattendibile.
        if self._runtime_thread is not None and self._runtime_thread.is_alive():
            self.mark_failed("previous_runtime_still_alive")
            return {"started": False, "state": self.state, "error": self.last_error}

        self.intentional_stop = False
        self._set_state("CONNECTING")
        self.running = True
        self.listener_started = True
        self.active_network_resources = 0
        self._runtime_ready.clear()

        self._runtime_thread = threading.Thread(
            target=self._runtime_main, name="telegram-listener-runtime", daemon=True
        )
        self._runtime_thread.start()

        # Attende l'esito della connessione entro connect_timeout. Un connect
        # che sfora il timeout e' una startup FALLITA: lasciare CONNECTING
        # per sempre bloccherebbe anche l'autoheal (restart soppresso durante
        # CONNECTING) con un runtime appeso indefinitamente.
        ready = self._runtime_ready.wait(timeout=self._connect_timeout)
        if not ready:
            # Serializzato col runtime: o il timeout marca FAILED prima che
            # il runtime pubblichi CONNECTED, o trova lo stato gia' CONNECTED
            # e non lo tocca. Mai un CONNECTED che sovrascrive il timeout.
            with self._state_lock:
                if self.state == "CONNECTING":
                    self.mark_failed("connect_timeout")
        return {
            "started": self.state not in {"FAILED", "STOPPED"},
            "chat_count": len(self.monitored_chats),
            "state": self.state,
            "error": self.last_error or None,
        }

    def stop(self):
        # Un login interattivo abbandonato (request_code senza sign_in) lascerebbe
        # il client/loop di login vivi (connessione Telethon orfana): chiudili su
        # OGNI stop, anche se il runtime era gia' STOPPED (rilievo Fugu/CodeRabbit).
        self._cleanup_login()

        if self.state == "STOPPED" and not self.running:
            return {"stopped": True, "reason": "already_stopped", "state": self.state}

        self.intentional_stop = True
        self.reconnect_in_progress = False

        loop, client = self._runtime_loop, self._client
        if loop is not None and client is not None and not loop.is_closed():
            try:
                future = asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
                future.result(timeout=self._stop_timeout)
            except Exception:
                logger.exception("[TelegramListener] Errore durante disconnect")
        thread = self._runtime_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=self._stop_timeout)

        # STOPPED solo a runtime davvero uscito: dichiararlo con un thread
        # ancora vivo permetterebbe a un nuovo start() di sovrapporsi.
        if thread is not None and thread.is_alive():
            self.mark_failed("stop_timeout_runtime_thread_alive")
            return {"stopped": False, "state": self.state, "error": self.last_error}

        self.running = False
        self.active_network_resources = 0
        self.runtime_handlers_registered = 0
        self._set_state("STOPPED")
        self._emit_status("STOPPED", "Listener fermato")
        return {"stopped": True, "state": self.state}

    # =========================================================
    # RUNTIME TELETHON (thread dedicato con event loop proprio)
    # =========================================================
    def _create_client(self):
        if self._client_factory is not None:
            return self._client_factory(self.api_id, self.api_hash, self.session_string)
        
        # Priorità al Bot Token se presente
        if self.bot_token:
            logger.info("[TelegramListener] Inizializzazione client tramite Bot Token")
            return TelegramClient(
                None, self.api_id, self.api_hash
            )
            
        return TelegramClient(
            StringSession(self.session_string), self.api_id, self.api_hash
        )

    def _runtime_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._runtime_loop = loop
        try:
            loop.run_until_complete(self._runtime_async())
        except Exception as exc:
            if not self.intentional_stop:
                # Preserva il traceback del thread runtime crashato per
                # diagnosticare la causa reale, ma REDATTO: il testo
                # dell'eccezione Telethon e' non controllato e potrebbe
                # contenere un segreto (host/session/token).
                logger.error(
                    "[TelegramListener] runtime thread crashed\n%s",
                    self._redact_sensitive(traceback.format_exc()),
                )
                self.mark_failed(f"runtime_error: {exc}")
        finally:
            if self._runtime_loop is loop:
                self.active_network_resources = 0
                self.runtime_handlers_registered = 0
                self._client = None
                self._runtime_loop = None
            self._runtime_ready.set()
            try:
                loop.close()
            except Exception:
                pass

    async def _runtime_async(self) -> None:
        client = self._create_client()
        self._client = client
        try:
            # Avvio tramite Bot Token o Sessione
            if self.bot_token:
                await client.start(bot_token=self.bot_token)
            else:
                await client.connect()
                # Abort PRIMA di altre chiamate Telethon
                if self.intentional_stop or self.state == "FAILED":
                    return
                authorized = await client.is_user_authorized()
                if not authorized:
                    self.mark_failed("session_not_authorized")
                    self._runtime_ready.set()
                    return

            # stop() o il timeout di start() possono arrivare mentre connect()
            # è ancora in corso: in quel caso NON va registrato alcun handler
            # (niente runtime vivo dopo STOPPED/FAILED); il finally disconnette.
            if self.intentional_stop or self.state == "FAILED":
                return

            # monitored_chats non vuoto ed events presente: garantiti dal preflight
            event_filter = events.NewMessage(chats=self.monitored_chats)
            client.add_event_handler(self._on_new_message_event, event_filter)
            self.runtime_handlers_registered = 1

            # Transizione a CONNECTED serializzata con il timeout di start():
            # se nel frattempo la startup e' stata marcata FAILED (o stop),
            # il runtime abbandona invece di sovrascrivere lo stato.
            with self._state_lock:
                if self.intentional_stop or self.state == "FAILED":
                    self.runtime_handlers_registered = 0
                    self._runtime_ready.set()
                    return
                self.active_network_resources = 1
                # Seed liveness: senza timestamp il guard segnerebbe stale un
                # canale sano ma silenzioso e l'autoheal lo riavvierebbe.
                self.last_successful_message_ts = datetime.now(timezone.utc).isoformat()
                self._set_state("CONNECTED")
            self._emit_status("CONNECTED", "Listener connesso a Telegram")
            self._runtime_ready.set()

            await client.run_until_disconnected()

            if self.intentional_stop:
                return
            # Disconnessione non richiesta: stato honesto, l'autoheal decide.
            self.mark_failed("disconnected_unexpectedly")
        finally:
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass

    async def _on_new_message_event(self, event) -> None:
        try:
            message = getattr(event, "message", None)
            text = getattr(event, "raw_text", None)
            if text is None:
                text = getattr(message, "message", "") if message else ""
            chat_id = getattr(event, "chat_id", None)
            # La data ORIGINALE del messaggio (non l'ora di ricezione): serve
            # alla guardia anti-stale in handle_incoming.
            message_date = getattr(message, "date", None) if message is not None else None
            # Offload su executor: parse_signal e i callback toccano il DB in
            # modo sincrono e non devono bloccare il loop Telethon.
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self.handle_incoming, text or "", chat_id, message_date
            )
        except Exception:
            logger.exception("[TelegramListener] Errore gestione messaggio")

    def handle_incoming(self, text: str, chat_id: int | None = None,
                        message_date: Any = None) -> Optional[Dict[str, Any]]:
        """Processa un messaggio ricevuto: liveness, callback, parsing, segnale.

        Difesa in profondita' sul chat_id: il filtro Telethon
        (events.NewMessage(chats=...)) e' l'unica barriera a monte; se un
        messaggio fuori lista arriva comunque qui va scartato integralmente.
        chat_id/monitored_chats assenti = chiamata interna/sim (il preflight
        di start() garantisce monitored_chats non vuoto in runtime).
        """
        if chat_id is not None and self.monitored_chats and chat_id not in self.monitored_chats:
            logger.warning(
                "[TelegramListener] Messaggio da chat non autorizzata %s scartato",
                chat_id,
            )
            return None

        now = datetime.now(timezone.utc)
        received_at = now.isoformat()
        self.last_successful_message_ts = received_at

        # Guardia anti-stale: il backlog consegnato dopo un reconnect non deve
        # piazzare bet su una situazione di gioco che non esiste piu'. La
        # liveness sopra resta aggiornata (il canale e' vivo, il segnale no).
        # message_date assente = chiamata interna/sim: nessun filtro.
        if message_date is not None:
            try:
                message_dt = message_date
                if message_dt.tzinfo is None:
                    message_dt = message_dt.replace(tzinfo=timezone.utc)
                age_seconds = (now - message_dt).total_seconds()
            except Exception:
                logger.warning(
                    "[TelegramListener] message_date non confrontabile (%r): "
                    "messaggio scartato fail-closed",
                    message_date,
                )
                return None
            # Guardia simmetrica: una data molto nel FUTURO (clock skew serio
            # tra server Telegram e host) neutralizzerebbe il controllo stale;
            # oltre la stessa tolleranza il messaggio va scartato fail-closed.
            if age_seconds < -self.max_message_age_seconds:
                logger.warning(
                    "[TelegramListener] Messaggio con data futura (%.0fs) scartato",
                    age_seconds,
                )
                return None
            if age_seconds > self.max_message_age_seconds:
                logger.warning(
                    "[TelegramListener] Messaggio stale (%.0fs > %.0fs) scartato",
                    age_seconds,
                    self.max_message_age_seconds,
                )
                return None

        cb = self._callbacks.get("on_message")
        if callable(cb):
            try:
                cb(text)
            except Exception:
                self.last_error = "on_message_callback_failed"
                logger.exception("[TelegramListener] Errore callback on_message")

        signal = self.parse_signal(text)
        if not signal:
            return None
        signal.setdefault("received_at", received_at)
        if chat_id is not None:
            signal.setdefault("chat_id", chat_id)
        self._emit_signal(signal)
        return signal

    def _redact_sensitive(self, text: str) -> str:
        """Redige le credenziali/identificativi NOTI al listener (token bot,
        session string, api_hash, api_id, telefono di login, chat-id monitorati)
        da una stringa destinata ai log. Serve perche' alcuni reason (in
        particolare ``runtime_error: <exc>``) inglobano il messaggio di
        un'eccezione Telethon NON controllato. E' una DIFESA IN PROFONDITA' sui
        segreti conosciuti, non una garanzia assoluta contro qualunque dato
        sensibile arbitrario (es. credenziali di terzi in un messaggio di
        errore). Fail-safe: non solleva mai.

        - Segreti-stringa (session/token/api_hash/telefono): match diretto.
        - Identificativi numerici (api_id, chat-id): match a CONFINE di cifra
          (``(?<!\\d)...(?!\\d)``) per non corrompere numeri estranei nel
          traceback (numeri di riga, offset, timestamp)."""
        out = str(text)
        try:
            string_secrets = [
                self.session_string,
                self.bot_token,
                self.api_hash,
                getattr(self, "_login_phone", None),
            ]
            for cand in string_secrets:
                secret = "" if cand is None else str(cand)
                # Soglia >=8: le credenziali Telegram reali sono lunghe (session
                # ~350 char, bot token ~46, api_hash 32); valori piu' corti
                # rischiano solo di corrompere testo legittimo (es. "sess"
                # sottostringa del reason "session_not_authorized").
                if len(secret) >= 8 and secret in out:
                    out = out.replace(secret, "[REDACTED]")
            numeric_tokens: list[str] = []
            if self.api_id is not None:
                numeric_tokens.append(str(self.api_id))
            for chat in (self.monitored_chats or []):
                chat_s = str(chat)
                numeric_tokens.append(chat_s)
                if chat_s.startswith("-"):
                    # Telethon/traceback possono esporre l'id senza segno o come
                    # channel_id (formato -100<channel_id>): redigi anche quelle
                    # varianti, altrimenti l'id passerebbe in chiaro.
                    numeric_tokens.append(chat_s[1:])
                    if chat_s.startswith("-100"):
                        numeric_tokens.append(chat_s[4:])
            for token in numeric_tokens:
                # Solo id realistici (i chat-id/api_id reali non sono cortissimi);
                # il confine di cifra evita match dentro numeri piu' lunghi.
                if len(token.lstrip("-")) >= 4:
                    out = re.sub(rf"(?<!\d){re.escape(token)}(?!\d)", "[REDACTED]", out)
        except Exception:  # pragma: no cover - la redazione non deve mai rompere il log
            return "[REDACTION_ERROR]"
        return out

    def mark_failed(self, error: str) -> None:
        # Redige il reason ALLA SORGENTE: last_error e' esposto anche via
        # status()/_emit_status (UI/telemetria/notifiche), quindi redigere solo
        # al log non basterebbe. Il path runtime_error puo' inglobare testo
        # d'eccezione non controllato -> possibile segreto.
        self.last_error = self._redact_sensitive(str(error or ""))
        # Diagnostica: TUTTI i fallimenti terminali passano da qui (timeout,
        # sessione non autorizzata, disconnessione inattesa, runtime_error,
        # reconnect_failed). Prima era silenzioso: un problema non lasciava mai
        # il motivo nei log.
        logger.error(
            "[TelegramListener] mark_failed: %s", self.last_error or "listener_failure"
        )
        self.running = False
        self.reconnect_in_progress = False
        # Rilascia il contatore delle risorse di rete PRIMA della transizione a
        # FAILED: un listener fallito (es. disconnessione inattesa) e' disconnesso,
        # quindi non deve riportare una risorsa di rete viva. Senza questo, uno
        # snapshot letto appena dopo aver visto FAILED osserva active_network_
        # resources=1 (incoerente con running=False/client_alive=False) finche' il
        # finally di _runtime_main non lo azzera piu' tardi. Allineato ai path
        # terminali stop()/end_reconnect_attempt() che gia' azzerano il contatore.
        self.active_network_resources = 0
        self._set_state("FAILED")
        self._emit_status("FAILED", self.last_error or "Listener failure")

    def begin_reconnect_attempt(self) -> bool:
        if self.intentional_stop or self.state in {"STOPPED", "FAILED"}:
            return False
        self.reconnect_attempts += 1
        self.reconnect_in_progress = True
        logger.warning("[TelegramListener] reconnect tentativo #%d avviato", self.reconnect_attempts)
        self._set_state("RECONNECTING")
        return True

    def end_reconnect_attempt(self, *, success: bool, error: str = "") -> None:
        self.reconnect_in_progress = False
        if success:
            self.last_error = ""
            logger.info(
                "[TelegramListener] reconnect riuscito dopo %d tentativi", self.reconnect_attempts
            )
            # Keep this truthful for current architecture: no live network resource.
            self._set_state("STOPPED")
            self.running = False
            self.active_network_resources = 0
            return
        self.mark_failed(error or "reconnect_failed")

    def status(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "running": bool(self.running),
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": int(self.reconnect_attempts),
            "reconnect_in_progress": bool(self.reconnect_in_progress),
            "last_error": self.last_error,
            "last_successful_message_ts": self.last_successful_message_ts,
            "listener_started": bool(self.listener_started),
            "handlers_registered": int(self.handlers_registered),
            "active_network_resources": int(self.active_network_resources),
            "monitored_chat_count": len(self.monitored_chats),
        }

    def runtime_snapshot(self) -> Dict[str, Any]:
        """Deterministic snapshot for invariant/health probe evaluation."""
        status = self.status()
        return {
            "state": status["state"],
            "running": status["running"],
            "listener_started": status["listener_started"],
            "client_alive": bool(self.running and self.active_network_resources > 0),
            # Verità runtime per l'invariant guard: conta gli handler Telethon
            # registrati (0 o 1), non i callback applicativi di status().
            "handlers_registered": int(self.runtime_handlers_registered),
            "reconnect_in_progress": status["reconnect_in_progress"],
            "reconnect_attempts": status["reconnect_attempts"],
            "active_network_resources": status["active_network_resources"],
            "intentional_stop": status["intentional_stop"],
            "retry_loop_active": bool(self.reconnect_in_progress),
            "last_error": status["last_error"],
            "last_successful_message_ts": status["last_successful_message_ts"],
        }

    # =========================================================
    # LOGIN INTERATTIVO USERBOT (telefono + codice + 2FA)
    # =========================================================
    def _ensure_login_loop(self) -> None:
        loop = self._login_loop
        if loop is not None and loop.is_running():
            return
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, name="tg-login-loop", daemon=True)
        thread.start()
        self._login_loop = loop
        self._login_thread = thread

    def _run_login_coro(self, coro, timeout: float):
        loop = self._login_loop
        if loop is None:
            raise RuntimeError("login loop non inizializzato")
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return fut.result(timeout=timeout)
        except Exception:
            # Su timeout/errore cancella la coroutine: non lasciarla pendente sul
            # loop di login che _cleanup_login sta per fermare/chiudere (Fugu/CR).
            fut.cancel()
            raise

    def _create_login_client(self):
        # Sessione VUOTA: il login serve a GENERARE una nuova session_string.
        if self._client_factory is not None:
            return self._client_factory(self.api_id, self.api_hash, None)
        return TelegramClient(StringSession(), self.api_id, self.api_hash)

    def _cleanup_login(self) -> None:
        client = self._login_client
        loop = self._login_loop
        thread = self._login_thread
        self._login_client = None
        self._login_phone = None
        self._login_code_hash = None
        self._login_awaiting_password = False
        if client is not None and loop is not None and loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(client.disconnect(), loop).result(
                    timeout=self._stop_timeout
                )
            except Exception:
                logger.debug("[TelegramListener] disconnect login client fallito", exc_info=True)
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        # Join del thread e chiusura del loop: evita proliferazione di thread su
        # request_code ripetuti e libera le risorse (rilievo Greptile/Codacy).
        # Guard self-join (rilievo Fugu): mai join sul thread corrente (se
        # _cleanup_login fosse chiamato dal loop di login -> RuntimeError).
        if (
            thread is not None
            and thread is not threading.current_thread()
            and thread.is_alive()
        ):
            thread.join(timeout=self._stop_timeout)
            if thread.is_alive():
                # Join scaduto (loop bloccato): non azzerare in silenzio un thread
                # ancora vivo -> logga il potenziale leak (rilievo Fable).
                logger.warning(
                    "[TelegramListener] thread del login loop ancora vivo dopo "
                    "join (timeout=%ss): possibile risorsa non rilasciata.",
                    self._stop_timeout,
                )
        if loop is not None and not loop.is_closed() and not loop.is_running():
            try:
                loop.close()
            except Exception:
                logger.debug("[TelegramListener] close login loop fallito", exc_info=True)
        self._login_loop = None
        self._login_thread = None

    def request_code(self, phone_number: str):
        """Login userbot step 1: invia il codice di verifica a `phone_number`.

        Tiene vivo il client Telethon su un loop dedicato per il successivo
        `sign_in`. Ritorna {"ok": True} oppure {"ok": False, "error": ...}. Su
        `FloodWaitError` include anche `retry_after` (secondi, può essere None):
        i chiamanti (GUI/headless) lo usano per dire "attendi N s, non rilanciare".
        """
        if TelegramClient is None and self._client_factory is None:
            return {"ok": False, "error": "telethon_not_available"}
        phone = str(phone_number or "").strip()
        if not phone:
            return {"ok": False, "error": "missing_phone"}
        self._cleanup_login()  # scarta un eventuale login pendente
        try:
            self._ensure_login_loop()
            client = self._create_login_client()
            # Assegna il client PRIMA di eseguire il coroutine (rilievo CodeRabbit
            # Major): se connect()/send_code_request() falliscono, _cleanup_login
            # nell'except deve poter disconnettere il client gia' creato/connesso.
            self._login_client = client
            self._login_phone = phone
            self._login_awaiting_password = False

            async def _do():
                await client.connect()
                sent = await client.send_code_request(phone)
                return getattr(sent, "phone_code_hash", None)

            code_hash = self._run_login_coro(_do(), self._connect_timeout + 30.0)
            self._login_code_hash = code_hash
            self._emit_status("CODE_SENT", f"Codice inviato a {phone}")
            return {"ok": True}
        except Exception as exc:
            self._cleanup_login()
            name = type(exc).__name__
            # FloodWait: troppi invii ravvicinati. Telegram impone un'attesa e
            # OGNI nuovo invio invalida i codici precedenti (causa tipica del
            # "codice non valido" dopo molti tentativi). Restituisci un errore
            # strutturato con `retry_after` così sia GUI sia headless possono
            # dire all'utente di attendere invece di rilanciare.
            if name == "FloodWaitError":
                seconds = getattr(exc, "seconds", None)
                # seconds mancante/None: messaggio generico invece di "attendi None
                # secondi" (rilievo Fable). `retry_after` resta il valore grezzo.
                # Helper condiviso con la GUI (unica fonte, gestisce None/0).
                wait_txt = format_floodwait_wait_text(seconds)
                msg = f"FloodWait: troppi tentativi, attendi {wait_txt} prima di riprovare (non rilanciare)"
                self._emit_status("FAILED", msg)
                return {"ok": False, "error": msg, "retry_after": seconds}
            # str(TimeoutError()) e' vuoto: fallback sul nome classe (rilievo Codacy).
            msg = str(exc) or name
            self._emit_status("FAILED", f"Invio codice fallito: {msg}")
            return {"ok": False, "error": msg}

    def sign_in(self, code: str, password_2fa: str | None = None):
        """Login userbot step 2: verifica il codice (e la 2FA se richiesta) e,
        al successo, RITORNA la `session_string` riutilizzabile.

        NON è più uno stub: chiama davvero `client.sign_in`. Il chiamante DEVE
        aver eseguito `request_code` prima. Contratto di ritorno:
        - {"ok": True, "session_string": "..."}    login riuscito
        - {"ok": False, "requires_password": True}  serve la password 2FA
        - {"ok": False, "error": "invalid_code"|"expired_code"|
                                  "invalid_password"|"not_authorized"|...}
        """
        if self._login_client is None or self._login_loop is None:
            return {"ok": False, "error": "request_code_first"}
        pwd = str(password_2fa or "").strip() or None
        # Sanitizza il codice via helper condiviso (sole cifre ASCII, estratte dopo
        # il marcatore "code" se presente). La password 2FA NON viene toccata.
        verification = sanitize_login_code(code)
        awaiting = self._login_awaiting_password
        if not awaiting and not verification:
            return {"ok": False, "error": "missing_code"}

        client = self._login_client
        phone = self._login_phone
        code_hash = self._login_code_hash

        async def _do():
            if awaiting:
                # Il codice è già stato accettato: manca solo la password 2FA.
                if not pwd:
                    return {"ok": False, "requires_password": True, "await_pwd": True}
                try:
                    await client.sign_in(password=pwd)
                except Exception as exc:
                    if type(exc).__name__ == "PasswordHashInvalidError":
                        return {"ok": False, "error": "invalid_password", "await_pwd": True}
                    raise
            else:
                try:
                    await client.sign_in(
                        phone=phone, code=verification, phone_code_hash=code_hash
                    )
                except Exception as exc:
                    name = type(exc).__name__
                    if name == "SessionPasswordNeededError":
                        if not pwd:
                            return {"ok": False, "requires_password": True, "await_pwd": True}
                        try:
                            await client.sign_in(password=pwd)
                        except Exception as exc2:
                            if type(exc2).__name__ == "PasswordHashInvalidError":
                                return {"ok": False, "error": "invalid_password", "await_pwd": True}
                            raise
                    elif name == "PhoneCodeInvalidError":
                        return {"ok": False, "error": "invalid_code"}
                    elif name == "PhoneCodeExpiredError":
                        return {"ok": False, "error": "expired_code", "terminal": True}
                    else:
                        raise
            if not await client.is_user_authorized():
                return {"ok": False, "error": "not_authorized"}
            return {"ok": True, "session_string": client.session.save()}

        try:
            result = self._run_login_coro(_do(), self._connect_timeout + 45.0)
        except Exception as exc:
            self._cleanup_login()
            # str(TimeoutError()) e' vuoto: fallback sul nome classe (rilievo Codacy).
            msg = str(exc) or type(exc).__name__
            self._emit_status("FAILED", f"Login fallito: {msg}")
            return {"ok": False, "error": msg}

        if result.get("ok"):
            session_string = result["session_string"]
            self.session_string = session_string
            self._emit_status("AUTHORIZED", "Login completato")
            self._cleanup_login()
            return {"ok": True, "session_string": session_string}

        if result.get("await_pwd"):
            # Codice ok ma serve la password (o password errata): il client
            # resta VIVO per il retry con `sign_in(code, password_2fa=...)`.
            self._login_awaiting_password = True
            if result.get("error") == "invalid_password":
                self._emit_status("FAILED", "Password 2FA non valida")
                return {"ok": False, "error": "invalid_password", "requires_password": True}
            self._emit_status("PASSWORD_REQUIRED", "Password 2FA richiesta")
            return {"ok": False, "requires_password": True}

        # Errore terminale (codice invalido/scaduto/non autorizzato): chiudi il login.
        self._cleanup_login()
        self._emit_status("FAILED", f"Login non riuscito: {result.get('error')}")
        return {"ok": False, "error": result.get("error")}

    # =========================================================
    # STATUS / EMIT
    # =========================================================
    def _emit_status(self, status: str, message: str):
        cb = self._callbacks.get("on_status")
        if callable(cb):
            try:
                cb(status, message)
            except Exception:
                self.last_error = "on_status_callback_failed"
                logger.exception("[TelegramListener] Errore callback on_status")

    def _emit_signal(self, signal: Dict[str, Any]):
        cb = self._callbacks.get("on_signal")
        if callable(cb):
            try:
                cb(signal)
                self.last_successful_message_ts = signal.get("received_at") or signal.get("timestamp")
            except Exception:
                self.last_error = "on_signal_callback_failed"
                logger.exception("[TelegramListener] Errore callback on_signal")

    # =========================================================
    # PARSING PUBLIC
    # =========================================================
    def parse_signal(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None

        parsed = self._parse_custom_patterns(text)
        if parsed:
            return parsed

        parsed = self._parse_master_signal(text)
        if parsed:
            return parsed

        parsed = self._parse_cashout_signal(text)
        if parsed:
            return parsed

        return None

    # =========================================================
    # CUSTOM PATTERNS
    # =========================================================
    def _parse_custom_patterns(self, text: str) -> Optional[Dict[str, Any]]:
        if not self.db or not hasattr(self.db, "get_signal_patterns"):
            return None

        try:
            patterns = self.db.get_signal_patterns(enabled_only=True)
        except TypeError:
            patterns = self.db.get_signal_patterns()
            patterns = [p for p in patterns if p.get("enabled", True)]
        except Exception:
            logger.exception("[TelegramListener] Errore get_signal_patterns")
            return None

        keyword_haystack = _keyword_searchable_text(text).lower()

        for cp in patterns or []:
            try:
                pattern = cp.get("pattern") or ""
                keyword = str(cp.get("keyword") or "").strip()

                if not pattern and not keyword:
                    continue

                regex_match = (
                    re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
                    if pattern else None
                )

                # Logica di attivazione:
                # - Solo regex: deve matchare
                # - Solo keyword: deve essere presente nel testo (escluse le
                #   righe statistiche, che contengono frasi tipo "Quota 0,5 HT")
                # - Entrambi: entrambi devono essere soddisfatti
                if pattern and keyword:
                    if not regex_match or keyword.lower() not in keyword_haystack:
                        continue
                elif pattern:
                    if not regex_match:
                        continue
                elif keyword:
                    if keyword.lower() not in keyword_haystack:
                        continue

                home_score, away_score = self._extract_score(text)
                minute = self._extract_minute(text)
                total_goals = home_score + away_score

                min_minute = cp.get("min_minute")
                max_minute = cp.get("max_minute")
                min_score  = cp.get("min_score")
                max_score  = cp.get("max_score")
                live_only  = bool(cp.get("live_only", False))
                prematch   = bool(cp.get("prematch", False))

                if live_only and prematch:
                    continue  # contraddizione: ignora

                # Filtri opzionali — se il minuto non è nel messaggio (0)
                # i filtri minuto vengono saltati per non bloccare inutilmente.
                if minute > 0:
                    min_minute_i = self._safe_int_or_none(min_minute)
                    max_minute_i = self._safe_int_or_none(max_minute)
                    if min_minute_i is not None and minute < min_minute_i:
                        continue
                    if max_minute_i is not None and minute > max_minute_i:
                        continue
                    if prematch:
                        continue  # regola solo pre-match: ignora messaggi live
                else:
                    # Minuto assente: blocca solo se live_only richiede minuto presente
                    if live_only:
                        continue

                min_score_i = self._safe_int_or_none(min_score)
                max_score_i = self._safe_int_or_none(max_score)
                if min_score_i is not None and total_goals < min_score_i:
                    continue
                if max_score_i is not None and total_goals > max_score_i:
                    continue

                selection_template = str(cp.get("selection_template") or "").strip()
                selection = self._render_selection_template(
                    selection_template=selection_template,
                    home_score=home_score,
                    away_score=away_score,
                    minute=minute,
                )

                event_name = self._extract_event_name(text)

                # 2.1-C: un copy-pattern con action cashout non costruisce una bet.
                action = str(cp.get("action") or "QUICK_BET").strip().upper()
                if action == "CASHOUT_ALL":
                    # CASHOUT_ALL chiude TUTTE le posizioni dell'evento: semantica
                    # non ambigua → emette lo stesso signal_type del cashout nativo
                    # e confluisce nel medesimo routing (2.1-B), un solo percorso.
                    return {
                        "signal_type": "CASHOUT_ALL",
                        "event_name": event_name,
                        "raw_text": text,
                        "pattern_id": cp.get("id"),
                        "pattern_label": cp.get("label") or cp.get("name") or "",
                    }
                if action == "CASHOUT":
                    # Fail-closed (scelta owner): un CASHOUT SINGOLO da copy-pattern
                    # confluirebbe nel routing nativo event-level, che chiude TUTTE
                    # le posizioni della partita — non la selezione/market
                    # configurati nel pattern. Vietato finché il CashoutRouter non
                    # supporta il targeting per-selezione (follow-up). Skip visibile:
                    # nessun segnale cashout emesso, nessun side-effect broker.
                    logger.warning(
                        "[TelegramListener] copy-pattern CASHOUT singolo ignorato "
                        "(pattern_id=%s): copy_pattern_cashout_single_requires_targeting",
                        cp.get("id"),
                    )
                    continue

                market_type = str(cp.get("market_type") or "MATCH_ODDS").strip()
                bet_side = str(cp.get("bet_side") or "BACK").strip().upper()

                if not selection:
                    selection = str(cp.get("label") or cp.get("name") or "Custom Pattern")

                # Stake: priorità → stake_fisso dal pattern → testo messaggio → default
                stake_fixed        = bool(cp.get("stake_fixed", False))
                stake_fixed_amount = cp.get("stake_fixed_amount")
                mm_auto            = bool(cp.get("mm_auto", False))

                if stake_fixed and stake_fixed_amount is not None:
                    stake = float(stake_fixed_amount)
                else:
                    stake = self._extract_stake(text) or 0.0  # 0 = lascia decidere al MM

                return {
                    "event_name":         event_name,
                    "selection":          selection,
                    "market_type":        market_type,
                    "bet_type":           bet_side,
                    "price":              self._extract_odds(text),   # None → broker cerca best price
                    "stake":              stake,
                    "minute":             minute,
                    "home_score":         home_score,
                    "away_score":         away_score,
                    "pattern_id":         cp.get("id"),
                    "pattern_label":      cp.get("label") or cp.get("name") or "",
                    "stake_fixed":        stake_fixed,
                    "stake_fixed_amount": stake_fixed_amount,
                    "mm_auto":            mm_auto,
                    "prematch":           prematch,
                    "raw_text":           text,
                }

            except Exception:
                logger.exception("[TelegramListener] Errore parse custom pattern id=%s", cp.get("id"))

        return None

    def _render_selection_template(
        self,
        *,
        selection_template: str,
        home_score: int,
        away_score: int,
        minute: int,
    ) -> str:
        if not selection_template:
            return ""

        total_goals = home_score + away_score
        over_line = total_goals + 0.5

        rendered = selection_template
        rendered = rendered.replace("{home_score}", str(home_score))
        rendered = rendered.replace("{away_score}", str(away_score))
        rendered = rendered.replace("{total_goals}", str(total_goals))
        rendered = rendered.replace("{minute}", str(minute))
        rendered = rendered.replace("{over_line}", str(over_line).replace(".0", ""))
        return rendered

    # =========================================================
    # LEGACY PATTERNS
    # =========================================================
    def _default_patterns(self) -> Dict[str, Any]:
        """
        Pattern di base neutri. Non contengono logica di tipster specifica.
        Vengono usati solo per l'estrazione strutturale dei dati.
        """
        return {
            "event_icon": r"🆚\s*(.+?)(?:\n|$)",
            "score": r"(\d+)\s*[-–]\s*(\d+)",
            "time": r"(\d+)m",
            "odds": r"@\s*(\d+[.,]\d+)",
            "stake": r"(?:stake|puntata|€)\s*(\d+(?:[.,]\d+)?)",
            "cashout": r"\b(COPY\s*CASHOUT|cashout|CASHOUT)\b",
            "cashout_all": r"\b(CASHOUT\s*ALL|CASHOUT\s*TUTTO|CHIUDI\s*TUTTO)\b",
            "ignore_patterns": [],
        }

    def _parse_legacy_signal(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Il motore neutro non supporta più il parsing 'legacy' hardcoded.
        Tutto il parsing deve passare dai Custom Patterns salvati nel DB dell'utente.
        """
        return None

    # =========================================================
    # MASTER SIGNAL / CASHOUT
    # =========================================================
    def _extract_master_field(self, field: str, text: str) -> str:
        m = re.search(rf"^{field}\s*:\s*(.+)$", text, flags=re.IGNORECASE | re.MULTILINE)
        return m.group(1).strip() if m else ""

    def _parse_master_signal(self, text: str) -> Optional[Dict[str, Any]]:
        upper = text.upper()
        if "MASTER SIGNAL" not in upper:
            return None

        market_id = self._extract_master_field("market_id", text)
        selection_id = self._extract_master_field("selection_id", text)
        action = self._extract_master_field("action", text) or "BACK"
        master_price = self._extract_master_field("master_price", text) or "2.0"
        event_name = self._extract_master_field("event_name", text)
        market_name = self._extract_master_field("market_name", text)
        selection = self._extract_master_field("selection", text)

        if not market_id or not selection_id:
            return None

        return {
            "market_id": market_id,
            "selection_id": int(float(selection_id)),
            "bet_type": str(action).upper(),
            "price": float(str(master_price).replace(",", ".")),
            "event_name": event_name,
            "market_name": market_name,
            "selection": selection,
            "stake": self._extract_stake(text) or 1.0,
            "raw_text": text,
        }

    def _parse_cashout_signal(self, text: str) -> Optional[Dict[str, Any]]:
        # event_name serve al routing del CASHOUT singolo per restringere la
        # chiusura alla partita (il CASHOUT_ALL lo ignora). Stringa vuota se non
        # estraibile: il router fa fail-closed (salta il singolo per quel mercato).
        p = self._default_patterns()
        if re.search(p["cashout_all"], text, flags=re.IGNORECASE):
            return {"signal_type": "CASHOUT_ALL", "raw_text": text,
                    "event_name": self._extract_event_name(text)}
        if re.search(p["cashout"], text, flags=re.IGNORECASE):
            return {"signal_type": "CASHOUT", "raw_text": text,
                    "event_name": self._extract_event_name(text)}
        return None

    # =========================================================
    # EXTRACTORS
    # =========================================================
    def _extract_event_name(self, text: str) -> str:
        # Formato classico con emoji 🆚
        m = re.search(r"🆚\s*(.+?)(?:\n|$)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip("* ").strip()
        # Formato con etichetta "Partita:" o "Match:" (con o senza emoji) seguito da riga con i nomi
        m = re.search(r"(?:partita|match)\s*[^:\n]*:?\s*\n\s*(.+?)(?:\n|$)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # Fallback: riga con " vs " tra due nomi di squadra
        m = re.search(r"^(.{3,40}\s+vs?\.?\s+.{3,40})$", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return m.group(1).strip()
        return ""

    def _extract_score(self, text: str) -> tuple[int, int]:
        # Formato "0 - 0" o "0-0" o "0–0"
        m = re.search(r"(\d+)\s*[-–]\s*(\d+)", text)
        if m:
            return int(m.group(1)), int(m.group(2))
        return 0, 0

    @staticmethod
    def _safe_int_or_none(value) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def _extract_minute(self, text: str) -> int:
        # Formato classico: "55m" o "55'"
        m = re.search(r"(\d+)\s*(?:m\b|')", text, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
        # Formato con etichetta: ⏱Minuto⏱\n55  oppure  Minuto: 55  oppure  Min: 55
        m = re.search(
            r"(?:⏱[^\n]*|minuto|minute|min)\s*[:\s]*\n?\s*(\d+)(?:\D|$)",
            text, flags=re.IGNORECASE,
        )
        if m:
            return int(m.group(1))
        return 0

    def _extract_odds(self, text: str) -> Optional[float]:
        # Formato @2.50 o @ 2,50
        m = re.search(r"@\s*(\d+(?:[.,]\d+)?)", text, flags=re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
        # Formato "Quota: 2.50" o "Odd: 2.50"
        m = re.search(r"(?:quota|odd|odds)\s*[:\s]+(\d+[.,]\d+)", text, flags=re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
        return None

    def _extract_stake(self, text: str) -> Optional[float]:
        m = re.search(r"(?:stake|puntata|€)\s*(\d+(?:[.,]\d+)?)", text, flags=re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
        return None
