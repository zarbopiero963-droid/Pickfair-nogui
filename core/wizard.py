"""#311 §3.4: Wizard di prima configurazione — logica PURA, testabile headless.

Cinque step guidati (issue #311): (1) token + test connessione bot; (2) chat ID +
messaggio di prova ricevuto; (3) parser su messaggio reale con anteprima CSV;
(4) verifica csv_path + scrittura CSV di test; (5) checklist finale prima di
disattivare la simulazione. La GUI (`wizard_gui`) è solo vista: qui vivono le
validazioni per-step, con le sonde Telegram **iniettabili** (nei test si passa un
mock: MAI Telegram live in CI).

Principi di sicurezza:
- **il token non finisce MAI in log/errori**: gli esiti delle sonde sono messaggi
  sanificati (niente URL con token, niente response echo);
- la sonda `getUpdates` NON passa `offset` e non conferma nulla: non "mangia" update
  al listener (il wizard è pensato per la prima configurazione, a listener fermo);
- il wizard NON attiva mai la modalità REALE: lo step 5 è una checklist informativa
  (si resta in Simulazione/Collaudo; il reale passa SEMPRE dal gate frase esistente);
- la scrittura di prova del CSV riusa `create_header_only_csv` (atomica, solo header).
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

from . import bridge_mode, csv_writer, health_check

_API = "https://api.telegram.org"
_TIMEOUT = 10   # secondi per sonda: il wizard non deve mai sembrare bloccato


@dataclass
class StepResult:
    """Esito di uno step: `ok` + messaggio utente (già sanificato) + dati extra."""

    ok: bool
    message: str
    data: "dict" = field(default_factory=dict)


def _sanitized_error(exc) -> str:
    """Messaggio d'errore SENZA token/URL: solo la classe e un suggerimento."""
    kind = type(exc).__name__
    if isinstance(exc, urllib.error.HTTPError):
        return (f"risposta HTTP {exc.code} da Telegram — token errato o revocato?"
                if exc.code in (401, 404) else f"errore HTTP {exc.code} da Telegram")
    if isinstance(exc, urllib.error.URLError):
        return "rete non raggiungibile (controlla connessione/proxy)"
    return f"errore imprevisto ({kind})"


def _call_telegram(token: str, method: str, params=None):
    """Chiamata one-shot alla Bot API (urllib, timeout). Il token vive SOLO nell'URL
    locale a questa funzione; qualsiasi errore esce già sanificato via i chiamanti.
    Il token è percent-encoded nel path (CodeRabbit #354): caratteri spuri da un
    incolla malformato (`#`, `?`, `/`, spazi) non troncano/deviano la richiesta —
    Telegram risponde 404 e l'esito esce sanificato. `:` resta letterale (è il
    separatore standard dei token: un token VALIDO non cambia URL)."""
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    tok = urllib.parse.quote(str(token), safe=":")
    with urllib.request.urlopen(f"{_API}/bot{tok}/{method}{qs}",
                                timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def probe_get_me(token: str):
    """Sonda reale `getMe` (usata dalla GUI; nei test si inietta un mock)."""
    return _call_telegram(token, "getMe")


def probe_get_updates(token: str):
    """Sonda reale `getUpdates` one-shot: NESSUN `offset` (non conferma nulla: il
    listener non perde update), short-poll con limite piccolo."""
    return _call_telegram(token, "getUpdates", {"timeout": 0, "limit": 20})


# ── Step 1: token + connessione ──────────────────────────────────────────────
def check_token(token, probe=probe_get_me) -> StepResult:
    """Valida il token chiamando `getMe` via `probe` (iniettabile). Fail-closed:
    token vuoto, errore di rete o risposta senza `ok` → step NON superato."""
    tok = str(token or "").strip()
    if not tok:
        return StepResult(False, "Inserisci il token del bot (da @BotFather).")
    try:
        resp = probe(tok)
    except Exception as exc:   # noqa: BLE001 — sonda: qualsiasi errore → esito sanificato
        return StepResult(False, f"Connessione fallita: {_sanitized_error(exc)}")
    if not (isinstance(resp, dict) and resp.get("ok") and isinstance(resp.get("result"), dict)):
        return StepResult(False, "Telegram ha rifiutato il token (getMe non ok).")
    username = str(resp["result"].get("username", "") or "?")
    return StepResult(True, f"Bot connesso: @{username}", {"username": username})


# ── Step 2: chat ID + messaggio di prova ─────────────────────────────────────
def check_chat(token, chat_id, probe=probe_get_updates) -> StepResult:
    """Verifica che dalla chat `chat_id` sia ARRIVATO un messaggio (l'utente ne invia
    uno di prova, poi preme «Controlla ora»). La sonda non passa offset: nessun update
    viene consumato. Fail-closed su chat vuota/malformata/errore/nessun messaggio."""
    cid = str(chat_id or "").strip()
    if not cid:
        return StepResult(False, "Inserisci il Chat ID della chat/canale sorgente.")
    if not (cid.lstrip("-").isdigit()):
        return StepResult(False, "Il Chat ID deve essere numerico (es. -1001234567890).")
    try:
        resp = probe(str(token or "").strip())
    except Exception as exc:   # noqa: BLE001 — sonda: qualsiasi errore → esito sanificato
        return StepResult(False, f"Verifica fallita: {_sanitized_error(exc)}")
    if not (isinstance(resp, dict) and resp.get("ok")):
        return StepResult(False, "Telegram ha rifiutato la richiesta (getUpdates non ok).")
    for upd in reversed(resp.get("result") or []):
        msg = (upd or {}).get("message") or (upd or {}).get("channel_post") or {}
        chat = msg.get("chat") or {}
        if str(chat.get("id", "")) == cid:
            first = str(msg.get("text", "") or "(senza testo)").splitlines()[0][:80]
            return StepResult(True, f"Messaggio ricevuto dalla chat {cid}: «{first}»",
                              {"first_line": first, "text": str(msg.get("text", "") or "")})
    return StepResult(False,
                      f"Nessun messaggio dalla chat {cid}. Verifica che il bot sia "
                      "ADMIN/membro della chat, invia un messaggio di prova e ripremi "
                      "«Controlla ora». (Se il listener è attivo, fermalo: consuma lui "
                      "gli update.)")


# ── Step 3: parser su messaggio reale (riusa il tester #350) ─────────────────
def check_parser(builder, message) -> StepResult:
    """Valuta il messaggio reale incollato col parser corrente del `ParserBuilder`
    (stessa pipeline read-only di «Prova messaggio»): superato solo se il verdetto è
    ✅. Il motivo esatto dello scarto è il verdetto stesso (#350)."""
    text = str(message or "").strip()
    if not text:
        return StepResult(False, "Incolla un messaggio segnale REALE del canale.")
    reports, _skipped = builder.batch_report(text)
    if not reports:
        return StepResult(False, "Nessun messaggio riconosciuto nel testo incollato.")
    rep = reports[0]
    if rep.ok:
        return StepResult(True, rep.verdict, {"rows": rep.rows})
    return StepResult(False, rep.verdict)


# ── Step 4: csv_path + scrittura di prova ────────────────────────────────────
def check_csv(path, *, do_write: bool = False) -> StepResult:
    """Verifica il `csv_path` con la sonda non invasiva (#351) e, su richiesta
    esplicita (`do_write=True`, bottone dedicato), scrive il CSV DI PROVA a solo
    header via `create_header_only_csv` (atomica; `force=False`: MAI sovrascrive un
    file esistente — potrebbe essere il CSV operativo con una riga attiva)."""
    state, motivo = health_check.csv_writable(path)
    if state == health_check.RED:
        return StepResult(False, f"csv_path non utilizzabile: {motivo}")
    if not do_write:
        return StepResult(True, f"Percorso valido ({motivo}). Ora premi "
                                "«Scrivi CSV di prova» per la verifica completa.")
    try:
        status = csv_writer.create_header_only_csv(str(path), force=False)
    except Exception as exc:   # noqa: BLE001 — I/O reale: esito onesto, mai crash del wizard
        return StepResult(False, f"Scrittura di prova FALLITA: {type(exc).__name__} — "
                                 "controlla permessi/lock della cartella.")
    if status == csv_writer.CSV_CREATE_REFUSED_ACTIVE:
        # CSV del bridge con una riga ATTIVA: non toccarlo è la cosa giusta (anti
        # doppia scommessa) e prova comunque che il percorso è quello operativo.
        return StepResult(True, "Il CSV esiste e contiene una riga ATTIVA: NON lo "
                                "tocco (protezione anti data-loss). Percorso verificato.")
    if status == csv_writer.CSV_CREATE_REFUSED_FOREIGN:
        # File ESTRANEO (header diverso): probabile percorso sbagliato dell'utente.
        return StepResult(False, "Il file esistente NON è un CSV del bridge: non lo "
                                 "sovrascrivo. Scegli un altro percorso (o rimuovilo "
                                 "a mano se è davvero da sostituire).")
    return StepResult(True, "CSV di prova scritto (solo header, formato XTrader).")


# ── Step 5: checklist finale ─────────────────────────────────────────────────
def final_checklist(cfg, *, parser_active: bool) -> "list[tuple[bool, str]]":
    """Checklist finale PRIMA di uscire dalla simulazione (informativa: il wizard non
    cambia mai la modalità — il reale passa SOLO dal gate frase esistente)."""
    cfg = cfg if isinstance(cfg, dict) else {}
    mode = bridge_mode.mode_from_cfg(cfg)
    csv_state, _ = health_check.csv_writable(cfg.get("csv_path", ""))
    return [
        (bool(str(cfg.get("bot_token", "") or "").strip()), "Token del bot configurato"),
        (bool(str(cfg.get("chat_id", "") or "").strip()
              or cfg.get("source_chats")), "Chat sorgente configurata"),
        (bool(parser_active), "Parser Personalizzato attivo (richiesto dallo START)"),
        (csv_state != health_check.RED, "Percorso CSV utilizzabile"),
        (mode == bridge_mode.SIMULAZIONE,
         "Modalità Simulazione attiva (passa a Collaudo/Reale SOLO dalla tab "
         "Sicurezza, coi suoi gate di conferma)"),
    ]
