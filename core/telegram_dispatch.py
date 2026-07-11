"""Decisione di instradamento di un messaggio Telegram in arrivo (audit #108).

La glue del listener (`App._run_bot._handle`) combina freschezza + filtro chat + chat
notifiche XTrader + `should_process` per decidere cosa fare di ogni update. Quella catena
era annidata in una closure async **non testabile in CI**: qui è estratta in `decide()`,
una funzione **pura** che restituisce un esito, così `App._handle` resta sottile e la
**semantica di instradamento** è esercitabile headless (chat ammessa → processa, chat
notifiche → conferma, stantio/senza-filtro/conflitto → ignora).

Non tocca GUI, Telegram runtime o scrittura: combina solo i moduli puri `message_freshness`
e `signal_router`.
"""

from . import message_freshness, signal_router

PROCESS = "PROCESS"                          # → _process (percorso di scrittura segnale)
CONFIRM = "CONFIRM"                          # → _process_confirmation (esito XTrader)
IGNORE_STALE = "IGNORE_STALE"                # messaggio troppo vecchio (arretrato post-outage)
IGNORE_NO_FILTER = "IGNORE_NO_FILTER"        # config viva senza filtro chat → fail-closed
IGNORE_CONFLICT = "IGNORE_CONFLICT"          # notif-chat == sorgente ammessa → ambiguo, fail-closed
IGNORE_NOT_RELEVANT = "IGNORE_NOT_RELEVANT"  # chat non ammessa o messaggio non pertinente


def decide(route, runtime_chat, text, msg_epoch, now, max_age, *, parsers_dir=None) -> str:
    """Decide l'esito per un messaggio in arrivo:

    1. **filtro chat** — config viva (`route`) senza alcun criterio chat → `IGNORE_NO_FILTER`
       (difesa-in-profondità: "nessun filtro" non deve voler dire "ammetti ogni chat").
       Valutato **per primo**, PRIMA dell'instradamento conferme: se la config viva viene
       salvata azzerando tutti i filtri sorgente mentre il listener gira ma
       `xtrader_notification_chat_id` resta impostato, l'instradamento conferma rimuoverebbe
       righe attive di coda e svuoterebbe il CSV partendo da uno stato prima fail-closed (#53,
       Codex). Il guard "nessun filtro" deve quindi precedere le conferme;
    2. **chat notifiche XTrader** — se `runtime_chat` è la notif-chat:
       - se coincide anche con una sorgente ammessa → `IGNORE_CONFLICT` (ambigua, fail-closed),
       - altrimenti → `CONFIRM` (percorso esiti, non scrittura). Valutata PRIMA della
         freschezza: una conferma XTrader **ritardata** (oltre `max_age`) deve comunque
         rimuovere il segnale attivo, non essere scartata come stantia (#53). Le conferme sono
         esiti, non nuovi segnali, quindi il filtro anti-backlog non le riguarda;
    3. **freschezza** — un nuovo segnale più vecchio di `max_age` (rispetto a `now`) →
       `IGNORE_STALE` (arretrato post-outage, non un segnale "live");
    4. **instradabilità** — chat non ammessa o messaggio non pertinente → `IGNORE_NOT_RELEVANT`;
    5. altrimenti → `PROCESS`.

    `parsers_dir` è inoltrato a `should_process` (default `None` = store reale, come nel
    runtime); i test lo iniettano per pilotare il ramo `PROCESS` con un parser su disco.
    """
    if not signal_router.has_chat_filter(route):
        return IGNORE_NO_FILTER
    if signal_router.is_notification_chat(route, runtime_chat):
        if signal_router.is_chat_allowed(route, runtime_chat):
            return IGNORE_CONFLICT
        return CONFIRM
    if message_freshness.is_stale(msg_epoch, now, max_age):
        return IGNORE_STALE
    if not signal_router.should_process(route, runtime_chat, text, parsers_dir=parsers_dir):
        return IGNORE_NOT_RELEVANT
    return PROCESS
