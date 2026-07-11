"""Privacy mode dei log: redazione del payload dei messaggi (audit #105 P1).

Di default il bridge **non** deve scrivere nei log su disco il TESTO COMPLETO dei
messaggi Telegram. Il contenuto di un canale privato (nomi, note operative, stake
impliciti, materiale proprietario) finirebbe in chiaro nel file di log
(`bridge-AAAA-MM-GG.log`), che vive sul disco e può essere copiato/condiviso. I
**token** sono già redatti dal sink di logging; questo modulo estende la stessa
prudenza al **contenuto** del messaggio.

`redact_message(text)` produce una forma **redatta** — impronta (hash) +
lunghezza + prima riga troncata — sufficiente per correlare/diagnosticare senza
conservare il contenuto. Il payload completo viene loggato **solo** quando
l'utente attiva esplicitamente `debug_message_payload` (opt-in, default OFF), che
è una scelta consapevole per il debug.

Modulo **puro** (nessuna dipendenza da GUI/Telegram/CSV): testabile headless.
"""

import hashlib

from . import event_log

# Quanti caratteri della PRIMA riga mostrare nella forma redatta: abbastanza per
# orientarsi ("è il segnale X?") senza riversare l'intero messaggio nel log.
FIRSTLINE_CHARS = 40


def redact_message(text, *, full=False) -> str:
    """Rappresentazione del messaggio per il log.

    - ``full=True``: ritorna il testo COMPLETO (su una sola riga: gli a-capo sono
      sostituiti da spazi, così resta una entry di log singola). Da usare solo
      quando l'utente ha attivato esplicitamente ``debug_message_payload``.
    - ``full=False`` (default, **privacy on**): ritorna ``[redatto: N char,
      sha256:<12 hex>] <prima riga troncata>`` — nessun contenuto oltre la prima
      riga troncata a ``FIRSTLINE_CHARS``.

    In entrambe le forme l'output passa per `event_log.redact_secrets` (#184 M8): un token a
    inizio messaggio non deve finire in chiaro nel log nonostante la privacy — né nel payload
    completo di debug, né nel prefisso della prima riga. Senza, la forma "redatta" mentirebbe sul
    proprio nome (era il path di leak più concreto dell'audit).

    `text` non stringa/``None`` è trattato come stringa (``None`` → vuoto)."""
    s = "" if text is None else str(text)
    if full:
        # Redige PRIMA di appiattire (Codex #251): se si comprimessero gli a-capo per primi, un
        # token registrato spezzato da CR/LF diventerebbe separato da SPAZI (es. "…LiveBotT oken"),
        # che né il match esatto né quello CR/LF-tollerante riconoscono → frammento persistente nel
        # log di debug. Redatto sul grezzo (CR/LF-tollerante), poi appiattito su una sola riga.
        return " ".join(event_log.redact_secrets(s).splitlines())
    n = len(s)
    digest = hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]
    first = s.splitlines()[0] if s else ""
    # Anteprima redatta con budget grezzo (#184 M8 + P2 Codex): mostra al più FIRSTLINE_CHARS char
    # GREZZI della prima riga, mascherando per intero un token che attraversa il confine senza
    # trascinare nell'anteprima testo oltre quel confine. L'ellissi resta legata al budget ORIGINALE
    # (la prima riga grezza superava i FIRSTLINE_CHARS). hash e lunghezza restano sul testo grezzo
    # (lo sha256 è una via sola e serve a correlare messaggi identici; la lunghezza non rivela
    # contenuto).
    preview = event_log.redact_preview(first, FIRSTLINE_CHARS)
    ellipsis = "…" if len(first) > FIRSTLINE_CHARS else ""
    return f"[redatto: {n} char, sha256:{digest}] {preview}{ellipsis}"


def redact_chat_id(chat_id):
    """Forma REDATTA di un `chat_id` Telegram per i log/ledger DUREVOLI.

    Il chat_id reale è un dato sensibile (regole Telegram safety: non deve finire in chiaro
    in un log persistente, come il diario eventi `event_journal.jsonl` sotto AppData). Ritorna
    un'impronta STABILE ``chat:sha256:<12 hex>`` che correla gli eventi della STESSA chat senza
    rivelare l'ID (lo sha256 è una via sola); ``None``/vuoto → ``None`` (campo omesso).

    `chat_id` non stringa (es. int dell'API Telegram) è normalizzato a stringa prima dell'hash,
    così lo stesso id come `int` o `str` produce la stessa impronta."""
    if chat_id is None:
        return None
    s = str(chat_id).strip()
    if not s:
        return None
    digest = hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]
    return f"chat:sha256:{digest}"
