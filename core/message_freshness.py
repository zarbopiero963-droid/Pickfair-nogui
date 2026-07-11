"""Scarto dei messaggi Telegram troppo vecchi (anti-segnale-stantio).

`python-telegram-bot` gestisce internamente le cadute di rete durante il polling e,
quando la connessione torna, **recupera** i messaggi accumulati offline. Per il
trading live un segnale vecchio di minuti è inutile/pericoloso (verrebbe scritto nel
CSV e ripiazzato). Questo modulo decide — in modo **puro e testabile** — se un
messaggio è troppo vecchio rispetto a "adesso", in base al suo timestamp (`msg.date`).

Simmetricamente, un messaggio dal **futuro** (clock skew: `msg.date` avanti rispetto al
clock locale) è tollerato solo entro `DEFAULT_MAX_CLOCK_SKEW` secondi (clampato ad
"adesso"); oltre è trattato come stantio (fail-closed), perché uno skew grande — tipico
di un clock locale indietro — farebbe passare un backlog vecchio come "fresco" (#311-3.5-d).
"""

import math

DEFAULT_MAX_AGE = 120  # secondi
# Tolleranza per un timestamp del messaggio nel FUTURO rispetto al clock locale (clock skew,
# #311-3.5-d). Un messaggio avanti fino a questo margine è clampato ad "adesso" (resta fresco);
# oltre è trattato come stantio (fail-closed). 60s presuppone orologi ragionevolmente
# sincronizzati (NTP): scelta del proprietario per la coda #311-3.5.
DEFAULT_MAX_CLOCK_SKEW = 60  # secondi


def effective_max_age(max_signal_age, clear_delay):
    """`max_age` EFFETTIVO per il filtro freschezza, **non superiore a `clear_delay`** (#53):
    un messaggio già più vecchio della vita della riga CSV (`clear_delay`) verrebbe scritto e
    scadrebbe quasi subito → meglio trattarlo come stantio. Ritorna `min(max_signal_age,
    clear_delay)` quando entrambi sono numeri positivi finiti.

    Casi limite (sicurezza prima di tutto):
    - `clear_delay` malformato/non positivo → **nessun clamp** (si ritorna `max_signal_age`
      così com'è; `is_stale` applica comunque la sua coercizione difensiva);
    - `max_signal_age` esplicitamente `<= 0` (filtro disattivato dall'utente) → resta tale:
      il clamp NON deve **ri-attivare** un filtro che l'utente ha spento di proposito;
    - `max_signal_age` malformato/`bool`/`NaN`/`inf` → si clampa il **valore effettivo** che
      `is_stale` userebbe (`DEFAULT_MAX_AGE`) al timeout, non si ritorna il valore rotto così
      com'è (Codex #250): se si restituisse il valore malformato, `is_stale` ricadrebbe su
      `DEFAULT_MAX_AGE` (120s) **bypassando il clamp** — con una riga da 90s un arretrato di
      100s passerebbe come "fresco" e verrebbe scritto, pur essendo già oltre la vita CSV."""
    if isinstance(clear_delay, bool):
        return max_signal_age
    try:
        cd = float(clear_delay)
    except (TypeError, ValueError, OverflowError):
        return max_signal_age
    if not math.isfinite(cd) or cd <= 0:
        return max_signal_age
    # A questo punto `cd` è il timeout valido (vita reale della riga). Determina il `max_age`
    # EFFETTIVO che `is_stale` userebbe e clampalo a `cd`, così NESSUN valore — anche uno
    # malformato/bool/NaN/inf che `is_stale` riconduce a `DEFAULT_MAX_AGE` — può superare la
    # vita della riga. Solo un `<= 0` esplicito (filtro spento dall'utente) resta tale.
    if isinstance(max_signal_age, bool):
        ma = float(DEFAULT_MAX_AGE)          # bool → DEFAULT in is_stale
    else:
        try:
            ma = float(max_signal_age)
        except (TypeError, ValueError, OverflowError):
            ma = float(DEFAULT_MAX_AGE)      # malformato → DEFAULT in is_stale
        else:
            if not math.isfinite(ma):
                ma = float(DEFAULT_MAX_AGE)  # NaN/inf → DEFAULT in is_stale
            elif ma <= 0:
                return max_signal_age        # filtro disattivato dall'utente: non ri-attivarlo
    return min(ma, cd)


def _coerce_positive_finite(value):
    """Ritorna `float(value)` se è un numero **finito e > 0** (e non `bool`), altrimenti `None`.

    Coercizione difensiva CONDIVISA dal clamp anti-doppia-scommessa (`capped_max_age`, review
    CodeRabbit): tenere UNA sola sorgente per la regola «bool/malformato/NaN/inf/<=0 → invalido»
    evita che una modifica futura aggiorni un ramo e non l'altro, indebolendo il fail-safe."""
    if isinstance(value, bool):
        return None
    try:
        v = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    return v


def capped_max_age(max_signal_age, clear_delay, dedupe_window):
    """`max_age` EFFETTIVO come `effective_max_age`, ma clampato **anche** alla finestra di
    deduplica del `SignalTracker` (#371).

    Motivo (anti-doppia-scommessa). Con `drop_pending_updates=False` sulle riconnessioni (issue
    #311-1.2), Telegram può **rideliverare** (at-least-once) un update già processato ma non ancora
    ack-ato prima del blip. La protezione a valle è la deduplica per **contenuto** del `SignalTracker`,
    che ha una finestra (`dedupe_window`, default 300s). Il filtro freschezza, invece, ammette un
    messaggio fino a `effective_max_age = min(max_signal_age, clear_delay)`, **senza** legame con la
    finestra dedup. Se l'utente configura l'età effettiva **oltre** la finestra dedup e un outage la
    supera, un update rideliverato è ancora "fresco" (`is_stale` = False) ma **non più deduplicato**
    (hash scaduto) → seconda scrittura CSV → doppia scommessa. Clampando il `max_age` **attivo** alla
    finestra dedup, un messaggio troppo vecchio per essere protetto dalla dedup è trattato come
    **stantio** (scartato): fail-closed, direzione sicura.

    Regole (coerenti con `effective_max_age`):
    - `dedupe_window` malformato / `bool` / non finito / `<= 0` → **nessun clamp** (si ritorna
      `effective_max_age` così com'è): una finestra dedup invalida non deve indebolire né
      irrigidire a caso il filtro;
    - un filtro freschezza **disattivato** dall'utente (`max_age <= 0` esplicito) **resta tale**: il
      clamp non ri-attiva un filtro spento di proposito (come in `effective_max_age`);
    - un `effective_max_age` malformato (stringa non numerica ecc.) è lasciato passare invariato:
      `is_stale` lo ricondurrà comunque al `DEFAULT_MAX_AGE` sicuro.

    Con i **default** (`max_signal_age`/`clear_delay` = 120s, finestra dedup 300s) l'effettivo è
    120 < 300 → **nessun cambiamento osservabile**: il clamp morde solo con config non-default
    (età effettiva > finestra dedup)."""
    eff = effective_max_age(max_signal_age, clear_delay)
    # Finestra dedup invalida (malformata/bool/NaN/inf/<=0) → nessun clamp: ritorna l'effettivo grezzo.
    dw = _coerce_positive_finite(dedupe_window)
    if dw is None:
        return eff
    # Clampa SOLO un max_age ATTIVO (numero finito > 0). Un filtro disattivato (<=0) resta tale; un
    # `eff` malformato è lasciato a `is_stale` (che ricade su DEFAULT_MAX_AGE).
    eff_f = _coerce_positive_finite(eff)
    if eff_f is None:
        return eff
    # Preserva il tipo dei valori originali (review Fable 5 / Fugu Ultra): se NON si clampa ritorna
    # `eff` così com'è (int 120 resta 120, non 120.0); se si clampa ritorna la `dedupe_window`
    # originale (int 300), non il `dw` coerciuto a float. `is_stale` accetta comunque qualunque
    # numerico, ma così un consumer che confronti per tipo/serializzi non vede differenze spurie.
    return eff if eff_f <= dw else dedupe_window


def is_stale(message_epoch, now, max_age=DEFAULT_MAX_AGE,
             max_skew=DEFAULT_MAX_CLOCK_SKEW) -> bool:
    """`True` se il messaggio (epoch UNIX `message_epoch`) è più vecchio di `max_age`
    secondi rispetto a `now` (epoch UNIX).

    - `max_age` **coerciuto a float** in modo sicuro. Un valore **malformato** in config
      (``"abc"``/`bool`/`NaN`/`inf`/`None`/`Decimal` enorme) NON disattiva il filtro:
      il filtro è una protezione di sicurezza, quindi si torna al **default sicuro**
      (`DEFAULT_MAX_AGE`), così un `max_signal_age` corrotto non lascia passare un
      backlog vecchio dopo un reconnect (audit P1). Solo un numero **esplicitamente
      ``<= 0``** disattiva il filtro (scelta dell'utente, documentata in config).
      Una stringa numerica (es. ``"120"`` editata a mano) funziona come il numero.
      Niente eccezioni: un valore rotto al più ricade sul default, non crasha l'handler;
    - **`message_epoch` mancante/illeggibile → STALE (fail-CLOSED, audit A4)**: un messaggio
      di backlog recuperato senza data (`msg.date is None`) o con timestamp illeggibile NON
      deve bypassare l'anti-stale — sarebbe esattamente ciò che il modulo deve impedire.
      Viene quindi trattato come stantio e scartato;
    - **`now` illeggibile → non stantio (fail-OPEN)**: il `now` è il TUO clock; se è
      illeggibile meglio processare un segnale buono che scartarlo per un now rotto;
    - un messaggio dal **futuro** (clock skew, #311-3.5-d): tollerato **entro `max_skew`
      secondi** (default `DEFAULT_MAX_CLOCK_SKEW` = 60) e clampato ad "adesso" → **non
      stantio**; **oltre `max_skew` è stantio (fail-CLOSED)**. Uno skew grande rende
      inaffidabile l'anti-stale — con il clock locale INDIETRO un backlog vecchio avrebbe
      `msg.date` "nel futuro" e passerebbe come fresco — quindi si scarta invece di
      ripiazzare un segnale non databile con certezza. `max_skew` è coerciuto in modo
      difensivo (bool/malformato/NaN/inf/`<=0` → `DEFAULT_MAX_CLOCK_SKEW`), così una
      config rotta non disattiva la protezione. Il confine è **inclusivo** (skew == max_skew
      → ancora fresco), coerente con la soglia `max_age`.
    """
    # bool non è una soglia in secondi: un True/False trapelato da config ricade sul
    # default sicuro invece di valere 1/0 (che disattiverebbe il filtro per sbaglio).
    if isinstance(max_age, bool):
        max_age = DEFAULT_MAX_AGE
    else:
        try:
            max_age = float(max_age)
        except (TypeError, ValueError, OverflowError):
            max_age = DEFAULT_MAX_AGE    # None/"abc"/Decimal enorme → default (no crash)
        else:
            if not math.isfinite(max_age):
                max_age = DEFAULT_MAX_AGE  # NaN/inf → default sicuro
    if max_age <= 0:
        return False                 # solo un valore esplicito <= 0 disattiva il filtro
    # Timestamp del MESSAGGIO mancante/illeggibile → stantio (fail-closed, A4): un backlog
    # senza data non deve passare l'anti-stale.
    try:
        msg_epoch = float(message_epoch)
    except (TypeError, ValueError, OverflowError):
        return True
    if not math.isfinite(msg_epoch):
        return True                  # NaN/inf nel timestamp messaggio → stantio
    # `now` (il TUO clock) illeggibile → fail-open: non scartare un segnale buono.
    try:
        now_f = float(now)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(now_f):
        return False
    # Messaggio dal FUTURO rispetto al clock locale (clock skew, #311-3.5-d). Un timestamp
    # Telegram avanti rispetto al TUO orologio indica orologi non sincronizzati:
    #   - futuro entro `max_skew` → clamp ad "adesso": trattato come fresco (non stantio);
    #   - futuro OLTRE `max_skew` → skew implausibile → fail-CLOSED (stantio, scartato). Con il
    #     clock locale indietro un backlog vecchio sembrerebbe "fresco": meglio scartarlo che
    #     ripiazzare un segnale la cui età non è databile con certezza.
    # `max_skew` coerciuto in modo difensivo: bool/malformato/NaN/inf/<=0 → DEFAULT_MAX_CLOCK_SKEW,
    # così una config rotta non spegne la protezione. Confine inclusivo (skew == max_skew → fresco).
    if msg_epoch > now_f:
        skew_tol = _coerce_positive_finite(max_skew)
        if skew_tol is None:
            skew_tol = float(DEFAULT_MAX_CLOCK_SKEW)
        return (msg_epoch - now_f) > skew_tol
    return (now_f - msg_epoch) > max_age
