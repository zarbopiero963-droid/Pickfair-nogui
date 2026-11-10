"""Redazione dei segreti nel testo (token Telegram canonici + segreti registrati per-literal).

Modulo SENZA dipendenze dal progetto, estratto da `core.event_log` (che lo riesporta con gli stessi nomi: API
invariata). Serve a chi sta nel percorso vivo di Pickfair (la scatola nera, PF-BLACKBOX-01) senza trascinare
`core.event_log` -> `core.config_store`, cioe' il Bridge XTrader, nel grafo degli import vivi
(tests/percorsi/test_via_dal_bridge.py).
"""

from __future__ import annotations

import functools
import re
import threading
from urllib.parse import quote

# Pattern di un bot token Telegram: <id numerico>:<~35 caratteri>. Va mascherato
# ovunque possa finire in un log (es. un'eccezione che incorpora il token), per
# rispettare l'invariante "mai token in chiaro nei log".
_TELEGRAM_TOKEN_RE = re.compile(r"\d{6,}:[A-Za-z0-9_-]{20,}")
_REDACTED = "[REDACTED_TOKEN]"

# Registro di segreti ESATTI da mascherare per-literal, OLTRE alla regex (issue #184 M7).
# La regex copre solo lo shape CANONICO del token (<id>:<20+ char>): una forma non-standard
# (porzione segreta < 20 char, URL-encoded, spezzata su righe) le sfuggirebbe. Registrando il
# token VIVO da config (`register_secret`), lo si maschera comunque ovunque compaia, in
# qualunque forma. Soglia minima di lunghezza per non mascherare frammenti banali. Thread-safe
# (additivo). Mirror della stessa idea in `betfair/log_safety`.
_MIN_SECRET_LEN = 8
_secret_lock = threading.Lock()
_secret_literals: set = set()


def register_secret(value) -> bool:
    """Registra un valore segreto (es. il bot token da config) da mascherare per-literal in
    `redact_secrets`, ovunque compaia e in QUALSIASI forma. Ritorna `True` se registrato.
    Valori vuoti/non-stringa o troppo corti (< 8 char) sono ignorati (`False`): meglio non
    mascherare un frammento banale che inquinerebbe i log."""
    if not value:
        return False
    s = str(value)
    if len(s) < _MIN_SECRET_LEN:
        return False
    with _secret_lock:
        _secret_literals.add(s)
    return True


def unregister_secret(value) -> None:
    """Rimuove un segreto dal registro (es. quando il token cambia)."""
    if not value:
        return
    with _secret_lock:
        _secret_literals.discard(str(value))
    # Non trattenere il segreto nella cache delle regex CR/LF oltre la sua registrazione.
    _crlf_tolerant_re.cache_clear()


def clear_secrets() -> None:
    """Svuota il registro dei segreti (utile nei test e in un reset completo)."""
    with _secret_lock:
        _secret_literals.clear()
    _crlf_tolerant_re.cache_clear()    # nessun segreto deve sopravvivere nella cache regex


def _secret_forms(secret: str):
    """Forme derivate di un segreto da mascherare, OLTRE al literal grezzo: la forma
    **URL-encoded** (`:`→`%3A`, ecc.), realistica quando il token finisce in un URL/HTTP (es. il
    path `…/bot<token>/…` o una query) dentro il testo di un'eccezione o nella diagnostica.

    Senza questa derivazione, registrare il token GREZZO (come fa `app._register_secret_token`)
    non maschererebbe la sua forma encoded, che né la regex né il match grezzo riconoscono
    (Codex #184 M7). Coprire OGNI possibile re-encoding/normalizzazione è impossibile: si coprono
    le forme realistiche; resta il limite residuo documentato in `redact_secrets`."""
    forms = {secret}
    enc = quote(secret, safe="")
    if enc != secret:
        forms.add(enc)
    return forms


@functools.lru_cache(maxsize=256)
def _crlf_tolerant_re(sec: str):
    """Regex che matcha `sec` anche se spezzato da CR/LF tra i caratteri (#203).

    Un token registrato può finire **wrappato** su più righe in un log/traceback
    (`123456789:\\nSecret…`): un `str.replace` esatto non lo riconoscerebbe e lo scriverebbe in
    chiaro. Inserendo `[\\r\\n]*` tra ogni carattere si maschera anche quella forma, restando
    retro-compatibile con le occorrenze su singola riga (zero newline → match identico). Si
    tollerano solo CR/LF (non spazi generici) per non sovra-redarre testo non correlato. Cache
    per non ricompilare lo stesso pattern a ogni redazione."""
    return re.compile(r"[\r\n]*".join(re.escape(ch) for ch in sec))


def redact_secrets(text: str) -> str:
    """Maschera i segreti nei log (GUI e file): un token incorporato per sbaglio (es. nel testo
    di un'eccezione) non viene mai scritto in chiaro.

    Due livelli (issue #184 M7):
    1. **regex** sullo shape CANONICO del bot token Telegram (`<id>:<20+ char>`) — euristica che
       intercetta anche token NON registrati (es. di un'altra fonte);
    2. **per-literal** dei segreti registrati con `register_secret` (es. il token VIVO da
       config), mascherati nelle loro forme derivate (`_secret_forms`: grezzo + URL-encoded) E
       tolleranti ai CR/LF inseriti tra i caratteri (`_crlf_tolerant_re`, #203) — così è coperta
       sia la forma encoded che la regex non riconosce (registrando solo il token GREZZO, Codex
       #184 M7), sia il token **spezzato su più righe** dal wrapping di un log/traceback.

    Limite residuo onesto: un segreto MAI registrato, o in una forma derivata non prevista
    (es. doppia codifica, separatori diversi da CR/LF), può ancora sfuggire; per questo il token
    di config va registrato (lo fa `app` a load/save)."""
    s = _TELEGRAM_TOKEN_RE.sub(_REDACTED, str(text or ""))
    # Espande ogni literal registrato nelle sue forme derivate (grezzo + URL-encoded), poi
    # sostituisce le più LUNGHE prima: evita che un segreto contenuto in un altro venga
    # mascherato a metà lasciando un frammento dell'altro in chiaro. Ogni forma è matchata in
    # modo CR/LF-tollerante così un token wrappato su più righe non sfugge.
    with _secret_lock:
        literals = list(_secret_literals)
    forms = set()
    for sec in literals:
        forms.update(_secret_forms(sec))
    for sec in sorted(forms, key=len, reverse=True):
        if not sec:
            continue
        s = _crlf_tolerant_re(sec).sub(_REDACTED, s)
    return s
