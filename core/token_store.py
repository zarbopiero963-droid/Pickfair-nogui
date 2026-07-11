"""Storage sicuro del Bot Token via OS keyring (audit #105 P1 — token storage).

Il `bot_token` Telegram è una **credenziale**: tenerlo in chiaro in
``%APPDATA%\\XTraderBridge\\config.json`` lo espone a malware locale o a un
backup/sync del profilo Windows. Questo modulo lo sposta nel **keyring del sistema
operativo** (Windows Credential Manager, macOS Keychain, Secret Service su Linux)
tramite la libreria `keyring`.

**Fallback esplicito.** `keyring` è una dipendenza che può mancare o non avere un
backend utilizzabile (CI headless, Linux senza Secret Service): in quel caso le
funzioni segnalano "non disponibile" e il chiamante (`config_store`) RIPIEGA sul
comportamento storico — token in chiaro nel `config.json` — loggando un avviso.
Mai un crash per assenza del backend, mai una perdita del token.

Tutte le operazioni keyring sono protette: **qualsiasi** eccezione è trattata come
backend non disponibile (fail-safe verso il fallback). Modulo a import "soft":
`keyring` viene importato solo quando serve, così l'assenza della libreria non
rompe nemmeno l'import di questo modulo.
"""

# Spazio dei nomi nel keyring. SERVICE identifica l'app, ACCOUNT la voce.
SERVICE = "XTraderBridge"
ACCOUNT = "bot_token"


def _keyring():
    """Il modulo `keyring` se importabile e con un backend, altrimenti ``None``
    (dipendenza opzionale: l'assenza NON deve propagarsi come errore)."""
    try:
        import keyring
        return keyring
    except Exception:
        return None


def available() -> bool:
    """``True`` se esiste un backend keyring **usabile**.

    Probe NON distruttivo: `get_password` su una voce sonda. Sul backend ``fail``
    (nessun keyring reale) solleva, quindi qui ritorniamo ``False`` → fallback."""
    kr = _keyring()
    if kr is None:
        return False
    try:
        kr.get_password(SERVICE, "__probe__")
        return True
    except Exception:
        return False


def save_token(token: str) -> bool:
    """Salva `token` nel keyring. ``True`` se riuscito, ``False`` se il backend è
    assente o solleva (il chiamante ripiega sul plaintext). Un token vuoto non va
    salvato: usa `delete_token`."""
    kr = _keyring()
    if kr is None or not token:
        return False
    try:
        kr.set_password(SERVICE, ACCOUNT, token)
        return True
    except Exception:
        return False


def load_token_status():
    """Come `load_token` ma **distingue assente da lettura fallita** (#140).

    Ritorna `(token, read_ok)`:
    - `read_ok=True`, `token` = valore (str) → c'è un token;
    - `read_ok=True`, `token=None` → il keyring è leggibile e NON c'è alcun token (assente);
    - `read_ok=False`, `token=None` → il backend manca o ha **sollevato**: non sappiamo se un
      token esista (lettura fallita). Il chiamante NON deve trattarlo come "assente" (es. nel
      rollback: cancellare distruggerebbe un token preesistente illeggibile)."""
    kr = _keyring()
    if kr is None:
        return None, False
    try:
        return kr.get_password(SERVICE, ACCOUNT), True
    except Exception:
        return None, False


def load_token():
    """Il token dal keyring, oppure ``None`` se assente o backend non disponibile.

    Non distingue "assente" da "lettura fallita": per quel caso usa `load_token_status`."""
    token, _ = load_token_status()
    return token


def delete_token() -> bool:
    """Rimuove il token dal keyring (best-effort). ``True`` se rimosso, ``False`` se
    assente/non disponibile (incl. la voce già inesistente)."""
    kr = _keyring()
    if kr is None:
        return False
    try:
        kr.delete_password(SERVICE, ACCOUNT)
        return True
    except Exception:
        # PasswordDeleteError (voce inesistente) o backend assente → niente da fare.
        return False
