"""Helper condivisi di validazione/sanitizzazione input (fonte UNICA, anti-drift).

Raccoglie regole prima **duplicate** in più moduli safety-critical, così una
correzione non rischia di applicarsi a una copia e dimenticarne un'altra
(audit #105 / #133 item 6, parte "validatori"):

- `require_positive_int` / `require_finite_now`: validazione difensiva di
  parametri numerici e timestamp — un valore malformato (bool da JSON, NaN/inf,
  ``<= 0``, non intero) deve fallire con `ValueError`, non rendere un limite
  inefficace o sempre bloccante. Usati da `safety_guard.DailyLimiter` e
  `signal_dedupe.SignalTracker`.
- `WIN_RESERVED` + `safe_filename_core`: nucleo comune della sanitizzazione del
  nome file (Windows). I chiamanti (`custom_parser`, `profile_store`) applicano
  poi il PROPRIO fallback, volutamente diverso (vedi sotto), quindi qui resta solo
  la parte condivisa.

Modulo **puro**: nessuna dipendenza da GUI/CSV/Telegram, testabile headless.
"""

import math

# Nomi device riservati di Windows: un file con questo nome-base (anche con
# estensione) non è creabile. Match ESATTO, case-insensitive (es. "con" sì,
# "console" no). Fonte unica condivisa.
WIN_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{i}" for i in range(1, 10)}
    | {f"lpt{i}" for i in range(1, 10)}
)


def require_positive_int(value, name: str) -> int:
    """`value` come int finito e > 0, altrimenti `ValueError`.

    Rifiuta esplicitamente `bool` (``True``/``False`` da JSON verrebbero coerciti a
    1/0: es. `max_per_day=True` capperebbe l'app a 1 segnale/giorno invece di essere
    trattato come config malformata) e `NaN`/`inf`/`<= 0`/non-interi (renderebbero il
    limite inefficace o sempre bloccante)."""
    if isinstance(value, bool):
        raise ValueError(f"{name} non valido: {value!r}")
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        # OverflowError (#318 L1-1): un int troppo grande per un float (es. 10**400) — da config
        # corrotta/manomessa — NON è sottoclasse di ValueError, quindi senza catturarlo esplicito
        # `float()` propagherebbe e farebbe crashare l'handler/START. Trattato come malformato
        # (ValueError controllato), coerente con message_freshness/csv_writer.
        raise ValueError(f"{name} non valido: {value!r}") from None
    if not math.isfinite(f) or f <= 0 or f != int(f):
        raise ValueError(f"{name} deve essere un intero > 0 (ricevuto {value!r})")
    return int(f)


def require_finite_now(now) -> float:
    """`now` (epoch) come float finito, altrimenti `ValueError`.

    Rifiuta `bool` (``True``/``False`` non sono timestamp) e `NaN`/`inf`, che
    falserebbero finestra di deduplica, conteggio al minuto e reset giornaliero."""
    if isinstance(now, bool):
        raise ValueError(f"now non valido: {now!r}")
    try:
        f = float(now)
    except (TypeError, ValueError, OverflowError):
        # OverflowError (#318 L1-1): un int troppo grande per un float (es. 10**400) non è
        # sottoclasse di ValueError → senza catturarlo `float()` crasherebbe il chiamante
        # (DailyLimiter/SignalTracker). Trattato come `now` malformato (ValueError controllato).
        raise ValueError(f"now non valido: {now!r}") from None
    if not math.isfinite(f):
        raise ValueError(f"now deve essere finito (ricevuto {now!r})")
    return f


# ---------------------------------------------------------------------------
# Campi d'ordine (#461 PR27, PKG-P24-A): fonte unica fail-closed per gli
# ingressi che convergono all'autorita' ordini (engine, runtime, OrderManager).
# Il client Betfair (#488) resta la difesa finale e NON si duplica: qui
# l'invalido si ferma prima di persistenza, anti-duplicazione, tavoli e
# trasporto. Gli errori portano gli stessi codici del client.
# ---------------------------------------------------------------------------
def finite_number(value):
    """`value` come float finito, altrimenti `None`.

    `bool` non e' un numero (True/False da JSON diventerebbero 1/0), e nemmeno
    NaN/inf o un testo non numerico. Testi numerici ("2.0") restano ammessi."""
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def order_market_id(value) -> str:
    """Market id non vuoto; un bool o un numero non finito non lo sono."""
    if isinstance(value, bool):
        raise ValueError("INVALID_MARKET_ID")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("INVALID_MARKET_ID")
    text = str(value if value is not None else "").strip()
    if not text:
        raise ValueError("INVALID_MARKET_ID")
    return text


def order_selection_id(value) -> int:
    """Selection id intero > 0, senza troncamenti: 5678.9 non diventa 5678."""
    if isinstance(value, bool):
        raise ValueError("INVALID_SELECTION_ID")
    if isinstance(value, int):
        n = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError("INVALID_SELECTION_ID")
        n = int(value)
    elif isinstance(value, str) and value.strip().isdigit():
        n = int(value.strip())
    else:
        raise ValueError("INVALID_SELECTION_ID")
    if n <= 0:
        raise ValueError("INVALID_SELECTION_ID")
    return n


def order_price(value) -> float:
    """Quota finita > 1.0, la stessa soglia del client."""
    f = finite_number(value)
    if f is None or f <= 1.0:
        raise ValueError("INVALID_PRICE")
    return f


def order_stake(value) -> float:
    """Stake finito > 0."""
    f = finite_number(value)
    if f is None or f <= 0.0:
        raise ValueError("INVALID_SIZE")
    return f


# ---------------------------------------------------------------------------
# Rischio dell'ordine (#461 PR28, PKG-P24-B): formula UNICA per tutti i
# consumer dei cap owner (money management, cap A2, tavoli, auto-next,
# RiskGate). BACK rischia lo stake; LAY rischia la liability stake*(prezzo-1)
# (decisione owner #393, H-14). La soglia vale con tolleranza +-epsilon: la
# somma float 0.1+0.2 o il prodotto 0.1*3 non devono negare un ordine esatto
# al cap, e un superamento reale (anche di un centesimo) resta un superamento.
# ---------------------------------------------------------------------------
EXPOSURE_REL_TOL = 1e-9
EXPOSURE_ABS_TOL = 1e-9


def order_exposure(side, stake, price) -> float:
    """Euro a rischio dell'ordine: BACK = stake, LAY = stake * (price - 1).

    Lato diverso da BACK/LAY (assente, bool, testo) => ValueError: il rischio
    di un lato sconosciuto non si stima."""
    lato = side.strip().upper() if isinstance(side, str) else None
    if lato not in ("BACK", "LAY"):
        raise ValueError("INVALID_SIDE")
    importo = order_stake(stake)
    if lato == "BACK":
        return importo
    return importo * (order_price(price) - 1.0)


def signal_side(signal, *, default=None) -> str:
    """Lato di un segnale runtime, risolto come il payload `CMD_QUICK_BET`
    (`bet_type` / `side` / `action`, maiuscolo). Assenza, ambiguita' o valore
    invalido sono rifiutati: un ordine non diventa mai BACK per default."""
    raw = None
    if isinstance(signal, dict):
        raw = signal.get("bet_type") or signal.get("side") or signal.get("action")
    if raw in (None, "") and default is not None:
        raw = default
    if not isinstance(raw, str):
        raise ValueError("INVALID_SIDE")
    side = raw.strip().upper()
    if side not in ("BACK", "LAY"):
        raise ValueError("INVALID_SIDE")
    aliases = []
    source = signal if isinstance(signal, dict) else {}   # non-dict: solo il default, mai AttributeError
    for key in ("bet_type", "side", "action"):
        value = source.get(key)
        if value not in (None, ""):
            if not isinstance(value, str) or value.strip().upper() not in ("BACK", "LAY"):
                raise ValueError("INVALID_SIDE")
            aliases.append(value.strip().upper())
    if len(set(aliases)) > 1:
        raise ValueError("INVALID_SIDE")
    return side


LEG_SIDE_FIELDS = ("side", "effectiveType", "bet_type")


def leg_side(leg, *, default=None) -> str:
    """Lato di UNA gamba Dutching (#529 ciclo A5, decisione owner). Regola unica per il RiskMiddleware e il
    DutchingController:

    - un campo di lato della gamba (`side`, `effectiveType`, `bet_type`) PRESENTE conta come esplicito,
      qualunque valore abbia: deve essere BACK o LAY (maiuscole/spazi esterni normalizzati). Vuoto, soli
      spazi, None, non stringa o sconosciuto => INVALID_SIDE, mai ereditato dal lotto;
    - piu' campi espliciti discordanti della stessa gamba => INVALID_SIDE, mai una scelta arbitraria;
    - solo se NESSUN campo e' presente vale il `default` (il lato del lotto), anch'esso BACK/LAY valido;
      altrimenti INVALID_SIDE (nessun BACK per default).
    """
    if not isinstance(leg, dict):
        raise ValueError("INVALID_SIDE")
    sides = []
    for key in LEG_SIDE_FIELDS:
        if key in leg:
            value = leg[key]
            if not isinstance(value, str) or value.strip().upper() not in ("BACK", "LAY"):
                raise ValueError("INVALID_SIDE")
            sides.append(value.strip().upper())
    if sides:
        if len(set(sides)) > 1:
            raise ValueError("INVALID_SIDE")
        return sides[0]
    if isinstance(default, str) and default.strip().upper() in ("BACK", "LAY"):
        return default.strip().upper()
    raise ValueError("INVALID_SIDE")


def order_exposure_or_inf(side, stake, price) -> float:
    """Come `order_exposure`, ma un ordine non stimabile vale inf (fail-closed)."""
    try:
        return order_exposure(side, stake, price)
    except ValueError:
        return float("inf")


def open_exposure_cap_reason(cap_raw, total_exposure, order_risk) -> str:
    """Cap A2 `max_open_exposure` (euro assoluti) sul rischio dell'ordine.

    Assente => "" (nessun cap); configurato ma illeggibile (NaN dal loader,
    inf, bool, testo) => nega (PR27); soglia esatta con tolleranza (PR28).
    Ritorna il motivo del rifiuto oppure ""."""
    if cap_raw is None:
        return ""
    cap = finite_number(cap_raw)
    if cap is None:
        return "max_open_exposure_non_valido"
    if exceeds_cap(total_exposure + order_risk, cap):
        return f"max_open_exposure_exceeded:limit={cap_raw}€"
    return ""


def signal_cap_reason(signal, stake, cap_raw, total_exposure) -> str:
    """Cap A2 sul rischio di un segnale (lato e quota come nel payload)."""
    sig = signal if isinstance(signal, dict) else {}
    try:
        side = signal_side(sig)
    except ValueError:
        return "lato_o_quota_non_validi"
    risk = order_exposure_or_inf(side, stake, sig.get("price") or sig.get("odds"))
    if not math.isfinite(risk):
        return "lato_o_quota_non_validi"
    return open_exposure_cap_reason(cap_raw, total_exposure, risk)


def absolute_cap_reason(name, cap_raw, current, order_risk) -> str:
    """Valida e applica un cap assoluto Roserpina, senza coercizione bool."""
    if cap_raw is None:
        return ""
    cap = finite_number(cap_raw)
    base = finite_number(current)
    risk = finite_number(order_risk)
    if cap is None or cap <= 0:
        return f"{name}_non_valido"
    if base is None or base < 0 or risk is None or risk < 0:
        return f"{name}_non_verificabile"
    if exceeds_cap(base + risk, cap):
        return f"{name}_exceeded:limit={cap_raw}€"
    return ""


# PFC-008 (roadmap #461, M019 C3): percentuali di rischio Roserpina OBBLIGATORIE.
# Una chiave assente non riceve piu' un valore di codice: il loader la passa come
# None e ogni nuovo ordine viene negato finche' l'utente non la salva (F10).
RISK_PCT_FIELDS = (
    "target_profit_cycle_pct",
    "max_single_bet_pct",
    "max_total_exposure_pct",
    "max_event_exposure_pct",
)


def risk_pct_reason(name, raw) -> str:
    """Percentuale di rischio obbligatoria: "" se valida, altrimenti il motivo.

    None (chiave assente) => `<name>_non_configurato`; non numerica, bool, NaN/inf,
    <= 0 o > 100 => `<name>_non_valido`. Un confronto con NaN e' sempre falso e
    lascerebbe passare qualunque esposizione: qui si nega prima."""
    if raw is None:
        return f"{name}_non_configurato"
    pct = finite_number(raw)
    if pct is None or pct <= 0 or pct > 100:
        return f"{name}_non_valido"
    return ""


def risk_pcts_reason(config, names=RISK_PCT_FIELDS) -> str:
    """Primo motivo di rifiuto fra le percentuali `names` di `config`, oppure ""."""
    for name in names:
        reason = risk_pct_reason(name, getattr(config, name, None))
        if reason:
            return reason
    return ""


def verified_cap_base(cash_equity, available):
    """Base dei cap percentuali di capacita'/esposizione (PFC-008, decisione owner #426 c.6369802091).

    La cassa complessiva VERIFICATA (disponibile + impegnato di uno stesso snapshot coerente, vedi
    `RuntimeController._cash_equity_verification`), oppure None: cassa assente, non finita, <= 0 o
    inferiore al disponibile. None => i chiamanti negano le nuove entrate (`cassa_non_verificata`):
    nessun ripiego, neppure sul disponibile. L'esposizione corrente si sottrae una sola volta, nel
    confronto del chiamante."""
    cash = finite_number(cash_equity)
    avail = finite_number(available)
    if cash is None or avail is None or avail < 0 or cash <= 0 or cash < avail:
        return None
    return cash


def desk_cash_basis(risk_desk, available, table_manager=None):
    """PFC-008: ``(base dei cap, esposizione oltre i tavoli)`` da un'unica lettura coerente della cassa.

    La base e' la cassa verificata. L'esposizione che il cap totale deve vedere e' la maggiore fra quella dei
    tavoli del bot (adesso) e ``impegnato della lettura + riserve del bot non ancora riflesse in quella
    lettura`` (`TableManager.pending_exposure(since=read_at)`, come il gate fondi PR28-e): il secondo valore
    tiene conto dell'impegnato fuori dai tavoli (bet manuali, altre app) senza che una riserva pendente lo
    "assorba". Si restituisce la parte oltre i tavoli, che i chiamanti sommano all'esposizione dei tavoli.
    Senza `pending_exposure` tutte le riserve contano come pendenti (prudente). ``(None, None)`` quando la
    cassa non e' verificata o un dato manca o non e' finito (un oggetto senza il campo non vale zero)."""
    reader = getattr(risk_desk, "cash_basis", None)
    if callable(reader):
        cash, committed_raw, read_at = reader()
    else:
        cash = getattr(risk_desk, "cash_equity_current", None)
        committed_raw = getattr(risk_desk, "cash_committed_exposure", None)
        read_at = getattr(risk_desk, "cash_read_at", None)
    base = verified_cap_base(cash, available)
    committed = finite_number(committed_raw)
    if base is None or committed is None or committed < 0:
        return None, None
    tables_now, pending = 0.0, 0.0
    if table_manager is not None:
        try:
            tables_now = float(table_manager.total_exposure())
            pending_fn = getattr(table_manager, "pending_exposure", None)
            pending = float(pending_fn(since=read_at)) if callable(pending_fn) else tables_now
        except Exception:
            return None, None
    if not (math.isfinite(tables_now) and math.isfinite(pending)):
        return None, None
    return base, max(0.0, committed + max(0.0, pending) - max(0.0, tables_now))


def reaches_limit(amount, limit) -> bool:
    """True se `amount` raggiunge `limit` (soglia di arresto, con tolleranza)."""
    return amount >= limit or math.isclose(amount, limit, rel_tol=EXPOSURE_REL_TOL, abs_tol=EXPOSURE_ABS_TOL)


def exceeds_cap(amount, cap) -> bool:
    """True se `amount` supera `cap` oltre la tolleranza float.

    Alla soglia esatta (anche con errore di arrotondamento) non supera."""
    if amount <= cap:
        return False
    return not math.isclose(amount, cap, rel_tol=EXPOSURE_REL_TOL, abs_tol=EXPOSURE_ABS_TOL)


def safe_filename_core(name: str) -> str:
    """Nucleo condiviso della sanitizzazione di un nome file (Windows).

    Tiene solo alfanumerici, ``-``, ``_`` e spazi (poi spazi → ``_``); evita path
    traversal e caratteri non validi; prefissa con ``_`` i NOMI DEVICE RISERVATI
    (``con``/``nul``/``com1``…). Ritorna la stringa pulita, **eventualmente vuota**:
    il fallback su vuoto è LASCIATO al chiamante, perché diverge per dominio —
    `custom_parser` usa un default (``"parser"``), `profile_store` rifiuta il nome
    vuoto. Per questo i due `_safe_filename` restano funzioni separate, ma il nucleo
    è unico (anti-drift).

    Annotato `name: str` per il contratto, ma `str(name)` resta come rete difensiva:
    un chiamante che passi un non-stringa per errore non deve far crashare l'I/O."""
    cleaned = "".join(c for c in str(name).strip() if c.isalnum() or c in " -_")
    cleaned = "_".join(cleaned.split())
    if cleaned.casefold() in WIN_RESERVED:
        cleaned = "_" + cleaned
    return cleaned
