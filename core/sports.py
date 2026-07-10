"""Sport del «blocco personale» — fonte UNICA (issue #86 PR-P9).

Centralizza l'elenco degli sport supportati e la loro mappatura allo
`event_type_id` ufficiale Betfair, così parser personalizzati, tab Betfair Sync e
catalogue client usano la **stessa** definizione (niente drift fra moduli).

Sport supportati dal blocco personale e relativo `event_type_id` Betfair:

- Calcio → 1
- Tennis → 2
- Basket → 7522
- Rugby Union → 5

Lo «sport non specificato» è la stringa vuota (`SPORT_UNSPECIFIED`): un parser
senza sport è **agnostico** (retro-compatibile con i parser salvati prima di PR-P9
e con i flussi che non dipendono dallo sport). Quando in PR successive un parser
risolverà ID Betfair (EventId/MarketId/SelectionId) dal dizionario locale, lo
sport servirà a restringere la ricerca all'`event_type_id` giusto.
"""

# Mappa canonica sport → event_type_id ufficiale Betfair. L'ordine è anche quello
# di visualizzazione in GUI (dict ordinato per inserimento).
SPORTS_EVENT_TYPE = {
    "Calcio": "1",
    "Tennis": "2",
    "Basket": "7522",
    "Rugby Union": "5",
}

# Tupla dei nomi sport supportati (ordine di visualizzazione).
SPORTS = tuple(SPORTS_EVENT_TYPE)

# Sentinella «sport non specificato» (parser agnostico).
SPORT_UNSPECIFIED = ""


def normalize_sport(name):
    """Ritorna lo sport canonico (match case-insensitive, spazi esterni ignorati) o
    ``None`` se non supportato. La stringa vuota/None → ``None`` (non è uno sport)."""
    key = str(name or "").strip().casefold()
    if not key:
        return None
    for sport in SPORTS:
        if sport.casefold() == key:
            return sport
    return None


def is_supported_sport(name) -> bool:
    """``True`` se `name` è uno sport supportato (case-insensitive)."""
    return normalize_sport(name) is not None


def event_type_id_for_sport(name):
    """`event_type_id` Betfair dello sport (canonicalizzato) o ``None`` se lo sport
    non è supportato / non specificato. Non solleva: i chiamati gestiscono ``None``
    fail-closed (nessuna risoluzione ID quando lo sport è ignoto)."""
    sport = normalize_sport(name)
    if sport is None:
        return None
    return SPORTS_EVENT_TYPE[sport]


# Inverso di `SPORTS_EVENT_TYPE`: `event_type_id` → nome sport canonico.
_EVENT_TYPE_TO_SPORT = {etid: sport for sport, etid in SPORTS_EVENT_TYPE.items()}


def sport_for_event_type_id(event_type_id):
    """Nome sport canonico dato l'`event_type_id` Betfair, o ``None`` se ignoto.

    Inverso di `event_type_id_for_sport`. Usato dall'harvest dei nomi squadra (#282)
    per salvare i nomi permanenti sotto il **nome** dello sport (la chiave con cui li
    consulta la mappatura nomi della GUI), non sotto l'id numerico. Non solleva:
    un id non riconosciuto → ``None`` (il chiamante salta, fail-closed).

    Check esplicito su ``None`` (non ``or ""``): così un id *falsy ma legittimo* — se
    Betfair introducesse un `event_type_id` come `0` — verrebbe cercato come ``"0"`` e
    non collasserebbe silenziosamente su ``""`` (robustezza, review Fable 5)."""
    if event_type_id is None:
        return None
    return _EVENT_TYPE_TO_SPORT.get(str(event_type_id))
