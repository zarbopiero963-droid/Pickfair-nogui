"""Mapping alias Telegram → selezione XTrader in italiano (PR-08).

Usa il dizionario reale (`data/dizionario_xtrader.csv`) per tradurre un alias
del segnale in MarketType / MarketName / SelectionName italiani, sostituendo i
placeholder dinamici ({HOME_TEAM}/{AWAY_TEAM}). Rende operativo il dizionario di
PR-07: niente più SelectionName inglesi tipo "Over 0.5 Goals" quando l'alias è
riconosciuto.

`resolve(market_alias, selection_alias)` lavora sugli alias propri del dizionario.
`resolve_shorthand(text)` accetta le forme brevi dei messaggi Telegram
(es. "OVER 2.5", "GG", "1") tramite la tabella SYNONYMS.
Alias sconosciuto → None (il chiamante decide; il blocco duro è PR-10).
"""

import threading

from .dizionario import alias_key, assert_no_duplicate_aliases, load_dizionario


def _ou(line: str, half: bool):
    """Genera le voci over/under per una linea ("0.5".."8.5").

    Ritorna coppie (chiave_shorthand_normalizzata) -> (market_alias, selection_alias)
    per FT (suffisso eliminato in normalizzazione) o HT (suffisso "ht" esplicito).
    """
    suffix = "ht" if half else "ft"
    market = f"over_under_{line}_{suffix}"
    out = {}
    for side in ("over", "under"):
        # FT: chiave senza suffisso (la normalizzazione toglie " ft").
        # HT: chiave con " ht" (non rimosso, cambia mercato).
        key = f"{side} {line} ht" if half else f"{side} {line}"
        out[key] = (market, f"{side} {line} {suffix}")
    return out


# Forme brevi Telegram (chiavi già normalizzate) → (MarketAliasTelegram, SelectionAliasTelegram).
SYNONYMS = {
    "1":       ("esito_finale", "1"),
    "x":       ("esito_finale", "x"),
    "2":       ("esito_finale", "2"),
    "gg":      ("goal_no_goal", "goal"),
    "goal":    ("goal_no_goal", "goal"),
    "ng":      ("goal_no_goal", "no goal"),
    "no goal": ("goal_no_goal", "no goal"),
}
for _line in ("0.5", "1.5", "2.5", "3.5", "4.5", "5.5", "6.5", "7.5", "8.5"):
    SYNONYMS.update(_ou(_line, half=False))     # FT 0.5–8.5
for _line in ("0.5", "1.5", "2.5"):
    SYNONYMS.update(_ou(_line, half=True))      # HT 0.5/1.5/2.5

_INDEX = None
# Lock per l'init lazy: senza, due thread al primo uso concorrente potrebbero
# costruire l'indice due volte (A8). La build è idempotente (stesso CSV → stesso
# dict), quindi era benigno; il lock lo rende comunque un'unica costruzione.
_INDEX_LOCK = threading.Lock()


def _index() -> dict:
    """Indice (lazy, in cache) del dizionario per chiave alias normalizzata.

    Double-checked locking (A8): si costruisce in un dict LOCALE e lo si pubblica
    in `_INDEX` solo a costruzione finita, così il check esterno (fuori dal lock)
    non vede mai un indice parzialmente popolato."""
    global _INDEX
    if _INDEX is None:
        with _INDEX_LOCK:
            if _INDEX is None:
                rows = load_dizionario()
                # Alias (Market, Selection) duplicato → lookup AMBIGUO: `idx[k]=row` farebbe
                # vincere in silenzio l'ULTIMA riga → selezione sbagliata = bet sbagliato.
                # Guardia condivisa col percorso live (value_maps), fail-closed (audit B3/Codex).
                assert_no_duplicate_aliases(rows)
                idx = {}
                for row in rows:
                    idx[alias_key(row["MarketAliasTelegram"],
                                  row["SelectionAliasTelegram"])] = row
                _INDEX = idx
    return _INDEX


def normalize_shorthand(text: str) -> str:
    """Normalizza una forma breve Telegram: minuscolo, spazi singoli,
    virgola→punto, e suffisso FT rimosso ("over 2.5 ft" / "OVER 2,5" -> "over 2.5").

    Funzione pubblica: fonte unica della normalizzazione shorthand, riusata anche
    dalle value-map (CP-03) così le grafie comuni dei messaggi combaciano."""
    s = " ".join(str(text).strip().lower().replace(",", ".").split())
    if s.endswith(" ft"):
        s = s[:-3].strip()
    return s


_norm_shorthand = normalize_shorthand  # alias interno storico


def _subst(value: str, home: str, away: str) -> str:
    out = str(value)
    if home:
        out = out.replace("{HOME_TEAM}", home)
    if away:
        out = out.replace("{AWAY_TEAM}", away)
    return out


def resolve(market_alias: str, selection_alias: str, home: str = "", away: str = ""):
    """Risolve una coppia di alias del dizionario in una selezione XTrader.

    Ritorna un dict (MarketType/MarketName/SelectionName/Handicap/BetType) con i
    placeholder sostituiti, oppure None se la coppia non è nel dizionario **o**
    se rimane un placeholder non sostituito (es. squadra mancante): in quel caso
    la selezione non è utilizzabile e non va scritta.
    """
    row = _index().get(alias_key(market_alias, selection_alias))
    if not row:
        return None
    market_name = _subst(row["MarketName_XTrader"], home, away)
    selection_name = _subst(row["SelectionName_XTrader"], home, away)
    if "{" in selection_name or "{" in market_name:
        return None     # placeholder non risolto (dati squadra incompleti)
    return {
        "MarketType":    row["MarketType_XTrader"],
        "MarketName":    market_name,
        "SelectionName": selection_name,
        "Handicap":      row["Handicap"] or "0",
        "BetType":       row["BetType_XTrader"],
    }


def is_known_shorthand(text: str) -> bool:
    """True se la forma breve è mappata (a prescindere dalla risoluzione squadre)."""
    return _norm_shorthand(text) in SYNONYMS


def resolve_shorthand(text: str, home: str = "", away: str = ""):
    """Risolve una forma breve Telegram (es. "OVER 2.5", "GG", "1") via SYNONYMS.

    Ritorna lo stesso dict di `resolve()`, oppure None se la forma non è mappata
    o se i placeholder non sono risolvibili.
    """
    pair = SYNONYMS.get(_norm_shorthand(text))
    if not pair:
        return None
    return resolve(pair[0], pair[1], home, away)
