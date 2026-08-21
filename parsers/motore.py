"""Applica i Parser Personalizzati dell'owner a un messaggio Telegram.

E' il punto che mancava. `services/telegram_signal_processor.py` provava a
fare questo lavoro con:

    from core.custom_parser_engine import CustomParserEngine   # non esiste
    ...
    except ImportError:
        pass

`CustomParserEngine` non e' definita in nessun punto del repository:
l'`ImportError` scattava sempre e veniva ingoiato, quindi i Parser
Personalizzati **non hanno mai girato** sul percorso di esecuzione di
Pickfair. E' il rilievo H-08 dell'audit #335.

Qui non si aggiunge un motore: quello esiste ed e' `custom_parser_engine`.
Si aggiunge cio' che non c'era — **caricare i parser dell'utente, sceglierne
uno, e tradurre l'estrazione nei nomi che usa Pickfair**.

## Le regole, tutte fail-closed

Questo modulo decide se un messaggio puo' diventare una scommessa. Ogni caso
incerto rifiuta, e ogni rifiuto ha un motivo leggibile in `Estrazione.motivo`:

- **nessun parser** → rifiuta. Nessun default, nessun parser implicito.
- **nessun parser riconosce il messaggio** → rifiuta.
- **piu' parser riconoscono lo stesso messaggio** → rifiuta. Sceglierne uno
  significherebbe indovinare quale scommessa voleva l'owner.
- **campi obbligatori mancanti** (`ready=False`) → rifiuta.
- **`BetType` non riconosciuto** → rifiuta: senza direzione non c'e' scommessa,
  e un default PUNTA sarebbe una posizione inventata.
- **un parser che solleva** → rifiuta quel parser e prosegue con gli altri; un
  file corrotto non deve impedire agli altri di funzionare, ma nemmeno essere
  scambiato per un rifiuto pulito.

Nota sul cancello di contenuto: `matches_message` esiste perche' un parser i
cui obbligatori sono tutti `fixed_value` produrrebbe una riga piazzabile per
QUALSIASI messaggio — cioe' la stessa scommessa a ogni messaggio del canale.
Va chiamato **sempre**, anche quando `apply_parser` dice `ready`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core import custom_parser, custom_parser_engine, value_maps

from .campi import VERSO_SEGNALE, cartella_parser, cartella_parser_del_bridge, normalizza_azione

# Motivi di rifiuto. Stringhe stabili: finiscono nei log e nei test.
NESSUN_PARSER = "NESSUN_PARSER"
NON_RICONOSCIUTO = "NON_RICONOSCIUTO"
PARSER_AMBIGUI = "PARSER_AMBIGUI"
CAMPI_MANCANTI = "CAMPI_MANCANTI"
DIREZIONE_ASSENTE = "DIREZIONE_ASSENTE"


def registro_value_map(*, con_dizionario: bool = True):
    """Registro delle value-map, tollerante all'assenza del dizionario.

    Le value-map traducono cio' che l'owner scrive nel canale ("GG") nel
    valore esatto del mercato ("Both Teams To Score" / "Yes"). Quelle
    interessanti derivano da un dizionario su CSV.

    **Quel dizionario oggi non esiste in questo repository**: il percorso e'
    `data/dizionario_xtrader.csv`, e `git ls-files` non lo trova. Senza, sono
    disponibili le sole mappe built-in (`bettype`).

    Se il dizionario manca o e' malformato NON si solleva: si degrada ai
    built-in. Il crash lo pagherebbe l'utente come "il bot non risponde",
    mentre la degradazione produce un rifiuto leggibile (`CAMPI_MANCANTI` sul
    campo che il dizionario avrebbe riempito).

    Restituisce `(registro, dizionario_disponibile)`, cosi' che il chiamante
    possa dire all'utente *perche'* i suoi parser non diventano piazzabili
    invece di lasciarlo indovinare.
    """
    if con_dizionario:
        try:
            return value_maps.registry(include_dizionario=True), True
        except Exception:
            pass
    try:
        return value_maps.registry(), False
    except Exception:
        return {}, False


@dataclass
class Estrazione:
    """Esito dell'applicazione dei parser dell'owner a un messaggio."""

    ok: bool
    motivo: str = ""
    parser: str = ""
    campi: Dict[str, Any] = field(default_factory=dict)
    dettagli: Dict[str, Any] = field(default_factory=dict)


def _rifiuta(motivo: str, **dettagli: Any) -> Estrazione:
    return Estrazione(ok=False, motivo=motivo, dettagli=dettagli)


def carica_parser(cartella: Optional[str] = None) -> List[Any]:
    """Carica i Parser Personalizzati salvati dall'owner.

    Un file illeggibile o non valido viene **saltato**, non fa fallire il
    caricamento: un parser rotto non deve mettere fuori uso quelli sani. I
    saltati sono comunque contati e restituiti dal chiamante che vuole saperlo
    (vedi `carica_parser_con_scarti`).
    """
    validi, _scartati = carica_parser_con_scarti(cartella)
    return validi


def carica_parser_con_scarti(cartella: Optional[str] = None):
    """Come `carica_parser`, ma restituisce anche i file scartati.

    Il chiamante che vuole avvisare l'utente ("hai 3 parser, uno e' rotto")
    usa questa; chi vuole solo lavorare usa `carica_parser`.
    """
    radice = cartella or cartella_parser()
    if not os.path.isdir(radice):
        _avvisa_se_i_parser_sono_rimasti_nel_bridge(radice)
        return [], []

    validi: List[Any] = []
    scartati: List[str] = []
    for percorso in sorted(custom_parser.list_parser_files(radice)):
        try:
            defn = custom_parser.load_parser(percorso)
        except Exception:
            scartati.append(percorso)
            continue
        if not custom_parser.is_valid(defn):
            scartati.append(percorso)
            continue
        validi.append(defn)
    return validi, scartati


def estrai(testo: str, parser: Optional[List[Any]] = None,
           cartella: Optional[str] = None,
           registro: Optional[Dict[str, Any]] = None) -> Estrazione:
    """Applica i parser dell'owner a `testo`.

    `parser` permette di passare le definizioni gia' caricate (il chiamante
    vivo le tiene in memoria invece di rileggere il disco a ogni messaggio).
    Se e' `None` vengono caricate da `cartella`.

    `registro` sono le value-map. Se `None` viene costruito qui con
    `registro_value_map()`; il chiamante vivo dovrebbe costruirlo una volta e
    passarlo, perche' con il dizionario presente e' una lettura da disco.
    """
    messaggio = str(testo or "")
    if not messaggio.strip():
        return _rifiuta(NON_RICONOSCIUTO, vuoto=True)

    definizioni = parser if parser is not None else carica_parser(cartella)
    if not definizioni:
        return _rifiuta(NESSUN_PARSER)

    mappe = registro if registro is not None else registro_value_map()[0]

    candidati = []
    for defn in definizioni:
        try:
            # Cancello di contenuto PRIMA dell'estrazione: un parser a soli
            # valori fissi e' "pronto" su qualunque testo.
            if not custom_parser_engine.matches_message(
                    defn, messaggio, getattr(defn, "mode", None)):
                continue
            esito = custom_parser_engine.apply_parser(defn, messaggio, mappe)
        except Exception:
            # Un parser che esplode non e' un parser che rifiuta: non deve
            # ne' passare, ne' zittire gli altri.
            continue
        candidati.append((defn, esito))

    if not candidati:
        return _rifiuta(NON_RICONOSCIUTO, valutati=len(definizioni))

    if len(candidati) > 1:
        return _rifiuta(PARSER_AMBIGUI,
                        parser=[getattr(d, "name", "?") for d, _ in candidati])

    defn, esito = candidati[0]
    nome = str(getattr(defn, "name", "") or "")

    if not getattr(esito, "ready", False):
        return Estrazione(ok=False, motivo=CAMPI_MANCANTI, parser=nome,
                          dettagli={"mancanti": list(getattr(esito, "missing_required", []))})

    campi = _verso_pickfair(dict(getattr(esito, "values", {}) or {}))
    if not campi.get("action"):
        return Estrazione(ok=False, motivo=DIREZIONE_ASSENTE, parser=nome)

    return Estrazione(ok=True, parser=nome, campi=campi)


def _verso_pickfair(valori: Dict[str, str]) -> Dict[str, Any]:
    """Traduce i campi del contratto nei nomi usati da Pickfair.

    Un campo non presente in `VERSO_SEGNALE` viene **scartato in silenzio**: e'
    deliberato e documentato in `parsers/campi.py`. In particolare `Points`,
    che nel Bridge trasportava lo stake, non viene inoltrato — in Pickfair lo
    stake lo decide il money management, non il messaggio.
    """
    fuori: Dict[str, Any] = {}
    for campo, valore in valori.items():
        chiave = VERSO_SEGNALE.get(campo)
        if not chiave:
            continue
        testo = str(valore or "").strip()
        if not testo:
            continue
        fuori[chiave] = testo
    if "action" in fuori:
        fuori["action"] = normalizza_azione(fuori["action"])
        if not fuori["action"]:
            del fuori["action"]
    return fuori


def _avvisa_se_i_parser_sono_rimasti_nel_bridge(radice: str) -> None:
    """Se non ci sono parser qui ma ce ne sono nella cartella del Bridge, dillo.

    Rilievo di GPT-5.6 Sol su #433: cambiando cartella, chi arrivasse da
    XTrader Signal Bridge si troverebbe i parser "invisibili".

    Non e' una migrazione e non lo diventa: i file NON vengono letti, copiati
    ne' spostati. Copiarli in automatico significherebbe far girare in
    Pickfair — sul percorso che porta alle scommesse — regole scritte per un
    altro programma, senza che nessuno le abbia riviste. Qui si dice soltanto
    dove sono, e la decisione resta di chi li ha scritti.
    """
    try:
        vecchia = cartella_parser_del_bridge()
        if not os.path.isdir(vecchia):
            return
        rimasti = [f for f in os.listdir(vecchia) if f.lower().endswith(".json")]
        if not rimasti:
            return
        logging.getLogger(__name__).warning(
            "Nessun Parser Personalizzato in %s, ma ne risultano %d in %s "
            "(cartella di XTrader Signal Bridge). Se sono tuoi, spostali o "
            "ricreali: Pickfair non li carica da li'.",
            radice, len(rimasti), vecchia)
    except Exception:
        # Una diagnostica non deve mai essere la causa di un guasto.
        pass
