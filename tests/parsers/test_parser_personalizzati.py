"""X1 · I Parser Personalizzati girano sul percorso vivo di Pickfair (H-08).

Prima di questa PR `services/telegram_signal_processor.py` importava una
classe `CustomParserEngine` **che non esiste in nessun punto del repository**,
e ingoiava l'`ImportError`. I parser dell'owner non hanno mai girato.

Questi test fissano tre cose distinte:

1. il **contratto dei campi** non dipende piu' da XTrader Signal Bridge;
2. il motore **rifiuta** in ogni caso incerto, con un motivo leggibile;
3. il percorso vivo **arricchisce davvero** un segnale, e non sovrascrive.
"""

from __future__ import annotations

import ast
import os
import pathlib
import tempfile

import pytest

from core import custom_parser
from core.custom_parser import CustomParserDef, FieldRule
from parsers import campi, motore
from services.telegram_signal_processor import TelegramSignalProcessor

RADICE = pathlib.Path(__file__).resolve().parents[2]

MESSAGGIO = "Match: Inter v Milan\nMercato: Both Teams To Score\nEsito: Yes\nQuota: 1,85\n"


def _parser_diretto(nome: str = "Canale Mio") -> CustomParserDef:
    """Un parser che estrae dal testo senza value-map (niente dizionario)."""
    return CustomParserDef(
        name=nome, mode="NAME_ONLY",
        rules=[
            FieldRule(target="EventName", start_after="Match: ", end_before="\n", required=True),
            FieldRule(target="MarketName", start_after="Mercato: ", end_before="\n", required=True),
            FieldRule(target="SelectionName", start_after="Esito: ", end_before="\n", required=True),
            FieldRule(target="Price", start_after="Quota: ", end_before="\n", required=True),
            FieldRule(target="BetType", fixed_value="PUNTA", required=True),
        ])


# ---------------------------------------------------------------------------
# 1 · Il contratto dei campi
# ---------------------------------------------------------------------------

def test_il_contratto_ha_i_quattordici_campi():
    assert len(campi.CAMPI_SEGNALE) == 14
    assert campi.CAMPI_SEGNALE[0] == "Provider"
    assert "MarketId" in campi.CAMPI_SEGNALE and "BetType" in campi.CAMPI_SEGNALE


def test_ogni_campo_e_mappato_o_dichiarato_non_inoltrato():
    """Nessun campo puo' sparire per distrazione.

    O e' tradotto verso Pickfair, o e' nell'elenco di quelli che NON si
    inoltrano di proposito. Un campo che non sta in nessuno dei due e' un
    campo dimenticato.
    """
    coperti = set(campi.VERSO_SEGNALE) | set(campi.CAMPI_NON_INOLTRATI)
    assert coperti == set(campi.CAMPI_SEGNALE), (
        f"campi senza destino: {set(campi.CAMPI_SEGNALE) - coperti}")


def test_points_non_arriva_al_percorso_dei_soldi():
    """`Points` trasportava lo stake nel CSV del Bridge.

    In Pickfair lo stake lo decide il money management sui tavoli. Se questo
    test diventa rosso, qualcuno ha appena permesso a un messaggio Telegram
    di dettare quanto si scommette.
    """
    assert "Points" not in campi.VERSO_SEGNALE
    assert "Points" in campi.CAMPI_NON_INOLTRATI


@pytest.mark.parametrize("grezzo,atteso", [
    ("PUNTA", "BACK"), ("punta", "BACK"), ("BANCA", "LAY"), (" lay ", "LAY"),
    ("BACK", "BACK"), ("", ""), (None, ""), ("pippo", ""), ("BAC", ""),
])
def test_la_direzione_non_ha_default(grezzo, atteso):
    """Un `BetType` non riconosciuto da "" e non "BACK".

    Il codice sostituito faceva `parsed.get("action", "BACK")`: un parser che
    sbagliava il campo produceva comunque una PUNTA.
    """
    assert campi.normalizza_azione(grezzo) == atteso


def test_la_cartella_parser_non_e_quella_del_bridge(monkeypatch):
    monkeypatch.delenv(campi.ENV_CARTELLA_PARSER, raising=False)
    percorso = campi.cartella_parser()
    assert "XTraderBridge" not in percorso
    assert percorso.endswith(os.path.join(".pickfair", "parsers"))


def test_la_cartella_parser_e_sovrascrivibile(monkeypatch, tmp_path):
    monkeypatch.setenv(campi.ENV_CARTELLA_PARSER, str(tmp_path))
    assert campi.cartella_parser() == str(tmp_path)


# ---------------------------------------------------------------------------
# 2 · Il motore rifiuta, e dice perche'
# ---------------------------------------------------------------------------

def test_nessun_parser_rifiuta():
    e = motore.estrai(MESSAGGIO, parser=[])
    assert not e.ok and e.motivo == motore.NESSUN_PARSER


def test_messaggio_estraneo_rifiuta():
    e = motore.estrai("ciao come stai", parser=[_parser_diretto()])
    assert not e.ok and e.motivo == motore.NON_RICONOSCIUTO


def test_due_parser_sullo_stesso_messaggio_rifiutano():
    """Ambiguo = rifiuto. Sceglierne uno vorrebbe dire indovinare."""
    e = motore.estrai(MESSAGGIO, parser=[_parser_diretto("A"), _parser_diretto("B")])
    assert not e.ok and e.motivo == motore.PARSER_AMBIGUI
    assert set(e.dettagli["parser"]) == {"A", "B"}


def test_campo_obbligatorio_mancante_rifiuta():
    defn = _parser_diretto()
    defn.rules.append(FieldRule(target="MarketId", start_after="MID: ",
                                end_before="\n", required=True))
    e = motore.estrai(MESSAGGIO, parser=[defn])
    assert not e.ok and e.motivo == motore.CAMPI_MANCANTI
    assert "MarketId" in e.dettagli["mancanti"]


def test_direzione_non_riconosciuta_rifiuta():
    defn = _parser_diretto()
    for regola in defn.rules:
        if regola.target == "BetType":
            regola.fixed_value = "FORSE"
    e = motore.estrai(MESSAGGIO, parser=[defn])
    assert not e.ok and e.motivo == motore.DIREZIONE_ASSENTE


def test_un_parser_che_esplode_non_zittisce_gli_altri():
    class Esplosivo:
        name = "rotto"
        mode = "NAME_ONLY"
        rules = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))

    e = motore.estrai(MESSAGGIO, parser=[Esplosivo(), _parser_diretto()])
    assert e.ok, f"il parser sano doveva vincere, invece: {e.motivo}"
    assert e.parser == "Canale Mio"


def test_estrazione_riuscita_traduce_nei_nomi_di_pickfair():
    e = motore.estrai(MESSAGGIO, parser=[_parser_diretto()])
    assert e.ok, e.motivo
    assert e.campi == {
        "event_name": "Inter v Milan",
        "market_name": "Both Teams To Score",
        "selection_name": "Yes",
        "price": "1.85",   # virgola italiana normalizzata alla sorgente
        "action": "BACK",
    }


def test_il_registro_regge_l_assenza_del_dizionario():
    """`data/dizionario_xtrader.csv` non e' nel repository.

    Deve degradare ai built-in, non sollevare: un crash qui l'utente lo vede
    come "il bot non risponde".
    """
    registro, disponibile = motore.registro_value_map()
    assert isinstance(registro, dict)
    assert "bettype" in registro
    assert disponibile is False or "selectionname" in registro


# ---------------------------------------------------------------------------
# 3 · Il percorso vivo
# ---------------------------------------------------------------------------

def _processore_con(defn, monkeypatch):
    cartella = tempfile.mkdtemp()
    monkeypatch.setenv(campi.ENV_CARTELLA_PARSER, cartella)
    custom_parser.save_parser(defn, cartella)
    p = TelegramSignalProcessor()
    assert p.ricarica_parser() == 1
    return p


def test_il_percorso_vivo_arricchisce_davvero(monkeypatch):
    """Il test che H-08 non aveva: il segnale esce con i campi del parser."""
    p = _processore_con(_parser_diretto(), monkeypatch)
    segnale = {"raw_text": MESSAGGIO}
    p.normalize_ingestion_signal(segnale)
    assert segnale["event_name"] == "Inter v Milan"
    assert segnale["market_name"] == "Both Teams To Score"
    assert segnale["action"] == "BACK"
    assert segnale["price"] == "1.85"


def test_l_arricchimento_non_sovrascrive_un_valore_gia_presente(monkeypatch):
    """Un `market_id` del segnale e' piu' autorevole del testo libero."""
    p = _processore_con(_parser_diretto(), monkeypatch)
    segnale = {"raw_text": MESSAGGIO, "event_name": "NON TOCCARE"}
    p.normalize_ingestion_signal(segnale)
    assert segnale["event_name"] == "NON TOCCARE"
    assert segnale["market_name"] == "Both Teams To Score"


def test_senza_parser_il_segnale_resta_intatto(monkeypatch, tmp_path):
    monkeypatch.setenv(campi.ENV_CARTELLA_PARSER, str(tmp_path))
    p = TelegramSignalProcessor()
    assert p.ricarica_parser() == 0
    segnale = {"raw_text": MESSAGGIO}
    esito = p.normalize_ingestion_signal(segnale)
    assert esito["ok"] is True
    assert "event_name" not in segnale


# ---------------------------------------------------------------------------
# 4 · Anti-regressione: il motore non deve ri-agganciarsi al Bridge
# ---------------------------------------------------------------------------

MODULI_DEL_BRIDGE = {
    "core.csv_writer", "core.config_store", "core.bridge_mode",
    "core.confirmation_reader", "core.autostart", "core.app",
}


def _chiusura(semi):
    """Chiusura transitiva degli import locali a partire da `semi`."""
    moduli = {}
    for percorso in RADICE.rglob("*.py"):
        parti = percorso.relative_to(RADICE).parts
        if parti[0] in {".git", ".venv", "venv", "tests", "build", "dist"}:
            continue
        if "__pycache__" in parti:
            continue
        nome = ".".join(parti)[:-3]
        if nome.endswith(".__init__"):
            nome = nome[:-9]
        moduli[nome] = percorso

    def importati(percorso):
        try:
            albero = ast.parse(percorso.read_text(encoding="utf-8"))
        except Exception:
            return set()
        fuori = set()
        pacchetto = ".".join(percorso.relative_to(RADICE).parts[:-1])
        for nodo in ast.walk(albero):
            if isinstance(nodo, ast.Import):
                fuori.update(a.name for a in nodo.names)
            elif isinstance(nodo, ast.ImportFrom):
                base = nodo.module or ""
                if nodo.level:
                    base = f"{pacchetto}.{base}" if base else pacchetto
                if base:
                    fuori.add(base)
                    fuori.update(f"{base}.{a.name}" for a in nodo.names)
        return fuori

    visti = {s for s in semi if s in moduli}
    coda = list(visti)
    while coda:
        for imp in importati(moduli[coda.pop()]):
            if imp in moduli and imp not in visti:
                visti.add(imp)
                coda.append(imp)
    return visti


def test_il_motore_dei_parser_non_dipende_da_xtrader_bridge():
    """La ragione d'essere di `parsers/campi.py`.

    Bastavano due `from .csv_writer import CSV_HEADER` per trascinare il CSV,
    la config del Bridge e la sua catena di conferme: 7.407 righe di un altro
    prodotto dentro il percorso dei segnali di Pickfair.
    """
    chiusura = _chiusura(["parsers.motore", "parsers.campi"])
    intrusi = chiusura & MODULI_DEL_BRIDGE
    assert not intrusi, f"il motore ha ri-agganciato XTrader Bridge: {sorted(intrusi)}"


def test_il_percorso_vivo_non_dipende_da_xtrader_bridge():
    chiusura = _chiusura(["services.telegram_signal_processor"])
    intrusi = chiusura & MODULI_DEL_BRIDGE
    assert not intrusi, f"il processore ha ri-agganciato XTrader Bridge: {sorted(intrusi)}"


def test_l_import_rotto_di_h08_non_torna():
    """`CustomParserEngine` non esiste: importarla e' sempre stato un no-op."""
    sorgente = (RADICE / "services" / "telegram_signal_processor.py").read_text(encoding="utf-8")
    righe = [r for r in sorgente.splitlines()
             if "import CustomParserEngine" in r and not r.lstrip().startswith("#")]
    assert not righe, f"import fantasma tornato: {righe}"


# ---------------------------------------------------------------------------
# 5 · Diagnostica di provenienza (rilievo GPT-5.6 Sol su #433)
# ---------------------------------------------------------------------------

def test_avvisa_se_i_parser_sono_rimasti_nella_cartella_del_bridge(
        monkeypatch, tmp_path, caplog):
    """Chi arriva dal Bridge deve leggere DOVE sono, non un silenzio."""
    vecchia = tmp_path / "XTraderBridge" / "parsers"
    vecchia.mkdir(parents=True)
    (vecchia / "Mio.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(motore, "cartella_parser_del_bridge", lambda: str(vecchia))

    nuova = tmp_path / "assente"
    with caplog.at_level("WARNING"):
        definizioni = motore.carica_parser(str(nuova))

    assert definizioni == []
    messaggi = [r.getMessage() for r in caplog.records]
    assert any(str(vecchia) in m for m in messaggi), messaggi
    assert any("Mio.json" in m or "1 in" in m or " 1 " in m for m in messaggi), messaggi


def test_la_diagnostica_non_carica_dalla_cartella_del_bridge(monkeypatch, tmp_path):
    """Avvisare non e' migrare.

    Copiare in automatico farebbe girare in Pickfair — sul percorso che porta
    alle scommesse — regole scritte per un altro programma, mai riviste.
    """
    vecchia = tmp_path / "XTraderBridge" / "parsers"
    vecchia.mkdir(parents=True)
    custom_parser.save_parser(_parser_diretto("Vecchio"), str(vecchia))
    monkeypatch.setattr(motore, "cartella_parser_del_bridge", lambda: str(vecchia))

    assert motore.carica_parser(str(tmp_path / "assente")) == []
    esito = motore.estrai(MESSAGGIO, cartella=str(tmp_path / "assente"))
    assert not esito.ok and esito.motivo == motore.NESSUN_PARSER


def test_la_diagnostica_non_puo_far_fallire_il_caricamento(monkeypatch, tmp_path):
    """Una diagnostica che solleva sarebbe peggio del problema che segnala."""
    def esplode():
        raise OSError("permesso negato")
    monkeypatch.setattr(motore, "cartella_parser_del_bridge", esplode)
    assert motore.carica_parser(str(tmp_path / "assente")) == []


# ---------------------------------------------------------------------------
# 6 · Rilievi dei reviewer su #433
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("grezzo,atteso", [
    ("1,85", "1.85"), ("1.85", "1.85"), (" 2,50 ", "2.50"), ("3", "3"),
    ("1.234,56", "1.234,56"),   # ambiguo: NON si indovina
])
def test_il_prezzo_esce_con_il_punto_decimale(grezzo, atteso):
    """Rilievo Fable: `price` usciva come `"1,85"`.

    `parse_price` a valle gestiva la virgola, ma il dict del segnale la
    conservava: chi leggesse `signal["price"]` direttamente otterrebbe
    `float("1,85")` → `ValueError`. Si normalizza alla sorgente.
    """
    assert motore._numero_decimale(grezzo) == atteso


def test_handicap_zero_non_viene_sovrascritto(monkeypatch):
    """Rilievo Fugu: `if not signal.get(k)` trattava `0` come "da riempire".

    `handicap` vale `0` in quasi ogni mercato senza handicap. Un handicap
    diverso e' una LINEA diversa: sarebbe un'altra scommessa.
    """
    defn = _parser_diretto()
    defn.rules.append(FieldRule(target="Handicap", fixed_value="-1.5", required=False))
    p = _processore_con(defn, monkeypatch)

    for valore in (0, "0", 0.0, False):
        segnale = {"raw_text": MESSAGGIO, "handicap": valore}
        p.normalize_ingestion_signal(segnale)
        assert segnale["handicap"] == valore, f"sovrascritto {valore!r}"


def test_campo_stringa_vuota_invece_viene_riempito(monkeypatch):
    """Il contrario del test sopra: "" e None SONO da riempire."""
    p = _processore_con(_parser_diretto(), monkeypatch)
    for vuoto in ("", "   ", None):
        segnale = {"raw_text": MESSAGGIO, "event_name": vuoto}
        p.normalize_ingestion_signal(segnale)
        assert segnale["event_name"] == "Inter v Milan"


def test_la_cache_non_e_mai_visibile_a_meta(monkeypatch, tmp_path):
    """Rilievo Fugu + Fable: parser e value-map devono cambiare insieme.

    Valorizzarli uno per uno lasciava una finestra in cui un altro thread
    vedeva parser nuovi con value-map vecchie.

    Non basta controllare la FORMA finale — un'assegnazione parziale seguita
    da quella completa la supererebbe. Qui si guarda lo stato **durante** il
    caricamento: fra l'inizio e la fine nessuno deve poter osservare una
    cache diversa da quella di partenza.
    """
    monkeypatch.setenv(campi.ENV_CARTELLA_PARSER, str(tmp_path))
    custom_parser.save_parser(_parser_diretto(), str(tmp_path))
    p = TelegramSignalProcessor()
    partenza = p._cache_parser
    assert partenza is None

    osservato = []
    vero = motore.registro_value_map

    def spia(*a, **kw):
        # Siamo a meta' caricamento: i parser sono gia' letti dal disco.
        osservato.append(p._cache_parser)
        return vero(*a, **kw)

    monkeypatch.setattr(motore, "registro_value_map", spia)
    p._parser_personalizzati()

    assert osservato, "il registro non e' stato costruito"
    assert all(v is partenza for v in osservato), (
        f"cache visibile a meta' caricamento: {osservato!r}")
    assert isinstance(p._cache_parser, tuple) and len(p._cache_parser) == 3


def test_la_ricarica_non_lascia_mai_la_cache_azzerata(monkeypatch, tmp_path):
    """Fra azzeramento e ricarica un messaggio vedeva `None` e ricaricava da se'."""
    monkeypatch.setenv(campi.ENV_CARTELLA_PARSER, str(tmp_path))
    custom_parser.save_parser(_parser_diretto(), str(tmp_path))
    p = TelegramSignalProcessor()
    assert p.ricarica_parser() == 1
    prima = p._cache_parser

    def esplode():
        raise OSError("disco")
    monkeypatch.setattr(motore, "carica_parser", esplode)
    assert p.ricarica_parser() == 1, "la ricarica fallita deve tenere lo stato buono"
    assert p._cache_parser is prima, "la cache non deve essere azzerata da un errore"


def test_avvisa_anche_se_la_cartella_esiste_ma_e_vuota(monkeypatch, tmp_path, caplog):
    """Rilievo GPT-5.6 Sol: e' il caso PIU' probabile, non quello raro.

    La cartella viene creata all'installazione o al primo salvataggio: restare
    senza avviso proprio lì rendeva la diagnostica inutile dove serviva di più.
    """
    vecchia = tmp_path / "XTraderBridge" / "parsers"
    vecchia.mkdir(parents=True)
    (vecchia / "Mio.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(motore, "cartella_parser_del_bridge", lambda: str(vecchia))

    vuota = tmp_path / "pickfair-parsers"
    vuota.mkdir()
    assert vuota.is_dir(), "la cartella deve ESISTERE ed essere vuota"

    with caplog.at_level("WARNING"):
        assert motore.carica_parser(str(vuota)) == []
    assert any(str(vecchia) in r.getMessage() for r in caplog.records), caplog.text


def test_il_mode_passato_al_cancello_e_sempre_una_stringa():
    """Rilievo DeepSource: `getattr(defn, "mode", None)` passava `None`."""
    visti = []

    class Finto:
        name = "senza mode"
        rules = []

    def spia(defn, testo, modo=None):
        visti.append(modo)
        return False

    import core.custom_parser_engine as cpe
    originale = cpe.matches_message
    cpe.matches_message = spia
    try:
        motore.estrai(MESSAGGIO, parser=[Finto()])
    finally:
        cpe.matches_message = originale

    assert visti, "il cancello di contenuto non e' stato chiamato"
    assert all(isinstance(m, str) for m in visti), visti


def test_end_to_end_il_segnale_esce_pronto_per_il_piazzamento(monkeypatch):
    """Rilievo Fable: verificare il contratto delle chiavi fino a valle.

    Il blocco sostituito (mai eseguito) emetteva `selection`/`odds`; il nuovo
    emette `selection_name`/`price`. Se il consumatore a valle si aspettasse
    le vecchie chiavi, l'arricchimento sarebbe un no-op silenzioso — cioe' H-08
    di nuovo, con un'altra faccia.

    Qui si parte dal testo grezzo e si arriva al segnale normalizzato,
    controllando **tipi e valori** dei campi che servono per piazzare.
    """
    defn = CustomParserDef(
        name="Completo", mode="NAME_ONLY",
        rules=[
            FieldRule(target="EventName", start_after="Match: ", end_before="\n", required=True),
            FieldRule(target="MarketName", start_after="Mercato: ", end_before="\n", required=True),
            FieldRule(target="SelectionName", start_after="Esito: ", end_before="\n", required=True),
            FieldRule(target="Price", start_after="Quota: ", end_before="\n", required=True),
            FieldRule(target="MarketId", start_after="MID: ", end_before="\n", required=True),
            FieldRule(target="SelectionId", start_after="SID: ", end_before="\n", required=True),
            FieldRule(target="BetType", fixed_value="PUNTA", required=True),
        ])
    p = _processore_con(defn, monkeypatch)

    esito = p.normalize_ingestion_signal({"raw_text": (
        "Match: Inter v Milan\nMercato: Match Odds\nEsito: Inter\n"
        "Quota: 1,85\nMID: 1.234567890\nSID: 47999\n")})

    assert esito["ok"] is True, esito.get("error_code")
    ns = esito["normalized_signal"]

    assert ns["market_id"] == "1.234567890"
    assert ns["selection_id"] == 47999 and isinstance(ns["selection_id"], int)
    assert ns["price"] == 1.85 and isinstance(ns["price"], float)
    assert ns["action"] == "BACK" and ns["bet_type"] == "BACK"
    assert ns["event_name"] == "Inter v Milan"
    assert ns["selection"] == "Inter"

    # Lo stake NON arriva dal messaggio: lo decide il money management.
    assert "stake" not in ns and "size" not in ns
