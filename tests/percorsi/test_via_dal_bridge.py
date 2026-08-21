"""X3a · Pickfair non chiede a XTrader Signal Bridge dove stanno i suoi dati.

`betfair_client.py` — il client Betfair, cioe' il percorso dei soldi — cercava
la configurazione del proxy con `core.config_store.config_path()`, che
restituisce `%APPDATA%/XTraderBridge/config.json`.

Due difetti in una riga, **entrambi introdotti in #430**, la PR che doveva
chiudere il primo difetto di quel modulo:

1. **Semantico** — Pickfair cercava la propria configurazione nella cartella
   dati di un altro prodotto. Chi non ha mai installato il Bridge non ha quel
   percorso: il candidato non trova nulla, esattamente come il percorso
   assoluto del VPS dismesso che #430 aveva appena tolto.
2. **Strutturale** — quell'import trascinava nel grafo vivo sei moduli del
   Bridge.
"""

from __future__ import annotations

import ast
import os
import pathlib
from collections import deque

import pytest

import percorsi

RADICE = pathlib.Path(__file__).resolve().parents[2]

MODULI_DEL_BRIDGE = {
    "core.config_store", "core.csv_writer", "core.bridge_mode",
    "core.confirmation_reader", "core.token_store", "core.language_select",
    "core.app", "core.autostart",
}

ENTRYPOINT = ("main", "mini_gui", "headless_main")


# ---------------------------------------------------------------------------
# 1 · La fonte unica dei percorsi
# ---------------------------------------------------------------------------

def test_la_cartella_dati_e_di_pickfair(monkeypatch):
    monkeypatch.delenv(percorsi.ENV_CARTELLA_DATI, raising=False)
    d = percorsi.cartella_dati()
    assert d.endswith(percorsi.NOME_CARTELLA_DATI)
    assert "XTraderBridge" not in d


def test_la_cartella_dati_e_sovrascrivibile(monkeypatch, tmp_path):
    monkeypatch.setenv(percorsi.ENV_CARTELLA_DATI, str(tmp_path))
    assert percorsi.cartella_dati() == str(tmp_path)
    assert percorsi.percorso_config() == os.path.join(str(tmp_path), "config.json")


def test_percorsi_non_importa_niente_da_core():
    """`percorsi.py` deve restare senza dipendenze dal progetto.

    E' il modulo che rompe il legame: se un giorno importasse `core.*`,
    ricreerebbe da solo il problema che esiste per togliere.
    """
    albero = ast.parse((RADICE / "percorsi.py").read_text(encoding="utf-8"))
    importati = set()
    for nodo in ast.walk(albero):
        if isinstance(nodo, ast.Import):
            importati.update(a.name for a in nodo.names)
        elif isinstance(nodo, ast.ImportFrom):
            importati.add(nodo.module or "")
    assert importati <= {"os", "__future__"}, f"dipendenze inattese: {importati}"


def test_il_percorso_del_bridge_e_calcolato_non_importato(monkeypatch, tmp_path):
    """Il percorso storico serve a DIRE dove sono i file, non a leggerli.

    Prima qui cercavo la stringa "config_store" nel sorgente — ma il modulo
    la NOMINA nei commenti che spiegano perche' non la importa, quindi il
    test falliva su codice corretto. Cercare una parola nella prosa non e'
    una verifica: quella vera e' sull'albero sintattico
    (`test_percorsi_non_importa_niente_da_core`), e qui si controlla che il
    percorso sia davvero **calcolato** e non letto da altrove.
    """
    monkeypatch.setenv("APPDATA", str(tmp_path))
    atteso = os.path.join(str(tmp_path), percorsi.NOME_CARTELLA_BRIDGE)
    assert percorsi.cartella_dati_del_bridge() == atteso


# ---------------------------------------------------------------------------
# 2 · Il client Betfair
# ---------------------------------------------------------------------------

def test_i_candidati_config_non_toccano_la_cartella_del_bridge(monkeypatch, tmp_path):
    import betfair_client

    monkeypatch.delenv(betfair_client.ENV_PERCORSO_CONFIG, raising=False)
    monkeypatch.setenv(percorsi.ENV_CARTELLA_DATI, str(tmp_path))
    candidati = betfair_client.percorsi_config_candidati()

    assert candidati, "nessun candidato: il proxy non sarebbe mai configurabile"
    for c in candidati:
        assert "XTraderBridge" not in c, f"candidato dentro il Bridge: {c}"
    assert any(str(tmp_path) in c for c in candidati), candidati


def test_il_client_betfair_non_importa_config_store():
    """Il rilievo strutturale, fissato sul sorgente.

    Un `from core.config_store import ...` dentro una funzione non si vede
    leggendo la testa del file: qui si guarda l'albero sintattico intero.
    """
    albero = ast.parse((RADICE / "betfair_client.py").read_text(encoding="utf-8"))
    for nodo in ast.walk(albero):
        if isinstance(nodo, ast.ImportFrom):
            assert "config_store" not in (nodo.module or ""), \
                f"import di config_store a riga {nodo.lineno}"
        elif isinstance(nodo, ast.Import):
            for a in nodo.names:
                assert "config_store" not in a.name, \
                    f"import di config_store a riga {nodo.lineno}"


def _vecchia_config(tmp_path, monkeypatch, contenuto: str):
    """Prepara una config nella cartella del Bridge e la fa trovare."""
    vecchia = tmp_path / "XTraderBridge"
    vecchia.mkdir(exist_ok=True)
    (vecchia / "config.json").write_text(contenuto, encoding="utf-8")
    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge", lambda: str(vecchia))
    return vecchia


PROXY_ATTIVO = '{"proxy": {"enabled": true, "type": "socks5", "host": "h", "port": 1080}}'
PROXY_SPENTO = '{"proxy": {"enabled": false, "host": "h"}}'


def test_un_proxy_dichiarato_nel_bridge_FERMA_l_avvio(monkeypatch, tmp_path):
    """Rilievo bloccante di GPT-5.6 Sol e Fable, sollevato da entrambi.

    Al primo giro qui c'era solo un warning e si proseguiva **senza proxy**:
    le scommesse sarebbero uscite sulla connessione diretta invece che sul
    proxy che l'owner aveva configurato. Un proxy nella vecchia cartella e'
    **dichiarato**: la regola del modulo dice eccezione, non avviso.
    """
    import betfair_client

    vecchia = _vecchia_config(tmp_path, monkeypatch, PROXY_ATTIVO)
    with pytest.raises(ValueError) as e:
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])
    messaggio = str(e.value)
    assert str(vecchia) in messaggio, "il messaggio deve dire DOVE sta il file"
    assert percorsi.percorso_config() in messaggio, "e DOVE va spostato"
    assert betfair_client.ENV_PERCORSO_CONFIG in messaggio, "e l'alternativa"


def test_una_vecchia_config_SENZA_proxy_avvisa_e_prosegue(monkeypatch, tmp_path, caplog):
    """Nessuna intenzione tradita: solo un file da spostare."""
    import betfair_client

    vecchia = _vecchia_config(tmp_path, monkeypatch, PROXY_SPENTO)
    with caplog.at_level("WARNING"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])
    assert any(str(vecchia) in r.getMessage() for r in caplog.records), caplog.text
    assert any(percorsi.percorso_config() in r.getMessage() for r in caplog.records), (
        "il messaggio deve dire dove spostarla, non solo «spostala»")


def test_una_vecchia_config_ILLEGGIBILE_ferma_l_avvio(monkeypatch, tmp_path):
    """Secondo rilievo di GPT-5.6 Sol e Fable, di nuovo entrambi.

    **Questo test prima asseriva il contrario**, cioe' che un file
    illeggibile lasciasse proseguire. Non era una svista rimasta scoperta:
    il fail-open era codificato e protetto da una verifica mia.

    Un JSON rotto **puo' contenere un proxy**. Dire «assente» sarebbe
    inventare, e su questo percorso inventare significa scommettere sulla
    connessione sbagliata.
    """
    import betfair_client

    _vecchia_config(tmp_path, monkeypatch, "{ questo non e' json")
    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_una_vecchia_config_NON_LEGGIBILE_ferma_l_avvio(monkeypatch, tmp_path):
    """Permessi negati, I/O, il file sparito fra due istruzioni."""
    import betfair_client

    _vecchia_config(tmp_path, monkeypatch, PROXY_ATTIVO)

    vero_open = open

    def open_che_nega(percorso, *a, **kw):
        if "XTraderBridge" in str(percorso):
            raise PermissionError("permesso negato")
        return vero_open(percorso, *a, **kw)

    monkeypatch.setattr("builtins.open", open_che_nega)
    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


@pytest.mark.parametrize("contenuto,atteso", [
    (PROXY_ATTIVO, "dichiarato"),
    (PROXY_SPENTO, "assente"),
    ('{"altro": 1}', "assente"),
    ('{"proxy": "non un dict"}', "incerto"),
    ('[1, 2, 3]', "incerto"),
    ('{ rotto', "incerto"),
])
def test_gli_esiti_dell_ispezione(tmp_path, contenuto, atteso):
    """«Non lo so» non e' «non c'e'»: gli esiti sono quattro, non due."""
    import betfair_client

    f = tmp_path / "c.json"
    f.write_text(contenuto, encoding="utf-8")
    assert betfair_client._stato_proxy_altrove(str(f)) == atteso


def test_il_file_che_non_c_e_e_un_esito_A_SE(tmp_path):
    """Il quarto esito, ed e' l'unico che vale «non c'e'».

    Serve distinto da `PROXY_ASSENTE`: quello dice *«l'ho letto e non
    dichiara proxy»* e merita un avviso («hai un file da spostare»); questo
    dice *«non esiste»* ed e' il caso di chiunque non abbia mai installato il
    Bridge, dove non c'e' niente da segnalare a nessuno.
    """
    import betfair_client

    assert betfair_client._stato_proxy_altrove(
        str(tmp_path / "mai-esistito.json")) == "nessun_file"


def test_un_symlink_ROTTO_non_e_un_file_assente(monkeypatch, tmp_path):
    """Rilievo BLOCCANTE di GPT-5.6 Sol su #434, quarto giro. Fondato.

    *«`FileNotFoundError` non prova l'assenza. Link/junction spezzati …
    Va distinto almeno il path legacy esistente tramite `lstat`.»*

    `open()` segue il link, non trova il bersaglio e alza ENOENT — identico a
    un file mai esistito. Ma la voce di directory **c'e'**, e ce l'ha messa
    qualcuno: e' un proxy indicato e non raggiungibile, cioe' «dichiarato ma
    inutilizzabile», che in questo modulo vale eccezione. Il repo lo dice gia'
    per il percorso di `PICKFAIR_CONFIG_PATH`; qui era incoerente.

    Verificato togliendo `lstat`: il test fallisce con `DID NOT RAISE`.
    """
    import betfair_client

    vecchia = tmp_path / "XTraderBridge"
    vecchia.mkdir()
    (vecchia / "config.json").symlink_to(tmp_path / "bersaglio-sparito.json")
    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge", lambda: str(vecchia))

    assert not os.path.exists(vecchia / "config.json"), \
        "il presupposto del test: per `exists` il link rotto non c'e'"
    assert os.path.lexists(vecchia / "config.json"), \
        "ma la voce di directory c'e' davvero"

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_l_assenza_si_ACCERTA_aprendo_non_la_si_deduce_da_exists(monkeypatch, tmp_path):
    """Rilievo BLOCCANTE di GPT-5.6 Sol su #434, terzo giro. Fondato.

    *«`os.path.exists(vecchia)` puo' restituire `False` su errori di accesso,
    soprattutto Windows. Il file legacy puo' quindi essere presente ma il
    controllo termina fail-open, consentendo traffico Betfair diretto.»*

    Qui si riproduce esattamente quello: il file **c'e' e dichiara un proxy
    attivo**, ma `os.path.exists` risponde `False` — come fa quando la
    cartella non e' attraversabile — mentre `open()` dice la verita' con un
    `PermissionError`.

    Col pre-controllo `if not os.path.exists(vecchia): return` questo test
    passava senza sollevare nulla: **il proxy dell'owner era li' e Pickfair
    partiva in chiaro.** Verificato togliendo il fix: il test fallisce con
    `DID NOT RAISE`.
    """
    import betfair_client

    _vecchia_config(tmp_path, monkeypatch, PROXY_ATTIVO)

    vero_exists = os.path.exists
    vero_open = open

    def exists_cieco(percorso, *a, **kw):
        if "XTraderBridge" in str(percorso):
            return False          # la bugia che Windows racconta davvero
        return vero_exists(percorso, *a, **kw)

    def open_onesto(percorso, *a, **kw):
        if "XTraderBridge" in str(percorso):
            raise PermissionError("cartella non attraversabile")
        return vero_open(percorso, *a, **kw)

    monkeypatch.setattr(os.path, "exists", exists_cieco)
    monkeypatch.setattr("builtins.open", open_onesto)

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_nessun_os_path_exists_sul_percorso_del_bridge(monkeypatch, tmp_path):
    """Il fix pinnato sulla struttura, non solo sul comportamento.

    Il test qui sopra dimostra l'effetto; questo impedisce che il
    pre-controllo rientri di soppiatto in una forma equivalente. Se qualcuno
    riscrive `if os.path.exists(vecchia)` — o `os.path.isfile`, che ha lo
    stesso difetto — questo fallisce.
    """
    import betfair_client

    _vecchia_config(tmp_path, monkeypatch, PROXY_SPENTO)
    guardati = []

    vero_exists, vero_isfile = os.path.exists, os.path.isfile

    def spia(vera, nome):
        def _f(percorso, *a, **kw):
            if "XTraderBridge" in str(percorso):
                guardati.append((nome, str(percorso)))
            return vera(percorso, *a, **kw)
        return _f

    monkeypatch.setattr(os.path, "exists", spia(vero_exists, "exists"))
    monkeypatch.setattr(os.path, "isfile", spia(vero_isfile, "isfile"))

    betfair_client._controlla_config_rimasta_nel_bridge(
        [str(tmp_path / "assente.json")])

    assert not guardati, (
        "l'esistenza del file del Bridge e' stata dedotta invece che accertata "
        f"aprendolo: {guardati}")


def test_un_errore_nel_calcolo_dei_percorsi_NON_e_un_permesso(monkeypatch, tmp_path):
    """Rilievo BLOCCANTE di Claude Fable 5 su #434, terzo giro. Fondato.

    *«l'`except Exception: return` esterno resta un fail-open residuo — se
    `percorsi.cartella_dati_del_bridge()` o `os.path.exists` sollevano, il
    controllo di sicurezza viene saltato in silenzio e il client parte senza
    proxy. E' la stessa classe di difetto che la PR chiude altrove.»*

    Aveva ragione, e la frase che quel `try` incarnava — *«se non so nemmeno
    DOVE guardare, lascio correre»* — e' parola per parola quella che questa
    PR cancella dagli altri due punti. Un errore imprevisto nel calcolo dei
    percorsi non prova che il proxy non serva.

    **Questo test prende il posto di uno mio che asseriva l'opposto**
    (`test_il_rilevamento_non_puo_far_fallire_il_client`, *«fermarsi su
    un'incertezza propria sarebbe rumore»*). E' la terza volta in questa PR
    che il fail-open non stava scoperto ma **protetto da una mia verifica**:
    ogni giro l'ho spostato di un livello — dal warning al file illeggibile,
    dal file illeggibile al percorso non calcolabile — e ogni volta ho
    scritto un test che lo difendeva. La regola del modulo non ammette il
    livello: *nessuna* incertezza lascia proseguire.

    Verificato rimettendo il `try/except Exception: return`: il test fallisce
    con `DID NOT RAISE`.
    """
    import betfair_client

    def non_lo_so():
        raise RuntimeError("cartella utente non determinabile")

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge", non_lo_so)

    with pytest.raises(RuntimeError, match="non determinabile"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_l_eccezione_ARRIVA_fino_alla_COSTRUZIONE_del_client(monkeypatch, tmp_path):
    """Secondo rilievo di Fable: verificare la propagazione, non la funzione.

    *«se un chiamante tratta le eccezioni come "nessun proxy", il blocco
    diventa di nuovo un proseguimento silenzioso»*. Rilievo giusto: un
    fail-closed che qualcuno assorbe piu' in alto e' un fail-open con piu'
    passaggi. Qui si costruisce un `BetfairClient` vero e si verifica che
    **non nasca**.
    """
    import betfair_client

    monkeypatch.delenv(betfair_client.ENV_PERCORSO_CONFIG, raising=False)
    _vecchia_config(tmp_path, monkeypatch, PROXY_ATTIVO)
    monkeypatch.setattr(betfair_client, "percorsi_config_candidati",
                        lambda: [str(tmp_path / "assente.json")])

    with pytest.raises(ValueError, match="proxy e' dichiarato e attivo"):
        betfair_client.BetfairClient(
            username="u", app_key="k", cert_pem="c", key_pem="p")


def test_su_una_macchina_SENZA_bridge_il_client_NASCE(monkeypatch, tmp_path, caplog):
    """La controprova del fail-closed: chiudere non deve chiudere su tutti.

    Il rischio di questo giro e' l'opposto di quello che chiude: alzare
    l'asticella dell'incertezza fino a **fermare l'avvio a chi il Bridge non
    l'ha mai avuto** — cioe' quasi tutti. E' il caso in cui il quarto stato
    `PROXY_NESSUN_FILE` esiste: «non c'e'», accertato aprendo, non e'
    un'incertezza e non blocca niente.

    Qui non c'e' nessun `XTraderBridge/` sul disco e si costruisce un client
    vero: deve nascere, in silenzio.
    """
    import betfair_client

    monkeypatch.delenv(betfair_client.ENV_PERCORSO_CONFIG, raising=False)
    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "mai-installato"))
    monkeypatch.setattr(betfair_client, "percorsi_config_candidati",
                        lambda: [str(tmp_path / "assente.json")])

    with caplog.at_level("WARNING"):
        client = betfair_client.BetfairClient(
            username="u", app_key="k", cert_pem="c", key_pem="p")

    assert client is not None
    assert not client.session.proxies, "nessuna config = nessun proxy"
    assert not [r for r in caplog.records if "XTraderBridge" in r.getMessage()], (
        "chi non ha mai avuto il Bridge non deve nemmeno sentirselo nominare")


def test_non_controlla_se_una_config_di_pickfair_esiste(monkeypatch, tmp_path, caplog):
    """Chi ha la sua configurazione non viene disturbato ne' bloccato."""
    import betfair_client

    _vecchia_config(tmp_path, monkeypatch, PROXY_ATTIVO)
    mia = tmp_path / "config.json"
    mia.write_text("{}", encoding="utf-8")
    with caplog.at_level("WARNING"):
        betfair_client._controlla_config_rimasta_nel_bridge([str(mia)])
    assert not [r for r in caplog.records if "XTraderBridge" in r.getMessage()]


# ---------------------------------------------------------------------------
# 3 · Anti-regressione sul grafo vivo
# ---------------------------------------------------------------------------

def _grafo_vivo():
    """Chiusura transitiva degli import dai veri entrypoint di Pickfair."""
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

    vivi = {m for m in ENTRYPOINT if m in moduli}
    coda = deque(vivi)
    while coda:
        for imp in importati(moduli[coda.popleft()]):
            if imp in moduli and imp not in vivi:
                vivi.add(imp)
                coda.append(imp)
    return vivi


def test_nessun_modulo_del_bridge_e_raggiungibile_da_pickfair():
    """Il test che vale per tutto il repository, non per un file solo.

    `main.py` avvia solo `mini_gui` o `headless_main`. Da li' non si deve
    poter arrivare a XTrader Signal Bridge per nessuna catena di import.
    """
    intrusi = sorted(_grafo_vivo() & MODULI_DEL_BRIDGE)
    assert not intrusi, (
        f"XTrader Signal Bridge e' tornato nel percorso vivo di Pickfair: {intrusi}")


@pytest.mark.parametrize("modulo", sorted(MODULI_DEL_BRIDGE))
def test_ogni_modulo_del_bridge_resta_fuori(modulo):
    """Uno per uno, cosi' il fallimento dice QUALE e' rientrato."""
    assert modulo not in _grafo_vivo()


def test_il_controllo_e_davvero_COLLEGATO(monkeypatch, tmp_path, caplog):
    """Che la funzione esista non basta: deve essere chiamata.

    Il primo giro di sabotaggi ha mostrato che sostituendo la chiamata con
    `pass` i test restavano verdi — perche' li' verificavo la funzione
    invocandola io. E' esattamente la forma di H-08: codice corretto che
    nessuno esegue. Qui si passa dal percorso vero, `_proxy_da_disco`.
    """
    import betfair_client

    monkeypatch.delenv(betfair_client.ENV_PERCORSO_CONFIG, raising=False)
    # Cartella dati vuota: nessun candidato di Pickfair esiste.
    monkeypatch.setenv(percorsi.ENV_CARTELLA_DATI, str(tmp_path / "vuota"))
    # Ma nel Bridge una config c'e'.
    vecchia = tmp_path / "XTraderBridge"
    vecchia.mkdir()
    (vecchia / "config.json").write_text(PROXY_SPENTO, encoding="utf-8")
    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge", lambda: str(vecchia))
    # Il candidato "accanto al programma" non deve esistere per questo test.
    monkeypatch.setattr(betfair_client, "percorsi_config_candidati",
                        lambda: [str(tmp_path / "vuota" / "config.json")])

    with caplog.at_level("WARNING"):
        esito = betfair_client.BetfairClient._proxy_da_disco()

    assert esito is None, "nessuna config: nessun proxy"
    assert any(str(vecchia) in r.getMessage() for r in caplog.records), (
        "il controllo non e' collegato al percorso reale: " + caplog.text)
