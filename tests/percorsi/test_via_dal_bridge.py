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


def _symlink_utilizzabili(tmp_path) -> bool:
    """Su Windows senza Developer Mode `symlink_to` alza: si prova, non si indovina."""
    try:
        (tmp_path / "_prova_link").symlink_to(tmp_path / "_prova_assente")
    except (OSError, NotImplementedError, AttributeError):
        return False
    return True


@pytest.mark.parametrize("errore_di_open", [FileNotFoundError, NotADirectoryError])
def test_una_VOCE_che_non_si_apre_non_e_un_file_assente(
        monkeypatch, tmp_path, errore_di_open):
    """Rilievo BLOCCANTE di GPT-5.6 Sol su #434, quarto giro. Fondato.

    *«`FileNotFoundError` non prova l'assenza. Link/junction spezzati …
    Va distinto almeno il path legacy esistente tramite `lstat`.»*

    Un symlink rotto: `open()` segue il link, non trova il bersaglio e alza
    ENOENT — identico a un file mai esistito. Ma la voce di directory **c'e'**,
    e ce l'ha messa qualcuno: e' un proxy *indicato* e non raggiungibile, cioe'
    «dichiarato ma inutilizzabile», che in questo modulo vale eccezione. Il
    repo lo dice gia' per il percorso di `PICKFAIR_CONFIG_PATH`; qui era
    incoerente.

    **Questo test e' a mock di proposito** (rilievo di GPT-5.6 Sol, Claude
    Fable 5 e Fugu Ultra al quinto giro, tutti e tre sullo stesso punto): la
    versione precedente creava un symlink vero, e su Windows senza Developer
    Mode `symlink_to` alza — il test sarebbe morto in `error`, non in `skip`,
    proprio sul sistema operativo su cui Pickfair gira davvero. Qui la
    condizione e' riprodotta senza toccare il filesystem, quindi **vale su
    ogni piattaforma**. Che i mock corrispondano alla realta' lo dimostra il
    test subito sotto, dove i symlink funzionano.

    Parametrizzato anche su `NotADirectoryError` per il secondo rilievo di
    Fable: una voce anomala nel mezzo del percorso e' incertezza, non assenza.
    """
    import betfair_client

    vero_open = open

    def open_che_non_trova(percorso, *a, **kw):
        if "XTraderBridge" in str(percorso):
            raise errore_di_open("come un link rotto")
        return vero_open(percorso, *a, **kw)

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "XTraderBridge"))
    monkeypatch.setattr("builtins.open", open_che_non_trova)
    # `lstat` riesce: la VOCE c'e', e' il bersaglio che manca.
    monkeypatch.setattr(os, "lstat", lambda p, *a, **kw: os.stat_result(
        (0o120777, 0, 0, 1, 0, 0, 0, 0, 0, 0)))

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_una_voce_ANOMALA_nel_percorso_e_incertezza_non_assenza(monkeypatch, tmp_path):
    """Secondo rilievo di Claude Fable 5 su #434, quinto giro. Fondato.

    *«se un componente intermedio del path e' un file regolare, `lstat` alza
    `NotADirectoryError` -> `PROXY_NESSUN_FILE` -> traffico Betfair diretto.
    Ma quella voce esiste ed e' anomala: e' piu' vicina a "dichiarato ma
    inutilizzabile" che ad assenza accertata.»*

    Avevo messo `NotADirectoryError` insieme a `FileNotFoundError`
    ragionando cosi': dentro un file non ci puo' stare un `config.json`,
    quindi non c'e'. Ma e' di nuovo una **deduzione** — vale se il percorso e'
    quello che sembra, e una junction o un mount possono averci messo altro in
    mezzo. Su questo percorso la deduzione non basta.

    Il costo del contrario e' trascurabile: perche' l'avvio si fermi serve un
    **file** chiamato `XTraderBridge` dentro `%APPDATA%`, e il messaggio dice
    cosa fare.

    **Questo test nasce da un sabotaggio riuscito**: rimettendo
    `NotADirectoryError` fra le assenze, la suite restava tutta verde. Il
    caso parametrizzato qui sopra non lo copriva, perche' li' `lstat` e'
    finto e *riesce* — l'errore non arrivava mai al ramo che avevo cambiato.
    Senza questo, avrei consegnato la correzione di Fable senza una verifica.
    """
    import betfair_client

    vero_open = open

    def esplode(percorso, *a, **kw):
        if "XTraderBridge" in str(percorso):
            raise NotADirectoryError("un pezzo del percorso e' un file")
        return vero_open(percorso, *a, **kw)

    def lstat_che_esplode(percorso, *a, **kw):
        raise NotADirectoryError("un pezzo del percorso e' un file")

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "XTraderBridge"))
    monkeypatch.setattr("builtins.open", esplode)
    monkeypatch.setattr(os, "lstat", lstat_che_esplode)

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


@pytest.mark.parametrize("winerror,atteso", [
    # Solo codici che CPython mappa su ENOENT: sono gli unici che possono
    # arrivare come `FileNotFoundError`, e quindi gli unici per cui
    # `_non_raggiungibile` viene mai consultata. Vedi
    # `test_i_codici_NON_enoent_sono_gia_fail_closed_per_altra_strada`.
    (53, "incerto"),        # ERROR_BAD_NETPATH    condivisione irraggiungibile
    (67, "incerto"),        # ERROR_BAD_NET_NAME   nome di rete non trovato
    (15, "incerto"),        # ERROR_INVALID_DRIVE  unita' non c'e'
    (2, "nessun_file"),     # ERROR_FILE_NOT_FOUND il file davvero non c'e'
    (3, "nessun_file"),     # ERROR_PATH_NOT_FOUND cartella intermedia assente
    (None, "nessun_file"),  # Linux: winerror non esiste
])
def test_una_UNC_caduta_non_e_un_file_assente(monkeypatch, tmp_path, winerror, atteso):
    """Rilievo BLOCCANTE di GPT-5.6 Sol, quarto E sesto giro.

    **La lista qui sopra e' stata potata dopo un rilievo di Fugu Ultra**: le
    righe 21/55/59/64/1231/1232 fabbricavano un `FileNotFoundError` con quei
    `winerror`, cioe' un oggetto che il sistema operativo non produce mai —
    su CPython la sottoclasse di `OSError` la sceglie `errno`, non `winerror`.
    Erano copertura falsa. Il comportamento per quei codici e' fissato dal
    test qui sotto, che passa dalla strada che percorrono davvero.

    **Al quarto avevo risposto che non si poteva fare**, e l'ho scritto sia nel
    codice sia sulla PR: *«sono lo stesso fatto osservabile»*. Sbagliato.
    E' vero guardando `errno`, che appiattisce tutto su ENOENT; falso
    guardando `winerror`, che su Windows sopravvive dentro l'eccezione — 53 o
    67 per una condivisione irraggiungibile, 64 per un profilo caduto, 2 per
    un file che non c'e'. Il discriminante stava nell'eccezione da sempre:
    avevo dichiarato un limite senza provarci, e per due giri quella frase e'
    rimasta nel codice a giustificare il fail-open che descriveva.

    Le ultime tre righe sono la controprova che il rimedio non allarga il
    blocco: un ENOENT normale (2 e 3) e Linux (`winerror` assente, `None`)
    restano assenza e lasciano proseguire.
    """
    import betfair_client

    def lstat_che_non_arriva(percorso, *a, **kw):
        errore = FileNotFoundError(2, "non trovato")
        if winerror is not None:
            errore.winerror = winerror
        raise errore

    f = tmp_path / "c.json"
    monkeypatch.setattr(os, "lstat", lstat_che_non_arriva)
    assert betfair_client._stato_proxy_altrove(str(f)) == atteso


# Riferimenti VERI catturati a import-time: i finti dei test qui sotto valgono
# solo sotto `tmp_path`, e per tutto il resto devono poter rispondere il
# filesystem vero — altrimenti pytest stesso, che tocca il disco mentre il
# monkeypatch e' attivo, fallisce per motivi estranei al test.
_vero_stat = os.stat
_vero_lstat = os.lstat
_vero_open = open


class _RisalitaSenzaFine(BaseException):
    """Non deriva da `Exception` di proposito: un `except Exception` nel codice
    in prova non deve poterla assorbire. Vedi
    `test_la_risalita_si_ferma_alla_radice`."""


def _lstat_finto(monkeypatch, per_percorso):
    """`os.lstat` che risponde secondo una tabella {frammento: winerror|None}.

    `None` come valore = riesce. Frammento non elencato = FileNotFoundError
    senza `winerror` (assenza normale, come Linux).
    """
    def finto(percorso, *a, **kw):
        for frammento, winerror in per_percorso.items():
            if frammento in str(percorso):
                if winerror is None:
                    return os.stat_result((0o40755, 0, 0, 1, 0, 0, 0, 0, 0, 0))
                errore = FileNotFoundError(2, "non trovato")
                errore.winerror = winerror
                raise errore
        raise FileNotFoundError(2, "non trovato")
    monkeypatch.setattr(os, "lstat", finto)
    # La cartella ora si interroga con `stat`, non `lstat` (rilievo di GPT-5.6
    # Sol, ottavo giro): il finto deve rispondere a entrambe, altrimenti il
    # test misurerebbe il filesystem vero invece della tabella.
    monkeypatch.setattr(os, "stat", finto)


@pytest.mark.parametrize("eccezione", [
    PermissionError(13, "accesso negato"),      # ERROR_NOT_READY -> EACCES
    OSError(22, "errore di rete"),              # ERROR_NETNAME_DELETED -> EINVAL
    TimeoutError(110, "host irraggiungibile"),  # ERROR_HOST_UNREACHABLE
])
def test_i_codici_NON_enoent_sono_gia_fail_closed_per_altra_strada(
        monkeypatch, tmp_path, eccezione):
    """Rilievo BLOCCANTE di OpenRouter Fugu Ultra. Fondato, e ben trovato.

    *«su CPython l'OSError concreto e' scelto da `errno`, non da `winerror`.
    Codici come 21 (->EACCES/PermissionError), 55, 59, 64, 1231, 1232
    (->EINVAL/OSError) non entrano nei rami `except FileNotFoundError`, quindi
    `_non_raggiungibile` non viene mai chiamato per essi su Windows reale.»*

    Verificato invece che dedotto:

        OSError(ENOENT) -> FileNotFoundError
        OSError(EACCES) -> PermissionError
        OSError(EINVAL) -> OSError

    Quei sei codici erano voci morte nel `frozenset`, e i test che li
    esercitavano costruivano oggetti impossibili: **copertura falsa**, cioe'
    la cosa che questa PR cerca di eliminare, comparsa nel rimedio.

    Il comportamento pero' non cambia, e questo test lo dimostra passando
    dalla strada che percorrono davvero: non essendo `FileNotFoundError`
    finiscono nell'`except Exception`, che risponde `PROXY_INCERTO`. Erano
    gia' fail-closed — solo, non per la ragione che avevo scritto.
    """
    import betfair_client

    def lstat_che_esplode(percorso, *a, **kw):
        raise eccezione

    monkeypatch.setattr(os, "lstat", lstat_che_esplode)
    monkeypatch.setattr(os, "stat", lstat_che_esplode)
    assert betfair_client._stato_proxy_altrove(str(tmp_path / "c.json")) == "incerto"


def test_winerror_3_con_la_cartella_RAGGIUNGIBILE_e_assenza(monkeypatch, tmp_path):
    """Rilievo di GPT-5.6 Sol, settimo giro — e la meta' che NON si puo' applicare.

    GPT chiede di trattare `winerror` 3 (`ERROR_PATH_NOT_FOUND`) come
    incertezza. **Applicato alla lettera romperebbe il caso principale**:
    aprire `%APPDATA%` / `XTraderBridge` / `config.json` quando la cartella
    `XTraderBridge` non esiste da' proprio 3, perche' manca un componente
    *intermedio*. E' il codice che riceve chiunque non abbia mai installato il
    Bridge — cioe' quasi tutti.

    Questo test fissa quel caso: il file da' 3, ma la cartella che doveva
    contenerlo **e' ispezionabile**, quindi il file davvero non c'e' e si
    prosegue. E' la controprova che il rimedio del test qui sotto non si e'
    trasformato in un blocco per tutti.
    """
    import betfair_client

    _lstat_finto(monkeypatch, {
        "config.json": 3,          # manca la cartella intermedia
        "XTraderBridge": None,     # ...ma risalendo si arriva, e si guarda
    })
    assert betfair_client._stato_proxy_altrove(
        str(tmp_path / "XTraderBridge" / "config.json")) == "nessun_file"


def test_winerror_3_con_la_cartella_IRRAGGIUNGIBILE_e_incertezza(monkeypatch, tmp_path):
    """L'altra meta' del rilievo, quella fondata, chiusa senza rompere la prima.

    *«un profilo reindirizzato o percorso di rete temporaneamente
    indisponibile puo' restituire `ERROR_PATH_NOT_FOUND`»*. Vero, e li' 3 non
    significa piu' «non c'e'».

    Distinguerlo **guardando il codice** e' impossibile: e' lo stesso 3 del
    test qui sopra. Distinguerlo **andando a guardare** si puo': se risalendo
    verso la cartella che doveva contenere il file si incontra un errore di
    rete, non ci siamo arrivati, e allora vale incertezza.

    Cosi' l'esito non dipende da quale codice Windows scelga in ogni
    situazione — cosa che da qui non posso verificare, e che l'ultima volta
    che ho dato per scontata mi ha dato torto.

    Verificato per sabotaggio: senza la risalita, `DID NOT RAISE`.
    """
    import betfair_client

    _lstat_finto(monkeypatch, {
        "config.json": 3,           # stesso codice del test qui sopra...
        "XTraderBridge": 53,        # ...ma la cartella e' su una rete caduta
    })
    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "XTraderBridge"))

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_la_risalita_NON_PUO_girare_a_vuoto(monkeypatch, tmp_path):
    """Il ciclo va reso limitato, non argomentato tale.

    La prima versione era un `while True` che si fidava di `dirname`. Sabotando
    la condizione d'uscita per verificarla, la suite **non e' fallita: si e'
    bloccata**, e l'ha uccisa un timeout esterno. Poi ho provato ad alzare una
    `BaseException` dal mock per non farla assorbire dall'`except Exception`
    del codice: si e' bloccata di nuovo, perche' un ALTRO test — quello sui
    codici UNC — monkeypatcha `lstat` senza contatore, e li' non c'era niente
    da alzare.

    Due tentativi di rendere *osservabile* il difetto, e nessuno dei due
    toccava la causa: un ciclo su un percorso che arriva da fuori. Ora c'e' un
    tetto (`MAX_PASSI_RISALITA`), e la domanda «puo' girare a vuoto?» non e'
    piu' una questione di ragionamento.

    **E c'e' un terzo capitolo.** Passando la risalita da `lstat` a `stat`
    (rilievo di GPT-5.6 Sol all'ottavo giro) questo test e' diventato
    **vacuo senza che niente diventasse rosso**: pilotava solo `os.lstat`, e
    la risalita interrogava il filesystem vero, che dopo un passo trova una
    cartella esistente e si ferma. Rifatto il sabotaggio: **51 passed** con
    la condizione d'uscita tolta. Un cambio di una riga nel codice aveva
    disinnescato la verifica che lo sorvegliava, in silenzio.

    Qui `stat` e `lstat` non trovano MAI niente: la risalita deve finire —
    per fixpoint o per tetto — e in un numero di passi contato.
    """
    import betfair_client

    passi = []

    def non_trova_mai(percorso, *a, **kw):
        passi.append(str(percorso))
        raise FileNotFoundError(2, "non trovato")

    monkeypatch.setattr(os, "lstat", non_trova_mai)
    monkeypatch.setattr(os, "stat", non_trova_mai)
    esito = betfair_client._stato_proxy_altrove(str(tmp_path / "a" / "b" / "c.json"))

    # Con la condizione d'uscita la risalita si ferma al fixpoint: pochi passi
    # e «non c'e'». Senza, arriva al tetto e risponde «incerto» dopo 64+ passi.
    # Accettare entrambi gli esiti rendeva il test cieco al sabotaggio: e' il
    # tetto a evitare il blocco, ma non e' il tetto che si sta verificando qui.
    assert esito == "nessun_file", esito
    assert len(passi) < betfair_client.MAX_PASSI_RISALITA, (
        f"la risalita non si e' fermata al fixpoint: {len(passi)} passi")


def test_toccare_il_tetto_e_incertezza_non_assenza(monkeypatch, tmp_path):
    """E se il tetto lo si tocca davvero, cosa si risponde?

    Non «non c'e'»: toccare il tetto vuol dire che non si e' stabilito niente,
    e in questo modulo non stabilire niente vale quanto un proxy dichiarato.
    Senza questo test il ramo del tetto potrebbe restituire assenza e nessuno
    se ne accorgerebbe.
    """
    import betfair_client

    # `dirname` non accorcia mai: la risalita puo' finire solo per tetto.
    monkeypatch.setattr(os.path, "dirname", lambda p: str(p) + "/x")

    def non_trova_mai(percorso, *a, **kw):
        raise FileNotFoundError(2, "non trovato")

    # Entrambe: la risalita usa `stat`, l'ispezione del file `lstat`.
    monkeypatch.setattr(os, "lstat", non_trova_mai)
    monkeypatch.setattr(os, "stat", non_trova_mai)
    assert betfair_client._stato_proxy_altrove(
        str(tmp_path / "c.json")) == "incerto"


def test_la_risalita_si_ferma_alla_radice(monkeypatch, tmp_path):
    """Un `while True` su un percorso va dimostrato limitato, non affermato tale.

    Se NIENTE del percorso esiste, `dirname` accorcia a ogni passo e alla
    radice restituisce se stessa: la risalita finisce e dice «non c'e'».

    **Il tetto sulle chiamate non alza `AssertionError`, e non e' un
    dettaglio.** La prima versione di questo test lo faceva, e sabotando la
    condizione d'uscita la suite **non falliva: si bloccava**, finche' non
    l'ha uccisa un `timeout` esterno. Il motivo e' che l'assertion nasce
    dentro `lstat`, cioe' dentro il `try` del codice in prova, e quel
    `except Exception` se la mangia — il codice riprendeva a risalire come se
    niente fosse. Una verifica che il codice in prova puo' assorbire non e'
    una verifica.

    `_RisalitaSenzaFine` eredita da `BaseException` proprio per non essere
    catturabile da un `except Exception`. Rifatto il sabotaggio: ora fallisce
    invece di appendersi.
    """
    import betfair_client

    passi = []

    def lstat_che_non_trova_mai(percorso, *a, **kw):
        passi.append(str(percorso))
        if len(passi) > 200:
            raise _RisalitaSenzaFine(str(percorso))
        raise FileNotFoundError(2, "non trovato")

    monkeypatch.setattr(os, "lstat", lstat_che_non_trova_mai)
    assert betfair_client._stato_proxy_altrove(
        str(tmp_path / "a" / "b" / "c.json")) == "nessun_file"
    assert len(passi) < 200, passi


def test_una_JUNCTION_verso_una_share_morta_non_e_arrivarci(monkeypatch, tmp_path):
    """Rilievo BLOCCANTE di GPT-5.6 Sol, ottavo giro. Fondato.

    *«`os.lstat(cartella)` non verifica che la directory sia realmente
    attraversabile. Su junction/symlink Windows verso share indisponibile puo'
    riuscire sul reparse point, classificando erroneamente
    `PROXY_NESSUN_FILE`.»*

    Esatto: `lstat` **non segue**, quindi su una junction verso una
    condivisione morta riesce guardando il puntatore invece della
    destinazione. «Ci sono arrivato» diventa falso, e il fail-open torna.

    Le due domande sono diverse e vogliono due chiamate diverse:

        sul FILE       «la voce esiste?»    -> `lstat`, che non segue
        sulla CARTELLA «ci sono arrivato?»  -> `stat`,  che segue

    **Questo test e' nato da un sabotaggio fallito.** Rimesso `lstat` sulla
    cartella, la suite restava verde: il finto condiviso rispondeva identico
    a `stat` e `lstat`, quindi la differenza fra le due — cioe' l'intero
    rilievo — non era osservabile. Qui invece rispondono **diverso**, che e'
    esattamente la situazione descritta.
    """
    import betfair_client

    def lstat_vede_il_puntatore(percorso, *a, **kw):
        # Sul FILE la voce non c'e' — altrimenti `_assenza_o_incertezza`
        # risponderebbe «incerto» prima ancora di arrivare alla risalita, e
        # il test passerebbe senza aver mai esercitato cio' che dichiara.
        # (Ci sono cascato: la prima versione mockava `lstat` per qualunque
        # percorso e il sabotaggio restava verde.)
        if str(percorso).endswith("config.json"):
            raise FileNotFoundError(2, "non trovato")
        # Sulla CARTELLA riesce: e' il reparse point della junction.
        return os.stat_result((0o40755, 0, 0, 1, 0, 0, 0, 0, 0, 0))

    def stat_segue_e_non_arriva(percorso, *a, **kw):
        errore = FileNotFoundError(2, "share non raggiungibile")
        errore.winerror = 53          # ERROR_BAD_NETPATH
        raise errore

    def open_non_trova(percorso, *a, **kw):
        raise FileNotFoundError(2, "non trovato")

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "XTraderBridge"))
    monkeypatch.setattr("builtins.open", open_non_trova)
    monkeypatch.setattr(os, "lstat", lstat_vede_il_puntatore)
    monkeypatch.setattr(os, "stat", stat_segue_e_non_arriva)

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_un_NODO_INTERMEDIO_morto_non_e_assenza(monkeypatch, tmp_path):
    """Rilievo BLOCCANTE di OpenRouter Fugu Ultra, nono giro. Fondato.

    *«se un nodo INTERMEDIO (es. `XTraderBridge` reindirizzato o di rete
    temporaneamente giu') restituisce 3 e `_non_raggiungibile` non lo
    intercetta, il loop sale al genitore locale, riesce, e ritorna
    `PROXY_NESSUN_FILE`. Falsa assenza → il controllo viene bypassato.»*

    La strada c'era davvero: verificato sabotando: senza il rimedio questo
    test non falliva, **non esisteva**.

    **Il rimedio chiesto pero' non si puo' applicare.** Fugu chiede di far
    coprire a `_non_raggiungibile` anche il `winerror` 3: ma 3 e' il codice
    che riceve chi non ha mai installato il Bridge, e bloccherebbe tutti loro
    — lo stesso motivo per cui avevo gia' rifiutato la stessa richiesta di
    GPT al settimo giro.

    La distinzione si fa senza indovinare il codice, con lo stesso
    discriminante gia' usato un livello piu' sotto sul file: **`stat` segue,
    `lstat` no.** Se la voce esiste ma non ci si passa, e' una junction verso
    qualcosa di morto — non e' assenza, e' non esserci arrivati.

    Qui: `XTraderBridge` esiste come voce (`lstat` riesce) ma non e'
    attraversabile (`stat` da' un banale ENOENT, senza codice di rete), e la
    cartella superiore e' sanissima. Prima si risaliva a `%APPDATA%`, lo si
    trovava, e si concludeva «non c'e' nessuna vecchia configurazione».
    """
    import betfair_client

    sotto_prova = str(tmp_path)

    def dentro_al_bridge(p):
        # **Rilievo di Claude Fable 5, decimo giro:** *«monkeypatch globale di
        # `os.stat`, `os.lstat` e `builtins.open` senza whitelist per i
        # percorsi interni di pytest/importlib; se `pytest.raises` o
        # l'assertion rewriting toccano il filesystem durante il blocco, il
        # test fallisce per motivi estranei»*. Fondato, e non teorico: al giro
        # scorso un finto troppo largo su `os.stat` aveva gia' fatto uscire il
        # controllo alla prima riga. Ora i finti valgono **solo sotto
        # `tmp_path`**; fuori risponde il filesystem vero.
        return str(p).startswith(sotto_prova) and "XTraderBridge" in str(p)

    def stat_non_ci_passa(percorso, *a, **kw):
        # `.json` esclusi: `os.path.exists` passa da `os.stat`, e far
        # sembrare esistente il candidato di Pickfair faceva uscire il
        # controllo alla prima riga — il test falliva con DID NOT RAISE
        # senza aver mai raggiunto il codice che voleva esercitare.
        if dentro_al_bridge(percorso) or (
                str(percorso).startswith(sotto_prova)
                and str(percorso).endswith(".json")):
            # `.json` esclusi: `os.path.exists` passa da `os.stat`, e far
            # sembrare esistente il candidato di Pickfair faceva uscire il
            # controllo alla prima riga — il test falliva con DID NOT RAISE
            # senza aver mai raggiunto il codice che voleva esercitare.
            raise FileNotFoundError(2, "non trovato")   # nessun winerror di rete
        if not str(percorso).startswith(sotto_prova):
            return _vero_stat(percorso, *a, **kw)
        return os.stat_result((0o40755, 0, 0, 1, 0, 0, 0, 0, 0, 0))

    def lstat_vede_la_voce(percorso, *a, **kw):
        if not str(percorso).startswith(sotto_prova):
            return _vero_lstat(percorso, *a, **kw)
        if str(percorso).endswith("config.json"):
            raise FileNotFoundError(2, "non trovato")
        # La CARTELLA c'e' come voce: e' il reparse point.
        return os.stat_result((0o40755, 0, 0, 1, 0, 0, 0, 0, 0, 0))

    def open_non_trova(percorso, *a, **kw):
        if not str(percorso).startswith(sotto_prova):
            return _vero_open(percorso, *a, **kw)
        raise FileNotFoundError(2, "non trovato")

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "XTraderBridge"))
    monkeypatch.setattr("builtins.open", open_non_trova)
    monkeypatch.setattr(os, "stat", stat_non_ci_passa)
    monkeypatch.setattr(os, "lstat", lstat_vede_la_voce)

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_una_cartella_ILLEGGIBILE_non_e_una_cartella_assente(monkeypatch, tmp_path):
    """Rilievo BLOCCANTE di GPT-5.6 Sol, Fable e Fugu Ultra — tutti e tre,
    indipendentemente, sulla stessa riga scritta al giro precedente.

    *«l'`except Exception` che ritorna `False` tratta un `PermissionError` su
    `lstat` come assenza. Ma se `lstat` fallisce per permessi la voce
    **esiste** ed e' inaccessibile: si risale al genitore e si puo' ancora
    produrre `PROXY_NESSUN_FILE` — la stessa falsa assenza che la PR vuole
    chiudere.»*

    Avevano ragione tutti e tre. Il rimedio al rilievo di Fugu del nono giro
    conteneva, **una riga piu' sotto**, lo stesso `except Exception` con
    default benigno che questa PR insegue da dieci giri. Non l'ho scritto per
    distrazione: l'ho scritto perche' e' un riflesso.

    Qui la cartella del Bridge non e' guardabile (`PermissionError` sia su
    `stat` che su `lstat`) e il genitore e' sanissimo. Prima si risaliva, lo
    si trovava, e si concludeva «non c'e' nessuna vecchia configurazione».
    """
    import betfair_client

    sotto_prova = str(tmp_path)

    def nel_bridge(percorso):
        return (str(percorso).startswith(sotto_prova)
                and "XTraderBridge" in str(percorso))

    def stat_non_trova(percorso, *a, **kw):
        # `stat` segue e dice «non trovato», SENZA codice di rete: e' la
        # risposta che manda la risalita a chiedere se la voce ci sia.
        #
        # **Solo il nodo del Bridge e' anomalo.** La cartella superiore deve
        # essere sanissima, altrimenti il sabotaggio sfugge: con il difetto
        # rimesso la risalita si fermerebbe LI', rispondendo «incerto» per il
        # genitore invece che per il Bridge, e il test passerebbe senza aver
        # misurato niente. Verificato tracciando le chiamate, non a occhio.
        if nel_bridge(percorso) or (
                str(percorso).startswith(sotto_prova)
                and str(percorso).endswith(".json")):
            raise FileNotFoundError(2, "non trovato")
        return _vero_stat(percorso, *a, **kw)

    def lstat_nega_la_voce(percorso, *a, **kw):
        # `lstat` sul FILE: non c'e' davvero, cosi' si arriva alla risalita.
        if str(percorso).endswith("config.json"):
            raise FileNotFoundError(2, "non trovato")
        # `lstat` sulla CARTELLA: permesso negato. La voce c'e' — e' il
        # sistema che non lascia guardare. Questa e' la riga che i tre
        # reviewer hanno segnalato: prima diventava «assente».
        if nel_bridge(percorso):
            raise PermissionError(13, "accesso negato")
        return _vero_lstat(percorso, *a, **kw)

    def open_non_trova(percorso, *a, **kw):
        if str(percorso).startswith(sotto_prova):
            raise FileNotFoundError(2, "non trovato")
        return _vero_open(percorso, *a, **kw)

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "XTraderBridge"))
    monkeypatch.setattr("builtins.open", open_non_trova)
    monkeypatch.setattr(os, "stat", stat_non_trova)
    monkeypatch.setattr(os, "lstat", lstat_nega_la_voce)

    with pytest.raises(ValueError, match="non e' stato possibile stabilire"):
        betfair_client._controlla_config_rimasta_nel_bridge(
            [str(tmp_path / "assente.json")])


def test_una_cartella_che_non_c_e_resta_assenza(monkeypatch, tmp_path):
    """La controprova del test qui sopra: il rimedio non blocca chi non ha nulla.

    Stessa forma, un'unica differenza: la voce `XTraderBridge` **non esiste**,
    quindi nemmeno `lstat` la trova. E' il caso di chiunque non abbia mai
    installato il Bridge, e deve restare assenza — altrimenti il rimedio al
    rilievo di Fugu sarebbe il blocco per tutti che ho rifiutato due volte.

    Niente mock qui: filesystem vero, cartella davvero assente.
    """
    import betfair_client

    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge",
                        lambda: str(tmp_path / "mai-installato"))
    assert betfair_client._stato_proxy_altrove(
        str(tmp_path / "mai-installato" / "config.json")) == "nessun_file"


def test_su_un_symlink_VERO_il_mock_qui_sopra_dice_il_vero(monkeypatch, tmp_path):
    """La prova che il test a mock non sta descrivendo un sistema immaginario.

    Un test interamente a mock puo' codificare una convinzione sbagliata su
    cosa faccia il sistema operativo e passare lo stesso. Qui si costruisce un
    symlink **vero** e si verifica che si comporti come il mock assume:
    `exists` dice di no, `lexists` dice di si', e l'avvio si ferma.

    Salta dove i symlink non si possono creare (Windows senza Developer Mode),
    ma **il comportamento resta coperto li' dal test a mock** — che e' il punto
    della coppia: nessuna piattaforma resta senza verifica.
    """
    if not _symlink_utilizzabili(tmp_path):
        pytest.skip("symlink non creabili qui; il test a mock copre comunque il caso")

    import betfair_client

    vecchia = tmp_path / "XTraderBridge"
    vecchia.mkdir()
    (vecchia / "config.json").symlink_to(tmp_path / "bersaglio-sparito.json")
    monkeypatch.setattr(percorsi, "cartella_dati_del_bridge", lambda: str(vecchia))

    assert not os.path.exists(vecchia / "config.json"), \
        "presupposto del mock: per `exists` il link rotto non c'e'"
    assert os.path.lexists(vecchia / "config.json"), \
        "presupposto del mock: ma la voce di directory c'e' davvero"

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
        """Due fail-open chiusi qui, segnalati da GPT-5.6 Sol sulla PR #435.

        Questa funzione e' il grafo che decide chi e' vivo: se sbaglia, autorizza
        una cancellazione sbagliata. Aveva gli stessi due difetti del guard nuovo:

        1. `except Exception: return set()` trasformava un file illeggibile o non
           analizzabile in un file *senza import*. Un file saltato in silenzio
           dentro un controllo di raggiungibilita' e' un falso verde.
        2. Gli import relativi con livello > 1 non risalivano: da `a/b/mod.py` un
           `from ..core import app` diventava `a.b.core.app` invece di
           `a.core.app`, cioe' una dipendenza viva che il grafo non vedeva.

        Nessuno dei due ha influito su una decisione presa finora — oggi nel
        repository ogni file si analizza e non esiste nessun import di livello > 1
        (verificato) — ma restano fail-open su un controllo di sicurezza.
        """
        try:
            sorgente = percorso.read_text(encoding="utf-8")
        except OSError as errore:
            raise AssertionError(
                f"{percorso}: non leggibile ({errore.__class__.__name__}). Il grafo "
                "vivo non e' attendibile se un file viene saltato.") from errore
        try:
            albero = ast.parse(sorgente)
        except SyntaxError as errore:
            raise AssertionError(
                f"{percorso}: non analizzabile ({errore}). Il grafo vivo non e' "
                "attendibile se un file viene saltato.") from errore

        fuori = set()
        parti_pacchetto = percorso.relative_to(RADICE).parts[:-1]
        for nodo in ast.walk(albero):
            if isinstance(nodo, ast.Import):
                fuori.update(a.name for a in nodo.names)
            elif isinstance(nodo, ast.ImportFrom):
                base = nodo.module or ""
                if nodo.level:
                    risalita = nodo.level - 1
                    if risalita >= len(parti_pacchetto):
                        raise AssertionError(
                            f"{percorso}: import relativo di livello {nodo.level} da "
                            f"un package profondo {len(parti_pacchetto)} — Python lo "
                            "rifiuta con «attempted relative import beyond top-level "
                            "package». Non e' una dipendenza da risolvere.")
                    radice_rel = ".".join(
                        parti_pacchetto[:len(parti_pacchetto) - risalita])
                    base = (f"{radice_rel}.{base}" if (radice_rel and base)
                            else (radice_rel or base))
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
