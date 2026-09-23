from __future__ import annotations

import importlib.util
import ipaddress
import json
import logging
import math
import os
import re
import ssl
import stat
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse

import requests
from requests.exceptions import HTTPError, RequestException, Timeout

import percorsi
from circuit_breaker import CircuitBreaker
from core.type_helpers import safe_float, safe_int, safe_side

logger = logging.getLogger(__name__)

#: Betfair `customerRef` (placeOrders): stringa client per de-dup di
#: ri-sottomissioni entro una finestra di 60s. Vincoli API: <=32 caratteri,
#: charset ALFANUMERICO + `- . _ + * : ; ~`. Un ref fuori da questo insieme
#: viene RIFIUTATO da Betfair (l'intera placeOrders fallirebbe), quindi si
#: valida al confine e, se non conforme, si OMETTE (mai inviare mangled).
#: Usato con `fullmatch` (niente ancore `^`/`$`: `$` accetterebbe un newline
#: finale — un ref con `\n` in coda passerebbe erroneamente).
_CUSTOMER_REF_RE = re.compile(r"[A-Za-z0-9\-._+*:;~]{1,32}")

#: Errori di `placeOrders` con la risposta GIA' arrivata ma illeggibile: il
#: POST e' partito, quindi l'esito e' ignoto (`order_unknown`), mai FAILED.
_RISPOSTA_ILLEGGIBILE_DOPO_L_INVIO = frozenset({
    "INVALID_JSON", "INVALID_JSON_RPC", "BET_NO_REPORT",
})

#: I soli lati che `placeOrders` accetta. Un valore fuori da qui e' un errore
#: PRIMA dell'invio (`INVALID_SIDE`), mai un BACK di ripiego.
_LATI_VALIDI = frozenset({"BACK", "LAY"})

#: Se valorizzata, ha la precedenza su tutto: serve a chi installa il programma
#: in un percorso non standard, e ai test.
ENV_PERCORSO_CONFIG = "PICKFAIR_CONFIG_PATH"


def percorso_config_esplicito() -> Optional[str]:
    """Il percorso dichiarato dall'ambiente, se c'e'.

    Serve a distinguere due casi che non vanno confusi: **cercare** la
    configurazione fra piu' candidati, e **puntarla**. Chi valorizza
    `PICKFAIR_CONFIG_PATH` sta facendo una promessa su dove sta il file; se
    quel file e' illeggibile, proseguire senza proxy sarebbe indovinare.
    """
    valore = os.environ.get(ENV_PERCORSO_CONFIG, "").strip()
    return valore or None


def percorsi_config_candidati() -> List[str]:
    """Dove cercare la configurazione, in ordine di precedenza.

    Qui c'era un percorso assoluto scritto nel codice
    (``/home/ubuntu/Pickfair-nogui/config.json``) che puntava a un VPS
    dismesso. Conseguenza misurata, non ipotizzata: ``os.path.exists`` era
    sempre ``False``, quindi **il proxy non veniva mai configurato** — e
    nessuno poteva accorgersene, perche' l'unico ``except`` registrava e
    proseguiva. Una funzione che si crede attiva e non lo e'.

    Nessun percorso assoluto: si parte da cio' che l'ambiente dichiara, poi
    dalla cartella dati di Pickfair, poi dalla cartella del programma.

    **Qui c'era un secondo difetto, introdotto da me nella stessa #430 che
    doveva chiudere il primo.** Il candidato intermedio non era la cartella di
    Pickfair: era `core.config_store.config_path()`, cioe'
    ``%APPDATA%/XTraderBridge/config.json`` — la cartella dati di **un altro
    prodotto**, XTrader Signal Bridge. Chi non l'ha mai installato non ha quel
    percorso, quindi il candidato non trovava nulla: lo stesso identico esito
    del percorso assoluto che avevo appena tolto.

    E importarlo trascinava nel grafo vivo sei moduli del Bridge
    (`csv_writer`, `bridge_mode`, `confirmation_reader`, `token_store`,
    `language_select`, `config_store`). Il commento che accompagnava quella
    riga notava che `config_store` «importa a sua volta parti del progetto»:
    il costo era stato visto e pagato lo stesso.

    Ora il percorso lo dichiara `percorsi.py`, che non importa niente da
    `core`.
    """
    candidati: List[str] = []
    da_ambiente = percorso_config_esplicito()
    if da_ambiente:
        candidati.append(da_ambiente)
    candidati.append(percorsi.percorso_config())
    candidati.append(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    )
    return candidati


# Esiti dell'ispezione di una configurazione rimasta nella cartella del Bridge.
# Sono quattro e non due di proposito. Le due distinzioni che costano:
#   «non lo so»  non e'  «non c'e'»   (INCERTO vs NESSUN_FILE: la prima ferma)
#   «non c'e'»   non e'  «c'e' e non dichiara niente» (la seconda avvisa)
PROXY_DICHIARATO = "dichiarato"   # c'e' un proxy attivo
PROXY_ASSENTE = "assente"         # letto, interpretato, nessun proxy attivo
PROXY_INCERTO = "incerto"         # non si e' potuto stabilire
PROXY_NESSUN_FILE = "nessun_file"  # il file non c'e', accertato aprendolo

# Codici Windows che arrivano come `FileNotFoundError` ma NON dicono «non c'e'»:
# dicono «non ci sono arrivato».
#
# **Rilievo BLOCCANTE di OpenRouter Fugu Ultra, arrivato in ritardo sul range
# precedente, e fondato:** *«su CPython l'OSError concreto e' scelto da
# `errno`, non da `winerror`»*. Verificato qui, senza dedurlo:
#
#     OSError(ENOENT)  -> FileNotFoundError      <- solo questi arrivano
#     OSError(EACCES)  -> PermissionError
#     OSError(EINVAL)  -> OSError
#
# `_non_raggiungibile` viene consultata **solo** dentro `except
# FileNotFoundError`. Quindi elencare codici che Windows mappa su un errno
# diverso non serviva a niente: quelle voci erano morte, e i test che le
# esercitavano fabbricavano oggetti che il sistema operativo non produce mai
# — copertura falsa, esattamente il difetto che continuo a cercare altrove.
#
# Restano i codici che CPython mappa su ENOENT e che significano
# irraggiungibilita' invece di assenza. Gli altri (21, 55, 59, 64, 1231,
# 1232) **erano gia' fail-closed** per un'altra strada: non essendo
# `FileNotFoundError` finiscono nell'`except Exception`, che risponde
# `PROXY_INCERTO`. Il comportamento non cambia; cambia che adesso il codice
# non dichiara piu' una copertura che non aveva. C'e' un test che lo fissa.
WINERROR_NON_RAGGIUNGIBILE = frozenset({
    15,  # ERROR_INVALID_DRIVE   unita' non c'e' (mappato su ENOENT)
    53,  # ERROR_BAD_NETPATH     percorso di rete non trovato
    67,  # ERROR_BAD_NET_NAME    nome di rete non trovato
})


# Tetto alla risalita verso la cartella che dovrebbe contenere il file. Un
# percorso vero non ha 64 livelli; toccare il tetto significa che sta
# succedendo qualcosa che non capiamo, e allora vale incertezza.
MAX_PASSI_RISALITA = 64


def _non_raggiungibile(errore: BaseException) -> bool:
    """L'errore dice «non ci sono arrivato», non «non c'e'».

    **Rilievo BLOCCANTE di GPT-5.6 Sol su #434, quarto e sesto giro — e al
    quarto avevo risposto che non si poteva fare. Mi sbagliavo.**

    Avevo scritto, qui sotto e sulla PR, che una UNC caduta e un file mai
    esistito *«sono lo stesso fatto osservabile»*. E' vero guardando `errno`,
    che appiattisce tutto su ENOENT — e falso guardando `winerror`, che su
    Windows sopravvive dentro l'eccezione. Una condivisione irraggiungibile
    da' 53 o 67, un profilo caduto 64: un file che non esiste da' 2. Sono
    numeri diversi, e stavano nell'eccezione tutto il tempo.

    Avevo dichiarato un limite **senza verificarlo**, e per due giri quella
    frase e' rimasta nel codice a giustificare il fail-open che descriveva.
    """
    return getattr(errore, "winerror", None) in WINERROR_NON_RAGGIUNGIBILE


def _stato_proxy_altrove(percorso: str) -> str:
    """Cosa dichiara la configurazione rimasta nella cartella del Bridge.

    **Rilievo BLOCCANTE di GPT-5.6 Sol e Claude Fable 5 su #434, secondo giro,
    sollevato di nuovo indipendentemente da entrambi.** Al giro precedente
    questa funzione restituiva `False` su QUALUNQUE eccezione — permessi
    negati, errore di I/O, JSON malformato, race fra `exists()` e `open()` —
    e il chiamante proseguiva senza proxy.

    Cioe': *«non riesco a leggere il file, quindi faccio come se non ci
    fosse un proxy»*. Su una macchina dove il proxy c'e' davvero, questo
    manda le scommesse sulla connessione diretta — **esattamente il difetto
    che questa PR esiste per chiudere.**

    Peggio: avevo scritto un test che asseriva quel comportamento
    (*«una vecchia config illeggibile non ferma l'avvio»*), quindi il
    fail-open non era una svista rimasta scoperta — **era codificato e
    protetto da una verifica.**

    **Rilievo BLOCCANTE di GPT-5.6 Sol su #434, terzo giro.** L'esistenza del
    file la decideva il chiamante con `os.path.exists()`, che **restituisce
    `False` anche quando il file c'e' ma non si riesce a guardarlo** — cartella
    senza permesso di attraversamento, unita' di rete caduta, path lungo su
    Windows. Quel `False` diceva «non c'e' nessuna vecchia configurazione» e il
    controllo terminava fail-open: il file legacy poteva essere li', col suo
    proxy, e Betfair veniva raggiunta lo stesso in chiaro.

    Adesso l'assenza non si deduce piu' da una domanda che sa solo dire di no:
    **si accerta aprendo.** Solo `FileNotFoundError` (e `NotADirectoryError`,
    che dice la stessa cosa di un pezzo di percorso) valgono «non c'e'».
    Qualunque altro errore vale «non lo so», che qui pesa quanto un proxy
    dichiarato.

    Gli esiti sono quattro, e solo i primi due lasciano proseguire:

    - il file non esiste           -> `PROXY_NESSUN_FILE`
    - letto e interpretato, niente -> `PROXY_ASSENTE`
    - proxy attivo                 -> `PROXY_DICHIARATO`
    - illeggibile o non interpretabile -> `PROXY_INCERTO`

    Il file viene aperto **solo per rispondere a questa domanda**, mai per
    configurare: usarne il blocco significherebbe far uscire il traffico
    verso Betfair da un host scelto in un altro programma e mai riesaminato
    qui.
    """
    try:
        with open(percorso, "r", encoding="utf-8") as f:
            contenuto = f.read()
    except (FileNotFoundError, NotADirectoryError) as errore:
        if _non_raggiungibile(errore):
            return PROXY_INCERTO
        # Nemmeno questo prova l'assenza da solo: lo si chiede a `lstat`.
        return _assenza_o_incertezza(percorso)
    except Exception:
        # Permessi, I/O, una directory al posto di un file, il file sparito
        # fra due istruzioni. Non sappiamo cosa contenga: e non saperlo, qui,
        # non e' un permesso di proseguire.
        return PROXY_INCERTO
    try:
        dati = json.loads(contenuto)
    except Exception:
        # Letto ma non interpretabile. Un JSON rotto puo' contenere un proxy:
        # dire "assente" sarebbe inventare.
        return PROXY_INCERTO
    if not isinstance(dati, dict):
        return PROXY_INCERTO
    blocco = dati.get("proxy")
    if blocco is None:
        return PROXY_ASSENTE
    if not isinstance(blocco, dict):
        return PROXY_INCERTO
    return PROXY_DICHIARATO if blocco.get("enabled") else PROXY_ASSENTE


def _assenza_o_incertezza(percorso: str) -> str:
    """`open()` ha detto ENOENT. Ma «non trovato» non e' ancora «non c'e'».

    **Rilievo BLOCCANTE di GPT-5.6 Sol su #434, quarto giro.** Fondato, e sul
    caso che il giro prima aveva risolto solo a meta':

    > *«`FileNotFoundError` non prova l'assenza. Link/junction spezzati o
    > profili/UNC temporaneamente irraggiungibili possono produrlo,
    > classificando `PROXY_NESSUN_FILE` e consentendo traffico Betfair
    > diretto. Va distinto almeno il path legacy esistente tramite `lstat`.»*

    Un **symlink rotto** e' il caso pulito: `open()` segue il link, non trova
    il bersaglio e alza ENOENT — ma **la voce di directory c'e'**, e ce l'ha
    messa qualcuno. E' un proxy *indicato* e non raggiungibile: esattamente
    «dichiarato ma inutilizzabile», che in questo modulo vale eccezione.
    Questo stesso repository lo dice gia' per il percorso di
    `PICKFAIR_CONFIG_PATH` (*«vale anche per un symlink rotto, che `exists`
    segnala come assente»*, rilievo di GPT su #430): trattarlo diversamente
    qui era un'incoerenza, non una scelta.

    `os.lstat` non segue il link, quindi separa le due cose:

    - la voce non esiste proprio      -> `PROXY_NESSUN_FILE` (si prosegue)
    - la voce c'e' ma non si apre     -> `PROXY_INCERTO` (si ferma)
    - non si riesce nemmeno a guardare -> `PROXY_INCERTO` (si ferma)

    **Secondo rilievo di Claude Fable 5, quinto giro.** `NotADirectoryError`
    stava con la prima riga: se un pezzo intermedio del percorso e' un file
    normale, dentro non ci puo' essere niente, quindi «assente». Ma quella e'
    di nuovo una **deduzione**, non un accertamento: mi dice che un
    `config.json` non ci puo' stare *se il percorso e' quello che sembra*, e
    non ho modo di escludere che una junction o un mount ci abbiano messo
    altro in mezzo. Su un percorso da cui dipendono i soldi la deduzione non
    basta, e il costo del contrario e' trascurabile — perche' l'avvio si
    fermi serve un **file** chiamato `XTraderBridge` dentro `%APPDATA%`, e il
    messaggio dice cosa fare. Quindi anche questa e' incertezza.

    **Qui al quarto giro avevo scritto che l'altra meta' del rilievo — UNC o
    profilo irraggiungibili — non era chiudibile, perche' *«sono lo stesso
    fatto osservabile»*. Era sbagliato**, e GPT-5.6 Sol l'ha ri-sollevato al
    sesto. Guardando `errno` sono davvero lo stesso fatto: ENOENT per
    entrambi. Guardando `winerror`, che su Windows sopravvive dentro
    l'eccezione, no: 53 / 64 / 67 per la rete, 2 per un file che non c'e'.
    Il discriminante stava nell'eccezione da sempre; l'ho dichiarato
    impossibile senza provarci. Ora lo fa `_non_raggiungibile`.

    Resta indistinguibile solo `ERROR_PATH_NOT_FOUND` (3) da una cartella
    intermedia che davvero non esiste — ma li' i due casi coincidono anche
    nella sostanza: se la cartella del Bridge non c'e', non c'e' nemmeno una
    configurazione arenata dentro. **Questa volta l'ho verificato**, invece di
    dedurlo: `getattr(errore, "winerror", None)` e' `None` su Linux, quindi
    quel ramo non tocca nessuna piattaforma dove il codice non esiste.
    """
    try:
        os.lstat(percorso)
    except FileNotFoundError as errore:
        if _non_raggiungibile(errore):
            return PROXY_INCERTO
        # Non c'e' la voce. Ma ci siamo arrivati, dove doveva stare?
        return _ci_siamo_arrivati(os.path.dirname(percorso))
    except Exception:
        return PROXY_INCERTO
    return PROXY_INCERTO


def _la_voce_e_ASSENTE(percorso: str) -> bool:
    """La voce di directory manca — **accertato**, non dedotto.

    `stat` segue, `lstat` no. Se `lstat` riesce dove `stat` ha fallito, allora
    qualcosa **c'e'** — una junction, un symlink, un mount — e il bersaglio non
    e' raggiungibile. Non e' assenza: e' non esserci arrivati. E' lo stesso
    discriminante usato sul file, applicato al nodo intermedio.

    **Rilievo BLOCCANTE di GPT-5.6 Sol, Claude Fable 5 e OpenRouter Fugu
    Ultra su #434, decimo giro. Tutti e tre, indipendentemente, sulla stessa
    riga.** La prima versione di questa funzione si chiamava
    `_la_voce_esiste_comunque` e finiva con:

        except Exception:
            return False        # -> il chiamante conclude «assente»

    Cioe' un `PermissionError` su `lstat` — la voce **c'e'** ma non si puo'
    guardare — diventava assenza, il chiamante risaliva a un genitore
    accessibile e rispondeva `PROXY_NESSUN_FILE`. **La stessa falsa assenza
    che questa funzione era stata scritta per chiudere, riaperta dentro il
    rimedio, una riga dopo.**

    E' il difetto che questa PR ha inseguito per dieci giri — `except
    Exception` con un default benigno — riprodotto da me nella correzione. Non
    e' una svista isolata: e' la prova che il pattern e' un riflesso, e che
    l'unico modo di non ripeterlo e' che il nome della funzione dica cosa deve
    essere **accertato**, non cosa si spera.

    Percio' adesso la domanda e' rovesciata, e una sola risposta lascia
    proseguire:

    - `FileNotFoundError`  -> assenza accertata  -> `True`  (si risale)
    - qualunque altro errore -> non si sa       -> `False` (il chiamante ferma)
    - `lstat` riesce         -> la voce c'e'    -> `False` (il chiamante ferma)
    """
    try:
        os.lstat(percorso)
    except FileNotFoundError:
        return True
    except Exception:
        # Permessi, I/O, rete: la voce potrebbe esserci benissimo. Non
        # saperlo non e' un permesso di concludere che non c'e'.
        return False
    return False


def _ci_siamo_arrivati(cartella: str) -> str:
    """Il file non c'e'. Siamo riusciti a guardare nel posto dove doveva stare?

    **Rilievo BLOCCANTE di GPT-5.6 Sol su #434, settimo giro:** `winerror` 3
    (`ERROR_PATH_NOT_FOUND`) restava classificato come assenza, ma *«un profilo
    reindirizzato o percorso di rete temporaneamente indisponibile puo'
    restituire ERROR_PATH_NOT_FOUND»*.

    **Il rimedio letterale — trattare 3 come incertezza — non si puo'
    applicare**, e la ragione e' il caso principale, non un dettaglio: aprire
    `%APPDATA%` + `XTraderBridge` + `config.json` quando la cartella `XTraderBridge`
    non esiste da' proprio 3, perche' manca un componente *intermedio*. Cioe'
    e' il codice che riceve **chiunque non abbia mai installato il Bridge**.
    Trattarlo come incertezza avrebbe impedito l'avvio a tutti loro — il
    difetto opposto, e piu' grave.

    Il rilievo pero' e' fondato, e si chiude smettendo di **dedurre dal
    codice** e andando a **guardare**: se la cartella che doveva contenere il
    file e' ispezionabile, allora il file davvero non c'e'; se risalendo si
    incontra un errore di rete, non ci siamo arrivati e vale incertezza.

    Cosi' il risultato **non dipende da quale codice Windows scelga** in ogni
    situazione — che e' bene, perche' quello non posso verificarlo da qui, e
    l'ultima volta che ho dato per scontato un comportamento del sistema
    operativo senza provarlo mi sbagliavo.

    **La risalita ha un tetto, e non e' pignoleria.** La prima versione era un
    `while True` che si fidava di `dirname`: accorcia a ogni passo e alla
    radice restituisce se stessa, quindi «non puo' girare a vuoto». Sabotando
    la condizione d'uscita per verificarlo, la suite **non e' fallita: si e'
    bloccata**, due volte, finche' non l'ha uccisa un timeout esterno. Un
    ciclo su un percorso che arriva da fuori non va argomentato limitato: va
    reso limitato. Toccato il tetto non si inventa una risposta — non si e'
    stabilito niente, quindi incertezza.
    """
    for _ in range(MAX_PASSI_RISALITA):
        try:
            # `stat`, non `lstat`. **Rilievo BLOCCANTE di GPT-5.6 Sol,
            # ottavo giro:** *«`os.lstat(cartella)` non verifica che la
            # directory sia realmente attraversabile. Su junction/symlink
            # Windows verso share indisponibile puo' riuscire sul reparse
            # point»*. Vero: `lstat` non segue, quindi su una junction verso
            # una condivisione morta riesce guardando il puntatore invece
            # della destinazione — e «ci sono arrivato» diventa falso.
            #
            # Le due domande sono diverse e vogliono due chiamate diverse:
            #   sul FILE       «la voce esiste?»       -> `lstat`, non segue
            #   sulla CARTELLA «ci sono arrivato?»     -> `stat`, segue
            os.stat(cartella)
        except FileNotFoundError as errore:
            if _non_raggiungibile(errore):
                return PROXY_INCERTO
            if not _la_voce_e_ASSENTE(cartella):
                # **Rilievo BLOCCANTE di OpenRouter Fugu Ultra, nono giro.**
                # *«se un nodo INTERMEDIO restituisce 3 e `_non_raggiungibile`
                # non lo intercetta, il loop sale al genitore locale, riesce, e
                # ritorna `PROXY_NESSUN_FILE`. Falsa assenza.»* Fondato: la
                # strada c'era.
                #
                # Il rimedio chiesto — mettere 3 fra i codici di
                # irraggiungibilita' — **non si puo' applicare**, per la stessa
                # ragione del giro scorso: 3 e' il codice che riceve chi non ha
                # mai installato il Bridge, e bloccherebbe tutti loro.
                #
                # Ma la distinzione non ha bisogno di indovinare il codice, ed
                # e' la stessa gia' usata un livello piu' sotto: se la voce
                # ESISTE (`lstat` riesce) ma non ci si passa (`stat` no), e'
                # una junction verso qualcosa di morto. Se non esiste
                # nemmeno la voce, e' assenza e si risale.
                return PROXY_INCERTO
            genitore = os.path.dirname(cartella)
            if not genitore or genitore == cartella:
                # Risalito fino alla radice senza trovare niente: non c'e'
                # nessun posto dove una vecchia configurazione possa stare.
                return PROXY_NESSUN_FILE
            cartella = genitore
            continue
        except Exception:
            # La cartella c'e' ma non si e' potuto guardarci: permessi, I/O.
            return PROXY_INCERTO
        # Ci siamo arrivati, e il file non c'era: assenza accertata.
        return PROXY_NESSUN_FILE
    return PROXY_INCERTO


def _controlla_config_rimasta_nel_bridge(candidati: List[str]) -> None:
    """Se la configurazione e' rimasta nella cartella del Bridge, non si tira dritto.

    Rilievo BLOCCANTE di GPT-5.6 Sol e Claude Fable 5 su #434, sollevato
    indipendentemente da entrambi in due giri successivi.

    Al primo giro qui c'era solo un `logger.warning` e il client proseguiva
    **senza proxy**: se il proxy serviva a far uscire il traffico da una rete
    precisa, un aggiornamento avrebbe mandato le scommesse sulla connessione
    diretta, in silenzio.

    Contraddiceva la regola scritta in questo stesso modulo poche righe piu'
    sotto —

        Configurazione DICHIARATA ma inutilizzabile  ->  eccezione.
        Nessuna configurazione                        ->  nessun proxy, in silenzio.

    Un proxy nella vecchia cartella **e' dichiarato**: che Pickfair non sappia
    piu' leggerlo da li' lo rende inutilizzabile, non inesistente.

    Al secondo giro restava un fail-open piu' sottile: un file **illeggibile**
    veniva trattato come «nessun proxy». Ora l'incertezza vale quanto la
    dichiarazione: si ferma. **Solo un'assenza accertata lascia proseguire.**

    Al terzo giro ne restavano due, sollevati da GPT-5.6 Sol e Claude Fable 5:

    1. l'esistenza del vecchio file la decideva `os.path.exists()`, che dice
       `False` anche quando il file c'e' ma non e' raggiungibile. Ora la
       decide `_stato_proxy_altrove` aprendolo davvero;
    2. **restava un `except Exception: return` attorno a tutto.** Serviva a
       dire «se non so nemmeno DOVE guardare, lascio correre» — che e' la
       stessa frase, parola per parola, che questa PR esiste per cancellare
       dagli altri due punti. Un errore imprevisto nel calcolo dei percorsi
       non e' una prova che il proxy non serva: e' l'ennesimo «non lo so», e
       qui si ferma come tutti gli altri. Se `percorsi` un giorno sollevasse,
       l'avvio deve rompersi in modo rumoroso — non partire in chiaro.

    Percio' qui non c'e' piu' nessun `try`.
    """
    if any(percorso and os.path.exists(percorso) for percorso in candidati):
        # Un `False` sbagliato qui e' innocuo, ed e' il motivo per cui
        # `os.path.exists` va ancora bene su QUESTI percorsi e non sull'altro:
        # se sbaglia, si finisce a controllare la cartella del Bridge, cioe' a
        # guardare piu' a fondo. Sull'altro percorso sbagliare significava
        # smettere di guardare.
        return

    vecchia = os.path.join(percorsi.cartella_dati_del_bridge(), percorsi.NOME_CONFIG)
    stato = _stato_proxy_altrove(vecchia)
    if stato == PROXY_NESSUN_FILE:
        # Il caso di chiunque non abbia mai avuto il Bridge installato.
        return

    dove_va = percorsi.percorso_config()
    if stato == PROXY_DICHIARATO:
        raise ValueError(
            f"un proxy e' dichiarato e attivo in {vecchia} (cartella di XTrader "
            f"Signal Bridge), ma Pickfair non legge da li'. Proseguire "
            f"manderebbe le scommesse sulla connessione diretta invece che sul "
            f"proxy configurato. Sposta il file in {dove_va}, oppure valorizza "
            f"{ENV_PERCORSO_CONFIG} con il suo percorso"
        )
    if stato == PROXY_INCERTO:
        raise ValueError(
            f"esiste una configurazione in {vecchia} (cartella di XTrader "
            f"Signal Bridge) ma non e' stato possibile stabilire se dichiari un "
            f"proxy: illeggibile o non interpretabile. Proseguire significherebbe "
            f"scommettere sperando che un proxy non ci fosse. Rendi leggibile "
            f"quel file, oppure spostalo in {dove_va}, oppure valorizza "
            f"{ENV_PERCORSO_CONFIG}"
        )
    logger.warning(
        "Nessuna configurazione trovata nei percorsi di Pickfair, ma ne risulta "
        "una in %s (cartella di XTrader Signal Bridge). Non dichiara alcun "
        "proxy attivo, quindi si prosegue senza. Se ti serve, spostala in %s "
        "oppure valorizza %s.",
        vecchia, dove_va, ENV_PERCORSO_CONFIG)


# ---------------------------------------------------------------------------
# LA REGOLA, una sola, da cui discende tutto il resto di questo modulo.
#
#   Configurazione DICHIARATA ma inutilizzabile  ->  eccezione.
#   Nessuna configurazione                        ->  nessun proxy, in silenzio.
#
# Cinque giri di review su #430 hanno trovato cinque punti in cui questo modulo
# tradiva la propria stessa regola, uno per volta, perche' la regola era
# applicata caso per caso invece che enunciata. Scritta qui, la tabella completa
# non lascia buchi da scoprire a strati:
#
#   | situazione                                   | esito         |
#   |----------------------------------------------|---------------|
#   | nessun file, nessun percorso dichiarato      | niente proxy  |
#   | percorso DICHIARATO assente o illeggibile    | ECCEZIONE     |
#   | candidato non dichiarato illeggibile         | si supera     |
#   | primo file leggibile, senza chiave `proxy`   | niente proxy  |
#   | primo file leggibile, `proxy` malformato     | ECCEZIONE     |
#   | `proxy` valido, `enabled` falso              | niente proxy  |
#   | `proxy` valido, `enabled` ma incompleto      | ECCEZIONE     |
#   | `port` non intera o fuori da 1-65535         | ECCEZIONE     |
#   | tipo `socks*` ma PySocks non installato      | ECCEZIONE     |
#   | `type` non fra gli schemi supportati          | ECCEZIONE     |
#   | `host` con caratteri che dirottano l'URL      | ECCEZIONE     |
#
# "Niente proxy" compare solo dove NESSUNO ne ha chiesto uno. Ovunque qualcuno
# l'abbia chiesto e non si possa dargliela, si ferma.
# ---------------------------------------------------------------------------


#: Gli unici schemi che `requests` sa davvero parlare come proxy. Non e' un
#: elenco difensivo: e' il contratto. `socks5h` e `socks4a` risolvono il DNS
#: dal lato del proxy, `socks5` e `socks4` in locale.
SCHEMI_PROXY_SUPPORTATI = ("http", "https", "socks4", "socks4a", "socks5", "socks5h")


#: Caratteri che, dentro l'host, non restano nell'host: spostano il confine fra
#: le parti dell'URL. Il piu' pericoloso e' `@`, perche' non rompe l'URL — lo
#: fa puntare altrove.
CARATTERI_CHE_DIROTTANO = set('@/\\?#: \t\n\r"\'')


def _senza_segreti(valore: str) -> str:
    """Il valore reso mostrabile in un messaggio d'errore o in un log.

    Rilievo BLOCCANTE di OpenRouter Fugu Ultra su #430, fondato e misurato. Il
    caso: chi sbaglia e mette un URL intero dentro `proxy.host` — che e'
    proprio l'errore che `_host_valido` esiste per prendere — si vedeva la
    password stampata nel messaggio dell'eccezione, e da li' nei log d'avvio:

        proxy.host contiene caratteri ... 'socks5://pippo:SuperSegreta123@h.example:1080'

    Un controllo che protegge il traffico e intanto pubblica la credenziale non
    protegge niente. Si taglia da `@` in avanti — tutto cio' che sta prima e'
    esattamente la parte che puo' contenere utente e password.
    """
    if "@" in valore or ":" in valore:
        # Rilievo BLOCCANTE di GPT-5.6 Sol su #430, fondato: la prima versione
        # oscurava solo su `@`, quindi un `utente:PasswordSegreta` incollato per
        # sbaglio nel campo host — che ha `:` ma non `@` — finiva stampato
        # intero. `:` e `@` sono ENTRAMBI separatori di credenziale in un URL:
        # basta uno dei due perche' il valore non sia piu' mostrabile.
        return "<oscurato: contiene un separatore di credenziale>"
    if len(valore) > 60:
        # Stesso rilievo: troncare a 60 caratteri stampa comunque i primi 60,
        # che possono essere il segreto. Della lunghezza non se ne fa niente
        # nessuno tranne chi deve capire che il valore e' assurdo.
        return f"<oscurato: {len(valore)} caratteri>"
    return valore


def _host_valido(valore: str) -> str:
    """L'host, oppure ``ValueError``.

    Rilievo BLOCCANTE di OpenRouter Fugu Ultra su #430, fondato e misurato.
    Prima bastava che `host` non fosse vuoto, e l'interpolazione faceva il
    resto:

        host='evil.example@vero.example'  ->  socks5://evil.example@vero.example:1080
                                              urlparse -> hostname 'vero.example'

    Il traffico verso Betfair sarebbe uscito da un host **diverso** da quello
    configurato, e senza nessun errore. `/`, `?` e `#` sono meno gravi ma della
    stessa famiglia: fanno sparire la porta (`urlparse` la legge come `None`).

    E' la stessa classe del difetto sulle credenziali non codificate, corretto
    tre giri fa: una parte dell'URL che invade quella accanto.
    """
    host = valore.strip()
    if host.startswith("[") and host.endswith("]"):
        # IPv6 letterale: i due punti sono legittimi solo dentro le parentesi.
        interno = host[1:-1]
        # Rilievo di GPT-5.6 Sol su #430: `ipaddress` accetta lo scope ID
        # (`fe80::1%eth0`, ma anche `fe80::1%qualunque-cosa`). Per un indirizzo
        # locale ha senso; per l'host di un PROXY no — non si instrada il
        # traffico verso Betfair attraverso un link-local con scope. Verificato
        # che senza questo controllo `%eth0` passava e finiva nell'URL, mentre
        # altri scope venivano fermati piu' a valle per motivi che non so
        # spiegare: un comportamento che non so spiegare, sul percorso dei
        # soldi, e' una ragione per fermarsi, non per lasciar correre.
        if "%" in interno:
            raise ValueError(
                "proxy.host: uno scope ID IPv6 (`%`) non e' utilizzabile come "
                "host di un proxy. Il valore non viene riportato"
            )
        try:
            ipaddress.IPv6Address(interno)
        except ValueError:
            # Rilievi BLOCCANTI in sequenza di Claude Fable 5 e xAI Grok 4.6
            # su #430, tutti fondati, su due giri.
            #
            # Primo giro: oscuravo solo su `@`, quindi `[pippo:SuperSegreta]`
            # stampava la password. Secondo giro: passavo a una whitelist di
            # caratteri esadecimali, ma un token esadecimale — la forma piu'
            # comune per una API key — la supera, quindi `[deadbeef:cafe1234]`
            # tornava in chiaro. Il leak si restringeva, non si chiudeva.
            #
            # La verita' e' piu' semplice di tutti i miei tentativi: qui dentro
            # ci si arriva SOLO quando il valore NON e' un IPv6 valido. Se non
            # lo e', non sappiamo cosa sia, quindi non si mostra. Punto. E la
            # validita' la decide `ipaddress`, non un mio elenco di caratteri.
            raise ValueError(
                f"proxy.host non e' un indirizzo IPv6 valido "
                f"({len(host)} caratteri). Il valore non viene riportato: "
                f"se non e' un IPv6 non sappiamo cosa contenga"
            ) from None
        return host
    dirottanti = sorted(set(host) & CARATTERI_CHE_DIROTTANO)
    if dirottanti:
        raise ValueError(
            f"proxy.host contiene caratteri che cambiano il significato "
            f"dell'URL ({''.join(dirottanti)!r}): {_senza_segreti(valore)!r}. Con `@` il "
            f"traffico uscirebbe da un host diverso da quello configurato"
        )
    return host


def _porta_valida(valore: Any) -> int:
    """La porta come intero fra 1 e 65535, oppure ``ValueError``.

    Rilievo di OpenRouter Fugu Ultra e Claude Fable 5 su #430, fondato e
    misurato: prima bastava che `port` non fosse vuota. Con ``port: "abc"`` il
    risultato era ``socks5://h.example:abc`` — una stringa che passa ogni
    controllo di questo modulo e fallisce alla PRIMA richiesta verso Betfair,
    cioe' esattamente il fallimento tardivo e silenzioso che la regola qui
    sopra esiste per impedire. Stesso discorso per ``port: 0`` e ``port:
    99999``, che passavano entrambe.
    """
    try:
        porta = int(str(valore).strip())
    except (TypeError, ValueError):
        raise ValueError(
            f"proxy.port non e' un numero intero: {valore!r}. Un valore non "
            f"numerico produrrebbe un URL che fallisce alla prima richiesta "
            f"verso Betfair, non all'avvio"
        ) from None
    if not 1 <= porta <= 65535:
        raise ValueError(
            f"proxy.port fuori dall'intervallo valido 1-65535: {porta}"
        )
    return porta


def _supporto_socks_disponibile() -> bool:
    """``True`` se `requests` sa parlare SOCKS, cioe' se PySocks e' installato.

    `requests` non dichiara SOCKS fra le sue dipendenze: lo supporta solo con
    l'extra `requests[socks]`, che installa PySocks. Senza, ogni richiesta con
    un proxy `socks*` solleva ``InvalidSchema: Missing dependencies for SOCKS
    support``.
    """
    return importlib.util.find_spec("socks") is not None


def costruisci_proxy_url(proxy_cfg: Any) -> Optional[str]:
    """URL del proxy da una configurazione, oppure ``None`` se non e' richiesto.

    Solleva ``ValueError`` se il proxy e' RICHIESTO (``enabled``) ma la
    configurazione non basta a costruirlo. E' deliberato: un proxy che non si
    applica non e' un dettaglio estetico — il traffico verso Betfair esce
    dall'indirizzo sbagliato, che e' esattamente cio' che il proxy esisteva per
    evitare. Meglio non partire che partire diversamente da come si crede.

    Le credenziali sono opzionali. Prima venivano interpolate sempre, quindi un
    proxy senza utente produceva ``socks5://None:None@host:porta`` — una stringa
    che sembra un URL valido e non lo e'.
    """
    if not isinstance(proxy_cfg, dict) or not proxy_cfg.get("enabled"):
        return None

    host_grezzo = str(proxy_cfg.get("host") or "").strip()
    porta_grezza = proxy_cfg.get("port")
    if not host_grezzo or porta_grezza in (None, ""):
        raise ValueError(
            "proxy.enabled e' attivo ma host o port mancano: il traffico "
            "uscirebbe senza proxy senza che nessuno se ne accorga"
        )
    host = _host_valido(host_grezzo)
    porta = _porta_valida(porta_grezza)

    tipo = (str(proxy_cfg.get("type") or "").strip() or "socks5").lower()

    # Rilievo BLOCCANTE di GPT-5.6 Sol su #430, fondato e misurato: `type` non
    # era validato affatto. `socks5x` superava il controllo PySocks qui sotto
    # (comincia per "socks") e produceva un URL che `requests` rifiuta solo alla
    # prima richiesta con `Unable to determine SOCKS version`. E non era il caso
    # peggiore: `ftp`, `javascript` e qualunque altra parola passavano identici.
    # Stessa classe degli altri: una configurazione inutilizzabile che il modulo
    # dichiarava buona e che falliva sul percorso dei soldi invece che all'avvio.
    if tipo not in SCHEMI_PROXY_SUPPORTATI:
        raise ValueError(
            f"proxy.type `{proxy_cfg.get('type')}` non e' uno schema supportato. "
            f"Ammessi: {', '.join(SCHEMI_PROXY_SUPPORTATI)}"
        )

    # Un proxy SOCKS senza PySocks non e' un proxy che funziona male: e' un bot
    # che non piazza piu' nulla. Misurato: `InvalidSchema: Missing dependencies
    # for SOCKS support` su OGNI richiesta. E finche' il proxy non si applicava
    # mai — il difetto che questa PR corregge — il guasto era invisibile, quindi
    # e' proprio questa correzione a renderlo raggiungibile (rilievo bloccante
    # di OpenRouter Fugu Ultra su #430, confermato da Claude Fable 5).
    if tipo.startswith("socks") and not _supporto_socks_disponibile():
        raise ValueError(
            f"proxy di tipo `{tipo}` richiesto, ma il supporto SOCKS non e' "
            f"installato: `requests` solleverebbe `InvalidSchema: Missing "
            f"dependencies for SOCKS support` alla prima chiamata verso "
            f"Betfair, non all'avvio. Installa la dipendenza con "
            f"`pip install PySocks`. La dichiarazione nei requirements e' "
            f"un follow-up separato: vedi il triage su #430"
        )

    if tipo == "socks5":
        # Non riscriviamo il tipo dichiarato dall'operatore, ma non lo taciamo
        # nemmeno: con `socks5` la risoluzione DNS avviene in locale. Betfair
        # vede comunque solo l'IP del proxy; a vedere il nome risolto e' il
        # resolver di casa. Con `socks5h` risolve il proxy (rilievo di
        # OpenRouter Fugu Ultra su #430).
        logger.warning(
            "BetfairClient: proxy `socks5`: il DNS viene risolto in locale. "
            "Usa `socks5h` se vuoi che anche la risoluzione passi dal proxy"
        )
    utente = str(proxy_cfg.get("username") or "").strip()
    password = str(proxy_cfg.get("password") or "").strip()

    # Meta' credenziale non e' una credenziale (rilievo di Claude Fable 5 e
    # GPT-5.6 Sol su #430). Prima veniva scartata in silenzio e la connessione
    # diventava anonima: un proxy che chiede autenticazione l'avrebbe rifiutata,
    # oppure — peggio — l'avrebbe accettata come utente diverso.
    if bool(utente) != bool(password):
        raise ValueError(
            "proxy: utente e password vanno insieme. Una sola delle due "
            "produrrebbe una connessione anonima invece dell'errore"
        )

    # Le credenziali vanno CODIFICATE (rilievo di Claude Fable 5 e GPT-5.6 Sol
    # su #430). Misurato prima della correzione: con password `pa@ss:word/x` il
    # risultato era `socks5://pippo:pa@ss:word/x@h.example:1080`, che un parser
    # legge come host `ss` e porta `word`. Non "malformato": diretto altrove.
    credenziali = ""
    if utente:
        credenziali = f"{quote(utente, safe='')}:{quote(password, safe='')}@"
    url = f"{tipo}://{credenziali}{host}:{porta}"

    # Cintura oltre alle bretelle: l'URL appena costruito deve rileggersi come
    # lo si e' inteso. I controlli qui sopra elencano i modi di sbagliare che
    # CONOSCIAMO; questo verifica il risultato, che e' cio' che conta davvero.
    # Otto difetti su questa funzione sono stati tutti della stessa forma — una
    # parte dell'URL che finisce per significare un'altra — e una post-condizione
    # li prende anche quando l'elenco non li prevede.
    try:
        riletto = urlparse(url)
        hostname_riletto, porta_riletta = riletto.hostname, riletto.port
    except ValueError as exc:
        # `urlparse` solleva da solo su certi host malformati (per esempio un
        # IPv6 fatto di soli due punti). Il suo messaggio non e' il nostro
        # contratto e potrebbe riportare pezzi del valore: si converte.
        raise ValueError(
            f"la configurazione del proxy produce un URL illeggibile: {exc.__class__.__name__}"
        ) from None
    if hostname_riletto != host.strip("[]").lower() or porta_riletta != porta:
        raise ValueError(
            f"la configurazione del proxy produce un URL che non si rilegge "
            f"come atteso: host {_senza_segreti(str(hostname_riletto))!r} "
            f"invece di {_senza_segreti(host)!r}"
        )
    return url



class BetfairClient:
    # certlogin (login non-interattivo mutual-TLS): host DEDICATO con `-cert`
    # (identitysso-cert), richiesto da Betfair per l'autenticazione via
    # certificato client. keepAlive usa invece l'host standard SENZA `-cert`;
    # logout() è local-only (nessuna richiesta HTTP). Confermato dal supporto
    # Betfair per l'exchange Italia (.it).
    IDENTITY_URL = "https://identitysso-cert.betfair.it/api/certlogin"
    KEEPALIVE_URL = "https://identitysso.betfair.it/api/keepAlive"
    BETTING_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
    ACCOUNT_URL = "https://api.betfair.com/exchange/account/json-rpc/v1"

    SOCCER_EVENT_TYPE_ID = "1"

    # =========================================================
    # INIT
    # =========================================================
    def _configura_proxy(self, proxy_config: Optional[Dict[str, Any]] = None) -> None:
        """Applica il proxy alla sessione, se ne e' stato chiesto uno.

        Tre esiti, tutti espliciti:

        - nessuna configurazione trovata, o `enabled` falso -> non si fa nulla;
        - configurazione valida -> il proxy si applica e viene registrato
          (host e porta, MAI le credenziali);
        - `enabled` attivo ma configurazione insufficiente -> eccezione.

        Il terzo caso e' il motivo di questo metodo. Prima l'intero blocco stava
        dentro un `try/except Exception` che registrava e proseguiva, quindi
        qualunque errore — percorso inesistente compreso — diventava silenzio.
        """
        cfg = proxy_config
        if cfg is None:
            cfg = self._proxy_da_disco()
        if cfg is None:
            return

        url = costruisci_proxy_url(cfg)
        if url is None:
            return

        self.session.proxies = {"http": url, "https": url}
        logger.info(
            "BetfairClient: proxy %s configurato su %s:%s",
            str(cfg.get("type") or "socks5"), cfg.get("host"), cfg.get("port"),
        )

    @staticmethod
    def _proxy_da_disco() -> Optional[Dict[str, Any]]:
        """Blocco `proxy` dal primo file di configurazione leggibile.

        Un file assente non e' un errore: significa "nessun proxy configurato".
        Un file presente ma illeggibile lo e', e viene registrato come tale
        invece di sparire.
        """
        esplicito = percorso_config_esplicito()
        candidati = percorsi_config_candidati()
        _controlla_config_rimasta_nel_bridge(candidati)
        for percorso in candidati:
            if not percorso or not os.path.exists(percorso):
                if esplicito and percorso == esplicito:
                    # `esplicito and ...` non e' ridondante: senza variabile
                    # d'ambiente `esplicito` e' None, e un candidato falsy —
                    # caso che il `not percorso` qui sopra prevede — renderebbe
                    # vero `None == None`, sollevando all'avvio senza che
                    # nessuno abbia dichiarato niente. Regressione introdotta
                    # da me al giro precedente, trovata da GPT-5.6 Sol e Claude
                    # Fable 5 indipendentemente.
                    #
                    # Rilievo di GPT-5.6 Sol su #430, secondo giro: avevo
                    # tracciato la linea fra "dichiarato ma illeggibile"
                    # (eccezione) e "dichiarato ma assente" (si prosegue). E'
                    # una linea incoerente — in entrambi i casi l'operatore ha
                    # detto dove sta il file e il file non e' utilizzabile.
                    # Vale anche per un symlink rotto, che `exists` segnala
                    # come assente.
                    raise ValueError(
                        f"il percorso dichiarato da {ENV_PERCORSO_CONFIG} non "
                        f"esiste o non e' raggiungibile: {percorso}"
                    )
                continue
            try:
                with open(percorso, "r", encoding="utf-8") as fh:
                    dati = json.load(fh)
            except (OSError, ValueError) as exc:
                if esplicito and percorso == esplicito:
                    # Rilievo di GPT-5.6 Sol su #430, fondato: la versione
                    # precedente registrava un avviso e proseguiva senza proxy.
                    # Ma qui l'operatore aveva DICHIARATO dove sta il file: se
                    # e' illeggibile non sappiamo se voleva un proxy, e partire
                    # in chiaro e' una supposizione sul percorso dei soldi.
                    raise ValueError(
                        f"configurazione illeggibile nel percorso dichiarato da "
                        f"{ENV_PERCORSO_CONFIG} ({percorso}): {exc}"
                    ) from exc
                logger.warning(
                    "BetfairClient: configurazione illeggibile in %s (%s)", percorso, exc
                )
                continue
            # Il PRIMO file leggibile vince, punto (rilievo di OpenRouter Fugu
            # Ultra su #430). Prima si proseguiva quando il file non conteneva
            # la chiave `proxy`, e si finiva per applicare il proxy di un file
            # a precedenza PIU' BASSA — magari vecchio, magari scrivibile da
            # altri. Il traffico Betfair sarebbe uscito da un proxy che nessuno
            # aveva scelto, e questo e' il punto: un `config.json` senza blocco
            # `proxy` significa "nessun proxy", non "guarda altrove".
            proxy = dati.get("proxy") if isinstance(dati, dict) else None
            if proxy is not None and not isinstance(proxy, dict):
                # Rilievo di Claude Fable 5 e GPT-5.6 Sol su #430, accolto: al
                # giro precedente avevo scelto un warning, ragionando "non
                # possiamo sapere se `enabled` era vero". Il ragionamento e'
                # rovesciato: e' proprio il NON SAPERE la ragione per non tirare
                # a indovinare. Un blocco `proxy` illeggibile significa che
                # qualcuno un proxy lo voleva, e proseguire in chiaro decide al
                # posto suo sul percorso dei soldi.
                raise ValueError(
                    f"blocco `proxy` malformato in {percorso}: atteso un "
                    f"oggetto, trovato {type(proxy).__name__}"
                )
            return proxy if isinstance(proxy, dict) else None
        return None

    # =========================================================
    def __init__(
        self,
        *,
        username: str,
        app_key: str,
        cert_pem: str,
        key_pem: str,
        session: Optional[requests.Session] = None,
        timeout: float = 20.0,
        max_retries: int = 2,
        proxy_config: Optional[Dict[str, Any]] = None,
    ):
        self.username = str(username or "").strip()
        self.app_key = str(app_key or "").strip()
        self.cert_pem = str(cert_pem or "").strip()
        self.key_pem = str(key_pem or "").strip()

        self.timeout = float(timeout or 20.0)
        self.max_retries = max(0, int(max_retries))

        self.session = session or requests.Session()
        self._configura_proxy(proxy_config)


        self.session_token = ""
        self.session_expiry = ""
        self.connected = False
        self._session_state_lock = threading.RLock()

        # Circuit breaker guards all JSON-RPC calls to Betfair API.
        # SESSION_EXPIRED does NOT trip the breaker; only network/HTTP failures do.
        self._api_breaker = CircuitBreaker(max_failures=5, reset_timeout=60.0)
        self._io_stats: Dict[str, Any] = {
            "last_operation": "",
            "last_latency_ms": 0.0,
            "last_status": "UNKNOWN",
            "total_calls": 0,
            "slow_calls": 0,
            "degraded_calls": 0,
            "unavailable_calls": 0,
            "last_error": "",
            "last_call_at": 0.0,
        }

    def _redact_error_text(self, text: Any, *, token_snapshot: str = "") -> str:
        """Maschera il valore del session token nelle stringhe d'errore.

        La redazione strutturata (observability/sanitizers) lavora per
        CHIAVE sui payload: le stringhe d'errore grezze (eccezioni di rete,
        risposte API) passerebbero intatte fino a log, io_snapshot e
        get_status()['runtime_io']. Redatta sia il token CORRENTE sia lo
        snapshot del token usato per la richiesta (un altro thread puo'
        ruotarlo/azzerarlo tra invio ed eccezione). Soglia minima di
        lunghezza per evitare sostituzioni spurie su token degeneri.
        """
        out = str(text or "")
        candidates = {self._session_token_value(), str(token_snapshot or "")}
        # Dal piu' lungo al piu' corto: se un token e' substring dell'altro,
        # sostituire prima il corto lascerebbe un residuo parziale del lungo.
        for token in sorted(candidates, key=len, reverse=True):
            if token and len(token) >= 8 and token in out:
                out = out.replace(token, "***SESSION_TOKEN***")
        return out

    def _record_io(self, *, operation: str, started_at: float, status: str, error: str = "") -> None:
        elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
        status_up = str(status or "UNKNOWN").strip().upper()
        self._io_stats["last_operation"] = str(operation)
        self._io_stats["last_latency_ms"] = round(elapsed_ms, 3)
        self._io_stats["last_status"] = status_up
        self._io_stats["last_error"] = self._redact_error_text(error)
        self._io_stats["last_call_at"] = time.time()
        self._io_stats["total_calls"] = int(self._io_stats.get("total_calls", 0) or 0) + 1
        if status_up == "SLOW":
            self._io_stats["slow_calls"] = int(self._io_stats.get("slow_calls", 0) or 0) + 1
        if status_up == "DEGRADED":
            self._io_stats["degraded_calls"] = int(self._io_stats.get("degraded_calls", 0) or 0) + 1
        if status_up == "UNAVAILABLE":
            self._io_stats["unavailable_calls"] = int(self._io_stats.get("unavailable_calls", 0) or 0) + 1

    def io_snapshot(self) -> Dict[str, Any]:
        return dict(self._io_stats)

    # =========================================================
    # SAFE UTILS
    # =========================================================
    def _safe_float(self, v: Any, d: float = 0.0) -> float:
        return safe_float(v, d)

    def _safe_int(self, v: Any, d: int = 0) -> int:
        return safe_int(v, d)

    def _safe_side(self, v: Any) -> str:
        return safe_side(v)

    def _cert_tuple(self) -> tuple[str, str]:
        if not os.path.exists(self.cert_pem):
            raise RuntimeError("CERT_FILE_MISSING")
        if not os.path.exists(self.key_pem):
            raise RuntimeError("CERT_KEY_MISSING")

        self._validate_cert_file(self.cert_pem, is_key=False)
        self._validate_cert_file(self.key_pem, is_key=True)
        self._validate_certificate_content(self.cert_pem)
        return (self.cert_pem, self.key_pem)

    def _validate_cert_file(self, path: str, *, is_key: bool) -> os.stat_result:
        try:
            file_stat = os.stat(path)
        except OSError as exc:
            raise RuntimeError(f"CERT_UNREADABLE: {path}: {exc}") from exc

        if not stat.S_ISREG(file_stat.st_mode):
            raise RuntimeError(f"CERT_UNREADABLE: {path}: not a regular file")

        try:
            with open(path, "rb") as fh:
                fh.read(1)
        except OSError as exc:
            raise RuntimeError(f"CERT_UNREADABLE: {path}: {exc}") from exc

        if os.name == "posix":
            mode = stat.S_IMODE(file_stat.st_mode)
            unsafe_bits = stat.S_IWGRP | stat.S_IWOTH | stat.S_IXGRP | stat.S_IXOTH
            if is_key:
                unsafe_bits |= stat.S_IRGRP | stat.S_IROTH
            if mode & unsafe_bits:
                raise RuntimeError(f"CERT_PERMISSIONS_UNSAFE: {path}: mode={oct(mode)}")

        return file_stat

    def _validate_certificate_content(self, cert_path: str) -> None:
        try:
            decoded = ssl._ssl._test_decode_cert(cert_path)
        except Exception as exc:
            raise RuntimeError(f"CERT_INVALID_FORMAT: {cert_path}") from exc

        not_after_raw = str(decoded.get("notAfter") or "").strip()
        if not not_after_raw:
            raise RuntimeError(f"CERT_INVALID_FORMAT: {cert_path}: missing_notAfter")

        try:
            expires_at = datetime.strptime(not_after_raw, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise RuntimeError(f"CERT_INVALID_FORMAT: {cert_path}: invalid_notAfter") from exc

        if expires_at <= datetime.now(timezone.utc):
            raise RuntimeError(f"CERT_EXPIRED: {cert_path}: notAfter={not_after_raw}")

    def _headers(self) -> Dict[str, str]:
        with self._session_state_lock:
            session_token = self.session_token
        headers = {
            "X-Application": self.app_key,
            "Content-Type": "application/json",
        }
        if session_token:
            headers["X-Authentication"] = session_token
        return headers

    def _set_session_state(
        self,
        *,
        session_token: Optional[str] = None,
        session_expiry: Optional[str] = None,
        connected: Optional[bool] = None,
    ) -> None:
        with self._session_state_lock:
            if session_token is not None:
                self.session_token = str(session_token)
            if session_expiry is not None:
                self.session_expiry = str(session_expiry)
            if connected is not None:
                self.connected = bool(connected)

    def _clear_session_state(self) -> None:
        self._set_session_state(session_token="", session_expiry="", connected=False)

    def _session_token_value(self) -> str:
        with self._session_state_lock:
            return str(self.session_token or "")

    def _parse_json(self, response: Any, err_code: str) -> Any:
        try:
            return response.json()
        except Exception as exc:
            raise RuntimeError(err_code) from exc

    # =========================================================
    # ERROR CLASSIFICATION
    # =========================================================
    def _classify_error(self, error: str) -> str:
        e = str(error).upper()

        if "TIMEOUT" in e:
            return "TRANSIENT"

        if "NETWORK_ERROR" in e:
            return "TRANSIENT"

        if "HTTP_5" in e:
            return "TRANSIENT"

        if "SESSION_EXPIRED" in e:
            return "PERMANENT"

        if "INVALID_JSON" in e:
            return "PERMANENT"

        if "INVALID_JSON_RPC" in e:
            return "PERMANENT"

        if "API_ERROR" in e:
            return "PERMANENT"

        return "UNKNOWN"

    # =========================================================
    # CORE JSON-RPC
    # =========================================================
    def _post_jsonrpc(self, url: str, method: str, params: Dict[str, Any],
                      *, single_shot: bool = False) -> Any:
        started_at = time.monotonic()
        if not self._session_token_value():
            self._record_io(operation=method, started_at=started_at, status="UNAVAILABLE", error="NOT_AUTHENTICATED")
            raise RuntimeError("NOT_AUTHENTICATED")

        if self._api_breaker.is_open():
            self._record_io(operation=method, started_at=started_at, status="UNAVAILABLE", error="CIRCUIT_BREAKER_OPEN")
            raise RuntimeError("CIRCUIT_BREAKER_OPEN")

        payload = [{
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }]

        last_error: Optional[str] = None

        # single_shot: le chiamate non idempotenti (placeOrders) non vanno MAI
        # re-inviate — un timeout non prova che l'ordine non sia stato piazzato.
        attempts = 1 if single_shot else (self.max_retries + 1)
        # Inizializzato PRIMA del try: se _headers() stessa solleva, il
        # branch except lo referenzia senza UnboundLocalError.
        token_snapshot = ""
        for attempt in range(attempts):
            try:
                headers = self._headers()
                # Snapshot del token del TENTATIVO: la redazione deve coprire
                # anche un token ruotato/azzerato da un altro thread prima
                # della gestione dell'eccezione.
                token_snapshot = str(headers.get("X-Authentication") or "")
                response = self.session.post(
                    url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=self.timeout,
                )

                response.raise_for_status()

                data = self._parse_json(response, "INVALID_JSON")

                if not isinstance(data, list) or not data:
                    raise RuntimeError("INVALID_JSON_RPC")

                item = data[0]

                if "error" in item:
                    # Redatta anche l'errore API: la risposta puo' riflettere
                    # il session token nel payload d'errore.
                    err = self._redact_error_text(item["error"], token_snapshot=token_snapshot)

                    if "INVALID_SESSION" in err or "NO_SESSION" in err:
                        self._clear_session_state()
                        raise RuntimeError("SESSION_EXPIRED")

                    raise RuntimeError(f"API_ERROR: {err}")

                result = item.get("result") or {}
                self._api_breaker.record_success()
                elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
                status = "SLOW" if elapsed_ms >= max(2000.0, self.timeout * 1000.0 * 0.8) else "SUCCESS"
                self._record_io(operation=method, started_at=started_at, status=status)
                return result

            except Timeout:
                last_error = "TIMEOUT"
                logger.warning("timeout attempt=%s method=%s", attempt, method)

            except HTTPError as exc:
                code = getattr(exc.response, "status_code", "UNKNOWN")
                last_error = f"HTTP_{code}"
                logger.warning("http error attempt=%s method=%s code=%s", attempt, method, code)

            except RequestException as exc:
                last_error = (
                    f"NETWORK_ERROR: {self._redact_error_text(exc, token_snapshot=token_snapshot)}"
                )
                logger.warning("network error attempt=%s method=%s error=%s", attempt, method, last_error)

            except RuntimeError:
                raise

            except Exception as exc:
                last_error = (
                    f"UNKNOWN_ERROR: {self._redact_error_text(exc, token_snapshot=token_snapshot)}"
                )
                logger.warning("unknown error attempt=%s method=%s error=%s", attempt, method, last_error)

        err = RuntimeError(f"REQUEST_FAILED: {last_error}")
        self._api_breaker.record_failure(err)
        failure_status = "DEGRADED" if (last_error or "").startswith(("TIMEOUT", "HTTP_5", "NETWORK_ERROR")) else "UNAVAILABLE"
        self._record_io(operation=method, started_at=started_at, status=failure_status, error=str(last_error or "REQUEST_FAILED"))
        raise err

    # =========================================================
    # LOGIN / LOGOUT
    # =========================================================
    def login(self, password: str) -> Dict[str, Any]:
        started_at = time.monotonic()
        # Snapshot del token vivo a inizio login: la redazione deve coprire
        # anche un token ruotato/azzerato da un altro thread prima della
        # gestione dell'eccezione (stessa difesa di _post_jsonrpc).
        token_snapshot = self._session_token_value()
        try:
            response = self.session.post(
                self.IDENTITY_URL,
                headers={
                    "X-Application": self.app_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"username": self.username, "password": password},
                cert=self._cert_tuple(),
                timeout=self.timeout,
            )

            response.raise_for_status()

            data = self._parse_json(response, "INVALID_LOGIN_JSON")

            if str(data.get("loginStatus")) != "SUCCESS":
                # Solo il loginStatus diagnostico: la risposta grezza puo'
                # contenere un sessionToken e finirebbe in log/last_error.
                raise RuntimeError(f"LOGIN_FAILED: {data.get('loginStatus')}")

            session_token = str(data.get("sessionToken") or "")
            session_expiry = str(data.get("sessionExpiryTime") or "")
            connected = bool(session_token)
            self._set_session_state(
                session_token=session_token,
                session_expiry=session_expiry,
                connected=connected,
            )
            elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
            status = "SLOW" if elapsed_ms >= max(2000.0, self.timeout * 1000.0 * 0.8) else "SUCCESS"
            self._record_io(operation="login", started_at=started_at, status=status)

            return {
                "connected": connected,
                "session_token": bool(session_token),
                "expiry": session_expiry,
            }

        except Timeout:
            self._record_io(operation="login", started_at=started_at, status="DEGRADED", error="LOGIN_TIMEOUT")
            # from None: come per gli altri handler, la causa originale
            # potrebbe contenere il token e finire nel traceback renderizzato.
            raise RuntimeError("LOGIN_TIMEOUT") from None

        except HTTPError as exc:
            err = self._redact_error_text(exc, token_snapshot=token_snapshot)
            self._record_io(operation="login", started_at=started_at, status="DEGRADED", error=f"LOGIN_HTTP_ERROR:{err}")
            # from None: la causa originale conterrebbe il token grezzo e
            # logger.exception/traceback la renderizzerebbero.
            raise RuntimeError(f"LOGIN_HTTP_ERROR: {err}") from None

        except RequestException as exc:
            err = self._redact_error_text(exc, token_snapshot=token_snapshot)
            self._record_io(operation="login", started_at=started_at, status="DEGRADED", error=f"LOGIN_NETWORK_ERROR:{err}")
            raise RuntimeError(f"LOGIN_NETWORK_ERROR: {err}") from None

    def logout(self) -> Dict[str, Any]:
        self._clear_session_state()
        return {
            "ok": True,
            "logged_out": True,
        }

    # =========================================================
    # ACCOUNT
    # =========================================================
    def get_account_funds(self) -> Dict[str, Any]:
        result = self._post_jsonrpc(
            self.ACCOUNT_URL,
            "AccountAPING/v1.0/getAccountFunds",
            {},
        )

        if not isinstance(result, dict):
            raise RuntimeError("INVALID_ACCOUNT_FUNDS")

        return {
            "available": self._safe_float(result.get("availableToBetBalance"), 0.0),
            "exposure": self._safe_float(result.get("exposure"), 0.0),
            "retained_commission": self._safe_float(result.get("retainedCommission"), 0.0),
            "exposure_limit": self._safe_float(result.get("exposureLimit"), 0.0),
            "discount_rate": self._safe_float(result.get("discountRate"), 0.0),
            "points_balance": self._safe_float(result.get("pointsBalance"), 0.0),
        }

    def keep_alive(self) -> Dict[str, Any]:
        """Estende la sessione betting via l'endpoint Betfair keepAlive.

        Per i doc Betfair (Login & Session Management) SOLO l'operazione
        keepAlive resetta il timeout della sessione (Italian exchange ~20 min);
        una normale API (es. getAccountFunds) NON estende il timer. Non modifica
        ordini ne' stato del conto. Propaga SESSION_EXPIRED / errori di rete/HTTP
        al chiamante: il loop di keepalive in BetfairService li intercetta e
        instrada un session-error al re-auth fail-closed (handle_session_expiry).
        """
        started_at = time.monotonic()
        token_snapshot = self._session_token_value()
        if not token_snapshot:
            raise RuntimeError("SESSION_EXPIRED")
        try:
            response = self.session.post(
                self.KEEPALIVE_URL,
                headers={
                    "X-Application": self.app_key,
                    "X-Authentication": token_snapshot,
                    "Accept": "application/json",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = self._parse_json(response, "INVALID_KEEPALIVE_JSON")
            if str(data.get("status")) != "SUCCESS":
                # Solo il campo error diagnostico (un codice tipo
                # INVALID_SESSION_INFORMATION), mai la risposta grezza che puo'
                # contenere un token. Il classifier del loop riconosce i codici
                # di sessione e instrada al re-auth.
                error = str(data.get("error") or data.get("status") or "KEEPALIVE_FAILED")
                self._record_io(operation="keep_alive", started_at=started_at, status="DEGRADED", error=f"KEEPALIVE_FAILED:{error}")
                raise RuntimeError(f"KEEPALIVE_FAILED: {error}")
            elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
            status = "SLOW" if elapsed_ms >= max(2000.0, self.timeout * 1000.0 * 0.8) else "SUCCESS"
            self._record_io(operation="keep_alive", started_at=started_at, status=status)
            return {"ok": True, "kept_alive": True}

        except Timeout:
            self._record_io(operation="keep_alive", started_at=started_at, status="DEGRADED", error="KEEPALIVE_TIMEOUT")
            raise RuntimeError("KEEPALIVE_TIMEOUT") from None

        except HTTPError as exc:
            err = self._redact_error_text(exc, token_snapshot=token_snapshot)
            self._record_io(operation="keep_alive", started_at=started_at, status="DEGRADED", error=f"KEEPALIVE_HTTP_ERROR:{err}")
            raise RuntimeError(f"KEEPALIVE_HTTP_ERROR: {err}") from None

        except RequestException as exc:
            err = self._redact_error_text(exc, token_snapshot=token_snapshot)
            self._record_io(operation="keep_alive", started_at=started_at, status="DEGRADED", error=f"KEEPALIVE_NETWORK_ERROR:{err}")
            raise RuntimeError(f"KEEPALIVE_NETWORK_ERROR: {err}") from None

    # =========================================================
    # CASHOUT
    # =========================================================
    def calculate_cashout(
        self,
        original_stake: Any,
        original_odds: Any,
        current_odds: Any,
        side: str = "BACK",
    ) -> Dict[str, Any]:
        original_stake_f = self._safe_float(original_stake, 0.0)
        original_odds_f = self._safe_float(original_odds, 0.0)
        current_odds_f = self._safe_float(current_odds, 0.0)
        safe_side = self._safe_side(side)

        default_side = "LAY" if safe_side == "BACK" else "BACK"

        if original_stake_f <= 0.0 or original_odds_f <= 1.0 or current_odds_f <= 1.0:
            return {
                "cashout_stake": 0.0,
                "profit_if_win": 0.0,
                "profit_if_lose": 0.0,
                "side_to_place": default_side,
            }

        cashout = round((original_stake_f * original_odds_f) / current_odds_f, 2)

        if safe_side == "BACK":
            profit_if_win = (
                original_stake_f * (original_odds_f - 1.0)
                - cashout * (current_odds_f - 1.0)
            )
            profit_if_lose = cashout - original_stake_f
            side_to_place = "LAY"
        else:
            profit_if_win = (
                cashout * (current_odds_f - 1.0)
                - original_stake_f * (original_odds_f - 1.0)
            )
            profit_if_lose = original_stake_f - cashout
            side_to_place = "BACK"

        return {
            "cashout_stake": cashout,
            "profit_if_win": round(profit_if_win, 2),
            "profit_if_lose": round(profit_if_lose, 2),
            "side_to_place": side_to_place,
        }

    # =========================================================
    # MARKET BOOK
    # =========================================================
    def get_market_book(
        self, market_id: str, *, include_prices: bool = False
    ) -> Optional[Dict[str, Any]]:
        # ``include_prices`` opt-in (default False = invariato per i chiamanti
        # esistenti): con True chiede le ladder EX_BEST_OFFERS, necessarie al
        # best-price DIRECT (B6.2 attivazione). Senza priceProjection Betfair NON
        # popola le ladder e l'estrattore difensivo cadrebbe sempre sul master.
        params: Dict[str, Any] = {"marketIds": [market_id]}
        if include_prices:
            params["priceProjection"] = {"priceData": ["EX_BEST_OFFERS"]}
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketBook",
            params,
        )

        if not result:
            return None

        try:
            book = result[0]
        except Exception:
            return None

        runners = book.get("runners") or []
        for runner in runners:
            ex = runner.get("ex") or {}
            runner["availableToBack"] = ex.get("availableToBack") or []
            runner["availableToLay"] = ex.get("availableToLay") or []

        return book

    # =========================================================
    # CATALOGO (discovery eventi / mercati)
    # =========================================================
    def list_events(
        self,
        event_type_ids: List[str],
        *,
        in_play_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Eventi per i tipi di sport richiesti (Betfair SportsAPING/listEvents).

        Ritorna la lista grezza Betfair: ogni elemento e' `{"event": {...},
        "marketCount": N}`. Riusa `_post_jsonrpc` (auth/breaker/retry gia' gestiti).
        NB: `listEvents` NON accetta `maxResults` (ritorna tutti gli eventi che
        matchano il filter); inviarlo puo' causare APINGException in LIVE.
        """
        filter_: Dict[str, Any] = {"eventTypeIds": [str(e) for e in event_type_ids]}
        if in_play_only:
            filter_["inPlayOnly"] = True
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listEvents",
            {"filter": filter_},
        )
        return result if isinstance(result, list) else []

    def list_market_catalogue(
        self,
        event_type_ids: List[str],
        event_ids: Optional[List[str]] = None,
        *,
        market_type_codes: Optional[List[str]] = None,
        max_results: int = 200,
    ) -> List[Dict[str, Any]]:
        """Catalogo mercati (Betfair SportsAPING/listMarketCatalogue) con runner,
        competition e orario. `market_type_codes` filtra per tipo mercato
        (es. ["MATCH_ODDS","CORRECT_SCORE"]) via `marketTypeCodes` nel filter.

        NB peso dati: Betfair accetta `maxResults` 1-1000, ma applica un limite
        di dati pesati (Σ peso projection × N mercati ≤ 200 punti; pesano solo
        `MARKET_DESCRIPTION`/`RUNNER_METADATA`). Qui la projection include
        `MARKET_DESCRIPTION` (peso 1), quindi un `maxResults` alto (es. 1000)
        genera APINGException TOO_MUCH_DATA in LIVE. 200 è un cap prudente: il
        chiamante deve paginare/chunkare gli eventi per stare nel limite.

        Su risposta MALFORMATA (non-lista) solleva `RuntimeError` invece di
        degradare a `[]`: un catalogo mercati "finto vuoto" da errore upstream
        farebbe girare il cleanup e cancellerebbe mercati/runner validi
        (fail-open money-path). Una lista vuota GENUINA (nessun mercato per il
        filtro) viene restituita normalmente.
        """
        filter_: Dict[str, Any] = {"eventTypeIds": [str(e) for e in event_type_ids]}
        if event_ids:
            filter_["eventIds"] = [str(e) for e in event_ids]
        if market_type_codes:
            filter_["marketTypeCodes"] = [str(m) for m in market_type_codes]
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketCatalogue",
            {
                "filter": filter_,
                "marketProjection": [
                    "EVENT",
                    "COMPETITION",
                    "MARKET_START_TIME",
                    "RUNNER_DESCRIPTION",
                    "MARKET_DESCRIPTION",
                ],
                "sort": "FIRST_TO_START",
                "maxResults": int(max_results),
            },
        )
        if not isinstance(result, list):
            raise RuntimeError(
                "listMarketCatalogue: risposta non-lista (%s) -> possibile errore "
                "upstream; non degrado a [] per non innescare un cleanup distruttivo."
                % type(result).__name__
            )
        return result

    # =========================================================
    # ORDERS
    # =========================================================
    @staticmethod
    def _normalize_customer_ref(customer_ref: Any) -> str:
        """Ref Betfair-valido da inviare come `customerRef`, o "" se non conforme.

        Fail-closed SUL REF (mai sull'ordine): un `customer_ref` non conforme ai
        vincoli Betfair (<=32, charset `[A-Za-z0-9-._+*:;~]`) viene OMESSO — mai
        troncato/ripulito (eviterebbe un falso-dedup) e mai inviato grezzo
        (Betfair rifiuterebbe l'INTERA placeOrders). Il percorso ordini reale usa
        `uuid4().hex` (32 hex) che passa invariato; i ref liberi da altri percorsi
        (es. cashout `source`) degradano in sicurezza al comportamento legacy
        (nessun customerRef) senza bloccare il piazzamento.
        """
        ref = str(customer_ref or "")
        return ref if _CUSTOMER_REF_RE.fullmatch(ref) else ""

    def place_bet(
        self,
        *,
        market_id: Any,
        selection_id: Any,
        side: Any,
        price: Any,
        size: Any,
        customer_ref: Any = "",
    ) -> Dict[str, Any]:
        market_id_s = str(market_id or "").strip()
        if not market_id_s:
            raise RuntimeError("INVALID_MARKET_ID")

        try:
            selection_id_i = int(selection_id)
        except Exception as exc:
            raise RuntimeError("INVALID_SELECTION_ID") from exc
        if selection_id_i <= 0:
            raise RuntimeError("INVALID_SELECTION_ID")

        try:
            price_f = float(price)
        except Exception as exc:
            raise RuntimeError("INVALID_PRICE") from exc
        if price_f <= 1.0:
            raise RuntimeError("INVALID_PRICE")

        try:
            size_f = float(size)
        except Exception as exc:
            raise RuntimeError("INVALID_SIZE") from exc
        if size_f <= 0.0:
            raise RuntimeError("INVALID_SIZE")

        # Il lato si VALIDA come gli altri argomenti, non si converte.
        # `_safe_side` trasforma in BACK qualsiasi valore che non sia BACK/LAY,
        # `None` compreso: un refuso o un lato mancante partivano come BACK, con
        # `ok=True`. DECISIONE-426 P13. `_safe_side` resta per
        # `calculate_cashout`, che la decisione lascia fuori.
        side_s = side.strip().upper() if isinstance(side, str) else ""
        if side_s not in _LATI_VALIDI:
            raise RuntimeError("INVALID_SIDE")

        params: Dict[str, Any] = {
            "marketId": market_id_s,
            "instructions": [{
                "selectionId": selection_id_i,
                "side": side_s,
                "orderType": "LIMIT",
                "limitOrder": {
                    "size": size_f,
                    "price": price_f,
                    "persistenceType": "LAPSE",
                },
            }],
        }
        # customerRef (#PR-C): chiave di de-dup lato Betfair (finestra 60s). Se il
        # ref e' conforme lo inviamo => una ri-sottomissione con lo stesso ref
        # NON piazza una seconda bet reale. Se non conforme/assente, la chiave e'
        # OMESSA e il payload resta identico al legacy (nessun customerRef).
        ref = self._normalize_customer_ref(customer_ref)
        if ref:
            params["customerRef"] = ref

        try:
            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/placeOrders",
                params,
                # placeOrders resta single_shot: un timeout NON prova che l'ordine
                # non sia passato. Con customerRef presente, un eventuale retry a
                # monte verrebbe deduplicato da Betfair (60s); senza, l'esito
                # incerto => order_unknown sotto, risolve la reconciliation.
                single_shot=True,
            )

            status = str(result.get("status") or "").upper()
            reports = result.get("instructionReports") or []

            if status != "SUCCESS":
                # errorCode nel testo => la classificazione order_unknown sotto puo'
                # riconoscere DUPLICATE_TRANSACTION (dedup customerRef, #PR-C/#452):
                # la prima bet e' VIVA, non e' un fallimento definitivo.
                error_code = str(result.get("errorCode") or "").strip()
                raise RuntimeError(f"BET_FAILED: {status} {error_code}".strip())

            if not reports:
                raise RuntimeError("BET_NO_REPORT")

            for report in reports:
                if str(report.get("status") or "").upper() not in {"SUCCESS", "PLACED"}:
                    raise RuntimeError(f"BET_REJECTED: {report}")

            return {
                "ok": True,
                "result": result,
            }

        except RuntimeError as exc:
            error_text = str(exc)
            error_upper = error_text.upper()
            return {
                "ok": False,
                "error": error_text,
                "classification": self._classify_error(error_text),
                # Esito SCONOSCIUTO (=> reconciliation, non fallimento definitivo):
                # - TIMEOUT/NETWORK_ERROR/HTTP_5xx: la richiesta puo' aver raggiunto
                #   Betfair anche se la risposta e' andata persa;
                # - DUPLICATE_TRANSACTION: Betfair ha deduplicato un customerRef gia'
                #   visto (60s) => la PRIMA bet e' VIVA. Marcarla FAILED sarebbe un
                #   falso-negativo su una bet reale: la reconciliation la ritrova.
                # - risposta ARRIVATA ma illeggibile (corpo 2xx non JSON, JSON-RPC
                #   malformato, SUCCESS senza instruction report): il POST e'
                #   partito, e nel terzo caso Betfair ha perfino detto SUCCESS.
                #   Confronto ESATTO sul codice, non per sottostringa: un rifiuto
                #   esplicito (`API_ERROR: ...`) resta definitivo. PR02/#461,
                #   rilievo P1 di Codex sulla #478.
                "order_unknown": any(
                    marker in error_upper
                    for marker in (
                        "TIMEOUT", "NETWORK_ERROR", "HTTP_5", "UNKNOWN_ERROR",
                        "DUPLICATE_TRANSACTION",
                    )
                ) or error_text in _RISPOSTA_ILLEGGIBILE_DOPO_L_INVIO,
            }

    # =========================================================
    # ORDERS – CANCEL
    # =========================================================
    def cancel_orders(
        self,
        *,
        market_id: Any,
        bet_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Cancel orders on a Betfair market via the cancelOrders API.

        When bet_ids is empty or None, cancels ALL unmatched orders on the
        market (the standard emergency-stop / flatten behaviour).
        Returns a result dict; never raises on API-level failure (logs and
        returns ok=False so callers can record the error without crashing).
        """
        market_id_s = str(market_id or "").strip()
        if not market_id_s:
            raise RuntimeError("INVALID_MARKET_ID")

        # Empty instructions list → cancel all active orders on the market.
        # Non-empty → cancel only the listed bet IDs.
        instructions: List[Dict[str, Any]] = (
            [{"betId": str(bid)} for bid in bet_ids if bid]
            if bet_ids else []
        )

        try:
            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/cancelOrders",
                {
                    "marketId": market_id_s,
                    "instructions": instructions,
                },
            )

            status = str(result.get("status") or "").upper()
            reports = result.get("instructionReports") or []

            if status == "FAILURE":
                err = str(result.get("errorCode") or "UNKNOWN")
                raise RuntimeError(f"CANCEL_FAILED: {err}")

            return {
                "ok": True,
                "market_id": market_id_s,
                "status": status or "SUCCESS",
                "cancelled_count": len(reports),
                "result": result,
            }

        except RuntimeError as exc:
            error_text = str(exc)
            return {
                "ok": False,
                "market_id": market_id_s,
                "error": error_text,
                "classification": self._classify_error(error_text),
            }

    # =========================================================
    # ORDERS – REPLACE (change price of an unmatched order)
    # =========================================================
    @staticmethod
    def _validate_replace_params(
        market_id: Any, bet_id: Any, new_price: Any
    ) -> tuple[str, str, float]:
        market_id_s = str(market_id or "").strip()
        if not market_id_s:
            raise RuntimeError("INVALID_MARKET_ID")
        bet_id_s = str(bet_id or "").strip()
        if not bet_id_s:
            raise RuntimeError("INVALID_BET_ID")
        try:
            new_price_f = float(new_price)
        except Exception as exc:
            raise RuntimeError("INVALID_PRICE") from exc
        # Reject NaN/Inf: float() accepts them and `nan <= 1.0` is False, so a
        # non-finite price would otherwise be serialized into the live request.
        if not math.isfinite(new_price_f) or new_price_f <= 1.0:
            raise RuntimeError("INVALID_PRICE")
        return market_id_s, bet_id_s, new_price_f

    @staticmethod
    def _lift_replacement_bet_ids(result: Dict[str, Any]) -> Dict[str, Any]:
        # A replaceOrders report's top-level betId (when present) is the OLD,
        # now-cancelled order; the replacement id is in placeInstructionReport.
        # ALWAYS overwrite the top-level betId with the replacement so the caller
        # tracks the new live order, never the cancelled one.
        for report in result.get("instructionReports") or []:
            if isinstance(report, dict):
                new_bid = (report.get("placeInstructionReport") or {}).get("betId")
                if new_bid:
                    report["betId"] = new_bid
        return result

    def replace_orders(
        self,
        *,
        market_id: Any,
        bet_id: Any,
        new_price: Any,
    ) -> Dict[str, Any]:
        """Replace an unmatched order's price via the replaceOrders API.

        Betfair's replaceOrders cancels the (unmatched) order and re-places it
        at ``new_price``, yielding a NEW bet id (in the report's
        ``placeInstructionReport``). Returns the raw Betfair response with that
        new bet id lifted to the report top level, so the order_manager saga can
        read ``instructionReports[0]['betId']`` directly. API/session/network
        failures PROPAGATE — order_manager wraps the call and maps them to
        REPLACE_REJECTED.
        """
        market_id_s, bet_id_s, new_price_f = self._validate_replace_params(
            market_id, bet_id, new_price
        )
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/replaceOrders",
            {
                "marketId": market_id_s,
                "instructions": [{"betId": bet_id_s, "newPrice": new_price_f}],
            },
            # replaceOrders is NOT idempotent (cancel + re-place) and carries no
            # customerRef: a retry after a timeout could replace twice. Never
            # re-send — an uncertain outcome is for reconciliation, not retry.
            single_shot=True,
        )
        return self._lift_replacement_bet_ids(result)

    # =========================================================
    # ORDERS – CURRENT (ghost-order detection)
    # =========================================================
    # listCurrentOrders is paginated: Betfair caps a single response at
    # CURRENT_ORDERS_PAGE_SIZE records and sets ``moreAvailable=True`` when the
    # result is truncated. We MUST walk every page — a truncated list would let
    # the reconciliation engine treat absent remote orders as reconciled
    # (fail-open ghost detection). The page cap is a runaway guard: exceeding it
    # raises (fail-closed) rather than returning a partial set.
    CURRENT_ORDERS_PAGE_SIZE = 1000
    CURRENT_ORDERS_MAX_PAGES = 20

    def get_current_orders(
        self,
        market_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch ALL current (unmatched/active) orders via listCurrentOrders.

        Walks every page (``moreAvailable``) and returns the concatenated
        ``currentOrders`` list of order dicts, filtered to ``market_ids`` when
        provided. Used by the reconciliation engine to detect ghost orders
        (B3 / UFA-005), so the contract is fail-closed: any API/session/network
        failure PROPAGATES (no silent empty list), a truncated response is
        never silently returned (pagination), and an unterminated pagination
        (cap exceeded) RAISES rather than returning a partial set — a fetch
        problem must never be mistaken for "no remote orders".
        """
        base_params: Dict[str, Any] = {}
        wanted = [str(m).strip() for m in (market_ids or []) if str(m).strip()]
        if wanted:
            base_params["marketIds"] = wanted

        all_orders: List[Dict[str, Any]] = []
        from_record = 0
        for _page in range(self.CURRENT_ORDERS_MAX_PAGES):
            params = dict(base_params)
            params["fromRecord"] = from_record
            params["recordCount"] = self.CURRENT_ORDERS_PAGE_SIZE

            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/listCurrentOrders",
                params,
            )

            page_orders = result.get("currentOrders") or []
            all_orders.extend(page_orders)

            # No more records → complete snapshot, return it.
            if not result.get("moreAvailable"):
                return all_orders

            # moreAvailable but an empty/missing page is an inconsistent or
            # stale paginated snapshot: fail closed instead of returning a
            # partial set the ghost detector would treat as the complete remote
            # state (and to avoid a non-advancing loop).
            if not page_orders:
                raise RuntimeError(
                    "CURRENT_ORDERS_TRUNCATED: moreAvailable with empty page"
                )

            from_record += len(page_orders)

        # Cap exceeded with moreAvailable still set: fail closed.
        raise RuntimeError("CURRENT_ORDERS_TRUNCATED: pagination cap exceeded")

    # listClearedOrders shares the same exchange-side page limit as
    # listCurrentOrders (1000 records per page); the cap mirrors
    # CURRENT_ORDERS_MAX_PAGES for the same reason (bounded, fail-closed).
    CLEARED_ORDERS_PAGE_SIZE = 1000
    CLEARED_ORDERS_MAX_PAGES = 20

    _CLEARED_BET_STATUSES = frozenset(
        {"SETTLED", "VOIDED", "LAPSED", "CANCELLED"}
    )
    # Enum GroupBy NORMATIVO (Betting Enums): niente "EXCHANGE" — compare solo
    # nella pagina roll-up come retaggio dell'era doppio-exchange; fuori
    # dall'enum si sta fail-closed (verifica del 2026-08-23 sui docs Betfair).
    _CLEARED_GROUP_BY = frozenset(
        {"EVENT_TYPE", "EVENT", "MARKET", "SIDE", "BET"}
    )
    # Limite documentato listClearedOrders: max 1000 betId per richiesta.
    _CLEARED_MAX_BET_IDS = 1000

    @staticmethod
    def _cleared_id_list(raw: Any, name: str) -> List[str]:
        """Filtro id fail-closed: solo una LISTA di stringhe e' un filtro.

        `str(x)` indiscriminato trasformava una stringa passata al posto della
        lista nei suoi caratteri e un `None` nell'id "None": filtri spazzatura
        che l'exchange non matcha, cioe' il report vuoto che sembra «nessun
        settlement». Qui: container str/bytes o elemento non-str => raise.
        """
        if raw is None:
            return []
        if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple, set, frozenset)):
            raise RuntimeError(f"INVALID_{name}: expected list of str, got {type(raw).__name__}")
        cleaned: List[str] = []
        for item in raw:
            if not isinstance(item, str):
                raise RuntimeError(f"INVALID_{name}: non-str element {item!r}")
            value = item.strip()
            if value:
                cleaned.append(value)
        return cleaned

    @staticmethod
    def _cleared_range_bound(raw: Any, key: str) -> "tuple[str, datetime]":
        """Estremo di settledDateRange: ISO-8601 parsabile, mai ignorato.

        Fornito ma vuoto/non-str/non-ISO => raise: un estremo droppato in
        silenzio allargherebbe la finestra di settlement del daily-loss.
        Ritorna (valore_wire, datetime_parsato); i naive sono ancorati a UTC
        cosi' i due estremi restano sempre confrontabili tra loro.
        """
        value = raw.strip() if isinstance(raw, str) else ""
        if not value:
            raise RuntimeError(f"INVALID_SETTLED_RANGE: {key}={raw!r}")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise RuntimeError(f"INVALID_SETTLED_RANGE: {key} not ISO-8601: {raw!r}")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return value, parsed

    def list_cleared_orders(
        self,
        *,
        bet_status: str = "SETTLED",
        market_ids: Optional[List[str]] = None,
        bet_ids: Optional[List[str]] = None,
        settled_after: Optional[str] = None,
        settled_before: Optional[str] = None,
        group_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch cleared (settled/voided/lapsed/cancelled) orders via
        listClearedOrders — la sorgente del SETTLEMENT reale.

        Questo e' il dato da cui il ciclo di chiusura posizioni ricava il PnL
        realizzato (con ``group_by="MARKET"`` il report porta profit e
        commission per mercato, coerente con l'aggregazione market-net del
        motore PnL). Contratto fail-closed, identico a ``get_current_orders``:

        - qualsiasi errore API/sessione/rete PROPAGA (mai una lista vuota
          silenziosa: un fetch fallito che sembra «niente da regolare»
          lascerebbe il daily-loss cieco su perdite gia' avvenute);
        - la paginazione (``moreAvailable``) viene percorsa per intero; una
          pagina vuota con ``moreAvailable`` o il cap superato SOLLEVANO
          invece di restituire un report parziale;
        - ``bet_status``/``group_by`` fuori dall'enum Betfair SOLLEVANO senza
          toccare la rete: un filtro sbagliato non deve degradare in un
          report vuoto che sembra «nessun settlement». Stessa sorte per ogni
          combinazione/forma che i docs Betfair documentano come
          report-vuoto-per-contratto: ``group_by`` con status non-SETTLED,
          ``market_ids``/``bet_ids`` non lista-di-stringhe, piu' di 1000
          ``bet_ids``, range settled invertito (``from`` dopo ``to``);
        - una risposta senza i campi obbligatori del report
          (``clearedOrders``/``moreAvailable``) SOLLEVA
          ``CLEARED_ORDERS_MALFORMED_RESPONSE`` invece di degradare in
          lista vuota.

        ``settled_after``/``settled_before`` (ISO-8601, es.
        ``2026-08-23T00:00:00Z``) delimitano ``settledDateRange``; forniti ma
        vuoti/non stringa/non parsabili ISO => errore, mai ignorati in
        silenzio. (Contratto irrobustito dal giro di verifica avversariale
        pre-PR del 2026-08-23: ogni ramo qui sopra nasce da un rilievo con
        mutazione dimostrata.)
        """
        status = str(bet_status or "").strip().upper()
        if status not in self._CLEARED_BET_STATUSES:
            raise RuntimeError(f"INVALID_BET_STATUS: {bet_status!r}")

        base_params: Dict[str, Any] = {"betStatus": status}

        wanted_markets = self._cleared_id_list(market_ids, "MARKET_IDS")
        if wanted_markets:
            base_params["marketIds"] = wanted_markets
        wanted_bets = self._cleared_id_list(bet_ids, "BET_IDS")
        if len(wanted_bets) > self._CLEARED_MAX_BET_IDS:
            # Limite documentato dell'API: oltre 1000 betId la richiesta e'
            # invalida — si rifiuta qui, senza toccare la rete.
            raise RuntimeError(f"TOO_MANY_BET_IDS: {len(wanted_bets)}")
        if wanted_bets:
            base_params["betIds"] = wanted_bets

        date_range: Dict[str, str] = {}
        parsed_bounds: Dict[str, datetime] = {}
        for key, raw in (("from", settled_after), ("to", settled_before)):
            if raw is None:
                continue
            value, parsed = self._cleared_range_bound(raw, key)
            date_range[key] = value
            parsed_bounds[key] = parsed
        if len(parsed_bounds) == 2 and parsed_bounds["from"] > parsed_bounds["to"]:
            # Betfair documenta che from > to restituisce ZERO risultati: un
            # range invertito diventerebbe un report vuoto che sembra
            # «nessun settlement». Fail-closed qui.
            raise RuntimeError(
                f"INVALID_SETTLED_RANGE: from {date_range['from']!r} "
                f"is after to {date_range['to']!r}"
            )
        if date_range:
            base_params["settledDateRange"] = date_range

        if group_by is not None:
            grouping = str(group_by or "").strip().upper()
            if grouping not in self._CLEARED_GROUP_BY:
                raise RuntimeError(f"INVALID_GROUP_BY: {group_by!r}")
            if status != "SETTLED":
                # Doc Betfair: groupBy «is only applicable to SETTLED
                # BetStatus» — con LAPSED/CANCELLED e rollup non-BET il report
                # torna vuoto per contratto. Un report vuoto da filtro
                # incompatibile e' indistinguibile da «nessun settlement»:
                # si rifiuta la combinazione, senza toccare la rete.
                raise RuntimeError(
                    f"INVALID_GROUP_BY_FOR_STATUS: groupBy={grouping} "
                    f"requires betStatus=SETTLED, got {status}"
                )
            base_params["groupBy"] = grouping

        all_cleared: List[Dict[str, Any]] = []
        from_record = 0
        for _page in range(self.CLEARED_ORDERS_MAX_PAGES):
            params = dict(base_params)
            params["fromRecord"] = from_record
            params["recordCount"] = self.CLEARED_ORDERS_PAGE_SIZE

            result = self._post_jsonrpc(
                self.BETTING_URL,
                "SportsAPING/v1.0/listClearedOrders",
                params,
            )

            # ClearedOrderSummaryReport ha ENTRAMBI i campi obbligatori, DEI
            # TIPI GIUSTI: un envelope senza `result` degrada in {} dentro
            # _post_jsonrpc (lista vuota con breaker success — il fetch
            # fallito invisibile), e un `clearedOrders` non-lista finirebbe
            # in extend() che itera contenuto arbitrario verso il PnL
            # (rilievo Fable sulla #439). isinstance copre anche l'assenza:
            # chiave mancante => None => non e' il tipo => raise.
            raw_orders = result.get("clearedOrders") if isinstance(result, dict) else None
            more_available = result.get("moreAvailable") if isinstance(result, dict) else None
            if not isinstance(raw_orders, list) or not isinstance(more_available, bool):
                raise RuntimeError(
                    "CLEARED_ORDERS_MALFORMED_RESPONSE: clearedOrders/"
                    "moreAvailable missing or wrong type"
                )

            page_orders = raw_orders
            all_cleared.extend(page_orders)

            if not more_available:
                return all_cleared

            # moreAvailable con pagina vuota/mancante = snapshot paginato
            # incoerente: fail-closed, mai un report parziale spacciato per
            # completo (e niente loop che non avanza).
            if not page_orders:
                raise RuntimeError(
                    "CLEARED_ORDERS_TRUNCATED: moreAvailable with empty page"
                )

            from_record += len(page_orders)

        raise RuntimeError("CLEARED_ORDERS_TRUNCATED: pagination cap exceeded")

    def cancel_order(
        self,
        *,
        bet_id: Any,
        market_id: Any = None,
    ) -> Dict[str, Any]:
        """Cancel a single order by bet id (ghost-order cancellation shim).

        The reconciliation engine cancels detected live ghosts one bet id at a
        time (``_cancel_ghost_orders`` calls ``cancel_order(bet_id=...)``), but
        the Betfair cancelOrders RPC needs the order's market id. When
        ``market_id`` is not supplied we resolve it from the current orders; if
        the bet is no longer a current order there is nothing on the exchange to
        cancel (no-op). Delegates the actual cancel to ``cancel_orders``.
        """
        bid = str(bet_id or "").strip()
        if not bid:
            raise RuntimeError("INVALID_BET_ID")

        market_id_s = str(market_id or "").strip()
        if not market_id_s:
            for order in self.get_current_orders():
                row_bid = str(
                    order.get("betId") or order.get("bet_id") or ""
                ).strip()
                if row_bid == bid:
                    market_id_s = str(
                        order.get("marketId") or order.get("market_id") or ""
                    ).strip()
                    break

        if not market_id_s:
            # Bet is not among the current orders → nothing to cancel.
            return {
                "ok": True,
                "bet_id": bid,
                "status": "NOT_CURRENT",
                "cancelled_count": 0,
            }

        result = self.cancel_orders(market_id=market_id_s, bet_ids=[bid])
        # cancel_orders converts API/session failures into ok=False (it does not
        # raise). The reconciliation ghost-cancel path only reacts to
        # exceptions, so surface a failed cancel as a raise — otherwise a ghost
        # that is still live on the exchange would be logged as cancelled
        # (fail-open).
        if isinstance(result, dict) and not result.get("ok"):
            raise RuntimeError(
                f"CANCEL_ORDER_FAILED: {result.get('error') or 'UNKNOWN'}"
            )

        # cancel_orders only flags a top-level FAILURE as ok=False; an ok=True
        # envelope can still hide PROCESSED_WITH_ERRORS/TIMEOUT or a
        # per-instruction FAILURE/TIMEOUT, i.e. the exchange did NOT confirm the
        # cancellation. For a single-bet ghost cancel, require explicit
        # confirmation (fail-closed) so a still-live ghost is never recorded as
        # cancelled.
        raw = result.get("result") if isinstance(result, dict) else None
        reports = (raw or {}).get("instructionReports") or []
        report_statuses = {
            str(r.get("status") or "").upper()
            for r in reports
            if isinstance(r, dict)
        }
        top_status = str((result or {}).get("status") or "").upper()
        if top_status != "SUCCESS" or report_statuses != {"SUCCESS"}:
            raise RuntimeError(
                "CANCEL_ORDER_UNCONFIRMED: "
                f"status={top_status or 'UNKNOWN'} "
                f"reports={sorted(report_statuses) or []}"
            )
        return result


    # =========================================================
    # STATUS
    # =========================================================
    def status(self) -> Dict[str, Any]:
        with self._session_state_lock:
            session_token = self.session_token
            session_expiry = self.session_expiry
        return {
            "connected": bool(session_token),
            "expiry": session_expiry,
        }
