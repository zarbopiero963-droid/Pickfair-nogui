"""Il proxy di `BetfairClient`: percorso configurabile e fallimenti non silenziosi.

Primo passo di R3 (#374). Prima di questa modifica il proxy veniva cercato a
``/home/ubuntu/Pickfair-nogui/config.json``, un percorso assoluto scritto nel
codice che punta a un VPS dismesso. Effetto misurato: ``os.path.exists`` sempre
``False``, quindi **il proxy non veniva mai applicato** — e l'unico
``except Exception`` registrava e proseguiva, cosi' nessuno poteva accorgersene.

Questi test fissano le tre cose che erano rotte: dove si cerca la
configurazione, come si costruisce l'URL, e cosa succede quando non si puo'.
"""

from __future__ import annotations

import json
from urllib.parse import urlparse

import pytest

import betfair_client as bc

#: La funzione vera, presa PRIMA che la fixture autouse la sostituisca: serve
#: al test che verifica che il controllo guardi davvero PySocks.
_SUPPORTO_SOCKS_ORIGINALE = bc._supporto_socks_disponibile


def _client(**kwargs):
    """Client minimo: il costruttore non fa rete, solo assegnazioni."""
    return bc.BetfairClient(username="u", app_key="k", cert_pem="c", key_pem="p", **kwargs)


@pytest.fixture(autouse=True)
def _socks_disponibile(monkeypatch):
    """PySocks presente, per default, in tutti i test di questo file.

    Serve a tenere separate due cose: cosa fa il modulo con una configurazione
    di proxy, e cosa fa quando la dipendenza SOCKS manca. Senza questo fissaggio
    ogni test con `type: socks5` misurerebbe l'ambiente della macchina invece
    del codice. I test che vogliono davvero l'assenza la dichiarano.
    """
    monkeypatch.setattr(bc, "_supporto_socks_disponibile", lambda: True)


class TestCostruzioneUrl:
    def test_non_richiesto_non_produce_url(self):
        assert bc.costruisci_proxy_url({"enabled": False, "host": "h", "port": 1}) is None
        assert bc.costruisci_proxy_url({}) is None
        assert bc.costruisci_proxy_url(None) is None

    def test_con_credenziali(self):
        url = bc.costruisci_proxy_url(
            {"enabled": True, "type": "socks5", "host": "h.example",
             "port": 1080, "username": "u", "password": "p"})
        assert url == "socks5://u:p@h.example:1080"

    def test_senza_credenziali_non_scrive_None_nell_url(self):
        """La regressione vera: prima le credenziali erano interpolate sempre.

        Un proxy senza utente produceva `socks5://None:None@host:porta`, che
        somiglia a un URL valido e non lo e'. Il traffico sarebbe uscito verso
        un proxy inesistente invece che verso quello configurato.
        """
        url = bc.costruisci_proxy_url({"enabled": True, "host": "h.example", "port": 1080})
        assert url == "socks5://h.example:1080"
        assert "None" not in url

    def test_tipo_predefinito_socks5(self):
        url = bc.costruisci_proxy_url({"enabled": True, "host": "h", "port": 1, "type": ""})
        assert url.startswith("socks5://")

    @pytest.mark.parametrize("cfg", [
        {"enabled": True, "port": 1080},                 # host mancante
        {"enabled": True, "host": "h.example"},          # porta mancante
        {"enabled": True, "host": "   ", "port": 1080},  # host vuoto
    ])
    def test_richiesto_ma_incompleto_solleva(self, cfg):
        """Non silenzio: un proxy chiesto e non applicabile ferma l'avvio.

        Il traffico verso Betfair uscirebbe dall'indirizzo sbagliato, che e'
        esattamente cio' che il proxy esiste per evitare."""
        with pytest.raises(ValueError):
            bc.costruisci_proxy_url(cfg)


class TestPercorsiCandidati:
    def test_nessun_percorso_assoluto_del_vps(self):
        """Il difetto che questa PR chiude: nessun candidato deve essere il
        percorso di una macchina specifica."""
        assert all("/home/ubuntu" not in p for p in bc.percorsi_config_candidati())

    def test_l_ambiente_ha_la_precedenza(self, monkeypatch):
        monkeypatch.setenv(bc.ENV_PERCORSO_CONFIG, "/tmp/scelto-da-me.json")
        assert bc.percorsi_config_candidati()[0] == "/tmp/scelto-da-me.json"

    def test_senza_ambiente_resta_almeno_un_candidato(self, monkeypatch):
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        assert bc.percorsi_config_candidati()


class TestClientApplicaIlProxy:
    def test_configurazione_iniettata_viene_applicata(self):
        c = _client(proxy_config={"enabled": True, "type": "socks5",
                                  "host": "h.example", "port": 1080,
                                  "username": "u", "password": "p"})
        assert c.session.proxies == {"http": "socks5://u:p@h.example:1080",
                                     "https": "socks5://u:p@h.example:1080"}

    def test_disabilitato_non_tocca_la_sessione(self):
        c = _client(proxy_config={"enabled": False, "host": "h", "port": 1})
        assert not c.session.proxies

    def test_richiesto_ma_rotto_impedisce_la_costruzione(self):
        with pytest.raises(ValueError):
            _client(proxy_config={"enabled": True, "host": "", "port": 1080})

    def test_legge_dal_percorso_indicato_dall_ambiente(self, tmp_path, monkeypatch):
        f = tmp_path / "config.json"
        f.write_text(json.dumps({"proxy": {"enabled": True, "type": "http",
                                           "host": "p.example", "port": 3128}}),
                     encoding="utf-8")
        monkeypatch.setenv(bc.ENV_PERCORSO_CONFIG, str(f))
        c = _client()
        assert c.session.proxies["https"] == "http://p.example:3128"

    def test_percorso_dichiarato_ma_assente_ferma_l_avvio(self, monkeypatch):
        """Secondo rilievo di GPT-5.6 Sol su #430, accolto — e di nuovo il mio
        test consolidava il bypass.

        Al giro precedente avevo tracciato la linea fra "dichiarato ma
        illeggibile" (eccezione) e "dichiarato ma assente" (si prosegue). E'
        una linea incoerente: in entrambi i casi l'operatore ha detto dove sta
        il file e il file non e' utilizzabile. Vale anche per un symlink rotto,
        che `os.path.exists` segnala come assente."""
        monkeypatch.setenv(bc.ENV_PERCORSO_CONFIG, "/percorso/che/non/esiste.json")
        with pytest.raises(ValueError):
            _client()

    def test_nessuna_configurazione_non_e_un_errore(self, monkeypatch, tmp_path):
        """L'altro lato: senza percorso DICHIARATO, non trovare configurazione
        significa "nessun proxy", non un guasto. E' il caso di chi non usa
        proxy affatto, che deve continuare a partire."""
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        monkeypatch.setattr(bc, "percorsi_config_candidati",
                            lambda: [str(tmp_path / "niente.json")])
        assert not _client().session.proxies

    def test_candidato_falsy_senza_ambiente_non_solleva(self, monkeypatch):
        """Regressione che ho introdotto io al giro precedente, trovata da
        GPT-5.6 Sol e Claude Fable 5 **indipendentemente**.

        La guardia era `if percorso == esplicito`. Senza variabile d'ambiente
        `esplicito` e' `None`, quindi un candidato falsy — caso che il codice
        stesso prevede con `if not percorso` — rendeva vero `None == None` e
        sollevava all'avvio **senza che nessuno avesse dichiarato niente**."""
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        monkeypatch.setattr(bc, "percorsi_config_candidati", lambda: [None, ""])
        assert not _client().session.proxies

    def test_blocco_proxy_malformato_ferma_l_avvio(self, tmp_path, monkeypatch):
        """Rilievo di Claude Fable 5 e GPT-5.6 Sol: al giro precedente avevo
        scelto un warning, e il test cristallizzava il fail-open.

        Un blocco `proxy` che non e' un oggetto significa che qualcuno un proxy
        lo voleva. Proseguire in chiaro decide al posto suo — ed e' il non
        sapere se `enabled` fosse vero a rendere la decisione inaccettabile,
        non il contrario."""
        f = tmp_path / "config.json"
        f.write_text(json.dumps({"proxy": "socks5://scritto-male"}), encoding="utf-8")
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        monkeypatch.setattr(bc, "percorsi_config_candidati", lambda: [str(f)])
        with pytest.raises(ValueError, match="malformato"):
            _client()

    def test_il_primo_file_leggibile_vince_anche_senza_proxy(self, tmp_path, monkeypatch):
        """Rilievo di OpenRouter Fugu Ultra su #430: precedenza pericolosa.

        Prima, se il file a precedenza piu' alta non conteneva la chiave
        `proxy`, la ricerca proseguiva e applicava il proxy di un file a
        precedenza PIU' BASSA — magari vecchio, magari scrivibile da altri. Il
        traffico Betfair sarebbe uscito da un proxy che nessuno aveva scelto.

        Un `config.json` senza blocco `proxy` significa "nessun proxy", non
        "guarda altrove"."""
        alto = tmp_path / "alto.json"
        alto.write_text(json.dumps({"betfair": {"username": "x"}}), encoding="utf-8")
        basso = tmp_path / "basso.json"
        basso.write_text(json.dumps({"proxy": {"enabled": True, "host": "inatteso.example",
                                               "port": 9999}}), encoding="utf-8")
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        monkeypatch.setattr(bc, "percorsi_config_candidati",
                            lambda: [str(alto), str(basso)])
        assert not _client().session.proxies, (
            "ha applicato il proxy di un file a precedenza piu' bassa"
        )

    def test_percorso_dichiarato_ma_illeggibile_ferma_l_avvio(self, tmp_path, monkeypatch):
        """Rilievo di GPT-5.6 Sol su #430, accolto: prima questo test asseriva
        il contrario e **consolidava il bypass**.

        Chi valorizza `PICKFAIR_CONFIG_PATH` dichiara dove sta il file. Se e'
        illeggibile non sappiamo se voleva un proxy: partire in chiaro sarebbe
        indovinare sul percorso dei soldi."""
        f = tmp_path / "config.json"
        f.write_text("{ questo non e' json", encoding="utf-8")
        monkeypatch.setenv(bc.ENV_PERCORSO_CONFIG, str(f))
        with pytest.raises(ValueError):
            _client()

    def test_candidato_non_dichiarato_illeggibile_si_supera(self, tmp_path, monkeypatch):
        """La distinzione che regge i casi sopra: un file trovato *cercando*,
        e non *dichiarato*, si puo' saltare.

        Rilievo non bloccante di Claude Fable 5: la versione precedente di
        questo test portava questo nome ma esercitava solo un file valido —
        non c'era nessun candidato illeggibile. Ora ce n'e' uno davvero, PRIMA
        di quello buono, e senza percorso dichiarato."""
        rotto = tmp_path / "rotto.json"
        rotto.write_text("{ non e' json", encoding="utf-8")
        buono = tmp_path / "buono.json"
        buono.write_text(json.dumps({"proxy": {"enabled": True, "type": "http",
                                               "host": "ok.example", "port": 8080}}),
                         encoding="utf-8")
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        monkeypatch.setattr(bc, "percorsi_config_candidati",
                            lambda: [str(rotto), str(buono)])
        assert _client().session.proxies["https"] == "http://ok.example:8080"


class TestCredenziali:
    """Due difetti trovati da Claude Fable 5 e GPT-5.6 Sol su #430."""

    def test_caratteri_speciali_vengono_codificati(self):
        """Misurato prima della correzione: con password `pa@ss:word/x` l'URL
        era `socks5://pippo:pa@ss:word/x@h.example:1080`, che un parser legge
        come host `ss` e porta `word`. Non malformato: **diretto altrove**."""
        url = bc.costruisci_proxy_url(
            {"enabled": True, "host": "h.example", "port": 1080,
             "username": "pippo", "password": "pa@ss:word/x"})
        assert url == "socks5://pippo:pa%40ss%3Aword%2Fx@h.example:1080"
        parsed = urlparse(url.replace("socks5://", "http://", 1))
        assert parsed.hostname == "h.example" and parsed.port == 1080

    @pytest.mark.parametrize("cfg", [
        {"enabled": True, "host": "h", "port": 1, "username": "solo-utente"},
        {"enabled": True, "host": "h", "port": 1, "password": "solo-password"},
    ])
    def test_credenziale_a_meta_solleva(self, cfg):
        """Prima veniva scartata in silenzio e la connessione diventava anonima."""
        with pytest.raises(ValueError):
            bc.costruisci_proxy_url(cfg)


class TestPorta:
    """La porta e' un numero, e va verificato PRIMA di costruire l'URL.

    Rilievo bloccante di OpenRouter Fugu Ultra su #430, confermato non
    bloccante da Claude Fable 5 e verificato a mano sul codice reale: con
    ``port: "abc"`` la funzione restituiva ``socks5://h.example:abc``, un URL
    che questo modulo considerava valido e che falliva solo alla prima
    richiesta verso Betfair.
    """

    @pytest.mark.parametrize("porta", ["non-un-numero", "8o80", "1080 e mezzo", 1080.5, True])
    def test_porta_non_intera_ferma_l_avvio(self, porta):
        with pytest.raises(ValueError, match="port"):
            bc.costruisci_proxy_url({"enabled": True, "host": "h.example", "port": porta})

    @pytest.mark.parametrize("porta", [0, -1, 65536, 99999])
    def test_porta_fuori_intervallo_ferma_l_avvio(self, porta):
        with pytest.raises(ValueError, match="1-65535"):
            bc.costruisci_proxy_url({"enabled": True, "host": "h.example", "port": porta})

    @pytest.mark.parametrize("porta,atteso", [(1080, 1080), ("1080", 1080), (" 1080 ", 1080),
                                              (1, 1), (65535, 65535)])
    def test_porta_valida_passa(self, porta, atteso):
        url = bc.costruisci_proxy_url({"enabled": True, "host": "h.example", "port": porta})
        assert url == f"socks5://h.example:{atteso}"
        assert urlparse(url).port == atteso

    def test_la_porta_normalizzata_finisce_nell_url(self):
        """Una porta scritta come stringa non deve restare stringa nell'URL."""
        assert bc.costruisci_proxy_url(
            {"enabled": True, "host": "h.example", "port": "01080"}
        ) == "socks5://h.example:1080"


class TestSupportoSocks:
    """Un proxy SOCKS senza PySocks non e' degradato: e' un bot fermo.

    Misurato con `requests` 2.32.3 e `trust_env=False`:
    ``InvalidSchema: Missing dependencies for SOCKS support`` su ogni richiesta.
    Finche' il proxy non si applicava mai — il difetto che questa PR corregge —
    il guasto era irraggiungibile: e' questa correzione a renderlo possibile.
    """

    @pytest.mark.parametrize("tipo", ["socks5", "socks5h", "socks4", "SOCKS5"])
    def test_socks_senza_pysocks_ferma_l_avvio(self, tipo, monkeypatch):
        monkeypatch.setattr(bc, "_supporto_socks_disponibile", lambda: False)
        with pytest.raises(ValueError, match="SOCKS"):
            bc.costruisci_proxy_url(
                {"enabled": True, "type": tipo, "host": "h.example", "port": 1080}
            )

    def test_il_tipo_predefinito_e_socks_quindi_richiede_pysocks(self, monkeypatch):
        """Senza `type` il default e' `socks5`: il controllo deve valere anche li'."""
        monkeypatch.setattr(bc, "_supporto_socks_disponibile", lambda: False)
        with pytest.raises(ValueError, match="SOCKS"):
            bc.costruisci_proxy_url({"enabled": True, "host": "h.example", "port": 1080})

    def test_http_non_richiede_pysocks(self, monkeypatch):
        monkeypatch.setattr(bc, "_supporto_socks_disponibile", lambda: False)
        assert bc.costruisci_proxy_url(
            {"enabled": True, "type": "http", "host": "h.example", "port": 8080}
        ) == "http://h.example:8080"

    def test_proxy_disabilitato_non_richiede_pysocks(self, monkeypatch):
        """`enabled` falso significa "nessun proxy": nessuna dipendenza serve."""
        monkeypatch.setattr(bc, "_supporto_socks_disponibile", lambda: False)
        assert bc.costruisci_proxy_url(
            {"enabled": False, "type": "socks5", "host": "h.example", "port": 1080}
        ) is None

    def test_il_controllo_guarda_davvero_pysocks(self, monkeypatch):
        """Non e' una costante: interroga `importlib.util.find_spec`."""
        import importlib.util

        monkeypatch.setattr(importlib.util, "find_spec", lambda nome: None)
        assert _SUPPORTO_SOCKS_ORIGINALE() is False

        monkeypatch.setattr(importlib.util, "find_spec", lambda nome: object())
        assert _SUPPORTO_SOCKS_ORIGINALE() is True


class TestConfigStoreFraICandidati:
    """`core.config_store.config_path()` era la sola tappa senza copertura.

    Rilievo di OpenRouter Fugu Ultra su #430: *«verificare la copertura reale
    di core.config_store.config_path()»*. E' la tappa che conta di piu' sulla
    macchina dell'owner — su Windows e' `%APPDATA%\\XTraderBridge` — perche' e'
    la config di runtime vera, quella che l'installazione usa davvero.
    """

    def test_config_store_e_fra_i_candidati(self, monkeypatch):
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        from core.config_store import config_path

        assert config_path() in bc.percorsi_config_candidati()

    def test_viene_prima_della_cartella_del_programma(self, monkeypatch):
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        from core.config_store import config_path

        candidati = bc.percorsi_config_candidati()
        accanto_al_programma = candidati[-1]
        assert candidati.index(config_path()) < candidati.index(accanto_al_programma)

    def test_il_proxy_di_config_store_viene_applicato(self, tmp_path, monkeypatch):
        """La tappa e' percorsa davvero, non solo elencata."""
        monkeypatch.delenv(bc.ENV_PERCORSO_CONFIG, raising=False)
        finto = tmp_path / "config.json"
        finto.write_text(json.dumps(
            {"proxy": {"enabled": True, "type": "socks5",
                       "host": "da-config-store.example", "port": 1080}}
        ), encoding="utf-8")
        monkeypatch.setattr(bc, "percorsi_config_candidati", lambda: [str(finto)])
        c = _client()
        assert c.session.proxies["https"] == "socks5://da-config-store.example:1080"


class TestSchemaDelProxy:
    """`type` e' un contratto, non testo libero.

    Rilievo BLOCCANTE di GPT-5.6 Sol su #430. Misurato prima della correzione:
    `socks5x` superava il controllo PySocks (comincia per "socks") e `requests`
    lo rifiutava solo alla prima richiesta con `Unable to determine SOCKS
    version`. E qualunque altra parola — `ftp`, `javascript`, `pippo` —
    passava identica.
    """

    @pytest.mark.parametrize("tipo", ["socks5x", "socks9", "sockshhh", "ftp",
                                      "javascript", "pippo", "socks", "http://"])
    def test_schema_sconosciuto_ferma_l_avvio(self, tipo):
        with pytest.raises(ValueError, match="non e' uno schema supportato"):
            bc.costruisci_proxy_url(
                {"enabled": True, "type": tipo, "host": "h.example", "port": 1080}
            )

    @pytest.mark.parametrize("tipo", ["http", "https", "socks4", "socks4a",
                                      "socks5", "socks5h"])
    def test_schemi_supportati_passano(self, tipo):
        assert bc.costruisci_proxy_url(
            {"enabled": True, "type": tipo, "host": "h.example", "port": 1080}
        ) == f"{tipo}://h.example:1080"

    @pytest.mark.parametrize("scritto,atteso", [("SOCKS5", "socks5"), ("Http", "http"),
                                                ("  socks5h  ", "socks5h")])
    def test_lo_schema_e_normalizzato(self, scritto, atteso):
        """Maiuscole e spazi non devono far fallire una configurazione valida."""
        assert bc.costruisci_proxy_url(
            {"enabled": True, "type": scritto, "host": "h.example", "port": 1080}
        ) == f"{atteso}://h.example:1080"

    def test_lo_schema_e_verificato_prima_di_pysocks(self, monkeypatch):
        """Un `socks5x` senza PySocks deve fallire per lo SCHEMA, non per la
        dipendenza: altrimenti installare PySocks nasconderebbe il difetto."""
        monkeypatch.setattr(bc, "_supporto_socks_disponibile", lambda: False)
        with pytest.raises(ValueError, match="non e' uno schema supportato"):
            bc.costruisci_proxy_url(
                {"enabled": True, "type": "socks5x", "host": "h.example", "port": 1080}
            )

    def test_disabilitato_non_valida_lo_schema(self):
        """`enabled` falso significa "nessun proxy": non c'e' schema da validare."""
        assert bc.costruisci_proxy_url(
            {"enabled": False, "type": "pippo", "host": "h", "port": 1}
        ) is None


class TestHost:
    """L'host deve restare l'host: nessun carattere che sposti i confini.

    Rilievo BLOCCANTE di OpenRouter Fugu Ultra su #430. Misurato prima della
    correzione — e il primo caso e' il grave, perche' non rompe niente:

        host='evil.example@vero.example'
          -> 'socks5://evil.example@vero.example:1080'
          -> urlparse legge hostname 'vero.example'

    Il traffico verso Betfair sarebbe uscito da un host diverso da quello
    configurato, in silenzio. Stessa famiglia del difetto sulle credenziali non
    codificate: una parte dell'URL che invade quella accanto.
    """

    @pytest.mark.parametrize("host", [
        "evil.example@vero.example", "h.example/percorso", "h.example:9999",
        "socks5://h.example", "h.example?x=1", "h.example#frag",
        "spazio nel mezzo", "h.example\ttab",
    ])
    def test_host_che_dirotta_ferma_l_avvio(self, host):
        # `match` sul messaggio di `_host_valido`, non su un generico "host":
        # la post-condizione a valle prenderebbe quasi tutti questi casi
        # comunque, e un test che si accontenta di quello non dimostra che il
        # controllo a monte esista. Verificato togliendo `_host_valido`: senza
        # questo vincolo il sabotaggio passava quasi inosservato.
        with pytest.raises(ValueError, match="caratteri che cambiano"):
            bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})

    def test_l_host_con_chiocciola_non_arriva_mai_all_url(self):
        """Il caso peggiore, fissato da solo: non deve passare nemmeno con
        credenziali valide, dove l'URL resterebbe sintatticamente corretto."""
        with pytest.raises(ValueError, match="caratteri che cambiano"):
            bc.costruisci_proxy_url({
                "enabled": True, "host": "evil.example@vero.example", "port": 1080,
                "username": "u", "password": "p",
            })

    @pytest.mark.parametrize("host", ["h.example", "192.168.1.10", "localhost",
                                      "proxy-1.vpn.example.com", "[::1]",
                                      "[2001:db8::1]"])
    def test_host_validi_passano_e_si_rileggono(self, host):
        url = bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})
        riletto = urlparse(url)
        assert riletto.hostname == host.strip("[]").lower()
        assert riletto.port == 1080

    @pytest.mark.parametrize("host", ["[]", "[non-esadecimale]", "[::gg]"])
    def test_ipv6_malformato_ferma_l_avvio(self, host):
        with pytest.raises(ValueError, match="IPv6"):
            bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})


class TestPostCondizione:
    """L'URL costruito si rilegge come lo si e' inteso.

    Otto difetti su `costruisci_proxy_url` hanno avuto tutti la stessa forma:
    una parte dell'URL che finisce per significare un'altra. I controlli
    elencano i modi di sbagliare che conosciamo; la post-condizione verifica il
    risultato, che e' cio' che conta.
    """

    def test_ogni_url_prodotto_si_rilegge_correttamente(self):
        combinazioni = [
            {"host": "h.example", "port": 1080},
            {"host": "h.example", "port": "1080", "type": "http"},
            {"host": "[::1]", "port": 9050, "type": "socks5h"},
            {"host": "h.example", "port": 3128, "username": "u", "password": "p"},
            {"host": "h.example", "port": 3128, "username": "pi ppo",
             "password": "pa@ss:word/x"},
        ]
        for extra in combinazioni:
            cfg = {"enabled": True, **extra}
            url = bc.costruisci_proxy_url(cfg)
            riletto = urlparse(url)
            atteso = str(extra["host"]).strip("[]").lower()
            assert riletto.hostname == atteso, f"{cfg} -> {url}"
            assert riletto.port == int(extra["port"]), f"{cfg} -> {url}"

    def test_la_post_condizione_e_attiva(self, monkeypatch):
        """Se un controllo a monte lasciasse passare un host che dirotta, la
        post-condizione deve prenderlo comunque."""
        monkeypatch.setattr(bc, "_host_valido", lambda v: v)
        with pytest.raises(ValueError, match="non si rilegge"):
            bc.costruisci_proxy_url(
                {"enabled": True, "host": "evil.example@vero.example", "port": 1080}
            )


class TestNessunSegretoNeiMessaggi:
    """Un controllo che protegge il traffico non deve pubblicare la credenziale.

    Rilievo BLOCCANTE di OpenRouter Fugu Ultra su #430, fondato e misurato. Chi
    sbaglia e incolla un URL intero dentro `proxy.host` — l'errore che
    `_host_valido` esiste per prendere — si vedeva la password nel messaggio
    dell'eccezione, e da li' nei log d'avvio.
    """

    SEGRETO = "SuperSegreta123"

    @pytest.mark.parametrize("host", [
        "socks5://pippo:SuperSegreta123@h.example:1080",
        "pippo:SuperSegreta123@h.example",
        "[SuperSegreta123@::1]",
        # Senza `@`. Rilievo BLOCCANTE di GPT-5.6 Sol su #430: la prima
        # correzione oscurava solo su `@`, quindi un `utente:password`
        # incollato per sbaglio nel campo host restava stampato per intero.
        # `:` e `@` sono entrambi separatori di credenziale in un URL.
        "pippo:SuperSegreta123",
        "SuperSegreta123:",
        ":SuperSegreta123",
    ])
    def test_la_password_non_finisce_nel_messaggio(self, host):
        with pytest.raises(ValueError) as info:
            bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})
        assert self.SEGRETO not in str(info.value)

    def test_la_password_non_finisce_nemmeno_nella_post_condizione(self, monkeypatch):
        """Anche scavalcando il controllo a monte, il messaggio resta pulito."""
        monkeypatch.setattr(bc, "_host_valido", lambda v: v)
        with pytest.raises(ValueError) as info:
            bc.costruisci_proxy_url({
                "enabled": True, "port": 1080,
                "host": f"pippo:{self.SEGRETO}@vero.example",
            })
        assert self.SEGRETO not in str(info.value)

    def test_un_host_senza_chiocciola_resta_leggibile(self):
        """Oscurare tutto renderebbe il messaggio inutile: si oscura solo dove
        puo' esserci una credenziale."""
        with pytest.raises(ValueError) as info:
            bc.costruisci_proxy_url(
                {"enabled": True, "host": "h.example/percorso", "port": 1080}
            )
        assert "h.example/percorso" in str(info.value)

    def test_le_credenziali_valide_non_compaiono_mai_nei_log(self, caplog):
        """Il caso normale: proxy corretto, log dell'avvio senza segreti."""
        import logging

        with caplog.at_level(logging.DEBUG):
            _client(proxy_config={"enabled": True, "type": "socks5",
                                  "host": "h.example", "port": 1080,
                                  "username": "pippo", "password": self.SEGRETO})
        assert self.SEGRETO not in caplog.text
        assert "pippo" not in caplog.text

    def test_il_troncamento_non_stampa_i_primi_caratteri(self):
        """Stesso rilievo di GPT: troncare a 60 caratteri stampa comunque i
        primi 60, che possono essere il segreto. Si dice solo la lunghezza."""
        lungo = self.SEGRETO * 10
        mostrato = bc._senza_segreti(lungo)
        assert self.SEGRETO not in mostrato
        assert str(len(lungo)) in mostrato

    @pytest.mark.parametrize("host", ["[pippo:SuperSegreta123]",
                                      "[SuperSegreta123@::1]",
                                      "[SuperSegreta123]"])
    def test_le_credenziali_fra_parentesi_non_si_stampano(self, host):
        """Rilievo BLOCCANTE di Claude Fable 5 e xAI Grok 4.6 su #430.

        Il push precedente si era ritagliato un'eccezione per il ramo IPv6 —
        "li' i due punti sono normali" — e con quella aveva riaperto lo stesso
        leak che stava chiudendo: `[pippo:SuperSegreta]` ha `:` ma non `@`, e
        finiva stampato per intero.
        """
        with pytest.raises(ValueError) as info:
            bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})
        assert self.SEGRETO not in str(info.value)

    @pytest.mark.parametrize("host", ["[deadbeef:cafe1234]", "[::gg]", "[]",
                                      "[non-hex]", "[pippo:SuperSegreta123]"])
    def test_un_ipv6_non_valido_non_riporta_mai_il_valore(self, host):
        """Secondo rilievo BLOCCANTE di Claude Fable 5 sullo stesso punto.

        Avevo sostituito l'oscuramento con una whitelist di caratteri
        esadecimali. Ma un token esadecimale — la forma piu' comune per una
        API key — la supera: `[deadbeef:cafe1234]` tornava in chiaro. Il leak
        si restringeva, non si chiudeva.

        Qui dentro ci si arriva SOLO quando il valore non e' un IPv6 valido.
        Se non lo e', non sappiamo cosa sia: non si mostra, e basta.
        """
        with pytest.raises(ValueError) as info:
            bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})
        messaggio = str(info.value)
        assert "non viene riportato" in messaggio
        interno = host.strip("[]")
        if interno:  # con `[]` l'interno e' vuoto e il confronto sarebbe sempre falso
            assert interno not in messaggio

    @pytest.mark.parametrize("host", ["[::1]", "[2001:db8::1]", "[fe80::1]",
                                      "[::ffff:192.168.1.1]"])
    def test_gli_ipv6_veri_passano_ancora(self, host):
        """La validita' la decide `ipaddress`, non un elenco di caratteri."""
        url = bc.costruisci_proxy_url({"enabled": True, "host": host, "port": 1080})
        assert urlparse(url).port == 1080

    def test_la_conversione_dell_errore_di_urlparse(self, monkeypatch):
        """Rilievo di Claude Fable 5, accolto: la versione precedente di questo
        test usava due host che `_host_valido` blocca PRIMA, quindi l'`except`
        non veniva mai eseguito e il test passava per il messaggio sbagliato.

        Con `ipaddress` a monte quel ramo e' irraggiungibile per costruzione:
        resta come difesa se un domani il controllo a monte si allenta. Lo si
        prova per quello che e' — una conversione — provocandolo.
        """
        def _esplode(_url):
            raise ValueError("dettaglio interno di urlparse con dentro SuperSegreta123")

        monkeypatch.setattr(bc, "urlparse", _esplode)
        with pytest.raises(ValueError) as info:
            bc.costruisci_proxy_url({"enabled": True, "host": "h.example", "port": 1080})
        assert "URL illeggibile" in str(info.value)
        assert "SuperSegreta123" not in str(info.value)

