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


def _client(**kwargs):
    """Client minimo: il costruttore non fa rete, solo assegnazioni."""
    return bc.BetfairClient(username="u", app_key="k", cert_pem="c", key_pem="p", **kwargs)


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
