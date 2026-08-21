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

    def test_file_assente_non_e_un_errore(self, monkeypatch):
        """Nessuna configurazione significa "nessun proxy", non un guasto."""
        monkeypatch.setenv(bc.ENV_PERCORSO_CONFIG, "/percorso/che/non/esiste.json")
        assert not _client().session.proxies

    def test_file_illeggibile_non_interrompe_l_avvio(self, tmp_path, monkeypatch):
        f = tmp_path / "config.json"
        f.write_text("{ questo non e' json", encoding="utf-8")
        monkeypatch.setenv(bc.ENV_PERCORSO_CONFIG, str(f))
        assert not _client().session.proxies
