"""Catalogo eventi e campi del Recorder (fase A) — `core/event_journal.py`.

Il modulo non aveva test in questo repository: questi coprono sia il comportamento
NUOVO sia quello STORICO che non deve cambiare. La parte di retro-compatibilita' non e'
cerimoniale — su quei tipi ci sono ledger gia' scritti su disco, e un rename silenzioso
li renderebbe illeggibili senza che nulla diventi rosso.
"""

import json

import pytest

from core import event_journal, event_log

pytestmark = pytest.mark.unit


# Gli 11 tipi dell'epoca bridge/CSV, ricopiati QUI a mano di proposito: se il test li
# importasse dal modulo, una rimozione accidentale renderebbe il test verde su un
# catalogo amputato. Scritti a mano, la rimozione diventa rossa.
LEGACY_STORICI = {
    "START", "STOP", "SIGNAL_RECEIVED", "SIGNAL_PARSED", "SIGNAL_VALIDATED",
    "CSV_WRITTEN", "CSV_CLEARED", "XTRADER_CONFIRMED", "XTRADER_REJECTED",
    "RECONNECT", "CRASH_RECOVERY_CSV_CLEARED",
}


class TestCatalogo:
    def test_categorie_disgiunte(self):
        """Nessun tipo in due categorie: `_CATEGORY_BY_TYPE` fa vincere l'ultima, quindi
        un duplicato darebbe una categoria arbitraria e silenziosa."""
        visti = {}
        duplicati = []
        for cat, tipi in event_journal.EVENT_CATEGORIES.items():
            for tipo in tipi:
                if tipo in visti:
                    duplicati.append((tipo, visti[tipo], cat))
                visti[tipo] = cat
        assert not duplicati, f"tipi in piu' categorie: {duplicati}"

    def test_event_types_e_unione_delle_categorie(self):
        unione = set()
        for tipi in event_journal.EVENT_CATEGORIES.values():
            unione |= set(tipi)
        assert set(event_journal.EVENT_TYPES) == unione

    def test_tipi_storici_ancora_presenti_e_in_legacy(self):
        assert LEGACY_STORICI <= set(event_journal.EVENT_TYPES)
        assert LEGACY_STORICI == set(event_journal.EVENT_CATEGORIES["LEGACY"])

    def test_category_of(self):
        assert event_journal.category_of("ORDER_MATCHED") == "ORDER"
        assert event_journal.category_of("START") == "LEGACY"
        assert event_journal.category_of("TG_MESSAGE_DROPPED") == "TELEGRAM"

    def test_category_of_non_solleva_su_tipo_ignoto(self):
        """Serve a classificare anche righe scritte da una versione diversa: un tipo
        fuori catalogo non deve rompere la LETTURA di un ledger."""
        assert event_journal.category_of("TIPO_CHE_NON_ESISTE") is None
        assert event_journal.category_of(None) is None

    def test_types_of_category(self):
        assert "ORDER_MATCHED" in event_journal.types_of_category("ORDER")
        assert "ORDER_MATCHED" in event_journal.types_of_category("order")
        assert event_journal.types_of_category("NON_ESISTE") == frozenset()

    def test_ogni_categoria_non_e_vuota(self):
        vuote = [c for c, t in event_journal.EVENT_CATEGORIES.items() if not t]
        assert not vuote, f"categorie vuote: {vuote}"


class TestMakeEvent:
    def test_chiamata_storica_resta_compatibile(self):
        """Un chiamante che non conosce il Recorder produce la stessa riga di prima piu'
        il solo `cat`, che e' derivato dal tipo e non richiede nulla a nessuno."""
        e = event_journal.make_event("START", {"a": 1}, now=100.0, event_id="x")
        assert e == {"id": "x", "ts": 100.0, "type": "START", "cat": "LEGACY",
                     "data": {"a": 1}}

    def test_fail_closed_su_tipo_sconosciuto(self):
        with pytest.raises(ValueError):
            event_journal.make_event("NON_ESISTE")

    def test_campi_recorder_solo_se_passati(self):
        e = event_journal.make_event("UI_CLICK", now=1.0, event_id="x")
        for campo in ("sess", "seq", "mono", "lvl", "corr", "root"):
            assert campo not in e

    def test_campi_recorder_presenti_quando_passati(self):
        e = event_journal.make_event(
            "UI_CLICK", {"c": "refresh"}, now=1.0, event_id="x",
            sess="s_1", seq=7, mono=42.5, lvl="warning", corr="c1", root="r1")
        assert e["sess"] == "s_1"
        assert e["seq"] == 7
        assert e["mono"] == 42.5
        assert e["lvl"] == "WARNING"
        assert e["corr"] == "c1"
        assert e["root"] == "r1"
        assert e["cat"] == "UI"

    def test_seq_zero_e_conservato(self):
        """`seq=0` e' il PRIMO evento della sessione, non un'assenza: la presenza si
        verifica con `is not None`. Con un test di verita' il primo evento di ogni
        sessione perderebbe il progressivo, e ogni rilevamento di buchi partirebbe
        sbagliato."""
        e = event_journal.make_event("UI_CLICK", now=1.0, event_id="x", seq=0)
        assert e["seq"] == 0

    def test_data_e_ultimo_campo(self):
        """`data` puo' essere lungo: se non fosse in fondo nasconderebbe i metadati
        quando si legge il .jsonl a occhio."""
        e = event_journal.make_event("UI_CLICK", {"a": 1}, now=1.0, event_id="x",
                                     sess="s", seq=1)
        assert list(e)[-1] == "data"

    @pytest.mark.parametrize("cattivo", [float("nan"), float("inf"), "non-un-numero"])
    def test_mono_non_finito_e_rifiutato(self, cattivo):
        with pytest.raises(ValueError):
            event_journal.make_event("UI_CLICK", now=1.0, mono=cattivo)

    def test_livello_ignoto_degrada_a_info_senza_perdere_l_evento(self):
        """Asimmetria voluta col tipo: un'etichetta sbagliata non deve far perdere il
        FATTO che l'evento e' successo."""
        e = event_journal.make_event("UI_CLICK", now=1.0, lvl="URGENTISSIMO")
        assert e["lvl"] == "INFO"

    def test_normalize_level_accetta_i_livelli_dichiarati(self):
        for lvl in event_journal.LEVELS:
            assert event_journal.normalize_level(lvl.lower()) == lvl


class TestScritturaSuFile:
    def test_round_trip_con_campi_recorder(self, tmp_path):
        path = str(tmp_path / "j.jsonl")
        event_journal.append_event(path, "ORDER_MATCHED", {"bet": "1"},
                                   now=5.0, event_id="e1", sess="s_1", seq=3,
                                   mono=9.5, lvl="INFO", corr="c", root="r")
        letti = event_journal.read_events(path)
        assert len(letti) == 1
        e = letti[0]
        assert e["type"] == "ORDER_MATCHED"
        assert e["cat"] == "ORDER"
        assert (e["sess"], e["seq"], e["mono"], e["corr"], e["root"]) == \
            ("s_1", 3, 9.5, "c", "r")

    def test_righe_storiche_senza_campi_nuovi_restano_leggibili(self, tmp_path):
        """Un ledger scritto prima della fase A non deve diventare illeggibile."""
        path = tmp_path / "j.jsonl"
        path.write_text(
            json.dumps({"id": "v", "ts": 1.0, "type": "START", "data": {}}) + "\n",
            encoding="utf-8")
        letti = event_journal.read_events(str(path))
        assert len(letti) == 1
        assert letti[0]["type"] == "START"
        assert "cat" not in letti[0]

    def test_redazione_attiva_anche_sul_percorso_nuovo(self, tmp_path):
        """Test di CABLAGGIO: se un domani `append_event` smettesse di passare da
        `redact_secrets`, questo diventa rosso. Senza, la perdita sarebbe silenziosa —
        il ledger continuerebbe a scriversi, solo con il segreto in chiaro."""
        segreto = "SEGRETISSIMO-1234567890"
        event_log.register_secret(segreto)
        try:
            path = str(tmp_path / "j.jsonl")
            event_journal.append_event(path, "TG_MESSAGE_IN", {"testo": segreto},
                                       now=1.0, sess="s", seq=0, mono=1.0)
            grezzo = open(path, encoding="utf-8").read()
            assert segreto not in grezzo
            assert "[REDACTED_TOKEN]" in grezzo
        finally:
            event_log.unregister_secret(segreto)

    def test_redazione_finale_copre_anche_i_campi_fuori_da_data(self, tmp_path):
        """`make_event` redige `data`; la ri-redazione della RIGA in `append_event` e' la
        difesa in profondita' che copre tutto il resto — i campi del Recorder inclusi.

        Senza questo test la seconda difesa era rimovibile senza far diventare rosso
        niente: la prima da sola manteneva verdi tutti gli altri casi, e la perdita si
        sarebbe vista solo il giorno in cui un segreto fosse finito in un campo diverso
        da `data`."""
        segreto = "SEGRETISSIMO-0987654321"
        event_log.register_secret(segreto)
        try:
            path = str(tmp_path / "j.jsonl")
            event_journal.append_event(path, "ORDER_MATCHED", {"ok": 1},
                                       now=1.0, root=segreto, corr=segreto)
            grezzo = open(path, encoding="utf-8").read()
            assert segreto not in grezzo
        finally:
            event_log.unregister_secret(segreto)
