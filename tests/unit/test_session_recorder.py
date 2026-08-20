"""Recorder di sessione (fase A) — `core/session_recorder.py`.

Copre le tre cose che rendono il diario ANALIZZABILE invece che soltanto scritto:
il progressivo (rileva gli eventi persi), l'orologio monotono (le latenze non mentono
quando il wall-clock salta) e la catena causale (dal messaggio alla giocata). Piu' le
due invarianti di sicurezza: il recorder non solleva mai, e spegnerlo non fa sparire
le prove.
"""

import ast
import pathlib

import pytest

from core import event_journal, session_recorder
from core.session_recorder import SessionRecorder

pytestmark = pytest.mark.unit


class OrologioFinto:
    """Orologio monotono pilotato dal test: `avanza()` invece di dormire."""

    def __init__(self, start=1000.0):
        self.valore = start

    def __call__(self):
        return self.valore

    def avanza(self, quanto):
        self.valore += quanto


def leggi(path):
    return event_journal.read_events(str(path))


@pytest.fixture
def rec(tmp_path):
    """Recorder ACCESO su un ledger temporaneo, con orologio pilotato."""
    orologio = OrologioFinto()
    r = SessionRecorder(str(tmp_path / "j.jsonl"), enabled=True,
                        session_id="s_test", mono=orologio, now=lambda: 1700000000.0)
    r._orologio = orologio          # comodita' per i test
    return r


class TestInterruttore:
    def test_spento_non_scrive_gli_eventi_di_rumore(self, tmp_path):
        path = tmp_path / "j.jsonl"
        r = SessionRecorder(str(path), enabled=False, session_id="s")
        assert r.record("UI_CLICK", control="refresh") is None
        assert leggi(path) == []

    def test_spento_scrive_comunque_il_percorso_soldi(self, tmp_path):
        """Un interruttore di verbosita' non deve poter cancellare la traccia di una
        scommessa: sarebbe una perdita di prove travestita da preferenza."""
        path = tmp_path / "j.jsonl"
        r = SessionRecorder(str(path), enabled=False, session_id="s")
        assert r.record("ORDER_PLACE_REQUEST", size=10) is not None
        assert r.record("RISK_GATE_DENY", regola="MAX_WIN") is not None
        assert r.record("UNCAUGHT_EXCEPTION", tipo="ValueError") is not None
        tipi = [e["type"] for e in leggi(path)]
        assert tipi == ["ORDER_PLACE_REQUEST", "RISK_GATE_DENY", "UNCAUGHT_EXCEPTION"]

    def test_acceso_scrive_tutto(self, rec):
        assert rec.record("UI_CLICK") is not None
        assert rec.record("ORDER_MATCHED") is not None
        assert len(leggi(rec.path)) == 2

    def test_tutti_i_tipi_legacy_sono_sempre_registrati(self):
        """Regressione: il diario del bridge esiste dal #230. Il recorder e' arrivato
        dopo e di default e' SPENTO — se il LEGACY non fosse in `ALWAYS_RECORDED`,
        introdurlo spegnerebbe una traccia forense preesistente."""
        legacy = event_journal.types_of_category("LEGACY")
        assert legacy <= session_recorder.ALWAYS_RECORDED

    def test_set_enabled(self, tmp_path):
        r = SessionRecorder(str(tmp_path / "j.jsonl"), enabled=False)
        assert r.enabled is False
        assert r.set_enabled(True) is True
        assert r.record("UI_CLICK") is not None


class TestProgressivo:
    def test_seq_parte_da_zero_e_cresce_di_uno(self, rec):
        for _ in range(4):
            rec.record("UI_CLICK")
        assert [e["seq"] for e in leggi(rec.path)] == [0, 1, 2, 3]

    def test_un_buco_nel_progressivo_e_rilevabile(self, rec):
        """E' l'intero scopo del campo: senza, un ledger amputato da un disco pieno
        sembra soltanto un ledger piu' corto."""
        for _ in range(3):
            rec.record("UI_CLICK")
        righe = pathlib.Path(rec.path).read_text(encoding="utf-8").splitlines()
        pathlib.Path(rec.path).write_text(righe[0] + "\n" + righe[2] + "\n",
                                          encoding="utf-8")
        seqs = [e["seq"] for e in leggi(rec.path)]
        assert seqs == [0, 2]
        assert seqs != list(range(len(seqs)))       # il buco si vede

    def test_seq_unico_sotto_concorrenza(self, tmp_path):
        """`self._seq += 1` non e' atomico: due thread potrebbero ricevere lo stesso
        numero, e un progressivo duplicato rende inaffidabile proprio il rilevamento
        dei buchi."""
        import threading
        r = SessionRecorder(str(tmp_path / "j.jsonl"), enabled=True)
        visti = []
        blocco = threading.Lock()

        def lavora():
            for _ in range(25):
                with blocco:
                    visti.append(r._next_seq())

        threads = [threading.Thread(target=lavora) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(visti) == len(set(visti)) == 100


class TestOrologioMonotono:
    def test_mono_avanza_col_proprio_orologio(self, rec):
        rec.record("UI_CLICK")
        rec._orologio.avanza(3.5)
        rec.record("UI_CLICK")
        monos = [e["mono"] for e in leggi(rec.path)]
        assert monos[1] - monos[0] == pytest.approx(3.5)

    def test_mono_non_segue_il_wall_clock_che_torna_indietro(self, tmp_path):
        """Il caso reale: NTP corregge l'ora, o il portatile esce dalla sospensione.
        `ts` puo' andare indietro; le durate vanno calcolate su `mono`, che non lo fa."""
        orologio = OrologioFinto()
        muri = [5000.0, 4000.0]                  # il wall-clock ARRETRA fra i due eventi
        # `session_id` esplicito: senza, la costruzione dell'id consumerebbe il primo
        # valore dell'orologio e il test misurerebbe altro.
        r = SessionRecorder(str(tmp_path / "j.jsonl"), enabled=True, session_id="s",
                            mono=orologio, now=lambda: muri.pop(0))
        for _ in range(2):
            r.record("UI_CLICK")
            orologio.avanza(1.0)
        eventi = leggi(r.path)
        # `ts` arretra — e chi calcolasse le durate su quello otterrebbe un tempo NEGATIVO.
        assert eventi[1]["ts"] < eventi[0]["ts"]
        # `mono` no: cresce comunque si comporti l'orologio di sistema.
        assert eventi[1]["mono"] > eventi[0]["mono"]

    def test_elapsed_non_e_mai_negativo(self, tmp_path):
        orologio = OrologioFinto()
        r = SessionRecorder(str(tmp_path / "j.jsonl"), mono=orologio)
        orologio.avanza(-100.0)                  # orologio patologico
        assert r.elapsed() >= 0.0


class TestCatenaCausale:
    def test_start_chain_apre_la_catena_e_include_se_stesso(self, rec):
        """`root` dell'evento capostipite e' l'evento stesso: filtrando per `root` si
        ottiene ANCHE il messaggio che ha originato la giocata, non solo il seguito."""
        radice = rec.record("TG_MESSAGE_IN", start_chain=True)
        rec.record("PARSE_OK")
        rec.record("ORDER_PLACE_REQUEST")
        eventi = leggi(rec.path)
        assert all(e["root"] == radice for e in eventi)

    def test_corr_e_l_evento_precedente_non_la_radice(self, rec):
        """La catena e' una lista concatenata (chi ha causato chi), non un ventaglio in
        cui tutto punta alla radice: e' cio' che permette di ricostruire l'ORDINE dei
        passaggi, non solo la loro appartenenza."""
        radice = rec.record("TG_MESSAGE_IN", start_chain=True)
        secondo = rec.record("PARSE_OK")
        rec.record("SIGNAL_CREATED")
        eventi = leggi(rec.path)
        assert eventi[1]["corr"] == radice
        assert eventi[2]["corr"] == secondo

    def test_filtrare_per_root_da_la_storia_di_una_giocata(self, rec):
        rec.record("TG_MESSAGE_IN", start_chain=True)
        rec.record("PARSE_OK")
        rec.clear_chain()
        rec.record("UI_CLICK")                    # rumore fuori dalla catena
        radice2 = rec.record("TG_MESSAGE_IN", start_chain=True)
        rec.record("ORDER_MATCHED")
        eventi = leggi(rec.path)
        catena2 = [e["type"] for e in eventi if e.get("root") == radice2]
        assert catena2 == ["TG_MESSAGE_IN", "ORDER_MATCHED"]

    def test_chain_ricuce_una_catena_e_ripristina_lo_stato(self, rec):
        rec.clear_chain()
        with rec.chain("r-esterno"):
            rec.record("ORDER_MATCHED")
        rec.record("UI_CLICK")
        eventi = leggi(rec.path)
        assert eventi[0]["root"] == "r-esterno"
        assert "root" not in eventi[1]            # lo stato e' stato ripristinato

    def test_root_esplicito_vince_sul_contesto(self, rec):
        rec.record("TG_MESSAGE_IN", start_chain=True)
        rec.record("ORDER_MATCHED", root="scelto-a-mano")
        assert leggi(rec.path)[1]["root"] == "scelto-a-mano"

    def test_open_chain_apre_e_chiude(self, rec):
        rec.clear_chain()
        with rec.open_chain("TG_MESSAGE_IN", chat="x") as radice:
            rec.record("PARSE_OK")
        rec.record("UI_CLICK")
        eventi = leggi(rec.path)
        assert [e.get("root") for e in eventi] == [radice, radice, None]

    def test_open_chain_gira_anche_a_recorder_spento(self, tmp_path):
        """Un recorder spento non deve cambiare il flusso del chiamante: il blocco
        `with` gira lo stesso, semplicemente senza `root`."""
        r = SessionRecorder(str(tmp_path / "j.jsonl"), enabled=False)
        eseguito = False
        with r.open_chain("TG_MESSAGE_IN") as radice:
            eseguito = True
        assert eseguito and radice is None

    def test_su_thread_RIUSATO_open_chain_non_fa_colare_la_catena(self, rec):
        """Rilievo di Fugu su #427, accolto.

        I `contextvars` sopravvivono nel thread che li ha impostati. In un pool, il
        lavoro successivo servito dallo STESSO thread erediterebbe la catena di quello
        prima: due giocate distinte apparirebbero come una sola nel diario. `open_chain`
        chiude per costruzione, quindi non cola.
        """
        import threading

        def pool():
            with rec.open_chain("TG_MESSAGE_IN", chat="A"):
                rec.record("ORDER_MATCHED")
            # secondo lavoro sullo STESSO thread, senza catena propria
            rec.record("UI_CLICK")

        t = threading.Thread(target=pool)
        t.start()
        t.join()
        eventi = leggi(rec.path)
        assert eventi[-1]["type"] == "UI_CLICK"
        assert "root" not in eventi[-1], "la catena e' colata nel lavoro successivo"

    def test_su_thread_RIUSATO_start_chain_grezzo_invece_cola(self, rec):
        """Documenta il comportamento della forma grezza, che resta disponibile per chi
        controlla lui stesso l'ambito. Non e' un difetto nascosto: e' il motivo per cui
        `open_chain` esiste ed e' la via da preferire."""
        import threading

        def pool():
            rec.record("TG_MESSAGE_IN", start_chain=True, chat="A")
            rec.record("UI_CLICK")            # lavoro successivo, stesso thread

        t = threading.Thread(target=pool)
        t.start()
        t.join()
        eventi = leggi(rec.path)
        assert eventi[-1]["type"] == "UI_CLICK"
        assert eventi[-1].get("root") == eventi[0]["id"]    # eredita: e' il rischio

    def test_senza_catena_non_ci_sono_campi_di_correlazione(self, rec):
        rec.clear_chain()
        rec.record("UI_CLICK")
        e = leggi(rec.path)[0]
        assert "root" not in e and "corr" not in e


class TestCampionamento:
    def test_scarta_le_ripetizioni_dentro_l_intervallo(self, rec):
        assert rec.record("PRICE_SNAPSHOT", sample_key="m1", min_interval=1.0)
        assert rec.record("PRICE_SNAPSHOT", sample_key="m1", min_interval=1.0) is None
        rec._orologio.avanza(1.5)
        assert rec.record("PRICE_SNAPSHOT", sample_key="m1", min_interval=1.0)
        assert len(leggi(rec.path)) == 2

    def test_chiavi_diverse_sono_indipendenti(self, rec):
        assert rec.record("PRICE_SNAPSHOT", sample_key="m1", min_interval=10.0)
        assert rec.record("PRICE_SNAPSHOT", sample_key="m2", min_interval=10.0)

    def test_intervallo_nullo_non_campiona(self, rec):
        for _ in range(3):
            assert rec.record("UI_SCROLL", sample_key="k", min_interval=0)
        assert len(leggi(rec.path)) == 3

    def test_il_dizionario_delle_chiavi_e_limitato(self, rec):
        """Una chiave per mercato, per tutta la sessione, crescerebbe senza limite."""
        for i in range(session_recorder._SAMPLE_KEYS_MAX + 50):
            rec.record("PRICE_SNAPSHOT", sample_key=f"m{i}", min_interval=10.0)
        assert len(rec._sampler._last) <= session_recorder._SAMPLE_KEYS_MAX


class TestNonSollevaMai:
    def test_tipo_sconosciuto_ritorna_none(self, rec):
        assert rec.record("TIPO_INVENTATO") is None

    def test_percorso_non_scrivibile_ritorna_none(self, tmp_path):
        ostacolo = tmp_path / "file"
        ostacolo.write_text("non sono una cartella", encoding="utf-8")
        r = SessionRecorder(str(ostacolo / "sotto" / "j.jsonl"), enabled=True)
        assert r.record("UI_CLICK") is None

    def test_dato_non_serializzabile_ritorna_none(self, rec):
        assert rec.record("UI_CLICK", oggetto=object()) is None

    def test_path_assente_e_un_no_op(self):
        assert SessionRecorder(None, enabled=True).record("ORDER_MATCHED") is None

    def test_orologio_rotto_non_impedisce_la_registrazione(self, tmp_path):
        def mono_rotto():
            raise RuntimeError("orologio guasto")
        r = SessionRecorder(str(tmp_path / "j.jsonl"), enabled=True, mono=mono_rotto)
        assert r.record("ORDER_MATCHED") is not None


class TestIdentitaDiSessione:
    def test_ogni_evento_porta_la_sessione(self, rec):
        rec.record("UI_CLICK")
        assert leggi(rec.path)[0]["sess"] == "s_test"

    def test_due_recorder_hanno_sessioni_diverse(self, tmp_path):
        a = SessionRecorder(str(tmp_path / "a.jsonl"))
        b = SessionRecorder(str(tmp_path / "b.jsonl"))
        assert a.session_id != b.session_id

    def test_due_sessioni_sullo_stesso_file_restano_distinguibili(self, tmp_path):
        path = str(tmp_path / "j.jsonl")
        SessionRecorder(path, enabled=True, session_id="s1").record("UI_CLICK")
        SessionRecorder(path, enabled=True, session_id="s2").record("UI_CLICK")
        assert [e["sess"] for e in leggi(path)] == ["s1", "s2"]


class TestCablaggioInApp:
    """Il recorder deve essere DAVVERO collegato al diario dell'app.

    Senza questi due test, rimuovere il cablaggio da `app.py` lascerebbe la suite
    verde: gli eventi continuerebbero a scriversi, solo senza sessione, progressivo
    e catena — cioe' il diario tornerebbe a essere scritto ma non analizzabile, in
    silenzio. Si legge il sorgente con `ast` invece di importare `core.app`, che
    tirerebbe dentro tkinter/customtkinter e non e' importabile headless.
    """

    @staticmethod
    def _albero():
        sorgente = pathlib.Path(__file__).resolve().parents[2] / "core" / "app.py"
        return ast.parse(sorgente.read_text(encoding="utf-8"))

    def test_app_costruisce_un_session_recorder(self):
        albero = self._albero()
        costruzioni = [
            n for n in ast.walk(albero)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "SessionRecorder"
        ]
        assert costruzioni, "core/app.py non costruisce piu' un SessionRecorder"

    def test_journal_passa_dal_recorder(self):
        albero = self._albero()
        funzioni = [n for n in ast.walk(albero)
                    if isinstance(n, ast.FunctionDef) and n.name == "_journal"]
        assert funzioni, "core/app.py non definisce piu' _journal"
        corpo = ast.dump(funzioni[0])
        assert "_recorder" in corpo, "_journal non passa piu' dal recorder"
        assert "record" in corpo, "_journal non chiama piu' recorder.record"

    def test_nessun_emettitore_usa_un_nome_riservato_di_record(self):
        """Rilievo di Fable su #427, accolto.

        `record(event_type, *, level, corr, root, ...)` raccoglie il payload con
        `**data`: un emettitore che passasse `level=...` come CAMPO del payload lo
        vedrebbe interpretato come PARAMETRO, e il campo sparirebbe dal diario **senza
        errore**. Oggi non succede, ma la fase A2 aggiunge decine di emettitori
        (`TG_*`, `ORDER_*`, `RISK_*`) ed e' esattamente il momento in cui una
        collisione entrerebbe inosservata.

        I nomi riservati si leggono dalla FIRMA, non da un elenco ricopiato: aggiungerne
        uno a `record` estende automaticamente il controllo, invece di lasciare il test
        a proteggere una versione vecchia della firma."""
        import inspect

        riservati = {
            nome for nome, par in inspect.signature(SessionRecorder.record).parameters.items()
            if par.kind is inspect.Parameter.KEYWORD_ONLY
        }
        assert riservati, "la firma di record() non ha piu' parametri keyword-only"

        collisioni = []
        for nodo in ast.walk(self._albero()):
            if (isinstance(nodo, ast.Call)
                    and isinstance(nodo.func, ast.Attribute)
                    and nodo.func.attr in ("_journal", "_journal_csv_cleared_if_had_row")):
                tipo = (nodo.args[0].value
                        if nodo.args and isinstance(nodo.args[0], ast.Constant) else "?")
                collisioni += [(tipo, k.arg) for k in nodo.keywords
                               if k.arg in riservati]
        assert not collisioni, (
            f"kwarg di payload che collidono coi parametri di record(): {collisioni}")
