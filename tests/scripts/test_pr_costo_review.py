"""Costo review per range — `scripts/pr_costo_review.py`.

Tre test portano il peso, e per lo stesso motivo: uno script che riassume soldi
può sbagliare in due direzioni, e solo una si vede. Un totale troppo ALTO stona
subito; un totale troppo BASSO — perché una review non è stata letta — si legge
come una buona notizia, e resta lì. Quindi:

- `test_review_senza_costo_non_sparisce` copre ciò che non si è riusciti a
  leggere;
- `test_senza_autore_non_entra_nel_totale` copre ciò che non si dovrebbe credere;
- `test_ma_viene_mostrata` copre il rovescio: scartare in silenzio è lo stesso
  difetto, girato al contrario.
"""

import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

RADICE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RADICE))

from scripts import pr_costo_review as pcr  # noqa: E402

pytestmark = pytest.mark.unit

SCRIPT = str(RADICE / "scripts" / "pr_costo_review.py")


def review(reviewer, rng, costo=None, extra=""):
    """Corpo di un commento di review come lo scrive il workflow, marker incluso.

    Il marker c'è perché è realistico, NON perché conti: dalla revisione di #428
    la fiducia sta solo nell'autore. Tenerlo qui serve proprio a dimostrarlo —
    `test_il_marker_da_solo_non_basta` passa questo stesso corpo senza autore."""
    ident = "".join(c for c in reviewer.lower() if c.isalnum())
    corpo = (f"<!-- {ident}-pr-review:pickfair-nogui:{rng} -->\n"
             f"# {reviewer} PR Review\n\n"
             f"**Modalità:** push-range diff via GitHub Compare API\n"
             f"**Range:** `{rng}`\n\n"
             f"## Bloccanti\nNessun bloccante evidente\n{extra}\n")
    if costo is not None:
        corpo += ("\n## Uso token / costo stimato\n"
                  "- Total token: `1234`\n"
                  f"- Costo stimato base: `~${costo}`\n")
    return corpo


def dal_bot(reviewer, rng, costo=None, extra=""):
    """Commento nella forma che restituisce l'API: corpo PIÙ autore.

    È l'unico input che il conteggio accetta senza rinunce esplicite, perché
    `user.login` lo scrive GitHub e non chi commenta."""
    return {"body": review(reviewer, rng, costo, extra),
            "user": {"login": "github-actions[bot]"}}


class TestEstrazione:
    def test_legge_reviewer_range_e_costo(self):
        r = pcr.estrai_review([dal_bot("Claude Fable 5", "aaa...bbb", "0.3092")])
        assert r.review == [("aaa...bbb", "Claude Fable 5", Decimal("0.3092"))]
        assert r.incomplete == [] and r.non_verificate == []

    def test_ignora_i_commenti_che_non_sono_review(self):
        """Non sono sospetti: sono semplicemente altro, e non vanno segnalati."""
        rumore = [{"body": t, "user": {"login": "github-actions[bot]"}} for t in (
            "## Verdetto dell'autore\nPronta.",
            "<!-- coderabbitai -->\nWalkthrough…",
            "You have reached your Codex usage limits.")]
        r = pcr.estrai_review(rumore)
        assert r.review == [] and r.incomplete == [] and r.non_verificate == []

    def test_un_reviewer_nuovo_entra_nel_conto_da_solo(self):
        """Il nome si cattura fra '#' e 'PR Review' invece di elencare i modelli:
        un reviewer aggiunto in futuro non deve sparire dal totale perché nessuno
        si è ricordato di aggiornare questo script."""
        r = pcr.estrai_review([dal_bot("Modello Che Non Esiste Ancora", "a...b", "1.00")])
        assert r.review == [("a...b", "Modello Che Non Esiste Ancora", Decimal("1.00"))]

    def test_review_senza_range_e_comunque_conteggiata(self):
        senza = {"body": "# GPT-5.6 Sol PR Review\n\nCosto stimato base: `~$0.05`\n",
                 "user": {"login": "github-actions[bot]"}}
        r = pcr.estrai_review([senza])
        assert r.review == [(pcr.SENZA_RANGE, "GPT-5.6 Sol", Decimal("0.05"))]

    def test_review_fallita_con_costo_conta_lo_stesso(self):
        """Un giro andato in 429 riporta comunque un costo: è stato speso."""
        fallita = dal_bot("xAI Grok 4.6", "a...b", "0.0087",
                          extra="## Review non completata\nHTTP 429: resource-exhausted")
        assert pcr.estrai_review([fallita]).review[0][2] == Decimal("0.0087")


class TestProvenienza:
    """Rilievo di GPT-5.6 Sol su #428, in due giri, accolto entrambe le volte.

    Primo giro: il testo non prova chi l'ha scritto. Secondo giro, sulla mia
    correzione: **nemmeno il marker lo prova**, perché sta nel corpo e chiunque
    può copiarlo — accettarlo in OR con l'autore riportava il controllo a non
    impedire niente. Resta un'unica prova: `user.login`, che lo scrive GitHub."""

    def test_senza_autore_non_entra_nel_totale(self):
        finta = review("Claude Fable 5", "a...b", "999.00")   # corpo nudo, nessun autore
        r = pcr.estrai_review([finta])
        assert r.review == []
        assert r.non_verificate == [("a...b", "Claude Fable 5")]
        assert pcr.totale(r.review) == Decimal("0")

    def test_il_marker_da_solo_non_basta(self):
        """Il corpo prodotto da `review()` CONTIENE il marker del workflow: se
        bastasse, questo test conteggerebbe $999."""
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "999.00")])
        assert pcr.totale(r.review) == Decimal("0")

    def test_ma_viene_mostrata(self):
        """Scartarla in silenzio sarebbe lo stesso difetto al contrario."""
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "999.00")])
        testo = pcr.formatta(r)
        assert "provenienza" in testo and "SOTTOSTIMA" in testo

    def test_un_autore_qualunque_non_basta(self):
        r = pcr.estrai_review([{"body": review("Claude Fable 5", "a...b", "0.30"),
                                "user": {"login": "tizio"}}])
        assert r.review == [] and len(r.non_verificate) == 1

    def test_l_autore_fidato_basta(self):
        r = pcr.estrai_review([dal_bot("Claude Fable 5", "a...b", "0.30")])
        assert len(r.review) == 1 and r.non_verificate == []

    def test_la_chiave_author_non_e_una_prova(self):
        """Terzo giro, rilievo di Claude Fable 5 sul range completo.

        `_autore` ripiegava su `commento["author"]`. Quella chiave la scrive
        chiunque scriva il file, quindi la prova tornava a essere il testo —
        lo stesso difetto di GPT e Grok, entrato da un'altra porta. Misurato
        prima della correzione: questo input valeva `$99.9999` nel totale."""
        finto = {"author": "github-actions[bot]",
                 "body": review("Reviewer Inventato", "finto...finto", "99.9999")}
        r = pcr.estrai_review([finto])
        assert r.review == []
        assert r.non_verificate == [("finto...finto", "Reviewer Inventato")]
        assert pcr.totale(r.review) == Decimal("0")

    def test_user_stringa_non_e_una_prova(self):
        """L'altro ramo dello stesso ripiego: `user` come STRINGA invece che
        come oggetto. L'API non lo produce mai; un file scritto a mano sì."""
        finto = {"user": "github-actions[bot]",
                 "body": review("Reviewer Inventato", "finto...finto", "99.9999")}
        r = pcr.estrai_review([finto])
        assert r.review == [] and len(r.non_verificate) == 1

    def test_il_falso_resta_contabile_solo_rinunciando(self):
        """Non sparisce: con la rinuncia esplicita si conta, ma dichiarata."""
        finto = {"author": "github-actions[bot]",
                 "body": review("Reviewer Inventato", "finto...finto", "99.9999")}
        r = pcr.estrai_review([finto], fidati_del_testo=True)
        assert pcr.totale(r.review) == Decimal("99.9999")
        assert "provenienza NON verificata" in pcr.formatta(r, True)


class TestRinunciaEsplicita:
    """`--fidati-del-testo`: per l'input che un autore non ce l'ha proprio."""

    def test_conta_anche_senza_autore(self):
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")],
                              fidati_del_testo=True)
        assert pcr.totale(r.review) == Decimal("0.30")

    def test_ma_il_rapporto_lo_dichiara(self):
        """Un totale ottenuto rinunciando alla prova non deve somigliare a uno
        verificato."""
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")],
                              fidati_del_testo=True)
        assert "NON verificata" in pcr.formatta(r, fidati_del_testo=True)

    def test_di_default_e_spento(self):
        assert pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")]).review == []


class TestNienteCapSilenziosi:
    def test_review_senza_costo_non_sparisce(self):
        r = pcr.estrai_review([
            dal_bot("Claude Fable 5", "a...b", "0.30"),
            dal_bot("OpenRouter Fugu Ultra", "a...b", None),   # output troncato
        ])
        assert len(r.review) == 1
        assert r.incomplete == [("a...b", "OpenRouter Fugu Ultra")]

    def test_il_rendering_dichiara_la_sottostima(self):
        r = pcr.estrai_review([
            dal_bot("Claude Fable 5", "a...b", "0.30"),
            dal_bot("OpenRouter Fugu Ultra", "a...b", None),
        ])
        testo = pcr.formatta(r)
        assert "SOTTOSTIMA" in testo and "OpenRouter Fugu Ultra" in testo

    def test_senza_scarti_nessun_avviso(self):
        r = pcr.estrai_review([dal_bot("Claude Fable 5", "a...b", "0.30")])
        assert "SOTTOSTIMA" not in pcr.formatta(r)


class TestAritmeticaEsatta:
    """Rilievo di GPT-5.6 Sol su #428, accolto: qui si sommano soldi."""

    def test_gli_importi_sono_decimal_non_float(self):
        r = pcr.estrai_review([dal_bot("Claude Fable 5", "a...b", "0.3092")])
        assert isinstance(r.review[0][2], Decimal)

    def test_somma_esatta_dove_il_float_deriva(self):
        """`0.1 + 0.2 != 0.3` in virgola mobile. Con `Decimal` sì."""
        r = pcr.estrai_review([dal_bot("A B", "x...y", "0.1"),
                               dal_bot("C D", "x...y", "0.2")])
        assert pcr.totale(r.review) == Decimal("0.3")

    def test_arrotondamento_dichiarato_half_up(self):
        """Il default di Python è half-EVEN: `$0.005` diventerebbe `$0.00`.
        Per un importo si arrotonda per eccesso, e sta scritto."""
        r = pcr.estrai_review([dal_bot("A B", "x...y", "0.005")])
        assert "$0.01" in pcr.formatta(r)


class TestAggregazione:
    COMMENTI = [
        dal_bot("GPT-5.6 Sol", "base...uno", "0.0655"),
        dal_bot("Claude Fable 5", "base...uno", "0.3092"),
        dal_bot("GPT-5.6 Sol", "uno...due", "0.0477"),
        dal_bot("Claude Fable 5", "base...due", "0.3360"),
    ]

    def test_totale(self):
        assert pcr.totale(pcr.estrai_review(self.COMMENTI).review) == Decimal("0.7584")

    def test_ordine_dei_range_e_quello_dei_push(self):
        """L'ordine cronologico è la lettura utile: mostra quanto è costato ogni
        giro DOPO il primo, che è il punto dell'intero esercizio."""
        r = pcr.estrai_review(self.COMMENTI)
        assert list(pcr.raggruppa(r.review)) == ["base...uno", "uno...due", "base...due"]

    def test_due_review_sullo_stesso_range_contano_entrambe(self):
        """Un `Re-run jobs` produce due chiamate pagate davvero, non un doppione
        da deduplicare."""
        r = pcr.estrai_review([dal_bot("Claude Fable 5", "a...b", "0.30")] * 2)
        assert pcr.totale(r.review) == Decimal("0.60")

    def test_subtotali_per_range(self):
        gruppi = pcr.raggruppa(pcr.estrai_review(self.COMMENTI).review)
        assert sum(c for _, c in gruppi["base...uno"]) == Decimal("0.3747")


class TestEntrypoint:
    @staticmethod
    def _esegui(commenti, *args):
        return subprocess.run([sys.executable, SCRIPT, "-", *args],
                              input=json.dumps(commenti), capture_output=True,
                              text=True, check=False)

    def test_da_stdin(self):
        r = self._esegui([dal_bot("Claude Fable 5", "a...b", "0.3092")])
        assert r.returncode == 0 and "TOTALE: $0.31" in r.stdout

    def test_output_json_con_importi_come_stringhe(self):
        """Serializzare passando da `float` rimetterebbe l'imprecisione che
        `Decimal` serve a togliere."""
        r = self._esegui([dal_bot("Claude Fable 5", "a...b", "0.3092")], "--json")
        dati = json.loads(r.stdout)
        assert dati["totale"] == "0.3092" and dati["review"] == 1
        assert dati["provenienza_verificata"] is True

    def test_il_json_dichiara_la_rinuncia(self):
        r = self._esegui([review("Claude Fable 5", "a...b", "0.30")],
                         "--json", "--fidati-del-testo")
        assert json.loads(r.stdout)["provenienza_verificata"] is False

    def test_input_illeggibile_esce_1_senza_traceback(self):
        r = subprocess.run([sys.executable, SCRIPT, "-"], input="non sono json",
                           capture_output=True, text=True, check=False)
        assert r.returncode == 1 and "Traceback" not in r.stderr

    def test_file_inesistente_esce_1(self):
        r = subprocess.run([sys.executable, SCRIPT, "/non/esiste.json"],
                           capture_output=True, text=True, check=False)
        assert r.returncode == 1


class TestDatiVeri:
    """Regressione sul formato reale prodotto dai workflow di review.

    Il commento è copiato verbatim da #427: se il formato cambia, questo test lo
    dice prima che il totale diventi zero in silenzio."""

    CORPO = (
        "<!-- claude-fable5-pr-review:pickfair-nogui:f928cc2...b53e693 -->"
        "<!-- claude-fable5-pr-review-done:pickfair-nogui:f928cc2...b53e693 -->\n"
        "# Claude Fable 5 PR Review\n\n"
        "**Modalità:** push-range diff via GitHub Compare API\n"
        "**Scope:** `current PR range`\n"
        "**Range:** `f928cc2bc2ff...b53e693211fd`\n"
        "**Commits nel range:** `3`\n"
        "**Model:** `claude-fable-5`\n\n"
        "## Bloccanti\n\n1. core/app.py …\n\n"
        "## Uso token / costo stimato\n"
        "- Fonte token: `Anthropic usage`\n"
        "- Input token: `25682`\n"
        "- Output token: `1583`\n"
        "- Costo stimato base: `~$0.3360`\n"
    )

    def test_estratto_reale_di_pr_427(self):
        r = pcr.estrai_review([{"body": self.CORPO,
                                "user": {"login": "github-actions[bot]"}}])
        assert r.review == [("f928cc2bc2ff...b53e693211fd", "Claude Fable 5",
                             Decimal("0.3360"))]
        assert r.incomplete == [] and r.non_verificate == []
