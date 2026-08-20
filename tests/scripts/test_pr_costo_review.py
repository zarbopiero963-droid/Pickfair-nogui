"""Costo review per range — `scripts/pr_costo_review.py`.

Due test portano il peso, e per lo stesso motivo: uno script che riassume soldi
può sbagliare in due direzioni, e solo una si vede. Un totale troppo ALTO stona
subito; un totale troppo BASSO — perché una review non è stata letta — si legge
come una buona notizia, e resta lì. Quindi:

- `test_review_senza_costo_non_sparisce` copre ciò che non si è riusciti a
  leggere;
- `test_review_senza_provenienza_non_entra_nel_totale` copre ciò che non si
  dovrebbe credere.

In entrambi i casi la cosa scartata viene MOSTRATA: scartare in silenzio è lo
stesso difetto, girato al contrario.
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


def review(reviewer, rng, costo=None, extra="", marker=True):
    """Un commento di review come lo scrive il workflow, ridotto all'essenziale."""
    corpo = ""
    if marker:
        # I workflow reali usano id senza punteggiatura ("gpt56sol", "claude-fable5"):
        # la fixture li imita, altrimenti proverebbe il marker sbagliato.
        ident = "".join(c for c in reviewer.lower() if c.isalnum())
        corpo += f"<!-- {ident}-pr-review:pickfair-nogui:{rng} -->\n"
    corpo += (f"# {reviewer} PR Review\n\n"
              f"**Modalità:** push-range diff via GitHub Compare API\n"
              f"**Range:** `{rng}`\n\n"
              f"## Bloccanti\nNessun bloccante evidente\n{extra}\n")
    if costo is not None:
        corpo += ("\n## Uso token / costo stimato\n"
                  "- Total token: `1234`\n"
                  f"- Costo stimato base: `~${costo}`\n")
    return corpo


class TestEstrazione:
    def test_legge_reviewer_range_e_costo(self):
        r = pcr.estrai_review([review("Claude Fable 5", "aaa...bbb", "0.3092")])
        assert r.review == [("aaa...bbb", "Claude Fable 5", Decimal("0.3092"))]
        assert r.incomplete == [] and r.non_verificate == []

    def test_accetta_sia_dict_dell_api_sia_stringhe(self):
        c = review("xAI Grok 4.6", "aaa...bbb", "0.0410")
        assert pcr.estrai_review([{"body": c}]).review == pcr.estrai_review([c]).review

    def test_ignora_i_commenti_che_non_sono_review(self):
        """Non sono sospetti: sono semplicemente altro, e non vanno segnalati."""
        rumore = ["## Verdetto dell'autore\nPronta.",
                  "<!-- coderabbitai -->\nWalkthrough…",
                  "You have reached your Codex usage limits."]
        r = pcr.estrai_review(rumore)
        assert r.review == [] and r.incomplete == [] and r.non_verificate == []

    def test_un_reviewer_nuovo_entra_nel_conto_da_solo(self):
        """Il nome si cattura fra '#' e 'PR Review' invece di elencare i modelli:
        un reviewer aggiunto in futuro non deve sparire dal totale perché nessuno
        si è ricordato di aggiornare questo script."""
        r = pcr.estrai_review([review("Modello Che Non Esiste Ancora", "a...b", "1.00")])
        assert r.review == [("a...b", "Modello Che Non Esiste Ancora", Decimal("1.00"))]

    def test_review_senza_range_e_comunque_conteggiata(self):
        senza = ("<!-- gpt56sol-pr-review:x -->\n# GPT-5.6 Sol PR Review\n\n"
                 "Costo stimato base: `~$0.05`\n")
        r = pcr.estrai_review([senza])
        assert r.review == [(pcr.SENZA_RANGE, "GPT-5.6 Sol", Decimal("0.05"))]

    def test_review_fallita_con_costo_conta_lo_stesso(self):
        """Un giro andato in 429 riporta comunque un costo: è stato speso."""
        fallita = review("xAI Grok 4.6", "a...b", "0.0087",
                         extra="## Review non completata\nHTTP 429: resource-exhausted")
        assert pcr.estrai_review([fallita]).review[0][2] == Decimal("0.0087")


class TestProvenienza:
    """Rilievo di GPT-5.6 Sol su #428, accolto.

    Il testo di un commento non prova chi l'ha scritto. Senza un controllo,
    chiunque possa commentare potrebbe incollare un'intestazione di review e un
    costo inventato, e il numero mostrato all'owner cambierebbe."""

    def test_review_senza_provenienza_non_entra_nel_totale(self):
        finta = review("Claude Fable 5", "a...b", "999.00", marker=False)
        r = pcr.estrai_review([finta])
        assert r.review == []
        assert r.non_verificate == [("a...b", "Claude Fable 5")]
        assert pcr.totale(r.review) == Decimal("0")

    def test_ma_viene_mostrata(self):
        """Scartarla in silenzio sarebbe lo stesso difetto al contrario."""
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "999.00", marker=False)])
        testo = pcr.formatta(r)
        assert "provenienza" in testo and "SOTTOSTIMA" in testo

    def test_il_marker_del_workflow_basta(self):
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")])
        assert len(r.review) == 1 and r.non_verificate == []

    def test_anche_l_autore_fidato_basta(self):
        """Copre un workflow futuro che smettesse di emettere il marker."""
        senza_marker = review("Claude Fable 5", "a...b", "0.30", marker=False)
        r = pcr.estrai_review([{"body": senza_marker,
                                "user": {"login": "github-actions[bot]"}}])
        assert len(r.review) == 1 and r.non_verificate == []

    def test_un_autore_qualunque_non_basta(self):
        senza_marker = review("Claude Fable 5", "a...b", "0.30", marker=False)
        r = pcr.estrai_review([{"body": senza_marker, "user": {"login": "tizio"}}])
        assert r.review == [] and len(r.non_verificate) == 1


class TestNienteCapSilenziosi:
    def test_review_senza_costo_non_sparisce(self):
        r = pcr.estrai_review([
            review("Claude Fable 5", "a...b", "0.30"),
            review("OpenRouter Fugu Ultra", "a...b", None),   # output troncato
        ])
        assert len(r.review) == 1
        assert r.incomplete == [("a...b", "OpenRouter Fugu Ultra")]

    def test_il_rendering_dichiara_la_sottostima(self):
        r = pcr.estrai_review([
            review("Claude Fable 5", "a...b", "0.30"),
            review("OpenRouter Fugu Ultra", "a...b", None),
        ])
        testo = pcr.formatta(r)
        assert "SOTTOSTIMA" in testo and "OpenRouter Fugu Ultra" in testo

    def test_senza_scarti_nessun_avviso(self):
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")])
        assert "SOTTOSTIMA" not in pcr.formatta(r)


class TestAritmeticaEsatta:
    """Rilievo di GPT-5.6 Sol su #428, accolto: qui si sommano soldi."""

    def test_gli_importi_sono_decimal_non_float(self):
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.3092")])
        assert isinstance(r.review[0][2], Decimal)

    def test_somma_esatta_dove_il_float_deriva(self):
        """`0.1 + 0.2 != 0.3` in virgola mobile. Con `Decimal` sì."""
        r = pcr.estrai_review([
            review("A B", "x...y", "0.1"),
            review("C D", "x...y", "0.2"),
        ])
        assert pcr.totale(r.review) == Decimal("0.3")

    def test_arrotondamento_dichiarato_half_up(self):
        """Il default di Python è half-EVEN: `$0.005` diventerebbe `$0.00`.
        Per un importo si arrotonda per eccesso, e sta scritto."""
        r = pcr.estrai_review([review("A B", "x...y", "0.005")])
        assert "$0.01" in pcr.formatta(r)


class TestAggregazione:
    COMMENTI = [
        review("GPT-5.6 Sol", "base...uno", "0.0655"),
        review("Claude Fable 5", "base...uno", "0.3092"),
        review("GPT-5.6 Sol", "uno...due", "0.0477"),
        review("Claude Fable 5", "base...due", "0.3360"),
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
        r = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")] * 2)
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
        r = self._esegui([review("Claude Fable 5", "a...b", "0.3092")])
        assert r.returncode == 0 and "TOTALE: $0.31" in r.stdout

    def test_output_json_con_importi_come_stringhe(self):
        """Serializzare passando da `float` rimetterebbe l'imprecisione che
        `Decimal` serve a togliere."""
        r = self._esegui([review("Claude Fable 5", "a...b", "0.3092")], "--json")
        dati = json.loads(r.stdout)
        assert dati["totale"] == "0.3092" and dati["review"] == 1

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

    Il commento qui sotto è copiato verbatim da #427, marker incluso: se il
    formato cambia, questo test lo dice prima che il totale diventi zero in
    silenzio."""

    def test_estratto_reale_di_pr_427(self):
        vero = (
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
        r = pcr.estrai_review([vero])
        assert r.review == [("f928cc2bc2ff...b53e693211fd", "Claude Fable 5",
                             Decimal("0.3360"))]
        assert r.incomplete == [] and r.non_verificate == []
