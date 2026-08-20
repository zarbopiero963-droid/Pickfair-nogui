"""Costo review per range — `scripts/pr_costo_review.py`.

Il test che conta davvero è `test_review_senza_costo_non_sparisce`: uno script
che somma numeri può sbagliare in due modi, e solo uno dei due si vede. Un
totale troppo ALTO stona subito; un totale troppo BASSO, perché una review non
è stata letta, si legge come una buona notizia. L'invariante «niente cap
silenziosi» è ciò che separa i due casi.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

RADICE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RADICE))

from scripts import pr_costo_review as pcr  # noqa: E402

pytestmark = pytest.mark.unit


def review(reviewer, rng, costo=None, extra=""):
    """Un commento di review come lo scrive il workflow, ridotto all'essenziale."""
    corpo = (f"# {reviewer} PR Review\n\n"
             f"**Modalità:** push-range diff via GitHub Compare API\n"
             f"**Range:** `{rng}`\n\n"
             f"## Bloccanti\nNessun bloccante evidente\n{extra}\n")
    if costo is not None:
        corpo += ("\n## Uso token / costo stimato\n"
                  f"- Total token: `1234`\n"
                  f"- Costo stimato base: `~${costo}`\n")
    return corpo


class TestEstrazione:
    def test_legge_reviewer_range_e_costo(self):
        trovate, mancanti = pcr.estrai_review([review("Claude Fable 5", "aaa...bbb", "0.3092")])
        assert trovate == [("aaa...bbb", "Claude Fable 5", 0.3092)]
        assert mancanti == []

    def test_accetta_sia_dict_dell_api_sia_stringhe(self):
        c = review("xAI Grok 4.6", "aaa...bbb", "0.0410")
        da_dict, _ = pcr.estrai_review([{"body": c}])
        da_str, _ = pcr.estrai_review([c])
        assert da_dict == da_str

    def test_ignora_i_commenti_che_non_sono_review(self):
        rumore = ["## Verdetto dell'autore\nPronta.",
                  "<!-- coderabbitai -->\nWalkthrough…",
                  "You have reached your Codex usage limits."]
        trovate, mancanti = pcr.estrai_review(rumore)
        assert trovate == [] and mancanti == []

    def test_un_reviewer_nuovo_entra_nel_conto_da_solo(self):
        """Il nome si cattura fra '#' e 'PR Review' invece di elencare i modelli:
        un reviewer aggiunto in futuro non deve sparire dal totale perché nessuno
        si è ricordato di aggiornare questo script."""
        trovate, _ = pcr.estrai_review([review("Modello Che Non Esiste Ancora", "a...b", "1.00")])
        assert trovate == [("a...b", "Modello Che Non Esiste Ancora", 1.0)]

    def test_review_senza_range_e_comunque_conteggiata(self):
        senza = "# GPT-5.6 Sol PR Review\n\nCosto stimato base: `~$0.05`\n"
        trovate, _ = pcr.estrai_review([senza])
        assert trovate == [(pcr.SENZA_RANGE, "GPT-5.6 Sol", 0.05)]

    def test_review_fallita_con_costo_conta_lo_stesso(self):
        """Un giro andato in 429 riporta comunque un costo: è stato speso."""
        fallita = review("xAI Grok 4.6", "a...b", "0.0087",
                         extra="## Review non completata\nHTTP 429: resource-exhausted")
        trovate, _ = pcr.estrai_review([fallita])
        assert trovate[0][2] == 0.0087


class TestNienteCapSilenziosi:
    def test_review_senza_costo_non_sparisce(self):
        """Una review riconosciuta ma senza riga di costo finisce fra le
        `incomplete`, non nel nulla. Se sparisse, il totale sarebbe più basso del
        vero e nessuno avrebbe modo di accorgersene."""
        trovate, mancanti = pcr.estrai_review([
            review("Claude Fable 5", "a...b", "0.30"),
            review("OpenRouter Fugu Ultra", "a...b", None),   # output troncato
        ])
        assert len(trovate) == 1
        assert mancanti == [("a...b", "OpenRouter Fugu Ultra")]

    def test_il_rendering_dichiara_la_sottostima(self):
        trovate, mancanti = pcr.estrai_review([
            review("Claude Fable 5", "a...b", "0.30"),
            review("OpenRouter Fugu Ultra", "a...b", None),
        ])
        testo = pcr.formatta(trovate, mancanti)
        assert "SOTTOSTIMA" in testo
        assert "OpenRouter Fugu Ultra" in testo

    def test_senza_incomplete_nessun_avviso(self):
        trovate, mancanti = pcr.estrai_review([review("Claude Fable 5", "a...b", "0.30")])
        assert "SOTTOSTIMA" not in pcr.formatta(trovate, mancanti)


class TestAggregazione:
    COMMENTI = [
        review("GPT-5.6 Sol", "base...uno", "0.0655"),
        review("Claude Fable 5", "base...uno", "0.3092"),
        review("GPT-5.6 Sol", "uno...due", "0.0477"),
        review("Claude Fable 5", "base...due", "0.3360"),
    ]

    def test_totale(self):
        trovate, _ = pcr.estrai_review(self.COMMENTI)
        assert pcr.totale(trovate) == pytest.approx(0.7584)

    def test_ordine_dei_range_e_quello_dei_push(self):
        """L'ordine cronologico è la lettura utile: mostra quanto è costato ogni
        giro DOPO il primo, che è il punto dell'intero esercizio."""
        trovate, _ = pcr.estrai_review(self.COMMENTI)
        assert list(pcr.raggruppa(trovate)) == ["base...uno", "uno...due", "base...due"]

    def test_due_review_sullo_stesso_range_contano_entrambe(self):
        """Un `Re-run jobs` produce due chiamate pagate davvero, non un doppione
        da deduplicare."""
        trovate, _ = pcr.estrai_review([
            review("Claude Fable 5", "a...b", "0.30"),
            review("Claude Fable 5", "a...b", "0.30"),
        ])
        assert pcr.totale(trovate) == pytest.approx(0.60)

    def test_subtotali_per_range(self):
        trovate, _ = pcr.estrai_review(self.COMMENTI)
        gruppi = pcr.raggruppa(trovate)
        assert sum(c for _, c in gruppi["base...uno"]) == pytest.approx(0.3747)


class TestEntrypoint:
    def _esegui(self, commenti, *args):
        return subprocess.run(
            [sys.executable, str(RADICE / "scripts" / "pr_costo_review.py"), "-", *args],
            input=json.dumps(commenti), capture_output=True, text=True)

    def test_da_stdin(self):
        r = self._esegui([review("Claude Fable 5", "a...b", "0.3092")])
        assert r.returncode == 0
        assert "TOTALE: $0.31" in r.stdout

    def test_output_json(self):
        r = self._esegui([review("Claude Fable 5", "a...b", "0.3092")], "--json")
        dati = json.loads(r.stdout)
        assert dati["totale"] == 0.3092 and dati["review"] == 1

    def test_input_illeggibile_esce_1_senza_traceback(self):
        r = subprocess.run(
            [sys.executable, str(RADICE / "scripts" / "pr_costo_review.py"), "-"],
            input="non sono json", capture_output=True, text=True)
        assert r.returncode == 1
        assert "Traceback" not in r.stderr


class TestDatiVeri:
    """Regressione sul formato reale prodotto dai workflow di review.

    Le intestazioni qui sotto sono copiate verbatim da #427: se il formato dei
    commenti cambia, questo test lo dice prima che il totale diventi zero in
    silenzio."""

    def test_estratto_reale_di_pr_427(self):
        vero = (
            "<!-- claude-fable5-pr-review:pickfair-nogui:f928cc2...b53e693 -->\n"
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
        trovate, mancanti = pcr.estrai_review([vero])
        assert trovate == [("f928cc2bc2ff...b53e693211fd", "Claude Fable 5", 0.3360)]
        assert mancanti == []
