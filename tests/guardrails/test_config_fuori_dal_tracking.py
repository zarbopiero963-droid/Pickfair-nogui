"""`config.json` non deve tornare nel repo, e il modello non deve diventarne una copia.

R1 di #374. Il file tracciato portava credenziali VERE — username, password e app
key Betfair piu' host, utente e password del proxy — ed e' rimasto committato
mentre il repository diventava **pubblico**.

Perche' questi test e non solo una riga di `.gitignore`: `.gitignore` non protegge
un file **gia' tracciato**, e un `git add -f` lo rimette dentro senza che nessuno
se ne accorga. Un test che gira in CI se ne accorge.

Cosa NON fanno questi test, e va detto: non rendono sicure le credenziali gia'
finite nella storia. Quelle si neutralizzano solo **ruotandole**, ed e' un'azione
dell'owner (registrata su #426). Qui si impedisce che il problema si ripeta.
"""

from __future__ import annotations

import json
import pathlib
import subprocess

RADICE = pathlib.Path(__file__).resolve().parents[2]

#: Campi il cui valore, se finisse nel modello, sarebbe un segreto vero.
CAMPI_SEGRETI = frozenset({
    "username", "password", "app_key", "host",
    "api_hash", "session_string", "certificate", "private_key",
})


def _tracciati() -> set:
    """File noti a git, letti da git stesso invece che dedotti dal disco."""
    out = subprocess.run(["git", "ls-files"], cwd=RADICE, check=True,
                         capture_output=True, text=True).stdout
    return set(out.splitlines())


class TestConfigNonTracciato:
    def test_config_json_non_e_tracciato(self):
        """Il punto di tutta la PR. Se questo diventa rosso, qualcuno ha rimesso
        nel repo un file che porta credenziali vere."""
        assert "config.json" not in _tracciati(), (
            "config.json e' tornato sotto controllo di versione: contiene "
            "credenziali reali e il repository e' pubblico."
        )

    def test_gitignore_lo_copre_ancorato_alla_radice(self):
        righe = (RADICE / ".gitignore").read_text(encoding="utf-8").splitlines()
        assert "/config.json" in [r.strip() for r in righe], (
            "manca `/config.json` in .gitignore. Deve essere ancorato alla radice: "
            "`config.json` senza slash ignorerebbe anche i file di test in "
            "sottocartella, che invece vanno versionati."
        )


class TestModelloSenzaSegreti:
    def test_il_modello_esiste_ed_e_json_valido(self):
        json.loads((RADICE / "config.example.json").read_text(encoding="utf-8"))

    def test_il_modello_non_contiene_valori_sensibili(self):
        """Ogni campo sensibile del modello deve essere un segnaposto `<COSI>`.

        Serve al caso in cui qualcuno aggiorni il modello copiandoci dentro il
        proprio config vero — l'errore piu' facile da fare, e il piu' silenzioso.
        """
        modello = json.loads((RADICE / "config.example.json").read_text(encoding="utf-8"))
        colpevoli = []

        def guarda(nodo, percorso=""):
            if isinstance(nodo, dict):
                for chiave, valore in nodo.items():
                    guarda(valore, f"{percorso}.{chiave}" if percorso else chiave)
                return
            nome = percorso.split(".")[-1]
            if nome in CAMPI_SEGRETI and isinstance(nodo, str) and nodo:
                if not (nodo.startswith("<") and nodo.endswith(">")):
                    colpevoli.append(percorso)

        guarda(modello)
        assert not colpevoli, (
            f"config.example.json ha valori non-segnaposto in campi sensibili: "
            f"{colpevoli}. Devono essere della forma `<NOME>`."
        )

    def test_il_modello_copre_i_campi_che_il_codice_legge(self):
        """`betfair_client.py` legge il blocco proxy: se il modello non lo
        descrive, chi installa non sa cosa deve compilare."""
        modello = json.loads((RADICE / "config.example.json").read_text(encoding="utf-8"))
        proxy = modello.get("proxy", {})
        for campo in ("enabled", "type", "host", "port", "username", "password"):
            assert campo in proxy, f"config.example.json: manca proxy.{campo}"
        for campo in ("username", "password", "app_key"):
            assert campo in modello.get("betfair", {}), f"manca betfair.{campo}"
