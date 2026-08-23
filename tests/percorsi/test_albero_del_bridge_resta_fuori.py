"""L'albero di `core/app.py` non deve rientrare, e la prova sta qui — non in una descrizione.

Quattro reviewer sulla PR #435 hanno sollevato la stessa obiezione, ed era fondata:
la cancellazione di 45 moduli era dichiarata non-raggiungibile nel corpo della PR,
ma il diff non lo dimostra (i file cancellati non entrano nel budget dei modelli, e
una descrizione non e' una prova). Fugu ha aggiunto tre paure concrete: senza
`log_privacy` i segreti finiscono in chiaro nei log, senza `real_mode` si perde la
distinzione simulazione/reale, senza `validator`/`signal_router` cadono i gate
fail-closed sul denaro.

Questo file risponde con fatti eseguibili invece che con parole:

1. i 45 moduli non esistono piu' come file;
2. nessun modulo del repository li importa — verificato con `ast`, non con `grep`,
   cosi' una menzione in un commento non conta e un import dentro una funzione si';
3. gli entrypoint veri di Pickfair restano importabili e il grafo vivo non li tocca;
4. le tre capacita' che Fugu temeva perse sono ancora nel percorso vivo, ognuna con
   il suo modulo. Se una futura cancellazione portasse via l'ultimo redattore di
   segreti o l'ultimo gate, questo test diventa rosso — che e' esattamente il punto.

Perche' un elenco scritto a mano e non ricalcolato dal grafo: un test che ricalcola
la stessa cosa che ha deciso la cancellazione approverebbe se' stesso. L'elenco e'
il contratto; il grafo e' il controllo.
"""

from __future__ import annotations

import ast
import pathlib
import sys

import pytest

RADICE = pathlib.Path(__file__).resolve().parents[2]

ENTRYPOINT = ("main", "mini_gui", "headless_main")

# I 45 moduli tolti dalla PR #435. Recuperabili con `git show 530c6f7:core/app.py`.
ALBERO_DEL_BRIDGE = frozenset({
    "core.app",
    "core.config_summary", "core.config_summary_gui", "core.csv_lock_escalation",
    "core.custom_parser_gui", "core.custom_pipeline", "core.dashboard_stats",
    "core.diagnostics", "core.dpi_awareness", "core.gui_utils",
    "core.guided_mapping_gui", "core.health_check", "core.i18n",
    "core.instance_lock", "core.journal_view_gui", "core.known_teams_gui",
    "core.live_guard", "core.log_privacy", "core.log_view",
    "core.market_mapping_store", "core.message_freshness", "core.multi_signal",
    "core.name_mapping_gui", "core.name_mapping_store", "core.parser_builder",
    "core.parser_diagnostics", "core.parser_manager", "core.profile_store",
    "core.profiles_gui", "core.provider_gui", "core.provider_store",
    "core.real_mode", "core.reconnect_policy", "core.settings_controller",
    "core.settings_validation", "core.signal_outcome", "core.signal_router",
    "core.source_chats_gui", "core.source_editor", "core.telegram_dispatch",
    "core.tools_gui", "core.validator", "core.wizard", "core.wizard_gui",
    "core.write_path",
})

# Le capacita' che i reviewer temevano di perdere, e il modulo VIVO che le tiene.
# Non basta che il file esista: deve stare nel grafo vivo e definire quel nome.
# NOTA, scritta perche' questo test l'ha scoperto e non va persa: `core.event_log`
# NON e' in questo elenco pur essendo il redattore piu' forte del repository
# (`redact_secrets`, deny-list da valori vivi). Il motivo e' che non e' raggiungibile
# da Pickfair — e non lo era neanche prima della PR #435: e' il debito gia' annotato
# nel follow-up del Recorder su #374 («il bundle diagnostico usa
# observability/sanitizers.py, redazione per chiave, debole»). Metterlo qui farebbe
# fallire il test per una colpa che questa PR non ha; ometterlo in silenzio sarebbe
# peggio. Va chiuso nel blocco W, non qui.
CAPACITA_CHE_DEVONO_RESTARE = (
    ("redazione del testo Telegram", "telegram_listener", "_redact_sensitive"),
    ("redazione degli errori Betfair", "betfair_client", "_redact_error_text"),
    ("distinzione simulazione / reale", "mini_gui", "_apply_simulation_mode_to_runtime"),
    ("gate di rischio fail-closed", "core.trading_engine", "_risk_gate"),
    ("dedup prima dell'ordine", "core.trading_engine", "_dedup_allow"),
)


def _moduli() -> dict[str, pathlib.Path]:
    """Ogni modulo Python del repository, escluse le cartelle non sorgente."""
    fuori = {".git", ".venv", "venv", "build", "dist"}
    trovati: dict[str, pathlib.Path] = {}
    for percorso in RADICE.rglob("*.py"):
        parti = percorso.relative_to(RADICE).parts
        if parti[0] in fuori or "__pycache__" in parti:
            continue
        nome = ".".join(parti)[:-3]
        if nome.endswith(".__init__"):
            nome = nome[:-9]
        trovati[nome] = percorso
    return trovati


def _pacchetto_risalito(percorso: pathlib.Path, livello: int) -> str:
    """Package di partenza di un import relativo, risalito di `livello`-1 gradini.

    `from . import x` (livello 1) parte dal package del file; `from .. import x`
    (livello 2) da quello sopra, e cosi' via. Rilievo di GPT-5.6 Sol sulla #435,
    accolto: la versione precedente incollava il package davanti al modulo senza
    risalire, quindi da `a/b/mod.py` un `from ..core import app` diventava
    `a.b.core.app` invece di `a.core.app` — una dipendenza viva che il grafo NON
    vedeva. Oggi nel repository non esiste nessun import con livello > 1, quindi il
    difetto e' latente e non ha influito su nessuna decisione; resta un fail-open su
    un controllo di sicurezza, e i fail-open qui si chiudono anche da latenti.
    """
    parti = percorso.relative_to(RADICE).parts[:-1]
    risalita = livello - 1
    if risalita > len(parti):
        raise AssertionError(
            f"{percorso}: import relativo di livello {livello} oltre la radice del "
            "repository — non risolvibile, e un grafo che tira a indovinare qui "
            "autorizzerebbe una cancellazione sbagliata")
    return ".".join(parti[:len(parti) - risalita])


def _importati(percorso: pathlib.Path) -> set[str]:
    """Nomi importati da un file, relativi risolti. `ast`, non `grep`.

    Un file che non si analizza **non** vale «nessun import»: secondo rilievo di
    GPT-5.6 Sol, accolto. Prima `except (OSError, SyntaxError): return set()`
    trasformava un file illeggibile in un file innocuo — cioe' proprio il falso
    verde che questo controllo esiste per impedire. Ora salta fuori, e dice quale.
    """
    try:
        sorgente = percorso.read_text(encoding="utf-8")
    except OSError as errore:
        raise AssertionError(
            f"{percorso}: non leggibile ({errore.__class__.__name__}). Un file che "
            "non si legge non e' un file senza import: il grafo non e' attendibile."
        ) from errore
    try:
        albero = ast.parse(sorgente)
    except SyntaxError as errore:
        raise AssertionError(
            f"{percorso}: non analizzabile ({errore}). Un file che non si analizza "
            "non e' un file senza import: il grafo non e' attendibile.") from errore

    fuori: set[str] = set()
    for nodo in ast.walk(albero):
        if isinstance(nodo, ast.Import):
            fuori.update(alias.name for alias in nodo.names)
        elif isinstance(nodo, ast.ImportFrom):
            base = nodo.module or ""
            if nodo.level:
                radice_rel = _pacchetto_risalito(percorso, nodo.level)
                base = f"{radice_rel}.{base}" if (radice_rel and base) else (radice_rel or base)
            if base:
                fuori.add(base)
                fuori.update(f"{base}.{alias.name}" for alias in nodo.names)
    return fuori


def _grafo_vivo(moduli: dict[str, pathlib.Path]) -> set[str]:
    """Chiusura transitiva degli import dai veri entrypoint di Pickfair."""
    from collections import deque

    vivi = {m for m in ENTRYPOINT if m in moduli}
    coda = deque(vivi)
    while coda:
        for nome in _importati(moduli[coda.popleft()]):
            if nome in moduli and nome not in vivi:
                vivi.add(nome)
                coda.append(nome)
    return vivi


@pytest.mark.parametrize("modulo", sorted(ALBERO_DEL_BRIDGE))
def test_il_file_non_e_tornato(modulo):
    """Uno per uno, cosi' il fallimento dice QUALE e' rientrato."""
    assert modulo not in _moduli(), (
        f"{modulo} e' tornato nel repository: era parte dell'albero di core/app.py, "
        "la GUI di XTrader Signal Bridge")


def test_nessun_modulo_del_repository_importa_l_albero():
    """Il controllo che vale per tutto il repository, test e script compresi.

    Non solo il percorso vivo: se un test o uno script di CI importasse uno dei 45,
    la suite si romperebbe con ImportError e questo test lo dice prima, dicendo chi.
    """
    moduli = _moduli()
    colpevoli = sorted(
        (nome, sorted(_importati(percorso) & ALBERO_DEL_BRIDGE))
        for nome, percorso in moduli.items()
        if _importati(percorso) & ALBERO_DEL_BRIDGE
    )
    assert not colpevoli, f"moduli che importano l'albero cancellato: {colpevoli}"


def test_gli_entrypoint_di_pickfair_stanno_in_piedi():
    """Il grafo vivo esiste, parte dai tre entrypoint e non contiene l'albero."""
    moduli = _moduli()
    for nome in ENTRYPOINT:
        assert nome in moduli, f"entrypoint mancante: {nome}"
    vivi = _grafo_vivo(moduli)
    assert vivi >= set(ENTRYPOINT)
    assert not (vivi & ALBERO_DEL_BRIDGE)
    # Un grafo vuoto o quasi passerebbe le due righe sopra senza dire nulla.
    assert len(vivi) > 50, f"grafo vivo sospettosamente piccolo: {len(vivi)} moduli"


@pytest.mark.parametrize("capacita,modulo,simbolo", CAPACITA_CHE_DEVONO_RESTARE)
def test_la_capacita_e_ancora_nel_percorso_vivo(capacita, modulo, simbolo):
    """Risposta ai bloccanti di Fugu, uno per uno.

    `core/log_privacy.py` e `core/real_mode.py` erano i moduli del **Bridge**;
    Pickfair ha i propri, e stanno nel grafo vivo. Qui non si controlla che il file
    esista — si controlla che sia VIVO e che definisca ancora quel nome.
    """
    moduli = _moduli()
    assert modulo in moduli, f"{capacita}: modulo {modulo} sparito"
    assert modulo in _grafo_vivo(moduli), (
        f"{capacita}: {modulo} esiste ma non e' piu' raggiungibile da Pickfair")
    albero = ast.parse(moduli[modulo].read_text(encoding="utf-8"))
    definiti = {
        n.name for n in ast.walk(albero)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert simbolo in definiti, f"{capacita}: {modulo} non definisce piu' {simbolo}"


def test_ogni_file_del_repository_e_analizzabile():
    """Il grafo vale solo se ha letto tutto: nessun file puo' essere saltato.

    `_importati` solleva su file illeggibili o non analizzabili invece di
    restituire un insieme vuoto; questo test lo esercita su TUTTO il repository,
    cosi' il fallimento arriva qui, con il nome del file, e non nascosto dentro
    un controllo di raggiungibilita' che sembrerebbe verde.
    """
    for nome, percorso in sorted(_moduli().items()):
        _importati(percorso)


def test_la_risalita_degli_import_relativi_e_corretta(tmp_path, monkeypatch):
    """`from ..core import app` deve risolvere fuori dal package del file.

    Senza questo, il difetto segnalato da GPT-5.6 Sol tornerebbe silenzioso il
    giorno che qualcuno scrive il primo import di livello 2 del repository.
    """
    monkeypatch.setattr(sys.modules[__name__], "RADICE", tmp_path)
    (tmp_path / "a" / "b").mkdir(parents=True)
    modulo = tmp_path / "a" / "b" / "mod.py"
    modulo.write_text(
        "from . import vicino\n"
        "from ..core import app\n"
        "from ...radice import cosa\n",
        encoding="utf-8")
    trovati = _importati(modulo)
    assert "a.b.vicino" in trovati, f"livello 1 sbagliato: {sorted(trovati)}"
    assert "a.core.app" in trovati, f"livello 2 sbagliato: {sorted(trovati)}"
    assert "radice.cosa" in trovati, f"livello 3 sbagliato: {sorted(trovati)}"


def test_un_file_non_analizzabile_fa_fallire_invece_di_valere_zero_import(tmp_path, monkeypatch):
    """Il fail-open chiuso, verificato: prima questo caso tornava `set()`."""
    monkeypatch.setattr(sys.modules[__name__], "RADICE", tmp_path)
    rotto = tmp_path / "rotto.py"
    rotto.write_text("def (: questo non e' Python\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="non analizzabile"):
        _importati(rotto)
