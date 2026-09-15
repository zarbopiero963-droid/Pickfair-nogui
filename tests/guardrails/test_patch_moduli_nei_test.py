"""Nei test non si muta a mano un attributo di modulo: si usa `monkeypatch`.

FOLLOW-UP #462. `tests/scripts/test_pr_flow_automation.py` installava lo stub di
`flow.pr_view` a mano::

    original = flow.pr_view
    try:
        flow.pr_view = lambda _repo, _pr: pr
        ...
    finally:
        flow.pr_view = original

Era l'unico punto di quel file, su una quindicina, a non usare `monkeypatch`.
Il `try/finally` protegge dalle eccezioni, non dall'interleaving: se un altro
test dello stesso processo legge `flow.pr_view` MENTRE lo stub e' installato, lo
salva come "originale" e poi lo ripristina — lo stub diventa permanente e ogni
test successivo del worker gira su una `pr_view` finta senza che nulla lo dica.
Un test che passa su un mock sbagliato e' indistinguibile da un test che passa.

`monkeypatch` non rende sicura l'esecuzione concorrente in-process (muta
comunque un globale di modulo): toglie la gestione a mano e garantisce il
ripristino, quindi l'avvelenamento permanente non puo' piu' accadere. Per
l'isolamento vero servirebbe un seam di iniezione nel codice di produzione —
scelta dell'owner, non di un test.

Questo guard blocca la comparsa di NUOVI punti. I punti preesistenti sono
elencati qui sotto CON IL LORO NUMERO, non nascosti: e' debito visibile, non una
deroga silenziosa — e un'assegnazione in piu' dentro un file derogato non passa
per il fatto che quel file compare nell'elenco.

FORME DI IMPORT: l'elenco e' ESAURITO. `import x`, `import x as y`, `import x.y`,
`from x import y`, `from x import y as z`, `from . import y`, e `from x import *`
— quest'ultimo fail-closed: con lo star import i nomi legati dipendono dal
runtime, quindi il file viene SEGNALATO invece di passare in silenzio. Non
esistono altre forme di import in Python: questa superficie e' chiusa, non
"chiusa fino al prossimo rilievo".

SOGGETTO DICHIARATO. Il guard parla di una cosa sola: un ATTRIBUTO di modulo
riassegnato o rimosso da una rotta che NOMINA il modulo. Sono coperte tutte:

    legare    = / : = / += / tupla / lista / starred / annidata
              for / async for / with as / comprehension / setattr
    slegare   del / delattr
    riflessive  mod.__dict__ (in qualunque uso) / vars(mod)
                object.__setattr__ / object.__delattr__

Sulle riflessive si segnala QUALUNQUE accesso, non solo la scrittura:
distinguere lettura da scrittura costerebbe un'analisi di flusso, e le
occorrenze legittime in questa suite sono ZERO — misurato, non supposto —
quindi la regola larga non paga falsi positivi.

Restano fuori `sys.modules[...] = ...` e `importlib.reload(mod)`. NON perche'
siano tollerati: perche' non sono mutazioni di un attributo — sostituiscono o
ricostruiscono il modulo intero, che e' un soggetto diverso e ha usi legittimi
(la suite ne ha uno). Un test lo dice, ma dice quello: «questo guard non parla
di loro», non «va bene farlo».

La differenza conta, ed e' un rilievo accolto da GPT-5.6 Sol: una versione
precedente asseriva che `mod.__dict__` NON dovesse essere segnalato, e cosi'
faceva del bypass un comportamento atteso, protetto dalla CI. Nessun test di
questo file dichiara lecita una mutazione reale.

`mock.patch.object` non e' nell'elenco perche' non e' il problema: ripristina da
solo, come `monkeypatch`.

I rilievi di GPT-5.6 Sol che hanno portato qui, tutti fondati: l'allow-list
ragionava per (file, attributo) senza contare, quindi in un file derogato una
seconda mutazione dello stesso attributo passava; il guard guardava solo
`ast.Assign`, quindi `x.y: T = ...`, `x.y += ...` e `setattr` lo aggiravano; il
controllo era sul primo livello dei bersagli, quindi `x.y, altro = ...` passava;
e mancavano le forme di cancellazione. Inseguirle una per giro di review costa
un push ciascuno e lascia sempre la successiva scoperta: da qui l'enumerazione
esplicita, verificata con `ast.parse`, e il confine scritto sopra.
"""

import ast
from pathlib import Path
from unittest import TestCase

ASSERTIONS = TestCase()

RADICE_TEST = Path(__file__).resolve().parents[1]

# Punti preesistenti al follow-up #462, fuori dal suo scope: (file relativo a
# tests/, "modulo.attributo") -> quante volte. Il conteggio e' parte della
# deroga: una mutazione IN PIU' nello stesso file, sullo stesso attributo, resta
# bloccata.
PREESISTENTI: dict[tuple[str, str], int] = {
    ("chaos/test_runtime_reconcile_under_stress.py", "time.sleep"): 2,
    ("chaos/test_runtime_reconcile_under_stress.py", "time.time"): 2,
    ("failure/test_reconcile_retry_policy.py", "time.sleep"): 2,
    ("parsers/test_parser_personalizzati.py", "cpe.matches_message"): 2,
    ("unit/test_tick_throttle_executor_determinism.py", "tick_module._dispatcher"): 2,
}


def _nomi_importati(albero: ast.Module) -> set[str]:
    """Ogni nome legato da un import, `from` COMPRESO.

    Rilievo di Fugu Ultra: si guardava solo `ast.Import`, quindi
    `from scripts import pr_flow_automation as flow` non entrava nell'insieme e
    `flow.pr_view = ...` sfuggiva. L'istanza che portava era sbagliata — il file
    in questione usa `import scripts.pr_flow_automation as flow`, che era gia'
    coperto, e il guard lo prende (verificato sabotandolo) — ma il buco e' reale:
    `from X import Y` e' un import, e il contratto qui sopra diceva «ogni nome
    legato da un import».

    Coprirlo non costa nulla: misurato sulla suite, 10 mutazioni prima e 10
    dopo. Oggi non c'e' un bypass vivo, ma il contratto smette di essere piu'
    largo del codice — che era esattamente il difetto di partenza di questa PR.

    Un nome legato da `from` puo' non essere un modulo (`from x import funzione`).
    Non si distingue staticamente, e non serve: riassegnare un attributo di un
    oggetto importato a livello di modulo e' la stessa mutazione globale senza
    ripristino, quindi merita lo stesso trattamento.
    """
    nomi: set[str] = set()
    for nodo in ast.walk(albero):
        if isinstance(nodo, ast.Import):
            for alias in nodo.names:
                nomi.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(nodo, ast.ImportFrom):
            for alias in nodo.names:
                if alias.name != "*":
                    nomi.add(alias.asname or alias.name)
    return nomi


def _star_import(albero: ast.Module) -> list[tuple[int, str]]:
    """`from X import *`: il guard non puo' piu' sapere COSA e' stato legato.

    Rilievo di GPT-5.6 Sol. Con lo star import i nomi disponibili dipendono dal
    modulo importato a runtime, quindi `qualcosa.attr = ...` in quel file puo'
    essere una mutazione di modulo senza che l'AST lo mostri. Non si puo'
    verificare => non si dichiara verificato: il file viene segnalato, come la
    collezione vuota nel guard delle chiavi jq.

    Costo oggi: zero. Misurato — `tests/` non contiene nessuno star import (gli
    unici del repo stanno in `.venv`, codice di terze parti che non scansioniamo).
    """
    return [
        (nodo.lineno, "from ... import * (il guard non puo' verificare questo file)")
        for nodo in ast.walk(albero)
        if isinstance(nodo, ast.ImportFrom)
        and any(alias.name == "*" for alias in nodo.names)
    ]


def _e_attributo_di_modulo(nodo: ast.expr, moduli: set[str]) -> bool:
    """`mod.attr` dove `mod` e' un modulo importato.

    `mod.attr.b` NON lo e' (muta un oggetto DENTRO il modulo, non il legame del
    modulo), e nemmeno `mod.attr[0]`.
    """
    return isinstance(nodo, ast.Attribute) and isinstance(nodo.value, ast.Name) and nodo.value.id in moduli


def _bersagli_di_binding(nodo: ast.expr | None) -> list[ast.Attribute]:
    """Gli attributi RIASSEGNATI da questo bersaglio, tuple e liste comprese.

    Rilievo di GPT-5.6 Sol: `mod.attr, x = valori` mette un `ast.Tuple` in
    `targets`, quindi il controllo sul solo primo livello lo mancava. Il
    problema non era quella forma: era guardare il primo livello. Qui si scende
    in `Tuple`/`List`/`Starred`, a qualunque profondita'.

    Si NON scende in `Subscript` (`mod.attr[0] = v` cambia il contenuto, non il
    legame) ne' in catene piu' lunghe (`mod.attr.b = v`): non sono
    riassegnazioni dell'attributo del modulo.
    """
    if isinstance(nodo, ast.Attribute):
        return [nodo]
    if isinstance(nodo, ast.Starred):
        return _bersagli_di_binding(nodo.value)
    if isinstance(nodo, (ast.Tuple, ast.List)):
        return [a for elemento in nodo.elts for a in _bersagli_di_binding(elemento)]
    return []


def _bersagli_del_nodo(nodo: ast.AST) -> list[ast.expr]:
    """I bersagli di ogni costrutto che puo' LEGARE o SLEGARE un attributo.

    Non solo l'assegnazione: `for mod.a in ...`, `with ... as mod.a` e il
    bersaglio di una comprehension legano anche loro, e `del mod.a` slega —
    lasciando il modulo senza l'attributo, che contamina i test successivi
    quanto uno stub. Sono tutti sintassi valida. Elencarli qui invece di
    inseguirli un rilievo alla volta.
    """
    if isinstance(nodo, (ast.Assign, ast.Delete)):
        return list(nodo.targets)
    if isinstance(nodo, (ast.AnnAssign, ast.AugAssign, ast.For, ast.AsyncFor)):
        return [nodo.target]
    if isinstance(nodo, (ast.With, ast.AsyncWith)):
        return [v.optional_vars for v in nodo.items if v.optional_vars is not None]
    if isinstance(nodo, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
        return [g.target for g in nodo.generators]
    return []


def _chiamata_su_modulo(nodo: ast.AST, moduli: set[str]) -> tuple[int, str] | None:
    """`setattr(mod, "a", v)` / `delattr(mod, "a")`: la stessa cosa, come chiamata."""
    if not (
        isinstance(nodo, ast.Call)
        and isinstance(nodo.func, ast.Name)
        and nodo.func.id in ("setattr", "delattr")
        and nodo.args
        and isinstance(nodo.args[0], ast.Name)
        and nodo.args[0].id in moduli
    ):
        return None
    attributo = nodo.args[1] if len(nodo.args) > 1 else None
    nome = (
        attributo.value
        if isinstance(attributo, ast.Constant) and isinstance(attributo.value, str)
        else "?"
    )
    return (nodo.lineno, f"{nodo.args[0].id}.{nome}")


def _namespace_di_modulo(nodo: ast.AST, moduli: set[str]) -> tuple[int, str] | None:
    """`mod.__dict__` o `vars(mod)`: la rotta riflessiva al namespace del modulo.

    Si segnala QUALUNQUE accesso, non solo la scrittura. Distinguere lettura da
    scrittura costerebbe un'analisi di flusso, e non serve: in questa suite le
    occorrenze legittime sono ZERO (misurato), quindi la regola larga non ha
    falsi positivi da pagare — e un `mod.__dict__` in un test e' comunque una
    cosa da guardare in faccia.
    """
    if (
        isinstance(nodo, ast.Attribute)
        and nodo.attr == "__dict__"
        and isinstance(nodo.value, ast.Name)
        and nodo.value.id in moduli
    ):
        return (nodo.lineno, f"{nodo.value.id}.__dict__")
    if (
        isinstance(nodo, ast.Call)
        and isinstance(nodo.func, ast.Name)
        and nodo.func.id == "vars"
        and nodo.args
        and isinstance(nodo.args[0], ast.Name)
        and nodo.args[0].id in moduli
    ):
        return (nodo.lineno, f"vars({nodo.args[0].id})")
    if (
        isinstance(nodo, ast.Call)
        and isinstance(nodo.func, ast.Attribute)
        and nodo.func.attr in ("__setattr__", "__delattr__")
        and isinstance(nodo.func.value, ast.Name)
        and nodo.func.value.id == "object"
        and nodo.args
        and isinstance(nodo.args[0], ast.Name)
        and nodo.args[0].id in moduli
    ):
        attributo = nodo.args[1] if len(nodo.args) > 1 else None
        nome = (
            attributo.value
            if isinstance(attributo, ast.Constant) and isinstance(attributo.value, str)
            else "?"
        )
        return (nodo.lineno, f"{nodo.args[0].id}.{nome}")
    return None


def _mutazioni_di_modulo(sorgente: str) -> list[tuple[int, str]]:
    """(riga, "modulo.attributo") per ogni mutazione a mano nel sorgente.

    Copre ogni costrutto che riassegna un attributo di modulo — assegnazione
    semplice, annotata, aumentata, con destrutturazione, `for`, `with as`,
    comprehension — piu' `setattr`. Un guard che vede una forma sola non
    protegge: si aggira riscrivendo la stessa riga in un altro modo, e da' solo
    l'impressione che qualcuno stia controllando.
    """
    try:
        albero = ast.parse(sorgente)
    except SyntaxError:  # pragma: no cover - un file rotto lo dice la raccolta
        return []
    moduli = _nomi_importati(albero)
    trovate: list[tuple[int, str]] = _star_import(albero)
    for nodo in ast.walk(albero):
        chiamata = _chiamata_su_modulo(nodo, moduli)
        if chiamata is not None:
            trovate.append(chiamata)
            continue
        riflessiva = _namespace_di_modulo(nodo, moduli)
        if riflessiva is not None:
            trovate.append(riflessiva)
            continue
        for bersaglio in _bersagli_del_nodo(nodo):
            for attributo in _bersagli_di_binding(bersaglio):
                if _e_attributo_di_modulo(attributo, moduli):
                    assert isinstance(attributo.value, ast.Name)  # ristretto sopra
                    trovate.append(
                        (attributo.lineno, f"{attributo.value.id}.{attributo.attr}")
                    )
    return sorted(set(trovate))


def _mutazioni_del_file(percorso: Path) -> list[tuple[int, str]]:
    return _mutazioni_di_modulo(percorso.read_text(encoding="utf-8"))


def _conteggio_reale() -> dict[tuple[str, str], int]:
    """(file, "modulo.attributo") -> quante mutazioni, su tutta la suite."""
    conteggio: dict[tuple[str, str], int] = {}
    for percorso in sorted(RADICE_TEST.rglob("test_*.py")):
        relativo = percorso.relative_to(RADICE_TEST).as_posix()
        for _riga, bersaglio in _mutazioni_del_file(percorso):
            conteggio[(relativo, bersaglio)] = conteggio.get((relativo, bersaglio), 0) + 1
    return conteggio


def test_block_nessuna_nuova_mutazione_diretta_di_un_modulo() -> None:
    moduli_test = sorted(RADICE_TEST.rglob("test_*.py"))
    # Anti-vacuo: se la scansione non trova la suite, il verde non prova nulla.
    ASSERTIONS.assertGreater(
        len(moduli_test), 100,
        f"solo {len(moduli_test)} file di test trovati sotto {RADICE_TEST}: "
        "la scansione e' rotta, non la suite",
    )

    conteggio = _conteggio_reale()
    eccedenze: list[str] = []
    for (relativo, bersaglio), quante in sorted(conteggio.items()):
        derogate = PREESISTENTI.get((relativo, bersaglio), 0)
        if quante > derogate:
            eccedenze.append(
                f"tests/{relativo}  {bersaglio}: {quante} mutazioni, "
                f"{derogate} derogate"
            )

    ASSERTIONS.assertEqual(
        eccedenze, [],
        "mutazione diretta di un attributo di modulo in un test:\n  "
        + "\n  ".join(eccedenze)
        + "\nUsa `monkeypatch.setattr(modulo, \"attributo\", ...)` — o "
        "`monkeypatch.context()` se la patch deve sparire prima della fine del "
        "test. Il ripristino a mano non regge l'interleaving e puo' rendere lo "
        "stub permanente.",
    )


def test_block_l_elenco_dei_preesistenti_non_invecchia() -> None:
    """Una deroga che non corrisponde piu' al codice va tolta, non lasciata.

    Vale in entrambe le direzioni: una voce sparita copre il nulla, e un
    conteggio piu' alto del reale lascia spazio libero per una mutazione nuova
    senza che nessuno se ne accorga.
    """
    conteggio = _conteggio_reale()
    obsolete = sorted(
        f"{f} -> {b}: derogate {quante}, reali {conteggio.get((f, b), 0)}"
        for (f, b), quante in PREESISTENTI.items()
        if conteggio.get((f, b), 0) != quante
    )
    ASSERTIONS.assertEqual(
        obsolete, [],
        f"voci dell'allow-list disallineate dal codice: {obsolete}. "
        "Allineale o toglile: una deroga piu' larga del reale e' spazio libero.",
    )


def test_block_ogni_forma_di_mutazione_e_vista() -> None:
    """BLOCK: nessun costrutto che lega un attributo di modulo deve sfuggire.

    Se questo test torna verde con una forma non rilevata, il guard e' di nuovo
    aggirabile riscrivendo la stessa mutazione in un altro modo — che e' il modo
    in cui un controllo diventa decorativo senza che nessuno se ne accorga.

    L'elenco nasce dai rilievi di GPT-5.6 Sol (prima `AnnAssign`/`AugAssign`/
    `setattr`, poi la destrutturazione) e dai costrutti che NON aveva nominato:
    inseguirli uno per giro di review costa un push ciascuno e lascia sempre il
    prossimo scoperto.
    """
    da_vedere = {
        "assegnazione":      "import flow\nflow.pr_view = lambda: 1\n",
        "annotata":          "import flow\nflow.pr_view: object = lambda: 1\n",
        "aumentata":         "import flow\nflow.contatore += 1\n",
        "setattr":           "import flow\nsetattr(flow, 'pr_view', lambda: 1)\n",
        "tupla":             "import flow\nflow.pr_view, altro = stub, 1\n",
        "lista":             "import flow\n[flow.pr_view, altro] = stub, 1\n",
        "starred":           "import flow\n*flow.resto, ultimo = valori\n",
        "annidata":          "import flow\n(a, (flow.pr_view, b)) = 1, (2, 3)\n",
        "for":               "import flow\nfor flow.pr_view in stub:\n    pass\n",
        "for destrutturato": "import flow\nfor flow.pr_view, x in stub:\n    pass\n",
        "with as":           "import flow\nwith aperto() as flow.pr_view:\n    pass\n",
        "comprehension":     "import flow\n[1 for flow.pr_view in stub]\n",
        "del":               "import flow\ndel flow.pr_view\n",
        "del multiplo":      "import flow\ndel flow.pr_view, flow.altro\n",
        "delattr":           "import flow\ndelattr(flow, 'pr_view')\n",
        "__dict__":          "import flow\nflow.__dict__['pr_view'] = 1\n",
        "__dict__.update":   "import flow\nflow.__dict__.update({'a': 1})\n",
        "vars()":            "import flow\nvars(flow)['pr_view'] = 1\n",
        "object.__setattr__":"import flow\nobject.__setattr__(flow, 'pr_view', 1)\n",
        "from import":       "from scripts import flow\nflow.pr_view = lambda: 1\n",
        "from import as":    "from scripts import pr_flow as flow\nflow.pr_view = lambda: 1\n",
        "from relativo":     "from . import flow\ndel flow.pr_view\n",
        "star import":       "from scripts import *\nflow.pr_view = lambda: 1\n",
    }
    for nome, sorgente in da_vedere.items():
        ASSERTIONS.assertTrue(
            _mutazioni_di_modulo(sorgente),
            f"forma «{nome}» non rilevata: il guard e' aggirabile cosi'",
        )

    # E nessun falso positivo: qui NON si riassegna l'attributo del modulo.
    da_non_vedere = {
        "oggetto locale":      "class C: pass\nc = C()\nc.attr = 1\n",
        "modulo non importato": "import flow\naltro.attr = 1\n",
        "contenuto, non legame": "import flow\nflow.registro[0] = 1\n",
        "catena piu' lunga":   "import flow\nflow.oggetto.campo = 1\n",
        "sola lettura":        "import flow\nx = flow.pr_view\n",
    }
    for nome, sorgente in da_non_vedere.items():
        ASSERTIONS.assertEqual(
            _mutazioni_di_modulo(sorgente), [],
            f"falso positivo su «{nome}»: non e' la riassegnazione di un "
            "attributo di modulo",
        )


def test_block_la_sostituzione_del_MODULO_resta_fuori_soggetto() -> None:
    """`sys.modules[...]` e `reload` non sono coperti — e non sono un bypass.

    Rilievo di GPT-5.6 Sol sulla versione precedente, ACCOLTO: un test che
    asserisse «`mod.__dict__` non deve essere segnalato» avrebbe reso il bypass
    un comportamento ATTESO, protetto dalla CI. Quelle rotte ora sono coperte, e
    quell'assertion e' sparita: nessun test di questo file dichiara lecita una
    mutazione reale.

    Restano fuori `sys.modules[...] = ...` e `importlib.reload(mod)`, che non
    sono mutazioni di un ATTRIBUTO: sostituiscono o ricostruiscono il modulo
    intero, che e' un soggetto diverso (e ha usi legittimi — la suite ne ha uno).
    Questo test non dice che vanno bene: dice che questo guard non parla di loro.
    """
    fuori_soggetto = {
        "sys.modules": "import sys\nsys.modules['flow'] = finto\n",
        "reload":      "import importlib, flow\nimportlib.reload(flow)\n",
    }
    for nome, sorgente in fuori_soggetto.items():
        ASSERTIONS.assertEqual(
            _mutazioni_di_modulo(sorgente), [],
            f"«{nome}» ora viene segnalata da un guard che parla di ATTRIBUTI: "
            "o e' un falso positivo, o il soggetto del guard e' cambiato e va "
            "riscritto il docstring del modulo",
        )
