"""Cio' che i reviewer marcano come critico, devono anche redigerlo.

Rilievo di Fugu Ultra e Claude Fable 5.1 sulla full-range della #477, e la
prima risposta dell'agente ("`app_?key` non compare affatto, premessa
inventata") era SBAGLIATA: il grep cercava `app_?key` come espressione, non
come testo letterale, e non lo trovava. Il pattern c'e', in tutti e cinque i
workflow:

    CRITICAL_PATTERNS: ... |certlogin|app_?key|config|settings
    REDACTIONS       : ... (api[_-]?key|token|secret|password|...)

Le due liste dicono cose diverse sullo stesso segreto. La prima dichiara che
una `app_key` Betfair nel diff e' materia da controllo manuale; la seconda non
la redige, quindi quel valore arriva in chiaro al modello e dentro il commento
pubblicato. Una asimmetria fra "lo considero critico" e "lo nascondo" e'
proprio il caso in cui un segreto esce.

Il test non legge la lista: ESEGUE la regex estratta dal workflow vero e
verifica che il valore sparisca. Una lista che sembra giusta ma non matcha
sarebbe un verde falso.
"""
from __future__ import annotations

import ast
import os
import re
import time
from pathlib import Path
from typing import Callable, List, Set

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Segreti che i workflow dichiarano critici e che quindi devono sparire.
# `app_key` e' la Betfair application key: money-critical su questo repo.
SEGRETI_DA_REDIGERE = (
    "api_key=SEGRETISSIMO",
    "app_key=SEGRETISSIMO",
    "appKey: SEGRETISSIMO",
    "token=SEGRETISSIMO",
    "secret=SEGRETISSIMO",
    "password=SEGRETISSIMO",
    # Forma QUOTATA, ed e' quella che conta di piu': una app key Betfair vive
    # in `config.json`, non in una riga shell. Rilievo di Claude Fable 5.1
    # sulla #477 — la prima versione di questo test non aveva un campione JSON
    # e restava VERDE sul buco, perche' la `"` fra chiave e `:` rompeva il
    # pattern. Il fix copriva la forma meno probabile delle due.
    '"app_key": "SEGRETISSIMO"',
    "'app_key': 'SEGRETISSIMO'",
    '"appKey":"SEGRETISSIMO"',
    '"api_key": "SEGRETISSIMO"',
    '"password": "SEGRETISSIMO"',
    # Valore quotato CON SPAZI. Rilievo convergente di Claude Fable 5.1 e Fugu
    # Ultra sulla #477: la classe del valore era `[^"'\s,;]+`, quindi si
    # fermava al primo spazio e lasciava in chiaro il resto —
    # `"password": "SEGRETISSIMO con spazi"` diventava
    # `"password=[REDACTED] con spazi"`. Una fuga PARZIALE su un gate di
    # redazione e' peggio di nessuna redazione, perche' il commento sembra
    # ripulito. I campioni senza spazi passavano, quindi il test era verde.
    '"password": "prefisso SEGRETISSIMO"',
    '"app_key": "abc SEGRETISSIMO"',
    "'token': 'x SEGRETISSIMO'",
    # Delimitatore ESCAPATO dentro il valore, e valore NON CHIUSO. Rilievo
    # convergente di GPT-5.6 Sol e Fugu Ultra sulla #477. Il secondo e' il
    # peggiore dei due: con un apice non chiuso il ramo quotato falliva, il
    # fallback escludeva le virgolette, e la regola non redigeva NIENTE — non
    # una fuga parziale, una fuga intera su un gate di segreti.
    '"password": "abc\\" SEGRETISSIMO"',
    '"password": "abc SEGRETISSIMO',
    "'token': 'abc\\' SEGRETISSIMO'",
)


# Righe che devono sopravvivere intatte: una redazione che divora il resto del
# diff rende la review cieca, ed e' il modo opposto di rompere lo stesso gate.
NON_DA_DIVORARE = (
    ('"password": "x"\nquota = 3.5\n', "quota = 3.5"),
    ('"password": "aperto\nselezione = 2\n', "selezione = 2"),
)  # il vecchio terzo caso e' la forma del difetto #479: ora e' un BLOCK (P14)
VALORE = "SEGRETISSIMO"


def _reviewer_su_disco() -> Set[str]:
    return {
        f".github/workflows/{p.name}"
        for p in (ROOT / ".github" / "workflows").glob("pr-review-*.yml")
    }


def _regex_generica(workflow: str) -> re.Pattern:
    """La regola `(chiave)\\s*[:=]\\s*valore` di REDACTIONS, compilata.

    Si cerca la riga che sostituisce con `\\1=[REDACTED]`: e' l'unica regola
    generica per coppie chiave/valore. Se non si trova, si FERMA invece di
    saltare il controllo: un workflow che perde la redazione generica senza
    farlo notare e' esattamente lo scenario da impedire.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    m = re.search(r're\.compile\(r"((?:[^"\\]|\\.)*)"\),\s*r"\\1=\[REDACTED\]"',
                  testo)
    assert m, (
        f"{workflow}: regola generica di REDACTIONS non trovata. Se la forma "
        f"e' cambiata, questo test va aggiornato nella stessa PR invece di "
        f"lasciare la redazione non verificata"
    )
    return re.compile(m.group(1))


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("campione", SEGRETI_DA_REDIGERE)
def test_block_il_valore_del_segreto_non_sopravvive_alla_redazione(
    workflow: str, campione: str
) -> None:
    redatto = _redazione(workflow)(campione)
    assert VALORE not in redatto, (
        f"{workflow}: {campione!r} resta in chiaro dopo la redazione "
        f"({redatto!r}). Il diff viene inviato al modello e ripubblicato nel "
        f"commento: un valore non redatto esce da entrambe le parti"
    )


def _chiavi_critiche(workflow: str) -> Set[str]:
    """Le chiavi-segreto nominate DENTRO `CRITICAL_PATTERNS`, non nel file.

    Rilievo di Claude Fable 5.1 sulla #477: cercare `[a-z]+key` in tutto il
    testo pescava qualunque parola che finisce per "key" — `hotkey`, `monkey`
    in un commento futuro — e avrebbe fatto diventare rosso un gate per
    rumore. Un gate che grida al lupo si smette di leggerlo, quindi il difetto
    era reale anche se il verso era l'opposto (falso allarme, non falsa calma).

    Qui si ritaglia il blocco `CRITICAL_PATTERNS = [ ... ]`, si buttano via le
    righe di solo commento e si prendono i membri di alternanza che finiscono
    per `key`.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    m = re.search(r"CRITICAL_PATTERNS = \[(.*?)\n          \]", testo, re.S)
    assert m, (
        f"{workflow}: blocco CRITICAL_PATTERNS non trovato. Se la forma e' "
        f"cambiata, questo test va aggiornato nella stessa PR invece di "
        f"smettere di controllare in silenzio"
    )
    blocco = "\n".join(r for r in m.group(1).splitlines()
                       if not r.strip().startswith("#"))
    return set(re.findall(r"[a-z]+[_-]?\??key\b", blocco.lower()))


def test_block_ogni_chiave_critica_dichiarata_e_anche_redatta() -> None:
    """`CRITICAL_PATTERNS` e `REDACTIONS` non devono dire cose diverse.

    Se un workflow dichiara critica una chiave-segreto, deve anche redigerla:
    marcare `manual-review-required` e poi spedire il valore in chiaro e' la
    combinazione peggiore, perche' sembra che il caso sia gestito.
    """
    mancanti = []
    for workflow in sorted(_reviewer_su_disco()):
        critiche = _chiavi_critiche(workflow)
        generica = _regex_generica(workflow).pattern
        for chiave in sorted(critiche):
            # `app_?key` nel sorgente e' una regex: il caso concreto e' `app_key`
            concreta = chiave.replace("?", "")
            if not _regex_generica(workflow).search(f"{concreta}=X"):
                mancanti.append(f"  {workflow}: '{chiave}' critica ma non redatta "
                                f"(generica: {generica[:60]}...)")
    assert not mancanti, (
        "chiavi dichiarate critiche che la redazione non copre —\n"
        + "\n".join(mancanti)
    )


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("campione,deve_restare", NON_DA_DIVORARE)
def test_block_la_redazione_non_divora_il_resto_del_diff(
    workflow: str, campione: str, deve_restare: str
) -> None:
    """Un ramo quotato troppo avido mangerebbe le righe successive.

    Il valore non chiuso e' il caso rischioso: per redigerlo bisogna
    consumare fino a fine riga, e se si dimentica di escludere il newline si
    divora il diff da li' in poi. Un reviewer che non vede il codice e' un
    check verde falso, cioe' lo stesso danno visto dall'altra parte.
    """
    redatto = _redazione(workflow)(campione)
    assert deve_restare in redatto, (
        f"{workflow}: la redazione ha divorato {deve_restare!r} "
        f"({redatto!r}). Sopra-redigere acceca la review"
    )


def _redazione(workflow: str) -> Callable[[str], str]:
    """`redact()` vero: tutte le regole di REDACTIONS in ordine, lette con `ast`."""
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    m = re.search(r"REDACTIONS = \[(.*?)\n          \]", testo, re.S)
    assert m and ("for pattern, replacement in REDACTIONS:\n                  "
                  "text = pattern.sub(replacement, text)") in testo, (
        f"{workflow}: REDACTIONS o redact() hanno cambiato forma")
    regole = [(re.compile(v.elts[0].args[0].value), v.elts[1].value)
              for v in ast.parse("X = [" + m.group(1) + "\n]").body[0].value.elts]

    def redigi(testo_: str) -> str:
        for regola, sostituto in regole:
            testo_ = regola.sub(sostituto, testo_)
        return testo_
    return redigi


# P14 (#479): valore nudo con spazi, chiave a inizio riga. Campioni montati per
# pezzi: per esteso li mutilerebbe la redazione dei reviewer (branch base).
PAROLE = ("PRIMOPEZZO", "SECONDOPEZZO", "TERZOPEZZO")
# Anche segreti codificati (base64, URL) e passphrase con operatori: la forma
# di codice non deve inghiottirli (GPT-5.6 Sol, GPT-6 Astra, Fugu Ultra, #483).
VALORI = ("{0} {1} {2}", "{0}, {1} {2}", "{0} ({1}) {2}", "{0}.{1} {2}",
          "{0}+{1}/{2}== {1}", "{0}%2F{1} {2}", "{0} + {1} {2}", "{0}={1} {2}", "{0}|{1} {2}",
          "{0} = {1} {2}", "{0} | {1} {2}")
FORME_DI_RIGA = ("{c}={v}", "{c}: {v}", "{c} = {v}", "export {C}={v}",
                 "db.{c}={v}", "DB_{C}={v}", "  - {C}={v}", "* {c}: {v}",
                 "# {c}: {v}", '"{c}": {v}', "set {C}={v}", "! {c} = {v}",
                 "-- {c} = {v}", "/* {c} = {v} */", "<!-- {c}: {v} -->")


def _chiavi(workflow: str) -> List[str]:
    """Chiavi della regola generica: `app[_-]?key` -> `app_key`."""
    m = re.match(r"\(\?i\)\(([^)]*)\)", _regex_generica(workflow).pattern)
    assert m, f"{workflow}: chiavi della regola generica non trovate"
    return [a.replace("[_-]?", "_") for a in m.group(1).split("|")]


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
def test_block_un_valore_nudo_con_spazi_sparisce_per_intero(workflow: str) -> None:
    redigi, chiavi, fughe = _redazione(workflow), _chiavi(workflow), []
    assert "password" in chiavi and "app_key" in chiavi
    for c in chiavi:
        for forma in FORME_DI_RIGA:
            for valore in VALORI:
                for marcatore in ("", "+", "-", " "):
                    riga = marcatore + forma.format(c=c, C=c.upper(), v=valore.format(*PAROLE))
                    redatta = redigi(riga + "\nquota = 3.5")
                    if any(p in redatta for p in PAROLE) or not redatta.endswith("\nquota = 3.5"):
                        fughe.append(f"  {riga!r} -> {redatta!r}")
    assert not fughe, f"{workflow}: {len(fughe)} fughe\n" + "\n".join(fughe[:12])


# Chiave a meta' riga, o valore con forma di codice a inizio istruzione
# (assegnazioni, annotazioni, chiamate: Claude Fable 5.1 e Codex sulla #483):
# redazione IDENTICA a quella della sola regola generica, cioe' a prima.
RIGHE_CHE_RESTANO = (
    "    login({c}=utente, user=u)", "    if {c} == atteso and not scaduto:",
    "    def rinnova(self, {c}: str) -> str:", '{c}: "{v}"  # nota',
    "{c}: ${{{{ secrets.X }}}}", "    {c}izer = crea(a, b)", "    {c}_hash = hash(pw, salt)",
    "    {c} = calcola(a, b)", '    self.{c} = dati["chiave"]', "    {c}: str = None",
    '    {c}: Optional[str] = "predefinito"', "    {c}: str | None = None,",
    "            {c}=config.valore,", '    "{c}": valore,', '    "{c}": bool(x),',
    '    {c} = (chiedi("x") or "").strip()', '    {c}=lambda p: "",',
    "        {c} == atteso and utente.admin", "    {c} := calcola()", "    '{c}' => $x,",
)


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
def test_pass_il_codice_resta_visibile_come_prima(workflow: str) -> None:
    redigi, generica, diversi = _redazione(workflow), _regex_generica(workflow), []
    for c in _chiavi(workflow):
        for forma in RIGHE_CHE_RESTANO:
            riga = "+" + forma.format(c=c, v=" ".join(PAROLE))
            redatta, prima = redigi(riga), generica.sub(r"\1=[REDACTED]", riga)
            if redatta != prima or any(p in redatta for p in PAROLE):
                diversi.append(f"  {riga!r} -> {redatta!r} (prima {prima!r})")
    assert not diversi, f"{workflow}: codice nascosto al reviewer\n" + "\n".join(diversi[:12])


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
def test_block_la_redazione_resta_lineare_sulle_patch_grandi(workflow: str) -> None:
    """Gira sulla patch intera: se esplode, il reviewer tace. ~0,2 s misurati."""
    redigi, c = _redazione(workflow), "password"
    inizio = time.perf_counter()
    for testo in ("+" + "a." * 100_000, "+" + "x" * 200_000 + c, "+" + c + " " * 200_000,
                  "+    if {c} == atteso:\n+{c}: a b\n".format(c=c) * 20_000):
        redigi(testo)
    assert time.perf_counter() - inizio < 10, f"{workflow}: redazione non lineare"


# #606 (Sol VALID_INCONCLUSIVE su 1948452): la redazione oscurava tre righe SENZA segreti, e il reviewer non poteva
# certificare il permesso OIDC del workflow. Restano visibili SOLO le due forme che non possono contenere un valore
# segreto: il permesso Actions `id-token` con valore read/write/none, e il riferimento a un'espressione di Actions
# `${{ ... }}` che occupa da sola tutto il valore (il segreto vero non e' mai nel diff, ne e' nominato solo il nome).
# Campioni montati per pezzi: per esteso li mutilerebbe la redazione dei reviewer (branch base).
_T, _E = "to" + "ken", "${" + "{"
RIGHE_SENZA_SEGRETI = (
    f"  id-{_T}: write", f"+  id-{_T}: write", f"  id-{_T}: read", f"  id-{_T}: none",
    f'    assert p == {{"contents": "read", "id-{_T}": "write",',
    f"'id-{_T}': 'read'", f"      GH_{_T.upper()}: {_E} github.{_T} }}}}",
    f"+      GH_{_T.upper()}: {_E} secrets.GITHUB_{_T.upper()} }}}}", f'  {_T}: "{_E} inputs.x }}}}"',
)
# Stesse chiavi, valori che POTREBBERO essere segreti: devono sparire come prima.
SEGRETI_SIMILI = (
    f"  id-{_T}: SEGRETISSIMO", f'"id-{_T}": "SEGRETISSIMO"', f"  id-{_T}: writeSEGRETISSIMO",
    f"  id-{_T}: write SEGRETISSIMO", f"  id-{_T}: write-SEGRETISSIMO", f'"id-{_T}": "write SEGRETISSIMO"',
    f"  my{_T}: SEGRETISSIMO", f"  {_T}: write SEGRETISSIMO", f"  access-{_T}: SEGRETISSIMO",
    f"  api_key: SEGRETISSIMO", f'  {_T}: "SEGRETISSIMO"',
)


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("riga", RIGHE_SENZA_SEGRETI)
def test_pass_permesso_oidc_e_riferimenti_actions_restano_visibili(workflow: str, riga: str) -> None:
    redatta = _redazione(workflow)(riga)
    assert redatta == riga, f"{workflow}: {riga!r} -> {redatta!r}: riga senza segreti nascosta al reviewer"


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("riga", SEGRETI_SIMILI)
def test_block_un_valore_che_puo_essere_segreto_sparisce_anche_su_id_token(workflow: str, riga: str) -> None:
    redatta = _redazione(workflow)(riga)
    assert VALORE not in redatta, f"{workflow}: {riga!r} -> {redatta!r}: valore in chiaro"


def test_le_cinque_copie_di_redactions_restano_identiche() -> None:
    blocchi = {w: re.search(r"REDACTIONS = \[(.*?)\n          \]", (ROOT / w).read_text(encoding="utf-8"), re.S).group(1)
               for w in sorted(_reviewer_su_disco())}
    assert len(set(blocchi.values())) == 1, "REDACTIONS divergente fra i workflow di review"


# Casi limite (owner, GO governance #606): l'eccezione `${{ }}` vale SOLO per riferimenti di contesto senza letterali
# (`secrets.`, `github.`, `inputs.`, ... uniti da `||`/`&&`). Un'espressione puo' contenere un LETTERALE, che e' un
# valore: resta redatto. Idem espressioni malformate, testo attaccato e formati reali di token dopo `id-token`.
_EX = "${" + "{"
CASI_LIMITE_DA_REDIGERE = (
    f"  password: {_EX} 'SEGRETISSIMO' }}}}", f'  password: {_EX} "SEGRETISSIMO" }}}}',
    f"  {_T}: {_EX} format('{{0}}', 'SEGRETISSIMO') }}}}", f"  {_T}: {_EX} secrets.X || 'SEGRETISSIMO' }}}}",
    f"  {_T}: {_EX} SEGRETISSIMO }}}}", f"  {_T}: {_EX} 1234SEGRETISSIMO }}}}",
    f'  {_T}: "{_EX} secrets.X }}}}SEGRETISSIMO"', f"  {_T}: {_EX} secrets.X }}}}SEGRETISSIMO",
    f"  {_T}: {_EX} secrets.X }} SEGRETISSIMO", f"  {_T}: {_EX} inputs.a == SEGRETISSIMO }}}}",
    f"  api_key: {_EX} 'SEGRETISSIMO' || secrets.X }}}}", f'"password": "{_EX} \'SEGRETISSIMO\' }}}}"',
    f"  id-{_T}: " + "gh" + "p_SEGRETISSIMOabcdefghijklmnopqrstuv", f"  id-{_T}: " + "ey" + "JhbGciOiJSUzI1NiJ9.SEGRETISSIMO.sig",
    f'"id-{_T}":"writeSEGRETISSIMO"', f"  id-{_T}: wr1teSEGRETISSIMO",
    f"export ID_{_T.upper()}=writeSEGRETISSIMO", f"  id-{_T}=SEGRETISSIMO",
    # Sol su #608 (d1c0f49): virgola DENTRO il valore quotato, e commento dopo il permesso
    f'  {_T}: "{_EX} secrets.X }}}}, SEGRETISSIMO"', f"  {_T}: '{_EX} secrets.X }}}}; SEGRETISSIMO'",
    f'  {_T}: "{_EX} secrets.X }}}}# SEGRETISSIMO"', f'"api_key": "{_EX} secrets.X }}}}, SEGRETISSIMO"',
    f"  id-{_T}: write # SEGRETISSIMO", f"  id-{_T}: write  # password SEGRETISSIMO",
    f"  {_T}: {_EX} secrets.X }}}} # SEGRETISSIMO",
)


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("riga", CASI_LIMITE_DA_REDIGERE)
def test_block_casi_limite_di_espressioni_e_id_token(workflow: str, riga: str) -> None:
    redatta = _redazione(workflow)(riga)
    assert VALORE not in redatta, f"{workflow}: {riga!r} -> {redatta!r}: valore in chiaro"


def _redazione_di_main(workflow: str) -> Callable[[str], str]:
    """La redazione di prima (origin/main): base del confronto differenziale."""
    import subprocess
    testo = subprocess.run(["git", "show", f"origin/main:{workflow}"], cwd=ROOT, capture_output=True,
                           text=True).stdout
    m = re.search(r"REDACTIONS = \[(.*?)\n          \]", testo, re.S)
    if not m:
        pytest.skip("origin/main non disponibile nel checkout")
    regole = [(re.compile(v.elts[0].args[0].value), v.elts[1].value)
              for v in ast.parse("X = [" + m.group(1) + "\n]").body[0].value.elts]

    def redigi(t: str) -> str:
        for regola, sostituto in regole:
            t = regola.sub(sostituto, t)
        return t
    return redigi


_FORME_CONSENTITE = re.compile(
    r"(?:[+\-#*! \t]|export |set )*[\"']?(?:[\w\-]+[.\-_])*(?i:api[_-]?key|app[_-]?key|token|secret|password|passwd|pwd|"
    r"private[_-]?key|cert)[\w\-]*[\"']?[ \t]*[:=][ \t]*[\"']?(?:\$\{\{[^'\"}]*\}\}|read|write|none)[\"']?[ \t,;}\]]*"
    r"(?:#.*)?")


def test_block_differenziale_nessun_valore_nascosto_prima_diventa_visibile() -> None:
    """Fuzz deterministico: ~60.000 righe (chiavi x prefissi x separatori x valori, letterali e riferimenti).
    Proprieta': se la redazione di main nascondeva il valore, la nuova lo nasconde ancora; ogni riga che cambia
    ha una delle due forme consentite (permesso id-token, riferimento di contesto puro)."""
    w = ".github/workflows/pr-review-openrouter-gpt56-sol.yml"
    vecchia, nuova = _redazione_di_main(w), _redazione(w)
    chiavi = [f"id-{_T}", _T, f"GH_{_T.upper()}", "api_key", "password", "secret", f"github-{_T}", "cert"]
    prefissi = ["", "+", "-", "  ", "+      ", "export ", "- ", "# ", '"', "'"]
    separatori = [": ", ":", "=", " = ", '": ', "': "]
    valori = ["write", "read", "none", "writeSEGRETISSIMO", "write SEGRETISSIMO", "SEGRETISSIMO",
              '"write"', "'read'", '"SEGRETISSIMO"', f"{_EX} secrets.X }}}}", f"{_EX} secrets.X }}}}SEGRETISSIMO",
              f"{_EX} 'SEGRETISSIMO' }}}}", f"{_EX} SEGRETISSIMO }}}}", f"{_EX} github.t || 'SEGRETISSIMO' }}}}",
              f'"{_EX} secrets.X }}}}"', f"{_EX} secrets.X", f"{_EX} env.A && vars.B }}}}",
              f"{_EX} secrets.X }} SEGRETISSIMO", f'"{_EX} secrets.X }}}}, SEGRETISSIMO"',
              f"'{_EX} secrets.X }}}}; SEGRETISSIMO'", f'"write, SEGRETISSIMO"', f'"{_EX} secrets.X }}}}', "gh" + "p_SEGRETISSIMO0123456789abcdefghijklmn"]  # montato per pezzi: niente forma di token nel diff
    code = ["", ",", " # nota", "  ", ";", "}", " SEGRETISSIMO", " # SEGRETISSIMO", "# SEGRETISSIMO",
            ", SEGRETISSIMO", "; SEGRETISSIMO", "} SEGRETISSIMO"]
    nuove_fughe, forme_strane, n = [], [], 0
    for c in chiavi:
        for p in prefissi:
            for s in separatori:
                for v in valori:
                    for k in code:
                        riga = f"{p}{c}{s}{v}{k}"
                        n += 1
                        a, b = vecchia(riga), nuova(riga)
                        if VALORE not in a and VALORE in b:
                            nuove_fughe.append(f"{riga!r} -> {b!r}")
                        # conta solo cio' che la nuova redazione mostra IN PIU' (meno [REDACTED]); redigere di piu' e' ok
                        # si esamina solo la parte MOSTRATA IN PIU': la coda comune era gia' visibile su main
                        coda = os.path.commonprefix([a[::-1], b[::-1]])[::-1]
                        if coda.startswith("]") or VALORE in b[:len(b) - len(coda)]:
                            coda = ""
                        if (b.count("[REDACTED") < a.count("[REDACTED")
                                and not _FORME_CONSENTITE.fullmatch(b.strip())
                                and not _FORME_CONSENTITE.fullmatch(b[:len(b) - len(coda)].strip())):
                            forme_strane.append(f"{riga!r}: {a!r} -> {b!r}")
    assert n > 50_000
    assert not nuove_fughe, f"{len(nuove_fughe)} valori nuovi in chiaro\n" + "\n".join(nuove_fughe[:15])
    assert not forme_strane, f"{len(forme_strane)} differenze fuori forma\n" + "\n".join(forme_strane[:15])


# Residuo PREESISTENTE e dichiarato (DECISIONE-426 P22/P23): valore QUOTATO seguito da altro testo sulla stessa riga.
# La regola di inizio riga non tocca i valori quotati (li lascia alla generica, che chiude alle virgolette) e vale per
# ogni chiave, non solo `id-token`. Qui si fissa che non PEGGIORA: la nuova redazione non mostra nulla in piu' di main.
RESIDUI_PREESISTENTI = (f"  id-{_T}: 'write' SEGRETISSIMO", f'"id-{_T}": "write" # SEGRETISSIMO', f'  password: "x" SEGRETISSIMO', f"  {_T}: 'a' SEGRETISSIMO")


@pytest.mark.parametrize("riga", RESIDUI_PREESISTENTI)
def test_residuo_quotato_preesistente_non_peggiora(riga: str) -> None:
    w = ".github/workflows/pr-review-openrouter-gpt56-sol.yml"
    a, b = _redazione_di_main(w)(riga), _redazione(w)(riga)
    assert b.count("[REDACTED") >= a.count("[REDACTED") and len(b.replace("[REDACTED]", "")) <= len(
        a.replace("[REDACTED]", "")), f"{riga!r}: main {a!r}, ora {b!r}"


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("riga", [f"  password: write", f"  {_T}: read", f'"secret": "none"', f"  access-{_T}: write",
                                  f"  api_key: {_EX} 'write' }}}}"])
def test_block_le_parole_di_permesso_valgono_solo_per_id_token(workflow: str, riga: str) -> None:
    # per ogni altra chiave `read`/`write`/`none` e' un valore come un altro: redatto come prima
    assert "[REDACTED" in _redazione(workflow)(riga), f"{workflow}: {riga!r} non redatto"
