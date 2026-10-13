# CHECKPOINT Gate A — close02 (13/10/2026, 15:4x ora di Roma)

**Solo continuità del lavoro.** Questo branch NON è integrazione, NON è revisione e NON è certificazione indipendente. Nessuna PR, nessun merge, nessun deploy, nessun LIVE. Branch costruito da `028cee9` (main), mai dal branch della #505. **Gate A: NON PASS.** Spesa AI aggiuntiva: 0 €.

## Contenuto (un solo commit sul branch, da 028cee9)
Il codice NON è applicato al branch: il limite dello strumento è di 4 file per commit. I patch sono inclusi come file e si applicano con `git apply`.
- `checkpoint/close02/close02_findings_proposal_v13.patch`: patch isolata cumulativa v1–v13, **verde**. sha256 6fef237dc74a37ec2ee9e447e6ac791e058b64ca5a317222c3302f8ebed4a1eb (uguale al locale, verificato).
- `checkpoint/close02/close02_findings_proposal_v14.patch`: v1–v14, con la ricostruzione del picco da getAccountStatement. **WIP NON VERIFICATO.** sha256 13c1bf8d2988a49c9ec48e6e9f7c49346d89a9cb2e98d1fd955420805cd47d22.
- `checkpoint/close02/mut_all.py`: script di mutazione ricostruito, M1–M80.

## Stato dei test (locali, non indipendenti)
- **v13:** suite completa 7502 passed, 15 skipped, 2 xfailed, 0 failed (xdist e seriale).
  - RED→GREEN: `test_close02_v12` 16 falliti a 028cee9 → 17/17 verdi; `test_close02_v13` 24 falliti su v12 → 25/25 verdi.
  - Mutanti: v12 9/10 uccisi (M51 equivalente); v13 13/13 uccisi.
  - L7b 315/0/11. L9 357/357.
- **v14 WIP:** `tests/close02/test_close02_v14.py` 23/23 verdi in locale, 23 falliti su v13 (RED); `tests/close02` 271 passed.
  - **Non ancora eseguiti:** suite completa su v14 e mutazione M1–M80 completa (interrotta a M29 per limite d'uso).
  - Mutanti sopravvissuti nel sottoinsieme di test: M8 (gate startup in `_on_signal_received`, ucciso in v9 da test fuori dal sottoinsieme?) e M27 (pattern ricostruito probabilmente equivalente). **Da rieseguire** sulla suite completa.
- **Comandi:** `python -m pytest -q -o addopts="" -n 4` (suite completa); `python -m pytest -q tests/close02`; mutazione: `python checkpoint/close02/mut_all.py [M8 M27 …]` dalla radice del repository.

## Finding
- **Chiusi nel patch** (proposta, non certificati):
  - t80u regole 1–3;
  - RSR-38..40;
  - M10 / M19 / M27 (v9);
  - picco persistito a ogni nuovo massimo;
  - registro sparito ⇒ fail-closed;
  - etichette commissione STIMATA/VERIFICATA;
  - netto conservativo 4,5% + 0,01 € per daily-loss e drawdown;
  - picco gonfiato dal P/L transitorio (difetto della base);
  - M39 ucciso con prova;
  - migrazione pre-v9 opzione A (v13).
- **Aperti:**
  - semantica `betIds`+`groupBy=MARKET` non confermata (P/L STIMATO, verifica rollup disattivata);
  - installazioni base senza picco persistito: v14 WIP;
  - il runtime confronta il picco con `availableToBetBalance` (non equity): scelta conservativa, da ratificare;
  - lotti Gate A L4 (379 + 20 record NOT_READY), L6 (190), CASH (5, nessuna sonda dinamica), L1 e L2 in attesa di revisione indipendente.

## Decisioni owner (#426)
- t80u: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426
- Migrazione pre-v9, opzione A: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6145958246
- Ricostruzione del picco, opzione A: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6146717266

## Prossime attività
1. v14: suite completa, mutazione completa M1–M80 (rieseguire M8 e M27 sulla suite completa), sonde di riavvio e kill -9, report su #506.
2. Lotti Gate A L4, L6, CASH (sonde dinamiche), revisione indipendente L1/L2 solo con autorizzazione owner.

## Come riprendere
```
git fetch origin checkpoint/gate-a-close02 && git checkout checkpoint/gate-a-close02
sha256sum checkpoint/close02/*.patch          # confronta con gli SHA qui sopra
git apply checkpoint/close02/close02_findings_proposal_v13.patch   # oppure ..._v14.patch (WIP)
python3.11 -m venv .venv && . .venv/bin/activate && pip install -r requirements-test.txt
python -m pytest -q -o addopts="" -n 4
```
Gli script di sonda, preflight, indep-check ed eval L7b/L9 e i record JSON **non sono inclusi** in questo checkpoint per limite d'uso: restano locali (`/workspace/pickfair/bf2/close02`) e vanno aggiunti al prossimo aggiornamento. Fixture v14 interamente sintetiche: nessun estratto conto reale. Scansione credenziali eseguita: solo password fittizie di test preesistenti.
