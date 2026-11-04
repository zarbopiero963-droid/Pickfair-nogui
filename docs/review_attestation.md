# Attestazione firmata delle review e gate PRE-LIVE (PF-REVIEW-ATTESTATION v2)

Riferimenti: #586 (c.6582070372 contratto v1, c.6582617372 v2), mandato owner PF-FINAL-AUTONOMOUS-CERTIFICATION-01,
spec `docs/auto_pr_flow_spec.md` §0.7.

## 1. Il problema
- **Il job di review chiude `success` anche con bloccanti.** Sol su `b6e82d2`: VALID_BLOCKERS, run `success`.
- **Un commento di `github-actions[bot]` può pubblicarlo qualunque workflow del repository.** Il marker nel commento
  da solo non prova chi ha fatto la review, su quale codice né con quale esito.

## 2. Produzione (workflow di review trusted)
`pr-review-openrouter-gpt56-sol.yml` e `pr-review-xai-grok46.yml` girano da `pull_request_target`, quindi dal branch
base, senza checkout né esecuzione del codice della PR. Dopo il commento della review:
1. **Scrittura.** Il workflow scrive `review-attestation.json` con questi campi: schema, repo, PR, `head_sha`,
   `base_sha`, range, `pr_author`, reviewer, provider e modello, `workflow_path`, `run_id` e `run_attempt`, `verdict`
   (`PASS`/`BLOCKERS`/`INCONCLUSIVE`/`ERROR`, dalla classificazione PF-REVIEW-RECOVERY-01), `files_sent`,
   `files_truncated`, `coverage_gaps` e `diff_sha256`. Quest'ultimo è l'impronta canonica delle patch della Compare
   API, `canonical_range_digest`.
2. **Firma.** Lo firma con `actions/attest-build-provenance` (permessi `id-token: write`, `attestations: write`). Il
   certificato Sigstore porta l'identità del workflow (`…/<workflow>.yml@refs/heads/main`), l'evento, il run e il
   commit del workflow.
3. **Pubblicazione.** Lo carica come artefatto `review-attestation-<attempt>`.

L'attestazione si scrive solo dentro Actions e solo dopo che la review è stata pubblicata: se l'aggiornamento finale
del commento fallisce, non si scrive nessuna attestazione e il job chiude **rosso**. Se scrittura, firma o upload
falliscono, il commento della review resta pubblicato ma il job chiude **rosso**. Il verificatore guarda **ogni tentativo** di ogni run dei workflow
trusted: un tentativo che non chiude `success` e non ha la propria attestazione (`review-attestation-<attempt>`) dà
`RUN_WITHOUT_ATTESTATION` e nega il PASS (`UNVERIFIED_ATTESTATION`). Vale anche se un rerun successivo chiude
verde. Quel tentativo può aver concluso una review BLOCKERS senza riuscire a pubblicarla, e un PASS di un altro
tentativo non lo copre. I salti legittimi (dedup, gate di costo,
range vuoto) chiudono `success` senza artefatto.

**Rerun di un range già revisionato.** Se firma o upload falliscono al primo giro, un rerun del workflow trova il
`done_marker` e non richiama il modello, quindi non ripaga la review. Prima di uscire però ri-emette l'attestazione
dal commento del bot (`github-actions[bot]`), solo se il verdetto è **non PASS** (`BLOCKERS` o `INCONCLUSIVE`):
- il verdetto è quello classificato allora, preso dall'ultimo tentativo valido;
- `generated_at` è l'`updated_at` del commento;
- `reemitted` vale `true`.

Perché solo non PASS: chi ha accesso in scrittura può modificare un commento del bot senza cambiarne l'autore, quindi
il commento non è una fonte autenticata. Un verdetto negativo ri-emesso può soltanto bloccare; un PASS ri-emesso da un
commento modificato sarebbe un PASS falso firmato. Commento assente, non del bot o senza uno stato valido: nessuna
attestazione. Su un rerun (attempt > 1) che non può ri-emettere (un PASS, o un commento illeggibile) il job chiude
rosso se il run non ha già un'attestazione da un tentativo precedente; se ce l'ha, chiude verde e quella resta valida.
Per leggere gli artefatti del proprio run i workflow hanno `actions: read`.

## 3. Verifica (`scripts/pr_review_attestation.py`, unica per Merge readiness e VPS)
- `verify-pr --repo R --pr N`:
  1. elenca i run `pull_request_target` dei workflow in allowlist per la PR, paginando fino ai run precedenti
     all'apertura della PR (oltre 20 pagine: non verificabile, mai una lista parziale). Un run appartiene alla PR se
     il suo `head_branch` è il branch della PR o se GitHub lo elenca in `pull_requests`: quel campo si svuota quando
     l'head cambia (osservato sui run di #601), quindi da solo perderebbe i range già firmati. Ciò che conta resta il
     campo `pr` firmato. Un run sul branch conta solo se creato dopo l'apertura della PR: un branch riusato non porta
     dentro i run di una PR precedente;
  2. scarica gli artefatti;
  3. per ognuno, verifica la firma con
     `gh attestation verify --signer-workflow R/<workflow> --source-ref refs/heads/main --cert-oidc-issuer …`;
  4. applica `decide`;
  5. una review dei workflow trusted ancora in corso per la PR (queued, in_progress, waiting…) dà `REVIEW_PENDING` e
     nega il PASS anche se la copertura più vecchia arriva all'head: può ancora pubblicare un BLOCKERS;
  6. con `--wait-seconds S` (Merge readiness: 1800) riprova ogni 60 s finché gli unici motivi sono
     `NOT_COVERED_TO_HEAD` o `REVIEW_PENDING`, cioè le review sono ancora in corso. Bloccanti, firme non valide, fork o API non leggibile non si aspettano.
- `decide` accetta un'attestazione solo se valgono tutte queste condizioni:
  - firma valida, issuer GitHub, digest del file uguale;
  - evento `pull_request_target`;
  - `job_workflow_ref` in allowlist **su main**;
  - commit del workflow raggiungibile da `main`;
  - repo, PR, run e autore coincidenti;
  - verdetto noto;
  - solo per un PASS: `files_sent > 0` e `files_truncated == 0`. Un verdetto non PASS autentico conta sempre come
    bloccante, anche se la sua review era incompleta: una copertura più vecchia non lo fa sparire;
  - solo per un PASS: `diff_sha256` uguale all'impronta ricalcolata dal verificatore, con la stessa normalizzazione del workflow
    (`pr_range_files`, parità testata con `solo_file_della_pr`). La normalizzazione usa la `base_sha` firmata: su una
    push che porta dentro main restano solo i file della PR, con il patch calcolato sulla base.

  Se main avanza senza che la PR cambi, il range iniziale firmato parte dalla base di allora: vale come inizio della
  catena solo se coincide con la `base_sha` firmata nell'attestazione. Qualsiasi altro SHA estraneo dà
  `RANGE_NOT_IN_PR`.

  **Copertura:** per ogni reviewer richiesto (Sol, Grok) i range PASS sono contigui dal base all'head. Un range non
  PASS che non è superato da un PASS successivo dà `OPEN_BLOCKERS` oppure `NOT_COVERED_TO_HEAD`.

  **Dopo un non-PASS serve una review dell'intero diff** (decisione owner sulla #606, governance). Un range BLOCKERS,
  INCONCLUSIVE, ERROR o incompleto resta aperto finché un PASS **emesso dopo** non copre la PR **dalla base** fino
  almeno alla fine di quel range. Un PASS sul solo delta successivo non lo chiude più, perché non prova che la PR
  intera sia corretta. Il PASS completo passa da tutti i controlli dell'attestazione: firma Sigstore, workflow su
  main, digest del diff ricalcolato, nessun file tagliato. Le attestazioni storiche non vengono riscritte.

  **Force-push (decisione owner su #607).** Un non-PASS autentico il cui head non è più nella PR (commit usciti dalla
  storia) non blocca per sempre, ma non si cancella da solo: resta `OPEN_BLOCKERS_FORCE_PUSHED` finché un PASS firmato
  **successivo** non copre l'intera PR dalla base fino all'**head esatto attuale**. Un PASS sui soli delta, o un PASS
  completo che si ferma prima dell'head, non basta. Togliere un commit dalla storia non fa sparire un rilievo: la
  review completa del diff corrente deve riconfermare che il problema non c'è più. Il workflow chiede la stessa review
  completa (head del non-PASS fuori ordine ⇒ serve un PASS dalla base fino a `HEAD_SHA`).

  **Lato workflow** (`rr_needs_full_review`, identica nei workflow di Sol e Grok): sulla push successiva il workflow
  legge **ogni** tentativo di ogni range di quel reviewer, solo nei commenti di `github-actions[bot]`: ogni tentativo
  emette un'attestazione firmata, quindi un non-PASS seguito da un PASS sullo stesso range delta resta aperto anche nel
  verificatore (rilievo Sol sulla review completa della #607). «Successivo» = istante del commento strettamente
  posteriore; il numero del tentativo conta solo dentro lo stesso commento. Un non-PASS senza istante non si chiude. Se un range
  non-PASS non è chiuso da un PASS che parte dalla base attuale, è più recente e arriva almeno all'head di quel range,
  la review copre `BASE_SHA...HEAD` con scope «intera PR (precedente non-PASS)». La posizione degli head viene
  dall'elenco dei commit della PR (`pr_commit_order`, al massimo 250 commit). Un head assente dall'elenco, per
  esempio dopo un force-push, non chiude niente. In caso contrario resta il push range, che costa meno. Se lo storico
  dei commenti o l'elenco dei commit non sono leggibili, si fa la review completa: fail-closed, al costo di una
  review in più (rilievo Sol su #607: un PASS `BASE...C1` più recente non chiude un BLOCKERS su `C1...C2`).

  «Successivo» vuol dire due cose:
  - è nell'istante di emissione firmato (`generated_at`, strettamente dopo). Non conta il numero del run, perché un
    rerun lo conserva: un BLOCKERS del run 10 rilanciato dopo un PASS del run 11 resta aperto;
  - il PASS deve portare commit nuovi oltre il non PASS (la correzione, revisionata nella push successiva) oppure
    ri-coprire tutto il suo range. Un PASS successivo solo su una parte dello stesso head non basta.

  Un BLOCKERS emesso dopo un PASS sullo stesso head annulla quel PASS. Un istante illeggibile blocca se il verdetto
  non è PASS e non sblocca nulla se è PASS. A parità di secondo, il PASS non sblocca.

  **Artefatto non verificabile** (firma, identità, verificatore in timeout, contenuto malformato): dà
  `UNVERIFIED_ATTESTATION` e nega il PASS anche con una copertura valida. Gli artefatti arrivano solo dai run dei
  workflow trusted, quindi quello illeggibile potrebbe essere un BLOCKERS. Non bloccano solo gli artefatti autentici
  che non si applicano: quelli di un'altra PR, o un PASS incompleto che semplicemente non copre.

  **Non PASS con partenza fuori catena** (force-push che torna a un head già revisionato da un tip divergente): se
  finisce su un commit della PR, conta come bloccante su tutto il tratto fino a quel commit. Un PASS fuori catena non
  copre mai nulla.

  Le PR da fork e gli errori API danno sempre non PASS.
- `verify-prelive --repo R --pr N`: il check `pf-betfair-certification` dell'app `pickfair-dedicated` deve essere
  `completed`/`success` sull'head esatto. Se una qualsiasi run di quel check sull'head non è conclusa, l'esito è
  `PRELIVE_PENDING`: una run in coda non viene mai nascosta da una SUCCESS più vecchia. Fra le run concluse decide
  quella con l'id più alto. `pf-betfair-live-certification` (neutral / LIVE_NOT_RUN) non soddisfa né
  blocca questo gate, e non abilita mai operazioni reali.

`pr-merge-readiness.yml` esegue i due controlli dopo il gate esistente (che resta), con permesso `attestations: read`.

**Anti-replay:** repo, PR e range sono nel contenuto firmato, quindi un'attestazione non vale per un'altra PR. Rilette
più volte per la stessa PR, le attestazioni sono idempotenti: la VPS rivaluta ogni 2 minuti.

## 4. Test
`tests/scripts/test_pr_review_attestation.py`:
- **positivi:** range completo, range di push concatenati, bloccante poi corretto da un PASS sull'intero diff;
- **negativi:**
  - firma invalida, issuer sbagliato o digest diverso;
  - evento `pull_request`;
  - workflow su `refs/pull/*`, fuori allowlist o di un fork;
  - commit del workflow non su main;
  - run diverso;
  - contenuto manomesso (PR, repo, autore, verdetto, schema, reviewer, path, SHA);
  - marker senza attestazione;
  - verdetti non PASS;
  - vecchio SHA dopo un push, buchi nella catena, reviewer mancante;
  - diff non visto (nessun file, troncato, impronta diversa);
  - API giù;
  - PRE-LIVE assente, pendente o non `success`, oppure di un'altra app;
  - LIVE neutral da solo.
- **struttura e parità:** i workflow firmano e caricano l'attestazione, e la loro `canonical_range_digest` e la loro
  `solo_file_della_pr` coincidono con quelle della libreria.
- **rilievi Sol/Codex su #602:**
  - PASS seguito da BLOCKERS sullo stesso head;
  - base avanzata;
  - digest di una push-merge;
  - paginazione dei run;
  - attesa limitata della readiness.

## 5. Limiti e prossimi passi
- **Prova integrata reale.** Va fatta sulla prima PR dopo il merge di questa: review firmate, `gh attestation verify`,
  stesso esito sulla VPS. I nomi dei campi del certificato letti da `gh attestation verify --format json`
  (`buildSignerURI`, `buildSignerDigest`, `githubWorkflowTrigger`, `runInvocationURI`) sono da confermare su quel run.
  Finché non è confermato, il verificatore restituisce non PASS: è fail-closed.
- **Modello push-range.** Un BLOCKERS su `a...b` è superato da un PASS successivo sui commit nuovi oltre `b`, cioè
  sulla correzione revisionata nella push dopo. Questo PASS non dimostra che il bloccante sia stato risolto: lo
  dimostrano il triage dell'agente nel thread e il test che lo riproduce (spec §0.3–§0.6). È lo stesso limite delle
  review diff-only già in vigore, non un allargamento.
- **Attestazione persa.** Se scrittura, firma o upload falliscono, il run chiude rosso senza attestazione e la PR
  non passa il gate finché un rerun non ri-emette un verdetto negativo. Un PASS perso non si ri-emette mai: il range
  resta non coperto e serve l'owner (fail-closed).
- **Range vuoto dopo un merge di main.** Una push che porta dentro solo file di main non produce review né
  attestazione, perché dopo la normalizzazione non resta nulla da rivedere. La catena ha allora un buco e la readiness
  resta `NOT_COVERED_TO_HEAD` finché non arriva una review che lo ricopre: è un falso negativo fail-closed, non un
  falso PASS. Un'attestazione «no-op» firmata per questi range è un incremento successivo.
- **Altri chiamanti della readiness.** Questa PR applica le attestazioni nel workflow `pr-merge-readiness.yml`. La
  lane Buildkite trusted (`scripts/ci/trusted_pr_guard.py`), disattivata per decisione owner, e `merge-decision` di
  `scripts/pr_autonomy_policy.py` usano ancora il gate a commenti. Portarci il verificatore è l'incremento successivo
  e va deciso con l'owner, perché tocca l'autorità di merge.
- **Questa PR.** Le sue review girano con i workflow di main ancora senza attestazione, quindi il nuovo controllo di
  readiness la vede non PASS. È atteso, e la PR è comunque a merge owner.
- **Reviewer Claude (`pr-review-claude-fable5.yml`, sospeso da P41).** Esito dell'audit: usa ancora la regola larga
  della label (`CRITICAL_PATTERNS` → `manual-review-required`) e non ha il classificatore dei tentativi né il modello
  push-range. Va riscritto sul modello di Sol, con attestazione e `MANUAL_AUTHORITY_PATTERNS` di spec §0.8, prima di
  riattivarlo nella UI. È l'incremento di governance successivo, come fallback dichiarato per l'indisponibilità di
  Sol o Grok.
