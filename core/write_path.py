"""Sezione critica del percorso di scrittura del segnale (#136 item 1, slice 6).

Estratta da `App._process`: la sequenza **valuta-guardrail → coda → scrittura CSV →
rollback**, cioè il cuore anti-doppia-scommessa. Tenerla qui la rende esercitabile in
CI con coda/tracker/daily **reali** e una `write_rows` iniettabile che può fallire.

INVARIANTI (non negoziabili):
- **Lock del chiamante.** `commit_signal` NON prende il lock: `App._process` lo invoca
  mentre tiene `_queue_lock`. `SignalTracker`/`DailyLimiter`/`SignalQueue` non hanno lock
  interno, e la sequenza «valuta + scrivi» deve restare atomica (audit A2), altrimenti
  due callback interlacciati potrebbero passare entrambi il dedup → doppia scommessa.
- **Solo WRITE scrive.** Ogni altro esito `live_guard` (DUPLICATE/RATE_LIMITED/
  DAILY_LIMITED/DRY_RUN) sopprime la scrittura e non tocca la coda. Inoltre lo stato dei
  guardrail riflette SOLO i WRITE reali: il consumo fatto da `evaluate` su un esito che NON
  scrive viene disfatto. DAILY_LIMITED → si annulla solo l'hash del tracker (il tetto non aveva
  consumato slot, solo normalizzato il giorno: si preserva, altrimenti un giorno corrotto
  bloccherebbe per sempre). DRY_RUN → si annulla l'hash e si **restituisce** la slot giornaliera
  con `DailyLimiter.release()` (mantenendo il giorno normalizzato). Così la simulazione non
  consuma tetto/dedupe reali e un segnale soppresso resta ritentabile, senza rischio di doppia
  scommessa (#184 low-tracker-nonwrite).
- **Rollback fail-safe.** Se la scrittura CSV fallisce, coda E guardrail tornano allo
  stato precedente (allineati al CSV ancora su disco): il segnale resta RITENTABILE e in
  OVERWRITE_LAST il precedente non va perso. Stesso rollback dei guardrail quando il
  segnale è oltre il tetto di righe attive (#136 p5): non accodato → ritentabile.
- **Nessun side-effect oltre `write_rows`.** Niente GUI, niente persistenza su disco dei
  guardrail (resta a carico di `App` dopo il commit), niente eccezioni propagate per un
  fallimento di scrittura (riportato in `CommitResult.write_error`).

`evaluate` (dedup/limiti/dry-run) è chiamato SENZA `now`: il dedup usa il proprio
wallclock persistito; `now` (monotòno) serve solo alla coda (expire/add), come in origine.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import live_guard, safety_guard, signal_dedupe, signal_queue


@dataclass(frozen=True)
class CommitResult:
    """Esito della sezione critica.

    - `decision`: esito `live_guard` (WRITE o un esito che sopprime la scrittura);
    - `blocked_by_cap`: True se — pur essendo WRITE — il nuovo segnale è oltre il tetto
      di righe attive (#136 p5): NON accodato, guardrail già ripristinati. Il CSV viene
      riscritto SOLO se `expire` ha rimosso righe scadute (disco da sincronizzare);
      altrimenti il contenuto attivo su disco è già identico e non si tocca (#259 C2);
    - `rows`: righe attive (post-expire) nel ramo WRITE — scritte, salvo il caso
      cap-senza-scaduti qui sopra dove sono riportate ma il CSV non è riscritto;
      `[]` altrimenti;
    - `write_error`: l'eccezione se la scrittura CSV è fallita (con rollback completo),
      altrimenti `None`;
    - `write_attempted`: True se `write_rows` è stata CHIAMATA (riuscita o fallita).
      Serve al contatore CSV-lock (#153 H2) del chiamante: un esito WRITE che NON ha
      toccato il disco (cap-senza-scaduti, #259 C2) non deve contare come successo di
      scrittura — falsificherebbe il recovery di un CSV bloccato.
    """

    decision: str
    blocked_by_cap: bool
    rows: list
    write_error: BaseException | None
    write_attempted: bool = False


def commit_signal(tracker, daily, queue, cfg, text, row, path, now, write_rows,
                  disk_dirty=False):
    """Esegue, SOTTO IL LOCK DEL CHIAMANTE, la sequenza valuta-guardrail → coda →
    scrittura con rollback fail-safe (vedi il docstring del modulo per le invarianti).

    `disk_dirty` (Codex P1 #300): True se il chiamante sa che il CSV su disco può essere
    STANTIO rispetto alla coda — una riscrittura precedente (post-conferma o post-scadenza)
    è fallita e il suo retry non è ancora riuscito. In quel caso il ramo cap-senza-scaduti
    NON salta la scrittura: il presupposto «disco già identico» non vale e il commit
    riallinea il disco alle righe attive correnti.

    Ritorna una `CommitResult`. Non solleva mai per un fallimento di scrittura: ripristina
    coda E guardrail e riporta l'eccezione in `write_error`."""
    decision = live_guard.WRITE
    blocked_by_cap = False
    rows = []
    write_error = None
    write_attempted = False
    tracker_snap = daily_snap = None

    def _restore_guards():
        # Riporta tracker E daily allo snapshot pre-valutazione: il segnale torna
        # RITENTABILE. Unico punto di rollback dei guardrail per tutti i rami
        # (cap-senza-scaduti, cap-con-scaduti, write fallita): tenerli in lockstep.
        if tracker is not None:
            tracker.restore_state(tracker_snap)
            if daily is not None and daily_snap is not None:
                daily.restore_state(daily_snap)

    # `tracker is None` (test/chiamanti senza guardrail) → resta WRITE di default.
    if tracker is not None:
        tracker_snap = tracker.state()
        daily_snap = daily.state() if daily is not None else None
        # kyW #192: controllo cross-namespace PRE-scrittura. La dedup single-row è sull'hash-messaggio,
        # ma la stessa riga può essere già stata scritta dal percorso MULTI (chiave per-riga) — anche
        # da uno stato dedupe PERSISTITO da una versione precedente. Se la chiave per-riga è già vista,
        # è un duplicato → non scrivere (fail-closed anti-doppia-scommessa), invece di controllare solo
        # l'hash-messaggio che il percorso multi non registra.
        if tracker.is_seen(signal_dedupe.row_dedup_key(text, row)):
            decision = live_guard.DUPLICATE
        else:
            decision = live_guard.evaluate(cfg, tracker, daily, text)

    if decision == live_guard.WRITE:
        # Coda dei segnali attivi: expire dei già scaduti, add del nuovo, riscrittura
        # atomica di TUTTE le righe attive. Snapshot per il rollback su write fallita.
        queue_snap = queue.state()
        expired = queue.expire(now=now)
        # Accoda la riga con la sua chiave PER-RIGA (provenienza): serve al commit MULTI di un
        # eventuale passaggio di modalità a runtime (single→multi). In OVERWRITE_LAST `commit_signals`
        # tiene una riga duplicata solo se la sua chiave è tra `queue.active_keys()`; senza questa
        # chiave la riga già attiva verrebbe scartata dal blocco A+B (kyh + kyW #192, Codex).
        sid = queue.add(row, now=now, dedup_key=signal_dedupe.row_dedup_key(text, row))
        # `add` ritorna None se il nuovo segnale è oltre il tetto (#136 p5): le righe
        # attive restano quelle correnti (post-expire).
        blocked_by_cap = sid is None
        rows = queue.active_rows()
        if blocked_by_cap and not expired and not disk_dirty:
            # #259 C2: bloccato dal tetto E nessuna riga scaduta rimossa → il contenuto attivo
            # su disco è già identico a `rows` (l'unico altro mutatore, `add`, non ha accodato).
            # Riscrivere sarebbe un no-op che tocca mtime/inode del CSV e riapre la finestra di
            # ri-lettura lato XTrader. Si scrive comunque in DUE casi in cui il disco va
            # riallineato: `expire` HA rimosso righe (coda sovra-riempita via force=True dal
            # percorso multi-row #192 che resta piena al tetto → le scadute sono ancora su
            # disco), oppure `disk_dirty` (una riscrittura precedente è fallita e il retry non
            # è ancora riuscito: il disco è indietro rispetto alla coda — Codex P1 #300).
            _restore_guards()   # segnale NON accodato → ritentabile
            return CommitResult(decision=decision, blocked_by_cap=True,
                                rows=rows, write_error=None, write_attempted=False)
        write_attempted = True   # unico punto in cui `write_rows` viene chiamata
        try:
            write_rows(rows, path)
        except Exception as ex:   # noqa: BLE001 — riportato al chiamante, no crash
            # Scrittura fallita: RIPRISTINA coda E guardrail (allineati al CSV su disco).
            queue.restore_state(queue_snap)
            _restore_guards()
            write_error = ex
        else:
            if blocked_by_cap:
                # Bloccato dal tetto (con righe scadute rimosse, vedi sopra): segnale NON
                # accodato → rollback guardrail (ritentabile).
                _restore_guards()
            elif tracker is not None:
                # Scrittura REALE riuscita (single-row, dedup a hash-messaggio): ombreggia ANCHE la
                # chiave PER-RIGA di questa riga (#192 kyW). Così un retry dello STESSO messaggio dopo
                # una transizione del parser a MULTI-riga riconosce la riga già scritta come duplicato
                # (la chiave per-riga di quella riga è già "vista"), invece di riscriverla → doppia
                # scommessa. Lo shadow non conta verso il rate-limit (`mark_seen`).
                tracker.mark_seen(signal_dedupe.row_dedup_key(text, row))
    elif tracker is not None and decision == live_guard.DAILY_LIMITED:
        # `evaluate` aveva registrato l'hash nel tracker (segnale NEW) ma `daily.allow()` ha
        # RIFIUTATO **senza consumare** una slot — ha solo (eventualmente) normalizzato il giorno
        # corrente. Si annulla SOLO l'hash del tracker (segnale ritentabile dopo il reset), NON si
        # tocca il daily: ripristinare il suo snapshot riporterebbe un giorno corrotto (state file
        # malformato) e lascerebbe il bridge bloccato per sempre (#184 low-tracker-nonwrite, Codex).
        tracker.restore_state(tracker_snap)
    elif tracker is not None and decision == live_guard.DRY_RUN:
        # Simulazione: `evaluate` ha registrato l'hash E consumato una slot giornaliera REALE per un
        # segnale MAI scritto. Si annulla l'hash e si RESTITUISCE la slot con `release()` (decremento
        # che MANTIENE il giorno normalizzato): così la simulazione non intacca tetto/dedupe reali
        # senza scartare la normalizzazione del giorno. DUPLICATE/RATE_LIMITED non aggiungono nulla.
        tracker.restore_state(tracker_snap)
        if daily is not None:
            daily.release()

    return CommitResult(decision=decision, blocked_by_cap=blocked_by_cap,
                        rows=rows, write_error=write_error,
                        write_attempted=write_attempted)


def _summary_decision(decisions: list, accepted: int) -> str:
    """Esito riassuntivo del commit multi-riga: WRITE se almeno una riga è stata accodata,
    altrimenti il primo esito di soppressione (DUPLICATE/RATE_LIMITED/DAILY_LIMITED) per la
    diagnostica."""
    if accepted > 0:
        return live_guard.WRITE
    for d in decisions:
        if d != live_guard.WRITE:
            return d
    return live_guard.WRITE


def _noop_decision(decisions: list) -> str:
    """Esito di un commit `OVERWRITE_LAST` che NON ha cambiato il CSV (reinvio identico, o righe
    tutte soppresse/non-attive): **mai** WRITE — nulla è stato scritto, quindi `_process` deve
    prendere il percorso non-write. Riporta il primo esito di soppressione se presente; se invece
    tutte le righe erano WRITE ma il blocco coincideva già con l'attivo (chiavi dedup scadute con
    `clear_delay` > finestra dedup), l'esito è `DUPLICATE` (di fatto un reinvio identico)."""
    for d in decisions:
        if d != live_guard.WRITE:
            return d
    return live_guard.DUPLICATE


def _same_rows_unordered(a: list, b: list) -> bool:
    """True se `a` e `b` contengono lo **stesso multiset di righe** a PRESCINDERE dall'ordine. In
    `OVERWRITE_LAST` un reinvio con le sole righe **riordinate** (`A+B` vs `B+A`) è semanticamente
    identico: il CSV non va riscritto (XTrader non deve riconsumare un'istruzione uguale) — Codex
    #281. Le righe sono dict con valori stringa (contratto CSV), quindi confrontabili per contenuto."""
    if len(a) != len(b):
        return False
    ca = sorted(tuple(sorted(r.items())) for r in a)
    cb = sorted(tuple(sorted(r.items())) for r in b)
    return ca == cb


def commit_signals(tracker, daily, queue, cfg, text, rows, path, now, write_rows):
    """Commit MULTI-RIGA (#192): un singolo messaggio produce più righe (MultiMarket/
    MultiSelection). Valuta OGNI riga con **deduplica PER-RIGA** (`signal_dedupe.row_dedup_key`),
    accoda le righe `WRITE` e riscrive ATOMICAMENTE tutte le righe attive, con rollback fail-safe.

    Stesse invarianti di `commit_signal` (vedi docstring del modulo): chiamato SOTTO il lock del
    chiamante; solo le righe `WRITE` finiscono in coda; in `DRY_RUN` il CSV operativo NON viene
    scritto e i guardrail consumati sono ripristinati; se la scrittura fallisce, coda E guardrail
    tornano allo stato precedente (segnali ritentabili). Per il single-row usare `commit_signal`
    (percorso legacy, comportamento bit-identico e invariato).

    Accodamento per modo coda (Codex/CodeRabbit #239/#192):
    - `OVERWRITE_LAST`: l'«ultima istruzione» è il BLOCCO INTERO del messaggio → il blocco è
      composto dalle righe NUOVE (`WRITE`) del messaggio PIÙ le righe `DUPLICATE` che sono **ancora
      attive con la STESSA provenienza** (chiave dedup memorizzata al piazzamento, confrontata via
      `queue.active_keys`), con i **valori del messaggio corrente**. Così: un'espansione `A→A+B`
      riscrive TENENDO A (kyh #192); un duplicato **scaduto** NON viene rivissuto (rispetta il
      clear-timeout) e due regole che risolvono alla **stessa riga** non la scrivono due volte
      (dedup intra-blocco) — Codex #281 P1. Il CSV è riscritto SOLO se il blocco **differisce, per
      contenuto, dalle righe già attive**: un reinvio identico non tocca il CSV (XTrader non
      riconsuma; su questo no-op i guardrail consumati da eventuali chiavi scadute sono
      **ripristinati**), uno shrink `A+B→A` riscrive togliendo B, un blocco vuoto NON svuota il CSV
      (lo svuotamento a timeout è dell'expire-tick).
    - `APPEND_ACTIVE`/`QUEUE_UNTIL_CONFIRMED`: `queue.add(..., force=True)` per ogni riga NUOVA →
      **auto-raise del tetto** (decisione proprietario #192): il blocco coerente di UN messaggio
      multi NON viene MAI spezzato dal tetto `max_active`. Elimina alla radice il partial-drop
      silenzioso (alcune righe scritte, altre troncate dal tetto senza avviso; Codex #281). Il
      tetto continua a limitare l'accumulo TRA messaggi distinti sul percorso single-row.

    Accounting guardrail per-riga (mirror del single-row): una riga `DAILY_LIMITED` NON è scritta →
    il suo consumo di tracker viene annullato (segnale ritentabile; il daily non aveva consumato
    slot, solo normalizzato il giorno). Se NESSUNA riga NUOVA è accettata (tutte duplicati/limiti),
    il CSV operativo NON viene toccato (come il single-row su DUPLICATE). In `DRY_RUN` non si scrive
    e i guardrail sono ripristinati. Se la scrittura fallisce, coda E guardrail tornano allo stato
    precedente. Per il single-row usare `commit_signal` (percorso legacy invariato)."""
    rows = list(rows or [])
    # kyW #192: fallback cross-namespace PRE-scrittura per uno stato dedupe PERSISTITO da una versione
    # precedente SOLO single-row (che ha registrato l'hash-messaggio ma NON le chiavi per-riga). Se
    # l'hash-messaggio è già visto e NESSUNA delle chiavi per-riga di questo messaggio lo è, il
    # messaggio è già stato processato come single-row → fail-closed: si sopprime l'intero blocco (non
    # è possibile identificare quale riga corrispondesse), invece di riscriverlo → doppia scommessa. Se
    # invece almeno una chiave per-riga è già vista, è un normale duplicato/espansione multi (gestito
    # per-riga, così l'espansione A→A+B non viene soppressa).
    if tracker is not None and rows:
        row_keys = [signal_dedupe.row_dedup_key(text, r) for r in rows]
        if tracker.is_seen(signal_dedupe.message_hash(text)) and \
                not any(tracker.is_seen(k) for k in row_keys):
            return CommitResult(decision=live_guard.DUPLICATE, blocked_by_cap=False,
                                rows=[], write_error=None)
    tracker_snap = tracker.state() if tracker is not None else None
    daily_snap = (daily.state() if (tracker is not None and daily is not None) else None)
    queue_snap = queue.state()
    overwrite = queue.mode == signal_queue.OVERWRITE_LAST
    queue.expire(now=now)
    # OVERWRITE_LAST: chiavi (PROVENIENZA esatta, memorizzate al piazzamento) delle righe ANCORA
    # attive dopo l'expire. Una riga duplicata dell'istruzione va tenuta nel blocco SOLO se
    # corrisponde a una di queste (stessa chiave = stessa riga già piazzata): così non si rivive un
    # segnale SCADUTO (clear-timeout) né si scambia una riga di un ALTRO messaggio con la corrente
    # (Codex #281 P1/provenance). Le chiavi sono lette dalla coda, NON ricalcolate dal testo corrente.
    active_keys = set(queue.active_keys(now=now)) if overwrite else set()

    decisions = []
    new_rows = []          # righe NUOVE (WRITE) di QUESTO messaggio, effettivamente piazzate
    block = []             # OVERWRITE_LAST: righe dell'istruzione corrente da tenere attive
    block_keys = []        # chiavi parallele a `block` (provenienza, memorizzate per il commit dopo)
    seen_in_block = set()  # dedup INTRA-blocco: una stessa chiave non entra due volte (no doppia riga)
    for row in rows:
        if tracker is None:
            # Chiamanti senza guardrail (test): ogni riga è NUOVA, nessuna dedup disponibile.
            decisions.append(live_guard.WRITE)
            new_rows.append(row)
            block.append(row)
            block_keys.append("")
            if not overwrite:
                queue.add(row, now=now, force=True)
            continue
        key = signal_dedupe.row_dedup_key(text, row)
        row_tracker_snap = tracker.state()
        d = live_guard.evaluate(cfg, tracker, daily, text, dedup_key=key)
        decisions.append(d)
        if d == live_guard.WRITE:
            new_rows.append(row)
            if overwrite:
                if key not in seen_in_block:          # dedup intra-blocco (due regole → stessa riga)
                    block.append(row)
                    block_keys.append(key)
                    seen_in_block.add(key)
            else:
                # Auto-raise del tetto (#192): `force=True` → la riga NUOVA entra sempre; la chiave
                # è memorizzata sul segnale per la provenienza al commit successivo.
                queue.add(row, now=now, force=True, dedup_key=key)
        elif d == live_guard.DUPLICATE:
            # OVERWRITE_LAST: una riga duplicata resta nel blocco SOLO se è ANCORA attiva con la
            # STESSA provenienza (kyh #192): altrimenti riscriverebbe un segnale scaduto (viola il
            # clear-timeout) o duplicherebbe una riga già presente (Codex #281 P1). Si usa la riga
            # del MESSAGGIO corrente (valori aggiornati), non quella stantia in coda; dedup intra-blocco.
            if overwrite and key in active_keys and key not in seen_in_block:
                block.append(row)
                block_keys.append(key)
                seen_in_block.add(key)
        elif d == live_guard.DAILY_LIMITED:
            # `daily.allow` ha rifiutato senza consumare slot ma `evaluate` ha registrato l'hash:
            # annulla SOLO il tracker (come single-row), non il daily (giorno normalizzato).
            tracker.restore_state(row_tracker_snap)

    # DRY_RUN: simulazione → NON scrivere il CSV operativo; ripristina coda E tracker, e RESTITUISCI
    # le slot daily consumate (una per riga passata a DRY_RUN) con `release()` invece di ripristinare
    # lo snapshot: `release` mantiene il giorno già normalizzato da `allow`, mentre `restore_state`
    # reintrodurrebbe un giorno malformato dello stato di partenza (allineato al single-row, Codex #281).
    if safety_guard.is_dry_run(cfg):
        queue.restore_state(queue_snap)
        if tracker is not None:
            tracker.restore_state(tracker_snap)
            if daily is not None:
                for _ in range(sum(1 for d in decisions if d == live_guard.DRY_RUN)):
                    daily.release()
        return CommitResult(decision=live_guard.DRY_RUN, blocked_by_cap=False,
                            rows=[], write_error=None)

    if overwrite:
        # Il blocco È l'istruzione corrente (righe nuove + duplicate ANCORA attive). Si riscrive il
        # CSV SOLO se differisce — per contenuto — dalle righe già attive: reinvio identico → nessuna
        # riscrittura (XTrader non riconsuma), espansione A→A+B → riscrive tenendo A (kyh #192),
        # shrink A+B→A → riscrive togliendo B. Un blocco vuoto (tutte soppresse/scadute) NON svuota
        # il CSV: lo svuotamento a timeout è compito dell'expire-tick, non di un reinvio.
        current_active = queue.active_rows(now=now)
        if not block or _same_rows_unordered(block, current_active):
            # NESSUN cambiamento reale (blocco == attivo, anche solo RIORDINATO) → non riscrivere e
            # RIPRISTINA i guardrail: una riga WRITE con chiave dedup SCADUTA (clear_delay > finestra
            # dedup) ha già registrato tracker/daily pur non scrivendo nulla; senza rollback conterebbe
            # un non-write contro i limiti e `_process` lo vedrebbe come WRITE riuscito (Codex #281).
            queue.restore_state(queue_snap)
            if tracker is not None:
                tracker.restore_state(tracker_snap)
                # daily: `release()` per ogni slot consumata dalle righe WRITE (aged-out), mantenendo
                # il giorno normalizzato — non `restore_state` (reintrodurrebbe un giorno malformato).
                if daily is not None:
                    for _ in range(len(new_rows)):
                        daily.release()
            return CommitResult(decision=_noop_decision(decisions), blocked_by_cap=False,
                                rows=current_active, write_error=None)
        queue.replace_block(block, now=now, keys=block_keys)
    elif not new_rows:
        # APPEND/QUEUE: nessuna riga NUOVA accodata (tutte duplicati/limiti) → CSV invariato (come il
        # single-row su DUPLICATE). Nessun guardrail da annullare: in append le righe WRITE sono
        # sempre accodate (`force=True`) quindi finiscono in `new_rows`; qui non ce n'erano.
        queue.restore_state(queue_snap)
        return CommitResult(decision=_summary_decision(decisions, 0), blocked_by_cap=False,
                            rows=[], write_error=None)

    active = queue.active_rows()
    try:
        write_rows(active, path)
    except Exception as ex:   # noqa: BLE001 — riportato al chiamante, no crash
        queue.restore_state(queue_snap)
        if tracker is not None:
            tracker.restore_state(tracker_snap)
            if daily is not None and daily_snap is not None:
                daily.restore_state(daily_snap)
        return CommitResult(decision=live_guard.WRITE, blocked_by_cap=False,
                            rows=[], write_error=ex, write_attempted=True)

    # Si arriva qui solo dopo un CAMBIAMENTO reale (blocco OVERWRITE differente, o righe NUOVE in
    # append) e una scrittura riuscita → l'esito è WRITE (uno shrink OVERWRITE ha `new_rows` vuoto
    # ma HA scritto: non va riportato come DUPLICATE, altrimenti `_process` salterebbe il post-write).
    if tracker is not None:
        # Scrittura REALE riuscita (multi, dedup PER-RIGA): ombreggia ANCHE l'hash-messaggio (#192
        # kyW). Così un retry dello STESSO messaggio dopo una transizione del parser a SINGLE-row —
        # che deduplica sull'hash-messaggio, mai registrato dal percorso multi — riconosce il
        # messaggio come già processato (DUPLICATE) invece di riscriverlo → doppia scommessa.
        # Fail-closed a livello di messaggio: al più restrittivo, mai una scommessa doppia. Lo
        # shadow non conta verso il rate-limit (`mark_seen`).
        tracker.mark_seen(signal_dedupe.message_hash(text))
    return CommitResult(decision=live_guard.WRITE, blocked_by_cap=False,
                        rows=active, write_error=None, write_attempted=True)
