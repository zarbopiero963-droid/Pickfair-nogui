"""Scrittura atomica unica e condivisa (audit #105 — helper trasversale).

Tutti i salvataggi safety-critical del bridge (config, CSV segnali, stato
dedupe/daily, parser, profili) devono essere ATOMICI: un crash/blackout o un
errore a metà scrittura non deve mai lasciare un file troncato al posto di uno
valido — altrimenti XTrader leggerebbe un CSV parziale o l'app ripartirebbe dai
default per un `config.json` mezzo scritto.

La sequenza corretta (``mkstemp`` nella STESSA cartella del file finale →
scrivi → ``flush`` → ``os.fsync`` → ``os.replace``, con rimozione del
temporaneo su QUALSIASI errore) era duplicata in 7 moduli, con rischio di drift
fra le copie. Qui è centralizzata una volta sola: i moduli delegano a queste
funzioni invece di re-implementare lo schema.

Garanzie:
- il temporaneo sta nella stessa cartella del file finale, così ``os.replace``
  è un rename atomico sullo stesso filesystem (niente copy cross-device);
- su qualsiasi eccezione (scrittura o rename) il temporaneo viene rimosso e
  l'eccezione ri-sollevata: il file preesistente resta intatto;
- ``mkstemp`` dà un nome unico, quindi due scritture concorrenti sullo stesso
  path non si pestano il temporaneo a vicenda.
"""

import json
import os
import tempfile


def _fsync_dir(d):
    """fsync della DIRECTORY contenitore dopo ``os.replace`` (issue #184 H2).

    I dati del file sono già fsync'd, ma POSIX **non** garantisce che la voce di
    directory creata dal rename sia durabile finché non si fsync-a anche la directory:
    su power-loss/crash subito dopo ``os.replace`` il file può tornare al contenuto
    precedente (CSV stantio, stato dedupe/daily/config vecchio). Qui si rende durabile
    anche il rename.

    **Best-effort e non solleva mai**: dove non è supportato (Windows non permette di
    aprire una directory come fd; alcuni filesystem rifiutano l'fsync di una dir) è un
    no-op silenzioso. Importante: viene chiamato DOPO un ``replace`` già riuscito, quindi
    un suo errore non deve propagare né rimuovere il file appena scritto."""
    try:
        dir_fd = os.open(d, os.O_RDONLY)
    except OSError:
        return                              # es. Windows: dir non apribile come fd → no-op
    try:
        os.fsync(dir_fd)
    except OSError:
        pass                                # fs che non supporta l'fsync di una dir → no-op
    finally:
        try:
            os.close(dir_fd)
        except OSError:
            pass                            # anche il close è best-effort: mai propagare
                                            # un errore DOPO un replace già riuscito (CodeRabbit)


def atomic_write(path, write_fn, *, prefix="tmp_", suffix=".tmp", mode="w",
                 encoding="utf-8", newline=None, replace=None):
    """Scrive `path` in modo atomico eseguendo ``write_fn(f)`` su un file
    temporaneo nella STESSA cartella, poi ``flush`` + ``os.fsync`` e infine
    ``replace(tmp, path)``.

    Su qualsiasi eccezione (anche dentro ``write_fn``, anche nel rename) il
    temporaneo viene rimosso e l'eccezione ri-sollevata, lasciando il file
    preesistente intatto.

    Parametri:
    - ``write_fn``: callback che riceve il file aperto e ci scrive il contenuto.
    - ``prefix``/``suffix``: nome del temporaneo. Alcuni chiamanti/test filtrano
      i file per prefisso (es. ``.segnali_`` del CSV, ``.parser_``), quindi va
      preservato per sito.
    - ``mode``/``encoding``/``newline``: passati a ``os.fdopen``. Per la modalità
      binaria (``"wb"``) usare ``encoding=None``: ``newline`` non viene passato.
    - ``replace``: rename finale. Iniettabile per il retry su lock Windows
      (``csv_writer._replace_with_retry``). ``None`` (default) usa ``os.replace``
      risolto a CALL-TIME, così un test può patchare ``atomic_io.os.replace``.
    """
    if replace is None:
        replace = os.replace
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=prefix, suffix=suffix)
    try:
        open_kwargs = {}
        if "b" not in mode:
            # `newline`/`encoding` sono argomenti SOLO della modalità testo:
            # passarli in binario solleverebbe ValueError.
            open_kwargs["encoding"] = encoding
            open_kwargs["newline"] = newline
        with os.fdopen(fd, mode, **open_kwargs) as f:
            write_fn(f)
            f.flush()
            os.fsync(f.fileno())
        replace(tmp, path)
        # Rende DURABILE anche la voce di directory del rename (H2). Best-effort e non
        # solleva: il file è già al suo posto, un fallimento qui non deve perderlo.
        _fsync_dir(d)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def sweep_orphan_temps(directory, prefix, suffix=".tmp"):
    """Rimuove i temporanei ORFANI (`{prefix}…{suffix}`) lasciati in `directory`
    da una scrittura atomica interrotta da crash/blackout TRA ``mkstemp`` e
    ``os.replace`` (issue #184 LOW — `atomic_io.py`).

    ``atomic_write`` rimuove il proprio temporaneo su qualsiasi eccezione *gestita*,
    ma un crash duro del processo (power-loss, kill) tra la creazione del tmp e il
    rename salta quel cleanup: il file FINALE resta intatto (il rename non è ancora
    avvenuto) ma il temporaneo resta su disco e si accumula riavvio dopo riavvio.
    Va chiamata **allo startup**, quando non c'è alcuna scrittura in volo: ogni file
    che combacia con `prefix`+`suffix` è per forza orfano di un processo morto.

    Sicurezza:
    - rimuove SOLO i nomi che iniziano con `prefix` **e** finiscono con `suffix`: i
      file finali (es. il CSV reale, `config.json`) non hanno quel prefisso/suffisso
      e non vengono mai toccati;
    - `prefix` vuoto è un **no-op** (rifiuto di spazzare un'intera cartella per suffisso);
    - **best-effort e non solleva mai**: cartella inesistente/non listabile → 0; un
      singolo `os.remove` fallito (file in uso, permessi) viene saltato. Non deve mai
      impedire l'avvio dell'app.

    Ritorna il numero di temporanei effettivamente rimossi (utile per il log)."""
    if not prefix:
        return 0                                # guardia: mai spazzare per solo suffisso
    d = str(directory or "").strip()
    if not d:
        return 0
    try:
        names = os.listdir(d)
    except OSError:
        return 0                                # cartella assente/non listabile → niente da fare
    removed = 0
    for name in names:
        if not (name.startswith(prefix) and name.endswith(suffix)):
            continue
        full = os.path.join(d, name)
        try:
            if os.path.isfile(full):            # mai rimuovere una sottocartella omonima
                os.remove(full)
                removed += 1
        except OSError:
            pass                                # file in uso/permessi: salta, best-effort
    return removed


def atomic_write_text(path, text, *, prefix="tmp_", suffix=".tmp",
                      encoding="utf-8", newline=None, replace=None):
    """Scrive la stringa `text` su `path` in modo atomico (vedi `atomic_write`)."""
    atomic_write(path, lambda f: f.write(text), prefix=prefix, suffix=suffix,
                 encoding=encoding, newline=newline, replace=replace)


def atomic_write_json(path, obj, *, prefix="tmp_", suffix=".tmp",
                      encoding="utf-8", replace=None, **dump_kwargs):
    """Serializza `obj` in JSON e lo scrive su `path` in modo atomico.

    ``dump_kwargs`` è inoltrato a ``json.dumps`` (es. ``indent=2``,
    ``ensure_ascii=False``)."""
    atomic_write_text(path, json.dumps(obj, **dump_kwargs), prefix=prefix,
                      suffix=suffix, encoding=encoding, replace=replace)
