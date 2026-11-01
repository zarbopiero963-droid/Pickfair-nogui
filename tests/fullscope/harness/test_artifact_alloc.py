"""F-33 — nomi degli artifact di scenario allocati in modo esclusivo (rilievo Codex su a721e09).

Prima: conteggio dei file esistenti + ``open(..., "w")``; due worker xdist con lo stesso scenario sceglievano lo
stesso ``<scenario>_NN.json`` e l'ultimo sovrascriveva il primo. PASS: N scrittori concorrenti -> N file distinti,
ciascuno integro, e un file esistente non viene mai riscritto.
"""
from __future__ import annotations

import json
import os
import threading

from tests.fullscope.harness import runner as H


def _race(tmp_path, writer, n=24):
    barrier = threading.Barrier(n)

    def work(i):
        barrier.wait()
        writer(str(tmp_path), "shared", {"writer": i})

    threads = [threading.Thread(target=work, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    files = sorted(f for f in os.listdir(tmp_path) if f.startswith("shared_"))
    writers = sorted(json.load(open(os.path.join(tmp_path, f)))["writer"] for f in files)
    return files, writers


def _new_scheme(art, scenario, payload):
    with H._open_new_artifact(art, scenario) as fh:
        json.dump(payload, fh)


def test_concurrent_writers_get_distinct_files(tmp_path):
    files, writers = _race(tmp_path, _new_scheme)
    assert writers == list(range(24))
    assert len(files) == 24


def test_existing_file_is_never_overwritten(tmp_path):
    (tmp_path / "shared_00.json").write_text('{"writer": "old"}')
    _new_scheme(str(tmp_path), "shared", {"writer": "new"})
    assert json.loads((tmp_path / "shared_00.json").read_text()) == {"writer": "old"}
    assert json.loads((tmp_path / "shared_01.json").read_text()) == {"writer": "new"}
