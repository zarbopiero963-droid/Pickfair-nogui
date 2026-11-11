"""Gate A T2b batch b3: core/atomic_io.py + core/async_db_writer.py (oracolo tecnico P35)."""
from __future__ import annotations

import os
import stat

import pytest

from core import atomic_io
from core.async_db_writer import AsyncDBWriter


def test_trg_628_b_0990(tmp_path, monkeypatch):
    """ID: TRG-628-B-0990 / PF-B1-R2-ATOMIC-REPLACE

    atomic_write: temporaneo nella STESSA directory, write -> flush -> fsync del
    file -> replace sulla destinazione -> fsync directory best effort (un suo
    errore non propaga e non perde il file). Errore in scrittura => temporaneo
    rimosso e file preesistente intatto.
    """
    target = tmp_path / "sub" / "state.json"
    os.makedirs(target.parent)
    target.write_text("OLD", encoding="utf-8")
    events = []
    real_fsync, real_replace = os.fsync, os.replace

    def _fsync(fd):
        is_dir = stat.S_ISDIR(os.fstat(fd).st_mode)
        events.append(("fsync_dir" if is_dir else "fsync_file",))
        if is_dir:
            raise OSError("dir fsync non supportato")       # best effort: non deve propagare
        real_fsync(fd)

    def _replace(src, dst):
        events.append(("replace", os.path.dirname(os.path.abspath(src)), os.path.abspath(dst)))
        assert open(src, encoding="utf-8").read() == "NEW"   # contenuto gia' durabile prima del rename
        real_replace(src, dst)

    def _write(f):
        events.append(("write",))
        f.write("NEW")

    monkeypatch.setattr(atomic_io.os, "fsync", _fsync)
    monkeypatch.setattr(atomic_io.os, "replace", _replace)
    atomic_io.atomic_write(str(target), _write)

    assert events == [
        ("write",),
        ("fsync_file",),
        ("replace", str(target.parent), str(target)),
        ("fsync_dir",),
    ]
    assert target.read_text(encoding="utf-8") == "NEW"
    assert sorted(os.listdir(target.parent)) == ["state.json"]

    # BLOCK: errore a meta' scrittura => file precedente intatto, nessun temporaneo residuo
    def _boom(f):
        f.write("PARZIALE")
        raise RuntimeError("crash")

    with pytest.raises(RuntimeError):
        atomic_io.atomic_write(str(target), _boom)
    assert target.read_text(encoding="utf-8") == "NEW"
    assert sorted(os.listdir(target.parent)) == ["state.json"]


class _AuditDb:
    def __init__(self, names):
        self.calls = []
        for name in names:
            setattr(self, name, (lambda n: (lambda payload: self.calls.append((n, payload))))(name))


def test_trg_628_b_0997():
    """ID: TRG-628-B-0997 / PF-B1-R2-ASYNC-AUDIT-CONTRACT

    AsyncDBWriter.write valida il tipo dict e il contratto audit del DB PRIMA
    del submit: payload non dict => TypeError; DB senza metodo audit =>
    AttributeError; in entrambi i casi nulla accodato.
    """
    w = AsyncDBWriter(_AuditDb(["insert_audit_event"]))
    for bad in (None, ["x"], "evt", 3):
        with pytest.raises(TypeError):
            w.write(bad)
    assert w.queue.qsize() == 0 and w.stats()["submitted"] == 0

    w2 = AsyncDBWriter(_AuditDb([]))
    with pytest.raises(AttributeError, match="audit contract mismatch"):
        w2.write({"event": "ORDER_PLACED"})
    assert w2.queue.qsize() == 0 and w2.stats()["submitted"] == 0

    # PASS: contratto valido => accodato
    assert w.write({"event": "ORDER_PLACED"}) is True
    assert w.queue.qsize() == 1


def test_trg_628_b_0998():
    """ID: TRG-628-B-0998 / PF-B1-R2-ASYNC-AUDIT-RESOLVE

    Il writer audit e' il PRIMO callable fra insert_audit_event,
    insert_order_event, append_order_event; attributi non callable ignorati.
    """
    db = _AuditDb(["insert_audit_event", "insert_order_event", "append_order_event"])
    AsyncDBWriter(db)._resolve_audit_writer()({"a": 1})
    assert db.calls == [("insert_audit_event", {"a": 1})]

    db = _AuditDb(["insert_order_event", "append_order_event"])
    AsyncDBWriter(db)._resolve_audit_writer()({"b": 2})
    assert db.calls == [("insert_order_event", {"b": 2})]

    db = _AuditDb(["append_order_event"])
    db.insert_audit_event = "non callable"
    db.insert_order_event = None
    AsyncDBWriter(db)._resolve_audit_writer()({"c": 3})
    assert db.calls == [("append_order_event", {"c": 3})]

    # il percorso reale di scrittura usa lo stesso writer risolto
    db = _AuditDb(["insert_order_event", "append_order_event"])
    AsyncDBWriter(db)._write("audit_event", {"d": 4})
    assert db.calls == [("insert_order_event", {"d": 4})]
