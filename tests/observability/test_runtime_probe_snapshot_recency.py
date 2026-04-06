import time

from observability.runtime_probe import RuntimeProbe


class _DbRecentDict:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [{"timestamp": time.time() - 5}]


class _DbStaleDict:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [{"timestamp": time.time() - 120}]


class _DbMissingTs:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [{"id": 1}]


class _DbInvalidTs:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [{"timestamp": "not-a-number"}]


class _RowObj:
    def __init__(self, timestamp):
        self.timestamp = timestamp


class _DbObjTs:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [_RowObj(time.time() - 10)]


class _DbRecentCreatedAtDict:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [{"created_at": time.time() - 5}]


class _RowCreatedAtObj:
    def __init__(self, created_at):
        self.timestamp = None
        self.created_at = created_at


class _DbObjCreatedAt:
    def get_recent_observability_snapshots(self, limit=1):
        _ = limit
        return [_RowCreatedAtObj(time.time() - 10)]


def test_snapshot_recency_recent_timestamp_true():
    probe = RuntimeProbe(db=_DbRecentDict())
    assert probe._forensics_state()["observability_snapshot_recent"] is True


def test_snapshot_recency_stale_timestamp_false():
    probe = RuntimeProbe(db=_DbStaleDict())
    assert probe._forensics_state()["observability_snapshot_recent"] is False


def test_snapshot_recency_missing_timestamp_false():
    probe = RuntimeProbe(db=_DbMissingTs())
    assert probe._forensics_state()["observability_snapshot_recent"] is False


def test_snapshot_recency_invalid_timestamp_false():
    probe = RuntimeProbe(db=_DbInvalidTs())
    assert probe._forensics_state()["observability_snapshot_recent"] is False


def test_snapshot_recency_object_timestamp_supported():
    probe = RuntimeProbe(db=_DbObjTs())
    assert probe._forensics_state()["observability_snapshot_recent"] is True


def test_snapshot_recency_created_at_dict_supported():
    probe = RuntimeProbe(db=_DbRecentCreatedAtDict())
    assert probe._forensics_state()["observability_snapshot_recent"] is True


def test_snapshot_recency_created_at_object_supported():
    probe = RuntimeProbe(db=_DbObjCreatedAt())
    assert probe._forensics_state()["observability_snapshot_recent"] is True
