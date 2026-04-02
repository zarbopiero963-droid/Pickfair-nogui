from __future__ import annotations

import pytest

from core.reconciliation_engine import ReconciliationEngine


@pytest.mark.parametrize(
    "local_status,remote_present,remote_status,saga_pending,expected",
    [
        ("UNKNOWN", False, None, False, "LOCAL_INFLIGHT_EXCHANGE_ABSENT"),
        ("PLACED", True, "MATCHED", False, "LOCAL_AMBIGUOUS_EXCHANGE_MATCHED"),
        ("ABSENT", True, "MATCHED", False, "LOCAL_ABSENT_EXCHANGE_PRESENT"),
        ("PLACED", True, "PARTIAL", False, "SPLIT_STATE"),
    ],
)
def test_classify_case(local_status, remote_present, remote_status, saga_pending, expected):
    remote = {"status": remote_status} if remote_present else None
    got = ReconciliationEngine._classify_case(local_status, remote, remote_status, saga_pending)
    assert got == expected 