from observability.correlation_engine import correlate_events


def test_correlate_events_groups_by_shared_identity_within_window():
    events = [
        {"id": "a", "ts": 1000, "request_id": "req-1", "user_id": "u-1"},
        {"id": "b", "ts": 1010, "request_id": "req-1", "user_id": "u-2"},
        {"id": "c", "ts": 1020, "request_id": "req-2", "user_id": "u-2"},
    ]

    correlations = correlate_events(events, window_seconds=60)

    assert len(correlations) == 1
    cluster = correlations[0]
    assert cluster["event_ids"] == ["a", "b", "c"]
    assert cluster["shared"] == {}


def test_correlate_events_respects_time_window():
    events = [
        {"id": "a", "ts": 1000, "trace_id": "t-1"},
        {"id": "b", "ts": 1405, "trace_id": "t-1"},
    ]

    correlations = correlate_events(events, window_seconds=300)

    assert correlations == []


def test_correlate_events_ignores_invalid_timestamps_and_does_not_mutate_inputs():
    events = [
        {"id": "a", "ts": "1000", "request_id": "req-1"},
        {"id": "b", "ts": 1005, "request_id": "req-1"},
        {"id": "c", "ts": 1006, "request_id": "req-1"},
    ]
    snapshot = [dict(item) for item in events]

    correlations = correlate_events(events)

    assert len(correlations) == 1
    assert correlations[0]["event_ids"] == ["b", "c"]
    assert events == snapshot
