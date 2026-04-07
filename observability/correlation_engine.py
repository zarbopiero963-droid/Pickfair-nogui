from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


DEFAULT_MATCH_FIELDS: Tuple[str, ...] = (
    "trace_id",
    "request_id",
    "session_id",
    "order_id",
    "user_id",
)


def correlate_events(
    events: Iterable[Mapping[str, Any]],
    *,
    timestamp_field: str = "ts",
    match_fields: Sequence[str] = DEFAULT_MATCH_FIELDS,
    window_seconds: int = 300,
) -> List[Dict[str, Any]]:
    """Correlate events by shared identity fields within a time window.

    This function is intentionally isolated and side-effect free; it consumes the
    input events and returns deterministic correlation clusters.
    """

    event_list = list(events)
    if not event_list:
        return []

    prepared: List[Dict[str, Any]] = []
    for index, event in enumerate(event_list):
        ts = event.get(timestamp_field)
        if not isinstance(ts, (int, float)):
            continue
        prepared.append({"index": index, "event": event, "ts": float(ts)})

    prepared.sort(key=lambda item: item["ts"])
    if not prepared:
        return []

    parents = list(range(len(prepared)))

    def find(node: int) -> int:
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for left in range(len(prepared)):
        for right in range(left + 1, len(prepared)):
            delta = prepared[right]["ts"] - prepared[left]["ts"]
            if delta > window_seconds:
                break
            for field in match_fields:
                left_value = prepared[left]["event"].get(field)
                if left_value is None or left_value == "":
                    continue
                if left_value == prepared[right]["event"].get(field):
                    union(left, right)
                    break

    groups: Dict[int, List[int]] = defaultdict(list)
    for idx in range(len(prepared)):
        groups[find(idx)].append(idx)

    correlations: List[Dict[str, Any]] = []
    for cluster_num, members in enumerate(groups.values(), start=1):
        if len(members) < 2:
            continue

        cluster_events = [prepared[item] for item in members]
        shared: Dict[str, Any] = {}
        for field in match_fields:
            values = {entry["event"].get(field) for entry in cluster_events}
            values.discard(None)
            values.discard("")
            if len(values) == 1:
                shared[field] = next(iter(values))

        correlations.append(
            {
                "cluster_id": f"corr-{cluster_num}",
                "event_indices": [entry["index"] for entry in cluster_events],
                "event_ids": [entry["event"].get("id") for entry in cluster_events],
                "start_ts": cluster_events[0]["ts"],
                "end_ts": cluster_events[-1]["ts"],
                "shared": shared,
            }
        )

    correlations.sort(key=lambda item: (item["start_ts"], item["cluster_id"]))
    return correlations
