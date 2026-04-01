from __future__ import annotations

import pytest

from order_manager import (
    map_betfair_status,
    OrderStatus,
    ReasonCode,
)


class TestBetfairMapping:
    def test_success_full_match(self) -> None:
        status, reason = map_betfair_status("SUCCESS", "", 10.0, 10.0)
        assert status == OrderStatus.MATCHED
        assert reason == ReasonCode.FULLY_MATCHED

    def test_success_partial_match(self) -> None:
        status, reason = map_betfair_status("SUCCESS", "", 4.0, 10.0)
        assert status == OrderStatus.PARTIALLY_MATCHED
        assert reason == ReasonCode.PARTIALLY_MATCHED

    def test_success_zero_match(self) -> None:
        status, reason = map_betfair_status("SUCCESS", "", 0.0, 10.0)
        assert status == OrderStatus.PLACED
        assert reason == ReasonCode.PLACED_OK

    def test_failure_leg(self) -> None:
        status, reason = map_betfair_status("FAILURE", "", 0.0, 10.0)
        assert status == OrderStatus.FAILED
        assert reason == ReasonCode.BROKER_REJECTED

    def test_unknown_leg_with_processed_overall_uses_sizes(self) -> None:
        status, reason = map_betfair_status("WEIRD", "PROCESSED", 5.0, 10.0)
        assert status == OrderStatus.PARTIALLY_MATCHED
        assert reason == ReasonCode.PARTIALLY_MATCHED

    def test_unknown_leg_with_processed_with_errors_overall_is_ambiguous(self) -> None:
        status, reason = map_betfair_status("WEIRD", "PROCESSED_WITH_ERRORS", 0.0, 10.0)
        assert status == OrderStatus.AMBIGUOUS
        assert reason == ReasonCode.AMBIGUOUS_OUTCOME

    def test_success_leg_wins_over_processed_with_errors_overall(self) -> None:
        status, reason = map_betfair_status("SUCCESS", "PROCESSED_WITH_ERRORS", 10.0, 10.0)
        assert status == OrderStatus.MATCHED
        assert reason == ReasonCode.FULLY_MATCHED

    @pytest.mark.parametrize(
        ("leg_status", "overall_status", "size_matched", "requested_stake"),
        [
            ("SUCCESS", "SUCCESS", 0.0, 10.0),
            ("SUCCESS", "SUCCESS", 4.0, 10.0),
            ("SUCCESS", "SUCCESS", 10.0, 10.0),
        ],
    )
    def test_success_mapping_is_monotonic_by_matched_size(
        self,
        leg_status: str,
        overall_status: str,
        size_matched: float,
        requested_stake: float,
    ) -> None:
        status, _ = map_betfair_status(leg_status, overall_status, size_matched, requested_stake)
        expected = (
            OrderStatus.PLACED if size_matched == 0.0
            else OrderStatus.PARTIALLY_MATCHED if size_matched < requested_stake
            else OrderStatus.MATCHED
        )
        assert status == expected