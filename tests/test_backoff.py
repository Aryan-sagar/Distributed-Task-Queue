import pytest

from broker.backoff import compute_backoff_seconds


def test_delay_is_within_bounds_for_each_attempt():
    for attempt in range(1, 8):
        expected_cap = min(60.0, 1.0 * (2 ** (attempt - 1)))
        for _ in range(20):
            delay = compute_backoff_seconds(attempt, base=1.0, cap=60.0)
            assert 0 <= delay <= expected_cap


def test_delay_respects_custom_cap():
    for _ in range(20):
        delay = compute_backoff_seconds(attempt=10, base=1.0, cap=5.0)
        assert 0 <= delay <= 5.0


def test_attempt_must_be_at_least_one():
    with pytest.raises(ValueError):
        compute_backoff_seconds(attempt=0)
