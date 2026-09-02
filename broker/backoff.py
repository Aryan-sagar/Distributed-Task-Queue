"""Exponential backoff with full jitter.

Plain exponential backoff (delay = base * 2^attempt, no randomness) means
every task that failed at the same instant retries at the same instant
again — if the failure was caused by a downstream service being
overloaded, that's the thundering-herd problem hitting it a second time.

Full jitter (AWS's recommended pattern: pick a random delay uniformly
between 0 and the exponential cap, rather than using the cap itself)
spreads retries out instead of clustering them.
"""
from __future__ import annotations

import random


def compute_backoff_seconds(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """attempt is 1-indexed: the first retry is attempt=1."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    exponential_delay = min(cap, base * (2 ** (attempt - 1)))
    return random.uniform(0, exponential_delay)
