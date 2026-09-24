from __future__ import annotations

import random
from typing import Callable

from app.models import Disposition


def should_retry(disposition: Disposition) -> bool:
    return disposition in {Disposition.NO_ANSWER, Disposition.FAILED}


def next_backoff_delay(
    attempt_no: int,
    *,
    initial_delay: float = 2.0,
    multiplier: float = 2.0,
    max_delay: float = 30.0,
    jitter_ratio: float = 0.2,
    rng: Callable[[], float] | None = None,
) -> float:
    if attempt_no < 1:
        raise ValueError("attempt_no must be >= 1")

    if rng is None:
        rng = random.random

    delay = min(initial_delay * (multiplier ** (attempt_no - 1)), max_delay)
    jitter = delay * jitter_ratio
    return max(0.0, delay + (rng() * 2.0 - 1.0) * jitter)
