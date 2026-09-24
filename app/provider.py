from __future__ import annotations

import asyncio
import random
import threading
import time
from dataclasses import dataclass

from app.models import Disposition


DISPOSITION_PROBABILITIES: dict[Disposition, float] = {
    Disposition.ANSWERED: 0.45,
    Disposition.NO_ANSWER: 0.30,
    Disposition.VOICEMAIL: 0.12,
    Disposition.BUSY: 0.08,
    Disposition.FAILED: 0.05,
}


@dataclass(frozen=True)
class ProviderConfig:
    min_delay_seconds: float = 0.1
    max_delay_seconds: float = 2.0
    probabilities: dict[Disposition, float] = None 

    def __post_init__(self) -> None:
        if self.probabilities is None:
            object.__setattr__(self, "probabilities", DISPOSITION_PROBABILITIES)


class MockTelephonyProvider:
    """Mock provider with documented synthetic probabilities.

    Distribution used in this assignment:
    - answered: 45%
    - no_answer: 30%
    - voicemail: 12%
    - busy: 8%
    - failed: 5%
    """

    def __init__(self, config: ProviderConfig | None = None, seed: int | None = None) -> None:
        self.config = config or ProviderConfig()
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        self._lock = threading.Lock()
        self._active_calls = 0
        self._max_observed = 0

    @property
    def max_observed_in_flight(self) -> int:
        return self._max_observed

    def dispatch_call(self, phone_number: str) -> tuple[Disposition, int]:
        with self._lock:
            self._active_calls += 1
            self._max_observed = max(self._max_observed, self._active_calls)

        try:
            delay_seconds = random.uniform(
                self.config.min_delay_seconds,
                self.config.max_delay_seconds,
            )
            time.sleep(delay_seconds)

            disposition = random.choices(
                list(self.config.probabilities.keys()),
                weights=list(self.config.probabilities.values()),
                k=1,
            )[0]
            return disposition, int(delay_seconds * 1000)
        finally:
            with self._lock:
                self._active_calls -= 1

    async def dispatch_call_async(self, phone_number: str) -> tuple[Disposition, int]:
        return await asyncio.to_thread(self.dispatch_call, phone_number)
