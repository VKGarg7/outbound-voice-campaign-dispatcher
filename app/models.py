from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Disposition(str, Enum):
    ANSWERED = "answered"
    NO_ANSWER = "no_answer"
    VOICEMAIL = "voicemail"
    BUSY = "busy"
    FAILED = "failed"


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    name: str
    contact_count: int = 0


@dataclass(frozen=True)
class Contact:
    campaign_id: str
    contact_id: str
    phone_number: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AttemptRecord:
    attempt_id: int | None = None
    campaign_id: str | None = None
    contact_id: str | None = None
    attempt_no: int | None = None
    disposition: Disposition | None = None
    attempt_ts: str | None = None
    latency_ms: int | None = None
    status: str | None = None
