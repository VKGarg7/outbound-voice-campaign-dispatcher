import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.analytics import compute_analytics
from app.database import connect_db, fetch_attempt_rows, init_db, upsert_campaign, upsert_contact
from app.dispatcher import CampaignDispatcher, DispatchJob, RetryPolicy
from app.models import Contact, Disposition
from app.provider import MockTelephonyProvider, ProviderConfig


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "campaign.db"
    conn = connect_db(path)
    init_db(conn)
    conn.close()
    return path


@pytest.fixture
def campaign_id() -> str:
    return "cmp_test"


class DeterministicProvider:
    def __init__(self, sequence: list[Disposition] | None = None):
        self.sequence = sequence or []
        self.calls: list[tuple[str, datetime]] = []
        self.in_flight = 0
        self.max_seen = 0
        self._lock = asyncio.Lock()

    async def dispatch_call_async(self, phone_number: str) -> tuple[Disposition, int]:
        async with self._lock:
            self.in_flight += 1
            self.max_seen = max(self.max_seen, self.in_flight)
        try:
            if self.sequence:
                disposition = self.sequence.pop(0)
            else:
                disposition = Disposition.ANSWERED
            self.calls.append((phone_number, datetime.now(timezone.utc)))
            await asyncio.sleep(0.01)
            return disposition, 50
        finally:
            async with self._lock:
                self.in_flight -= 1


@pytest.mark.asyncio
async def test_concurrency_limit_1(db_path: Path, campaign_id: str) -> None:
    contacts = [
        Contact(campaign_id, f"c_{i:03d}", f"+155500{i:03d}", {"idx": i})
        for i in range(1, 21)
    ]
    for contact in contacts:
        upsert_campaign(connect_db(db_path), campaign_id, "test")
        upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED] * len(contacts))
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=1, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    await dispatcher.run_campaign(contacts)

    assert provider.max_seen <= 1


@pytest.mark.asyncio
async def test_concurrency_limit_5(db_path: Path, campaign_id: str) -> None:
    contacts = [
        Contact(campaign_id, f"c_{i:03d}", f"+155500{i:03d}", {"idx": i})
        for i in range(1, 200)
    ]
    for contact in contacts:
        upsert_campaign(connect_db(db_path), campaign_id, "test")
        upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED] * len(contacts))
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=5, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    await dispatcher.run_campaign(contacts)

    assert provider.max_seen <= 5


@pytest.mark.asyncio
async def test_same_contact_attempt_twice_is_idempotent(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED, Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=10, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    await dispatcher._process_job(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1), asyncio.Queue())
    await dispatcher._process_job(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1), asyncio.Queue())

    conn = connect_db(db_path)
    rows = fetch_attempt_rows(conn, campaign_id)
    conn.close()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_duplicate_requests_concurrently_are_single_call(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=10, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    queue = asyncio.Queue()
    for _ in range(10):
        queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))

    workers = [asyncio.create_task(dispatcher._worker(queue)) for _ in range(10)]
    await queue.join()
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_retry_no_answer_then_answered(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.NO_ANSWER, Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    queue = asyncio.Queue()
    queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))

    worker = asyncio.create_task(dispatcher._worker(queue))
    await queue.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert [r["attempt_no"] for r in rows] == [1, 2]
    assert [r["disposition"] for r in rows if r["status"] == "completed"] == ["no_answer", "answered"]


@pytest.mark.asyncio
async def test_retry_failed_then_answered(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.FAILED, Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    queue = asyncio.Queue()
    queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))

    worker = asyncio.create_task(dispatcher._worker(queue))
    await queue.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert [r["attempt_no"] for r in rows] == [1, 2]
    assert [r["disposition"] for r in rows if r["status"] == "completed"] == ["failed", "answered"]


@pytest.mark.asyncio
async def test_retry_exhaustion_until_max_attempts(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.NO_ANSWER, Disposition.NO_ANSWER, Disposition.NO_ANSWER])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    queue = asyncio.Queue()
    queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))

    worker = asyncio.create_task(dispatcher._worker(queue))
    await queue.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert [r["attempt_no"] for r in rows] == [1, 2, 3]
    assert [r["disposition"] for r in rows if r["status"] == "completed"] == ["no_answer", "no_answer", "no_answer"]


@pytest.mark.asyncio
async def test_immediate_answered_no_retry(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    queue = asyncio.Queue()
    queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))

    worker = asyncio.create_task(dispatcher._worker(queue))
    await queue.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert len(rows) == 1
    assert rows[0]["disposition"] == Disposition.ANSWERED.value
    assert rows[0]["attempt_no"] == 1


@pytest.mark.asyncio
async def test_voicemail_and_busy_are_not_retried(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    for disposition in (Disposition.VOICEMAIL, Disposition.BUSY):
        db_path2 = db_path.parent / f"{disposition.value}.db"
        conn = connect_db(db_path2)
        init_db(conn)
        conn.close()

        upsert_campaign(connect_db(db_path2), campaign_id, "test")
        upsert_contact(connect_db(db_path2), contact)

        provider = DeterministicProvider([disposition])
        dispatcher = CampaignDispatcher(str(db_path2), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

        queue = asyncio.Queue()
        queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))
        worker = asyncio.create_task(dispatcher._worker(queue))
        await queue.join()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

        rows = fetch_attempt_rows(connect_db(db_path2), campaign_id)
        assert len(rows) == 1
        assert rows[0]["disposition"] == disposition.value


@pytest.mark.asyncio
async def test_persistence_every_actual_attempt(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.NO_ANSWER, Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    queue = asyncio.Queue()
    queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))
    worker = asyncio.create_task(dispatcher._worker(queue))
    await queue.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert len(rows) == 2
    assert all(r["latency_ms"] is not None for r in rows)
    assert all(r["attempt_ts"] is not None for r in rows)
    assert all(r["disposition"] is not None for r in rows)


@pytest.mark.asyncio
async def test_analytics_metrics(db_path: Path, campaign_id: str) -> None:
    contact_a = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    contact_b = Contact(campaign_id, "c_002", "+155500002", {"idx": 2})
    contact_c = Contact(campaign_id, "c_003", "+155500003", {"idx": 3})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    for contact in (contact_a, contact_b, contact_c):
        upsert_contact(connect_db(db_path), contact)

    conn = connect_db(db_path)
    conn.execute(
        "INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms) VALUES (?, ?, ?, 'completed', ?, '2026-01-01T00:00:00Z', 500)",
        (campaign_id, "c_001", 1, Disposition.ANSWERED.value),
    )
    conn.execute(
        "INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms) VALUES (?, ?, ?, 'completed', ?, '2026-01-01T00:00:20Z', 500)",
        (campaign_id, "c_002", 1, Disposition.NO_ANSWER.value),
    )
    conn.execute(
        "INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms) VALUES (?, ?, ?, 'completed', ?, '2026-01-01T00:00:50Z', 500)",
        (campaign_id, "c_003", 1, Disposition.ANSWERED.value),
    )
    conn.commit()
    conn.close()

    analytics = compute_analytics(connect_db(db_path), campaign_id)

    assert analytics["total_unique_contacts"] == 3
    assert analytics["connection_rate"] == pytest.approx(2 / 3)
    assert analytics["disposition_counts"][Disposition.ANSWERED.value] == 2
    assert analytics["disposition_percentages"][Disposition.ANSWERED.value] == pytest.approx(66.6666666667, rel=1e-6)
    assert analytics["avg_attempts_before_terminal"] == pytest.approx(1.0)
    # Per-contact definition: first answered timestamp minus that contact's first dispatch.
    # In this synthetic dataset each answered contact's first dispatch is the answered attempt,
    # so the per-contact delta is 0. Expect average 0.0 accordingly.
    assert analytics["avg_time_to_first_connect_seconds"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_empty_cohort(db_path: Path, campaign_id: str) -> None:
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    analytics = compute_analytics(connect_db(db_path), campaign_id)
    assert analytics["total_unique_contacts"] == 0
    assert analytics["connection_rate"] == 0.0


@pytest.mark.asyncio
async def test_single_contact(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=5, provider=provider, retry_policy=RetryPolicy(max_attempts=1))

    await dispatcher.run_campaign([contact])
    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert len(rows) == 1
    assert rows[0]["disposition"] == Disposition.ANSWERED.value


@pytest.mark.asyncio
async def test_max_attempts_one(db_path: Path, campaign_id: str) -> None:
    contact = Contact(campaign_id, "c_001", "+155500001", {"idx": 1})
    upsert_campaign(connect_db(db_path), campaign_id, "test")
    upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.NO_ANSWER])
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=2, provider=provider, retry_policy=RetryPolicy(max_attempts=1))

    queue = asyncio.Queue()
    queue.put_nowait(DispatchJob(campaign_id, "c_001", "+155500001", {"idx": 1}, attempt_no=1))
    worker = asyncio.create_task(dispatcher._worker(queue))
    await queue.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    rows = fetch_attempt_rows(connect_db(db_path), campaign_id)
    assert len(rows) == 1
    assert rows[0]["attempt_no"] == 1


@pytest.mark.asyncio
async def test_concurrency_gt_cohort_size(db_path: Path, campaign_id: str) -> None:
    contacts = [
        Contact(campaign_id, f"c_{i:03d}", f"+155500{i:03d}", {"idx": i})
        for i in range(1, 11)
    ]
    for contact in contacts:
        upsert_campaign(connect_db(db_path), campaign_id, "test")
        upsert_contact(connect_db(db_path), contact)

    provider = DeterministicProvider([Disposition.ANSWERED] * len(contacts))
    dispatcher = CampaignDispatcher(str(db_path), max_in_flight=50, provider=provider, retry_policy=RetryPolicy(max_attempts=3))

    await dispatcher.run_campaign(contacts)

    assert provider.max_seen <= 50
