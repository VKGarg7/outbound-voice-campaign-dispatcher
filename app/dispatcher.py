from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.database import complete_attempt, connect_db, fetch_attempt, reserve_attempt
from app.models import Contact, Disposition
from app.provider import MockTelephonyProvider
from app.retry import next_backoff_delay, should_retry


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    initial_delay: float = 2.0
    multiplier: float = 2.0
    max_delay: float = 30.0
    jitter_ratio: float = 0.2


@dataclass
class DispatchJob:
    campaign_id: str
    contact_id: str
    phone_number: str
    payload: dict[str, Any]
    attempt_no: int = 1


class CampaignDispatcher:
    def __init__(
        self,
        db_path: str,
        max_in_flight: int,
        provider: MockTelephonyProvider,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.db_path = db_path
        self.max_in_flight = max_in_flight
        self.provider = provider
        self.retry_policy = retry_policy or RetryPolicy()

    async def run_campaign(self, contacts: list[Contact]) -> None:
        queue: asyncio.Queue[DispatchJob] = asyncio.Queue()
        for contact in contacts:
            queue.put_nowait(
                DispatchJob(
                    campaign_id=contact.campaign_id,
                    contact_id=contact.contact_id,
                    phone_number=contact.phone_number,
                    payload=contact.payload,
                    attempt_no=1,
                )
            )

        workers = [asyncio.create_task(self._worker(queue)) for _ in range(self.max_in_flight)]
        await queue.join()
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def _worker(self, queue: asyncio.Queue[DispatchJob]) -> None:
        while True:
            try:
                job = await asyncio.wait_for(queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                return
            try:
                await self._process_job(job, queue)
            finally:
                queue.task_done()

    async def _process_job(self, job: DispatchJob, queue: asyncio.Queue[DispatchJob]) -> None:
        contact = Contact(job.campaign_id, job.contact_id, job.phone_number, job.payload)
        reserved = await asyncio.to_thread(self._reserve_attempt, job.campaign_id, job.contact_id, job.attempt_no)
        if not reserved:
            existing = await asyncio.to_thread(self._read_existing_attempt, job.campaign_id, job.contact_id, job.attempt_no)
            if existing is not None and existing.disposition is not None:
                return
            return

        try:
            disposition, latency_ms = await self.provider.dispatch_call_async(contact.phone_number)
            timestamp = datetime.now(timezone.utc).isoformat()
            await asyncio.to_thread(
                self._persist_result,
                job.campaign_id,
                job.contact_id,
                job.attempt_no,
                disposition,
                latency_ms,
                timestamp,
            )

            if disposition is Disposition.ANSWERED:
                return
            if disposition in {Disposition.VOICEMAIL, Disposition.BUSY}:
                return
            if should_retry(disposition) and job.attempt_no < self.retry_policy.max_attempts:
                delay = next_backoff_delay(
                    job.attempt_no,
                    initial_delay=self.retry_policy.initial_delay,
                    multiplier=self.retry_policy.multiplier,
                    max_delay=self.retry_policy.max_delay,
                    jitter_ratio=self.retry_policy.jitter_ratio,
                )
                await asyncio.sleep(delay)
                queue.put_nowait(
                    DispatchJob(
                        campaign_id=job.campaign_id,
                        contact_id=job.contact_id,
                        phone_number=job.phone_number,
                        payload=job.payload,
                        attempt_no=job.attempt_no + 1,
                    )
                )
        finally:
            pass

    def _reserve_attempt(self, campaign_id: str, contact_id: str, attempt_no: int) -> bool:
        conn = connect_db(self.db_path)
        try:
            return reserve_attempt(conn, campaign_id, contact_id, attempt_no)
        finally:
            conn.close()

    def _read_existing_attempt(self, campaign_id: str, contact_id: str, attempt_no: int):
        conn = connect_db(self.db_path)
        try:
            return fetch_attempt(conn, campaign_id, contact_id, attempt_no)
        finally:
            conn.close()

    def _persist_result(
        self,
        campaign_id: str,
        contact_id: str,
        attempt_no: int,
        disposition: Disposition,
        latency_ms: int,
        attempt_ts: str,
    ) -> None:
        conn = connect_db(self.db_path)
        try:
            complete_attempt(conn, campaign_id, contact_id, attempt_no, disposition, latency_ms, attempt_ts)
        finally:
            conn.close()
