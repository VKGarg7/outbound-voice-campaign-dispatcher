from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.analytics import compute_analytics
from app.database import connect_db, init_db, upsert_campaign, upsert_contact
from app.dispatcher import CampaignDispatcher, RetryPolicy
from app.models import Contact
from app.provider import MockTelephonyProvider


def generate_contacts(campaign_id: str, count: int) -> list[Contact]:
    contacts: list[Contact] = []
    for index in range(count):
        contacts.append(
            Contact(
                campaign_id=campaign_id,
                contact_id=f"c_{index + 1:04d}",
                phone_number=f"+1555{1000000 + index}",
                payload={"segment": "synthetic", "idx": index},
            )
        )
    return contacts


def export_results(path: str | None, analytics: dict[str, object], total_attempts: int, max_observed_concurrency: int, duration_seconds: float) -> None:
    if path is None:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix.lower() == ".csv":
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["metric", "value"])
            writer.writerow(["connection_rate", analytics.get("connection_rate", 0)])
            writer.writerow(["total_unique_contacts", analytics.get("total_unique_contacts", 0)])
            writer.writerow(["avg_attempts_before_terminal", analytics.get("avg_attempts_before_terminal", 0)])
            writer.writerow(["avg_time_to_first_connect_seconds", analytics.get("avg_time_to_first_connect_seconds", 0)])
            writer.writerow(["max_observed_concurrency", max_observed_concurrency])
            writer.writerow(["total_attempts", total_attempts])
            writer.writerow(["campaign_duration_seconds", round(duration_seconds, 3)])
    else:
        payload = {
            "analytics": analytics,
            "max_observed_concurrency": max_observed_concurrency,
            "total_attempts": total_attempts,
            "campaign_duration_seconds": round(duration_seconds, 3),
        }
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


async def run_campaign(campaign_id: str, db_path: Path, contacts_count: int, concurrency: int, max_attempts: int, seed: int | None) -> tuple[dict[str, object], int, int, float, MockTelephonyProvider]:
    if seed is not None:
        random.seed(seed)

    conn = connect_db(db_path)
    init_db(conn)
    conn.close()

    contacts = generate_contacts(campaign_id, contacts_count)
    provider = MockTelephonyProvider(seed=seed)
    dispatcher = CampaignDispatcher(
        db_path=str(db_path),
        max_in_flight=concurrency,
        provider=provider,
        retry_policy=RetryPolicy(max_attempts=max_attempts),
    )

    upsert_campaign(connect_db(db_path), campaign_id, f"Campaign {campaign_id}")
    for contact in contacts:
        upsert_contact(connect_db(db_path), contact)

    start = time.perf_counter()
    await dispatcher.run_campaign(contacts)
    duration = time.perf_counter() - start

    analytics = compute_analytics(connect_db(db_path), campaign_id)
    total_attempts = connect_db(db_path).execute("SELECT COUNT(*) FROM call_attempts WHERE campaign_id = ?", (campaign_id,)).fetchone()[0]
    max_observed_concurrency = provider.max_observed_in_flight
    return analytics, total_attempts, max_observed_concurrency, duration, provider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a synthetic outbound voice campaign dispatch.")
    parser.add_argument("--contacts", type=int, default=300, help="Number of synthetic contacts to process (default: 300).")
    parser.add_argument("--concurrency", type=int, default=10, help="Maximum provider calls in flight (default: 10).")
    parser.add_argument("--max-attempts", type=int, default=4, help="Maximum retry attempts per contact (default: 4).")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for deterministic results.")
    parser.add_argument("--output", type=str, default=None, help="Optional export path for JSON or CSV output.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    if args.contacts < 1:
        raise ValueError("--contacts must be >= 1")
    if args.concurrency < 1:
        raise ValueError("--concurrency must be >= 1")
    if args.max_attempts < 1:
        raise ValueError("--max-attempts must be >= 1")

    campaign_id = "cmp_demo"
    db_path = Path("campaign.db")
    analytics, total_attempts, max_observed_concurrency, duration, _ = await run_campaign(
        campaign_id=campaign_id,
        db_path=db_path,
        contacts_count=args.contacts,
        concurrency=args.concurrency,
        max_attempts=args.max_attempts,
        seed=args.seed,
    )

    print(f"Campaign: {campaign_id}")
    print(f"Contacts: {args.contacts}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Max attempts: {args.max_attempts}")
    print(f"Campaign duration: {duration:.2f}s")
    print(f"Total attempts: {total_attempts}")
    print(f"Max observed provider concurrency: {max_observed_concurrency}")
    print("\nAnalytics:")
    print(json.dumps(analytics, indent=2, sort_keys=True))

    export_results(args.output, analytics, total_attempts, max_observed_concurrency, duration)

    if args.output:
        print(f"\nExported results to: {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
