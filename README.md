# Sarvam outbound campaign dispatcher

## 1. Overview

This project implements a small outbound voice campaign dispatcher in Python using asyncio and SQLite. It is intentionally lightweight and built for a take-home assignment, not a production dialer.

The dispatcher reads a synthetic campaign dataset, enforces a configurable concurrency cap, retries only retryable call outcomes, persists every real provider call attempt, and prints analytics at the end of the run.

## 2. Requirements

Implemented requirements from the assignment:

- Python 3.11+
- asyncio-based scheduling
- SQLite persistence
- configurable concurrency limit
- synthetic contacts for a campaign of 199–500+ contacts
- deterministic seed option for repeatable runs
- mock telephony provider with a documented disposition distribution
- retry behavior for `no_answer` and `failed`
- backoff with jitter
- idempotent attempt handling for the same campaign/contact/attempt key
- analytics for connection rate, disposition counts and percentages, average attempts before terminal state, and time-to-first-connect
- simple CLI entrypoint without UI or external infrastructure

## 3. Setup

From the project root:

```bash
python -m pip install pytest pytest-asyncio
```

No additional runtime dependencies are required beyond the Python standard library.

## 4. How to run

Run the driver script:

```bash
python scripts/run_campaign.py --contacts 300 --concurrency 10 --max-attempts 3
```

The script:

1. creates a synthetic contact cohort
2. initializes the SQLite database if needed
3. schedules the campaign
4. enforces the concurrency cap
5. persists each actual provider call attempt
6. calculates and prints the final analytics

## 5. Example command

```bash
python scripts/run_campaign.py --contacts 300 --concurrency 10 --max-attempts 3 --seed 42
```

Optional export:

```bash
python scripts/run_campaign.py --contacts 300 --concurrency 10 --max-attempts 3 --output out/results.json
```

The export path accepts JSON or CSV.

## 6. Architecture

The project is intentionally small and split into clear modules:

- `app/models.py`: dataclasses and dispositions
- `app/database.py`: SQLite schema, inserts, updates, and lookup helpers
- `app/provider.py`: mock telephony provider and documented probabilities
- `app/retry.py`: retry classification and backoff calculation
- `app/dispatcher.py`: async campaign orchestration, retry loop, and concurrency guard
- `app/analytics.py`: analytics aggregation over persisted attempts
- `scripts/run_campaign.py`: CLI driver for a campaign run

## 7. Concurrency design

The dispatcher uses a fixed-size async worker pool with an internal queue. Jobs are queued first, then a bounded number of workers pulls work and executes provider calls.

This keeps the number of provider calls in flight at or below the configured concurrency limit. The queue can hold more jobs than the concurrency limit, but only the active workers are allowed to call the provider at one time.

## 8. Idempotency design

Idempotency is enforced by reserving the logical attempt key before the provider call is made. The key is:

- campaign_id
- contact_id
- attempt_no

The SQLite schema enforces a unique constraint on `(campaign_id, contact_id, attempt_no)`. If the same logical attempt is submitted concurrently, only one insert can succeed; the losing task sees the uniqueness violation and does not call the provider again.

This is sufficient for the assignment’s local SQLite design. It prevents duplicate orchestration requests from creating duplicate real provider calls for the same logical attempt.

## 9. Retry policy

The retry policy is intentionally conservative and simple:

- retry: `no_answer`, `failed`
- do not retry: `answered`, `voicemail`, `busy`

Default values:

- max attempts: 4
- initial delay: 2.0 seconds
- multiplier: 2.0
- max delay: 30.0 seconds
- jitter: ±20%

This uses exponential backoff with jitter and stops when the contact reaches the maximum configured attempt count.

## 10. Persistence

SQLite stores every real provider call attempt, including:

- campaign id
- contact id
- attempt number
- disposition
- attempt timestamp
- latency in milliseconds
- lifecycle status (`reserved` / `completed`)

The database is also used as the source of truth for deduplication and analytics.

## 11. Analytics

The analytics module calculates:

- connection rate = answered / total unique contacts
- disposition count by outcome
- disposition percentage by outcome
- average attempts before terminal state
- average time-to-first-connect for contacts ultimately answered

Analytics are computed from persisted attempt rows, not from in-memory state, so the numbers reflect the actual recorded run.

## 12. Testing

A focused automated test suite is included under `tests/test_dispatcher.py`.

It covers:

- concurrency limits
- duplicate concurrent requests
- retry flow
- persistence of actual attempts
- analytics correctness
- edge cases like empty cohorts and max-attempt cutoff

The provider is intentionally deterministic in tests instead of relying on randomness.

## 13. Example output

```text
Campaign: cmp_demo
Contacts: 300
Concurrency: 10
Max attempts: 3
Campaign duration: 15.88s
Total attempts: 459
Max observed provider concurrency: 10

Analytics:
{
  "avg_attempts_before_terminal": 1.53,
  "avg_time_to_first_connect_seconds": 11.99,
  "connection_rate": 0.6733333333333333,
  "disposition_counts": {
    "answered": 202,
    "busy": 39,
    "failed": 26,
    "no_answer": 140,
    "voicemail": 52
  },
  "disposition_percentages": {
    "answered": 44.01,
    "busy": 8.50,
    "failed": 5.66,
    "no_answer": 30.50,
    "voicemail": 11.33
  },
  "total_unique_contacts": 300
}
```

## 14. Design trade-offs

This project favors correctness and simplicity over abstraction.

- SQLite is used instead of a more complex queueing system because the assignment explicitly allows SQLite and is scoped to a short take-home.
- Retry logic is intentionally simplified to a small subset of dispositions rather than modeling a complex telephony stack.
- The code keeps all important state in SQLite so the analytics and dedupe logic are durable and inspectable.
- The implementation avoids multiple worker processes, distributed coordination, and external infra because they are outside the assignment scope.

## 15. Known limitations / production gaps

This is not production-grade telephony infrastructure.

Known gaps include:

- no durable recovery of stalled `reserved` rows after process crash
- no provider-side idempotency key contract beyond the local SQLite reservation
- no real telephony API, rate limiting, or telephony-specific failure semantics
- SQLite is sufficient for the assignment but not for multi-instance high-scale outbound workloads
- no alerting, dashboards, or operational automation beyond local reporting

## 16. AI assistance disclosure

This project was implemented with AI assistance during development, but the design decisions were reviewed and constrained to the assignment requirements. The final implementation keeps the scope narrow and does not add infrastructure or abstractions beyond what is needed for the take-home task.

The code was adjusted to align with the assignment’s actual constraints: local SQLite, asyncio scheduling, small worker pool, deterministic test behavior, and minimal CLI surface.
