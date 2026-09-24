from __future__ import annotations

import sqlite3
from collections import Counter
from statistics import mean

from app.models import Disposition


def compute_analytics(conn: sqlite3.Connection, campaign_id: str) -> dict[str, object]:
    unique_contacts = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE campaign_id = ?",
        (campaign_id,),
    ).fetchone()[0]

    if unique_contacts == 0:
        return {
            "connection_rate": 0.0,
            "disposition_counts": {},
            "disposition_percentages": {},
            "avg_attempts_before_terminal": 0.0,
            "avg_time_to_first_connect_seconds": 0.0,
            "total_unique_contacts": 0,
        }

    attempt_rows = conn.execute(
        """
        SELECT contact_id, attempt_no, disposition, attempt_ts
        FROM call_attempts
        WHERE campaign_id = ?
        ORDER BY contact_id, attempt_no
        """,
        (campaign_id,),
    ).fetchall()

    by_contact: dict[str, list[sqlite3.Row]] = {}
    for row in attempt_rows:
        by_contact.setdefault(row["contact_id"], []).append(row)

    answered_count = 0
    disposition_counter: Counter[str] = Counter()
    attempts_before_terminal: list[int] = []
    time_to_first_connect: list[float] = []

    for contact_id, rows in by_contact.items():
        if rows and any(r["disposition"] == Disposition.ANSWERED.value for r in rows):
            answered_count += 1
        for row in rows:
            disposition_counter[row["disposition"]] += 1

        terminal = rows[-1]
        attempts_before_terminal.append(terminal["attempt_no"])

        answered_row = next((r for r in rows if r["disposition"] == Disposition.ANSWERED.value), None)
        if answered_row is not None and answered_row["attempt_ts"]:
            first_row = rows[0]
            if first_row["attempt_ts"] and answered_row["attempt_ts"]:
                from datetime import datetime

                first_dt = datetime.fromisoformat(first_row["attempt_ts"].replace("Z", "+00:00"))
                answered_dt = datetime.fromisoformat(answered_row["attempt_ts"].replace("Z", "+00:00"))
                time_to_first_connect.append((answered_dt - first_dt).total_seconds())

    total_dispositions = sum(disposition_counter.values())
    disposition_percentages = {
        key: (count / total_dispositions * 100.0) if total_dispositions else 0.0
        for key, count in sorted(disposition_counter.items())
    }

    connection_rate = answered_count / unique_contacts if unique_contacts else 0.0
    avg_attempts = mean(attempts_before_terminal) if attempts_before_terminal else 0.0
    avg_time_to_connect = mean(time_to_first_connect) if time_to_first_connect else 0.0

    return {
        "connection_rate": connection_rate,
        "disposition_counts": dict(sorted(disposition_counter.items())),
        "disposition_percentages": disposition_percentages,
        "avg_attempts_before_terminal": avg_attempts,
        "avg_time_to_first_connect_seconds": avg_time_to_connect,
        "total_unique_contacts": unique_contacts,
    }
