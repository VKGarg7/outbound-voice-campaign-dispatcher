from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from app.models import AttemptRecord, Contact, Disposition


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS campaigns (
            campaign_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS contacts (
            campaign_id TEXT NOT NULL,
            contact_id TEXT NOT NULL,
            phone_number TEXT NOT NULL,
            payload_json TEXT,
            PRIMARY KEY (campaign_id, contact_id),
            FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS call_attempts (
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id TEXT NOT NULL,
            contact_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
            status TEXT NOT NULL CHECK (status IN ('reserved', 'completed')),
            disposition TEXT,
            attempt_ts TEXT,
            latency_ms INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (campaign_id, contact_id, attempt_no),
            FOREIGN KEY (campaign_id, contact_id)
                REFERENCES contacts(campaign_id, contact_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_contacts_campaign_phone ON contacts (campaign_id, phone_number)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_attempts_campaign_contact ON call_attempts (campaign_id, contact_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_attempts_contact_attempt ON call_attempts (contact_id, attempt_no)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_attempts_disposition ON call_attempts (disposition)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_attempts_ts ON call_attempts (attempt_ts)"
    )
    conn.commit()


def upsert_campaign(conn: sqlite3.Connection, campaign_id: str, name: str) -> None:
    conn.execute(
        """
        INSERT INTO campaigns (campaign_id, name)
        VALUES (?, ?)
        ON CONFLICT(campaign_id) DO UPDATE SET name = excluded.name
        """,
        (campaign_id, name),
    )
    conn.commit()


def upsert_contact(conn: sqlite3.Connection, contact: Contact) -> None:
    conn.execute(
        """
        INSERT INTO contacts (campaign_id, contact_id, phone_number, payload_json)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(campaign_id, contact_id) DO UPDATE SET
            phone_number = excluded.phone_number,
            payload_json = excluded.payload_json
        """,
        (contact.campaign_id, contact.contact_id, contact.phone_number, repr(contact.payload)),
    )
    conn.commit()


def reserve_attempt(conn: sqlite3.Connection, campaign_id: str, contact_id: str, attempt_no: int) -> bool:
    try:
        conn.execute(
            """
            INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms)
            VALUES (?, ?, ?, 'reserved', NULL, NULL, NULL)
            """,
            (campaign_id, contact_id, attempt_no),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        return False


def complete_attempt(
    conn: sqlite3.Connection,
    campaign_id: str,
    contact_id: str,
    attempt_no: int,
    disposition: Disposition,
    latency_ms: int,
    attempt_ts: str,
) -> None:
    conn.execute(
        """
        UPDATE call_attempts
        SET status = 'completed',
            disposition = ?,
            attempt_ts = ?,
            latency_ms = ?
        WHERE campaign_id = ? AND contact_id = ? AND attempt_no = ?
        """,
        (disposition.value, attempt_ts, latency_ms, campaign_id, contact_id, attempt_no),
    )
    conn.commit()


def fetch_attempt(
    conn: sqlite3.Connection,
    campaign_id: str,
    contact_id: str,
    attempt_no: int,
) -> AttemptRecord | None:
    row = conn.execute(
        """
        SELECT attempt_id, campaign_id, contact_id, attempt_no, disposition, attempt_ts, latency_ms, status
        FROM call_attempts
        WHERE campaign_id = ? AND contact_id = ? AND attempt_no = ?
        """,
        (campaign_id, contact_id, attempt_no),
    ).fetchone()
    if row is None:
        return None
    disposition = Disposition(row["disposition"]) if row["disposition"] else None
    return AttemptRecord(
        attempt_id=row["attempt_id"],
        campaign_id=row["campaign_id"],
        contact_id=row["contact_id"],
        attempt_no=row["attempt_no"],
        disposition=disposition,
        attempt_ts=row["attempt_ts"],
        latency_ms=row["latency_ms"],
        status=row["status"],
    )


def fetch_contact_rows(conn: sqlite3.Connection, campaign_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT campaign_id, contact_id, phone_number, payload_json FROM contacts WHERE campaign_id = ? ORDER BY contact_id",
        (campaign_id,),
    ).fetchall()


def fetch_attempt_rows(conn: sqlite3.Connection, campaign_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms
        FROM call_attempts
        WHERE campaign_id = ?
        ORDER BY contact_id, attempt_no
        """,
        (campaign_id,),
    ).fetchall()
