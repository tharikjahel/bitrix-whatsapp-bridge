"""
SQLite storage for:
  - OAuth tokens (Bitrix24 access/refresh)
  - Chat session mapping: bitrix_chat_id <-> (WhatsApp phone + instance)
"""
import sqlite3
import threading
import time
from pathlib import Path

DB_PATH = Path("data.db")
_lock = threading.Lock()


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_sessions (
                bitrix_chat_id TEXT PRIMARY KEY,
                wa_phone       TEXT NOT NULL,
                wa_name        TEXT,
                instance_name  TEXT NOT NULL,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS oauth_tokens (
                id            INTEGER PRIMARY KEY CHECK (id = 1),
                access_token  TEXT,
                refresh_token TEXT,
                expires_at    REAL,
                domain        TEXT
            )
        """)
        conn.commit()


# ─── Sessions ───────────────────────────────────────────────────────────────

def save_session(
    bitrix_chat_id: str,
    wa_phone: str,
    instance_name: str,
    wa_name: str | None = None,
):
    with _lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO chat_sessions (bitrix_chat_id, wa_phone, instance_name, wa_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(bitrix_chat_id) DO UPDATE SET
                wa_phone      = excluded.wa_phone,
                instance_name = excluded.instance_name,
                wa_name       = excluded.wa_name
            """,
            (bitrix_chat_id, wa_phone, instance_name, wa_name),
        )
        conn.commit()


def get_session_by_chat(bitrix_chat_id: str) -> dict | None:
    """Return {wa_phone, instance_name, wa_name} or None."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT wa_phone, instance_name, wa_name "
            "FROM chat_sessions WHERE bitrix_chat_id = ?",
            (bitrix_chat_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "wa_phone":      row[0],
        "instance_name": row[1],
        "wa_name":       row[2],
    }


# ─── OAuth tokens ───────────────────────────────────────────────────────────

def save_oauth(
    access_token: str,
    refresh_token: str,
    expires_in: int,
    domain: str,
):
    expires_at = time.time() + int(expires_in)
    with _lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO oauth_tokens (id, access_token, refresh_token, expires_at, domain)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                access_token  = excluded.access_token,
                refresh_token = excluded.refresh_token,
                expires_at    = excluded.expires_at,
                domain        = excluded.domain
            """,
            (access_token, refresh_token, expires_at, domain),
        )
        conn.commit()


def get_oauth() -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT access_token, refresh_token, expires_at, domain "
            "FROM oauth_tokens WHERE id = 1"
        ).fetchone()
    if not row:
        return None
    return {
        "access_token":  row[0],
        "refresh_token": row[1],
        "expires_at":    row[2],
        "domain":        row[3],
    }
