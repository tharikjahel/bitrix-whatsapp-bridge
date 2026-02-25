"""
SQLite storage for:
  - OAuth tokens (Bitrix24 access/refresh)
  - Chat session mapping: bitrix_chat_id <-> (WhatsApp phone + instance)
  - Instance lines: instance_name <-> Bitrix24 line_id (set via /setup)
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
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                access_token    TEXT,
                refresh_token   TEXT,
                expires_at      REAL,
                domain          TEXT,
                member_id       TEXT,
                server_endpoint TEXT,
                portal_domain   TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS instance_lines (
                instance_name      TEXT PRIMARY KEY,
                evolution_instance TEXT NOT NULL,
                bitrix_line_id     TEXT NOT NULL,
                label              TEXT DEFAULT ''
            )
        """)
        # migrate existing databases that lack the new columns
        for col_def in ("member_id TEXT", "server_endpoint TEXT", "portal_domain TEXT"):
            try:
                conn.execute(f"ALTER TABLE oauth_tokens ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass  # column already exists
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
    member_id: str = "",
    server_endpoint: str = "",
    portal_domain: str = "",
):
    expires_at = time.time() + int(expires_in)
    with _lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO oauth_tokens
                (id, access_token, refresh_token, expires_at, domain,
                 member_id, server_endpoint, portal_domain)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                access_token    = excluded.access_token,
                refresh_token   = excluded.refresh_token,
                expires_at      = excluded.expires_at,
                domain          = excluded.domain,
                member_id       = excluded.member_id,
                server_endpoint = excluded.server_endpoint,
                portal_domain   = excluded.portal_domain
            """,
            (access_token, refresh_token, expires_at, domain, member_id, server_endpoint, portal_domain),
        )
        conn.commit()


def get_oauth() -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT access_token, refresh_token, expires_at, domain, "
            "member_id, server_endpoint, portal_domain "
            "FROM oauth_tokens WHERE id = 1"
        ).fetchone()
    if not row:
        return None
    return {
        "access_token":    row[0],
        "refresh_token":   row[1],
        "expires_at":      row[2],
        "domain":          row[3],
        "member_id":       row[4],
        "server_endpoint": row[5],
        "portal_domain":   row[6] or "",
    }


# ─── Instance lines ─────────────────────────────────────────────────────────

def save_instance_line(
    instance_name: str,
    evolution_instance: str,
    bitrix_line_id: str,
    label: str = "",
):
    """Persist an instance_name → (evolution_instance, bitrix_line_id) mapping."""
    with _lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO instance_lines (instance_name, evolution_instance, bitrix_line_id, label)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(instance_name) DO UPDATE SET
                evolution_instance = excluded.evolution_instance,
                bitrix_line_id     = excluded.bitrix_line_id,
                label              = excluded.label
            """,
            (instance_name, evolution_instance, bitrix_line_id, label),
        )
        conn.commit()


def get_instance_lines() -> list[dict]:
    """Return all configured instance→line mappings."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT instance_name, evolution_instance, bitrix_line_id, label "
            "FROM instance_lines"
        ).fetchall()
    return [
        {
            "instance_name":      r[0],
            "evolution_instance": r[1],
            "bitrix_line_id":     r[2],
            "label":              r[3] or "",
        }
        for r in rows
    ]


def get_line_by_instance(instance_name: str) -> dict | None:
    """Lookup by instance_name. Returns None if not configured."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT instance_name, evolution_instance, bitrix_line_id, label "
            "FROM instance_lines WHERE instance_name = ?",
            (instance_name,),
        ).fetchone()
    if not row:
        return None
    return {
        "instance_name":      row[0],
        "evolution_instance": row[1],
        "bitrix_line_id":     row[2],
        "label":              row[3] or "",
    }


def get_instance_by_line_db(line_id: str) -> dict | None:
    """Lookup by Bitrix24 line_id. Returns None if not found."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT instance_name, evolution_instance, bitrix_line_id, label "
            "FROM instance_lines WHERE bitrix_line_id = ?",
            (line_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "instance_name":      row[0],
        "evolution_instance": row[1],
        "bitrix_line_id":     row[2],
        "label":              row[3] or "",
    }
