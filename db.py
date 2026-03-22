"""
db.py — Database Layer
=======================
All SQLite helpers extracted into a dedicated module.
bot.py and backup.py import from here.

No logic changes vs bot_v2 — this is a clean extraction for modularity.
"""

import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, date
from typing import Optional

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "salon.db")


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

@contextmanager
def get_db():
    """Context manager — yields a WAL-mode SQLite connection with Row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# INITIALISATION
# ══════════════════════════════════════════════════════════════════════════════

def init_db():
    """Create all tables and insert defaults if they don't already exist."""
    with get_db() as db:

        db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        db.execute("""
            CREATE TABLE IF NOT EXISTS staff (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                title      TEXT NOT NULL DEFAULT 'Stylist',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        db.execute("""
            CREATE TABLE IF NOT EXISTS services (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                duration    INTEGER NOT NULL DEFAULT 60,
                price       INTEGER NOT NULL DEFAULT 0,
                active      INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        db.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id               TEXT PRIMARY KEY,
                user_id          INTEGER NOT NULL,
                client_name      TEXT NOT NULL,
                username         TEXT,
                service          TEXT NOT NULL,
                stylist          TEXT NOT NULL,
                date             TEXT NOT NULL,
                time             TEXT NOT NULL,
                duration         TEXT NOT NULL DEFAULT '60 min',
                price            TEXT NOT NULL DEFAULT 'TBD',
                notes            TEXT,
                status           TEXT NOT NULL DEFAULT 'confirmed',
                reminder_24h_sent INTEGER NOT NULL DEFAULT 0,
                reminder_1h_sent  INTEGER NOT NULL DEFAULT 0,
                created_at       TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        db.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                user_id     INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                username    TEXT,
                visit_count INTEGER NOT NULL DEFAULT 0,
                notes       TEXT,
                first_seen  TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen   TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        # ── Default settings ──────────────────────────────────────────────
        defaults = {
            "salon_name":    "Maison Lumière",
            "salon_address": "12 Rue de la Paix, Suite 3",
            "salon_phone":   "+1 (555) 012-3456",
            "open_time":     "09:00",
            "close_time":    "18:00",
            "slot_duration": "60",
            "working_days":  "1,2,3,4,5",   # 0=Mon … 6=Sun
            "currency":      "₹",
        }
        for key, val in defaults.items():
            db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, val),
            )

        # ── Default services ──────────────────────────────────────────────
        for name, dur, price in [
            ("Haircut & Style", 45,  850),
            ("Hair Coloring",  120, 1450),
            ("Mani & Pedi",     60,  750),
            ("Facial Ritual",   75, 1200),
        ]:
            db.execute(
                "INSERT OR IGNORE INTO services (name, duration, price) VALUES (?, ?, ?)",
                (name, dur, price),
            )

        # ── Default staff ─────────────────────────────────────────────────
        for name, title in [
            ("Isabelle Moreau", "Creative Director"),
            ("Sophie Laurent",  "Color Specialist"),
            ("Chloé Petit",     "Skin & Nail Expert"),
        ]:
            db.execute(
                "INSERT OR IGNORE INTO staff (name, title) VALUES (?, ?)",
                (name, title),
            )

    logger.info(f"[DB] Initialized: {DB_PATH}")


# ══════════════════════════════════════════════════════════════════════════════
# SETTINGS
# ══════════════════════════════════════════════════════════════════════════════

def get_setting(key: str, default: str = "") -> str:
    with get_db() as db:
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key: str, value: str):
    with get_db() as db:
        db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )


def get_all_settings() -> dict:
    with get_db() as db:
        rows = db.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}


# ══════════════════════════════════════════════════════════════════════════════
# AVAILABILITY
# ══════════════════════════════════════════════════════════════════════════════

def get_booked_slots(date_str: str) -> list:
    with get_db() as db:
        rows = db.execute(
            "SELECT time FROM bookings WHERE date=? AND status NOT IN ('cancelled')",
            (date_str,),
        ).fetchall()
        return [r["time"] for r in rows]


def get_available_slots(date_str: str) -> list:
    s = get_all_settings()
    open_h,  open_m  = map(int, s["open_time"].split(":"))
    close_h, close_m = map(int, s["close_time"].split(":"))
    slot_mins    = int(s.get("slot_duration", "60"))
    working_days = [int(d) for d in s["working_days"].split(",")]

    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    if date_obj.weekday() not in working_days:
        return []

    slots, current = [], datetime(
        date_obj.year, date_obj.month, date_obj.day, open_h, open_m
    )
    end = datetime(date_obj.year, date_obj.month, date_obj.day, close_h, close_m)
    while current < end:
        slots.append(current.strftime("%H:%M"))
        current += timedelta(minutes=slot_mins)

    booked = get_booked_slots(date_str)
    return [slot for slot in slots if slot not in booked]


def is_slot_available(date_str: str, time_str: str) -> bool:
    return time_str in get_available_slots(date_str)


# ══════════════════════════════════════════════════════════════════════════════
# BOOKINGS
# ══════════════════════════════════════════════════════════════════════════════

def save_booking(data: dict, user) -> str:
    """
    Persist a booking to SQLite and upsert the client record.
    Returns the new booking reference ID (e.g. ML-A1B2C3).
    """
    ref = "ML-" + uuid.uuid4().hex[:6].upper()
    with get_db() as db:
        db.execute(
            """
            INSERT INTO bookings
              (id, user_id, client_name, username, service, stylist,
               date, time, duration, price, notes, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'confirmed')
            """,
            (
                ref,
                user.id,
                user.full_name or "Unknown",
                user.username or "",
                data.get("service",  "—"),
                data.get("stylist",  "—"),
                data.get("date",     "—"),
                data.get("time",     "—"),
                data.get("duration", "—"),
                str(data.get("price", "TBD")),
                data.get("notes", ""),
            ),
        )
        db.execute(
            """
            INSERT INTO clients (user_id, name, username, visit_count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
              name=excluded.name,
              username=excluded.username,
              visit_count=visit_count+1,
              last_seen=datetime('now')
            """,
            (user.id, user.full_name or "Unknown", user.username or ""),
        )
    return ref


def get_booking(ref: str) -> Optional[sqlite3.Row]:
    with get_db() as db:
        return db.execute("SELECT * FROM bookings WHERE id=?", (ref,)).fetchone()


def cancel_booking(ref: str) -> bool:
    with get_db() as db:
        result = db.execute(
            "UPDATE bookings SET status='cancelled' WHERE id=? AND status='confirmed'",
            (ref,),
        )
        return result.rowcount > 0


def get_user_bookings(user_id: int) -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM bookings WHERE user_id=? ORDER BY date DESC, time DESC LIMIT 10",
            (user_id,),
        ).fetchall()


def get_todays_bookings() -> list:
    today = date.today().isoformat()
    with get_db() as db:
        return db.execute(
            "SELECT * FROM bookings WHERE date=? AND status='confirmed' ORDER BY time",
            (today,),
        ).fetchall()


def get_all_bookings_for_export() -> list:
    """Return all bookings ordered by date — used by backup.py."""
    with get_db() as db:
        return db.execute(
            "SELECT * FROM bookings ORDER BY date DESC, time DESC"
        ).fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# REMINDERS
# ══════════════════════════════════════════════════════════════════════════════

def get_upcoming_reminders() -> list:
    """Return list of (kind, booking_row) tuples that need reminders right now."""
    now         = datetime.utcnow()
    now_plus_24 = now + timedelta(hours=24)
    now_plus_1  = now + timedelta(hours=1)

    with get_db() as db:
        remind_24 = db.execute(
            """
            SELECT * FROM bookings
            WHERE status='confirmed' AND reminder_24h_sent=0
              AND datetime(date || ' ' || time) BETWEEN ? AND ?
            """,
            (
                (now_plus_24 - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M"),
                (now_plus_24 + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M"),
            ),
        ).fetchall()

        remind_1 = db.execute(
            """
            SELECT * FROM bookings
            WHERE status='confirmed' AND reminder_1h_sent=0
              AND datetime(date || ' ' || time) BETWEEN ? AND ?
            """,
            (
                (now_plus_1 - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M"),
                (now_plus_1 + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M"),
            ),
        ).fetchall()

    return [("24h", b) for b in remind_24] + [("1h", b) for b in remind_1]


def mark_reminder_sent(ref: str, kind: str):
    col = "reminder_24h_sent" if kind == "24h" else "reminder_1h_sent"
    with get_db() as db:
        db.execute(f"UPDATE bookings SET {col}=1 WHERE id=?", (ref,))


# ══════════════════════════════════════════════════════════════════════════════
# STAFF & SERVICES
# ══════════════════════════════════════════════════════════════════════════════

def get_staff() -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM staff WHERE active=1 ORDER BY id"
        ).fetchall()


def get_services() -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM services WHERE active=1 ORDER BY id"
        ).fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# CLIENTS
# ══════════════════════════════════════════════════════════════════════════════

def get_client(user_id: int) -> Optional[sqlite3.Row]:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM clients WHERE user_id=?", (user_id,)
        ).fetchone()


def get_all_clients_for_export() -> list:
    """Return all clients ordered by visit count — used by backup.py."""
    with get_db() as db:
        return db.execute(
            "SELECT * FROM clients ORDER BY visit_count DESC"
        ).fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# FORMATTERS (shared helpers used by bot.py and backup.py)
# ══════════════════════════════════════════════════════════════════════════════

def fmt_price(raw, currency: str = None) -> str:
    s = str(raw).strip()
    if s.upper() == "TBD" or s == "":
        return "TBD"
    try:
        cur = currency or get_setting("currency", "₹")
        return f"{cur}{int(s):,}"
    except ValueError:
        return s


def fmt_date(iso: str) -> str:
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%A, %B %d, %Y")
    except Exception:
        return iso
