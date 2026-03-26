"""
sheets.py — Google Sheets as the primary database for bookings
==============================================================
ENV REQUIRED:
  GOOGLE_CREDENTIALS = {one-line JSON}
  GOOGLE_SHEET_NAME = Salon CRM

All booking operations (create, read, cancel, reminders) now go through
Google Sheets. SQLite (db.py) is only used for settings, staff, services.
"""

import logging
import os
import time
import json
import uuid
from datetime import datetime, timedelta
from typing import Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Salon CRM")
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

RETRY_COUNT = 3
RETRY_DELAY = 2

# ── TABS ─────────────────────────────────────────────────
TAB_BOOKINGS = "Bookings"
TAB_CLIENTS  = "Clients"
TAB_REVENUE  = "Revenue"
TAB_BACKUP   = "Backup"

# ── HEADERS ──────────────────────────────────────────────
# Column positions matter — keep in sync with helper functions below
HEADERS = {
    TAB_BOOKINGS: [
        "Booking ID",        # A  col 1
        "User ID",           # B  col 2
        "Client Name",       # C  col 3
        "Username",          # D  col 4
        "Service",           # E  col 5
        "Stylist",           # F  col 6
        "Date",              # G  col 7
        "Time",              # H  col 8
        "Duration",          # I  col 9
        "Price",             # J  col 10
        "Status",            # K  col 11
        "Notes",             # L  col 12
        "Reminder 24h Sent", # M  col 13
        "Reminder 1h Sent",  # N  col 14
        "Created At",        # O  col 15
    ],
    TAB_CLIENTS: [
        "User ID", "Name", "Username",
        "Visit Count", "First Seen", "Last Seen", "VIP",
    ],
    TAB_REVENUE: [
        "Date", "Bookings", "Revenue",
    ],
    TAB_BACKUP: [
        "Booking ID", "Client Name", "Telegram ID", "Username",
        "Service", "Stylist", "Date", "Time", "Duration",
        "Price", "Status", "Notes",
        "24h Reminder Sent", "1h Reminder Sent", "Booked At",
    ],
}

# Column index constants for TAB_BOOKINGS (1-based for gspread)
COL_BOOKING_ID    = 1
COL_USER_ID       = 2
COL_CLIENT_NAME   = 3
COL_USERNAME      = 4
COL_SERVICE       = 5
COL_STYLIST       = 6
COL_DATE          = 7
COL_TIME          = 8
COL_DURATION      = 9
COL_PRICE         = 10
COL_STATUS        = 11
COL_NOTES         = 12
COL_REMINDER_24H  = 13
COL_REMINDER_1H   = 14
COL_CREATED_AT    = 15


# ════════════════════════════════════════════════════════
# CONNECTION CACHE
# ════════════════════════════════════════════════════════

class SheetsClient:
    _gc: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    _last_auth: float = 0
    _AUTH_TTL: int = 3000

    @classmethod
    def _needs_reauth(cls):
        return cls._gc is None or (time.time() - cls._last_auth) > cls._AUTH_TTL

    @classmethod
    def get_spreadsheet(cls):
        try:
            if cls._needs_reauth():
                if not CREDS_JSON:
                    logger.warning("GOOGLE_CREDENTIALS missing")
                    return None
                creds_dict = json.loads(CREDS_JSON)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
                cls._gc = gspread.authorize(creds)
                cls._spreadsheet = None
                cls._last_auth = time.time()
                logger.info("Sheets auth success")

            if cls._spreadsheet is None:
                cls._spreadsheet = _get_or_create_spreadsheet(cls._gc)

            return cls._spreadsheet

        except Exception as e:
            logger.error(f"Sheets connection error: {e}")
            cls._gc = None
            cls._spreadsheet = None
            return None


# ════════════════════════════════════════════════════════
# SETUP
# ════════════════════════════════════════════════════════

def _get_or_create_spreadsheet(gc):
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)
    _ensure_tabs(sh)
    return sh


def _ensure_tabs(sh):
    existing = {ws.title for ws in sh.worksheets()}
    for tab, headers in HEADERS.items():
        if tab not in existing:
            ws = sh.add_worksheet(title=tab, rows=1000, cols=len(headers))
            ws.append_row(headers)
        else:
            ws = sh.worksheet(tab)
            if not ws.row_values(1):
                ws.insert_row(headers, 1)


# ════════════════════════════════════════════════════════
# RETRY HELPER
# ════════════════════════════════════════════════════════

def _with_retry(func, *args, **kwargs):
    for attempt in range(RETRY_COUNT):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt < RETRY_COUNT - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise e


# ════════════════════════════════════════════════════════
# WRITE FUNCTIONS
# ════════════════════════════════════════════════════════

def save_booking_to_sheets(data: dict, user) -> str:
    """
    Save a new booking to Google Sheets.
    Returns the booking reference ID (e.g. ML-A1B2C3).
    Also upserts the client row.
    """
    ref = "ML-" + uuid.uuid4().hex[:6].upper()
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        raise Exception("Google Sheets unavailable")

    ws = sh.worksheet(TAB_BOOKINGS)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    row = [
        ref,
        str(user.id),
        user.full_name or "Unknown",
        user.username or "",
        data.get("service", "—"),
        data.get("stylist", "—"),
        data.get("date", "—"),
        data.get("time", "—"),
        data.get("duration", "—"),
        str(data.get("price", "TBD")),
        "confirmed",
        data.get("notes", ""),
        "NO",   # reminder_24h_sent
        "NO",   # reminder_1h_sent
        now,
    ]

    _with_retry(ws.append_row, row)
    logger.info(f"[SHEETS] Booking saved: {ref}")

    # Upsert client
    try:
        upsert_client(user)
    except Exception as e:
        logger.warning(f"[SHEETS] Client upsert failed (non-fatal): {e}")

    return ref


def upsert_client(user):
    """Insert or update client visit count in Clients tab."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return

    ws = sh.worksheet(TAB_CLIENTS)
    records = ws.get_all_records()
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    uid = str(user.id)

    for i, row in enumerate(records, start=2):
        if str(row.get("User ID")) == uid:
            visit_count = int(row.get("Visit Count", 0)) + 1
            vip = "YES" if visit_count > 5 else "NO"
            ws.update_cell(i, 4, visit_count)
            ws.update_cell(i, 6, now)
            ws.update_cell(i, 7, vip)
            return

    # New client
    ws.append_row([
        uid,
        user.full_name or "Unknown",
        user.username or "",
        1,
        now,
        now,
        "NO",
    ])


def cancel_booking_in_sheets(ref: str) -> bool:
    """Set booking status to 'cancelled'. Returns True if found and updated."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return False

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()

        for i, row in enumerate(records, start=2):
            if str(row.get("Booking ID")) == ref:
                if row.get("Status") == "cancelled":
                    return False  # already cancelled
                ws.update_cell(i, COL_STATUS, "cancelled")
                return True

        return False  # not found

    except Exception as e:
        logger.error(f"[SHEETS] cancel_booking error: {e}")
        return False


def update_booking_status(booking_id: str, new_status: str) -> bool:
    """Update the Status column for a given booking ID."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return False

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()

        for i, row in enumerate(records, start=2):
            if str(row.get("Booking ID")) == str(booking_id):
                ws.update_cell(i, COL_STATUS, new_status)
                return True

        return False

    except Exception as e:
        logger.error(f"[SHEETS] update_booking_status error: {e}")
        return False


def mark_reminder_sent_sheets(ref: str, kind: str):
    """Mark reminder as sent (24h or 1h) in Sheets."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()
        col = COL_REMINDER_24H if kind == "24h" else COL_REMINDER_1H

        for i, row in enumerate(records, start=2):
            if str(row.get("Booking ID")) == ref:
                ws.update_cell(i, col, "YES")
                return

    except Exception as e:
        logger.error(f"[SHEETS] mark_reminder_sent error: {e}")


# ════════════════════════════════════════════════════════
# READ FUNCTIONS
# ════════════════════════════════════════════════════════

def get_all_bookings() -> list:
    """Return all bookings as list of dicts, newest first."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return []

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()
        # Normalize keys to match what bot.py expects
        return [_normalize_booking(r) for r in reversed(records)]
    except Exception as e:
        logger.error(f"[SHEETS] get_all_bookings error: {e}")
        return []


def get_booking(ref: str) -> Optional[dict]:
    """Return a single booking dict by ID, or None."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return None

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()

        for row in records:
            if str(row.get("Booking ID")) == ref:
                return _normalize_booking(row)

        return None

    except Exception as e:
        logger.error(f"[SHEETS] get_booking error: {e}")
        return None


def get_user_bookings(user_id: int) -> list:
    """Return bookings for a specific user, newest first, max 10."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return []

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()

        result = [
            _normalize_booking(r)
            for r in records
            if str(r.get("User ID")) == str(user_id)
        ]
        # Sort by date+time descending
        result.sort(key=lambda b: (b["date"], b["time"]), reverse=True)
        return result[:10]

    except Exception as e:
        logger.error(f"[SHEETS] get_user_bookings error: {e}")
        return []


def get_todays_bookings() -> list:
    """Return confirmed bookings for today, sorted by time."""
    today = datetime.now().strftime("%Y-%m-%d")
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return []

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()

        result = [
            _normalize_booking(r)
            for r in records
            if r.get("Date") == today and r.get("Status") == "confirmed"
        ]
        result.sort(key=lambda b: b["time"])
        return result

    except Exception as e:
        logger.error(f"[SHEETS] get_todays_bookings error: {e}")
        return []


def get_upcoming_reminders() -> list:
    """
    Return list of (kind, booking_dict) that need reminders right now.
    Checks 24h and 1h windows.
    """
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return []

    try:
        ws = sh.worksheet(TAB_BOOKINGS)
        records = ws.get_all_records()

        now = datetime.utcnow()
        result = []

        for row in records:
            if row.get("Status") != "confirmed":
                continue

            date_str = row.get("Date", "")
            time_str = row.get("Time", "")
            if not date_str or not time_str:
                continue

            try:
                appt_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            except ValueError:
                continue

            diff = appt_dt - now

            # 24h reminder: appointment is 23–25 hours away
            if row.get("Reminder 24h Sent", "NO") == "NO":
                if timedelta(hours=23) <= diff <= timedelta(hours=25):
                    result.append(("24h", _normalize_booking(row)))

            # 1h reminder: appointment is 45–75 minutes away
            if row.get("Reminder 1h Sent", "NO") == "NO":
                if timedelta(minutes=45) <= diff <= timedelta(minutes=75):
                    result.append(("1h", _normalize_booking(row)))

        return result

    except Exception as e:
        logger.error(f"[SHEETS] get_upcoming_reminders error: {e}")
        return []


def get_all_clients() -> list:
    """Return all clients for /clients command."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return []

    try:
        ws = sh.worksheet(TAB_CLIENTS)
        records = ws.get_all_records()
        # Sort by visit count desc
        records.sort(key=lambda r: int(r.get("Visit Count", 0)), reverse=True)
        return records[:15]
    except Exception as e:
        logger.error(f"[SHEETS] get_all_clients error: {e}")
        return []


def get_all_user_ids() -> list:
    """Return list of all user IDs for broadcast."""
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return []

    try:
        ws = sh.worksheet(TAB_CLIENTS)
        records = ws.get_all_records()
        return [int(r["User ID"]) for r in records if r.get("User ID")]
    except Exception as e:
        logger.error(f"[SHEETS] get_all_user_ids error: {e}")
        return []


# ════════════════════════════════════════════════════════
# BACKUP / REVENUE (unchanged)
# ════════════════════════════════════════════════════════

def sync_booking(data: dict):
    """Legacy compat — now a no-op since save_booking_to_sheets handles everything."""
    pass


def sync_client(data: dict):
    """Legacy compat — no-op."""
    pass


def push_backup_to_sheet(data: dict):
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return False

    try:
        ws = sh.worksheet(TAB_BACKUP)
        row = [
            data.get("id", ""),
            data.get("client_name", ""),
            str(data.get("user_id", "")),
            data.get("username", ""),
            data.get("service", ""),
            data.get("stylist", ""),
            data.get("date", ""),
            data.get("time", ""),
            data.get("duration", ""),
            str(data.get("price", "")),
            data.get("status", ""),
            data.get("notes", ""),
            data.get("reminder_24h", ""),
            data.get("reminder_1h", ""),
            data.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ]
        _with_retry(ws.append_row, row)
        return True
    except Exception as e:
        logger.error(f"push_backup_to_sheet error: {e}")
        return False


def push_all_revenue(total_bookings: int, total_revenue: float):
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return False

    try:
        ws = sh.worksheet(TAB_REVENUE)
        row = [
            datetime.now().strftime("%Y-%m-%d"),
            total_bookings,
            total_revenue,
        ]
        _with_retry(ws.append_row, row)
        return True
    except Exception as e:
        logger.error(f"push_all_revenue error: {e}")
        return False


def sheets_health_check():
    try:
        return SheetsClient.get_spreadsheet() is not None
    except Exception:
        return False


# ════════════════════════════════════════════════════════
# INTERNAL NORMALIZER
# ════════════════════════════════════════════════════════

def _normalize_booking(row: dict) -> dict:
    """
    Convert a Sheets record (with column name keys) into the dict shape
    that bot.py expects (same keys as the old SQLite Row objects).
    """
    return {
        "id":                 str(row.get("Booking ID", "")),
        "user_id":            int(row.get("User ID", 0) or 0),
        "client_name":        row.get("Client Name", ""),
        "username":           row.get("Username", ""),
        "service":            row.get("Service", ""),
        "stylist":            row.get("Stylist", ""),
        "date":               row.get("Date", ""),
        "time":               row.get("Time", ""),
        "duration":           row.get("Duration", ""),
        "price":              row.get("Price", "TBD"),
        "status":             row.get("Status", "confirmed"),
        "notes":              row.get("Notes", ""),
        "reminder_24h_sent":  1 if row.get("Reminder 24h Sent") == "YES" else 0,
        "reminder_1h_sent":   1 if row.get("Reminder 1h Sent") == "YES" else 0,
        "created_at":         row.get("Created At", ""),
    }
