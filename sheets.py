"""
sheets.py — Google Sheets Integration Layer
=============================================
All Google Sheets logic lives here. bot.py and backup.py import from this module.

Features:
  ✅ Cached gspread connection (no reconnect on every request)
  ✅ Auto-create spreadsheet + sheets if they don't exist
  ✅ Booking sync      → "Bookings" sheet
  ✅ Client sync       → "Clients" sheet  (with VIP logic)
  ✅ Backup push       → "Backup" sheet   (overwrite on each run)
  ✅ Revenue tracking  → "Revenue" sheet  (daily upsert)
  ✅ All failures wrapped in try/except — NEVER crash the bot
  ✅ Retry logic for transient Google API errors

Setup:
  1. Place creds.json (service account key) in project root
  2. Set env vars: GOOGLE_SHEET_NAME, GOOGLE_CREDS_FILE
  3. Share the spreadsheet with the service-account email in creds.json
"""

import logging
import os
import time
from datetime import datetime, date
from typing import Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SHEET_NAME  = os.getenv("GOOGLE_SHEET_NAME", "Salon CRM")
CREDS_FILE  = os.getenv("GOOGLE_CREDS_FILE", "creds.json")
SCOPES      = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
RETRY_COUNT = 3        # retries for transient Google API errors
RETRY_DELAY = 2        # seconds between retries

# ── Sheet tab names ───────────────────────────────────────────────────────────
TAB_BOOKINGS = "Bookings"
TAB_CLIENTS  = "Clients"
TAB_BACKUP   = "Backup"
TAB_REVENUE  = "Revenue"

# ── Column headers ────────────────────────────────────────────────────────────
HEADERS = {
    TAB_BOOKINGS: [
        "Booking ID", "User ID", "Name", "Username",
        "Service", "Stylist", "Date", "Time",
        "Price", "Status", "Created At",
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


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION CACHE
# ══════════════════════════════════════════════════════════════════════════════

class SheetsClient:
    """
    Singleton-style cached Google Sheets client.
    Reconnects only when credentials expire or a connection error occurs.
    """
    _gc: Optional[gspread.Client]        = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    _last_auth: float = 0
    _AUTH_TTL: int    = 3000   # re-auth every ~50 min (tokens expire at 60 min)

    @classmethod
    def _needs_reauth(cls) -> bool:
        return cls._gc is None or (time.time() - cls._last_auth) > cls._AUTH_TTL

    @classmethod
    def get_spreadsheet(cls) -> Optional[gspread.Spreadsheet]:
        """
        Returns the cached spreadsheet, re-authenticating if needed.
        Returns None if credentials are missing or any error occurs.
        """
        try:
            if cls._needs_reauth():
                if not os.path.exists(CREDS_FILE):
                    logger.warning(
                        f"[SHEETS] creds.json not found at '{CREDS_FILE}'. "
                        "Google Sheets integration disabled."
                    )
                    return None

                creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPES)
                cls._gc         = gspread.authorize(creds)
                cls._last_auth  = time.time()
                cls._spreadsheet = None   # force sheet reload after re-auth
                logger.info("[SHEETS] Authenticated with Google Sheets API")

            if cls._spreadsheet is None:
                cls._spreadsheet = _get_or_create_spreadsheet(cls._gc)

            return cls._spreadsheet

        except FileNotFoundError:
            logger.warning("[SHEETS] creds.json missing — sheets integration disabled")
            return None
        except Exception as e:
            logger.error(f"[SHEETS] Connection error: {e}")
            cls._gc          = None   # force full reconnect next time
            cls._spreadsheet = None
            return None

    @classmethod
    def invalidate(cls):
        """Force reconnect on next access (call after persistent errors)."""
        cls._gc          = None
        cls._spreadsheet = None
        cls._last_auth   = 0


# ══════════════════════════════════════════════════════════════════════════════
# SPREADSHEET SETUP
# ══════════════════════════════════════════════════════════════════════════════

def _get_or_create_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    """Open existing 'Salon CRM' spreadsheet or create it with all tabs."""
    try:
        sh = gc.open(SHEET_NAME)
        logger.info(f"[SHEETS] Opened existing spreadsheet: '{SHEET_NAME}'")
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)
        logger.info(f"[SHEETS] Created new spreadsheet: '{SHEET_NAME}'")

    _ensure_tabs(sh)
    return sh


def _ensure_tabs(sh: gspread.Spreadsheet):
    """Create missing tabs and add headers if the sheet is empty."""
    existing = {ws.title for ws in sh.worksheets()}

    for tab_name, headers in HEADERS.items():
        if tab_name not in existing:
            ws = sh.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
            ws.append_row(headers, value_input_option="RAW")
            logger.info(f"[SHEETS] Created tab '{tab_name}' with headers")
        else:
            ws = sh.worksheet(tab_name)
            # If sheet is empty, add headers
            if ws.row_count == 0 or not ws.row_values(1):
                ws.insert_row(headers, 1)
                logger.info(f"[SHEETS] Re-added headers to empty tab '{tab_name}'")

    # Remove default "Sheet1" if it still exists
    for ws in sh.worksheets():
        if ws.title == "Sheet1":
            try:
                sh.del_worksheet(ws)
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# RETRY HELPER
# ══════════════════════════════════════════════════════════════════════════════

def _with_retry(func, *args, **kwargs):
    """
    Execute func(*args, **kwargs) with up to RETRY_COUNT retries.
    Returns True on success, False on all failures.
    """
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            func(*args, **kwargs)
            return True
        except gspread.exceptions.APIError as e:
            status = getattr(e.response, 'status_code', None)
            if status == 429:
                # Rate limited — wait longer
                wait = RETRY_DELAY * attempt * 2
                logger.warning(f"[SHEETS] Rate limited. Waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
            else:
                logger.error(f"[SHEETS] API error on attempt {attempt}: {e}")
                if attempt == RETRY_COUNT:
                    return False
                time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"[SHEETS] Unexpected error on attempt {attempt}: {e}")
            if attempt == RETRY_COUNT:
                SheetsClient.invalidate()
                return False
            time.sleep(RETRY_DELAY)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — called from bot.py and backup.py
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Sync a new booking ─────────────────────────────────────────────────────

def sync_booking(booking_data: dict):
    """
    Append a confirmed booking row to the 'Bookings' sheet.

    booking_data keys (all strings):
        id, user_id, client_name, username, service, stylist,
        date, time, price, status, created_at
    """
    sh = SheetsClient.get_spreadsheet()
    if sh is None:
        return

    try:
        ws  = sh.worksheet(TAB_BOOKINGS)
        row = [
            booking_data.get("id",          ""),
            str(booking_data.get("user_id", "")),
            booking_data.get("client_name", ""),
            booking_data.get("username",    ""),
            booking_data.get("service",     ""),
            booking_data.get("stylist",     ""),
            booking_data.get("date",        ""),
            booking_data.get("time",        ""),
            str(booking_data.get("price",   "")),
            booking_data.get("status",      "confirmed"),
            booking_data.get("created_at",  datetime.now().strftime("%Y-%m-%d %H:%M")),
        ]
        success = _with_retry(ws.append_row, row, value_input_option="USER_ENTERED")
        if success:
            logger.info(f"[SHEETS] Booking synced: {booking_data.get('id')}")
        else:
            logger.error(f"[SHEETS] Failed to sync booking: {booking_data.get('id')}")
    except Exception as e:
        logger.error(f"[SHEETS] sync_booking error: {e}")


# ── 2. Update booking status (cancel / reschedule) ───────────────────────────

def update_booking_status(booking_id: str, new_status: str):
    """
    Find the row with the given booking_id in 'Bookings' sheet
    and update the Status column.
    """
    sh = SheetsClient.get_spreadsheet()
    if sh is None:
        return

    try:
        ws    = sh.worksheet(TAB_BOOKINGS)
        # Column index for "Status" (1-based) = position in HEADERS[TAB_BOOKINGS]
        col_status = HEADERS[TAB_BOOKINGS].index("Status") + 1
        col_id     = HEADERS[TAB_BOOKINGS].index("Booking ID") + 1

        cell = ws.find(booking_id, in_column=col_id)
        if cell:
            _with_retry(ws.update_cell, cell.row, col_status, new_status)
            logger.info(f"[SHEETS] Booking {booking_id} status → '{new_status}'")
        else:
            logger.warning(f"[SHEETS] Booking {booking_id} not found for status update")
    except Exception as e:
        logger.error(f"[SHEETS] update_booking_status error: {e}")


# ── 3. Sync / upsert a client ─────────────────────────────────────────────────

def sync_client(client_data: dict):
    """
    Add or update a client row in the 'Clients' sheet.
    VIP = YES if visit_count > 5.

    client_data keys:
        user_id, name, username, visit_count,
        first_seen, last_seen
    """
    sh = SheetsClient.get_spreadsheet()
    if sh is None:
        return

    try:
        ws         = sh.worksheet(TAB_CLIENTS)
        col_uid    = HEADERS[TAB_CLIENTS].index("User ID") + 1
        visit_count = int(client_data.get("visit_count", 0))
        vip        = "YES" if visit_count > 5 else "NO"

        new_row = [
            str(client_data.get("user_id",     "")),
            client_data.get("name",            ""),
            client_data.get("username",        ""),
            visit_count,
            client_data.get("first_seen",      ""),
            client_data.get("last_seen",       ""),
            vip,
        ]

        cell = ws.find(str(client_data.get("user_id", "")), in_column=col_uid)
        if cell:
            # Update existing row
            row_range = f"A{cell.row}:{chr(64 + len(new_row))}{cell.row}"
            _with_retry(ws.update, row_range, [new_row], value_input_option="USER_ENTERED")
            logger.info(f"[SHEETS] Client updated: {client_data.get('name')} (VIP={vip})")
        else:
            # Append new client
            _with_retry(ws.append_row, new_row, value_input_option="USER_ENTERED")
            logger.info(f"[SHEETS] Client added: {client_data.get('name')} (VIP={vip})")

    except Exception as e:
        logger.error(f"[SHEETS] sync_client error: {e}")


# ── 4. Push backup CSV data → "Backup" sheet ─────────────────────────────────

def push_backup_to_sheet(bookings: list, stats: dict):
    """
    Overwrite the 'Backup' sheet with fresh booking data.
    Called from backup.py after CSV export.

    bookings: list of sqlite3.Row objects (or dicts) from the DB
    stats:    {"bookings": int, "clients": int}
    """
    sh = SheetsClient.get_spreadsheet()
    if sh is None:
        return

    try:
        ws = sh.worksheet(TAB_BACKUP)

        # Build rows
        rows = [HEADERS[TAB_BACKUP]]   # header row first
        for b in bookings:
            rows.append([
                b["id"],
                b["client_name"],
                str(b["user_id"]),
                b["username"] or "",
                b["service"],
                b["stylist"],
                b["date"],
                b["time"],
                b["duration"],
                str(b["price"]),
                b["status"],
                b["notes"] or "",
                "Yes" if b["reminder_24h_sent"] else "No",
                "Yes" if b["reminder_1h_sent"]  else "No",
                b["created_at"],
            ])

        # Overwrite sheet: clear then write
        ws.clear()
        _with_retry(
            ws.update,
            f"A1:{chr(64 + len(HEADERS[TAB_BACKUP]))}{len(rows)}",
            rows,
            value_input_option="USER_ENTERED",
        )
        logger.info(
            f"[SHEETS] Backup sheet updated: "
            f"{stats.get('bookings', 0)} bookings, "
            f"{stats.get('clients', 0)} clients"
        )
    except Exception as e:
        logger.error(f"[SHEETS] push_backup_to_sheet error: {e}")


# ── 5. Upsert daily revenue row ───────────────────────────────────────────────

def upsert_revenue_row(date_str: str, booking_count: int, revenue: int):
    """
    Insert or update a row in the 'Revenue' sheet for the given date.
    Format: Date | Bookings | Revenue

    Called from backup.py revenue summary step.
    """
    sh = SheetsClient.get_spreadsheet()
    if sh is None:
        return

    try:
        ws      = sh.worksheet(TAB_REVENUE)
        col_date = HEADERS[TAB_REVENUE].index("Date") + 1

        new_row = [date_str, booking_count, revenue]
        cell    = ws.find(date_str, in_column=col_date)

        if cell:
            row_range = f"A{cell.row}:C{cell.row}"
            _with_retry(ws.update, row_range, [new_row], value_input_option="USER_ENTERED")
            logger.info(f"[SHEETS] Revenue updated for {date_str}: {booking_count} bookings, {revenue}")
        else:
            _with_retry(ws.append_row, new_row, value_input_option="USER_ENTERED")
            logger.info(f"[SHEETS] Revenue added for {date_str}: {booking_count} bookings, {revenue}")

    except Exception as e:
        logger.error(f"[SHEETS] upsert_revenue_row error: {e}")


# ── 6. Bulk revenue backfill ─────────────────────────────────────────────────

def push_all_revenue(revenue_rows: list):
    """
    Bulk-push all revenue rows to the Revenue sheet.
    revenue_rows: list of (date_str, booking_count, revenue) tuples.
    Called during full backup to keep Revenue sheet complete.
    """
    sh = SheetsClient.get_spreadsheet()
    if sh is None:
        return

    try:
        ws = sh.worksheet(TAB_REVENUE)
        ws.clear()

        rows = [HEADERS[TAB_REVENUE]]
        for date_str, count, rev in revenue_rows:
            rows.append([date_str, count, rev])

        _with_retry(
            ws.update,
            f"A1:C{len(rows)}",
            rows,
            value_input_option="USER_ENTERED",
        )
        logger.info(f"[SHEETS] Revenue sheet fully refreshed: {len(revenue_rows)} rows")
    except Exception as e:
        logger.error(f"[SHEETS] push_all_revenue error: {e}")


# ── 7. Health check ───────────────────────────────────────────────────────────

def sheets_health_check() -> bool:
    """
    Returns True if Google Sheets connection is working.
    Used by /status command in bot.py.
    """
    try:
        sh = SheetsClient.get_spreadsheet()
        return sh is not None
    except Exception:
        return False
