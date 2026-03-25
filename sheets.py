"""
sheets.py — Google Sheets Integration Layer (ENV VERSION)
=========================================================
Uses GOOGLE_CREDENTIALS env var (one-line JSON)

Setup:
  1. Set env var: GOOGLE_CREDENTIALS = {one-line JSON}
  2. Set env var: GOOGLE_SHEET_NAME = Salon CRM
  3. Share sheet with service account email
"""

import logging
import os
import time
import json
from datetime import datetime
from typing import Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Salon CRM")
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

RETRY_COUNT = 3
RETRY_DELAY = 2

# ── Sheet Tabs ────────────────────────────────────────────────────────────────
TAB_BOOKINGS = "Bookings"
TAB_CLIENTS = "Clients"
TAB_BACKUP = "Backup"
TAB_REVENUE = "Revenue"

# ── Headers ───────────────────────────────────────────────────────────────────
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
    _gc: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    _last_auth: float = 0
    _AUTH_TTL: int = 3000

    @classmethod
    def _needs_reauth(cls) -> bool:
        return cls._gc is None or (time.time() - cls._last_auth) > cls._AUTH_TTL

    @classmethod
    def get_spreadsheet(cls) -> Optional[gspread.Spreadsheet]:
        try:
            if cls._needs_reauth():

                if not CREDS_JSON:
                    logger.warning("[SHEETS] GOOGLE_CREDENTIALS missing")
                    return None

                creds_dict = json.loads(CREDS_JSON)

                creds = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, SCOPES
                )

                cls._gc = gspread.authorize(creds)
                cls._last_auth = time.time()
                cls._spreadsheet = None

                logger.info("[SHEETS] Auth success (ENV)")

            if cls._spreadsheet is None:
                cls._spreadsheet = _get_or_create_spreadsheet(cls._gc)

            return cls._spreadsheet

        except Exception as e:
            logger.error(f"[SHEETS] Connection error: {e}")
            cls._gc = None
            cls._spreadsheet = None
            return None

    @classmethod
    def invalidate(cls):
        cls._gc = None
        cls._spreadsheet = None
        cls._last_auth = 0


# ══════════════════════════════════════════════════════════════════════════════
# SETUP
# ══════════════════════════════════════════════════════════════════════════════

def _get_or_create_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)

    _ensure_tabs(sh)
    return sh


def _ensure_tabs(sh: gspread.Spreadsheet):
    existing = {ws.title for ws in sh.worksheets()}

    for tab, headers in HEADERS.items():
        if tab not in existing:
            ws = sh.add_worksheet(title=tab, rows=1000, cols=len(headers))
            ws.append_row(headers)
        else:
            ws = sh.worksheet(tab)
            if not ws.row_values(1):
                ws.insert_row(headers, 1)

    for ws in sh.worksheets():
        if ws.title == "Sheet1":
            try:
                sh.del_worksheet(ws)
            except:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# RETRY
# ══════════════════════════════════════════════════════════════════════════════

def _with_retry(func, *args, **kwargs):
    for i in range(RETRY_COUNT):
        try:
            func(*args, **kwargs)
            return True
        except Exception as e:
            time.sleep(RETRY_DELAY)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def sync_booking(data: dict):
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return

    try:
        ws = sh.worksheet(TAB_BOOKINGS)

        row = [
            data.get("id", ""),
            str(data.get("user_id", "")),
            data.get("client_name", ""),
            data.get("username", ""),
            data.get("service", ""),
            data.get("stylist", ""),
            data.get("date", ""),
            data.get("time", ""),
            str(data.get("price", "")),
            data.get("status", "confirmed"),
            data.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ]

        _with_retry(ws.append_row, row)

    except Exception as e:
        logger.error(f"sync_booking error: {e}")


def sync_client(data: dict):
    sh = SheetsClient.get_spreadsheet()
    if not sh:
        return

    try:
        ws = sh.worksheet(TAB_CLIENTS)

        visit = int(data.get("visit_count", 0))
        vip = "YES" if visit > 5 else "NO"

        row = [
            str(data.get("user_id", "")),
            data.get("name", ""),
            data.get("username", ""),
            visit,
            data.get("first_seen", ""),
            data.get("last_seen", ""),
            vip,
        ]

        ws.append_row(row)

    except Exception as e:
        logger.error(f"sync_client error: {e}")


def sheets_health_check() -> bool:
    try:
        return SheetsClient.get_spreadsheet() is not None
    except:
        return False
