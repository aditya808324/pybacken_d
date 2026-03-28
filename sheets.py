import os
import json
import uuid
import gspread
import logging
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

# Config from Environment Variables
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

class SheetsClient:
    _sh = None
    @classmethod
    def get_sh(cls):
        if not cls._sh:
            if not CREDS_JSON:
                raise ValueError("GOOGLE_CREDENTIALS environment variable is missing!")
            creds_dict = json.loads(CREDS_JSON)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
            cls._sh = gspread.authorize(creds).open(SHEET_NAME)
        return cls._sh

def fetch_master_data():
    """Fetches all data needed to build the local cache."""
    sh = SheetsClient.get_sh()
    return {
        "staff": sh.worksheet("Staff").get_all_records(),
        "services": sh.worksheet("Services").get_all_records(),
        "bookings": sh.worksheet("Bookings").get_all_records()
    }

def save_booking_to_sheets(data: dict, user_id: int, full_name: str, username: str = "") -> str:
    """Saves a booking to Google Sheets and updates the local cache."""
    ref = "ML-" + uuid.uuid4().hex[:6].upper()
    sh = SheetsClient.get_sh()
    ws = sh.worksheet("Bookings")
    
    # Format row for Google Sheets
    row = [
        ref, str(user_id), full_name, username,
        data.get("service"), data.get("stylist"), data.get("date"),
        data.get("time"), "60 min", str(data.get("price", "0")),
        "confirmed", data.get("notes", ""), "NO", "NO", datetime.now().strftime("%Y-%m-%d %H:%M")
    ]
    
    ws.append_row(row)
    
    # Update local cache immediately so the slot shows as 'taken' for others
    from db import sync_booking_to_cache
    sync_booking_to_cache(ref, data, user_id, full_name)
    
    return ref
