import sqlite3
import os
import logging
from contextlib import contextmanager

DB_PATH = "salon.db"
logger = logging.getLogger(__name__)

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()

def init_db():
    """Initializes the table structure."""
    with get_db() as db:
        db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS staff (id INTEGER PRIMARY KEY, name TEXT, title TEXT, active INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS services (id INTEGER PRIMARY KEY, name TEXT, duration INTEGER, price INTEGER, active INTEGER)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id TEXT PRIMARY KEY, 
                user_id INTEGER, 
                client_name TEXT, 
                service TEXT, 
                stylist TEXT, 
                date TEXT, 
                time TEXT, 
                status TEXT DEFAULT 'confirmed'
            )
        """)

def rehydrate_cache(master_data: dict):
    """Wipes the local SQLite and refills it with fresh data from Google Sheets."""
    with get_db() as db:
        db.execute("DELETE FROM settings")
        db.execute("DELETE FROM staff")
        db.execute("DELETE FROM services")
        db.execute("DELETE FROM bookings")

        # Sync Staff
        for s in master_data.get('staff', []):
            db.execute("INSERT INTO staff VALUES (?, ?, ?, ?)", 
                       (s.get('ID'), s.get('Name'), s.get('Title'), 1 if str(s.get('Active')).upper() == 'YES' else 0))
        
        # Sync Services
        for s in master_data.get('services', []):
            db.execute("INSERT INTO services VALUES (?, ?, ?, ?, ?)", 
                       (s.get('ID'), s.get('Name'), s.get('Duration'), s.get('Price'), 1))

        # Sync Bookings
        for b in master_data.get('bookings', []):
            db.execute("INSERT INTO bookings (id, user_id, client_name, service, stylist, date, time, status) VALUES (?,?,?,?,?,?,?,?)",
                       (str(b.get('Booking ID')), b.get('User ID'), b.get('Client Name'), b.get('Service'), b.get('Stylist'), b.get('Date'), b.get('Time'), b.get('Status')))
    print("✅ Local cache synchronized with Google Sheets.")

def sync_booking_to_cache(ref, data, user_id, name):
    """Adds a single booking to the cache immediately after writing to Sheets."""
    with get_db() as db:
        db.execute("INSERT INTO bookings (id, user_id, client_name, service, stylist, date, time) VALUES (?,?,?,?,?,?,?)",
                   (ref, user_id, name, data.get('service'), data.get('stylist'), data.get('date'), data.get('time')))

def get_booked_slots(date_str: str) -> list:
    """Returns a list of times that are already taken for a specific date."""
    with get_db() as db:
        rows = db.execute("SELECT time FROM bookings WHERE date=? AND status='confirmed'", (date_str,)).fetchall()
        return [r['time'] for r in rows]
