import os
import json
import uuid
import logging
import sqlite3
import gspread
import asyncio
from datetime import datetime
from contextlib import contextmanager
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, executor, types
from aiohttp import web

# --- CONFIGURATION ---
API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_CHAT_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
DB_PATH = "salon.db"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# --- DATABASE CACHE LAYER ---
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    with get_db() as db:
        db.execute("CREATE TABLE IF NOT EXISTS staff (id INTEGER PRIMARY KEY, name TEXT, title TEXT, active INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS services (id INTEGER PRIMARY KEY, name TEXT, duration INTEGER, price INTEGER, active INTEGER)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id TEXT PRIMARY KEY, user_id INTEGER, client_name TEXT, 
                service TEXT, stylist TEXT, date TEXT, time TEXT, status TEXT
            )
        """)

def rehydrate_cache(data):
    with get_db() as db:
        db.execute("DELETE FROM staff")
        db.execute("DELETE FROM services")
        db.execute("DELETE FROM bookings")
        for s in data['staff']:
            db.execute("INSERT INTO staff VALUES (?,?,?,?)", (s.get('ID'), s.get('Name'), s.get('Title'), 1))
        for s in data['services']:
            db.execute("INSERT INTO services VALUES (?,?,?,?,?)", (s.get('ID'), s.get('Name'), s.get('Duration'), s.get('Price'), 1))
        for b in data['bookings']:
            db.execute("INSERT INTO bookings VALUES (?,?,?,?,?,?,?,?)", 
                       (b.get('Booking ID'), b.get('User ID'), b.get('Client Name'), b.get('Service'), b.get('Stylist'), b.get('Date'), b.get('Time'), b.get('Status')))

# --- GOOGLE SHEETS LAYER ---
class SheetsClient:
    _sh = None
    @classmethod
    def get_sh(cls):
        if not cls._sh:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(CREDS_JSON), SCOPES)
            cls._sh = gspread.authorize(creds).open(SHEET_NAME)
        return cls._sh

async def fetch_sheets_data():
    sh = SheetsClient.get_sh()
    return {
        "staff": sh.worksheet("Staff").get_all_records(),
        "services": sh.worksheet("Services").get_all_records(),
        "bookings": sh.worksheet("Bookings").get_all_records()
    }

# --- API HANDLERS (For Vercel) ---
async def api_init(request):
    with get_db() as db:
        services = [dict(r) for r in db.execute("SELECT * FROM services").fetchall()]
        staff = [dict(r) for r in db.execute("SELECT * FROM staff").fetchall()]
    return web.json_response({"services": services, "staff": staff}, headers={"Access-Control-Allow-Origin": "*"})

async def api_book(request):
    data = await request.json()
    ref = "ML-" + uuid.uuid4().hex[:6].upper()
    sh = SheetsClient.get_sh()
    ws = sh.worksheet("Bookings")
    
    row = [ref, data.get('user_id'), data.get('full_name'), data.get('username', ''), 
           data.get('service'), data.get('stylist'), data.get('date'), data.get('time'), 
           "60 min", data.get('price'), "confirmed", data.get('notes', ''), "NO", "NO", datetime.now().isoformat()]
    ws.append_row(row)
    
    # Update local cache
    with get_db() as db:
        db.execute("INSERT INTO bookings VALUES (?,?,?,?,?,?,?,?)", 
                   (ref, data.get('user_id'), data.get('full_name'), data.get('service'), data.get('stylist'), data.get('date'), data.get('time'), 'confirmed'))
    
    await bot.send_message(ADMIN_ID, f"🆕 **New Booking**\nRef: {ref}\nClient: {data.get('full_name')}\nService: {data.get('service')}", parse_mode="Markdown")
    return web.json_response({"success": True, "booking_id": ref}, headers={"Access-Control-Allow-Origin": "*"})

async def api_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
    })

# --- BOT HANDLERS ---
@dp.message_handler(commands=['start'])
async def start_cmd(message: types.Message):
    url = os.getenv("MINIAPP_URL")
    markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("✨ Book Now", web_app=types.WebAppInfo(url=url)))
    await message.answer("Welcome to Maison Lumière! Tap below to book.", reply_markup=markup)

async def on_startup(_):
    init_db()
    data = await fetch_sheets_data()
    rehydrate_cache(data)
    
    # Start API server on the port Railway provides
    app = web.Application()
    app.router.add_get('/api/init', api_init)
    app.router.add_post('/api/book', api_book)
    app.router.add_options('/{tail:.*}', api_options)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080)))
    await site.start()

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
