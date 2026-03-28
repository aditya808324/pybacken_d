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

# --- CONFIG ---
API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_CHAT_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
DB_PATH = "salon.db"

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# --- DATABASE LAYER ---
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
        db.execute("CREATE TABLE IF NOT EXISTS staff (id INTEGER, name TEXT, title TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS services (id INTEGER, name TEXT, duration INTEGER, price INTEGER)")
        db.execute("""CREATE TABLE IF NOT EXISTS bookings 
                     (id TEXT PRIMARY KEY, user_id TEXT, name TEXT, service TEXT, 
                      date TEXT, time TEXT, status TEXT)""")

def sync_from_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(CREDS_JSON), scope)
        client = gspread.authorize(creds)
        sh = client.open(SHEET_NAME)
        
        svcs = sh.worksheet("Services").get_all_records()
        stf = sh.worksheet("Staff").get_all_records()
        
        with get_db() as db:
            db.execute("DELETE FROM services")
            db.execute("DELETE FROM staff")
            for s in svcs:
                db.execute("INSERT INTO services VALUES (?,?,?,?)", (s['ID'], s['Name'], s['Duration'], s['Price']))
            for s in stf:
                db.execute("INSERT INTO staff VALUES (?,?,?)", (s['ID'], s['Name'], s.get('Title', 'Expert')))
        print("✅ Cache synced.")
    except Exception as e:
        print(f"❌ Sync Error: {e}")

# --- API HANDLERS ---
async def api_services(request):
    with get_db() as db:
        rows = db.execute("SELECT * FROM services").fetchall()
        return web.json_response([dict(r) for r in rows], headers={"Access-Control-Allow-Origin": "*"})

async def api_staff(request):
    with get_db() as db:
        rows = db.execute("SELECT * FROM staff").fetchall()
        return web.json_response([dict(r) for r in rows], headers={"Access-Control-Allow-Origin": "*"})

async def api_slots(request):
    date_val = request.query.get('date')
    all_slots = ["10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00"]
    with get_db() as db:
        booked = db.execute("SELECT time FROM bookings WHERE date=? AND status='confirmed'", (date_val,)).fetchall()
        booked_list = [r['time'] for r in booked]
    return web.json_response([{"time": t, "booked": t in booked_list} for t in all_slots], headers={"Access-Control-Allow-Origin": "*"})

async def api_book(request):
    data = await request.json()
    booking_id = "SH-" + uuid.uuid4().hex[:6].upper()
    
    with get_db() as db:
        db.execute("INSERT INTO bookings VALUES (?,?,?,?,?,?,?)", 
                   (booking_id, data['user_id'], data['clientName'], data['serviceNames'], data['date'], data['slot'], 'confirmed'))
    
    # Update Google Sheets asynchronously
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(CREDS_JSON), scope)
        ws = gspread.authorize(creds).open(SHEET_NAME).worksheet("Bookings")
        ws.append_row([booking_id, data['user_id'], data['clientName'], data['serviceNames'], data['date'], data['slot'], 'confirmed', datetime.now().isoformat()])
    except: pass

    msg = f"🌸 *New Booking: {booking_id}*\n\n👤 {data['clientName']}\n✂️ {data['serviceNames']}\n📅 {data['date']} @ {data['slot']}"
    await bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    return web.json_response({"success": True, "booking_id": booking_id}, headers={"Access-Control-Allow-Origin": "*"})

# --- BOT COMMANDS ---
@dp.message_handler(commands=['start'])
async def cmd_start(m: types.Message):
    kb = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("✨ Book Appointment", web_app=types.WebAppInfo(url=os.getenv("MINIAPP_URL"))))
    await m.answer("Welcome to Shringar Studio! Click below to book your glow-up.", reply_markup=kb)

@dp.message_handler(commands=['cancel'])
async def cmd_cancel(m: types.Message):
    parts = m.text.split()
    if len(parts) < 2: return await m.answer("Please provide ID: `/cancel SH-XXXXXX`", parse_mode="Markdown")
    bid = parts[1].upper()
    with get_db() as db:
        db.execute("UPDATE bookings SET status='cancelled' WHERE id=?", (bid,))
    await m.answer(f"✅ Booking {bid} has been cancelled.")

# --- SERVER START ---
async def on_startup(_):
    init_db()
    sync_from_sheets()
    app = web.Application()
    app.router.add_get('/api/services', api_services)
    app.router.add_get('/api/staff', api_staff)
    app.router.add_get('/api/slots', api_slots)
    app.router.add_post('/api/book', api_book)
    
    async def opts(request): return web.Response(headers={"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, GET, OPTIONS", "Access-Control-Allow-Headers": "Content-Type"})
    app.router.add_options('/{tail:.*}', opts)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080))).start()

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
