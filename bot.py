import os
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiohttp import web
import db
import sheets

# Env Vars
API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_CHAT_ID")

# Logging
logging.basicConfig(level=logging.INFO)

# Init Bot
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# --- STARTUP LOGIC ---
async def on_startup(_):
    print("🚀 Bot starting...")
    db.init_db()
    try:
        # Pull from Sheets and populate the local SQLite cache
        master_data = sheets.fetch_master_data()
        db.rehydrate_cache(master_data)
        print("✅ System ready and synced.")
    except Exception as e:
        print(f"❌ Initial Sync Failed: {e}")

# --- BOT HANDLERS ---
@dp.message_handler(commands=['start'])
async def start_cmd(message: types.Message):
    # Link to your Mini App
    url = os.getenv("MINIAPP_URL")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✨ Book Appointment", web_app=types.WebAppInfo(url=url)))
    await message.answer(f"Welcome to Maison Lumière, {message.from_user.first_name}! 🙏\nTap below to book your session.", reply_markup=markup)

# --- MINI APP API ENDPOINTS ---
async def get_init_data(request):
    """API for the Mini App to get services/staff."""
    with db.get_db() as conn:
        services = [dict(r) for r in conn.execute("SELECT * FROM services WHERE active=1").fetchall()]
        staff = [dict(r) for r in conn.execute("SELECT * FROM staff WHERE active=1").fetchall()]
    return web.json_response({"services": services, "staff": staff})

async def handle_booking(request):
    """API for the Mini App to submit a booking."""
    data = await request.json()
    try:
        booking_id = sheets.save_booking_to_sheets(
            data, 
            data.get('user_id'), 
            data.get('full_name'), 
            data.get('username', '')
        )
        # Notify Admin
        await bot.send_message(ADMIN_ID, f"🔔 New Booking: {booking_id}\nClient: {data['full_name']}\nService: {data['service']}")
        return web.json_response({"success": True, "booking_id": booking_id})
    except Exception as e:
        return web.json_response({"success": False, "error": str(e)})

# --- RUNNER ---
if __name__ == '__main__':
    # Start the bot
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
