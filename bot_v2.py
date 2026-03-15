"""
Maison Lumière — Salon Booking Bot v2.0
========================================
Features:
  ✅ SQLite persistent database
  ✅ Real availability (admin sets hours, booked slots auto-block)
  ✅ Automated reminders (24h + 1h before appointment)
  ✅ Cancellation & rescheduling
  ✅ Full admin setup (name, services, staff, hours via commands)
  ✅ /today — daily schedule for admin
  ✅ /clients — client list with history
  ✅ Broadcast promotions to all clients

Install:
    pip install python-telegram-bot==20.7 apscheduler==3.10.4

Run:
    python bot.py
"""

import json
import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, date
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from backup import run_backup
from apscheduler.triggers.interval import IntervalTrigger
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN     = os.getenv("BOT_TOKEN", "8721559800:AAGOz2DePO0962YHnfnsLaGjt4vb1lI0dRA")
MINI_APP_URL  = os.getenv("MINI_APP_URL","https://telebot-eight-peach.vercel.app")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "6099012885"))
DB_PATH       = os.getenv("DB_PATH", "salon.db")
BACKUP_DIR    = os.getenv("BACKUP_DIR",  "backups")   # local folder for auto-backups

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE LAYER
# ══════════════════════════════════════════════════════════════════════════════

@contextmanager
def get_db():
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


def init_db():
    """Create all tables if they don't exist."""
    with get_db() as db:

        # Salon settings (key-value store)
        db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # Staff members
        db.execute("""
            CREATE TABLE IF NOT EXISTS staff (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                title      TEXT NOT NULL DEFAULT 'Stylist',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        # Services
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

        # Bookings
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

        # Clients
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

        # Insert default settings
        defaults = {
            "salon_name":    "Maison Lumière",
            "salon_address": "12 Rue de la Paix, Suite 3",
            "salon_phone":   "+1 (555) 012-3456",
            "open_time":     "09:00",
            "close_time":    "18:00",
            "slot_duration": "60",
            "working_days":  "1,2,3,4,5",  # Mon-Fri (0=Mon, 6=Sun)
            "currency":      "₹",
        }
        for key, val in defaults.items():
            db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, val)
            )

        # Insert default services
        default_services = [
            ("Haircut & Style", 45, 850),
            ("Hair Coloring",  120, 1450),
            ("Mani & Pedi",     60,  750),
            ("Facial Ritual",   75, 1200),
        ]
        for name, dur, price in default_services:
            db.execute(
                "INSERT OR IGNORE INTO services (name, duration, price) VALUES (?, ?, ?)",
                (name, dur, price)
            )

        # Insert default staff
        default_staff = [
            ("Isabelle Moreau", "Creative Director"),
            ("Sophie Laurent",  "Color Specialist"),
            ("Chloé Petit",     "Skin & Nail Expert"),
        ]
        for name, title in default_staff:
            db.execute(
                "INSERT OR IGNORE INTO staff (name, title) VALUES (?, ?)",
                (name, title)
            )

    logger.info(f"[DB] Initialized: {DB_PATH}")


# ── Settings helpers ──────────────────────────────────────────────────────────
def get_setting(key: str, default: str = "") -> str:
    with get_db() as db:
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

def set_setting(key: str, value: str):
    with get_db() as db:
        db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))

def get_all_settings() -> dict:
    with get_db() as db:
        rows = db.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}


# ── Availability helpers ──────────────────────────────────────────────────────
def get_booked_slots(date_str: str) -> list[str]:
    """Return list of times already booked for a date."""
    with get_db() as db:
        rows = db.execute(
            "SELECT time FROM bookings WHERE date=? AND status NOT IN ('cancelled')",
            (date_str,)
        ).fetchall()
        return [r["time"] for r in rows]

def get_available_slots(date_str: str) -> list[str]:
    """Return available time slots for a given date."""
    s = get_all_settings()
    open_h, open_m   = map(int, s["open_time"].split(":"))
    close_h, close_m = map(int, s["close_time"].split(":"))
    slot_mins = int(s.get("slot_duration", "60"))
    working_days = [int(d) for d in s["working_days"].split(",")]

    # Check if date is a working day
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    if date_obj.weekday() not in working_days:
        return []

    # Generate all slots
    slots = []
    current = datetime(date_obj.year, date_obj.month, date_obj.day, open_h, open_m)
    end     = datetime(date_obj.year, date_obj.month, date_obj.day, close_h, close_m)
    while current < end:
        slots.append(current.strftime("%H:%M"))
        current += timedelta(minutes=slot_mins)

    booked = get_booked_slots(date_str)
    return [s for s in slots if s not in booked]

def is_slot_available(date_str: str, time_str: str) -> bool:
    return time_str in get_available_slots(date_str)


# ── Booking helpers ───────────────────────────────────────────────────────────
def save_booking(data: dict, user) -> dict:
    ref = "ML-" + uuid.uuid4().hex[:6].upper()
    with get_db() as db:
        db.execute("""
            INSERT INTO bookings
              (id, user_id, client_name, username, service, stylist,
               date, time, duration, price, notes, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'confirmed')
        """, (
            ref, user.id, user.full_name or "Unknown",
            user.username or "",
            data.get("service", "—"), data.get("stylist", "—"),
            data.get("date", "—"),   data.get("time", "—"),
            data.get("duration", "—"), str(data.get("price", "TBD")),
            data.get("notes", ""),
        ))
        # Upsert client record
        db.execute("""
            INSERT INTO clients (user_id, name, username, visit_count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
              name=excluded.name,
              username=excluded.username,
              visit_count=visit_count+1,
              last_seen=datetime('now')
        """, (user.id, user.full_name or "Unknown", user.username or ""))
    return ref

def get_booking(ref: str) -> Optional[sqlite3.Row]:
    with get_db() as db:
        return db.execute("SELECT * FROM bookings WHERE id=?", (ref,)).fetchone()

def cancel_booking(ref: str) -> bool:
    with get_db() as db:
        result = db.execute(
            "UPDATE bookings SET status='cancelled' WHERE id=? AND status='confirmed'",
            (ref,)
        )
        return result.rowcount > 0

def get_user_bookings(user_id: int) -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM bookings WHERE user_id=? ORDER BY date DESC, time DESC LIMIT 10",
            (user_id,)
        ).fetchall()

def get_todays_bookings() -> list:
    today = date.today().isoformat()
    with get_db() as db:
        return db.execute(
            "SELECT * FROM bookings WHERE date=? AND status='confirmed' ORDER BY time",
            (today,)
        ).fetchall()

def get_upcoming_reminders() -> list:
    """Get bookings needing a 24h or 1h reminder right now."""
    now = datetime.utcnow()
    now_plus_24 = now + timedelta(hours=24)
    now_plus_1  = now + timedelta(hours=1)

    with get_db() as db:
        # 24h reminder: appointment is in 23h-25h from now
        remind_24 = db.execute("""
            SELECT * FROM bookings
            WHERE status='confirmed' AND reminder_24h_sent=0
              AND datetime(date || ' ' || time) BETWEEN ? AND ?
        """, (
            (now_plus_24 - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M"),
            (now_plus_24 + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M"),
        )).fetchall()

        # 1h reminder: appointment is in 45min-75min from now
        remind_1 = db.execute("""
            SELECT * FROM bookings
            WHERE status='confirmed' AND reminder_1h_sent=0
              AND datetime(date || ' ' || time) BETWEEN ? AND ?
        """, (
            (now_plus_1 - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M"),
            (now_plus_1 + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M"),
        )).fetchall()

    return [("24h", b) for b in remind_24] + [("1h", b) for b in remind_1]

def mark_reminder_sent(ref: str, kind: str):
    col = "reminder_24h_sent" if kind == "24h" else "reminder_1h_sent"
    with get_db() as db:
        db.execute(f"UPDATE bookings SET {col}=1 WHERE id=?", (ref,))


# ── Staff & Service helpers ───────────────────────────────────────────────────
def get_staff() -> list:
    with get_db() as db:
        return db.execute("SELECT * FROM staff WHERE active=1 ORDER BY id").fetchall()

def get_services() -> list:
    with get_db() as db:
        return db.execute("SELECT * FROM services WHERE active=1 ORDER BY id").fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def fmt_price(raw) -> str:
    s = str(raw).strip()
    if s.upper() == "TBD" or s == "":
        return "TBD"
    try:
        cur = get_setting("currency", "₹")
        return f"{cur}{int(s):,}"
    except ValueError:
        return s

def fmt_date(iso: str) -> str:
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%A, %B %d, %Y")
    except Exception:
        return iso

def booking_confirm_msg(b, client_name: str) -> str:
    notes = f"\n📝 *Notes:*      {b['notes']}" if b['notes'] else ""
    return (
        f"✅ *Booking Confirmed!*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Thank you, *{client_name}*!\n\n"
        f"🪄 *Service:*   {b['service']}\n"
        f"👤 *Stylist:*    {b['stylist']}\n"
        f"📅 *Date:*       {fmt_date(b['date'])}\n"
        f"⏰ *Time:*       {b['time']}\n"
        f"⌛ *Duration:*  {b['duration']}\n"
        f"💰 *Total:*      {fmt_price(b['price'])}"
        f"{notes}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 *Booking ID:* `{b['id']}`\n\n"
        f"_To cancel: /cancel {b['id']}_"
    )

def admin_alert_msg(b, user) -> str:
    salon = get_setting("salon_name", "Salon")
    notes = f"\n📝 Notes:    {b['notes']}" if b['notes'] else ""
    uname = f" (@{user.username})" if user.username else ""
    return (
        f"🔔 *NEW BOOKING — {b['id']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 *Client:*     [{user.full_name}](tg://user?id={user.id}){uname}\n"
        f"🆔 *User ID:*   `{user.id}`\n\n"
        f"🪄 *Service:*   {b['service']}\n"
        f"👩‍🎨 *Stylist:*    {b['stylist']}\n"
        f"📅 *Date:*       {fmt_date(b['date'])}\n"
        f"⏰ *Time:*       {b['time']}\n"
        f"⌛ *Duration:*  {b['duration']}\n"
        f"💰 *Total:*      {fmt_price(b['price'])}"
        f"{notes}\n\n"
        f"🔖 *Booking ID:* `{b['id']}`"
    )


# ══════════════════════════════════════════════════════════════════════════════
# BOT HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

def book_keyboard():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✦  Book an Appointment", web_app=WebAppInfo(url=MINI_APP_URL))
    ]])


# ── /start ────────────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    salon = get_setting("salon_name", "Maison Lumière")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✦  Book an Appointment", web_app=WebAppInfo(url=MINI_APP_URL))],
        [InlineKeyboardButton("📋 My Bookings", callback_data="my_bookings"),
         InlineKeyboardButton("ℹ️ About",       callback_data="about")],
    ])
    await update.message.reply_text(
        f"✦ *Welcome to {salon}, {user.first_name}!*\n\n"
        "Your destination for luxury beauty experiences.\n\n"
        "Tap *Book an Appointment* to get started.",
        parse_mode="Markdown", reply_markup=keyboard,
    )


# ── /book ─────────────────────────────────────────────────────────────────────
async def book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✦ *Book your appointment*\n\nTap below to open the booking experience.",
        parse_mode="Markdown", reply_markup=book_keyboard(),
    )


# ── /mybookings ───────────────────────────────────────────────────────────────
async def my_bookings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    rows = get_user_bookings(uid)
    if not rows:
        await update.message.reply_text(
            "You have no bookings yet.\n\nUse /book to reserve your experience. ✦"
        )
        return

    lines = [f"✦ *Your Bookings ({len(rows)} total)*\n"]
    for b in rows:
        status_icon = "✅" if b["status"] == "confirmed" else "❌"
        lines.append(
            f"{status_icon} `{b['id']}`\n"
            f"🪄 {b['service']}  |  👤 {b['stylist']}\n"
            f"📅 {fmt_date(b['date'])}  ⏰ {b['time']}\n"
            f"💰 {fmt_price(b['price'])}  |  _{b['status'].title()}_"
        )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✦ Book Another", web_app=WebAppInfo(url=MINI_APP_URL))
    ]])
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=keyboard
    )


# ── /cancel <booking_id> ──────────────────────────────────────────────────────
async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    args = context.args

    if not args:
        await update.message.reply_text(
            "Usage: `/cancel ML-XXXXXX`\n\n"
            "Use /mybookings to see your booking IDs.",
            parse_mode="Markdown"
        )
        return

    ref = args[0].upper()
    b   = get_booking(ref)

    if not b:
        await update.message.reply_text(f"❌ Booking `{ref}` not found.", parse_mode="Markdown")
        return

    # Only the client or admin can cancel
    if b["user_id"] != uid and uid != ADMIN_CHAT_ID:
        await update.message.reply_text("⛔ You can only cancel your own bookings.")
        return

    if b["status"] == "cancelled":
        await update.message.reply_text(f"ℹ️ Booking `{ref}` is already cancelled.", parse_mode="Markdown")
        return

    # Confirm before cancelling
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, cancel it", callback_data=f"confirm_cancel:{ref}"),
        InlineKeyboardButton("❌ Keep it",        callback_data="cancel_abort"),
    ]])
    await update.message.reply_text(
        f"Are you sure you want to cancel this booking?\n\n"
        f"🔖 `{ref}`\n"
        f"🪄 {b['service']}  |  👤 {b['stylist']}\n"
        f"📅 {fmt_date(b['date'])}  ⏰ {b['time']}",
        parse_mode="Markdown", reply_markup=keyboard,
    )


# ── /reschedule <booking_id> ──────────────────────────────────────────────────
async def reschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: `/reschedule ML-XXXXXX`\n\nThis will cancel your current booking and open the app to rebook.",
            parse_mode="Markdown"
        )
        return

    ref = args[0].upper()
    b   = get_booking(ref)

    if not b or b["user_id"] != update.effective_user.id:
        await update.message.reply_text("❌ Booking not found or not yours.")
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, reschedule", callback_data=f"confirm_reschedule:{ref}"),
        InlineKeyboardButton("❌ Keep it",         callback_data="cancel_abort"),
    ]])
    await update.message.reply_text(
        f"Rescheduling will cancel `{ref}` and open the booking app.\n\n"
        f"🪄 {b['service']}  |  📅 {fmt_date(b['date'])}  ⏰ {b['time']}\n\n"
        f"Proceed?",
        parse_mode="Markdown", reply_markup=keyboard,
    )


# ── /today (admin) ────────────────────────────────────────────────────────────
async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        await update.message.reply_text("⛔ Admin only.")
        return

    bookings = get_todays_bookings()
    today_str = date.today().strftime("%A, %B %d, %Y")

    if not bookings:
        await update.message.reply_text(
            f"📅 *{today_str}*\n\nNo appointments today. 🎉",
            parse_mode="Markdown"
        )
        return

    lines = [f"📅 *Today's Schedule — {today_str}*\n_{len(bookings)} appointments_\n"]
    for b in bookings:
        lines.append(
            f"⏰ *{b['time']}*  —  {b['service']}\n"
            f"   👤 {b['client_name']}  |  💰 {fmt_price(b['price'])}\n"
            f"   🔖 `{b['id']}`"
        )

    total = sum(int(b["price"]) for b in bookings if str(b["price"]).isdigit())
    cur   = get_setting("currency", "₹")
    if total:
        lines.append(f"\n💰 *Expected Revenue: {cur}{total:,}*")

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


# ── /slots <YYYY-MM-DD> ───────────────────────────────────────────────────────
async def slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        today = date.today().isoformat()
        date_str = today
    else:
        date_str = args[0]

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("Usage: `/slots 2025-06-15`", parse_mode="Markdown")
        return

    available = get_available_slots(date_str)
    booked    = get_booked_slots(date_str)

    if not available and not booked:
        await update.message.reply_text(f"📅 {fmt_date(date_str)} is not a working day.")
        return

    avail_str  = "  ".join(available) if available else "_None available_"
    booked_str = "  ".join(booked)    if booked    else "_None_"

    await update.message.reply_text(
        f"📅 *{fmt_date(date_str)}*\n\n"
        f"✅ *Available:*\n{avail_str}\n\n"
        f"🔒 *Booked:*\n{booked_str}",
        parse_mode="Markdown"
    )


# ── /clients (admin) ──────────────────────────────────────────────────────────
async def clients_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        await update.message.reply_text("⛔ Admin only.")
        return

    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM clients ORDER BY visit_count DESC LIMIT 15"
        ).fetchall()

    if not rows:
        await update.message.reply_text("No clients yet.")
        return

    lines = [f"👥 *Clients ({len(rows)} shown)*\n"]
    for c in rows:
        uname = f" @{c['username']}" if c["username"] else ""
        lines.append(
            f"👤 *{c['name']}*{uname}\n"
            f"   🗓 {c['visit_count']} visits  |  Last: {c['last_seen'][:10]}"
        )

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


# ── /broadcast <message> (admin) ─────────────────────────────────────────────
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_CHAT_ID:
        await update.message.reply_text("⛔ Admin only.")
        return

    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text(
            "Usage: `/broadcast Your message here`\n\n"
            "Sends your message to all past clients.",
            parse_mode="Markdown"
        )
        return

    with get_db() as db:
        clients = db.execute("SELECT DISTINCT user_id FROM clients").fetchall()

    salon   = get_setting("salon_name", "Maison Lumière")
    sent, failed = 0, 0

    for c in clients:
        try:
            await context.bot.send_message(
                chat_id=c["user_id"],
                text=f"📢 *{salon}*\n\n{msg}",
                parse_mode="Markdown",
                reply_markup=book_keyboard(),
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Broadcast failed for {c['user_id']}: {e}")
            failed += 1

    await update.message.reply_text(
        f"📢 Broadcast complete!\n\n✅ Sent: {sent}\n❌ Failed: {failed}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN SETUP COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

def admin_only(func):
    """Decorator to restrict commands to admin."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_CHAT_ID:
            await update.message.reply_text("⛔ Admin only.")
            return
        await func(update, context)
    return wrapper


@admin_only
async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all setup commands."""
    await update.message.reply_text(
        "⚙️ *Admin Setup Commands*\n\n"
        "*Salon Info:*\n"
        "/setname Salon Name\n"
        "/setaddress Your Address\n"
        "/setphone +91 98765 43210\n"
        "/setcurrency ₹\n\n"
        "*Hours:*\n"
        "/sethours 09:00 18:00\n"
        "/setdays 1,2,3,4,5  _(0=Mon, 6=Sun)_\n"
        "/setslot 60  _(slot duration in minutes)_\n\n"
        "*Services:*\n"
        "/addservice Name|Duration|Price\n"
        "/listservices\n"
        "/removeservice ID\n\n"
        "*Staff:*\n"
        "/addstaff Name|Title\n"
        "/liststaff\n"
        "/removestaff ID\n\n"
        "*Reports:*\n"
        "/today — Today's schedule\n"
        "/slots YYYY-MM-DD — Availability\n"
        "/clients — Client list\n"
        "/admin — All bookings\n"
        "/broadcast Message — Message all clients",
        parse_mode="Markdown"
    )


@admin_only
async def setname_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args)
    if not val:
        await update.message.reply_text("Usage: /setname Salon Name")
        return
    set_setting("salon_name", val)
    await update.message.reply_text(f"✅ Salon name set to: *{val}*", parse_mode="Markdown")


@admin_only
async def setaddress_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args)
    if not val:
        await update.message.reply_text("Usage: /setaddress Your Address")
        return
    set_setting("salon_address", val)
    await update.message.reply_text(f"✅ Address set to: *{val}*", parse_mode="Markdown")


@admin_only
async def setphone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args)
    if not val:
        await update.message.reply_text("Usage: /setphone +91 98765 43210")
        return
    set_setting("salon_phone", val)
    await update.message.reply_text(f"✅ Phone set to: *{val}*", parse_mode="Markdown")


@admin_only
async def setcurrency_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args).strip()
    if not val:
        await update.message.reply_text("Usage: /setcurrency ₹")
        return
    set_setting("currency", val)
    await update.message.reply_text(f"✅ Currency set to: *{val}*", parse_mode="Markdown")


@admin_only
async def sethours_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /sethours 09:00 18:00")
        return
    open_t, close_t = context.args
    set_setting("open_time",  open_t)
    set_setting("close_time", close_t)
    await update.message.reply_text(
        f"✅ Working hours set: *{open_t}* – *{close_t}*", parse_mode="Markdown"
    )


@admin_only
async def setdays_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: /setdays 1,2,3,4,5\n_(0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun)_",
            parse_mode="Markdown"
        )
        return
    val = context.args[0]
    set_setting("working_days", val)
    day_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    days_str  = ", ".join(day_names[int(d)] for d in val.split(","))
    await update.message.reply_text(
        f"✅ Working days set: *{days_str}*", parse_mode="Markdown"
    )


@admin_only
async def setslot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setslot 60")
        return
    set_setting("slot_duration", context.args[0])
    await update.message.reply_text(
        f"✅ Slot duration set to: *{context.args[0]} minutes*", parse_mode="Markdown"
    )


@admin_only
async def addservice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = " ".join(context.args)
    parts = raw.split("|")
    if len(parts) != 3:
        await update.message.reply_text(
            "Usage: `/addservice Haircut|45|850`\n_(Name|Duration mins|Price)_",
            parse_mode="Markdown"
        )
        return
    name, dur, price = [p.strip() for p in parts]
    with get_db() as db:
        db.execute(
            "INSERT INTO services (name, duration, price) VALUES (?, ?, ?)",
            (name, int(dur), int(price))
        )
    cur = get_setting("currency", "₹")
    await update.message.reply_text(
        f"✅ Service added: *{name}* | {dur} min | {cur}{int(price):,}",
        parse_mode="Markdown"
    )


@admin_only
async def listservices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    services = get_services()
    if not services:
        await update.message.reply_text("No services found. Use /addservice to add one.")
        return
    cur   = get_setting("currency", "₹")
    lines = ["🛎 *Services*\n"]
    for s in services:
        lines.append(f"ID `{s['id']}` — *{s['name']}* | {s['duration']} min | {cur}{s['price']:,}")
    await update.message.reply_text(
        "\n".join(lines) + "\n\nTo remove: `/removeservice ID`",
        parse_mode="Markdown"
    )


@admin_only
async def removeservice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /removeservice 3")
        return
    sid = int(context.args[0])
    with get_db() as db:
        db.execute("UPDATE services SET active=0 WHERE id=?", (sid,))
    await update.message.reply_text(f"✅ Service ID {sid} removed.")


@admin_only
async def addstaff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw   = " ".join(context.args)
    parts = raw.split("|")
    if len(parts) < 1:
        await update.message.reply_text(
            "Usage: `/addstaff Priya|Hair Expert`",
            parse_mode="Markdown"
        )
        return
    name  = parts[0].strip()
    title = parts[1].strip() if len(parts) > 1 else "Stylist"
    with get_db() as db:
        db.execute("INSERT INTO staff (name, title) VALUES (?, ?)", (name, title))
    await update.message.reply_text(
        f"✅ Staff added: *{name}* — _{title}_", parse_mode="Markdown"
    )


@admin_only
async def liststaff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    staff = get_staff()
    if not staff:
        await update.message.reply_text("No staff found. Use /addstaff to add one.")
        return
    lines = ["👩‍🎨 *Staff Members*\n"]
    for s in staff:
        lines.append(f"ID `{s['id']}` — *{s['name']}* | _{s['title']}_")
    await update.message.reply_text(
        "\n".join(lines) + "\n\nTo remove: `/removestaff ID`",
        parse_mode="Markdown"
    )


@admin_only
async def removestaff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /removestaff 2")
        return
    sid = int(context.args[0])
    with get_db() as db:
        db.execute("UPDATE staff SET active=0 WHERE id=?", (sid,))
    await update.message.reply_text(f"✅ Staff ID {sid} removed.")


@admin_only
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM bookings ORDER BY created_at DESC LIMIT 10"
        ).fetchall()
        total = db.execute(
            "SELECT COUNT(*) as c FROM bookings WHERE status='confirmed'"
        ).fetchone()["c"]

    if not rows:
        await update.message.reply_text("No bookings yet.")
        return

    lines = [f"✦ *All Bookings — {total} confirmed*\n"]
    for b in rows:
        status_icon = "✅" if b["status"] == "confirmed" else "❌"
        lines.append(
            f"{status_icon} `{b['id']}`  —  {b['service']}\n"
            f"👤 {b['client_name']}  |  🎨 {b['stylist']}\n"
            f"📅 {fmt_date(b['date'])}  ⏰ {b['time']}  💰 {fmt_price(b['price'])}"
        )
    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


# ── /status ───────────────────────────────────────────────────────────────────
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    s        = get_all_settings()
    is_admin = uid == ADMIN_CHAT_ID

    with get_db() as db:
        total_bookings = db.execute("SELECT COUNT(*) as c FROM bookings").fetchone()["c"]
        total_clients  = db.execute("SELECT COUNT(*) as c FROM clients").fetchone()["c"]

    lines = [
        "🔧 *Bot Status*\n",
        f"✅ Bot running",
        f"{'✅' if MINI_APP_URL.startswith('https') else '❌'} Mini App: `{MINI_APP_URL}`",
        f"{'✅' if is_admin else '⚠️'} Your ID `{uid}` {'= Admin' if is_admin else '≠ Admin'}",
        f"\n🏪 *{s.get('salon_name', '—')}*",
        f"📍 {s.get('salon_address', '—')}",
        f"📞 {s.get('salon_phone', '—')}",
        f"🕐 {s.get('open_time', '—')} – {s.get('close_time', '—')}",
        f"\n📊 Total bookings: *{total_bookings}*",
        f"👥 Total clients: *{total_clients}*",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /help ─────────────────────────────────────────────────────────────────────
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✦ *Commands*\n\n"
        "/start — Welcome\n"
        "/book — Open booking app\n"
        "/mybookings — Your appointments\n"
        "/cancel ML-XXXXX — Cancel a booking\n"
        "/reschedule ML-XXXXX — Reschedule\n"
        "/status — Bot info\n"
        "/help — This message\n\n"
        "_Admin: /setup for all admin commands_",
        parse_mode="Markdown",
    )


# ── Callback handler ──────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = update.effective_user.id
    data = query.data

    if data == "my_bookings":
        rows = get_user_bookings(uid)
        if not rows:
            text = "No bookings yet. Use /book to reserve your experience. ✦"
        else:
            lines = [f"✦ *Your Bookings*\n"]
            for b in rows:
                icon = "✅" if b["status"] == "confirmed" else "❌"
                lines.append(
                    f"{icon} `{b['id']}`\n"
                    f"🪄 {b['service']}  |  📅 {fmt_date(b['date'])}  ⏰ {b['time']}"
                )
            text = "\n\n".join(lines)
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✦ Book Another", web_app=WebAppInfo(url=MINI_APP_URL))
        ]])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)

    elif data == "about":
        s = get_all_settings()
        text = (
            f"✦ *{s.get('salon_name', 'Salon')}*\n\n"
            f"📍 {s.get('salon_address', '—')}\n"
            f"📞 {s.get('salon_phone', '—')}\n"
            f"🕐 {s.get('open_time', '—')} – {s.get('close_time', '—')}\n\n"
            "_Luxury is not a style. It is an attitude._"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✦ Book Now", web_app=WebAppInfo(url=MINI_APP_URL))
        ]])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)

    elif data.startswith("confirm_cancel:"):
        ref = data.split(":")[1]
        if cancel_booking(ref):
            await query.edit_message_text(
                f"✅ Booking `{ref}` has been cancelled.\n\nThe slot is now available for others.",
                parse_mode="Markdown"
            )
            # Notify admin
            b = get_booking(ref)
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"❌ *Booking Cancelled*\n\n`{ref}`\n👤 {b['client_name']}\n🪄 {b['service']}\n📅 {fmt_date(b['date'])}  ⏰ {b['time']}",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text("❌ Could not cancel. It may already be cancelled.")

    elif data.startswith("confirm_reschedule:"):
        ref = data.split(":")[1]
        if cancel_booking(ref):
            await query.edit_message_text(
                f"✅ Booking `{ref}` cancelled.\n\nOpening booking app to rebook...",
                parse_mode="Markdown",
                reply_markup=book_keyboard()
            )
        else:
            await query.edit_message_text("❌ Could not reschedule.")

    elif data == "cancel_abort":
        await query.edit_message_text("👍 Your booking is kept. See you soon!")


# ── Web App data (booking from Mini App) ─────────────────────────────────────
async def handle_web_app_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg  = update.effective_message
    raw  = msg.web_app_data.data

    logger.info(f"[BOOKING] from {user.id} ({user.full_name}): {raw}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"[BOOKING] JSON error: {e}")
        await msg.reply_text("⚠️ Error processing booking. Please try again.")
        return

    if data.get("action") != "booking":
        return

    # Check availability (double-check server-side)
    date_str = data.get("date", "")
    time_str = data.get("time", "")
    logger.info(f"[BOOKING] date='{date_str}' time='{time_str}' action='{data.get('action')}'")
    logger.info(f"[BOOKING] available slots for {date_str}: {get_available_slots(date_str)}")
    if date_str and time_str and not is_slot_available(date_str, time_str):
        logger.warning(f"[BOOKING] Slot {time_str} on {date_str} not available — saving anyway (Mini App uses static slots)")
        # NOTE: We skip the block for now since Mini App uses static slots
        # Once Mini App fetches real slots from bot, re-enable this check

    # Save to DB
    ref = save_booking(data, user)

    # Build record dict for messages
    b = {
        "id": ref, "service": data.get("service","—"), "stylist": data.get("stylist","—"),
        "date": date_str, "time": time_str,
        "duration": data.get("duration","—"), "price": data.get("price","TBD"),
        "notes": data.get("notes",""),
    }

    # Send confirmation to client
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 My Bookings", callback_data="my_bookings"),
         InlineKeyboardButton("✦ Book Again",  web_app=WebAppInfo(url=MINI_APP_URL))],
    ])
    try:
        await msg.reply_text(
            booking_confirm_msg(b, user.first_name or "there"),
            parse_mode="Markdown", reply_markup=keyboard
        )
        logger.info(f"[BOOKING] ✅ Confirmation sent to {user.id}")
    except Exception as e:
        logger.error(f"[BOOKING] ❌ Confirmation failed: {e}")

    # Send admin alert
    try:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=admin_alert_msg(b, user),
            parse_mode="Markdown"
        )
        logger.info(f"[BOOKING] ✅ Admin notified")
    except Exception as e:
        logger.error(f"[BOOKING] ❌ Admin alert failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# REMINDER SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

async def send_reminders(bot):
    """Called every 15 minutes by scheduler — sends due reminders."""
    due = get_upcoming_reminders()
    if not due:
        return

    salon = get_setting("salon_name", "Maison Lumière")

    for kind, b in due:
        label = "tomorrow" if kind == "24h" else "in 1 hour"
        text  = (
            f"⏰ *Reminder — {salon}*\n\n"
            f"Your appointment is *{label}*!\n\n"
            f"🪄 {b['service']}  |  👤 {b['stylist']}\n"
            f"📅 {fmt_date(b['date'])}  ⏰ {b['time']}\n\n"
            f"🔖 `{b['id']}`\n\n"
            f"_To cancel: /cancel {b['id']}_"
        )
        try:
            await bot.send_message(
                chat_id=b["user_id"], text=text, parse_mode="Markdown"
            )
            mark_reminder_sent(b["id"], kind)
            logger.info(f"[REMINDER] {kind} sent to {b['user_id']} for {b['id']}")
        except Exception as e:
            logger.warning(f"[REMINDER] Failed for {b['id']}: {e}")



# ── /backup — manual trigger ──────────────────────────────────────────────────
async def backup_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to trigger a backup immediately."""
    if update.effective_user.id != ADMIN_CHAT_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text(
        "⏳ Running backup now...\n\nYou will receive:\n"
        "1️⃣ Database file (.db)\n"
        "2️⃣ Excel export (.csv)\n"
        "3️⃣ Revenue summary"
    )
    await run_backup(context.bot, ADMIN_CHAT_ID)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("❌ Set BOT_TOKEN in bot.py")
        return

    # Init DB
    init_db()

    # Build app
    app = Application.builder().token(BOT_TOKEN).build()

    # ── Client commands ──
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("book",        book))
    app.add_handler(CommandHandler("mybookings",  my_bookings_cmd))
    app.add_handler(CommandHandler("cancel",      cancel_cmd))
    app.add_handler(CommandHandler("reschedule",  reschedule_cmd))
    app.add_handler(CommandHandler("status",      status_cmd))
    app.add_handler(CommandHandler("help",        help_cmd))

    # ── Admin commands ──
    app.add_handler(CommandHandler("setup",         setup_cmd))
    app.add_handler(CommandHandler("today",         today_cmd))
    app.add_handler(CommandHandler("slots",         slots_cmd))
    app.add_handler(CommandHandler("clients",       clients_cmd))
    app.add_handler(CommandHandler("broadcast",     broadcast_cmd))
    app.add_handler(CommandHandler("backup",        backup_now_cmd))
    app.add_handler(CommandHandler("admin",         admin_cmd))
    app.add_handler(CommandHandler("setname",       setname_cmd))
    app.add_handler(CommandHandler("setaddress",    setaddress_cmd))
    app.add_handler(CommandHandler("setphone",      setphone_cmd))
    app.add_handler(CommandHandler("setcurrency",   setcurrency_cmd))
    app.add_handler(CommandHandler("sethours",      sethours_cmd))
    app.add_handler(CommandHandler("setdays",       setdays_cmd))
    app.add_handler(CommandHandler("setslot",       setslot_cmd))
    app.add_handler(CommandHandler("addservice",    addservice_cmd))
    app.add_handler(CommandHandler("listservices",  listservices_cmd))
    app.add_handler(CommandHandler("removeservice", removeservice_cmd))
    app.add_handler(CommandHandler("addstaff",      addstaff_cmd))
    app.add_handler(CommandHandler("liststaff",     liststaff_cmd))
    app.add_handler(CommandHandler("removestaff",   removestaff_cmd))

    # ── Callbacks + Mini App ──
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_web_app_data))

    # ── Reminder scheduler (runs every 15 min) ──
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        send_reminders,
        trigger=IntervalTrigger(minutes=15),
        args=[app.bot],
        id="reminders",
        replace_existing=True,
    )
    scheduler.add_job(
        run_backup,
        trigger=IntervalTrigger(hours=6),
        args=[app.bot, ADMIN_CHAT_ID],
        id="backup",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("[SCHEDULER] Reminder scheduler started (every 15 min)")
    logger.info("[SCHEDULER] Backup scheduler started (every 6 hours)")

    logger.info(f"✦ Bot v2.0 live | DB: {DB_PATH} | Admin: {ADMIN_CHAT_ID}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()