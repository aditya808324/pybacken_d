"""
bot.py — Maison Lumière Salon Booking Bot v3.1 (Sheets-Only)
=============================================================
Changes vs v3.0:
✅ All booking reads/writes go through Google Sheets
✅ SQLite (db.py) only used for settings, staff, services
✅ No more data loss on Railway redeploy
✅ /admin, /mybookings, /today, reminders all read from Sheets
"""

import json
import logging
import os
from datetime import datetime, date

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

# ── Our modules ───────────────────────────────────────────────────────────────
from db import (
    init_db, get_db,
    get_setting, set_setting, get_all_settings,
    get_available_slots, get_booked_slots, is_slot_available,
    get_staff, get_services,
    fmt_price, fmt_date,
)
from sheets import (
    save_booking_to_sheets,
    cancel_booking_in_sheets,
    update_booking_status,
    mark_reminder_sent_sheets,
    get_all_bookings,
    get_booking,
    get_user_bookings,
    get_todays_bookings,
    get_upcoming_reminders,
    get_all_clients,
    get_all_user_ids,
    push_backup_to_sheet,
    push_all_revenue,
    sheets_health_check,
)
from backup import run_backup

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN     = os.getenv("BOT_TOKEN")
MINI_APP_URL  = os.getenv("MINI_APP_URL", "https://telebot-eight-peach.vercel.app")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

if not BOT_TOKEN:
    raise EnvironmentError("BOT_TOKEN environment variable is not set!")
if ADMIN_CHAT_ID == 0:
    raise EnvironmentError("ADMIN_CHAT_ID environment variable is not set!")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def booking_confirm_msg(b: dict, client_name: str) -> str:
    notes = f"\n📝 *Notes:* {b['notes']}" if b.get("notes") else ""
    return (
        f"✅ *You're all set, {client_name}!*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"We're looking forward to seeing you ✨\n\n"
        f"🪄 *Service:* {b['service']}\n"
        f"👤 *Stylist:* {b['stylist']}\n"
        f"📅 *Date:* {fmt_date(b['date'])}\n"
        f"⏰ *Time:* {b['time']}\n"
        f"⌛ *Duration:* {b['duration']}\n"
        f"💰 *Total:* {fmt_price(b['price'])}"
        f"{notes}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 *Booking ID:* `{b['id']}`\n\n"
        f"_Need to cancel? Just tap /cancel {b['id']}_"
    )


def admin_alert_msg(b: dict, user) -> str:
    salon = get_setting("salon_name", "Salon")
    notes = f"\n📝 Notes: {b['notes']}" if b.get("notes") else ""
    uname = f" (@{user.username})" if user.username else ""
    return (
        f"🔔 *NEW BOOKING — {b['id']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 *Client:* [{user.full_name}](tg://user?id={user.id}){uname}\n"
        f"🆔 *User ID:* `{user.id}`\n\n"
        f"🪄 *Service:* {b['service']}\n"
        f"👩‍🎨 *Stylist:* {b['stylist']}\n"
        f"📅 *Date:* {fmt_date(b['date'])}\n"
        f"⏰ *Time:* {b['time']}\n"
        f"⌛ *Duration:* {b['duration']}\n"
        f"💰 *Total:* {fmt_price(b['price'])}"
        f"{notes}\n\n"
        f"🔖 *Booking ID:* `{b['id']}`"
    )


# ══════════════════════════════════════════════════════════════════════════════
# KEYBOARD HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def book_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✦ Book an Appointment", web_app=WebAppInfo(url=MINI_APP_URL))
    ]])


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN DECORATOR
# ══════════════════════════════════════════════════════════════════════════════

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_CHAT_ID:
            await update.message.reply_text("⛔ Admin only.")
            return
        await func(update, context)
    return wrapper


# ══════════════════════════════════════════════════════════════════════════════
# CLIENT COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    salon = get_setting("salon_name", "Maison Lumière")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✦ Book an Appointment", web_app=WebAppInfo(url=MINI_APP_URL))],
        [
            InlineKeyboardButton("📋 My Bookings", callback_data="my_bookings"),
            InlineKeyboardButton("ℹ️ About", callback_data="about"),
        ],
    ])
    await update.message.reply_text(
        f"✦ *Welcome to {salon}, {user.first_name}!*\n\n"
        "Your destination for luxury beauty experiences.\n\n"
        "Tap *Book an Appointment* to get started.",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✦ *Book your appointment*\n\nTap below to open the booking experience.",
        parse_mode="Markdown",
        reply_markup=book_keyboard(),
    )


async def my_bookings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    rows = get_user_bookings(uid)   # ← reads from Sheets

    if not rows:
        await update.message.reply_text(
            "You have no bookings yet.\n\nUse /book to reserve your experience. ✦"
        )
        return

    lines = [f"✦ *Your Bookings ({len(rows)} total)*\n"]
    for b in rows:
        icon = "✅" if b["status"] == "confirmed" else "❌"
        lines.append(
            f"{icon} `{b['id']}`\n"
            f"🪄 {b['service']} | 👤 {b['stylist']}\n"
            f"📅 {fmt_date(b['date'])} ⏰ {b['time']}\n"
            f"💰 {fmt_price(b['price'])} | _{b['status'].title()}_"
        )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✦ Book Another", web_app=WebAppInfo(url=MINI_APP_URL))
    ]])
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=keyboard
    )


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args

    if not args:
        await update.message.reply_text(
            "Usage: `/cancel ML-XXXXXX`\n\nUse /mybookings to see your booking IDs.",
            parse_mode="Markdown",
        )
        return

    ref = args[0].upper()
    b = get_booking(ref)   # ← reads from Sheets

    if not b:
        await update.message.reply_text(f"❌ Booking `{ref}` not found.", parse_mode="Markdown")
        return

    if b["user_id"] != uid and uid != ADMIN_CHAT_ID:
        await update.message.reply_text("⛔ You can only cancel your own bookings.")
        return

    if b["status"] == "cancelled":
        await update.message.reply_text(
            f"ℹ️ Booking `{ref}` is already cancelled.", parse_mode="Markdown"
        )
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, cancel it", callback_data=f"confirm_cancel:{ref}"),
        InlineKeyboardButton("❌ Keep it", callback_data="cancel_abort"),
    ]])
    await update.message.reply_text(
        f"Are you sure you want to cancel this booking?\n\n"
        f"🔖 `{ref}`\n"
        f"🪄 {b['service']} | 👤 {b['stylist']}\n"
        f"📅 {fmt_date(b['date'])} ⏰ {b['time']}",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def reschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: `/reschedule ML-XXXXXX`\n\n"
            "This will cancel your current booking and open the app to rebook.",
            parse_mode="Markdown",
        )
        return

    ref = args[0].upper()
    b = get_booking(ref)   # ← Sheets

    if not b or b["user_id"] != update.effective_user.id:
        await update.message.reply_text("❌ Booking not found or not yours.")
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, reschedule", callback_data=f"confirm_reschedule:{ref}"),
        InlineKeyboardButton("❌ Keep it", callback_data="cancel_abort"),
    ]])
    await update.message.reply_text(
        f"Rescheduling will cancel `{ref}` and open the booking app.\n\n"
        f"🪄 {b['service']} | 📅 {fmt_date(b['date'])} ⏰ {b['time']}\n\n"
        f"Proceed?",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    s = get_all_settings()
    is_admin = uid == ADMIN_CHAT_ID

    # Count from Sheets
    all_bookings = get_all_bookings()
    total_bookings = len(all_bookings)
    confirmed = sum(1 for b in all_bookings if b["status"] == "confirmed")

    sheets_ok = sheets_health_check()

    lines = [
        "🔧 *Bot Status*\n",
        "✅ Bot running",
        f"{'✅' if MINI_APP_URL.startswith('https') else '❌'} Mini App: `{MINI_APP_URL}`",
        f"{'✅' if is_admin else '⚠️'} Your ID `{uid}` {'= Admin' if is_admin else '≠ Admin'}",
        f"{'✅' if sheets_ok else '⚠️'} Google Sheets: {'Connected' if sheets_ok else 'Unavailable'}",
        f"\n🏪 *{s.get('salon_name', '—')}*",
        f"📍 {s.get('salon_address', '—')}",
        f"📞 {s.get('salon_phone', '—')}",
        f"🕐 {s.get('open_time', '—')} – {s.get('close_time', '—')}",
        f"\n📊 Total bookings: *{total_bookings}* ({confirmed} confirmed)",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


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


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@admin_only
async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚙️ *Admin Setup Commands*\n\n"
        "*Salon Info:*\n"
        "/setname Salon Name\n"
        "/setaddress Your Address\n"
        "/setphone +91 98765 43210\n"
        "/setcurrency ₹\n\n"
        "*Hours:*\n"
        "/sethours 09:00 18:00\n"
        "/setdays 1,2,3,4,5 _(0=Mon, 6=Sun)_\n"
        "/setslot 60 _(slot duration in minutes)_\n\n"
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
        "/broadcast Message — Message all clients\n"
        "/backup — Trigger backup now",
        parse_mode="Markdown",
    )


@admin_only
async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bookings = get_todays_bookings()   # ← Sheets
    today_str = date.today().strftime("%A, %B %d, %Y")

    if not bookings:
        await update.message.reply_text(
            f"📅 *{today_str}*\n\nNo appointments today. Enjoy the break! 🎉",
            parse_mode="Markdown",
        )
        return

    lines = [f"📅 *Today's Schedule — {today_str}*\n_{len(bookings)} appointments_\n"]
    total = 0
    for b in bookings:
        lines.append(
            f"⏰ *{b['time']}* — {b['service']}\n"
            f" 👤 {b['client_name']} | 💰 {fmt_price(b['price'])}\n"
            f" 🔖 `{b['id']}`"
        )
        try:
            total += int(str(b["price"]).replace(",", ""))
        except (ValueError, TypeError):
            pass

    cur = get_setting("currency", "₹")
    if total:
        lines.append(f"\n💰 *Expected Revenue: {cur}{total:,}*")

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


@admin_only
async def slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_str = context.args[0] if context.args else date.today().isoformat()
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

    avail_str  = " ".join(available) if available else "_None available_"
    booked_str = " ".join(booked)    if booked    else "_None_"

    await update.message.reply_text(
        f"📅 *{fmt_date(date_str)}*\n\n"
        f"✅ *Available:*\n{avail_str}\n\n"
        f"🔒 *Booked:*\n{booked_str}",
        parse_mode="Markdown",
    )


@admin_only
async def clients_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_all_clients()   # ← Sheets

    if not rows:
        await update.message.reply_text("No clients yet.")
        return

    lines = [f"👥 *Top Clients ({len(rows)} shown)*\n"]
    for c in rows:
        uname = f" @{c['Username']}" if c.get("Username") else ""
        visit = int(c.get("Visit Count", 0))
        vip   = " 🌟 VIP" if c.get("VIP") == "YES" else ""
        last  = str(c.get("Last Seen", ""))[:10]
        lines.append(
            f"👤 *{c['Name']}*{uname}{vip}\n"
            f" 🗓 {visit} visits | Last: {last}"
        )

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


@admin_only
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text(
            "Usage: `/broadcast Your message here`\n\nSends to all past clients.",
            parse_mode="Markdown",
        )
        return

    client_ids = get_all_user_ids()   # ← Sheets
    salon = get_setting("salon_name", "Maison Lumière")
    sent, failed = 0, 0

    for uid in client_ids:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 *{salon}*\n\n{msg}",
                parse_mode="Markdown",
                reply_markup=book_keyboard(),
            )
            sent += 1
        except Exception as e:
            logger.warning(f"[BROADCAST] Failed for {uid}: {e}")
            failed += 1

    await update.message.reply_text(
        f"📢 Broadcast complete!\n\n✅ Sent: {sent}\n❌ Failed: {failed}"
    )


@admin_only
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_all_bookings()   # ← Sheets
    confirmed = sum(1 for b in rows if b["status"] == "confirmed")

    if not rows:
        await update.message.reply_text("No bookings yet.")
        return

    lines = [f"✦ *All Bookings — {confirmed} confirmed*\n"]
    for b in rows[:10]:   # show latest 10
        icon = "✅" if b["status"] == "confirmed" else "❌"
        lines.append(
            f"{icon} `{b['id']}` — {b['service']}\n"
            f"👤 {b['client_name']} | 🎨 {b['stylist']}\n"
            f"📅 {fmt_date(b['date'])} ⏰ {b['time']} 💰 {fmt_price(b['price'])}"
        )

    await update.message.reply_text("\n\n".join(lines), parse_mode="Markdown")


@admin_only
async def backup_now_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⏳ Running backup now...\n\nYou will receive:\n"
        "1️⃣ Excel export (.csv)\n"
        "2️⃣ Revenue summary\n"
        "3️⃣ Google Sheets sync (Backup + Revenue tabs)"
    )
    await run_backup(context.bot, ADMIN_CHAT_ID)


# ── Salon info setters ────────────────────────────────────────────────────────

@admin_only
async def setname_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args)
    if not val:
        await update.message.reply_text("Usage: /setname Salon Name"); return
    set_setting("salon_name", val)
    await update.message.reply_text(f"✅ Salon name: *{val}*", parse_mode="Markdown")

@admin_only
async def setaddress_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args)
    if not val:
        await update.message.reply_text("Usage: /setaddress Your Address"); return
    set_setting("salon_address", val)
    await update.message.reply_text(f"✅ Address: *{val}*", parse_mode="Markdown")

@admin_only
async def setphone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args)
    if not val:
        await update.message.reply_text("Usage: /setphone +91 98765 43210"); return
    set_setting("salon_phone", val)
    await update.message.reply_text(f"✅ Phone: *{val}*", parse_mode="Markdown")

@admin_only
async def setcurrency_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = " ".join(context.args).strip()
    if not val:
        await update.message.reply_text("Usage: /setcurrency ₹"); return
    set_setting("currency", val)
    await update.message.reply_text(f"✅ Currency: *{val}*", parse_mode="Markdown")

@admin_only
async def sethours_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /sethours 09:00 18:00"); return
    open_t, close_t = context.args
    set_setting("open_time", open_t)
    set_setting("close_time", close_t)
    await update.message.reply_text(
        f"✅ Hours: *{open_t}* – *{close_t}*", parse_mode="Markdown"
    )

@admin_only
async def setdays_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: /setdays 1,2,3,4,5\n_(0=Mon … 6=Sun)_", parse_mode="Markdown"
        ); return
    val = context.args[0]
    set_setting("working_days", val)
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    days_str = ", ".join(day_names[int(d)] for d in val.split(","))
    await update.message.reply_text(f"✅ Working days: *{days_str}*", parse_mode="Markdown")

@admin_only
async def setslot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setslot 60"); return
    set_setting("slot_duration", context.args[0])
    await update.message.reply_text(
        f"✅ Slot duration: *{context.args[0]} min*", parse_mode="Markdown"
    )

# ── Service management ────────────────────────────────────────────────────────

@admin_only
async def addservice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = " ".join(context.args).split("|")
    if len(parts) != 3:
        await update.message.reply_text(
            "Usage: `/addservice Haircut|45|850`\n_(Name|Duration mins|Price)_",
            parse_mode="Markdown",
        ); return
    name, dur, price = [p.strip() for p in parts]
    with get_db() as db:
        db.execute(
            "INSERT INTO services (name, duration, price) VALUES (?, ?, ?)",
            (name, int(dur), int(price)),
        )
    cur = get_setting("currency", "₹")
    await update.message.reply_text(
        f"✅ Service added: *{name}* | {dur} min | {cur}{int(price):,}",
        parse_mode="Markdown",
    )

@admin_only
async def listservices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    services = get_services()
    if not services:
        await update.message.reply_text("No services. Use /addservice."); return
    cur   = get_setting("currency", "₹")
    lines = ["🛎 *Services*\n"]
    for s in services:
        lines.append(f"ID `{s['id']}` — *{s['name']}* | {s['duration']} min | {cur}{s['price']:,}")
    await update.message.reply_text(
        "\n".join(lines) + "\n\nTo remove: `/removeservice ID`",
        parse_mode="Markdown",
    )

@admin_only
async def removeservice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /removeservice 3"); return
    with get_db() as db:
        db.execute("UPDATE services SET active=0 WHERE id=?", (int(context.args[0]),))
    await update.message.reply_text(f"✅ Service ID {context.args[0]} removed.")

# ── Staff management ──────────────────────────────────────────────────────────

@admin_only
async def addstaff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = " ".join(context.args).split("|")
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
        await update.message.reply_text("No staff. Use /addstaff."); return
    lines = ["👩‍🎨 *Staff Members*\n"]
    for s in staff:
        lines.append(f"ID `{s['id']}` — *{s['name']}* | _{s['title']}_")
    await update.message.reply_text(
        "\n".join(lines) + "\n\nTo remove: `/removestaff ID`",
        parse_mode="Markdown",
    )

@admin_only
async def removestaff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /removestaff 2"); return
    with get_db() as db:
        db.execute("UPDATE staff SET active=0 WHERE id=?", (int(context.args[0]),))
    await update.message.reply_text(f"✅ Staff ID {context.args[0]} removed.")


# ══════════════════════════════════════════════════════════════════════════════
# CALLBACK QUERY HANDLER
# ══════════════════════════════════════════════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = update.effective_user.id
    data = query.data

    if data == "my_bookings":
        rows = get_user_bookings(uid)   # ← Sheets
        if not rows:
            text = "No bookings yet. Use /book to reserve your experience. ✦"
        else:
            lines = ["✦ *Your Bookings*\n"]
            for b in rows:
                icon = "✅" if b["status"] == "confirmed" else "❌"
                lines.append(
                    f"{icon} `{b['id']}`\n"
                    f"🪄 {b['service']} | 📅 {fmt_date(b['date'])} ⏰ {b['time']}"
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
        if cancel_booking_in_sheets(ref):   # ← Sheets
            b = get_booking(ref)
            await query.edit_message_text(
                f"✅ Booking `{ref}` has been cancelled.\n\n"
                f"The slot is now available for others.",
                parse_mode="Markdown",
            )
            if b:
                await context.bot.send_message(
                    chat_id=ADMIN_CHAT_ID,
                    text=(
                        f"❌ *Booking Cancelled*\n\n"
                        f"`{ref}`\n"
                        f"👤 {b['client_name']}\n"
                        f"🪄 {b['service']}\n"
                        f"📅 {fmt_date(b['date'])} ⏰ {b['time']}"
                    ),
                    parse_mode="Markdown",
                )
        else:
            await query.edit_message_text("❌ Could not cancel. It may already be cancelled.")

    elif data.startswith("confirm_reschedule:"):
        ref = data.split(":")[1]
        if cancel_booking_in_sheets(ref):   # ← Sheets
            update_booking_status(ref, "rescheduled")
            await query.edit_message_text(
                f"✅ Booking `{ref}` cancelled.\n\nOpening booking app to rebook...",
                parse_mode="Markdown",
                reply_markup=book_keyboard(),
            )
        else:
            await query.edit_message_text("❌ Could not reschedule.")

    elif data == "cancel_abort":
        await query.edit_message_text("👍 Your booking is kept. See you soon!")


# ══════════════════════════════════════════════════════════════════════════════
# WEB APP DATA HANDLER (Mini App → Bot booking submission)
# ══════════════════════════════════════════════════════════════════════════════

async def handle_web_app_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg  = update.effective_message
    raw  = msg.web_app_data.data

    logger.info(f"[BOOKING] Incoming from {user.id} ({user.full_name}): {raw}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"[BOOKING] JSON parse error: {e}")
        await msg.reply_text("⚠️ Error processing booking. Please try again.")
        return

    if data.get("action") != "booking":
        return

    # ── Save directly to Google Sheets ───────────────────────────────
    try:
        ref = save_booking_to_sheets(data, user)
        logger.info(f"[BOOKING] Saved to Sheets: {ref}")
    except Exception as e:
        logger.error(f"[BOOKING] Failed to save to Sheets: {e}")
        await msg.reply_text(
            "⚠️ Could not save your booking right now. Please try again in a moment."
        )
        return

    b = {
        "id":       ref,
        "service":  data.get("service", "—"),
        "stylist":  data.get("stylist", "—"),
        "date":     data.get("date", ""),
        "time":     data.get("time", ""),
        "duration": data.get("duration", "—"),
        "price":    data.get("price", "TBD"),
        "notes":    data.get("notes", ""),
    }

    # ── Send confirmation to client ───────────────────────────────────
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 My Bookings", callback_data="my_bookings"),
            InlineKeyboardButton("✦ Book Again", web_app=WebAppInfo(url=MINI_APP_URL)),
        ],
    ])
    try:
        await msg.reply_text(
            booking_confirm_msg(b, user.first_name or "there"),
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        logger.info(f"[BOOKING] ✅ Confirmation sent to {user.id}")
    except Exception as e:
        logger.error(f"[BOOKING] ❌ Confirmation send failed: {e}")

    # ── Send admin alert ──────────────────────────────────────────────
    try:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=admin_alert_msg(b, user),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"[BOOKING] ❌ Admin alert failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# REMINDER SCHEDULER JOB
# ══════════════════════════════════════════════════════════════════════════════

async def send_reminders(bot):
    due = get_upcoming_reminders()   # ← Sheets
    if not due:
        return

    salon = get_setting("salon_name", "Maison Lumière")

    for kind, b in due:
        if kind == "24h":
            label = "tomorrow"
            intro = "Just a friendly reminder — your appointment is coming up!"
            emoji = "🌟"
        else:
            label = "in about 1 hour"
            intro = "Almost time! We're getting ready for your visit."
            emoji = "✨"

        text = (
            f"{emoji} *Reminder — {salon}*\n\n"
            f"{intro}\n\n"
            f"Your appointment is *{label}*:\n\n"
            f"🪄 {b['service']} | 👤 {b['stylist']}\n"
            f"📅 {fmt_date(b['date'])} ⏰ {b['time']}\n\n"
            f"🔖 `{b['id']}`\n\n"
            f"_Need to cancel? /cancel {b['id']}_\n"
            f"_See you soon! 💇_"
        )

        try:
            await bot.send_message(
                chat_id=b["user_id"], text=text, parse_mode="Markdown"
            )
            mark_reminder_sent_sheets(b["id"], kind)   # ← Sheets
            logger.info(f"[REMINDER] {kind} sent to {b['user_id']} for {b['id']}")
        except Exception as e:
            logger.warning(f"[REMINDER] Failed for {b['id']}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # ── Initialise database (settings/staff/services only) ───────────
    init_db()

    # ── Build Telegram application ────────────────────────────────────
    app = Application.builder().token(BOT_TOKEN).build()

    # ── Client commands ───────────────────────────────────────────────
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("book",        book))
    app.add_handler(CommandHandler("mybookings",  my_bookings_cmd))
    app.add_handler(CommandHandler("cancel",      cancel_cmd))
    app.add_handler(CommandHandler("reschedule",  reschedule_cmd))
    app.add_handler(CommandHandler("status",      status_cmd))
    app.add_handler(CommandHandler("help",        help_cmd))

    # ── Admin commands ────────────────────────────────────────────────
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

    # ── Callbacks + Mini App data ─────────────────────────────────────
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_web_app_data))

    # ── Schedulers ────────────────────────────────────────────────────
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
    logger.info("[SCHEDULER] Reminder job started (every 15 min)")
    logger.info("[SCHEDULER] Backup job started (every 6 hours)")

    logger.info(f"✦ Salon Bot v3.1 live | Admin: {ADMIN_CHAT_ID}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
