"""
backup.py — Salon Booking Bot Backup System (v3 — CRM Upgrade)
===============================================================
Handles all backup logic:
  - Every 6 hours automatically (via APScheduler in bot.py)
  - On-demand via /backup admin command

Delivers:
  1. salon_YYYYMMDD_HHMM.db     → saved locally in backups/
  2. salon_export_YYYYMMDD.csv  → saved locally in backups/
  3. .db file      → sent to admin Telegram
  4. .csv file     → sent to admin Telegram
  5. Revenue summary → sent to admin Telegram

  NEW (Google Sheets):
  6. Booking data  → pushed to "Backup" sheet (overwrite)
  7. Revenue rows  → pushed to "Revenue" sheet (full refresh)

Local backups kept: last 30 files per type (configurable via BACKUP_KEEP env var)
"""

import csv
import glob
import logging
import os
import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, date

# ── Import from our new modules ───────────────────────────────────────────────
# db.py for data access, sheets.py for Google Sheets push
from db import (
    get_db,
    get_setting,
    get_all_settings,
    get_all_bookings_for_export,
    get_all_clients_for_export,
    fmt_price,
)
from sheets import push_backup_to_sheet, push_all_revenue

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH     = os.getenv("DB_PATH",    "salon.db")
BACKUP_DIR  = os.getenv("BACKUP_DIR", "backups")
BACKUP_KEEP = int(os.getenv("BACKUP_KEEP", "30"))


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_dir():
    os.makedirs(BACKUP_DIR, exist_ok=True)


def _purge_old(pattern: str):
    """Remove oldest backup files beyond BACKUP_KEEP limit."""
    files = sorted(glob.glob(os.path.join(BACKUP_DIR, pattern)))
    for old in files[:-BACKUP_KEEP]:
        try:
            os.remove(old)
            logger.info(f"[BACKUP] Purged old file: {old}")
        except Exception as e:
            logger.warning(f"[BACKUP] Could not purge {old}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Save .db file locally
# ══════════════════════════════════════════════════════════════════════════════

def save_db_locally() -> str:
    """
    Copy salon.db → backups/salon_YYYYMMDD_HHMM.db
    Returns the path of the saved file.
    """
    _ensure_dir()
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    dest = os.path.join(BACKUP_DIR, f"salon_{ts}.db")
    shutil.copy2(DB_PATH, dest)
    _purge_old("salon_*.db")
    logger.info(f"[BACKUP] DB saved locally: {dest}")
    return dest


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Export all data to CSV locally
# ══════════════════════════════════════════════════════════════════════════════

def save_csv_locally() -> tuple:
    """
    Export bookings, clients, services, staff, and settings to a single CSV.
    Returns (filepath, stats_dict).
    """
    _ensure_dir()
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    path = os.path.join(BACKUP_DIR, f"salon_export_{ts}.csv")

    bookings = get_all_bookings_for_export()
    clients  = get_all_clients_for_export()

    with get_db() as db:
        services = db.execute("SELECT * FROM services").fetchall()
        staff    = db.execute("SELECT * FROM staff").fetchall()
        settings = db.execute("SELECT * FROM settings ORDER BY key").fetchall()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:   # utf-8-sig for Excel compat
        w = csv.writer(f)

        # ── BOOKINGS ──────────────────────────────────────────────────────
        w.writerow(["BOOKINGS"])
        w.writerow([
            "Booking ID", "Client Name", "Telegram ID", "Username",
            "Service", "Stylist", "Date", "Time", "Duration",
            "Price", "Status", "Notes",
            "24h Reminder Sent", "1h Reminder Sent", "Booked At",
        ])
        for b in bookings:
            w.writerow([
                b["id"], b["client_name"], b["user_id"], b["username"] or "",
                b["service"], b["stylist"], b["date"], b["time"], b["duration"],
                b["price"], b["status"], b["notes"] or "",
                "Yes" if b["reminder_24h_sent"] else "No",
                "Yes" if b["reminder_1h_sent"]  else "No",
                b["created_at"],
            ])

        w.writerow([])

        # ── CLIENTS ───────────────────────────────────────────────────────
        w.writerow(["CLIENTS"])
        w.writerow([
            "Telegram ID", "Name", "Username",
            "Total Visits", "Notes", "First Seen", "Last Seen",
        ])
        for c in clients:
            w.writerow([
                c["user_id"], c["name"], c["username"] or "",
                c["visit_count"], c["notes"] or "",
                c["first_seen"], c["last_seen"],
            ])

        w.writerow([])

        # ── SERVICES ──────────────────────────────────────────────────────
        w.writerow(["SERVICES"])
        w.writerow(["ID", "Name", "Duration (min)", "Price", "Active", "Created At"])
        for s in services:
            w.writerow([
                s["id"], s["name"], s["duration"],
                s["price"], "Yes" if s["active"] else "No", s["created_at"],
            ])

        w.writerow([])

        # ── STAFF ─────────────────────────────────────────────────────────
        w.writerow(["STAFF"])
        w.writerow(["ID", "Name", "Title", "Active", "Created At"])
        for s in staff:
            w.writerow([
                s["id"], s["name"], s["title"],
                "Yes" if s["active"] else "No", s["created_at"],
            ])

        w.writerow([])

        # ── SETTINGS ──────────────────────────────────────────────────────
        w.writerow(["SETTINGS"])
        w.writerow(["Key", "Value"])
        for s in settings:
            w.writerow([s["key"], s["value"]])

    _purge_old("salon_export_*.csv")
    logger.info(f"[BACKUP] CSV saved locally: {path}")
    return path, {"bookings": len(bookings), "clients": len(clients)}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Build revenue summary (SQLite → Python dict)
# ══════════════════════════════════════════════════════════════════════════════

def get_revenue_summary() -> dict:
    """Calculate revenue stats for today / this week / this month / all-time."""
    today    = date.today().isoformat()
    wk_start = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    mo_start = date.today().replace(day=1).isoformat()

    def _rev(where, param):
        with get_db() as db:
            row = db.execute(
                f"SELECT COALESCE(SUM(CAST(price AS INTEGER)),0) as t "
                f"FROM bookings WHERE status='confirmed' AND CAST(price AS INTEGER)>0 AND {where}",
                (param,),
            ).fetchone()
            return row["t"] if row else 0

    def _cnt(where, param):
        with get_db() as db:
            row = db.execute(
                f"SELECT COUNT(*) as c FROM bookings WHERE status='confirmed' AND {where}",
                (param,),
            ).fetchone()
            return row["c"] if row else 0

    with get_db() as db:
        total_rev   = db.execute(
            "SELECT COALESCE(SUM(CAST(price AS INTEGER)),0) as t "
            "FROM bookings WHERE status='confirmed' AND CAST(price AS INTEGER)>0"
        ).fetchone()["t"]
        total_count = db.execute(
            "SELECT COUNT(*) as c FROM bookings WHERE status='confirmed'"
        ).fetchone()["c"]
        cancelled   = db.execute(
            "SELECT COUNT(*) as c FROM bookings WHERE status='cancelled'"
        ).fetchone()["c"]
        clients     = db.execute("SELECT COUNT(*) as c FROM clients").fetchone()["c"]
        top_svc     = db.execute(
            "SELECT service, COUNT(*) as c FROM bookings WHERE status='confirmed' "
            "GROUP BY service ORDER BY c DESC LIMIT 1"
        ).fetchone()
        top_sty     = db.execute(
            "SELECT stylist, COUNT(*) as c FROM bookings WHERE status='confirmed' "
            "GROUP BY stylist ORDER BY c DESC LIMIT 1"
        ).fetchone()

    cur = get_setting("currency", "₹")
    return {
        "cur":         cur,
        "today_rev":   _rev("date=?", today),
        "today_count": _cnt("date=?", today),
        "week_rev":    _rev("date>=?", wk_start),
        "week_count":  _cnt("date>=?", wk_start),
        "month_rev":   _rev("date>=?", mo_start),
        "month_count": _cnt("date>=?", mo_start),
        "total_rev":   total_rev,
        "total_count": total_count,
        "cancelled":   cancelled,
        "clients":     clients,
        "top_service": top_svc["service"] if top_svc else "—",
        "top_stylist": top_sty["stylist"]  if top_sty else "—",
    }


def _get_all_daily_revenue() -> list:
    """
    Return list of (date_str, booking_count, total_revenue) for every date
    that has confirmed bookings. Used for Google Sheets Revenue sheet refresh.
    """
    with get_db() as db:
        rows = db.execute(
            """
            SELECT date,
                   COUNT(*) as bookings,
                   COALESCE(SUM(CAST(price AS INTEGER)),0) as revenue
            FROM bookings
            WHERE status='confirmed' AND CAST(price AS INTEGER)>0
            GROUP BY date
            ORDER BY date
            """
        ).fetchall()
    return [(r["date"], r["bookings"], r["revenue"]) for r in rows]


def build_summary_message(r: dict, salon: str, ts: str, backup_path: str) -> str:
    """Format the Telegram revenue summary message."""
    return (
        f"📈 *{salon} — Business Summary*\n"
        f"🕐 Backup at `{ts}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*Today*\n"
        f"  📅 {r['today_count']} bookings  |  💰 {r['cur']}{r['today_rev']:,}\n\n"
        f"*This Week*\n"
        f"  📅 {r['week_count']} bookings  |  💰 {r['cur']}{r['week_rev']:,}\n\n"
        f"*This Month*\n"
        f"  📅 {r['month_count']} bookings  |  💰 {r['cur']}{r['month_rev']:,}\n\n"
        f"*All Time*\n"
        f"  📅 {r['total_count']} confirmed  |  ❌ {r['cancelled']} cancelled\n"
        f"  💰 {r['cur']}{r['total_rev']:,} total revenue\n"
        f"  👥 {r['clients']} clients\n\n"
        f"🌟 Top Service: *{r['top_service']}*\n"
        f"👩‍🎨 Top Stylist: *{r['top_stylist']}*\n\n"
        f"_Local backups saved to:_\n`{os.path.abspath(backup_path)}`"
    )


# ══════════════════════════════════════════════════════════════════════════════
# MAIN BACKUP JOB
# ══════════════════════════════════════════════════════════════════════════════

async def run_backup(bot, admin_chat_id: int):
    """
    Full backup routine — called by APScheduler every 6 hours
    and by the /backup admin command.

    Steps:
      1. Save .db locally
      2. Save .csv locally
      3. Send .db  → Telegram admin
      4. Send .csv → Telegram admin
      5. Send revenue summary → Telegram admin
      6. [NEW] Push bookings  → Google Sheets "Backup" tab
      7. [NEW] Push revenue   → Google Sheets "Revenue" tab
    """
    salon = get_setting("salon_name", "Salon")
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.info(f"[BACKUP] Starting full backup — {ts}")

    # ── 1 & 2: Save locally ───────────────────────────────────────────────
    try:
        db_path          = save_db_locally()
        csv_path, stats  = save_csv_locally()
    except Exception as e:
        logger.error(f"[BACKUP] Local save failed: {e}")
        await bot.send_message(
            chat_id=admin_chat_id,
            text=f"⚠️ *Backup Failed — Local Save*\n\n`{e}`",
            parse_mode="Markdown",
        )
        return

    # ── 3: Send .db to Telegram ───────────────────────────────────────────
    try:
        with open(db_path, "rb") as f:
            await bot.send_document(
                chat_id=admin_chat_id,
                document=f,
                filename=os.path.basename(db_path),
                caption=(
                    f"🗄️ *{salon} — Database Backup*\n"
                    f"`{ts}`\n\n"
                    f"Full SQLite database.\n"
                    f"Open with DB Browser for SQLite to inspect all data."
                ),
                parse_mode="Markdown",
            )
        logger.info("[BACKUP] .db sent to Telegram")
    except Exception as e:
        logger.error(f"[BACKUP] .db Telegram send failed: {e}")

    # ── 4: Send .csv to Telegram ──────────────────────────────────────────
    try:
        with open(csv_path, "rb") as f:
            await bot.send_document(
                chat_id=admin_chat_id,
                document=f,
                filename=os.path.basename(csv_path),
                caption=(
                    f"📊 *{salon} — Data Export (CSV)*\n"
                    f"`{ts}`\n\n"
                    f"📋 {stats['bookings']} bookings\n"
                    f"👥 {stats['clients']} clients\n\n"
                    f"Open with Excel or Google Sheets."
                ),
                parse_mode="Markdown",
            )
        logger.info("[BACKUP] CSV sent to Telegram")
    except Exception as e:
        logger.error(f"[BACKUP] CSV Telegram send failed: {e}")

    # ── 5: Send revenue summary ───────────────────────────────────────────
    try:
        r       = get_revenue_summary()
        summary = build_summary_message(r, salon, ts, db_path)
        await bot.send_message(
            chat_id=admin_chat_id,
            text=summary,
            parse_mode="Markdown",
        )
        logger.info("[BACKUP] Revenue summary sent to Telegram")
    except Exception as e:
        logger.error(f"[BACKUP] Revenue summary Telegram send failed: {e}")

    # ── 6: [NEW] Push booking data → Google Sheets "Backup" tab ──────────
    try:
        bookings = get_all_bookings_for_export()
        push_backup_to_sheet(bookings, stats)   # non-fatal if sheets unavailable
    except Exception as e:
        logger.error(f"[BACKUP] Google Sheets backup push failed: {e}")

    # ── 7: [NEW] Push revenue data → Google Sheets "Revenue" tab ─────────
    try:
        daily_revenue = _get_all_daily_revenue()
        push_all_revenue(daily_revenue)          # non-fatal if sheets unavailable
    except Exception as e:
        logger.error(f"[BACKUP] Google Sheets revenue push failed: {e}")

    logger.info(f"[BACKUP] ✅ Complete — files in {os.path.abspath(BACKUP_DIR)}/")
