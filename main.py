#!/usr/bin/env python3
"""
Professional Production-Ready Telegram Bot V2
Single-file implementation using python-telegram-bot + SQLite.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from dotenv import load_dotenv
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
    User,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8875160341:AAHsTsiqG8xl8liUalnQqAoZ1qWhFsc7PoQ").strip()
OWNER_ID = int(os.getenv("OWNER_ID") or "8209524871")
DB_PATH = os.getenv("DB_PATH", "bot_data.db").strip() or "bot_data.db"
PAGE_SIZE = 10
AUDIT_PAGE = 10
MAX_CAPTION = 1024
MAX_TITLE = 80
MAX_TEXT = 4000

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bot")

db_lock = threading.RLock()

# In-memory UI state (DB persists data; this is only conversation steps)
USER_STATE: Dict[int, Dict[str, Any]] = {}

REQUIRED_TABLES = {
    "users",
    "statistics",
    "settings",
    "admin_message_map",
    "message_history",
    "admins",
    "content_items",
    "content_views",
    "content_likes",
    "admin_audit_log",
    "outgoing_messages",
    "referrals",
    "referral_points",
    "user_content_access",
    "categories",
    "premium_subscriptions",
    "notification_preferences",
    "auto_delete_jobs",
    "broadcasts",
    "broadcast_messages",
    "pending_ops",
}

DEFAULT_CATEGORIES = ["Movies", "Videos", "Photos", "Documents", "Education", "Other"]

DEFAULT_SETTINGS = {
    "welcome_type": "text",
    "welcome_text": "👋 स्वागत है!\n\nयह एक Admin-controlled content bot है। नीचे मेनू से सामग्री देखें या Admin को संदेश भेजें।",
    "welcome_file_id": "",
    "welcome_caption": "",
    "free_access_mode": "unlimited",  # This build gives every user unlimited content access
    "free_total_limit": "10",
    "free_daily_limit": "5",
    "referral_reward_type": "items",  # items | premium_days
    "referral_reward_value": "5",
    "maintenance": "0",
    "timer_seconds": "0",
    "content_timer_seconds": "0",
    "hosting_expiry_ts": "0",
    "hosting_reminded": "",
}

STAT_KEYS = (
    "messages_received",
    "replies_sent",
    "broadcasts",
    "messages_sent",
    "failed_messages",
)


# ---------------------------------------------------------------------------
# Time / text helpers
# ---------------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().strftime("%Y-%m-%d %H:%M:%S")


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s[:19], fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v), quote=True)


def clip(text: Optional[str], n: int = 200) -> str:
    t = (text or "").strip()
    return t if len(t) <= n else t[: n - 1] + "…"


def auto_title(text: Optional[str], caption: Optional[str], fallback: str = "Untitled") -> str:
    src = (caption or text or "").strip().replace("\n", " ")
    if not src:
        return fallback
    return src[:MAX_TITLE]


def parse_duration_arg(raw: str) -> Optional[int]:
    s = (raw or "").strip().lower()
    if s in ("off", "0", "disable", "none"):
        return 0
    m = re.fullmatch(r"(\d+)\s*([smhd])?", s)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2) or "s"
    return n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def fmt_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "—"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s and not h:
        parts.append(f"{s}s")
    return " ".join(parts) or "0s"


def fmt_ts(ts: Optional[str]) -> str:
    return ts or "—"


def display_name(user: Optional[User] = None, row: Optional[sqlite3.Row] = None) -> str:
    if user:
        return user.full_name or user.first_name or str(user.id)
    if row:
        return row["first_name"] or str(row["telegram_id"])
    return "User"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


@contextmanager
def db() -> Iterable[sqlite3.Connection]:
    with db_lock:
        con = _connect()
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


def execute(sql: str, params: Sequence[Any] = ()) -> None:
    with db() as con:
        con.execute(sql, params)


def fetchone(sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
    with db() as con:
        return con.execute(sql, params).fetchone()


def fetchall(sql: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
    with db() as con:
        return list(con.execute(sql, params).fetchall())


def fetchval(sql: str, params: Sequence[Any] = (), default: Any = None) -> Any:
    row = fetchone(sql, params)
    if not row:
        return default
    return row[0]


def init_db() -> None:
    with db() as con:
        c = con.cursor()
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                join_time TEXT,
                last_seen TEXT,
                status TEXT DEFAULT 'active',
                referred_by INTEGER,
                extra_items INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS statistics (
                key TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS admins (
                telegram_id INTEGER PRIMARY KEY,
                added_by INTEGER,
                added_at TEXT,
                role TEXT DEFAULT 'admin'
            );
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                position INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS content_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                file_id TEXT,
                text TEXT,
                caption TEXT,
                title TEXT,
                category_id INTEGER,
                thumbnail TEXT,
                duration INTEGER,
                created_at TEXT,
                enabled INTEGER DEFAULT 1,
                position INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS content_views (
                content_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                view_count INTEGER DEFAULT 0,
                first_view_time TEXT,
                last_view_time TEXT,
                PRIMARY KEY (content_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS content_likes (
                content_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                created_at TEXT,
                PRIMARY KEY (content_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS user_content_access (
                user_id INTEGER NOT NULL,
                content_id INTEGER NOT NULL,
                first_access_at TEXT,
                PRIMARY KEY (user_id, content_id)
            );
            CREATE TABLE IF NOT EXISTS premium_subscriptions (
                user_id INTEGER PRIMARY KEY,
                is_lifetime INTEGER DEFAULT 0,
                expiry TEXT,
                activated_by INTEGER,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER NOT NULL,
                referred_id INTEGER UNIQUE NOT NULL,
                created_at TEXT,
                reward_given INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS referral_points (
                user_id INTEGER PRIMARY KEY,
                points INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS notification_preferences (
                user_id INTEGER PRIMARY KEY,
                new_content INTEGER DEFAULT 1,
                broadcast INTEGER DEFAULT 1,
                referral INTEGER DEFAULT 1,
                premium INTEGER DEFAULT 1,
                expiry INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS message_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_id INTEGER,
                to_id INTEGER,
                direction TEXT,
                content_type TEXT,
                file_id TEXT,
                text TEXT,
                telegram_message_id INTEGER,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS admin_message_map (
                admin_chat_id INTEGER NOT NULL,
                admin_message_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_message_id INTEGER,
                PRIMARY KEY (admin_chat_id, admin_message_id)
            );
            CREATE TABLE IF NOT EXISTS outgoing_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                kind TEXT,
                broadcast_id TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS auto_delete_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                delete_at REAL NOT NULL,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS broadcasts (
                id TEXT PRIMARY KEY,
                admin_id INTEGER,
                created_at TEXT,
                total INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                status TEXT
            );
            CREATE TABLE IF NOT EXISTS broadcast_messages (
                broadcast_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admin_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                details TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS pending_ops (
                user_id INTEGER PRIMARY KEY,
                op TEXT,
                data TEXT
            );
            """
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_enabled_pos ON content_items(enabled, position, id)"
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_content_cat ON content_items(category_id, enabled)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_views_content ON content_views(content_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_likes_content ON content_likes(content_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_access_user ON user_content_access(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_jobs_when ON auto_delete_jobs(delete_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_audit_time ON admin_audit_log(created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bcast_msgs ON broadcast_messages(broadcast_id)")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_search ON content_items(title, category_id)"
        )

        for k, v in DEFAULT_SETTINGS.items():
            c.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v))
        for k in STAT_KEYS:
            c.execute("INSERT OR IGNORE INTO statistics(key, value) VALUES(?, 0)", (k,))

        cat_count = c.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        if cat_count == 0:
            for i, name in enumerate(DEFAULT_CATEGORIES):
                c.execute(
                    "INSERT INTO categories(name, position) VALUES(?, ?)",
                    (name, i),
                )

        if OWNER_ID:
            c.execute(
                """
                INSERT INTO admins(telegram_id, added_by, added_at, role)
                VALUES(?, ?, ?, 'owner')
                ON CONFLICT(telegram_id) DO UPDATE SET role='owner'
                """,
                (OWNER_ID, OWNER_ID, now_iso()),
            )


def get_setting(key: str, default: str = "") -> str:
    row = fetchone("SELECT value FROM settings WHERE key=?", (key,))
    if row is None:
        return default
    return "" if row["value"] is None else str(row["value"])


def set_setting(key: str, value: Any) -> None:
    execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def bump_stat(key: str, n: int = 1) -> None:
    execute(
        "INSERT INTO statistics(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=value+?",
        (key, n, n),
    )


def get_stat(key: str) -> int:
    return int(fetchval("SELECT value FROM statistics WHERE key=?", (key,), 0) or 0)


def audit(admin_id: int, action: str, details: str = "") -> None:
    execute(
        "INSERT INTO admin_audit_log(admin_id, action, details, created_at) VALUES(?,?,?,?)",
        (admin_id, action, clip(details, 500), now_iso()),
    )


# ---------------------------------------------------------------------------
# Users / admins / premium
# ---------------------------------------------------------------------------
def upsert_user(user: User) -> None:
    now = now_iso()
    with db() as con:
        exists = con.execute(
            "SELECT telegram_id FROM users WHERE telegram_id=?", (user.id,)
        ).fetchone()
        if exists:
            con.execute(
                "UPDATE users SET first_name=?, username=?, last_seen=? WHERE telegram_id=?",
                (user.first_name or "", user.username or "", now, user.id),
            )
        else:
            con.execute(
                """
                INSERT INTO users(telegram_id, first_name, username, join_time, last_seen, status, extra_items)
                VALUES(?,?,?,?,?,'active',0)
                """,
                (user.id, user.first_name or "", user.username or "", now, now),
            )
        con.execute(
            "INSERT OR IGNORE INTO notification_preferences(user_id) VALUES(?)",
            (user.id,),
        )
        con.execute(
            "INSERT OR IGNORE INTO referral_points(user_id, points) VALUES(?,0)",
            (user.id,),
        )


def get_user(uid: int) -> Optional[sqlite3.Row]:
    return fetchone("SELECT * FROM users WHERE telegram_id=?", (uid,))


def mark_blocked(uid: int) -> None:
    execute("UPDATE users SET status='blocked' WHERE telegram_id=?", (uid,))


def set_user_status(uid: int, status: str) -> None:
    execute("UPDATE users SET status=? WHERE telegram_id=?", (status, uid))


def is_owner(uid: int) -> bool:
    return bool(OWNER_ID) and uid == OWNER_ID


def is_admin(uid: int) -> bool:
    if is_owner(uid):
        return True
    row = fetchone("SELECT telegram_id FROM admins WHERE telegram_id=?", (uid,))
    return row is not None


def admin_role(uid: int) -> str:
    if is_owner(uid):
        return "owner"
    row = fetchone("SELECT role FROM admins WHERE telegram_id=?", (uid,))
    return row["role"] if row else ""


def maintenance_on() -> bool:
    return get_setting("maintenance", "0") == "1"


def premium_row(uid: int) -> Optional[sqlite3.Row]:
    return fetchone("SELECT * FROM premium_subscriptions WHERE user_id=?", (uid,))


def is_premium(uid: int) -> bool:
    row = premium_row(uid)
    if not row:
        return False
    if int(row["is_lifetime"] or 0) == 1:
        return True
    exp = parse_iso(row["expiry"])
    return bool(exp and exp > utcnow())


def premium_label(uid: int) -> Tuple[str, str]:
    row = premium_row(uid)
    if not row:
        return "Free User", "—"
    if int(row["is_lifetime"] or 0) == 1:
        return "Premium (Lifetime)", "Lifetime"
    exp = parse_iso(row["expiry"])
    if exp and exp > utcnow():
        return "Premium User", row["expiry"]
    return "Free User", (row["expiry"] or "Expired")


def grant_premium(uid: int, days: Optional[int], lifetime: bool, by_admin: int) -> None:
    now = utcnow()
    row = premium_row(uid)
    if lifetime:
        execute(
            """
            INSERT INTO premium_subscriptions(user_id, is_lifetime, expiry, activated_by, updated_at)
            VALUES(?,1,NULL,?,?)
            ON CONFLICT(user_id) DO UPDATE SET is_lifetime=1, expiry=NULL, activated_by=?, updated_at=?
            """,
            (uid, by_admin, now_iso(), by_admin, now_iso()),
        )
        return
    days = int(days or 0)
    start = now
    if row and parse_iso(row["expiry"]) and parse_iso(row["expiry"]) > now:
        start = parse_iso(row["expiry"])  # type: ignore
    expiry = (start + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    execute(
        """
        INSERT INTO premium_subscriptions(user_id, is_lifetime, expiry, activated_by, updated_at)
        VALUES(?,0,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET is_lifetime=0, expiry=?, activated_by=?, updated_at=?
        """,
        (uid, expiry, by_admin, now_iso(), expiry, by_admin, now_iso()),
    )


def revoke_premium(uid: int) -> None:
    execute("DELETE FROM premium_subscriptions WHERE user_id=?", (uid,))


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------
def unique_access_count(uid: int) -> int:
    return int(fetchval("SELECT COUNT(*) FROM user_content_access WHERE user_id=?", (uid,), 0) or 0)


def unique_access_today(uid: int) -> int:
    today = utcnow().strftime("%Y-%m-%d")
    return int(
        fetchval(
            "SELECT COUNT(*) FROM user_content_access WHERE user_id=? AND substr(first_access_at,1,10)=?",
            (uid, today),
            0,
        )
        or 0
    )


def extra_items(uid: int) -> int:
    return int(fetchval("SELECT extra_items FROM users WHERE telegram_id=?", (uid,), 0) or 0)


def has_accessed(uid: int, cid: int) -> bool:
    return fetchone(
        "SELECT 1 FROM user_content_access WHERE user_id=? AND content_id=?",
        (uid, cid),
    ) is not None


def record_access(uid: int, cid: int) -> None:
    execute(
        "INSERT OR IGNORE INTO user_content_access(user_id, content_id, first_access_at) VALUES(?,?,?)",
        (uid, cid, now_iso()),
    )


def check_content_access(uid: int, cid: int) -> Tuple[bool, str]:
    """All users have unlimited content access in this build.

    Referrals are tracked only as a visible statistic and never act as an
    access gate. Re-opening content remains allowed as well.
    """
    if get_content(cid) is None:
        return False, "Content not found."
    if is_admin(uid):
        return True, "admin"
    if is_premium(uid):
        return True, "premium"
    return True, "unlimited"


def access_summary(uid: int) -> str:
    if is_admin(uid):
        return "Unlimited (Admin)"
    if is_premium(uid):
        return "Unlimited (Premium)"
    return "Unlimited (Free User)"


# ---------------------------------------------------------------------------
# Content / likes / views
# ---------------------------------------------------------------------------
def get_content(cid: int) -> Optional[sqlite3.Row]:
    return fetchone("SELECT * FROM content_items WHERE id=?", (cid,))


def content_like_count(cid: int) -> int:
    return int(fetchval("SELECT COUNT(*) FROM content_likes WHERE content_id=?", (cid,), 0) or 0)


def content_view_total(cid: int) -> int:
    return int(
        fetchval("SELECT COALESCE(SUM(view_count),0) FROM content_views WHERE content_id=?", (cid,), 0)
        or 0
    )


def content_unique_viewers(cid: int) -> int:
    return int(fetchval("SELECT COUNT(*) FROM content_views WHERE content_id=?", (cid,), 0) or 0)


def user_liked(uid: int, cid: int) -> bool:
    return fetchone(
        "SELECT 1 FROM content_likes WHERE content_id=? AND user_id=?",
        (cid, uid),
    ) is not None


def toggle_like(uid: int, cid: int) -> bool:
    if user_liked(uid, cid):
        execute("DELETE FROM content_likes WHERE content_id=? AND user_id=?", (cid, uid))
        return False
    execute(
        "INSERT OR IGNORE INTO content_likes(content_id, user_id, created_at) VALUES(?,?,?)",
        (cid, uid, now_iso()),
    )
    return True


def record_view(uid: int, cid: int) -> None:
    now = now_iso()
    with db() as con:
        row = con.execute(
            "SELECT view_count FROM content_views WHERE content_id=? AND user_id=?",
            (cid, uid),
        ).fetchone()
        if row:
            con.execute(
                "UPDATE content_views SET view_count=view_count+1, last_view_time=? WHERE content_id=? AND user_id=?",
                (now, cid, uid),
            )
        else:
            con.execute(
                """
                INSERT INTO content_views(content_id, user_id, view_count, first_view_time, last_view_time)
                VALUES(?,?,1,?,?)
                """,
                (cid, uid, now, now),
            )


def category_name(cid: Optional[int]) -> str:
    if not cid:
        return "Other"
    row = fetchone("SELECT name FROM categories WHERE id=?", (cid,))
    return row["name"] if row else "Other"


def enabled_content_query(extra_where: str = "", params: Sequence[Any] = ()) -> Tuple[str, Tuple]:
    where = "WHERE enabled=1 " + extra_where
    return f"FROM content_items {where}", tuple(params)


def paginate(total: int, page: int, size: int = PAGE_SIZE) -> Tuple[int, int, int]:
    pages = max(1, (total + size - 1) // size)
    page = max(0, min(page, pages - 1))
    return page, pages, page * size


# ---------------------------------------------------------------------------
# State / keyboards
# ---------------------------------------------------------------------------
def st_get(uid: int) -> Dict[str, Any]:
    return USER_STATE.setdefault(uid, {"op": None, "data": {}})


def st_set(uid: int, op: Optional[str], **data: Any) -> None:
    USER_STATE[uid] = {"op": op, "data": data}
    execute(
        "INSERT INTO pending_ops(user_id, op, data) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET op=?, data=?",
        (uid, op or "", json.dumps(data), op or "", json.dumps(data)),
    )


def st_clear(uid: int) -> None:
    USER_STATE.pop(uid, None)
    execute("DELETE FROM pending_ops WHERE user_id=?", (uid,))


def restore_states() -> None:
    for row in fetchall("SELECT user_id, op, data FROM pending_ops"):
        try:
            USER_STATE[row["user_id"]] = {
                "op": row["op"] or None,
                "data": json.loads(row["data"] or "{}"),
            }
        except Exception:
            continue


def ik(rows: List[List[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(rows)


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=data)


def nav_row(back: str = "u:home", home: bool = True, cancel: bool = True) -> List[InlineKeyboardButton]:
    row = [btn("⬅️ Back", back)]
    if home:
        row.append(btn("🏠 Home", "u:home"))
    if cancel:
        row.append(btn("❌ Cancel", "u:cancel"))
    return row


def user_menu_kb() -> InlineKeyboardMarkup:
    return ik(
        [
            [btn("📚 सामग्री", "u:lib:0"), btn("🔍 Search", "u:search")],
            [btn("❤️ My Likes", "u:likes:0"), btn("👤 My Profile", "u:profile")],
            [btn("🔗 Refer & Earn", "u:ref"), btn("📩 Admin को भेजें", "u:msgadm")],
            [btn("ℹ️ Bot Info", "u:info"), btn("🆘 Help", "u:help")],
            [btn("🔔 Notifications", "u:notif")],
        ]
    )


def admin_panel_kb() -> InlineKeyboardMarkup:
    return ik(
        [
            [btn("📊 Dashboard", "a:dash"), btn("📈 Analytics", "a:anal")],
            [btn("👥 Users", "a:users:0"), btn("📚 Content", "a:cont")],
            [btn("📢 Broadcast", "a:bcast"), btn("🎁 Welcome", "a:welcome")],
            [btn("🔗 Referrals", "a:refs"), btn("💎 Premium", "a:prem")],
            [btn("⚙️ Access Settings", "a:access"), btn("🔔 Notifications", "a:anotif")],
            [btn("🔧 Maintenance", "a:maint"), btn("👑 Admins", "a:admins")],
            [btn("💾 Backup", "a:backup"), btn("📝 Audit Log", "a:audit:0")],
            [btn("🆘 Help", "a:help"), btn("🏠 User Home", "u:home")],
        ]
    )


def like_kb(cid: int, liked: bool, extra: Optional[List[List[InlineKeyboardButton]]] = None) -> InlineKeyboardMarkup:
    label = "💔 Unlike" if liked else "❤️ Like"
    rows = [[btn(label, f"u:like:{cid}:d"), btn("🏠 Home", "u:home")]]
    if extra:
        rows = extra + rows
    return ik(rows)


# ---------------------------------------------------------------------------
# Safe Telegram I/O
# ---------------------------------------------------------------------------
async def safe_answer(q: CallbackQuery, text: str = "", alert: bool = False) -> None:
    try:
        if text:
            await q.answer(text[:180], show_alert=alert)
        else:
            await q.answer()
    except BadRequest:
        pass
    except TelegramError:
        pass


async def safe_delete_message(bot, chat_id: int, message_id: int) -> bool:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except (BadRequest, Forbidden, TelegramError):
        return False


def remember_outgoing(chat_id: int, message_id: int, kind: str, broadcast_id: Optional[str] = None) -> None:
    execute(
        "INSERT INTO outgoing_messages(chat_id, message_id, kind, broadcast_id, created_at) VALUES(?,?,?,?,?)",
        (chat_id, message_id, kind, broadcast_id, now_iso()),
    )
    bump_stat("messages_sent", 1)


def schedule_autodel(application: Application, chat_id: int, message_id: int, seconds: int) -> None:
    if seconds <= 0:
        return
    delete_at = time.time() + seconds
    execute(
        "INSERT INTO auto_delete_jobs(chat_id, message_id, delete_at, created_at) VALUES(?,?,?,?)",
        (chat_id, message_id, delete_at, now_iso()),
    )
    jq = application.job_queue
    if jq:
        jq.run_once(job_delete_message, when=seconds, data={"chat_id": chat_id, "message_id": message_id})


async def job_delete_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if chat_id is None or message_id is None:
        return
    await safe_delete_message(context.bot, chat_id, message_id)
    execute(
        "DELETE FROM auto_delete_jobs WHERE chat_id=? AND message_id=?",
        (chat_id, message_id),
    )


async def restore_autodel_jobs(application: Application) -> None:
    now = time.time()
    rows = fetchall("SELECT id, chat_id, message_id, delete_at FROM auto_delete_jobs")
    jq = application.job_queue
    for row in rows:
        delay = max(0, float(row["delete_at"]) - now)
        if delay <= 0:
            await safe_delete_message(application.bot, row["chat_id"], row["message_id"])
            execute("DELETE FROM auto_delete_jobs WHERE id=?", (row["id"],))
        elif jq:
            jq.run_once(
                job_delete_message,
                when=delay,
                data={"chat_id": row["chat_id"], "message_id": row["message_id"]},
            )


async def handle_forbidden(uid: int) -> None:
    mark_blocked(uid)
    log.info("Marked user %s blocked (Forbidden)", uid)


async def send_copy_or_forward(bot, chat_id: int, src: Message) -> Optional[Message]:
    try:
        return await bot.copy_message(chat_id=chat_id, from_chat_id=src.chat_id, message_id=src.message_id)
    except Forbidden:
        await handle_forbidden(chat_id)
        return None
    except BadRequest:
        try:
            return await src.forward(chat_id)
        except Forbidden:
            await handle_forbidden(chat_id)
            return None
        except TelegramError as e:
            log.warning("forward failed: %s", e)
            return None
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            return await bot.copy_message(chat_id=chat_id, from_chat_id=src.chat_id, message_id=src.message_id)
        except TelegramError:
            return None
    except TelegramError as e:
        log.warning("copy failed: %s", e)
        return None


async def deliver_content_item(bot, chat_id: int, item: sqlite3.Row, reply_markup=None) -> Optional[Message]:
    t = item["type"]
    caption = clip(item["caption"] or item["text"] or item["title"] or "", MAX_CAPTION)
    file_id = item["file_id"]
    try:
        if t == "text":
            return await bot.send_message(
                chat_id, item["text"] or item["caption"] or item["title"] or "", reply_markup=reply_markup
            )
        if t == "photo":
            return await bot.send_photo(chat_id, file_id, caption=caption or None, reply_markup=reply_markup)
        if t == "video":
            return await bot.send_video(
                chat_id,
                file_id,
                caption=caption or None,
                duration=item["duration"] or None,
                thumbnail=item["thumbnail"] or None,
                reply_markup=reply_markup,
            )
        if t == "document":
            return await bot.send_document(chat_id, file_id, caption=caption or None, reply_markup=reply_markup)
        if t == "audio":
            return await bot.send_audio(
                chat_id, file_id, caption=caption or None, duration=item["duration"] or None, reply_markup=reply_markup
            )
        if t == "voice":
            return await bot.send_voice(
                chat_id, file_id, caption=caption or None, duration=item["duration"] or None, reply_markup=reply_markup
            )
        if t == "animation":
            return await bot.send_animation(chat_id, file_id, caption=caption or None, reply_markup=reply_markup)
        if t == "video_note":
            msg = await bot.send_video_note(chat_id, file_id, duration=item["duration"] or None)
            if reply_markup:
                try:
                    await bot.send_message(chat_id, "⬆️ Content", reply_markup=reply_markup)
                except TelegramError:
                    pass
            return msg
        if t == "sticker":
            msg = await bot.send_sticker(chat_id, file_id)
            if reply_markup:
                try:
                    await bot.send_message(chat_id, item["title"] or "Sticker", reply_markup=reply_markup)
                except TelegramError:
                    pass
            return msg
        return await bot.send_message(chat_id, caption or "Content", reply_markup=reply_markup)
    except Forbidden:
        await handle_forbidden(chat_id)
        return None
    except RetryAfter as e:
        await asyncio.sleep(min(e.retry_after, 30) + 0.5)
        try:
            return await bot.send_message(chat_id, caption or "Content")
        except TelegramError:
            return None
    except TelegramError as e:
        log.warning("deliver content %s failed: %s", item["id"], e)
        return None


async def safe_show(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    markup: Optional[InlineKeyboardMarkup] = None,
    query: Optional[CallbackQuery] = None,
) -> Optional[Message]:
    q = query or (update.callback_query if update else None)
    if q and q.message:
        try:
            return await q.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except BadRequest as e:
            msg = str(e).lower()
            if "not modified" in msg:
                return q.message
            try:
                await q.edit_message_caption(caption=text[:1024], reply_markup=markup, parse_mode=ParseMode.HTML)
                return q.message
            except BadRequest:
                pass
        except TelegramError:
            pass
        try:
            return await context.bot.send_message(
                q.message.chat_id, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
        except Forbidden:
            await handle_forbidden(q.from_user.id)
            return None
        except TelegramError:
            return None
    chat = update.effective_chat if update else None
    if not chat:
        return None
    try:
        return await context.bot.send_message(
            chat.id, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
    except Forbidden:
        await handle_forbidden(chat.id)
        return None
    except TelegramError as e:
        log.warning("send failed: %s", e)
        return None


def detect_message_payload(msg: Message) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "type": "text",
        "file_id": None,
        "text": msg.text or msg.caption or "",
        "caption": msg.caption or "",
        "duration": None,
        "thumbnail": None,
    }
    if msg.photo:
        data["type"] = "photo"
        data["file_id"] = msg.photo[-1].file_id
    elif msg.video:
        data["type"] = "video"
        data["file_id"] = msg.video.file_id
        data["duration"] = msg.video.duration
        if msg.video.thumbnail:
            data["thumbnail"] = msg.video.thumbnail.file_id
    elif msg.document:
        data["type"] = "document"
        data["file_id"] = msg.document.file_id
        if msg.document.thumbnail:
            data["thumbnail"] = msg.document.thumbnail.file_id
    elif msg.audio:
        data["type"] = "audio"
        data["file_id"] = msg.audio.file_id
        data["duration"] = msg.audio.duration
    elif msg.voice:
        data["type"] = "voice"
        data["file_id"] = msg.voice.file_id
        data["duration"] = msg.voice.duration
    elif msg.animation:
        data["type"] = "animation"
        data["file_id"] = msg.animation.file_id
        data["duration"] = msg.animation.duration
        if msg.animation.thumbnail:
            data["thumbnail"] = msg.animation.thumbnail.file_id
    elif msg.video_note:
        data["type"] = "video_note"
        data["file_id"] = msg.video_note.file_id
        data["duration"] = msg.video_note.duration
    elif msg.sticker:
        data["type"] = "sticker"
        data["file_id"] = msg.sticker.file_id
    elif msg.text:
        data["type"] = "text"
        data["text"] = msg.text
    else:
        data["type"] = "text"
        data["text"] = msg.caption or ""
    return data


# ---------------------------------------------------------------------------
# Referrals
# ---------------------------------------------------------------------------
def apply_referral(new_user: User, referrer_id: int, bot_username: str = "") -> None:
    uid = new_user.id
    if referrer_id == uid:
        return
    if not get_user(referrer_id) and not is_admin(referrer_id):
        # referrer must exist
        if not fetchone("SELECT telegram_id FROM users WHERE telegram_id=?", (referrer_id,)):
            return
    row = get_user(uid)
    if not row:
        return
    if row["referred_by"]:
        return
    existing = fetchone("SELECT id FROM referrals WHERE referred_id=?", (uid,))
    if existing:
        return
    execute("UPDATE users SET referred_by=? WHERE telegram_id=? AND referred_by IS NULL", (referrer_id, uid))
    execute(
        "INSERT OR IGNORE INTO referrals(referrer_id, referred_id, created_at, reward_given) VALUES(?,?,?,1)",
        (referrer_id, uid, now_iso()),
    )
    # Referral is statistics-only in this build. It does not restrict or unlock content.
    execute(
        """
        INSERT INTO referral_points(user_id, points) VALUES(?,1)
        ON CONFLICT(user_id) DO UPDATE SET points=points+1
        """,
        (referrer_id,),
    )


def referral_stats(uid: int) -> Dict[str, int]:
    successful = int(fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,), 0) or 0)
    points = int(fetchval("SELECT points FROM referral_points WHERE user_id=?", (uid,), 0) or 0)
    used = unique_access_count(uid)
    return {"successful": successful, "points": points, "used": used, "unlocked": -1, "extra": 0}


# ---------------------------------------------------------------------------
# Welcome / start
# ---------------------------------------------------------------------------
async def send_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    wtype = get_setting("welcome_type", "text")
    text = get_setting("welcome_text", DEFAULT_SETTINGS["welcome_text"])
    file_id = get_setting("welcome_file_id", "")
    caption = get_setting("welcome_caption", "") or text
    markup = user_menu_kb()
    extra = ""
    if is_admin(update.effective_user.id):
        extra_kb = [list(markup.inline_keyboard[0])]
        # rebuild with admin button
        rows = [list(r) for r in markup.inline_keyboard]
        rows.append([btn("👑 Admin Panel", "a:panel")])
        markup = ik(rows)
        extra = "\n\n<i>Admin access detected.</i>"
    try:
        if wtype == "photo" and file_id:
            await context.bot.send_photo(chat_id, file_id, caption=clip(caption, MAX_CAPTION) + extra.replace("<i>", "").replace("</i>", ""), reply_markup=markup)
        elif wtype == "video" and file_id:
            await context.bot.send_video(chat_id, file_id, caption=clip(caption, MAX_CAPTION), reply_markup=markup)
        else:
            await context.bot.send_message(chat_id, text + extra, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Forbidden:
        await handle_forbidden(chat_id)
    except TelegramError:
        try:
            await context.bot.send_message(chat_id, text, reply_markup=user_menu_kb())
        except TelegramError:
            pass


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    is_new = fetchone("SELECT telegram_id FROM users WHERE telegram_id=?", (user.id,)) is None
    upsert_user(user)
    if maintenance_on() and not is_admin(user.id):
        await update.message.reply_text("🔧 Bot अभी Maintenance Mode में है।")
        return
    args = context.args or []
    if args:
        payload = args[0]
        m = re.match(r"(?:ref_)?(\d+)$", payload)
        if m:
            try:
                ref_id = int(m.group(1))
                if is_new:
                    apply_referral(user, ref_id)
                    pref = fetchone(
                        "SELECT referral FROM notification_preferences WHERE user_id=?",
                        (ref_id,),
                    )
                    if pref and pref["referral"]:
                        try:
                            await context.bot.send_message(
                                ref_id,
                                f"🎉 New successful referral: {esc(user.first_name)}",
                                parse_mode=ParseMode.HTML,
                            )
                        except Forbidden:
                            await handle_forbidden(ref_id)
                        except TelegramError:
                            pass
            except ValueError:
                pass
    st_clear(user.id)
    await send_welcome(update, context)


def gate_user(fn: Callable) -> Callable:
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *a, **k):
        user = update.effective_user
        if not user:
            return
        upsert_user(user)
        row = get_user(user.id)
        if row and row["status"] == "blocked" and not is_admin(user.id):
            if update.callback_query:
                await safe_answer(update.callback_query, "You are blocked.", True)
            elif update.message:
                await update.message.reply_text("⛔ आपका access block है।")
            return
        if maintenance_on() and not is_admin(user.id):
            if update.callback_query:
                await safe_answer(update.callback_query, "Maintenance", True)
                await safe_show(update, context, "🔧 Bot अभी Maintenance Mode में है।")
            elif update.message:
                await update.message.reply_text("🔧 Bot अभी Maintenance Mode में है।")
            return
        return await fn(update, context, *a, **k)

    return wrapped


# ---------------------------------------------------------------------------
# User screens
# ---------------------------------------------------------------------------
def content_list_text(title: str, page: int, pages: int, total: int) -> str:
    return f"<b>{esc(title)}</b>\n\nPage {page + 1}/{pages} • Total {total}\nSelect an item for preview."


def content_item_button(row: sqlite3.Row) -> InlineKeyboardButton:
    views = content_view_total(row["id"])
    likes = content_like_count(row["id"])
    name = clip(row["title"] or f"#{row['id']}", 28)
    return btn(f"{name} • 👁{views} ❤️{likes}", f"u:prev:{row['id']}")


def pager_row(prefix: str, page: int, pages: int) -> List[InlineKeyboardButton]:
    row = []
    if page > 0:
        row.append(btn("⬅️ Previous", f"{prefix}{page - 1}"))
    if page + 1 < pages:
        row.append(btn("➡️ Next", f"{prefix}{page + 1}"))
    return row


def range_label(page: int, total: int) -> str:
    start = page * PAGE_SIZE + 1
    end = min((page + 1) * PAGE_SIZE, total)
    return f"{start}–{end}" if total else "0"


async def show_library(update, context, page: int = 0, cat_id: Optional[int] = None) -> None:
    if cat_id:
        total = int(
            fetchval("SELECT COUNT(*) FROM content_items WHERE enabled=1 AND category_id=?", (cat_id,), 0) or 0
        )
        page, pages, off = paginate(total, page)
        rows = fetchall(
            """
            SELECT * FROM content_items WHERE enabled=1 AND category_id=?
            ORDER BY position ASC, id DESC LIMIT ? OFFSET ?
            """,
            (cat_id, PAGE_SIZE, off),
        )
        title = f"📚 {category_name(cat_id)}"
        prefix = f"u:cat:{cat_id}:"
        back = "u:lib:0"
    else:
        total = int(fetchval("SELECT COUNT(*) FROM content_items WHERE enabled=1", (), 0) or 0)
        page, pages, off = paginate(total, page)
        rows = fetchall(
            "SELECT * FROM content_items WHERE enabled=1 ORDER BY position ASC, id DESC LIMIT ? OFFSET ?",
            (PAGE_SIZE, off),
        )
        title = "📚 सामग्री"
        prefix = "u:lib:"
        back = "u:home"
    kb: List[List[InlineKeyboardButton]] = []
    cats = fetchall("SELECT id, name FROM categories ORDER BY position, id")
    cat_btns = [btn(clip(c["name"], 12), f"u:cat:{c['id']}:0") for c in cats[:6]]
    if cat_btns and not cat_id:
        for i in range(0, len(cat_btns), 3):
            kb.append(cat_btns[i : i + 3])
    for r in rows:
        kb.append([content_item_button(r)])
    rng = range_label(page, total)
    pr = pager_row(prefix, page, pages)
    if pr:
        kb.append(pr)
    kb.append([btn(f"📄 {rng}", "u:noop")])
    kb.append(nav_row(back))
    await safe_show(update, context, content_list_text(title, page, pages, total), ik(kb))


async def show_preview(update, context, cid: int) -> None:
    item = get_content(cid)
    if not item or not item["enabled"]:
        await safe_show(update, context, "❌ Content उपलब्ध नहीं है (deleted/disabled)।", ik([nav_row("u:lib:0")]))
        return
    uid = update.effective_user.id
    liked = user_liked(uid, cid)
    text = (
        f"🎬 <b>{esc(item['title'] or 'Untitled')}</b>\n\n"
        f"📁 Category: {esc(category_name(item['category_id']))}\n"
        f"⏱ Duration: {esc(fmt_duration(item['duration']))}\n"
        f"👁 Views (Open/Watch): {content_view_total(cid)}\n"
        f"❤️ Likes: {content_like_count(cid)}\n"
        f"🆔 ID: <code>{cid}</code>\n\n"
        f"<i>Preview view count नहीं बढ़ाता और access limit consume नहीं करता।</i>"
    )
    kb = ik(
        [
            [btn("▶️ Open / Watch", f"u:open:{cid}")],
            [btn("💔 Unlike" if liked else "❤️ Like", f"u:like:{cid}:p")],
            nav_row("u:lib:0"),
        ]
    )
    await safe_show(update, context, text, kb)


async def open_content(update, context, cid: int) -> None:
    uid = update.effective_user.id
    item = get_content(cid)
    q = update.callback_query
    if not item or not item["enabled"]:
        if q:
            await safe_answer(q, "Content not found", True)
        return
    allowed, reason = check_content_access(uid, cid)
    if not allowed:
        if q:
            await safe_answer(q, "Limit reached", True)
        await safe_show(
            update,
            context,
            reason,
            ik([[btn("🔙 Back", f"u:prev:{cid}")], nav_row("u:home")]),
        )
        return
    record_access(uid, cid)  # INSERT OR IGNORE: repeat open does not consume extra unique slots
    record_view(uid, cid)
    liked = user_liked(uid, cid)
    msg = await deliver_content_item(
        context.bot, uid, item, like_kb(cid, liked, [[btn("🔙 Preview", f"u:prev:{cid}")]])
    )
    if msg:
        remember_outgoing(msg.chat_id, msg.message_id, "content")
        sec = int(get_setting("content_timer_seconds", "0") or 0)
        schedule_autodel(context.application, msg.chat_id, msg.message_id, sec)
        if q:
            await safe_answer(q, "Opened")
    else:
        if q:
            await safe_answer(q, "Send failed", True)


async def show_likes(update, context, page: int) -> None:
    uid = update.effective_user.id
    total = int(
        fetchval(
            """
            SELECT COUNT(*) FROM content_likes l
            JOIN content_items c ON c.id=l.content_id
            WHERE l.user_id=? AND c.enabled=1
            """,
            (uid,),
            0,
        )
        or 0
    )
    page, pages, off = paginate(total, page)
    rows = fetchall(
        """
        SELECT c.* FROM content_likes l
        JOIN content_items c ON c.id=l.content_id
        WHERE l.user_id=? AND c.enabled=1
        ORDER BY l.created_at DESC LIMIT ? OFFSET ?
        """,
        (uid, PAGE_SIZE, off),
    )
    kb = [[content_item_button(r)] for r in rows] or [[btn("No likes yet", "u:noop")]]
    pr = pager_row("u:likes:", page, pages)
    if pr:
        kb.append(pr)
    kb.append(nav_row("u:home"))
    await safe_show(update, context, f"<b>❤️ My Likes</b>\nPage {page + 1}/{pages} • {total} items", ik(kb))


async def show_profile(update, context) -> None:
    uid = update.effective_user.id
    u = get_user(uid)
    if not u:
        return
    unique_v = int(
        fetchval("SELECT COUNT(*) FROM content_views WHERE user_id=?", (uid,), 0) or 0
    )
    total_v = int(
        fetchval("SELECT COALESCE(SUM(view_count),0) FROM content_views WHERE user_id=?", (uid,), 0) or 0
    )
    likes = int(fetchval("SELECT COUNT(*) FROM content_likes WHERE user_id=?", (uid,), 0) or 0)
    refs = int(fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,), 0) or 0)
    pstatus, pexp = premium_label(uid)
    text = (
        f"<b>👤 My Profile</b>\n\n"
        f"Name: {esc(u['first_name'])}\n"
        f"Username: @{esc(u['username'] or '—')}\n"
        f"Telegram ID: <code>{uid}</code>\n"
        f"Unique Content Views: {unique_v}\n"
        f"Total Views (Open/Watch): {total_v}\n"
        f"Total Likes: {likes}\n"
        f"Total Referrals: {refs}\n"
        f"Premium Status: {esc(pstatus)}\n"
        f"Premium Expiry: {esc(pexp)}\n"
        f"Access: {esc(access_summary(uid))}\n"
        f"Joined: {esc(u['join_time'])}"
    )
    await safe_show(update, context, text, ik([nav_row("u:home")]))


async def show_referral(update, context) -> None:
    uid = update.effective_user.id
    me = await context.bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{uid}"
    st = referral_stats(uid)
    unlocked = "Unlimited" if st["unlocked"] < 0 else str(st["unlocked"])
    rtype = get_setting("referral_reward_type", "items")
    rval = get_setting("referral_reward_value", "5")
    reward = f"+{rval} Day Premium" if rtype == "premium_days" else f"+{rval} Items"
    text = (
        f"<b>🔗 Refer & Earn</b>\n\n"
        f"आपका unique link:\n{esc(link)}\n\n"
        f"Reward per successful first referral: <b>{esc(reward)}</b>\n"
        f"(Self-referral और duplicate नहीं गिने जाते।)\n\n"
        f"✅ Successful Referrals: {st['successful']}\n"
        f"⭐ Points: {st['points']}\n"
        f"📚 Unique Content Used: {st['used']}\n"
        f"🔓 Total Unlocked: {esc(unlocked)}\n"
        f"🎁 Bonus items: +{st['extra']}"
    )
    await safe_show(
        update,
        context,
        text,
        ik([[btn("🏆 Leaderboard", "u:refboard")], nav_row("u:home")]),
    )


async def show_ref_board(update, context) -> None:
    rows = fetchall(
        """
        SELECT referrer_id, COUNT(*) AS n FROM referrals
        GROUP BY referrer_id ORDER BY n DESC LIMIT 10
        """
    )
    medals = ["🏆 #1", "🥈 #2", "🥉 #3"]
    lines = ["<b>🏆 Referral Leaderboard</b>\n"]
    for i, r in enumerate(rows):
        u = get_user(r["referrer_id"])
        name = esc(u["first_name"] if u else r["referrer_id"])
        prefix = medals[i] if i < 3 else f"#{i + 1}"
        lines.append(f"{prefix} {name} — {r['n']}")
    if len(lines) == 1:
        lines.append("अभी कोई referral नहीं।")
    await safe_show(update, context, "\n".join(lines), ik([nav_row("u:ref")]))


async def show_bot_info(update, context) -> None:
    text = (
        "<b>ℹ️ Bot Info</b>\n\n"
        "• यह एक <b>Admin Controlled Bot</b> है।\n"
        "• Admin content उपलब्ध करा सकता है।\n"
        "• Users Admin को message भेज सकते हैं।\n"
        "• Admin broadcast कर सकता है।\n"
        "• Admin user को reply कर सकता है।\n"
        "• Views = Open/Watch action (Telegram यह confirm नहीं करता कि वीडियो पूरा देखा गया)।"
    )
    await safe_show(update, context, text, ik([nav_row("u:home")]))


async def show_user_help(update, context) -> None:
    text = (
        "<b>🆘 Help</b>\n\n"
        "📚 <b>सामग्री</b> — library, category, pagination, preview फिर Open/Watch\n"
        "🔍 <b>Search</b> — title, caption, text, category, content ID\n"
        "❤️ <b>My Likes</b> — आपके liked items\n"
        "👤 <b>Profile</b> — stats और premium status\n"
        "🔗 <b>Refer & Earn</b> — unique link, bonus items/premium\n"
        "📩 <b>Admin को भेजें</b> — text/media भेजें\n"
        "🔔 Notifications — New Content / Broadcast / Referral / Premium / Expiry\n\n"
        "Free users की access limit Admin Settings से बदलती है। "
        "एक ही content दोबारा खोलने पर unique limit दोबारा नहीं कटती।\n"
        "Premium = unlimited content access (manual admin activation)।"
    )
    await safe_show(update, context, text, ik([nav_row("u:home")]))


async def show_user_notif(update, context) -> None:
    uid = update.effective_user.id
    p = fetchone("SELECT * FROM notification_preferences WHERE user_id=?", (uid,))
    if not p:
        execute("INSERT OR IGNORE INTO notification_preferences(user_id) VALUES(?)", (uid,))
        p = fetchone("SELECT * FROM notification_preferences WHERE user_id=?", (uid,))
    def onoff(v):
        return "ON" if int(v or 0) else "OFF"

    text = "<b>🔔 Notification Preferences</b>\nTap to toggle."
    kb = ik(
        [
            [btn(f"New Content: {onoff(p['new_content'])}", "u:nset:new_content")],
            [btn(f"Broadcast: {onoff(p['broadcast'])}", "u:nset:broadcast")],
            [btn(f"Referral: {onoff(p['referral'])}", "u:nset:referral")],
            [btn(f"Premium: {onoff(p['premium'])}", "u:nset:premium")],
            [btn(f"Expiry: {onoff(p['expiry'])}", "u:nset:expiry")],
            nav_row("u:home"),
        ]
    )
    await safe_show(update, context, text, kb)


async def toggle_user_notif(update, context, key: str) -> None:
    uid = update.effective_user.id
    allowed = {"new_content", "broadcast", "referral", "premium", "expiry"}
    if key not in allowed:
        return
    execute(
        f"UPDATE notification_preferences SET {key}=CASE WHEN {key}=1 THEN 0 ELSE 1 END WHERE user_id=?",
        (uid,),
    )
    await show_user_notif(update, context)


async def prompt_search(update, context) -> None:
    st_set(update.effective_user.id, "search")
    await safe_show(
        update,
        context,
        "🔍 Search query भेजें (title / caption / text / category / content ID)।\n/cancel से रद्द करें।",
        ik([nav_row("u:home")]),
    )


async def prompt_msg_admin(update, context) -> None:
    st_set(update.effective_user.id, "msg_admin")
    await safe_show(
        update,
        context,
        "📩 Admin को भेजने के लिए अगला text/photo/video/document/audio/voice/animation/etc. भेजें।\n/cancel से रद्द करें।",
        ik([nav_row("u:home")]),
    )


async def run_search(update, context, query: str, page: int = 0) -> None:
    qraw = (query or "").strip()
    st_set(update.effective_user.id, None, search=qraw)
    if not qraw:
        await safe_show(update, context, "खाली search।", ik([nav_row("u:search")]))
        return
    like = f"%{qraw}%"
    cid = None
    if qraw.isdigit():
        cid = int(qraw)
    cat_ids = [r["id"] for r in fetchall("SELECT id FROM categories WHERE name LIKE ?", (like,))]
    where = """
        enabled=1 AND (
            title LIKE ? OR IFNULL(caption,'') LIKE ? OR IFNULL(text,'') LIKE ?
            OR CAST(id AS TEXT)=?
    """
    params: List[Any] = [like, like, like, qraw]
    if cat_ids:
        where += " OR category_id IN (" + ",".join("?" * len(cat_ids)) + ")"
        params.extend(cat_ids)
    where += ")"
    if cid is not None:
        where = f"enabled=1 AND (id=? OR ({where}))"
        params = [cid] + params
    total = int(fetchval(f"SELECT COUNT(*) FROM content_items WHERE {where}", params, 0) or 0)
    page, pages, off = paginate(total, page)
    rows = fetchall(
        f"SELECT * FROM content_items WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
        params + [PAGE_SIZE, off],
    )
    kb = [[content_item_button(r)] for r in rows] or [[btn("No results", "u:noop")]]
    # store query in state for pagination
    st_set(update.effective_user.id, None, search=qraw)
    pr = pager_row("u:sres:", page, pages)
    if pr:
        kb.append(pr)
    kb.append(nav_row("u:search"))
    await safe_show(
        update,
        context,
        f"<b>🔍 Results for</b> {esc(qraw)}\nPage {page + 1}/{pages} • {total}",
        ik(kb),
    )


async def notify_new_content(context: ContextTypes.DEFAULT_TYPE, item: sqlite3.Row) -> None:
    users = fetchall(
        """
        SELECT u.telegram_id FROM users u
        JOIN notification_preferences n ON n.user_id=u.telegram_id
        WHERE u.status='active' AND n.new_content=1
        """
    )
    text = f"🔔 New Content Available\n\n{item['title'] or 'New item'}\nID: {item['id']}"
    kb = ik([[btn("🎬 Preview", f"u:prev:{item['id']}")]])
    for u in users:
        try:
            await context.bot.send_message(u["telegram_id"], text, reply_markup=kb)
            await asyncio.sleep(0.04)
        except Forbidden:
            await handle_forbidden(u["telegram_id"])
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramError:
            continue


# ---------------------------------------------------------------------------
# Admin screens
# ---------------------------------------------------------------------------
def require_admin(update: Update) -> bool:
    u = update.effective_user
    return bool(u and is_admin(u.id))


async def show_admin_panel(update, context) -> None:
    await safe_show(update, context, "<b>👑 Admin Panel</b>\nProduction control center.", admin_panel_kb())


async def show_dashboard(update, context) -> None:
    today = utcnow().strftime("%Y-%m-%d")
    total_users = int(fetchval("SELECT COUNT(*) FROM users", (), 0) or 0)
    active = int(fetchval("SELECT COUNT(*) FROM users WHERE status='active'", (), 0) or 0)
    blocked = int(fetchval("SELECT COUNT(*) FROM users WHERE status='blocked'", (), 0) or 0)
    new_today = int(
        fetchval("SELECT COUNT(*) FROM users WHERE substr(join_time,1,10)=?", (today,), 0) or 0
    )
    total_c = int(fetchval("SELECT COUNT(*) FROM content_items", (), 0) or 0)
    prem = int(
        fetchval(
            """
            SELECT COUNT(*) FROM premium_subscriptions
            WHERE is_lifetime=1 OR (expiry IS NOT NULL AND expiry > ?)
            """,
            (now_iso(),),
            0,
        )
        or 0
    )
    refs = int(fetchval("SELECT COUNT(*) FROM referrals", (), 0) or 0)
    bcasts = int(fetchval("SELECT COUNT(*) FROM broadcasts", (), 0) or 0)
    text = (
        f"<b>📊 Dashboard</b>\n\n"
        f"👥 Total Users: {total_users}\n"
        f"✅ Active: {active}\n"
        f"⛔ Blocked: {blocked}\n"
        f"🆕 New Today: {new_today}\n"
        f"📚 Total Content: {total_c}\n"
        f"📩 Messages Received: {get_stat('messages_received')}\n"
        f"↩️ Replies Sent: {get_stat('replies_sent')}\n"
        f"📢 Broadcasts: {bcasts}\n"
        f"📤 Messages Sent: {get_stat('messages_sent')}\n"
        f"❌ Failed Messages: {get_stat('failed_messages')}\n"
        f"💎 Premium Users: {prem}\n"
        f"🔗 Referrals: {refs}\n"
        f"🔧 Maintenance: {'ON' if maintenance_on() else 'OFF'}\n"
        "⚙️ Access: Unlimited for all users"
    )
    await safe_show(update, context, text, ik([[btn("⬅️ Back", "a:panel")]]))


async def show_analytics(update, context) -> None:
    total_c = int(fetchval("SELECT COUNT(*) FROM content_items", (), 0) or 0)
    total_v = int(fetchval("SELECT COALESCE(SUM(view_count),0) FROM content_views", (), 0) or 0)
    uniq = int(fetchval("SELECT COUNT(DISTINCT user_id) FROM content_views", (), 0) or 0)
    likes = int(fetchval("SELECT COUNT(*) FROM content_likes", (), 0) or 0)
    week = (utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    top_view = fetchall(
        """
        SELECT c.id, c.title, COALESCE(SUM(v.view_count),0) AS n
        FROM content_items c LEFT JOIN content_views v ON v.content_id=c.id
        GROUP BY c.id ORDER BY n DESC LIMIT 10
        """
    )
    top_like = fetchall(
        """
        SELECT c.id, c.title, COUNT(l.user_id) AS n
        FROM content_items c LEFT JOIN content_likes l ON l.content_id=c.id
        GROUP BY c.id ORDER BY n DESC LIMIT 10
        """
    )
    trending = fetchall(
        """
        SELECT c.id, c.title, COALESCE(SUM(v.view_count),0) AS n
        FROM content_items c
        JOIN content_views v ON v.content_id=c.id
        WHERE v.last_view_time >= ?
        GROUP BY c.id ORDER BY n DESC LIMIT 10
        """,
        (week,),
    )

    def fmt_top(rows, empty="—"):
        if not rows:
            return empty
        return "\n".join(
            f"{i+1}. {esc(clip(r['title'] or r['id'],40))} ({r['n']})" for i, r in enumerate(rows) if r["n"]
        ) or empty

    text = (
        f"<b>📈 Content Analytics</b>\n\n"
        f"Total Content: {total_c}\n"
        f"Total Views (Open/Watch): {total_v}\n"
        f"Unique Viewers: {uniq}\n"
        f"Total Likes: {likes}\n\n"
        f"<b>Most Viewed</b>\n{fmt_top(top_view)}\n\n"
        f"<b>Most Liked</b>\n{fmt_top(top_like)}\n\n"
        f"<b>Trending (7 days)</b>\n{fmt_top(trending)}"
    )
    await safe_show(update, context, text, ik([[btn("⬅️ Back", "a:panel")]]))


async def show_users_page(update, context, page: int) -> None:
    total = int(fetchval("SELECT COUNT(*) FROM users", (), 0) or 0)
    page, pages, off = paginate(total, page)
    rows = fetchall(
        "SELECT telegram_id, first_name, username, status FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?",
        (PAGE_SIZE, off),
    )
    kb = []
    for r in rows:
        flag = "✅" if r["status"] == "active" else "⛔"
        kb.append(
            [
                btn(
                    f"{flag} {clip(r['first_name'] or '',18)} @{clip(r['username'] or '—',12)} {r['telegram_id']}",
                    f"a:user:{r['telegram_id']}",
                )
            ]
        )
    pr = pager_row("a:users:", page, pages)
    if pr:
        kb.append(pr)
    kb.append([btn("⬅️ Back", "a:panel")])
    await safe_show(update, context, f"<b>👥 Users</b>\nPage {page+1}/{pages} • {total}", ik(kb))


async def show_user_card(update, context, uid: int) -> None:
    u = get_user(uid)
    if not u:
        await safe_show(update, context, "User नहीं मिला।", ik([[btn("⬅️ Back", "a:users:0")]]))
        return
    pstatus, pexp = premium_label(uid)
    text = (
        f"<b>User</b> {esc(u['first_name'])}\n"
        f"ID: <code>{uid}</code>\n"
        f"Username: @{esc(u['username'] or '—')}\n"
        f"Status: {esc(u['status'])}\n"
        f"Joined: {esc(u['join_time'])}\n"
        f"Last seen: {esc(u['last_seen'])}\n"
        f"Premium: {esc(pstatus)} ({esc(pexp)})\n"
        f"Access: {esc(access_summary(uid))}\n"
        f"Referrals: {int(fetchval('SELECT COUNT(*) FROM referrals WHERE referrer_id=?', (uid,), 0) or 0)}"
    )
    kb = ik(
        [
            [btn("⛔ Block", f"a:ublk:{uid}:1"), btn("✅ Unblock", f"a:ublk:{uid}:0")],
            [btn("💎 +1d", f"a:pg:{uid}:1"), btn("+7d", f"a:pg:{uid}:7"), btn("+30d", f"a:pg:{uid}:30")],
            [btn("+90d", f"a:pg:{uid}:90"), btn("Lifetime", f"a:pg:{uid}:0"), btn("Revoke", f"a:pg:{uid}:-1")],
            [btn("📜 History", f"a:uhist:{uid}:0")],
            [btn("⬅️ Back", "a:users:0")],
        ]
    )
    await safe_show(update, context, text, kb)


async def show_content_menu(update, context) -> None:
    total = int(fetchval("SELECT COUNT(*) FROM content_items", (), 0) or 0)
    text = f"<b>📚 Content Management</b>\nTotal items: {total}"
    kb = ik(
        [
            [btn("➕ Add Photo / Video / File", "a:cadd")],
            [btn("📋 List / Edit", "a:clist:0")],
            [btn("📁 Categories", "a:cats")],
            [btn("🗑 Bulk Select & Delete", "a:cbulk:0")],
            [btn("⚠️ Delete All", "a:cdelall")],
            [btn("⬅️ Back", "a:panel")],
        ]
    )
    await safe_show(update, context, text, kb)


async def show_content_list_admin(update, context, page: int, bulk: bool = False) -> None:
    uid = update.effective_user.id
    total = int(fetchval("SELECT COUNT(*) FROM content_items", (), 0) or 0)
    page, pages, off = paginate(total, page)
    rows = fetchall(
        "SELECT id, title, enabled, type FROM content_items ORDER BY position ASC, id DESC LIMIT ? OFFSET ?",
        (PAGE_SIZE, off),
    )
    selected = set(st_get(uid).get("data", {}).get("selected") or [])
    kb = []
    prefix = "a:cbulk:" if bulk else "a:clist:"
    for r in rows:
        flag = "✅" if r["enabled"] else "🚫"
        if bulk:
            mark = "☑️" if r["id"] in selected else "☐"
            kb.append([btn(f"{mark} {flag} #{r['id']} {clip(r['title'],22)}", f"a:csel:{r['id']}:{page}")])
        else:
            kb.append([btn(f"{flag} #{r['id']} {clip(r['title'],24)} [{r['type']}]", f"a:citem:{r['id']}")])
    pr = pager_row(prefix, page, pages)
    if pr:
        kb.append(pr)
    if bulk:
        kb.append([btn(f"🗑 Delete Selected ({len(selected)})", "a:cdelsel")])
        kb.append([btn("⬅️ Back", "a:cont")])
    else:
        kb.append([btn("⬅️ Back", "a:cont")])
    title = "Bulk Select" if bulk else "Content List"
    await safe_show(update, context, f"<b>{title}</b>\nPage {page+1}/{pages} • {total}", ik(kb))


async def show_content_item_admin(update, context, cid: int) -> None:
    item = get_content(cid)
    if not item:
        await safe_show(update, context, "Deleted content.", ik([[btn("⬅️ Back", "a:clist:0")]]))
        return
    text = (
        f"<b>Content #{cid}</b>\n"
        f"Title: {esc(item['title'])}\n"
        f"Type: {esc(item['type'])}\n"
        f"Category: {esc(category_name(item['category_id']))}\n"
        f"Enabled: {'Yes' if item['enabled'] else 'No'}\n"
        f"Position: {item['position']}\n"
        f"Duration: {esc(fmt_duration(item['duration']))}\n"
        f"Views: {content_view_total(cid)} | Unique: {content_unique_viewers(cid)} | Likes: {content_like_count(cid)}\n"
        f"Created: {esc(item['created_at'])}\n"
        f"Caption: {esc(clip(item['caption'], 200))}"
    )
    kb = ik(
        [
            [btn("✏️ Title", f"a:cedit:{cid}:title"), btn("✏️ Caption", f"a:cedit:{cid}:caption")],
            [btn("📁 Category", f"a:ccat:{cid}"), btn("🖼 Thumbnail", f"a:cedit:{cid}:thumb")],
            [btn("⬆️ Order", f"a:cord:{cid}:-1"), btn("⬇️ Order", f"a:cord:{cid}:1")],
            [
                btn("🚫 Disable" if item["enabled"] else "✅ Enable", f"a:ctog:{cid}"),
                btn("🗑 Delete", f"a:cdel:{cid}"),
            ],
            [btn("🔔 Notify Users", f"a:cnotify:{cid}")],
            [btn("⬅️ Back", "a:clist:0")],
        ]
    )
    await safe_show(update, context, text, kb)


async def show_categories(update, context) -> None:
    rows = fetchall("SELECT * FROM categories ORDER BY position, id")
    kb = []
    for r in rows:
        n = int(fetchval("SELECT COUNT(*) FROM content_items WHERE category_id=?", (r["id"],), 0) or 0)
        kb.append(
            [
                btn(f"{r['name']} ({n})", f"a:catx:{r['id']}"),
                btn("✏️", f"a:catren:{r['id']}"),
                btn("🗑", f"a:catdel:{r['id']}"),
            ]
        )
    kb.append([btn("➕ Add Category", "a:catadd")])
    kb.append([btn("⬅️ Back", "a:cont")])
    await safe_show(update, context, "<b>📁 Categories</b>", ik(kb))


async def show_welcome_admin(update, context) -> None:
    text = (
        f"<b>🎁 Welcome Message</b>\n\n"
        f"Type: {esc(get_setting('welcome_type'))}\n"
        f"Text: {esc(clip(get_setting('welcome_text'), 300))}\n"
        f"Caption: {esc(clip(get_setting('welcome_caption'), 200))}\n"
        f"File: {'set' if get_setting('welcome_file_id') else '—'}"
    )
    kb = ik(
        [
            [btn("📝 Set Text", "a:wset:text")],
            [btn("🖼 Photo + Caption", "a:wset:photo")],
            [btn("🎥 Video + Caption", "a:wset:video")],
            [btn("⬅️ Back", "a:panel")],
        ]
    )
    await safe_show(update, context, text, kb)


async def show_access_settings(update, context) -> None:
    mode = "unlimited"
    text = (
        "<b>⚙️ Access Settings</b>\n\n"
        "Free users: <b>Unlimited</b>\n"
        "Premium users: <b>Unlimited</b>\n"
        "Admins: <b>Unlimited</b>\n\n"
        "Referral is statistics-only and does not unlock/limit content.\n"
        "Total/Daily limit controls are retained for database compatibility but "
        "are not used by the access gate in this build."
    )
    kb = ik([[btn("🔄 Refresh", "a:access")], [btn("⬅️ Back", "a:panel")]])
    await safe_show(update, context, text, kb)


async def show_referral_admin(update, context) -> None:
    total = int(fetchval("SELECT COUNT(*) FROM referrals", (), 0) or 0)
    text = (
        "<b>🔗 Referral Statistics</b>\n\n"
        f"Successful referrals: <b>{total}</b>\n\n"
        "Referral links are unique and self/duplicate referrals are blocked.\n"
        "Referral does <b>not</b> restrict content and does <b>not</b> grant content unlocks. "
        "It is only a number/statistic visible to users and admins."
    )
    await safe_show(update, context, text, ik([[btn("⬅️ Back", "a:panel")]]))


async def show_premium_admin(update, context) -> None:
    n = int(
        fetchval(
            "SELECT COUNT(*) FROM premium_subscriptions WHERE is_lifetime=1 OR expiry>?",
            (now_iso(),),
            0,
        )
        or 0
    )
    text = (
        f"<b>💎 Premium</b>\n\n"
        f"Active premium users: {n}\n\n"
        f"Manual activation only (no fake payment gateway).\n"
        f"Commands:\n"
        f"<code>/premium USER_ID 1|7|30|90|life</code>\n"
        f"<code>/unpremium USER_ID</code>\n\n"
        f"या Users list से user खोलें।"
    )
    await safe_show(update, context, text, ik([[btn("👥 Users", "a:users:0")], [btn("⬅️ Back", "a:panel")]]))


async def show_admin_notif(update, context) -> None:
    text = (
        "<b>🔔 Notifications</b>\n\n"
        "नया content add करने के बाद item पर <b>Notify Users</b> दबाएं "
        "(optional 🔔 New Content Available)।\n"
        "Users अपनी preferences से ON/OFF कर सकते हैं।"
    )
    await safe_show(update, context, text, ik([[btn("⬅️ Back", "a:panel")]]))


async def show_maint(update, context) -> None:
    on = maintenance_on()
    text = f"<b>🔧 Maintenance</b>\nStatus: <b>{'ON' if on else 'OFF'}</b>\n\nON होने पर normal users को maintenance message दिखेगा। Admins manage कर सकेंगे।"
    await safe_show(
        update,
        context,
        text,
        ik([[btn("Toggle /maintenance", "a:mtog")], [btn("⬅️ Back", "a:panel")]]),
    )


async def show_admins(update, context) -> None:
    rows = fetchall("SELECT * FROM admins ORDER BY role DESC, telegram_id")
    lines = ["<b>👑 Admins</b>\n"]
    for r in rows:
        lines.append(f"• <code>{r['telegram_id']}</code> ({esc(r['role'])})")
    lines.append("\nOwner: <code>/addadmin USER_ID</code> / <code>/deladmin USER_ID</code>")
    await safe_show(update, context, "\n".join(lines), ik([[btn("⬅️ Back", "a:panel")]]))


async def show_backup_menu(update, context) -> None:
    text = (
        "<b>💾 Backup / Restore</b>\n\n"
        "Backup: पूरा SQLite DB Telegram पर .db file के रूप में भेजा जाएगा।\n"
        "Restore: .db upload करें। Restore से पहले automatic safety backup बनेगा। "
        "Integrity + required tables check होंगे।"
    )
    kb = ik(
        [
            [btn("📦 Send Backup Now", "a:baknow")],
            [btn("♻️ Restore from .db", "a:restore")],
            [btn("⬅️ Back", "a:panel")],
        ]
    )
    await safe_show(update, context, text, kb)


async def show_audit(update, context, page: int) -> None:
    total = int(fetchval("SELECT COUNT(*) FROM admin_audit_log", (), 0) or 0)
    page, pages, off = paginate(total, page, AUDIT_PAGE)
    rows = fetchall(
        "SELECT * FROM admin_audit_log ORDER BY id DESC LIMIT ? OFFSET ?",
        (AUDIT_PAGE, off),
    )
    lines = [f"<b>📝 Audit Log</b> ({page+1}/{pages})\n"]
    for r in rows:
        lines.append(f"• {esc(r['created_at'])} | {r['admin_id']} | {esc(r['action'])} | {esc(clip(r['details'],80))}")
    if len(lines) == 1:
        lines.append("No logs.")
    kb = []
    pr = pager_row("a:audit:", page, pages)
    if pr:
        kb.append(pr)
    kb.append([btn("⬅️ Back", "a:panel")])
    await safe_show(update, context, "\n".join(lines), ik(kb))


async def show_admin_help(update, context) -> None:
    text = (
        "<b>🆘 Admin Help</b>\n\n"
        "<code>/start</code> user menu\n"
        "<code>/panel</code> admin panel\n"
        "<code>/msg USER_ID</code> next message → that user\n"
        "<code>/cancel</code> pending op\n"
        "<code>/user /block /unblock /history USER_ID</code>\n"
        "<code>/premium USER_ID 1|7|30|90|life</code>\n"
        "<code>/unpremium USER_ID</code>\n"
        "<code>/addadmin /deladmin USER_ID</code> (owner)\n"
        "<code>/maintenance</code>\n"
        "<code>/backup</code>\n"
        "<code>/timer 60|5m|1h|1d|off</code> general outgoing auto-delete\n"
        "<code>/contenttimer 30s|5m|1h|off</code> content auto-delete\n"
        "<code>/deletemsg USER_ID MESSAGE_ID</code>\n"
        "<code>/deletebroadcast BROADCAST_ID</code>\n"
        "<code>/setexpiry 3d|24h|off</code> hosting reminder\n"
        "Broadcast: Admin Panel → Broadcast, then send any message.\n"
        "Views = Open/Watch clicks, not guaranteed full video watch."
    )
    await safe_show(update, context, text, ik([[btn("⬅️ Back", "a:panel")]]))


# ---------------------------------------------------------------------------
# User → admin messages / replies / targeted / broadcast
# ---------------------------------------------------------------------------
async def forward_to_admins(update: Update, context: ContextTypes.DEFAULT_TYPE, src: Message) -> None:
    user = update.effective_user
    payload = detect_message_payload(src)
    bump_stat("messages_received", 1)
    execute(
        """
        INSERT INTO message_history(from_id, to_id, direction, content_type, file_id, text, telegram_message_id, created_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (user.id, OWNER_ID, "user_to_admin", payload["type"], payload["file_id"], clip(payload["text"] or payload["caption"], 1000), src.message_id, now_iso()),
    )
    header = (
        f"📩 User message\n"
        f"Name: {user.full_name}\n"
        f"Username: @{user.username or '—'}\n"
        f"User ID: {user.id}\n"
        f"Msg ID: {src.message_id}\n"
        f"Reply to this message to answer the user."
    )
    admins = fetchall("SELECT telegram_id FROM admins")
    ids = {r["telegram_id"] for r in admins}
    if OWNER_ID:
        ids.add(OWNER_ID)
    sent_any = False
    for aid in ids:
        try:
            h = await context.bot.send_message(aid, header)
            copied = await send_copy_or_forward(context.bot, aid, src)
            execute(
                "INSERT OR REPLACE INTO admin_message_map(admin_chat_id, admin_message_id, user_id, user_message_id) VALUES(?,?,?,?)",
                (aid, h.message_id, user.id, src.message_id),
            )
            if copied:
                execute(
                    "INSERT OR REPLACE INTO admin_message_map(admin_chat_id, admin_message_id, user_id, user_message_id) VALUES(?,?,?,?)",
                    (aid, copied.message_id, user.id, src.message_id),
                )
            sent_any = True
        except Forbidden:
            continue
        except TelegramError as e:
            log.warning("admin notify failed %s: %s", aid, e)
    if sent_any:
        try:
            await src.reply_text("✅ Message Sent")
        except TelegramError:
            pass
    else:
        try:
            await src.reply_text("❌ Admin तक नहीं पहुँच सका। बाद में कोशिश करें।")
        except TelegramError:
            pass
    st_clear(user.id)


async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    msg = update.message
    if not msg or not msg.reply_to_message or not is_admin(update.effective_user.id):
        return False
    mapped = fetchone(
        "SELECT user_id, user_message_id FROM admin_message_map WHERE admin_chat_id=? AND admin_message_id=?",
        (msg.chat_id, msg.reply_to_message.message_id),
    )
    if not mapped:
        return False
    uid = mapped["user_id"]
    payload = detect_message_payload(msg)
    copied = await send_copy_or_forward(context.bot, uid, msg)
    if copied:
        sec = int(get_setting("timer_seconds", "0") or 0)
        remember_outgoing(copied.chat_id, copied.message_id, "reply")
        schedule_autodel(context.application, copied.chat_id, copied.message_id, sec)
        bump_stat("replies_sent", 1)
        execute(
            """
            INSERT INTO message_history(from_id, to_id, direction, content_type, file_id, text, telegram_message_id, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                update.effective_user.id,
                uid,
                "admin_to_user",
                payload["type"],
                payload["file_id"],
                clip(payload["text"] or payload["caption"], 1000),
                copied.message_id,
                now_iso(),
            ),
        )
        try:
            await msg.reply_text(f"✅ Reply sent to {uid} (msg {copied.message_id})")
        except TelegramError:
            pass
    else:
        try:
            await msg.reply_text("❌ Reply failed (user blocked bot or invalid).")
        except TelegramError:
            pass
        bump_stat("failed_messages", 1)
    return True


async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, src: Message) -> None:
    admin_id = update.effective_user.id
    users = fetchall("SELECT telegram_id FROM users WHERE status='active'")
    bid = uuid.uuid4().hex[:12]
    total = len(users)
    execute(
        "INSERT INTO broadcasts(id, admin_id, created_at, total, sent, failed, status) VALUES(?,?,?,?,0,0,'running')",
        (bid, admin_id, now_iso(), total),
    )
    bump_stat("broadcasts", 1)
    audit(admin_id, "broadcast_start", bid)
    progress = await src.reply_text(f"📢 Broadcast {bid}\nTotal: {total}\nSent: 0\nFailed: 0\nProgress: 0%")
    sent = failed = 0
    sec = int(get_setting("timer_seconds", "0") or 0)
    for i, u in enumerate(users, 1):
        uid = u["telegram_id"]
        pref = fetchone("SELECT broadcast FROM notification_preferences WHERE user_id=?", (uid,))
        if pref is not None and int(pref["broadcast"] or 0) == 0:
            continue
        copied = await send_copy_or_forward(context.bot, uid, src)
        if copied:
            sent += 1
            remember_outgoing(copied.chat_id, copied.message_id, "broadcast", bid)
            execute(
                "INSERT INTO broadcast_messages(broadcast_id, chat_id, message_id) VALUES(?,?,?)",
                (bid, copied.chat_id, copied.message_id),
            )
            schedule_autodel(context.application, copied.chat_id, copied.message_id, sec)
        else:
            failed += 1
            bump_stat("failed_messages", 1)
        if i % 15 == 0 or i == total:
            execute("UPDATE broadcasts SET sent=?, failed=? WHERE id=?", (sent, failed, bid))
            try:
                pct = int(i * 100 / max(total, 1))
                await progress.edit_text(
                    f"📢 Broadcast {bid}\nTotal Users: {total}\nSent: {sent}\nFailed: {failed}\nProgress: {pct}%"
                )
            except BadRequest:
                pass
            except TelegramError:
                pass
        await asyncio.sleep(0.05)
    execute(
        "UPDATE broadcasts SET sent=?, failed=?, status='done' WHERE id=?",
        (sent, failed, bid),
    )
    try:
        await progress.edit_text(
            f"✅ Broadcast complete\nID: <code>{bid}</code>\nTotal: {total}\nSent: {sent}\nFailed: {failed}",
            parse_mode=ParseMode.HTML,
        )
    except TelegramError:
        pass
    st_clear(admin_id)


async def send_db_backup(chat_id: int, context: ContextTypes.DEFAULT_TYPE, caption: str = "💾 Database backup") -> None:
    if not os.path.exists(DB_PATH):
        await context.bot.send_message(chat_id, "Database file missing.")
        return
    # checkpoint wal
    with db() as con:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    try:
        with open(DB_PATH, "rb") as f:
            await context.bot.send_document(chat_id, document=f, filename=os.path.basename(DB_PATH), caption=caption)
    except TelegramError as e:
        await context.bot.send_message(chat_id, f"Backup send failed: {e}")


def validate_restore_db(path: str) -> Tuple[bool, str]:
    if not os.path.exists(path):
        return False, "File exists check failed."
    size = os.path.getsize(path)
    if size < 100 or size > 200 * 1024 * 1024:
        return False, f"Invalid file size ({size} bytes)."
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        integ = cur.execute("PRAGMA integrity_check").fetchone()
        if not integ or str(integ[0]).lower() != "ok":
            con.close()
            return False, f"SQLite integrity check failed: {integ}"
        tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        con.close()
        missing = REQUIRED_TABLES - tables
        if missing:
            return False, "Required tables missing: " + ", ".join(sorted(missing))
        return True, "ok"
    except sqlite3.Error as e:
        return False, f"Not a valid SQLite DB: {e}"


async def restore_from_file(update, context, src: Message) -> None:
    doc = src.document
    if not doc:
        await src.reply_text("कृपया .db document भेजें।")
        return
    tmp = f"restore_upload_{uuid.uuid4().hex}.db"
    safety = f"bot_safety_before_restore_{int(time.time())}.db"
    try:
        file = await doc.get_file()
        await file.download_to_drive(tmp)
        ok, reason = validate_restore_db(tmp)
        if not ok:
            await src.reply_text(f"❌ Restore validation failed:\n{reason}")
            return
        with db() as con:
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if os.path.exists(DB_PATH):
            shutil.copy2(DB_PATH, safety)
        os.replace(tmp, DB_PATH)
        tmp = ""
        audit(update.effective_user.id, "restore", f"safety={safety}")
        await src.reply_text(
            "✅ Restore successful.\n"
            f"Safety backup: {safety}\n\n"
            "⚠️ Bot process को अभी restart/reload करें ताकि नया database पूरी तरह load हो "
            "(running connections पुरानी file को पकड़ सकती हैं)।"
        )
    except TelegramError as e:
        await src.reply_text(f"Restore failed: {e}")
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        st_clear(update.effective_user.id)


# ---------------------------------------------------------------------------
# Incoming message router
# ---------------------------------------------------------------------------
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    user = update.effective_user
    if not msg or not user:
        return
    upsert_user(user)
    if await handle_admin_reply(update, context):
        return
    if maintenance_on() and not is_admin(user.id):
        await msg.reply_text("🔧 Bot अभी Maintenance Mode में है।")
        return
    row = get_user(user.id)
    if row and row["status"] == "blocked" and not is_admin(user.id):
        return

    state = st_get(user.id)
    op = state.get("op")

    if op == "search":
        st_clear(user.id)
        await run_search(update, context, msg.text or msg.caption or "")
        return
    if op == "msg_admin":
        await forward_to_admins(update, context, msg)
        return
    if op == "welcome_text":
        set_setting("welcome_type", "text")
        set_setting("welcome_text", msg.text or "")
        audit(user.id, "welcome_text", "")
        st_clear(user.id)
        await msg.reply_text("✅ Welcome text saved.")
        return
    if op in ("welcome_photo", "welcome_video"):
        p = detect_message_payload(msg)
        need = "photo" if op == "welcome_photo" else "video"
        if p["type"] != need:
            await msg.reply_text(f"कृपया {need} भेजें।")
            return
        set_setting("welcome_type", need)
        set_setting("welcome_file_id", p["file_id"])
        set_setting("welcome_caption", p["caption"] or p["text"] or "")
        audit(user.id, "welcome_media", need)
        st_clear(user.id)
        await msg.reply_text("✅ Welcome media saved.")
        return
    if op == "add_content":
        await save_new_content(update, context, msg)
        return
    if op and str(op).startswith("edit_"):
        await apply_content_edit(update, context, msg, op)
        return
    if op == "cat_add":
        name = (msg.text or "").strip()
        if not name:
            await msg.reply_text("Name required.")
            return
        try:
            execute(
                "INSERT INTO categories(name, position) VALUES(?, COALESCE((SELECT MAX(position)+1 FROM categories),0))",
                (name,),
            )
            audit(user.id, "cat_add", name)
            await msg.reply_text("✅ Category added.")
        except sqlite3.IntegrityError:
            await msg.reply_text("यह नाम पहले से है।")
        st_clear(user.id)
        return
    if op == "cat_ren":
        cid = int(state["data"].get("cat_id") or 0)
        name = (msg.text or "").strip()
        if cid and name:
            execute("UPDATE categories SET name=? WHERE id=?", (name, cid))
            audit(user.id, "cat_ren", f"{cid}->{name}")
            await msg.reply_text("✅ Renamed.")
        st_clear(user.id)
        return
    if op == "custom_total":
        if not (msg.text or "").isdigit():
            await msg.reply_text("Number भेजें।")
            return
        set_setting("free_total_limit", str(int(msg.text)))
        audit(user.id, "free_total", msg.text)
        st_clear(user.id)
        await msg.reply_text("✅ Free total limit updated.")
        return
    if op == "custom_daily":
        if not (msg.text or "").isdigit():
            await msg.reply_text("Number भेजें।")
            return
        set_setting("free_daily_limit", str(int(msg.text)))
        audit(user.id, "free_daily", msg.text)
        st_clear(user.id)
        await msg.reply_text("✅ Free daily limit updated.")
        return
    if op and str(op).startswith("rrewc_"):
        st_clear(user.id)
        await msg.reply_text("Referral reward settings are disabled: referrals are statistics-only.")
        return
    if op == "broadcast":
        await do_broadcast(update, context, msg)
        return
    if op == "target_msg":
        tid = int(state["data"].get("target") or 0)
        copied = await send_copy_or_forward(context.bot, tid, msg)
        if copied:
            sec = int(get_setting("timer_seconds", "0") or 0)
            remember_outgoing(copied.chat_id, copied.message_id, "targeted")
            schedule_autodel(context.application, copied.chat_id, copied.message_id, sec)
            await msg.reply_text(f"✅ Sent to {tid} (message_id {copied.message_id})")
            bump_stat("replies_sent", 1)
        else:
            await msg.reply_text("❌ Failed to send.")
            bump_stat("failed_messages", 1)
        st_clear(user.id)
        return
    if op == "restore":
        await restore_from_file(update, context, msg)
        return

    # default: ignore random chat, or if user sent media without state, hint
    if is_admin(user.id):
        return


async def save_new_content(update, context, msg: Message) -> None:
    """Save one Telegram message as a content-library item.

    This intentionally stores Telegram file_id values, so the bot does not
    need to download/re-upload media and works with photos, videos and files.
    """
    user = update.effective_user
    if not user or not is_admin(user.id):
        st_clear(user.id if user else 0)
        return

    p = detect_message_payload(msg)
    supported = {"text", "photo", "video", "document", "audio", "voice", "animation", "video_note", "sticker"}
    if p["type"] not in supported:
        await msg.reply_text("❌ यह Telegram message type content library में supported नहीं है।")
        return
    if p["type"] == "text" and not (p["text"] or "").strip():
        await msg.reply_text("⚠️ खाली text save नहीं किया जा सकता।")
        return

    title = auto_title(
        p.get("text"),
        p.get("caption"),
        fallback=f"{p['type'].title()} {utcnow().strftime('%H:%M:%S')}",
    )
    other = fetchval("SELECT id FROM categories WHERE name='Other'")
    if other is None:
        other = fetchval("SELECT id FROM categories ORDER BY position, id LIMIT 1")

    try:
        with db() as con:
            pos = con.execute("SELECT COALESCE(MAX(position),0)+1 FROM content_items").fetchone()[0]
            cur = con.execute(
                """
                INSERT INTO content_items
                (type, file_id, text, caption, title, category_id, thumbnail, duration, created_at, enabled, position)
                VALUES(?,?,?,?,?,?,?,?,?,1,?)
                """,
                (
                    p["type"],
                    p.get("file_id"),
                    p.get("text") if p["type"] == "text" else None,
                    p.get("caption") or "",
                    title,
                    other,
                    p.get("thumbnail"),
                    p.get("duration"),
                    now_iso(),
                    pos,
                ),
            )
            cid = cur.lastrowid
    except Exception as exc:
        log.exception("Failed to save content")
        await msg.reply_text(f"❌ Content save नहीं हुआ।\n{esc(str(exc))[:500]}")
        return

    audit(user.id, "content_add", f"id={cid} type={p['type']}")
    st_clear(user.id)
    kb = ik([
        [btn("📁 Change Category", f"a:ccat:{cid}"), btn("✏️ Edit", f"a:citem:{cid}")],
        [btn("🔔 Notify Users", f"a:cnotify:{cid}")],
        [btn("➕ Add Another", "a:cadd"), btn("📋 Content List", "a:clist:0")],
        [btn("🏠 Admin", "a:panel")],
    ])
    await msg.reply_text(
        f"✅ <b>Content saved successfully!</b>\n\n🆔 ID: <code>{cid}</code>\n"
        f"📦 Type: <b>{esc(p['type'])}</b>\n🏷️ Title: <b>{esc(title)}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


async def apply_content_edit(update, context, msg: Message, op: str) -> None:
    uid = update.effective_user.id
    cid = int(st_get(uid)["data"].get("cid") or 0)
    item = get_content(cid)
    if not item:
        await msg.reply_text("Content missing.")
        st_clear(uid)
        return
    if op == "edit_title":
        execute("UPDATE content_items SET title=? WHERE id=?", ((msg.text or "")[:MAX_TITLE], cid))
    elif op == "edit_caption":
        execute("UPDATE content_items SET caption=? WHERE id=?", (msg.text or msg.caption or "", cid))
    elif op == "edit_thumb":
        p = detect_message_payload(msg)
        if p["type"] != "photo":
            await msg.reply_text("Thumbnail के लिए photo भेजें।")
            return
        execute("UPDATE content_items SET thumbnail=? WHERE id=?", (p["file_id"], cid))
    audit(uid, op, str(cid))
    st_clear(uid)
    await msg.reply_text("✅ Updated.")


# ---------------------------------------------------------------------------
# Callback router
# ---------------------------------------------------------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user:
        return
    user = q.from_user
    upsert_user(user)
    data = q.data or ""
    uid = user.id

    if data == "u:noop":
        await safe_answer(q)
        return
    if data == "u:cancel":
        st_clear(uid)
        await safe_answer(q, "Cancelled")
        await safe_show(update, context, "❌ Cancelled.", user_menu_kb(), q)
        return

    try:
        if data.startswith("u:"):
            urec = get_user(uid)
            if urec and urec["status"] == "blocked" and not is_admin(uid):
                await safe_answer(q, "Blocked", True)
                return
            if maintenance_on() and not is_admin(uid):
                await safe_answer(q)
                await safe_show(update, context, "🔧 Bot अभी Maintenance Mode में है।", query=q)
                return
            await handle_user_cb(update, context, data)
            return

        if data.startswith("a:"):
            if not is_admin(uid):
                await safe_answer(q, "Admins only", True)
                return
            await handle_admin_cb(update, context, data)
            return

        await safe_answer(q, "Unknown action", True)
    except (ValueError, IndexError, TypeError, sqlite3.Error) as e:
        log.warning("callback error data=%s err=%s", data, e)
        await safe_answer(q, "Invalid or failed action", True)


async def handle_user_cb(update, context, data: str) -> None:
    q = update.callback_query
    uid = q.from_user.id
    parts = data.split(":")
    key = parts[1] if len(parts) > 1 else ""
    if key not in ("like", "open"):
        await safe_answer(q)

    if key == "home":
        st_clear(uid)
        await safe_show(update, context, "🏠 Main Menu", user_menu_kb() if not is_admin(uid) else ik(
            [list(r) for r in user_menu_kb().inline_keyboard] + [[btn("👑 Admin Panel", "a:panel")]]
        ), q)
        return
    if key == "lib":
        page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        await show_library(update, context, page)
        return
    if key == "cat":
        cat_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        await show_library(update, context, page, cat_id)
        return
    if key == "prev":
        await show_preview(update, context, int(parts[2]))
        return
    if key == "open":
        await open_content(update, context, int(parts[2]))
        return
    if key == "like":
        cid = int(parts[2])
        src = parts[3] if len(parts) > 3 else "p"
        if not get_content(cid):
            await safe_answer(q, "Missing", True)
            return
        liked = toggle_like(uid, cid)
        await safe_answer(q, "Liked ❤️" if liked else "Unliked")
        if src == "p":
            await show_preview(update, context, cid)
        elif src == "d":
            try:
                await q.edit_message_reply_markup(like_kb(cid, liked, [[btn("🔙 Preview", f"u:prev:{cid}")]]))
            except BadRequest:
                pass
        else:
            await show_likes(update, context, 0)
        return
    if key == "likes":
        page = int(parts[2]) if len(parts) > 2 else 0
        await show_likes(update, context, page)
        return
    if key == "search":
        await prompt_search(update, context)
        return
    if key == "sres":
        page = int(parts[2]) if len(parts) > 2 else 0
        qtext = st_get(uid).get("data", {}).get("search") or ""
        await run_search(update, context, qtext, page)
        return
    if key == "profile":
        await show_profile(update, context)
        return
    if key == "ref":
        await show_referral(update, context)
        return
    if key == "refboard":
        await show_ref_board(update, context)
        return
    if key == "msgadm":
        await prompt_msg_admin(update, context)
        return
    if key == "info":
        await show_bot_info(update, context)
        return
    if key == "help":
        await show_user_help(update, context)
        return
    if key == "notif":
        await show_user_notif(update, context)
        return
    if key == "nset":
        await toggle_user_notif(update, context, parts[2] if len(parts) > 2 else "")
        return
    await safe_show(update, context, "Unknown action.", user_menu_kb())


async def handle_admin_cb(update, context, data: str) -> None:
    q = update.callback_query
    uid = q.from_user.id
    await safe_answer(q)
    parts = data.split(":")
    key = parts[1] if len(parts) > 1 else ""

    if key == "panel":
        await show_admin_panel(update, context)
    elif key == "dash":
        await show_dashboard(update, context)
    elif key == "anal":
        await show_analytics(update, context)
    elif key == "users":
        await show_users_page(update, context, int(parts[2]) if len(parts) > 2 else 0)
    elif key == "user":
        await show_user_card(update, context, int(parts[2]))
    elif key == "ublk":
        tid, mode = int(parts[2]), parts[3]
        set_user_status(tid, "blocked" if mode == "1" else "active")
        audit(uid, "block" if mode == "1" else "unblock", str(tid))
        await show_user_card(update, context, tid)
    elif key == "pg":
        tid, days = int(parts[2]), int(parts[3])
        if days == -1:
            revoke_premium(tid)
            audit(uid, "premium_revoke", str(tid))
        elif days == 0:
            grant_premium(tid, None, True, uid)
            audit(uid, "premium_life", str(tid))
        else:
            grant_premium(tid, days, False, uid)
            audit(uid, "premium_grant", f"{tid}:{days}d")
        pref = fetchone("SELECT premium FROM notification_preferences WHERE user_id=?", (tid,))
        if pref and pref["premium"] and days != -1:
            try:
                await context.bot.send_message(tid, "💎 Premium activated/extended by admin.")
            except Forbidden:
                await handle_forbidden(tid)
            except TelegramError:
                pass
        await show_user_card(update, context, tid)
    elif key == "uhist":
        tid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        total = int(fetchval("SELECT COUNT(*) FROM message_history WHERE from_id=? OR to_id=?", (tid, tid), 0) or 0)
        page, pages, off = paginate(total, page)
        rows = fetchall(
            "SELECT * FROM message_history WHERE from_id=? OR to_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
            (tid, tid, PAGE_SIZE, off),
        )
        lines = [f"<b>History {tid}</b> {page+1}/{pages}\n"]
        for r in rows:
            lines.append(f"{esc(r['created_at'])} {esc(r['direction'])} {esc(r['content_type'])}: {esc(clip(r['text'],60))}")
        kb: List[List[InlineKeyboardButton]] = []
        navp: List[InlineKeyboardButton] = []
        if page > 0:
            navp.append(btn("⬅️ Previous", f"a:uhist:{tid}:{page-1}"))
        if page + 1 < pages:
            navp.append(btn("➡️ Next", f"a:uhist:{tid}:{page+1}"))
        if navp:
            kb.append(navp)
        kb.append([btn("⬅️ Back", f"a:user:{tid}")])
        await safe_show(update, context, "\n".join(lines) if len(lines) > 1 else lines[0] + "\nEmpty", ik(kb))
    elif key == "cont":
        await show_content_menu(update, context)
    elif key == "cadd":
        st_set(uid, "add_content")
        await safe_show(
            update,
            context,
            "➕ अगला message content के रूप में save होगा (text/photo/video/document/audio/voice/GIF/video note/sticker)।\n/cancel से रद्द।",
            ik([nav_row("a:cont")]),
        )
    elif key == "clist":
        await show_content_list_admin(update, context, int(parts[2]) if len(parts) > 2 else 0, False)
    elif key == "citem":
        await show_content_item_admin(update, context, int(parts[2]))
    elif key == "cedit":
        cid, field = int(parts[2]), parts[3]
        if field == "title":
            st_set(uid, "edit_title", cid=cid)
            await safe_show(update, context, "नया title भेजें।", ik([nav_row(f"a:citem:{cid}")]))
        elif field == "caption":
            st_set(uid, "edit_caption", cid=cid)
            await safe_show(update, context, "नया caption भेजें।", ik([nav_row(f"a:citem:{cid}")]))
        elif field == "thumb":
            st_set(uid, "edit_thumb", cid=cid)
            await safe_show(update, context, "नई thumbnail photo भेजें।", ik([nav_row(f"a:citem:{cid}")]))
    elif key == "ccat":
        cid = int(parts[2])
        cats = fetchall("SELECT id, name FROM categories ORDER BY position")
        kb = [[btn(c["name"], f"a:csetcat:{cid}:{c['id']}")] for c in cats]
        kb.append([btn("⬅️ Back", f"a:citem:{cid}")])
        await safe_show(update, context, "Category चुनें:", ik(kb))
    elif key == "csetcat":
        cid, catid = int(parts[2]), int(parts[3])
        execute("UPDATE content_items SET category_id=? WHERE id=?", (catid, cid))
        audit(uid, "content_cat", f"{cid}->{catid}")
        await show_content_item_admin(update, context, cid)
    elif key == "cord":
        cid, delta = int(parts[2]), int(parts[3])
        item = get_content(cid)
        if item:
            execute("UPDATE content_items SET position=position+? WHERE id=?", (delta, cid))
        await show_content_item_admin(update, context, cid)
    elif key == "ctog":
        cid = int(parts[2])
        execute("UPDATE content_items SET enabled=CASE WHEN enabled=1 THEN 0 ELSE 1 END WHERE id=?", (cid,))
        audit(uid, "content_toggle", str(cid))
        await show_content_item_admin(update, context, cid)
    elif key == "cdel":
        cid = int(parts[2])
        execute("DELETE FROM content_items WHERE id=?", (cid,))
        audit(uid, "content_delete", str(cid))
        await safe_show(update, context, f"Deleted #{cid}", ik([[btn("⬅️ Back", "a:clist:0")]]))
    elif key == "cnotify":
        cid = int(parts[2])
        item = get_content(cid)
        if item:
            await q.edit_message_text("🔔 Sending notifications…")
            await notify_new_content(context, item)
            audit(uid, "content_notify", str(cid))
            await show_content_item_admin(update, context, cid)
    elif key == "cbulk":
        page = int(parts[2]) if len(parts) > 2 else 0
        st_get(uid).setdefault("data", {}).setdefault("selected", [])
        await show_content_list_admin(update, context, page, True)
    elif key == "csel":
        cid, page = int(parts[2]), int(parts[3]) if len(parts) > 3 else 0
        data_st = st_get(uid).setdefault("data", {})
        sel = set(data_st.get("selected") or [])
        if cid in sel:
            sel.remove(cid)
        else:
            sel.add(cid)
        data_st["selected"] = list(sel)
        st_set(uid, None, **data_st)
        await show_content_list_admin(update, context, page, True)
    elif key == "cdelsel":
        sel = list(st_get(uid).get("data", {}).get("selected") or [])
        for cid in sel:
            execute("DELETE FROM content_items WHERE id=?", (cid,))
        audit(uid, "content_bulk_delete", str(sel))
        st_set(uid, None, selected=[])
        await safe_show(update, context, f"Deleted {len(sel)} items.", ik([[btn("⬅️ Back", "a:cont")]]))
    elif key == "cdelall":
        await safe_show(
            update,
            context,
            "⚠️ Delete ALL content? यह सब items हटा देगा (users नहीं)।",
            ik([[btn("✅ Confirm Delete All", "a:cdelall2")], [btn("⬅️ Back", "a:cont")]]),
        )
    elif key == "cdelall2":
        execute("DELETE FROM content_items")
        audit(uid, "content_delete_all", "")
        await safe_show(update, context, "✅ All content deleted.", ik([[btn("⬅️ Back", "a:cont")]]))
    elif key == "cats":
        await show_categories(update, context)
    elif key == "catadd":
        st_set(uid, "cat_add")
        await safe_show(update, context, "नई category का नाम भेजें।", ik([nav_row("a:cats")]))
    elif key == "catren":
        st_set(uid, "cat_ren", cat_id=int(parts[2]))
        await safe_show(update, context, "नया नाम भेजें।", ik([nav_row("a:cats")]))
    elif key == "catdel":
        catid = int(parts[2])
        other = fetchval("SELECT id FROM categories WHERE name='Other'")
        execute("UPDATE content_items SET category_id=? WHERE category_id=?", (other, catid))
        execute("DELETE FROM categories WHERE id=?", (catid,))
        audit(uid, "cat_del", str(catid))
        await show_categories(update, context)
    elif key == "catx":
        await show_library(update, context, 0, int(parts[2]))
    elif key == "bcast":
        st_set(uid, "broadcast")
        await safe_show(
            update,
            context,
            "📢 अगला message सभी active users को broadcast होगा (text/media).\nUsers जिन्होंने Broadcast OFF किया है skip होंगे।\n/cancel से रद्द।",
            ik([nav_row("a:panel")]),
        )
    elif key == "welcome":
        await show_welcome_admin(update, context)
    elif key == "wset":
        kind = parts[2]
        if kind == "text":
            st_set(uid, "welcome_text")
            await safe_show(update, context, "Welcome text भेजें।", ik([nav_row("a:welcome")]))
        else:
            st_set(uid, f"welcome_{kind}")
            await safe_show(update, context, f"Welcome {kind} + caption भेजें।", ik([nav_row("a:welcome")]))
    elif key == "refs":
        await show_referral_admin(update, context)
    elif key == "rrew":
        audit(uid, "referral_reward_ignored", "Referral is statistics-only in this build")
        await show_referral_admin(update, context)
    elif key == "rrewc":
        audit(uid, "referral_reward_ignored", "Referral is statistics-only in this build")
        await show_referral_admin(update, context)
    elif key == "prem":
        await show_premium_admin(update, context)
    elif key == "access":
        await show_access_settings(update, context)
    elif key == "amode":
        set_setting("free_access_mode", parts[2])
        audit(uid, "access_mode", parts[2])
        await show_access_settings(update, context)
    elif key == "atotal":
        set_setting("free_total_limit", parts[2])
        audit(uid, "free_total", parts[2])
        await show_access_settings(update, context)
    elif key == "adaily":
        set_setting("free_daily_limit", parts[2])
        audit(uid, "free_daily", parts[2])
        await show_access_settings(update, context)
    elif key == "atotalc":
        st_set(uid, "custom_total")
        await safe_show(update, context, "Free total limit number भेजें।", ik([nav_row("a:access")]))
    elif key == "adailyc":
        st_set(uid, "custom_daily")
        await safe_show(update, context, "Free daily limit number भेजें।", ik([nav_row("a:access")]))
    elif key == "anotif":
        await show_admin_notif(update, context)
    elif key == "maint":
        await show_maint(update, context)
    elif key == "mtog":
        set_setting("maintenance", "0" if maintenance_on() else "1")
        audit(uid, "maintenance", get_setting("maintenance"))
        await show_maint(update, context)
    elif key == "admins":
        await show_admins(update, context)
    elif key == "backup":
        await show_backup_menu(update, context)
    elif key == "baknow":
        await send_db_backup(uid, context)
        audit(uid, "backup", "")
    elif key == "restore":
        st_set(uid, "restore")
        await safe_show(update, context, "Restore के लिए .db document भेजें। पहले safety backup बनेगा।", ik([nav_row("a:backup")]))
    elif key == "audit":
        await show_audit(update, context, int(parts[2]) if len(parts) > 2 else 0)
    elif key == "help":
        await show_admin_help(update, context)
    else:
        await safe_show(update, context, "Unknown admin action.", admin_panel_kb())


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def parse_id(args: List[str]) -> Optional[int]:
    if not args:
        return None
    try:
        return int(args[0])
    except ValueError:
        return None


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    st_clear(update.effective_user.id)
    await update.message.reply_text("❌ Cancelled.", reply_markup=user_menu_kb())


async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin access नहीं है। OWNER_ID सही सेट करें।")
        return
    await show_admin_panel(update, context)


async def cmd_addcontent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Direct, reliable entry point for adding media/content from the admin chat."""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("⛔ केवल Admin इस command का उपयोग कर सकता है।")
        return
    st_set(uid, "add_content")
    await update.message.reply_text(
        "➕ <b>ADD CONTENT MODE ON</b>\n\n"
        "अब एक-एक करके ये भेज सकते हैं:\n"
        "• Text\n• Photo\n• Video\n• Document\n• Audio\n"
        "• Voice\n• GIF/Animation\n• Video Note\n• Sticker\n\n"
        "हर message अलग content के रूप में save होगा।\n"
        "Title automatic बनेगा; बाद में Edit से title/caption/category बदल सकते हैं।\n\n"
        "❌ बंद करने के लिए /cancel",
        parse_mode=ParseMode.HTML,
    )


async def cmd_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /msg USER_ID")
        return
    st_set(update.effective_user.id, "target_msg", target=tid)
    await update.message.reply_text(f"अगला message {tid} को जाएगा। /cancel से रद्द।")


async def cmd_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /user USER_ID")
        return
    u = get_user(tid)
    if not u:
        await update.message.reply_text("Not found.")
        return
    await update.message.reply_text(
        f"{u['first_name']} @{u['username']}\nID {tid}\n{u['status']}\n{access_summary(tid)}\n{premium_label(tid)}"
    )


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /block USER_ID")
        return
    set_user_status(tid, "blocked")
    audit(update.effective_user.id, "block", str(tid))
    await update.message.reply_text(f"Blocked {tid}")


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /unblock USER_ID")
        return
    set_user_status(tid, "active")
    audit(update.effective_user.id, "unblock", str(tid))
    await update.message.reply_text(f"Unblocked {tid}")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /history USER_ID")
        return
    rows = fetchall(
        "SELECT * FROM message_history WHERE from_id=? OR to_id=? ORDER BY id DESC LIMIT 20",
        (tid, tid),
    )
    if not rows:
        await update.message.reply_text("No history.")
        return
    lines = [f"{r['created_at']} {r['direction']} {r['content_type']}: {clip(r['text'],80)}" for r in rows]
    await update.message.reply_text("\n".join(lines)[:4000])


async def cmd_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text("Usage: /premium USER_ID 1|7|30|90|life")
        return
    try:
        tid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid USER_ID")
        return
    token = args[1].lower()
    if token in ("life", "lifetime"):
        grant_premium(tid, None, True, update.effective_user.id)
    else:
        try:
            days = int(token)
        except ValueError:
            await update.message.reply_text("Invalid duration")
            return
        grant_premium(tid, days, False, update.effective_user.id)
    audit(update.effective_user.id, "premium_cmd", f"{tid} {token}")
    await update.message.reply_text(f"Premium updated for {tid}")


async def cmd_unpremium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /unpremium USER_ID")
        return
    revoke_premium(tid)
    audit(update.effective_user.id, "unpremium", str(tid))
    await update.message.reply_text("Premium revoked.")


async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /addadmin USER_ID")
        return
    execute(
        "INSERT OR REPLACE INTO admins(telegram_id, added_by, added_at, role) VALUES(?,?,?,'admin')",
        (tid, update.effective_user.id, now_iso()),
    )
    audit(update.effective_user.id, "addadmin", str(tid))
    await update.message.reply_text(f"Admin added: {tid}")


async def cmd_deladmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    tid = parse_id(context.args or [])
    if not tid:
        await update.message.reply_text("Usage: /deladmin USER_ID")
        return
    if tid == OWNER_ID:
        await update.message.reply_text("Main Owner को remove नहीं किया जा सकता।")
        return
    execute("DELETE FROM admins WHERE telegram_id=?", (tid,))
    audit(update.effective_user.id, "deladmin", str(tid))
    await update.message.reply_text("Admin removed.")


async def cmd_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    set_setting("maintenance", "0" if maintenance_on() else "1")
    await update.message.reply_text(f"Maintenance: {'ON' if maintenance_on() else 'OFF'}")
    audit(update.effective_user.id, "maintenance", get_setting("maintenance"))


async def cmd_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    await send_db_backup(update.effective_chat.id, context)
    audit(update.effective_user.id, "backup", "cmd")


async def cmd_timer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(f"Current timer: {get_setting('timer_seconds')}s\nUsage: /timer 60|5m|1h|1d|off")
        return
    sec = parse_duration_arg(args[0])
    if sec is None:
        await update.message.reply_text("Invalid. Examples: 60, 5m, 1h, 1d, off")
        return
    set_setting("timer_seconds", str(sec))
    audit(update.effective_user.id, "timer", str(sec))
    await update.message.reply_text("Timer off." if sec == 0 else f"Outgoing auto-delete: {sec}s")


async def cmd_contenttimer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            f"Current content timer: {get_setting('content_timer_seconds')}s\nUsage: /contenttimer 30s|5m|1h|off"
        )
        return
    sec = parse_duration_arg(args[0])
    if sec is None:
        await update.message.reply_text("Invalid.")
        return
    set_setting("content_timer_seconds", str(sec))
    audit(update.effective_user.id, "contenttimer", str(sec))
    await update.message.reply_text("Content timer off." if sec == 0 else f"Content auto-delete: {sec}s")


async def cmd_deletemsg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text("Usage: /deletemsg USER_ID MESSAGE_ID")
        return
    try:
        chat_id, mid = int(args[0]), int(args[1])
    except ValueError:
        await update.message.reply_text("Invalid IDs")
        return
    ok = await safe_delete_message(context.bot, chat_id, mid)
    await update.message.reply_text("Deleted" if ok else "Failed")


async def cmd_deletebroadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /deletebroadcast BROADCAST_ID")
        return
    bid = args[0]
    rows = fetchall("SELECT chat_id, message_id FROM broadcast_messages WHERE broadcast_id=?", (bid,))
    deleted = failed = 0
    for r in rows:
        if await safe_delete_message(context.bot, r["chat_id"], r["message_id"]):
            deleted += 1
        else:
            failed += 1
        await asyncio.sleep(0.03)
    await update.message.reply_text(f"Broadcast {bid}\nDeleted: {deleted}\nFailed: {failed}")
    audit(update.effective_user.id, "deletebroadcast", bid)


async def cmd_setexpiry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /setexpiry 3d|24h|off")
        return
    if args[0].lower() == "off":
        set_setting("hosting_expiry_ts", "0")
        set_setting("hosting_reminded", "")
        await update.message.reply_text("Hosting expiry off.")
        return
    sec = parse_duration_arg(args[0])
    if sec is None or sec <= 0:
        await update.message.reply_text("Invalid.")
        return
    ts = int(time.time()) + sec
    set_setting("hosting_expiry_ts", str(ts))
    set_setting("hosting_reminded", "")
    when = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    audit(update.effective_user.id, "setexpiry", when)
    await update.message.reply_text(f"Hosting expiry set: {when}\nReminders at 24h, 12h, 6h, 1h, and at expiry.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_admin(update.effective_user.id):
        await show_admin_help(update, context)
    else:
        await show_user_help(update, context)


# ---------------------------------------------------------------------------
# Hosting expiry job
# ---------------------------------------------------------------------------
async def job_hosting_expiry(context: ContextTypes.DEFAULT_TYPE) -> None:
    ts = int(float(get_setting("hosting_expiry_ts", "0") or 0))
    if ts <= 0:
        return
    now = int(time.time())
    left = ts - now
    reminded = set(filter(None, get_setting("hosting_reminded", "").split(",")))
    marks = []
    if left <= 0:
        marks.append("0")
    elif left <= 3600:
        marks.append("1h")
    elif left <= 6 * 3600:
        marks.append("6h")
    elif left <= 12 * 3600:
        marks.append("12h")
    elif left <= 24 * 3600:
        marks.append("24h")
    new_marks = [m for m in marks if m not in reminded]
    if not new_marks:
        return
    msg = (
        "⚠️ Hosting/bot expiry reached."
        if "0" in new_marks
        else f"⏰ Hosting expiry reminder: {new_marks[0]} remaining (target unix {ts})."
    )
    admins = {r["telegram_id"] for r in fetchall("SELECT telegram_id FROM admins")}
    if OWNER_ID:
        admins.add(OWNER_ID)
    for aid in admins:
        pref = fetchone("SELECT expiry FROM notification_preferences WHERE user_id=?", (aid,))
        if pref is not None and int(pref["expiry"] or 0) == 0:
            continue
        try:
            await context.bot.send_message(aid, msg)
        except Forbidden:
            await handle_forbidden(aid)
        except TelegramError:
            pass
    reminded.update(new_marks)
    set_setting("hosting_reminded", ",".join(sorted(reminded)))


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    log.error("Update error: %s\n%s", err, traceback.format_exc())
    if isinstance(err, (TimedOut, NetworkError, RetryAfter)):
        return
    if isinstance(err, (Forbidden, BadRequest, TelegramError)):
        return
    # never crash


async def post_init(application: Application) -> None:
    restore_states()
    await restore_autodel_jobs(application)
    jq = application.job_queue
    if jq:
        jq.run_repeating(job_hosting_expiry, interval=60, first=10)
    me = await application.bot.get_me()
    log.info("Bot started as @%s", me.username)


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Set BOT_TOKEN in environment or .env")
    if not OWNER_ID:
        log.warning("OWNER_ID is 0 — set OWNER_ID so admin commands work.")
    init_db()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(CommandHandler("addcontent", cmd_addcontent))
    app.add_handler(CommandHandler("msg", cmd_msg))
    app.add_handler(CommandHandler("user", cmd_user))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("unblock", cmd_unblock))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("premium", cmd_premium))
    app.add_handler(CommandHandler("unpremium", cmd_unpremium))
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CommandHandler("deladmin", cmd_deladmin))
    app.add_handler(CommandHandler("maintenance", cmd_maintenance))
    app.add_handler(CommandHandler("backup", cmd_backup))
    app.add_handler(CommandHandler("timer", cmd_timer))
    app.add_handler(CommandHandler("contenttimer", cmd_contenttimer))
    app.add_handler(CommandHandler("deletemsg", cmd_deletemsg))
    app.add_handler(CommandHandler("deletebroadcast", cmd_deletebroadcast))
    app.add_handler(CommandHandler("setexpiry", cmd_setexpiry))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, on_message))
    app.add_error_handler(on_error)
    log.info("Polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()
