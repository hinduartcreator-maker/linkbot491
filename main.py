"""
Telegram Bot Hosting Manager
- Accepts ONE hosted Python bot per manager instance.
- Upload: .py and optional requirements.txt
- Runs the hosted bot as a subprocess.
- Sends a heartbeat/counter every 10 minutes.
- Creates a ZIP backup of the hosted bot workspace every 12 hours and sends it to OWNER_ID.
- Keeps manager state in manager_state.json and hosted files in hosted_bot/.
- Designed for a persistent Linux host/container volume.

Environment variables:
  MANAGER_BOT_TOKEN = Telegram token of THIS hosting-manager bot
  OWNER_ID          = Telegram numeric ID of the owner/admin
  HEARTBEAT_MINUTES = 10 (optional)
  BACKUP_HOURS      = 12 (optional)
"""

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters
)

BASE = Path(__file__).resolve().parent
HOSTED = BASE / "hosted_bot"
STATE_FILE = BASE / "manager_state.json"
BACKUP_DIR = BASE / "backups"

TOKEN = os.getenv("MANAGER_BOT_TOKEN", "8400765415:AAEHYxy2k8xBdr2nvy8Y0H894C3__bBfKM0").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "8209524871") or 0)
HEARTBEAT_MINUTES = max(1, int(os.getenv("HEARTBEAT_MINUTES", "10") or 10))
BACKUP_HOURS = max(1, int(os.getenv("BACKUP_HOURS", "12") or 12))

HOSTED.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("hosting-manager")

process: Optional[subprocess.Popen] = None
started_at = 0.0
heartbeat_count = 0
upload_lock = asyncio.Lock()


def load_state():
    global heartbeat_count
    if not STATE_FILE.exists():
        return {"heartbeat_count": 0, "last_backup": 0, "last_started": 0}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"heartbeat_count": 0, "last_backup": 0, "last_started": 0}


def save_state(**changes):
    state = load_state()
    state.update(changes)
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


def is_owner(user_id: int) -> bool:
    return bool(OWNER_ID) and user_id == OWNER_ID


def hosted_files():
    return [p for p in HOSTED.rglob("*") if p.is_file()]


def main_script() -> Optional[Path]:
    preferred = HOSTED / "bot.py"
    if preferred.exists():
        return preferred
    py = sorted(HOSTED.glob("*.py"))
    return py[0] if py else None


def stop_process():
    global process
    if process is None:
        return
    if process.poll() is None:
        try:
            if os.name == "nt":
                process.terminate()
            else:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except Exception:
            try:
                process.terminate()
            except Exception:
                pass
        try:
            process.wait(timeout=8)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
    process = None


def install_requirements() -> tuple[bool, str]:
    req = HOSTED / "requirements.txt"
    if not req.exists():
        return True, "requirements.txt नहीं मिला; install की जरूरत नहीं।"

    # The uploaded requirements file must contain package names, not Python imports.
    text = req.read_text(encoding="utf-8", errors="replace")
    bad = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("import ") or s.startswith("from "):
            bad.append(s)
    if bad:
        return False, (
            "requirements.txt गलत है। इसमें Python import नहीं, package names होने चाहिए.\n"
            + "\n".join(bad[:5])
        )

    cmd = [sys.executable, "-m", "pip", "install", "-r", str(req)]
    log.info("Installing hosted requirements...")
    try:
        r = subprocess.run(cmd, cwd=HOSTED, capture_output=True, text=True, timeout=900)
    except Exception as e:
        return False, f"pip error: {e}"
    if r.returncode != 0:
        return False, (r.stdout + "\n" + r.stderr)[-5000:]
    return True, "Requirements installed."


def start_process() -> tuple[bool, str]:
    global process, started_at
    script = main_script()
    if not script:
        return False, "कोई .py bot file नहीं मिली। पहले Python file भेजें।"

    stop_process()
    ok, msg = install_requirements()
    if not ok:
        return False, msg

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    try:
        if os.name == "nt":
            process = subprocess.Popen(
                [sys.executable, str(script)],
                cwd=HOSTED,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            process = subprocess.Popen(
                [sys.executable, str(script)],
                cwd=HOSTED,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        started_at = time.time()
        save_state(last_started=int(started_at))
        return True, f"Bot started: {script.name}"
    except Exception as e:
        process = None
        return False, f"Bot start failed: {e}"


def make_backup() -> Path:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"hosted_bot_backup_{stamp}.zip"

    # Stop briefly so SQLite/WAL files and other state are captured consistently.
    was_running = process is not None and process.poll() is None
    if was_running:
        stop_process()

    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for p in HOSTED.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(BASE))
        if STATE_FILE.exists():
            z.write(STATE_FILE, STATE_FILE.relative_to(BASE))

    if was_running:
        start_process()

    save_state(last_backup=int(time.time()))
    return out


async def send_backup(context: ContextTypes.DEFAULT_TYPE, chat_id: int, automatic=False):
    try:
        path = await asyncio.to_thread(make_backup)
        caption = (
            "💾 <b>Automatic 12-hour backup</b>\n"
            "Hosted bot files + data/state ZIP attached."
            if automatic else
            "💾 <b>Manual backup</b>"
        )
        with path.open("rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=path.name,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
    except Exception as e:
        log.exception("Backup failed")
        await context.bot.send_message(chat_id, f"❌ Backup failed: {e}")


async def heartbeat_job(context: ContextTypes.DEFAULT_TYPE):
    global heartbeat_count
    if OWNER_ID == 0:
        return
    heartbeat_count = int(load_state().get("heartbeat_count", 0)) + 1
    save_state(heartbeat_count=heartbeat_count)

    running = process is not None and process.poll() is None
    status = "🟢 RUNNING" if running else "🔴 STOPPED"
    await context.bot.send_message(
        OWNER_ID,
        f"⏱️ <b>Hosting heartbeat #{heartbeat_count}</b>\n"
        f"Status: {status}\n"
        f"Interval: {HEARTBEAT_MINUTES} minutes",
        parse_mode=ParseMode.HTML,
    )

    # Auto-restart if hosted process died.
    if not running:
        ok, msg = await asyncio.to_thread(start_process)
        await context.bot.send_message(
            OWNER_ID,
            ("♻️ Auto-restart: " + msg) if ok else ("❌ Auto-restart failed:\n" + msg)
        )


async def backup_job(context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID:
        await send_backup(context, OWNER_ID, automatic=True)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        await update.effective_message.reply_text("⛔ Owner only.")
        return
    await update.effective_message.reply_text(
        "🤖 <b>Bot Hosting Manager</b>\n\n"
        "📤 Python file भेजें → bot.py के रूप में save होगी\n"
        "📦 requirements.txt भेजें → dependencies install होंगी\n\n"
        "/run — hosted bot start\n"
        "/stop — hosted bot stop\n"
        "/restart — restart\n"
        "/status — status\n"
        "/backup — तुरंत ZIP backup\n"
        "/files — hosted files\n"
        "/clear — hosted files हटाएँ\n\n"
        f"⏱️ Heartbeat: हर {HEARTBEAT_MINUTES} मिनट\n"
        f"💾 Backup: हर {BACKUP_HOURS} घंटे",
        parse_mode=ParseMode.HTML,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    running = process is not None and process.poll() is None
    age = int(time.time() - started_at) if running and started_at else 0
    files = hosted_files()
    await update.effective_message.reply_text(
        f"📊 <b>Hosting Status</b>\n\n"
        f"Bot: {'🟢 RUNNING' if running else '🔴 STOPPED'}\n"
        f"Uptime: {age // 3600}h {(age % 3600)//60}m\n"
        f"Files: {len(files)}\n"
        f"Heartbeat count: {load_state().get('heartbeat_count', 0)}\n"
        f"Last backup: {load_state().get('last_backup', 0) or 'Never'}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    ok, msg = await asyncio.to_thread(start_process)
    await update.effective_message.reply_text(("✅ " if ok else "❌ ") + msg)


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    stop_process()
    await update.effective_message.reply_text("🛑 Hosted bot stopped.")


async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    ok, msg = await asyncio.to_thread(start_process)
    await update.effective_message.reply_text(("♻️ " if ok else "❌ ") + msg)


async def cmd_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    await send_backup(context, update.effective_chat.id, automatic=False)


async def cmd_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    files = hosted_files()
    if not files:
        await update.effective_message.reply_text("📂 Hosted folder खाली है.")
        return
    lines = ["📂 <b>Hosted files</b>"]
    for p in files:
        lines.append(f"• {p.relative_to(HOSTED)}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    stop_process()
    for p in HOSTED.iterdir():
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            try:
                p.unlink()
            except OSError:
                pass
    await update.effective_message.reply_text("🧹 Hosted folder cleared. Backup पहले रखना बेहतर है: /backup")


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return
    doc = update.message.document
    name = (doc.file_name or "").lower()

    if not (name.endswith(".py") or name == "requirements.txt"):
        await update.message.reply_text("❌ केवल .py या requirements.txt भेजें।")
        return

    async with upload_lock:
        try:
            f = await doc.get_file()
            if name.endswith(".py"):
                dest = HOSTED / "bot.py"
            else:
                dest = HOSTED / "requirements.txt"

            await f.download_to_drive(str(dest))
            await update.message.reply_text(
                f"✅ {dest.name} upload हो गया.\n"
                "अब /run दबाएँ."
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Upload failed: {e}")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return
    text = update.message.text.strip()
    if text.startswith("/"):
        return
    await update.message.reply_text(
        "ℹ️ Commands: /run /stop /restart /status /backup /files /clear"
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Manager error", exc_info=context.error)


async def main():
    if not TOKEN:
        raise SystemExit("MANAGER_BOT_TOKEN environment variable is missing.")
    if not OWNER_ID:
        raise SystemExit("OWNER_ID environment variable is missing.")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("restart", cmd_restart))
    app.add_handler(CommandHandler("backup", cmd_backup))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(error_handler)

    jq = app.job_queue
    jq.run_repeating(heartbeat_job, interval=HEARTBEAT_MINUTES * 60, first=60)
    jq.run_repeating(backup_job, interval=BACKUP_HOURS * 3600, first=BACKUP_HOURS * 3600)

    # Start hosted bot after manager starts, if a previous deployment exists.
    if main_script():
        ok, msg = await asyncio.to_thread(start_process)
        log.info(msg)

    log.info("Hosting manager started.")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    try:
        await asyncio.Event().wait()
    finally:
        stop_process()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
