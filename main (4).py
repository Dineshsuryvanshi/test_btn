import logging
import sqlite3
import pytz
import re
import asyncio
import os
import json
import httpx
import time
from functools import wraps
from datetime import datetime, time as dtime
from typing import List
from urllib.parse import urlparse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =====================
# Logging
# =====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

try:
    IST = pytz.timezone('Asia/Kolkata')
except pytz.UnknownTimeZoneError:
    logger.error("Could not load timezone 'Asia/Kolkata'. Please check pytz installation.")
    # आप चाहें तो बॉट को यहाँ बंद कर सकते हैं या एक डिफ़ॉल्ट टाइमज़ोन का उपयोग कर सकते हैं
    IST = None 

# =====================
# Config
# =====================
# नया और सुरक्षित तरीका
TOKEN = "7624784085:AAG3TTp5yYvnwbYz2SW4ZcWtwlRL8jUKT28"  # अपना बॉट टोकन डालें
ADMIN_IDS = [5865209445]           # अपने Telegram User IDs
OWNER_ID = 5865209445  # अपना ID

# अगर TOKEN या OWNER_ID नहीं मिला तो बॉट को क्रैश कर दें
if not TOKEN or not OWNER_ID:
    raise ValueError("BOT_TOKEN and OWNER_ID environment variables must be set.")


MAX_CHANNELS_PER_BUTTON = 20
MAX_TIMES_PER_BUTTON = 11
BATCH_SIZE = 30

DB_NAME = "bot_data.db"


# =====================
# SQLite only for Metadata (channels, schedules, users)
# =====================
def init_db() -> None:
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # messages table हटाया गया है (SQlite queue use हो रही है)
    c.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            button_id TEXT,
            channel_id TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY,
            button_id TEXT,
            schedule_time TEXT  -- 'HH:MM'
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            role TEXT DEFAULT 'user',
            is_authorized BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

      # --- यहाँ नई messages टेबल जोड़ें ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        button_id TEXT NOT NULL,
        content TEXT,
        media_type TEXT NOT NULL,
        file_id TEXT,
        status TEXT DEFAULT 'pending' -- 'pending', 'sent'
    )
    """)
    # ------------------------------------

    c.execute("CREATE INDEX IF NOT EXISTS idx_channels_button ON channels(button_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_schedules_button ON schedules(button_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_users_authorized ON users(is_authorized)")
      # --- नई टेबल के लिए इंडेक्स जोड़ें ---
    c.execute("CREATE INDEX IF NOT EXISTS idx_messages_button_status ON messages(button_id, status)")
    # ------------------------------------


    conn.commit()
    conn.close()


# Async DB helpers (run sync sqlite in executor safely)
async def db_fetchall(query: str, params=()):
    loop = asyncio.get_event_loop()

    def _do():
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        return rows

    return await loop.run_in_executor(None, _do)


async def db_execute(query: str, params=()):
    loop = asyncio.get_event_loop()

    def _do():
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()
        conn.close()

    await loop.run_in_executor(None, _do)

# Utils
# =====================
def is_valid_time_str(s: str) -> bool:
    try:
        parts = s.split(":")
        if len(parts) != 2:
            return False
        h, m = int(parts[0]), int(parts[1])
        return 0 <= h <= 23 and 0 <= m <= 59
    except Exception:
        return False



def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = getattr(update, 'effective_user', None)
        
        if not user:
            print("DEBUG: Update received without a user. Ignoring.")
            return

        if user.id != OWNER_ID:
            # --- अनधिकृत पहुंच का प्रयास ---
            print(f"Unauthorized access attempt by user {user.id} ({user.username or 'N/A'}).")
            
            # उपयोगकर्ता को संदेश भेजें
            if update.effective_message:
                await update.effective_message.reply_text(
                    "❌ आप इस बॉट को use नहीं कर सकते!\nकृपया Owner से contact करें।"
                )
            
            # एडमिन को सूचना भेजें
            alert_message = (
                f"⚠️ *अनधिकृत पहुंच का प्रयास* ⚠️\n\n"
                f"*User ID:* `{user.id}`\n"
                f"*Username:* @{user.username or 'N/A'}\n"
                f"*First Name:* {user.first_name}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=alert_message,
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    logger.error(f"Failed to send unauthorized access alert to admin {admin_id}: {e}")
            # --------------------------------

            return # फंक्शन को आगे न चलाएं

        # अगर यूजर ओनर है, तो ही असली फंक्शन चलाएं
        return await func(update, context, *args, **kwargs)
    return wrapper




# =====================
# Status (async) - uses SQliteand SQLite
# =====================
async def get_button_status(button_id: str) -> str:
    channels = await db_fetchall("SELECT channel_id FROM channels WHERE button_id=?", (button_id,))
    schedules = await db_fetchall("SELECT schedule_time FROM schedules WHERE button_id=? ORDER BY schedule_time", (button_id,))
    
    # --- SQLite से पेंडिंग संदेशों की गिनती करें ---
    pending_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
    pending = pending_rows[0][0] if pending_rows else 0
    # ----------------------------------------------

    status = []
    status.append(f"बटन {button_id} स्टेटस:")
    status.append("")
    status.append(f"चैनल्स: {len(channels)}")
    status.append(f"पेंडिंग मैसेजेस: {pending}")
    status.append("शेड्यूल टाइम्स:")
    if schedules:
        for i, row in enumerate(schedules, 1):
            status.append(f"{i}. {row[0]}")
    else:
        status.append("(कोई टाइम सेट नहीं)")
    return "\n".join(status)

# =====================
# Message sending with exponential backoff
async def send_message_with_backoff(bot, chat_id, text, media_type, file_id):
    """
    मैसेज भेजने के लिए एक मज़बूत फ़ंक्शन जो नेटवर्क एरर और फ्लड कंट्रोल को संभालता है।
    """
    retry_wait = 2
    max_retries = 5
    attempt = 0

    while attempt < max_retries:
        attempt += 1
        try:
            if media_type == 'text':
                await bot.send_message(chat_id=chat_id, text=text, read_timeout=60, write_timeout=60, connect_timeout=60)
            elif media_type == 'photo':
                await bot.send_photo(chat_id=chat_id, photo=file_id, caption=text or None, read_timeout=60, write_timeout=60, connect_timeout=60)
            elif media_type == 'video':
                await bot.send_video(chat_id=chat_id, video=file_id, caption=text or None, read_timeout=60, write_timeout=60, connect_timeout=60)
            elif media_type == 'document':
                await bot.send_document(chat_id=chat_id, document=file_id, caption=text or None, read_timeout=60, write_timeout=60, connect_timeout=60)
            else:
                logger.warning(f"Unknown media_type {media_type}")
            
            # सफलतापूर्वक भेजा गया, तो लूप से बाहर निकलें
            return

        except error.RetryAfter as e:
            # टेलीग्राम ने जितने समय के लिए कहा है, उतने समय रुकें
            retry_seconds = e.retry_after
            logger.warning(f"Flood control exceeded for {chat_id}. Retrying in {retry_seconds} seconds.")
            await asyncio.sleep(retry_seconds)
            
        except (error.TimedOut, httpx.ReadError, httpx.ConnectError) as e:
            # नेटवर्क एरर के लिए फिर से कोशिश करें
            logger.warning(f"Network error on attempt {attempt} for {chat_id}: {e}. Retrying in {retry_wait}s...")
            await asyncio.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, 60) # इंतज़ार का समय बढ़ाएं

        except Exception as e:
            # किसी अन्य एरर के लिए लॉग करें और बाहर निकलें
            logger.error(f"An unhandled error occurred for {chat_id}: {e}", exc_info=True)
            break
    
    logger.error(f"Failed to send message to {chat_id} after {max_retries} attempts.")




# =====================
# Empty Queue Reminder (5 minutes)
# =====================
async def empty_queue_reminder(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    button_id = data.get("button_id")
    notify_chat_id = data.get("notify_chat_id")

    try:
        # SQLite में पेंडिंग संदेशों की गिनती करें
        pending_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
        pending_count = pending_rows[0][0] if pending_rows else 0

        if pending_count == 0:
            await context.bot.send_message(
                notify_chat_id,
                f"⏰ रिमाइंडर: बटन {button_id} की मैसेज क्यू अब भी खाली है। कृपया नए मैसेज जोड़ें।"
            )
        else:
            # अगर क्यू खाली नहीं है, तो इस रिमाइंडर जॉब को हटा दें
            context.job.schedule_removal()
            print(f"DEBUG: Reminder for {button_id} removed as queue is no longer empty.")
            
    except Exception as e:
        logger.error(f"empty_queue_reminder error: {e}")

# =====================
# Forwarding Job - uses SQlitequeue
# =====================
async def forward_messages_job(context: ContextTypes.DEFAULT_TYPE):
    """
    यह जॉब शेड्यूल के अनुसार SQLite से मैसेज फॉरवर्ड करती है और रिमाइंडर को मैनेज करती है।
    """
    data = context.job.data or {}
    button_id = data.get("button_id")
    notify_chat_id = data.get("notify_chat_id")
    sched_time = data.get("time")
    print("-" * 30)
    print(f"DEBUG: Running forward job for button_id = {button_id} at {sched_time}")

    try:
        # 1. चैनल प्राप्त करें
        channels_rows = await db_fetchall("SELECT channel_id FROM channels WHERE button_id=?", (button_id,))
        channels = [r[0] for r in channels_rows]

        if not channels:
            if notify_chat_id:
                await context.bot.send_message(notify_chat_id, f"⚠️ {button_id}: फॉरवर्डिंग असफल! इस बटन में कोई चैनल नहीं जुड़ा है।")
            return

        # 2. SQLite से 'pending' मैसेज निकालें
        messages_rows = await db_fetchall(
            "SELECT id, content, media_type, file_id FROM messages WHERE button_id=? AND status='pending' ORDER BY id ASC LIMIT ?",
            (button_id, BATCH_SIZE)
        )

        # --- यहाँ रिमाइंडर लॉजिक जोड़ें (जब कोई संदेश न मिले) ---
        if not messages_rows:
            if notify_chat_id:
                await context.bot.send_message(notify_chat_id, f"ℹ️ {button_id}: भेजने के लिए कोई पेंडिंग मैसेज नहीं है।")
            
            # अगर पहले से कोई रिमाइंडर जॉब नहीं चल रही है, तो नई जॉब बनाएं
            if notify_chat_id and not context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}"):
                print(f"DEBUG: Queue for {button_id} is empty. Scheduling 5-minute reminder.")
                context.job_queue.run_repeating(
                    empty_queue_reminder,
                    interval=300, first=300,  # 5 मिनट
                    name=f"empty_notify_{button_id}",
                    data={"button_id": button_id, "notify_chat_id": notify_chat_id},
                )
            return
        # --------------------------------------------------------

        # 3. मैसेज भेजें
        sent_count = 0
        sent_message_ids = []
        for msg_id, content, media_type, file_id in messages_rows:
            tasks = []
            for ch in channels:
                tasks.append(send_message_with_backoff(context.bot, ch, content, media_type, file_id))
            await asyncio.gather(*tasks)
            sent_count += 1
            sent_message_ids.append(msg_id)
            if sent_count % 5 == 0:
                await asyncio.sleep(1)

        # 4. भेजे गए संदेशों को डेटाबेस से हटाएं
        if sent_message_ids:
            query = f"DELETE FROM messages WHERE id IN ({','.join(['?'] * len(sent_message_ids))})"
            await db_execute(query, tuple(sent_message_ids))

        # 5. सफलता का अलर्ट भेजें
        if notify_chat_id:
            await context.bot.send_message(notify_chat_id, f"✅ {button_id}: {sent_count} मैसेज सफलतापूर्वक फॉरवर्ड कर दिए गए।")

        # --- यहाँ रिमाइंडर लॉजिक जोड़ें (जब संदेश भेजने के बाद क्यू खाली हो जाए) ---
        remaining_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
        remaining_count = remaining_rows[0][0] if remaining_rows else 0

        if remaining_count == 0 and notify_chat_id:
             if not context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}"):
                print(f"DEBUG: Queue for {button_id} is now empty. Scheduling 5-minute reminder.")
                context.job_queue.run_repeating(
                    empty_queue_reminder,
                    interval=300, first=300,  # 5 मिनट
                    name=f"empty_notify_{button_id}",
                    data={"button_id": button_id, "notify_chat_id": notify_chat_id},
                )
        # --------------------------------------------------------------------

    except Exception as e:
        logger.exception(f"Serious error in forward job for {button_id}")
        if notify_chat_id:
            try:
                await context.bot.send_message(notify_chat_id, f"❌ {button_id}: फॉरवर्डिंग में गंभीर त्रुटि हुई: {e}")
            except Exception as ex:
                logger.error(f"Failed to send error notification: {ex}")



# =====================
# Bot UI Handlers
# =====================
@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("👉 US", callback_data="btn1")],
        [InlineKeyboardButton("👉 IOS", callback_data="btn2")],
        [InlineKeyboardButton("👉 SFB", callback_data="btn3")],
        [InlineKeyboardButton("👉 personal ", callback_data="btn4")],
        [InlineKeyboardButton("बटन 5", callback_data="btn5")],
    ]
    await update.message.reply_text("मुख्य मेनू:", reply_markup=InlineKeyboardMarkup(keyboard))

@owner_only
async def open_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data  # e.g., 'btn1'
    keyboard = [
        [InlineKeyboardButton("➕चैनल जोड़ें➕", callback_data=f"add_chn_{button_id}")],
        [InlineKeyboardButton("💢चैनल हटाएं💢", callback_data=f"del_chn_{button_id}")],
        [InlineKeyboardButton("💌मैसेज जोड़ें💌", callback_data=f"add_msg_{button_id}")],
        [InlineKeyboardButton("🕕टाइम सेट करें🕛", callback_data=f"set_time_{button_id}")],
        [InlineKeyboardButton("➰फॉरवर्डिंग शुरू करें➰", callback_data=f"start_fw_{button_id}")],
        [InlineKeyboardButton("✔स्टेटस देखें🎦", callback_data=f"status_{button_id}")],
    ]
    # status text (async)
    status_text = await get_button_status(button_id)
    await query.edit_message_text(status_text, reply_markup=InlineKeyboardMarkup(keyboard))

@owner_only
async def add_channels_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    context.user_data["action"] = f"add_channels_{button_id}"
    await query.edit_message_text("चैनल ID भेजें (एक लाइन में एक, @channel या -100... फॉर्मेट):")

@owner_only
async def delete_channel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT channel_id FROM channels WHERE button_id=?", (button_id,))
    channels = [row[0] for row in c.fetchall()]
    conn.close()

    if not channels:
        await query.edit_message_text("❌ इस बटन में कोई चैनल नहीं है")
        return

    keyboard: List[List[InlineKeyboardButton]] = []
    for ch in channels:
        keyboard.append([InlineKeyboardButton(f"{ch}", callback_data=f"confirm_del_{button_id}_{ch}")])
    keyboard.append([InlineKeyboardButton(" 🔙वापस", callback_data=f"{button_id}")])

    await query.edit_message_text("निम्नलिखित चैनल्स में से हटाने के लिए चुनें:", reply_markup=InlineKeyboardMarkup(keyboard))

@owner_only
async def confirm_delete_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, _, button_id, channel = query.data.split("_", 3)
    keyboard = [
        [InlineKeyboardButton("✅ हाँ, हटाएं", callback_data=f"final_del_{button_id}_{channel}")],
        [InlineKeyboardButton("❌ नहीं", callback_data=f"{button_id}")],
    ]
    await query.edit_message_text(f"क्या आप वाकई चैनल {channel} को हटाना चाहते हैं?", reply_markup=InlineKeyboardMarkup(keyboard))

@owner_only
async def final_delete_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, _, button_id, channel = query.data.split("_", 3)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM channels WHERE button_id=? AND channel_id=?", (button_id, channel))
    conn.commit()
    conn.close()

    await query.edit_message_text(f"✅ चैनल {channel} सफलतापूर्वक हटाया गया")


@owner_only
async def add_messages_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    context.user_data["action"] = f"add_messages_{button_id}"
    await query.edit_message_text("🔜मैसेज भेजें (टेक्स्ट/फोटो/डॉक्युमेंट/वीडियो). फोटो/डॉक्युमेंट के साथ कैप्शन भी भेज सकते हैं:")

@owner_only
async def set_times_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    context.user_data["action"] = f"set_times_{button_id}"
    await query.edit_message_text("टाइम भेजें (HH:MM फॉर्मेट में, एक लाइन में एक):")

@owner_only
async def start_forwarding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # start_forwarding फ़ंक्शन में

    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]

    rows = await db_fetchall("SELECT schedule_time FROM schedules WHERE button_id=?", (button_id,))
    times = [r[0] for r in rows]
    if not times:
        await query.edit_message_text("❌ पहले टाइम सेट करें!")
        return

    # cancel existing jobs for this button
    all_jobs = context.job_queue.jobs()
    for job in all_jobs:
        if job.name and job.name.startswith(f"job_{button_id}_"):
            job.schedule_removal()
    
       # ... (for job in all_jobs: ... के बाद)
    created = 0
    for t in times:
        try:
            h, m = map(int, t.split(":"))
            when = dtime(hour=h, minute=m, tzinfo=IST)
            
            context.job_queue.run_daily(
                forward_messages_job,
                time=when,
                name=f"job_{button_id}_{t}",
                data={"button_id": button_id, "notify_chat_id": query.message.chat_id, "time": t},
                job_kwargs={'misfire_grace_time': 300} 
            )
            created += 1
        except (ValueError, IndexError):
            logger.warning(f"Invalid time format found: {t}. Skipping.")
            continue # अगर टाइम फॉर्मेट गलत है तो अगले पर जाएं
    # ... (for लूप के बाद)
    
        if created > 0:
            await query.edit_message_text(f"✅ फॉरवर्डिंग शुरू! {created} टाइम्स पर मैसेज भेजे जाएंगे।")
        else:
            await query.edit_message_text("❌ कोई भी वैध टाइम शेड्यूल नहीं किया जा सका। कृपया सही HH:MM फॉर्मेट में टाइम भेजें।")
    



# =====================
# Message capture
# =====================
@owner_only
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.get("action")
    if not action or not action.startswith("add_messages_"):
        return

    button_id = action.split("_")[-1]
    content = update.message.caption or None

    media_type = None
    file_id = None

    if update.message.photo:
        media_type = 'photo'
        file_id = update.message.photo[-1].file_id
    elif update.message.video:
        media_type = 'video'
        file_id = update.message.video.file_id
    elif update.message.document:
        media_type = 'document'
        file_id = update.message.document.file_id
    elif update.message.text:
        media_type = 'text'
        file_id = None
        content = update.message.text

    if not media_type:
        await update.message.reply_text("⚠️ सपोर्टेड मीडिया: टेक्स्ट, फोटो, वीडियो, डॉक्यूमेंट")
        return

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    logger.info(f"Enqueueing message for button {button_id}: {message_data}")
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    # message_data should be defined once, then used
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    logger.info(f"Enqueueing message for button {button_id}: {message_data}")
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,

    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,

    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,

    }


    # enqueue AFTER log to be sure message_data is correct
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    # enqueue
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }


    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }

    # enqueue message once
    message_data = {
        "content": content,
        "media_type": media_type,
        "file_id": file_id,
    }
   # ... (media_type और file_id सेट करने के बाद)
    logger.info(f"Adding message to SQLite for button {button_id}")
    await db_execute(
        "INSERT INTO messages (button_id, content, media_type, file_id) VALUES (?, ?, ?, ?)",
        (button_id, content, media_type, file_id)
    )
    
    # आप चाहें तो एक्शन को यहाँ क्लियर कर सकते हैं या यूजर को और मैसेज जोड़ने दे सकते हैं
    pending_count_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
    pending_count = pending_count_rows[0][0]
    # --- मौजूदा रिमाइंडर को रद्द करें क्योंकि अब क्यू खाली नहीं है ---
    jobs = context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}")
    for j in jobs:
        j.schedule_removal()
        print(f"DEBUG: Removed empty queue reminder for {button_id} as a new message was added.")
    # -------------------------------------------------------------
    await update.message.reply_text(f"✅ मीडिया मैसेज जोड़ा गया! (कुल पेंडिंग: {pending_count})")


@owner_only
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.get("action")
    if not action:
        return

    # चैनल जोड़ने का लॉजिक
    if action.startswith("add_channels_"):
        button_id = action.split("_")[-1]
        lines = [ln.strip() for ln in (update.message.text or "").splitlines() if ln.strip()]
        added = 0
        for ch in lines:
            if ch.startswith("@") or (ch.startswith("-") and ch[1:].isdigit()):
                await db_execute("INSERT INTO channels (button_id, channel_id) VALUES (?, ?)", (button_id, ch))
                added += 1
        context.user_data.pop("action", None)
        await update.message.reply_text(f"✅ {added} चैनल सफलतापूर्वक जोड़े गए!")

    # टाइम सेट करने का लॉजिक
    elif action.startswith("set_times_"):
        button_id = action.split("_")[-1]
        lines = [ln.strip() for ln in (update.message.text or "").splitlines() if ln.strip()]
        valid_times = [t for t in lines if is_valid_time_str(t)][:MAX_TIMES_PER_BUTTON]
        
        await db_execute("DELETE FROM schedules WHERE button_id=?", (button_id,))
        for t in valid_times:
            await db_execute("INSERT INTO schedules (button_id, schedule_time) VALUES (?, ?)", (button_id, t))
            
        context.user_data.pop("action", None)
        await update.message.reply_text(f"✅ {len(valid_times)} टाइम सेट किए गए!")

    # टेक्स्ट मैसेज जोड़ने का लॉजिक
    elif action.startswith("add_messages_"):
        button_id = action.split("_")[-1]
        content = update.message.text
        if not content:
            await update.message.reply_text("⚠️ संदेश खाली है!")
            return
        
        logger.info(f"Adding text message to SQLite for button {button_id}")
        await db_execute(
            "INSERT INTO messages (button_id, content, media_type, file_id) VALUES (?, ?, ?, ?)",
            (button_id, content, 'text', None)
        )
    
        pending_count_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
        pending_count = pending_count_rows[0][0]
        # --- मौजूदा रिमाइंडर को रद्द करें क्योंकि अब क्यू खाली नहीं है ---
        jobs = context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}")
        for j in jobs:
            j.schedule_removal()
            print(f"DEBUG: Removed empty queue reminder for {button_id} as a new message was added.")
        # -------------------------------------------------------------
        await update.message.reply_text(f"✅ टेक्स्ट मैसेज जोड़ा गया! (कुल पेंडिंग: {pending_count})")




# =====================
# Misc handlers
# =====================
async def show_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    status_text = await get_button_status(button_id)
    await query.edit_message_text(status_text)


# =====================
# Error Handler
# =====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Exception while handling an update: {context.error}")
    # --- DEBUG PRINT यहाँ डालें ---
    print(f"DEBUG: Error occurred with update object: {update}")
    # ---

    try:
        msg = f"Error: {context.error}\n"
        if update and hasattr(update, "effective_user") and update.effective_user:
            msg += f"User: {update.effective_user.id}\n"
        if update and hasattr(update, 'message') and update.message:
            txt = update.message.text or update.message.caption
            if txt:
                msg += f"Msg: {txt[:300]}\n"
        for admin_id in ADMIN_IDS:
            await context.bot.send_message(admin_id, f"⚠️ Bot Error Alert:\n{msg}")
    except Exception as ex:
        logger.error(f"Failed sending error alert: {ex}")


# =====================
# Main
# =====================
def main() -> None:
    init_db()
    app = Application.builder().token(TOKEN).pool_timeout(60).connect_timeout(60).read_timeout(60).build()

    # UI handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(open_button, pattern=r"^btn[1-5]$"))
    app.add_handler(CallbackQueryHandler(add_channels_prompt, pattern=r"^add_chn_"))
    app.add_handler(CallbackQueryHandler(delete_channel_menu, pattern=r"^del_chn_"))
    app.add_handler(CallbackQueryHandler(confirm_delete_channel, pattern=r"^confirm_del_"))
    app.add_handler(CallbackQueryHandler(final_delete_channel, pattern=r"^final_del_"))
    app.add_handler(CallbackQueryHandler(add_messages_prompt, pattern=r"^add_msg_"))
    app.add_handler(CallbackQueryHandler(set_times_prompt, pattern=r"^set_time_"))
    app.add_handler(CallbackQueryHandler(start_forwarding, pattern=r"^start_fw_"))
    app.add_handler(CallbackQueryHandler(show_status, pattern=r"^status_"))

    # Messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VIDEO, handle_media))

    # Error handler
    app.add_error_handler(error_handler)

    logger.info("Bot started…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
#    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
