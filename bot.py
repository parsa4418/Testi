# -*- coding: utf-8 -*-
"""A configurable Telegram message bot with a Persian admin panel.

The admin composes messages directly in Telegram. The bot stores their message
IDs and uses copyMessage to deliver them, preserving Telegram formatting and
supported media without trying to rebuild entities manually.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import threading
import time
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ChatType
from telegram.error import BadRequest, Forbidden, TelegramError
from flask import Flask, jsonify, request, send_from_directory

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE_DIR / "messenger.sqlite3")))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {1297047765, 8904869158}
MINI_APP_URL = os.getenv("MINI_APP_URL", "").strip().rstrip("/")
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")))

ADMIN_ID = min(ADMIN_IDS)  # Legacy single-admin reference; authorization uses ADMIN_IDS.

# Admin conversation state is deliberately lightweight. All saved content and
# configuration are persisted in SQLite; only an in-progress wizard is in RAM.
ADMIN_STATES: dict[int, dict[str, Any]] = {}

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)
logger = logging.getLogger("messenger_bot")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@contextmanager
def db() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.executescript(
            """
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL CHECK(kind IN ('start', 'after')),
                position INTEGER NOT NULL DEFAULT 0,
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_messages_kind_position
                ON messages(kind, position, id);

            CREATE TABLE IF NOT EXISTS buttons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                admin_name TEXT NOT NULL,
                label TEXT NOT NULL,
                action_type TEXT NOT NULL CHECK(action_type IN ('reply', 'url')),
                action_chat_id INTEGER,
                action_message_id INTEGER,
                action_url TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_buttons_message_id
                ON buttons(message_id, id);

            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                messages_received INTEGER NOT NULL DEFAULT 0
            );
            """
        )


def get_start_message() -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM messages WHERE kind='start' ORDER BY id LIMIT 1"
        ).fetchone()


def get_message(message_id: int) -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()


def list_messages(kind: str) -> list[sqlite3.Row]:
    with db() as conn:
        return list(
            conn.execute(
                "SELECT * FROM messages WHERE kind=? ORDER BY position, id", (kind,)
            ).fetchall()
        )


def add_message(kind: str, message: Any) -> int:
    title = describe_message(message)
    with db() as conn:
        if kind == "start":
            existing = conn.execute(
                "SELECT id FROM messages WHERE kind='start' ORDER BY id LIMIT 1"
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE messages SET source_chat_id=?, source_message_id=?, title=? WHERE id=?",
                    (message.chat_id, message.message_id, title, existing["id"]),
                )
                return int(existing["id"])
            cur = conn.execute(
                "INSERT INTO messages(kind, position, source_chat_id, source_message_id, title) VALUES('start', 0, ?, ?, ?)",
                (message.chat_id, message.message_id, title),
            )
            return int(cur.lastrowid)

        next_pos = conn.execute(
            "SELECT COALESCE(MAX(position), 0) + 1 AS p FROM messages WHERE kind='after'"
        ).fetchone()["p"]
        cur = conn.execute(
            "INSERT INTO messages(kind, position, source_chat_id, source_message_id, title) VALUES('after', ?, ?, ?, ?)",
            (next_pos, message.chat_id, message.message_id, title),
        )
        return int(cur.lastrowid)


def replace_message(message_id: int, message: Any) -> None:
    with db() as conn:
        conn.execute(
            "UPDATE messages SET source_chat_id=?, source_message_id=?, title=? WHERE id=?",
            (message.chat_id, message.message_id, describe_message(message), message_id),
        )


def delete_message(message_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM messages WHERE id=?", (message_id,))
        reorder_after_messages(conn)


def reorder_after_messages(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id FROM messages WHERE kind='after' ORDER BY position, id"
    ).fetchall()
    for position, row in enumerate(rows, start=1):
        conn.execute(
            "UPDATE messages SET position=? WHERE id=?", (position, row["id"])
        )


def move_message(message_id: int, direction: int) -> bool:
    with db() as conn:
        rows = conn.execute(
            "SELECT id, position FROM messages WHERE kind='after' ORDER BY position, id"
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        if message_id not in ids:
            return False
        index = ids.index(message_id)
        new_index = index + direction
        if not 0 <= new_index < len(ids):
            return False
        ids[index], ids[new_index] = ids[new_index], ids[index]
        for position, item_id in enumerate(ids, start=1):
            conn.execute(
                "UPDATE messages SET position=? WHERE id=?", (position, item_id)
            )
        return True


def list_buttons(message_id: int) -> list[sqlite3.Row]:
    with db() as conn:
        return list(
            conn.execute(
                "SELECT * FROM buttons WHERE message_id=? ORDER BY id", (message_id,)
            ).fetchall()
        )


def get_button(button_id: int) -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute("SELECT * FROM buttons WHERE id=?", (button_id,)).fetchone()


def add_button(
    message_id: int,
    admin_name: str,
    label: str,
    action_type: str,
    action_chat_id: int | None = None,
    action_message_id: int | None = None,
    action_url: str | None = None,
) -> int:
    with db() as conn:
        cur = conn.execute(
            """INSERT INTO buttons(
                message_id, admin_name, label, action_type,
                action_chat_id, action_message_id, action_url
            ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
            (
                message_id,
                admin_name,
                label,
                action_type,
                action_chat_id,
                action_message_id,
                action_url,
            ),
        )
        return int(cur.lastrowid)


def update_button(button_id: int, **fields: Any) -> None:
    allowed = {
        "admin_name",
        "label",
        "action_type",
        "action_chat_id",
        "action_message_id",
        "action_url",
    }
    updates = [(key, value) for key, value in fields.items() if key in allowed]
    if not updates:
        return
    clause = ", ".join(f"{key}=?" for key, _ in updates)
    values = [value for _, value in updates]
    values.append(button_id)
    with db() as conn:
        conn.execute(f"UPDATE buttons SET {clause} WHERE id=?", values)


def delete_button(button_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM buttons WHERE id=?", (button_id,))


def touch_user(chat_id: int) -> None:
    with db() as conn:
        conn.execute(
            """INSERT INTO users(chat_id) VALUES(?)
               ON CONFLICT(chat_id) DO UPDATE SET
               last_seen=CURRENT_TIMESTAMP,
               messages_received=messages_received + 1""",
            (chat_id,),
        )


def user_stats() -> tuple[int, int]:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS users, COALESCE(SUM(messages_received), 0) AS msgs FROM users"
        ).fetchone()
        return int(row["users"]), int(row["msgs"])


# ---------------------------------------------------------------------------
# Helpers and UI
# ---------------------------------------------------------------------------

def is_admin_update(update: Update) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    return bool(
        user
        and chat
        and user.id in ADMIN_IDS
        and chat.type == ChatType.PRIVATE
    )


def set_state(user_id: int, state: str, **data: Any) -> None:
    ADMIN_STATES[user_id] = {"state": state, **data}


def get_state(user_id: int) -> dict[str, Any]:
    return ADMIN_STATES.get(user_id, {})


def clear_state(user_id: int) -> None:
    ADMIN_STATES.pop(user_id, None)


def describe_message(message: Any) -> str:
    text = (getattr(message, "text", None) or getattr(message, "caption", None) or "").strip()
    if text:
        text = re.sub(r"\s+", " ", text)
        return text[:42] + ("…" if len(text) > 42 else "")

    types = [
        ("photo", "🖼 عکس"),
        ("video", "🎬 ویدیو"),
        ("animation", "🎞 گیف"),
        ("document", "📎 فایل"),
        ("audio", "🎵 فایل صوتی"),
        ("voice", "🎤 ویس"),
        ("video_note", "⭕ ویدیو مسیج"),
        ("sticker", "🎟 استیکر"),
        ("location", "📍 موقعیت مکانی"),
        ("venue", "🏢 مکان"),
        ("contact", "👤 مخاطب"),
        ("poll", "📊 نظرسنجی"),
        ("dice", "🎲 دایس"),
    ]
    for attr, label in types:
        if getattr(message, attr, None) is not None:
            return label
    return "پیام تلگرامی"


def has_copyable_content(message: Any) -> bool:
    if getattr(message, "text", None) or getattr(message, "caption", None):
        return True
    fields = (
        "photo", "video", "animation", "document", "audio", "voice",
        "video_note", "sticker", "location", "venue", "contact", "poll", "dice",
    )
    return any(getattr(message, field, None) is not None for field in fields)


def short(value: str, limit: int = 45) -> str:
    value = (value or "").replace("\n", " ").strip()
    return value if len(value) <= limit else value[: limit - 1] + "…"


def home_keyboard() -> InlineKeyboardMarkup:
    rows = [
            [InlineKeyboardButton("🚀 پیام استارت", callback_data="home:start")],
            [InlineKeyboardButton("📩 پیام‌های بعد از دریافت پیام", callback_data="home:after")],
            [
                InlineKeyboardButton("👁 پیش‌نمایش استارت", callback_data="home:preview"),
                InlineKeyboardButton("📊 آمار", callback_data="home:stats"),
            ],
            [InlineKeyboardButton("ℹ️ راهنما", callback_data="home:help")],
        ]
    if MINI_APP_URL:
        rows.insert(0, [InlineKeyboardButton("🎨 بازکردن استودیوی طراحی", web_app=WebAppInfo(url=MINI_APP_URL + "/miniapp"))])
    return InlineKeyboardMarkup(rows)


def start_keyboard() -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton("➕ ثبت / تغییر پیام استارت", callback_data="start:set")]
    rows = [row]
    if get_start_message():
        rows.append([InlineKeyboardButton("🔘 مدیریت دکمه‌های استارت", callback_data="start:buttons")])
        rows.append([InlineKeyboardButton("👁 پیش‌نمایش پیام استارت", callback_data="home:preview")])
    rows.append([InlineKeyboardButton("↩️ بازگشت به پنل", callback_data="home:main")])
    return InlineKeyboardMarkup(rows)


def after_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    messages = list_messages("after")
    for index, row in enumerate(messages, start=1):
        label = f"{index:02d} | {short(row['title'], 34)}"
        rows.append([InlineKeyboardButton(label, callback_data=f"msg:open:{row['id']}")])
    rows.append([InlineKeyboardButton("➕ ثبت پیام بعدی", callback_data="after:add")])
    if messages:
        rows.append([InlineKeyboardButton("👁 پیش‌نمایش زنجیره", callback_data="after:preview")])
    rows.append([InlineKeyboardButton("↩️ بازگشت به پنل", callback_data="home:main")])
    return InlineKeyboardMarkup(rows)


def message_keyboard(message_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✏️ جایگزینی محتوا", callback_data=f"msg:replace:{message_id}")],
        [InlineKeyboardButton("🔘 مدیریت دکمه‌های شیشه‌ای", callback_data=f"btn:list:{message_id}")],
        [InlineKeyboardButton("👁 پیش‌نمایش", callback_data=f"msg:preview:{message_id}")],
    ]
    message = get_message(message_id)
    if message and message["kind"] == "after":
        rows.extend(
            [
                [
                    InlineKeyboardButton("⬆️ انتقال به بالا", callback_data=f"msg:up:{message_id}"),
                    InlineKeyboardButton("⬇️ انتقال به پایین", callback_data=f"msg:down:{message_id}"),
                ],
                [InlineKeyboardButton("🗑 حذف پیام", callback_data=f"msg:delete:{message_id}")],
            ]
        )
        rows.append([InlineKeyboardButton("↩️ بازگشت به فهرست پیام‌ها", callback_data="home:after")])
    else:
        rows.append([InlineKeyboardButton("↩️ بازگشت", callback_data="home:start")])
    return InlineKeyboardMarkup(rows)


def button_list_keyboard(message_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for button in list_buttons(message_id):
        rows.append(
            [
                InlineKeyboardButton(
                    f"✏️ {short(button['admin_name'], 42)}",
                    callback_data=f"btn:open:{button['id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("➕ افزودن دکمه شیشه‌ای", callback_data=f"btn:add:{message_id}")])
    rows.append([InlineKeyboardButton("↩️ بازگشت به پیام", callback_data=f"msg:open:{message_id}")])
    return InlineKeyboardMarkup(rows)


def button_edit_keyboard(button_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✏️ تغییر نام مدیریتی", callback_data=f"btn:editname:{button_id}")],
            [InlineKeyboardButton("🔤 تغییر متن روی دکمه", callback_data=f"btn:editlabel:{button_id}")],
            [InlineKeyboardButton("⚙️ تغییر عملکرد دکمه", callback_data=f"btn:editaction:{button_id}")],
            [InlineKeyboardButton("🗑 حذف دکمه", callback_data=f"btn:delete:{button_id}")],
            [InlineKeyboardButton("↩️ بازگشت به فهرست دکمه‌ها", callback_data=f"btn:listfrom:{button_id}")],
        ]
    )


def action_choice_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📨 با کلیک، یک پیام ارسال شود", callback_data=f"{prefix}:reply")],
            [InlineKeyboardButton("🔗 با کلیک، لینک باز شود", callback_data=f"{prefix}:url")],
            [InlineKeyboardButton("✖️ انصراف", callback_data="wizard:cancel")],
        ]
    )


def user_keyboard(message_id: int) -> InlineKeyboardMarkup | None:
    buttons = list_buttons(message_id)
    if not buttons:
        return None
    keyboard: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for button in buttons:
        if button["action_type"] == "url":
            item = InlineKeyboardButton(button["label"], url=button["action_url"])
        else:
            item = InlineKeyboardButton(button["label"], callback_data=f"do:{button['id']}")
        current_row.append(item)
        if len(current_row) == 2:
            keyboard.append(current_row)
            current_row = []
    if current_row:
        keyboard.append(current_row)
    return InlineKeyboardMarkup(keyboard)


async def show_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "╭━━━ ✦ پنل پیام‌رسان ✦ ━━━╮\n"
        "│  مرکز طراحی و مدیریت پیام‌ها\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "از بخش‌های زیر پیام‌هایت را بساز. متن، عکس، ویدیو، فایل، استیکر و قالب‌بندی را مستقیم از تلگرام بفرست؛ لازم نیست چیزی را دستی کدنویسی کنی."
    )
    if update.callback_query:
        await safe_edit(update.callback_query, text, home_keyboard())
    else:
        await update.effective_message.reply_text(text, reply_markup=home_keyboard())


async def safe_edit(query: Any, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    # Panels intentionally use plain text; strip Markdown markers from authored labels.
    text = text.replace("**", "")
    try:
        await query.edit_message_text(text=text, reply_markup=markup)
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            await query.message.reply_text(text, reply_markup=markup)


async def copy_saved_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    row: sqlite3.Row,
) -> None:
    await context.bot.copy_message(
        chat_id=chat_id,
        from_chat_id=int(row["source_chat_id"]),
        message_id=int(row["source_message_id"]),
        reply_markup=user_keyboard(int(row["id"])),
    )


# ---------------------------------------------------------------------------
# Commands and incoming messages
# ---------------------------------------------------------------------------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return

    if is_admin_update(update):
        clear_state(update.effective_user.id)
        await show_panel(update, context)
        return

    user_id = update.effective_chat.id
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO users(chat_id) VALUES(?)", (user_id,))
        conn.execute(
            "UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE chat_id=?", (user_id,)
        )

    start_message = get_start_message()
    if start_message:
        try:
            await copy_saved_message(context, user_id, start_message)
        except TelegramError:
            logger.exception("Could not copy the configured start message to %s", user_id)
            await context.bot.send_message(
                chat_id=user_id,
                text="سلام! پیام شروع موقتاً در دسترس نیست. لطفاً پیام خودت را بفرست.",
            )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="سلام! هنوز پیام استارت تنظیم نشده؛ پیام خودت را بفرست.",
        )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_update(update):
        return
    clear_state(update.effective_user.id)
    await show_panel(update, context)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_update(update):
        return
    clear_state(update.effective_user.id)
    await update.effective_message.reply_text(
        "عملیات لغو شد.", reply_markup=home_keyboard()
    )


async def receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user or chat.type != ChatType.PRIVATE:
        return

    if user.id in ADMIN_IDS:
        state = get_state(user.id)
        stage = state.get("state")
        if not stage:
            return
        if not has_copyable_content(message):
            await message.reply_text(
                "این نوع پیام قابل ثبت نیست. یک متن، عکس، ویدیو، فایل، استیکر یا پیام پشتیبانی‌شده بفرست؛ یا /cancel را بزن."
            )
            return

        if stage == "start_message":
            saved_id = add_message("start", message)
            clear_state(user.id)
            await message.reply_text(
                "✅ پیام استارت ذخیره شد. قالب‌بندی، نقل‌قول و ایموجی سفارشی با کپی پیام حفظ می‌شوند.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🔘 مدیریت دکمه‌های استارت", callback_data="start:buttons")],
                        [InlineKeyboardButton("👁 پیش‌نمایش", callback_data="home:preview")],
                        [InlineKeyboardButton("↩️ پنل اصلی", callback_data="home:main")],
                    ]
                ),
            )
            return

        if stage == "after_message":
            saved_id = add_message("after", message)
            clear_state(user.id)
            row = get_message(saved_id)
            index = int(row["position"]) if row else len(list_messages("after"))
            await message.reply_text(
                f"✅ پیام شمارهٔ {index} ثبت شد.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🔘 افزودن دکمه شیشه‌ای", callback_data=f"btn:list:{saved_id}")],
                        [InlineKeyboardButton("➕ ثبت پیام بعدی", callback_data="after:add")],
                        [InlineKeyboardButton("📋 فهرست پیام‌ها", callback_data="home:after")],
                    ]
                ),
            )
            return

        if stage == "replace_message":
            message_id = int(state["message_id"])
            replace_message(message_id, message)
            clear_state(user.id)
            await message.reply_text(
                "✅ محتوای پیام جایگزین شد.",
                reply_markup=message_keyboard(message_id),
            )
            return

        if stage == "button_reply_message":
            draft = state
            add_button(
                message_id=int(draft["message_id"]),
                admin_name=str(draft["admin_name"]),
                label=str(draft["label"]),
                action_type="reply",
                action_chat_id=message.chat_id,
                action_message_id=message.message_id,
            )
            message_id = int(draft["message_id"])
            clear_state(user.id)
            await message.reply_text(
                "✅ دکمه ساخته شد و به همین پیام وصل شد.",
                reply_markup=button_list_keyboard(message_id),
            )
            return

        if stage == "button_edit_reply":
            button_id = int(state["button_id"])
            update_button(
                button_id,
                action_type="reply",
                action_chat_id=message.chat_id,
                action_message_id=message.message_id,
                action_url=None,
            )
            clear_state(user.id)
            button = get_button(button_id)
            await message.reply_text(
                "✅ پیامِ پشت دکمه تغییر کرد.",
                reply_markup=button_edit_keyboard(button_id),
            )
            return

        if stage in {"button_name", "button_label", "button_edit_name", "button_edit_label"}:
            value = (message.text or message.caption or "").strip()
            if not value:
                await message.reply_text("لطفاً مقدار را به صورت متن بفرست.")
                return

            if stage in {"button_label", "button_edit_label"} and len(value) > 64:
                await message.reply_text("متن دکمه حداکثر ۶۴ نویسه باشد؛ کوتاه‌تر بفرست.")
                return

            if stage == "button_name":
                ADMIN_STATES[user.id]["admin_name"] = value[:80]
                ADMIN_STATES[user.id]["state"] = "button_label"
                await message.reply_text("حالا متن نمایشی دکمه را بفرست (حداکثر ۶۴ نویسه).")
                return

            if stage == "button_label":
                ADMIN_STATES[user.id]["label"] = value[:64]
                await message.reply_text(
                    "عملکرد دکمه را انتخاب کن:",
                    reply_markup=action_choice_keyboard("ba"),
                )
                ADMIN_STATES[user.id]["state"] = "button_action_choice"
                return

            if stage == "button_edit_name":
                button_id = int(state["button_id"])
                update_button(button_id, admin_name=value[:80])
                clear_state(user.id)
                button = get_button(button_id)
                await message.reply_text(
                    "✅ نام مدیریتی دکمه تغییر کرد.",
                    reply_markup=button_edit_keyboard(button_id),
                )
                return

            if stage == "button_edit_label":
                button_id = int(state["button_id"])
                update_button(button_id, label=value[:64])
                clear_state(user.id)
                await message.reply_text(
                    "✅ متن روی دکمه تغییر کرد.",
                    reply_markup=button_edit_keyboard(button_id),
                )
                return

        if stage in {"button_url", "button_edit_url"}:
            value = (message.text or "").strip()
            if not re.match(r"^(https?://|tg://)\S+$", value, re.IGNORECASE):
                await message.reply_text(
                    "لینک معتبر بفرست که با https:// یا http:// یا tg:// شروع شود."
                )
                return
            if stage == "button_url":
                add_button(
                    message_id=int(state["message_id"]),
                    admin_name=str(state["admin_name"]),
                    label=str(state["label"]),
                    action_type="url",
                    action_url=value,
                )
                message_id = int(state["message_id"])
                clear_state(user.id)
                await message.reply_text(
                    "✅ دکمهٔ لینک‌دار ثبت شد.",
                    reply_markup=button_list_keyboard(message_id),
                )
                return

            button_id = int(state["button_id"])
            update_button(
                button_id,
                action_type="url",
                action_chat_id=None,
                action_message_id=None,
                action_url=value,
            )
            clear_state(user.id)
            await message.reply_text(
                "✅ لینکِ دکمه تغییر کرد.",
                reply_markup=button_edit_keyboard(button_id),
            )
            return

        return

    # The public-facing bot works in private chats. Each ordinary user message
    # triggers the configured response sequence once.
    if message.text and message.text.startswith("/"):
        return

    touch_user(chat.id)
    sequence = list_messages("after")
    if not sequence:
        await message.reply_text("پیامت دریافت شد ✅")
        return

    for index, saved in enumerate(sequence):
        try:
            await copy_saved_message(context, chat.id, saved)
        except (Forbidden, TelegramError):
            logger.exception(
                "Could not deliver response step %s to user %s", index + 1, chat.id
            )
            # Continue the sequence if one stored item is no longer copyable.
        if index < len(sequence) - 1:
            await asyncio.sleep(1.05)


# ---------------------------------------------------------------------------
# Callback handling
# ---------------------------------------------------------------------------

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    user = query.from_user

    # Runtime button actions are available to all users.
    if data.startswith("do:"):
        try:
            button_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("این دکمه معتبر نیست.", show_alert=True)
            return
        button = get_button(button_id)
        if not button or button["action_type"] != "reply":
            await query.answer("عملکرد این دکمه در دسترس نیست.", show_alert=True)
            return
        await query.answer()
        try:
            await context.bot.copy_message(
                chat_id=query.message.chat_id if query.message else user.id,
                from_chat_id=int(button["action_chat_id"]),
                message_id=int(button["action_message_id"]),
            )
        except TelegramError:
            logger.exception("Could not send button response %s", button_id)
            if query.message:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="متأسفانه پیام این دکمه فعلاً قابل ارسال نیست.",
                )
        return

    if user.id not in ADMIN_IDS or not query.message or query.message.chat.type != ChatType.PRIVATE:
        await query.answer("این بخش فقط برای مدیر بات است.", show_alert=True)
        return

    await query.answer()

    # Main navigation
    if data == "home:main":
        clear_state(user.id)
        await show_panel(update, context)
        return

    if data == "home:start":
        clear_state(user.id)
        row = get_start_message()
        desc = f"وضعیت: ✅ ثبت شده\nمحتوا: {row['title']}" if row else "وضعیت: هنوز ثبت نشده"
        await safe_edit(
            query,
            "🚀 **مدیریت پیام استارت**\n\n" + desc + "\n\nاین پیام با دستور /start برای کاربران فرستاده می‌شود.",
            start_keyboard(),
        )
        return

    if data == "home:after":
        clear_state(user.id)
        messages = list_messages("after")
        text = (
            "📩 **پیام‌های بعد از دریافت پیام**\n\n"
            f"تعداد پیام‌های ثبت‌شده: {len(messages)}\n"
            "هر بار که کاربر یک پیام برای بات بفرستد، پیام‌های این فهرست به ترتیب ارسال می‌شوند."
        )
        await safe_edit(query, text, after_keyboard())
        return

    if data == "home:preview":
        clear_state(user.id)
        row = get_start_message()
        if not row:
            await query.message.reply_text("هنوز پیام استارت ثبت نشده است.")
            return
        try:
            await copy_saved_message(context, query.message.chat_id, row)
        except TelegramError:
            logger.exception("Could not preview start message")
            await query.message.reply_text("پیش‌نمایش این پیام ممکن نشد.")
        return

    if data == "home:stats":
        users, messages = user_stats()
        await safe_edit(
            query,
            "📊 **آمار بات**\n\n"
            f"👥 کاربران ثبت‌شده: {users:,}\n"
            f"📨 پیام‌های دریافتی: {messages:,}",
            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="home:main")]]),
        )
        return

    if data == "home:help":
        help_text = (
            "ℹ️ **راهنمای سریع**\n\n"
            "۱) پیام استارت را باز کن و متن یا رسانه‌ات را مستقیم برای بات بفرست.\n"
            "۲) در بخش پیام‌های بعد از دریافت، هر تعداد پیام خواستی به ترتیب ثبت کن.\n"
            "۳) برای هر پیام، از بخش دکمه‌های شیشه‌ای دکمه بساز.\n"
            "۴) هر دکمه یک نام مدیریتی، یک متن نمایشی و یک عملکرد دارد: ارسال پیام یا بازکردن لینک.\n\n"
            "برای لغو هر مرحله /cancel را بفرست. محتوای پیام‌ها با شناسهٔ پیام اصلی ذخیره می‌شود تا قالب‌بندی و رسانه بازسازیِ دستی نشود."
        )
        await safe_edit(
            query,
            help_text,
            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ بازگشت", callback_data="home:main")]]),
        )
        return

    # Start message setup
    if data == "start:set":
        set_state(user.id, "start_message")
        await query.message.reply_text(
            "🚀 پیام استارت جدید را همین‌جا بفرست.\n\nمی‌توانی متن قالب‌بندی‌شده، عکس، ویدیو، فایل، استیکر و موارد پشتیبانی‌شده را ارسال کنی. پیام قبلی جایگزین می‌شود.\n\nبرای انصراف /cancel"
        )
        return

    if data == "start:buttons":
        row = get_start_message()
        if not row:
            await query.message.reply_text("اول پیام استارت را ثبت کن.")
            return
        await safe_edit(
            query,
            "🔘 **دکمه‌های شیشه‌ای پیام استارت**\n\nبرای ویرایش هر دکمه روی نام مدیریتی آن بزن.",
            button_list_keyboard(int(row["id"])),
        )
        return

    # After-message chain
    if data == "after:add":
        set_state(user.id, "after_message")
        await query.message.reply_text(
            "📩 پیام بعدیِ زنجیره را بفرست.\n\nاین می‌تواند متن، عکس، ویدیو، فایل، استیکر، ویس و پیام‌های پشتیبانی‌شده باشد. ترتیب ارسال مطابق شمارهٔ پیام‌هاست.\n\nبرای انصراف /cancel"
        )
        return

    if data == "after:preview":
        messages = list_messages("after")
        if not messages:
            await query.message.reply_text("هنوز پیامی ثبت نشده است.")
            return
        await query.message.reply_text("👁 پیش‌نمایش زنجیره:")
        for index, saved in enumerate(messages):
            try:
                await copy_saved_message(context, query.message.chat_id, saved)
            except TelegramError:
                logger.exception("Could not preview after-message %s", saved["id"])
            if index < len(messages) - 1:
                await asyncio.sleep(0.5)
        return

    # Message item operations
    if data.startswith("msg:open:"):
        message_id = int(data.rsplit(":", 1)[1])
        row = get_message(message_id)
        if not row:
            await query.message.reply_text("این پیام پیدا نشد.")
            return
        index_text = "پیام استارت" if row["kind"] == "start" else f"پیام شمارهٔ {row['position']}"
        await safe_edit(
            query,
            f"📝 **{index_text}**\n\nمحتوا: {row['title']}\nشناسه: {message_id}\n\nاز گزینه‌های زیر برای مدیریت استفاده کن.",
            message_keyboard(message_id),
        )
        return

    if data.startswith("msg:replace:"):
        message_id = int(data.rsplit(":", 1)[1])
        if not get_message(message_id):
            await query.message.reply_text("این پیام پیدا نشد.")
            return
        set_state(user.id, "replace_message", message_id=message_id)
        await query.message.reply_text("محتوای جدید را بفرست تا جایگزین محتوای قبلی شود.\nبرای انصراف /cancel")
        return

    if data.startswith("msg:preview:"):
        message_id = int(data.rsplit(":", 1)[1])
        row = get_message(message_id)
        if not row:
            await query.message.reply_text("این پیام پیدا نشد.")
            return
        try:
            await copy_saved_message(context, query.message.chat_id, row)
        except TelegramError:
            logger.exception("Could not preview message %s", message_id)
            await query.message.reply_text("پیش‌نمایش این پیام ممکن نشد.")
        return

    if data.startswith("msg:delete:"):
        message_id = int(data.rsplit(":", 1)[1])
        delete_message(message_id)
        await safe_edit(query, "🗑 پیام و دکمه‌های متصل به آن حذف شدند.", after_keyboard())
        return

    if data.startswith("msg:up:") or data.startswith("msg:down:"):
        direction = -1 if data.startswith("msg:up:") else 1
        message_id = int(data.rsplit(":", 1)[1])
        moved = move_message(message_id, direction)
        row = get_message(message_id)
        if row:
            await safe_edit(
                query,
                "✅ ترتیب پیام تغییر کرد." if moved else "این پیام در آن جهت جابه‌جا نمی‌شود.",
                message_keyboard(message_id),
            )
        return

    # Button management
    if data.startswith("btn:list:"):
        message_id = int(data.rsplit(":", 1)[1])
        await safe_edit(
            query,
            "🔘 **مدیریت دکمه‌های شیشه‌ای**\n\nدکمه‌های ثبت‌شده در این فهرست نمایش داده می‌شوند. برای ویرایش، نام مدیریتی را انتخاب کن.",
            button_list_keyboard(message_id),
        )
        return

    if data.startswith("btn:add:"):
        message_id = int(data.rsplit(":", 1)[1])
        if not get_message(message_id):
            await query.message.reply_text("پیام موردنظر پیدا نشد.")
            return
        set_state(user.id, "button_name", message_id=message_id)
        await query.message.reply_text(
            "اسم مدیریتی دکمه را بفرست.\n\nاین اسم فقط در پنل مدیریت دیده می‌شود تا دکمه‌ها را راحت پیدا کنی."
        )
        return

    if data.startswith("btn:open:"):
        button_id = int(data.rsplit(":", 1)[1])
        button = get_button(button_id)
        if not button:
            await query.message.reply_text("این دکمه پیدا نشد.")
            return
        action = "ارسال یک پیام" if button["action_type"] == "reply" else f"بازکردن لینک: {button['action_url']}"
        await safe_edit(
            query,
            "🔘 **ویرایش دکمه**\n\n"
            f"نام مدیریتی: {button['admin_name']}\n"
            f"متن روی دکمه: {button['label']}\n"
            f"عملکرد: {action}",
            button_edit_keyboard(button_id),
        )
        return

    if data.startswith("btn:editname:"):
        button_id = int(data.rsplit(":", 1)[1])
        set_state(user.id, "button_edit_name", button_id=button_id)
        await query.message.reply_text("نام مدیریتی جدید را بفرست.")
        return

    if data.startswith("btn:editlabel:"):
        button_id = int(data.rsplit(":", 1)[1])
        set_state(user.id, "button_edit_label", button_id=button_id)
        await query.message.reply_text("متن جدیدی که کاربر روی دکمه می‌بیند را بفرست (حداکثر ۶۴ نویسه).")
        return

    if data.startswith("btn:editaction:"):
        button_id = int(data.rsplit(":", 1)[1])
        set_state(user.id, "button_edit_action_choice", button_id=button_id)
        await query.message.reply_text(
            "عملکرد جدید دکمه را انتخاب کن:",
            reply_markup=action_choice_keyboard("be"),
        )
        return

    if data.startswith("btn:delete:"):
        button_id = int(data.rsplit(":", 1)[1])
        button = get_button(button_id)
        message_id = int(button["message_id"]) if button else 0
        delete_button(button_id)
        await safe_edit(
            query,
            "🗑 دکمه حذف شد.",
            button_list_keyboard(message_id) if message_id else home_keyboard(),
        )
        return

    if data.startswith("btn:listfrom:"):
        button_id = int(data.rsplit(":", 1)[1])
        button = get_button(button_id)
        if button:
            message_id = int(button["message_id"])
            await safe_edit(query, "🔘 **فهرست دکمه‌ها**", button_list_keyboard(message_id))
        return

    # Add-button action choice
    if data == "ba:reply":
        state = get_state(user.id)
        if state.get("state") != "button_action_choice":
            return
        state["state"] = "button_reply_message"
        await query.message.reply_text(
            "حالا پیامی را بفرست که با زدن این دکمه برای کاربر ارسال شود. متن یا رسانه قابل قبول است.\nبرای انصراف /cancel"
        )
        return

    if data == "ba:url":
        state = get_state(user.id)
        if state.get("state") != "button_action_choice":
            return
        state["state"] = "button_url"
        await query.message.reply_text("لینکی را بفرست که با زدن دکمه باز شود.")
        return

    # Edit-button action choice
    if data.startswith("be:reply"):
        state = get_state(user.id)
        if state.get("state") != "button_edit_action_choice":
            return
        state["state"] = "button_edit_reply"
        await query.message.reply_text("پیام جدیدی را بفرست که با زدن دکمه ارسال شود.")
        return

    if data.startswith("be:url"):
        state = get_state(user.id)
        if state.get("state") != "button_edit_action_choice":
            return
        state["state"] = "button_edit_url"
        await query.message.reply_text("لینک جدید را بفرست (https:// یا http:// یا tg://).")
        return

    if data == "wizard:cancel":
        clear_state(user.id)
        await query.message.reply_text("ساخت دکمه لغو شد.", reply_markup=home_keyboard())
        return


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception while processing update", exc_info=context.error)


# ---------------------------------------------------------------------------
# Telegram Mini App (Flask sidecar in the same process)
# ---------------------------------------------------------------------------

web_app = Flask(__name__, static_folder="miniapp", static_url_path="/assets")


def validate_webapp_init_data(init_data: str) -> tuple[bool, int | None]:
    """Validate Telegram Mini App initData and return the authenticated user id."""
    if not BOT_TOKEN or not init_data:
        return False, None
    from urllib.parse import parse_qsl
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = pairs.pop("hash", "")
        if not received_hash:
            return False, None
        check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calculated, received_hash):
            return False, None
        auth_date = int(pairs.get("auth_date", "0"))
        if abs(int(time.time()) - auth_date) > 86400:
            return False, None
        user = json.loads(pairs.get("user", "{}"))
        return True, int(user.get("id", 0))
    except (ValueError, TypeError, json.JSONDecodeError):
        return False, None


def mini_admin():
    ok, uid = validate_webapp_init_data(request.headers.get("X-Telegram-Init-Data", ""))
    return bool(ok and uid in ADMIN_IDS)


@web_app.get("/miniapp")
def miniapp_index():
    return send_from_directory(BASE_DIR / "miniapp", "index.html")


@web_app.get("/api/state")
def api_state():
    if not mini_admin():
        return jsonify({"error": "شما اجازه دسترسی به این بخش را ندارید❌"}), 403
    start = get_start_message()
    after = list_messages("after")
    items = ([start] if start else []) + after
    result = []
    for row in items:
        mid = int(row["id"])
        result.append({
            "id": mid, "kind": row["kind"], "position": int(row["position"]),
            "title": row["title"], "buttons": [
                {"id": int(b["id"]), "admin_name": b["admin_name"], "label": b["label"],
                 "action_type": b["action_type"], "action_url": b["action_url"]}
                for b in list_buttons(mid)
            ]
        })
    users, messages_count = user_stats()
    return jsonify({"messages": result, "users": users, "received": messages_count})


@web_app.post("/api/messages/<int:message_id>/delete")
def api_delete_message(message_id: int):
    if not mini_admin():
        return jsonify({"error": "شما اجازه دسترسی به این بخش را ندارید❌"}), 403
    row = get_message(message_id)
    if not row or row["kind"] != "after":
        return jsonify({"error": "not_found_or_protected"}), 404
    delete_message(message_id)
    return jsonify({"ok": True})


@web_app.post("/api/messages/reorder")
def api_reorder_messages():
    if not mini_admin():
        return jsonify({"error": "شما اجازه دسترسی به این بخش را ندارید❌"}), 403
    ids = request.json.get("ids", []) if request.is_json else []
    current = [int(x["id"]) for x in list_messages("after")]
    try:
        requested = [int(x) for x in ids]
    except (ValueError, TypeError):
        return jsonify({"error": "invalid_ids"}), 400
    if len(requested) != len(current) or set(requested) != set(current):
        return jsonify({"error": "ids_must_match"}), 400
    with db() as conn:
        for pos, mid in enumerate(requested, start=1):
            conn.execute("UPDATE messages SET position=? WHERE id=?", (pos, mid))
    return jsonify({"ok": True})


@web_app.post("/api/buttons")
def api_add_button():
    if not mini_admin():
        return jsonify({"error": "شما اجازه دسترسی به این بخش را ندارید❌"}), 403
    data = request.get_json(silent=True) or {}
    try:
        mid = int(data.get("message_id"))
        name = str(data.get("admin_name", "")).strip()
        label = str(data.get("label", "")).strip()
        action = str(data.get("action_type", "url"))
        if not get_message(mid) or not name or not label or len(label) > 64:
            raise ValueError
        if action == "url":
            url = str(data.get("action_url", "")).strip()
            if not (url.startswith("https://") or url.startswith("http://") or url.startswith("tg://")):
                raise ValueError
            bid = add_button(mid, name, label, "url", action_url=url)
        elif action == "reply":
            target = get_message(int(data.get("reply_message_id")))
            if not target:
                raise ValueError
            bid = add_button(mid, name, label, "reply", int(target["source_chat_id"]), int(target["source_message_id"]))
        else:
            raise ValueError
        return jsonify({"ok": True, "id": bid})
    except (ValueError, TypeError):
        return jsonify({"error": "invalid_button_data"}), 400


@web_app.patch("/api/buttons/<int:button_id>")
def api_edit_button(button_id: int):
    if not mini_admin():
        return jsonify({"error": "شما اجازه دسترسی به این بخش را ندارید❌"}), 403
    button = get_button(button_id)
    if not button:
        return jsonify({"error": "not_found"}), 404
    data = request.get_json(silent=True) or {}
    fields = {}
    for key in ("admin_name", "label"):
        if key in data:
            value = str(data[key]).strip()
            if not value or (key == "label" and len(value) > 64):
                return jsonify({"error": "invalid_value"}), 400
            fields[key] = value
    if "action_url" in data:
        url = str(data["action_url"]).strip()
        if not (url.startswith("https://") or url.startswith("http://") or url.startswith("tg://")):
            return jsonify({"error": "invalid_url"}), 400
        fields.update(action_type="url", action_url=url, action_chat_id=None, action_message_id=None)
    if fields:
        update_button(button_id, **fields)
    return jsonify({"ok": True})


@web_app.delete("/api/buttons/<int:button_id>")
def api_remove_button(button_id: int):
    if not mini_admin():
        return jsonify({"error": "شما اجازه دسترسی به این بخش را ندارید❌"}), 403
    if not get_button(button_id):
        return jsonify({"error": "not_found"}), 404
    delete_button(button_id)
    return jsonify({"ok": True})


def run_web_server() -> None:
    web_app.run(host=WEB_HOST, port=WEB_PORT, threaded=True, use_reloader=False)


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing. Copy .env.example to .env and set your token.")

    init_db()
    if MINI_APP_URL:
        threading.Thread(target=run_web_server, daemon=True, name="miniapp-web").start()
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.ALL, receive_message))
    application.add_error_handler(error_handler)

    logger.info("Messenger bot is starting. Admin IDs: %s", sorted(ADMIN_IDS))
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()
