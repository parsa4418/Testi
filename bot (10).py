# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, json, hmac, hashlib, sqlite3, threading, logging, asyncio
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qsl
from flask import Flask, jsonify, request, send_from_directory
from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Kb, WebAppInfo
from telegram.constants import ChatType
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

BASE = Path(__file__).resolve().parent
TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {1297047765, 8904869158}
PORT = int(os.getenv("PORT", "10000"))
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE / "data" / "messenger.sqlite3")))
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
MINI_APP_URL = (os.getenv("MINI_APP_URL", "").strip().rstrip("/") or RENDER_URL)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gugu-messenger")
app = Flask(__name__, static_folder=str(BASE / "miniapp"))

states = {}      # admin input flows (temporary, in memory)
locks = {}       # one lock per user chat -> no duplicate sends
clicks = {}      # button click throttle

# ───────────────────────── Database ─────────────────────────
def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    return c

@contextmanager
def tx():
    c = connect()
    try:
        yield c
        c.commit()
    finally:
        c.close()

def init_db():
    with tx() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS messages(
          id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
          position INTEGER NOT NULL DEFAULT 0, chat_id INTEGER NOT NULL,
          message_id INTEGER NOT NULL, title TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS buttons(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          admin_name TEXT NOT NULL, label TEXT NOT NULL,
          action_type TEXT NOT NULL, action_chat_id INTEGER,
          action_message_id INTEGER, action_url TEXT);
        CREATE TABLE IF NOT EXISTS users(
          chat_id INTEGER PRIMARY KEY, first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
          last_seen TEXT DEFAULT CURRENT_TIMESTAMP, received INTEGER DEFAULT 0);
        """)
        cols = {r["name"] for r in c.execute("PRAGMA table_info(users)")}
        if "step" not in cols:   # how many scenario messages were sent to this user
            c.execute("ALTER TABLE users ADD COLUMN step INTEGER NOT NULL DEFAULT 0")
        if "status" not in cols: # new | waiting | done
            c.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'new'")

def rows(sql, args=()):
    with tx() as c: return c.execute(sql, args).fetchall()

def one(sql, args=()):
    with tx() as c: return c.execute(sql, args).fetchone()

def execute(sql, args=(), count=False):
    with tx() as c:
        cur = c.execute(sql, args)
        return cur.rowcount if count else cur.lastrowid

# ───────────────────────── Scenario data ─────────────────────────
def scenario():
    """Ordered scenario messages: the start message first, then the rest by position."""
    return rows("SELECT * FROM messages WHERE kind IN ('start','after') "
                "ORDER BY CASE kind WHEN 'start' THEN 0 ELSE 1 END, position, id")

def msg_by_id(mid):
    return one("SELECT * FROM messages WHERE id=?", (mid,))

def set_order(ids):
    with tx() as c:
        for i, mid in enumerate(ids):
            c.execute("UPDATE messages SET kind=?,position=? WHERE id=?", ("start" if i == 0 else "after", i, mid))

def add_scenario_message(m):
    if one("SELECT id FROM messages WHERE kind='start' LIMIT 1"):
        pos = one("SELECT COALESCE(MAX(position),0)+1 p FROM messages WHERE kind='after'")["p"]
        return execute("INSERT INTO messages(kind,position,chat_id,message_id,title) VALUES('after',?,?,?,?)",
                       (pos, m.chat_id, m.message_id, title_of(m)))
    return execute("INSERT INTO messages(kind,position,chat_id,message_id,title) VALUES('start',0,?,?,?)",
                   (m.chat_id, m.message_id, title_of(m)))

def add_reply_message(m):
    # Button replies are stored apart from the scenario so they never become scenario steps.
    return execute("INSERT INTO messages(kind,position,chat_id,message_id,title) VALUES('reply',0,?,?,?)",
                   (m.chat_id, m.message_id, title_of(m)))

def title_of(m):
    s = (m.text or m.caption or "").strip()
    if s: return " ".join(s.split())[:42]
    for k, v in [("photo","🖼 عکس"),("video","🎬 ویدیو"),("animation","🎞 گیف"),("document","📎 فایل"),
                 ("audio","🎵 صوت"),("voice","🎤 ویس"),("sticker","🎟 استیکر"),("video_note","⭕ ویدیو مسیج")]:
        if getattr(m, k, None): return v
    return "پیام تلگرامی"

def get_buttons(mid):
    return rows("SELECT * FROM buttons WHERE message_id=? ORDER BY id", (mid,))

def add_button(mid, label, typ, url=None, cid=None, mid2=None):
    return execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_chat_id,action_message_id,action_url) "
                   "VALUES(?,?,?,?,?,?,?)", (mid, label, label, typ, cid, mid2, url))

def user_markup(mid):
    bs = get_buttons(mid)
    if not bs: return None
    keys = [Btn(b["label"], url=b["action_url"]) if b["action_type"] == "url"
            else Btn(b["label"], callback_data=f"send:{b['id']}") for b in bs]
    return Kb([keys[i:i+2] for i in range(0, len(keys), 2)])

async def copy_saved(bot, chat_id, record, markup=True):
    await bot.copy_message(chat_id=chat_id, from_chat_id=record["chat_id"], message_id=record["message_id"],
                           reply_markup=user_markup(record["id"]) if markup else None)

# ───────────────────────── Scenario engine (per user) ─────────────────────────
def lock_for(cid): return locks.setdefault(cid, asyncio.Lock())

def touch(cid):
    execute("INSERT INTO users(chat_id,received) VALUES(?,1) ON CONFLICT(chat_id) DO UPDATE SET "
            "last_seen=CURRENT_TIMESTAMP, received=received+1", (cid,))

async def begin(bot, cid):
    """/start: restart the scenario from the beginning and send only message #1."""
    async with lock_for(cid):
        execute("INSERT INTO users(chat_id) VALUES(?) ON CONFLICT(chat_id) DO UPDATE SET step=0,status='new'", (cid,))
        seq = scenario()
        if not seq:
            await bot.send_message(cid, "سلام! هنوز پیامی برای این ربات تنظیم نشده است.")
            return
        execute("UPDATE users SET step=1,status='waiting' WHERE chat_id=?", (cid,))
        try:
            await copy_saved(bot, cid, seq[0])
        except Exception:
            log.exception("Could not deliver first message")
            execute("UPDATE users SET step=0,status='new' WHERE chat_id=?", (cid,))

async def advance(bot, cid, trigger=None):
    """User replied (or pressed a button of the pending message): send exactly ONE next message."""
    async with lock_for(cid):
        u = one("SELECT step,status FROM users WHERE chat_id=?", (cid,))
        if not u or u["status"] != "waiting": return
        seq, step = scenario(), u["step"]
        if trigger is not None and not (0 < step <= len(seq) and seq[step-1]["id"] == trigger):
            return  # a button of an older message: don't skip ahead
        if step >= len(seq):  # final reply received -> scenario finished, nothing more is sent
            execute("UPDATE users SET status='done' WHERE chat_id=?", (cid,))
            return
        if not execute("UPDATE users SET step=step+1 WHERE chat_id=? AND step=? AND status='waiting'", (cid, step), count=True):
            return
        try:
            await copy_saved(bot, cid, seq[step])
        except Exception:
            log.exception("Could not deliver scenario message")
            execute("UPDATE users SET step=? WHERE chat_id=? AND status='waiting'", (step, cid))

async def user_click(bot, q, bid):
    b = one("SELECT * FROM buttons WHERE id=?", (bid,))
    cid = q.message.chat_id if q.message else q.from_user.id
    if not b: return
    now = time.monotonic()
    if now - clicks.get((cid, bid), 0) < 1.5: return  # ignore double taps
    clicks[(cid, bid)] = now
    if b["action_type"] == "reply" and b["action_chat_id"]:
        try:
            await bot.copy_message(chat_id=cid, from_chat_id=b["action_chat_id"], message_id=b["action_message_id"])
        except Exception:
            log.exception("Button reply failed")
    await advance(bot, cid, trigger=b["message_id"])

# ───────────────────────── Admin panel views ─────────────────────────
def kc(n): return "".join(d + "\ufe0f\u20e3" for d in str(n))
def admin(update):
    u, ch = update.effective_user, update.effective_chat
    return bool(u and ch and u.id in ADMIN_IDS and ch.type == ChatType.PRIVATE)
TYPES = {"url": "🔗 لینک", "reply": "💬 ارسال پیام", "next": "➡️ ادامه سناریو"}

def home_view():
    n = len(scenario()); u = one("SELECT COUNT(*) n FROM users")["n"]
    text = f"🏛 GUGU EMPIRE MESSENGER\nپنل مدیریت سناریو\n\n📨 پیام‌ها: {n}   👥 کاربران: {u}"
    kb = [[Btn("➕ افزودن پیام", callback_data="p:add"), Btn("📊 آمار", callback_data="p:stats")],
          [Btn("📋 نمایش فهرست کامل پیام‌ها", callback_data="p:list")],
          [Btn("🧪 تست سناریو", callback_data="p:test")]]
    if MINI_APP_URL: kb.append([Btn("🎨 استودیوی طراحی", web_app=WebAppInfo(url=MINI_APP_URL + "/miniapp"))])
    return text, Kb(kb)

def list_view():
    seq = scenario()
    head = "📋 فهرست پیام‌های سناریو\n\n"
    if not seq:
        return head + "هنوز پیامی ثبت نشده است.", Kb([[Btn("➕ افزودن پیام", callback_data="p:add")], [Btn("🔙 پنل", callback_data="p:home")]])
    lines = []
    for i, m in enumerate(seq, 1):
        nb = len(get_buttons(m["id"]))
        lines.append(f"{kc(i)} {m['title']}" + (f"  · 🔘{nb}" if nb else ""))
    text = head + "\n".join(lines) + f"\n\n📊 تعداد کل پیام‌ها: {len(seq)}\n\nبرای مدیریت هر پیام، شماره‌اش را بزن:"
    nums = [Btn(str(i), callback_data=f"m:{m['id']}") for i, m in enumerate(seq, 1)]
    kb = [nums[i:i+5] for i in range(0, len(nums), 5)]
    kb.append([Btn("➕ افزودن پیام", callback_data="p:add"), Btn("🔙 پنل", callback_data="p:home")])
    return text, Kb(kb)

def stats_view():
    def cnt(status=None):
        if status: return one("SELECT COUNT(*) n FROM users WHERE status=?", (status,))["n"]
        return one("SELECT COUNT(*) n FROM users")["n"]
    text = ("📊 آمار\n\n"
            f"👥 کل کاربران: {cnt()}\n"
            f"⏳ در حال دریافت سناریو: {cnt('waiting')}\n"
            f"✅ تمام‌شده: {cnt('done')}\n"
            f"📨 پیام‌های سناریو: {len(scenario())}")
    return text, Kb([[Btn("🔙 پنل", callback_data="p:home")]])

def msg_view(mid):
    seq = scenario(); ids = [m["id"] for m in seq]
    if mid not in ids: return list_view()
    i = ids.index(mid); m = seq[i]
    text = (f"{kc(i+1)} پیام شماره {i+1} از {len(seq)}\n{'⭐ پیام شروع' if i == 0 else '📩 مرحله ' + str(i+1)}\n\n"
            f"📝 {m['title']}\n🔘 دکمه‌ها: {len(get_buttons(mid))}")
    move = []
    if i > 0: move.append(Btn("⬆️ بالا", callback_data=f"mu:{mid}"))
    if i < len(seq) - 1: move.append(Btn("⬇️ پایین", callback_data=f"md:{mid}"))
    kb = [[Btn("👁 پیش‌نمایش", callback_data=f"pv:{mid}"), Btn("🔘 دکمه‌ها", callback_data=f"b:{mid}")]]
    if move: kb.append(move)
    kb += [[Btn("✏️ جایگزینی محتوا", callback_data=f"mr:{mid}"), Btn("🗑 حذف", callback_data=f"mx:{mid}")],
           [Btn("🔙 فهرست", callback_data="p:list")]]
    return text, Kb(kb)

def btns_view(mid):
    bs = get_buttons(mid)
    text = "🔘 دکمه‌های این پیام\n\n" + ("\n".join(f"• {b['label']} — {TYPES.get(b['action_type'], '')}" for b in bs)
                                       if bs else "هنوز دکمه‌ای ندارد.")
    kb = [[Btn(b["label"], callback_data=f"be:{b['id']}")] for b in bs]
    kb.append([Btn("➕ دکمه جدید", callback_data=f"ba:{mid}")])
    kb.append([Btn("🔙 بازگشت", callback_data=f"m:{mid}")])
    return text, Kb(kb)

def btn_view(bid):
    b = one("SELECT * FROM buttons WHERE id=?", (bid,))
    if not b: return home_view()
    text = f"🔘 دکمه: {b['label']}\nعملکرد: {TYPES.get(b['action_type'], '')}"
    if b["action_type"] == "url": text += f"\n🔗 {b['action_url']}"
    kb = [[Btn("✏️ تغییر متن", callback_data=f"bl:{bid}")]]
    if b["action_type"] == "url": kb.append([Btn("🔗 تغییر لینک", callback_data=f"bu:{bid}")])
    if b["action_type"] == "reply":
        kb.append([Btn("💬 تغییر پاسخ", callback_data=f"br:{bid}"), Btn("👁 پیش‌نمایش پاسخ", callback_data=f"bp:{bid}")])
    kb.append([Btn("🗑 حذف دکمه", callback_data=f"bx:{bid}")])
    kb.append([Btn("🔙 بازگشت", callback_data=f"b:{b['message_id']}")])
    return text, Kb(kb)

def cancel_kb(back): return Kb([[Btn("❌ انصراف", callback_data=back)]])

async def show(q, text, markup=None):
    try:
        await q.edit_message_text(text, reply_markup=markup)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            await q.message.reply_text(text, reply_markup=markup)

# ───────────────────────── Handlers ─────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type != ChatType.PRIVATE: return
    touch(chat.id)
    if admin(update):
        states.pop(update.effective_user.id, None)
        t, k = home_view(); await update.message.reply_text(t, reply_markup=k)
        return
    await begin(context.bot, chat.id)

async def admin_cmd(update, context):
    if not admin(update): return
    states.pop(update.effective_user.id, None)
    t, k = home_view(); await update.message.reply_text(t, reply_markup=k)

async def test_cmd(update, context):
    if not admin(update): return
    states.pop(update.effective_user.id, None)
    touch(update.effective_chat.id)
    await update.message.reply_text("🧪 حالت تست: سناریو را مثل یک کاربر ببین. برای خروج /cancel")
    await begin(context.bot, update.effective_chat.id)

async def cancel(update, context):
    if not update.effective_user: return
    states.pop(update.effective_user.id, None)
    if admin(update):
        execute("UPDATE users SET status='done' WHERE chat_id=? AND status='waiting'", (update.effective_chat.id,))
    t, k = home_view() if admin(update) else ("لغو شد.", None)
    await update.message.reply_text(t, reply_markup=k)

async def admin_input(update, context, st):
    m = update.message; kind = st["kind"]; text = (m.text or "").strip()
    if kind == "add":
        mid = add_scenario_message(m); n = len(scenario())
        await m.reply_text(f"✅ پیام {n} ثبت شد.\nپیام بعدی را بفرست یا پایان را بزن.",
                           reply_markup=Kb([[Btn("🔘 دکمه‌های همین پیام", callback_data=f"b:{mid}"), Btn("✅ پایان", callback_data="p:list")]]))
    elif kind == "replace":
        execute("UPDATE messages SET chat_id=?,message_id=?,title=? WHERE id=?", (m.chat_id, m.message_id, title_of(m), st["mid"]))
        states.pop(update.effective_user.id, None)
        await m.reply_text("✅ محتوا جایگزین شد.", reply_markup=msg_view(st["mid"])[1])
    elif kind in ("btn_label", "edit_label"):
        if not text or len(text) > 64:
            await m.reply_text("یک متن کوتاه (حداکثر ۶۴ حرف) بفرست."); return
        if kind == "edit_label":
            execute("UPDATE buttons SET label=?,admin_name=? WHERE id=?", (text, text, st["bid"]))
            states.pop(update.effective_user.id, None)
            v = btn_view(st["bid"]); await m.reply_text(v[0], reply_markup=v[1]); return
        st["label"], st["kind"] = text, "btn_type"
        await m.reply_text("عملکرد دکمه چیست؟", reply_markup=Kb([
            [Btn("💬 ارسال پیام ذخیره‌شده", callback_data="act:reply")],
            [Btn("🔗 بازکردن لینک", callback_data="act:url")],
            [Btn("➡️ فقط ادامه سناریو", callback_data="act:next")]]))
    elif kind in ("btn_url", "edit_url"):
        if not text.startswith(("https://", "http://", "tg://")):
            await m.reply_text("لینک معتبر بفرست (https:// یا http:// یا tg://)."); return
        if kind == "edit_url":
            execute("UPDATE buttons SET action_type='url',action_url=?,action_chat_id=NULL,action_message_id=NULL WHERE id=?", (text, st["bid"]))
            back = btn_view(st["bid"])
        else:
            add_button(st["message_id"], st["label"], "url", url=text); back = btns_view(st["message_id"])
        states.pop(update.effective_user.id, None)
        await m.reply_text("✅ ذخیره شد.\n\n" + back[0], reply_markup=back[1])
    elif kind in ("btn_reply", "edit_reply"):
        rid = add_reply_message(m); rec = msg_by_id(rid)
        if kind == "edit_reply":
            execute("UPDATE buttons SET action_type='reply',action_chat_id=?,action_message_id=?,action_url=NULL WHERE id=?",
                    (rec["chat_id"], rec["message_id"], st["bid"])); back = btn_view(st["bid"])
        else:
            add_button(st["message_id"], st["label"], "reply", cid=rec["chat_id"], mid2=rec["message_id"]); back = btns_view(st["message_id"])
        states.pop(update.effective_user.id, None)
        await m.reply_text("✅ ذخیره شد.\n\n" + back[0], reply_markup=back[1])
    else:
        await m.reply_text("یکی از گزینه‌های بالا را انتخاب کن یا /cancel بزن.")

async def incoming(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or update.effective_chat.type != ChatType.PRIVATE: return
    cid, uid = update.effective_chat.id, update.effective_user.id
    if admin(update):
        st = states.get(uid)
        if st:
            await admin_input(update, context, st); return
        u = one("SELECT status FROM users WHERE chat_id=?", (cid,))
        if not (u and u["status"] == "waiting"):
            await msg.reply_text("برای مدیریت /admin بزن؛ برای تست سناریو /test."); return
    touch(cid)
    u = one("SELECT status FROM users WHERE chat_id=?", (cid,))
    if u["status"] == "new": await begin(context.bot, cid)
    elif u["status"] == "waiting": await advance(context.bot, cid)
    # status 'done': scenario is finished, nothing else is sent

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u, data, bot = q.from_user, q.data or "", context.bot
    if data.startswith("send:"):
        await user_click(bot, q, int(data[5:])); return
    if u.id not in ADMIN_IDS or not q.message or q.message.chat.type != ChatType.PRIVATE: return
    key, _, arg = data.partition(":"); n = int(arg) if arg.isdigit() else 0
    if key != "act": states.pop(u.id, None)
    if key == "p":
        if arg == "home": await show(q, *home_view())
        elif arg in ("list", "done"): await show(q, *list_view())
        elif arg == "stats": await show(q, *stats_view())
        elif arg == "add":
            states[u.id] = {"kind": "add"}
            await show(q, "➕ پیام‌ها را یکی‌یکی بفرست (متن، عکس، ویدیو، ویس، فایل و...).\nهر پیام یک مرحله از سناریو می‌شود.",
                       Kb([[Btn("✅ پایان", callback_data="p:list")]]))
        elif arg == "test":
            touch(q.message.chat_id)
            await q.message.reply_text("🧪 حالت تست: سناریو را مثل یک کاربر ببین. برای خروج /cancel")
            await begin(bot, q.message.chat_id)
    elif key == "m": await show(q, *msg_view(n))
    elif key == "pv":
        rec = msg_by_id(n)
        if rec: await copy_saved(bot, q.message.chat_id, rec)
    elif key in ("mu", "md"):
        ids = [m["id"] for m in scenario()]
        if n in ids:
            i = ids.index(n); j = i - 1 if key == "mu" else i + 1
            if 0 <= j < len(ids): ids[i], ids[j] = ids[j], ids[i]; set_order(ids)
        await show(q, *msg_view(n))
    elif key == "mx":
        await show(q, "⚠️ این پیام و دکمه‌هایش حذف شود؟", Kb([[Btn("✅ بله، حذف", callback_data=f"mxx:{n}"), Btn("❌ خیر", callback_data=f"m:{n}")]]))
    elif key == "mxx":
        execute("DELETE FROM messages WHERE id=?", (n,)); set_order([m["id"] for m in scenario()])
        await show(q, *list_view())
    elif key == "mr":
        states[u.id] = {"kind": "replace", "mid": n}
        await show(q, "✏️ محتوای جدید را بفرست (جایگزین پیام فعلی می‌شود؛ دکمه‌ها حفظ می‌شوند).", cancel_kb(f"m:{n}"))
    elif key == "b": await show(q, *btns_view(n))
    elif key == "ba":
        states[u.id] = {"kind": "btn_label", "message_id": n}
        await show(q, "➕ متن دکمه را بفرست (مثلاً: ادامه ✅):", cancel_kb(f"b:{n}"))
    elif key == "be": await show(q, *btn_view(n))
    elif key in ("bl", "bu", "br"):
        b = one("SELECT message_id FROM buttons WHERE id=?", (n,))
        if not b: return
        states[u.id] = {"kind": {"bl": "edit_label", "bu": "edit_url", "br": "edit_reply"}[key], "bid": n}
        ask = {"bl": "متن جدید دکمه را بفرست:", "bu": "لینک جدید را بفرست:", "br": "پیام پاسخ جدید را بفرست:"}[key]
        await show(q, ask, cancel_kb(f"be:{n}"))
    elif key == "bp":
        b = one("SELECT * FROM buttons WHERE id=?", (n,))
        if b and b["action_chat_id"]:
            await bot.copy_message(chat_id=q.message.chat_id, from_chat_id=b["action_chat_id"], message_id=b["action_message_id"])
    elif key == "bx":
        b = one("SELECT message_id FROM buttons WHERE id=?", (n,))
        if b:
            execute("DELETE FROM buttons WHERE id=?", (n,)); await show(q, *btns_view(b["message_id"]))
    elif key == "act":
        st = states.get(u.id)
        if not st or st.get("kind") != "btn_type":
            await show(q, "این مرحله منقضی شده؛ دوباره از پنل شروع کن.", Kb([[Btn("🔙 پنل", callback_data="p:home")]])); return
        mid = st["message_id"]
        if arg == "next":
            add_button(mid, st["label"], "next"); states.pop(u.id, None); await show(q, *btns_view(mid))
        elif arg == "url":
            st["kind"] = "btn_url"; await show(q, "🔗 لینک را بفرست:", cancel_kb(f"b:{mid}"))
        elif arg == "reply":
            st["kind"] = "btn_reply"; await show(q, "💬 پیامی را که با زدن دکمه ارسال شود بفرست:", cancel_kb(f"b:{mid}"))

# ───────────────────────── Mini App API (unchanged contract) ─────────────────────────
def valid_init_data(raw):
    if not TOKEN or not raw: return False, None
    try:
        p = dict(parse_qsl(raw, keep_blank_values=True)); received = p.pop("hash", "")
        check = "\n".join(f"{k}={v}" for k, v in sorted(p.items()))
        secret = hmac.new(b"WebAppData", TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, received): return False, None
        if abs(int(time.time()) - int(p.get("auth_date", "0"))) > 86400: return False, None
        user = json.loads(p.get("user", "{}")); return True, int(user.get("id", 0))
    except Exception: return False, None

def mini_admin():
    ok, uid = valid_init_data(request.headers.get("X-Telegram-Init-Data", ""))
    return ok and uid in ADMIN_IDS

DENIED = "شما اجازه دسترسی به این بخش را ندارید❌"

@app.get("/")
def home(): return send_from_directory(BASE / "miniapp", "index.html")

@app.get("/miniapp")
def miniapp(): return send_from_directory(BASE / "miniapp", "index.html")

@app.get("/healthz")
def health(): return jsonify({"ok": True, "service": "gugu-empire-messenger"})

@app.get("/api/state")
def state_api():
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    result = []
    for m in scenario():
        bs = get_buttons(m["id"])
        result.append({"id": m["id"], "kind": m["kind"], "position": m["position"], "title": m["title"],
                       "buttons": [{"id": b["id"], "admin_name": b["admin_name"], "label": b["label"],
                                    "action_type": b["action_type"], "action_url": b["action_url"]} for b in bs]})
    users = one("SELECT COUNT(*) n FROM users")["n"]; received = one("SELECT COALESCE(SUM(received),0) n FROM users")["n"]
    return jsonify({"messages": result, "users": users, "received": received})

@app.post("/api/messages/<int:mid>/delete")
def delete_api(mid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    m = one("SELECT kind FROM messages WHERE id=?", (mid,))
    if not m or m["kind"] != "after": return jsonify({"error": "not_found_or_protected"}), 404
    execute("DELETE FROM messages WHERE id=?", (mid,))
    set_order([r["id"] for r in scenario()])
    return jsonify({"ok": True})

@app.post("/api/messages/reorder")
def reorder_api():
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    ids = (request.get_json(silent=True) or {}).get("ids", [])
    current = [r["id"] for r in rows("SELECT id FROM messages WHERE kind='after' ORDER BY position,id")]
    try: ids = [int(x) for x in ids]
    except Exception: return jsonify({"error": "invalid_ids"}), 400
    if len(ids) != len(current) or set(ids) != set(current): return jsonify({"error": "ids_must_match"}), 400
    with tx() as c:
        for pos, mid in enumerate(ids, 1): c.execute("UPDATE messages SET position=? WHERE id=?", (pos, mid))
    return jsonify({"ok": True})

@app.post("/api/buttons")
def add_button_api():
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    d = request.get_json(silent=True) or {}
    try:
        mid = int(d["message_id"]); name = str(d["admin_name"]).strip(); label = str(d["label"]).strip(); typ = d["action_type"]
        if not name or not label or len(label) > 64 or not msg_by_id(mid): raise ValueError()
        if typ == "url":
            url = str(d.get("action_url", "")).strip()
            if not url.startswith(("https://", "http://", "tg://")): raise ValueError()
            execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_url) VALUES(?,?,?,'url',?)", (mid, name, label, url))
        elif typ == "reply":
            target = msg_by_id(int(d["reply_message_id"]))
            if not target: raise ValueError()
            execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_chat_id,action_message_id) VALUES(?,?,?,'reply',?,?)",
                    (mid, name, label, target["chat_id"], target["message_id"]))
        else: raise ValueError()
        return jsonify({"ok": True})
    except Exception: return jsonify({"error": "invalid_button_data"}), 400

@app.patch("/api/buttons/<int:bid>")
def edit_button_api(bid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    b = one("SELECT * FROM buttons WHERE id=?", (bid,))
    if not b: return jsonify({"error": "not_found"}), 404
    d = request.get_json(silent=True) or {}
    name = str(d.get("admin_name", b["admin_name"])).strip(); label = str(d.get("label", b["label"])).strip()
    if not name or not label or len(label) > 64: return jsonify({"error": "invalid_value"}), 400
    if "action_url" in d:
        url = str(d["action_url"]).strip()
        if not url.startswith(("https://", "http://", "tg://")): return jsonify({"error": "invalid_url"}), 400
        execute("UPDATE buttons SET admin_name=?,label=?,action_type='url',action_url=?,action_chat_id=NULL,action_message_id=NULL WHERE id=?", (name, label, url, bid))
    else: execute("UPDATE buttons SET admin_name=?,label=? WHERE id=?", (name, label, bid))
    return jsonify({"ok": True})

@app.delete("/api/buttons/<int:bid>")
def remove_button_api(bid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    execute("DELETE FROM buttons WHERE id=?", (bid,)); return jsonify({"ok": True})

# ───────────────────────── Run ─────────────────────────
def run_bot():
    if not TOKEN: raise RuntimeError("BOT_TOKEN is missing")
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_cmd))
    application.add_handler(CommandHandler("test", test_cmd))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CallbackQueryHandler(callbacks))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, incoming))
    log.info("Bot polling thread starting")
    application.run_polling(drop_pending_updates=False, stop_signals=None)

if __name__ == "__main__":
    init_db()
    if not TOKEN: raise SystemExit("Set BOT_TOKEN in Render Environment.")
    threading.Thread(target=run_bot, daemon=True, name="telegram-bot").start()
    log.info("Flask Mini App listening on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
