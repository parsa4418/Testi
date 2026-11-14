# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, time, json, hmac, hashlib, html, threading, logging, asyncio
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qsl
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from flask import Flask, jsonify, request, send_from_directory
from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Kb, WebAppInfo
from telegram.constants import ChatType
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

BASE = Path(__file__).resolve().parent
TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {1297047765, 8904869158}
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"): DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
MINI_APP_URL = (os.getenv("MINI_APP_URL", "").strip().rstrip("/") or RENDER_URL)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gugu-messenger")
app = Flask(__name__, static_folder=str(BASE / "miniapp"))

states = {}      # admin input flows (temporary, in memory)
locks = {}       # one lock per user chat -> no duplicate sends
clicks = {}      # button click throttle

# ───────────────────────── Database ─────────────────────────
NOW = "to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
ID_TABLES = {"messages", "buttons", "flows", "flow_steps", "submissions"}
pool = None

def open_pool():
    global pool
    pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=6, open=False, check=ConnectionPool.check_connection,
                          kwargs={"row_factory": dict_row, "prepare_threshold": None})
    pool.open(wait=True, timeout=30)

class Conn:
    """Thin wrapper: write SQL with '?' placeholders, run on Postgres."""
    def __init__(self, c): self.c = c
    def execute(self, sql, args=()): return self.c.execute(sql.replace("?", "%s"), args)

@contextmanager
def tx():
    with pool.connection() as c:   # commits on success, rolls back on error
        yield Conn(c)

def init_db():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS messages(
      id BIGSERIAL PRIMARY KEY, kind TEXT NOT NULL,
      position INTEGER NOT NULL DEFAULT 0, chat_id BIGINT NOT NULL,
      message_id BIGINT NOT NULL, title TEXT NOT NULL DEFAULT '');
    CREATE TABLE IF NOT EXISTS buttons(
      id BIGSERIAL PRIMARY KEY,
      message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
      admin_name TEXT NOT NULL, label TEXT NOT NULL,
      action_type TEXT NOT NULL, action_chat_id BIGINT,
      action_message_id BIGINT, action_url TEXT);
    CREATE TABLE IF NOT EXISTS users(
      chat_id BIGINT PRIMARY KEY, first_seen TEXT DEFAULT {NOW},
      last_seen TEXT DEFAULT {NOW}, received BIGINT DEFAULT 0,
      step INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'new');
    CREATE TABLE IF NOT EXISTS flows(
      id BIGSERIAL PRIMARY KEY,
      button_id BIGINT NOT NULL REFERENCES buttons(id) ON DELETE CASCADE,
      name TEXT NOT NULL DEFAULT '');
    CREATE TABLE IF NOT EXISTS flow_steps(
      id BIGSERIAL PRIMARY KEY,
      flow_id BIGINT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
      position INTEGER NOT NULL DEFAULT 0,
      kind TEXT NOT NULL,
      text TEXT, chat_id BIGINT, message_id BIGINT,
      title TEXT NOT NULL DEFAULT '',
      field TEXT, input_type TEXT, btn_label TEXT);
    CREATE TABLE IF NOT EXISTS flow_runs(
      chat_id BIGINT PRIMARY KEY, flow_id BIGINT NOT NULL, step_id BIGINT NOT NULL,
      answers TEXT NOT NULL DEFAULT '[]', who TEXT NOT NULL DEFAULT '{{}}');
    CREATE TABLE IF NOT EXISTS submissions(
      id BIGSERIAL PRIMARY KEY, flow_id BIGINT, flow_name TEXT,
      chat_id BIGINT, username TEXT, full_name TEXT, answers TEXT NOT NULL DEFAULT '[]',
      status TEXT NOT NULL DEFAULT 'new', created TEXT DEFAULT {NOW})
    """
    with tx() as c:
        for stmt in ddl.split(";"):
            if stmt.strip(): c.execute(stmt)

def rows(sql, args=()):
    with tx() as c: return c.execute(sql, args).fetchall()

def one(sql, args=()):
    with tx() as c: return c.execute(sql, args).fetchone()

def execute(sql, args=(), count=False):
    m = re.match(r"\s*INSERT INTO (\w+)", sql, re.I)
    ret = bool(m and m.group(1).lower() in ID_TABLES and not count and "RETURNING" not in sql.upper())
    if ret: sql += " RETURNING id"
    with tx() as c:
        cur = c.execute(sql, args)
        if count: return cur.rowcount
        return cur.fetchone()["id"] if ret else None

# ───────────────────────── Scenario data ─────────────────────────
def scenario():
    """Ordered scenario messages: the start message first, then the rest by position."""
    return rows("SELECT * FROM messages WHERE kind IN ('start','after') "
                "ORDER BY CASE kind WHEN 'start' THEN 0 ELSE 1 END, position, id")

def msg_by_id(mid):
    return one("SELECT * FROM messages WHERE id=?", (mid,))

GROUPS = {"start": ("🚀 پیام‌های استارت", 0), "after": ("📩 ادامه پیام‌ها", 1)}

def group(kind):
    return rows("SELECT * FROM messages WHERE kind=? ORDER BY position,id", (kind,))

def renumber():
    """Keep positions tidy inside each group (start: 0.., after: 1..)."""
    with tx() as c:
        for kind, (_, base) in GROUPS.items():
            ids = [r["id"] for r in c.execute("SELECT id FROM messages WHERE kind=? ORDER BY position,id", (kind,)).fetchall()]
            for i, mid in enumerate(ids, base): c.execute("UPDATE messages SET position=? WHERE id=?", (i, mid))

def move_msg(mid, d):
    m = msg_by_id(mid)
    if not m or m["kind"] not in GROUPS: return
    ids = [r["id"] for r in group(m["kind"])]; i = ids.index(mid); j = i + d
    if not 0 <= j < len(ids): return
    ids[i], ids[j] = ids[j], ids[i]
    with tx() as c:
        for k, x in enumerate(ids, GROUPS[m["kind"]][1]): c.execute("UPDATE messages SET position=? WHERE id=?", (k, x))

def switch_group(mid):
    m = msg_by_id(mid)
    if not m or m["kind"] not in GROUPS: return
    new = "after" if m["kind"] == "start" else "start"
    pos = one("SELECT COALESCE(MAX(position),-1)+1 p FROM messages WHERE kind=?", (new,))["p"]
    execute("UPDATE messages SET kind=?,position=? WHERE id=?", (new, pos, mid)); renumber()

def add_scenario_message(m, grp="after"):
    grp = grp if grp in GROUPS else "after"
    pos = one("SELECT COALESCE(MAX(position),CAST(? AS INTEGER))+1 p FROM messages WHERE kind=?", (GROUPS[grp][1] - 1, grp))["p"]
    return execute("INSERT INTO messages(kind,position,chat_id,message_id,title) VALUES(?,?,?,?,?)",
                   (grp, pos, m.chat_id, m.message_id, title_of(m)))

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
            f"last_seen={NOW}, received=users.received+1", (cid,))

def n_start(seq): return sum(1 for m in seq if m["kind"] == "start")

async def begin(bot, cid):
    """/start: restart the scenario; send ALL start messages at once, then wait for the user's reply."""
    async with lock_for(cid):
        execute("INSERT INTO users(chat_id) VALUES(?) ON CONFLICT(chat_id) DO UPDATE SET step=0,status='new'", (cid,))
        seq = scenario()
        if not seq:
            await bot.send_message(cid, "سلام! هنوز پیامی برای این ربات تنظیم نشده است.")
            return
        ns = n_start(seq) or 1          # no start messages -> the first staged message acts as start
        execute("UPDATE users SET step=?,status='waiting' WHERE chat_id=?", (ns, cid))
        sent = 0
        for m in seq[:ns]:
            try:
                await copy_saved(bot, cid, m); sent += 1
            except Exception:
                log.exception("Could not deliver start message")
            if ns > 1: await asyncio.sleep(0.4)
        if not sent:
            execute("UPDATE users SET step=0,status='new' WHERE chat_id=?", (cid,))

async def advance(bot, cid, trigger=None):
    """User replied (or pressed a button of a pending message): send exactly ONE next staged message."""
    async with lock_for(cid):
        u = one("SELECT step,status FROM users WHERE chat_id=?", (cid,))
        if not u or u["status"] != "waiting": return
        seq, step = scenario(), u["step"]
        if trigger is not None:
            ns = n_start(seq) or 1
            pending = {m["id"] for m in seq[:ns]} if step <= ns else ({seq[step-1]["id"]} if step <= len(seq) else set())
            if trigger not in pending: return   # button of an older message: don't skip ahead
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

# ───────────────────────── Flow engine (button -> multi-step conversation) ─────────────────────────
_DIG = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
ITYPES = {"text": "📝 متن", "number": "🔢 عدد", "card": "💳 شماره کارت", "photo": "📷 عکس"}
SKIND = {"ask": "❓ سوال", "info": "💬 پیام", "confirm": "✅ تایید"}

def flow_steps(fid): return rows("SELECT * FROM flow_steps WHERE flow_id=? ORDER BY position,id", (fid,))
def get_run(cid): return one("SELECT * FROM flow_runs WHERE chat_id=?", (cid,))
def drop_run(cid): execute("DELETE FROM flow_runs WHERE chat_id=?", (cid,))
def esc(x): return html.escape(str(x), quote=False)

def add_step(fid, kind, **kw):
    pos = one("SELECT COALESCE(MAX(position),0)+1 p FROM flow_steps WHERE flow_id=?", (fid,))["p"]
    return execute("INSERT INTO flow_steps(flow_id,position,kind,text,chat_id,message_id,title,field,input_type,btn_label) "
                   "VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (fid, pos, kind, kw.get("text"), kw.get("chat_id"), kw.get("message_id"), kw.get("title", ""),
                    kw.get("field"), kw.get("input_type"), kw.get("btn_label")))

def set_step_order(ids):
    with tx() as c:
        for i, sid in enumerate(ids, 1): c.execute("UPDATE flow_steps SET position=? WHERE id=?", (i, sid))

def validate(msg, t):
    txt = (msg.text or "").strip()
    if t == "photo":
        if msg.photo or (msg.document and (msg.document.mime_type or "").startswith("image/")): return True, "📷 عکس"
        return False, "❌ لطفاً یک عکس بفرست."
    if not txt: return False, "❌ لطفاً به‌صورت متن بفرست."
    if t == "number":
        d = re.sub(r"[\s,،]", "", txt.translate(_DIG))
        return (True, d) if d.isdigit() else (False, "❌ فقط عدد بفرست.")
    if t == "card":
        d = re.sub(r"[\s-]", "", txt.translate(_DIG))
        return (True, " ".join(d[i:i+4] for i in range(0, 16, 4))) if re.fullmatch(r"\d{16}", d) \
            else (False, "❌ شماره کارت باید ۱۶ رقم باشد.")
    return True, txt[:1000]

def summary_text(answers):
    return "📋 اطلاعات واردشده:\n\n" + "\n".join(f"▫️ {a['label']}: {a['value']}" for a in answers)

def step_markup(s):
    cancel = Btn("❌ انصراف", callback_data="fq")
    if s["kind"] == "ask": return Kb([[cancel]])
    if s["kind"] == "confirm": return Kb([[Btn(s["btn_label"] or "✅ تایید", callback_data=f"fc:{s['id']}")], [cancel]])
    return None

async def send_step(bot, cid, s):
    mk = step_markup(s)
    if s["chat_id"]:
        await bot.copy_message(chat_id=cid, from_chat_id=s["chat_id"], message_id=s["message_id"], reply_markup=mk)
    else:
        await bot.send_message(cid, s["text"] or "…", reply_markup=mk)

async def flow_continue(bot, cid, fid, idx, answers, who):
    """Send steps from idx on, pause at the first ask/confirm, finish when steps run out."""
    steps = flow_steps(fid)
    while idx < len(steps):
        s = steps[idx]
        try:
            if s["kind"] == "confirm" and answers: await bot.send_message(cid, summary_text(answers))
            await send_step(bot, cid, s)
        except Exception:
            log.exception("Flow step failed"); drop_run(cid)
            try: await bot.send_message(cid, "⚠️ خطایی رخ داد. لطفاً دوباره تلاش کن.")
            except Exception: pass
            return
        if s["kind"] in ("ask", "confirm"):
            execute("INSERT INTO flow_runs(chat_id,flow_id,step_id,answers,who) VALUES(?,?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET "
                    "flow_id=excluded.flow_id,step_id=excluded.step_id,answers=excluded.answers,who=excluded.who",
                    (cid, fid, s["id"], json.dumps(answers, ensure_ascii=False), who))
            return
        idx += 1
        await asyncio.sleep(0.3)
    drop_run(cid)
    await finish_flow(bot, cid, fid, answers, who)

async def start_flow(bot, cid, b, user):
    fl = one("SELECT * FROM flows WHERE button_id=?", (b["id"],))
    async with lock_for(cid):
        drop_run(cid)
        if not fl or not flow_steps(fl["id"]):
            await bot.send_message(cid, "این بخش هنوز تنظیم نشده است."); return
        who = json.dumps({"id": user.id, "u": user.username, "n": user.full_name}, ensure_ascii=False)
        await flow_continue(bot, cid, fl["id"], 0, [], who)

async def flow_input(bot, msg):
    cid = msg.chat_id
    async with lock_for(cid):
        run = get_run(cid)
        if not run: return
        steps = flow_steps(run["flow_id"])
        idx = next((i for i, s in enumerate(steps) if s["id"] == run["step_id"]), None)
        if idx is None:
            drop_run(cid); await msg.reply_text("این بخش تغییر کرده؛ لطفاً دوباره /start بزن."); return
        s = steps[idx]
        if s["kind"] == "confirm":
            await msg.reply_text("لطفاً با دکمهٔ پیام بالا ادامه بده یا انصراف بزن."); return
        ok, val = validate(msg, s["input_type"] or "text")
        if not ok: await msg.reply_text(val); return
        answers = json.loads(run["answers"] or "[]")
        answers.append({"label": s["field"] or s["title"], "type": s["input_type"] or "text", "value": val,
                        "chat_id": msg.chat_id, "message_id": msg.message_id})
        await flow_continue(bot, cid, run["flow_id"], idx + 1, answers, run["who"])

async def flow_confirm(bot, q, sid):
    cid = q.message.chat_id if q.message else q.from_user.id
    async with lock_for(cid):
        run = get_run(cid)
        if not run or run["step_id"] != sid: return   # old / duplicate press
        steps = flow_steps(run["flow_id"])
        idx = next((i for i, s in enumerate(steps) if s["id"] == sid), None)
        if idx is None: drop_run(cid); return
        try: await q.edit_message_reply_markup(reply_markup=None)
        except Exception: pass
        await flow_continue(bot, cid, run["flow_id"], idx + 1, json.loads(run["answers"] or "[]"), run["who"])

async def flow_cancel(bot, q):
    cid = q.message.chat_id if q.message else q.from_user.id
    drop_run(cid)
    try: await q.edit_message_reply_markup(reply_markup=None)
    except Exception: pass
    await bot.send_message(cid, "❌ لغو شد. برای شروع دوباره /start را بزن.")

async def finish_flow(bot, cid, fid, answers, who_json):
    if not answers: return
    fl = one("SELECT name FROM flows WHERE id=?", (fid,)); name = fl["name"] if fl else ""
    who = json.loads(who_json or "{}")
    sid = execute("INSERT INTO submissions(flow_id,flow_name,chat_id,username,full_name,answers) VALUES(?,?,?,?,?,?)",
                  (fid, name, cid, who.get("u") or "", who.get("n") or "", json.dumps(answers, ensure_ascii=False)))
    user = f'<a href="tg://user?id={cid}">{esc(who.get("n") or cid)}</a>' + (f' (@{esc(who["u"])})' if who.get("u") else "")
    text = (f"📥 <b>درخواست جدید #{sid}</b> — {esc(name)}\n👤 {user}\n🆔 <code>{cid}</code>\n\n"
            + "\n".join(f"▫️ <b>{esc(a['label'])}:</b> {esc(a['value'])}" for a in answers))
    kb = Kb([[Btn("✅ انجام شد", callback_data=f"sd:{sid}")]])
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, text, parse_mode="HTML", reply_markup=kb)
            for a in answers:
                if a["type"] == "photo": await bot.copy_message(aid, a["chat_id"], a["message_id"])
        except Exception:
            log.warning("Could not notify admin %s", aid)

async def user_click(bot, q, bid):
    b = one("SELECT * FROM buttons WHERE id=?", (bid,))
    cid = q.message.chat_id if q.message else q.from_user.id
    if not b: return
    now = time.monotonic()
    if now - clicks.get((cid, bid), 0) < 1.5: return  # ignore double taps
    clicks[(cid, bid)] = now
    if b["action_type"] == "flow":   # opens a multi-step flow, does not advance the main scenario
        await start_flow(bot, cid, b, q.from_user); return
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
TYPES = {"url": "🔗 لینک", "reply": "💬 ارسال پیام", "next": "➡️ ادامه سناریو", "flow": "🧩 مسیر چندمرحله‌ای"}

def new_count(): return one("SELECT COUNT(*) n FROM submissions WHERE status='new'")["n"]

def home_view():
    n = len(scenario()); u = one("SELECT COUNT(*) n FROM users")["n"]
    o = new_count()
    text = f"🏛 GUGU EMPIRE MESSENGER\nپنل مدیریت سناریو\n\n📨 پیام‌ها: {n}   👥 کاربران: {u}   📥 درخواست جدید: {o}"
    kb = [[Btn("🚀 پیام‌های استارت", callback_data="p:adds"), Btn("📩 ادامه پیام‌ها", callback_data="p:adda")],
          [Btn("📊 آمار", callback_data="p:stats")],
          [Btn("📋 نمایش فهرست کامل پیام‌ها", callback_data="p:list")],
          [Btn("🧪 تست سناریو", callback_data="p:test")]]
    if MINI_APP_URL: kb.append([Btn("🎨 استودیوی طراحی", web_app=WebAppInfo(url=MINI_APP_URL + "/miniapp"))])
    return text, Kb(kb)

def list_view():
    seq = scenario()
    head = "📋 فهرست پیام‌های سناریو\n\n"
    add_row = [Btn("➕ استارت", callback_data="p:adds"), Btn("➕ ادامه", callback_data="p:adda")]
    if not seq:
        return head + "هنوز پیامی ثبت نشده است.", Kb([add_row, [Btn("🔙 پنل", callback_data="p:home")]])
    lines, n = [], 0
    for kind, (name, _) in GROUPS.items():
        items = [m for m in seq if m["kind"] == kind]
        sub = "(همه فوری و پشت‌سرهم ارسال می‌شوند)" if kind == "start" else "(هر مرحله بعد از پاسخ کاربر)"
        lines.append(f"{name} {sub}")
        if not items: lines.append("— خالی —")
        for m in items:
            n += 1; nb = len(get_buttons(m["id"]))
            lines.append(f"{kc(n)} {m['title']}" + (f"  · 🔘{nb}" if nb else ""))
        lines.append("")
    text = head + "\n".join(lines) + f"\n📊 تعداد کل پیام‌ها: {len(seq)}\n\nبرای مدیریت هر پیام، شماره‌اش را بزن:"
    nums = [Btn(str(i), callback_data=f"m:{m['id']}") for i, m in enumerate(seq, 1)]
    kb = [nums[i:i+5] for i in range(0, len(nums), 5)]
    kb.append(add_row); kb.append([Btn("🔙 پنل", callback_data="p:home")])
    return text, Kb(kb)

def stats_view():
    def cnt(status=None):
        if status: return one("SELECT COUNT(*) n FROM users WHERE status=?", (status,))["n"]
        return one("SELECT COUNT(*) n FROM users")["n"]
    text = ("📊 آمار\n\n"
            f"👥 کل کاربران: {cnt()}\n"
            f"⏳ در حال دریافت سناریو: {cnt('waiting')}\n"
            f"✅ تمام‌شده: {cnt('done')}\n"
            f"📨 پیام‌های سناریو: {len(scenario())}\n"
            f"📥 درخواست‌های جدید: {new_count()}\n"
            f"🗂 کل درخواست‌ها: {one('SELECT COUNT(*) n FROM submissions')['n']}")
    return text, Kb([[Btn("🔙 پنل", callback_data="p:home")]])

def msg_view(mid):
    seq = scenario(); ids = [m["id"] for m in seq]
    if mid not in ids: return list_view()
    i = ids.index(mid); m = seq[i]; g = [x["id"] for x in seq if x["kind"] == m["kind"]]; gi = g.index(mid)
    where = "🚀 پیام استارت (فوری)" if m["kind"] == "start" else f"📩 ادامه — مرحله {gi + 1}"
    text = (f"{kc(i+1)} پیام شماره {i+1} از {len(seq)}\n{where}\n\n📝 {m['title']}\n🔘 دکمه‌ها: {len(get_buttons(mid))}")
    move = []
    if gi > 0: move.append(Btn("⬆️ بالا", callback_data=f"mu:{mid}"))
    if gi < len(g) - 1: move.append(Btn("⬇️ پایین", callback_data=f"md:{mid}"))
    other = "📩 انتقال به ادامه" if m["kind"] == "start" else "🚀 انتقال به استارت"
    kb = [[Btn("👁 پیش‌نمایش", callback_data=f"pv:{mid}"), Btn("🔘 دکمه‌ها", callback_data=f"b:{mid}")]]
    if move: kb.append(move)
    kb += [[Btn("✏️ جایگزینی محتوا", callback_data=f"mr:{mid}"), Btn("🗑 حذف", callback_data=f"mx:{mid}")],
           [Btn(other, callback_data=f"mg:{mid}")],
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
    if b["action_type"] == "flow":
        fl = one("SELECT id FROM flows WHERE button_id=?", (bid,))
        if fl: kb.append([Btn("🧩 ویرایش مراحل مسیر", callback_data=f"fl:{fl['id']}")])
    if b["action_type"] == "reply":
        kb.append([Btn("💬 تغییر پاسخ", callback_data=f"br:{bid}"), Btn("👁 پیش‌نمایش پاسخ", callback_data=f"bp:{bid}")])
    kb.append([Btn("🗑 حذف دکمه", callback_data=f"bx:{bid}")])
    kb.append([Btn("🔙 بازگشت", callback_data=f"b:{b['message_id']}")])
    return text, Kb(kb)

def flow_view(fid):
    fl = one("SELECT * FROM flows WHERE id=?", (fid,))
    if not fl: return home_view()
    b = one("SELECT * FROM buttons WHERE id=?", (fl["button_id"],))
    steps = flow_steps(fid)
    head = f"🧩 مسیر دکمهٔ «{b['label'] if b else fl['name']}»\n\nکاربر با زدن دکمه، مرحله‌ها را به‌ترتیب می‌بیند. سوال و تایید منتظر کاربر می‌مانند، پیام‌ها خودکار ادامه می‌دهند.\n\n"
    if not steps:
        lines = "هنوز مرحله‌ای ندارد؛ از پایین اضافه کن."
    else:
        lines = "\n".join(f"{kc(i)} {SKIND[s['kind']].split()[0]} {s['title'] or '—'}"
                          + (f" ← {ITYPES.get(s['input_type'], '')}" if s["kind"] == "ask" else "") for i, s in enumerate(steps, 1))
    nums = [Btn(str(i), callback_data=f"fs:{s['id']}") for i, s in enumerate(steps, 1)]
    kb = [nums[i:i+5] for i in range(0, len(nums), 5)]
    kb.append([Btn("➕ سوال", callback_data=f"fa:{fid}:q"), Btn("➕ پیام", callback_data=f"fa:{fid}:i"),
               Btn("➕ تایید", callback_data=f"fa:{fid}:c")])
    kb.append([Btn("🔙 دکمه", callback_data=f"be:{fl['button_id']}")])
    return head + lines + "\n\nبرای ویرایش هر مرحله شماره‌اش را بزن:", Kb(kb)

def step_view(sid):
    s = one("SELECT * FROM flow_steps WHERE id=?", (sid,))
    if not s: return home_view()
    steps = flow_steps(s["flow_id"]); ids = [x["id"] for x in steps]; i = ids.index(sid)
    text = f"{kc(i+1)} مرحله {i+1} از {len(steps)}\n{SKIND[s['kind']]}\n\n📝 {s['title'] or '—'}"
    if s["kind"] == "ask": text += f"\n🏷 نام فیلد: {s['field']}\n📥 نوع جواب: {ITYPES.get(s['input_type'], '')}"
    if s["kind"] == "confirm": text += f"\n🔘 متن دکمه: {s['btn_label']}"
    move = []
    if i > 0: move.append(Btn("⬆️ بالا", callback_data=f"fu:{sid}"))
    if i < len(steps) - 1: move.append(Btn("⬇️ پایین", callback_data=f"fd:{sid}"))
    kb = [[Btn("👁 پیش‌نمایش", callback_data=f"fp:{sid}"), Btn("✏️ جایگزینی پیام", callback_data=f"fr:{sid}")]]
    if s["kind"] in ("ask", "confirm"):
        kb.append([Btn("🏷 تغییر نام فیلد" if s["kind"] == "ask" else "🔘 تغییر متن دکمه", callback_data=f"fn:{sid}")])
    if move: kb.append(move)
    kb.append([Btn("🗑 حذف", callback_data=f"fz:{sid}")])
    kb.append([Btn("🔙 مسیر", callback_data=f"fl:{s['flow_id']}")])
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
    touch(chat.id); drop_run(chat.id)
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
    states.pop(update.effective_user.id, None); drop_run(update.effective_chat.id)
    if admin(update):
        execute("UPDATE users SET status='done' WHERE chat_id=? AND status='waiting'", (update.effective_chat.id,))
    t, k = home_view() if admin(update) else ("لغو شد.", None)
    await update.message.reply_text(t, reply_markup=k)

async def admin_input(update, context, st):
    m = update.message; kind = st["kind"]; text = (m.text or "").strip()
    if kind == "add":
        grp = st.get("grp", "after"); mid = add_scenario_message(m, grp); k = len(group(grp))
        await m.reply_text(f"✅ {GROUPS[grp][0]} — پیام {k} ثبت شد.\nپیام بعدی را بفرست یا پایان را بزن.",
                           reply_markup=Kb([[Btn("🔘 افزودن دکمه به این پیام", callback_data=f"ba:{mid}")],
                                            [Btn("✅ پایان", callback_data="p:list")]]))
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
            [Btn("🧩 مسیر چندمرحله‌ای (سوال و جواب)", callback_data="act:flow")],
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
    elif kind == "fs_msg":
        st.update(chat_id=m.chat_id, message_id=m.message_id, title=title_of(m))
        if st["stype"] == "q":
            st["kind"] = "fs_type"
            await m.reply_text("جواب کاربر چه نوعی باشد؟", reply_markup=Kb([
                [Btn("📝 متن", callback_data="ft:text"), Btn("🔢 عدد", callback_data="ft:number")],
                [Btn("💳 شماره کارت", callback_data="ft:card"), Btn("📷 عکس", callback_data="ft:photo")]]))
        elif st["stype"] == "c":
            st["kind"] = "fs_label"; await m.reply_text("متن دکمهٔ تایید را بفرست (مثلاً: ✅ تایید):")
        else:
            add_step(st["fid"], "info", chat_id=st["chat_id"], message_id=st["message_id"], title=st["title"])
            states.pop(update.effective_user.id, None); v = flow_view(st["fid"])
            await m.reply_text("✅ پیام اضافه شد.\n\n" + v[0], reply_markup=v[1])
    elif kind in ("fs_field", "fs_label", "fs_rename"):
        if not text or len(text) > (40 if kind == "fs_field" else 64):
            await m.reply_text("یک متن کوتاه بفرست."); return
        if kind == "fs_rename":
            sp = one("SELECT * FROM flow_steps WHERE id=?", (st["sid"],))
            if sp: execute("UPDATE flow_steps SET " + ("field" if sp["kind"] == "ask" else "btn_label") + "=? WHERE id=?", (text, sp["id"]))
            states.pop(update.effective_user.id, None); v = step_view(st["sid"])
            await m.reply_text("✅ ذخیره شد.\n\n" + v[0], reply_markup=v[1]); return
        if kind == "fs_field":
            add_step(st["fid"], "ask", chat_id=st["chat_id"], message_id=st["message_id"], title=st["title"], field=text, input_type=st["itype"])
        else:
            add_step(st["fid"], "confirm", chat_id=st["chat_id"], message_id=st["message_id"], title=st["title"], btn_label=text)
        states.pop(update.effective_user.id, None); v = flow_view(st["fid"])
        await m.reply_text("✅ مرحله اضافه شد.\n\n" + v[0], reply_markup=v[1])
    elif kind == "fs_replace":
        execute("UPDATE flow_steps SET chat_id=?,message_id=?,text=NULL,title=? WHERE id=?", (m.chat_id, m.message_id, title_of(m), st["sid"]))
        states.pop(update.effective_user.id, None); v = step_view(st["sid"])
        await m.reply_text("✅ محتوا جایگزین شد.\n\n" + v[0], reply_markup=v[1])
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
        if not (u and u["status"] == "waiting") and not get_run(cid):
            await msg.reply_text("برای مدیریت /admin بزن؛ برای تست سناریو /test."); return
    touch(cid)
    if get_run(cid):   # user is inside a flow: the message is an answer
        await flow_input(context.bot, msg); return
    u = one("SELECT status FROM users WHERE chat_id=?", (cid,))
    if u["status"] == "new": await begin(context.bot, cid)
    elif u["status"] == "waiting": await advance(context.bot, cid)
    # status 'done': scenario is finished, nothing else is sent

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u, data, bot = q.from_user, q.data or "", context.bot
    if data.startswith("send:"):
        await user_click(bot, q, int(data[5:])); return
    if data.startswith("fc:"):
        await flow_confirm(bot, q, int(data[3:])); return
    if data == "fq":
        await flow_cancel(bot, q); return
    if u.id not in ADMIN_IDS or not q.message or q.message.chat.type != ChatType.PRIVATE: return
    key, _, arg = data.partition(":"); n = int(arg) if arg.isdigit() else 0
    if key not in ("act", "ft"): states.pop(u.id, None)
    if key == "p":
        if arg == "home": await show(q, *home_view())
        elif arg in ("list", "done"): await show(q, *list_view())
        elif arg == "stats": await show(q, *stats_view())
        elif arg in ("adds", "adda"):
            grp = "start" if arg == "adds" else "after"; states[u.id] = {"kind": "add", "grp": grp}
            note = ("🚀 پیام‌های استارت را یکی‌یکی بفرست (متن، عکس، ویدیو، ویس، فایل و...).\n"
                    "با /start همه‌شان فوری و پشت‌سرهم برای کاربر ارسال می‌شوند.") if grp == "start" else \
                   ("📩 پیام‌های ادامه را یکی‌یکی بفرست (هر نوع محتوا).\n"
                    "بعد از استارت، با هر پاسخ کاربر فقط یکی از این‌ها ارسال می‌شود.")
            await show(q, note, Kb([[Btn("✅ پایان", callback_data="p:list")]]))
        elif arg == "test":
            touch(q.message.chat_id)
            await q.message.reply_text("🧪 حالت تست: سناریو را مثل یک کاربر ببین. برای خروج /cancel")
            await begin(bot, q.message.chat_id)
    elif key == "m": await show(q, *msg_view(n))
    elif key == "pv":
        rec = msg_by_id(n)
        if rec: await copy_saved(bot, q.message.chat_id, rec)
    elif key in ("mu", "md"):
        move_msg(n, -1 if key == "mu" else 1); await show(q, *msg_view(n))
    elif key == "mg":
        switch_group(n); await show(q, *msg_view(n))
    elif key == "mx":
        await show(q, "⚠️ این پیام و دکمه‌هایش حذف شود؟", Kb([[Btn("✅ بله، حذف", callback_data=f"mxx:{n}"), Btn("❌ خیر", callback_data=f"m:{n}")]]))
    elif key == "mxx":
        execute("DELETE FROM messages WHERE id=?", (n,)); renumber()
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
        elif arg == "flow":
            bid = add_button(mid, st["label"], "flow"); fid = execute("INSERT INTO flows(button_id,name) VALUES(?,?)", (bid, st["label"]))
            states.pop(u.id, None); v = flow_view(fid)
            await show(q, "✅ دکمه ساخته شد؛ حالا مرحله‌های مسیرش را اضافه کن.\n\n" + v[0], v[1])
    elif key == "fl": await show(q, *flow_view(n))
    elif key == "fa":
        sfid, _, t = arg.partition(":")
        if t not in ("q", "i", "c") or not sfid.isdigit(): return
        states[u.id] = {"kind": "fs_msg", "fid": int(sfid), "stype": t}
        ask = {"q": "❓ پیام سوال را بفرست (مثلاً: شماره کارتت را بفرست):",
               "i": "💬 پیام توضیحی را بفرست (متن یا رسانه):",
               "c": "✅ پیامی را بفرست که دکمهٔ تایید زیرش بیاید:"}[t]
        await show(q, ask, cancel_kb(f"fl:{sfid}"))
    elif key == "ft":
        st = states.get(u.id)
        if not st or st.get("kind") != "fs_type" or arg not in ITYPES:
            await show(q, "این مرحله منقضی شده؛ دوباره از پنل شروع کن.", Kb([[Btn("🔙 پنل", callback_data="p:home")]])); return
        st.update(kind="fs_field", itype=arg)
        await show(q, "🏷 نام این اطلاعات برای گزارش را بفرست (مثلاً: شماره کارت):", cancel_kb(f"fl:{st['fid']}"))
    elif key == "fs": await show(q, *step_view(n))
    elif key == "fp":
        sp = one("SELECT * FROM flow_steps WHERE id=?", (n,))
        if sp:
            try: await send_step(bot, q.message.chat_id, sp)
            except Exception: log.exception("preview failed")
    elif key in ("fu", "fd"):
        sp = one("SELECT flow_id FROM flow_steps WHERE id=?", (n,))
        if sp:
            ids = [x["id"] for x in flow_steps(sp["flow_id"])]; i = ids.index(n); j = i - 1 if key == "fu" else i + 1
            if 0 <= j < len(ids): ids[i], ids[j] = ids[j], ids[i]; set_step_order(ids)
        await show(q, *step_view(n))
    elif key == "fz":
        await show(q, "⚠️ این مرحله حذف شود؟", Kb([[Btn("✅ بله، حذف", callback_data=f"fzz:{n}"), Btn("❌ خیر", callback_data=f"fs:{n}")]]))
    elif key == "fzz":
        sp = one("SELECT flow_id FROM flow_steps WHERE id=?", (n,))
        if sp:
            execute("DELETE FROM flow_steps WHERE id=?", (n,)); await show(q, *flow_view(sp["flow_id"]))
    elif key == "fr":
        states[u.id] = {"kind": "fs_replace", "sid": n}
        await show(q, "✏️ پیام جدید را بفرست (جایگزین محتوای این مرحله می‌شود):", cancel_kb(f"fs:{n}"))
    elif key == "fn":
        states[u.id] = {"kind": "fs_rename", "sid": n}
        await show(q, "✏️ متن جدید را بفرست:", cancel_kb(f"fs:{n}"))
    elif key == "sd":
        execute("UPDATE submissions SET status='done' WHERE id=?", (n,))
        try: await q.edit_message_reply_markup(reply_markup=Kb([[Btn("✅ انجام شد", callback_data=f"sd:{n}")]]))
        except Exception: pass

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
        result.append({"id": m["id"], "kind": m["kind"], "position": len(result), "title": m["title"],
                       "buttons": [{"id": b["id"], "admin_name": b["admin_name"], "label": b["label"],
                                    "action_type": b["action_type"], "action_url": b["action_url"],
                                    "flow_id": (one("SELECT id FROM flows WHERE button_id=?", (b["id"],)) or {"id": None})["id"]}
                                   for b in bs]})
    users = one("SELECT COUNT(*) n FROM users")["n"]; received = one("SELECT CAST(COALESCE(SUM(received),0) AS BIGINT) n FROM users")["n"]
    return jsonify({"messages": result, "users": users, "received": received, "orders": new_count()})

@app.post("/api/messages/<int:mid>/delete")
def delete_api(mid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    m = one("SELECT kind FROM messages WHERE id=?", (mid,))
    if not m or m["kind"] not in GROUPS: return jsonify({"error": "not_found_or_protected"}), 404
    execute("DELETE FROM messages WHERE id=?", (mid,))
    renumber()
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
        elif typ == "flow":
            bid = execute("INSERT INTO buttons(message_id,admin_name,label,action_type) VALUES(?,?,?,'flow')", (mid, name, label))
            execute("INSERT INTO flows(button_id,name) VALUES(?,?)", (bid, label))
        elif typ == "next":
            execute("INSERT INTO buttons(message_id,admin_name,label,action_type) VALUES(?,?,?,'next')", (mid, name, label))
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

# ── Flow + submissions API (Mini App) ──
def step_json(s):
    return {"id": s["id"], "kind": s["kind"], "title": s["title"], "text": s["text"] or "", "media": bool(s["chat_id"]),
            "field": s["field"] or "", "input_type": s["input_type"] or "", "btn_label": s["btn_label"] or ""}

def clean_step(d, kind):
    text = str(d.get("text", "")).strip()[:3500]
    field = str(d.get("field", "")).strip()[:40]
    itype = d.get("input_type") or "text"
    label = str(d.get("btn_label", "")).strip()[:64]
    if kind == "ask" and (not field or itype not in ITYPES): raise ValueError()
    if kind == "confirm" and not label: raise ValueError()
    return text, field, itype, label

@app.get("/api/flows/<int:fid>")
def flow_api(fid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    fl = one("SELECT * FROM flows WHERE id=?", (fid,))
    if not fl: return jsonify({"error": "not_found"}), 404
    return jsonify({"flow": {"id": fid, "name": fl["name"], "button_id": fl["button_id"]},
                    "steps": [step_json(x) for x in flow_steps(fid)]})

@app.post("/api/flows/<int:fid>/steps")
def add_step_api(fid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    d = request.get_json(silent=True) or {}
    try:
        if not one("SELECT id FROM flows WHERE id=?", (fid,)): raise ValueError()
        kind = d.get("kind")
        if kind not in SKIND: raise ValueError()
        text, field, itype, label = clean_step(d, kind)
        if not text: raise ValueError()
        add_step(fid, kind, text=text, title=" ".join(text.split())[:42],
                 field=field if kind == "ask" else None, input_type=itype if kind == "ask" else None,
                 btn_label=label if kind == "confirm" else None)
        return jsonify({"ok": True})
    except Exception: return jsonify({"error": "invalid_step_data"}), 400

@app.patch("/api/steps/<int:sid>")
def edit_step_api(sid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    sp = one("SELECT * FROM flow_steps WHERE id=?", (sid,))
    if not sp: return jsonify({"error": "not_found"}), 404
    d = request.get_json(silent=True) or {}
    try:
        text, field, itype, label = clean_step(d, sp["kind"])
        if text:   # new text replaces the old content (also for copied-media steps)
            execute("UPDATE flow_steps SET text=?,title=?,chat_id=NULL,message_id=NULL WHERE id=?",
                    (text, " ".join(text.split())[:42], sid))
        elif not sp["chat_id"]: raise ValueError()
        if sp["kind"] == "ask": execute("UPDATE flow_steps SET field=?,input_type=? WHERE id=?", (field, itype, sid))
        if sp["kind"] == "confirm": execute("UPDATE flow_steps SET btn_label=? WHERE id=?", (label, sid))
        return jsonify({"ok": True})
    except Exception: return jsonify({"error": "invalid_step_data"}), 400

@app.delete("/api/steps/<int:sid>")
def del_step_api(sid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    execute("DELETE FROM flow_steps WHERE id=?", (sid,)); return jsonify({"ok": True})

@app.post("/api/flows/<int:fid>/reorder")
def reorder_steps_api(fid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    try: ids = [int(x) for x in (request.get_json(silent=True) or {}).get("ids", [])]
    except Exception: return jsonify({"error": "invalid_ids"}), 400
    if set(ids) != {x["id"] for x in flow_steps(fid)} or len(ids) != len(set(ids)): return jsonify({"error": "ids_must_match"}), 400
    set_step_order(ids); return jsonify({"ok": True})

@app.get("/api/submissions")
def submissions_api():
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    out = []
    for r in rows("SELECT * FROM submissions ORDER BY id DESC LIMIT 100"):
        ans = [{"label": a.get("label"), "value": a.get("value")} for a in json.loads(r["answers"] or "[]")]
        out.append({"id": r["id"], "flow": r["flow_name"], "chat_id": r["chat_id"], "username": r["username"],
                    "name": r["full_name"], "status": r["status"], "created": r["created"], "answers": ans})
    return jsonify({"items": out})

@app.post("/api/submissions/<int:sid>/status")
def submission_status_api(sid):
    if not mini_admin(): return jsonify({"error": DENIED}), 403
    st = (request.get_json(silent=True) or {}).get("status")
    if st not in ("new", "done"): return jsonify({"error": "invalid_status"}), 400
    execute("UPDATE submissions SET status=? WHERE id=?", (st, sid)); return jsonify({"ok": True})

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
    if not TOKEN: raise SystemExit("Set BOT_TOKEN in Render Environment.")
    if not DATABASE_URL: raise SystemExit("Set DATABASE_URL (Supabase Session pooler) in Render Environment.")
    open_pool(); init_db()
    threading.Thread(target=run_bot, daemon=True, name="telegram-bot").start()
    log.info("Flask Mini App listening on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
