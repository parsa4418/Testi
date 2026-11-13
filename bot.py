# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, json, hmac, hashlib, sqlite3, threading, logging
from pathlib import Path
from urllib.parse import parse_qsl
from flask import Flask, jsonify, request, send_from_directory
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.constants import ChatType
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

BASE = Path(__file__).resolve().parent
TOKEN = os.getenv("9199866071:AAFl2aJNLYAUFNDOFVV4VMevsm8-PRbGiwE").strip()
ADMIN_IDS = {1297047765, 8904869158}
PORT = int(os.getenv("PORT", "10000"))
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE / "data" / "messenger.sqlite3")))
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
MINI_APP_URL = (os.getenv("MINI_APP_URL", "").strip().rstrip("/") or RENDER_URL)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gugu-messenger")
app = Flask(__name__, static_folder=str(BASE / "miniapp"))

states = {}

def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    return c

def init_db():
    with connect() as c:
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

def rows(sql, args=()):
    with connect() as c: return c.execute(sql, args).fetchall()

def one(sql, args=()):
    with connect() as c: return c.execute(sql, args).fetchone()

def execute(sql, args=()):
    with connect() as c: cur=c.execute(sql,args); return cur.lastrowid

def admin(update):
    u=update.effective_user; ch=update.effective_chat
    return bool(u and ch and u.id in ADMIN_IDS and ch.type == ChatType.PRIVATE)

def title_of(m):
    s=(m.text or m.caption or "").strip()
    if s: return " ".join(s.split())[:42]
    for k,v in [("photo","🖼 عکس"),("video","🎬 ویدیو"),("animation","🎞 گیف"),("document","📎 فایل"),("audio","🎵 صوت"),("voice","🎤 ویس"),("sticker","🎟 استیکر"),("video_note","⭕ ویدیو مسیج")]:
        if getattr(m,k,None): return v
    return "پیام تلگرامی"

def get_buttons(mid):
    return rows("SELECT * FROM buttons WHERE message_id=? ORDER BY id",(mid,))

def user_markup(mid):
    bs=get_buttons(mid)
    if not bs: return None
    keys=[]
    for b in bs:
        if b["action_type"]=="url": keys.append(InlineKeyboardButton(b["label"],url=b["action_url"]))
        else: keys.append(InlineKeyboardButton(b["label"],callback_data=f"send:{b['id']}"))
    return InlineKeyboardMarkup([keys[i:i+2] for i in range(0,len(keys),2)])

async def copy_saved(bot, chat_id, record):
    await bot.copy_message(chat_id=chat_id, from_chat_id=record["chat_id"],
                           message_id=record["message_id"], reply_markup=user_markup(record["id"]))

def save_message(kind,m):
    if kind=="start":
        old=one("SELECT id FROM messages WHERE kind='start' ORDER BY id LIMIT 1")
        if old:
            execute("UPDATE messages SET chat_id=?,message_id=?,title=? WHERE id=?",(m.chat_id,m.message_id,title_of(m),old["id"]))
            return old["id"]
        return execute("INSERT INTO messages(kind,position,chat_id,message_id,title) VALUES('start',0,?,?,?)",(m.chat_id,m.message_id,title_of(m)))
    pos=(one("SELECT COALESCE(MAX(position),0)+1 p FROM messages WHERE kind='after'")["p"])
    return execute("INSERT INTO messages(kind,position,chat_id,message_id,title) VALUES('after',?,?,?,?)",(pos,m.chat_id,m.message_id,title_of(m)))

def main_keyboard():
    items=[]
    if MINI_APP_URL: items.append([InlineKeyboardButton("🎨 استودیوی طراحی",web_app=WebAppInfo(url=MINI_APP_URL+"/miniapp"))])
    items += [[InlineKeyboardButton("🚀 پیام استارت",callback_data="admin:start")],
              [InlineKeyboardButton("📩 پیام‌های بعدی",callback_data="admin:after")],
              [InlineKeyboardButton("📊 آمار",callback_data="admin:stats")]]
    return InlineKeyboardMarkup(items)

async def start(update:Update, context:ContextTypes.DEFAULT_TYPE):
    chat=update.effective_chat
    execute("""INSERT INTO users(chat_id) VALUES(?) ON CONFLICT(chat_id) DO UPDATE SET last_seen=CURRENT_TIMESTAMP,received=received+1""",(chat.id,))
    if admin(update):
        await update.message.reply_text("به پنل مدیریت GUGU EMPIRE MESSENGER خوش آمدی.",reply_markup=main_keyboard())
        return
    m=one("SELECT * FROM messages WHERE kind='start' ORDER BY id LIMIT 1")
    if m:
        await copy_saved(context.bot,chat.id,m)
    else:
        await update.message.reply_text("سلام! هنوز پیام شروع تنظیم نشده است.")

async def admin_cmd(update,context):
    if not admin(update): return
    await update.message.reply_text("پنل مدیریت پیام‌رسان:",reply_markup=main_keyboard())

async def incoming(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    u=update.effective_user
    if admin(update):
        st=states.get(u.id)
        if st:
            kind=st.get("kind")
            if kind in ("start","after"):
                mid=save_message(kind,update.message); states.pop(u.id,None)
                await update.message.reply_text(f"پیام ثبت شد. شناسه: {mid}",reply_markup=main_keyboard()); return
            if kind=="button_name":
                st["admin_name"]=update.message.text or "Button"; st["step"]="label"
                await update.message.reply_text("متن نمایشی دکمه را بفرست:"); return
            if kind=="button_label":
                st["label"]=update.message.text or "ادامه"; st["step"]="action"
                await update.message.reply_text("عملکرد را انتخاب کن:",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("ارسال پیام ذخیره‌شده",callback_data="act:reply")],[InlineKeyboardButton("بازکردن لینک",callback_data="act:url")]])); return
            if kind=="button_url":
                url=update.message.text or ""
                if not url.startswith(("https://","http://","tg://")):
                    await update.message.reply_text("لینک معتبر بفرست (https:// یا http:// یا tg://)."); return
                execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_url) VALUES(?,?,?,'url',?)",(st["message_id"],st["admin_name"],st["label"],url))
                states.pop(u.id,None); await update.message.reply_text("دکمه ثبت شد.",reply_markup=main_keyboard()); return
            if kind=="button_reply":
                mid=save_message("after",update.message)
                rec=one("SELECT * FROM messages WHERE id=?",(mid,))
                execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_chat_id,action_message_id) VALUES(?,?,?,'reply',?,?)",(st["message_id"],st["admin_name"],st["label"],rec["chat_id"],rec["message_id"]))
                states.pop(u.id,None); await update.message.reply_text("دکمه ارسال پیام ساخته شد.",reply_markup=main_keyboard()); return
        await update.message.reply_text("برای مدیریت از /admin استفاده کن؛ برای لغو فرایند /cancel")
        return
    execute("""INSERT INTO users(chat_id) VALUES(?) ON CONFLICT(chat_id) DO UPDATE SET last_seen=CURRENT_TIMESTAMP,received=received+1""",(update.effective_chat.id,))
    for m in rows("SELECT * FROM messages WHERE kind='after' ORDER BY position,id"):
        try: await copy_saved(context.bot,update.effective_chat.id,m)
        except Exception: log.exception("Could not deliver sequence message")

async def cancel(update,context):
    if update.effective_user: states.pop(update.effective_user.id,None)
    await update.message.reply_text("لغو شد.")

async def callbacks(update:Update,context:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    u=q.from_user; data=q.data or ""
    if data.startswith("send:"):
        b=one("SELECT * FROM buttons WHERE id=?",(int(data.split(":")[1]),))
        if b and b["action_type"]=="reply":
            try: await context.bot.copy_message(chat_id=q.message.chat_id,from_chat_id=b["action_chat_id"],message_id=b["action_message_id"])
            except Exception: log.exception("Button reply failed")
        return
    if u.id not in ADMIN_IDS or q.message.chat.type!="private": return
    if data=="admin:start":
        states[u.id]={"kind":"start"}; await q.message.reply_text("پیام استارت را بفرست؛ متن، عکس، ویدیو، فایل یا هر پیام قابل کپی تلگرام.")
    elif data=="admin:after":
        states[u.id]={"kind":"after"}; await q.message.reply_text("پیام بعدی در زنجیره را بفرست.")
    elif data=="admin:stats":
        n=one("SELECT COUNT(*) n FROM users")["n"]; m=one("SELECT COALESCE(SUM(received),0) n FROM users")["n"]
        await q.message.reply_text(f"کاربران: {n}\nپیام‌های دریافتی: {m}",reply_markup=main_keyboard())
    elif data=="act:url":
        states[u.id]["kind"]="button_url"; await q.message.reply_text("لینک را بفرست:")
    elif data=="act:reply":
        states[u.id]["kind"]="button_reply"; await q.message.reply_text("پیامی را که باید ارسال شود بفرست:")

def valid_init_data(raw):
    if not TOKEN or not raw: return False,None
    try:
        p=dict(parse_qsl(raw,keep_blank_values=True)); received=p.pop("hash","")
        check="\n".join(f"{k}={v}" for k,v in sorted(p.items()))
        secret=hmac.new(b"WebAppData",TOKEN.encode(),hashlib.sha256).digest()
        calc=hmac.new(secret,check.encode(),hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc,received): return False,None
        if abs(int(time.time())-int(p.get("auth_date","0")))>86400: return False,None
        user=json.loads(p.get("user","{}")); return True,int(user.get("id",0))
    except Exception: return False,None

def mini_admin():
    ok,uid=valid_init_data(request.headers.get("X-Telegram-Init-Data",""))
    return ok and uid in ADMIN_IDS

DENIED="شما اجازه دسترسی به این بخش را ندارید❌"

@app.get("/")
def home(): return send_from_directory(BASE/"miniapp","index.html")

@app.get("/miniapp")
def miniapp(): return send_from_directory(BASE/"miniapp","index.html")

@app.get("/healthz")
def health(): return jsonify({"ok":True,"service":"gugu-empire-messenger"})

@app.get("/api/state")
def state_api():
    if not mini_admin(): return jsonify({"error":DENIED}),403
    ms=rows("SELECT * FROM messages ORDER BY CASE kind WHEN 'start' THEN 0 ELSE 1 END,position,id")
    result=[]
    for m in ms:
        bs=get_buttons(m["id"])
        result.append({"id":m["id"],"kind":m["kind"],"position":m["position"],"title":m["title"],
          "buttons":[{"id":b["id"],"admin_name":b["admin_name"],"label":b["label"],"action_type":b["action_type"],"action_url":b["action_url"]} for b in bs]})
    users=one("SELECT COUNT(*) n FROM users")["n"]; received=one("SELECT COALESCE(SUM(received),0) n FROM users")["n"]
    return jsonify({"messages":result,"users":users,"received":received})

@app.post("/api/messages/<int:mid>/delete")
def delete_api(mid):
    if not mini_admin(): return jsonify({"error":DENIED}),403
    m=one("SELECT kind FROM messages WHERE id=?",(mid,))
    if not m or m["kind"]!="after": return jsonify({"error":"not_found_or_protected"}),404
    execute("DELETE FROM messages WHERE id=?",(mid,))
    with connect() as c:
        for i,r in enumerate(c.execute("SELECT id FROM messages WHERE kind='after' ORDER BY position,id"),1): c.execute("UPDATE messages SET position=? WHERE id=?",(i,r["id"]))
    return jsonify({"ok":True})

@app.post("/api/messages/reorder")
def reorder_api():
    if not mini_admin(): return jsonify({"error":DENIED}),403
    ids=(request.get_json(silent=True) or {}).get("ids",[])
    current=[r["id"] for r in rows("SELECT id FROM messages WHERE kind='after' ORDER BY position,id")]
    try: ids=[int(x) for x in ids]
    except Exception: return jsonify({"error":"invalid_ids"}),400
    if len(ids)!=len(current) or set(ids)!=set(current): return jsonify({"error":"ids_must_match"}),400
    with connect() as c:
        for pos,mid in enumerate(ids,1): c.execute("UPDATE messages SET position=? WHERE id=?",(pos,mid))
    return jsonify({"ok":True})

@app.post("/api/buttons")
def add_button_api():
    if not mini_admin(): return jsonify({"error":DENIED}),403
    d=request.get_json(silent=True) or {}
    try:
        mid=int(d["message_id"]); name=str(d["admin_name"]).strip(); label=str(d["label"]).strip(); typ=d["action_type"]
        if not name or not label or len(label)>64 or not one("SELECT id FROM messages WHERE id=?",(mid,)): raise ValueError()
        if typ=="url":
            url=str(d.get("action_url","")).strip()
            if not url.startswith(("https://","http://","tg://")): raise ValueError()
            execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_url) VALUES(?,?,?,'url',?)",(mid,name,label,url))
        elif typ=="reply":
            target=one("SELECT * FROM messages WHERE id=?",(int(d["reply_message_id"]),))
            if not target: raise ValueError()
            execute("INSERT INTO buttons(message_id,admin_name,label,action_type,action_chat_id,action_message_id) VALUES(?,?,?,'reply',?,?)",(mid,name,label,target["chat_id"],target["message_id"]))
        else: raise ValueError()
        return jsonify({"ok":True})
    except Exception: return jsonify({"error":"invalid_button_data"}),400

@app.patch("/api/buttons/<int:bid>")
def edit_button_api(bid):
    if not mini_admin(): return jsonify({"error":DENIED}),403
    b=one("SELECT * FROM buttons WHERE id=?",(bid,))
    if not b: return jsonify({"error":"not_found"}),404
    d=request.get_json(silent=True) or {}; name=str(d.get("admin_name",b["admin_name"])).strip(); label=str(d.get("label",b["label"])).strip()
    if not name or not label or len(label)>64: return jsonify({"error":"invalid_value"}),400
    if "action_url" in d:
        url=str(d["action_url"]).strip()
        if not url.startswith(("https://","http://","tg://")): return jsonify({"error":"invalid_url"}),400
        execute("UPDATE buttons SET admin_name=?,label=?,action_type='url',action_url=?,action_chat_id=NULL,action_message_id=NULL WHERE id=?",(name,label,url,bid))
    else: execute("UPDATE buttons SET admin_name=?,label=? WHERE id=?",(name,label,bid))
    return jsonify({"ok":True})

@app.delete("/api/buttons/<int:bid>")
def remove_button_api(bid):
    if not mini_admin(): return jsonify({"error":DENIED}),403
    execute("DELETE FROM buttons WHERE id=?",(bid,)); return jsonify({"ok":True})

def run_bot():
    if not TOKEN: raise RuntimeError("BOT_TOKEN is missing")
    application=Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start",start))
    application.add_handler(CommandHandler("admin",admin_cmd))
    application.add_handler(CommandHandler("cancel",cancel))
    application.add_handler(CallbackQueryHandler(callbacks))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND,incoming))
    log.info("Bot polling thread starting")
    application.run_polling(drop_pending_updates=False, stop_signals=None)

if __name__=="__main__":
    init_db()
    if not TOKEN: raise SystemExit("Set BOT_TOKEN in Render Environment.")
    threading.Thread(target=run_bot,daemon=True,name="telegram-bot").start()
    log.info("Flask Mini App listening on port %s",PORT)
    app.run(host="0.0.0.0",port=PORT,threaded=True,use_reloader=False)
