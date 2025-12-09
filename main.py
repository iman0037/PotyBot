from telebot.async_telebot import AsyncTeleBot, types
from telebot.formatting import hbold, hspoiler, escape_html
import json
import os
import random
import time
from telethon import TelegramClient
import asyncio
import re
import uuid
from dotenv import load_dotenv

from fastapi import FastAPI, Request, HTTPException

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = AsyncTeleBot(BOT_TOKEN, parse_mode=None)


app = FastAPI()

USERS_DATA_PATH = os.getenv("DB_PATH")

ADMINS = [8200758971]

users_data = {}
users = []
global_messages = {}
reply_counts = {}
origin_index = {}
origin_locks = {}

def prune_origins(max_age_seconds=86400):
    now = time.time()
    for oid, entry in list(origin_index.items()):
        if now - entry.get("ts", 0) > max_age_seconds:
            origin_index.pop(oid, None)
            origin_locks.pop(oid, None)

async def prune_loop(interval_seconds: int = 3600, max_age_seconds: int = 86400):
    while True:
        try:
            now = time.time()
            for oid, entry in list(origin_index.items()):
                try:
                    if now - entry.get("ts", 0) > max_age_seconds:
                        lock = origin_locks.get(oid)
                        if lock and getattr(lock, "locked", None) and lock.locked():
                            continue
                        origin_index.pop(oid, None)
                        origin_locks.pop(oid, None)
                except Exception as e:
                    print("prune: error on single origin:", e)
        except Exception as e:
            print("prune_loop error:", e)
        await asyncio.sleep(interval_seconds)


def ensure_origin_lock(origin_id: str) -> asyncio.Lock:
    lock = origin_locks.get(origin_id)
    if lock is None:
        lock = asyncio.Lock()
        origin_locks[origin_id] = lock
    return lock

async def store_local_record(user_id: int, sent_message_id: int, header_plain: str, body_plain: str, source_chat_id: int, origin_id: str, is_bold_body: bool):
    ukey = str(user_id)
    msgs = global_messages.setdefault(ukey, [])
    existing = next((m for m in msgs if m.get("message_id") == sent_message_id), None)
    display_name = await get_display_name(user_id) if user_id != source_chat_id else await get_display_name(source_chat_id)
    if existing:
        existing.update({
            "text": body_plain,
            "header": header_plain,
            "source_chat_id": source_chat_id,
            "display_name": display_name,
            "sender_guid": str(source_chat_id),
            "origin_id": origin_id,
            "is_bot_message": True,
            "is_bold_body": is_bold_body
        })
    else:
        msgs.append({
            "message_id": sent_message_id,
            "text": body_plain,
            "header": header_plain,
            "source_chat_id": source_chat_id,
            "display_name": display_name,
            "sender_guid": str(source_chat_id),
            "origin_id": origin_id,
            "is_bot_message": True,
            "is_bold_body": is_bold_body
        })

    entry = origin_index.setdefault(origin_id, {"sender": source_chat_id, "is_bold_body": is_bold_body, "user_map": {}, "ts": time.time()})
    entry["user_map"][ukey] = sent_message_id
    entry["is_bold_body"] = is_bold_body
    entry["ts"] = time.time()

async def send_and_store(u_int: int, header_plain: str, body_plain: str, origin_id: str, is_bold_body: bool, reply_to_local_mid: int = None, source_chat_id: int = None):
    if source_chat_id is None:
        source_chat_id = u_int
    header_html = f"<b>{escape_html(header_plain)}</b>"
    body_html = f"<b>{escape_html(body_plain)}</b>" if is_bold_body else escape_html(body_plain)
    payload = f"{header_html}\n\n{body_html}"
    try:
        if reply_to_local_mid:
            sent = await bot.send_message(u_int, payload, parse_mode="HTML", reply_to_message_id=reply_to_local_mid)
        else:
            sent = await bot.send_message(u_int, payload, parse_mode="HTML")
    except Exception as e:
        print("send_and_store to", u_int, "failed:", e)
        return None


    lock = ensure_origin_lock(origin_id)
    async with lock:
        await store_local_record(u_int, sent.message_id, header_plain, body_plain, source_chat_id, origin_id, is_bold_body)
    return sent.message_id

def find_user_record_by_origin(user_key: int, source_chat_id: int, origin_id: str):
    entry = origin_index.get(origin_id)
    if entry:
        local_mid = entry["user_map"].get(str(user_key))
        if local_mid:
            recs = global_messages.get(str(user_key), [])
            return next((m for m in recs if m.get("message_id") == local_mid), None)
    for m in global_messages.get(str(user_key), []):
        if m.get("source_chat_id") == source_chat_id and m.get("origin_id") == origin_id:
            return m
    return None

async def increment_and_edit_reply_count_for_local(user_id_str: str, local_mid: int):
    key = (str(user_id_str), int(local_mid))
    user_msgs = global_messages.get(str(user_id_str), [])
    user_ref_local = next((m for m in user_msgs if m.get("message_id") == int(local_mid)), None)
    if not user_ref_local:
        return
    reply_counts[key] = reply_counts.get(key, 0) + 1
    if reply_counts[key] > 1:
        pers = persian_digits(reply_counts[key])
        ref_header_plain = user_ref_local.get("header") or ("🙎🏻‍♂ You:" if str(user_id_str) == str(user_ref_local.get("source_chat_id")) else f"👤 {user_ref_local.get('display_name','ناشناس')}:")
        ref_body_plain = user_ref_local.get("text", "")
        header_html = f"<b>{escape_html(ref_header_plain)}</b>"
        if user_ref_local.get("is_bold_body"):
            body_html = f"<b>{escape_html(ref_body_plain)}</b>"
        else:
            body_html = escape_html(ref_body_plain)
        new_text = f"{header_html}\n\n{body_html}\n\n⤶{pers}"
        try:
            await bot.edit_message_text(new_text, chat_id=int(user_id_str), message_id=int(local_mid), parse_mode="HTML")
        except Exception as e:
            print("increment_and_edit_reply_count_for_local to", user_id_str, "failed:", e)


def fmt_amount(num):
    try:
        return f"{num:,}"
    except Exception:
        return str(num)
    
ZERO_WIDTH_RE = re.compile(r"[\u200B\u200C\u200D\uFEFF]")

def normalize_text_for_check(s: str) -> str:
    if s is None:
        return ""
    s = ZERO_WIDTH_RE.sub("", s)
    s = re.sub(r"\r\n", "\n", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_plain_official_text(wallet: int) -> str:
    return f"🙎🏻‍♂ You:\n\n💰موجودی من :\n{fmt_amount(wallet)} 🪙"


# ---------- فایل خواندن/ذخیره ----------
def load_data():
    global users_data, users
    if os.path.exists(USERS_DATA_PATH):
        try:
            with open(USERS_DATA_PATH, "r", encoding="utf-8") as f:
                users_data = json.load(f)
        except Exception:
            users_data = {}
    else:
        users_data = {}
    users = list(users_data.keys())

SAVE_LOCK = asyncio.Lock()

async def save_data():
    data = json.dumps(users_data, ensure_ascii=False, indent=4)
    loop = asyncio.get_running_loop()
    def _write():
        with open(USERS_DATA_PATH, "w", encoding="utf-8") as f:
            f.write(data)
    async with SAVE_LOCK:
        await loop.run_in_executor(None, _write)


load_data()

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
phone = os.getenv("PHONE")
session = os.getenv("SESSION_NAME")
client = TelegramClient(session, api_id, api_hash)
async def get_chat_id(identifier: str):
    if identifier is None:
        return None
    if isinstance(identifier, str) and identifier.startswith("@"):
        identifier = identifier[1:]
    try:
        if isinstance(identifier, str) and identifier.isdigit():
            return int(identifier)
    except Exception:
        pass

    try:
        entity = await client.get_entity(identifier)
        return getattr(entity, "id", None)
    except Exception as e:
        print("Telethon resolve error:", e)
        return None


# ---------- کمکی‌ها ----------
def persian_digits(num):
    s = str(num)
    trans = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")
    return s.translate(trans)


async def get_display_name(chat_id):
    try:
        ch = await bot.get_chat(chat_id)
        if getattr(ch, "username", None):
            return "@" + ch.username
        name = getattr(ch, "first_name", "") or ""
        if getattr(ch, "last_name", None):
            name += " " + ch.last_name
        return name.strip() or "ناشناس"
    except Exception:
        return "ناشناس"

async def ensure_user(chat_id):
    global users
    key = str(chat_id)
    if key not in users_data:
        users_data[key] = {
            "wallet": 50000,
            "state": None,
            "bet_amount": 0,
            "pending_msg_id": None,
            "last_global_sent": None,
            "temp_gift_to": None
        }
        await save_data()
        users = list(users_data.keys())
    return users_data[key]

def user_exists(chat_id):
    return str(chat_id) in users_data

def easy_input(user_input):
    s = user_input.strip()
    try:
        if s.endswith("میل"):
            return int(s[:-3]) * 1_000_000
        if s.endswith("m"):
            return int(s[:-1]) * 1_000_000
        if s.endswith("k"):
            return int(s[:-1]) * 1_000
        if s.endswith("کا"):
            return int(s[:-2]) * 1_000
        if s.endswith("بیل"):
            return int(s[:-3]) * 1_000_000_000
        if s.endswith("b"):
            return int(s[:-1]) * 1_000_000_000
        return int(s)
    except Exception:
        raise ValueError("invalid amount")

# ---------- کیبوردها ----------
def main_keyboard(chat_id):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(types.KeyboardButton("🎲 تاس"), types.KeyboardButton("🌱 گل یا پوچ"))
    kb.row(types.KeyboardButton("💰 موجودی"), types.KeyboardButton("🎁 گیفت"))
    kb.row(types.KeyboardButton("🏆 برترین‌ها"), types.KeyboardButton("👥️️ تعداد اعضای چت جهانی"))
    if int(chat_id) in ADMINS:
        kb.row(types.KeyboardButton("ℹ️ درباره ما"), types.KeyboardButton("👩‍🚀 پنل مدیریت"))
    else:
        kb.row(types.KeyboardButton("ℹ️ درباره ما"))
    return kb

def manage_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(types.KeyboardButton("🪙 تغییر سکه"), types.KeyboardButton("💰 نمایش موجودی"))
    kb.row(types.KeyboardButton("بازگشت ↪️"))
    return kb


def back_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row(types.KeyboardButton("بازگشت ↪️"))
    return kb

def bet_amount_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row(types.KeyboardButton("نصف"), types.KeyboardButton("مکس"))
    kb.row(types.KeyboardButton("بازگشت ↪️"))
    return kb

def dice_choice_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row(types.KeyboardButton("زوج"), types.KeyboardButton("فرد"))
    kb.row(types.KeyboardButton("1"), types.KeyboardButton("2"), types.KeyboardButton("3"))
    kb.row(types.KeyboardButton("4"), types.KeyboardButton("5"), types.KeyboardButton("6"))
    kb.row(types.KeyboardButton("بازگشت ↪️"))
    return kb

def rps_choice_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row(types.KeyboardButton("چپ 🤚"), types.KeyboardButton("راست ✋"))
    kb.row(types.KeyboardButton("بازگشت ↪️"))
    return kb

# ---------- هندلر شروع ----------
@bot.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    uid = message.chat.id
    user = await ensure_user(uid)
    user["state"] = None
    user["bet_amount"] = 0
    user["temp_gift_to"] = None
    await save_data()
    txt = f"سلام! به ربات PotyBot {hspoiler('(نسخه آزمایشی)')} خوش اومدی 🌹\n\n🌐 برای ارسال پیام در چت جهانی کافیه اول پیامتون نقطه بزارید. مثال:\n.سلام به همگی"
    await bot.send_message(uid, txt, parse_mode="HTML", reply_markup=main_keyboard(uid))

# ---------- هندلر پیام‌های متنی ----------
@bot.message_handler(func=lambda m: True, content_types=['text'])
async def main_message_handler(message: types.Message):
        if time.time() - message.date > 30:
            pass

        uid = message.chat.id
        text = message.text.strip()
        user = await ensure_user(uid)

        # دکمه بازگشت
        if text == "بازگشت ↪️":
            user["state"] = None
            user["bet_amount"] = 0
            user["temp_gift_to"] = None
            await save_data()
            await bot.send_message(uid, "بازگشت به منوی اصلی", reply_markup=main_keyboard(uid))
            return

        # منوی اصلی
        if text == "💰 موجودی":
            await bot.send_message(uid, f"💰 موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=main_keyboard(uid))
            return

        if text == "ℹ️ درباره ما":
            await bot.send_message(uid, f"‌‌{hbold(' ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌‌ ‌  ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌‌ ‌‌ ‌ ‌ ‌ ‌ ‌ ‌ ‌• PotyBot •')}\n\n🧑🏻‍🚀 سازنده: @iman_h37\n\n🤖 لینک ربات پاتی بات: @PotyBot_Robot\n\n{hspoiler('نسخه آزمایشی')}", parse_mode="HTML", reply_markup=main_keyboard(uid))
            return

        if text == "👩‍🚀 پنل مدیریت" and int(uid) in ADMINS:
            user["state"] = None
            user["admin_target"] = None
            await save_data()
            await bot.send_message(uid, "به پنل مدیریت خوش اومدی\n\nیک گزینه را انتخاب کن:", reply_markup=manage_keyboard())
            return

        if text == "💰 نمایش موجودی" and int(uid) in ADMINS:
            user["state"] = "awaiting_admin_show_target"
            user["admin_target"] = None
            await save_data()
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            kb.row(types.KeyboardButton("بازگشت ↪️"))
            await bot.send_message(uid, "آیدی کاربر موردنظر را وارد کن:", reply_markup=kb)
            return

        
        if text == "🪙 تغییر سکه" and int(uid) in ADMINS:
            user["state"] = "awaiting_admin_change_target"
            user["admin_target"] = None
            await save_data()
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            kb.row(types.KeyboardButton("خودم"), types.KeyboardButton("بازگشت ↪️"))
            await bot.send_message(uid, "آیدی کاربر موردنظر را وارد یا «خودم» را انتخاب کن:", reply_markup=kb)
            return

        if text == "👥️️ تعداد اعضای چت جهانی":
            cnt = 0
            ms = await bot.send_message(uid, "درحال دریافت ...")
            for u in list(users_data.keys()):
                try:
                    u_int = int(u)
                    if u_int != uid:
                        sent = await bot.send_message(u_int, ".")
                        if sent:
                            try:
                                await bot.delete_message(u_int, sent.message_id)
                            except Exception:
                                pass
                            cnt += 1
                except Exception:
                    continue
            await bot.edit_message_text(f"👥️️ تعداد عضوهای چت جهانی: {cnt+1:,}", uid, ms.message_id)
            return
        if text == "🎲 تاس":
            user["state"] = "awaiting_bet_amount"
            await save_data()
            sent = await bot.send_message(uid, f"🪙 مقدار شرط رو وارد کن:\n💰موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=bet_amount_keyboard())
            user["pending_msg_id"] = sent.message_id
            await save_data()
            return

        if text == "🌱 گل یا پوچ":
            user["state"] = "awaiting_rps_amount"
            await save_data()
            sent = await bot.send_message(uid, f"🪙 مقدار شرط رو وارد کن:\n💰موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=bet_amount_keyboard())
            user["pending_msg_id"] = sent.message_id
            await save_data()
            return

        # ---------- برترین‌ها ----------
        if text == "🏆 برترین‌ها":
            arr = []
            for k, v in users_data.items():
                try:
                    if int(k) not in ADMINS:
                        arr.append((int(k), int(v.get("wallet", 0))))
                except Exception:
                    continue
            arr.sort(key=lambda x: x[1], reverse=True)
            top5 = arr[:5]
            if not top5:
                await bot.send_message(uid, "هنوز کاربری ثبت نشده است.", reply_markup=main_keyboard(uid))
                return
            lines = ["🏆 5 نفر برتر بیشترین سکه:\n"]
            i = 1
            for chatid, amt in top5:
                name = await get_display_name(chatid)
                lines.append(f"{i}. {name}  —  {fmt_amount(amt)} 🪙")
                i += 1
            text_out = "\n".join(lines)
            await bot.send_message(uid, text_out, reply_markup=main_keyboard(uid))
            return

        # ---------- گیفت ----------
        if text == "🎁 گیفت":
            user["state"] = "awaiting_gift_recipient"
            user["temp_gift_to"] = None
            await save_data()
            await bot.send_message(uid, "آیدی فرد گیرنده سکه را وارد کنید:", reply_markup=back_keyboard())
            return


        if user.get("state") == "awaiting_admin_show_target" and int(uid) in ADMINS:
            rec_text = text.strip()
            rec_text = rec_text.replace(" ", "").replace("\u200f", "").replace("\u200e", "")
            rec_id = None
            if rec_text.isdigit():
                rec_id = int(rec_text)
            else:
                try:
                    rec_id = await get_chat_id(rec_text)
                except Exception:
                    rec_id = None

            if not rec_id:
                await bot.send_message(uid, "آیدی نامعتبر است. دوباره وارد کن یا «بازگشت ↪️» بزن.", reply_markup=back_keyboard())
                return

            if not user_exists(rec_id):
                await bot.send_message(uid, "کاربر در دیتابیس موجود نیست — کاربر باید ابتدا /start را بزند تا حسابش ساخته شود.", reply_markup=manage_keyboard())
                user["state"] = None
                await save_data()
                return

            target = await ensure_user(rec_id)
            wallet = int(target.get("wallet", 0))
            name = await get_display_name(rec_id)

            await bot.send_message(uid, f"💰 موجودی کاربر {name}:\n{fmt_amount(wallet)} 🪙", reply_markup=manage_keyboard())
            user["state"] = None
            user["admin_target"] = None
            await save_data()
            return


        if user.get("state") == "awaiting_admin_change_target" and int(uid) in ADMINS:
            rec_text = text.strip()
            rec_text = rec_text.replace(" ", "").replace("\u200f", "").replace("\u200e", "")
            rec_id = None

            if rec_text == "خودم":
                rec_id = uid
            else:
                if rec_text.isdigit():
                    rec_id = int(rec_text)
                else:
                    try:
                        rec_id = await get_chat_id(rec_text)
                    except Exception as e:
                        rec_id = None

            if not rec_id:
                await bot.send_message(uid, "آیدی نامعتبر است. دوباره وارد کن یا «بازگشت ↪️» بزن.", reply_markup=back_keyboard())
                return

            user["admin_target"] = int(rec_id)
            user["state"] = "awaiting_admin_change_amount"
            await save_data()
            moj = await ensure_user(rec_id)
            await bot.send_message(uid, f"💰موجودی فعلی کاربر {await get_display_name(rec_id)}:\n{fmt_amount(moj['wallet'])} 🪙\n\nمقدار جدید سکه کاربر را وارد کنید:", reply_markup=back_keyboard())
            return

        # دریافت مقدار جدید و اعمال تغییر
        if user.get("state") == "awaiting_admin_change_amount" and int(uid) in ADMINS:
            try:
                amount = easy_input(text)
            except Exception:
                await bot.send_message(uid, "مقدار نامعتبر است.", reply_markup=back_keyboard())
                return

            rec_id = user.get("admin_target")
            if not rec_id:
                await bot.send_message(uid, "کاربر مشخص نشده، دوباره از گزینهٔ تغییر سکه استفاده کن.", reply_markup=main_keyboard(uid))
                user["state"] = None
                user["admin_target"] = None
                await save_data()
                return

            target = await ensure_user(rec_id)
            prev = int(target.get("wallet", 0))
            target["wallet"] = int(amount)

            user["state"] = None
            user["admin_target"] = None
            await save_data()

            await bot.send_message(uid, f"✅ تغییر سکه انجام شد.\n\nآیدی کاربر: {await get_display_name(rec_id)}\nموجودی قبلی: {fmt_amount(prev)} 🪙\nموجودی جدید: {fmt_amount(target['wallet'])} 🪙", reply_markup=main_keyboard(uid))
            return

        # ---------- اگر در حالت انتظار مقدار شرط باشیم ----------
        if user.get("state") == "awaiting_bet_amount":
            # پشتیبانی از 'نصف' و 'مکس'
            try:
                if text == "نصف":
                    amount = int(user["wallet"] / 2)
                elif text == "مکس":
                    amount = int(user["wallet"])
                else:
                    amount = easy_input(text)
            except Exception:
                await bot.send_message(uid, "مقدار معتبر نیست ❌", reply_markup=bet_amount_keyboard())
                return

            if amount <= 0:
                await bot.send_message(uid, "مقدار معتبر نیست ❌", reply_markup=bet_amount_keyboard())
                return
            if amount > user["wallet"]:
                await bot.send_message(uid, f"❌ موجودی شما کافی نیست\n\n💰موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=bet_amount_keyboard())
                return

            user["bet_amount"] = amount
            user["state"] = "awaiting_even_odd"
            await save_data()

            # ویرایش پیام قبلی (اگر داریم) یا ارسال پیام جدید
            try:
                if user.get("pending_msg_id"):
                    await bot.edit_message_text(f"🪙 مقدار شرط: {fmt_amount(amount)} \n نوع شرط رو انتخاب کن 👇", uid, user["pending_msg_id"], reply_markup=None)
                    await bot.send_message(uid, "انتخاب کن:", reply_markup=dice_choice_keyboard())
                else:
                    sent = await bot.send_message(uid, f"🪙 مقدار شرط: {fmt_amount(amount)} \n نوع شرط رو انتخاب کن 👇", reply_markup=dice_choice_keyboard())
                    user["pending_msg_id"] = sent.message_id
                    await save_data()
            except Exception:
                sent = await bot.send_message(uid, f"🪙 مقدار شرط: {fmt_amount(amount)} \n نوع شرط رو انتخاب کن 👇", reply_markup=dice_choice_keyboard())
                user["pending_msg_id"] = sent.message_id
                await save_data()
            return

        # ---------- انتخاب زوج/فرد یا عدد (حالت تاس) ----------
        if user.get("state") == "awaiting_even_odd":
            choice = text
            bet = user["bet_amount"]
            if choice in ['زوج', 'فرد']:
                dice = random.randint(1, 6)
                Dice_mode = 'فرد' if dice in [1, 3, 5] else 'زوج'
                if choice == Dice_mode:
                    user["wallet"] += bet
                    try:
                        await bot.edit_message_text(f'شما برنده شدید🙂✅\n\n➕{fmt_amount(bet)} سکه به شما اضافه شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی قبلی شما : {fmt_amount(user["wallet"] - bet)}\n=============================\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}', uid, user.get("pending_msg_id") or 0)
                    except Exception:
                        await bot.send_message(uid, f'شما برنده شدید🙂✅\n\n➕{fmt_amount(bet)} سکه به شما اضافه شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}')
                else:
                    user["wallet"] -= bet
                    if user["wallet"] < 1000:
                        user["wallet"] = 1000
                    try:
                        await bot.edit_message_text(f'شما بازنده شدید🥺❌\n\n➖{fmt_amount(bet)} سکه از شما کم شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی قبلی شما : {fmt_amount(user["wallet"] + bet)}\n=============================\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}', uid, user.get("pending_msg_id") or 0)
                    except Exception:
                        await bot.send_message(uid, f'شما بازنده شدید🥺❌\n\n➖{fmt_amount(bet)} سکه از شما کم شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}')
            elif choice.isnumeric() and int(choice) in [1, 2, 3, 4, 5, 6]:
                dice = random.randint(1, 6)
                if int(choice) == dice:
                    user["wallet"] += bet * 6
                    try:
                        await bot.edit_message_text(f'شما برنده شدید🙂✅\n\n➕{fmt_amount(bet*6)} سکه به شما اضافه شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی قبلی شما : {fmt_amount(user["wallet"] - bet * 6)}\n============================\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}', uid, user.get("pending_msg_id") or 0)
                    except Exception:
                        await bot.send_message(uid, f'شما برنده شدید🙂✅\n\n➕{fmt_amount(bet*6)} سکه به شما اضافه شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}')
                else:
                    user["wallet"] -= bet
                    if user["wallet"] < 1000:
                        user["wallet"] = 1000
                    try:
                        await bot.edit_message_text(f'شما بازنده شدید🥺❌\n\n➖{fmt_amount(bet)} سکه از شما کم شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی قبلی شما : {fmt_amount(user["wallet"] + bet)}\n=============================\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}', uid, user.get("pending_msg_id") or 0)
                    except Exception:
                        await bot.send_message(uid, f'شما بازنده شدید🥺❌\n\n➖{fmt_amount(bet)} سکه از شما کم شد\n\n🎲 تاس رو شده: {dice}\n\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}')
            else:
                await bot.send_message(uid, "انتخاب نامعتبر است.", reply_markup=dice_choice_keyboard())
                return

            # پایان بازی تاس
            user["state"] = None
            user["bet_amount"] = 0
            user["pending_msg_id"] = None
            await save_data()
            await bot.send_message(uid, "برگشت به منوی اصلی", reply_markup=main_keyboard(uid))
            return

        # ---------- شرط RPS (گل یا پوچ) ----------
        if user.get("state") == "awaiting_rps_amount":
            try:
                if text == "نصف":
                    amount = int(user["wallet"] / 2)
                elif text == "مکس":
                    amount = int(user["wallet"])
                else:
                    amount = easy_input(text)
            except Exception:
                await bot.send_message(uid, "مقدار معتبر نیست ❌", reply_markup=bet_amount_keyboard())
                return

            if amount <= 0:
                await bot.send_message(uid, "مقدار معتبر نیست ❌", reply_markup=bet_amount_keyboard())
                return
            if amount > user["wallet"]:
                await bot.send_message(uid, f"❌ موجودی شما کافی نیست\n\n💰موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=bet_amount_keyboard())
                return

            user["bet_amount"] = amount
            user["state"] = "awaiting_rps_choice"
            await save_data()
            try:
                if user.get("pending_msg_id"):
                    await bot.edit_message_text(f"🪙 مقدار شرط: {fmt_amount(amount)} \n حدس بزن گل تو کدوم دست رباته 👇", uid, user["pending_msg_id"])
                    await bot.send_message(uid, "انتخاب کن:", reply_markup=rps_choice_keyboard())
                else:
                    sent = await bot.send_message(uid, f"🪙 مقدار شرط: {fmt_amount(amount)} \n حدس بزن گل تو کدوم دست رباته 👇", reply_markup=rps_choice_keyboard())
                    user["pending_msg_id"] = sent.message_id
                    await save_data()
            except Exception:
                sent = await bot.send_message(uid, f"🪙 مقدار شرط: {fmt_amount(amount)} \n حدس بزن گل تو کدوم دست رباته 👇", reply_markup=rps_choice_keyboard())
                user["pending_msg_id"] = sent.message_id
                await save_data()
            return

        if user.get("state") == "awaiting_rps_choice" and text in ["چپ 🤚", "راست ✋"]:
            bet = user["bet_amount"]
            bot_choice = random.choice(["چپ 🤚", "راست ✋"])
            if bot_choice == text:
                user["wallet"] += bet
                try:
                    await bot.edit_message_text(f'شما گل را درست حدس زدید✅🙂\n\n➕{fmt_amount(bet)} سکه به شما اضافه شد\n\n🪙موجودی قبلی شما : {fmt_amount(user["wallet"] - bet)}\n=============================\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}', uid, user.get("pending_msg_id") or 0)
                except Exception:
                    await bot.send_message(uid, f'شما گل را درست حدس زدید✅🙂\n\n➕{fmt_amount(bet)} سکه به شما اضافه شد\n\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}')
            else:
                user["wallet"] -= bet
                if user["wallet"] < 1000:
                    user["wallet"] = 1000
                try:
                    await bot.edit_message_text(f'شما نتوانستید گل را حدس بزنید❌🥺\n\n➖{fmt_amount(bet)} سکه از شما کم شد\n\n🪙موجودی قبلی شما : {fmt_amount(user["wallet"] + bet)}\n=============================\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}', uid, user.get("pending_msg_id") or 0)
                except Exception:
                    await bot.send_message(uid, f'شما نتوانستید گل را حدس بزنید❌🥺\n\n➖{fmt_amount(bet)} سکه از شما کم شد\n\n🪙موجودی فعلی شما : {fmt_amount(user["wallet"])}')
            # پایان بازی rps
            user["state"] = None
            user["bet_amount"] = 0
            user["pending_msg_id"] = None
            await save_data()
            await bot.send_message(uid, "برگشت به منوی اصلی", reply_markup=main_keyboard(uid))
            return

        # ---------- جریان گیفت: دریافت آیدی گیرنده ----------
        if user.get("state") == "awaiting_gift_recipient":
            rec_text = text
            rec_id = None
            try:
                # with client:
                rec_id = await get_chat_id(rec_text)
            except Exception:
                await bot.send_message(uid, f"آیدی نامعتبر است", reply_markup=back_keyboard())
                return

            if rec_id == uid:
                await bot.send_message(uid, "نمی‌توانید به خودتان گیفت بزنید.", reply_markup=main_keyboard(uid))
                user["state"] = None
                user["temp_gift_to"] = None
                await save_data()
                return

            # گیرنده باید قبلاً با ربات شروع کرده باشد
            if not user_exists(rec_id):
                await bot.send_message(uid, "گیرنده در دیتابیس موجود نیست — گیرنده باید ابتدا /start رو بزنه تا حسابش ساخته بشه.", reply_markup=main_keyboard(uid))
                user["state"] = None
                user["temp_gift_to"] = None
                return

            user["temp_gift_to"] = int(rec_id)
            user["state"] = "awaiting_gift_amount"
            await save_data()
            await bot.send_message(uid, f"مقدار سکه رو وارد کن:\n💰 موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=bet_amount_keyboard())
            return

        # ---------- جریان گیفت: دریافت مقدار و انجام انتقال ----------
        if user.get("state") == "awaiting_gift_amount":
            # مقدار می‌تواند 'نصف' یا 'مکس' یا عدد با easy_input باشد
            try:
                if text == "نصف":
                    amount = int(user["wallet"] / 2)
                elif text == "مکس":
                    amount = int(user["wallet"])
                else:
                    amount = easy_input(text)
            except Exception:
                await bot.send_message(uid, "مقدار نامعتبر است.", reply_markup=bet_amount_keyboard())
                return

            if amount <= 0:
                await bot.send_message(uid, "مقدار باید بزرگتر از صفر باشد.", reply_markup=bet_amount_keyboard())
                return
            if amount > user["wallet"]:
                await bot.send_message(uid, f"موجودی کافی نیست. موجودی شما: {fmt_amount(user['wallet'])}", reply_markup=main_keyboard(uid))
                return

            rec_id = user.get("temp_gift_to")
            if not rec_id:
                await bot.send_message(uid, "گیرنده مشخص نشده، لطفا دوباره از گزینهٔ گیفت استفاده کنید.", reply_markup=main_keyboard(uid))
                user["state"] = None
                user["temp_gift_to"] = None
                return

            # نهایی‌سازی انتقال
            recipient_data = await ensure_user(rec_id)

            user["wallet"] -= amount
            recipient_data["wallet"] += amount

            # بازنشانی state و ذخیره
            user["state"] = None
            user["temp_gift_to"] = None
            await save_data()

            # پیام به فرستنده
            try:
                await bot.send_message(uid, f"🎁گیفت با موفقیت انجام شد✅\n\n🔄انتقال {fmt_amount(amount)} 🪙\n↗️از: @{message.chat.username}\n↙️به: {await get_display_name(rec_id)}\n\n➖{fmt_amount(amount)} سکه از شما کم شد\n\n🪙موجودی فرد مقابل : {fmt_amount(recipient_data['wallet'])}\n=============================\n🪙موجودی شما : {fmt_amount(user['wallet'])}", reply_markup=main_keyboard(uid))
            except Exception:
                pass

            # پیام به گیرنده — اگر ارسال پیام با خطا مواجه شد، به فرستنده اطلاع می‌دهیم
            try:
                await bot.send_message(rec_id, hbold(f'🎁 رسید گیفت:\n🔄 انتقال: {fmt_amount(amount)} 🪙\n↗️ از: @{message.chat.username}\n↙️ به: {await get_display_name(rec_id)}\n\n🪙موجودی شما : {fmt_amount(recipient_data["wallet"])}'), parse_mode="HTML", reply_markup=main_keyboard(rec_id))
            except Exception:
                pass
            return
        
        # ---------- پیام‌های چت جهانی (شروع با نقطه) ----------
        if text.startswith('.'):
            try:
                await bot.delete_message(uid, message.message_id)
            except Exception:
                pass

            user_plain = text[1:].strip()

            # ---------- 1) دستور رسمی .موجودی ----------
            if user_plain in ("موجودی", "موجودی من"):
                try:
                    user_wallet = int(user.get("wallet", 0))
                except Exception:
                    user_wallet = 0
                display_name = await get_display_name(uid)
                body_plain = f"💰موجودی من :\n{fmt_amount(user_wallet)} 🪙"
                origin_id = str(uuid.uuid4())

                reply_mid = message.reply_to_message.message_id if message.reply_to_message else None
                # رکورد مرجع در لیست owner (اگر او روی یک پیام ریپلای کرده)
                ref_owner = None
                if reply_mid:
                    ref_owner = next((m for m in global_messages.get(str(uid), []) if m.get("message_id") == reply_mid), None)

                # 1) ارسال به owner (You) و ذخیره
                owner_local_mid = await send_and_store(uid, "🙎🏻‍♂ You:", body_plain, origin_id, is_bold_body=True, reply_to_local_mid=reply_mid if reply_mid else None, source_chat_id=uid)

                # 2) ارسال به همهٔ دیگران
                for u in list(users_data.keys()):
                    try:
                        u_int = int(u)
                    except Exception:
                        continue
                    if u_int == uid:
                        continue

                    # تعیین reply_to محلی برای این گیرنده براساس origin_index/ref_owner
                    reply_to_for_user = None
                    if reply_mid and ref_owner:
                        rec = find_user_record_by_origin(u_int, ref_owner.get("source_chat_id"), ref_owner.get("origin_id"))
                        if rec:
                            reply_to_for_user = rec.get("message_id")

                    header_plain = f"👤 {display_name}:"
                    sent_mid = await send_and_store(u_int, header_plain, body_plain, origin_id, is_bold_body=True, reply_to_local_mid=reply_to_for_user, source_chat_id=uid)

                    # اگر reply_to_for_user بود فوراً شمارش را افزایش بده و ویرایش کن
                    if reply_to_for_user:
                        await increment_and_edit_reply_count_for_local(str(u_int), reply_to_for_user)

                # 3) حالا برای owner هم اگر ref_owner وجود دارد شمارش را افزایش بده
                if reply_mid and ref_owner:
                    await increment_and_edit_reply_count_for_local(str(uid), reply_mid)

                return

            # ---------- 2) بقیه پیام‌های نقطه‌ای ----------
            # جلوگیری از جعلِ plain رسمی
            try:
                my_wallet = int(user.get("wallet", 0))
            except Exception:
                my_wallet = 0
            expected_plain = build_plain_official_text(my_wallet)
            if normalize_text_for_check(user_plain) == normalize_text_for_check(expected_plain):
                try:
                    alert = await bot.send_message(uid, "⚠️ تلاش جعل موجودی شناسایی شد — ارسال شما پخش نخواهد شد.", reply_markup=main_keyboard(uid))
                    await asyncio.sleep(3)
                    try:
                        await bot.delete_message(uid, alert.message_id)
                    except Exception:
                        pass
                except Exception:
                    pass
                return

            # sanitize
            if "💰" in user_plain:
                user_plain = user_plain.replace("💰", " ")
            if "✅" in user_plain:
                user_plain = user_plain.replace("✅", "☑️")
            sanitized_body = user_plain
            origin_id = str(uuid.uuid4())

            # reply local case
            if message.reply_to_message:
                reply_mid = message.reply_to_message.message_id
                # مرجع در لیست sender
                ref = next((m for m in global_messages.get(str(uid), []) if m.get("message_id") == reply_mid), None)

                for u in list(users_data.keys()):
                    try:
                        u_int = int(u)
                    except Exception:
                        continue

                    reply_to_for_user = None
                    if ref:
                        rec = find_user_record_by_origin(u_int, ref.get("source_chat_id"), ref.get("origin_id"))
                        if rec:
                            reply_to_for_user = rec.get("message_id")

                    if u_int == uid:
                        header_plain = "🙎🏻‍♂ You:"
                    else:
                        header_plain = f"👤 {await get_display_name(message.from_user.id)}:"

                    sent_mid = await send_and_store(u_int, header_plain, sanitized_body, origin_id, is_bold_body=False, reply_to_local_mid=reply_to_for_user, source_chat_id=uid)

                    if reply_to_for_user:
                        await increment_and_edit_reply_count_for_local(str(u_int), reply_to_for_user)

                return

            # no-reply broadcast
            for u in list(users_data.keys()):
                try:
                    u_int = int(u)
                except Exception:
                    continue
                if u_int == uid:
                    header_plain = "🙎🏻‍♂ You:"
                else:
                    header_plain = f"👤 {await get_display_name(message.from_user.id)}:"
                await send_and_store(u_int, header_plain, sanitized_body, origin_id, is_bold_body=False, source_chat_id=uid)

            return

        await bot.send_message(uid, ("برای بازی با ربات از دکمه ها استفاده کن 🔣\n\nدر صورت نبودن دکمه ها /start رو بزن❗\n\n🌐 برای ارسال پیام در چت جهانی کافیه اول پیامتون نقطه بزارید. مثال:\n.سلام به همگی"), reply_markup=main_keyboard(uid))


@app.on_event("startup")
async def on_startup():
    # start telethon
    try:
        await client.start()
        print("Telethon started")
    except Exception as e:
        print("Telethon start failed:", e)
    # start prune loop background
    app.state.prune_task = asyncio.create_task(prune_loop())

@app.on_event("shutdown")
async def on_shutdown():
    # cancel prune
    task = getattr(app.state, "prune_task", None)
    if task:
        task.cancel()
        try:
            await task
        except Exception:
            pass
    # remove webhook (optional)
    try:
        await bot.remove_webhook()
    except Exception:
        pass
    try:
        await client.disconnect()
    except Exception:
        pass

@app.post(f"/{BOT_TOKEN}")
async def telegram_webhook(req: Request):
    try:
        body = await req.body()
        if not body:
            raise HTTPException(400)
        update = types.Update.de_json(body.decode("utf-8"))
        # AsyncTeleBot supports process_new_updates as coroutine
        await bot.process_new_updates([update])
        return {"ok": True}
    except Exception as e:
        print("webhook error:", e)
        raise HTTPException(500)
    
# ---------- اجرای بات ----------
# async def main():
#     print("Starting Telethon client...")
#     await client.start()

#     # start prune background task
#     prune_task = asyncio.create_task(prune_loop(interval_seconds=3600, max_age_seconds=86400))

#     print("Telethon started. Starting bot polling...")
#     polling_task = asyncio.create_task(bot._process_polling(timeout=60))

#     try:
#         await polling_task
#     except asyncio.CancelledError:
#         pass
#     finally:
#         # cleanup
#         try:
#             polling_task.cancel()
#         except Exception:
#             pass
#         try:
#             prune_task.cancel()
#         except Exception:
#             pass
#         try:
#             await client.disconnect()
#         except Exception:
#             pass
#         try:
#             await bot.close()
#         except Exception:
#             pass



# if __name__ == "__main__":
#     asyncio.run(main())