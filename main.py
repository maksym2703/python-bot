import os
import sqlite3
from datetime import datetime
from statistics import median

from dotenv import load_dotenv
from pybit.exceptions import FailedRequestError
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import Updater, CommandHandler, CallbackContext

# ===================== .env (override) =====================
DOTENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(DOTENV_PATH, override=True)

# ===================== Налаштування =====================
# Bybit: свічки беруться з цього прапорця; баланс іде за режимом користувача (/link)
TESTNET = os.getenv("BYBIT_TESTNET", "false").strip().lower() == "true"

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TG_CHAT_ID_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_CHAT_ID = int(TG_CHAT_ID_STR) if TG_CHAT_ID_STR.isdigit() else None

# Аналітика / алерти
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").strip()
INTERVAL = os.getenv("INTERVAL", "240").strip()  # "1","3","5","15","60","240","D"
CANDLES_LIMIT = int(os.getenv("LIMIT", "200"))
EPS_PCT = float(os.getenv("EPS_PCT", "0.008"))  # 0.8%
ALERT_PCT = float(os.getenv("ALERT_PCT", "0.002"))  # 0.2%
PING_SECONDS = int(os.getenv("PING_SECONDS", "14400"))  # дефолт 4 години

if not TG_TOKEN or TG_CHAT_ID is None:
    raise RuntimeError("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID не задані або некоректні в .env")

# Публічна сесія для свічок (без ключів)
public_session = HTTP(testnet=TESTNET)

# ===================== SQLite (ключі + whitelist/ACL) =====================
DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")


def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # ключі користувачів
    cur.execute("""
                CREATE TABLE IF NOT EXISTS users
                (
                    user_id
                    INTEGER
                    PRIMARY
                    KEY,
                    api_key
                    TEXT
                    NOT
                    NULL,
                    api_secret
                    TEXT
                    NOT
                    NULL,
                    testnet
                    INTEGER
                    NOT
                    NULL
                    CHECK (
                    testnet
                    IN
                (
                    0,
                    1
                ))
                    )
                """)
    # whitelist (ACL)
    cur.execute("""
                CREATE TABLE IF NOT EXISTS acl
                (
                    user_id
                    INTEGER
                    PRIMARY
                    KEY,
                    role
                    TEXT
                    NOT
                    NULL
                    DEFAULT
                    'user' -- 'admin' або 'user'
                )
                """)
    # власник бота з .env — адміністратор за замовчуванням
    cur.execute("INSERT OR IGNORE INTO acl(user_id, role) VALUES(?, 'admin')", (TG_CHAT_ID,))
    con.commit()
    con.close()


def save_user(user_id: int, api_key: str, api_secret: str, testnet: bool):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "REPLACE INTO users(user_id, api_key, api_secret, testnet) VALUES (?,?,?,?)",
        (user_id, api_key.strip(), api_secret.strip(), 1 if testnet else 0),
    )
    con.commit()
    con.close()


def get_user(user_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT api_key, api_secret, testnet FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    if row:
        return {"api_key": row[0], "api_secret": row[1], "testnet": bool(row[2])}
    return None


def allow_user(user_id: int, role: str = "user"):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("REPLACE INTO acl(user_id, role) VALUES(?,?)", (user_id, role))
    con.commit()
    con.close()


def deny_user(user_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM acl WHERE user_id=?", (user_id,))
    con.commit()
    con.close()


def get_role(user_id: int) -> str:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT role FROM acl WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else ""


def is_admin(user_id: int) -> bool:
    return get_role(user_id) == "admin"


def is_allowed(user_id: int) -> bool:
    return get_role(user_id) in ("user", "admin")


# ===================== Хелпери аналітики =====================
def fmt(n):
    try:
        return f"{float(n):,.4f}".replace(",", " ")
    except Exception:
        return str(n)


def fetch_klines(symbol: str, interval: str, limit: int):
    """Bybit V5 Spot kline → список свічок (за часом зрост.)."""
    r = public_session.get_kline(category="spot", symbol=symbol, interval=interval, limit=limit)
    raw = r["result"]["list"]
    kl = []
    for it in raw:
        ts, o, h, l, c, v, t = it
        kl.append({"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)})
    kl.sort(key=lambda x: x["ts"])
    return kl


def local_extrema(kl):
    """Локальні міні/максі за 3-свічковим правилом."""
    lows, highs = [], []
    for i in range(1, len(kl) - 1):
        if kl[i]["low"] <= kl[i - 1]["low"] and kl[i]["low"] <= kl[i + 1]["low"]:
            lows.append(kl[i]["low"])
        if kl[i]["high"] >= kl[i - 1]["high"] and kl[i]["high"] >= kl[i + 1]["high"]:
            highs.append(kl[i]["high"])
    return lows, highs


def cluster_levels(values, eps_pct: float):
    """Кластеризація значень (±eps_pct) → [(медіана, кількість), ...] за частотою."""
    if not values:
        return []
    values = sorted(values)
    clusters = [[values[0]]]
    for x in values[1:]:
        ref = clusters[-1][-1]
        if abs(x - ref) / ref <= eps_pct:
            clusters[-1].append(x)
        else:
            clusters.append([x])
    res = [(median(g), len(g)) for g in clusters]
    res.sort(key=lambda e: e[1], reverse=True)
    return res


def find_peak_levels(symbol: str, interval: str, limit: int, eps_pct: float):
    """→ (top_min, top_max, last_close), де top_* = (price, count)."""
    kl = fetch_klines(symbol, interval, limit)
    lows, highs = local_extrema(kl)
    low_clusters = cluster_levels(lows, eps_pct)
    high_clusters = cluster_levels(highs, eps_pct)
    best_min = low_clusters[0] if low_clusters else (None, 0)
    best_max = high_clusters[0] if high_clusters else (None, 0)
    last_close = kl[-1]["close"] if kl else None
    return best_min, best_max, last_close


def get_usdt_balance_for(user_id: int):
    """
    Баланс для конкретного юзера. Пробуємо UNIFIED → SPOT → CONTRACT.
    Повертає float або None.
    """
    u = get_user(user_id)
    if not u:
        return None
    ses = HTTP(testnet=u["testnet"], api_key=u["api_key"], api_secret=u["api_secret"])
    for acct in ("UNIFIED", "SPOT", "CONTRACT"):
        try:
            r = ses.get_wallet_balance(accountType=acct)
            lst = r.get("result", {}).get("list", [])
            if not lst:
                continue
            coins = lst[0].get("coin", [])
            usdt = next((c for c in coins if c.get("coin") == "USDT"), None)
            if usdt is not None:
                return float(usdt.get("walletBalance", 0.0))
        except FailedRequestError:
            continue
        except Exception:
            continue
    return None


# ===================== Команди =====================
def cmd_start(update: Update, context: CallbackContext):
    text = (
        "Привіт! Я бот для піків Bybit.\n"
        f"Символ: {SYMBOL}, інтервал: {INTERVAL}m\n\n"
        "Команди:\n"
        "/now — ціна зараз + піки\n"
        "/peaks — топові мін/макс\n"
        "/balance — баланс USDT (доступ за списком + /link)\n"
        "/link <API_KEY> <API_SECRET> [testnet|live] — зберегти ключі (лише для дозволених)\n"
        "/unlink — прибрати свої ключі\n"
        "/me — показати свій статус\n\n"
        "Адмін: /allow <user_id>, /deny <user_id>\n"
    )
    update.message.reply_text(text)


def cmd_now(update: Update, context: CallbackContext):
    min_level, max_level, price = find_peak_levels(SYMBOL, INTERVAL, CANDLES_LIMIT, EPS_PCT)
    text = (
        f"⏱ {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"📊 {SYMBOL} {INTERVAL}m\n"
        f"• Ціна: {fmt(price)}\n"
        f"• Мін:  {fmt(min_level[0])} (x{min_level[1]})\n"
        f"• Макс: {fmt(max_level[0])} (x{max_level[1]})"
    )
    update.message.reply_text(text)


def cmd_peaks(update: Update, context: CallbackContext):
    min_level, max_level, _ = find_peak_levels(SYMBOL, INTERVAL, CANDLES_LIMIT, EPS_PCT)
    update.message.reply_text(
        f"📈 Піки (кластер {EPS_PCT * 100:.1f}%):\n"
        f"• Мін:  {fmt(min_level[0])} (x{min_level[1]})\n"
        f"• Макс: {fmt(max_level[0])} (x{max_level[1]})"
    )


def cmd_me(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    u = get_user(uid)
    role = get_role(uid) or "none"
    allowed = "так" if is_allowed(uid) else "ні"
    if not u:
        update.message.reply_text(
            f"👤 user_id: {uid}\n🔐 ключі: нема\n✅ доступ: {allowed}\n📜 роль: {role}\n"
            f"Щоб отримати доступ — надішли цей user_id адміну (див. /allow)."
        )
    else:
        update.message.reply_text(
            f"👤 user_id: {uid}\n🔐 ключі: збережені\n🌐 режим: {'TESTNET' if u['testnet'] else 'LIVE'}\n"
            f"✅ доступ: {allowed}\n📜 роль: {role}"
        )


def cmd_balance(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_allowed(uid):
        update.message.reply_text("⛔ Немає доступу. Попроси адміна додати тебе: /allow <твій id> (див. /me)")
        return
    usdt = get_usdt_balance_for(uid)
    if usdt is None:
        update.message.reply_text("⚠️ Нема ключів або доступу. Спершу: /link <API_KEY> <API_SECRET> [testnet|live]")
    else:
        update.message.reply_text(f"💰 Баланс USDT: {fmt(usdt)}")


def cmd_link(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_allowed(uid):
        update.message.reply_text("⛔ Немає доступу. Попроси адміна: /allow <твій id> (див. /me)")
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text("Формат: /link <API_KEY> <API_SECRET> [testnet|live]")
        return
    api_key, api_secret = args[0], args[1]
    mode = args[2].lower() if len(args) >= 3 else "testnet"
    testnet = (mode != "live")
    save_user(uid, api_key, api_secret, testnet)
    update.message.reply_text(
        f"✅ Ключі збережено для @{update.effective_user.username or uid}. "
        f"Режим: {'TESTNET' if testnet else 'LIVE'}"
    )


def cmd_unlink(update: Update, context: CallbackContext):
    # дозвіл не потрібен — кожен може прибрати СВОЇ ключі
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM users WHERE user_id=?", (update.effective_user.id,))
    con.commit()
    con.close()
    update.message.reply_text("🗑 Ключі видалено.")


def cmd_allow(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        update.message.reply_text("⛔ Тільки адмін може додавати користувачів.")
        return
    if not context.args:
        update.message.reply_text("Формат: /allow <telegram_user_id>")
        return
    try:
        uid = int(context.args[0])
        allow_user(uid, "user")
        update.message.reply_text(f"✅ Доступ надано для user_id={uid}")
    except Exception:
        update.message.reply_text("Невірний user_id.")


def cmd_deny(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        update.message.reply_text("⛔ Тільки адмін може забирати доступ.")
        return
    if not context.args:
        update.message.reply_text("Формат: /deny <telegram_user_id>")
        return
    try:
        uid = int(context.args[0])
        deny_user(uid)
        update.message.reply_text(f"🗑 Доступ прибрано для user_id={uid}")
    except Exception:
        update.message.reply_text("Невірний user_id.")


# ===================== Алерти (для адміна з .env) =====================
_last_alert_signature = None


def alert_job(context: CallbackContext):
    global _last_alert_signature
    try:
        min_level, max_level, price = find_peak_levels(SYMBOL, INTERVAL, CANDLES_LIMIT, EPS_PCT)
        signature = (
            round((min_level[0] or 0), 2), int(min_level[1] or 0),
            round((max_level[0] or 0), 2), int(max_level[1] or 0)
        )
        if signature == _last_alert_signature:
            return

        near_min = price and min_level[0] and abs(price - min_level[0]) / min_level[0] <= ALERT_PCT
        near_max = price and max_level[0] and abs(price - max_level[0]) / max_level[0] <= ALERT_PCT
        flags = []
        if near_min: flags.append("🔥 біля МІН")
        if near_max: flags.append("❄️ біля МАКС")
        flag_txt = (" | " + " & ".join(flags)) if flags else ""

        lines = [
            f"📊 {SYMBOL} {INTERVAL}m — піки (кластер {EPS_PCT * 100:.1f}%) {flag_txt}",
            f"• Мін: {fmt(min_level[0])} (x{min_level[1]})",
            f"• Макс: {fmt(max_level[0])} (x{max_level[1]})",
            "ℹ️ Приватні команди: /balance (потрібен доступ /allow і /link)",
        ]
        context.bot.send_message(chat_id=TG_CHAT_ID, text="\n".join(lines))
        _last_alert_signature = signature
    except Exception as e:
        if TG_CHAT_ID:
            context.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ Помилка алерту: {e}")


# ===================== Error handler =====================
def on_error(update, context: CallbackContext):
    try:
        raise context.error
    except TelegramError as e:
        if TG_CHAT_ID:
            context.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ Telegram error: {e}")
    except Exception as e:
        if TG_CHAT_ID:
            context.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ Error: {e}")


# ===================== Запуск =====================
def main():
    db_init()

    updater = Updater(TG_TOKEN)
    bot = updater.bot

    try:
        bot.delete_webhook()
    except Exception:
        pass

    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("now", cmd_now))
    dp.add_handler(CommandHandler("peaks", cmd_peaks))
    dp.add_handler(CommandHandler("balance", cmd_balance))
    dp.add_handler(CommandHandler("link", cmd_link))
    dp.add_handler(CommandHandler("unlink", cmd_unlink))
    dp.add_handler(CommandHandler("me", cmd_me))
    dp.add_handler(CommandHandler("allow", cmd_allow))
    dp.add_handler(CommandHandler("deny", cmd_deny))
    dp.add_error_handler(on_error)

    # лог стартових значень (див. journalctl)
    print(f"[startup] TESTNET={TESTNET} PING_SECONDS={PING_SECONDS} SYMBOL={SYMBOL} INTERVAL={INTERVAL}m")

    updater.job_queue.run_repeating(alert_job, interval=PING_SECONDS, first=5)

    updater.start_polling(drop_pending_updates=True)
    updater.idle()


if __name__ == "__main__":
    main()
