import os
from datetime import datetime
from statistics import median

from dotenv import load_dotenv
from pybit.exceptions import FailedRequestError
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ===================== Налаштування =====================
load_dotenv()

# Bybit
API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()
TESTNET = os.getenv("BYBIT_TESTNET", "true").strip().lower() == "true"

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TG_CHAT_ID_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_CHAT_ID = int(TG_CHAT_ID_STR) if TG_CHAT_ID_STR.isdigit() else None

# Аналітика / алерти
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").strip()
INTERVAL = os.getenv("INTERVAL", "1").strip()  # "1","3","5","15","60","240","D"
CANDLES_LIMIT = int(os.getenv("LIMIT", "200"))
EPS_PCT = float(os.getenv("EPS_PCT", "0.008"))  # 0.8% кластеризація піків
ALERT_PCT = float(os.getenv("ALERT_PCT", "0.002"))  # 0.2% близькість до рівня
PING_SECONDS = int(os.getenv("PING_SECONDS", "60"))

if not TG_TOKEN or TG_CHAT_ID is None:
    raise RuntimeError("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID не задані або некоректні в .env")

# Bybit session: важливо передати testnet=TESTNET
session = HTTP(testnet=TESTNET, api_key=API_KEY, api_secret=API_SECRET)


# ===================== Хелпери =====================
def fmt(n):
    try:
        return f"{float(n):,.4f}".replace(",", " ")
    except Exception:
        return str(n)


def fetch_klines(symbol: str, interval: str, limit: int):
    """
    Bybit V5 Spot kline: повертає список словників (свічок), відсортований за часом.
    Елемент list: [start, open, high, low, close, volume, turnover]
    """
    r = session.get_kline(category="spot", symbol=symbol, interval=interval, limit=limit)
    raw = r["result"]["list"]
    kl = []
    for it in raw:
        ts, o, h, l, c, v, t = it
        kl.append({"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)})
    kl.sort(key=lambda x: x["ts"])
    return kl


def local_extrema(kl):
    """Прості локальні міні/максі за 3-свічковим правилом."""
    lows, highs = [], []
    for i in range(1, len(kl) - 1):
        if kl[i]["low"] <= kl[i - 1]["low"] and kl[i]["low"] <= kl[i + 1]["low"]:
            lows.append(kl[i]["low"])
        if kl[i]["high"] >= kl[i - 1]["high"] and kl[i]["high"] >= kl[i + 1]["high"]:
            highs.append(kl[i]["high"])
    return lows, highs


def cluster_levels(values, eps_pct: float):
    """Групує близькі значення (±eps_pct) у кластери. Повертає [(медіана, кількість), ...] за спаданням частоти."""
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
    """Повертає (top_min, top_max, last_close). top_* = (price, count)."""
    kl = fetch_klines(symbol, interval, limit)
    lows, highs = local_extrema(kl)
    low_clusters = cluster_levels(lows, eps_pct)
    high_clusters = cluster_levels(highs, eps_pct)
    best_min = low_clusters[0] if low_clusters else (None, 0)
    best_max = high_clusters[0] if high_clusters else (None, 0)
    last_close = kl[-1]["close"] if kl else None
    return best_min, best_max, last_close


def get_usdt_balance():
    """
    Повертає float (баланс) або None, якщо авторизація не пройшла (401 / IP whitelist / права).
    """
    try:
        r = session.get_wallet_balance(accountType="UNIFIED")
        coins = r["result"]["list"][0]["coin"]
        usdt = next((c for c in coins if c["coin"] == "USDT"), None)
        return float(usdt["walletBalance"]) if usdt else 0.0
    except FailedRequestError:
        return None
    except Exception:
        return None


# ===================== Telegram-команди =====================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "✅ Бот запущений.\n"
        "Команди:\n"
        "/now — ціна + піки зараз\n"
        "/peaks — топові мін/макс\n"
        "/balance — баланс USDT\n"
    )
    await update.message.reply_text(text)


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    min_level, max_level, price = find_peak_levels(SYMBOL, INTERVAL, CANDLES_LIMIT, EPS_PCT)
    text = (
        f"⏱ {datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"📊 {SYMBOL} {INTERVAL}m\n"
        f"• Ціна: {fmt(price)}\n"
        f"• Мін:  {fmt(min_level[0])} (x{min_level[1]})\n"
        f"• Макс: {fmt(max_level[0])} (x{max_level[1]})"
    )
    await update.message.reply_text(text)


async def cmd_peaks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    min_level, max_level, _ = find_peak_levels(SYMBOL, INTERVAL, CANDLES_LIMIT, EPS_PCT)
    await update.message.reply_text(
        f"📈 Піки (кластер {EPS_PCT * 100:.1f}%):\n"
        f"• Мін:  {fmt(min_level[0])} (x{min_level[1]})\n"
        f"• Макс: {fmt(max_level[0])} (x{max_level[1]})"
    )


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    usdt = get_usdt_balance()
    if usdt is None:
        await update.message.reply_text("⚠️ Баланс недоступний: перевір ключі/права/IP і BYBIT_TESTNET у .env")
    else:
        await update.message.reply_text(f"💰 Баланс USDT: {fmt(usdt)}")


# ===================== Anti-duplicate для алертів =====================
_last_alert_signature = None  # (min_price, min_cnt, max_price, max_cnt, usdt)


# ===================== Фоновий алерт =====================
async def alert_job(context: ContextTypes.DEFAULT_TYPE):
    global _last_alert_signature
    try:
        min_level, max_level, price = find_peak_levels(SYMBOL, INTERVAL, CANDLES_LIMIT, EPS_PCT)
        usdt = get_usdt_balance()

        signature = (
            round(min_level[0] or 0, 2), int(min_level[1] or 0),
            round(max_level[0] or 0, 2), int(max_level[1] or 0),
            round(usdt or 0, 2)
        )
        if signature == _last_alert_signature:
            return  # не дублюємо те саме

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
        ]
        if usdt is not None:
            lines.append(f"💰 Баланс USDT: {fmt(usdt)}")
        else:
            lines.append("⚠️ Баланс недоступний: перевір ключі Testnet/Live, права Read, IP whitelist")

        await context.bot.send_message(chat_id=TG_CHAT_ID, text="\n".join(lines))
        _last_alert_signature = signature

    except Exception as e:
        if TG_CHAT_ID:
            await context.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ Помилка алерту: {e}")


# ===================== Обробка помилок =====================
async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    try:
        raise context.error
    except TelegramError as e:
        if TG_CHAT_ID:
            await context.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ Telegram error: {e}")
    except Exception as e:
        if TG_CHAT_ID:
            await context.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ Error: {e}")


# ===================== Ініціалізація =====================
async def on_startup(app):
    # прибираємо webhook, щоб не було 409 Conflict
    await app.bot.delete_webhook(drop_pending_updates=True)
    # одна (!) job для алертів
    app.job_queue.run_repeating(alert_job, interval=PING_SECONDS, first=5)


def main():
    app = (
        ApplicationBuilder()
        .token(TG_TOKEN)
        .post_init(on_startup)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("peaks", cmd_peaks))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_error_handler(on_error)

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
