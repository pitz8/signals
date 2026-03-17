import asyncio
import threading
import ccxt
import pandas as pd
import ta
import numpy as np
import time
import logging
import sqlite3
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (ApplicationBuilder, MessageHandler, CommandHandler,
                           CallbackQueryHandler, filters, ContextTypes)
import os

USER_FILE = "registered_users.txt"
DB_FILE = "signals.db"

##############################
# DATABASE
##############################

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT,
            signal_type TEXT,
            entry_price REAL,
            tp REAL,
            sl REAL,
            net_score REAL,
            opened_at TEXT,
            closed_at TEXT,
            outcome TEXT,
            duration_hours REAL
        )
    """)
    conn.commit()
    conn.close()


def save_signal(asset, signal_type, entry_price, tp, sl, net_score):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute(
        "INSERT INTO signals (asset, signal_type, entry_price, tp, sl, net_score, opened_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (asset, signal_type, entry_price, tp, sl, net_score, datetime.now().isoformat())
    )
    sig_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sig_id


def close_signal(sig_id, outcome):
    conn = sqlite3.connect(DB_FILE)
    now = datetime.now()
    row = conn.execute("SELECT opened_at FROM signals WHERE id=?", (sig_id,)).fetchone()
    duration = None
    if row:
        opened = datetime.fromisoformat(row[0])
        duration = (now - opened).total_seconds() / 3600
    conn.execute(
        "UPDATE signals SET closed_at=?, outcome=?, duration_hours=? WHERE id=?",
        (now.isoformat(), outcome, round(duration, 2) if duration else None, sig_id)
    )
    conn.commit()
    conn.close()
    return duration


def get_stats():
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute(
        "SELECT asset, signal_type, outcome, duration_hours FROM signals WHERE outcome IS NOT NULL"
    ).fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        return None

    tp_hits = sum(1 for r in rows if r[2] == 'TP_HIT')
    sl_hits = sum(1 for r in rows if r[2] == 'SL_HIT')
    expired = sum(1 for r in rows if r[2] == 'EXPIRED')
    durations = [r[3] for r in rows if r[3] is not None]
    avg_duration = sum(durations) / len(durations) if durations else 0

    assets = {}
    for r in rows:
        a = r[0]
        if a not in assets:
            assets[a] = {'tp': 0, 'sl': 0, 'expired': 0, 'total': 0}
        assets[a]['total'] += 1
        if r[2] == 'TP_HIT':
            assets[a]['tp'] += 1
        elif r[2] == 'SL_HIT':
            assets[a]['sl'] += 1
        elif r[2] == 'EXPIRED':
            assets[a]['expired'] += 1

    closed = tp_hits + sl_hits
    win_rate = (tp_hits / closed * 100) if closed > 0 else 0

    return {
        'total': total,
        'tp_hits': tp_hits,
        'sl_hits': sl_hits,
        'expired': expired,
        'win_rate': win_rate,
        'avg_duration': avg_duration,
        'assets': assets
    }


def get_open_signals():
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, opened_at "
        "FROM signals WHERE outcome IS NULL ORDER BY opened_at DESC"
    ).fetchall()
    conn.close()
    return rows


def format_stats(stats):
    closed = stats['tp_hits'] + stats['sl_hits']
    text = (
        f"📊 *SIGNAL ACCURACY REPORT*\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"Total Signals: `{stats['total']}`\n"
        f"Closed (TP+SL): `{closed}`\n"
        f"✅ TP Hit: `{stats['tp_hits']}` — Win Rate: `{stats['win_rate']:.1f}%`\n"
        f"❌ SL Hit: `{stats['sl_hits']}`\n"
        f"⏳ Expired (7d): `{stats['expired']}`\n"
        f"⏱ Avg Duration: `{stats['avg_duration']:.1f}h`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"*Per Asset:*\n"
    )
    for asset, data in stats['assets'].items():
        closed_a = data['tp'] + data['sl']
        wr = (data['tp'] / closed_a * 100) if closed_a > 0 else 0
        text += f"• *{asset}*: `{wr:.0f}%` WR — {data['tp']}✅ {data['sl']}❌ ({data['total']} tot)\n"
    return text


def format_open_trades(open_sigs):
    if not open_sigs:
        return "📋 No open trades being monitored."
    text = f"📋 *OPEN TRADES ({len(open_sigs)})*\n━━━━━━━━━━━━━━━━━\n"
    for sig in open_sigs:
        sig_id, asset, signal_type, entry_price, tp, sl, opened_at = sig
        try:
            opened = datetime.fromisoformat(opened_at)
            age = (datetime.now() - opened).total_seconds() / 3600
            age_str = f"{age:.1f}h"
        except Exception:
            age_str = "?"
        text += (
            f"*{asset}* — `{signal_type}`\n"
            f"Entry: `${entry_price:,.2f}` | Age: `{age_str}`\n"
            f"TP: `${tp:,.2f}` | SL: `${sl:,.2f}`\n\n"
        )
    return text.strip()


##############################
# USER MANAGEMENT
##############################

def load_users():
    if os.path.exists(USER_FILE):
        with open(USER_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    chat_ids.add(int(line))
        print(f"Loaded {len(chat_ids)} users from storage.")


def save_user(chat_id):
    with open(USER_FILE, "a") as f:
        f.write(f"{chat_id}\n")


##############################
# TELEGRAM BOT SETUP
##############################

TELEGRAM_BOT_TOKEN = "8710025937:AAFLoQOsAOvHI4qgB9TBG53Al9sYfiDBilE"

chat_ids = set()
message_queue = asyncio.Queue()
bot_loop = None


async def capture_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in chat_ids:
        chat_ids.add(chat_id)
        save_user(chat_id)
        print(f"New user saved: {chat_id}")
        await update.message.reply_text(
            "✅ You are now registered for Premium Signals.\n\n"
            "Commands:\n"
            "/stats — Signal accuracy report\n"
            "/trades — Open monitored trades"
        )
    else:
        await update.message.reply_text(
            "You are already on the list!\n\n"
            "/stats — Signal accuracy report\n"
            "/trades — Open monitored trades"
        )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in chat_ids:
        chat_ids.add(chat_id)
        save_user(chat_id)
    await update.message.reply_text(
        "📡 *Premium Signals Bot*\n"
        "━━━━━━━━━━━━━━━━━\n"
        "You'll receive BTC & ETH trading signals with TP/SL.\n"
        "Each signal is tracked and verified automatically.\n\n"
        "Commands:\n"
        "/stats — Signal accuracy report\n"
        "/trades — Open monitored trades",
        parse_mode='Markdown'
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = get_stats()
    if not stats:
        await update.message.reply_text(
            "📊 No completed signals yet.\nStats appear after the first TP or SL is hit."
        )
        return
    await update.message.reply_text(format_stats(stats), parse_mode='Markdown')


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    open_sigs = get_open_signals()
    await update.message.reply_text(format_open_trades(open_sigs), parse_mode='Markdown')


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        stats = get_stats()
        if not stats:
            text = "📊 No completed signals yet. Stats will appear after the first TP/SL hit."
        else:
            text = format_stats(stats)
        await query.message.reply_text(text, parse_mode='Markdown')

    elif query.data == "open_trades":
        open_sigs = get_open_signals()
        await query.message.reply_text(format_open_trades(open_sigs), parse_mode='Markdown')


async def message_worker(app):
    print("Message worker started...")
    while True:
        item = await message_queue.get()

        if isinstance(item, dict):
            text = item['text']
            keyboard = item.get('keyboard')
        else:
            text = item
            keyboard = None

        for chat_id in list(chat_ids):
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='Markdown',
                    reply_markup=keyboard
                )
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"Failed to send to {chat_id}: {e}")
        message_queue.task_done()


def run_bot():
    global bot_loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bot_loop = loop

    async def _start_bot():
        app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("stats", cmd_stats))
        app.add_handler(CommandHandler("trades", cmd_trades))
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, capture_chat_id))

        loop.create_task(message_worker(app))

        print("Bot is starting polling...")
        await app.initialize()
        await app.updater.start_polling(drop_pending_updates=True)
        await app.start()

        while True:
            await asyncio.sleep(1)

    loop.run_until_complete(_start_bot())


def send_message(item):
    """Safely puts a message (str or dict with text+keyboard) into the bot's queue."""
    print("Queueing message for sending...")
    if bot_loop is not None:
        bot_loop.call_soon_threadsafe(message_queue.put_nowait, item)
    else:
        print("Bot loop not ready yet.")


##############################
# USER CONFIG
##############################

CHECK_INTERVAL_MINUTES = 15
SIGNAL_EXPIRY_HOURS = 168  # 7 days
MONITOR_INTERVAL_SECONDS = 300  # 5 minutes

SYMBOLS = {
    "BTC": "BTC/USDT",
    "ETH": "ETH/USDT"
}
TIMEFRAMES = ["15m", "1h", "4h", "1d"]
CANDLE_LIMIT = 250

logging.basicConfig(
    filename="trading_audit.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

exchange = ccxt.binance({'enableRateLimit': True})

##############################
# DATA ACQUISITION
##############################

def fetch_crypto(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=CANDLE_LIMIT)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except Exception as e:
        logging.error(f"Error fetching {symbol} @ {timeframe}: {e}")
        return pd.DataFrame()


##############################
# SIGNAL GENERATION
##############################

def compute_indicators(df):
    if df.empty or len(df) < 200:
        return df

    close = df["close"]
    volume = df["volume"]

    df["rsi"] = ta.momentum.RSIIndicator(close).rsi()
    macd = ta.trend.MACD(close)
    df["macd_diff"] = macd.macd_diff()

    df["ema20"] = ta.trend.EMAIndicator(close, window=20).ema_indicator()
    df["ema50"] = ta.trend.EMAIndicator(close, window=50).ema_indicator()
    df["ema200"] = ta.trend.EMAIndicator(close, window=200).ema_indicator()

    df["vol_ma20"] = volume.rolling(window=20).mean()
    df["vol_spike"] = volume / df["vol_ma20"]

    df["atr"] = ta.volatility.AverageTrueRange(df["high"], df["low"], close).average_true_range()
    df["bb_pct"] = ta.volatility.BollingerBands(close).bollinger_pband()
    df["adx"] = ta.trend.ADXIndicator(df["high"], df["low"], close).adx()

    return df


def get_order_book_analysis(symbol):
    try:
        ob = exchange.fetch_order_book(symbol, limit=20)
        bids_vol = sum([v[1] for v in ob['bids']])
        asks_vol = sum([v[1] for v in ob['asks']])

        if (bids_vol + asks_vol) == 0:
            return 0, 0, "Liquidity: Empty"

        imbalance = (bids_vol - asks_vol) / (bids_vol + asks_vol)

        if imbalance > 0.15:
            return 2, 0, f"Strong Buy Walls ({imbalance:.1%})"
        elif imbalance < -0.15:
            return 0, 2, f"Strong Sell Walls ({abs(imbalance):.1%})"

        return 0, 0, "Liquidity: Balanced"
    except Exception as e:
        logging.error(f"Order book error for {symbol}: {e}")
        return 0, 0, "Liquidity: Data Error"


def get_detailed_score(df):
    if df.empty or len(df) < 2:
        return 0, 0, []

    last = df.iloc[-1]
    prev = df.iloc[-2]
    bull, bear, details = 0, 0, []

    if (last["ema50"] > last["ema200"]) and (prev["ema50"] <= prev["ema200"]):
        bull += 4
        details.append("🌟 GOLDEN CROSS")
    elif (last["ema50"] < last["ema200"]) and (prev["ema50"] >= prev["ema200"]):
        bear += 4
        details.append("💀 DEATH CROSS")

    if last["adx"] > 45:
        if last["rsi"] > 75:
            bull -= 3
            details.append("⚡ EXHAUSTION (Top Risk)")
        elif last["rsi"] < 25:
            bear -= 3
            details.append("⚡ EXHAUSTION (Bottom Risk)")

    if last["vol_spike"] > 3.0:
        details.append(f"🐋 WHALE SPIKE ({last['vol_spike']:.1f}x)")
        if last["close"] > prev["close"]:
            bull += 2
        else:
            bear += 2

    if last["macd_diff"] > 0 and last["macd_diff"] > prev["macd_diff"]:
        bull += 1
    elif last["macd_diff"] < 0 and last["macd_diff"] < prev["macd_diff"]:
        bear += 1

    if last["rsi"] < 30:
        bull += 2
    elif last["rsi"] > 70:
        bear += 2

    if last["bb_pct"] < 0.05:
        bull += 1
    elif last["bb_pct"] > 0.95:
        bear += 1

    return bull, bear, details


def analyze_asset(name):
    mtf_results = {}
    total_bull_points = 0
    total_bear_points = 0
    symbol = SYMBOLS[name]

    for tf in TIMEFRAMES:
        df = fetch_crypto(symbol, tf)
        df = compute_indicators(df)
        bull, bear, details = get_detailed_score(df)

        weight = {"1d": 2.0, "4h": 1.5, "1h": 1.0, "15m": 0.5}.get(tf, 1.0)
        total_bull_points += (bull * weight)
        total_bear_points += (bear * weight)

        mtf_results[tf] = {
            "score": f"{bull}B / {bear}S",
            "signals": ", ".join(details) if details else "Neutral"
        }

    ob_bull, ob_bear, ob_msg = get_order_book_analysis(symbol)
    total_bull_points += (ob_bull * 1.5)
    total_bear_points += (ob_bear * 1.5)

    net_score = total_bull_points - total_bear_points
    if net_score >= 8:
        signal = "🔥 STRONG BUY"
    elif net_score >= 3:
        signal = "✅ BUY"
    elif net_score <= -8:
        signal = "🚨 STRONG SELL"
    elif net_score <= -3:
        signal = "🔻 SELL"
    else:
        signal = "⚖️ NEUTRAL/HOLD"

    latest_df = fetch_crypto(symbol, "1h")
    latest_df = compute_indicators(latest_df)
    current_price = latest_df["close"].iloc[-1]
    atr = latest_df["atr"].iloc[-1]

    if "BUY" in signal:
        tp = current_price + (atr * 2.5)
        sl = current_price - (atr * 1.5)
    else:
        tp = current_price - (atr * 2.5)
        sl = current_price + (atr * 1.5)

    return {
        "asset": name,
        "signal": signal,
        "price": f"{current_price:,.2f}",
        "price_raw": current_price,
        "tp_raw": tp,
        "sl_raw": sl,
        "net_score": round(net_score, 1),
        "liquidity": ob_msg,
        "mtf": mtf_results,
        "tp": f"{tp:,.2f}",
        "sl": f"{sl:,.2f}",
        "timestamp": datetime.now().strftime("%H:%M")
    }


##############################
# SIGNAL MONITOR
##############################

def monitor_open_signals():
    """Checks open signals every 5 min and closes them when TP/SL is hit."""
    print("Signal monitor started...")
    while True:
        time.sleep(MONITOR_INTERVAL_SECONDS)

        open_sigs = get_open_signals()
        if not open_sigs:
            continue

        for sig in open_sigs:
            sig_id, asset, signal_type, entry_price, tp, sl, opened_at = sig
            symbol = SYMBOLS.get(asset)
            if not symbol:
                continue

            try:
                # Check expiry first
                opened = datetime.fromisoformat(opened_at)
                age_hours = (datetime.now() - opened).total_seconds() / 3600
                if age_hours >= SIGNAL_EXPIRY_HOURS:
                    duration = close_signal(sig_id, 'EXPIRED')
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    msg = (
                        f"⏳ *SIGNAL EXPIRED — {asset}*\n"
                        f"━━━━━━━━━━━━━━━━━\n"
                        f"Signal: `{signal_type}`\n"
                        f"Entry: `${entry_price:,.2f}` | Current: `${current_price:,.2f}`\n"
                        f"TP: `${tp:,.2f}` | SL: `${sl:,.2f}`\n"
                        f"Duration: `{age_hours:.1f}h` — Neither hit in 7 days\n"
                        f"━━━━━━━━━━━━━━━━━"
                    )
                    send_message(msg)
                    logging.info(f"Signal {sig_id} expired after {age_hours:.1f}h")
                    continue

                ticker = exchange.fetch_ticker(symbol)
                current_price = ticker['last']

                hit = None
                if 'BUY' in signal_type:
                    if current_price >= tp:
                        hit = 'TP_HIT'
                    elif current_price <= sl:
                        hit = 'SL_HIT'
                else:  # SELL
                    if current_price <= tp:
                        hit = 'TP_HIT'
                    elif current_price >= sl:
                        hit = 'SL_HIT'

                if hit:
                    duration = close_signal(sig_id, hit)
                    pnl_pct = abs(current_price - entry_price) / entry_price * 100
                    emoji = '✅' if hit == 'TP_HIT' else '❌'
                    result_label = 'TARGET HIT' if hit == 'TP_HIT' else 'STOP LOSS HIT'

                    # Format duration nicely
                    if duration and duration < 1:
                        dur_str = f"{duration * 60:.0f}m"
                    elif duration and duration < 24:
                        dur_str = f"{duration:.1f}h"
                    else:
                        dur_str = f"{duration / 24:.1f}d" if duration else "?"

                    msg = (
                        f"{emoji} *{result_label} — {asset}*\n"
                        f"━━━━━━━━━━━━━━━━━\n"
                        f"Signal: `{signal_type}`\n"
                        f"Entry: `${entry_price:,.2f}`\n"
                        f"Exit:  `${current_price:,.2f}`\n"
                        f"Move:  `{pnl_pct:.2f}%`\n"
                        f"⏱ Duration: `{dur_str}`\n"
                        f"━━━━━━━━━━━━━━━━━"
                    )
                    send_message(msg)
                    logging.info(f"Signal {sig_id} closed: {hit} after {dur_str}")

            except Exception as e:
                logging.error(f"Monitor error for signal {sig_id} ({asset}): {e}")


##############################
# SCANNER CYCLE
##############################

def run_cycle():
    assets = ["BTC", "ETH"]

    for asset_name in assets:
        try:
            result = analyze_asset(asset_name)

            try:
                price_f = result['price_raw']
                tp_f = result['tp_raw']
                sl_f = result['sl_raw']
                risk = abs(price_f - sl_f)
                reward = abs(tp_f - price_f)
                rr_ratio = reward / risk if risk != 0 else 0
            except Exception:
                rr_ratio = 0

            warnings = []
            for tf, data in result['mtf'].items():
                if "EXHAUSTION" in data['signals'].upper():
                    warnings.append(f"⚠️ {tf.upper()} Exhaustion")
                if "CROSS" in data['signals'].upper():
                    warnings.append(f"⚡ {tf.upper()} {data['signals']}")
                if "WHALE" in data['signals'].upper():
                    warnings.append(f"🐋 {tf.upper()} Whale Spike")

            is_actionable = "BUY" in result['signal'] or "SELL" in result['signal']
            has_major_alert = len(warnings) > 0

            if not (is_actionable or has_major_alert):
                continue

            # Save to DB if actionable (has TP/SL to track)
            sig_id = None
            if is_actionable:
                sig_id = save_signal(
                    asset=asset_name,
                    signal_type=result['signal'],
                    entry_price=result['price_raw'],
                    tp=result['tp_raw'],
                    sl=result['sl_raw'],
                    net_score=result['net_score']
                )

            message = (
                f"📡 *{result['asset']} MARKET INTELLIGENCE*\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"💰 **Price:** `${result['price']}`\n"
                f"🎯 **Signal:** `{result['signal']}`\n"
                f"📊 **Confidence:** `{result['net_score']}/15.0`\n"
                f"🌊 **Liquidity:** {result['liquidity']}\n"
            )

            if warnings:
                message += f"❗ **Alerts:** {', '.join(warnings)}\n"

            if is_actionable:
                message += (
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"📈 **Target (TP):** `${result['tp']}`\n"
                    f"🛡️ **Defense (SL):** `${result['sl']}`\n"
                    f"⚖️ **R/R Ratio:** `{rr_ratio:.2f}`\n"
                    f"🔍 **Tracking:** Signal #{sig_id} — auto-monitoring TP/SL\n"
                )

            message += (
                f"━━━━━━━━━━━━━━━━━\n"
                f"📑 **MTF DATA DEEP-DIVE:**\n"
            )

            for tf, data in result['mtf'].items():
                message += f"• *{tf.upper()}*: {data['score']}\n"
                message += f"  └ _Triggers:_ {data['signals']}\n"

            message += (
                f"━━━━━━━━━━━━━━━━━\n"
                f"⏰ *Update:* {result['timestamp']} UTC"
            )

            # Inline buttons
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📊 Stats", callback_data="stats"),
                    InlineKeyboardButton("📋 Open Trades", callback_data="open_trades"),
                ]
            ])

            send_message({"text": message, "keyboard": keyboard})
            logging.info(f"Report sent for {asset_name} (Score: {result['net_score']}, Signal #{sig_id})")

        except Exception as e:
            logging.error(f"Critical error in cycle for {asset_name}: {e}")


def run_scanner():
    print("Premium Scanner Started...")
    while True:
        try:
            run_cycle()
        except Exception as e:
            logging.error(f"Main Loop Error: {e}")
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


##############################
# MAIN EXECUTION
##############################

def main():
    logging.basicConfig(level=logging.INFO)

    init_db()
    load_users()

    bot_thread = threading.Thread(target=run_bot, daemon=True)
    scanner_thread = threading.Thread(target=run_scanner, daemon=True)
    monitor_thread = threading.Thread(target=monitor_open_signals, daemon=True)

    bot_thread.start()
    scanner_thread.start()
    monitor_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")


if __name__ == "__main__":
    main()
