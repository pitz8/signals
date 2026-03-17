import asyncio
import threading
import ccxt
import pandas as pd
import ta
import yfinance as yf
import numpy as np
import time
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import os

USER_FILE = "registered_users.txt"


def load_users():
    """Reads chat IDs from the file into the global set."""
    if os.path.exists(USER_FILE):
        with open(USER_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    chat_ids.add(int(line))
        print(f"Loaded {len(chat_ids)} users from storage.")


def save_user(chat_id):
    """Appends a single new chat ID to the text file."""
    # Only write if it's not already in our file
    with open(USER_FILE, "a") as f:
        f.write(f"{chat_id}\n")

##############################
# TELEGRAM BOT SETUP
##############################


TELEGRAM_BOT_TOKEN = "8710025937:AAFLoQOsAOvHI4qgB9TBG53Al9sYfiDBilE"

chat_ids = set()
message_queue = asyncio.Queue()
bot_loop = None  # Global to store the bot's event loop


async def capture_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in chat_ids:
        chat_ids.add(chat_id)
        save_user(chat_id)  # Save to file immediately
        print(f"New user saved: {chat_id}")
        await update.message.reply_text("✅ You are now registered for Premium Signals.")
    else:
        await update.message.reply_text("You are already on the list!")


async def message_worker(app):
    """Background task to process the queue and send messages."""
    print("Message worker started...")
    while True:
        msg = await message_queue.get()
        for chat_id in list(chat_ids):
            try:
                # Use parse_mode Markdown to support the formatting in your message
                await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode='Markdown')
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"Failed to send to {chat_id}: {e}")
        message_queue.task_done()


def run_bot():
    """Sets up and runs the bot in its own event loop."""
    global bot_loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bot_loop = loop  # Store for thread-safe calls

    async def _start_bot():
        app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, capture_chat_id))

        # Start worker as a task
        loop.create_task(message_worker(app))

        print("Bot is starting polling...")
        await app.initialize()
        await app.updater.start_polling(drop_pending_updates=True)
        await app.start()

        # Keep running
        while True:
            await asyncio.sleep(1)

    loop.run_until_complete(_start_bot())


def send_message(msg: str):
    print("Queueing message for sending...")
    """Safely puts a message into the bot's queue from another thread."""
    if bot_loop is not None:
        bot_loop.call_soon_threadsafe(message_queue.put_nowait, msg)
    else:
        print("Bot loop not ready yet.")

##############################
# USER CONFIG
##############################


CHECK_INTERVAL_MINUTES = 15
SYMBOLS = {
    "BTC": "BTC/USDT",
    "ETH": "ETH/USDT"
}
GOLD_SYMBOL = "GC=F"
# Required timeframes for Premium Analysis
TIMEFRAMES = ["15m", "1h", "4h", "1d"]
CANDLE_LIMIT = 250

# Telegram - Replace with your actual credentials
# TELEGRAM_CHAT_ID = "2130207607"

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
        ohlcv = exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, limit=CANDLE_LIMIT)
        df = pd.DataFrame(
            ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
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

    # --- Momentum & Trend ---
    df["rsi"] = ta.momentum.RSIIndicator(close).rsi()
    macd = ta.trend.MACD(close)
    df["macd_diff"] = macd.macd_diff()

    # EMAs for Trend Analysis
    df["ema20"] = ta.trend.EMAIndicator(close, window=20).ema_indicator()
    df["ema50"] = ta.trend.EMAIndicator(close, window=50).ema_indicator()
    df["ema200"] = ta.trend.EMAIndicator(close, window=200).ema_indicator()

    # --- Volume Spike Detection ---
    # Calculates the average volume of the last 20 candles
    df["vol_ma20"] = volume.rolling(window=20).mean()
    # Ratio of current volume vs average (e.g., 2.0 = 200% of normal volume)
    df["vol_spike"] = volume / df["vol_ma20"]

    # --- Volatility & Strength ---
    df["atr"] = ta.volatility.AverageTrueRange(
        df["high"], df["low"], close).average_true_range()
    df["bb_pct"] = ta.volatility.BollingerBands(close).bollinger_pband()
    df["adx"] = ta.trend.ADXIndicator(df["high"], df["low"], close).adx()

    return df


def get_order_book_analysis(symbol):
    """
    Analyzes liquidity depth and bid/ask pressure.
    Acts as a proxy for a liquidity heatmap.
    """
    try:
        # Fetch top 20 levels
        ob = exchange.fetch_order_book(symbol, limit=20)
        bids_vol = sum([v[1] for v in ob['bids']])  # Total Buy Volume
        asks_vol = sum([v[1] for v in ob['asks']])  # Total Sell Volume

        if (bids_vol + asks_vol) == 0:
            return 0, 0, "Liquidity: Empty"

        imbalance = (bids_vol - asks_vol) / (bids_vol + asks_vol)

        # Scoring based on volume wall pressure
        if imbalance > 0.15:  # 15% more buy pressure
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

    # --- 1. EMA 50/200 Cross (Major Trend) ---
    if (last["ema50"] > last["ema200"]) and (prev["ema50"] <= prev["ema200"]):
        bull += 4
        details.append("🌟 GOLDEN CROSS")
    elif (last["ema50"] < last["ema200"]) and (prev["ema50"] >= prev["ema200"]):
        bear += 4
        details.append("💀 DEATH CROSS")

    # --- 2. Trend Exhaustion Logic (NEW) ---
    # ADX > 45 indicates an extremely overextended trend
    if last["adx"] > 45:
        if last["rsi"] > 75:  # Price is pumping but exhausted
            bull -= 3  # Deduct points from Bullish side
            details.append("⚡ EXHAUSTION (Top Risk)")
        elif last["rsi"] < 25:  # Price is dumping but exhausted
            bear -= 3  # Deduct points from Bearish side
            details.append("⚡ EXHAUSTION (Bottom Risk)")

    # --- 3. Whale Volume Spike ---
    if last["vol_spike"] > 3.0:
        details.append(f"🐋 WHALE SPIKE ({last['vol_spike']:.1f}x)")
        if last["close"] > prev["close"]:
            bull += 2
        else:
            bear += 2

    # --- 4. Standard Indicators (MACD/RSI/BB) ---
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
    """
    Combines MTF Tech Analysis + Order Book Liquidity + ATR Risk Management.
    """
    mtf_results = {}
    total_bull_points = 0
    total_bear_points = 0
    # Ensure SYMBOLS = {"BTC": "BTC/USDT", "ETH": "ETH/USDT"}
    symbol = SYMBOLS[name]

    # --- PART 1: MULTI-TIMEFRAME ANALYSIS ---
    for tf in TIMEFRAMES:
        df = fetch_crypto(symbol, tf)
        df = compute_indicators(df)
        bull, bear, details = get_detailed_score(df)

        # Weighting: 1d (2.0), 4h (1.5), 1h (1.0), 15m (0.5)
        weight = {"1d": 2.0, "4h": 1.5, "1h": 1.0, "15m": 0.5}.get(tf, 1.0)

        total_bull_points += (bull * weight)
        total_bear_points += (bear * weight)

        mtf_results[tf] = {
            "score": f"{bull}B / {bear}S",
            "signals": ", ".join(details) if details else "Neutral"
        }

    # --- PART 2: ORDER BOOK ANALYSIS ---
    ob_bull, ob_bear, ob_msg = get_order_book_analysis(symbol)
    total_bull_points += (ob_bull * 1.5)
    total_bear_points += (ob_bear * 1.5)

    # --- PART 3: SIGNAL CALCULATION ---
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

    # --- PART 4: RISK MANAGEMENT (1H ATR) ---
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
        "net_score": round(net_score, 1),
        "liquidity": ob_msg,
        "mtf": mtf_results,
        "tp": f"{tp:,.2f}",
        "sl": f"{sl:,.2f}",
        "timestamp": datetime.now().strftime("%H:%M")
    }


def run_cycle():
    # Only BTC and ETH as requested
    assets = ["BTC", "ETH"]

    for asset_name in assets:
        try:
            # 1. Get the complete analysis
            result = analyze_asset(asset_name)

            # 2. Calculate Risk/Reward Ratio with safety
            try:
                price_f = float(result['price'].replace(',', ''))
                tp_f = float(result['tp'].replace(',', ''))
                sl_f = float(result['sl'].replace(',', ''))
                risk = abs(price_f - sl_f)
                reward = abs(tp_f - price_f)
                rr_ratio = reward / risk if risk != 0 else 0
            except Exception:
                rr_ratio = 0

            # 3. Compile Critical Alerts
            warnings = []
            for tf, data in result['mtf'].items():
                # We check the 'signals' string for our custom trigger keywords
                if "EXHAUSTION" in data['signals'].upper():
                    warnings.append(f"⚠️ {tf.upper()} Exhaustion")
                if "CROSS" in data['signals'].upper():
                    warnings.append(f"⚡ {tf.upper()} {data['signals']}")
                if "WHALE" in data['signals'].upper():
                    warnings.append(f"🐋 {tf.upper()} Whale Spike")

            # 4. Construct the Premium Telegram Message
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

            message += (
                f"━━━━━━━━━━━━━━━━━\n"
                f"📈 **Target (TP):** `{result['tp']}`\n"
                f"🛡️ **Defense (SL):** `{result['sl']}`\n"
                f"⚖️ **R/R Ratio:** `{rr_ratio:.2f}`\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"📑 **MTF DATA DEEP-DIVE:**\n"
            )

            # 5. Add Timeframe Triggers
            for tf, data in result['mtf'].items():
                message += f"• *{tf.upper()}*: {data['score']}\n"
                message += f"  └ _Triggers:_ {data['signals']}\n"

            message += (
                f"━━━━━━━━━━━━━━━━━\n"
                f"⏰ *Update:* {result['timestamp']} UTC"
            )

            # 6. Filter & Send
            # We send if it's a Buy/Sell OR if a major event (Whale/Cross) occurred
            is_actionable = "BUY" in result['signal'] or "SELL" in result['signal']
            has_major_alert = len(warnings) > 0

            if is_actionable or has_major_alert:
                send_message(message)
                logging.info(
                    f"Report sent for {asset_name} (Score: {result['net_score']})")

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

    load_users()

    # 2. Start the bot thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)

    # 3. Start the scanner thread
    scanner_thread = threading.Thread(target=run_scanner, daemon=True)

    bot_thread.start()
    scanner_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")


if __name__ == "__main__":
    main()
