"""
ETH/USDT 15m Trading Bot
Setup: as per setup_ETH_USDT_15m_2026-01-01_2026-03-20.txt
- Sends Telegram messages instead of real orders
- Checks TP/SL on every poll and moves trades to closed
- Exposes a local HTTP API on port 5050 for the dashboard (dashboard.html)
"""

import time
import threading
import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone
from flask import Flask, jsonify
from flask_cors import CORS
import ccxt
import uuid

# ─────────────────────────────────────────
#  CONFIG — fill these in
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN = None
TELEGRAM_CHAT_ID = None

EXCHANGE_ID = "binance"
SYMBOL = "ETH/USDT"

POLL_INTERVAL_SECONDS = 60
DASHBOARD_PORT = 5050

# ─────────────────────────────────────────
#  STRATEGY CONSTANTS (from setup file)
# ─────────────────────────────────────────
PRIMARY_TF = "15m"
CONFIRM_TF = "4h"

TRADE_SIZE = 1.0
MAX_TRADES = 3

# Long
LONG_MIN_SCORE = 7.5
LONG_RSI_PRI = 35
LONG_RSI_HTF = 40
LONG_STOCH_RSI = 20
LONG_ADX_MIN = 20
LONG_TP_PCT = 1.0
LONG_SL_PCT = 3.0
REQUIRE_GOLDEN = True

# Short
SHORT_MIN_SCORE = 7.5
SHORT_RSI_PRI = 65
SHORT_RSI_HTF = 60
SHORT_STOCH_RSI = 80
SHORT_ADX_MIN = 20
SHORT_TP_PCT = 1.0
SHORT_SL_PCT = 3.0
REQUIRE_DEATH = True

# Shared indicators
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
BB_PERIOD, BB_STD = 20, 2.0
EMA_FAST, EMA_SLOW = 9, 21
CROSS_EMA_FAST = 50
CROSS_EMA_SLOW = 200
ATR_PERIOD = 14
VOLUME_MULT = 1.5
REQUIRE_VOL_SPIKE = False
CROSS_LOOKBACK = 50

# ─────────────────────────────────────────
#  STATE (protected by lock)
# ─────────────────────────────────────────
state_lock = threading.Lock()
open_trades = []
closed_trades = []
console_log = []
last_check = None
current_price = None


def log(text: str, kind: str = "info"):
    entry = {"time": datetime.now(timezone.utc).strftime(
        "%H:%M:%S"), "text": text, "kind": kind}
    with state_lock:
        console_log.append(entry)
        if len(console_log) > 200:
            console_log.pop(0)
    print(f"[{entry['time']}] {text}")


# ─────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────
def send_telegram(msg: str):
    if (not TELEGRAM_BOT_TOKEN) or (not TELEGRAM_CHAT_ID):
        print(msg)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID,
               "text": msg, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log(f"Telegram error: {e}", "error")


# ─────────────────────────────────────────
#  DATA FETCH
# ─────────────────────────────────────────
def fetch_ohlcv(exchange, timeframe: str, limit: int = 300) -> pd.DataFrame:
    raw = exchange.fetch_ohlcv(SYMBOL, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(
        raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    return df


# ─────────────────────────────────────────
#  INDICATORS
# ─────────────────────────────────────────
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["rsi"] = ta.rsi(df["close"], length=14)

    stoch = ta.stochrsi(df["close"], length=14, rsi_length=14, k=3, d=3)
    df["stoch_rsi_k"] = stoch[[
        c for c in stoch.columns if c.startswith("STOCHRSIk_")][0]]

    adx = ta.adx(df["high"], df["low"], df["close"], length=14)
    df["adx"] = adx["ADX_14"]

    macd = ta.macd(df["close"], fast=MACD_FAST,
                   slow=MACD_SLOW, signal=MACD_SIGNAL)
    df["macd"] = macd[[c for c in macd.columns if c.startswith("MACD_")][0]]
    df["macd_signal"] = macd[[
        c for c in macd.columns if c.startswith("MACDs_")][0]]
    df["macd_hist"] = macd[[
        c for c in macd.columns if c.startswith("MACDh_")][0]]

    bb = ta.bbands(df["close"], length=BB_PERIOD, std=BB_STD)
    df["bb_upper"] = bb[[c for c in bb.columns if c.startswith("BBU_")][0]]
    df["bb_lower"] = bb[[c for c in bb.columns if c.startswith("BBL_")][0]]
    df["bb_mid"] = bb[[c for c in bb.columns if c.startswith("BBM_")][0]]

    df[f"ema{EMA_FAST}"] = ta.ema(df["close"], length=EMA_FAST)
    df[f"ema{EMA_SLOW}"] = ta.ema(df["close"], length=EMA_SLOW)
    df[f"ema{CROSS_EMA_FAST}"] = ta.ema(df["close"], length=CROSS_EMA_FAST)
    df[f"ema{CROSS_EMA_SLOW}"] = ta.ema(df["close"], length=CROSS_EMA_SLOW)
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=ATR_PERIOD)
    return df


def golden_cross_within(df, lookback):
    fast = df[f"ema{CROSS_EMA_FAST}"].iloc[-lookback:]
    slow = df[f"ema{CROSS_EMA_SLOW}"].iloc[-lookback:]
    return ((fast.shift(1) < slow.shift(1)) & (fast >= slow)).any()


def death_cross_within(df, lookback):
    fast = df[f"ema{CROSS_EMA_FAST}"].iloc[-lookback:]
    slow = df[f"ema{CROSS_EMA_SLOW}"].iloc[-lookback:]
    return ((fast.shift(1) > slow.shift(1)) & (fast <= slow)).any()


# ─────────────────────────────────────────
#  SCORING
# ─────────────────────────────────────────
def score_long(row_pri, row_htf, df_pri):
    s = 0.0
    if row_pri["rsi"] <= LONG_RSI_PRI:
        s += 2
    if row_htf["rsi"] <= LONG_RSI_HTF:
        s += 1.5
    if row_pri["stoch_rsi_k"] <= LONG_STOCH_RSI:
        s += 1.5
    if row_pri["adx"] >= LONG_ADX_MIN:
        s += 1
    if row_pri["macd_hist"] > 0:
        s += 1
    if row_pri["close"] <= row_pri["bb_lower"] * 1.01:
        s += 0.5
    if row_pri[f"ema{EMA_FAST}"] > row_pri[f"ema{EMA_SLOW}"]:
        s += 0.5
    if REQUIRE_VOL_SPIKE:
        avg_vol = df_pri["volume"].iloc[-20:-1].mean()
        if row_pri["volume"] >= avg_vol * VOLUME_MULT:
            s += 0.5
    return s


def score_short(row_pri, row_htf, df_pri):
    s = 0.0
    if row_pri["rsi"] >= SHORT_RSI_PRI:
        s += 2
    if row_htf["rsi"] >= SHORT_RSI_HTF:
        s += 1.5
    if row_pri["stoch_rsi_k"] >= SHORT_STOCH_RSI:
        s += 1.5
    if row_pri["adx"] >= SHORT_ADX_MIN:
        s += 1
    if row_pri["macd_hist"] < 0:
        s += 1
    if row_pri["close"] >= row_pri["bb_upper"] * 0.99:
        s += 0.5
    if row_pri[f"ema{EMA_FAST}"] < row_pri[f"ema{EMA_SLOW}"]:
        s += 0.5
    if REQUIRE_VOL_SPIKE:
        avg_vol = df_pri["volume"].iloc[-20:-1].mean()
        if row_pri["volume"] >= avg_vol * VOLUME_MULT:
            s += 0.5
    return s


# ─────────────────────────────────────────
#  TP / SL CHECK
# ─────────────────────────────────────────
def check_tp_sl(price: float):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    still_open = []

    with state_lock:
        trades_snapshot = list(open_trades)

    for t in trades_snapshot:
        hit = None
        if t["side"] == "LONG":
            if price >= t["tp"]:
                hit = "TP"
            elif price <= t["sl"]:
                hit = "SL"
        else:
            if price <= t["tp"]:
                hit = "TP"
            elif price >= t["sl"]:
                hit = "SL"

        if hit:
            exit_price = t["tp"] if hit == "TP" else t["sl"]
            if t["side"] == "LONG":
                pnl_pct = (exit_price - t["entry"]) / t["entry"] * 100
            else:
                pnl_pct = (t["entry"] - exit_price) / t["entry"] * 100
            pnl_usd = round(pnl_pct / 100 * t["entry"] * t["size"], 4)
            pnl_pct = round(pnl_pct, 3)

            closed = {**t, "exit_price": exit_price, "exit_time": now,
                      "result": hit, "pnl_usd": pnl_usd, "pnl_pct": pnl_pct}
            with state_lock:
                closed_trades.append(closed)

            emoji = "✅" if hit == "TP" else "🛑"
            sign = "+" if pnl_usd >= 0 else ""
            tg_msg = (
                f"{emoji} *{hit} HIT — {t['side']} {SYMBOL}*\n"
                f"Entry  : {t['entry']}\n"
                f"Exit   : {exit_price}\n"
                f"P&L    : {sign}{pnl_usd} USDT  ({sign}{pnl_pct}%)\n"
                f"Closed : {now}"
            )
            kind = "close_win" if pnl_usd >= 0 else "close_loss"
            log(f"{hit} hit | {t['side']} | Entry {t['entry']} → Exit {exit_price} | P&L {sign}{pnl_usd} USDT ({sign}{pnl_pct}%)", kind)
            send_telegram(tg_msg)
        else:
            still_open.append(t)

    with state_lock:
        open_trades[:] = still_open


# ─────────────────────────────────────────
#  SIGNAL CHECK
# ─────────────────────────────────────────
def check_signals(exchange):
    global last_check, current_price

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    last_check = now_str

    df_pri = fetch_ohlcv(exchange, PRIMARY_TF, limit=300)
    df_htf = fetch_ohlcv(exchange, CONFIRM_TF, limit=100)
    df_pri = add_indicators(df_pri)
    df_htf = add_indicators(df_htf)

    price = float(df_pri.iloc[-1]["close"])
    current_price = price

    # Check TP/SL first
    check_tp_sl(price)

    with state_lock:
        n_open = len(open_trades)

    if n_open >= MAX_TRADES:
        log(
            f"Max concurrent trades reached ({MAX_TRADES}), skipping signal scan.", "info")
        return

    row_pri = df_pri.iloc[-2]
    row_htf = df_htf.iloc[-2]

    # ── LONG ──
    long_score = score_long(row_pri, row_htf, df_pri)
    golden_ok = (not REQUIRE_GOLDEN) or golden_cross_within(
        df_pri, CROSS_LOOKBACK)

    if long_score >= LONG_MIN_SCORE and golden_ok:
        tp = round(price * (1 + LONG_TP_PCT / 100), 4)
        sl = round(price * (1 - LONG_SL_PCT / 100), 4)
        trade = {"id": str(uuid.uuid4())[:8], "side": "LONG", "entry": price,
                 "tp": tp, "sl": sl, "size": TRADE_SIZE,
                 "score": long_score, "time": now_str}
        with state_lock:
            open_trades.append(trade)
        tg_msg = (
            f"🟢 *LONG SIGNAL — {SYMBOL}*\n"
            f"Time   : {now_str}\n"
            f"Score  : {long_score:.1f} / 10\n"
            f"Entry  : {price}\n"
            f"TP     : {tp}  (+{LONG_TP_PCT}%)\n"
            f"SL     : {sl}  (-{LONG_SL_PCT}%)\n"
            f"Size   : {TRADE_SIZE} ETH | Golden cross ✅"
        )
        log(f"LONG signal | Score {long_score:.1f} | Entry {price} | TP {tp} | SL {sl}", "long")
        send_telegram(tg_msg)

    # ── SHORT ──
    short_score = score_short(row_pri, row_htf, df_pri)
    death_ok = (not REQUIRE_DEATH) or death_cross_within(
        df_pri, CROSS_LOOKBACK)

    if short_score >= SHORT_MIN_SCORE and death_ok:
        tp = round(price * (1 - SHORT_TP_PCT / 100), 4)
        sl = round(price * (1 + SHORT_SL_PCT / 100), 4)
        trade = {"id": str(uuid.uuid4())[:8], "side": "SHORT", "entry": price,
                 "tp": tp, "sl": sl, "size": TRADE_SIZE,
                 "score": short_score, "time": now_str}
        with state_lock:
            open_trades.append(trade)
        tg_msg = (
            f"🔴 *SHORT SIGNAL — {SYMBOL}*\n"
            f"Time   : {now_str}\n"
            f"Score  : {short_score:.1f} / 10\n"
            f"Entry  : {price}\n"
            f"TP     : {tp}  (-{SHORT_TP_PCT}%)\n"
            f"SL     : {sl}  (+{SHORT_SL_PCT}%)\n"
            f"Size   : {TRADE_SIZE} ETH | Death cross ✅"
        )
        log(f"SHORT signal | Score {short_score:.1f} | Entry {price} | TP {tp} | SL {sl}", "short")
        send_telegram(tg_msg)

    if long_score < LONG_MIN_SCORE and short_score < SHORT_MIN_SCORE:
        log(f"No signal | Price {price} | Long score {long_score:.1f} | Short score {short_score:.1f}", "info")


# ─────────────────────────────────────────
#  FLASK API
# ─────────────────────────────────────────
app = Flask(__name__)
CORS(app)


@app.route("/api/state")
def api_state():
    with state_lock:
        wins = sum(1 for t in closed_trades if t["pnl_usd"] >= 0)
        total = len(closed_trades)
        total_pnl = round(sum(t["pnl_usd"] for t in closed_trades), 4)
        return jsonify({
            "open_trades":   list(open_trades),
            "closed_trades": list(reversed(closed_trades)),
            "console_log":   list(reversed(console_log)),
            "last_check":    last_check,
            "current_price": current_price,
            "symbol":        SYMBOL,
            "stats": {
                "total_closed": total,
                "wins":         wins,
                "losses":       total - wins,
                "win_rate":     round(wins / total * 100, 1) if total else 0,
                "total_pnl":    total_pnl,
            }
        })


def run_flask():
    app.run(host="0.0.0.0", port=DASHBOARD_PORT,
            debug=False, use_reloader=False)


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
def main():
    exchange = getattr(ccxt, EXCHANGE_ID)({"enableRateLimit": True})

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    log(f"Bot started | {SYMBOL} {PRIMARY_TF} | confirm {CONFIRM_TF} | dashboard → http://localhost:{DASHBOARD_PORT}", "info")
    send_telegram(
        f"🤖 Trading bot started — `{SYMBOL}` `{PRIMARY_TF}` — dashboard on port {DASHBOARD_PORT}")

    while True:
        try:
            check_signals(exchange)
        except Exception as e:
            log(f"ERROR: {e}", "error")
            send_telegram(f"⚠️ Bot error: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
