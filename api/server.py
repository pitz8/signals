import os
import json
import logging
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, send_file
import requests as http
import ccxt
import pandas as pd
import ta
import numpy as np

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

##############################
# CONFIG
##############################

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8710025937:AAFLoQOsAOvHI4qgB9TBG53Al9sYfiDBilE')
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
CRON_SECRET = os.environ.get('CRON_SECRET', '')

SYMBOLS = {"BTC": "BTC/USDT", "ETH": "ETH/USDT"}
TIMEFRAMES = ["15m", "1h", "4h", "1d"]
CANDLE_LIMIT = 150  # reduced for faster cold starts on Vercel
SIGNAL_EXPIRY_HOURS = 168  # 7 days

exchange = ccxt.binance({'enableRateLimit': True})

##############################
# DATABASE
##############################

def is_postgres():
    return bool(os.environ.get('POSTGRES_URL'))

def get_conn():
    if is_postgres():
        import psycopg2
        return psycopg2.connect(os.environ['POSTGRES_URL'])
    db_path = '/tmp/signals.db' if os.environ.get('VERCEL') else 'signals.db'
    return sqlite3.connect(db_path)

def ph():
    return '%s' if is_postgres() else '?'

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    pk = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        chat_id BIGINT PRIMARY KEY,
        registered_at TEXT
    )""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS signals (
        id {pk},
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
    )""")
    conn.commit()
    conn.close()

try:
    init_db()
except Exception as e:
    logging.error(f"DB init error: {e}")


def get_all_users():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM users")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def register_user(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    try:
        cur.execute(f"INSERT INTO users (chat_id, registered_at) VALUES ({p},{p}) ON CONFLICT DO NOTHING"
                    if is_postgres() else
                    f"INSERT OR IGNORE INTO users (chat_id, registered_at) VALUES ({p},{p})",
                    (chat_id, datetime.now().isoformat()))
        conn.commit()
    except Exception:
        conn.rollback()
    conn.close()


def save_signal(asset, signal_type, entry_price, tp, sl, net_score):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now().isoformat()
    if is_postgres():
        cur.execute(
            f"INSERT INTO signals (asset,signal_type,entry_price,tp,sl,net_score,opened_at) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p}) RETURNING id",
            (asset, signal_type, entry_price, tp, sl, net_score, now)
        )
        sig_id = cur.fetchone()[0]
    else:
        cur.execute(
            f"INSERT INTO signals (asset,signal_type,entry_price,tp,sl,net_score,opened_at) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p})",
            (asset, signal_type, entry_price, tp, sl, net_score, now)
        )
        sig_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sig_id


def close_signal(sig_id, outcome):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now()
    row = conn.execute(f"SELECT opened_at FROM signals WHERE id={p}", (sig_id,)).fetchone()
    duration = None
    if row:
        try:
            opened = datetime.fromisoformat(row[0])
            duration = round((now - opened).total_seconds() / 3600, 2)
        except Exception:
            pass
    cur.execute(
        f"UPDATE signals SET closed_at={p}, outcome={p}, duration_hours={p} WHERE id={p}",
        (now.isoformat(), outcome, duration, sig_id)
    )
    conn.commit()
    conn.close()
    return duration


def get_open_signals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, opened_at "
        "FROM signals WHERE outcome IS NULL ORDER BY opened_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_stats():
    conn = get_conn()
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
    closed = tp_hits + sl_hits
    win_rate = (tp_hits / closed * 100) if closed > 0 else 0
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
    return {'total': total, 'tp_hits': tp_hits, 'sl_hits': sl_hits,
            'expired': expired, 'win_rate': win_rate,
            'avg_duration': avg_duration, 'assets': assets}

##############################
# TELEGRAM
##############################

def tg_send(chat_id, text, keyboard=None):
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown'}
    if keyboard:
        payload['reply_markup'] = json.dumps({'inline_keyboard': keyboard})
    try:
        http.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")


def tg_answer_callback(callback_id):
    try:
        http.post(f"{TELEGRAM_API}/answerCallbackQuery",
                  json={'callback_query_id': callback_id}, timeout=5)
    except Exception:
        pass


def broadcast(text, keyboard=None):
    for chat_id in get_all_users():
        tg_send(chat_id, text, keyboard)


def signal_keyboard():
    return [[
        {'text': '📊 Stats', 'callback_data': 'stats'},
        {'text': '📋 Open Trades', 'callback_data': 'open_trades'}
    ]]


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
# ANALYSIS
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


def compute_indicators(df):
    if df.empty or len(df) < 50:
        return df
    close = df["close"]
    volume = df["volume"]
    df["rsi"] = ta.momentum.RSIIndicator(close).rsi()
    macd = ta.trend.MACD(close)
    df["macd_diff"] = macd.macd_diff()
    df["ema20"] = ta.trend.EMAIndicator(close, window=20).ema_indicator()
    df["ema50"] = ta.trend.EMAIndicator(close, window=50).ema_indicator()
    df["ema200"] = ta.trend.EMAIndicator(close, window=min(200, len(df) - 1)).ema_indicator()
    df["vol_ma20"] = volume.rolling(window=20).mean()
    df["vol_spike"] = volume / df["vol_ma20"]
    df["atr"] = ta.volatility.AverageTrueRange(df["high"], df["low"], close).average_true_range()
    df["bb_pct"] = ta.volatility.BollingerBands(close).bollinger_pband()
    df["adx"] = ta.trend.ADXIndicator(df["high"], df["low"], close).adx()
    return df


def get_order_book_analysis(symbol):
    try:
        ob = exchange.fetch_order_book(symbol, limit=20)
        bids_vol = sum(v[1] for v in ob['bids'])
        asks_vol = sum(v[1] for v in ob['asks'])
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
    symbol = SYMBOLS[name]
    total_bull, total_bear = 0, 0
    mtf_results = {}

    # Fetch all timeframes in parallel
    with ThreadPoolExecutor(max_workers=4) as ex:
        tf_futures = {tf: ex.submit(fetch_crypto, symbol, tf) for tf in TIMEFRAMES}
        tf_data = {tf: tf_futures[tf].result() for tf in TIMEFRAMES}

    for tf in TIMEFRAMES:
        df = compute_indicators(tf_data[tf])
        bull, bear, details = get_detailed_score(df)
        weight = {"1d": 2.0, "4h": 1.5, "1h": 1.0, "15m": 0.5}.get(tf, 1.0)
        total_bull += bull * weight
        total_bear += bear * weight
        mtf_results[tf] = {
            "score": f"{bull}B / {bear}S",
            "signals": ", ".join(details) if details else "Neutral"
        }

    ob_bull, ob_bear, ob_msg = get_order_book_analysis(symbol)
    total_bull += ob_bull * 1.5
    total_bear += ob_bear * 1.5

    net_score = total_bull - total_bear
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

    df1h = compute_indicators(tf_data["1h"])
    current_price = df1h["close"].iloc[-1]
    atr = df1h["atr"].iloc[-1]

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
        "timestamp": datetime.utcnow().strftime("%H:%M")
    }

##############################
# SCANNER + MONITOR LOGIC
##############################

def run_cycle():
    results = []
    for asset_name in ["BTC", "ETH"]:
        try:
            result = analyze_asset(asset_name)
            price_f = result['price_raw']
            tp_f = result['tp_raw']
            sl_f = result['sl_raw']
            risk = abs(price_f - sl_f)
            reward = abs(tp_f - price_f)
            rr_ratio = reward / risk if risk != 0 else 0

            warnings = []
            for tf, data in result['mtf'].items():
                sig_upper = data['signals'].upper()
                if "EXHAUSTION" in sig_upper:
                    warnings.append(f"⚠️ {tf.upper()} Exhaustion")
                if "CROSS" in sig_upper:
                    warnings.append(f"⚡ {tf.upper()} {data['signals']}")
                if "WHALE" in sig_upper:
                    warnings.append(f"🐋 {tf.upper()} Whale Spike")

            is_actionable = "BUY" in result['signal'] or "SELL" in result['signal']
            has_major_alert = len(warnings) > 0

            if not (is_actionable or has_major_alert):
                results.append({'asset': asset_name, 'sent': False, 'signal': result['signal']})
                continue

            sig_id = None
            if is_actionable:
                sig_id = save_signal(
                    asset=asset_name,
                    signal_type=result['signal'],
                    entry_price=price_f,
                    tp=tp_f,
                    sl=sl_f,
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
                    f"🔍 **Tracking:** Signal #{sig_id}\n"
                )
            message += f"━━━━━━━━━━━━━━━━━\n📑 **MTF DATA:**\n"
            for tf, data in result['mtf'].items():
                message += f"• *{tf.upper()}*: {data['score']} — _{data['signals']}_\n"
            message += f"━━━━━━━━━━━━━━━━━\n⏰ *Update:* {result['timestamp']} UTC"

            broadcast(message, keyboard=signal_keyboard())
            logging.info(f"Signal sent: {asset_name} {result['signal']} #{sig_id}")
            results.append({'asset': asset_name, 'sent': True, 'signal': result['signal'], 'sig_id': sig_id})

        except Exception as e:
            logging.error(f"Cycle error {asset_name}: {e}")
            results.append({'asset': asset_name, 'error': str(e)})
    return results


def check_open_signals():
    open_sigs = get_open_signals()
    closed = []
    for sig in open_sigs:
        sig_id, asset, signal_type, entry_price, tp, sl, opened_at = sig
        symbol = SYMBOLS.get(asset)
        if not symbol:
            continue
        try:
            opened = datetime.fromisoformat(opened_at)
            age_hours = (datetime.now() - opened).total_seconds() / 3600

            if age_hours >= SIGNAL_EXPIRY_HOURS:
                ticker = exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                close_signal(sig_id, 'EXPIRED')
                msg = (
                    f"⏳ *SIGNAL EXPIRED — {asset}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"Signal: `{signal_type}`\n"
                    f"Entry: `${entry_price:,.2f}` | Now: `${current_price:,.2f}`\n"
                    f"Duration: `{age_hours:.1f}h` — Neither TP nor SL hit in 7 days\n"
                    f"━━━━━━━━━━━━━━━━━"
                )
                broadcast(msg)
                closed.append({'id': sig_id, 'outcome': 'EXPIRED'})
                continue

            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']

            is_buy = 'BUY' in signal_type
            hit = None
            if is_buy:
                if current_price >= tp:
                    hit = 'TP_HIT'
                elif current_price <= sl:
                    hit = 'SL_HIT'
            else:
                if current_price <= tp:
                    hit = 'TP_HIT'
                elif current_price >= sl:
                    hit = 'SL_HIT'

            if hit:
                duration = close_signal(sig_id, hit)
                pnl_pct = abs(current_price - entry_price) / entry_price * 100
                if duration and duration < 1:
                    dur_str = f"{duration * 60:.0f}m"
                elif duration and duration < 24:
                    dur_str = f"{duration:.1f}h"
                else:
                    dur_str = f"{duration / 24:.1f}d" if duration else "?"

                emoji = '✅' if hit == 'TP_HIT' else '❌'
                label = 'TARGET HIT' if hit == 'TP_HIT' else 'STOP LOSS HIT'
                msg = (
                    f"{emoji} *{label} — {asset}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"Signal: `{signal_type}`\n"
                    f"Entry:  `${entry_price:,.2f}`\n"
                    f"Exit:   `${current_price:,.2f}`\n"
                    f"Move:   `{pnl_pct:.2f}%`\n"
                    f"⏱ Duration: `{dur_str}`\n"
                    f"━━━━━━━━━━━━━━━━━"
                )
                broadcast(msg, keyboard=signal_keyboard())
                logging.info(f"Signal #{sig_id} closed: {hit} in {dur_str}")
                closed.append({'id': sig_id, 'outcome': hit, 'duration': dur_str})

        except Exception as e:
            logging.error(f"Monitor error signal #{sig_id}: {e}")
    return closed

##############################
# FLASK ROUTES
##############################

def check_secret():
    # Vercel Cron sends this header automatically
    if request.headers.get('x-vercel-cron') == '1':
        return True
    if not CRON_SECRET:
        return True
    secret = request.headers.get('X-Cron-Secret') or request.args.get('secret', '')
    return secret == CRON_SECRET


@app.route('/')
def dashboard():
    path = os.path.join(os.path.dirname(__file__), '..', 'dashboard.html')
    return send_file(os.path.abspath(path))


@app.route('/api/webhook', methods=['POST'])
def webhook():
    data = request.json
    if not data:
        return 'ok'

    if 'message' in data:
        msg = data['message']
        chat_id = msg['chat']['id']
        text = msg.get('text', '')
        register_user(chat_id)

        if text == '/start' or not text.startswith('/'):
            tg_send(chat_id,
                "📡 *Premium Signals Bot*\n"
                "━━━━━━━━━━━━━━━━━\n"
                "BTC & ETH signals with TP/SL — automatically tracked.\n\n"
                "/stats — Accuracy report\n"
                "/trades — Open monitored trades")
        elif text == '/stats':
            stats = get_stats()
            tg_send(chat_id, format_stats(stats) if stats else "📊 No completed signals yet.")
        elif text == '/trades':
            tg_send(chat_id, format_open_trades(get_open_signals()))

    elif 'callback_query' in data:
        query = data['callback_query']
        chat_id = query['message']['chat']['id']
        cb = query['data']
        tg_answer_callback(query['id'])
        if cb == 'stats':
            stats = get_stats()
            tg_send(chat_id, format_stats(stats) if stats else "📊 No completed signals yet.")
        elif cb == 'open_trades':
            tg_send(chat_id, format_open_trades(get_open_signals()))

    return 'ok'


@app.route('/api/scanner')
def scanner():
    if not check_secret():
        return jsonify({'error': 'Unauthorized'}), 401
    results = run_cycle()
    return jsonify({'status': 'ok', 'results': results})


@app.route('/api/monitor')
def monitor():
    if not check_secret():
        return jsonify({'error': 'Unauthorized'}), 401
    closed = check_open_signals()
    return jsonify({'status': 'ok', 'closed': closed})


@app.route('/api/stats')
def api_stats():
    stats = get_stats()
    return jsonify(stats or {})


@app.route('/api/open')
def api_open():
    rows = get_open_signals()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'opened_at']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/signals')
def api_signals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, net_score, "
        "opened_at, closed_at, outcome, duration_hours "
        "FROM signals ORDER BY opened_at DESC LIMIT 100"
    )
    rows = cur.fetchall()
    conn.close()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'net_score',
            'opened_at', 'closed_at', 'outcome', 'duration_hours']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/setup')
def setup_webhook():
    """Visit this once after deploy to register Telegram webhook."""
    base = request.url_root.rstrip('/')
    webhook_url = f"{base}/api/webhook"
    resp = http.post(f"{TELEGRAM_API}/setWebhook", json={'url': webhook_url})
    return jsonify({'webhook_url': webhook_url, 'telegram_response': resp.json()})


@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.utcnow().isoformat()})


if __name__ == '__main__':
    app.run(debug=True, port=5001)
