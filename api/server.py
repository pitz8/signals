import os
import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta

def now_it():
    """Current time in Italy (UTC+1/+2)."""
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1)))
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, send_file
import requests as http
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

SYMBOLS = {"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}
TIMEFRAMES = ["15m", "1h", "4h", "1d"]
TF_MAP = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}
SIGNAL_EXPIRY_HOURS = 168  # 7 days
KRAKEN_API = "https://api.kraken.com/0/public"

STARTING_BUDGET = 2000.0
POSITION_ETH = 1.0       # always trade 1 ETH
LEVERAGE = 25             # 25x leverage

# ── Strat 2 constants ──────────────────────────────────────────────────────
S2_LONG_TP_PCT  = 1.0   # +1% TP
S2_LONG_SL_PCT  = 3.0   # -3% SL
S2_SHORT_TP_PCT = 1.0   # -1% TP
S2_SHORT_SL_PCT = 3.0   # +3% SL
S2_LONG_MIN_SCORE  = 7.5
S2_SHORT_MIN_SCORE = 7.5
S2_MAX_TRADES      = 3
S2_CROSS_LOOKBACK  = 50

# ── S3: Inverted Strategy ─────────────────────────────────────────────────
S3_SYMBOLS      = {"ETH": "ETHUSD", "BTC": "XBTUSD", "SOL": "SOLUSD"}
S3_ASSETS       = ["ETH", "BTC", "SOL"]
S3_THRESHOLD    = 9.0
S3_ATR_TP_MULT  = 2.0
S3_ATR_SL_MULT  = 3.0
S3_MAX_OPEN     = 3
S3_COOLDOWN_H   = 4.0
S3_RISK_PCT     = 0.02

##############################
# DATABASE
##############################

def is_postgres():
    return bool(os.environ.get('POSTGRES_URL'))

def get_conn():
    if is_postgres():
        import psycopg2
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        url = os.environ['POSTGRES_URL']
        # Strip params psycopg2 doesn't support (channel_binding)
        parsed = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()
                  if k not in ('channel_binding',)}
        url = urlunparse(parsed._replace(query=urlencode(params)))
        return psycopg2.connect(url)
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
    cur.execute(f"""CREATE TABLE IF NOT EXISTS portfolio (
        id INTEGER PRIMARY KEY,
        budget REAL DEFAULT {STARTING_BUDGET},
        updated_at TEXT
    )""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS signals_s2 (
        id {pk},
        asset TEXT,
        signal_type TEXT,
        entry_price REAL,
        tp REAL,
        sl REAL,
        score REAL,
        opened_at TEXT,
        closed_at TEXT,
        outcome TEXT,
        duration_hours REAL,
        pnl_usd REAL,
        margin_used REAL
    )""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS signals_s3 (
        id {pk},
        asset TEXT,
        signal_type TEXT,
        entry_price REAL,
        tp REAL,
        sl REAL,
        score REAL,
        atr_used REAL,
        opened_at TEXT,
        closed_at TEXT,
        outcome TEXT,
        duration_hours REAL,
        pnl_usd REAL,
        margin_used REAL
    )""")
    p = '%s' if is_postgres() else '?'
    if is_postgres():
        cur.execute(f"INSERT INTO portfolio (id, budget, updated_at) VALUES (1, {STARTING_BUDGET}, {p}) ON CONFLICT DO NOTHING",
                    (datetime.now().isoformat(),))
    else:
        cur.execute(f"INSERT OR IGNORE INTO portfolio (id, budget, updated_at) VALUES (1, {STARTING_BUDGET}, {p})",
                    (datetime.now().isoformat(),))
    conn.commit()
    conn.close()

try:
    init_db()
    for col_sql in [
        "ALTER TABLE signals ADD COLUMN details TEXT",
        "ALTER TABLE signals ADD COLUMN liq_price REAL",
        "ALTER TABLE signals ADD COLUMN margin_used REAL",
        "ALTER TABLE signals ADD COLUMN pnl_usd REAL",
    ]:
        _conn = get_conn()
        _cur = _conn.cursor()
        try:
            _cur.execute(col_sql)
            _conn.commit()
        except Exception:
            _conn.rollback()
        finally:
            _conn.close()
except Exception as e:
    logging.error(f"DB init error: {e}")


def get_budget():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT budget FROM portfolio WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    return float(row[0]) if row else STARTING_BUDGET


def update_budget(delta):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    cur.execute(f"UPDATE portfolio SET budget = budget + {p}, updated_at = {p} WHERE id = 1",
                (delta, datetime.now().isoformat()))
    conn.commit()
    conn.close()


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


def save_signal(asset, signal_type, entry_price, tp, sl, net_score, details=None, liq_price=None, margin_used=None):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now().isoformat()
    details_json = json.dumps(details) if details else None
    if is_postgres():
        cur.execute(
            f"INSERT INTO signals (asset,signal_type,entry_price,tp,sl,net_score,opened_at,details,liq_price,margin_used) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p}) RETURNING id",
            (asset, signal_type, entry_price, tp, sl, net_score, now, details_json, liq_price, margin_used)
        )
        sig_id = cur.fetchone()[0]
    else:
        cur.execute(
            f"INSERT INTO signals (asset,signal_type,entry_price,tp,sl,net_score,opened_at,details,liq_price,margin_used) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
            (asset, signal_type, entry_price, tp, sl, net_score, now, details_json, liq_price, margin_used)
        )
        sig_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sig_id


def close_signal(sig_id, outcome, pnl_usd=None):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now()
    cur.execute(f"SELECT opened_at FROM signals WHERE id={p}", (sig_id,))
    row = cur.fetchone()
    duration = None
    if row:
        try:
            opened = datetime.fromisoformat(row[0])
            duration = round((now - opened).total_seconds() / 3600, 2)
        except Exception:
            pass
    cur.execute(
        f"UPDATE signals SET closed_at={p}, outcome={p}, duration_hours={p}, pnl_usd={p} WHERE id={p}",
        (now.isoformat(), outcome, duration, pnl_usd, sig_id)
    )
    conn.commit()
    conn.close()
    if pnl_usd is not None:
        update_budget(pnl_usd)
    return duration


def get_open_signals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, opened_at, details, liq_price, margin_used "
        "FROM signals WHERE outcome IS NULL ORDER BY opened_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT asset, signal_type, outcome, duration_hours FROM signals WHERE outcome IS NOT NULL"
    )
    rows = cur.fetchall()
    conn.close()
    total = len(rows)
    if total == 0:
        return None
    tp_hits = sum(1 for r in rows if r[2] == 'TP_HIT')
    sl_hits = sum(1 for r in rows if r[2] == 'SL_HIT')
    expired = sum(1 for r in rows if r[2] == 'EXPIRED')
    reversed_ = sum(1 for r in rows if r[2] == 'CLOSED_REVERSED')
    durations = [r[3] for r in rows if r[3] is not None]
    avg_duration = sum(durations) / len(durations) if durations else 0
    closed = tp_hits + sl_hits
    win_rate = (tp_hits / closed * 100) if closed > 0 else 0
    assets = {}
    for r in rows:
        a = r[0]
        if a not in assets:
            assets[a] = {'tp': 0, 'sl': 0, 'expired': 0, 'reversed': 0, 'manual': 0, 'total': 0}
        assets[a]['total'] += 1
        if r[2] == 'TP_HIT':
            assets[a]['tp'] += 1
        elif r[2] == 'SL_HIT':
            assets[a]['sl'] += 1
        elif r[2] == 'EXPIRED':
            assets[a]['expired'] += 1
        elif r[2] == 'CLOSED_REVERSED':
            assets[a]['reversed'] += 1
        elif r[2] == 'CLOSED_MANUAL':
            assets[a]['manual'] += 1
    return {'total': total, 'tp_hits': tp_hits, 'sl_hits': sl_hits,
            'expired': expired, 'reversed': reversed_, 'win_rate': win_rate,
            'avg_duration': avg_duration, 'assets': assets}

##############################
# TELEGRAM
##############################

def tg_send(chat_id, text, keyboard=None):
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown'}
    if keyboard:
        payload['reply_markup'] = {'inline_keyboard': keyboard}
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
        f"🔄 Reversed: `{stats.get('reversed', 0)}`\n"
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
        sig_id, asset, signal_type, entry_price, tp, sl, opened_at = sig[:7]
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
        resp = http.get(f"{KRAKEN_API}/OHLC",
                        params={"pair": symbol, "interval": TF_MAP.get(timeframe, 60)},
                        timeout=15)
        resp.raise_for_status()
        data = resp.json()
        pair_key = next(k for k in data['result'] if k != 'last')
        rows = data['result'][pair_key]
        # Kraken OHLC: [time, open, high, low, close, vwap, volume, count]
        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "vwap", "volume", "count"])
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
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
        resp = http.get(f"{KRAKEN_API}/Depth", params={"pair": symbol, "count": 20}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pair_key = next(iter(data['result']))
        ob = data['result'][pair_key]
        bids_vol = sum(float(v[1]) for v in ob['bids'])
        asks_vol = sum(float(v[1]) for v in ob['asks'])
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

    tf_indicators = {}
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
        if not df.empty and len(df) >= 2:
            last = df.iloc[-1]
            def _f(col):
                v = last.get(col, float('nan'))
                return round(float(v), 2) if pd.notna(v) else None
            ema20, ema50, ema200 = _f('ema20'), _f('ema50'), _f('ema200')
            if ema20 and ema50 and ema200:
                ema_trend = 'bull' if ema20 > ema50 > ema200 else 'bear' if ema20 < ema50 < ema200 else 'mixed'
            else:
                ema_trend = 'mixed'
            tf_indicators[tf] = {
                'rsi': round(float(last['rsi']), 1) if pd.notna(last.get('rsi', float('nan'))) else None,
                'macd_diff': _f('macd_diff'),
                'adx': round(float(last['adx']), 1) if pd.notna(last.get('adx', float('nan'))) else None,
                'vol_spike': _f('vol_spike'),
                'bb_pct': _f('bb_pct'),
                'ema_trend': ema_trend,
            }

    ob_bull, ob_bear, ob_msg = get_order_book_analysis(symbol)
    total_bull += ob_bull * 1.5
    total_bear += ob_bear * 1.5

    net_score = total_bull - total_bear
    if net_score >= 9:
        signal = "🔥 STRONG BUY"
    elif net_score >= 7:
        signal = "✅ BUY"
    elif net_score <= -9:
        signal = "🚨 STRONG SELL"
    elif net_score <= -7:
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
        "price_raw": float(current_price),
        "tp_raw": float(tp),
        "sl_raw": float(sl),
        "net_score": round(net_score, 1),
        "liquidity": ob_msg,
        "mtf": mtf_results,
        "indicators": tf_indicators,
        "tp": f"{tp:,.2f}",
        "sl": f"{sl:,.2f}",
        "timestamp": now_it().strftime("%H:%M")
    }

##############################
# SCANNER + MONITOR LOGIC
##############################

def sig_direction(signal_type):
    """Returns ('BUY'|'SELL'|'NEUTRAL', is_strong)"""
    if 'STRONG BUY' in signal_type:
        return 'BUY', True
    elif 'BUY' in signal_type:
        return 'BUY', False
    elif 'STRONG SELL' in signal_type:
        return 'SELL', True
    elif 'SELL' in signal_type:
        return 'SELL', False
    return 'NEUTRAL', False


def run_cycle():
    # Analyze all assets first, then apply conflict filter
    analyses = {}
    for asset_name in ["BTC", "ETH"]:
        try:
            analyses[asset_name] = analyze_asset(asset_name)
        except Exception as e:
            logging.error(f"Analysis error {asset_name}: {e}")

    # Conflict check: if BTC and ETH point in opposite directions, keep only the stronger one
    actionable = {k: v for k, v in analyses.items()
                  if "BUY" in v['signal'] or "SELL" in v['signal']}
    if len(actionable) == 2:
        dirs = {k: sig_direction(v['signal'])[0] for k, v in actionable.items()}
        if len(set(dirs.values())) == 2:  # opposite directions
            scores = {k: abs(v['net_score']) for k, v in actionable.items()}
            winner = max(scores, key=scores.get)
            loser = [k for k in actionable if k != winner][0]
            logging.info(f"Conflict filter: {loser} skipped (score {scores[loser]:.1f} < {scores[winner]:.1f})")
            analyses[loser]['_conflict_skipped'] = True
            broadcast(
                f"⚖️ *CONFLICT FILTER — {loser} skipped*\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"BTC `{analyses['BTC']['signal']}` (score `{analyses['BTC']['net_score']}`) vs "
                f"ETH `{analyses['ETH']['signal']}` (score `{analyses['ETH']['net_score']}`)\n"
                f"→ Trading only *{winner}* (stronger conviction)\n"
                f"━━━━━━━━━━━━━━━━━"
            )

    results = []
    for asset_name in ["BTC", "ETH"]:
        try:
            result = analyses.get(asset_name)
            if not result:
                continue
            if result.get('_conflict_skipped'):
                results.append({'asset': asset_name, 'sent': False, 'signal': 'conflict_skipped'})
                continue
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

            # Scale-in / flip logic
            to_flip = []
            if is_actionable:
                open_on_asset = [s for s in get_open_signals() if s[1] == asset_name]
                new_dir, new_strong = sig_direction(result['signal'])
                skip = False

                for s in open_on_asset:
                    existing_dir, existing_strong = sig_direction(s[2])
                    if existing_dir == new_dir:
                        # Same direction: allow only if escalating (normal → strong)
                        if new_strong and not existing_strong:
                            pass  # scale-in allowed
                        else:
                            skip = True
                            break
                    else:
                        # Opposite direction: flip only if new signal is STRONG
                        if new_strong:
                            to_flip.append(s)
                        else:
                            skip = True
                            break

                if skip:
                    results.append({'asset': asset_name, 'sent': False, 'signal': 'duplicate_skipped'})
                    continue

                # Close any trades being flipped
                for s in to_flip:
                    flip_id, _, flip_type, flip_entry, _, _, _ = s
                    r2 = http.get(f"{KRAKEN_API}/Ticker", params={"pair": SYMBOLS[asset_name]}, timeout=10)
                    pair_key2 = next(iter(r2.json()['result']))
                    cur_price = float(r2.json()['result'][pair_key2]['c'][0])
                    duration = close_signal(flip_id, 'CLOSED_REVERSED')
                    pnl = (cur_price - flip_entry) / flip_entry * 100
                    if 'SELL' in flip_type:
                        pnl = -pnl
                    dur_str = f"{duration:.1f}h" if duration else "?"
                    flip_msg = (
                        f"🔄 *REVERSED — {asset_name}*\n"
                        f"━━━━━━━━━━━━━━━━━\n"
                        f"Closed: `{flip_type}` → New: `{result['signal']}`\n"
                        f"Entry: `${flip_entry:,.2f}` | Exit: `${cur_price:,.2f}`\n"
                        f"P&L: `{'+' if pnl>=0 else ''}{pnl:.2f}%` | Duration: `{dur_str}`\n"
                        f"━━━━━━━━━━━━━━━━━"
                    )
                    broadcast(flip_msg)
                    logging.info(f"Signal #{flip_id} reversed: {flip_type} → {result['signal']}")

            sig_id = None
            if is_actionable:
                is_buy = 'BUY' in result['signal']
                is_strong = 'STRONG' in result['signal']
                eth_price = analyses.get('ETH', {}).get('price_raw') or price_f
                margin = round(eth_price / LEVERAGE * (1.5 if is_strong else 1.0), 2)
                liq = round(price_f * (1 - 1/LEVERAGE) if is_buy else price_f * (1 + 1/LEVERAGE), 2)
                sig_id = save_signal(
                    asset=asset_name,
                    signal_type=result['signal'],
                    entry_price=price_f,
                    tp=tp_f,
                    sl=sl_f,
                    net_score=result['net_score'],
                    details={
                        'liquidity': result['liquidity'],
                        'mtf': result['mtf'],
                        'indicators': result['indicators'],
                    },
                    liq_price=liq,
                    margin_used=margin,
                )

            scale_tag = ""
            if is_actionable:
                if to_flip:
                    scale_tag = "🔄 *REVERSAL — position flipped*\n"
                else:
                    open_after = [s for s in get_open_signals() if s[1] == asset_name and s[0] != sig_id]
                    if open_after:
                        scale_tag = "📈 *SCALE-IN — adding to position*\n"

            message = (
                f"{scale_tag}📡 *[S1] {result['asset']} MARKET INTELLIGENCE*\n"
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


def calc_pnl_usd(signal_type, entry_price, exit_price, margin_used):
    is_buy = 'BUY' in signal_type
    eth_pos = margin_used * LEVERAGE if margin_used else entry_price * POSITION_ETH
    pnl = (exit_price - entry_price) / entry_price * eth_pos if is_buy else (entry_price - exit_price) / entry_price * eth_pos
    return round(pnl, 2)


def fmt_dur(duration):
    if not duration:
        return "?"
    if duration < 1:
        return f"{duration * 60:.0f}m"
    if duration < 24:
        return f"{duration:.1f}h"
    return f"{duration / 24:.1f}d"


def check_open_signals():
    open_sigs = get_open_signals()
    closed = []
    for sig in open_sigs:
        sig_id, asset, signal_type, entry_price, tp, sl, opened_at = sig[:7]
        liq_price = sig[8] if len(sig) > 8 else None
        margin_used = sig[9] if len(sig) > 9 else None
        symbol = SYMBOLS.get(asset)
        if not symbol:
            continue
        try:
            opened = datetime.fromisoformat(opened_at)
            age_hours = (datetime.now() - opened).total_seconds() / 3600

            r = http.get(f"{KRAKEN_API}/Ticker", params={"pair": symbol}, timeout=10)
            pair_key = next(iter(r.json()['result']))
            current_price = float(r.json()['result'][pair_key]['c'][0])

            if age_hours >= SIGNAL_EXPIRY_HOURS:
                close_signal(sig_id, 'EXPIRED')
                broadcast(
                    f"⏳ *SIGNAL EXPIRED — {asset}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"Signal: `{signal_type}`\n"
                    f"Entry: `${entry_price:,.2f}` | Now: `${current_price:,.2f}`\n"
                    f"Duration: `{age_hours:.1f}h` — No TP/SL in 7 days\n"
                    f"━━━━━━━━━━━━━━━━━"
                )
                closed.append({'id': sig_id, 'outcome': 'EXPIRED'})
                continue

            is_buy = 'BUY' in signal_type
            hit = None

            # Check liquidation first
            if liq_price:
                if is_buy and current_price <= liq_price:
                    hit = 'LIQUIDATED'
                elif not is_buy and current_price >= liq_price:
                    hit = 'LIQUIDATED'

            # Then TP/SL
            if not hit:
                if is_buy:
                    if current_price >= tp:   hit = 'TP_HIT'
                    elif current_price <= sl: hit = 'SL_HIT'
                else:
                    if current_price <= tp:   hit = 'TP_HIT'
                    elif current_price >= sl: hit = 'SL_HIT'

            if hit:
                if hit == 'LIQUIDATED':
                    exit_price = liq_price
                    pnl_usd = -margin_used if margin_used else round(-entry_price * POSITION_ETH / LEVERAGE, 2)
                elif hit == 'TP_HIT':
                    exit_price = tp
                    pnl_usd = calc_pnl_usd(signal_type, entry_price, exit_price, margin_used)
                else:  # SL_HIT
                    exit_price = sl
                    pnl_usd = calc_pnl_usd(signal_type, entry_price, exit_price, margin_used)

                duration = close_signal(sig_id, hit, pnl_usd)
                dur_str = fmt_dur(duration)
                budget_after = get_budget()

                if hit == 'LIQUIDATED':
                    msg = (
                        f"💥 *LIQUIDATED — {asset}*\n"
                        f"━━━━━━━━━━━━━━━━━\n"
                        f"Signal: `{signal_type}`\n"
                        f"Entry: `${entry_price:,.2f}` | Liq: `${exit_price:,.2f}`\n"
                        f"Loss: `${abs(pnl_usd):.2f}` (margin lost)\n"
                        f"⏱ Duration: `{dur_str}`\n"
                        f"💼 Budget remaining: `${budget_after:,.2f}`\n"
                        f"━━━━━━━━━━━━━━━━━"
                    )
                else:
                    emoji = '✅' if hit == 'TP_HIT' else '❌'
                    label = 'TARGET HIT' if hit == 'TP_HIT' else 'STOP LOSS HIT'
                    pnl_str = f"+${pnl_usd:.2f}" if pnl_usd >= 0 else f"-${abs(pnl_usd):.2f}"
                    pnl_pct = abs(current_price - entry_price) / entry_price * 100
                    msg = (
                        f"{emoji} *{label} — {asset}*\n"
                        f"━━━━━━━━━━━━━━━━━\n"
                        f"Signal: `{signal_type}`\n"
                        f"Entry: `${entry_price:,.2f}` | Exit: `${exit_price:,.2f}`\n"
                        f"Move: `{pnl_pct:.2f}%` | P&L: `{pnl_str}`\n"
                        f"⏱ Duration: `{dur_str}`\n"
                        f"💼 Budget remaining: `${budget_after:,.2f}`\n"
                        f"━━━━━━━━━━━━━━━━━"
                    )
                broadcast(msg, keyboard=signal_keyboard())
                logging.info(f"Signal #{sig_id} closed: {hit} pnl={pnl_usd:.2f} budget={budget_after:.2f}")
                closed.append({'id': sig_id, 'outcome': hit, 'pnl_usd': pnl_usd})

        except Exception as e:
            logging.error(f"Monitor error signal #{sig_id}: {e}")
    return closed

##############################
# STRAT 2 — ETH 15m + 4h
##############################

def save_signal_s2(signal_type, entry_price, tp, sl, score, margin_used):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now().isoformat()
    if is_postgres():
        cur.execute(
            f"INSERT INTO signals_s2 (asset,signal_type,entry_price,tp,sl,score,opened_at,margin_used) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p}) RETURNING id",
            ('ETH', signal_type, entry_price, tp, sl, score, now, margin_used)
        )
        sig_id = cur.fetchone()[0]
    else:
        cur.execute(
            f"INSERT INTO signals_s2 (asset,signal_type,entry_price,tp,sl,score,opened_at,margin_used) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p})",
            ('ETH', signal_type, entry_price, tp, sl, score, now, margin_used)
        )
        sig_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sig_id


def close_signal_s2(sig_id, outcome, pnl_usd=None):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now()
    cur.execute(f"SELECT opened_at FROM signals_s2 WHERE id={p}", (sig_id,))
    row = cur.fetchone()
    duration = None
    if row:
        try:
            opened = datetime.fromisoformat(row[0])
            duration = round((now - opened).total_seconds() / 3600, 2)
        except Exception:
            pass
    cur.execute(
        f"UPDATE signals_s2 SET closed_at={p}, outcome={p}, duration_hours={p}, pnl_usd={p} WHERE id={p}",
        (now.isoformat(), outcome, duration, pnl_usd, sig_id)
    )
    conn.commit()
    conn.close()
    if pnl_usd is not None:
        update_budget(pnl_usd)
    return duration


def get_open_signals_s2():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, opened_at, score, margin_used "
        "FROM signals_s2 WHERE outcome IS NULL ORDER BY opened_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_stats_s2():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT signal_type, outcome, duration_hours, pnl_usd FROM signals_s2 WHERE outcome IS NOT NULL")
    rows = cur.fetchall()
    conn.close()
    total = len(rows)
    tp_hits = sum(1 for r in rows if r[1] == 'TP_HIT')
    sl_hits = sum(1 for r in rows if r[1] == 'SL_HIT')
    closed = tp_hits + sl_hits
    win_rate = (tp_hits / closed * 100) if closed > 0 else 0
    durations = [r[2] for r in rows if r[2] is not None]
    avg_dur = sum(durations) / len(durations) if durations else 0
    total_pnl = sum(r[3] for r in rows if r[3] is not None)
    return {'total': total, 'tp_hits': tp_hits, 'sl_hits': sl_hits,
            'win_rate': win_rate, 'avg_duration': avg_dur, 'total_pnl': round(total_pnl, 2)}


# ── S3 DB helpers ────────────────────────────────────────────────────────────

def get_open_signals_s3():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, opened_at, score, margin_used "
        "FROM signals_s3 WHERE outcome IS NULL ORDER BY opened_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_last_signal_time_s3(asset):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    cur.execute(f"SELECT opened_at FROM signals_s3 WHERE asset={p} ORDER BY opened_at DESC LIMIT 1", (asset,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def save_signal_s3(asset, signal_type, entry, tp, sl, score, margin, atr):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now().isoformat()
    if is_postgres():
        cur.execute(
            f"INSERT INTO signals_s3 (asset,signal_type,entry_price,tp,sl,score,atr_used,opened_at,margin_used) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p}) RETURNING id",
            (asset, signal_type, entry, tp, sl, score, atr, now, margin)
        )
        sig_id = cur.fetchone()[0]
    else:
        cur.execute(
            f"INSERT INTO signals_s3 (asset,signal_type,entry_price,tp,sl,score,atr_used,opened_at,margin_used) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p})",
            (asset, signal_type, entry, tp, sl, score, atr, now, margin)
        )
        sig_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sig_id


def close_signal_s3(sig_id, outcome, pnl_usd=None):
    conn = get_conn()
    cur = conn.cursor()
    p = ph()
    now = datetime.now()
    cur.execute(f"SELECT opened_at FROM signals_s3 WHERE id={p}", (sig_id,))
    row = cur.fetchone()
    duration = None
    if row:
        try:
            opened = datetime.fromisoformat(row[0])
            duration = round((now - opened).total_seconds() / 3600, 2)
        except Exception:
            pass
    cur.execute(
        f"UPDATE signals_s3 SET closed_at={p}, outcome={p}, duration_hours={p}, pnl_usd={p} WHERE id={p}",
        (now.isoformat(), outcome, duration, pnl_usd, sig_id)
    )
    conn.commit()
    conn.close()
    if pnl_usd is not None:
        update_budget(pnl_usd)
    return duration


def get_stats_s3():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT outcome, duration_hours, pnl_usd FROM signals_s3 WHERE outcome IS NOT NULL")
    rows = cur.fetchall()
    conn.close()
    total = len(rows)
    tp_hits = sum(1 for r in rows if r[0] == 'TP_HIT')
    sl_hits = sum(1 for r in rows if r[0] == 'SL_HIT')
    closed = tp_hits + sl_hits
    win_rate = (tp_hits / closed * 100) if closed > 0 else 0
    durations = [r[1] for r in rows if r[1] is not None]
    avg_dur = sum(durations) / len(durations) if durations else 0
    total_pnl = sum(r[2] for r in rows if r[2] is not None)
    return {'total': total, 'tp_hits': tp_hits, 'sl_hits': sl_hits,
            'win_rate': round(win_rate, 1), 'avg_duration': round(avg_dur, 2),
            'total_pnl': round(total_pnl, 2)}


def run_cycle_s3():
    open_s3 = get_open_signals_s3()
    if len(open_s3) >= S3_MAX_OPEN:
        return {'sent': False, 'reason': 'max_trades_reached'}
    open_assets = {r[1] for r in open_s3}
    results = []
    for asset in S3_ASSETS:
        if asset in open_assets:
            continue
        last_t = get_last_signal_time_s3(asset)
        if last_t:
            try:
                age_h = (datetime.now() - datetime.fromisoformat(last_t)).total_seconds() / 3600
                if age_h < S3_COOLDOWN_H:
                    continue
            except Exception:
                pass
        symbol = S3_SYMBOLS[asset]
        try:
            with ThreadPoolExecutor(max_workers=4) as ex:
                tf_futures = {tf: ex.submit(fetch_crypto, symbol, tf) for tf in TIMEFRAMES}
                tf_data = {tf: tf_futures[tf].result() for tf in TIMEFRAMES}
        except Exception as e:
            logging.error(f"S3 fetch error {asset}: {e}")
            continue
        total_bull, total_bear = 0.0, 0.0
        for tf in TIMEFRAMES:
            df = compute_indicators(tf_data[tf])
            bull, bear, _ = get_detailed_score(df)
            weight = {"1d": 2.0, "4h": 1.5, "1h": 1.0, "15m": 0.5}.get(tf, 1.0)
            total_bull += bull * weight
            total_bear += bear * weight
        net_score = total_bull - total_bear
        if abs(net_score) < S3_THRESHOLD:
            continue
        df_1h = compute_indicators(tf_data["1h"])
        if df_1h.empty or len(df_1h) < 2:
            continue
        price = float(df_1h["close"].iloc[-1])
        atr   = float(df_1h["atr"].iloc[-1])
        if atr <= 0 or price <= 0:
            continue
        if net_score >= S3_THRESHOLD:
            signal_type = "SHORT"
            tp = round(price - atr * S3_ATR_TP_MULT, 2)
            sl = round(price + atr * S3_ATR_SL_MULT, 2)
            pnl_est = round((price - tp) / price * get_budget() * S3_RISK_PCT * LEVERAGE, 2)
            emoji = "🔴"
            original = "STRONG BUY"
        else:
            signal_type = "LONG"
            tp = round(price + atr * S3_ATR_TP_MULT, 2)
            sl = round(price - atr * S3_ATR_SL_MULT, 2)
            pnl_est = round((tp - price) / price * get_budget() * S3_RISK_PCT * LEVERAGE, 2)
            emoji = "🟢"
            original = "STRONG SELL"
        margin = round(get_budget() * S3_RISK_PCT, 2)
        sig_id = save_signal_s3(asset, signal_type, price, tp, sl,
                                 round(net_score, 1), margin, round(atr, 4))
        broadcast(
            f"{emoji} *[S3-INV] {signal_type} — {asset}/USD*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📊 Score: `{net_score:+.1f}` (original: {original}) — *INVERTED*\n"
            f"💰 Price: `${price:,.2f}`\n"
            f"🎯 TP: `${tp:,.2f}` ({S3_ATR_TP_MULT}×ATR)\n"
            f"🛡️ SL: `${sl:,.2f}` ({S3_ATR_SL_MULT}×ATR)\n"
            f"📐 ATR: `${atr:,.2f}` | R:R = 1:{S3_ATR_SL_MULT/S3_ATR_TP_MULT:.1f}\n"
            f"💼 Margin: `${margin}` | Est. P&L: `+${pnl_est}`\n"
            f"🔍 Signal S3#{sig_id}\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        results.append({'asset': asset, 'side': signal_type, 'score': net_score,
                         'price': price, 'tp': tp, 'sl': sl, 'sig_id': sig_id})
    return {'sent': len(results) > 0, 'results': results}


def check_open_s3():
    open_sigs = get_open_signals_s3()
    closed = []
    for sig in open_sigs:
        sig_id, asset, signal_type, entry_price, tp, sl, opened_at, score, margin_used = sig
        symbol = S3_SYMBOLS.get(asset, asset + "USD")
        try:
            r = http.get(f"{KRAKEN_API}/Ticker", params={"pair": symbol}, timeout=10)
            pair_key = next(iter(r.json()['result']))
            current_price = float(r.json()['result'][pair_key]['c'][0])
            is_long = signal_type == 'LONG'
            hit = None
            if is_long:
                if current_price >= tp:   hit = 'TP_HIT'
                elif current_price <= sl: hit = 'SL_HIT'
            else:
                if current_price <= tp:   hit = 'TP_HIT'
                elif current_price >= sl: hit = 'SL_HIT'
            if not hit and opened_at:
                try:
                    age_h = (datetime.now() - datetime.fromisoformat(opened_at)).total_seconds() / 3600
                    if age_h >= 168:
                        hit = 'EXPIRED'
                except Exception:
                    pass
            if hit:
                exit_price = current_price if hit == 'EXPIRED' else (tp if hit == 'TP_HIT' else sl)
                exposure = (margin_used or get_budget() * S3_RISK_PCT) * LEVERAGE
                if is_long:
                    pnl_usd = round((exit_price - entry_price) / entry_price * exposure, 2)
                else:
                    pnl_usd = round((entry_price - exit_price) / entry_price * exposure, 2)
                duration = close_signal_s3(sig_id, hit, pnl_usd)
                dur_str = fmt_dur(duration)
                budget_after = get_budget()
                emoji = '✅' if hit == 'TP_HIT' else ('⚠️' if hit == 'EXPIRED' else '❌')
                label = 'TARGET HIT' if hit == 'TP_HIT' else ('EXPIRED' if hit == 'EXPIRED' else 'STOP LOSS HIT')
                pnl_str = f"+${pnl_usd:.2f}" if pnl_usd >= 0 else f"-${abs(pnl_usd):.2f}"
                broadcast(
                    f"{emoji} *[S3-INV] {label} — {asset}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"Side: `{signal_type}` | Exit: `{hit}`\n"
                    f"Entry: `${entry_price:,.2f}` → Exit: `${exit_price:,.2f}`\n"
                    f"P&L: `{pnl_str}`\n"
                    f"⏱ Duration: `{dur_str}`\n"
                    f"💼 Budget: `${budget_after:,.2f}`\n"
                    f"━━━━━━━━━━━━━━━━━"
                )
                closed.append({'id': sig_id, 'asset': asset, 'outcome': hit, 'pnl_usd': pnl_usd})
        except Exception as e:
            logging.error(f"S3 monitor error signal #{sig_id}: {e}")
    return closed


def compute_indicators_s2(df):
    close = df['close']
    high = df['high']
    low = df['low']
    df['rsi'] = ta.momentum.RSIIndicator(close, window=14).rsi()
    stochrsi = ta.momentum.StochRSIIndicator(close, window=14, smooth1=3, smooth2=3)
    df['stoch_rsi_k'] = stochrsi.stochrsi_k() * 100
    adx = ta.trend.ADXIndicator(high, low, close, window=14)
    df['adx'] = adx.adx()
    macd = ta.trend.MACD(close, window_fast=12, window_slow=26, window_sign=9)
    df['macd_hist'] = macd.macd_diff()
    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    df['ema9'] = ta.trend.EMAIndicator(close, window=9).ema_indicator()
    df['ema21'] = ta.trend.EMAIndicator(close, window=21).ema_indicator()
    df['ema50'] = ta.trend.EMAIndicator(close, window=50).ema_indicator()
    df['ema200'] = ta.trend.EMAIndicator(close, window=min(200, len(df) - 1)).ema_indicator()
    return df


def golden_cross_s2(df, lookback=50):
    """EMA50 currently above EMA200 (bullish trend filter)."""
    if len(df) < 2:
        return False
    return float(df['ema50'].iloc[-2]) > float(df['ema200'].iloc[-2])


def death_cross_s2(df, lookback=50):
    """EMA50 currently below EMA200 (bearish trend filter)."""
    if len(df) < 2:
        return False
    return float(df['ema50'].iloc[-2]) < float(df['ema200'].iloc[-2])


def score_long_s2(row_15m, row_4h):
    s = 0.0
    if row_15m['rsi'] <= 35:            s += 2.0
    if row_4h['rsi'] <= 40:             s += 1.5
    if row_15m['stoch_rsi_k'] <= 20:    s += 1.5
    if row_15m['adx'] >= 20:            s += 1.0
    if row_15m['macd_hist'] > 0:        s += 1.0
    if row_15m['close'] <= row_15m['bb_lower'] * 1.01: s += 0.5
    if row_15m['ema9'] > row_15m['ema21']:              s += 0.5
    return s


def score_short_s2(row_15m, row_4h):
    s = 0.0
    if row_15m['rsi'] >= 65:            s += 2.0
    if row_4h['rsi'] >= 60:             s += 1.5
    if row_15m['stoch_rsi_k'] >= 80:    s += 1.5
    if row_15m['adx'] >= 20:            s += 1.0
    if row_15m['macd_hist'] < 0:        s += 1.0
    if row_15m['close'] >= row_15m['bb_upper'] * 0.99: s += 0.5
    if row_15m['ema9'] < row_15m['ema21']:              s += 0.5
    return s


def run_cycle_s2():
    open_s2 = get_open_signals_s2()
    if len(open_s2) >= S2_MAX_TRADES:
        return {'sent': False, 'reason': 'max_trades_reached'}

    df_15m = fetch_crypto('ETHUSD', '15m')
    df_4h  = fetch_crypto('ETHUSD', '4h')
    if df_15m.empty or df_4h.empty:
        return {'sent': False, 'reason': 'no_data'}

    df_15m = compute_indicators_s2(df_15m)
    df_4h  = compute_indicators_s2(df_4h)

    row_15m = df_15m.iloc[-2]
    row_4h  = df_4h.iloc[-2]
    price   = float(df_15m.iloc[-1]['close'])

    eth_price = price
    margin = round(eth_price / LEVERAGE, 2)

    long_score  = score_long_s2(row_15m, row_4h)
    short_score = score_short_s2(row_15m, row_4h)
    golden_ok   = golden_cross_s2(df_15m, S2_CROSS_LOOKBACK)
    death_ok    = death_cross_s2(df_15m, S2_CROSS_LOOKBACK)

    result = {'sent': False, 'long_score': round(long_score, 1), 'short_score': round(short_score, 1),
              'golden_cross': bool(golden_ok), 'death_cross': bool(death_ok)}

    if long_score >= S2_LONG_MIN_SCORE and golden_ok:
        tp = round(price * (1 + S2_LONG_TP_PCT / 100), 2)
        sl = round(price * (1 - S2_LONG_SL_PCT / 100), 2)
        sig_id = save_signal_s2('LONG', price, tp, sl, long_score, margin)
        pnl_est = round((tp - price) / price * margin * LEVERAGE, 2)
        broadcast(
            f"🟢 *[S2] LONG — ETH/USD*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📊 Score: `{long_score:.1f}/8.0` | Golden Cross ✅\n"
            f"💰 Price: `${price:,.2f}`\n"
            f"📈 TP: `${tp:,.2f}` (+{S2_LONG_TP_PCT}%)\n"
            f"🛡️ SL: `${sl:,.2f}` (-{S2_LONG_SL_PCT}%)\n"
            f"💼 Margin: `${margin}` | Est. P&L: `+${pnl_est}`\n"
            f"🔍 Tracking: Signal S2#{sig_id}\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        result.update({'sent': True, 'side': 'LONG', 'price': price, 'tp': tp, 'sl': sl, 'sig_id': sig_id})

    elif short_score >= S2_SHORT_MIN_SCORE and death_ok:
        tp = round(price * (1 - S2_SHORT_TP_PCT / 100), 2)
        sl = round(price * (1 + S2_SHORT_SL_PCT / 100), 2)
        sig_id = save_signal_s2('SHORT', price, tp, sl, short_score, margin)
        pnl_est = round((price - tp) / price * margin * LEVERAGE, 2)
        broadcast(
            f"🔴 *[S2] SHORT — ETH/USD*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📊 Score: `{short_score:.1f}/8.0` | Death Cross ✅\n"
            f"💰 Price: `${price:,.2f}`\n"
            f"📉 TP: `${tp:,.2f}` (-{S2_SHORT_TP_PCT}%)\n"
            f"🛡️ SL: `${sl:,.2f}` (+{S2_SHORT_SL_PCT}%)\n"
            f"💼 Margin: `${margin}` | Est. P&L: `+${pnl_est}`\n"
            f"🔍 Tracking: Signal S2#{sig_id}\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        result.update({'sent': True, 'side': 'SHORT', 'price': price, 'tp': tp, 'sl': sl, 'sig_id': sig_id})

    return result


def check_open_s2():
    open_sigs = get_open_signals_s2()
    closed = []
    for sig in open_sigs:
        sig_id, asset, signal_type, entry_price, tp, sl, opened_at, score, margin_used = sig
        try:
            r = http.get(f"{KRAKEN_API}/Ticker", params={"pair": "ETHUSD"}, timeout=10)
            pair_key = next(iter(r.json()['result']))
            current_price = float(r.json()['result'][pair_key]['c'][0])

            is_long = signal_type == 'LONG'
            hit = None
            if is_long:
                if current_price >= tp:   hit = 'TP_HIT'
                elif current_price <= sl: hit = 'SL_HIT'
            else:
                if current_price <= tp:   hit = 'TP_HIT'
                elif current_price >= sl: hit = 'SL_HIT'

            if hit:
                exit_price = tp if hit == 'TP_HIT' else sl
                eth_pos = (margin_used or entry_price / LEVERAGE) * LEVERAGE
                if is_long:
                    pnl_usd = round((exit_price - entry_price) / entry_price * eth_pos, 2)
                else:
                    pnl_usd = round((entry_price - exit_price) / entry_price * eth_pos, 2)
                duration = close_signal_s2(sig_id, hit, pnl_usd)
                dur_str = fmt_dur(duration)
                budget_after = get_budget()
                emoji = '✅' if hit == 'TP_HIT' else '❌'
                label = 'TARGET HIT' if hit == 'TP_HIT' else 'STOP LOSS HIT'
                pnl_str = f"+${pnl_usd:.2f}" if pnl_usd >= 0 else f"-${abs(pnl_usd):.2f}"
                broadcast(
                    f"{emoji} *[S2] {label} — ETH*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"Side: `{signal_type}`\n"
                    f"Entry: `${entry_price:,.2f}` | Exit: `${exit_price:,.2f}`\n"
                    f"P&L: `{pnl_str}`\n"
                    f"⏱ Duration: `{dur_str}`\n"
                    f"💼 Budget: `${budget_after:,.2f}`\n"
                    f"━━━━━━━━━━━━━━━━━"
                )
                closed.append({'id': sig_id, 'outcome': hit, 'pnl_usd': pnl_usd})
        except Exception as e:
            logging.error(f"S2 monitor error signal #{sig_id}: {e}")
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


@app.route('/chart')
def equity_chart():
    path = os.path.join(os.path.dirname(__file__), '..', 'chart_equity.html')
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


@app.route('/api/portfolio')
def api_portfolio():
    return jsonify({'budget': get_budget(), 'starting_budget': STARTING_BUDGET,
                    'leverage': LEVERAGE, 'position_eth': POSITION_ETH})


@app.route('/api/open')
def api_open():
    rows = get_open_signals()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'opened_at', 'details', 'liq_price', 'margin_used']
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        if d['details']:
            try:
                d['details'] = json.loads(d['details'])
            except Exception:
                d['details'] = None
        result.append(d)
    return jsonify(result)


@app.route('/api/signals')
def api_signals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, net_score, "
        "opened_at, closed_at, outcome, duration_hours, details "
        "FROM signals ORDER BY opened_at DESC LIMIT 100"
    )
    rows = cur.fetchall()
    conn.close()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'net_score',
            'opened_at', 'closed_at', 'outcome', 'duration_hours', 'details']
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        if d['details']:
            try:
                d['details'] = json.loads(d['details'])
            except Exception:
                d['details'] = None
        result.append(d)
    return jsonify(result)


@app.route('/api/setup')
def setup_webhook():
    """Visit this once after deploy to register Telegram webhook."""
    base = request.url_root.rstrip('/')
    webhook_url = f"{base}/api/webhook"
    resp = http.post(f"{TELEGRAM_API}/setWebhook", json={'url': webhook_url})
    return jsonify({'webhook_url': webhook_url, 'telegram_response': resp.json()})


@app.route('/api/fix-outcome/<int:sig_id>', methods=['POST'])
def fix_outcome(sig_id):
    try:
        outcome = request.json.get('outcome')
        conn = get_conn()
        cur = conn.cursor()
        p = ph()
        cur.execute(f"UPDATE signals SET outcome={p} WHERE id={p}", (outcome, sig_id))
        conn.commit()
        conn.close()
        return jsonify({'updated': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/close-signal/<int:sig_id>', methods=['POST'])
def manual_close(sig_id):
    try:
        outcome_type = (request.json or {}).get('outcome_type', 'MANUAL')
        conn = get_conn()
        cur = conn.cursor()
        p = ph()
        cur.execute(f"SELECT asset, entry_price, signal_type, tp, sl, margin_used FROM signals WHERE id={p} AND outcome IS NULL", (sig_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({'error': 'Signal not found or already closed'}), 404
        asset, entry_price, signal_type, tp, sl, margin_used = row
        is_buy = 'BUY' in signal_type
        if outcome_type == 'TP':
            outcome = 'TP_HIT'
            exit_price = tp
        elif outcome_type == 'SL':
            outcome = 'SL_HIT'
            exit_price = sl
        else:
            outcome = 'CLOSED_MANUAL'
            symbol = SYMBOLS.get(asset, 'ETHUSD')
            r = http.get(f"{KRAKEN_API}/Ticker", params={"pair": symbol}, timeout=10)
            pair_key = next(iter(r.json()['result']))
            exit_price = float(r.json()['result'][pair_key]['c'][0])
        pnl_usd = round((exit_price - entry_price) if is_buy else (entry_price - exit_price), 2)
        duration = close_signal(sig_id, outcome, pnl_usd)
        budget_after = get_budget()
        labels = {'TP_HIT': '✅ TP HIT', 'SL_HIT': '❌ SL HIT', 'CLOSED_MANUAL': '🛑 MANUAL CLOSE'}
        pnl_str = f"+${pnl_usd:.2f}" if pnl_usd >= 0 else f"-${abs(pnl_usd):.2f}"
        broadcast(
            f"{labels[outcome]} — {asset}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Entry: `${entry_price:,.2f}` | Exit: `${exit_price:,.2f}`\n"
            f"P&L: `{pnl_str}`\n"
            f"💼 Budget: `${budget_after:,.2f}`\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        return jsonify({'closed': True, 'outcome': outcome, 'pnl_usd': pnl_usd, 'budget': budget_after})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scanner2')
def scanner2():
    if not check_secret():
        return jsonify({'error': 'Unauthorized'}), 401
    result = run_cycle_s2()
    return jsonify({'status': 'ok', 'result': result})


@app.route('/api/monitor2')
def monitor2():
    if not check_secret():
        return jsonify({'error': 'Unauthorized'}), 401
    closed = check_open_s2()
    return jsonify({'status': 'ok', 'closed': closed})


@app.route('/api/strat2/open')
def api_s2_open():
    rows = get_open_signals_s2()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'opened_at', 'score', 'margin_used']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/strat2/stats')
def api_s2_stats():
    return jsonify(get_stats_s2())


@app.route('/api/strat2/signals')
def api_s2_signals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, score, "
        "opened_at, closed_at, outcome, duration_hours, pnl_usd "
        "FROM signals_s2 ORDER BY opened_at DESC LIMIT 100"
    )
    rows = cur.fetchall()
    conn.close()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'score',
            'opened_at', 'closed_at', 'outcome', 'duration_hours', 'pnl_usd']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/scanner3')
def scanner3():
    if not check_secret():
        return jsonify({'error': 'Unauthorized'}), 401
    result = run_cycle_s3()
    return jsonify({'status': 'ok', 'result': result})


@app.route('/api/monitor3')
def monitor3():
    if not check_secret():
        return jsonify({'error': 'Unauthorized'}), 401
    closed = check_open_s3()
    return jsonify({'status': 'ok', 'closed': closed})


@app.route('/api/strat3/open')
def api_s3_open():
    rows = get_open_signals_s3()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'opened_at', 'score', 'margin_used']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/strat3/stats')
def api_s3_stats():
    return jsonify(get_stats_s3())


@app.route('/api/strat3/signals')
def api_s3_signals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, asset, signal_type, entry_price, tp, sl, score, atr_used, "
        "opened_at, closed_at, outcome, duration_hours, pnl_usd "
        "FROM signals_s3 ORDER BY opened_at DESC LIMIT 100"
    )
    rows = cur.fetchall()
    conn.close()
    cols = ['id', 'asset', 'signal_type', 'entry_price', 'tp', 'sl', 'score', 'atr_used',
            'opened_at', 'closed_at', 'outcome', 'duration_hours', 'pnl_usd']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/close-signal-s2/<int:sig_id>', methods=['POST'])
def manual_close_s2(sig_id):
    try:
        outcome_type = (request.json or {}).get('outcome_type', 'MANUAL')
        conn = get_conn()
        cur = conn.cursor()
        p = ph()
        cur.execute(f"SELECT entry_price, signal_type, tp, sl, margin_used FROM signals_s2 WHERE id={p} AND outcome IS NULL", (sig_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({'error': 'Signal not found or already closed'}), 404
        entry_price, signal_type, tp, sl, margin_used = row
        is_long = signal_type == 'LONG'
        if outcome_type == 'TP':
            outcome, exit_price = 'TP_HIT', tp
        elif outcome_type == 'SL':
            outcome, exit_price = 'SL_HIT', sl
        else:
            outcome = 'CLOSED_MANUAL'
            r = http.get(f"{KRAKEN_API}/Ticker", params={"pair": "ETHUSD"}, timeout=10)
            pair_key = next(iter(r.json()['result']))
            exit_price = float(r.json()['result'][pair_key]['c'][0])
        eth_pos = (margin_used or entry_price / LEVERAGE) * LEVERAGE
        pnl_usd = round((exit_price - entry_price) / entry_price * eth_pos if is_long
                        else (entry_price - exit_price) / entry_price * eth_pos, 2)
        duration = close_signal_s2(sig_id, outcome, pnl_usd)
        budget_after = get_budget()
        labels = {'TP_HIT': '✅ TP HIT', 'SL_HIT': '❌ SL HIT', 'CLOSED_MANUAL': '🛑 MANUAL CLOSE'}
        pnl_str = f"+${pnl_usd:.2f}" if pnl_usd >= 0 else f"-${abs(pnl_usd):.2f}"
        broadcast(
            f"{labels[outcome]} *[S2] — ETH*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Side: `{signal_type}` | Entry: `${entry_price:,.2f}` | Exit: `${exit_price:,.2f}`\n"
            f"P&L: `{pnl_str}` | 💼 Budget: `${budget_after:,.2f}`\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        return jsonify({'closed': True, 'outcome': outcome, 'pnl_usd': pnl_usd, 'budget': budget_after})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.utcnow().isoformat()})


@app.route('/api/debug')
def debug():
    try:
        df = fetch_crypto("XBTUSD", "1h")
        return jsonify({'rows': len(df), 'last_close': float(df['close'].iloc[-1]) if not df.empty else None})
    except Exception as e:
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True, port=5001)
