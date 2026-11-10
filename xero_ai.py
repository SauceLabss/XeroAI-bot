"""
XERO AI SOFTWARE — Single Timeframe Signal Bot
=====================================================
Dashboard : http://localhost:8050

Candle construction: BUILT FROM RAW TICKS (no OHLC stream)
────────────────────────────────────────────────────────────
  HTF candle = every 5 ticks

Strategy  : SINGLE TIMEFRAME — SIGNALS via TELEGRAM ALERTS (this script never opens trades)
──────────────────────────────────────────────────────────────
HTF — FIRE:
  HTF completes excursion + curve + re-entry (BUY or SELL)
  A signal is sent to Telegram immediately when the FIRE condition triggers.

FIRE condition:
  LONG BUY   : MAP exits below BB Lower → forms trough → curves up → crosses back above BB Lower
         AND price > External BB Upper
  SHORT SELL : MAP exits above BB Upper → forms peak  → curves dn → crosses back below BB Upper
         AND price < External BB Lower

Requirements:
    pip install websocket-client requests google-auth
"""

import os
import sys
import signal
import websocket
import json
import time
import threading
import math
import requests
from collections import deque
from datetime import datetime, timedelta


# Cloud sync to Firestore (feeds the website). Optional: the bot runs fine without it.
try:
    import firebase_sync
except Exception as _fb_err:
    firebase_sync = None
    print(f"[firebase] firebase_sync not loaded: {_fb_err}")

# ============================================================
# HELPERS
# ============================================================

def _run_label():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return f"GitHub Actions (run #{os.environ.get('GITHUB_RUN_NUMBER', '?')})"
    return "local machine"

def get_ts():
    return datetime.now().strftime("%H:%M:%S")

def get_dt():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Internal direction codes stay 'BUY' / 'SELL'; this is what the user sees.
SIGNAL_LABEL = {'BUY': 'LONG BUY', 'SELL': 'SHORT SELL'}
SIGNAL_ICON  = {'BUY': '▲',        'SELL': '▼'}

def signal_text(direction):
    return f"{SIGNAL_ICON[direction]} {SIGNAL_LABEL[direction]}"

# ============================================================
# CONFIGURATION
# ============================================================

API_TOKEN  = os.environ.get("DERIV_PAT", "")
APP_ID     = os.environ.get("DERIV_APP_ID", "")
REST_BASE  = "https://api.derivws.com"

# ---- Telegram ----
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
# True  = Telegram ONLY tells you the bot has started. Signals still go to the website (Firebase) and the log.
# False = Telegram also sends every signal, reconnect and error message, as before.
TELEGRAM_ONLY_START = True

# ────────────────────────────────────────────────────────────
# TICK-BASED CANDLE SIZE
# HTF_TICKS : number of ticks that form one HTF candle
# ────────────────────────────────────────────────────────────
HTF_TICKS = 5    # 1 HTF candle = 5 raw ticks

# ---- Warm-up prefill ----
# On a fresh start, fetch recent historical ticks so indicators are ready in seconds
# instead of waiting ~10-20 minutes for live ticks. If Deriv refuses the request the
# bot simply warms up live, exactly as before.
PREFILL_ENABLED = os.environ.get("PREFILL", "true").strip().lower() in ("1", "true", "yes")
PREFILL_TICKS   = int(os.environ.get("PREFILL_TICKS", "800"))
REPLAY          = False   # True only while history is being replayed: no alerts, no stats
pending_history = set()

# ---- MAP parameters (MQL4 DCE) ----
PERIOD1       = 34
PERIOD2       = 22
PERIOD3       = 18
PERIOD4       = 33
PERIOD5       = 29
PERIOD6       = 14
SIGNAL_PERIOD = 5

# ---- Internal Bollinger Bands on MAP ----
BB_PERIOD    = 89
BB_DEVIATION = 1.0

# ---- External Bollinger Bands on close price ----
EXT_BB_PERIOD    = 60
EXT_BB_DEVIATION = 0.600

# ---- Minimum excursion depth ----
MIN_EXCURSION_DEPTH_FRAC = 0.05

MIN_CANDLES       = max(PERIOD1, PERIOD2, PERIOD3, PERIOD4,
                        PERIOD5, PERIOD6, BB_PERIOD, EXT_BB_PERIOD) + 30
PRICE_WINDOW_SIZE = MIN_CANDLES + 100
MAP_WINDOW_SIZE   = BB_PERIOD + SIGNAL_PERIOD + 50

# ============================================================
# SYMBOL LIST
# ============================================================

TRADING_SYMBOLS = [
    {"symbol": "R_10",    "name": "Volatility 10 Index"},
    {"symbol": "R_25",    "name": "Volatility 25 Index"},
    {"symbol": "R_50",    "name": "Volatility 50 Index"},
    {"symbol": "R_75",    "name": "Volatility 75 Index"},
    {"symbol": "R_100",   "name": "Volatility 100 Index"},
    {"symbol": "1HZ10V",  "name": "Volatility 10 (1s) Index"},
    {"symbol": "1HZ15V",  "name": "Volatility 15 (1s) Index"},
    {"symbol": "1HZ25V",  "name": "Volatility 25 (1s) Index"},
    {"symbol": "1HZ30V",  "name": "Volatility 30 (1s) Index"},
    {"symbol": "1HZ50V",  "name": "Volatility 50 (1s) Index"},
    {"symbol": "1HZ75V",  "name": "Volatility 75 (1s) Index"},
    {"symbol": "1HZ90V",  "name": "Volatility 90 (1s) Index"},
    {"symbol": "1HZ100V", "name": "Volatility 100 (1s) Index"},
    {"symbol": "JD10",    "name": "Jump 10 Index"},
    {"symbol": "JD25",    "name": "Jump 25 Index"},
    {"symbol": "JD50",    "name": "Jump 50 Index"},
    {"symbol": "JD75",    "name": "Jump 75 Index"},
    {"symbol": "JD100",   "name": "Jump 100 Index"},
    {"symbol": "stpRNG",  "name": "Step Range Index"},
    {"symbol": "stpRNG2", "name": "Step Range Index 2"},
    {"symbol": "stpRNG3", "name": "Step Range Index 3"},
    {"symbol": "stpRNG4", "name": "Step Range Index 4"},
    {"symbol": "stpRNG5", "name": "Step Range Index 5"},
    {"symbol": "CRASH500",  "name": "Crash 500 Index"},
    {"symbol": "frxEURUSD", "name": "EUR/USD"},
    {"symbol": "frxGBPUSD", "name": "GBP/USD"},
    {"symbol": "frxUSDJPY", "name": "USD/JPY"},
    {"symbol": "frxUSDCAD", "name": "USD/CAD"},
]

# ============================================================
# RUNTIME STATE
# ============================================================

symbol_data        = {}
subscribed_symbols = set()
ws                 = None
running            = True
data_lock          = threading.Lock()

dashboard_data = {
    'htf_candles': {},
    'alerts':      [],
    # Every FIRE and REJECT decision the strategy makes, newest first.
    # Powers "AI Decision Breakdown" and "AI Confidence Trend".
    'decisions':   [],
    # Every signal sent, newest first.
    # Powers "Recent Signals" and "Signals by Market".
    'signals':     [],
}

# ============================================================
# TICK BUFFER — builds candles from raw ticks
# ============================================================
# Each symbol has one tick buffer:
#   tick_buf_htf[symbol] — accumulates ticks toward next HTF candle
#
# When the buffer reaches HTF_TICKS a closed OHLC candle is
# emitted to on_candle_closed(), then the buffer is cleared.

tick_buf_htf = {}   # symbol -> list of float prices (partial HTF candle)
forming_htf  = {}   # symbol -> forming OHLC dict (dashboard)


def _buf_to_ohlc(prices, epoch):
    """Convert a list of tick prices into an OHLC dict."""
    return {
        'open':  prices[0],
        'high':  max(prices),
        'low':   min(prices),
        'close': prices[-1],
        'epoch': epoch,
    }


def _update_forming(symbol):
    """Rebuild the dashboard forming-candle entry from the current buffer."""
    now_dt  = datetime.now()
    htf_buf = tick_buf_htf.get(symbol, [])
    if htf_buf:
        forming_htf[symbol] = {
            'time':   now_dt,
            'open':   htf_buf[0],
            'high':   max(htf_buf),
            'low':    min(htf_buf),
            'close':  htf_buf[-1],
            'map':    None, 'signal': None, 'bb_up': None, 'bb_dn': None,
            'ticks':  len(htf_buf),
        }


def on_tick(symbol, price, epoch):
    """
    Called for every incoming raw tick.
    Feeds the HTF buffer. When full, a closed candle is emitted and buffer resets.
    """
    price = float(price)

    if firebase_sync:                       # feeds the website's market cards
        firebase_sync.push_tick(symbol, price, epoch)

    if symbol not in tick_buf_htf:
        tick_buf_htf[symbol] = []
    tick_buf_htf[symbol].append(price)

    if len(tick_buf_htf[symbol]) >= HTF_TICKS:
        candle = _buf_to_ohlc(tick_buf_htf[symbol], epoch)
        tick_buf_htf[symbol] = []
        forming_htf.pop(symbol, None)
        on_candle_closed(symbol, candle)

    _update_forming(symbol)

# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message: str, important: bool = False):
    if TELEGRAM_ONLY_START and not important:
        return                      # silenced: only the "STARTED" message (important=True) goes out
    def _send():
        try:
            url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                print(f"[{get_ts()}] Telegram sent ✓")
            else:
                print(f"[{get_ts()}] Telegram error: {r.text}")
        except Exception as e:
            print(f"[{get_ts()}] Telegram exception: {e}")
    threading.Thread(target=_send, daemon=True).start()


# ============================================================
# SIGNAL ALERTS
# ============================================================

def alert_htf_fired(symbol, name, direction):
    label = signal_text(direction)
    send_telegram(
        f"🔔 <b>XERO SIGNAL</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"Pair: <b>{name}</b>\nSymbol: {symbol}\nSignal: <b>{label}</b>\n"
        f"Timeframe: HTF {HTF_TICKS}-tick\nTime: {get_dt()}\n━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Excursion + curve + re-entry + ExtBB confirmed</i>")
    _log_alert(symbol, "🔔 SIGNAL", direction, f"HTF {SIGNAL_LABEL[direction]} signal sent")


def _log_alert(symbol, alert_type, direction, message):
    dashboard_data['alerts'].insert(0, {
        'time': get_dt(), 'symbol': symbol,
        'type': alert_type, 'direction': direction, 'message': message,
    })
    if len(dashboard_data['alerts']) > 100:
        dashboard_data['alerts'].pop()


def calc_confidence(depth_cleared, min_depth, excursion_candles, price_clearance, band_width):
    """
    Heuristic 0-100 confidence score for a FIRE signal, built entirely from
    values the strategy already computes — NOT a machine-learning output,
    just a transparent composite of three signal-strength factors:

      - depth:     how far the excursion exceeded the minimum required
                   depth before curving back (deeper = more conviction)
      - candles:   how many candles the excursion sustained before
                   re-entry (more confirmation, up to a point)
      - clearance: how far price cleared the external Bollinger Band
                   relative to the band's own width (further = stronger)

    Each factor is capped and weighted, then combined. This is one
    reasonable way to grade signal strength — not the only way — and can
    be re-tuned later without touching anything else that depends on it.
    """
    depth_ratio = min(depth_cleared / min_depth, 2.0) / 2.0 if min_depth > 0 else 0.5
    candle_score = min(excursion_candles / 5.0, 1.0)
    clearance_ratio = min(price_clearance / band_width, 1.0) if band_width > 0 else 0.5

    confidence = (depth_ratio * 0.4 + candle_score * 0.3 + clearance_ratio * 0.3) * 100
    return round(max(5.0, min(99.0, confidence)), 1)


def _log_decision(symbol, direction, decision, confidence, detail):
    """decision is 'FIRE' or 'REJECT'. confidence is None for rejects."""
    if REPLAY:
        return
    dashboard_data['decisions'].insert(0, {
        'time': get_dt(), 'ts': datetime.now(),
        'symbol': symbol, 'direction': direction,
        'decision': decision, 'confidence': confidence, 'detail': detail,
    })
    if len(dashboard_data['decisions']) > 200:
        dashboard_data['decisions'].pop()
    if firebase_sync:
        firebase_sync.record_decision(direction, decision, confidence)


def _log_signal(symbol, direction, confidence):
    dashboard_data['signals'].insert(0, {
        'time': get_dt(), 'ts': datetime.now(),
        'symbol': symbol, 'direction': direction, 'confidence': confidence,
    })
    if len(dashboard_data['signals']) > 200:
        dashboard_data['signals'].pop()
    if firebase_sync:
        firebase_sync.push_signal(symbol, get_symbol_name(symbol), direction, confidence,
                                  f"{HTF_TICKS}-tick")

# ============================================================
# SYMBOL STATE
# ============================================================

def get_symbol_name(symbol):
    for s in TRADING_SYMBOLS:
        if s['symbol'] == symbol:
            return s['name']
    return symbol


def _make_tf_state():
    return {
        'candle_count': 0,
        'close_prices': deque(maxlen=PRICE_WINDOW_SIZE),
        'map_window':   deque(maxlen=MAP_WINDOW_SIZE),

        'prev_map':   None,
        'prev_bb_up': None,
        'prev_bb_dn': None,
        'prev_sig':   None,
        'prev_close': None,

        'warming_up': True,

        'last_map':        None,
        'last_sig':        None,
        'last_bb_up':      None,
        'last_bb_dn':      None,
        'last_ext_bb_up':  None,
        'last_ext_bb_dn':  None,

        'excursion_state':   None,
        'excursion_candles': 0,
        'excursion_extreme': None,
        'excursion_curved':  False,
    }


def _make_symbol_state():
    return {
        'htf': _make_tf_state(),
        'signal_count':   0,
        'buy_count':      0,
        'sell_count':     0,
        'last_signal':    None,   # 'BUY' / 'SELL'
        'last_signal_at': None,
    }


def init_symbol(symbol):
    if symbol not in symbol_data:
        symbol_data[symbol] = _make_symbol_state()
        dashboard_data['htf_candles'][symbol] = deque(maxlen=300)
        tick_buf_htf[symbol] = []
        print(f"[{get_ts()}] Initialised [{symbol}]")

# ============================================================
# INDICATOR MATH
# ============================================================

def ic_tma(prices_list, period):
    n = len(prices_list)
    if n < period:
        return None
    sumw  = period + 1
    total = sumw * prices_list[-1]
    for j in range(1, period):
        k   = period - j
        idx = n - 1 - j
        sumw  += k
        total += k * (prices_list[idx] if idx >= 0 else prices_list[0])
    return total / sumw


def compute_map(prices_list):
    a1 = ic_tma(prices_list, PERIOD2)
    a2 = ic_tma(prices_list, PERIOD1)
    a3 = ic_tma(prices_list, PERIOD4)
    a4 = ic_tma(prices_list, PERIOD3)
    a5 = ic_tma(prices_list, PERIOD6)
    a6 = ic_tma(prices_list, PERIOD5)
    if None in (a1, a2, a3, a4, a5, a6):
        return None
    return (a1 - a2) + (a3 - a4) + (a5 - a6)


def calc_sma(dq, period):
    lst = list(dq)
    if len(lst) < period:
        return None
    return sum(lst[-period:]) / period


def calc_bb(values_list, period, deviation):
    lst = list(values_list)
    if len(lst) < period:
        return None, None
    seg      = lst[-period:]
    mean     = sum(seg) / period
    variance = sum((x - mean) ** 2 for x in seg) / period
    std      = math.sqrt(variance)
    return mean + deviation * std, mean - deviation * std

# ============================================================
# CORE FIRE CONDITION (excursion + curve + re-entry + ExtBB filter)
# ============================================================

def _process_candle(tf, candle, symbol, dashboard_key, ts):
    """
    Process one closed tick-based candle.
    Returns 'BUY', 'SELL', or None.
    """
    close_price = float(candle['close'])

    tf['close_prices'].append(close_price)

    close_list = list(tf['close_prices'])

    map_val = compute_map(close_list) if len(close_list) >= PERIOD1 else None

    if map_val is not None:
        tf['map_window'].append(map_val)
        mlen    = len(tf['map_window'])
        sig_val = calc_sma(tf['map_window'], SIGNAL_PERIOD) if mlen >= SIGNAL_PERIOD else None
        bb_up, bb_dn = (
            calc_bb(tf['map_window'], BB_PERIOD, BB_DEVIATION)
            if mlen >= BB_PERIOD else (None, None)
        )
    else:
        sig_val = bb_up = bb_dn = None

    # ── External BB (EXT_BB_PERIOD, EXT_BB_DEVIATION) on close prices ──────────────
    ext_bb_up, ext_bb_dn = (
        calc_bb(tf['close_prices'], EXT_BB_PERIOD, EXT_BB_DEVIATION)
        if len(close_list) >= EXT_BB_PERIOD else (None, None)
    )

    tf['last_map']       = map_val
    tf['last_sig']       = sig_val
    tf['last_bb_up']     = bb_up
    tf['last_bb_dn']     = bb_dn
    tf['last_ext_bb_up'] = ext_bb_up
    tf['last_ext_bb_dn'] = ext_bb_dn

    if map_val is not None and tf['candle_count'] % 10 == 0:
        ext_str = (f"ExtBB↑={ext_bb_up:.5f}  ExtBB↓={ext_bb_dn:.5f}"
                   if ext_bb_up is not None else "ExtBB=warm")
        if bb_up is not None:
            print(f"   [{symbol}] HTF MAP={map_val:.4f}  "
                  f"BB↑={bb_up:.4f}  BB↓={bb_dn:.4f}  {ext_str}")
        else:
            print(f"   [{symbol}] HTF MAP={map_val:.4f}  BB=warming")

    all_ready = None not in (map_val, sig_val, bb_up, bb_dn)
    if all_ready and tf['warming_up']:
        tf['warming_up'] = False
        print(f"[{get_ts()}] [{symbol}] HTF READY — {tf['candle_count']} candles")
    elif tf['candle_count'] % 20 == 0 and tf['warming_up']:
        print(f"[{get_ts()}] [{symbol}] HTF warming  "
              f"candles:{tf['candle_count']}/{MIN_CANDLES}")

    # Store in dashboard
    dashboard_data[dashboard_key][symbol].append({
        'time':       ts,
        'open':       float(candle['open']),
        'high':       float(candle['high']),
        'low':        float(candle['low']),
        'close':      float(candle['close']),
        'map':        map_val,
        'signal':     sig_val,
        'bb_up':      bb_up,
        'bb_dn':      bb_dn,
        'ext_bb_up':  ext_bb_up,   # External BB on price (row 1)
        'ext_bb_dn':  ext_bb_dn,
    })

    prev_map   = tf['prev_map']
    prev_bb_up = tf['prev_bb_up']
    prev_bb_dn = tf['prev_bb_dn']

    if map_val is not None:
        tf['prev_map']   = map_val
        tf['prev_bb_up'] = bb_up
        tf['prev_bb_dn'] = bb_dn
    if sig_val is not None:
        tf['prev_sig'] = sig_val
    tf['prev_close'] = close_price

    if tf['warming_up']:
        return None
    if None in (map_val, bb_up, bb_dn, prev_map, prev_bb_up, prev_bb_dn):
        return None

    fire_result = None
    state       = tf['excursion_state']

    if state is None:
        # Start a BUY excursion when MAP drops below the lower band
        if map_val < bb_dn:
            tf['excursion_state']   = 'below'
            tf['excursion_candles'] = 1
            tf['excursion_extreme'] = map_val
            tf['excursion_curved']  = False
        # Start a SELL excursion when MAP rises above the upper band
        elif map_val > bb_up:
            tf['excursion_state']   = 'above'
            tf['excursion_candles'] = 1
            tf['excursion_extreme'] = map_val
            tf['excursion_curved']  = False

    elif state == 'below':
        band_width = (bb_up - bb_dn) if (bb_up and bb_dn) else 0
        min_depth  = band_width * MIN_EXCURSION_DEPTH_FRAC

        if map_val < bb_dn:
            tf['excursion_candles'] += 1
            if map_val < tf['excursion_extreme']:
                tf['excursion_extreme'] = map_val
            elif map_val > prev_map:
                depth_cleared = bb_dn - tf['excursion_extreme']
                if depth_cleared >= min_depth:
                    tf['excursion_curved'] = True
        else:
            curved = tf['excursion_curved']
            rising = map_val > prev_map
            if curved and rising:
                # ── External filter ───────────────────────────────────
                price_above_ext_bb = (ext_bb_up is not None and close_price > ext_bb_up)
                if price_above_ext_bb:
                    print(f"   [{symbol}] HTF ▲ LONG BUY FIRE  "
                          f"MAP={map_val:.4f}  BB↓={bb_dn:.4f}  "
                          f"exc={tf['excursion_candles']}c  trough={tf['excursion_extreme']:.4f}  "
                          f"Price={close_price:.5f}>ExtBB↑={ext_bb_up:.5f}")
                    fire_result = 'BUY'
                    depth_cleared = bb_dn - tf['excursion_extreme']
                    confidence = calc_confidence(
                        depth_cleared, min_depth, tf['excursion_candles'],
                        close_price - ext_bb_up, band_width)
                    _log_decision(symbol, 'BUY', 'FIRE', confidence,
                                   f"Excursion curved after {tf['excursion_candles']} candles, "
                                   f"price cleared ExtBB↑ by {close_price - ext_bb_up:.5f}")
                else:
                    ep = f"{ext_bb_up:.5f}" if ext_bb_up is not None else "N/A"
                    print(f"   [{symbol}] HTF ▲ LONG BUY re-entry — excursion OK but FILTER BLOCKED: "
                          f"price {close_price:.5f} NOT above ExtBB↑ {ep}")
                    _log_decision(symbol, 'BUY', 'REJECT', None,
                                   f"Excursion curved but price {close_price:.5f} did not clear ExtBB↑ {ep}")
            else:
                reasons = []
                if not curved: reasons.append("no curve (too shallow or still falling)")
                if not rising: reasons.append("not rising on re-entry")
                print(f"   [{symbol}] HTF ▲ LONG BUY re-entry REJECTED — {', '.join(reasons)}")
                _log_decision(symbol, 'BUY', 'REJECT', None, ', '.join(reasons))
            tf['excursion_state']   = None
            tf['excursion_candles'] = 0
            tf['excursion_extreme'] = None
            tf['excursion_curved']  = False

    elif state == 'above':
        band_width = (bb_up - bb_dn) if (bb_up and bb_dn) else 0
        min_depth  = band_width * MIN_EXCURSION_DEPTH_FRAC

        if map_val > bb_up:
            tf['excursion_candles'] += 1
            if map_val > tf['excursion_extreme']:
                tf['excursion_extreme'] = map_val
            elif map_val < prev_map:
                depth_cleared = tf['excursion_extreme'] - bb_up
                if depth_cleared >= min_depth:
                    tf['excursion_curved'] = True
        else:
            curved  = tf['excursion_curved']
            falling = map_val < prev_map
            if curved and falling:
                # ── External filter ───────────────────────────────────
                price_below_ext_bb = (ext_bb_dn is not None and close_price < ext_bb_dn)
                if price_below_ext_bb:
                    print(f"   [{symbol}] HTF ▼ SHORT SELL FIRE  "
                          f"MAP={map_val:.4f}  BB↑={bb_up:.4f}  "
                          f"exc={tf['excursion_candles']}c  peak={tf['excursion_extreme']:.4f}  "
                          f"Price={close_price:.5f}<ExtBB↓={ext_bb_dn:.5f}")
                    fire_result = 'SELL'
                    depth_cleared = tf['excursion_extreme'] - bb_up
                    confidence = calc_confidence(
                        depth_cleared, min_depth, tf['excursion_candles'],
                        ext_bb_dn - close_price, band_width)
                    _log_decision(symbol, 'SELL', 'FIRE', confidence,
                                   f"Excursion curved after {tf['excursion_candles']} candles, "
                                   f"price cleared ExtBB↓ by {ext_bb_dn - close_price:.5f}")
                else:
                    ep = f"{ext_bb_dn:.5f}" if ext_bb_dn is not None else "N/A"
                    print(f"   [{symbol}] HTF ▼ SHORT SELL re-entry — excursion OK but FILTER BLOCKED: "
                          f"price {close_price:.5f} NOT below ExtBB↓ {ep}")
                    _log_decision(symbol, 'SELL', 'REJECT', None,
                                   f"Excursion curved but price {close_price:.5f} did not clear ExtBB↓ {ep}")
            else:
                reasons = []
                if not curved:  reasons.append("no curve (too shallow or still rising)")
                if not falling: reasons.append("not falling on re-entry")
                print(f"   [{symbol}] HTF ▼ SHORT SELL re-entry REJECTED — {', '.join(reasons)}")
                _log_decision(symbol, 'SELL', 'REJECT', None, ', '.join(reasons))
            tf['excursion_state']   = None
            tf['excursion_candles'] = 0
            tf['excursion_extreme'] = None
            tf['excursion_curved']  = False

    return fire_result

# ============================================================
# CANDLE CLOSE HANDLER
# ============================================================

def on_candle_closed(symbol, candle):
    """
    Called when a complete HTF_TICKS-tick HTF candle has been assembled.
    Checks for a new 4-step FIRE signal and, if one fires, sends it to Telegram.
    This script never opens or manages trades.
    """
    if symbol not in symbol_data:
        return

    sd   = symbol_data[symbol]
    name = get_symbol_name(symbol)
    ts   = datetime.now()

    tf = sd['htf']
    tf['candle_count'] += 1

    sig = _process_candle(tf, candle, symbol, 'htf_candles', ts)

    if sig and not tf['warming_up'] and not REPLAY:
        signal_num = sd.get('signal_count', 0) + 1
        label      = SIGNAL_LABEL[sig]
        print(f"\n[{get_ts()}] [{symbol}] ✅ HTF {label} — SIGNAL #{signal_num} — sending alert...")

        # _process_candle just logged this FIRE decision; reuse its confidence score.
        last_dec = dashboard_data['decisions'][0] if dashboard_data['decisions'] else None
        conf = (last_dec['confidence']
                if last_dec and last_dec['decision'] == 'FIRE' and last_dec['symbol'] == symbol
                else None)

        alert_htf_fired(symbol, name, sig)
        _log_signal(symbol, sig, conf)

        sd['signal_count'] = signal_num
        sd['buy_count' if sig == 'BUY' else 'sell_count'] += 1
        sd['last_signal']    = sig
        sd['last_signal_at'] = get_dt()

# ============================================================
# WEBSOCKET MESSAGE HANDLER
# ============================================================

def on_message(ws_app, message):
    try:
        data     = json.loads(message)
        msg_type = data.get("msg_type")
    except Exception:
        return

    # ── Authorization — not sent explicitly (OTP already authenticates),
    # kept only as a safety net in case Deriv ever sends one unsolicited ──
    if msg_type == "authorize":
        if "error" in data:
            err = data['error'].get('message', 'unknown error')
            print(f"[{get_ts()}] ❌ AUTHORIZE FAILED: {err}")
            print(f"[{get_ts()}] Check that API_TOKEN and APP_ID belong to the "
                  f"same Deriv account.")
            send_telegram(
                f"❌ <b>AUTHORIZE FAILED</b>\n"
                f"{err}\n"
                f"Check API_TOKEN / APP_ID and token permissions."
            )
        else:
            acct = data.get("authorize", {})
            print(f"[{get_ts()}] Authorised ✓  "
                  f"(loginid={acct.get('loginid')}, "
                  f"account={'DEMO' if str(acct.get('loginid','')).startswith('VRT') else 'REAL'})")
            _after_authorize(ws_app)

    # ── Historical ticks (warm-up prefill) ────────────────────────────
    elif msg_type == "history" or ("error" in data and "ticks_history" in (data.get("echo_req") or {})):
        _on_history(data)

    # ── Live tick stream ──────────────────────────────────────────────
    elif msg_type == "tick":
        if "error" in data:
            return
        tick   = data.get("tick", {})
        symbol = tick.get("symbol")
        quote  = tick.get("quote")
        epoch  = tick.get("epoch", int(time.time()))

        if not symbol or quote is None:
            return

        with data_lock:
            init_symbol(symbol)
            on_tick(symbol, quote, epoch)


def on_error(ws_app, error):
    print(f"[{get_ts()}] WebSocket error: {error}")


def on_open(ws_app):
    print(f"[{get_ts()}] Connection open — session already authenticated via OTP.")
    _after_authorize(ws_app)


def _after_authorize(ws_app):
    """Runs once Deriv confirms the authorize call succeeded."""
    global subscribed_symbols

    already_ready = sum(
        1 for st in symbol_data.values()
        if not st.get('htf', {}).get('warming_up', True)
    )
    is_reconnect = already_ready > 0

    if is_reconnect:
        print(f"[{get_ts()}] WebSocket RECONNECTED — "
              f"preserving indicator state for {already_ready}/{len(TRADING_SYMBOLS)} warmed-up symbols")
        subscribed_symbols.clear()
        for symbol in list(tick_buf_htf.keys()):
            tick_buf_htf[symbol] = []
        forming_htf.clear()
        send_telegram(
            f"🔄 <b>XERO AI — RECONNECTED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Status    : Back online\n"
            f"Warmed up : {already_ready}/{len(TRADING_SYMBOLS)} symbols retained\n"
            f"Action    : Resuming signal monitoring\n"
            f"Time      : {get_dt()}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>No warmup needed — indicators preserved</i>"
        )
    else:
        print(f"[{get_ts()}] WebSocket connected — fresh start")
        symbol_data.clear()
        subscribed_symbols.clear()
        tick_buf_htf.clear()
        forming_htf.clear()
        dashboard_data['htf_candles'].clear()

    use_prefill = PREFILL_ENABLED and not is_reconnect
    print(f"[{get_ts()}] Subscribing to {len(TRADING_SYMBOLS)} symbols"
          f"{' (with history prefill)' if use_prefill else ''}...")
    for cfg in TRADING_SYMBOLS:
        if use_prefill:
            _request_history(cfg['symbol'])
        else:
            _subscribe_ticks(cfg['symbol'])
        time.sleep(0.25)
    if use_prefill:
        threading.Thread(target=_history_watchdog, daemon=True).start()


def on_close(ws_app, code, msg):
    global running, subscribed_symbols
    print(f"[{get_ts()}] WebSocket closed: {code} {msg}")
    subscribed_symbols.clear()
    if running:
        print(f"[{get_ts()}] Reconnecting in 5s...")
        time.sleep(5)
        _start_ws_thread()


def _request_history(symbol):
    """Ask Deriv for recent ticks; the live subscription starts once they are replayed."""
    pending_history.add(symbol)
    try:
        ws.send(json.dumps({"ticks_history": symbol, "adjust_start_time": 1,
                            "count": PREFILL_TICKS, "end": "latest", "style": "ticks"}))
    except Exception as e:
        print(f"[{get_ts()}] History request error [{symbol}]: {e}")
        pending_history.discard(symbol)
        _subscribe_ticks(symbol)


def _replay_history(symbol, prices, times):
    """Warm indicators from history WITHOUT sending alerts or logging decisions."""
    global REPLAY
    with data_lock:
        init_symbol(symbol)
        REPLAY = True
        try:
            for p, t in zip(prices, times):
                on_tick(symbol, p, t)
        finally:
            REPLAY = False


def _on_history(data):
    symbol = (data.get("echo_req") or {}).get("ticks_history")
    if not symbol:
        return
    if "error" in data:
        print(f"[{get_ts()}] History unavailable [{symbol}] "
              f"({data['error'].get('message', 'error')}) — warming up live instead")
    else:
        hist   = data.get("history") or {}
        prices = hist.get("prices") or []
        times  = hist.get("times") or []
        if prices:
            _replay_history(symbol, prices, times)
            print(f"[{get_ts()}] Prefilled [{symbol}] with {len(prices)} ticks")
    pending_history.discard(symbol)
    _subscribe_ticks(symbol)


def _history_watchdog():
    """If any history reply never arrives, subscribe live anyway after 25s."""
    time.sleep(25)
    for symbol in list(pending_history):
        print(f"[{get_ts()}] No history reply [{symbol}] — warming up live instead")
        pending_history.discard(symbol)
        _subscribe_ticks(symbol)


def _subscribe_ticks(symbol):
    """Subscribe to the raw ticks stream for one symbol."""
    if symbol in subscribed_symbols:
        return
    subscribed_symbols.add(symbol)
    try:
        ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
        print(f"[{get_ts()}] Subscribed ticks [{symbol}]")
    except Exception as e:
        print(f"[{get_ts()}] Subscribe error [{symbol}]: {e}")
        subscribed_symbols.discard(symbol)

# ============================================================
# OTP / REST AUTHENTICATION
# ============================================================

def _get_account_id():
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID":  APP_ID,
        "Content-Type":  "application/json",
    }
    try:
        print(f"[{get_ts()}] Fetching accounts from Deriv...")
        resp = requests.get(f"{REST_BASE}/trading/v1/options/accounts",
                            headers=headers, timeout=15)
        print(f"[{get_ts()}] Accounts GET status: {resp.status_code}")
        if resp.status_code == 200:
            data     = resp.json()
            accounts = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(accounts, list):
                accounts = [accounts]
            for acct in accounts:
                atype   = (acct.get("account_type") or acct.get("type") or "").lower()
                acct_id = acct.get("id") or acct.get("account_id")
                if acct_id and "demo" in atype:
                    print(f"[{get_ts()}] Found DEMO account ID: {acct_id}")
                    return acct_id
            for acct in accounts:
                acct_id = acct.get("id") or acct.get("account_id")
                if acct_id:
                    print(f"[{get_ts()}] Found account ID: {acct_id}")
                    return acct_id
    except Exception as e:
        print(f"[{get_ts()}] Accounts GET exception: {e}")

    try:
        print(f"[{get_ts()}] Creating demo account...")
        resp = requests.post(
            f"{REST_BASE}/trading/v1/options/accounts",
            headers=headers,
            json={"currency": "USD", "group": "row", "account_type": "demo"},
            timeout=15)
        if resp.status_code in (200, 201):
            data    = resp.json()
            acct    = data.get("data", data)
            acct_id = acct.get("id") or acct.get("account_id")
            if acct_id:
                return acct_id
    except Exception as e:
        print(f"[{get_ts()}] Create account exception: {e}")
    return None


def _get_otp_ws_url():
    acct_id = _get_account_id()
    if not acct_id:
        print(f"[{get_ts()}] Could not retrieve account ID")
        return None
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID":  APP_ID,
        "Content-Type":  "application/json",
    }
    try:
        print(f"[{get_ts()}] Requesting OTP for account {acct_id}...")
        resp = requests.post(
            f"{REST_BASE}/trading/v1/options/accounts/{acct_id}/otp",
            headers=headers, timeout=15)
        if resp.status_code == 200:
            data   = resp.json()
            ws_url = (data.get("data", {}).get("url") or
                      data.get("url") or
                      data.get("data", {}).get("websocket_url"))
            if ws_url:
                print(f"[{get_ts()}] OTP WS URL obtained ✓")
                return ws_url
    except Exception as e:
        print(f"[{get_ts()}] OTP exception: {e}")
    return None


def _run_ws():
    global ws
    # Deriv's current Options-platform flow: REST call gets a short-lived
    # OTP, embedded in a ready-to-use WebSocket URL. The OTP must be
    # requested fresh right before each connection attempt (including
    # reconnects) — no separate `authorize` message is needed since the
    # OTP in the URL already authenticates the session.
    ws_url = _get_otp_ws_url()
    if not ws_url:
        print(f"[{get_ts()}] Could not obtain OTP WebSocket URL — "
              f"check API_TOKEN / APP_ID and retry in 5s...")
        time.sleep(5)
        if running:
            _start_ws_thread()
        return

    print(f"[{get_ts()}] Connecting to Deriv Options WebSocket...")
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close,
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)


def _start_ws_thread():
    threading.Thread(target=_run_ws, daemon=True).start()

def _firebase_status():
    """Extra fields written to bot_status/current with every heartbeat."""
    sds = list(symbol_data.values())
    return {
        'symbolsReady': sum(1 for st in sds if not st.get('htf', {}).get('warming_up', True)),
        'symbolsTotal': len(TRADING_SYMBOLS),
        'signalsSent':  sum(st.get('signal_count', 0) for st in sds),
    }


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("XERO AI SOFTWARE — TICK-BASED SINGLE TIMEFRAME SIGNAL BOT")
    print("="*70)
    print(f"  Mode           : SIGNALS ONLY — this script never opens trades")
    print(f"  Candle source  : RAW TICKS (built in-memory)")
    print(f"  HTF candle     : every {HTF_TICKS} ticks")
    print(f"  IntBB on MAP   : period={BB_PERIOD}, dev={BB_DEVIATION}")
    print(f"  ExtBB on price : period={EXT_BB_PERIOD}, dev={EXT_BB_DEVIATION}")
    print(f"  Symbols        : {len(TRADING_SYMBOLS)}")
    print(f"  Telegram       : {'configured' if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID else 'NOT configured'}")
    print("="*70)
    print()
    print("  ── SIGNAL condition — 4 steps ──")
    print("    LONG BUY   : 1. MAP exits below IntBB Lower (oversold excursion)")
    print("                 2. MAP forms trough + curves up (depth ≥ 5% band width)")
    print("                 3. MAP re-enters above IntBB Lower while rising")
    print("                 4. Price > ExtBB Upper  → LONG BUY signal sent")
    print()
    print("    SHORT SELL : 1. MAP exits above IntBB Upper (overbought excursion)")
    print("                 2. MAP forms peak + curves down (depth ≥ 5% band width)")
    print("                 3. MAP re-enters below IntBB Upper while falling")
    print("                 4. Price < ExtBB Lower  → SHORT SELL signal sent")
    print()
    print("  ── FLOW ──")
    print("    1. Raw tick arrives → fed to HTF buffer")
    print(f"    2. HTF buffer reaches {HTF_TICKS} ticks → candle closed → indicators computed")
    print("    3. All 4 conditions met → signal sent to Telegram")
    print("="*70 + "\n")

    _missing = [n for n, v in (("DERIV_PAT", API_TOKEN), ("DERIV_APP_ID", APP_ID),
                               ("TELEGRAM_TOKEN", TELEGRAM_TOKEN), ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)) if not v]
    if _missing:
        print(f"[{get_ts()}] ❌ Missing environment variable(s): {', '.join(_missing)} — set them and start again.")
        sys.exit(2)

    # Connect to Firestore and wait for the single-instance lock BEFORE announcing
    # anything, so a standby copy (e.g. phone + PC) never sends duplicate messages.
    if firebase_sync:
        firebase_sync.init(status_fn=_firebase_status)

    send_telegram(
        f"🚀 <b>XERO AI SOFTWARE STARTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode          : SIGNALS ONLY\n"
        f"Candle source : RAW TICKS\n"
        f"HTF           : {HTF_TICKS}-tick candles\n"
        f"IntBB on MAP  : period={BB_PERIOD}, dev={BB_DEVIATION}\n"
        f"ExtBB on price: period={EXT_BB_PERIOD}, dev={EXT_BB_DEVIATION}\n"
        f"Signal (4 steps):\n"
        f"  1. MAP excursion outside IntBB\n"
        f"  2. Trough/Peak curve forms\n"
        f"  3. MAP re-enters IntBB\n"
        f"  LONG BUY   : Price &gt; ExtBB Upper\n"
        f"  SHORT SELL : Price &lt; ExtBB Lower\n"
        f"Symbols       : {len(TRADING_SYMBOLS)}\n"
        f"Time          : {get_dt()}\n"
        f"Running on    : {_run_label()}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Candles built live from raw price ticks</i>",
        important=True
    )

    _start_ws_thread()
    time.sleep(2)
    print(f"[{get_ts()}] Feed running — signals go to Telegram and the website")

    # Android/Termux stops apps with SIGTERM: treat it like Ctrl+C so we shut down cleanly.
    def _on_sigterm(signum, frame):
        raise KeyboardInterrupt
    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except Exception:
        pass

    hb = 0
    try:
        while running:
            time.sleep(30)
            hb += 1
            if hb % 4 == 0:
                ready_htf = sum(1 for st in symbol_data.values()
                                if not st.get('htf', {}).get('warming_up', True))
                total_htf_candles = sum(st.get('htf', {}).get('candle_count', 0)
                                        for st in list(symbol_data.values()))
                total_signals     = sum(st.get('signal_count', 0)
                                        for st in list(symbol_data.values()))
                print(f"\n[{get_ts()}] ♥  "
                      f"HTF-ready:{ready_htf}/{len(TRADING_SYMBOLS)}  "
                      f"HTF-candles:{total_htf_candles}  "
                      f"Signals-sent:{total_signals}")
    except KeyboardInterrupt:
        print(f"\n[{get_ts()}] Stopped by user")
        running = False
        signal.signal(signal.SIGINT, signal.SIG_IGN)   # a second Ctrl+C must not cut the shutdown short
        _next_line = ""
        if os.environ.get("GITHUB_ACTIONS") == "true":
            _next_line = (f"\nNext run starts at about {(datetime.now() + timedelta(minutes=5)).strftime('%H:%M')} "
                          f"(in about 5 minutes).")
        send_telegram(f"⏹ <b>XERO AI SOFTWARE — STOPPED</b>\nRunning on: {_run_label()}\n"
                      f"Time: {get_dt()}{_next_line}", important=True)
        time.sleep(3)   # let the message leave before the process exits
        if ws:
            ws.close()

    if firebase_sync:
        firebase_sync.shutdown()

    print(f"\n[{get_ts()}] Shutdown complete")
