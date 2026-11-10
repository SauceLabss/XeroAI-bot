"""
firebase_sync.py — pushes XERO AI bot output to Firestore so the website updates live.
(Termux-friendly: talks to Firestore over plain HTTPS/REST. No firebase-admin, no gRPC,
 so it installs on a phone with only pure-Python packages.)

Writes:
  bot_signals/{auto-id}   one doc per FIRED signal   -> Live AI Signals card + AI Timeline
  bot_daily/{YYYY-MM-DD}  per-day counters           -> AI Confidence Trend + AI Decision Breakdown
  bot_markets/current     live price + mini-chart + % change for the dashboard market cards
  bot_status/current      heartbeat every 30s        -> LIVE / OFFLINE pill
  bot_status/lock         "who is running" lock      -> stops two copies (phone + PC) sending doubles

Everything network-related runs on a background thread, so the tick thread is never blocked.
If the key or network is missing the bot keeps running; sync just logs a warning.

Setup: put your Firebase service-account key JSON next to this file as serviceAccountKey.json
(or set XEROAI_FIREBASE_KEY). NEVER upload that key to the website or a public repo.
"""
import json
import os
import queue
import random
import string
import threading
import time
import uuid
from collections import deque
from datetime import datetime

try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
    _IMPORT_OK = True
except ImportError:
    _IMPORT_OK = False

SCOPES = ["https://www.googleapis.com/auth/datastore"]
HEARTBEAT_SECS = 30
FLUSH_SECS = 15
LOCK_WATCH_SECS = 40          # how long a newcomer watches the lock for signs of life
MARKET_PUSH_SECS = 10         # how often market cards refresh (keep >= 10 to stay in Firestore's free quota)
CHANGE_TICKS = 5              # % change is measured over the last N ticks
SPARK_POINTS = 60             # ticks drawn in each card's mini chart
MARKET_SYMBOLS = {"1HZ100V", "R_100", "R_10", "R_75", "1HZ10V",
                  "CRASH500", "frxEURUSD", "frxGBPUSD", "frxUSDJPY", "frxUSDCAD"}   # the 10 dashboard cards
HERE = os.path.dirname(os.path.abspath(__file__))

_enabled = False
_session = None
_pid = None
_base = None
_status_fn = None
_thread = None
_have_lock = False
_ME = f"{os.environ.get('XEROAI_HOST', 'bot')}-{uuid.uuid4().hex[:6]}"
_stop = threading.Event()
_q = queue.Queue(maxsize=2000)
_ticks = {}     # symbol -> deque of recent prices (market cards)
_ticks_lock = threading.Lock()
_pending = {}   # 'YYYY-MM-DD' -> counters waiting to be flushed


def _log(msg):
    print(f"[{datetime.now():%H:%M:%S}] [firebase] {msg}", flush=True)


# ---------------- Firestore REST helpers ----------------

def _name(path):
    return f"projects/{_pid}/databases/(default)/documents/{path}"


def _val(v):
    if isinstance(v, bool):
        return {"booleanValue": v}
    if isinstance(v, int):
        return {"integerValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    return {"stringValue": "" if v is None else str(v)}


def _server_time(field):
    return {"fieldPath": field, "setToServerValue": "REQUEST_TIME"}


def _inc(field, n):
    if isinstance(n, float):
        return {"fieldPath": field, "increment": {"doubleValue": n}}
    return {"fieldPath": field, "increment": {"integerValue": str(int(n))}}


def _write(path, fields, transforms=None):
    """Create-or-merge a document (only the listed fields are touched)."""
    w = {"update": {"name": _name(path), "fields": {k: _val(v) for k, v in fields.items()}},
         "updateMask": {"fieldPaths": list(fields.keys())}}
    if transforms:
        w["updateTransforms"] = transforms
    return w


def _commit(writes):
    r = _session.post(f"{_base}:commit", json={"writes": writes}, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Firestore {r.status_code}: {r.text[:200]}")


def _get_doc(path):
    r = _session.get(f"{_base}/{path}", timeout=20)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Firestore {r.status_code}: {r.text[:200]}")
    return r.json().get("fields", {})


def _retry(fn, tries=3):
    for i in range(tries):
        try:
            return fn()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(2)


# ---------------- lock ----------------

def _read_lock():
    f = _get_doc("bot_status/lock") or {}
    holder = (f.get("holder") or {}).get("stringValue", "")
    beat = (f.get("lastBeat") or {}).get("timestampValue", "")
    return holder, beat


def _claim_lock():
    global _have_lock
    _commit([_write("bot_status/lock", {"holder": _ME}, [_server_time("lastBeat")])])
    _have_lock = True


def _wait_for_lock():
    """Block while another live instance holds the lock; take over if it looks dead."""
    global _have_lock
    announced = False
    while not _stop.is_set():
        holder, beat = _read_lock()
        if holder and holder != _ME:
            time.sleep(LOCK_WATCH_SECS)                  # is it still beating?
            holder2, beat2 = _read_lock()
            if holder2 and (holder2 != holder or beat2 != beat):
                if not announced:
                    _log(f"another instance ({holder2}) is active — standing by until it stops...")
                    announced = True
                time.sleep(30)
                continue
            _log(f"previous instance ({holder}) looks dead — taking over.")
        _claim_lock()
        time.sleep(2)
        if _read_lock()[0] == _ME:                       # confirm we really won
            _log(f"lock acquired as {_ME}")
            return True
        _have_lock = False
    return False


# ---------------- public API ----------------

def init(status_fn=None):
    """Connect, wait for the lock, start the sync thread. Returns True if sync is enabled."""
    global _enabled, _session, _pid, _base, _status_fn, _thread
    if not _IMPORT_OK:
        _log("google-auth is not installed (pip install google-auth requests) — cloud sync disabled.")
        return False
    key = os.environ.get("XEROAI_FIREBASE_KEY") or "serviceAccountKey.json"
    if not os.path.isabs(key):
        key = os.path.join(HERE, key)
    if not os.path.exists(key):
        _log(f"service account key not found at {key} — cloud sync disabled.")
        return False
    try:
        creds = service_account.Credentials.from_service_account_file(key, scopes=SCOPES)
        _pid = creds.project_id
        _session = AuthorizedSession(creds)
        _base = f"https://firestore.googleapis.com/v1/projects/{_pid}/databases/(default)/documents"
        _read_lock()                                     # proves the key + network work
    except Exception as e:
        _log(f"could not connect: {e} — cloud sync disabled.")
        return False
    _status_fn = status_fn
    _enabled = True
    if not _wait_for_lock():
        _enabled = False
        return False
    _stop.clear()
    _thread = threading.Thread(target=_worker, daemon=True, name="firebase-sync")
    _thread.start()
    _log("connected — signals, daily stats and status will sync to Firestore.")
    return True


def push_signal(symbol, name, direction, confidence, timeframe):
    _enqueue(("signal", {"symbol": symbol, "name": name, "direction": direction,
                         "confidence": confidence, "timeframe": timeframe}, 0))


def record_decision(direction, decision, confidence):
    _enqueue(("decision", {"direction": direction, "decision": decision, "confidence": confidence,
                           "day": datetime.now().strftime("%Y-%m-%d")}, 0))


def push_tick(symbol, price, epoch=None):
    """Called for every tick. Only remembers it in memory (no network); the worker sends it."""
    if symbol not in MARKET_SYMBOLS:
        return
    with _ticks_lock:
        _ticks.setdefault(symbol, deque(maxlen=SPARK_POINTS + CHANGE_TICKS + 5)).append(float(price))


def shutdown():
    """Flush pending stats, mark the bot OFFLINE and release the lock."""
    global _have_lock
    if not _enabled:
        return
    _stop.set()
    if _thread:
        _thread.join(timeout=5)
    try:
        _flush()
        _commit([_write("bot_status/current", {"online": False}, [_server_time("lastHeartbeat")]),
                 _write("bot_status/lock", {"holder": ""}, [_server_time("lastBeat")])])
        _have_lock = False
    except Exception as e:
        _log(f"shutdown error: {e}")


# ---------------- internals ----------------

def _enqueue(item):
    if not _enabled:
        return
    try:
        _q.put_nowait(item)
    except queue.Full:
        pass


def _new_id():
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(20))


def _write_signal(p):
    direction = p["direction"]
    fields = {
        "type": "✅ ENTRY",              # shows as "Confirmed" in the website timeline
        "symbol": p["symbol"],
        "name": p["name"],
        "assetType": "synthetic",
        "direction": direction,          # 'BUY' | 'SELL'
        "timeframe": p["timeframe"],
        "message": f"HTF {'LONG BUY' if direction == 'BUY' else 'SHORT SELL'} signal",
    }
    if p.get("confidence") is not None:
        fields["confidence"] = float(p["confidence"])
    _retry(lambda: _commit([_write(f"bot_signals/{_new_id()}", fields, [_server_time("createdAt")])]))


def _aggregate(p):
    c = _pending.setdefault(p["day"], {"fireCount": 0, "confSum": 0.0, "buy": 0, "sell": 0, "hold": 0})
    if p["decision"] == "FIRE":
        c["fireCount"] += 1
        if p.get("confidence") is not None:
            c["confSum"] += float(p["confidence"])
        c["buy" if p["direction"] == "BUY" else "sell"] += 1
    else:                                # REJECT = the AI chose to hold
        c["hold"] += 1


def _flush():
    for day in list(_pending.keys()):
        c = _pending[day]
        tr = [_inc("fireCount", c["fireCount"]), _inc("confSum", float(c["confSum"])),
              _inc("buy", c["buy"]), _inc("sell", c["sell"]), _inc("hold", c["hold"]),
              _server_time("updatedAt")]
        _retry(lambda: _commit([_write(f"bot_daily/{day}", {"date": day}, tr)]))
        del _pending[day]                # only cleared after a successful write


def _push_markets():
    out = {}
    with _ticks_lock:
        for sym, dq in _ticks.items():
            v = list(dq)
            if not v:
                continue
            last = v[-1]
            base = v[-1 - CHANGE_TICKS] if len(v) > CHANGE_TICKS else v[0]
            pct = (last - base) / base * 100 if base else 0.0
            out[sym] = {"p": last, "c": round(pct, 4), "s": v[-SPARK_POINTS:]}
    if not out:
        return
    _retry(lambda: _commit([_write("bot_markets/current",
                                   {"data": json.dumps(out, separators=(",", ":")),
                                    "changeTicks": CHANGE_TICKS},
                                   [_server_time("updatedAt")])]))


def _heartbeat():
    fields = {"online": True}
    if _status_fn:
        try:
            fields.update(_status_fn() or {})
        except Exception:
            pass
    _retry(lambda: _commit([
        _write("bot_status/current", fields, [_server_time("lastHeartbeat")]),
        _write("bot_status/lock", {"holder": _ME}, [_server_time("lastBeat")]),
    ]))


def _worker():
    last_hb = last_flush = last_mk = 0.0
    while not _stop.is_set():
        try:
            item = _q.get(timeout=1)
        except queue.Empty:
            item = None
        try:
            if item:
                kind, payload, attempt = item
                try:
                    if kind == "signal":
                        _write_signal(payload)
                    else:
                        _aggregate(payload)
                except Exception as e:
                    if attempt < 5:                      # keep it and try again later
                        _enqueue((kind, payload, attempt + 1))
                        time.sleep(5)
                    _log(f"write failed ({kind}): {e}")
            now = time.time()
            if now - last_flush >= FLUSH_SECS:
                _flush()
                last_flush = now
            if now - last_mk >= MARKET_PUSH_SECS:
                _push_markets()
                last_mk = now
            if now - last_hb >= HEARTBEAT_SECS:
                _heartbeat()
                last_hb = now
        except Exception as e:
            _log(f"sync error: {e}")
