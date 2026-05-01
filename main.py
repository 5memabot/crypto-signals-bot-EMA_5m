import requests
import time
import logging
import numpy as np
from datetime import datetime, timedelta
from threading import Thread, Lock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

import os
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

EMA_FAST        = 11
EMA_SLOW        = 26
EMA_SELL_FAST   = 7
EMA_SELL_SLOW   = 16
CONFIRM_MIN_PCT = 0.002
WAIT_MINUTES    = 3
MAX_CHECKS      = 30
BUY_EXPIRY_HRS  = 24
COOLDOWN_MIN    = 75

ZRTI_DELTA       = 0.3
ZRTI_SELL_THRESH = 75
ZRTI_Z_SELL      = 0.5

REQUEST_DELAY = {
    "Binance" : 0.15,
    "MEXC"    : 0.25,
    "Bybit"   : 0.25,
    "Gate"    : 0.35,
    "KuCoin"  : 0.35,
}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CryptoBot/1.0)",
    "Accept"    : "application/json",
}

SESSION_BINANCE = requests.Session()
SESSION_MEXC    = requests.Session()
SESSION_BYBIT   = requests.Session()
SESSION_GATE    = requests.Session()
SESSION_KUCOIN  = requests.Session()

for _s in [SESSION_BINANCE, SESSION_MEXC, SESSION_BYBIT, SESSION_GATE, SESSION_KUCOIN]:
    _s.headers.update(_HEADERS)

def retry_get(session, url, params, retries=3, timeout=5):
    waits = [0, 2, 5]
    for attempt in range(retries):
        try:
            if waits[attempt] > 0:
                time.sleep(waits[attempt])
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                log.warning("Rate limit — انتظار 10 ثوانٍ")
                time.sleep(10)
                continue
            if r.status_code != 200:
                continue
            return r
        except Exception as e:
            log.warning(f"retry {attempt+1}: {e}")
    return None

# ── Candles ──────────────────────────────────────────────────────

def get_closes_binance(symbol, limit=152):
    try:
        r = retry_get(SESSION_BINANCE, "https://api.binance.com/api/v3/klines",
                      {"symbol": symbol, "interval": "15m", "limit": limit})
        if r is None: return None
        data = r.json()
        if isinstance(data, list) and len(data) >= EMA_SLOW + 5:
            closes = [float(c[4]) for c in data[:-1]]
            if all(x > 0 for x in closes[-5:]): return closes
    except Exception as e: log.warning(f"Binance {symbol}: {e}")
    return None

def get_closes_mexc(symbol, limit=152):
    try:
        r = retry_get(SESSION_MEXC, "https://api.mexc.com/api/v3/klines",
                      {"symbol": symbol, "interval": "15m", "limit": limit})
        if r is None: return None
        data = r.json()
        if isinstance(data, list) and len(data) >= EMA_SLOW + 5:
            closes = [float(c[4]) for c in data[:-1]]
            if all(x > 0 for x in closes[-5:]): return closes
    except Exception as e: log.warning(f"MEXC {symbol}: {e}")
    return None

def get_closes_bybit(symbol, limit=152):
    try:
        r = retry_get(SESSION_BYBIT, "https://api.bybit.com/v5/market/kline",
                      {"symbol": symbol, "interval": "15", "limit": limit, "category": "spot"})
        if r is None: return None
        data = r.json()
        if data.get("retCode") == 0:
            candles = data["result"]["list"]
            if len(candles) >= EMA_SLOW + 5:
                ordered = list(reversed(candles))
                closes = [float(c[4]) for c in ordered[:-1]]
                if all(x > 0 for x in closes[-5:]): return closes
    except Exception as e: log.warning(f"Bybit {symbol}: {e}")
    return None

def get_closes_gate(symbol, limit=152):
    try:
        pair = symbol.replace("USDT", "_USDT")
        r = retry_get(SESSION_GATE, "https://api.gateio.ws/api/v4/spot/candlesticks",
                      {"currency_pair": pair, "interval": "15m", "limit": limit})
        if r is None: return None
        data = r.json()
        if isinstance(data, list) and len(data) >= EMA_SLOW + 5:
            closes = [float(c[2]) for c in data[:-1]]
            if all(v > 0 for v in closes[-5:]): return closes
    except Exception as e: log.warning(f"Gate {symbol}: {e}")
    return None

def get_closes_kucoin(symbol, limit=152):
    try:
        pair = symbol.replace("USDT", "-USDT")
        r = retry_get(SESSION_KUCOIN, "https://api.kucoin.com/api/v1/market/candles",
                      {"symbol": pair, "type": "15min"})
        if r is None: return None
        data = r.json()
        if data.get("code") == "200000" and data.get("data"):
            candles = data["data"][:limit]
            if len(candles) >= EMA_SLOW + 5:
                ordered = list(reversed(candles))
                closes = [float(c[2]) for c in ordered[:-1]]
                if all(x > 0 for x in closes[-5:]): return closes
    except Exception as e: log.warning(f"KuCoin {symbol}: {e}")
    return None

# ── Current Price (سريع) ─────────────────────────────────────────

def get_price_binance(symbol):
    try:
        r = retry_get(SESSION_BINANCE, "https://api.binance.com/api/v3/ticker/price", {"symbol": symbol})
        if r: return float(r.json()["price"])
    except: pass
    return None

def get_price_mexc(symbol):
    try:
        r = retry_get(SESSION_MEXC, "https://api.mexc.com/api/v3/ticker/price", {"symbol": symbol})
        if r: return float(r.json()["price"])
    except: pass
    return None

def get_price_bybit(symbol):
    try:
        r = retry_get(SESSION_BYBIT, "https://api.bybit.com/v5/market/tickers",
                      {"category": "spot", "symbol": symbol})
        if r:
            d = r.json()
            if d.get("retCode") == 0:
                return float(d["result"]["list"][0]["lastPrice"])
    except: pass
    return None

def get_price_gate(symbol):
    try:
        pair = symbol.replace("USDT", "_USDT")
        r = retry_get(SESSION_GATE, "https://api.gateio.ws/api/v4/spot/tickers",
                      {"currency_pair": pair})
        if r: return float(r.json()[0]["last"])
    except: pass
    return None

def get_price_kucoin(symbol):
    try:
        pair = symbol.replace("USDT", "-USDT")
        r = retry_get(SESSION_KUCOIN,
                      "https://api.kucoin.com/api/v1/market/orderbook/level1",
                      {"symbol": pair})
        if r:
            d = r.json()
            if d.get("code") == "200000":
                return float(d["data"]["price"])
    except: pass
    return None

# ── Symbols ───────────────────────────────────────────────────────

BINANCE_SYMBOLS = [
    "BTCUSDT","ETHUSDT","XRPUSDT","ADAUSDT","SOLUSDT","DOTUSDT","DOGEUSDT",
    "AVAXUSDT","LTCUSDT","LINKUSDT","ATOMUSDT","XLMUSDT","FILUSDT","TRXUSDT",
    "ALGOUSDT","XMRUSDT","ICPUSDT","EGLDUSDT","HBARUSDT","NEARUSDT","APEUSDT",
    "DASHUSDT","ZILUSDT","ZECUSDT","ZENUSDT","STORJUSDT","RAREUSDT","OPUSDT",
    "ARBUSDT","SEIUSDT","TIAUSDT","WLDUSDT","ORDIUSDT","RENDERUSDT","PHAUSDT",
    "POLUSDT","TRBUSDT","VIRTUALUSDT","WALUSDT","APTUSDT","BCHUSDT","BIOUSDT",
    "CHRUSDT","GRTUSDT","ARKMUSDT","AGLDUSDT","OPENUSDT","PLUMEUSDT",
    "SAHARAUSDT","SUSDT","LINEAUSDT","XPLUSDT",
]
MEXC_SYMBOLS = [
    "XCNUSDT","COREUSDT","PIUSDT","XDCUSDT","RIOUSDT","PLAYUSDT","STABLEUSDT",
    "BLESSUSDT","COAIUSDT","CROSSUSDT","FHEUSDT","GRASSUSDT","GRIFFAINUSDT",
    "HUSDT","LIGHTUSDT","ALEOUSDT","PINUSDT","PORT3USDT","KGENUSDT","ABUSDT",
    "ATHUSDT","AIOUSDT","A8USDT","ALUUSDT","XPRUSDT","OMGUSDT",
]
BYBIT_SYMBOLS = [
    "UXLINKUSDT","KASUSDT","MNTUSDT","FLOCKUSDT","PAALUSDT","L3USDT","ALCHUSDT",
    "ZIGUSDT","MONUSDT","CSPRUSDT","INSPUSDT","MOVEUSDT","COOKIEUSDT","LRCUSDT",
    "ZROUSDT","MOVRUSDT","TONUSDT","FETUSDT","SUIUSDT","GALAUSDT","TAOUSDT",
    "QNTUSDT","SANDUSDT","ETCUSDT","TNSRUSDT","KAIAUSDT","PYTHUSDT","AIXBTUSDT",
    "BLURUSDT","ZKUSDT","JASMYUSDT","PARTIUSDT","THETAUSDT","BICOUSDT","POLUSDT",
]
GATE_SYMBOLS = [
    "AKTUSDT","RADUSDT","ALTUSDT","BATUSDT","MINAUSDT","IDUSDT","MTLUSDT",
    "BANDUSDT","ICXUSDT","STGUSDT","PROVEUSDT","STXUSDT","SKLUSDT","GLMUSDT",
    "XTZUSDT","IQUSDT","HOTUSDT","LAUSDT","RLCUSDT","VANAUSDT","BEAMUSDT",
    "PONDUSDT","LPTUSDT","MIRAUSDT","GUSDT","POWRUSDT","ARCUSDT",
]
KUCOIN_SYMBOLS = [
    "AIOZUSDT","DUSKUSDT","IOTXUSDT","MANTAUSDT","NIGHTUSDT","CELRUSDT",
    "ANKRUSDT","ENSUSDT","API3USDT","WUSDT","MANAUSDT","CELOUSDT","EIGENUSDT",
    "GASUSDT","ENJUSDT","GMTUSDT","IOUSDT","KAITOUSDT","ACTUSDT","CHZUSDT",
    "DEXEUSDT","HNTUSDT","FLUXUSDT","PORTALUSDT","EDUUSDT","IOSTUSDT","VETUSDT",
]

# (exchange, candles_func, price_func)
SYMBOL_MAP = {}
for s in BINANCE_SYMBOLS: SYMBOL_MAP[s] = ("Binance", get_closes_binance, get_price_binance)
for s in MEXC_SYMBOLS:    SYMBOL_MAP[s] = ("MEXC",    get_closes_mexc,    get_price_mexc)
for s in BYBIT_SYMBOLS:   SYMBOL_MAP[s] = ("Bybit",   get_closes_bybit,   get_price_bybit)
for s in GATE_SYMBOLS:    SYMBOL_MAP[s] = ("Gate",    get_closes_gate,    get_price_gate)
for s in KUCOIN_SYMBOLS:  SYMBOL_MAP[s] = ("KuCoin",  get_closes_kucoin,  get_price_kucoin)

ALL_SYMBOLS = list(SYMBOL_MAP.keys())

pending_buy = {}
active_buy  = {}
cooldown    = {}
state_lock  = Lock()

# ═══════════════════════════════════════════════════
# EMA + ZRTI
# ═══════════════════════════════════════════════════

def calc_ema_series(closes, period):
    if len(closes) < period + 1: return None, None
    k    = 2 / (period + 1)
    ema  = sum(closes[:period]) / period
    prev = ema
    for price in closes[period:]:
        prev = ema
        ema  = price * k + ema * (1 - k)
    return ema, prev

def compute_zrti(closes):
    if len(closes) < 55: return 0.0, 0.0
    try:
        eps = 1e-10
        arr = np.array(closes, dtype=float)
        dP      = arr - np.roll(arr, 1); dP[0] = 0.0
        dP_prev = np.roll(dP, 1); dP_prev[0] = dP[1] if len(dP) > 1 else 0.0
        streak  = np.ones(len(arr), dtype=float)
        for i in range(1, len(arr)):
            if np.sign(dP[i]) == np.sign(dP[i-1]):
                streak[i] = streak[i-1] + 1
        ratio = np.clip(np.abs(dP) / (np.abs(dP_prev) + eps), 0, 10)
        M     = np.clip(1 - ratio * np.exp(-ZRTI_DELTA * streak), 0, 1)
        vp    = np.abs(dP)
        vm    = np.convolve(vp, np.ones(20)/20, mode='same')
        E     = np.clip(vp / (vm + eps), 0, 5) / 5.0
        mu    = np.convolve(arr, np.ones(50)/50, mode='same')
        sigma = np.array([np.std(arr[max(0,i-50):i+1]) for i in range(len(arr))])
        z_arr = (arr - mu) / (sigma + eps)
        Phi   = np.clip(z_arr**2, 0, 9) / 9.0
        zrti  = float((0.35*M + 0.25*E + 0.40*Phi)[-1] * 100)
        z     = float(z_arr[-1])
        if np.isnan(zrti) or np.isinf(zrti): zrti = 0.0
        if np.isnan(z)    or np.isinf(z):    z    = 0.0
        return zrti, z
    except Exception as e:
        log.debug(f"ZRTI: {e}"); return 0.0, 0.0

def check_sell_signal(closes):
    """EMA 5/13 bearish crossover + ZRTI تأكيد"""
    if len(closes) < 60: return False
    e5n, e5p   = calc_ema_series(closes, EMA_SELL_FAST)
    e13n, e13p = calc_ema_series(closes, EMA_SELL_SLOW)
    if any(v is None for v in [e5n, e5p, e13n, e13p]): return False
    if not ((e5p >= e13p) and (e5n < e13n)): return False   # لا يوجد كروس
    zrti, z = compute_zrti(closes)
    return (zrti > ZRTI_SELL_THRESH) and (z > ZRTI_Z_SELL)

# ═══════════════════════════════════════════════════
# Supabase
# ═══════════════════════════════════════════════════

def get_code_from_db(code):
    for table, days in [("licenses_30",30),("licenses_90",90),("licenses_180",180),("licenses_360",360)]:
        url = f"{SUPABASE_URL}/rest/v1/{table}?code=eq.{code}&status=eq.free&limit=1"
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
            data = r.json()
            if data and isinstance(data, list) and len(data) > 0:
                row = data[0]; row["duration_days"] = days; row["_table"] = table
                return row
        except Exception as e: log.warning(f"{table}: {e}")
    return None

def mark_code_used(code, user_id, table="licenses_30"):
    url = f"{SUPABASE_URL}/rest/v1/{table}?code=eq.{code}"
    r   = requests.patch(url, headers=HEADERS,
                         json={"status":"used","used_by":user_id,"used_at":datetime.utcnow().isoformat()})
    if r.status_code != 204: log.warning(f"فشل تحديث {code}")

def activate_user(user_id, expire):
    h = HEADERS.copy(); h["Prefer"] = "resolution=merge-duplicates"
    r = requests.post(f"{SUPABASE_URL}/rest/v1/users?on_conflict=user_id", headers=h,
                      json={"user_id":user_id,"expire_at":expire.isoformat(),"is_active":True})
    if r.status_code in (200,201,204): log.info(f"تفعيل {user_id} حتى {expire}")
    else: log.warning(f"فشل تفعيل {user_id}")

def get_active_users():
    now = datetime.utcnow().isoformat()
    r   = requests.get(f"{SUPABASE_URL}/rest/v1/users?expire_at=gt.{now}&is_active=eq.true",
                       headers=HEADERS)
    data = r.json()
    return [u["user_id"] for u in data] if isinstance(data, list) else []

# ═══════════════════════════════════════════════════
# Telegram
# ═══════════════════════════════════════════════════

def send_message_to_user(user_id, text, reply_to=None):
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": user_id, "text": text, "parse_mode": "HTML"}
    if reply_to: data["reply_to_message_id"] = reply_to
    try:
        r = requests.post(url, data=data, timeout=8).json()
        if r.get("ok"): return r["result"]["message_id"]
    except Exception as e: log.warning(f"إرسال {user_id}: {e}")
    return None

def broadcast_message(text, reply_to_map=None):
    result_map = {}
    for uid in get_active_users():
        try:
            mid = send_message_to_user(uid, text, reply_to=(reply_to_map or {}).get(uid))
            if mid: result_map[uid] = mid
        except Exception as e: log.warning(f"broadcast {uid}: {e}")
    return result_map

def send_message(text, reply_to=None, pin=False):
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to: data["reply_to_message_id"] = reply_to
    try:
        r = requests.post(url, data=data, timeout=5).json()
        if r.get("ok"):
            mid = r["result"]["message_id"]
            if pin:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/pinChatMessage",
                              data={"chat_id": CHAT_ID, "message_id": mid, "disable_notification": True},
                              timeout=5)
            return mid
    except Exception as e: log.error(f"Telegram: {e}")
    return None

# ═══════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════

def fmt_symbol(s): return s[:-4]+"/USDT" if s.endswith("USDT") else s

def fmt_price(p):
    if p >= 1000: return f"{p:,.2f}"
    elif p >= 1:  return f"{p:.4f}"
    else:         return f"{p:.6f}"

def can_signal(symbol):
    if symbol not in cooldown: return True
    return (datetime.now() - cooldown[symbol]) >= timedelta(minutes=COOLDOWN_MIN)

def cleanup_cooldown():
    with state_lock:
        for s in list(cooldown):
            if datetime.now() - cooldown[s] > timedelta(hours=12):
                del cooldown[s]

# ═══════════════════════════════════════════════════
# Daily Report
# ═══════════════════════════════════════════════════
daily_results = {}
daily_lock    = Lock()
last_report   = datetime.now()

def record_sell_result(symbol, pair, buy_price, close_price, peak_price):
    with daily_lock:
        bp = buy_price
        daily_results[symbol] = {
            "pair": pair, "buy_price": bp,
            "close_price": close_price, "peak_price": peak_price,
            "pct":      (close_price-bp)/bp*100 if bp>0 else 0,
            "peak_pct": (peak_price -bp)/bp*100 if bp>0 else 0,
        }

def send_daily_report():
    with daily_lock:
        if not daily_results: return
        results = dict(daily_results); daily_results.clear()
    total       = len(results)
    win_ath     = {s:v for s,v in results.items() if v["peak_pct"]>0}
    wins_close  = {s:v for s,v in results.items() if v["pct"]>=0}
    loses_close = {s:v for s,v in results.items() if v["pct"]<0}
    success_pct = int(len(win_ath)/total*100) if total>0 else 0
    msg = "💲ئەنجامێ 24 دەمژمێرێن چوی یێن کوینا ل قازانج و خساربونێ دا.💱⚡👾💯💯\n\n"
    if wins_close:
        msg += "Win💲🚀\n\n"
        for v in sorted(wins_close.values(), key=lambda x:x["pct"], reverse=True):
            msg += f"{v['pair']} (<b><u>+{v['pct']:.1f}%</u></b>)✅   --- ATH: (<b><u>+{v['peak_pct']:.1f}%</u></b>){'✅' if v['peak_pct']>0 else '❌'}\n"
        msg += "____________________________\n\n"
    if loses_close:
        msg += "LOSE 🐹💔\n\n"
        for v in sorted(loses_close.values(), key=lambda x:x["pct"]):
            msg += f"{v['pair']} (<b><u>{v['pct']:.1f}%</u></b>)❌    --- ATH: (<b><u>+{v['peak_pct']:.1f}%</u></b>){'✅' if v['peak_pct']>0 else '❌'}\n"
    msg += f"\n🔴👈 ژمارا سێگنالان {total}\n"
    msg += f"👾👈 ئەنجمامێ 24 دەمژمێرێن چوی کو د سەرکەفتی بن دبێتە = {success_pct}%  💯\n"
    msg += "🚨تێبینی: هەتا ئەو سیگنالێت ATH ب plus (+) دابیت هەر بەسەرکەفتی دهێتە دانان ئەگەر خو ل قائیما LOSE یش دابیت.\n"
    msg += '❤️👾 Instagram 👉 <a href="https://www.instagram.com/azad__x__?igsh=MXgzdnZnMGo2NmZncA==">Azad_Bashqali</a>\n'
    msg += '👾❤️ TikTok 👉 <a href="https://www.tiktok.com/@azad_x__?_r=1&_t=ZS-95lY9xVauEX">Azad_X</a>\n'
    msg += "🔥👾 Telegram 👉 @Azad_X_01 By Guardex Quant LABs 🔥The Founder : Azad Smaeel Abdullah"
    send_message(msg, pin=True)
    broadcast_message(msg)
    log.info("📊 التقرير اليومي أُرسل")

# ═══════════════════════════════════════════════════
# SELL LOOP — كل 15 دقيقة عند إغلاق الشمعة
# ═══════════════════════════════════════════════════

def check_sell_symbol(symbol, fetch_func):
    with state_lock:
        if symbol not in active_buy: return

    closes = fetch_func(symbol)
    if not closes or len(closes) < 60: return

    price = closes[-1]
    with state_lock:
        if symbol in active_buy and price > active_buy[symbol].get("peak_price", 0):
            active_buy[symbol]["peak_price"] = price

    if not check_sell_signal(closes): return

    with state_lock:
        if symbol not in active_buy: return
        pair        = fmt_symbol(symbol)
        buy_price   = active_buy[symbol].get("buy_price", 0)
        peak_price  = active_buy[symbol].get("peak_price", 0)
        reply_map   = active_buy[symbol].get("reply_map", {})
        close_price = closes[-1]

    if buy_price > 0 and close_price > 0:
        cp = (close_price - buy_price) / buy_price * 100
        pp = (peak_price  - buy_price) / buy_price * 100
        if cp >= 0:
            sell_msg = (f"<b>{pair}</b>\nSELL NOW ❌\n"
                        f"ATH: {fmt_price(peak_price)} (<b><u>+{pp:.1f}%</u></b>) __ "
                        f"CLOSE: {fmt_price(close_price)} (<b><u>+{cp:.1f}%</u></b>)")
        else:
            sell_msg = (f"<b>{pair}</b>\nSELL NOW ❌🐹\n"
                        f"CLOSE: {fmt_price(close_price)} (<b><u>{cp:.1f}%</u></b>) __ "
                        f"ATH: {fmt_price(peak_price)} (<b><u>+{pp:.1f}%</u></b>)")
    else:
        sell_msg = f"<b>{pair}</b>\nSELL NOW ❌"

    broadcast_message(sell_msg, reply_to_map=reply_map)
    zv, zz = compute_zrti(closes)
    log.info(f"🔴 SELL: {symbol} | EMA5/13 + ZRTI={zv:.1f} z={zz:+.2f}")
    if buy_price > 0 and close_price > 0:
        record_sell_result(symbol, pair, buy_price, close_price, peak_price)
    with state_lock:
        active_buy.pop(symbol, None)


def sell_loop():
    while True:
        now  = datetime.utcnow()
        secs = (now.minute % 15) * 60 + now.second
        wait = (900 - secs) + 5
        log.info(f"⏳ [SELL] انتظار {wait:.0f}ث لإغلاق الشمعة...")
        time.sleep(wait)

        with state_lock:
            active_count = len(active_buy)
        if active_count == 0:
            continue

        log.info(f"🔍 [SELL] فحص {active_count} عملة نشطة")

        def _run(syms, fn):
            for s in syms:
                try: check_sell_symbol(s, fn)
                except Exception as e: log.warning(f"SELL {s}: {e}")

        threads = [
            Thread(target=_run, args=(BINANCE_SYMBOLS, get_closes_binance)),
            Thread(target=_run, args=(MEXC_SYMBOLS,    get_closes_mexc)),
            Thread(target=_run, args=(BYBIT_SYMBOLS,   get_closes_bybit)),
            Thread(target=_run, args=(GATE_SYMBOLS,    get_closes_gate)),
            Thread(target=_run, args=(KUCOIN_SYMBOLS,  get_closes_kucoin)),
        ]
        for t in threads: t.start()
        for t in threads: t.join()

# ═══════════════════════════════════════════════════
# BUY LOOP — كل 60 ثانية
# ═══════════════════════════════════════════════════

def check_buy_symbol(symbol, exchange, fetch_func, price_func):
    now = datetime.now()

    # تنظيف expired
    with state_lock:
        if symbol in active_buy:
            if now - active_buy[symbol]["buy_time"] > timedelta(hours=BUY_EXPIRY_HRS):
                log.info(f"🗑️ {symbol} حُذف (24h)")
                active_buy.pop(symbol, None)
                return
            # تحديث peak_price عبر السعر الفوري كل دقيقة
            cur = price_func(symbol)
            if cur and cur > active_buy[symbol].get("peak_price", 0):
                active_buy[symbol]["peak_price"] = cur
            return  # SELL loop يتكفل بالعملات النشطة

    with state_lock:
        in_pending = symbol in pending_buy
        entry      = dict(pending_buy[symbol]) if in_pending else None

    if not in_pending:
        # ── كشف كروس BUY جديد ──────────────────────────────
        closes = fetch_func(symbol)
        if not closes or len(closes) < EMA_SLOW + 5: return

        e11n, e11p = calc_ema_series(closes, EMA_FAST)
        e26n, e26p = calc_ema_series(closes, EMA_SLOW)
        if any(v is None for v in [e11n, e11p, e26n, e26p]): return

        bullish = (e11p <= e26p) and (e11n > e26n) and (e26n > e26p)

        with state_lock:
            ok = can_signal(symbol) and symbol not in active_buy
        if bullish and ok:
            price = closes[-1]
            with state_lock:
                pending_buy[symbol] = {"cross_price": price, "cross_time": now,
                                       "exchange": exchange, "checks": 0}
            log.info(f"⏳ BUY كروس: {symbol} @ {fmt_price(price)} [{exchange}]")
        return

    # ── العملة في pending ─────────────────────────────────────
    elapsed = (now - entry["cross_time"]).total_seconds() / 60
    if elapsed < WAIT_MINUTES:
        return   # انتظر 8 دقائق

    # ← جلب السعر الحالي فقط (سريع، بدون candles كاملة)
    current_price = price_func(symbol)
    if current_price is None:
        return

    with state_lock:
        if symbol not in pending_buy: return
        pending_buy[symbol]["checks"] += 1
        checks      = pending_buy[symbol]["checks"]
        cross_price = pending_buy[symbol]["cross_price"]

    if checks > MAX_CHECKS:
        log.info(f"⏰ انتهت المهلة: {symbol}")
        with state_lock: pending_buy.pop(symbol, None)
        return

    pct = (current_price - cross_price) / cross_price

    if pct < 0:
        # السعر انخفض عن سعر الكروس — إلغاء
        log.info(f"↩️ انعكاس: {symbol} ({pct*100:.2f}%)")
        with state_lock: pending_buy.pop(symbol, None)
        return

    if pct >= CONFIRM_MIN_PCT:
        # ✅ أرسل BUY فوراً بغض النظر عن الشمعة
        pair = fmt_symbol(symbol)
        msg  = (
            f"👇💱👾🔥💥🚀🌕💯💯\n\n"
            f"<b>{pair}</b>\n"
            f"BUY NOW ✅\n"
            f"Price: {fmt_price(current_price)}\n\n"
            f"⚠️ Be Careful and Don't be greedy — take your profits.\n"
            f"⚠️ گەلەک تەماع نەبە _ و فایدێ خو وەربگرە.\n\n"
            f"💸💵💴💰💹💲💱👾"
        )
        reply_map = broadcast_message(msg)
        log.info(f"✅ BUY: {symbol} @ {fmt_price(current_price)} (+{pct*100:.2f}%) [{exchange}]")
        with state_lock:
            active_buy[symbol] = {
                "buy_price" : current_price,
                "buy_time"  : now,
                "peak_price": current_price,
                "reply_map" : reply_map,
            }
            cooldown[symbol] = now
            pending_buy.pop(symbol, None)


def buy_loop():
    while True:
        with state_lock:
            pc = len(pending_buy); ac = len(active_buy)
        log.info(f"🔄 [BUY] pending={pc} | active={ac}")

        def _run(syms, exch, cfn, pfn, delay):
            for s in syms:
                try: check_buy_symbol(s, exch, cfn, pfn)
                except Exception as e: log.warning(f"BUY [{exch}] {s}: {e}")
                time.sleep(delay)

        threads = [
            Thread(target=_run, args=(BINANCE_SYMBOLS,"Binance",get_closes_binance,get_price_binance,REQUEST_DELAY["Binance"])),
            Thread(target=_run, args=(MEXC_SYMBOLS,   "MEXC",   get_closes_mexc,   get_price_mexc,   REQUEST_DELAY["MEXC"])),
            Thread(target=_run, args=(BYBIT_SYMBOLS,  "Bybit",  get_closes_bybit,  get_price_bybit,  REQUEST_DELAY["Bybit"])),
            Thread(target=_run, args=(GATE_SYMBOLS,   "Gate",   get_closes_gate,   get_price_gate,   REQUEST_DELAY["Gate"])),
            Thread(target=_run, args=(KUCOIN_SYMBOLS, "KuCoin", get_closes_kucoin, get_price_kucoin, REQUEST_DELAY["KuCoin"])),
        ]
        for t in threads: t.start()
        for t in threads: t.join()
        cleanup_cooldown()
        time.sleep(60)  # كل دقيقة

# ═══════════════════════════════════════════════════
# Telegram Updates
# ═══════════════════════════════════════════════════

def handle_user_message(user_id, text):
    if text == "/start":
        send_message_to_user(user_id, "أرسل كود التفعيل"); return
    code_data = get_code_from_db(text)
    if not code_data:
        send_message_to_user(user_id, "❌ كود غير صحيح"); return
    duration = code_data["duration_days"]
    table    = code_data.get("_table", "licenses_30")
    expire   = datetime.utcnow() + timedelta(days=duration)
    activate_user(user_id, expire)
    mark_code_used(text, user_id, table)
    send_message_to_user(user_id, f"✅ تم التفعيل لمدة {duration} يوم")

def handle_updates():
    last_id = None
    while True:
        params = {"timeout": 10}
        if last_id: params["offset"] = last_id + 1
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                             params=params, timeout=15).json()
            for u in r.get("result", []):
                last_id = u["update_id"]
                if "message" not in u: continue
                msg = u["message"]
                handle_user_message(msg["chat"]["id"], msg.get("text",""))
        except Exception as e:
            log.warning(f"handle_updates: {e}"); time.sleep(5)

# ═══════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════

def main():
    total = len(ALL_SYMBOLS)
    send_message(
        f"🚀 <b>البوت بدأ العمل</b>\n\n"
        f"📊 إجمالي العملات: <b>{total}</b>\n"
        f"🔷 Binance: {len(BINANCE_SYMBOLS)} | 🔶 MEXC: {len(MEXC_SYMBOLS)}\n"
        f"🟣 Bybit: {len(BYBIT_SYMBOLS)} | 🔵 Gate: {len(GATE_SYMBOLS)} | 🟢 KuCoin: {len(KUCOIN_SYMBOLS)}\n\n"
        f"📈 By Guardex Quant LABs | 15M Timeframe\n"
        f"✅ BUY : EMA 11/26 Cross → {WAIT_MINUTES}د + {CONFIRM_MIN_PCT*100:.1f}% ← فوري\n"
        f"🔴 SELL: EMA 5/13 Cross + ZRTI &gt; {ZRTI_SELL_THRESH} ← عند إغلاق الشمعة\n"
        f"⏱ Cooldown: {COOLDOWN_MIN}min"
    )
    log.info(f"✅ البوت يعمل — {total} عملة")

    Thread(target=handle_updates, daemon=True).start()
    Thread(target=buy_loop,       daemon=True).start()
    Thread(target=sell_loop,      daemon=True).start()

    global last_report
    while True:
        time.sleep(60)
        if datetime.now() - last_report >= timedelta(hours=24):
            send_daily_report()
            last_report = datetime.now()

if __name__ == "__main__":
    main()
