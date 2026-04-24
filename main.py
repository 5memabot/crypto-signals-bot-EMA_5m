import requests
import time
import logging
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
CONFIRM_MIN_PCT = 0.0025
WAIT_CANDLES    = 2
MAX_CHECKS      = 30
SELL_WAIT_MIN   = 5
BUY_EXPIRY_HRS  = 24
COOLDOWN_MIN    = 30

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
                log.warning("⏸️  Rate limit — انتظار 10 ثوانٍ")
                time.sleep(10)
                continue
            if r.status_code != 200:
                log.warning(f"⚠️  HTTP {r.status_code} من {url}")
                continue
            return r
        except Exception as e:
            log.warning(f"⚠️  retry {attempt+1}: {e}")
    return None

def get_closes_binance(symbol, limit=152):
    try:
        r = retry_get(SESSION_BINANCE, "https://api.binance.com/api/v3/klines",
                      {"symbol": symbol, "interval": "5m", "limit": limit})
        if r is None: return None
        data = r.json()
        if isinstance(data, list) and len(data) >= EMA_SLOW + 5:
            closes = [float(c[4]) for c in data[:-1]]
            if all(isinstance(x, float) and x > 0 for x in closes[-5:]):
                return closes
    except Exception as e:
        log.warning(f"⚠️ Binance {symbol}: {e}")
    return None

def get_closes_mexc(symbol, limit=152):
    try:
        r = retry_get(SESSION_MEXC, "https://api.mexc.com/api/v3/klines",
                      {"symbol": symbol, "interval": "5m", "limit": limit})
        if r is None: return None
        data = r.json()
        if isinstance(data, list) and len(data) >= EMA_SLOW + 5:
            closes = [float(c[4]) for c in data[:-1]]
            if all(isinstance(x, float) and x > 0 for x in closes[-5:]):
                return closes
    except Exception as e:
        log.warning(f"⚠️ MEXC {symbol}: {e}")
    return None

def get_closes_bybit(symbol, limit=152):
    try:
        r = retry_get(SESSION_BYBIT, "https://api.bybit.com/v5/market/kline",
                      {"symbol": symbol, "interval": "5", "limit": limit, "category": "spot"})
        if r is None: return None
        data = r.json()
        if data.get("retCode") == 0:
            candles = data["result"]["list"]
            if len(candles) >= EMA_SLOW + 5:
                ordered = list(reversed(candles))
                closes = [float(c[4]) for c in ordered[:-1]]
                if all(isinstance(x, float) and x > 0 for x in closes[-5:]):
                    return closes
    except Exception as e:
        log.warning(f"⚠️ Bybit {symbol}: {e}")
    return None

def get_closes_gate(symbol, limit=152):
    try:
        pair = symbol.replace("USDT", "_USDT")
        r = retry_get(SESSION_GATE, "https://api.gateio.ws/api/v4/spot/candlesticks",
                      {"currency_pair": pair, "interval": "5m", "limit": limit})
        if r is None: return None
        data = r.json()
        if isinstance(data, list) and len(data) >= EMA_SLOW + 5:
            closes = [float(c[2]) for c in data[:-1]]
            if all(v > 0 for v in closes[-5:]):
                return closes
    except Exception as e:
        log.warning(f"⚠️ Gate {symbol}: {e}")
    return None

def get_closes_kucoin(symbol, limit=152):
    try:
        pair = symbol.replace("USDT", "-USDT")
        r = retry_get(SESSION_KUCOIN, "https://api.kucoin.com/api/v1/market/candles",
                      {"symbol": pair, "type": "5min"})
        if r is None: return None
        data = r.json()
        if data.get("code") == "200000" and data.get("data"):
            candles = data["data"][:limit]
            if len(candles) >= EMA_SLOW + 5:
                ordered = list(reversed(candles))
                closes = [float(c[2]) for c in ordered[:-1]]
                if all(isinstance(x, float) and x > 0 for x in closes[-5:]):
                    return closes
    except Exception as e:
        log.warning(f"⚠️ KuCoin {symbol}: {e}")
    return None

BINANCE_SYMBOLS = [
    "BTCUSDT",    "ETHUSDT",    "XRPUSDT",    "ADAUSDT",    "SOLUSDT",
    "DOTUSDT",    "DOGEUSDT",   "AVAXUSDT",   "LTCUSDT",    "LINKUSDT",
    "ATOMUSDT",   "XLMUSDT",    "FILUSDT",    "TRXUSDT",    "ALGOUSDT",
    "XMRUSDT",    "ICPUSDT",    "EGLDUSDT",   "HBARUSDT",   "NEARUSDT",
    "APEUSDT",    "DASHUSDT",   "ZILUSDT",    "ZECUSDT",    "ZENUSDT",
    "STORJUSDT",  "RAREUSDT",   "OPUSDT",     "ARBUSDT",    "SEIUSDT",
    "TIAUSDT",    "WLDUSDT",    "ORDIUSDT",   "RENDERUSDT", "PHAUSDT",
    "POLUSDT",    "TRBUSDT",    "VIRTUALUSDT","WALUSDT",    "APTUSDT",
    "BCHUSDT",    "BIOUSDT",    "CHRUSDT",    "GRTUSDT",    "ARKMUSDT",
    "AGLDUSDT",   "OPENUSDT",   "PLUMEUSDT",  "SAHARAUSDT", "SUSDT",
    "LINEAUSDT",  "XPLUSDT",
]

MEXC_SYMBOLS = [
    "XCNUSDT",      "COREUSDT",    "PIUSDT",      "XDCUSDT",     "RIOUSDT",
    "PLAYUSDT",     "STABLEUSDT",  "COAIUSDT",    "CROSSUSDT",   "GRASSUSDT",   
    "GRIFFAINUSDT", "HUSDT",       "KGENUSDT",    "ABUSDT",      "ATHUSDT",         
    "A8USDT",       "ALUUSDT",     "XPRUSDT",
    
]

BYBIT_SYMBOLS = [
    "KASUSDT",     "MNTUSDT",     "FLOCKUSDT",   "PARTIUSDT",  "THETAUSDT",
    "ALCHUSDT",    "MONUSDT",     "GALAUSDT",    "POLUSDT",    "BLURUSDT",
    "MOVEUSDT",    "COOKIEUSDT",  "LRCUSDT",     "ZROUSDT",    "AIXBTUSDT",
    "MOVRUSDT",    "TONUSDT",     "FETUSDT",     "SUIUSDT",    "PYTHUSDT", 
    "TAOUSDT",     "QNTUSDT",     "SANDUSDT",    "ETCUSDT",    "KAIAUSDT",
                           
]

GATE_SYMBOLS = [
    "AKTUSDT",     "ALTUSDT",     "BATUSDT",     "MINAUSDT",   "XTZUSDT",
    "IDUSDT",      "MTLUSDT",     "VANAUSDT",    "MIRAUSDT",   "LAUSDT",
    "PROVEUSDT",   "STXUSDT",     "POWRUSDT",    "BEAMUSDT",   "LPTUSDT",
                                     
]

KUCOIN_SYMBOLS = [
    "AIOZUSDT",    "IOTXUSDT",    "NIGHTUSDT",   "ARCUSDT",     "VETUSDT",
    "CELRUSDT",    "ANKRUSDT",    "ENSUSDT",     "API3USDT",    "WUSDT",
    "CELOUSDT",    "EIGENUSDT",   "ENJUSDT",     "HNTUSDT",     "DEXEUSDT",
    "GMTUSDT",     "IOUSDT",      "KAITOUSDT",   "CHZUSDT",
                               
]

SYMBOL_MAP = {}
for sym in BINANCE_SYMBOLS: SYMBOL_MAP[sym] = ("Binance", get_closes_binance)
for sym in MEXC_SYMBOLS:    SYMBOL_MAP[sym] = ("MEXC",   get_closes_mexc)
for sym in BYBIT_SYMBOLS:   SYMBOL_MAP[sym] = ("Bybit",   get_closes_bybit)
for sym in GATE_SYMBOLS:    SYMBOL_MAP[sym] = ("Gate",   get_closes_gate)
for sym in KUCOIN_SYMBOLS:  SYMBOL_MAP[sym] = ("KuCoin", get_closes_kucoin)

ALL_SYMBOLS = list(SYMBOL_MAP.keys())

pending_buy  = {}
active_buy   = {}
pending_sell = {}
cooldown     = {}
state_lock   = Lock()

# ═══════════════════════════════════════════════════
# Supabase
# ═══════════════════════════════════════════════════

def get_code_from_db(code):
    tables = [
        ("licenses_30",  30),
        ("licenses_90",  90),
        ("licenses_180", 180),
        ("licenses_360", 360),
    ]
    for table, days in tables:
        url = f"{SUPABASE_URL}/rest/v1/{table}?code=eq.{code}&status=eq.free&limit=1"
        try:
            r    = requests.get(url, headers=HEADERS, timeout=5)
            data = r.json()
            if data and isinstance(data, list) and len(data) > 0:
                row = data[0]
                row["duration_days"] = days
                row["_table"]        = table
                return row
        except Exception as e:
            log.warning(f"⚠️ خطأ في البحث بـ {table}: {e}")
    return None

def mark_code_used(code, user_id, table="licenses_30"):
    url = f"{SUPABASE_URL}/rest/v1/{table}?code=eq.{code}"
    payload = {
        "status"  : "used",
        "used_by" : user_id,
        "used_at" : datetime.utcnow().isoformat()
    }
    r = requests.patch(url, headers=HEADERS, json=payload)
    if r.status_code != 204:
        log.warning(f"❌ فشل تحديث الكود {code} في {table} — status: {r.status_code}")

def activate_user(user_id, expire):
    url = f"{SUPABASE_URL}/rest/v1/users?on_conflict=user_id"
    payload = {
        "user_id"  : user_id,
        "expire_at": expire.isoformat(),
        "is_active": True
    }
    upsert_headers = HEADERS.copy()
    upsert_headers["Prefer"] = "resolution=merge-duplicates"
    r = requests.post(url, headers=upsert_headers, json=payload)
    if r.status_code not in (200, 201, 204):
        log.warning(f"⚠️ فشل تفعيل المستخدم {user_id} — status: {r.status_code}")
    else:
        log.info(f"✅ تم تفعيل المستخدم {user_id} حتى {expire}")

def get_active_users():
    now = datetime.utcnow().isoformat()
    url = f"{SUPABASE_URL}/rest/v1/users?expire_at=gt.{now}&is_active=eq.true"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    return [u["user_id"] for u in data] if isinstance(data, list) else []

# ═══════════════════════════════════════════════════
# Telegram
# ═══════════════════════════════════════════════════

def send_message_to_user(user_id, text, reply_to=None):
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": user_id, "text": text, "parse_mode": "HTML"}
    if reply_to:
        data["reply_to_message_id"] = reply_to
    try:
        r   = requests.post(url, data=data, timeout=8)
        res = r.json()
        if res.get("ok"):
            return res["result"]["message_id"]
    except Exception as e:
        log.warning(f"⚠️ فشل إرسال لـ {user_id}: {e}")
    return None

def broadcast_message(text, reply_to_map=None):
    """
    يرسل لكل المستخدمين النشطين.
    reply_to_map: dict { user_id: message_id } للرد على رسائل BUY
    """
    users = get_active_users()
    result_map = {}
    for user_id in users:
        try:
            reply_to = (reply_to_map or {}).get(user_id)
            msg_id   = send_message_to_user(user_id, text, reply_to=reply_to)
            if msg_id:
                result_map[user_id] = msg_id
        except Exception as e:
            log.warning(f"⚠️ فشل broadcast لـ {user_id}: {e}")
    return result_map

def send_message(text, reply_to=None, pin=False):
    """إرسال للـ CHAT_ID الرئيسي (للتقارير والإشعارات)"""
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to:
        data["reply_to_message_id"] = reply_to
    try:
        r   = requests.post(url, data=data, timeout=5)
        res = r.json()
        if res.get("ok"):
            msg_id = res["result"]["message_id"]
            # تثبيت رسالة التقرير اليومي لتبقى للأبد
            if pin:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/pinChatMessage",
                    data={"chat_id": CHAT_ID, "message_id": msg_id, "disable_notification": True},
                    timeout=5,
                )
            return msg_id
    except Exception as e:
        log.error(f"❌ Telegram error: {e}", exc_info=True)
    return None

# ═══════════════════════════════════════════════════
# EMA + تنسيق
# ═══════════════════════════════════════════════════

def calc_ema_series(closes, period):
    if len(closes) < period + 1:
        return None, None
    k    = 2 / (period + 1)
    ema  = sum(closes[:period]) / period
    prev = ema
    for price in closes[period:]:
        prev = ema
        ema  = price * k + ema * (1 - k)
    return ema, prev

def fmt_symbol(symbol):
    if symbol.endswith("USDT"):
        return symbol[:-4] + "/USDT"
    return symbol

def fmt_price(price):
    if price >= 1000:
        return f"{price:,.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    else:
        return f"{price:.6f}"

def can_signal(symbol):
    if symbol not in cooldown:
        return True
    return (datetime.now() - cooldown[symbol]) >= timedelta(minutes=COOLDOWN_MIN)

def cleanup_cooldown():
    with state_lock:
        for sym in list(cooldown):
            if datetime.now() - cooldown[sym] > timedelta(hours=2):
                del cooldown[sym]

# ═══════════════════════════════════════════════════
# Daily Report
# ═══════════════════════════════════════════════════
daily_results = {}
daily_lock    = Lock()
last_report   = datetime.now()

def record_sell_result(symbol, pair, buy_price, close_price, peak_price):
    with daily_lock:
        daily_results[symbol] = {
            "pair"       : pair,
            "buy_price"  : buy_price,
            "close_price": close_price,
            "peak_price" : peak_price,
            "pct"        : (close_price - buy_price) / buy_price * 100 if buy_price > 0 else 0,
            "peak_pct"   : (peak_price  - buy_price) / buy_price * 100 if buy_price > 0 else 0,
        }

def send_daily_report():
    with daily_lock:
        if not daily_results:
            return
        results = dict(daily_results)
        daily_results.clear()

    total      = len(results)
    # صفقة ناجحة = ATH > 0
    win_ath    = {s: v for s, v in results.items() if v["peak_pct"] > 0}
    lose_ath   = {s: v for s, v in results.items() if v["peak_pct"] <= 0}
    wins_close = {s: v for s, v in results.items() if v["pct"] >= 0}
    loses_close= {s: v for s, v in results.items() if v["pct"] < 0}

    success_pct = int(len(win_ath) / total * 100) if total > 0 else 0

    msg = "💲ئەنجامێ 24 دەمژمێرێن چوی یێن کوینا ل قازانج و خساربونێ دا.💱⚡👾💯💯\n\n"

    if wins_close:
        msg += "Win💲🚀\n\n"
        for v in sorted(wins_close.values(), key=lambda x: x["pct"], reverse=True):
            ath_icon = "✅" if v["peak_pct"] > 0 else "❌"
            msg += (
                f"{v['pair']} (<b><u>+{v['pct']:.1f}%</u></b>)✅"
                f"   --- ATH: (<b><u>+{v['peak_pct']:.1f}%</u></b>){ath_icon}\n"
            )
        msg += "____________________________\n\n"

    if loses_close:
        msg += "LOSE 🐹💔\n\n"
        for v in sorted(loses_close.values(), key=lambda x: x["pct"]):
            ath_icon = "✅" if v["peak_pct"] > 0 else "❌"
            msg += (
                f"{v['pair']} (<b><u>{v['pct']:.1f}%</u></b>)❌"
                f"    --- ATH: (<b><u>+{v['peak_pct']:.1f}%</u></b>){ath_icon}\n"
            )

    msg += f"\n🔴👈 ژمارا سێگنالان {total}\n"
    msg += f"👾👈 ئەنجمامێ 24 دەمژمێرێن چوی کو د سەرکەفتی بن دبێتە = {success_pct}%  💯\n"
    msg += "🚨تێبینی: هەتا ئەو سیگنالێت ATH ب plus (+) دابیت هەر بەسەرکەفتی دهێتە دانان ئەگەر خو ل قائیما LOSE یش دابیت.\n"
    msg += '❤️👾 Instagram 👉 <a href="https://www.instagram.com/azad__x__?igsh=MXgzdnZnMGo2NmZncA==">Azad_Bashqali</a>\n'
    msg += '👾❤️ TikTok 👉 <a href="https://www.tiktok.com/@azad_x__?_r=1&_t=ZS-95lY9xVauEX">Azad_X</a>\n'
    msg += "🔥👾 Telegram 👉 @Azad_X_01"
    msg += " By Guardex Quant LABs "
    msg += "🔥The Founder : Azad Smaeel Abdullah"


    # إرسال مع تثبيت للـ admin + broadcast للمستخدمين
    send_message(msg, pin=True)
    broadcast_message(msg)
    log.info("📊 تم إرسال التقرير اليومي")

# ═══════════════════════════════════════════════════
# المنطق الرئيسي
# ═══════════════════════════════════════════════════

def check_symbol(symbol, exchange, fetch_func):
    now = datetime.now()

    # تنظيف BUY المنتهية
    with state_lock:
        if symbol in active_buy:
            if now - active_buy[symbol]["buy_time"] > timedelta(hours=BUY_EXPIRY_HRS):
                log.info(f"🗑️  {symbol} حُذف (24h)")
                del active_buy[symbol]
                pending_sell.pop(symbol, None)
                return

    # SELL معلق
    with state_lock:
        in_sell  = symbol in pending_sell
        sell_age = (now - pending_sell[symbol]["sell_time"]).total_seconds() / 60 if in_sell else 0

    if in_sell and sell_age >= SELL_WAIT_MIN:
        closes = fetch_func(symbol)
        if closes and len(closes) >= EMA_SLOW + 2:
            ema11, _ = calc_ema_series(closes, EMA_FAST)
            ema26, _ = calc_ema_series(closes, EMA_SLOW)
            if ema11 is not None and ema26 is not None and ema11 < ema26:
                with state_lock:
                    pair        = fmt_symbol(symbol)
                    buy_price   = active_buy.get(symbol, {}).get("buy_price", 0)
                    peak_price  = active_buy.get(symbol, {}).get("peak_price", 0)
                    reply_map   = active_buy.get(symbol, {}).get("reply_map", {})
                    close_price = closes[-1]

                if buy_price > 0 and close_price > 0:
                    close_pct = (close_price - buy_price) / buy_price * 100
                    peak_pct  = (peak_price  - buy_price) / buy_price * 100

                    if close_pct >= 0:
                        # ربح — ATH + CLOSE
                        sell_msg = (
                            f"<b>{pair}</b>\n"
                            f"SELL NOW ❌\n"
                            f"ATH: {fmt_price(peak_price)} (<b><u>+{peak_pct:.1f}%</u></b>) __ "
                            f"CLOSE: {fmt_price(close_price)} (<b><u>+{close_pct:.1f}%</u></b>)"
                        )
                    else:
                        # خسارة — CLOSE + ATH
                        sell_msg = (
                            f"<b>{pair}</b>\n"
                            f"SELL NOW ❌🐹\n"
                            f"CLOSE: {fmt_price(close_price)} (<b><u>{close_pct:.1f}%</u></b>) __ "
                            f"ATH: {fmt_price(peak_price)} (<b><u>+{peak_pct:.1f}%</u></b>)"
                        )
                else:
                    sell_msg = f"<b>{pair}</b>\nSELL NOW ❌"

                # إرسال كـ reply لكل مستخدم على رسالة BUY الخاصة به
                broadcast_message(sell_msg, reply_to_map=reply_map)
                log.info(f"🔴 SELL: {symbol}")

                if buy_price > 0 and close_price > 0:
                    record_sell_result(symbol, pair, buy_price, close_price, peak_price)

        with state_lock:
            pending_sell.pop(symbol, None)
            active_buy.pop(symbol, None)
        return

    # جلب البيانات
    closes = fetch_func(symbol)
    if not closes or len(closes) < EMA_SLOW + 5:
        return

    price = closes[-1]

    # تحديث peak_price
    with state_lock:
        if symbol in active_buy:
            if price > active_buy[symbol].get("peak_price", 0):
                active_buy[symbol]["peak_price"] = price

    ema11_now, ema11_prev = calc_ema_series(closes, EMA_FAST)
    ema26_now, ema26_prev = calc_ema_series(closes, EMA_SLOW)

    if any(v is None for v in [ema11_now, ema11_prev, ema26_now, ema26_prev]):
        return

    bullish = (ema11_prev <= ema26_prev) and (ema11_now > ema26_now) and (ema26_now > ema26_prev)
    bearish = (ema11_prev >= ema26_prev) and (ema11_now < ema26_now)

    with state_lock:
        already_pending = symbol in pending_buy
        already_active  = symbol in active_buy
        ok_cooldown     = can_signal(symbol)

    if bullish and not already_pending and not already_active and ok_cooldown:
        with state_lock:
            pending_buy[symbol] = {"cross_price": price, "cross_time": now, "checks": 0}
        log.info(f"⏳ BUY كروس: {symbol} @ {fmt_price(price)} [{exchange}]")
        return

    with state_lock:
        in_active = symbol in active_buy
        in_sell2  = symbol in pending_sell

    if bearish and in_active and not in_sell2:
        with state_lock:
            pending_sell[symbol] = {"sell_time": now}
            pending_buy.pop(symbol, None)
        log.info(f"⏳ SELL كروس: {symbol} — انتظار {SELL_WAIT_MIN} دقائق")
        return

    with state_lock:
        in_pending = symbol in pending_buy
        entry      = dict(pending_buy[symbol]) if in_pending else None

    if not in_pending:
        return

    elapsed = int((now - entry["cross_time"]).total_seconds() / 300)
    if elapsed < WAIT_CANDLES:
        return

    with state_lock:
        if symbol in pending_buy:
            pending_buy[symbol]["checks"] += 1
            checks = pending_buy[symbol]["checks"]
        else:
            return

    if checks > MAX_CHECKS:
        log.info(f"⏰ انتهت المهلة: {symbol}")
        with state_lock:
            pending_buy.pop(symbol, None)
        return

    if ema11_now is not None and ema26_now is not None and ema11_now < ema26_now:
        log.info(f"↩️  كروس عكسي: {symbol}")
        with state_lock:
            pending_buy.pop(symbol, None)
        return

    pct = (price - entry["cross_price"]) / entry["cross_price"]
    if pct >= CONFIRM_MIN_PCT:
        pair = fmt_symbol(symbol)
        msg  = (
            f"👇💱👾🔥💥🚀🌕💯💯\n\n"
            f"<b>{pair}</b>\n"
            f"BUY NOW ✅\n"
            f"Price: {fmt_price(price)}\n\n"
            f"⚠️ Be Careful and Don't be greedy — take your profits.\n"
            f"⚠️ گەلەک تەماع نەبە _ و فایدێ خو وەربگرە.\n\n"
            f"💸💵💴💰💹💲💱👾"
        )
        # broadcast وتخزين reply_map لكل مستخدم
        reply_map = broadcast_message(msg)
        log.info(f"✅ BUY: {symbol} @ {fmt_price(price)} (+{pct*100:.2f}%) [{exchange}]")

        with state_lock:
            active_buy[symbol] = {
                "buy_price" : price,
                "buy_time"  : now,
                "peak_price": price,
                "reply_map" : reply_map,  # { user_id: message_id }
            }
            cooldown[symbol] = now
            pending_buy.pop(symbol, None)

# ═══════════════════════════════════════════════════
# scan_all
# ═══════════════════════════════════════════════════

def run_exchange(symbols, exchange, fetch_func):
    delay = REQUEST_DELAY.get(exchange, 0.25)
    for sym in symbols:
        try:
            check_symbol(sym, exchange, fetch_func)
        except Exception as e:
            log.warning(f"⚠️  [{exchange}] {sym}: {e}")
        time.sleep(delay)

def scan_all():
    exchanges = [
        (BINANCE_SYMBOLS, "Binance", get_closes_binance),
        (MEXC_SYMBOLS,    "MEXC",    get_closes_mexc),
        (BYBIT_SYMBOLS,   "Bybit",   get_closes_bybit),
        (GATE_SYMBOLS,    "Gate",    get_closes_gate),
        (KUCOIN_SYMBOLS,  "KuCoin",  get_closes_kucoin),
    ]
    threads = [Thread(target=run_exchange, args=(syms, exch, fn)) for syms, exch, fn in exchanges]
    for t in threads: t.start()
    for t in threads: t.join()

# ═══════════════════════════════════════════════════
# استقبال المستخدمين
# ═══════════════════════════════════════════════════

def handle_user_message(user_id, text):
    if text == "/start":
        send_message_to_user(user_id, "أرسل كود التفعيل")
        return

    code_data = get_code_from_db(text)

    if not code_data:
        send_message_to_user(user_id, "❌ كود غير صحيح")
        return

    duration = code_data["duration_days"]
    table    = code_data.get("_table", "licenses_30")
    expire   = datetime.utcnow() + timedelta(days=duration)

    activate_user(user_id, expire)
    mark_code_used(text, user_id, table)
    send_message_to_user(user_id, f"✅ تم التفعيل لمدة {duration} يوم")

def handle_updates():
    last_update_id = None
    while True:
        url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 10}
        if last_update_id:
            params["offset"] = last_update_id + 1
        try:
            r = requests.get(url, params=params, timeout=15).json()
            for update in r.get("result", []):
                last_update_id = update["update_id"]
                if "message" not in update:
                    continue
                msg     = update["message"]
                user_id = msg["chat"]["id"]
                text    = msg.get("text", "")
                handle_user_message(user_id, text)
        except Exception as e:
            log.warning(f"⚠️ handle_updates error: {e}")
            time.sleep(5)

# ═══════════════════════════════════════════════════
# Candle Alignment
# ═══════════════════════════════════════════════════

def wait_for_candle_close():
    now  = datetime.utcnow()
    secs = (now.minute % 5) * 60 + now.second
    wait = (300 - secs) + 5
    log.info(f"⏳ انتظار {wait:.0f} ثانية حتى إغلاق الشمعة...")
    time.sleep(wait)

# ═══════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════

def main():
    Thread(target=handle_updates, daemon=True).start()

    total = len(ALL_SYMBOLS)
    send_message(
        f"🚀 <b>البوت بدأ العمل</b>\n\n"
        f"📊 إجمالي العملات: <b>{total}</b>\n"
        f"🔷 Binance: {len(BINANCE_SYMBOLS)}\n"
        f"🔶 MEXC:    {len(MEXC_SYMBOLS)}\n"
        f"🟣 Bybit:   {len(BYBIT_SYMBOLS)}\n"
        f"🔵 Gate:    {len(GATE_SYMBOLS)}\n"
        f"🟢 KuCoin:  {len(KUCOIN_SYMBOLS)}\n\n"
        f"📈 By Guardex Quant LABs |           5m Timeframe\n"
        f"✅ Confirmation: 0.1% | Cooldown: {COOLDOWN_MIN}min"
    )
    log.info(f"✅ البوت يعمل — {total} عملة على 5 منصات")

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.now().strftime("%H:%M:%S")
        log.info(f"{'═'*55}")
        log.info(f"🔍 دورة #{cycle} | {ts} | {total} عملة")
        with state_lock:
            log.info(f"⏳pending={len(pending_buy)} | ✅active={len(active_buy)} | 🔴sell={len(pending_sell)}")

        wait_for_candle_close()
        scan_all()
        cleanup_cooldown()

        global last_report
        if datetime.now() - last_report >= timedelta(hours=24):
            send_daily_report()
            last_report = datetime.now()

if __name__ == "__main__":
    main()
