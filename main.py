import requests
import time
import logging
from datetime import datetime, timedelta
from threading import Thread, Lock

# ═══════════════════════════════════════════════════
# Logging — يحفظ كل شيء في ملف + يعرضه في الـ terminal
# ═══════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════
# إعداداتك الشخصية — غيّر هذين السطرين فقط
# ═══════════════════════════════════════════════════
import os
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")

# ═══════════════════════════════════════════════════
# Supabase Configuration (مهم: استخدم anon key فقط)
# ═══════════════════════════════════════════════════
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# ═══════════════════════════════════════════════════
# الإعدادات
# ═══════════════════════════════════════════════════
EMA_FAST        = 11      # EMA السريع
EMA_SLOW        = 26      # EMA البطيء
CONFIRM_MIN_PCT = 0.001   # 0.1% ارتفاع مطلوب للتأكيد
WAIT_CANDLES    = 1       # انتظار شمعتين (5 دقائق) قبل التأكيد
MAX_CHECKS      = 30      # أقصى محاولات تأكيد = 150 دقيقة
SELL_WAIT_MIN   = 5      # انتظار دقائق قبل SELL
BUY_EXPIRY_HRS  = 24      # حذف BUY بعد 24 ساعة
COOLDOWN_MIN    = 30      # cooldown بعد كل إشارة BUY

# تأخير مخصص لكل منصة (ثانية)
REQUEST_DELAY = {
    "Binance" : 0.15,
    "MEXC"    : 0.25,
    "Bybit"   : 0.25,
    "Gate"    : 0.35,
    "KuCoin"  : 0.35,
}

# ═══════════════════════════════════════════════════
# Sessions — session واحد لكل منصة يقلل latency
# ═══════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════
# Retry مع Backoff
# ═══════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════
# دوال جلب البيانات (كما هي)
# ═══════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════
# العملات لكل منصة
# ═══════════════════════════════════════════════════

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
    "XCNUSDT",     "COREUSDT",    "PIUSDT",      "XDCUSDT",     "RIOUSDT",
    "PLAYUSDT",    "STABLEUSDT",  "BLESSUSDT",   "COAIUSDT",    "CROSSUSDT",
    "FHEUSDT",     "GRASSUSDT",   "GRIFFAINUSDT","HUSDT",       "LIGHTUSDT",
    "ALEOUSDT",    "PINUSDT",     "PORT3USDT",   "KGENUSDT",    "ABUSDT",
    "ATHUSDT",     "ARCUSDT",     "AIOUSDT",     "A8USDT",      "ALUUSDT",
    "XPRUSDT",     "OMGUSDT",
]

BYBIT_SYMBOLS = [
    "UXLINKUSDT",  "KASUSDT",     "MNTUSDT",     "FLOCKUSDT",   "PAALUSDT",
    "L3USDT",      "ALCHUSDT",    "ZIGUSDT",     "MONUSDT",     "CSPRUSDT",
    "INSPUSDT",    "MOVEUSDT",    "COOKIEUSDT",  "LRCUSDT",     "ZROUSDT",
    "MOVRUSDT",    "TONUSDT",     "FETUSDT",     "SUIUSDT",     "GALAUSDT",
    "TAOUSDT",     "QNTUSDT",     "SANDUSDT",    "ETCUSDT",     "TNSRUSDT",
    "KAIAUSDT",    "PYTHUSDT",    "AIXBTUSDT",   "BLURUSDT",    "ZKUSDT",
    "JASMYUSDT",   "PARTIUSDT",   "THETAUSDT",   "BICOUSDT",    "POLUSDT",
]

GATE_SYMBOLS = [
    "AKTUSDT",     "RADUSDT",     "ALTUSDT",     "BATUSDT",     "MINAUSDT",
    "IDUSDT",      "MTLUSDT",     "BANDUSDT",    "ICXUSDT",     "STGUSDT",
    "PROVEUSDT",   "STXUSDT",     "SKLUSDT",     "GLMUSDT",     "XTZUSDT",
    "IQUSDT",      "HOTUSDT",     "LAUSDT",      "RLCUSDT",     "VANAUSDT",
    "BEAMUSDT",    "PONDUSDT",    "LPTUSDT",     "MIRAUSDT",    "GUSDT",
    "POWRUSDT",
]

KUCOIN_SYMBOLS = [
    "AIOZUSDT",    "DUSKUSDT",    "IOTXUSDT",    "MANTAUSDT",   "NIGHTUSDT",
    "CELRUSDT",    "ANKRUSDT",    "ENSUSDT",     "API3USDT",    "WUSDT",
    "MANAUSDT",    "CELOUSDT",    "EIGENUSDT",   "GASUSDT",     "ENJUSDT",
    "GMTUSDT",     "IOUSDT",      "KAITOUSDT",   "ACTUSDT",     "CHZUSDT",
    "DEXEUSDT",    "HNTUSDT",     "FLUXUSDT",    "PORTALUSDT",  "EDUUSDT",
    "IOSTUSDT",    "VETUSDT",
]

# SYMBOL_MAP
SYMBOL_MAP = {}
for sym in BINANCE_SYMBOLS: SYMBOL_MAP[sym] = ("KuCoin", get_closes_kucoin)
for sym in MEXC_SYMBOLS:    SYMBOL_MAP[sym] = ("MEXC",    get_closes_mexc)
for sym in BYBIT_SYMBOLS:   SYMBOL_MAP[sym] = ("Gate",    get_closes_gate)
for sym in GATE_SYMBOLS:    SYMBOL_MAP[sym] = ("Gate",    get_closes_gate)
for sym in KUCOIN_SYMBOLS:  SYMBOL_MAP[sym] = ("KuCoin",  get_closes_kucoin)

ALL_SYMBOLS = list(SYMBOL_MAP.keys())

# ═══════════════════════════════════════════════════
# قوائم الحالة + Lock
# ═══════════════════════════════════════════════════
pending_buy  = {}
active_buy   = {}
pending_sell = {}
cooldown     = {}
state_lock   = Lock()

# ═══════════════════════════════════════════════════
# Supabase Functions (محدثة بالكامل)
# ═══════════════════════════════════════════════════

def get_code_from_db(code):
    """
    يبحث في الجداول الأربعة بالترتيب:
    licenses_30 → licenses_90 → licenses_180 → licenses_360
    يرجع بيانات الكود مع مدته إذا وجد
    """
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
                row["duration_days"] = days   # أضف المدة تلقائياً
                row["_table"]        = table  # احفظ اسم الجدول للتحديث لاحقاً
                return row
        except Exception as e:
            log.warning(f"⚠️ خطأ في البحث بـ {table}: {e}")
    return None


def mark_code_used(code, user_id, table="licenses_30"):
    """يحدث الكود في جدوله الأصلي"""
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
    """UPSERT صحيح 100% باستخدام on_conflict (الطريقة الموصى بها في Supabase)"""
    url = f"{SUPABASE_URL}/rest/v1/users?on_conflict=user_id"
    
    payload = {
        "user_id": user_id,
        "expire_at": expire.isoformat(),
        "is_active": True
    }

    upsert_headers = HEADERS.copy()
    upsert_headers["Prefer"] = "resolution=merge-duplicates"

    r = requests.post(url, headers=upsert_headers, json=payload)
    
    if r.status_code not in (200, 201, 204):
        log.warning(f"⚠️ فشل تفعيل المستخدم {user_id} — status: {r.status_code}")
    else:
        log.info(f"✅ تم تفعيل/تحديث المستخدم {user_id} حتى {expire}")


def get_active_users():
    """يجلب فقط المستخدمين النشطين"""
    now = datetime.utcnow().isoformat()
    url = f"{SUPABASE_URL}/rest/v1/users?expire_at=gt.{now}&is_active=eq.true"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    return [u["user_id"] for u in data]


def send_message_to_user(user_id, text):
    """إرسال رسالة لمستخدم واحد"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": user_id,
        "text": text,
        "parse_mode": "HTML"
    }
    requests.post(url, data=data, timeout=8)


def broadcast_message(text):
    """إرسال لكل المستخدمين النشطين"""
    users = get_active_users()
    for user_id in users:
        try:
            send_message_to_user(user_id, text)
        except Exception as e:
            log.warning(f"⚠️ فشل إرسال لـ {user_id}: {e}")


# ═══════════════════════════════════════════════════
# Telegram (الدالة القديمة محتفظ بها للتقارير فقط)
# ═══════════════════════════════════════════════════
def send_message(text, reply_to=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to:
        data["reply_to_message_id"] = reply_to
    try:
        r = requests.post(url, data=data, timeout=5)
        res = r.json()
        if res.get("ok"):
            return res["result"]["message_id"]
    except Exception as e:
        log.error(f"❌ Telegram error: {e}", exc_info=True)
    return None

# ═══════════════════════════════════════════════════
# باقي الدوال (EMA, fmt, cooldown, check_symbol, daily report ...)
# ═══════════════════════════════════════════════════
def calc_ema_series(closes, period):
    if len(closes) < period + 1:
        return None, None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    prev = ema
    for price in closes[period:]:
        prev = ema
        ema = price * k + ema * (1 - k)
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
# المنطق الرئيسي (محدث: BUY + SELL يستخدمان broadcast)
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
        in_sell = symbol in pending_sell
        sell_age = (now - pending_sell[symbol]["sell_time"]).total_seconds() / 60 if in_sell else 0

    if in_sell and sell_age >= SELL_WAIT_MIN:
        closes = fetch_func(symbol)
        if closes and len(closes) >= EMA_SLOW + 2:
            ema11, _ = calc_ema_series(closes, EMA_FAST)
            ema26, _ = calc_ema_series(closes, EMA_SLOW)
            if ema11 is not None and ema26 is not None and ema11 < ema26:
                with state_lock:
                    pair = fmt_symbol(symbol)
                    buy_price = active_buy.get(symbol, {}).get("buy_price", 0)
                    peak_price = active_buy.get(symbol, {}).get("peak_price", 0)
                    close_price = closes[-1]

                if buy_price > 0 and close_price > 0:
                    close_pct = (close_price - buy_price) / buy_price * 100
                    peak_pct = (peak_price - buy_price) / buy_price * 100

                    if close_pct >= 0:
                        sell_msg = (
                            f"<b>{pair}</b>\n"
                            f"SELL NOW ❌\n"
                            f"ATH: {fmt_price(peak_price)} (<b><u>+{peak_pct:.1f}%</u></b>) __ "
                            f"CLOSE: {fmt_price(close_price)} (<b><u>+{close_pct:.1f}%</u></b>)"
                        )
                    else:
                        sell_msg = (
                            f"<b>{pair}</b>\n"
                            f"SELL NOW ❌\n"
                            f"CLOSE: {fmt_price(close_price)} (<b><u>{close_pct:.1f}%</u></b>)"
                        )
                else:
                    sell_msg = f"<b>{pair}</b>\nSELL NOW ❌"

                broadcast_message(sell_msg)
                log.info(f"🔴 SELL: {symbol}")

                if buy_price > 0 and close_price > 0:
                    record_sell_result(symbol, pair, buy_price, close_price)

        with state_lock:
            pending_sell.pop(symbol, None)
            active_buy.pop(symbol, None)
        return

    # جلب البيانات + التحليل (نفس الكود السابق)
    closes = fetch_func(symbol)
    if not closes or len(closes) < EMA_SLOW + 5:
        return

    price = closes[-1]

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

    # BUY كروس
    with state_lock:
        already_pending = symbol in pending_buy
        already_active = symbol in active_buy
        ok_cooldown = can_signal(symbol)

    if bullish and not already_pending and not already_active and ok_cooldown:
        with state_lock:
            pending_buy[symbol] = {"cross_price": price, "cross_time": now, "checks": 0}
        log.info(f"⏳ BUY كروس: {symbol} @ {fmt_price(price)} [{exchange}]")
        return

    # SELL كروس
    with state_lock:
        in_active = symbol in active_buy
        in_sell2 = symbol in pending_sell

    if bearish and in_active and not in_sell2:
        with state_lock:
            pending_sell[symbol] = {"sell_time": now}
            pending_buy.pop(symbol, None)
        log.info(f"⏳ SELL كروس: {symbol} — انتظار 10 دقائق")
        return

    # تأكيد BUY
    with state_lock:
        in_pending = symbol in pending_buy
        entry = dict(pending_buy[symbol]) if in_pending else None

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
        msg = (
            f"👇💱👾🔥💥🚀🌕💯💯\n\n"
            f"<b>{pair}</b>\n"
            f"BUY NOW ✅\n"
            f"Price: {fmt_price(price)}\n\n"
            f"⚠️ Be Careful and Don't be greedy — take your profits.\n"
            f"⚠️ گەلەک تەماع نەبە _ و فایدێ خو وەربگرە.\n\n"
            f"💸💵💴💰💹💲💱👾"
        )
        broadcast_message(msg)
        log.info(f"✅ BUY: {symbol} @ {fmt_price(price)} (+{pct*100:.2f}%) [{exchange}]")

        with state_lock:
            active_buy[symbol] = {
                "buy_price": price,
                "buy_time": now,
                "message_id": None,
                "peak_price": price,
            }
            cooldown[symbol] = now
            pending_buy.pop(symbol, None)

# ═══════════════════════════════════════════════════
# Daily Report + باقي الدوال
# ═══════════════════════════════════════════════════
daily_results = {}
daily_lock = Lock()
last_report = datetime.now()

def record_sell_result(symbol, pair, buy_price, close_price):
    with daily_lock:
        daily_results[symbol] = {
            "pair": pair,
            "buy_price": buy_price,
            "close_price": close_price,
            "pct": (close_price - buy_price) / buy_price * 100 if buy_price > 0 else 0,
        }

def send_daily_report():
    with daily_lock:
        if not daily_results:
            return
        results = dict(daily_results)
        daily_results.clear()

    wins = {s: v for s, v in results.items() if v["pct"] >= 0}
    loses = {s: v for s, v in results.items() if v["pct"] < 0}

    msg = "💲ئەنجامێ 24 دەمژمێرن چوی یێن کوینا ل قازانج و خساربونێ دا.💱⚡👾💯💯\n\n"

    if wins:
        msg += "Win💲🚀\n\n"
        for v in sorted(wins.values(), key=lambda x: x["pct"], reverse=True):
            msg += f"{v['pair']} (<b><u>+{v['pct']:.1f}%</u></b>)✅\n"
        msg += "____________________________\n\n"

    if loses:
        msg += "LOSE 🐹💔\n\n"
        for v in sorted(loses.values(), key=lambda x: x["pct"]):
            msg += f"{v['pair']} (<b><u>{v['pct']:.1f}%</u></b>)❌\n"

    send_message(msg)
    log.info("📊 تم إرسال التقرير اليومي")

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

    if code_data.get("used"):
        send_message_to_user(user_id, "❌ الكود مستخدم")
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
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 10}
        if last_update_id:
            params["offset"] = last_update_id + 1

        try:
            r = requests.get(url, params=params, timeout=15).json()
            for update in r.get("result", []):
                last_update_id = update["update_id"]
                if "message" not in update:
                    continue
                msg = update["message"]
                user_id = msg["chat"]["id"]
                text = msg.get("text", "")
                handle_user_message(user_id, text)
        except Exception as e:
            log.warning(f"⚠️ handle_updates error: {e}")
            time.sleep(5)


# ═══════════════════════════════════════════════════
# Candle Alignment
# ═══════════════════════════════════════════════════
def wait_for_candle_close():
    now = datetime.utcnow()
    secs = (now.minute % 5) * 60 + now.second
    wait = (300 - secs) + 5
    log.info(f"⏳ انتظار {wait:.0f} ثانية حتى إغلاق الشمعة...")
    time.sleep(wait)


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
        f"📈 EMA{EMA_FAST} / EMA{EMA_SLOW} | 5m Timeframe\n"
        f"✅ Confirmation: 0.2% | Cooldown: {COOLDOWN_MIN}min"
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
