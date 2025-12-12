import os
import time
import json
import logging
import signal
from datetime import datetime, date, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ============================================================
# Constants / ENV
# ============================================================
BINANCE_FAPI_URL = "https://fapi.binance.com"
TELEGRAM_API_URL = "https://api.telegram.org"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_ADMIN_ID = (os.environ.get("TG_ADMIN_ID", "") or "").strip().strip('"').strip("'")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

# Optional: if you still store some default chat for broadcasting (NOT required)
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()

# Optional: FOMC dates can be added later (UTC datetimes)
FOMC_DATES_UTC: List[datetime] = []

# ============================================================
# Bot Configuration (Smooth + Anti-spike)
# ============================================================
CONFIG: Dict[str, Any] = {
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "15m",
    "SCAN_INTERVAL_SECONDS": 600,

    "MAX_SIGNALS_PER_DAY": 7,
    "MAX_SIGNALS_PER_SCAN": 1,

    "MIN_QUOTE_VOLUME": 50_000_000,
    "RISK_REWARD": 1.7,

    # ATR filter (avoid "pila")
    "MIN_ATR_PCT": 0.25,
    "MAX_ATR_PCT": 5.0,

    # Anti-spike stop: extremum ± (k * ATR_abs)
    "STOP_ATR_MULTIPLIER": 0.6,   # can be 0.8 from admin panel

    # Safety bounds for stop distance (from entry, %)
    "MIN_STOP_PCT": 0.20,
    "MAX_STOP_PCT": 1.50,

    # Cooldown for the same symbol
    "SYMBOL_COOLDOWN_SECONDS": 1800,

    # BTC context filter
    "BTC_FILTER_ENABLED": True,
}

# ============================================================
# State
# ============================================================
class SignalState:
    def __init__(self) -> None:
        self.signals_sent_today: int = 0
        self.total_signals_sent: int = 0
        self.last_reset_date: date = date.today()
        self.symbol_last_signal_ts: Dict[str, float] = {}
        self.risk_off: bool = False

    def reset_if_new_day(self) -> None:
        today = date.today()
        if today != self.last_reset_date:
            logging.info("Новый день, обнуляем счётчики сигналов.")
            self.signals_sent_today = 0
            self.last_reset_date = today

    def can_send_signal(self, symbol: str) -> bool:
        self.reset_if_new_day()
        if self.signals_sent_today >= CONFIG["MAX_SIGNALS_PER_DAY"]:
            return False
        now = time.time()
        last_ts = self.symbol_last_signal_ts.get(symbol)
        if last_ts is not None and now - last_ts < CONFIG["SYMBOL_COOLDOWN_SECONDS"]:
            return False
        return True

    def register_signal(self, symbol: str) -> None:
        self.signals_sent_today += 1
        self.total_signals_sent += 1
        self.symbol_last_signal_ts[symbol] = time.time()

    def is_risk_off(self) -> bool:
        return self.risk_off

    def set_risk_off(self, value: bool) -> None:
        self.risk_off = value


STATE = SignalState()

# ============================================================
# Helpers: Admin / Command normalisation
# ============================================================
def is_admin_user(user_id: str) -> bool:
    admin = (TG_ADMIN_ID or "").strip().strip('"').strip("'")
    return bool(admin) and str(user_id) == admin

def normalize_command(text: str) -> str:
    """
    Normalizes text received from reply-keyboard buttons.
    Removes emojis and extra spaces, lowercases the command.
    """
    if not text:
        return ""
    t = text.strip()

    # Remove common emojis used in buttons (safe, even if not present)
    emojis = [
        "🚀", "📊", "ℹ️", "ℹ", "📴", "🛠", "⚙️", "🆔",
        "✅", "🛑", "👥", "💹", "🧪", "⏱", "🎯",
    ]
    for em in emojis:
        t = t.replace(em, "")
    t = " ".join(t.split())  # collapse multiple spaces
    return t.strip().lower()

# ============================================================
# DB
# ============================================================
DB_CONN = None

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = True
    return conn

def db_ensure_tables() -> None:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id BIGINT PRIMARY KEY,
                is_admin BOOLEAN NOT NULL DEFAULT FALSE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    logging.info("Таблица subscribers проверена/создана.")

def db_add_or_update_subscriber(chat_id: str, is_admin: bool) -> None:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute(
            """
            INSERT INTO subscribers (chat_id, is_admin, is_active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (chat_id)
            DO UPDATE SET
                is_admin = EXCLUDED.is_admin,
                is_active = TRUE,
                last_seen_at = NOW();
            """,
            (int(chat_id), is_admin),
        )
    logging.info("Добавлен/обновлён подписчик: %s (admin=%s)", chat_id, is_admin)

def db_unsubscribe(chat_id: str) -> None:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute(
            """
            UPDATE subscribers
            SET is_active = FALSE, last_seen_at = NOW()
            WHERE chat_id = %s;
            """,
            (int(chat_id),),
        )
    logging.info("Подписчик %s помечен как неактивный.", chat_id)

def db_get_active_subscribers() -> List[int]:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute("SELECT chat_id FROM subscribers WHERE is_active = TRUE;")
        rows = cur.fetchall()
    return [int(r["chat_id"]) for r in rows]

def db_get_subscribers_count() -> int:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM subscribers WHERE is_active = TRUE;")
        row = cur.fetchone()
    return int(row["c"]) if row else 0

# ============================================================
# Telegram API wrappers
# ============================================================
def send_telegram_request(method: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не задан.")
        return None
    url = f"{TELEGRAM_API_URL}/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=data, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error("Ошибка запроса к Telegram (%s): %s", method, e)
        return None

def send_telegram_message(
    text: str,
    chat_id: str,
    html: bool = False,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> None:
    payload: Dict[str, Any] = {
        "chat_id": int(chat_id),
        "text": text,
        "disable_web_page_preview": True,
    }
    if html:
        payload["parse_mode"] = "HTML"
    if reply_markup:
        payload["reply_markup"] = reply_markup
    res = send_telegram_request("sendMessage", payload)
    if res and res.get("ok"):
        logging.info("Сообщение отправлено: chat_id=%s", chat_id)
    else:
        logging.error("Ошибка отправки в Telegram: %s", res)

def edit_telegram_message(
    chat_id: str,
    message_id: int,
    text: str,
    html: bool = False,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> None:
    payload: Dict[str, Any] = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "text": text,
        "disable_web_page_preview": True,
    }
    if html:
        payload["parse_mode"] = "HTML"
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    send_telegram_request("editMessageText", payload)

def answer_callback_query(callback_query_id: str, text: str = "", show_alert: bool = False) -> None:
    payload = {
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": show_alert,
    }
    send_telegram_request("answerCallbackQuery", payload)

# ============================================================
# Reply keyboard (RU) — user vs admin
# ============================================================
def get_reply_keyboard(chat_id: str, user_id: Optional[str] = None) -> Dict[str, Any]:
    uid = str(user_id) if user_id is not None else str(chat_id)
    admin = is_admin_user(uid)

    rows = [
        [{"text": "🚀 Старт"}, {"text": "📊 Статус"}],
        [{"text": "ℹ️ Помощь"}, {"text": "📴 Стоп"}],
        [{"text": "🆔 Мой ID"}],
    ]
    if admin:
        rows.append([{"text": "🛠 Админ"}, {"text": "⚙️ Настройки"}])

    return {
        "keyboard": rows,
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }

# ============================================================
# Admin inline panel (RU)
# ============================================================
def admin_inline_panel() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Статус", "callback_data": "admin:status"},
                {"text": "👥 Подписчики", "callback_data": "admin:subs"},
            ],
            [
                {"text": "🛑 Risk OFF", "callback_data": "admin:risk_off"},
                {"text": "✅ Risk ON", "callback_data": "admin:risk_on"},
            ],
            [
                {"text": "💹 BTC фильтр: ON/OFF", "callback_data": "admin:btc_toggle"},
                {"text": "🧪 Тест-скан", "callback_data": "admin:test"},
            ],
            [
                {"text": "⏱ 5 мин", "callback_data": "admin:int_300"},
                {"text": "⏱ 10 мин", "callback_data": "admin:int_600"},
                {"text": "⏱ 15 мин", "callback_data": "admin:int_900"},
            ],
            [
                {"text": "🎯 k=0.6", "callback_data": "admin:k_06"},
                {"text": "🎯 k=0.8", "callback_data": "admin:k_08"},
            ],
        ]
    }

def admin_status_text() -> str:
    subs_count = db_get_subscribers_count()
    risk_off_state = "ON" if STATE.is_risk_off() else "OFF"
    btc_state = "ON" if CONFIG["BTC_FILTER_ENABLED"] else "OFF"
    return (
        "<b>🛠 Админ-панель</b>\n\n"
        f"👥 Подписчиков: {subs_count}\n"
        f"📌 Сигналы сегодня: {STATE.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}\n"
        f"🛑 Risk OFF: {risk_off_state}\n"
        f"💹 BTC фильтр: {btc_state}\n\n"
        f"⏱ Интервал сканирования: {CONFIG['SCAN_INTERVAL_SECONDS']} сек\n"
        f"📈 TF: {CONFIG['TIMEFRAME']} (MTF: {CONFIG['HTF_TIMEFRAME']})\n"
        f"💰 Мин. объём: {CONFIG['MIN_QUOTE_VOLUME']:,} USDT\n"
        f"🔥 ATR min/max: {CONFIG['MIN_ATR_PCT']}–{CONFIG['MAX_ATR_PCT']}%\n"
        f"🎯 k (ATR-стоп): {CONFIG.get('STOP_ATR_MULTIPLIER', 0.6)}"
    )

def admin_subs_text() -> str:
    subs = db_get_active_subscribers()
    lines = ["<b>👥 Подписчики</b>", ""]
    if not subs:
        lines.append("Нет активных подписчиков.")
    else:
        for cid in subs[:60]:
            lines.append(f"• <code>{cid}</code>")
        if len(subs) > 60:
            lines.append(f"\n…и ещё {len(subs) - 60}")
    return "\n".join(lines)

# ============================================================
# Binance helpers
# ============================================================
def fetch_binance(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{BINANCE_FAPI_URL}{path}"
    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=12)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            delay = 2 ** attempt
            logging.error(
                "Ошибка запроса к %s (попытка %d/5): %s. Ждём %d сек...",
                path,
                attempt + 1,
                e,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"Не удалось получить данные с Binance: {path}")

def get_usdt_perp_symbols() -> List[str]:
    data = fetch_binance("/fapi/v1/exchangeInfo")
    symbols = []
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    logging.info("Найдено %d USDT-M PERPETUAL символов", len(symbols))
    return symbols

def get_24h_volume_filter(symbols: List[str]) -> List[str]:
    tickers = fetch_binance("/fapi/v1/ticker/24hr")
    vol_map: Dict[str, float] = {}
    for t in tickers:
        s = t.get("symbol")
        if s in symbols:
            try:
                vol_map[s] = float(t.get("quoteVolume", 0.0))
            except Exception:
                continue
    filtered = [s for s in symbols if vol_map.get(s, 0.0) >= CONFIG["MIN_QUOTE_VOLUME"]]
    logging.info("После фильтра по объёму (>= %s): %d символов", f"{CONFIG['MIN_QUOTE_VOLUME']:,} USDT", len(filtered))
    return filtered

# ============================================================
# Indicators
# ============================================================
def kline_to_floats(klines: List[List[Any]]) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    timestamps: List[float] = []
    for k in klines:
        ts = k[0]
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])
        c = float(k[4])
        timestamps.append(ts)
        opens.append(o)
        highs.append(h)
        lows.append(l)
        closes.append(c)
    return opens, highs, lows, closes, timestamps

def calc_ema(values: List[float], period: int) -> List[float]:
    if not values or period <= 1:
        return values[:]
    k = 2 / (period + 1)
    ema: List[float] = []
    ema_prev = values[0]
    ema.append(ema_prev)
    for price in values[1:]:
        ema_prev = price * k + ema_prev * (1 - k)
        ema.append(ema_prev)
    return ema

def calc_rsi(values: List[float], period: int) -> List[float]:
    if len(values) <= period:
        return [50.0 for _ in values]
    gains: List[float] = [0.0]
    losses: List[float] = [0.0]
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[1: period + 1]) / period
    avg_loss = sum(losses[1: period + 1]) / period
    rsi: List[float] = [50.0] * period
    if avg_loss == 0:
        rsi.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi.append(100 - 100 / (1 + rs))
    for i in range(period + 1, len(values)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi.append(100 - 100 / (1 + rs))
    return rsi

def calc_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[float]:
    trs: List[float] = []
    for i in range(len(highs)):
        if i == 0:
            tr = highs[i] - lows[i]
        else:
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        trs.append(tr)
    if len(trs) <= period:
        return trs
    atr: List[float] = []
    first = sum(trs[:period]) / period
    atr.extend([first] * period)
    prev = first
    for i in range(period, len(trs)):
        prev = (prev * (period - 1) + trs[i]) / period
        atr.append(prev)
    return atr

def calc_macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float]]:
    ema_fast = calc_ema(values, fast)
    ema_slow = calc_ema(values, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = calc_ema(macd_line, signal)
    return macd_line, signal_line

def calc_stoch_rsi(values: List[float], period: int = 14) -> List[float]:
    rsi = calc_rsi(values, period)
    if len(rsi) <= period:
        return [50.0 for _ in rsi]
    stoch: List[float] = [50.0] * period
    for i in range(period, len(rsi)):
        window = rsi[i - period + 1: i + 1]
        rmin = min(window)
        rmax = max(window)
        if rmax - rmin == 0:
            stoch.append(50.0)
        else:
            stoch.append((rsi[i] - rmin) / (rmax - rmin) * 100.0)
    return stoch

# ============================================================
# Context: BTC
# ============================================================
def get_btc_context() -> Dict[str, Any]:
    kl = fetch_binance("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "5m", "limit": 300})
    _, _, _, closes, _ = kline_to_floats(kl)
    ema200 = calc_ema(closes, 200)[-1]
    rsi = calc_rsi(closes, 14)[-1]
    ticker = fetch_binance("/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"})
    price = float(ticker.get("lastPrice", closes[-1]))
    change_pct = float(ticker.get("priceChangePercent", 0.0))
    return {"price": price, "ema200": ema200, "rsi": rsi, "change_pct": change_pct}

def in_fomc_window(now_utc: datetime) -> bool:
    if not FOMC_DATES_UTC:
        return False
    for dt in FOMC_DATES_UTC:
        if abs((now_utc - dt).total_seconds()) <= 3600:
            return True
    return False

# ============================================================
# Signal build / Strategy
# ============================================================
def build_signal_text(idea: Dict[str, Any]) -> str:
    symbol = idea["symbol"]
    side = idea["side"]
    leverage = idea["leverage"]
    entry = idea["entry"]
    take_profit = idea["take_profit"]
    stop_loss = idea["stop_loss"]
    ema200 = idea["ema200"]
    rsi = idea["rsi"]
    impulse_time = idea["impulse_time"]
    atr_pct = idea["atr_pct"]
    macd_val = idea["macd"]
    stoch_val = idea["stoch_rsi"]

    arrow = "🟢" if side == "long" else "🔴"
    impulse_str = impulse_time.isoformat()

    return (
        f"{arrow} <b>{symbol}</b> {side}\n"
        f"Плечо {leverage}х\n"
        f"Вход (ориентир) - {entry:.6f}\n"
        f"Тейк - {take_profit:.6f}\n"
        f"Стоп - {stop_loss:.6f}\n\n"
        f"Таймфрейм: {CONFIG['TIMEFRAME']} (MTF: {CONFIG['HTF_TIMEFRAME']})\n"
        f"EMA200: {ema200:.5f}\n"
        f"RSI(14): {rsi:.1f}\n"
        f"ATR: {atr_pct:.2f}% (k={CONFIG.get('STOP_ATR_MULTIPLIER', 0.6)})\n"
        f"MACD: {macd_val:.5f}\n"
        f"StochRSI: {stoch_val:.1f}\n"
        f"Импульсная свеча (UTC): {impulse_str}\n\n"
        "Логика: импульс + антишпилька-стоп (экстремум ± k×ATR), тейк по RR, "
        "фильтр по тренду, ATR, BTC и осцилляторам."
    )

def analyse_symbol(symbol: str, btc_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # 5m
    kl_5m = fetch_binance("/fapi/v1/klines", {"symbol": symbol, "interval": CONFIG["TIMEFRAME"], "limit": 300})
    o5, h5, l5, c5, t5 = kline_to_floats(kl_5m)

    # 15m
    kl_15m = fetch_binance("/fapi/v1/klines", {"symbol": symbol, "interval": CONFIG["HTF_TIMEFRAME"], "limit": 200})
    _, _, _, c15, _ = kline_to_floats(kl_15m)

    if len(c5) < 210 or len(c15) < 210:
        return None

    ema200_5m = calc_ema(c5, 200)
    ema200_15m = calc_ema(c15, 200)

    rsi_5m = calc_rsi(c5, 14)
    atr_list = calc_atr(h5, l5, c5, 14)
    macd_line, signal_line = calc_macd(c5)
    stoch = calc_stoch_rsi(c5)

    if not ema200_5m or not atr_list or not rsi_5m or not macd_line or not signal_line or not stoch:
        return None

    close = c5[-1]
    ema = ema200_5m[-1]
    ema_htf = ema200_15m[-1] if ema200_15m else ema
    rsi = rsi_5m[-1]

    atr_abs = atr_list[-1]
    atr_pct = atr_abs / close * 100.0

    macd_val = macd_line[-1]
    macd_signal = signal_line[-1]
    stoch_val = stoch[-1]

    # ATR filter
    if not (CONFIG["MIN_ATR_PCT"] <= atr_pct <= CONFIG["MAX_ATR_PCT"]):
        return None

    # Trend filters (simple)
    price_above = close > ema * 1.001
    price_below = close < ema * 0.999

    # HTF confirmation
    htf_ok_long = close > ema_htf
    htf_ok_short = close < ema_htf

    side: Optional[str] = None
    if price_above and htf_ok_long and rsi > 52 and macd_val > macd_signal and stoch_val > 20:
        side = "long"
    elif price_below and htf_ok_short and rsi < 48 and macd_val < macd_signal and stoch_val < 80:
        side = "short"

    if side is None:
        return None

    # BTC filter
    if CONFIG["BTC_FILTER_ENABLED"]:
        btc_price = btc_ctx["price"]
        btc_ema = btc_ctx["ema200"]
        btc_rsi = btc_ctx["rsi"]
        btc_change = btc_ctx["change_pct"]
        if side == "long":
            if btc_price < btc_ema or btc_rsi < 45 or btc_change < -3.0:
                return None
        else:
            if btc_price > btc_ema or btc_rsi > 55 or btc_change > 3.0:
                return None

    # Impulse candle = previous candle
    impulse_idx = len(c5) - 2
    impulse_low = l5[impulse_idx]
    impulse_high = h5[impulse_idx]
    impulse_time = datetime.fromtimestamp(t5[impulse_idx] / 1000, timezone.utc)

    # Anti-spike stop (extremum ± k*ATR)
    k = float(CONFIG.get("STOP_ATR_MULTIPLIER", 0.6))
    if side == "long":
        stop_loss = impulse_low - (k * atr_abs)
        stop_pct = abs((close - stop_loss) / close) * 100.0
        if not (CONFIG["MIN_STOP_PCT"] <= stop_pct <= CONFIG["MAX_STOP_PCT"]):
            return None
        take_profit = close + (close - stop_loss) * CONFIG["RISK_REWARD"]
    else:
        stop_loss = impulse_high + (k * atr_abs)
        stop_pct = abs((stop_loss - close) / close) * 100.0
        if not (CONFIG["MIN_STOP_PCT"] <= stop_pct <= CONFIG["MAX_STOP_PCT"]):
            return None
        take_profit = close - (stop_loss - close) * CONFIG["RISK_REWARD"]

    return {
        "symbol": symbol,
        "side": side,
        "leverage": 20,
        "entry": close,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "ema200": ema,
        "rsi": rsi,
        "impulse_time": impulse_time,
        "atr_pct": atr_pct,
        "macd": macd_val,
        "stoch_rsi": stoch_val,
    }

def scan_market_and_send_signals() -> int:
    """
    Returns number of signals sent in this scan.
    """
    if STATE.is_risk_off():
        logging.info("Режим Risk OFF, сканирование пропускается.")
        return 0

    now_utc = datetime.now(timezone.utc)
    if in_fomc_window(now_utc):
        logging.info("Сейчас окно FOMC, сканирование отключено.")
        return 0

    btc_ctx = get_btc_context()

    symbols = get_usdt_perp_symbols()
    symbols = get_24h_volume_filter(symbols)

    active_subs = db_get_active_subscribers()
    if not active_subs:
        logging.info("Нет активных подписчиков — сигналы не отправляются.")
        return 0

    signals_for_scan = 0
    for symbol in symbols:
        if signals_for_scan >= CONFIG["MAX_SIGNALS_PER_SCAN"]:
            break
        if not STATE.can_send_signal(symbol):
            continue

        idea = analyse_symbol(symbol, btc_ctx)
        if not idea:
            continue

        text = build_signal_text(idea)
        for cid in active_subs:
            send_telegram_message(text, chat_id=str(cid), html=True)

        STATE.register_signal(symbol)
        signals_for_scan += 1
        logging.info("Сигнал отправлен: %s %s", symbol, idea["side"])

    logging.info(
        "Скан завершён. Отправлено сигналов: %d; сегодня: %d/%d",
        signals_for_scan,
        STATE.signals_sent_today,
        CONFIG["MAX_SIGNALS_PER_DAY"],
    )
    return signals_for_scan

# ============================================================
# Callback (admin inline buttons)
# ============================================================
def handle_callback_query_update(upd: Dict[str, Any]) -> None:
    cq = upd.get("callback_query") or {}
    cq_id = cq.get("id", "")
    data = cq.get("data", "") or ""

    from_user = cq.get("from", {}) or {}
    user_id = str(from_user.get("id", ""))

    msg = cq.get("message", {}) or {}
    chat = msg.get("chat", {}) or {}
    chat_id = str(chat.get("id", ""))
    message_id = int(msg.get("message_id", 0))

    if not is_admin_user(user_id):
        answer_callback_query(cq_id, "Нет доступа", show_alert=True)
        return

    answer_callback_query(cq_id)

    if data == "admin:status":
        edit_telegram_message(chat_id, message_id, admin_status_text(), html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:subs":
        edit_telegram_message(chat_id, message_id, admin_subs_text(), html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:risk_off":
        STATE.set_risk_off(True)
        edit_telegram_message(chat_id, message_id, "<b>🛑 Risk OFF включён</b>", html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:risk_on":
        STATE.set_risk_off(False)
        edit_telegram_message(chat_id, message_id, "<b>✅ Risk OFF выключен</b>", html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:btc_toggle":
        CONFIG["BTC_FILTER_ENABLED"] = not CONFIG["BTC_FILTER_ENABLED"]
        edit_telegram_message(chat_id, message_id, admin_status_text(), html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:test":
        try:
            sent = scan_market_and_send_signals()
            edit_telegram_message(
                chat_id,
                message_id,
                f"<b>🧪 Тест-скан выполнен</b>\nОтправлено сигналов: {sent}\n\n{admin_status_text()}",
                html=True,
                reply_markup=admin_inline_panel(),
            )
        except Exception as e:
            edit_telegram_message(chat_id, message_id, f"<b>🧪 Тест-скан ошибка</b>\n<code>{e}</code>", html=True, reply_markup=admin_inline_panel())
        return

    if data in ("admin:int_300", "admin:int_600", "admin:int_900"):
        CONFIG["SCAN_INTERVAL_SECONDS"] = int(data.split("_")[1])
        edit_telegram_message(chat_id, message_id, admin_status_text(), html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:k_06":
        CONFIG["STOP_ATR_MULTIPLIER"] = 0.6
        edit_telegram_message(chat_id, message_id, admin_status_text(), html=True, reply_markup=admin_inline_panel())
        return

    if data == "admin:k_08":
        CONFIG["STOP_ATR_MULTIPLIER"] = 0.8
        edit_telegram_message(chat_id, message_id, admin_status_text(), html=True, reply_markup=admin_inline_panel())
        return

    edit_telegram_message(chat_id, message_id, "<b>Неизвестная кнопка</b>", html=True, reply_markup=admin_inline_panel())

# ============================================================
# Commands (RU): user + admin
# ============================================================
def handle_command(update: Dict[str, Any]) -> None:
    msg = update.get("message") or update.get("edited_message") or {}
    text_raw = (msg.get("text", "") or "").strip()

    chat = msg.get("chat", {}) or {}
    chat_id = str(chat.get("id", ""))

    user = msg.get("from", {}) or {}
    user_id = str(user.get("id", ""))

    admin = is_admin_user(user_id)
    kb = get_reply_keyboard(chat_id, user_id)

    # Safe tokens
    parts = text_raw.split()
    first_token = parts[0] if parts else ""
    cmd = normalize_command(text_raw)

    # --- universal: myid
    if first_token == "/myid" or cmd == "мой id":
        send_telegram_message(f"Ваш user_id: <code>{user_id}</code>", chat_id=chat_id, html=True, reply_markup=kb)
        return

    # --- common commands for everyone
    if first_token == "/start" or cmd == "старт":
        db_add_or_update_subscriber(chat_id, is_admin=admin)

        intro = (
            "🚀 <b>Бот запущен!</b> Вы подписались на торговые сигналы Binance Futures (USDT-M).\n\n"
            "Я сканирую Binance Futures, основной анализ на 5m, тренд подтверждается на 15m, "
            "учитываю EMA200, RSI, StochRSI, MACD, контекст BTCUSDT, ATR и уровни.\n\n"
            "Команды для всех:\n"
            "• 🚀 Старт — подписка / обновить клавиатуру\n"
            "• 📊 Статус — состояние бота\n"
            "• ℹ️ Помощь — описание логики\n"
            "• 📴 Стоп — отписаться\n\n"
            f"Первый скан будет через {CONFIG['SCAN_INTERVAL_SECONDS']} секунд."
        )
        send_telegram_message(intro, chat_id=chat_id, html=True, reply_markup=kb)
        return

    if first_token == "/help" or cmd == "помощь":
        msg_text = (
            "<b>ℹ️ Описание логики бота</b>\n\n"
            "• TF: 5m, подтверждение: 15m\n"
            "• Фильтры: EMA200, RSI(14), StochRSI, MACD\n"
            "• Волатильность: ATR-фильтр (чтобы не лезть в «пилу»)\n"
            "• Антишпилька: стоп за экстремумом импульсной свечи + k×ATR\n"
            "• Контекст: BTCUSDT (тренд/RSI/24h изменение)\n\n"
            "⚠️ Сигналы не являются финансовой рекомендацией."
        )
        send_telegram_message(msg_text, chat_id=chat_id, html=True, reply_markup=kb)
        return

    if first_token == "/stop" or cmd == "стоп":
        db_unsubscribe(chat_id)
        send_telegram_message("Вы отписались от сигналов. Вернуться можно через 🚀 Старт.", chat_id=chat_id, html=False, reply_markup=kb)
        return

    if first_token == "/status" or cmd == "статус":
        risk_off_state = "активен" if STATE.is_risk_off() else "выключен"
        lines = [
            "<b>📊 Статус торгового бота</b>",
            "",
            f"⏱ Интервал скана: {CONFIG['SCAN_INTERVAL_SECONDS']} сек",
            f"🎯 Лимит сигналов/день: {CONFIG['MAX_SIGNALS_PER_DAY']}",
            f"📈 Анализ: {CONFIG['TIMEFRAME']} + {CONFIG['HTF_TIMEFRAME']}",
            f"💹 BTC фильтр: {'включён' if CONFIG['BTC_FILTER_ENABLED'] else 'выключен'}",
            f"🔥 ATR фильтр: {CONFIG['MIN_ATR_PCT']}–{CONFIG['MAX_ATR_PCT']}%",
            f"🎯 k (ATR-стоп): {CONFIG.get('STOP_ATR_MULTIPLIER', 0.6)}",
            f"🛑 Risk OFF: {risk_off_state}",
        ]
        if admin:
            lines.append(f"👥 Подписчиков: {db_get_subscribers_count()}")
            lines.append(f"📌 Сигналы сегодня: {STATE.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}")
        send_telegram_message("\n".join(lines), chat_id=chat_id, html=True, reply_markup=kb)
        return

    # --- admin-only extensions (RU)
    if admin:
        if first_token == "/admin" or cmd == "админ":
            send_telegram_message(admin_status_text(), chat_id=chat_id, html=True, reply_markup=admin_inline_panel())
            return

        if first_token == "/risk_off" or cmd == "risk off":
            STATE.set_risk_off(True)
            send_telegram_message("🛑 Режим <b>Risk OFF</b> включён. Сканирование остановлено.", chat_id=chat_id, html=True, reply_markup=kb)
            return

        if first_token == "/risk_on" or cmd == "risk on":
            STATE.set_risk_off(False)
            send_telegram_message("✅ Режим <b>Risk OFF</b> выключен. Сканирование включено.", chat_id=chat_id, html=True, reply_markup=kb)
            return

        if first_token == "/scan" or cmd in ("тест-скан", "тест скан"):
            try:
                sent = scan_market_and_send_signals()
                send_telegram_message(f"🧪 Тест-скан выполнен. Отправлено сигналов: {sent}", chat_id=chat_id, html=False, reply_markup=kb)
            except Exception as e:
                send_telegram_message(f"🧪 Ошибка тест-скана: {e}", chat_id=chat_id, html=False, reply_markup=kb)
            return

        if first_token == "/settings" or cmd == "настройки":
            if len(parts) == 1:
                msg_text = (
                    "<b>⚙️ Текущие настройки</b>\n\n"
                    f"• MIN_QUOTE_VOLUME: {CONFIG['MIN_QUOTE_VOLUME']:,} USDT\n"
                    f"• MAX_SIGNALS_PER_DAY: {CONFIG['MAX_SIGNALS_PER_DAY']}\n"
                    f"• SCAN_INTERVAL_SECONDS: {CONFIG['SCAN_INTERVAL_SECONDS']} сек\n"
                    f"• MIN_ATR_PCT: {CONFIG['MIN_ATR_PCT']}%\n"
                    f"• STOP_ATR_MULTIPLIER (k): {CONFIG.get('STOP_ATR_MULTIPLIER', 0.6)}\n\n"
                    "Чтобы изменить, используйте формат:\n"
                    "<code>/settings volume=70000000 max_signals=5 interval=900 atr=0.25 k=0.8</code>"
                )
                send_telegram_message(msg_text, chat_id=chat_id, html=True, reply_markup=kb)
                return

            changes: List[str] = []
            for token in parts[1:]:
                if "=" not in token:
                    continue
                key, val = token.split("=", 1)
                key = key.strip().lower()
                val = val.strip()

                if key in ("volume", "min_volume"):
                    try:
                        CONFIG["MIN_QUOTE_VOLUME"] = int(val)
                        changes.append(f"MIN_QUOTE_VOLUME → {CONFIG['MIN_QUOTE_VOLUME']:,}")
                    except ValueError:
                        pass
                elif key in ("max_signals", "max_per_day"):
                    try:
                        CONFIG["MAX_SIGNALS_PER_DAY"] = int(val)
                        changes.append(f"MAX_SIGNALS_PER_DAY → {CONFIG['MAX_SIGNALS_PER_DAY']}")
                    except ValueError:
                        pass
                elif key in ("interval", "scan_interval"):
                    try:
                        CONFIG["SCAN_INTERVAL_SECONDS"] = int(val)
                        changes.append(f"SCAN_INTERVAL_SECONDS → {CONFIG['SCAN_INTERVAL_SECONDS']} сек")
                    except ValueError:
                        pass
                elif key in ("atr", "atr_min", "atrmin"):
                    try:
                        CONFIG["MIN_ATR_PCT"] = float(val)
                        changes.append(f"MIN_ATR_PCT → {CONFIG['MIN_ATR_PCT']}%")
                    except ValueError:
                        pass
                elif key in ("k", "stop_k", "stopatr"):
                    try:
                        CONFIG["STOP_ATR_MULTIPLIER"] = float(val)
                        changes.append(f"STOP_ATR_MULTIPLIER (k) → {CONFIG['STOP_ATR_MULTIPLIER']}")
                    except ValueError:
                        pass

            if not changes:
                send_telegram_message(
                    "Не удалось разобрать параметры.\n"
                    "Пример: <code>/settings volume=70000000 max_signals=5 interval=900 atr=0.25 k=0.8</code>",
                    chat_id=chat_id,
                    html=True,
                    reply_markup=kb,
                )
                return

            send_telegram_message("<b>⚙️ Настройки обновлены:</b>\n" + "\n".join(f"• {c}" for c in changes), chat_id=chat_id, html=True, reply_markup=kb)
            return

        # Admin fallback
        send_telegram_message(
            "Команда не распознана.\n\n"
            "Команды админа:\n"
            "• 🛠 Админ\n• ⚙️ Настройки\n• /risk_on /risk_off\n• /scan\n"
            "Используйте кнопки.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
        return

    # --- user fallback
    send_telegram_message(
        "Я пока не понимаю эту команду.\nИспользуйте кнопки под полем ввода или /help.",
        chat_id=chat_id,
        html=False,
        reply_markup=kb,
    )

# ============================================================
# Updates / Polling (single loop)
# ============================================================
def get_updates(offset: Optional[int]) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    params: Dict[str, Any] = {"timeout": 25}
    if offset is not None:
        params["offset"] = offset
    url = f"{TELEGRAM_API_URL}/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    resp = requests.get(url, params=params, timeout=35)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        return [], offset

    updates = data.get("result", []) or []
    new_offset = offset
    for upd in updates:
        uid = upd.get("update_id")
        if isinstance(uid, int):
            nxt = uid + 1
            if new_offset is None or nxt > new_offset:
                new_offset = nxt
    return updates, new_offset

# ============================================================
# Main loop
# ============================================================
def main_loop() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не задан. Выход.")
        return

    db_ensure_tables()

    logging.info("=" * 70)
    logging.info("Запуск Binance Futures Signal Bot (RU + Admin + Smooth Anti-spike)")
    logging.info("=" * 70)
    logging.info("ADMIN (TG_ADMIN_ID) = %s", TG_ADMIN_ID or "(не задан)")
    logging.info("Конфиг: interval=%ss, max/day=%s, atr_min=%s, k=%s, btc_filter=%s",
                 CONFIG["SCAN_INTERVAL_SECONDS"],
                 CONFIG["MAX_SIGNALS_PER_DAY"],
                 CONFIG["MIN_ATR_PCT"],
                 CONFIG.get("STOP_ATR_MULTIPLIER", 0.6),
                 "ON" if CONFIG["BTC_FILTER_ENABLED"] else "OFF")

    last_scan_ts = 0.0
    offset: Optional[int] = None

    def handle_sigterm(signum, frame):
        logging.info("Получен сигнал остановки. Завершение работы...")
        raise SystemExit

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    while True:
        # Telegram updates
        try:
            updates, offset = get_updates(offset)
            for upd in updates:
                if upd.get("callback_query"):
                    handle_callback_query_update(upd)
                    continue

                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue

                text = (msg.get("text", "") or "").strip()
                if not text:
                    continue

                # All text routed through handle_command (proper fallbacks)
                handle_command(upd)

        except Exception as e:
            logging.error("Ошибка polling: %s", e)
            time.sleep(3)

        # Scheduled scan
        now = time.time()
        if now - last_scan_ts >= CONFIG["SCAN_INTERVAL_SECONDS"]:
            logging.info("Начало сканирования рынка...")
            try:
                scan_market_and_send_signals()
            except Exception as e:
                logging.error("Ошибка при сканировании рынка: %s", e)
            last_scan_ts = time.time()


if __name__ == "__main__":
    try:
        main_loop()
    except SystemExit:
        logging.info("Бот остановлен.")
    except Exception as e:
        logging.error("Критическая ошибка: %s", e)
