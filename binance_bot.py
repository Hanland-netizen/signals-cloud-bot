import os
import sys
import time
import logging
import threading
import requests
import psycopg2
from datetime import datetime, date, UTC, timedelta, timezone
from typing import List, Dict, Optional

CONFIG = {
    "BASE_URL": "https://fapi.binance.com",
    "MIN_QUOTE_VOLUME": 50_000_000,
    "CONTRACT_TYPE": "PERPETUAL",
    "QUOTE_ASSET": "USDT",
    "TIMEFRAME": "5m",
    "CANDLES_LIMIT": 300,
    "EMA_PERIOD": 200,
    "RSI_PERIOD": 14,
    "LOOKBACK_CANDLES": 5,
    "HTF_TIMEFRAME": "15m",
    "HTF_EMA_PERIOD": 200,
    "HTF_RSI_PERIOD": 14,
    "BODY_MULTIPLIER": 2.0,
    "VOLUME_MULTIPLIER": 2.0,
    "MIN_BODY_TO_RANGE": 0.45,
    "IMPULSE_BREAK_LOOKBACK": 10,
    "ATR_PERIOD": 14,
    "MIN_ATR_PCT": 0.15,
    "MAX_ATR_PCT": 5.0,
    "RISK_REWARD": 1.7,
    "RSI_OVERBOUGHT": 70,
    "RSI_OVERSOLD": 30,
    "MIN_RISK_PCT": 0.35,
    "MIN_TP_PCT": 0.7,
    "STOP_ATR_MULTIPLIER": 0.2,
    "LEVEL_LOOKBACK": 30,
    "LEVEL_MAX_TAKE_PORTION": 0.6,
    "LEVERAGE_RULES": [
        (0.7, 20),
        (1.5, 15),
        (3.0, 10),
        (5.0, 7),
        (float("inf"), 5),
    ],
    "MACD_FAST": 12,
    "MACD_SLOW": 26,
    "MACD_SIGNAL": 9,
    "STOCH_RSI_PERIOD": 14,
    "STOCH_RSI_K_MIN": 10,
    "STOCH_RSI_K_MAX": 90,
    "MAX_SIGNALS_PER_DAY": 7,
    "SCAN_INTERVAL_SECONDS": 600,
    "MAX_SIGNALS_PER_SCAN": 1,
    "SYMBOL_COOLDOWN_SECONDS": 1800,
    "BTC_SYMBOL": "BTCUSDT",
    "BTC_FILTER_ENABLED": True,
    "FOMC_DATES_UTC": [],
    "FOMC_BLOCK_BEFORE": 3600,
    "FOMC_BLOCK_AFTER": 3600,
    "RISK_OFF_DEFAULT_SECONDS": 3 * 3600,
    "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", ""),
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),
    "DATABASE_URL": os.getenv("DATABASE_URL", ""),
}

SUBSCRIBERS: set[str] = set()
LAST_UPDATE_ID: Optional[int] = None
STOP_EVENT = threading.Event()
STATE = None
SIGNALS_LOG_FILE = "signals_log.csv"

logger = logging.getLogger("binance_signals_bot")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)


class BotState:
    def __init__(self):
        self.signals_sent_today = 0
        self.last_reset_date: date = datetime.now().date()
        self.sent_signal_ids: set[str] = set()
        self.risk_off_until: Optional[datetime] = None
        self.last_signal_time_by_symbol: Dict[str, datetime] = {}

    def reset_daily_if_needed(self):
        today = datetime.now().date()
        if today != self.last_reset_date:
            logger.info("Новый день, сбрасываем счётчики сигналов.")
            self.signals_sent_today = 0
            self.sent_signal_ids.clear()
            self.last_reset_date = today

    def can_send_signal(self) -> bool:
        return self.signals_sent_today < CONFIG["MAX_SIGNALS_PER_DAY"]

    def register_signal(self, signal_id: str, symbol: str):
        self.sent_signal_ids.add(signal_id)
        self.signals_sent_today += 1
        self.last_signal_time_by_symbol[symbol] = datetime.now(timezone.utc)

    def is_symbol_cooled_down(self, symbol: str) -> bool:
        cooldown = CONFIG["SYMBOL_COOLDOWN_SECONDS"]
        if cooldown <= 0:
            return True
        last_time = self.last_signal_time_by_symbol.get(symbol)
        if last_time is None:
            return True
        now_utc = datetime.now(timezone.utc)
        if (now_utc - last_time).total_seconds() >= cooldown:
            return True
        return False

    def is_risk_off(self) -> bool:
        if self.risk_off_until is None:
            return False
        now_utc = datetime.now(timezone.utc)
        if now_utc >= self.risk_off_until:
            self.risk_off_until = None
            return False
        return True

    def activate_risk_off(self, seconds: int):
        now_utc = datetime.now(timezone.utc)
        self.risk_off_until = now_utc + timedelta(seconds=seconds)
        logger.info(f"Risk OFF активирован до {self.risk_off_until.isoformat()} (UTC).")

    def deactivate_risk_off(self):
        self.risk_off_until = None
        logger.info("Risk OFF режим отключён вручную.")


def db_execute(query: str, params: Optional[tuple] = None, fetch: bool = False):
    if not CONFIG["DATABASE_URL"]:
        logger.error("DATABASE_URL не настроен.")
        sys.exit(1)
    conn = psycopg2.connect(CONFIG["DATABASE_URL"])
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                if fetch:
                    return cur.fetchall()
    finally:
        conn.close()
    return None


def db_init_and_load_subscribers():
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id BIGINT PRIMARY KEY,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS unsubscribes (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            unsubscribed_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    rows = db_execute("SELECT chat_id FROM subscribers;", fetch=True) or []
    SUBSCRIBERS.clear()
    for (cid,) in rows:
        SUBSCRIBERS.add(str(cid))
    logger.info(f"Загружено подписчиков из БД: {len(SUBSCRIBERS)}")


def db_add_subscriber(chat_id: str, is_admin: bool = False):
    cid = int(chat_id)
    was_new = str(chat_id) not in SUBSCRIBERS
    db_execute(
        """
        INSERT INTO subscribers (chat_id, is_admin)
        VALUES (%s, %s)
        ON CONFLICT (chat_id) DO UPDATE SET is_admin = EXCLUDED.is_admin;
        """,
        (cid, is_admin),
    )
    SUBSCRIBERS.add(str(chat_id))
    logger.info(f"Добавлен подписчик в БД: {chat_id} (admin={is_admin})")
    admin_chat = CONFIG["TG_CHAT_ID"]
    if was_new and not is_admin and admin_chat and str(chat_id) != admin_chat:
        send_telegram_message(
            f"🔔 Новый подписчик: {chat_id}",
            chat_id=admin_chat,
            html=False,
        )


def db_remove_subscriber(chat_id: str):
    cid = int(chat_id)
    db_execute("INSERT INTO unsubscribes (chat_id) VALUES (%s);", (cid,))
    db_execute("DELETE FROM subscribers WHERE chat_id = %s;", (cid,))
    if str(chat_id) in SUBSCRIBERS:
        SUBSCRIBERS.remove(str(chat_id))
    logger.info(f"Удалён подписчик из БД: {chat_id}")
    admin_chat = CONFIG["TG_CHAT_ID"]
    if admin_chat and str(chat_id) != admin_chat:
        send_telegram_message(
            f"🔔 Пользователь отписался: {chat_id}",
            chat_id=admin_chat,
            html=False,
        )


def db_get_subscribers_count() -> int:
    rows = db_execute("SELECT COUNT(*) FROM subscribers;", fetch=True)
    if not rows:
        return 0
    return int(rows[0][0])


def is_fomc_block_active(now_utc: Optional[datetime] = None) -> bool:
    if not CONFIG["FOMC_DATES_UTC"]:
        return False
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    before = CONFIG["FOMC_BLOCK_BEFORE"]
    after = CONFIG["FOMC_BLOCK_AFTER"]
    for dt_str in CONFIG["FOMC_DATES_UTC"]:
        try:
            fomc_time = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
            fomc_time = fomc_time.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        block_start = fomc_time - timedelta(seconds=before)
        block_end = fomc_time + timedelta(seconds=after)
        if block_start <= now_utc <= block_end:
            logger.info(
                f"Активно окно FOMC для {dt_str} (UTC). "
                f"Сканирование рынка временно отключено."
            )
            return True
    return False


def binance_request(endpoint: str, params: Optional[Dict] = None, max_retries: int = 5) -> Optional[Dict]:
    url = f"{CONFIG['BASE_URL']}{endpoint}"
    for attempt in range(1, max_retries + 1):
        if STOP_EVENT.is_set():
            return None
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_sec = 2 * attempt
                logger.warning(
                    f"Ошибка запроса к {endpoint} (попытка {attempt}/{max_retries}): {e}. "
                    f"Ждём {wait_sec} сек перед повтором..."
                )
                time.sleep(wait_sec)
            else:
                logger.error(
                    f"Не удалось получить ответ от {endpoint} после {max_retries} попыток: {e}"
                )
                return None


def get_trading_symbols() -> List[str]:
    logger.info("Загружаем список торгуемых USDT-M PERPETUAL символов...")
    exchange_info = binance_request("/fapi/v1/exchangeInfo")
    if not exchange_info:
        return []
    symbols_info = exchange_info.get("symbols", [])
    futures_symbols = [
        s["symbol"]
        for s in symbols_info
        if s.get("contractType") == CONFIG["CONTRACT_TYPE"]
        and s.get("quoteAsset") == CONFIG["QUOTE_ASSET"]
        and s.get("status") == "TRADING"
    ]
    if not futures_symbols:
        logger.warning("Не найдено подходящих USDT-M PERPETUAL символов.")
        return []
    ticker_24h = binance_request("/fapi/v1/ticker/24hr")
    if not ticker_24h:
        return futures_symbols
    volume_dict = {
        item["symbol"]: float(item.get("quoteVolume", 0.0))
        for item in ticker_24h
    }
    filtered_symbols = [
        symbol
        for symbol in futures_symbols
        if volume_dict.get(symbol, 0.0) >= CONFIG["MIN_QUOTE_VOLUME"]
    ]
    logger.info(f"Найдено {len(futures_symbols)} USDT-M PERPETUAL символов")
    logger.info(
        f"После фильтрации по объёму (>= {CONFIG['MIN_QUOTE_VOLUME']:,} USDT): "
        f"{len(filtered_symbols)} символов"
    )
    return filtered_symbols


def get_klines(symbol: str, interval: str, limit: int) -> Optional[List[List]]:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    return binance_request("/fapi/v1/klines", params=params)


def ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema_values: List[float] = []
    ema_prev = sum(values[:period]) / period
    ema_values.append(ema_prev)
    for price in values[period:]:
        ema_prev = price * k + ema_prev * (1 - k)
        ema_values.append(ema_prev)
    return ema_values


def rsi(values: List[float], period: int) -> List[float]:
    if len(values) <= period:
        return []
    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi_values: List[float] = []
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rs = float("inf")
        else:
            rs = avg_gain / avg_loss
        rsi_values.append(100 - (100 / (1 + rs)))
    return rsi_values


def atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
    if len(closes) <= period:
        return None
    trs = []
    prev_close = closes[0]
    for i in range(1, len(closes)):
        high = highs[i]
        low = lows[i]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)
        prev_close = closes[i]
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def latest_macd(values: List[float]) -> Optional[Dict[str, float]]:
    fast = CONFIG["MACD_FAST"]
    slow = CONFIG["MACD_SLOW"]
    signal_p = CONFIG["MACD_SIGNAL"]
    if len(values) < slow + signal_p + 5:
        return None
    ema_fast = ema(values, fast)
    ema_slow = ema(values, slow)
    if not ema_fast or not ema_slow:
        return None
    min_len = min(len(ema_fast), len(ema_slow))
    macd_line = [ema_fast[-min_len + i] - ema_slow[-min_len + i] for i in range(min_len)]
    signal_line = ema(macd_line, signal_p)
    if not signal_line:
        return None
    macd_val = macd_line[-1]
    signal_val = signal_line[-1]
    hist_val = macd_val - signal_val
    return {"macd": macd_val, "signal": signal_val, "hist": hist_val}


def latest_stoch_rsi_from_rsi(rsi_values: List[float]) -> Optional[float]:
    period = CONFIG["STOCH_RSI_PERIOD"]
    if len(rsi_values) < period:
        return None
    window = rsi_values[-period:]
    rsi_min = min(window)
    rsi_max = max(window)
    if rsi_max == rsi_min:
        return None
    last_rsi = window[-1]
    k = (last_rsi - rsi_min) / (rsi_max - rsi_min) * 100
    return k


def get_btc_context() -> Optional[Dict]:
    symbol = CONFIG["BTC_SYMBOL"]
    klines = get_klines(symbol, CONFIG["TIMEFRAME"], CONFIG["CANDLES_LIMIT"])
    if not klines:
        logger.warning("Не удалось загрузить свечи для BTC, фильтр по BTC отключен.")
        return None
    closes = [float(k[4]) for k in klines]
    ema_values = ema(closes, CONFIG["EMA_PERIOD"])
    rsi_values = rsi(closes, CONFIG["RSI_PERIOD"])
    if not ema_values or not rsi_values:
        logger.warning("Недостаточно данных для EMA/RSI BTC, фильтр по BTC отключен.")
        return None
    macd_ctx = latest_macd(closes)
    stoch_ctx = latest_stoch_rsi_from_rsi(rsi_values)
    btc_price = closes[-1]
    btc_ema = ema_values[-1]
    btc_rsi = rsi_values[-1]
    ticker_24h = binance_request("/fapi/v1/ticker/24hr", params={"symbol": symbol})
    change_pct = 0.0
    if isinstance(ticker_24h, dict):
        try:
            change_pct = float(ticker_24h.get("priceChangePercent", 0.0))
        except (ValueError, TypeError):
            change_pct = 0.0
    ctx = {
        "price": btc_price,
        "ema200": btc_ema,
        "rsi": btc_rsi,
        "change24": change_pct,
    }
    if macd_ctx is not None:
        ctx["macd"] = macd_ctx["macd"]
        ctx["macd_signal"] = macd_ctx["signal"]
        ctx["macd_hist"] = macd_ctx["hist"]
    if stoch_ctx is not None:
        ctx["stoch_rsi_k"] = stoch_ctx
    logger.info(
        f"BTC контекст: цена={btc_price:.2f}, EMA200={btc_ema:.2f}, "
        f"RSI={btc_rsi:.1f}, 24h изменение={change_pct:.2f}%"
    )
    return ctx


def find_impulse_candle(
    closes: List[float],
    volumes: List[float],
    highs: List[float],
    lows: List[float],
    lookback: int,
) -> Optional[int]:
    if len(closes) <= lookback + 1:
        return None
    bodies = [abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    avg_body = sum(bodies[:-lookback]) / max(len(bodies[:-lookback]), 1)
    avg_volume = sum(volumes[:-lookback]) / max(len(volumes[:-lookback]), 1)
    body_mult = CONFIG["BODY_MULTIPLIER"]
    vol_mult = CONFIG["VOLUME_MULTIPLIER"]
    min_body_to_range = CONFIG["MIN_BODY_TO_RANGE"]
    break_lookback = CONFIG["IMPULSE_BREAK_LOOKBACK"]
    for idx in range(len(closes) - lookback, len(closes)):
        body = abs(closes[idx] - closes[idx - 1])
        vol = volumes[idx]
        high = highs[idx]
        low = lows[idx]
        candle_range = max(high - low, 1e-9)
        body_to_range = body / candle_range
        if body < body_mult * avg_body:
            continue
        if vol < vol_mult * avg_volume:
            continue
        if body_to_range < min_body_to_range:
            continue
        start = max(0, idx - break_lookback)
        prev_high = max(highs[start:idx]) if idx > start else highs[idx]
        prev_low = min(lows[start:idx]) if idx > start else lows[idx]
        is_bullish = closes[idx] > closes[idx - 1]
        is_bearish = closes[idx] < closes[idx - 1]
        if is_bullish:
            if high <= prev_high:
                continue
        if is_bearish:
            if low >= prev_low:
                continue
        return idx
    return None


def choose_leverage(risk_pct: float) -> int:
    for threshold, lev in CONFIG["LEVERAGE_RULES"]:
        if risk_pct <= threshold:
            return lev
    return CONFIG["LEVERAGE_RULES"][-1][1]


def check_htf_trend(symbol: str, direction: str) -> bool:
    klines = get_klines(symbol, CONFIG["HTF_TIMEFRAME"], CONFIG["CANDLES_LIMIT"])
    if not klines:
        return True
    closes = [float(k[4]) for k in klines]
    ema_values = ema(closes, CONFIG["HTF_EMA_PERIOD"])
    rsi_values = rsi(closes, CONFIG["HTF_RSI_PERIOD"])
    if not ema_values or not rsi_values:
        return True
    price = closes[-1]
    ema_val = ema_values[-1]
    rsi_val = rsi_values[-1]
    if direction == "long":
        if price < ema_val:
            return False
        if rsi_val < 40:
            return False
    else:
        if price > ema_val:
            return False
        if rsi_val > 60:
            return False
    return True


def level_filter(
    symbol: str,
    direction: str,
    entry: float,
    take: float,
    highs: List[float],
    lows: List[float],
):
    lookback = CONFIG["LEVEL_LOOKBACK"]
    portion = CONFIG["LEVEL_MAX_TAKE_PORTION"]
    if len(highs) < lookback or len(lows) < lookback:
        return True
    recent_high = max(highs[-lookback:])
    recent_low = min(lows[-lookback:])
    if direction == "long":
        if entry < recent_high < take:
            dist_to_level = recent_high - entry
            dist_to_take = take - entry
            if dist_to_level < dist_to_take * portion:
                logger.info(
                    f"{symbol} long отклонён: уровень сопротивления слишком близко "
                    f"(до уровня {dist_to_level:.5f}, до тейка {dist_to_take:.5f})."
                )
                return False
    else:
        if take < recent_low < entry:
            dist_to_level = entry - recent_low
            dist_to_take = entry - take
            if dist_to_level < dist_to_take * portion:
                logger.info(
                    f"{symbol} short отклонён: уровень поддержки слишком близко "
                    f"(до уровня {dist_to_level:.5f}, до тейка {dist_to_take:.5f})."
                )
                return False
    return True


def analyze_symbol(symbol: str, btc_ctx: Optional[Dict]) -> Optional[Dict]:
    klines = get_klines(symbol, CONFIG["TIMEFRAME"], CONFIG["CANDLES_LIMIT"])
    if not klines:
        return None
    timestamps = [int(k[0]) for k in klines]
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    volumes = [float(k[5]) for k in klines]
    ema_values = ema(closes, CONFIG["EMA_PERIOD"])
    if not ema_values:
        return None
    current_ema = ema_values[-1]
    current_price = closes[-1]
    rsi_values = rsi(closes, CONFIG["RSI_PERIOD"])
    if not rsi_values:
        return None
    current_rsi = rsi_values[-1]
    stoch_k = latest_stoch_rsi_from_rsi(rsi_values)
    macd_vals = latest_macd(closes)
    if macd_vals is None or stoch_k is None:
        return None
    atr_val = atr(highs, lows, closes, CONFIG["ATR_PERIOD"])
    if atr_val is not None:
        atr_pct = atr_val / current_price * 100
        if atr_pct < CONFIG["MIN_ATR_PCT"] or atr_pct > CONFIG["MAX_ATR_PCT"]:
            logger.info(
                f"{symbol} отклонён по ATR: {atr_pct:.2f}% "
                f"(допустимо {CONFIG['MIN_ATR_PCT']}–{CONFIG['MAX_ATR_PCT']}%)."
            )
            return None
    else:
        atr_pct = None
    stop_buffer = CONFIG["STOP_ATR_MULTIPLIER"] * atr_val if atr_val is not None else 0.0
    impulse_idx = find_impulse_candle(
        closes, volumes, highs, lows, CONFIG["LOOKBACK_CANDLES"]
    )
    if impulse_idx is None:
        return None
    is_bullish = closes[impulse_idx] > closes[impulse_idx - 1]
    is_bearish = closes[impulse_idx] < closes[impulse_idx - 1]
    stoch_min = CONFIG["STOCH_RSI_K_MIN"]
    stoch_max = CONFIG["STOCH_RSI_K_MAX"]
    if is_bearish:
        if current_price <= current_ema:
            return None
        if current_rsi >= CONFIG["RSI_OVERBOUGHT"]:
            return None
        if not (stoch_min < stoch_k < stoch_max):
            return None
        if not (macd_vals["macd"] > macd_vals["signal"] and macd_vals["hist"] >= 0):
            return None
        if CONFIG["BTC_FILTER_ENABLED"] and btc_ctx is not None:
            btc_price = btc_ctx["price"]
            btc_ema = btc_ctx["ema200"]
            btc_rsi = btc_ctx["rsi"]
            if btc_price < btc_ema:
                return None
            if btc_rsi > 70:
                return None
        if not check_htf_trend(symbol, "long"):
            return None
        stop = lows[impulse_idx] - stop_buffer
        entry = current_price
        risk = entry - stop
        if risk <= 0:
            return None
        risk_pct = (risk / entry) * 100
        if risk_pct < CONFIG["MIN_RISK_PCT"]:
            logger.info(
                f"Сигнал {symbol} long отклонён: слишком маленький стоп ({risk_pct:.2f}%)"
            )
            return None
        take = entry + CONFIG["RISK_REWARD"] * risk
        tp_pct = abs(take - entry) / entry * 100
        if tp_pct < CONFIG["MIN_TP_PCT"]:
            logger.info(
                f"Сигнал {symbol} long отклонён: слишком маленький потенциал тейка ({tp_pct:.2f}%)"
            )
            return None
        if not level_filter(symbol, "long", entry, take, highs, lows):
            return None
        signal = {
            "symbol": symbol,
            "direction": "long",
            "entry": entry,
            "stop": stop,
            "take": take,
            "ema200": current_ema,
            "rsi": current_rsi,
            "stoch_rsi_k": stoch_k,
            "macd": macd_vals["macd"],
            "macd_signal": macd_vals["signal"],
            "macd_hist": macd_vals["hist"],
            "impulse_time": datetime.fromtimestamp(
                timestamps[impulse_idx] / 1000, UTC
            ).isoformat(),
            "risk_pct": risk_pct,
            "atr_pct": atr_pct,
        }
        return signal
    if is_bullish:
        if current_price >= current_ema:
            return None
        if current_rsi <= CONFIG["RSI_OVERSOLD"]:
            return None
        if not (stoch_min < stoch_k < stoch_max):
            return None
        if not (macd_vals["macd"] < macd_vals["signal"] and macd_vals["hist"] <= 0):
            return None
        if CONFIG["BTC_FILTER_ENABLED"] and btc_ctx is not None:
            btc_price = btc_ctx["price"]
            btc_ema = btc_ctx["ema200"]
            btc_rsi = btc_ctx["rsi"]
            if btc_price > btc_ema:
                return None
            if btc_rsi < 30:
                return None
        if not check_htf_trend(symbol, "short"):
            return None
        stop = highs[impulse_idx] + stop_buffer
        entry = current_price
        risk = stop - entry
        if risk <= 0:
            return None
        risk_pct = (risk / entry) * 100
        if risk_pct < CONFIG["MIN_RISK_PCT"]:
            logger.info(
                f"Сигнал {symbol} short отклонён: слишком маленький стоп ({risk_pct:.2f}%)"
            )
            return None
        take = entry - CONFIG["RISK_REWARD"] * risk
        tp_pct = abs(take - entry) / entry * 100
        if tp_pct < CONFIG["MIN_TP_PCT"]:
            logger.info(
                f"Сигнал {symbol} short отклонён: слишком маленький потенциал тейка ({tp_pct:.2f}%)"
            )
            return None
        if not level_filter(symbol, "short", entry, take, highs, lows):
            return None
        signal = {
            "symbol": symbol,
            "direction": "short",
            "entry": entry,
            "stop": stop,
            "take": take,
            "ema200": current_ema,
            "rsi": current_rsi,
            "stoch_rsi_k": stoch_k,
            "macd": macd_vals["macd"],
            "macd_signal": macd_vals["signal"],
            "macd_hist": macd_vals["hist"],
            "impulse_time": datetime.fromtimestamp(
                timestamps[impulse_idx] / 1000, UTC
            ).isoformat(),
            "risk_pct": risk_pct,
            "atr_pct": atr_pct,
        }
        return signal
    return None


def send_telegram_message(
    message: str,
    chat_id: Optional[str] = None,
    html: bool = True,
    reply_markup: Optional[Dict] = None,
) -> bool:
    token = CONFIG["TG_BOT_TOKEN"]
    default_chat = CONFIG["TG_CHAT_ID"]
    if not token:
        logger.warning("Telegram token не настроен")
        return False
    target_chat = chat_id or default_chat
    if not target_chat:
        logger.warning("Telegram chat_id не настроен")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: Dict[str, object] = {"chat_id": target_chat, "text": message}
    if html:
        payload["parse_mode"] = "HTML"
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=20)
        if resp.status_code != 200:
            logger.error(
                f"Ошибка отправки в Telegram ({resp.status_code}): {resp.text}"
            )
            return False
        logger.info("Сообщение отправлено в Telegram")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
        return False


def broadcast_to_subscribers(message: str, html: bool = False) -> int:
    if not SUBSCRIBERS:
        logger.info("Нет подписчиков, сигнал не разсылаем.")
        return 0
    sent = 0
    for cid in list(SUBSCRIBERS):
        if send_telegram_message(message, chat_id=cid, html=html):
            sent += 1
        time.sleep(0.3)
    logger.info(f"Сигнал разослан {sent} подписчикам.")
    return sent


def format_signal_message(signal: Dict) -> str:
    direction_emoji = "🟢 long" if signal["direction"] == "long" else "🔴 short"
    lev = choose_leverage(signal["risk_pct"])
    impulse_iso = signal.get("impulse_time")
    impulse_str = str(impulse_iso)
    try:
        dt = datetime.fromisoformat(impulse_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)
        impulse_str = dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        pass
    atr_str = ""
    if signal.get("atr_pct") is not None:
        atr_str = f"\nATR: {signal['atr_pct']:.2f}%"
    macd_str = ""
    if signal.get("macd") is not None and signal.get("macd_signal") is not None:
        macd_str = f"\nMACD: {signal['macd']:.5f}, signal: {signal['macd_signal']:.5f}"
    stoch_str = ""
    if signal.get("stoch_rsi_k") is not None:
        stoch_str = f"\nStochRSI: {signal['stoch_rsi_k']:.1f}"
    msg = (
        f"🎯 {signal['symbol']} {direction_emoji}\n"
        f"Плечо {lev}х\n"
        f"Вход (ориентир) - {signal['entry']:.5f}\n"
        f"Тейк - {signal['take']:.5f}\n"
        f"Стоп - {signal['stop']:.5f}\n\n"
        f"Таймфрейм: {CONFIG['TIMEFRAME']} (MTF: {CONFIG['HTF_TIMEFRAME']})\n"
        f"EMA200: {signal['ema200']:.5f}\n"
        f"RSI({CONFIG['RSI_PERIOD']}): {signal['rsi']:.1f}"
        f"{atr_str}"
        f"{macd_str}"
        f"{stoch_str}\n"
        f"Импульсная свеча: {impulse_str}\n\n"
        f"Логика: импульс, стоп за экстремумом c буфером по ATR, "
        f"тейк по RR {CONFIG['RISK_REWARD']}, фильтр по тренду, "
        f"RSI, StochRSI, MACD, ATR, BTC и 15m-тренду."
    )
    return msg


def log_signal(signal: Dict):
    header_needed = not os.path.exists(SIGNALS_LOG_FILE)
    try:
        with open(SIGNALS_LOG_FILE, "a", encoding="utf-8") as f:
            if header_needed:
                f.write(
                    "timestamp_utc,symbol,direction,entry,stop,take,"
                    "risk_pct,atr_pct,macd,macd_signal,macd_hist,stoch_rsi_k,"
                    "timeframe,htf_timeframe,source\n"
                )
            ts = datetime.now(timezone.utc).isoformat()
            line = (
                f"{ts},"
                f"{signal['symbol']},"
                f"{signal['direction']},"
                f"{signal['entry']:.8f},"
                f"{signal['stop']:.8f},"
                f"{signal['take']:.8f},"
                f"{signal['risk_pct']:.4f},"
                f"{signal.get('atr_pct') if signal.get('atr_pct') is not None else ''},"
                f"{signal.get('macd') if signal.get('macd') is not None else ''},"
                f"{signal.get('macd_signal') if signal.get('macd_signal') is not None else ''},"
                f"{signal.get('macd_hist') if signal.get('macd_hist') is not None else ''},"
                f"{signal.get('stoch_rsi_k') if signal.get('stoch_rsi_k') is not None else ''},"
                f"{CONFIG['TIMEFRAME']},"
                f"{CONFIG['HTF_TIMEFRAME']},"
                f"impulse_ema_rsi_macd_stoch_btc_mtf\n"
            )
            f.write(line)
    except Exception as e:
        logger.error(f"Ошибка записи сигнала в лог: {e}")


def is_admin_chat(chat_id: str) -> bool:
    return CONFIG["TG_CHAT_ID"] and chat_id == CONFIG["TG_CHAT_ID"]


def get_reply_keyboard(chat_id: str) -> Dict:
    if is_admin_chat(chat_id):
        rows = [
            [{"text": "🚀 Старт"}, {"text": "📊 Статус"}],
            [{"text": "ℹ️ Помощь"}, {"text": "📴 Стоп"}],
            [{"text": "📈 Статистика"}, {"text": "👥 Подписчики"}],
            [{"text": "⚙️ Настройки"}, {"text": "🛑 Risk OFF"}],
            [{"text": "🟢 Risk ON"}],
        ]
    else:
        rows = [
            [{"text": "🚀 Старт"}, {"text": "📊 Статус"}],
            [{"text": "ℹ️ Помощь"}, {"text": "📴 Стоп"}],
        ]
    return {"keyboard": rows, "resize_keyboard": True}


def handle_command(message: Dict):
    global STATE
    chat = message.get("chat", {})
    chat_id = str(chat.get("id"))
    text = (message.get("text") or "").strip()
    if not text:
        return
    kb = get_reply_keyboard(chat_id)
    lower = text.lower()
    first_token = text.split()[0]
    is_admin = is_admin_chat(chat_id)
    if first_token == "/start" or lower == "🚀 старт":
        db_add_subscriber(chat_id, is_admin=is_admin)
        subs_count = db_get_subscribers_count()
        welcome = (
            "<b>🚀 Бот запущен!</b>\n\n"
            "Вы подписались на торговые сигналы Binance Futures (USDT-M).\n\n"
            "Я использую:\n"
            "• импульсный анализ на 5m\n"
            "• подтверждение тренда на 15m\n"
            "• фильтр по EMA200, RSI, StochRSI, MACD\n"
            "• контекст BTCUSDT и ATR-волатильность\n"
            "• фильтрацию по уровням и ежедневный лимит сигналов\n\n"
            "Основные команды:\n"
            "• <b>🚀 Старт</b> — подписка или обновление клавиатуры\n"
            "• <b>📊 Статус</b> — состояние бота\n"
            "• <b>ℹ️ Помощь</b> — описание логики\n"
            "• <b>📴 Стоп</b> — отписаться от сигналов\n"
        )
        send_telegram_message(welcome, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/stop" or lower == "📴 стоп":
        db_remove_subscriber(chat_id)
        msg = (
            "📴 Вы отписались от сигналов.\n"
            "Если захотите вернуться — нажмите «🚀 Старт» или отправьте /start."
        )
        send_telegram_message(msg, chat_id=chat_id, html=False, reply_markup=kb)
    elif first_token == "/status" or lower == "📊 статус":
        subs_count = db_get_subscribers_count()
        risk_off_state = "активен" if (STATE and STATE.is_risk_off()) else "выключен"
        msg_lines = [
            "<b>📊 Статус торгового бота</b>",
            "",
            f"⏱ Интервал сканирования: {CONFIG['SCAN_INTERVAL_SECONDS']} сек",
            f"🎯 Лимит сигналов в день: {CONFIG['MAX_SIGNALS_PER_DAY']}",
            f"📈 Multi-TF анализ: {CONFIG['TIMEFRAME']} + {CONFIG['HTF_TIMEFRAME']}",
            f"💹 Фильтр BTC: {'включён' if CONFIG['BTC_FILTER_ENABLED'] else 'выключен'}",
            f"🔥 ATR-фильтр: {CONFIG['MIN_ATR_PCT']}–{CONFIG['MAX_ATR_PCT']}%",
            f"💰 Мин. объём за 24ч: {CONFIG['MIN_QUOTE_VOLUME']:,} USDT",
            "",
            f"👥 Подписчиков: {subs_count}",
        ]
        if is_admin and STATE:
            msg_lines.append(
                f"📌 Сигналы сегодня: "
                f"{STATE.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}"
            )
        msg_lines.append(f"🛑 Режим Risk OFF: {risk_off_state}")
        if CONFIG["FOMC_DATES_UTC"]:
            msg_lines.append("📅 FOMC-окна: настроены (бот не сканирует ±1 час).")
        else:
            msg_lines.append("📅 FOMC-окна: не заданы (список дат пуст).")
        msg = "\n".join(msg_lines)
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/help" or lower == "ℹ️ помощь":
        help_msg = (
            "<b>ℹ️ Что делает бот</b>\n\n"
            "Бот автоматически:\n"
            "• сканирует USDT-M фьючерсы Binance\n"
            "• ищет импульсные свечи на 5m\n"
            "• подтверждает тренд на 15m\n"
            "• фильтрует по EMA200, RSI, StochRSI, MACD\n"
            "• учитывает контекст BTCUSDT\n"
            "• проверяет волатильность через ATR\n"
            "• фильтрует сигналы по уровням и периодам\n"
            "• ограничивает сигналы по дню и по инструменту\n\n"
            "Сигналы не являются финансовой рекомендацией. "
            "Торговля фьючерсами связана с повышенным риском."
        )
        send_telegram_message(help_msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/admin_subs" or lower == "👥 подписчики":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        subs_count = db_get_subscribers_count()
        msg = f"👥 Текущих подписчиков в базе: <b>{subs_count}</b>."
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/admin_stats" or lower == "📈 статистика":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        if STATE:
            msg = (
                "<b>📈 Статистика за сегодня</b>\n\n"
                f"Отправлено сигналов: {STATE.signals_sent_today}/"
                f"{CONFIG['MAX_SIGNALS_PER_DAY']}\n"
                "Подробные сигналы записываются в файл signals_log.csv "
                "на стороне сервера."
            )
        else:
            msg = "Статистика временно недоступна."
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/admin_subscribers_list":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        rows = db_execute(
            "SELECT chat_id, is_admin, created_at FROM subscribers ORDER BY created_at;",
            fetch=True,
        ) or []
        if not rows:
            msg = "👥 В базе сейчас нет подписчиков."
        else:
            lines = ["<b>👥 Список подписчиков</b>"]
            for (cid, adm, created_at) in rows:
                dt = created_at.astimezone(UTC)
                t_str = dt.strftime("%Y-%m-%d %H:%M UTC")
                role = "admin" if adm else "user"
                lines.append(f"- {cid} ({role}), с {t_str}")
            msg = "\n".join(lines)
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/admin_growth":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        rows = db_execute(
            """
            SELECT (created_at AT TIME ZONE 'UTC')::date AS d, COUNT(*)
            FROM subscribers
            GROUP BY d
            ORDER BY d;
            """,
            fetch=True,
        ) or []
        if not rows:
            msg = "Пока нет данных по росту подписчиков."
        else:
            lines = ["<b>📊 Динамика роста подписчиков</b>", ""]
            for d, cnt in rows:
                lines.append(f"{d.isoformat()}: {cnt}")
            msg = "\n".join(lines)
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/admin_unsub_stats":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        rows = db_execute(
            """
            SELECT chat_id, unsubscribed_at
            FROM unsubscribes
            ORDER BY unsubscribed_at DESC
            LIMIT 30;
            """,
            fetch=True,
        ) or []
        if not rows:
            msg = "Пока никто не отписывался."
        else:
            lines = ["<b>📉 Отписки (последние события)</b>", ""]
            for cid, unsub_at in rows:
                dt = unsub_at.astimezone(UTC)
                t_str = dt.strftime("%Y-%m-%d %H:%M UTC")
                lines.append(f"- {cid}: {t_str}")
            msg = "\n".join(lines)
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/settings" or lower.startswith("⚙️ настройки"):
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        parts = text.split()
        if len(parts) == 1:
            msg = (
                "<b>⚙️ Текущие настройки</b>\n\n"
                f"• MIN_QUOTE_VOLUME: {CONFIG['MIN_QUOTE_VOLUME']:,} USDT\n"
                f"• MAX_SIGNALS_PER_DAY: {CONFIG['MAX_SIGNALS_PER_DAY']}\n"
                f"• SCAN_INTERVAL_SECONDS: {CONFIG['SCAN_INTERVAL_SECONDS']} сек\n\n"
                "Чтобы изменить, используйте формат:\n"
                "<code>/settings volume=70000000 max_signals=5 interval=900</code>"
            )
            send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
            return
        changes = []
        for token in parts[1:]:
            if "=" not in token:
                continue
            key, val = token.split("=", 1)
            key = key.strip().lower()
            val = val.strip()
            try:
                ival = int(val)
            except ValueError:
                continue
            if key in ("volume", "min_volume"):
                CONFIG["MIN_QUOTE_VOLUME"] = ival
                changes.append(f"MIN_QUOTE_VOLUME → {ival:,}")
            elif key in ("max_signals", "max_per_day"):
                CONFIG["MAX_SIGNALS_PER_DAY"] = ival
                changes.append(f"MAX_SIGNALS_PER_DAY → {ival}")
            elif key in ("interval", "scan_interval"):
                CONFIG["SCAN_INTERVAL_SECONDS"] = ival
                changes.append(f"SCAN_INTERVAL_SECONDS → {ival} сек")
        if not changes:
            msg = (
                "Не удалось разобрать параметры.\n"
                "Пример: <code>/settings volume=70000000 max_signals=5 interval=900</code>"
            )
            send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
        else:
            msg = "<b>⚙️ Обновлённые настройки:</b>\n" + "\n".join(f"• {c}" for c in changes)
            send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/risk_off" or lower == "🛑 risk off":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        if STATE:
            STATE.activate_risk_off(CONFIG["RISK_OFF_DEFAULT_SECONDS"])
        msg = (
            "🛑 <b>Risk-OFF активирован.</b>\n"
            "Торговые сигналы временно отключены.\n"
            "Для включения используйте «🟢 Risk ON»."
        )
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/risk_on" or lower == "🟢 risk on":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        if STATE:
            STATE.deactivate_risk_off()
        msg = "🟢 <b>Risk-OFF отключён.</b>\nСигналы снова активны."
        send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)


def telegram_polling():
    global LAST_UPDATE_ID
    token = CONFIG["TG_BOT_TOKEN"]
    if not token:
        logger.warning("Telegram token не настроен, polling отключён.")
        return
    logger.info("Запуск Telegram bot polling...")
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    timeout = 30
    while not STOP_EVENT.is_set():
        params: Dict[str, object] = {"timeout": timeout}
        if LAST_UPDATE_ID is not None:
            params["offset"] = LAST_UPDATE_ID + 1
        try:
            resp = requests.get(url, params=params, timeout=timeout + 5)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка получения обновлений: {e}")
            time.sleep(5)
            continue
        results = data.get("result", [])
        for update in results:
            LAST_UPDATE_ID = update.get("update_id", LAST_UPDATE_ID)
            msg = update.get("message") or update.get("channel_post")
            if not msg:
                continue
            text = msg.get("text", "") or ""
            if text.startswith("/"):
                handle_command(msg)
            else:
                lower = text.lower()
                if lower in (
                    "🚀 старт",
                    "📊 статус",
                    "ℹ️ помощь",
                    "📴 стоп",
                    "📈 статистика",
                    "👥 подписчики",
                    "⚙️ настройки",
                    "🛑 risk off",
                    "🟢 risk on",
                ):
                    handle_command(msg)


def scan_market(state: BotState):
    state.reset_daily_if_needed()
    if state.is_risk_off():
        logger.info("Risk OFF режим активен, сканирование пропущено.")
        return
    if is_fomc_block_active():
        return
    if not state.can_send_signal():
        logger.info("Дневной лимит сигналов достигнут, сканирование пропущено.")
        return
    btc_ctx = get_btc_context()
    symbols = get_trading_symbols()
    if not symbols:
        return
    logger.info(f"Анализ {len(symbols)} символов...")
    signals_found: List[Dict] = []
    for symbol in symbols:
        if STOP_EVENT.is_set():
            return
        try:
            signal = analyze_symbol(symbol, btc_ctx)
            if signal is None:
                continue
            symbol_name = signal["symbol"]
            if not state.is_symbol_cooled_down(symbol_name):
                logger.info(
                    f"Сигнал по {symbol_name} отклонён: cooldown по символу ещё не вышел."
                )
                continue
            signal_id = f"{signal['symbol']}_{signal['direction']}_{signal['impulse_time']}"
            if signal_id in state.sent_signal_ids:
                continue
            signals_found.append(signal)
            logger.info(f"Найден сигнал: {signal['symbol']} {signal['direction']}")
        except Exception as e:
            logger.error(f"Ошибка анализа символа {symbol}: {e}", exc_info=True)
            continue
    if not signals_found:
        logger.info("Подходящих сигналов не найдено.")
        return
    signals_found.sort(key=lambda s: s["risk_pct"])
    signals_sent_this_scan = 0
    max_per_scan = CONFIG["MAX_SIGNALS_PER_SCAN"]
    for signal in signals_found:
        if not state.can_send_signal():
            logger.info("Достигнут дневной лимит сигналов, прекращаем отправку.")
            break
        if signals_sent_this_scan >= max_per_scan:
            logger.info("Достигнут лимит сигналов за этот скан, останавливаем отправку.")
            break
        msg = format_signal_message(signal)
        sent_count = broadcast_to_subscribers(msg, html=False)
        if sent_count > 0:
            signal_id = f"{signal['symbol']}_{signal['direction']}_{signal['impulse_time']}"
            state.register_signal(signal_id, signal["symbol"])
            signals_sent_this_scan += 1
            log_signal(signal)
            logger.info(
                f"Сигнал {signal['symbol']} {signal['direction']} отправлен "
                f"{sent_count} подписчикам."
            )
        time.sleep(1)
    logger.info(
        f"Сканирование завершено. Отправлено сигналов: "
        f"{signals_sent_this_scan}, всего за день: "
        f"{state.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}"
    )


def main():
    global STATE
    logger.info("=" * 60)
    logger.info("Запуск Binance Futures Signal Bot")
    logger.info("=" * 60)
    logger.info("Конфигурация:")
    logger.info(f"  - Минимальный объём: {CONFIG['MIN_QUOTE_VOLUME']:,} USDT")
    logger.info(f"  - Таймфрейм: {CONFIG['TIMEFRAME']}")
    logger.info(f"  - Старший ТФ: {CONFIG['HTF_TIMEFRAME']}")
    logger.info(f"  - Интервал сканирования: {CONFIG['SCAN_INTERVAL_SECONDS']} сек")
    logger.info(f"  - Лимит сигналов в день: {CONFIG['MAX_SIGNALS_PER_DAY']}")
    logger.info(f"  - Risk/Reward: {CONFIG['RISK_REWARD']}")
    logger.info(f"  - Мин. стоп: {CONFIG['MIN_RISK_PCT']}%")
    logger.info(f"  - Мин. тейк: {CONFIG['MIN_TP_PCT']}%")
    logger.info(f"  - Макс. сигналов за скан: {CONFIG['MAX_SIGNALS_PER_SCAN']}")
    logger.info(f"  - Cooldown на символ: {CONFIG['SYMBOL_COOLDOWN_SECONDS']} сек")
    logger.info(f"  - BTC фильтр: {'ON' if CONFIG['BTC_FILTER_ENABLED'] else 'OFF'}")
    if CONFIG["FOMC_DATES_UTC"]:
        logger.info("  - FOMC-окна: настроены (±1 час вокруг решения).")
    else:
        logger.info("  - FOMC-окна: не настроены (список дат пуст).")
    tg_token = CONFIG["TG_BOT_TOKEN"]
    tg_chat = CONFIG["TG_CHAT_ID"]
    if not tg_token:
        logger.error("TG_BOT_TOKEN не настроен. Выход.")
        return
    if not CONFIG["DATABASE_URL"]:
        logger.error("DATABASE_URL не настроен. Выход.")
        return
    db_init_and_load_subscribers()
    if tg_chat:
        db_add_subscriber(tg_chat, is_admin=True)
    subs_count = db_get_subscribers_count()
    logger.info(f"Подписчиков в БД после инициализации: {subs_count}")
    if tg_chat:
        welcome_msg = (
            "<b>🚀 Бот запущен!</b>\n\n"
            "Я сканирую Binance Futures (USDT-M). Основной анализ на 5m, "
            "тренд подтверждается на 15m. Используются EMA, RSI, StochRSI, MACD, "
            "ATR, BTC-контекст и фильтр уровней.\n\n"
            f"Первый скан будет через {CONFIG['SCAN_INTERVAL_SECONDS']} секунд.\n"
            f"Текущее количество подписчиков: {subs_count}."
        )
        send_telegram_message(
            welcome_msg,
            chat_id=tg_chat,
            html=True,
            reply_markup=get_reply_keyboard(tg_chat),
        )
    polling_thread = threading.Thread(target=telegram_polling, daemon=True)
    polling_thread.start()
    STATE = BotState()
    logger.info("Ожидание перед первым сканированием...")
    time.sleep(CONFIG["SCAN_INTERVAL_SECONDS"])
    try:
        while not STOP_EVENT.is_set():
            try:
                scan_market(STATE)
            except Exception as e:
                logger.error(f"Ошибка в цикле сканирования: {e}", exc_info=True)
            logger.info(
                f"Ожидание {CONFIG['SCAN_INTERVAL_SECONDS']} секунд до следующего сканирования..."
            )
            time.sleep(CONFIG["SCAN_INTERVAL_SECONDS"])
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки. Завершение работы...")
    finally:
        STOP_EVENT.set()
        logger.info("Бот остановлен.")


if __name__ == "__main__":
    main()
