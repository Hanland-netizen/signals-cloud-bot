import os
import sys
import time
import math
import json
import logging
import signal
import threading
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import RealDictCursor


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

BINANCE_FAPI_URL = "https://fapi.binance.com"
TELEGRAM_API_URL = "https://api.telegram.org"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_ADMIN_ID = os.environ.get("TG_ADMIN_ID", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

FOMC_DATES_UTC: List[datetime] = []

CONFIG: Dict[str, Any] = {
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "15m",
    "SCAN_INTERVAL_SECONDS": 600,
    "MAX_SIGNALS_PER_DAY": 10,
    "MIN_QUOTE_VOLUME": 50_000_000,
    "RISK_REWARD": 1.7,
    "MIN_ATR_PCT": 0.15,
    "MAX_ATR_PCT": 5.0,
    "MIN_STOP_PCT": 0.15,
    "MAX_STOP_PCT": 0.80,
    "MAX_SIGNALS_PER_SCAN": 1,
    "GLOBAL_SIGNAL_COOLDOWN_SECONDS": 2400,
    "SYMBOL_COOLDOWN_SECONDS": 3600,
    "BTC_FILTER_ENABLED": True,
    "EXCLUDED_SYMBOLS": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],

}


class SignalState:
    def __init__(self) -> None:
        self.signals_sent_today: int = 0
        self.total_signals_sent: int = 0
        self.last_reset_date: date = date.today()
        self.symbol_last_signal_ts: Dict[str, float] = {}
        self.last_any_signal_ts: Optional[float] = None
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
        # Global cooldown between ANY signals
        gcd = float(CONFIG.get("GLOBAL_SIGNAL_COOLDOWN_SECONDS", 0) or 0)
        if gcd > 0 and self.last_any_signal_ts is not None and (now - self.last_any_signal_ts) < gcd:
            return False
        last_ts = self.symbol_last_signal_ts.get(symbol)
        if (
            last_ts is not None
            and now - last_ts < CONFIG["SYMBOL_COOLDOWN_SECONDS"]
        ):
            return False
        return True

    def register_signal(self, symbol: str) -> None:
        self.signals_sent_today += 1
        self.total_signals_sent += 1
        self.symbol_last_signal_ts[symbol] = time.time()
        self.last_any_signal_ts = self.symbol_last_signal_ts[symbol]

    def is_risk_off(self) -> bool:
        return self.risk_off

    def set_risk_off(self, value: bool) -> None:
        self.risk_off = value


STATE = SignalState()


def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = True
    return conn


DB_CONN = None


def db_ensure_tables() -> None:
    """Создаёт таблицы БД, если они отсутствуют."""
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

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals_log (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                htf_timeframe TEXT NOT NULL,
                entry DOUBLE PRECISION NOT NULL,
                stop_loss DOUBLE PRECISION NOT NULL,
                take_profit DOUBLE PRECISION NOT NULL,
                atr_pct DOUBLE PRECISION NOT NULL,
                stop_pct DOUBLE PRECISION NOT NULL,
                tp_pct DOUBLE PRECISION NOT NULL,
                sent_to INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_log_ts ON signals_log(ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_log_symbol ON signals_log(symbol);")

    DB_CONN.commit()
    logging.info("Таблицы subscribers/signals_log проверены/созданы.")


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
    logging.info(
        "Добавлен подписчик в БД: %s (admin=%s)", chat_id, "True" if is_admin else "False"
    )


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
        cur.execute(
            "SELECT chat_id FROM subscribers WHERE is_active = TRUE;"
        )
        rows = cur.fetchall()
    return [int(r["chat_id"]) for r in rows]


def db_get_subscribers_count() -> int:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM subscribers WHERE is_active = TRUE;"
        )
        row = cur.fetchone()
    return int(row["c"]) if row else 0

def db_log_signal(idea: Dict[str, Any], sent_to: int) -> None:
    """Логируем сигнал в БД для статистики в админке."""
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute(
            """
            INSERT INTO signals_log
                (symbol, side, timeframe, htf_timeframe, entry, stop_loss, take_profit, atr_pct, stop_pct, tp_pct, sent_to)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                idea.get("symbol"),
                idea.get("side"),
                CONFIG["TIMEFRAME"],
                CONFIG["HTF_TIMEFRAME"],
                float(idea.get("entry", 0.0)),
                float(idea.get("stop_loss", 0.0)),
                float(idea.get("take_profit", 0.0)),
                float(idea.get("atr_pct", 0.0)),
                float(idea.get("stop_pct", 0.0)),
                float(idea.get("tp_pct", 0.0)),
                int(sent_to),
            ),
        )
    DB_CONN.commit()

def db_fetch_one(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def db_fetch_all(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def admin_stats_text(days: int = 7) -> str:
    """Операционная статистика (без PnL) за последние N дней."""
    total = db_fetch_one("SELECT COUNT(*) AS c FROM signals_log;") or {"c": 0}
    last_n = db_fetch_one("SELECT COUNT(*) AS c FROM signals_log WHERE ts >= NOW() - (%s || ' days')::interval;", (days,)) or {"c": 0}
    last_24 = db_fetch_one("SELECT COUNT(*) AS c FROM signals_log WHERE ts >= NOW() - interval '24 hours';") or {"c": 0}

    avg = db_fetch_one(
        "SELECT AVG(atr_pct) AS a_atr, AVG(stop_pct) AS a_stop, AVG(tp_pct) AS a_tp "
        "FROM signals_log WHERE ts >= NOW() - (%s || ' days')::interval;",
        (days,),
    ) or {}

    sides = db_fetch_all(
        "SELECT side, COUNT(*) AS c FROM signals_log WHERE ts >= NOW() - (%s || ' days')::interval "
        "GROUP BY side ORDER BY c DESC;",
        (days,),
    )
    side_str = ", ".join([f"{r['side']}: {r['c']}" for r in sides]) if sides else "нет"

    top = db_fetch_all(
        "SELECT symbol, COUNT(*) AS c FROM signals_log WHERE ts >= NOW() - (%s || ' days')::interval "
        "GROUP BY symbol ORDER BY c DESC LIMIT 7;",
        (days,),
    )
    top_text = "\n".join([f"• {r['symbol']}: {r['c']}" for r in top]) if top else "нет данных"

    last = db_fetch_one("SELECT ts, symbol, side, atr_pct, stop_pct, tp_pct FROM signals_log ORDER BY ts DESC LIMIT 1;")
    last_line = "нет"
    if last:
        ts = last.get("ts")
        ts_str = ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if isinstance(ts, datetime) else str(ts)
        last_line = (
            f"{ts_str} — {last.get('symbol')} {last.get('side')} "
            f"(ATR {float(last.get('atr_pct') or 0):.2f}%, стоп {float(last.get('stop_pct') or 0):.2f}%, тейк {float(last.get('tp_pct') or 0):.2f}%)"
        )

    return (
        "<b>📈 Статистика сигналов</b>\n"
        f"Период: последние {days} дней\n\n"
        f"Всего сигналов: <b>{int(total.get('c', 0))}</b>\n"
        f"За {days} дней: <b>{int(last_n.get('c', 0))}</b>\n"
        f"За 24 часа: <b>{int(last_24.get('c', 0))}</b>\n\n"
        f"Распределение: {side_str}\n"
        f"Средний ATR: {float(avg.get('a_atr') or 0):.2f}%\n"
        f"Средний стоп: {float(avg.get('a_stop') or 0):.2f}%\n"
        f"Средний тейк: {float(avg.get('a_tp') or 0):.2f}%\n\n"
        "<b>Топ символов:</b>\n"
        f"{top_text}\n\n"
        "<b>Последний сигнал:</b>\n"
        f"{last_line}\n\n"
        f"🕒 Обновлено: {datetime.now().strftime('%H:%M:%S')}"
    )


def db_get_admin_chat_ids() -> List[int]:
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = db_connect()
    with DB_CONN.cursor() as cur:
        cur.execute(
            "SELECT chat_id FROM subscribers WHERE is_admin = TRUE AND is_active = TRUE;"
        )
        rows = cur.fetchall()
    return [int(r["chat_id"]) for r in rows]


def send_telegram_request(method: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не задан.")
        return None
    url = f"{TELEGRAM_API_URL}/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=data, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error("Ошибка запроса к Telegram (%s): %s", method, e)
        return None


def send_telegram_message(
    text: str,
    chat_id: Optional[str] = None,
    html: bool = False,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> None:
    if chat_id is None:
        if not TG_ADMIN_ID:
            logging.error("Нет chat_id для отправки сообщения.")
            return
        chat_id = TG_ADMIN_ID
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
        logging.info("Сообщение отправлено в Telegram")
    else:
        logging.error("Ошибка отправки в Telegram: %s", res)


def get_reply_keyboard(chat_id: str) -> Dict[str, Any]:
    is_admin = (str(chat_id) == TG_ADMIN_ID) if TG_ADMIN_ID else False

    rows = [
        [{"text": "🚀 Старт"}, {"text": "📊 Статус"}],
        [{"text": "ℹ️ Помощь"}, {"text": "📴 Стоп"}],
        [{"text": "🆔 Мой ID"}],
    ]
    if is_admin:
        rows.append([{"text": "🛠 Админ"}, {"text": "⚙️ Настройки"}])
        rows.append([{"text": "🛑 Risk OFF"}, {"text": "✅ Risk ON"}])
        rows.append([{"text": "🧪 Тест-скан"}, {"text": "📈 Статистика"}])

    return {
        "keyboard": rows,
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def fetch_binance(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{BINANCE_FAPI_URL}{path}"
    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            delay = 2 ** attempt
            logging.error(
                "Ошибка запроса к %s (попытка %d/5): %s. Ждём %d сек перед повтором...",
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
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
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
    filtered = [
        s for s in symbols if vol_map.get(s, 0.0) >= CONFIG["MIN_QUOTE_VOLUME"]
    ]
    logging.info(
        "После фильтрации по объёму (>= %s USDT): %d символов",
        f"{CONFIG['MIN_QUOTE_VOLUME']:,}",
        len(filtered),
    )
    return filtered


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
    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period
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
        window = rsi[i - period + 1 : i + 1]
        rmin = min(window)
        rmax = max(window)
        if rmax - rmin == 0:
            stoch.append(50.0)
        else:
            stoch.append((rsi[i] - rmin) / (rmax - rmin) * 100.0)
    return stoch


def get_btc_context() -> Dict[str, Any]:
    kl = fetch_binance(
        "/fapi/v1/klines",
        {"symbol": "BTCUSDT", "interval": "5m", "limit": 300},
    )
    _, _, _, closes, _ = kline_to_floats(kl)
    ema200 = calc_ema(closes, 200)[-1]
    rsi = calc_rsi(closes, 14)[-1]
    ticker = fetch_binance("/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"})
    price = float(ticker.get("lastPrice", closes[-1]))
    change_pct = float(ticker.get("priceChangePercent", 0.0))
    ctx = {
        "price": price,
        "ema200": ema200,
        "rsi": rsi,
        "change_pct": change_pct,
    }
    logging.info(
        "BTC контекст: цена=%.2f, EMA200=%.2f, RSI=%.1f, 24h изменение=%.2f%%",
        price,
        ema200,
        rsi,
        change_pct,
    )
    return ctx


def in_fomc_window(now_utc: datetime) -> bool:
    if not FOMC_DATES_UTC:
        return False
    for dt in FOMC_DATES_UTC:
        if abs((now_utc - dt).total_seconds()) <= 3600:
            return True
    return False


def build_signal_text(
    symbol: str,
    side: str,
    leverage: int,
    entry: float,
    take_profit: float,
    stop_loss: float,
    timeframe: str,
    ema200: float,
    rsi: float,
    impulse_time: datetime,
    atr_pct: float,
    macd: float,
    stoch_rsi: float,
) -> str:
    arrow = "🟢" if side == "long" else "🔴"
    side_str = "long" if side == "long" else "short"
    impulse_str = impulse_time.isoformat()
    return (
        f"{arrow} <b>{symbol}</b> {side_str}\n"
        f"Плечо {leverage}х\n"
        f"Вход (ориентир) - {entry:.6f}\n"
        f"Тейк - {take_profit:.6f}\n"
        f"Стоп - {stop_loss:.6f}\n\n"
        f"Таймфрейм: {timeframe} (MTF: {CONFIG['HTF_TIMEFRAME']})\n"
        f"EMA200: {ema200:.5f}\n"
        f"RSI(14): {rsi:.1f}\n"
        f"ATR: {atr_pct:.2f}%\n"
        f"MACD: {macd:.5f}\n"
        f"StochRSI: {stoch_rsi:.1f}\n"
        f"Импульсная свеча (UTC): {impulse_str}\n\n"
        "Логика: импульсная свеча, стоп за экстремумом, тейк по RR "
        f"{CONFIG['RISK_REWARD']}, фильтр по тренду, ATR, BTC и осцилляторам."
    )


def analyse_symbol(
    symbol: str,
    btc_ctx: Dict[str, Any],
    klines_5m: List[Dict[str, Any]],
    klines_15m: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:

    # ❌ полностью исключаем BTC из сигналов
    if symbol == "BTCUSDT":
        return None


    params = {"symbol": symbol, "interval": CONFIG["TIMEFRAME"], "limit": 300}
    kl_5m = fetch_binance("/fapi/v1/klines", params)
    o5, h5, l5, c5, t5 = kline_to_floats(kl_5m)
    params_htf = {"symbol": symbol, "interval": CONFIG["HTF_TIMEFRAME"], "limit": 200}
    kl_15m = fetch_binance("/fapi/v1/klines", params_htf)
    _, _, _, c15, _ = kline_to_floats(kl_15m)

    ema200_5m = calc_ema(c5, 200)
    ema200_15m = calc_ema(c15, 200)
    rsi_5m = calc_rsi(c5, 14)
    atr_list = calc_atr(h5, l5, c5, 14)
    macd_line, signal_line = calc_macd(c5)
    stoch_rsi = calc_stoch_rsi(c5)

    if len(c5) < 210 or len(ema200_5m) < 1 or len(atr_list) < 1:
        return None

    close = c5[-1]
    ema = ema200_5m[-1]
    ema_htf = ema200_15m[-1] if ema200_15m else ema
    rsi = rsi_5m[-1]
    atr_abs = atr_list[-1]
    atr_pct = atr_abs / close * 100.0
    macd_val = macd_line[-1]
    macd_prev = macd_line[-2]
    macd_signal = signal_line[-1]
    stoch_val = stoch_rsi[-1]

    if not (CONFIG["MIN_ATR_PCT"] <= atr_pct <= CONFIG["MAX_ATR_PCT"]):
        logging.info(
            "%s отклонён по ATR: %.2f%% (допустимо %.2f–%.2f%%).",
            symbol,
            atr_pct,
            CONFIG["MIN_ATR_PCT"],
            CONFIG["MAX_ATR_PCT"],
        )
        return None

    price_above = close > ema * 1.001
    price_below = close < ema * 0.999

    side: Optional[str] = None
    if price_above and rsi > 50 and macd_val > macd_signal and stoch_val > 20:
        side = "long"
    elif price_below and rsi < 50 and macd_val < macd_signal and stoch_val < 80:
        side = "short"

    if side is None:
        return None

    if CONFIG["BTC_FILTER_ENABLED"]:
        btc_price = btc_ctx["price"]
        btc_ema = btc_ctx["ema200"]
        btc_rsi = btc_ctx["rsi"]
        btc_change = btc_ctx["change_pct"]
        if side == "long":
            if btc_price < btc_ema or btc_rsi < 45 or btc_change < -3.0:
                logging.info("%s отклонён по BTC-фильтру для long.", symbol)
                return None
        else:
            if btc_price > btc_ema or btc_rsi > 55 or btc_change > 3.0:
                logging.info("%s отклонён по BTC-фильтру для short.", symbol)
                return None

    impulse_idx = len(c5) - 2
    impulse_close = c5[impulse_idx]
    impulse_low = l5[impulse_idx]
    impulse_high = h5[impulse_idx]
    impulse_time = datetime.fromtimestamp(t5[impulse_idx] / 1000, timezone.utc)

    if side == "long":
        stop_loss = impulse_low * 0.999
        stop_pct = abs((close - stop_loss) / close) * 100.0
        if not (CONFIG["MIN_STOP_PCT"] <= stop_pct <= CONFIG["MAX_STOP_PCT"]):
            return None
        take_profit = close + (close - stop_loss) * CONFIG["RISK_REWARD"]
    else:
        stop_loss = impulse_high * 1.001
        stop_pct = abs((stop_loss - close) / close) * 100.0
        if not (CONFIG["MIN_STOP_PCT"] <= stop_pct <= CONFIG["MAX_STOP_PCT"]):
            return None
        take_profit = close - (stop_loss - close) * CONFIG["RISK_REWARD"]

    leverage = 20 if side == "short" else 20

    # проценты для статистики
    if side == "long":
        tp_pct = abs((take_profit - close) / close) * 100.0
    else:
        tp_pct = abs((close - take_profit) / close) * 100.0

    return {
        "symbol": symbol,
        "side": side,
        "leverage": leverage,
        "entry": close,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "ema200": ema,
        "rsi": rsi,
        "impulse_time": impulse_time,
        "atr_pct": atr_pct,
        "stop_pct": stop_pct,
        "tp_pct": tp_pct,
        "macd": macd_val,
        "stoch_rsi": stoch_val,
    }


def scan_market_and_send_signals() -> int:
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
    logging.info("Анализ %d символов...", len(symbols))

    active_subs = db_get_active_subscribers()
    if not active_subs:
        logging.info("Нет активных подписчиков, сигналы отправляться не будут.")
        return 0
    signals_for_scan = 0
    for symbol in symbols:
        if symbol in CONFIG.get("EXCLUDED_SYMBOLS", []):
            continue
        if signals_for_scan >= CONFIG["MAX_SIGNALS_PER_SCAN"]:
            break
        if not STATE.can_send_signal(symbol):
            continue
        idea = analyse_symbol(symbol, btc_ctx)
        if not idea:
            continue
        text = build_signal_text(
            symbol=idea["symbol"],
            side=idea["side"],
            leverage=idea["leverage"],
            entry=idea["entry"],
            take_profit=idea["take_profit"],
            stop_loss=idea["stop_loss"],
            timeframe=CONFIG["TIMEFRAME"],
            ema200=idea["ema200"],
            rsi=idea["rsi"],
            impulse_time=idea["impulse_time"],
            atr_pct=idea["atr_pct"],
            macd=idea["macd"],
            stoch_rsi=idea["stoch_rsi"],
        )
        for cid in active_subs:
            send_telegram_message(text, chat_id=str(cid), html=True)
        STATE.register_signal(symbol)
        signals_for_scan += 1
        try:
            db_log_signal(idea, sent_to=len(active_subs))
        except Exception as e:
            logging.error("Не удалось записать сигнал в БД: %s", e)
        logging.info(
            "Сигнал отправлен: %s %s", symbol, idea["side"]
        )

    logging.info(
        "Сканирование завершено. Отправлено сигналов: %d, всего за день: %d/%d",
        signals_for_scan,
        STATE.signals_sent_today,
        CONFIG["MAX_SIGNALS_PER_DAY"],
    )
    return signals_for_scan


def handle_command(update: Dict[str, Any]) -> None:
    msg = update.get("message") or update.get("edited_message") or {}
    text_in = (msg.get("text", "") or "").strip()
    chat = msg.get("chat", {}) or {}
    chat_id = str(chat.get("id", ""))
    user = msg.get("from", {}) or {}
    user_id = str(user.get("id", ""))
    username = (user.get("username") or "").strip()

    if not chat_id:
        return

    is_admin = bool(TG_ADMIN_ID) and (user_id == str(TG_ADMIN_ID))
    kb = get_reply_keyboard(chat_id)

    # нормализуем ввод (кнопки/эмодзи)
    lower = text_in.lower()
    first_token = (text_in.split()[:1] or [""])[0].lower()

    # ===== Пользовательские команды =====
    if first_token in ("/start",) or lower in ("старт", "🚀 старт"):
        db_add_or_update_subscriber(chat_id, is_admin=is_admin)
        send_telegram_message(
            "✅ Подписка включена. Буду присылать сигналы, когда появятся условия.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
        return

    if first_token in ("/stop",) or lower in ("стоп", "📴 стоп"):
        db_unsubscribe(chat_id)
        send_telegram_message(
            "📴 Подписка выключена. Если передумаете — нажмите 🚀 Старт.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
        return

    if first_token in ("/help",) or lower in ("помощь", "ℹ️ помощь"):
        help_text = (
            "<b>ℹ️ Помощь</b>\n\n"
            "• 🚀 <b>Старт</b> — подписаться на сигналы\n"
            "• 📴 <b>Стоп</b> — отписаться\n"
            "• 📊 <b>Статус</b> — параметры/режимы бота\n\n"
            "Если вы админ — появятся дополнительные кнопки."
        )
        send_telegram_message(help_text, chat_id=chat_id, html=True, reply_markup=kb)
        return

    if first_token in ("/id",) or lower in ("мой id", "🆔 мой id", "id"):
        send_telegram_message(
            f"🆔 Ваш Telegram ID: <code>{user_id}</code>",
            chat_id=chat_id,
            html=True,
            reply_markup=kb,
        )
        return

    if first_token in ("/status",) or lower in ("статус", "📊 статус"):
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
            f"🛑 Risk OFF: {risk_off_state}",
        ]
        if is_admin:
            msg_lines.append(f"👥 Подписчиков: {db_get_subscribers_count()}")
        msg_lines.append("")
        msg_lines.append(f"🕒 Обновлено: {datetime.now().strftime('%H:%M:%S')}")
        send_telegram_message("\n".join(msg_lines), chat_id=chat_id, html=True, reply_markup=kb)
        return

    # ===== Админские команды =====
    if not is_admin:
        # не админ и не распознали команду
        send_telegram_message(
            "Я пока не понимаю эту команду.\nИспользуйте кнопки под полем ввода или /help.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
        return

    # Админ: панель
    if first_token in ("/admin",) or lower in ("админ", "🛠 админ"):
        msg_admin = (
            "<b>🛠 Админ-панель</b>\n\n"
            f"👥 Подписчиков: {db_get_subscribers_count()}\n"
            f"📌 Сигналы сегодня: {STATE.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}\n"
            f"🛑 Risk OFF: {'ON' if STATE.is_risk_off() else 'OFF'}\n"
            f"💹 BTC фильтр: {'ON' if CONFIG['BTC_FILTER_ENABLED'] else 'OFF'}\n"
            f"🎯 k (STOP_ATR_MULTIPLIER): {CONFIG.get('STOP_ATR_MULTIPLIER', 0.6)}\n"
            f"🔥 ATR min/max: {CONFIG['MIN_ATR_PCT']}–{CONFIG['MAX_ATR_PCT']}%\n\n"
            "Используйте кнопки админа ниже 👇"
        )
        send_telegram_message(msg_admin, chat_id=chat_id, html=True, reply_markup=kb)
        return

    # Админ: статистика
    if first_token in ("/stats",) or lower.startswith("📈 статистика") or lower.startswith("статистика"):
        days = 7
        parts = text_in.split()
        if len(parts) >= 2:
            try:
                days = max(1, min(365, int(parts[1])))
            except Exception:
                days = 7
        send_telegram_message(admin_stats_text(days), chat_id=chat_id, html=True, reply_markup=kb)
        return

    # Админ: Risk OFF/ON
    if first_token in ("/risk_off",) or lower in ("🛑 risk off", "risk off"):
        STATE.set_risk_off(True)
        send_telegram_message("🛑 Режим <b>Risk OFF</b> включён. Сканирование остановлено.", chat_id=chat_id, html=True, reply_markup=kb)
        return

    if first_token in ("/risk_on",) or lower in ("✅ risk on", "risk on"):
        STATE.set_risk_off(False)
        send_telegram_message("✅ Режим <b>Risk OFF</b> выключен. Сканирование включено.", chat_id=chat_id, html=True, reply_markup=kb)
        return

    # Админ: тест-скан (асинхронно)
    if first_token in ("/scan",) or lower in ("🧪 тест-скан", "тест-скан", "тест скан"):
        send_telegram_message("🧪 Тест-скан запущен…\n⏳ Это может занять 10–60 секунд.", chat_id=chat_id, html=False, reply_markup=kb)

        def _run_scan_async(admin_chat_id: str) -> None:
            try:
                sent = scan_market_and_send_signals()
                send_telegram_message(f"✅ 🧪 Тест-скан завершён. Отправлено сигналов: {sent}", chat_id=admin_chat_id, html=False, reply_markup=kb)
            except Exception as e:
                send_telegram_message(f"❌ Ошибка тест-скана: {e}", chat_id=admin_chat_id, html=False, reply_markup=kb)

        threading.Thread(target=_run_scan_async, args=(chat_id,), daemon=True).start()
        return

    # Админ: настройки (если в коде есть /settings обработчик — оставляем отдельно ниже)
    if first_token in ("/settings",) or lower.startswith("⚙️ настройки"):
        # если ниже по файлу есть полноценный обработчик /settings — можно оставить;
        # здесь просто подсказка на случай, если настройки реализованы в отдельном блоке.
        send_telegram_message("⚙️ Настройки: используйте /settings и параметры, как в инструкции в админке.", chat_id=chat_id, html=False, reply_markup=kb)
        return

    send_telegram_message(
        "Я пока не понимаю эту команду.\nИспользуйте кнопки под полем ввода или /help.",
        chat_id=chat_id,
        html=False,
        reply_markup=kb,
    )


def telegram_polling_loop() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не задан. Завершение.")
        return 0
    last_update_id = None
    logging.info("Запуск Telegram bot polling...")
    while True:
        params: Dict[str, Any] = {"timeout": 25}
        if last_update_id is not None:
            params["offset"] = last_update_id + 1
        try:
            url = f"{TELEGRAM_API_URL}/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error("Ошибка получения обновлений: %s", e)
            time.sleep(5)
            continue
        if not data.get("ok"):
            time.sleep(2)
            continue
        for upd in data.get("result", []):
            last_update_id = upd.get("update_id", last_update_id)
            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            # ВАЖНО: пропускаем ВСЕ входящие сообщения в общий обработчик,
            # иначе часть кнопок (Админ/Статистика/Мой ID) никогда не сработает.
            handle_command(upd)

def main_loop() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не задан. Выход.")
        return 0
    db_ensure_tables()

    logging.info("=" * 60)
    logging.info("Запуск Binance Futures Signal Bot")
    logging.info("=" * 60)
    logging.info("Конфигурация:")
    logging.info("  - Минимальный объём: %s USDT", f"{CONFIG['MIN_QUOTE_VOLUME']:,}")
    logging.info("  - Таймфрейм: %s", CONFIG["TIMEFRAME"])
    logging.info("  - Интервал сканирования: %d сек", CONFIG["SCAN_INTERVAL_SECONDS"])
    logging.info("  - Лимит сигналов в день: %d", CONFIG["MAX_SIGNALS_PER_DAY"])
    logging.info("  - Risk/Reward: %.2f", CONFIG["RISK_REWARD"])
    logging.info("  - Мин. стоп: %.3f%%", CONFIG["MIN_STOP_PCT"])
    logging.info("  - Мин. ATR: %.3f%%", CONFIG["MIN_ATR_PCT"])
    logging.info("  - Макс. сигналов за скан: %d", CONFIG["MAX_SIGNALS_PER_SCAN"])
    logging.info("  - BTC фильтр: %s", "ON" if CONFIG["BTC_FILTER_ENABLED"] else "OFF")
    if not FOMC_DATES_UTC:
        logging.info("  - FOMC-окна: не настроены (список дат пуст).")
    else:
        logging.info("  - FOMC-окна: %d дат.", len(FOMC_DATES_UTC))

    last_scan_ts = 0.0

    def handle_sigterm(signum, frame):
        logging.info("Получен сигнал остановки. Завершение работы...")
        raise SystemExit

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

        # Start Telegram polling in background thread
    threading.Thread(target=telegram_polling_loop, daemon=True).start()

    while True:
        now = time.time()
        if now - last_scan_ts >= CONFIG["SCAN_INTERVAL_SECONDS"]:
            logging.info("Начало сканирования рынка...")
            try:
                scan_market_and_send_signals()
            except Exception as e:
                logging.error("Ошибка при сканировании рынка: %s", e)
            last_scan_ts = time.time()
            logging.info(
                "Ожидание %d секунд до следующего сканирования...",
                CONFIG["SCAN_INTERVAL_SECONDS"],
            )


        time.sleep(1)
if __name__ == "__main__":
    try:
        main_loop()
    except SystemExit:
        logging.info("Бот остановлен.")
    except Exception:
        logging.exception("Критическая ошибка")
