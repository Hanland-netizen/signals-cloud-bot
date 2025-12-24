import os
import time
import math
import json
import logging
import signal
import threading
import re
from collections import deque
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

BINANCE_FAPI_URL = "https://fapi.binance.com"
TELEGRAM_API_URL = "https://api.telegram.org"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_ADMIN_ID = os.environ.get("TG_ADMIN_ID", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

FOMC_DATES_UTC: List[datetime] = []

# Кэш параметров символов (tickSize/stepSize) из exchangeInfo
SYMBOL_TICK_SIZE: Dict[str, float] = {}
SYMBOL_STEP_SIZE: Dict[str, float] = {}


CONFIG: Dict[str, Any] = {
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "15m",
    "SCAN_INTERVAL_SECONDS": 600,  # 10 минут
    "MAX_SIGNALS_PER_DAY": 8,
    "MAX_SIGNALS_PER_HOUR": 2,
    "MAX_SIGNALS_PER_SCAN": 1,  # Только 1 лучший из скана
    "MIN_SEND_GAP_SECONDS": 1800,  # 30 минут между отправками
    "MIN_QUOTE_VOLUME": 40_000_000,  # 40M USDT
    "RISK_REWARD": 1.7,
    "LEVERAGE": 15,  # стандартное плечо для сигналов

    "MIN_ATR_PCT": 0.45,  # Убираем микроскальпы
    "MAX_ATR_PCT": 5.0,
    "MIN_STOP_PCT": 0.30,  # Стоп не слишком близко
    "MAX_STOP_PCT": 1.20,
    "MIN_TP_PCT": 0.70,  # Минимальный тейк 0.7%
    "STOP_BUFFER_LONG": 0.20,
    "STOP_BUFFER_SHORT": 0.20,
    "TP_EXTRA_PCT": 0.15,
    "MIN_TP_DISTANCE_PCT": 0.50,  # Устаревший
    "SYMBOL_COOLDOWN_SECONDS": 28800,  # 8 часов
    "BTC_FILTER_ENABLED": True,
    "DEBUG_REASONS": False,
    
    # Строгое подтверждение 15m (MTF)
    "STRICT_MTF_CONFIRM": True,
    "MTF_REQUIRE_TREND": True,        # 15m тоже по тренду (относительно EMA200)
    "MTF_REQUIRE_MACD": True,         # MACD 15m в сторону сделки
    "MTF_REQUIRE_RSI": True,          # RSI 15m в сторону сделки
    "MTF_RSI_LONG_MIN": 52.0,         # long: RSI15m >= 52
    "MTF_RSI_SHORT_MAX": 48.0,        # short: RSI15m <= 48
    "MTF_NEUTRAL_BODY_PCT": 0.10,     # порог для нейтральной свечи (doji)
    
    # ✅ НОВЫЙ: Фильтры против late-entry
    "RSI_SHORT_MIN": 40.0,            # short запрещён если RSI < 40 (перепродано)
    "RSI_LONG_MAX": 60.0,             # long запрещён если RSI > 60 (перекуплено)
    "STOCH_SHORT_MIN": 70.0,          # short только если StochRSI >= 70 (от перекупленности)
    "STOCH_LONG_MAX": 30.0,           # long только если StochRSI <= 30 (от перепроданности)
}


class SignalState:
    def __init__(self) -> None:
        self.signals_sent_today: int = 0
        self.signals_sent_this_hour: int = 0
        self.total_signals_sent: int = 0
        self.last_reset_date: date = date.today()
        self.last_hour_reset: int = datetime.now().hour
        self.symbol_last_signal_ts: Dict[str, float] = {}
        self.risk_off: bool = False
        self.sent_signals_cache: set = set()

    def reset_if_new_day(self) -> None:
        today = date.today()
        if today != self.last_reset_date:
            logging.info("Новый день, обнуляем счётчики сигналов.")
            self.signals_sent_today = 0
            self.last_reset_date = today
            self.sent_signals_cache.clear()

    def reset_if_new_hour(self) -> None:
        """Сбрасываем часовой счётчик при смене часа"""
        current_hour = datetime.now().hour
        if current_hour != self.last_hour_reset:
            logging.info("Новый час, обнуляем часовой счётчик. Было: %d", self.signals_sent_this_hour)
            self.signals_sent_this_hour = 0
            self.last_hour_reset = current_hour

    def can_send_global(self) -> bool:
        """✅ НОВАЯ ФУНКЦИЯ: Проверяет только глобальные лимиты (день/час)"""
        self.reset_if_new_day()
        self.reset_if_new_hour()
        
        if self.signals_sent_today >= CONFIG["MAX_SIGNALS_PER_DAY"]:
            return False
        
        if self.signals_sent_this_hour >= CONFIG["MAX_SIGNALS_PER_HOUR"]:
            return False
        
        return True

    def can_send_symbol(self, symbol: str) -> bool:
        """✅ НОВАЯ ФУНКЦИЯ: Проверяет только cooldown по символу"""
        now = time.time()
        last_ts = self.symbol_last_signal_ts.get(symbol)
        if last_ts is not None and now - last_ts < CONFIG["SYMBOL_COOLDOWN_SECONDS"]:
            return False
        return True

    def can_send_signal(self, symbol: str) -> bool:
        """✅ УСТАРЕВШАЯ: Для обратной совместимости. Используйте can_send_global() и can_send_symbol()"""
        return self.can_send_global() and self.can_send_symbol(symbol)

    def register_signal(self, symbol: str) -> None:
        self.signals_sent_today += 1
        self.signals_sent_this_hour += 1
        self.total_signals_sent += 1
        self.symbol_last_signal_ts[symbol] = time.time()

    def is_risk_off(self) -> bool:
        return self.risk_off

    def set_risk_off(self, value: bool) -> None:
        self.risk_off = value


STATE = SignalState()

# ✅ Очередь отправки сигналов
SEND_QUEUE: deque = deque()
LAST_SEND_TS: float = 0.0


def normalize_command(text: str) -> str:
    """Нормализует команду, убирая эмодзи и лишние пробелы."""
    text = re.sub(r'^[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF]+\s*', '', text)
    return text.strip()


def enqueue_signal(signal_data: Dict[str, Any]) -> None:
    """✅ НОВАЯ ФУНКЦИЯ: Добавляет сигнал в очередь отправки"""
    global SEND_QUEUE
    SEND_QUEUE.append(signal_data)
    logging.info("Сигнал добавлен в очередь: %s %s (в очереди: %d)",
                 signal_data["symbol"], signal_data["side"], len(SEND_QUEUE))


def try_send_from_queue() -> None:
    """✅ ПЕРЕПИСАНО: Отправляет сигнал из очереди с правильными проверками"""
    global SEND_QUEUE, LAST_SEND_TS
    
    if not SEND_QUEUE:
        return
    
    now = time.time()
    
    # 1. Проверяем временной интервал (MIN_SEND_GAP_SECONDS)
    if now - LAST_SEND_TS < CONFIG["MIN_SEND_GAP_SECONDS"]:
        return
    
    # 2. Проверяем глобальные лимиты (день/час)
    if not STATE.can_send_global():
        logging.info("Достигнут глобальный лимит (день: %d/%d, час: %d/%d). Очередь: %d",
                     STATE.signals_sent_today, CONFIG["MAX_SIGNALS_PER_DAY"],
                     STATE.signals_sent_this_hour, CONFIG["MAX_SIGNALS_PER_HOUR"],
                     len(SEND_QUEUE))
        return
    
    # 3. Берём первый сигнал из очереди
    signal_data = SEND_QUEUE.popleft()
    symbol = signal_data["symbol"]
    signal_key = signal_data.get("signal_key")
    
    # 4. Проверяем антидубликат (может быть уже отправлен)
    if signal_key and signal_key in STATE.sent_signals_cache:
        logging.info("Сигнал %s уже был отправлен (дубликат), пропускаем", symbol)
        return  # НЕ возвращаем в очередь
    
    # 5. Проверяем cooldown для символа
    if not STATE.can_send_symbol(symbol):
        # ✅ ИСПРАВЛЕНО: Возвращаем в КОНЕЦ очереди, чтобы не блокировать
        SEND_QUEUE.append(signal_data)
        logging.info("Символ %s ещё в cooldown, возвращён в конец очереди (в очереди: %d)",
                     symbol, len(SEND_QUEUE))
        return
    
    # 6. Отправляем сигнал
    active_subs = db_get_active_subscribers()
    if not active_subs:
        logging.info("Нет активных подписчиков, сигнал пропущен")
        return
    
    text = build_signal_text(
        symbol=signal_data["symbol"],
        side=signal_data["side"],
        leverage=signal_data["leverage"],
        entry=signal_data["entry"],
        take_profit=signal_data["take_profit"],
        stop_loss=signal_data["stop_loss"],
        timeframe=CONFIG["TIMEFRAME"],
        ema200=signal_data["ema200"],
        rsi=signal_data["rsi"],
        impulse_time=signal_data["impulse_time"],
        atr_pct=signal_data["atr_pct"],
        macd=signal_data["macd"],
        stoch_rsi=signal_data["stoch_rsi"],
    )
    
    for cid in active_subs:
        send_telegram_message(text, chat_id=str(cid), html=True)
    
    # 7. Регистрируем отправку
    if signal_key:  # ✅ ИСПРАВЛЕНО: добавляем только если key truthy
        STATE.sent_signals_cache.add(signal_key)
    STATE.register_signal(symbol)
    LAST_SEND_TS = now
    
    # 8. Логируем в БД
    try:
        db_log_signal(signal_data, sent_to=len(active_subs))
    except Exception as e:
        logging.error("Не удалось записать сигнал в БД: %s", e)
    
    logging.info("✅ Сигнал отправлен: %s %s (score: %.2f, осталось в очереди: %d)",
                 symbol, signal_data["side"], signal_data.get("score", 0), len(SEND_QUEUE))


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
        [{"text": "ℹ️ Помощь"}, {"text": "🔴 Стоп"}],
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


def _get_tick_size(symbol: str) -> float:
    """Получить tickSize из кэша, при необходимости обновить exchangeInfo."""
    ts = SYMBOL_TICK_SIZE.get(symbol)
    if ts and ts > 0:
        return ts
    # fallback: обновим кэш один раз
    try:
        data = fetch_binance("/fapi/v1/exchangeInfo")
        for s in data.get("symbols", []):
            sym = s.get("symbol")
            if not sym:
                continue
            try:
                for f in s.get("filters", []):
                    if f.get("filterType") == "PRICE_FILTER":
                        SYMBOL_TICK_SIZE[sym] = float(f.get("tickSize", 0.0))
                    elif f.get("filterType") == "LOT_SIZE":
                        SYMBOL_STEP_SIZE[sym] = float(f.get("stepSize", 0.0))
            except Exception:
                continue
        ts = SYMBOL_TICK_SIZE.get(symbol)
        return ts if ts and ts > 0 else 0.0
    except Exception:
        return 0.0


def round_price_to_tick(symbol: str, price: float) -> float:
    """Округляет цену к шагу цены (tickSize) Binance Futures."""
    tick = _get_tick_size(symbol)
    if not tick or tick <= 0:
        # безопасный fallback: 1e-6
        return round(price, 6)
    # округление вниз к тик-спейсу (чтобы лимит/стоп не ушли за шаг)
    steps = math.floor(price / tick)
    rounded = steps * tick
    # устранение плавающей ошибки
    prec = max(0, int(round(-math.log10(tick), 0))) if tick < 1 else 0
    return float(f"{rounded:.{prec}f}")


def _fix_prices_for_side(symbol: str, side: str, entry: float, stop_loss: float, take_profit: float) -> Tuple[float, float, float]:
    """Гарантирует корректный порядок цен после округлений."""
    entry_r = round_price_to_tick(symbol, entry)
    sl_r = round_price_to_tick(symbol, stop_loss)
    tp_r = round_price_to_tick(symbol, take_profit)

    if side == "long":
        # должно быть: SL < ENTRY < TP
        if sl_r >= entry_r:
            sl_r = round_price_to_tick(symbol, entry_r * (1.0 - 0.001))  # -0.1% fallback
        if tp_r <= entry_r:
            tp_r = round_price_to_tick(symbol, entry_r * (1.0 + 0.0017))  # +0.17% fallback
    else:
        # short: TP < ENTRY < SL
        if sl_r <= entry_r:
            sl_r = round_price_to_tick(symbol, entry_r * (1.0 + 0.001))  # +0.1% fallback
        if tp_r >= entry_r:
            tp_r = round_price_to_tick(symbol, entry_r * (1.0 - 0.0017))  # -0.17% fallback

    return entry_r, sl_r, tp_r


def get_usdt_perp_symbols() -> List[str]:
    data = fetch_binance("/fapi/v1/exchangeInfo")
    symbols = []
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
            sym = s["symbol"]
            symbols.append(sym)
            # Сохраняем параметры цены/кол-ва для корректного округления
            try:
                for f in s.get("filters", []):
                    if f.get("filterType") == "PRICE_FILTER":
                        SYMBOL_TICK_SIZE[sym] = float(f.get("tickSize", 0.0))
                    elif f.get("filterType") == "LOT_SIZE":
                        SYMBOL_STEP_SIZE[sym] = float(f.get("stepSize", 0.0))
            except Exception:
                pass
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
    """✅ ИСПРАВЛЕНО: Используем закрытую свечу [-2]"""
    kl = fetch_binance(
        "/fapi/v1/klines",
        {"symbol": "BTCUSDT", "interval": "5m", "limit": 300},
    )
    _, _, _, closes, _ = kline_to_floats(kl)
    ema200 = calc_ema(closes, 200)
    rsi = calc_rsi(closes, 14)
    
    # ✅ ИСПРАВЛЕНО: Берём закрытую свечу, а не текущую
    idx = -2
    price = closes[idx]
    ema200_val = ema200[idx]
    rsi_val = rsi[idx]
    
    # Для 24h change используем ticker (он корректен)
    ticker = fetch_binance("/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"})
    change_pct = float(ticker.get("priceChangePercent", 0.0))
    
    ctx = {
        "price": price,
        "ema200": ema200_val,
        "rsi": rsi_val,
        "change_pct": change_pct,
    }
    logging.info(
        "BTC контекст: цена=%.2f, EMA200=%.2f, RSI=%.1f, 24h изменение=%.2f%%",
        price,
        ema200_val,
        rsi_val,
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
) -> Optional[Dict[str, Any]]:
    """
    Улучшенный анализ с:
    - Проверкой импульса
    - MTF подтверждением на 15m
    - Мягким BTC-фильтром
    - Защитой от шпилек
    """
    params = {"symbol": symbol, "interval": CONFIG["TIMEFRAME"], "limit": 300}
    kl_5m = fetch_binance("/fapi/v1/klines", params)
    o5, h5, l5, c5, t5 = kline_to_floats(kl_5m)
    
    params_htf = {"symbol": symbol, "interval": CONFIG["HTF_TIMEFRAME"], "limit": 200}
    kl_15m = fetch_binance("/fapi/v1/klines", params_htf)
    o15, h15, l15, c15, _ = kline_to_floats(kl_15m)

    ema200_5m = calc_ema(c5, 200)
    ema200_15m = calc_ema(c15, 200)
    rsi_5m = calc_rsi(c5, 14)
    rsi_15m = calc_rsi(c15, 14)
    atr_list = calc_atr(h5, l5, c5, 14)
    macd_line, signal_line = calc_macd(c5)
    stoch_rsi = calc_stoch_rsi(c5)
    
    # ✅ НОВЫЙ: MACD на 15m для строгого подтверждения
    macd15_line, macd15_signal = calc_macd(c15, 12, 26, 9)

    if len(c5) < 210 or len(ema200_5m) < 1 or len(atr_list) < 1 or len(ema200_15m) < 1:
        return None

    # ✅ ПАТЧ №1: Работаем ТОЛЬКО по закрытым свечам
    # Индекс последней ЗАКРЫТОЙ свечи на 5m
    idx_5m = -2
    
    close = c5[idx_5m]
    ema = ema200_5m[idx_5m]
    rsi = rsi_5m[idx_5m]
    macd_val = macd_line[idx_5m]
    macd_signal = signal_line[idx_5m]
    stoch_val = stoch_rsi[idx_5m]
    atr_abs = atr_list[idx_5m]
    atr_pct = atr_abs / close * 100.0
    
    # ✅ ПАТЧ №1: HTF тоже только закрытая свеча
    idx_15m = -2
    ema_htf = ema200_15m[idx_15m]
    rsi_htf = rsi_15m[idx_15m]
    htf_close = c15[idx_15m]
    
    # ✅ НОВЫЙ: MACD 15m по закрытой свече
    macd15 = macd15_line[idx_15m]
    macd15_sig = macd15_signal[idx_15m]

    # 1. ATR фильтр
    if not (CONFIG["MIN_ATR_PCT"] <= atr_pct <= CONFIG["MAX_ATR_PCT"]):
        if CONFIG.get("DEBUG_REASONS"):
            logging.info("%s отклонён по ATR: %.2f%% (допустимо %.2f—%.2f%%).",
                         symbol, atr_pct, CONFIG["MIN_ATR_PCT"], CONFIG["MAX_ATR_PCT"])
        return None

    # 2. Проверка импульса (важно для импульсной модели)
    # Импульсная свеча = предпоследняя закрытая (idx_5m уже -2)
    impulse_idx = len(c5) - 2
    impulse_close = c5[impulse_idx]
    impulse_open = o5[impulse_idx]
    impulse_body = abs(impulse_close - impulse_open)
    
    # Средний размер тела свечи за предыдущие 10 закрытых свечей
    avg_body = sum(abs(c5[i] - o5[i]) for i in range(len(c5) - 12, len(c5) - 2)) / 10
    
    # Импульс должен быть хотя бы на 20% больше среднего тела
    if impulse_body < avg_body * 1.2:
        if CONFIG.get("DEBUG_REASONS"):
            logging.info("%s отклонён: слабый импульс. body=%.6f avg=%.6f", 
                         symbol, impulse_body, avg_body)
        return None
    
    # ✅ УЛУЧШЕНИЕ №2: импульс должен быть значимым относительно ATR
    min_impulse_atr = atr_abs * 0.3
    if impulse_body < min_impulse_atr:
        if CONFIG.get("DEBUG_REASONS"):
            logging.info("%s отклонён: слабый импульс относительно ATR. body=%.6f min_atr=%.6f",
                         symbol, impulse_body, min_impulse_atr)
        return None

    # 3. Определение направления на 5m
    price_above = close > ema * 1.0002
    price_below = close < ema * 0.9998

    side: Optional[str] = None
    
    # ✅ УЛУЧШЕНИЕ №3: ужесточены RSI и StochRSI диапазоны
    # Long условия
    # Long условия
    if price_above and 50 < rsi < 70 and macd_val >= macd_signal:
        side = "long"
    # Short условия
    elif price_below and 30 < rsi < 50 and macd_val <= macd_signal:
        side = "short"

    if side is None:
        if CONFIG.get("DEBUG_REASONS"):
            logging.info("%s: нет направления. close=%.6f ema=%.6f rsi=%.1f macd=%.5f stoch=%.1f",
                         symbol, close, ema, rsi, macd_val, stoch_val)
        return None

    # ✅ НОВЫЙ: Фильтры против late-entry (применяются ПОСЛЕ определения side)
    # 1. RSI фильтр: не шортить перепроданное, не лонговать перекупленное
    # Зачем: Избежать входов после уже произошедшего движения (late entry)
    if side == "short":
        rsi_short_min = float(CONFIG.get("RSI_SHORT_MIN", 40.0))
        if rsi < rsi_short_min:
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: RSI слишком низкий для short (late entry). rsi=%.1f < %.1f",
                             symbol, rsi, rsi_short_min)
            return None
    elif side == "long":
        rsi_long_max = float(CONFIG.get("RSI_LONG_MAX", 60.0))
        if rsi > rsi_long_max:
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: RSI слишком высокий для long (late entry). rsi=%.1f > %.1f",
                             symbol, rsi, rsi_long_max)
            return None
    
    # 2. StochRSI фильтр: входить в зонах разворота, а не в середине движения
    # Зачем: Short от перекупленности (>=70), Long от перепроданности (<=30) - лучшие точки входа
    if side == "short":
        stoch_short_min = float(CONFIG.get("STOCH_SHORT_MIN", 70.0))
        if stoch_val < stoch_short_min:
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: StochRSI слишком низкий для short (не от перекупленности). stoch=%.1f < %.1f",
                             symbol, stoch_val, stoch_short_min)
            return None
    elif side == "long":
        stoch_long_max = float(CONFIG.get("STOCH_LONG_MAX", 30.0))
        if stoch_val > stoch_long_max:
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: StochRSI слишком высокий для long (не от перепроданности). stoch=%.1f > %.1f",
                             symbol, stoch_val, stoch_long_max)
            return None

    # ✅ НОВЫЙ: Строгое подтверждение 15m (MTF)
    if CONFIG.get("STRICT_MTF_CONFIRM", True):
        # 1. Проверка тренда на 15m (цена относительно EMA200)
        # Зачем: Убедиться, что 15m тоже в нужном направлении, не против тренда
        if CONFIG.get("MTF_REQUIRE_TREND", True):
            if side == "long" and htf_close < ema_htf:
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF нет long тренда (strict). htf_close=%.6f < ema_htf=%.6f",
                                 symbol, htf_close, ema_htf)
                return None
            if side == "short" and htf_close > ema_htf:
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF нет short тренда (strict). htf_close=%.6f > ema_htf=%.6f",
                                 symbol, htf_close, ema_htf)
                return None
        
        # 2. Проверка RSI на 15m
        # Зачем: Избежать входов в зонах перекупленности/перепроданности на старшем ТФ
        if CONFIG.get("MTF_REQUIRE_RSI", True):
            mtf_rsi_long_min = float(CONFIG.get("MTF_RSI_LONG_MIN", 52.0))
            mtf_rsi_short_max = float(CONFIG.get("MTF_RSI_SHORT_MAX", 48.0))
            
            if side == "long" and rsi_htf < mtf_rsi_long_min:
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF RSI слишком низкий для long (strict). rsi_htf=%.1f < %.1f",
                                 symbol, rsi_htf, mtf_rsi_long_min)
                return None
            if side == "short" and rsi_htf > mtf_rsi_short_max:
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF RSI слишком высокий для short (strict). rsi_htf=%.1f > %.1f",
                                 symbol, rsi_htf, mtf_rsi_short_max)
                return None
        
        # 3. Проверка MACD на 15m
        # Зачем: Подтверждение импульса на старшем ТФ (MACD показывает силу движения)
        if CONFIG.get("MTF_REQUIRE_MACD", True):
            # Long: MACD выше сигнальной И >= 0 (бычий импульс)
            if side == "long" and not (macd15 > macd15_sig and macd15 >= 0):
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF MACD не подтверждает long (strict). macd15=%.5f sig=%.5f",
                                 symbol, macd15, macd15_sig)
                return None
            # Short: MACD ниже сигнальной И <= 0 (медвежий импульс)
            if side == "short" and not (macd15 < macd15_sig and macd15 <= 0):
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF MACD не подтверждает short (strict). macd15=%.5f sig=%.5f",
                                 symbol, macd15, macd15_sig)
                return None
        
        # 4. Проверка импульсной свечи 15m (МЯГКО: не против, а не обязательно за)
        # Зачем: Отсечь явно противоположные свечи, но разрешить нейтральные (doji)
        htf_impulse_idx = len(c15) - 2
        htf_impulse_close = c15[htf_impulse_idx]
        htf_impulse_open = o15[htf_impulse_idx]
        htf_body = abs(htf_impulse_close - htf_impulse_open)
        htf_body_pct = (htf_body / htf_impulse_close) * 100.0
        
        neutral_threshold = float(CONFIG.get("MTF_NEUTRAL_BODY_PCT", 0.10))
        
        # Если тело слишком маленькое - считаем нейтральной (разрешено)
        if htf_body_pct >= neutral_threshold:
            # Тело значимое - проверяем направление
            if side == "long" and htf_impulse_close < htf_impulse_open:
                # Явно медвежья свеча при long - запрещаем
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF свеча явно медвежья при long. body_pct=%.3f%%",
                                 symbol, htf_body_pct)
                return None
            if side == "short" and htf_impulse_close > htf_impulse_open:
                # Явно бычья свеча при short - запрещаем
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён: HTF свеча явно бычья при short. body_pct=%.3f%%",
                                 symbol, htf_body_pct)
                return None
        # Если тело < neutral_threshold - пропускаем (нейтральная свеча OK)

    # 5. BTC-фильтр (МЯГКИЙ - только жёсткие условия)
    if CONFIG["BTC_FILTER_ENABLED"]:
        btc_price = btc_ctx["price"]
        btc_ema = btc_ctx["ema200"]
        btc_rsi = btc_ctx["rsi"]
        btc_change = btc_ctx["change_pct"]

        if side == "long":
            # Блокируем long только при явном медвежьем рынке BTC
            if btc_price < btc_ema * 0.996 or btc_rsi < 38 or btc_change < -6.0:
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён по BTC-фильтру для long. BTC: price=%.2f ema=%.2f rsi=%.1f change=%.2f%%",
                                 symbol, btc_price, btc_ema, btc_rsi, btc_change)
                return None
        else:  # short
            # Блокируем short только при явном бычьем рынке BTC
            if btc_price > btc_ema * 1.004 or btc_rsi > 62 or btc_change > 6.0:
                if CONFIG.get("DEBUG_REASONS"):
                    logging.info("%s отклонён по BTC-фильтру для short. BTC: price=%.2f ema=%.2f rsi=%.1f change=%.2f%%",
                                 symbol, btc_price, btc_ema, btc_rsi, btc_change)
                return None

    # 6. Расчёт стопа и тейка с защитой от шпилек
    impulse_low = l5[impulse_idx]
    impulse_high = h5[impulse_idx]
    impulse_time = datetime.fromtimestamp(t5[impulse_idx] / 1000, timezone.utc)

    # ✅ ПАТЧ №2: Стоп считать ТОЛЬКО по закрытым свечам
    # Берём swing за последние 4 закрытые свечи (исключая текущую "живую")
    swing_lookback = 4
    swing_low = min(l5[-(swing_lookback + 1):-1])
    swing_high = max(h5[-(swing_lookback + 1):-1])
    
    buf_long = float(CONFIG.get("STOP_BUFFER_LONG", 0.30)) / 100.0
    buf_short = float(CONFIG.get("STOP_BUFFER_SHORT", 0.30)) / 100.0
    tp_extra = 1.0 + float(CONFIG.get("TP_EXTRA_PCT", 0.15)) / 100.0

    if side == "long":
        stop_loss = swing_low * (1.0 - buf_long)
        # ✅ НОВОЕ: стоп не должен быть слишком близко к входу относительно ATR
        min_stop_abs = atr_abs * float(CONFIG.get("MIN_STOP_ATR_MULT", 0.60))
        if (close - stop_loss) < min_stop_abs:
            stop_loss = close - min_stop_abs
        # Защита: стоп не может быть выше или равен входу
        if stop_loss >= close:
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: стоп long выше входа. stop=%.6f close=%.6f",
                             symbol, stop_loss, close)
            return None
        stop_pct = abs((close - stop_loss) / close) * 100.0
        
        # ✅ QUALITY: Проверка MIN_STOP_PCT
        if not (CONFIG["MIN_STOP_PCT"] <= stop_pct <= CONFIG["MAX_STOP_PCT"]):
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: стоп %.3f%% вне диапазона %.3f—%.3f%%",
                             symbol, stop_pct, CONFIG["MIN_STOP_PCT"], CONFIG["MAX_STOP_PCT"])
            return None
        
        take_profit = close + (close - stop_loss) * CONFIG["RISK_REWARD"] * tp_extra
        tp_pct = abs((take_profit - close) / close) * 100.0
    else:  # short
        stop_loss = swing_high * (1.0 + buf_short)
        # ✅ НОВОЕ: стоп не должен быть слишком близко к входу относительно ATR
        min_stop_abs = atr_abs * float(CONFIG.get("MIN_STOP_ATR_MULT", 0.60))
        if (stop_loss - close) < min_stop_abs:
            stop_loss = close + min_stop_abs
        # Защита: стоп не может быть ниже или равен входу
        if stop_loss <= close:
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: стоп short ниже входа. stop=%.6f close=%.6f",
                             symbol, stop_loss, close)
            return None
        stop_pct = abs((stop_loss - close) / close) * 100.0
        
        # ✅ QUALITY: Проверка MIN_STOP_PCT
        if not (CONFIG["MIN_STOP_PCT"] <= stop_pct <= CONFIG["MAX_STOP_PCT"]):
            if CONFIG.get("DEBUG_REASONS"):
                logging.info("%s отклонён: стоп %.3f%% вне диапазона %.3f—%.3f%%",
                             symbol, stop_pct, CONFIG["MIN_STOP_PCT"], CONFIG["MAX_STOP_PCT"])
            return None
        
        take_profit = close - (stop_loss - close) * CONFIG["RISK_REWARD"] * tp_extra
        tp_pct = abs((close - take_profit) / close) * 100.0
    leverage = int(CONFIG.get("LEVERAGE", 15))
    # ✅ QUALITY: Фильтр минимального тейка (главный против скальпов)
    if tp_pct < CONFIG["MIN_TP_PCT"]:
        if CONFIG.get("DEBUG_REASONS"):
            logging.info("%s отклонён: тейк слишком близко %.3f%% (мин %.3f%%)",
                         symbol, tp_pct, CONFIG["MIN_TP_PCT"])
        return None

    # ✅ ПАТЧ №4: Защита от дублей
    signal_key = f"{symbol}_{side}_{impulse_time.isoformat()}"
    if signal_key in STATE.sent_signals_cache:
        if CONFIG.get("DEBUG_REASONS"):
            logging.info("%s отклонён: дубликат сигнала (уже отправлен)", symbol)
        return None

    # ✅ НОВЫЙ: Расчёт score для выбора лучшего сигнала
    # Учитываем: ATR (волатильность), MACD (импульс), расстояние от EMA (тренд)
    distance_from_ema_pct = abs((close - ema) / ema) * 100.0
    score = (atr_pct * 10.0) + (abs(macd_val) * 2.0) + (distance_from_ema_pct * 100.0)

    # ✅ Округляем entry/SL/TP к tickSize Binance и страхуем порядок цен
    entry_r, stop_r, tp_r = _fix_prices_for_side(symbol, side, close, stop_loss, take_profit)

    return {
        "symbol": symbol,
        "side": side,
        "leverage": leverage,
        "entry": entry_r,
        "take_profit": tp_r,
        "stop_loss": stop_r,
        "ema200": ema,
        "rsi": rsi,
        "impulse_time": impulse_time,
        "atr_pct": atr_pct,
        "stop_pct": stop_pct,
        "tp_pct": tp_pct,
        "macd": macd_val,
        "stoch_rsi": stoch_val,
        "signal_key": signal_key,  # Для регистрации в кэше
        "score": score,  # ✅ НОВЫЙ: для выбора лучшего
    }


def scan_market_and_send_signals() -> int:
    """✅ ФИНАЛЬНАЯ ВЕРСИЯ: Только 1 лучший сигнал из скана"""
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

    # 1. Собираем ВСЕ кандидатов
    candidates: List[Dict[str, Any]] = []
    
    for symbol in symbols:
        try:
            idea = analyse_symbol(symbol, btc_ctx)
        except Exception as e:
            logging.error("Ошибка анализа %s: %s", symbol, e)
            continue
            
        if idea:
            candidates.append(idea)
    
    if not candidates:
        logging.info("Сканирование завершено. Кандидатов не найдено.")
        return 0
    
    # 2. Сортируем по score (лучшие сверху)
    candidates.sort(key=lambda x: x.get("score", 0), reverse=True)
    
    # 3. ✅ Берём ТОЛЬКО 1 лучший (MAX_SIGNALS_PER_SCAN = 1)
    best_candidate = candidates[0]
    
    logging.info("Найдено кандидатов: %d. Лучший: %s %s (score: %.2f, ATR: %.2f%%, тейк: %.2f%%)",
                 len(candidates),
                 best_candidate["symbol"],
                 best_candidate["side"],
                 best_candidate.get("score", 0),
                 best_candidate.get("atr_pct", 0),
                 best_candidate.get("tp_pct", 0))
    
    # 4. Проверяем антидубликат ПЕРЕД добавлением в очередь
    signal_key = best_candidate.get("signal_key")
    if signal_key and signal_key in STATE.sent_signals_cache:
        logging.info("Лучший сигнал %s уже был отправлен (дубликат), пропускаем", best_candidate["symbol"])
        return 0
    
    # 5. Добавляем в очередь
    enqueue_signal(best_candidate)
    
    # 6. Пытаемся отправить из очереди сразу (если можем)
    try_send_from_queue()
    
    logging.info(
        "Сканирование завершено. В очереди: %d, отправлено за день: %d/%d, за час: %d/%d",
        len(SEND_QUEUE),
        STATE.signals_sent_today,
        CONFIG["MAX_SIGNALS_PER_DAY"],
        STATE.signals_sent_this_hour,
        CONFIG["MAX_SIGNALS_PER_HOUR"],
    )
    return 1


def handle_command(update: Dict[str, Any]) -> None:
    msg = update.get("message") or update.get("edited_message") or {}
    text_in = (msg.get("text", "") or "").strip()
    chat = msg.get("chat", {}) or {}
    chat_id = str(chat.get("id", ""))
    user = msg.get("from", {}) or {}
    user_id = str(user.get("id", ""))

    if not chat_id:
        return

    is_admin = bool(TG_ADMIN_ID) and (user_id == str(TG_ADMIN_ID))
    kb = get_reply_keyboard(chat_id)

    lower = normalize_command(text_in).lower()
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

    if first_token in ("/stop",) or lower in ("стоп", "🔴 стоп"):
        db_unsubscribe(chat_id)
        send_telegram_message(
            "🔴 Подписка выключена. Если передумаете — нажмите 🚀 Старт.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
        return

    if first_token in ("/help",) or lower in ("помощь", "ℹ️ помощь"):
        help_text = (
            "<b>ℹ️ Помощь</b>\n\n"
            "• 🚀 <b>Старт</b> — подписаться на сигналы\n"
            "• 🔴 <b>Стоп</b> — отписаться\n"
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
            f"🔥 ATR-фильтр: {CONFIG['MIN_ATR_PCT']}—{CONFIG['MAX_ATR_PCT']}%",
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
        send_telegram_message(
            "Я пока не понимаю эту команду.\nИспользуйте кнопки под полем ввода или /help.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
        return

    if first_token in ("/admin",) or lower in ("админ", "🛠 админ"):
        msg_admin = (
            "<b>🛠 Админ-панель</b>\n\n"
            f"👥 Подписчиков: {db_get_subscribers_count()}\n"
            f"📌 Сигналы сегодня: {STATE.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}\n"
            f"🛑 Risk OFF: {'ON' if STATE.is_risk_off() else 'OFF'}\n"
            f"💹 BTC фильтр: {'ON' if CONFIG['BTC_FILTER_ENABLED'] else 'OFF'}\n"
            f"🔥 ATR min/max: {CONFIG['MIN_ATR_PCT']}—{CONFIG['MAX_ATR_PCT']}%\n\n"
            "Используйте кнопки админа ниже 👇"
        )
        send_telegram_message(msg_admin, chat_id=chat_id, html=True, reply_markup=kb)
        return

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

    if first_token in ("/risk_off",) or lower in ("🛑 risk off", "risk off"):
        STATE.set_risk_off(True)
        send_telegram_message("🛑 Режим <b>Risk OFF</b> включён. Сканирование остановлено.", chat_id=chat_id, html=True, reply_markup=kb)
        return

    if first_token in ("/risk_on",) or lower in ("✅ risk on", "risk on"):
        STATE.set_risk_off(False)
        send_telegram_message("✅ Режим <b>Risk OFF</b> выключен. Сканирование включено.", chat_id=chat_id, html=True, reply_markup=kb)
        return

    if first_token in ("/scan",) or lower in ("🧪 тест-скан", "тест-скан", "тест скан"):
        send_telegram_message("🧪 Тест-скан запущен…\n⏳ Это может занять 10—60 секунд.", chat_id=chat_id, html=False, reply_markup=kb)

        def _run_scan_async(admin_chat_id: str) -> None:
            try:
                sent = scan_market_and_send_signals()
                send_telegram_message(f"✅ 🧪 Тест-скан завершён. Отправлено сигналов: {sent}", chat_id=admin_chat_id, html=False, reply_markup=kb)
            except Exception as e:
                send_telegram_message(f"❌ Ошибка тест-скана: {e}", chat_id=admin_chat_id, html=False, reply_markup=kb)

        threading.Thread(target=_run_scan_async, args=(chat_id,), daemon=True).start()
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
        return
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
            text = msg.get("text", "") or ""
            if text.startswith("/") or any(kw in text.lower() for kw in ["старт", "стоп", "помощь", "статус", "админ", "настройки", "risk", "тест-скан", "статистика", "мой id"]):
                handle_command(upd)
            else:
                chat_id = str(msg.get("chat", {}).get("id"))
                send_telegram_message(
                    "Я пока не понимаю эту команду.\n"
                    "Пожалуйста, используйте кнопки под полем ввода или /help.",
                    chat_id=chat_id,
                    html=False,
                    reply_markup=get_reply_keyboard(chat_id),
                )


def main_loop() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не задан. Выход.")
        return
    db_ensure_tables()

    logging.info("=" * 60)
    logging.info("Запуск Binance Futures Signal Bot (УЛУЧШЕННАЯ ВЕРСИЯ)")
    logging.info("=" * 60)
    logging.info("Конфигурация:")
    logging.info("  - Минимальный объём: %s USDT", f"{CONFIG['MIN_QUOTE_VOLUME']:,}")
    logging.info("  - Таймфрейм: %s + %s", CONFIG["TIMEFRAME"], CONFIG["HTF_TIMEFRAME"])
    logging.info("  - Интервал сканирования: %d сек", CONFIG["SCAN_INTERVAL_SECONDS"])
    logging.info("  - Лимит сигналов в день: %d", CONFIG["MAX_SIGNALS_PER_DAY"])
    logging.info("  - Лимит сигналов в час: %d", CONFIG["MAX_SIGNALS_PER_HOUR"])
    logging.info("  - Макс. сигналов за скан: %d", CONFIG["MAX_SIGNALS_PER_SCAN"])
    logging.info("  - Мин. интервал между отправками: %d сек", CONFIG["MIN_SEND_GAP_SECONDS"])
    logging.info("  - Risk/Reward: %.2f", CONFIG["RISK_REWARD"])
    logging.info("  - Мин. стоп: %.3f%%", CONFIG["MIN_STOP_PCT"])
    logging.info("  - Мин. ATR: %.3f%%", CONFIG["MIN_ATR_PCT"])
    logging.info("  - BTC фильтр: %s", "ON" if CONFIG["BTC_FILTER_ENABLED"] else "OFF")
    logging.info("  - Cooldown на символ: %d сек", CONFIG["SYMBOL_COOLDOWN_SECONDS"])

    last_scan_ts = 0.0

    def handle_sigterm(signum, frame):
        logging.info("Получен сигнал остановки. Завершение работы...")
        raise SystemExit

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    threading.Thread(target=telegram_polling_loop, daemon=True).start()

    while True:
        now = time.time()
        
        # ✅ НОВАЯ ЛОГИКА: Пытаемся отправить из очереди при каждой итерации
        try:
            try_send_from_queue()
        except Exception as e:
            logging.error("Ошибка при отправке из очереди: %s", e)
        
        # Сканируем рынок по расписанию
        if now - last_scan_ts >= CONFIG["SCAN_INTERVAL_SECONDS"]:
            logging.info("Начало сканирования рынка...")
            try:
                scan_market_and_send_signals()
            except Exception as e:
                logging.error("Ошибка при сканировании рынка: %s", e)
            last_scan_ts = time.time()
        
        time.sleep(1)


if __name__ == "__main__":
    try:
        main_loop()
    except SystemExit:
        logging.info("Бот остановлен.")
    except Exception as e:
        logging.error("Критическая ошибка: %s", e)