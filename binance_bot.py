import os
import time
import math
import json
import logging
import signal
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

CONFIG: Dict[str, Any] = {
    "TIMEFRAME": "5m",
    "HTF_TIMEFRAME": "15m",
    "SCAN_INTERVAL_SECONDS": 600,
    "MAX_SIGNALS_PER_DAY": 7,
    "MIN_QUOTE_VOLUME": 50_000_000,
    "RISK_REWARD": 1.7,
    "MIN_ATR_PCT": 0.05,
    "MAX_ATR_PCT": 5.0,
    "MIN_STOP_PCT": 0.15,
    "MAX_STOP_PCT": 0.80,
    "MAX_SIGNALS_PER_SCAN": 1,
    "SYMBOL_COOLDOWN_SECONDS": 1800,
    "BTC_FILTER_ENABLED": True,
}


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
    is_admin = TG_ADMIN_ID and str(chat_id) == TG_ADMIN_ID
    user_buttons = [
        [{"text": "🚀 Старт"}, {"text": "📊 Статус"}],
        [{"text": "ℹ️ Помощь"}, {"text": "📴 Стоп"}],
    ]
    if is_admin:
        user_buttons.append([{"text": "⚙️ Настройки"}])
    return {
        "keyboard": user_buttons,
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
) -> Optional[Dict[str, Any]]:
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
        "macd": macd_val,
        "stoch_rsi": stoch_val,
    }


def scan_market_and_send_signals() -> None:
    if STATE.is_risk_off():
        logging.info("Режим Risk OFF, сканирование пропускается.")
        return

    now_utc = datetime.now(timezone.utc)
    if in_fomc_window(now_utc):
        logging.info("Сейчас окно FOMC, сканирование отключено.")
        return

    btc_ctx = get_btc_context()
    symbols = get_usdt_perp_symbols()
    symbols = get_24h_volume_filter(symbols)
    logging.info("Анализ %d символов...", len(symbols))

    active_subs = db_get_active_subscribers()
    if not active_subs:
        logging.info("Нет активных подписчиков, сигналы отправляться не будут.")
        return

    signals_for_scan = 0
    for symbol in symbols:
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
        logging.info(
            "Сигнал отправлен: %s %s", symbol, idea["side"]
        )

    logging.info(
        "Сканирование завершено. Отправлено сигналов: %d, всего за день: %d/%d",
        signals_for_scan,
        STATE.signals_sent_today,
        CONFIG["MAX_SIGNALS_PER_DAY"],
    )


def handle_command(update: Dict[str, Any]) -> None:
    msg = update.get("message") or update.get("edited_message") or {}
    text = msg.get("text", "") or ""
    chat = msg.get("chat", {}) or {}
    chat_id = str(chat.get("id"))
    user = msg.get("from", {}) or {}
    user_id = str(user.get("id"))
    is_admin = TG_ADMIN_ID and user_id == TG_ADMIN_ID
    lower = text.strip().lower()
    first_token = text.split()[0]

    kb = get_reply_keyboard(chat_id)

    if first_token in ("/start", "🚀 старт"):
        db_add_or_update_subscriber(chat_id, is_admin=is_admin)
        if TG_ADMIN_ID and user_id != TG_ADMIN_ID and TG_ADMIN_ID.isdigit():
            admin_message = (
                f"Новый подписчик: <code>{chat_id}</code>\n"
                f"Имя: {user.get('first_name', '')} {user.get('last_name', '')}\n"
                f"Username: @{user.get('username', '')}"
            )
            send_telegram_message(
                admin_message,
                chat_id=TG_ADMIN_ID,
                html=True,
            )
        intro = (
            "🚀 <b>Бот запущен!</b> Вы подписались на торговые сигналы Binance Futures (USDT-M).\n\n"
            "Я сканирую Binance Futures, основной анализ на 5m, тренд подтверждается на 15m, "
            "учитываю EMA200, RSI, StochRSI, MACD, контекст BTCUSDT, ATR и базовые уровни.\n\n"
            "Основные команды:\n"
            "• 🚀 Старт — подписка или обновление клавиатуры\n"
            "• 📊 Статус — состояние бота\n"
            "• ℹ️ Помощь — описание логики\n"
            "• 📴 Стоп — отписаться от сигналов\n\n"
            f"Первый скан будет через {CONFIG['SCAN_INTERVAL_SECONDS']} секунд."
        )
        send_telegram_message(intro, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token in ("/help", "ℹ️ помощь"):
        msg_text = (
            "<b>ℹ️ Описание логики бота</b>\n\n"
            "• Основной таймфрейм: 5m\n"
            "• Подтверждение тренда: 15m\n"
            "• Фильтры: EMA200, RSI(14), StochRSI, MACD\n"
            "• Волатильность: ATR-фильтр по 5m свече\n"
            "• Контекст: BTCUSDT (тренд, RSI, 24h изменение)\n"
            "• Фильтрация по 24h объёму и дневному лимиту сигналов\n\n"
            "Сигналы не являются финансовой рекомендацией. Торгуйте на свой риск."
        )
        send_telegram_message(msg_text, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token in ("/stop", "📴 стоп"):
        db_unsubscribe(chat_id)
        send_telegram_message(
            "Вы отписались от сигналов. В любой момент можете вернуться с помощью команды /start.",
            chat_id=chat_id,
            html=False,
            reply_markup=kb,
        )
    elif first_token in ("/status", "📊 статус"):
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
        ]
        if is_admin:
            subs_count = db_get_subscribers_count()
            msg_lines.append(f"👥 Подписчиков: {subs_count}")
            if STATE:
                msg_lines.append(
                    f"📌 Сигналы сегодня: "
                    f"{STATE.signals_sent_today}/{CONFIG['MAX_SIGNALS_PER_DAY']}"
                )
        else:
            msg_lines.append("👤 Вы подписаны на сигналы этого бота.")
        msg_lines.append(f"🛑 Режим Risk OFF: {risk_off_state}")
        if FOMC_DATES_UTC:
            msg_lines.append("📅 FOMC-окна: настроены (бот не сканирует ±1 час).")
        else:
            msg_lines.append("📅 FOMC-окна: не заданы (список дат пуст).")
        msg = "\n".join(msg_lines)
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
                f"• SCAN_INTERVAL_SECONDS: {CONFIG['SCAN_INTERVAL_SECONDS']} сек\n"
                f"• MIN_ATR_PCT: {CONFIG['MIN_ATR_PCT']}%\n\n"
                "Чтобы изменить, используйте формат:\n"
                "<code>/settings volume=70000000 max_signals=5 interval=900 atr=0.05</code>"
            )
            send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
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
                    ival = int(val)
                except ValueError:
                    continue
                CONFIG["MIN_QUOTE_VOLUME"] = ival
                changes.append(f"MIN_QUOTE_VOLUME → {ival:,}")
            elif key in ("max_signals", "max_per_day"):
                try:
                    ival = int(val)
                except ValueError:
                    continue
                CONFIG["MAX_SIGNALS_PER_DAY"] = ival
                changes.append(f"MAX_SIGNALS_PER_DAY → {ival}")
            elif key in ("interval", "scan_interval"):
                try:
                    ival = int(val)
                except ValueError:
                    continue
                CONFIG["SCAN_INTERVAL_SECONDS"] = ival
                changes.append(f"SCAN_INTERVAL_SECONDS → {ival} сек")
            elif key in ("atr", "atr_min", "atrmin"):
                try:
                    fval = float(val)
                except ValueError:
                    continue
                CONFIG["MIN_ATR_PCT"] = fval
                changes.append(f"MIN_ATR_PCT → {fval}%")
        if not changes:
            msg = (
                "Не удалось разобрать параметры.\n"
                "Пример: <code>/settings volume=70000000 max_signals=5 interval=900 atr=0.05</code>"
            )
            send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
        else:
            msg = "<b>⚙️ Обновлённые настройки:</b>\n" + "\n".join(
                f"• {c}" for c in changes
            )
            send_telegram_message(msg, chat_id=chat_id, html=True, reply_markup=kb)
    elif first_token == "/risk_off":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        STATE.set_risk_off(True)
        send_telegram_message(
            "Режим <b>Risk OFF</b> включён. Новые сигналы сканироваться не будут.",
            chat_id=chat_id,
            html=True,
            reply_markup=kb,
        )
    elif first_token == "/risk_on":
        if not is_admin:
            send_telegram_message(
                "⛔ Эта команда доступна только администратору.",
                chat_id=chat_id,
                html=False,
                reply_markup=kb,
            )
            return
        STATE.set_risk_off(False)
        send_telegram_message(
            "Режим <b>Risk OFF</b> выключен. Сигналы снова будут сканироваться.",
            chat_id=chat_id,
            html=True,
            reply_markup=kb,
        )
    else:
        msg = (
            "Я пока не понимаю эту команду.\n"
            "Используйте кнопки под полем ввода или /help."
        )
        send_telegram_message(msg, chat_id=chat_id, html=False, reply_markup=kb)


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
            chat_id = str(msg.get("chat", {}).get("id"))
            lower = text.lower()
            if text.startswith("/"):
                handle_command(upd)
            elif lower in (
                "🚀 старт",
                "📊 статус",
                "ℹ️ помощь",
                "📴 стоп",
                "⚙️ настройки",
            ):
                handle_command(upd)
            else:
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

    while True:
        telegram_polling_loop()
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


if __name__ == "__main__":
    try:
        main_loop()
    except SystemExit:
        logging.info("Бот остановлен.")
    except Exception as e:
        logging.error("Критическая ошибка: %s", e)
