import os
import sys
import time
import logging
import threading
import requests
from datetime import datetime, date, UTC
from typing import List, Dict, Optional, Tuple

CONFIG = {
    # Binance API
    "BASE_URL": "https://fapi.binance.com",

    # Параметры фильтрации
    "MIN_QUOTE_VOLUME": 50_000_000,  # минимальный объем в USDT за 24ч
    "CONTRACT_TYPE": "PERPETUAL",
    "QUOTE_ASSET": "USDT",

    # Параметры анализа
    "TIMEFRAME": "5m",
    "CANDLES_LIMIT": 300,
    "EMA_PERIOD": 200,
    "RSI_PERIOD": 14,
    "LOOKBACK_CANDLES": 5,  # сколько последних свечей смотреть на импульс

    # Параметры импульсной свечи
    "BODY_MULTIPLIER": 2.0,   # тело свечи > N * среднее тело
    "VOLUME_MULTIPLIER": 2.0, # объем > N * средний объем

    # Параметры сигнала
    "RISK_REWARD": 1.7,

    # Фильтры RSI
    "RSI_OVERBOUGHT": 70,
    "RSI_OVERSOLD": 30,

    # Минимальный размер стопа в % (слишком мелкие — отбрасываем)
    "MIN_RISK_PCT": 0.35,

    # Рекомендации по плечу (стоп в %) — максимум 20х
    "LEVERAGE_RULES": [
        (0.7, 20),
        (1.5, 15),
        (3.0, 10),
        (5.0, 7),
        (float("inf"), 5),
    ],

    # Лимиты
    "MAX_SIGNALS_PER_DAY": 7,
    "SCAN_INTERVAL_SECONDS": 600,  # 10 минут
    "MAX_SIGNALS_PER_SCAN": 1,     # максимум сигналов за один проход

    # Учет движения биткоина
    "BTC_SYMBOL": "BTCUSDT",
    "BTC_FILTER_ENABLED": True,  # если True, сверяемся с BTC-контекстом

    # Telegram
    "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", ""),
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),
}

# Файл и набор подписчиков
SUBSCRIBERS_FILE = "subscribers.txt"
SUBSCRIBERS = set()

# Глобальное состояние для Telegram polling и остановки
LAST_UPDATE_ID: Optional[int] = None
STOP_EVENT = threading.Event()

logger = logging.getLogger("binance_signals_bot")
logger.setLevel(logging.INFO)

if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)

class BotState:
    """Состояние бота: учёт сигналов и лимитов."""
    def __init__(self):
        self.signals_sent_today = 0
        self.last_reset_date: date = datetime.now().date()
        self.sent_signal_ids = set()

    def reset_daily_if_needed(self):
        today = datetime.now().date()
        if today != self.last_reset_date:
            logger.info("Новый день, сбрасываем счётчики сигналов.")
            self.signals_sent_today = 0
            self.sent_signal_ids.clear()
            self.last_reset_date = today

    def can_send_signal(self) -> bool:
        return self.signals_sent_today < CONFIG["MAX_SIGNALS_PER_DAY"]

    def register_signal(self, signal_id: str):
        self.sent_signal_ids.add(signal_id)
        self.signals_sent_today += 1

def load_subscribers():
    global SUBSCRIBERS
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
                SUBSCRIBERS = {
                    line.strip()
                    for line in lines
                    if line.strip() and all(ch.isdigit() or ch == "-" for ch in line.strip())
                }
            logger.info(f"Загружено подписчиков: {len(SUBSCRIBERS)}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке подписчиков: {e}")
            SUBSCRIBERS = set()
    else:
        logger.info("Файл подписчиков не найден, начинаем с пустого списка.")
        SUBSCRIBERS = set()

def save_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
            for cid in SUBSCRIBERS:
                f.write(str(cid) + "\n")
    except Exception as e:
        logger.error(f"Ошибка при сохранении подписчиков: {e}")

def add_subscriber(chat_id: str):
    SUBSCRIBERS.add(str(chat_id))
    save_subscribers()
    logger.info(f"Добавлен подписчик: {chat_id}")

def remove_subscriber(chat_id: str):
    if str(chat_id) in SUBSCRIBERS:
        SUBSCRIBERS.remove(str(chat_id))
        save_subscribers()
        logger.info(f"Удалён подписчик: {chat_id}")

def binance_request(
    endpoint: str,
    params: Optional[Dict] = None,
    max_retries: int = 5
) -> Optional[Dict]:
    """Запрос к Binance API с повторными попытками."""
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
    """Получение списка USDT-M PERPETUAL символов с фильтром по объёму."""
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

def get_klines(symbol: str, interval: str, limit: int) -> Optional[List[List]]:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    return binance_request("/fapi/v1/klines", params=params)

def get_btc_context() -> Optional[Dict]:
    """Получаем контекст по BTCUSDT (EMA, RSI, текущая цена, 24h изменение)."""
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

    btc_price = closes[-1]
    btc_ema = ema_values[-1]
    btc_rsi = rsi_values[-1]

    # 24h изменение цены
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
    """Ищем импульсную свечу в последних lookback свечах."""
    if len(closes) <= lookback:
        return None

    bodies = [abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    avg_body = sum(bodies[:-lookback]) / max(len(bodies[:-lookback]), 1)
    avg_volume = sum(volumes[:-lookback]) / max(len(volumes[:-lookback]), 1)

    body_mult = CONFIG["BODY_MULTIPLIER"]
    vol_mult = CONFIG["VOLUME_MULTIPLIER"]

    for idx in range(len(closes) - lookback, len(closes)):
        body = abs(closes[idx] - closes[idx - 1])
        vol = volumes[idx]
        if body >= body_mult * avg_body and vol >= vol_mult * avg_volume:
            return idx
    return None

def choose_leverage(risk_pct: float) -> int:
    """Выбор плеча по размеру стопа в %."""
    for threshold, lev in CONFIG["LEVERAGE_RULES"]:
        if risk_pct <= threshold:
            return lev
    return CONFIG["LEVERAGE_RULES"][-1][1]

def analyze_symbol(symbol: str, btc_ctx: Optional[Dict]) -> Optional[Dict]:
    """Анализ одного символа с учетом BTC-контекста."""
    klines = get_klines(symbol, CONFIG["TIMEFRAME"], CONFIG["CANDLES_LIMIT"])
    if not klines:
        return None

    timestamps = [int(k[0]) for k in klines]
    opens = [float(k[1]) for k in klines]
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

    impulse_idx = find_impulse_candle(
        closes, volumes, highs, lows, CONFIG["LOOKBACK_CANDLES"]
    )
    if impulse_idx is None:
        return None

    is_bullish = closes[impulse_idx] > opens[impulse_idx]
    is_bearish = closes[impulse_idx] < opens[impulse_idx]

    # LONG-сценарий: был медвежий импульс, цена выше своей EMA, RSI не перекуплен
    if is_bearish:
        if current_price <= current_ema:
            return None
        if current_rsi >= CONFIG["RSI_OVERBOUGHT"]:
            return None

        # Фильтр по BTC для LONG
        if CONFIG["BTC_FILTER_ENABLED"] and btc_ctx is not None:
            btc_price = btc_ctx["price"]
            btc_ema = btc_ctx["ema200"]
            btc_rsi = btc_ctx["rsi"]
            # хотим, чтобы BTC тоже был в восходящем / нейтральном контексте
            if btc_price < btc_ema:
                return None
            if btc_rsi > 70:
                return None

        stop = lows[impulse_idx]
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

        signal = {
            "symbol": symbol,
            "direction": "long",
            "entry": entry,
            "stop": stop,
            "take": take,
            "ema200": current_ema,
            "rsi": current_rsi,
            "impulse_time": datetime.fromtimestamp(
                timestamps[impulse_idx] / 1000, UTC
            ).isoformat(),
            "risk_pct": risk_pct,
        }
        return signal

    # SHORT-сценарий: был бычий импульс, цена ниже EMA, RSI не перепродан
    if is_bullish:
        if current_price >= current_ema:
            return None
        if current_rsi <= CONFIG["RSI_OVERSOLD"]:
            return None

        # Фильтр по BTC для SHORT
        if CONFIG["BTC_FILTER_ENABLED"] and btc_ctx is not None:
            btc_price = btc_ctx["price"]
            btc_ema = btc_ctx["ema200"]
            btc_rsi = btc_ctx["rsi"]
            if btc_price > btc_ema:
                return None
            if btc_rsi < 30:
                return None

        stop = highs[impulse_idx]
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

        signal = {
            "symbol": symbol,
            "direction": "short",
            "entry": entry,
            "stop": stop,
            "take": take,
            "ema200": current_ema,
            "rsi": current_rsi,
            "impulse_time": datetime.fromtimestamp(
                timestamps[impulse_idx] / 1000, UTC
            ).isoformat(),
            "risk_pct": risk_pct,
        }
        return signal

    return None

def send_telegram_message(
    message: str,
    chat_id: Optional[str] = None,
    html: bool = True
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
    """Рассылка сигнала всем подписчикам (/start)."""
    if not SUBSCRIBERS:
        logger.info("Нет подписчиков, сигнал не рассылаем.")
        return 0

    sent = 0
    for cid in list(SUBSCRIBERS):
        if send_telegram_message(message, chat_id=cid, html=html):
            sent += 1
        time.sleep(0.3)  # небольшая пауза, чтобы не спамить API

    logger.info(f"Сигнал разослан {sent} подписчикам.")
    return sent

def format_signal_message(signal: Dict) -> str:
    direction_emoji = "🟢 long" if signal["direction"] == "long" else "🔴 short"
    lev = choose_leverage(signal["risk_pct"])
    msg = (
        f"🎯 {signal['symbol']} {direction_emoji}\n"
        f"Плечо {lev}х\n"
        f"Вход (ориентир) - {signal['entry']:.5f}\n"
        f"Тейк - {signal['take']:.5f}\n"
        f"Стоп - {signal['stop']:.5f}\n\n"
        f"Таймфрейм: {CONFIG['TIMEFRAME']}\n"
        f"EMA200: {signal['ema200']:.5f}\n"
        f"RSI({CONFIG['RSI_PERIOD']}): {signal['rsi']:.1f}\n"
        f"Импульсная свеча (UTC): {signal['impulse_time']}\n\n"
        f"Логика: импульс, стоп за экстремумом, тейк по RR {CONFIG['RISK_REWARD']}, "
        f"фильтр по тренду, RSI и BTC."
    )
    return msg

def handle_command(message: Dict):
    chat = message.get("chat", {})
    chat_id = str(chat.get("id"))
    text = message.get("text", "") or ""
    cmd = text.strip().split()[0]

    if cmd == "/start":
        add_subscriber(chat_id)
        welcome = (
            "<b>👋 Привет!</b>\n\n"
            "Вы подписались на сигналы Binance Futures (USDT-M).\n"
            "Я сканирую рынок, учитываю движение BTC и буду "
            "присылать сюда торговые сигналы.\n\n"
            "Команды:\n"
            "/status — статус бота\n"
            "/stop — отписаться от сигналов\n"
            "/help — справка\n"
        )
        send_telegram_message(welcome, chat_id=chat_id, html=True)

    elif cmd == "/stop":
        remove_subscriber(chat_id)
        msg = (
            "❌ Вы отписались от сигналов.\n"
            "Введите /start, если захотите снова подписаться."
        )
        send_telegram_message(msg, chat_id=chat_id, html=False)

    elif cmd == "/status":
        status_msg = (
            "<b>📊 Статус бота</b>\n\n"
            f"Интервал сканирования: {CONFIG['SCAN_INTERVAL_SECONDS']} сек.\n"
            f"Максимум сигналов в день: {CONFIG['MAX_SIGNALS_PER_DAY']}.\n"
            f"Максимум сигналов за один скан: {CONFIG['MAX_SIGNALS_PER_SCAN']}.\n"
            f"Подписчиков: {len(SUBSCRIBERS)}.\n"
            f"Фильтр BTC: {'включен' if CONFIG['BTC_FILTER_ENABLED'] else 'выключен'}.\n"
        )
        send_telegram_message(status_msg, chat_id=chat_id, html=True)

    elif cmd == "/help":
        help_msg = (
            "<b>ℹ️ Справка</b>\n\n"
            "Бот автоматически:\n"
            "• Сканирует USDT-M фьючерсы Binance\n"
            "• Ищет импульсные свечи\n"
            "• Фильтрует по EMA200 и RSI\n"
            "• Учитывает контекст BTCUSDT\n"
            "• Считает вход, стоп, тейк и плечо\n"
            "• Отправляет сигналы всем подписчикам (/start)\n"
        )
        send_telegram_message(help_msg, chat_id=chat_id, html=True)

def telegram_polling():
    """Polling для обработки команд Telegram."""
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

def scan_market(state: BotState):
    """Сканирование рынка и отправка сигналов подписчикам."""
    state.reset_daily_if_needed()

    if not state.can_send_signal():
        logger.info("Дневной лимит сигналов достигнут, сканирование пропущено.")
        return

    # Сначала получаем BTC-контекст
    btc_ctx = get_btc_context()

    symbols = get_trading_symbols()
    if not symbols:
        return

    logger.info(f"Анализ {len(symbols)} символов...")

    signals_found: List[Dict] = []

    for symbol in symbols:
        if STOP_EVENT.is_set():
            return
        # Можно пропустить сам BTC, если хотим сигналы только по альтам:
        # if symbol == CONFIG["BTC_SYMBOL"]:
        #     continue
        try:
            signal = analyze_symbol(symbol, btc_ctx)
            if signal is None:
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

    # Сортируем по размеру стопа (чем меньше риск, тем выше приоритет)
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
            state.register_signal(signal_id)
            signals_sent_this_scan += 1
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
    logger.info("=" * 60)
    logger.info("Запуск Binance Futures Signal Bot")
    logger.info("=" * 60)
    logger.info("Конфигурация:")
    logger.info(f"  - Минимальный объём: {CONFIG['MIN_QUOTE_VOLUME']:,} USDT")
    logger.info(f"  - Таймфрейм: {CONFIG['TIMEFRAME']}")
    logger.info(f"  - Интервал сканирования: {CONFIG['SCAN_INTERVAL_SECONDS']} сек")
    logger.info(f"  - Лимит сигналов в день: {CONFIG['MAX_SIGNALS_PER_DAY']}")
    logger.info(f"  - Risk/Reward: {CONFIG['RISK_REWARD']}")
    logger.info(f"  - Мин. стоп: {CONFIG['MIN_RISK_PCT']}%")
    logger.info(f"  - Макс. сигналов за скан: {CONFIG['MAX_SIGNALS_PER_SCAN']}")
    logger.info(f"  - BTC фильтр: {'ON' if CONFIG['BTC_FILTER_ENABLED'] else 'OFF'}")

    tg_token = CONFIG["TG_BOT_TOKEN"]
    tg_chat = CONFIG["TG_CHAT_ID"]
    if not tg_token:
        logger.error("TG_BOT_TOKEN не настроен. Выход.")
        return

    # Загружаем подписчиков из файла
    load_subscribers()

    # Приветствие только тебе (основному chat_id), если указан
    if tg_chat:
        welcome_msg = (
            "<b>🚀 Бот запущен!</b>\n\n"
            "Я сканирую Binance Futures (USDT-M), учитываю движение BTC и "
            "буду отправлять сигналы всем пользователям, нажавшим /start.\n\n"
            f"Первый скан будет через {CONFIG['SCAN_INTERVAL_SECONDS']} секунд."
        )
        send_telegram_message(welcome_msg, chat_id=tg_chat, html=True)

    # Запускаем polling в отдельном потоке
    polling_thread = threading.Thread(target=telegram_polling, daemon=True)
    polling_thread.start()

    state = BotState()

    logger.info("Ожидание перед первым сканированием...")
    time.sleep(CONFIG["SCAN_INTERVAL_SECONDS"])

    try:
        while not STOP_EVENT.is_set():
            try:
                scan_market(state)
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
