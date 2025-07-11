import os
import json
import asyncio
from urllib.parse import urlparse, parse_qs
import uuid
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import requests
from cloudscraper.exceptions import CloudflareChallengeError
import cloudscraper
from supabase_client import supabase

# --- Load config ---
load_dotenv()

# ── fingerprint & timezone, one‑time per launch ──
DEVICE_ID = os.getenv("DEVICE_ID") or str(uuid.uuid4())
FP_DID    = os.getenv("FP_DID")    or str(uuid.uuid4())
TZ_OFFSET = (os.getenv("TZ_OFFSET", "-18000").strip("'\"")  # strip quotes if any
             or "-18000")

TOKEN_API_URL_FILE = "api_url.txt"

# --- Logging ---
import logging
logger = logging.getLogger(__name__)

API_PARAMS = {
    "device_id":  DEVICE_ID,
    "client_id":  os.getenv("CLIENT_ID"),
    "from_app":   os.getenv("FROM_APP"),
    "app_ver":    os.getenv("APP_VER"),
    "tz_name":    os.getenv("TZ_NAME"),
    "tz_offset":  TZ_OFFSET,
    "app_lang":   os.getenv("APP_LANG"),
    "fp_did":     FP_DID,
    "os":         os.getenv("OS", "web"),
}

# Убрали User-Agent, чтобы cloudscraper генерировал его автоматически
HEADERS = {
    "Accept":       os.getenv("ACCEPT", "application/json, text/plain, */*"),
    "Content-Type": os.getenv("CONTENT_TYPE", "application/json"),
    "Referer":      os.getenv("REFERER", "https://gmgn.ai/"),
}

# --- Utils ---
def sanitize_string(value: str) -> str:
    if isinstance(value, str):
        return value.replace(' ', '').replace('\n', '')
    return value
def format_launchpad(lp: str) -> str:
    if not lp:
        return "unknown"
    s = sanitize_string(lp)
    return s.lower().replace('.', '')

# --- DB upsert ---
async def upsert_tokens_batch_in_db(tokens):
    if not tokens:
        return
    await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: supabase.table("tokens")
            .upsert(tokens, on_conflict="contract_address")
            .execute()
    )
    print(f"Upserted {len(tokens)} tokens.")

# --- Sync fetch with cloudscraper ---
def fetch_sync(url, payload, headers):
    """Синхронная функция для выполнения POST-запроса через cloudscraper."""
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
    )
    logger.debug(f"Requesting {url} with payload {payload} and headers {headers}")
    response = scraper.post(url, json=payload, headers=headers)
    response.raise_for_status()
    raw = response.text
    if raw.startswith(")]}',"):
        raw = raw.split('\n', 1)[1]
    logger.debug(f"Received response: {response.status_code} {raw[:100]}...")
    return json.loads(raw or "{}")

# --- Main fetch ---
async def fetch_tokens(categories=["new_creation", "completed", "completing"], time_window_hours=None):
    print(f"Запуск fetch_tokens... Категории={categories}, окно={time_window_hours}ч")

    # Прочитать URL из файла
    try:
        raw_url = open(TOKEN_API_URL_FILE).read().strip()
    except FileNotFoundError:
        print(f"{TOKEN_API_URL_FILE} не найден")
        return []

    parsed = urlparse(raw_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    params0 = {k: v[0] for k, v in parse_qs(parsed.query).items()}

    # Объединить с env-параметрами
    payload = {**params0, **API_PARAMS}

    # Фильтр по времени
    threshold = None
    if time_window_hours is not None:
        try:
            dt = datetime.now(timezone.utc) - timedelta(hours=float(time_window_hours))
            threshold = dt.timestamp()
        except:
            print("Неверное time_window_hours — пропускаем фильтр")

    tokens_map = {}
    loop = asyncio.get_event_loop()

    # Выполнение запроса с повторами
    for attempt in range(3):
        try:
            data = await loop.run_in_executor(None, fetch_sync, base_url, payload, HEADERS)
            break
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.warning("403 Forbidden → retry %d/3", attempt + 1)
                await asyncio.sleep(5)
                # Обновляем fingerprint
                payload["device_id"] = str(uuid.uuid4())
                payload["fp_did"] = str(uuid.uuid4())
                continue
            else:
                logger.error(f"HTTP error: {e}")
                raise
        except CloudflareChallengeError as e:
            logger.error(f"Cloudflare challenge failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    # Обработка данных
    for cat in categories:
        key = "pump" if cat == "completing" else cat
        for itm in data.get("data", {}).get(key, []):
            addr = sanitize_string(itm.get("address", "")).strip()
            if not addr:
                continue
            ts = max(itm.get("open_timestamp", 0), itm.get("created_timestamp", 0))
            if threshold and ts < threshold:
                continue
            iso = datetime.fromtimestamp(ts, timezone.utc).isoformat()
            tokens_map[addr] = {
                "ticker": sanitize_string(itm.get("symbol", "N/A")),
                "name": sanitize_string(itm.get("name", "N/A")),
                "contract_address": addr,
                "creator": sanitize_string(itm.get("creator")),
                "migration_time": iso,
                "category": cat,
                "launchpad": format_launchpad(itm.get("launchpad_platform")),
                "api_open_timestamp": itm.get("open_timestamp"),
                "api_created_timestamp": itm.get("created_timestamp")
            }

    tokens_list = list(tokens_map.values())
    if tokens_list:
        await upsert_tokens_batch_in_db(tokens_list)
    else:
        print("Новых токенов не найдено")

    print(f"fetch_tokens завершён, записано {len(tokens_list)}")
    return tokens_list

# Для отладки
if __name__ == "__main__":
    asyncio.run(fetch_tokens())