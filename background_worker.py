# background_worker.py

import os
import sys
import certifi
import httpx
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)
os.environ['SSL_CERT_FILE'] = certifi.where()

import asyncio
import random  # for jitter back‑off
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta

from supabase_client import supabase
from fetch_tokens import fetch_tokens
import fetch_dev_pnl

# --- Настройка логирования ---
LOGFILE = "background_worker.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        RotatingFileHandler(LOGFILE, maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Конфигурация Воркера ---
DEV_STATS_BATCH_SIZE = 50          # обрабатываем больше адресов за цикл
TOKEN_FETCH_LOOP_SLEEP_SECONDS = 20
DEV_DISCOVERY_TOKEN_HOURS = 48     # оставляем как есть
DEV_DISCOVERY_LOOP_SLEEP_SECONDS = 300
DEV_STATS_LOOP_SLEEP_SECONDS = 10  # короткая пауза; high‑throughput

# --- Функции-помощники ---

def is_valid_solana_address(address: str) -> bool:
    """Простая проверка, похож ли адрес на адрес Solana."""
    if not isinstance(address, str):
        return False
    return 32 <= len(address) <= 44


async def safe_upsert(table: str, rows: list, *, on_conflict: str,
                      chunk: int = 250, max_retries: int = 4,
                      base_delay: float = 1.0):
    """
    Upsert rows to Supabase in manageable chunks with exponential
    back‑off & jitter to avoid http2 stream resets / 429 throttling.
    """
    loop = asyncio.get_event_loop()
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        for attempt in range(1, max_retries + 1):
            try:
                await loop.run_in_executor(
                    None,
                    lambda p=part: supabase.table(table)
                                    .upsert(p, on_conflict=on_conflict)
                                    .execute()
                )
                break  # success
            except Exception as exc:
                if attempt == max_retries:
                    logger.error("UPSERT %s FAILED after %s tries: %s",
                                 table, attempt, exc)
                    break
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
                logger.warning("UPSERT %s retry %s/%s in %.1fs",
                               table, attempt, max_retries, delay)
                await asyncio.sleep(delay)


async def upsert_data_to_supabase(stats_data: list, tokens_data: list):
    """Wrapper that delegates to safe_upsert with sensible chunks."""
    try:
        if stats_data:
            await safe_upsert("developer_stats", stats_data,
                              on_conflict="developer_address", chunk=100)
        if tokens_data:
            await safe_upsert("dev_deployed_tokens", tokens_data,
                              on_conflict="token_address", chunk=250)
    except Exception as e:
        logger.error("SUPABASE_UPSERT_ERROR (final): %s", e, exc_info=True)


# --- Основные циклы воркера ---

async def token_fetch_loop():
    """Цикл сбора новых токенов."""
    while True:
        logger.info("TOKEN_FETCH_LOOP: [START] Looking for new tokens...")
        try:
            # Вызываем вашу функцию из fetch_tokens.py
            await fetch_tokens(categories=["new_creation", "completed", "completing"])
        except Exception as e:
            logger.error(f"TOKEN_FETCH_LOOP: A critical error occurred: {e}", exc_info=True)
        
        logger.info(f"TOKEN_FETCH_LOOP: [END] Sleeping for {TOKEN_FETCH_LOOP_SLEEP_SECONDS} seconds.")
        await asyncio.sleep(TOKEN_FETCH_LOOP_SLEEP_SECONDS)

async def dev_stats_update_loop():
    """
    Основной цикл: находит девов для обновления, получает по ним свежие данные через API и сохраняет в БД.
    """
    # ИСПРАВЛЕНО: Убираем `async with httpx.AsyncClient...` так как он больше не нужен.
    while True:
        logger.info("DEV_STATS_LOOP: [START] Looking for developers to update...")
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, 
                lambda: supabase.table("developer_stats")
                            .select("developer_address")
                            .order("last_updated_at", desc=False, nullsfirst=True)
                            .limit(DEV_STATS_BATCH_SIZE)
                            .execute()
            )
            
            if not response.data:
                logger.info("DEV_STATS_LOOP: No developers to update. Waiting...")
                await asyncio.sleep(DEV_STATS_LOOP_SLEEP_SECONDS)
                continue

            dev_addresses_to_process = [
                item['developer_address'] for item in response.data 
                if is_valid_solana_address(item.get('developer_address'))
            ]
            
            if not dev_addresses_to_process:
                logger.info("DEV_STATS_LOOP: No VALID developers to update in this batch.")
                await asyncio.sleep(DEV_STATS_LOOP_SLEEP_SECONDS)
                continue

            logger.info(f"DEV_STATS_LOOP: Found {len(dev_addresses_to_process)} valid developers to process.")

            # ── Параллельная обработка девов ────────────────────────────────
            semaphore = asyncio.Semaphore(20)  # максимум 20 одновременных запросов

            async def process_one(addr: str):
                async with semaphore:
                    try:
                        stats, tokens = await fetch_dev_pnl.fetch_dev_data_from_api(addr)
                        if stats and len(stats) > 1:
                            stats["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                            await upsert_data_to_supabase([stats], tokens or [])
                        logger.info("DEV_STATS_LOOP: Done %s", addr)
                    except Exception as exc:
                        logger.error("DEV_STATS_LOOP: Error processing %s: %s", addr, exc, exc_info=True)

            await asyncio.gather(*(process_one(a) for a in dev_addresses_to_process))

        except Exception as e:
            logger.error(f"DEV_STATS_LOOP: A critical error occurred: {e}", exc_info=True)
        
        logger.info(f"DEV_STATS_LOOP: [END] Sleeping for {DEV_STATS_LOOP_SLEEP_SECONDS} seconds.")
        await asyncio.sleep(DEV_STATS_LOOP_SLEEP_SECONDS)


async def developer_discovery_loop():
    """
    Ищет в таблице токенов НОВЫХ разработчиков и добавляет их в developer_stats.
    ИСПРАВЛЕНО: Теперь ищет только по СВЕЖИМ токенам.
    """
    while True:
        logger.info("DEV_DISCOVERY_LOOP: [START] Looking for new developers from recent tokens...")
        try:
            loop = asyncio.get_event_loop()
            
            # ИСПРАВЛЕНИЕ: Добавляем временной фильтр
            time_window_start = datetime.now(timezone.utc) - timedelta(hours=DEV_DISCOVERY_TOKEN_HOURS)
            
            creators_res = await loop.run_in_executor(None,
                lambda: supabase.table("tokens")
                            .select("creator")
                            .in_("category", ["completed", "completing", "migrated"])
                            .gte("migration_time", time_window_start.isoformat()) # <-- ФИЛЬТР ПО ВРЕМЕНИ
                            .execute()
            )
            
            if not creators_res.data:
                logger.info("DEV_DISCOVERY_LOOP: No creators found for recent tokens.")
                await asyncio.sleep(DEV_DISCOVERY_LOOP_SLEEP_SECONDS)
                continue
            
            creators = list({item['creator'] for item in creators_res.data if is_valid_solana_address(item.get('creator'))})
            if not creators:
                logger.info("DEV_DISCOVERY_LOOP: No valid unique creators found.")
                await asyncio.sleep(DEV_DISCOVERY_LOOP_SLEEP_SECONDS)
                continue
            
            # ... (остальная логика поиска new_devs и их добавления пачками без изменений) ...
            known_devs = set()
            for i in range(0, len(creators), 500):
                batch = creators[i:i + 500]
                known_res = await loop.run_in_executor(None, lambda b=batch: supabase.table("developer_stats").select("developer_address").in_("developer_address", b).execute())
                if known_res.data: known_devs.update(item['developer_address'] for item in known_res.data)
            new_devs = list(set(creators) - known_devs)
            if new_devs:
                logger.info(f"DEV_DISCOVERY_LOOP: Found {len(new_devs)} new developers.")
                data_to_insert = [{"developer_address": addr} for addr in new_devs]
                await loop.run_in_executor(None, lambda: supabase.table("developer_stats").upsert(data_to_insert, on_conflict="developer_address").execute())
            else:
                logger.info("DEV_DISCOVERY_LOOP: No new developers found this cycle.")

        except Exception as e:
            logger.error(f"DEV_DISCOVERY_LOOP: A critical error occurred: {e}", exc_info=True)
        
        logger.info(f"DEV_DISCOVERY_LOOP: [END] Sleeping for {DEV_DISCOVERY_LOOP_SLEEP_SECONDS} seconds.")
        await asyncio.sleep(DEV_DISCOVERY_LOOP_SLEEP_SECONDS)


async def main():
    logger.info("BACKGROUND WORKER: Starting all automatic loops...")
    # УДАЛИЛИ trader_fetch_loop
    await asyncio.gather(
        token_fetch_loop(),
        developer_discovery_loop(),
        dev_stats_update_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())