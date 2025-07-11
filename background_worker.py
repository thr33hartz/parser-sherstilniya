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
DEV_STATS_BATCH_SIZE = 15
TOKEN_FETCH_LOOP_SLEEP_SECONDS = 20
# ИСПРАВЛЕНО: Добавляем временное окно для поиска
DEV_DISCOVERY_TOKEN_HOURS = 48  # Искать девов по токенам за последние 48 часов
DEV_DISCOVERY_LOOP_SLEEP_SECONDS = 300
DEV_STATS_LOOP_SLEEP_SECONDS = 120

# --- Функции-помощники ---

def is_valid_solana_address(address: str) -> bool:
    """Простая проверка, похож ли адрес на адрес Solana."""
    if not isinstance(address, str):
        return False
    return 32 <= len(address) <= 44

async def upsert_data_to_supabase(stats_data: list, tokens_data: list):
    """Сохраняет пачки данных в Supabase."""
    loop = asyncio.get_event_loop()
    try:
        if stats_data:
            await loop.run_in_executor(None, lambda: supabase.table("developer_stats").upsert(stats_data, on_conflict="developer_address").execute())
            logger.info(f"Upserted {len(stats_data)} developer stats records.")
        if tokens_data:
            await loop.run_in_executor(None, lambda: supabase.table("dev_deployed_tokens").upsert(tokens_data, on_conflict="token_address").execute())
            logger.info(f"Upserted {len(tokens_data)} deployed token records.")
    except Exception as e:
        logger.error(f"SUPABASE_UPSERT_ERROR: {e}", exc_info=True)


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

            for address in dev_addresses_to_process:
                # ИСПРАВЛЕНО: Вызываем функцию только с одним аргументом `address`
                stats, tokens = await fetch_dev_pnl.fetch_dev_data_from_api(address)
                
                if stats and len(stats) > 1:
                    stats['last_updated_at'] = datetime.now(timezone.utc).isoformat()
                    await upsert_data_to_supabase([stats], tokens or [])
                
                logger.info(f"DEV_STATS_LOOP: Processed {address}. Waiting 10 seconds...")
                await asyncio.sleep(3)

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