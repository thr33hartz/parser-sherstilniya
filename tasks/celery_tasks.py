import os
import shutil
import tempfile
import asyncio
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta
from asgiref.sync import async_to_sync
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import psutil
import random
import numpy as np

from celery_app import celery
import config
from services import supabase_service
from workers.get_trader_pnl import perform_pnl_fetch
from workers.get_top_traders import perform_toplevel_traders_fetch
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKENS_CHUNK_SIZE = 1000
TRADERS_CHUNK_SIZE = 40000

redis_url = os.getenv('REDIS_URL')
redis = Redis.from_url(redis_url)

def init_worker_driver():
    logger.info("CELERY_TASK: Инициализация Selenium-драйвера...")
    temp_dir = tempfile.mkdtemp(prefix='chrome_profile_')
    logger.info(f"Используется уникальный user-data-dir: {temp_dir}")
    
    opts = Options()
    opts.add_argument(f"--user-data-dir={temp_dir}")
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
    
    prefs = {
        "download.default_directory": config.DOWNLOAD_DIR,
        "download.prompt_for_download": False,
    }
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=opts)
    driver.temp_dir = temp_dir
    logger.info("CELERY_TASK: Драйвер успешно создан.")
    return driver

async def _run_all_in_parse_periodic_task_async(template: dict):
    lock_key = "all_in_parse_lock"
    try:
        if not redis.set(lock_key, "locked", nx=True, ex=3600):
            logger.info("Periodic task already running, skipping.")
            return
        
        temp_files_to_clean = []
        batch_id = str(uuid.uuid4())
        batch_created_at = datetime.now(timezone.utc)

        logger.info(f"Batch {batch_id}: Этап 1 - Поиск токенов")
        hours = int(template.get('time_period', '24h').replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        categories = [cat for cat in template.get('categories', []) if cat in ['completed', 'completing']]
        tokens = await supabase_service.fetch_tokens_by_criteria(start_time, template.get('platforms', []), categories)

        if not tokens:
            logger.warning(f"Batch {batch_id}: Токены не найдены")
            return

        token_addresses = [t['contract_address'] for t in tokens]
        logger.info(f"Batch {batch_id}: Найдено {len(tokens)} токенов")

        logger.info(f"Batch {batch_id}: Этап 2 - Получение топ-трейдеров")
        token_chunks = [token_addresses[i:i + TOKENS_CHUNK_SIZE] for i in range(0, len(token_addresses), TOKENS_CHUNK_SIZE)]
        all_traders_files = []

        for chunk in token_chunks:
            driver = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt", encoding='utf-8') as tmp_f:
                    tmp_f.write("\n".join(chunk))
                    temp_filepath = tmp_f.name
                    temp_files_to_clean.append(temp_filepath)

                driver = init_worker_driver()
                for attempt in range(3):
                    try:
                        driver.get("https://discord.com/channels/@me/1331338750789419090")
                        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "main[class*='chatContent']")))
                        break
                    except (TimeoutException, WebDriverException) as e:
                        logger.warning(f"Retry {attempt+1}/3 for top traders: {e}")
                        time.sleep(5)
                else:
                    raise Exception("Failed to load Discord DM for top traders")

                result_path = perform_toplevel_traders_fetch(driver, temp_filepath)
                if result_path:
                    all_traders_files.append(result_path)
            finally:
                if driver:
                    driver.quit()
                    if hasattr(driver, 'temp_dir') and os.path.exists(driver.temp_dir):
                        shutil.rmtree(driver.temp_dir, ignore_errors=True)
                        logger.info(f"Очищен temp: {driver.temp_dir}")

        if not all_traders_files:
            logger.error(f"Batch {batch_id}: Не удалось получить топ-трейдеров")
            raise Exception("Top Traders fetch failed")

        final_trader_list = []
        for path in all_traders_files:
            with open(path, 'r', encoding='utf-8') as f:
                final_trader_list.extend([line.strip() for line in f if line.strip() and not line.startswith('---')])
            temp_files_to_clean.append(path)

        unique_traders = list(set(final_trader_list))
        logger.info(f"Batch {batch_id}: Найдено {len(unique_traders)} уникальных трейдеров")

        logger.info(f"Batch {batch_id}: Этап 3 - Получение PNL")
        trader_chunks = [unique_traders[i:i + TRADERS_CHUNK_SIZE] for i in range(0, len(unique_traders), TRADERS_CHUNK_SIZE)]
        all_pnl_reports_paths = []

        for chunk in trader_chunks:
            driver = None
            try:
                driver = init_worker_driver()
                for attempt in range(3):
                    try:
                        driver.get("https://discord.com/channels/@me/1331338750789419090")
                        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "main[class*='chatContent']")))
                        break
                    except (TimeoutException, WebDriverException) as e:
                        logger.warning(f"Retry {attempt+1}/3 for PNL: {e}")
                        time.sleep(5)
                else:
                    raise Exception("Failed to load Discord DM for PNL")

                pnl_csv_path = perform_pnl_fetch(driver, chunk)
                if pnl_csv_path:
                    all_pnl_reports_paths.append(pnl_csv_path)
                    temp_files_to_clean.append(pnl_csv_path)
            finally:
                if driver:
                    driver.quit()
                    if hasattr(driver, 'temp_dir') and os.path.exists(driver.temp_dir):
                        shutil.rmtree(driver.temp_dir, ignore_errors=True)
                        logger.info(f"Очищен temp: {driver.temp_dir}")

        if not all_pnl_reports_paths:
            logger.error(f"Batch {batch_id}: Не удалось получить PNL")
            raise Exception("PNL fetch failed")

        logger.info(f"Batch {batch_id}: Этап 4 - Сохранение в Supabase")
        df_list = [pd.read_csv(path) for path in all_pnl_reports_paths]
        merged_df = pd.concat(df_list, ignore_index=True).drop_duplicates(subset=['wallet'])

        # ЧИСТКА ДАННЫХ: Конвертируем numeric, invalid -> NaN -> None
        numeric_cols = [
            "balance", "wsol_balance", "roi_7d", "roi_30d", "winrate_7d", "winrate_30d",
            "avg_holding_time", "usd_profit_7d", "usd_profit_30d", "avg_token_age_7d", "avg_token_age_30d",
            "top_three_pnl", "avg_quick_buy_and_sell_percentage", "avg_bundled_token_buys_percentage",
            "avg_sold_more_than_bought_percentage", "avg_first_buy_mcap_7d", "total_buys_7d", "pf_buys_7d",
            "pf_swap_buys_7d", "bonk_buys_7d", "raydium_buys_7d", "boop_buys_7d", "meteora_buys_7d",
            "avg_first_buy_mcap_30d", "total_buys_30d", "pf_buys_30d", "pf_swap_buys_30d", "bonk_buys_30d",
            "raydium_buys_30d", "boop_buys_30d", "meteora_buys_30d", "avg_buys_per_token_7d",
            "avg_buys_per_token_30d", "total_sells_7d", "total_sells_30d", "avg_forwarder_tip",
            "avg_token_cost_7d", "avg_token_cost_30d", "unrealised_pnl_7d", "unrealised_pnl_30d",
            "total_cost_7d", "total_cost_30d", "traded_tokens"  # traded_tokens integer
        ]
        for col in numeric_cols:
            if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

        batch_data = []
        for _, row in merged_df.iterrows():
            batch_entry = {
                "batch_id": batch_id,
                "batch_created_at": batch_created_at.isoformat(),
                "wallet": row["wallet"],
                "last_trade_time": row.get("last_trade_time"),
            }
            for col in numeric_cols:
                val = row.get(col)
                batch_entry[col] = None if pd.isna(val) else val
            batch_data.append(batch_entry)

        response = supabase_service.client.table("pnl_batches").insert(batch_data).execute()
        logger.info(f"Batch {batch_id}: Сохранено {len(batch_data)} записей в Supabase")

    except Exception as e:
        logger.error(f"Batch {batch_id}: Ошибка - {e}")
        raise
    finally:
        for f_path in temp_files_to_clean:
            if os.path.exists(f_path):
                os.remove(f_path)
        redis.delete(lock_key)
        logger.info(f"Batch {batch_id}: Временные файлы очищены")

@celery.task
def run_all_in_parse_periodic_task(template: dict):
    async_to_sync(_run_all_in_parse_periodic_task_async)(template)
    
async def _all_in_parse_pipeline_async(chat_id: int, template: dict, message_id: int):
    """
    Асинхронная версия пайплайна.
    ИСПРАВЛЕНО: Принимает message_id и редактирует существующее сообщение.
    """
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    temp_files_to_clean = []
    
    try:
        # --- Уведомление о старте ---
        # Это сообщение теперь редактирует то, что уже есть в чате
        start_text = (
            f"✅ Ваша очередь подошла! Начинаю 'All-In Parse' по шаблону '{template.get('template_name', '...')}'.\n\n"
            "Это может занять много времени. Я буду присылать файлы по мере готовности."
        )
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=start_text, disable_web_page_preview=True)

        # --- ЭТАП 1: GET TOKENS ---
        # Уведомления об этапах теперь тоже отправляются как новые сообщения
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"🚀 Этап 1/3: Поиск токенов...", disable_web_page_preview=True)
        hours = int(template.get('time_period', '24h').replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        categories = [cat for cat in template.get('categories', []) if cat in ['completed', 'completing']]
        tokens = await supabase_service.fetch_tokens_by_criteria(start_time, template.get('platforms', []), categories)

        if not tokens:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ Не найдено токенов по вашему шаблону. Задача остановлена.", disable_web_page_preview=True)
            return

        token_addresses = [t['contract_address'] for t in tokens]
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"✅ Этап 1 завершен. Найдено {len(tokens)} токенов.", disable_web_page_preview=True)

        # --- ЭТАП 2: GET TOP TRADERS ---
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"👥 Этап 2/3: Получение трейдеров для {len(token_addresses)} токенов. Это может занять время...", disable_web_page_preview=True)
        
        token_chunks = [token_addresses[i:i + TOKENS_CHUNK_SIZE] for i in range(0, len(token_addresses), TOKENS_CHUNK_SIZE)]
        all_traders_files = []
        
        for i, chunk in enumerate(token_chunks, 1):
            if len(token_chunks) > 1:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"👥 Этап 2/3: Обрабатываю пакет токенов {i} из {len(token_chunks)}...", disable_web_page_preview=True)
            
            driver_traders = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt", encoding='utf-8') as tmp_f:
                    tmp_f.write("\n".join(chunk))
                    temp_filepath = tmp_f.name
                    temp_files_to_clean.append(temp_filepath)
                
                driver_traders = init_worker_driver()
                result_path = perform_toplevel_traders_fetch(driver_traders, temp_filepath)
                if result_path:
                    all_traders_files.append(result_path)
            finally:
                if driver_traders: driver_traders.quit()

        if not all_traders_files:
             await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ Не удалось получить список трейдеров. Задача остановлена.", disable_web_page_preview=True)
             raise Exception("Top Traders fetch failed")

        final_trader_list = []
        for path in all_traders_files:
            with open(path, 'r', encoding='utf-8') as f:
                final_trader_list.extend([line.strip() for line in f if line.strip() and not line.startswith('---')])
            temp_files_to_clean.append(path)
        
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="✅ Этап 2 завершен. Список трейдеров собран.", disable_web_page_preview=True)

        # --- ЭТАП 3: GET PNL ---
        unique_traders = list(set(final_trader_list))
        if not unique_traders:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="⚠️ Трейдеры для анализа PNL не найдены. Завершаю задачу.", disable_web_page_preview=True)
            return

        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"📊 Этап 3/3: Получение PNL для {len(unique_traders)} трейдеров.", disable_web_page_preview=True)
        
        trader_chunks = [unique_traders[i:i + TRADERS_CHUNK_SIZE] for i in range(0, len(unique_traders), TRADERS_CHUNK_SIZE)]
        all_pnl_reports_paths = []
        
        for i, chunk in enumerate(trader_chunks, 1):
            if len(trader_chunks) > 1:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"🪓 Обрабатываю PNL-пакет {i} из {len(trader_chunks)}...", disable_web_page_preview=True)
            
            driver_pnl = None
            try:
                driver_pnl = init_worker_driver()
                pnl_csv_path = perform_pnl_fetch(driver_pnl, chunk)
                if pnl_csv_path:
                    all_pnl_reports_paths.append(pnl_csv_path)
                    temp_files_to_clean.append(pnl_csv_path)
            finally:
                if driver_pnl: driver_pnl.quit()

        if not all_pnl_reports_paths:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ Не удалось получить ни одного PNL отчета. Задача прервана.", disable_web_page_preview=True)
            raise Exception("PNL fetch failed")

        # --- ЭТАП 4: ОБЪЕДИНЕНИЕ И ОТПРАВКА PNL ---
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"🖇️ Этап 4/4: Объединение и фильтрация PNL-отчетов...", disable_web_page_preview=True)
        
        df_list = [pd.read_csv(path) for path in all_pnl_reports_paths]
        merged_df = pd.concat(df_list, ignore_index=True).drop_duplicates(subset=['wallet'])
        
        # --- НОВЫЙ ШАГ: ПРИМЕНЯЕМ ПРОДВИНУТЫЕ ФИЛЬТРЫ ---
        pnl_filters = template.get('pnl_filters', {})
        if pnl_filters:
            logger.info(f"Applying PNL filters: {pnl_filters}")
            filtered_df = apply_pnl_filters(merged_df, pnl_filters)
        else:
            filtered_df = merged_df # Если фильтров нет, используем объединенный DF
        # ----------------------------------------------------

        final_filename = f"all_in_parse_final_pnl_{uuid.uuid4()}.csv"
        final_csv_path = os.path.join(config.FILES_DIR, final_filename)
        filtered_df.to_csv(final_csv_path, index=False) # Сохраняем отфильтрованный DF
        temp_files_to_clean.append(final_csv_path)

        caption = (
            f"✅ All-In Parse завершен!\n\n"
            f"Анализ на основе:\n"
            f"  - Токенов найдено: {len(tokens)}\n"
            f"  - Уникальных трейдеров: {len(unique_traders)}\n\n"
            f"В этом файле финальный PNL-отчет для {len(filtered_df)} трейдеров (после фильтрации)."
        )
        
        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data="main_menu")]])
        with open(final_csv_path, "rb") as f:
            await bot.send_document(
                chat_id=chat_id, document=f,
                caption=caption,
                reply_markup=back_button_markup,
            )
        
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="Все готово!", disable_web_page_preview=True)
        
    except Exception as e:
        print(f"CELERY_ERROR: 'All-In Parse' провалился: {e}")
        error_text = f"❌ Произошла критическая ошибка во время 'All-In Parse'."
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text, disable_web_page_preview=True)
        except:
            await bot.send_message(chat_id=chat_id, text=error_text, disable_web_page_preview=True)
    finally:
        for f_path in temp_files_to_clean:
            if os.path.exists(f_path):
                os.remove(f_path)
        print("CELERY_TASK: 'All-In Parse' завершен, временные файлы очищены.")