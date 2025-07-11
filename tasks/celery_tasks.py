import os
import shutil
import tempfile
import asyncio
import io
import csv
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta
from telegram import Bot, InputMediaDocument, InlineKeyboardMarkup, InlineKeyboardButton
from asgiref.sync import async_to_sync

from celery_app import celery
import config
from services import supabase_service, queue_service
from workers.get_trader_pnl import perform_pnl_fetch
from workers.get_program_swaps import perform_program_swaps
from workers.get_top_traders import perform_toplevel_traders_fetch
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging

logging.basicConfig(level=logging.INFO) # Basic configuration
logger = logging.getLogger(__name__)

# --- Лимиты для разделения на части ---
TOKENS_CHUNK_SIZE = 1000
TRADERS_CHUNK_SIZE = 40000

def init_worker_driver():
    """Создает и возвращает новый экземпляр Selenium-драйвера."""
    print("CELERY_TASK: Инициализация Selenium-драйвера...")
    opts = Options()
    opts.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # Настройки для автоматического скачивания
    prefs = {
        "download.default_directory": config.DOWNLOAD_DIR,
        "download.prompt_for_download": False,
    }
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=opts)
    print("CELERY_TASK: Драйвер успешно создан.")
    return driver

# --- Фоновые задачи Celery ---

@celery.task
async def _pnl_fetch_async(wallets: list, chat_id: int):
    """
    Асинхронная логика для получения PNL.
    ИСПРАВЛЕНО: Убраны промежуточные сообщения о разбивке на пакеты.
    """
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    temp_files_to_clean = []
    
    try:
        unique_wallets = list(set(wallets))
        wallet_chunks = [unique_wallets[i:i + TRADERS_CHUNK_SIZE] for i in range(0, len(unique_wallets), TRADERS_CHUNK_SIZE)]
        all_pnl_files = []

        # # --- БЛОК УВЕДОМЛЕНИЙ УДАЛЕН ---

        for i, chunk in enumerate(wallet_chunks, 1):
            driver = None
            try:
                driver = init_worker_driver()
                result_path = perform_pnl_fetch(driver, chunk)
                if result_path:
                    all_pnl_files.append(result_path)
                    temp_files_to_clean.append(result_path)
            finally:
                if driver: driver.quit()

        if not all_pnl_files:
            raise Exception("PNL fetch failed for all chunks.")

        if len(all_pnl_files) > 1:
            print("Merging PNL reports...")
            df_list = [pd.read_csv(path) for path in all_pnl_files]
            merged_df = pd.concat(df_list, ignore_index=True).drop_duplicates(subset=['wallet'])
            final_path = os.path.join(config.FILES_DIR, f"pnl_merged_{uuid.uuid4()}.csv")
            merged_df.to_csv(final_path, index=False)
            temp_files_to_clean.append(final_path)
        else:
            final_path = all_pnl_files[0]
        
        caption = f"✅ Ваш PNL-отчет готов. Обработано {len(unique_wallets)} уникальных кошельков."
        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data="main_menu")]])
        
        with open(final_path, "rb") as f:
            await bot.send_document(chat_id=chat_id, document=f, caption=caption, reply_markup=back_button_markup)

    except Exception as e:
        print(f"CELERY_ERROR: Задача PNL провалилась: {e}")
        await bot.send_message(chat_id=chat_id, text="❌ Произошла критическая ошибка при выполнении вашего запроса на PNL.")
    finally:
        for f_path in temp_files_to_clean:
            if os.path.exists(f_path):
                os.remove(f_path)

@celery.task
async def run_swaps_fetch_task(program: str, interval: str, chat_id: int):
    """
    Фоновая задача для получения Program Swaps.
    """
    print(f"CELERY_TASK: Запущена задача Program Swaps для чата {chat_id}.")
    driver = None
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    try:
        # ИСПРАВЛЕНО: Присваиваем результат только одной переменной
        driver = init_worker_driver()
        
        file_path = perform_program_swaps(driver, program, interval)

        if file_path and os.path.exists(file_path):
            back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data="main_menu")]])
            with open(file_path, "rb") as f:
                asyncio.run(bot.send_document(
                    chat_id=chat_id, document=f, caption=f"✅ Ваш отчет по Program Swaps для `{program}` готов.", reply_markup=back_button_markup
                ))
            # os.remove(file_path)
        else:
            asyncio.run(bot.send_message(chat_id=chat_id, text=f"❌ Не удалось получить отчет по Program Swaps от Discord-бота."))
    except Exception as e:
        print(f"CELERY_ERROR: Задача Swaps провалилась: {e}")
        asyncio.run(bot.send_message(chat_id=chat_id, text="❌ Произошла критическая ошибка при выполнении вашего запроса на Swaps."))
    finally:
        # ИСПРАВЛЕНО: Очистка временного профиля больше не нужна
        if driver:
            driver.quit()
        print(f"CELERY_TASK: Драйвер для задачи Swaps (чат {chat_id}) был закрыт.")

@celery.task
async def _traders_fetch_async(file_content_str: str, chat_id: int):
    """
    Асинхронная логика для получения трейдеров.
    ИСПРАВЛЕНО: Убраны промежуточные сообщения о разбивке на пакеты.
    """
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    temp_files_to_clean = []
    
    try:
        token_addresses = [line.strip() for line in file_content_str.splitlines() if line.strip()]
        token_chunks = [token_addresses[i:i + TOKENS_CHUNK_SIZE] for i in range(0, len(token_addresses), TOKENS_CHUNK_SIZE)]
        all_traders_files = []

        # # --- БЛОК УВЕДОМЛЕНИЙ УДАЛЕН ---

        for i, chunk in enumerate(token_chunks, 1):
            driver = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt", encoding='utf-8') as tmp_f:
                    tmp_f.write("\n".join(chunk))
                    temp_filepath = tmp_f.name
                    temp_files_to_clean.append(temp_filepath)
                
                driver = init_worker_driver()
                result_path = perform_toplevel_traders_fetch(driver, temp_filepath)
                if result_path:
                    all_traders_files.append(result_path)
            finally:
                if driver: driver.quit()

        if not all_traders_files:
            raise Exception("Top Traders fetch failed for all chunks.")
        
        final_trader_list = []
        for path in all_traders_files:
            with open(path, 'r', encoding='utf-8') as f:
                final_trader_list.extend([line.strip() for line in f if line.strip() and not line.startswith('---')])
            temp_files_to_clean.append(path)
        
        final_text = "\n".join(final_trader_list)
        final_path = os.path.join(config.TOP_TRADERS_DIR, f"traders_merged_{uuid.uuid4()}.txt")
        temp_files_to_clean.append(final_path)
        with open(final_path, 'w', encoding='utf-8') as f: f.write(final_text)

        caption = f"✅ Ваш отчет по топ-трейдерам готов. Обработано {len(token_addresses)} токенов."
        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data="main_menu")]])
        
        with open(final_path, "rb") as f:
            await bot.send_document(chat_id=chat_id, document=f, caption=caption, filename="top_traders_merged.txt", reply_markup=back_button_markup)

    except Exception as e:
        print(f"CELERY_ERROR: Задача Top Traders провалилась: {e}")
        await bot.send_message(chat_id=chat_id, text="❌ Произошла критическая ошибка при выполнении вашего запроса.")
    finally:
        for f_path in temp_files_to_clean:
            if os.path.exists(f_path):
                os.remove(f_path)
        
@celery.task
def run_token_parse_task(chat_id: int, settings: dict):
    """
    Фоновая задача для парсинга токенов по заданным критериям.
    ИСПРАВЛЕНА: Использует async_to_sync для вызова async-функций.
    """
    print(f"CELERY_TASK: Запущена задача Token Parse для чата {chat_id}")
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    
    # Создаем обертку, которая сможет запускать наши async-функции
    sync_send_message = async_to_sync(bot.send_message)
    sync_send_document = async_to_sync(bot.send_document)
    
    try:
        platforms = settings.get('platforms', [])
        period_key = settings.get('period', '24h')
        categories = settings.get('categories', [])
        lang = settings.get('lang', 'en')
        
        hours = int(period_key.replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        # ИСПРАВЛЕНО: Правильно вызываем асинхронный сервис
        results = async_to_sync(supabase_service.fetch_tokens_by_criteria)(start_time, platforms, categories)
        
        if not results:
            sync_send_message(chat_id=chat_id, text="🤷 По вашим критериям токены не найдены.")
            return

        df = pd.DataFrame(results)
        output = io.StringIO()
        fieldnames = ["contract_address", "ticker", "name", "migration_time", "launchpad", "category"]
        df_final = df.reindex(columns=fieldnames)
        df_final.to_csv(output, index=False, header=True)
        
        csv_file_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        csv_file_bytes.name = f"tokens_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        caption = f"✅ Ваш отчет готов. Найдено {len(df)} токенов."
        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data="main_menu")]])

        sync_send_document(
            chat_id=chat_id, 
            document=csv_file_bytes, 
            caption=caption,
            reply_markup=back_button_markup
        )
        
        print(f"CELERY_TASK: Задача Token Parse для чата {chat_id} успешно завершена.")

    except Exception as e:
        print(f"CELERY_ERROR: Задача Token Parse провалилась: {e}")
        sync_send_message(chat_id=chat_id, text="❌ Произошла ошибка при выполнении вашего запроса на парсинг токенов.")
        
@celery.task
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
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=start_text)

        # --- ЭТАП 1: GET TOKENS ---
        # Уведомления об этапах теперь тоже отправляются как новые сообщения
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"🚀 Этап 1/3: Поиск токенов...")
        hours = int(template.get('time_period', '24h').replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        categories = [cat for cat in template.get('categories', []) if cat in ['completed', 'completing']]
        tokens = await supabase_service.fetch_tokens_by_criteria(start_time, template.get('platforms', []), categories)

        if not tokens:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ Не найдено токенов по вашему шаблону. Задача остановлена.")
            return

        token_addresses = [t['contract_address'] for t in tokens]
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"✅ Этап 1 завершен. Найдено {len(tokens)} токенов.")

        # --- ЭТАП 2: GET TOP TRADERS ---
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"👥 Этап 2/3: Получение трейдеров для {len(token_addresses)} токенов. Это может занять время...")
        
        token_chunks = [token_addresses[i:i + TOKENS_CHUNK_SIZE] for i in range(0, len(token_addresses), TOKENS_CHUNK_SIZE)]
        all_traders_files = []
        
        for i, chunk in enumerate(token_chunks, 1):
            if len(token_chunks) > 1:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"👥 Этап 2/3: Обрабатываю пакет токенов {i} из {len(token_chunks)}...")
            
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
             await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ Не удалось получить список трейдеров. Задача остановлена.")
             raise Exception("Top Traders fetch failed")

        final_trader_list = []
        for path in all_traders_files:
            with open(path, 'r', encoding='utf-8') as f:
                final_trader_list.extend([line.strip() for line in f if line.strip() and not line.startswith('---')])
            temp_files_to_clean.append(path)
        
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="✅ Этап 2 завершен. Список трейдеров собран.")

        # --- ЭТАП 3: GET PNL ---
        unique_traders = list(set(final_trader_list))
        if not unique_traders:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="⚠️ Трейдеры для анализа PNL не найдены. Завершаю задачу.")
            return

        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"📊 Этап 3/3: Получение PNL для {len(unique_traders)} трейдеров.")
        
        trader_chunks = [unique_traders[i:i + TRADERS_CHUNK_SIZE] for i in range(0, len(unique_traders), TRADERS_CHUNK_SIZE)]
        all_pnl_reports_paths = []
        
        for i, chunk in enumerate(trader_chunks, 1):
            if len(trader_chunks) > 1:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"🪓 Обрабатываю PNL-пакет {i} из {len(trader_chunks)}...")
            
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
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="❌ Не удалось получить ни одного PNL отчета. Задача прервана.")
            raise Exception("PNL fetch failed")

        # --- ЭТАП 4: ОБЪЕДИНЕНИЕ И ОТПРАВКА PNL ---
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"🖇️ Этап 4/4: Объединение и фильтрация PNL-отчетов...")
        
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
                reply_markup=back_button_markup
            )
        
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="Все готово!")
        
    except Exception as e:
        print(f"CELERY_ERROR: 'All-In Parse' провалился: {e}")
        error_text = f"❌ Произошла критическая ошибка во время 'All-In Parse'."
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text)
        except:
            await bot.send_message(chat_id=chat_id, text=error_text)
    finally:
        for f_path in temp_files_to_clean:
            if os.path.exists(f_path):
                os.remove(f_path)
        print("CELERY_TASK: 'All-In Parse' завершен, временные файлы очищены.")
        
def apply_pnl_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Применяет сохраненные PNL-фильтры к DataFrame."""
    if not filters:
        return df

    filtered_df = df.copy()
    for column, rules in filters.items():
        if column not in filtered_df.columns:
            continue
        
        # Убедимся, что колонка числовая, игнорируя ошибки
        filtered_df[column] = pd.to_numeric(filtered_df[column], errors='coerce')
        
        min_val = rules.get('min')
        max_val = rules.get('max')

        if min_val is not None:
            filtered_df = filtered_df[filtered_df[column] >= min_val]
        if max_val is not None:
            filtered_df = filtered_df[filtered_df[column] <= max_val]
            
    return filtered_df

@celery.task
def run_all_in_parse_pipeline_task_wrapper(chat_id: int, template: dict, message_id: int):
    """
    СИНХРОННАЯ задача-обертка, которая запускает наш асинхронный пайплайн.
    """
    async_to_sync(_all_in_parse_pipeline_async)(chat_id, template, message_id) # <-- вернули message_id
    
@celery.task
def run_pnl_fetch_task(wallets: list, chat_id: int):
    """СИНХРОННАЯ обертка для _pnl_fetch_async."""
    async_to_sync(_pnl_fetch_async)(wallets, chat_id)

@celery.task
def run_traders_fetch_task(file_content_str: str, chat_id: int):
    """СИНХРОННАЯ обертка для _traders_fetch_async."""
    async_to_sync(_traders_fetch_async)(file_content_str, chat_id)