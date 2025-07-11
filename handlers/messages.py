import asyncio
import os
import io
import csv
import pandas as pd
import tempfile
from datetime import datetime

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaDocument
from telegram.ext import ContextTypes
from services import supabase_service, discord_scraper, queue_service
from tasks.celery_tasks import run_swaps_fetch_task, run_pnl_fetch_task, run_traders_fetch_task

# --- Импорты из нашей новой архитектуры ---

# Контекст приложения
from app_context import driver, driver_lock

# UI компоненты
from ui.translations import get_text
from ui.keyboards import get_main_menu_inline_keyboard
from ui.keyboards import get_template_settings_keyboard, get_template_view_keyboard
from ui.translations import get_text
from ui.keyboards import get_template_view_keyboard, get_pnl_filter_main_menu_keyboard # <-- ADD IT HERE
from ui.translations import get_text
# Конфигурация
from config import MAX_ADDRESS_LIST_SIZE, FILES_DIR

# --- Временные импорты и хелперы (в будущем переедут в services) ---

# TODO: Перенести в services/discord_scraper.py или task_orchestrator.py
from workers.get_trader_pnl import perform_pnl_fetch
from workers.get_program_swaps import perform_program_swaps
# from workers.get_top_traders import perform_toplevel_traders_fetch # Импорт для новой функции

# TODO: Перенести в services/supabase_service.py
from supabase_client import supabase

#
# =================================================================================
#  Раздел 1: Функции-исполнители (вызываются из обработчиков)
# =================================================================================
#

async def fetch_pnl_via_discord(wallets: list[str]) -> str | None:
    """(Временный хелпер) Вызывает Selenium для получения PNL."""
    async with driver_lock:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: perform_pnl_fetch(driver, wallets)
        )

# TODO: Эта функция должна быть перенесена в services/
async def process_wallet_stats(addresses: list[str], update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает список кошельков для получения PNL, разбивая на части при необходимости.
    """
    lang = context.user_data.get('lang', 'en')
    main_msg_id = context.user_data.get("main_message_id")
    chat_id = update.effective_chat.id

    address_chunks = [addresses[i:i + MAX_ADDRESS_LIST_SIZE] for i in range(0, len(addresses), MAX_ADDRESS_LIST_SIZE)]
    num_chunks = len(address_chunks)
    all_csv_paths = []
    final_csv_path = None

    try:
        if num_chunks > 1:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=f"⏳ Ваш список из {len(addresses)} кошельков будет обработан в {num_chunks} захода...")
        else:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "pnl_fetch_started"))

        for i, chunk in enumerate(address_chunks, 1):
            if num_chunks > 1:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=f"⚙️ Обрабатываю пакет {i} из {num_chunks}...")

            csv_path = await fetch_pnl_via_discord(chunk)
            if csv_path and os.path.exists(csv_path):
                all_csv_paths.append(csv_path)
            else:
                # logger.error(...)
                pass

        if not all_csv_paths:
            raise ValueError("Не удалось получить ни одного отчета от Discord-бота.")

        if len(all_csv_paths) > 1:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=f"🖇️ Объединяю {len(all_csv_paths)} отчетов...")
            df_list = [pd.read_csv(path) for path in all_csv_paths]
            merged_df = pd.concat(df_list, ignore_index=True)
            
            merged_filename = f"pnl_merged_{uuid.uuid4()}.csv"
            final_csv_path = os.path.join(FILES_DIR, merged_filename)
            merged_df.to_csv(final_csv_path, index=False)
        else:
            final_csv_path = all_csv_paths[0]

        caption = get_text(lang, "pnl_report_caption", len(addresses))
        with open(final_csv_path, "rb") as f:
            await context.bot.edit_message_media(
                chat_id=chat_id, message_id=main_msg_id,
                media=InputMediaDocument(media=f, caption=caption),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]])
            )
    except Exception as e:
        # logger.error(...)
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "error_occurred"))
    finally:
        for path in all_csv_paths:
            if os.path.exists(path): os.remove(path)
        if final_csv_path and final_csv_path not in all_csv_paths and os.path.exists(final_csv_path):
             os.remove(final_csv_path)

# TODO: И другие функции-исполнители, такие как `handle_dev_stats_request`, `search_by_wallet_address`...
# Я их пропущу для краткости, но их нужно перенести сюда.


#
# =================================================================================
#  Раздел 2: Основные обработчики сообщений
# =================================================================================
#

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает входящие текстовые сообщения в зависимости от состояния (state).
    """
    lang = context.user_data.get('lang', 'en')
    state = context.user_data.get('state')

    if not state:
        return

    main_msg_id = context.user_data.get('main_message_id')
    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    try:
        await update.message.delete()
    except Exception:
        pass

    # --- Диалог для Bundle Tracker ---
    if state == 'awaiting_bundle_address':
        if not (32 <= len(text) <= 44):
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_fail_invalid_address"))
            return
        context.user_data['bundle_tracker_data'] = {'address_to_track': text}
        context.user_data['state'] = 'awaiting_bundle_minutes'
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_step2_minutes"), parse_mode='Markdown')
        
    if state == 'awaiting_pnl_filter_value':
        column_to_set = context.user_data.get('pnl_filter_to_set')
        if not column_to_set:
            # Обработка ошибки, если колонка не найдена
            return

        template_data = context.user_data.get('template_data', {})
        # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ ---
        # Если pnl_filters не существует или он None, создаем пустой словарь.
        pnl_filters = template_data.get('pnl_filters')
        if pnl_filters is None:
            pnl_filters = {}
        # ---------------------------

        # Логика сброса или установки фильтра
        if text == '0':
            pnl_filters.pop(column_to_set, None) # Безопасно удаляем
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=f"Фильтр для `{column_to_set}` сброшен.")
        else:
            try:
                parts = text.split()
                if len(parts) == 1:
                    min_val, max_val = float(parts[0]), None
                elif len(parts) == 2:
                    min_val, max_val = float(parts[0]), float(parts[1])
                    if min_val > max_val: # Проверка корректности диапазона
                         await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text="Ошибка: минимальное значение не может быть больше максимального.")
                         return
                else:
                    raise ValueError("Неверное количество аргументов.")
                
                pnl_filters[column_to_set] = {"min": min_val, "max": max_val}
                await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=f"✅ Фильтр для `{column_to_set}` установлен.")
            
            except ValueError:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text="Ошибка: пожалуйста, введите одно или два числа.")
                return

        # Обновляем данные шаблона
        template_data['pnl_filters'] = pnl_filters
        context.user_data['template_data'] = template_data
        
        # Сбрасываем под-состояния
        context.user_data.pop('pnl_filter_to_set', None)
        context.user_data['state'] = 'awaiting_template_settings'
        
        await asyncio.sleep(2) # Пауза, чтобы пользователь увидел подтверждение
        
        # --- ИСПРАВЛЕНИЕ: Возвращаемся в меню PNL-фильтров ---
        text_to_show = "📊 **PNL-фильтры**\n\nВыберите категорию для настройки или нажмите 'Назад', когда закончите.\n\n**Текущие фильтры:**\n"
        if not pnl_filters:
            text_to_show += "_Пусто_"
        else:
            for col, val in pnl_filters.items():
                min_v = val.get('min', '-∞')
                max_v = val.get('max', '+∞')
                text_to_show += f"- `{col}`: от `{min_v}` до `{max_v}`\n"
        
        reply_markup = get_pnl_filter_main_menu_keyboard(template_data)
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=text_to_show, parse_mode="Markdown", reply_markup=reply_markup)
        
        return
    
    elif state == 'awaiting_bundle_minutes':
        try:
            minutes = int(text)
            if minutes < 1: raise ValueError("Value too small")
            context.user_data['bundle_tracker_data']['time_gap_min'] = minutes
            context.user_data['state'] = 'awaiting_bundle_count'
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_step3_count"), parse_mode='Markdown')
        except ValueError:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_fail_invalid_number", "(min: 1)"))

    elif state == 'awaiting_bundle_count':
        try:
            count = int(text)
            if count < 1: raise ValueError("Value too small")
            context.user_data['bundle_tracker_data']['min_cnt'] = count
            context.user_data['state'] = 'awaiting_bundle_diff'
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_step4_diff"), parse_mode='Markdown')
        except ValueError:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_fail_invalid_number"))

    elif state == 'awaiting_bundle_diff':
        try:
            diff = float(text.replace(',', '.'))
            if diff <= 0: raise ValueError("Value must be positive")
            context.user_data['bundle_tracker_data']['amount_step'] = diff
            context.user_data['state'] = 'awaiting_bundle_min_amount'
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_step5_min_amount"), parse_mode='Markdown')
        except ValueError:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_fail_invalid_number"))

    elif state == 'awaiting_bundle_min_amount':
        try:
            min_amount = float(text.replace(',', '.'))
            if min_amount < 0: raise ValueError("Value must be non-negative")
            context.user_data['bundle_tracker_data']['min_transfer_amount'] = min_amount
            context.user_data['state'] = 'awaiting_bundle_max_amount'
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_step6_max_amount"), parse_mode='Markdown')
        except ValueError:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "bundle_add_fail_invalid_number"))

    elif state == 'awaiting_bundle_max_amount':
        try:
            max_amount = float(text.replace(',', '.'))
            if max_amount < 0: raise ValueError("Value must be non-negative")
            
            ud = context.user_data['bundle_tracker_data']
            ud['max_transfer_amount'] = max_amount if max_amount > 0 else None
            
            final_data = {
                "user_id": update.effective_user.id,
                "chat_id": chat_id,
                "is_active": True,
                **ud
            }
            
            # ИСПОЛЬЗУЕМ СЕРВИС для сохранения данных
            await supabase_service.upsert_bundle_alert(final_data)
            
            max_amount_text = str(ud['max_transfer_amount']) if ud['max_transfer_amount'] is not None else "∞"
            success_key = "bundle_edit_success" if context.user_data.get("editing_existing") else "bundle_add_success"
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=main_msg_id,
                text=get_text(lang, success_key, ud['address_to_track'], ud['time_gap_min'], ud['min_cnt'], ud['amount_step'], ud['min_transfer_amount'], max_amount_text),
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="bundle_back_to_main")]])
            )
            
            # Очищаем состояние
            context.user_data.pop("editing_existing", None)
            context.user_data.pop('state', None)
            context.user_data.pop('bundle_tracker_data', None)
        except (ValueError, Exception) as e:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "error_occurred"))
            print(f"ERROR saving bundle: {e}")

    # --- Диалог создания шаблона ---
    elif state == 'awaiting_template_name':
        template_name = text
        if not template_name:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text="Название шаблона не может быть пустым.")
            return
        
        # ИСПРАВЛЕНО: Мы не создаем шаблон сразу.
        # Мы создаем временный словарь и переходим в режим настройки.
        
        # Используем единый ключ 'template_data' и для новых, и для старых шаблонов
        context.user_data['template_data'] = {
            "template_name": template_name,
            "platforms": [],
            "categories": ["completed", "completing"], # Значения по умолчанию
            "time_period": "24h"
        }
        context.user_data['state'] = 'awaiting_template_settings'
        
        reply_markup = get_template_settings_keyboard(lang, context.user_data['template_data'])
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=f"Настраиваем новый шаблон '{template_name}':",
            reply_markup=reply_markup
        )

    # --- Диалог для Program Parse ---
    elif state == 'awaiting_program_parse_program':
        context.user_data['program_parse_program'] = text
        context.user_data['state'] = 'awaiting_program_parse_interval'
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "program_parse_prompt_interval"))

    elif state == 'awaiting_program_parse_interval':
        if text.lower() not in ("3h", "6h", "12h", "24h"):
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "program_parse_interval_invalid"))
            return

        program = context.user_data.pop('program_parse_program')
        context.user_data.pop('state', None)
        # 1. Получаем текущую длину очереди
        current_queue_len = queue_service.get_queue_length()

        # 2. Наша задача будет следующей
        user_position = current_queue_len + 1

        # 3. Формируем новое сообщение для пользователя
        queue_text = f"⏳ Ваш запрос принят. Вы {user_position}-й в очереди. Пожалуйста, подождите..."

        # 4. Ставим задачу в очередь
        run_swaps_fetch_task.delay(program=program, interval=text, chat_id=chat_id)

        # 5. Мгновенно отвечаем пользователю с указанием его места
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=queue_text
        )


    # --- Состояния, которые ожидают файл, а не текст ---
    elif state in ['awaiting_trader_list', 'awaiting_wallet_stats', 'awaiting_dev_address']:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text="Пожалуйста, отправьте список адресов в виде .txt файла, а не текстом."
        )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает входящие .txt файлы для "тяжелых" задач.
    ИСПРАВЛЕНО: Все вызовы get_text теперь используют .format().
    """
    lang = context.user_data.get('lang', 'en')
    state = context.user_data.get('state')
    
    if state not in ['awaiting_trader_list', 'awaiting_wallet_stats']:
        return

    await update.message.delete()

    doc = update.message.document
    if not doc.file_name.lower().endswith('.txt'):
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get("main_message_id"),
            text="Поддерживаются только файлы формата .txt"
        )
        return

    main_msg_id = context.user_data.get("main_message_id")
    chat_id = update.effective_chat.id
    
    tg_file = await doc.get_file()
    file_content_bytes = await tg_file.download_as_bytearray()
    file_content_str = file_content_bytes.decode('utf-8')
    addresses = [line.strip() for line in file_content_str.splitlines() if line.strip()]

    # --- Валидация ---
    if not addresses:
        # ИСПОЛЬЗУЕМ .format()
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "input_empty_error"))
        return
        
    invalid_lines = [addr for addr in addresses if not (32 <= len(addr) <= 44)]
    if invalid_lines:
        error_sample = "\n".join(f"`{line}`" for line in invalid_lines[:5])
        # ИСПОЛЬЗУЕМ .format()
        error_text = get_text(lang, "input_address_length_error").format(error_sample)
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=error_text, parse_mode='Markdown')
        return

    # --- Логика очереди ---
    current_queue_len = queue_service.get_queue_length()
    user_position = current_queue_len + 1
    queue_text = f"⏳ Ваш запрос принят. Вы {user_position}-й в очереди на выполнение."
    
    # --- Запуск задачи ---
    if state == 'awaiting_trader_list':
        run_traders_fetch_task.delay(file_content_str=file_content_str, chat_id=chat_id)
    elif state == 'awaiting_wallet_stats':
        run_pnl_fetch_task.delay(wallets=addresses, chat_id=chat_id)
    
    context.user_data.pop('state', None)
    await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=queue_text)
    