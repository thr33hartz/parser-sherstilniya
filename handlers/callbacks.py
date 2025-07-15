def get_user_lang(context):
    return context.user_data.get("lang", "en")
import asyncio
import os
import io
import csv
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
logger = logging.getLogger(__name__)

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from tasks.celery_tasks import run_token_parse_task
from tasks.celery_tasks import run_all_in_parse_pipeline_task_wrapper
from ui.keyboards import get_template_settings_keyboard, get_template_category_keyboard, get_dev_parse_settings_keyboard

# --- Импорты из нашей новой архитектуры ---

# Контекст приложения (для доступа к driver и lock)
from app_context import driver, driver_lock
from services import supabase_service, discord_scraper, queue_service, price_service # <-- Убедитесь, что price_service здесь

# UI компоненты
from ui.keyboards import (
    get_main_menu_inline_keyboard, get_parse_submenu_keyboard, get_token_parse_settings_keyboard,
    get_platform_selection_keyboard, get_period_selection_keyboard, get_category_selection_keyboard,
    get_bundle_tracker_keyboard, get_template_management_keyboard, get_template_view_keyboard,
    get_template_edit_keyboard, get_dev_parse_period_keyboard, get_pnl_filter_submenu_keyboard,
    get_pnl_filter_main_menu_keyboard, get_language_keyboard, get_dev_pnl_filter_main_menu_keyboard, get_dev_pnl_filter_submenu_keyboard
)
from ui.translations import get_text, TRANSLATIONS

# Конфигурация и хелперы
from config import TOKEN_CATEGORIES, MAX_TRACKING_TASKS_PER_USER
from .commands import ensure_main_msg, send_new_main_menu  # Импортируем из соседнего файла в этой же папке

# --- Временные импорты (в будущем переедут в services) ---
# TODO: Перенести всю работу с Supabase в services/supabase_service.py
from supabase_client import supabase

# TODO: Перенести всю работу с "тяжелыми" задачами в services/task_orchestrator.py
# и workers/
from workers.get_trader_pnl import perform_pnl_fetch
from workers.get_program_swaps import perform_program_swaps
from fetch_tokens import fetch_tokens
from fetch_traders import process_tokens_for_traders

#
# =================================================================================
#  Раздел 1: Функции-помощники (в будущем переедут в 'services')
# =================================================================================
#

# TODO: Перенести в services/discord_scraper.py
async def fetch_pnl_via_discord(wallets: list[str]) -> str | None:
    """Вызывает Selenium-скрипт в пуле потоков, отдаёт путь к csv."""
    async with driver_lock:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: perform_pnl_fetch(driver, wallets)
        )

# TODO: Перенести в services/supabase_service.py
async def fetch_unique_launchpads() -> list:
    try:
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: supabase.table("tokens").select("launchpad").execute())
        if response.data:
            return sorted(list(set(item['launchpad'] for item in response.data if item['launchpad'] and item['launchpad'] != 'unknown')))
        return []
    except Exception as e:
        print(f"Ошибка при получении списка лаунчпадов: {e}")
        return []

# TODO: Перенести в services/supabase_service.py
async def fetch_user_templates(user_id: int) -> list:
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").select("*").eq("user_id", user_id).execute()
        )
        return response.data or []
    except Exception as e:
        print(f"Ошибка получения шаблонов для пользователя {user_id}: {e}")
        return []

# TODO: Перенести в services/supabase_service.py
async def create_template(user_id: int, template_name: str, platforms: list, time_period: str, categories: list) -> dict:
    template_data = {
        "id": str(uuid.uuid4()), "user_id": int(user_id), "template_name": template_name,
        "platforms": platforms, "time_period": time_period, "categories": categories,
    }
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").insert(template_data).execute()
        )
        return response.data[0]
    except Exception as e:
        print(f"Ошибка создания шаблона для пользователя {user_id}: {e}")
        raise

# TODO: Перенести в services/supabase_service.py
async def update_template(template_id: str, template_name: str, platforms: list, time_period: str, categories: list) -> dict:
    updates = {"template_name": template_name, "platforms": platforms, "time_period": time_period, "categories": categories}
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").update(updates).eq("id", template_id).execute()
        )
        return response.data[0]
    except Exception as e:
        print(f"Ошибка обновления шаблона {template_id}: {e}")
        raise

# TODO: Перенести в services/supabase_service.py
async def delete_template(template_id: str) -> None:
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").delete().eq("id", template_id).execute()
        )
    except Exception as e:
        print(f"Ошибка удаления шаблона {template_id}: {e}")
        raise

#
# =================================================================================
#  Раздел 2: Основные обработчики колбэков (`..._callback`)
# =================================================================================
#

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает главное меню. Корректно обрабатывает возврат из сообщений с файлами.
    """
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')

    # --- Получаем актуальные данные для меню ---
    sol_price = await price_service.get_sol_price()
    price_str = f"{sol_price:.2f}" if sol_price else "N/A"
    
    # Используем вашу функцию get_text для получения шаблона
    text_template = get_text(lang, "main_menu_message") 
    menu_text = text_template.format(price_str)
    
    main_menu_keyboard = get_main_menu_inline_keyboard(lang, context.user_data.get("premium", False))

    # --- УМНАЯ ЛОГИКА ---
    # Проверяем, есть ли в сообщении, с которого пришел запрос, документ (файл)
    if query.message.document:
        # Если ДА (это сообщение с CSV), то мы не можем его редактировать в текст.
        # 1. Убираем с него клавиатуру, чтобы кнопка "Назад" стала неактивной.
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            # Ничего страшного, если у сообщения и так не было клавиатуры
            print(f"Info: Could not remove keyboard from media message: {e}")

        # 2. Отправляем ПОЛНОСТЬЮ НОВОЕ сообщение с главным меню.
        new_menu_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=menu_text,
            reply_markup=main_menu_keyboard,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        # 3. КРИТИЧЕСКИ ВАЖНО: Обновляем ID главного сообщения в памяти бота.
        #    Теперь все последующие нажатия на кнопки меню будут редактировать это новое сообщение.
        context.user_data["main_message_id"] = new_menu_msg.message_id
    else:
        # Если НЕТ (это было обычное текстовое сообщение), то просто редактируем его.
        await query.message.edit_text(
            text=menu_text,
            reply_markup=main_menu_keyboard,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        
async def set_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает выбор языка, редактирует сообщение и показывает главное меню.
    Эта функция - чистое UI, поэтому ее логика не меняется.
    """
    query = update.callback_query
    await query.answer()

    lang_code = query.data.split("_")[-1]
    context.user_data["lang"] = lang_code
    
    # Редактируем сообщение, превращая его в главное меню.
    await ensure_main_msg(
        context.bot,
        query.message.chat_id,
        context,
        get_text(lang_code, "main_menu_message"),
        reply_markup=get_main_menu_inline_keyboard(lang_code, context.user_data.get("premium", False)),
        parse_mode="Markdown"
    )

def apply_dev_pnl_filters(dev_stats_list: list, pnl_filters: dict) -> list:
    """
    Применяет PNL-фильтры к списку словарей со статистикой разработчиков.
    """
    if not pnl_filters:
        return dev_stats_list

    # Используем pandas для удобной и быстрой фильтрации
    df = pd.DataFrame(dev_stats_list)
    
    filtered_df = df.copy()

    for column, rules in pnl_filters.items():
        if column not in filtered_df.columns:
            continue
        
        # Принудительно преобразуем колонку в числовой формат
        filtered_df[column] = pd.to_numeric(filtered_df[column], errors='coerce')
        filtered_df.dropna(subset=[column], inplace=True) # Удаляем строки, где преобразование не удалось

        min_val = rules.get('min')
        max_val = rules.get('max')

        if min_val is not None:
            filtered_df = filtered_df[filtered_df[column] >= min_val]
        if max_val is not None:
            filtered_df = filtered_df[filtered_df[column] <= max_val]
            
    return filtered_df.to_dict('records')

async def main_menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает нажатия на кнопки главного меню.
    Переводит пользователя в соответствующее подменю или состояние.
    """
    query = update.callback_query
    await query.answer()

    # Сохраняем ID сообщения, чтобы мы всегда знали, какое сообщение редактировать
    context.user_data["main_message_id"] = query.message.message_id
    
    lang = get_user_lang(context)
    action = query.data.replace("mainmenu_", "")
    
    # Готовим универсальную кнопку "Назад" для всех подменю
    back_button = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]])

    if action == "parse":
        reply_markup = get_parse_submenu_keyboard(lang)
        await query.message.edit_text(
            get_text(lang, "parse_menu_prompt"), 
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    elif action == "dev_parse":
        # ИСПРАВЛЕНО: Открываем новое меню настроек
        context.user_data['dev_parse_platforms'] = []
        context.user_data['dev_parse_categories'] = ['completed', 'completing']
        context.user_data['dev_parse_period'] = '72h'
        reply_markup = get_dev_parse_settings_keyboard(lang, context)
        await query.message.edit_text(
            get_text(lang, "dev_parse_menu_prompt"), # <-- ИЗМЕНЕНО
            reply_markup=get_dev_parse_settings_keyboard(lang, context),
            disable_web_page_preview=True
        )
                
    elif action == "program_parse":
        context.user_data["state"] = "awaiting_program_parse_program"
        await query.message.edit_text(
            get_text(lang, "program_parse_prompt_program"), # <-- ИЗМЕНЕНО
            reply_markup=back_button,
            disable_web_page_preview=True
        )
        
    elif action == "bundle_tracker":
        reply_markup = get_bundle_tracker_keyboard(lang)
        await query.message.edit_text(
            get_text(lang, "bundle_tracker_menu_prompt"), # <-- ИЗМЕНЕНО
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    elif action == "settings":
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌍 Language", callback_data="settings_language")],
            [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
        ])
        await query.message.edit_text(
            get_text(lang, "settings_menu_prompt"),
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    elif action.startswith("settings_"):
        await settings_callback_handler(update, context)
        return
    else:  # Для кнопок "Copytrade simulation" и других неизвестных
        await query.message.edit_text(
            get_text(lang, "feature_in_development"), 
            reply_markup=back_button,
            disable_web_page_preview=True
        )
    

async def parse_submenu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает нажатия на кнопки в подменю "Parse".
    """
    query = update.callback_query
    await query.answer()

    lang = get_user_lang(context)
    command = query.data
    
    # --- Навигация ---
    if command == "parse_back":
        # Используем edit_text для плавного возврата в главное меню
        await query.message.edit_text(
            text=get_text(lang, "main_menu_message"),
            reply_markup=get_main_menu_inline_keyboard(lang, context.user_data.get("premium", False)),
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        return

    # --- Установка состояний для будущих задач ---
    
    # Готовим универсальную кнопку "Назад"
    back_button = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]])

    if command == "parse_all_in":
        user_id = update.effective_user.id
        reply_markup = get_template_management_keyboard(lang, user_id)
        await query.message.edit_text(
            get_text(lang, "template_management_prompt"), # <-- ИЗМЕНЕНО
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        
    elif command == "parse_get_tokens":
        # Шаг 1: Получаем все доступные платформы
        all_platforms = await supabase_service.fetch_unique_launchpads()
        
        # Шаг 2: Устанавливаем ЭТОТ ЖЕ СПИСОК как изначально выбранный
        context.user_data.update({
            'token_parse_platforms': all_platforms, # <-- ИСПРАВЛЕНИЕ
            'token_parse_period': '24h',
            'token_parse_categories': TOKEN_CATEGORIES.copy()
        })
        
        # Теперь клавиатура будет сгенерирована с правильными данными
        reply_markup = get_token_parse_settings_keyboard(lang, context)
        await query.message.edit_text(
            text=get_text(lang, "get_tokens_prompt"),
            reply_markup=reply_markup
        )
        
    elif command == "parse_get_traders":
        context.user_data['state'] = 'awaiting_trader_list'
        # Обновляем текст, чтобы явно просить только файл
        prompt_text = get_text(lang, "get_traders_prompt").replace("как обычный текст или .txt файл.", "в виде .txt файла.")
        await query.message.edit_text(text=prompt_text, reply_markup=back_button, disable_web_page_preview=True)
        
    elif command == "parse_get_stats":
        context.user_data['state'] = 'awaiting_wallet_stats'
        prompt_text = get_text(lang, "get_traders_prompt").replace("контрактов", "кошельков")
        await query.message.edit_text(text=prompt_text, reply_markup=back_button, disable_web_page_preview=True)


async def token_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает нажатия в меню настроек парсинга токенов.
    При нажатии 'Parse' ставит задачу в очередь Celery.
    """
    query = update.callback_query
    await query.answer()
    lang = get_user_lang(context)
    command = query.data
    
    if command == "tokensettings_platforms":
        all_platforms = await supabase_service.fetch_unique_launchpads()
        selected_platforms = context.user_data.get('token_parse_platforms', [])
        reply_markup = get_platform_selection_keyboard(lang, all_platforms, selected_platforms)
        await query.message.edit_text(text=get_text(lang, "platforms_menu_prompt"), reply_markup=reply_markup, disable_web_page_preview=True)

    elif command == "tokensettings_category":
        selected_categories = context.user_data.get('token_parse_categories', [])
        # ИСПРАВЛЕНИЕ: Добавляем `context` как третий аргумент
        reply_markup = get_category_selection_keyboard(lang, selected_categories, context)
        await query.message.edit_text(text=get_text(lang, "category_prompt"), reply_markup=reply_markup, disable_web_page_preview=True)

    elif command == "tokensettings_period":
        current_period = context.user_data.get('token_parse_period', '24h')
        reply_markup = get_period_selection_keyboard(lang, current_period)
        await query.message.edit_text(text=get_text(lang, "time_period_prompt"), reply_markup=reply_markup, disable_web_page_preview=True)

    elif command == "tokensettings_execute":
        # === ГЛАВНОЕ ИЗМЕНЕНИЕ ===
        # Мы больше не выполняем тяжелую функцию здесь.
        # Мы ставим задачу в очередь и сразу отвечаем пользователю.
        
        chat_id = update.effective_chat.id
        settings = {
            "platforms": context.user_data.get('token_parse_platforms', []),
            "period": context.user_data.get('token_parse_period', '24h'),
            "categories": context.user_data.get('token_parse_categories', []),
            "lang": lang,
        }
        
        # Ставим задачу в очередь. Метод .delay() не блокирует бота.
        run_token_parse_task.delay(chat_id=chat_id, settings=settings)
        
        # Сразу же отвечаем пользователю
        await query.message.edit_text(text="✅ Ваш запрос принят в очередь и уже выполняется в фоне. Вы получите файл, как только он будет готов.", disable_web_page_preview=True)

    elif command == "main_menu": # Эта кнопка возвращает в главное меню из настроек
        await ensure_main_msg(
            context.bot,
            query.message.chat_id,
            context,
            text=get_text(lang, "main_menu_message"),
            reply_markup=get_main_menu_inline_keyboard(lang, context.user_data.get("premium", False)),
            parse_mode="Markdown",
            disable_web_page_preview=True
        )


async def platform_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    "Умный" обработчик выбора платформ.
    ИСПРАВЛЕНО: Теперь корректно возвращает в меню Dev Parse.
    """
    query = update.callback_query
    await query.answer()
    lang = get_user_lang(context)
    command = query.data
    current_state = context.user_data.get('state')

    # Определяем, где хранятся данные
    # (в шаблоне, в настройках Dev Parse или в настройках Get Tokens)
    if current_state == 'template_editing_platforms':
        data_source_key = 'template_data'
        list_key = 'platforms'
    elif current_state == 'dev_parse_editing_platforms':
        data_source_key = 'user_data'
        list_key = 'dev_parse_platforms'
    else:
        data_source_key = 'user_data'
        list_key = 'token_parse_platforms'
    
    data_source = context.user_data.get(data_source_key, context.user_data)
    selected_list = data_source.get(list_key, [])

    # --- Логика для кнопок ---
    if command == "platform_done":
        # Возвращаемся в предыдущее меню в зависимости от состояния
        if current_state == 'template_editing_platforms':
            reply_markup = get_template_settings_keyboard(lang, data_source)
            await query.message.edit_text(f"Настраиваем шаблон '{data_source.get('template_name', '')}':", reply_markup=reply_markup)
            context.user_data['state'] = 'awaiting_template_settings'
        
        elif current_state == 'dev_parse_editing_platforms':
            reply_markup = get_dev_parse_settings_keyboard(lang, context)
            await query.message.edit_text(get_text(lang, "dev_parse_menu_prompt"), reply_markup=reply_markup)
            context.user_data['state'] = None # Сбрасываем под-состояние
            
        else: # Возврат в обычный Get Tokens
            reply_markup = get_token_parse_settings_keyboard(lang, context)
            await query.message.edit_text(text=get_text(lang, "get_tokens_prompt"), reply_markup=reply_markup)
        return

    # Логика переключения платформы
    platform_name = command.replace("platform_toggle_", "")
    if platform_name in selected_list:
        selected_list.remove(platform_name)
    else:
        selected_list.append(platform_name)
    data_source[list_key] = selected_list

    # Обновляем клавиатуру
    all_platforms = await supabase_service.fetch_unique_launchpads()
    reply_markup = get_platform_selection_keyboard(lang, all_platforms, selected_list)
    await query.message.edit_reply_markup(reply_markup=reply_markup)

async def period_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """"Умный" обработчик выбора периода для ВСЕХ меню."""
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')
    command = query.data
    current_state = context.user_data.get('state')

    # Определяем, где хранить данные и куда возвращаться
    if current_state == 'dev_parse_editing_period':
        data_source = context.user_data
        key = 'dev_parse_period'
    elif current_state == 'template_editing_period':
        data_source = context.user_data.get('template_data', {})
        key = 'time_period'
    else: # По умолчанию для Get Tokens
        data_source = context.user_data
        key = 'token_parse_period'

    # --- Логика для кнопок ---
    if command == "period_done":
        context.user_data['state'] = None # Сбрасываем под-состояние
        if current_state == 'dev_parse_editing_period':
            await query.message.edit_text(
                get_text(lang, "dev_parse_menu_prompt"),
                reply_markup=get_dev_parse_settings_keyboard(lang, context)
            )
        elif current_state == 'template_editing_period':
            await query.message.edit_text(
                f"Настраиваем шаблон '{data_source.get('template_name', '')}':",
                reply_markup=get_template_settings_keyboard(lang, data_source)
            )
        else: # Возврат в Get Tokens
            await query.message.edit_text(
                get_text(lang, "get_tokens_prompt"),
                reply_markup=get_token_parse_settings_keyboard(lang, context)
            )
        return

    # Логика выбора периода
    selected_period = command.replace("period_select_", "")
    data_source[key] = selected_period
    
    # Обновляем клавиатуру
    if current_state == 'dev_parse_editing_period':
        reply_markup = get_dev_parse_period_keyboard(lang, selected_period)
    else:
        reply_markup = get_period_selection_keyboard(lang, selected_period)
    
    await query.message.edit_reply_markup(reply_markup=reply_markup)


async def category_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """"Умный" обработчик выбора категорий для ВСЕХ меню."""
    query = update.callback_query
    await query.answer()
    lang = get_user_lang(context)
    command = query.data
    current_state = context.user_data.get('state')

    # Определяем, где хранить данные
    if current_state == 'dev_parse_editing_categories':
        data_source = context.user_data
        list_key = 'dev_parse_categories'
    else: # По умолчанию для Get Tokens
        data_source = context.user_data
        list_key = 'token_parse_categories'
    
    selected_list = data_source.get(list_key, [])

    # Логика кнопки "Назад"
    if command == "category_done":
        if current_state == 'dev_parse_editing_categories':
            context.user_data['state'] = None
            reply_markup = get_dev_parse_settings_keyboard(lang, context)
            await query.message.edit_text(get_text(lang, "dev_parse_menu_prompt"), reply_markup=reply_markup)
        else: # Возврат в Get Tokens
            reply_markup = get_token_parse_settings_keyboard(lang, context)
            await query.message.edit_text(text=get_text(lang, "get_tokens_prompt"), reply_markup=reply_markup)
        return

    # Логика переключения категории
    category_name = command.replace("category_toggle_", "")
    if category_name in selected_list:
        selected_list.remove(category_name)
    else:
        selected_list.append(category_name)
    data_source[list_key] = selected_list

    # Обновляем клавиатуру
    reply_markup = get_category_selection_keyboard(lang, selected_list, context)
    await query.message.edit_reply_markup(reply_markup=reply_markup)
    
async def template_management_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает ВЕРХНЕУРОВНЕВЫЕ действия с шаблонами.
    ИСПРАВЛЕНО: Убрана лишняя и неправильная логика "template_save".
    """
    query = update.callback_query
    await query.answer()
    lang = get_user_lang(context)
    user_id = update.effective_user.id
    command = query.data
    
    if command == "template_create":
        context.user_data['state'] = 'awaiting_template_name'
        # Сохраняем пустой объект для настроек
        context.user_data['template_data'] = {
            "platforms": [],
            "time_period": "24h",
            "categories": ["completed", "completing"],
            "pnl_filters": {}
        }
        lang = context.user_data.get('lang', 'ru')
        await query.message.edit_text(get_text(lang, "prompt_template_name"))

    elif command == "template_view":
        templates = await supabase_service.fetch_user_templates(user_id)
        reply_markup = get_template_view_keyboard(lang, templates)
        await query.message.edit_text(
            text=get_text(lang, "template_view_prompt"),
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

    elif command.startswith("template_select_"):
        template_id = command.replace("template_select_", "")
        templates = await supabase_service.fetch_user_templates(user_id)
        selected_template = next((t for t in templates if t["id"] == template_id), None)
        if not selected_template:
            await query.message.edit_text(get_text(lang, "template_not_found_error"))
            return

        queue_len = queue_service.get_queue_length()
        position = queue_len + 1
        queue_text = get_text(lang, "all_in_parse_queued").format(position)
        await query.message.edit_text(text=queue_text)

        run_all_in_parse_pipeline_task_wrapper.delay(
            chat_id=update.effective_chat.id, 
            template=selected_template,
            message_id=query.message.message_id 
        )

    elif command.startswith("template_edit_"):
        template_id = command.replace("template_edit_", "")
        templates = await supabase_service.fetch_user_templates(user_id)
        template = next((t for t in templates if t["id"] == template_id), None)
        if not template:
            await query.message.edit_text(get_text(lang, "template_not_found_error"))
            return
        
        context.user_data['template_data'] = template 
        context.user_data['state'] = 'awaiting_template_settings'
        
        reply_markup = get_template_settings_keyboard(lang, template)
        await query.message.edit_text(
            get_text(lang, "template_editing_prompt").format(template['template_name']),
            reply_markup=reply_markup
        )
    
    elif command.startswith("template_delete_"):
        template_id = command.replace("template_delete_", "")
        await supabase_service.delete_template(template_id)
        templates = await supabase_service.fetch_user_templates(user_id)
        await query.message.edit_text(
            text=get_text(lang, "template_deleted"),
            reply_markup=get_template_view_keyboard(lang, templates)
        )

    elif command == "template_back_to_menu":
        # Эта кнопка возвращает в подменю "Parse"
        reply_markup = get_parse_submenu_keyboard(lang)
        await query.message.edit_text(
            get_text(lang, "parse_menu_prompt"), 
            reply_markup=reply_markup
        )

async def template_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает ВСЕ кнопки на экране создания/редактирования шаблона.
    """
    query = update.callback_query
    await query.answer()
    
    lang = get_user_lang(context)
    command = query.data
    template_data = context.user_data.get('template_data')

    if not template_data and "template_set" in command:
        await query.message.edit_text("Ошибка: данные для шаблона не найдены. Пожалуйста, начните заново.")
        return

    # --- Роутер для кнопок ---
    if command == "template_set_platforms":
        context.user_data['state'] = 'template_editing_platforms'
        all_platforms = await supabase_service.fetch_unique_launchpads()
        await query.message.edit_text("Выберите платформы:", reply_markup=get_platform_selection_keyboard(lang, all_platforms, template_data.get('platforms', [])))

    elif command == "template_set_category":
        context.user_data['state'] = 'template_editing_categories'
        await query.message.edit_text(get_text(lang, "category_prompt"), reply_markup=get_template_category_keyboard(lang, template_data.get('categories', [])))
    
    # ИСПРАВЛЕНО: Добавлена логика для переключения категорий
    elif command.startswith("template_set_toggle_category_"):
        category = command.replace("template_set_toggle_category_", "")
        selected_categories = template_data.get('categories', [])
        if category in selected_categories:
            selected_categories.remove(category)
        else:
            selected_categories.append(category)
        template_data['categories'] = selected_categories
        # Обновляем клавиатуру, не меняя текст сообщения
        await query.message.edit_reply_markup(reply_markup=get_template_category_keyboard(lang, selected_categories))

    # ИСПРАВЛЕНО: Добавлена логика для кнопки "Назад" из меню категорий
    elif command == "template_set_category_done":
        context.user_data['state'] = 'awaiting_template_settings'
        reply_markup = get_template_settings_keyboard(lang, template_data)
        await query.message.edit_text(f"Настраиваем шаблон '{template_data['template_name']}':", reply_markup=reply_markup)

    elif command == "template_set_period":
        context.user_data['state'] = 'template_editing_period'
        await query.message.edit_text(get_text(lang, "time_period_prompt"), reply_markup=get_period_selection_keyboard(lang, template_data.get('time_period', '24h')))

    elif command == "template_set_save":
        # Если у шаблона есть 'id', значит мы его редактируем
        if 'id' in template_data:
            # Передаем весь объект template_data, который уже содержит pnl_filters
            await supabase_service.update_template(template_data['id'], template_data)
            await query.message.edit_text(
                text=get_text(lang, "template_updated_successfully"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="template_view")]
                ])
            )        # Если 'id' нет, значит это новый шаблон
        else:
            template_data['user_id'] = update.effective_user.id
            # Передаем весь объект template_data, который будет включать и pnl_filters
            await supabase_service.create_template(template_data)
            # --- ВСТАВКА chat_id и main_msg_id перед edit_message_text ---
            chat_id = update.effective_chat.id
            main_msg_id = context.user_data.get("main_message_id", query.message.message_id)
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text="✅ Шаблон успешно создан!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]
                ]), disable_web_page_preview=True
            )
        
        context.user_data.pop('state', None)
        context.user_data.pop('template_data', None)
    
    elif command == "template_set_pnl_filters":
        pnl_filters = template_data.get('pnl_filters', {})
        # Формируем красивое сообщение с текущими фильтрами
        text = "📊 **PNL-фильтры**\n\nВыберите категорию для настройки.\n\n**Текущие фильтры:**\n"
        if not pnl_filters:
            text += "_Пусто_"
        else:
            for col, val in pnl_filters.items():
                min_val = val.get('min', 'N/A')
                max_val = val.get('max', 'N/A')
                text += f"- `{col}`: от `{min_val}` до `{max_val}`\n"
        
        reply_markup = get_pnl_filter_main_menu_keyboard(template_data)
        await query.message.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)


    elif command == "template_set_cancel":
        context.user_data.pop('state', None)
        context.user_data.pop('template_data', None)
        await query.message.edit_text(get_text(lang, "template_cancelled"))
        # Можно добавить кнопку для возврата в меню управления шаблонами
        user_id = update.effective_user.id
        reply_markup = get_template_management_keyboard(lang, user_id)
        await query.message.edit_text(
            get_text(lang, "template_management_prompt"),
            reply_markup=reply_markup
        )

        
async def show_user_bundle_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Отображает пользователю список его активных трекеров бандлов.
    Теперь использует supabase_service для получения данных.
    """
    query = update.callback_query
    lang = context.user_data.get('lang', 'en')
    user_id = update.effective_user.id
    
    try:
        # ИСПОЛЬЗУЕМ СЕРВИС
        alerts = await supabase_service.get_user_bundle_alerts(user_id)
        
        if not alerts:
            await query.edit_message_text(
                get_text(lang, "bundle_view_empty"), 
                reply_markup=get_bundle_tracker_keyboard(lang)
            )
            return
            
        keyboard_buttons = []
        message_text = get_text(lang, "bundle_view_title")
        for i, alert in enumerate(alerts, 1):
            addr   = alert['address_to_track']
            custom_name = alert.get('custom_name', '')
            display_name = f"{custom_name} ({addr[:6]}..{addr[-4:]})" if custom_name else addr
            window = alert['time_gap_min']
            cnt    = alert['min_cnt']
            diff   = alert['amount_step']
            amin   = alert['min_transfer_amount']
            amax   = alert['max_transfer_amount'] if alert['max_transfer_amount'] is not None else "∞"
            message_text += (
                f"{i}. {display_name}\n"
                f"   • {window} min   |   ≥{cnt} tx\n"
                f"   • Δ≤{diff} SOL   |   {amin} – {amax} SOL\n\n"
            )
            keyboard_buttons.append([
                InlineKeyboardButton(
                    f"{get_text(lang, 'bundle_edit_btn')} {addr[:6]}…{addr[-4:]}",
                    callback_data=f"bundle_edit_{addr}"
                ),
                InlineKeyboardButton(
                    f"{get_text(lang, 'bundle_delete_btn')} {addr[:6]}…{addr[-4:]}",
                    callback_data=f"bundle_delete_{addr}"
                )
            ])
        keyboard_buttons.append([InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="bundle_back_to_main")])
        await query.edit_message_text(text=message_text, reply_markup=InlineKeyboardMarkup(keyboard_buttons), parse_mode='Markdown')
    except Exception as e:
        print(f"ERROR in show_user_bundle_alerts: {e}")
        await query.edit_message_text(get_text(lang, "error_occurred"))


async def bundle_tracker_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает нажатия на кнопки в меню 'Bundle Tracker'.
    """
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')
    user_id = update.effective_user.id
    command = query.data
    
    if command == "bundle_add":
        try:
            # ИСПОЛЬЗУЕМ СЕРВИС для подсчета
            count = await supabase_service.count_user_bundle_alerts(user_id)
            if count >= MAX_TRACKING_TASKS_PER_USER:
                await query.edit_message_text(get_text(lang, "bundle_add_limit_reached", MAX_TRACKING_TASKS_PER_USER), parse_mode='Markdown')
                return

            context.user_data['bundle_tracker_data'] = {}
            context.user_data['state'] = 'awaiting_bundle_address'
            await query.edit_message_text(get_text(lang, "bundle_add_step1_address"), parse_mode='Markdown')
            context.user_data['main_message_id'] = query.message.message_id
        except Exception as e:
            print(f"ERROR in bundle_tracker_callback (bundle_add): {e}")
            await query.edit_message_text(get_text(lang, "error_occurred"))

    elif command == "bundle_view":
        # Вызываем нашу обновленную функцию-хелпер
        await show_user_bundle_alerts(update, context)

    elif command.startswith("bundle_edit_"):
        # Логика для редактирования. Она в основном управляет состоянием,
        # а сохранение произойдет в handle_text, который тоже будет использовать сервис.
        address = command.replace("bundle_edit_", "")
        # Здесь можно было бы получить данные через сервис, но код и так уже есть в старой версии
        # ... (остальная логика без изменений) ...
        pass # Оставим как есть, т.к. основная работа в `messages.py`

    elif command.startswith("bundle_delete_"):
        address_to_delete = command.replace("bundle_delete_", "")
        # ИСПОЛЬЗУЕМ СЕРВИС
        success = await supabase_service.delete_bundle_alert(user_id, address_to_delete)
        if success:
            # После успешного удаления обновляем список
            await show_user_bundle_alerts(update, context)
        else:
            await query.edit_message_text(get_text(lang, "error_occurred"))

    elif command == "bundle_back_to_main":
        # Плавный возврат в главное меню
        await query.message.edit_text(
            text=get_text(lang, "main_menu_message"),
            reply_markup=get_main_menu_inline_keyboard(lang, context.user_data.get("premium", False)),
            parse_mode="Markdown"
        )
        
async def dev_stats_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')
    parts = query.data.split("_", 3)
    if len(parts) < 4:
        return
    _, _, choice, address = parts
    # Handle bulk actions
    if address == "bulk":
        addresses = context.user_data.get('dev_addresses', [])
        if not addresses:
            await query.edit_message_text(get_text(lang, "input_empty_error"))
            return
        if choice == "main":
            await send_bulk_dev_csv(addresses, lang, update, context)
        elif choice == "tokens":
            await send_bulk_dev_tokens_csv(addresses, lang, update)
        elif choice == "all":
            await send_bulk_dev_all_csv(addresses, lang, update)
        return
    if choice == "main":
        await send_dev_main_stats(address, lang, update)
    elif choice == "tokens":
        await send_dev_tokens_csv(address, lang, update)
    elif choice == "all":
        await send_dev_main_stats(address, lang, update)
        await send_dev_tokens_csv(address, lang, update)

#
# =================================================================================
#  Раздел 3: Функции-исполнители бизнес-логики
# =================================================================================
#

async def execute_token_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполняет поиск токенов по заданным критериям и отправляет CSV."""
    query = update.callback_query
    lang = context.user_data.get('lang', 'en')
    main_msg_id = context.user_data.get("main_message_id", query.message.message_id)
    chat_id = query.message.chat_id

    await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "executing_parse"), disable_web_page_preview=True)
    try:
        ud = context.user_data
        selected_platforms = ud.get('token_parse_platforms', [])
        period_key = ud.get('token_parse_period', '24h')
        selected_categories = ud.get('token_parse_categories', [])
        
        hours = int(period_key.replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        sql_query = supabase.table("tokens").select("contract_address, ticker, name, migration_time, launchpad, category")
        sql_query = sql_query.gte("migration_time", start_time.isoformat())
        if selected_platforms:
            sql_query = sql_query.in_("launchpad", selected_platforms)
        if selected_categories and set(selected_categories) != set(TOKEN_CATEGORIES):
            sql_query = sql_query.in_("category", selected_categories)
            
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: sql_query.range(0, 10000).execute())
        df = pd.DataFrame(response.data or [])

        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(TRANSLATIONS[lang]["back_btn"], callback_data="parse_back")]])

        if df.empty:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "no_tokens_found"), reply_markup=back_button_markup, disable_web_page_preview=True)
            return
            
        output = io.StringIO()
        fieldnames = ["contract_address", "ticker", "name", "migration_time", "launchpad", "category"]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(df.to_dict(orient='records'))
        
        csv_file = io.BytesIO(output.getvalue().encode('utf-8'))
        csv_file.name = f"tokens_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        media_to_upload = InputMediaDocument(media=csv_file_bytes, caption=caption)
        await query.message.edit_media(media=media_to_upload, reply_markup=back_button_markup)
    except Exception as e:
        # logger.error(...)
        await context.bot.edit_message_text(chat_id=chat_id, message_id=main_msg_id, text=get_text(lang, "error_occurred"), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(TRANSLATIONS[lang]["back_btn"], callback_data="parse_back")]]), disable_web_page_preview=True)

async def run_all_in_parse_pipeline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = context.user_data.get('lang', 'en')
    chat_id = update.effective_chat.id
    main_msg_id = context.user_data.get("main_message_id")
    from telegram import InputMediaDocument

    try:
        selected_template = context.user_data.get('selected_template', {})
        if not selected_template or not isinstance(selected_template, dict) or 'id' not in selected_template:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text="Шаблон не выбран или поврежден." if lang == "ru" else "No template selected or template is corrupted.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back" if lang == "ru" else "⬅️ Back", callback_data="parse_back")]
                ]), disable_web_page_preview=True
            )
            return

        # Получаем данные шаблона из базы, если их нет или они некорректны
        user_id = update.effective_user.id
        if not selected_template.get('platforms') or not selected_template.get('time_period') or not selected_template.get('categories'):
            templates = await fetch_user_templates(user_id)
            selected_template = next((t for t in templates if t["id"] == selected_template['id']), {})
            if not selected_template:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=main_msg_id,
                    text="Шаблон не найден в базе." if lang == "ru" else "Template not found in database.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Back" if lang == "ru" else "⬅️ Back", callback_data="main_menu")]
                    ]), disable_web_page_preview=True
                )
                return
            context.user_data['selected_template'] = selected_template

        platforms = selected_template.get('platforms', [])
        time_period = selected_template.get('time_period', '24h')
        categories = selected_template.get('categories', TOKEN_CATEGORIES)

        hours = int(time_period.replace('h', ''))
        logger.info(f"ALL-IN-PARSE: Fetching tokens with platforms={platforms}, time_period={time_period}, categories={categories}...")
        tokens_data_from_api = await fetch_tokens(time_window_hours=hours)

        if not tokens_data_from_api:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text="🤷 За указанный период токены не найдены." if lang == "ru" else "🤷 No tokens found for the specified period.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back" if lang == "ru" else "⬅️ Back", callback_data="main_menu")]
                ]), disable_web_page_preview=True
            )
            return

        # Фильтрация токенов по платформам и категориям
        filtered_tokens = [
            t for t in tokens_data_from_api
            if (not platforms or t['launchpad'] in platforms) and (not categories or t['category'] in categories)
        ]

        if not filtered_tokens:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text="🤷 По заданным критериям токены не найдены." if lang == "ru" else "🤷 No tokens found for the selected criteria.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад" if lang == "ru" else "⬅️ Back", callback_data="main_menu")]
                ]), disable_web_page_preview=True
            )
            return

        response_with_ids = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table('tokens')
                           .select('id, contract_address, category')
                           .in_('contract_address', [t['contract_address'] for t in filtered_tokens])
                           .execute()
        )
        tokens_with_ids = response_with_ids.data

        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "all_in_step1_done"), disable_web_page_preview=True
        )
        logger.info(f"ALL-IN-PARSE: Step 2: Fetching traders for {len(tokens_with_ids)} tokens...")
        await process_tokens_for_traders(tokens_with_ids)
        token_ids = [t['id'] for t in tokens_with_ids]
        traders_response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table("traders").select("trader_address").in_("token_id", token_ids).execute()
        )
        if not traders_response.data:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text=get_text(lang, "all_in_no_traders"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(TRANSLATIONS[lang]["back_btn"], callback_data="main_menu")]
                ]), disable_web_page_preview=True
            )
            return

        unique_trader_addresses = list(set(item['trader_address'] for item in traders_response.data))
        logger.info(f"ALL-IN-PARSE: Found {len(unique_trader_addresses)} unique traders.")
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "all_in_step2_done"), disable_web_page_preview=True
        )
        logger.info(f"ALL-IN-PARSE: Step 3: Fetching PNL for {len(unique_trader_addresses)} traders...")
        csv_path = await fetch_pnl_via_discord(unique_trader_addresses)

        if csv_path and os.path.exists(csv_path):
            with open(csv_path, 'rb') as csv_file:
                await context.bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=main_msg_id,
                    media=InputMediaDocument(media=csv_file, caption=get_text(lang, "all_in_final_caption")),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(TRANSLATIONS[lang]["back_btn"], callback_data="main_menu")]
                    ])
                )
            logger.info(f"ALL-IN-PARSE: Final report sent to chat {chat_id}")
            os.remove(csv_path)
        else:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text=get_text(lang, "error_occurred"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(TRANSLATIONS[lang]["back_btn"], callback_data="main_menu")]
                ]), disable_web_page_preview=True
            )
    except Exception as e:
        logger.error(f"ALL-IN-PARSE: A critical error occurred in the pipeline: {e}", exc_info=True)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "error_occurred"),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(TRANSLATIONS[lang]["back_btn"], callback_data="main_menu")]
            ]), disable_web_page_preview=True        )
async def dev_pnl_filter_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает всю навигацию внутри меню PNL-фильтров для Dev Parse.
    """
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')
    command = query.data
    ud = context.user_data

    # --- Навигация ---
    if command == "dev_pnl_filter_back_to_settings":
        # Возврат в главное меню настроек Dev Parse
        context.user_data['state'] = None # Сбрасываем под-состояние
        reply_markup = get_dev_parse_settings_keyboard(lang, context)
        await query.message.edit_text(get_text(lang, "dev_parse_menu_prompt"), reply_markup=reply_markup)
        return

    if command == "dev_pnl_filter_back_to_main":
        # Возврат к списку категорий PNL-фильтров
        pnl_filters = ud.get('dev_pnl_filters', {})
        text = "📊 **Фильтры PNL для разработчиков**\n\nВыберите категорию для настройки.\n\n**Текущие фильтры:**\n"
        if not pnl_filters:
            text += "_Пусто_"
        else:
            for col, val in pnl_filters.items():
                min_v = val.get('min', '-∞')
                max_v = val.get('max', '+∞')
                text += f"- `{col.replace('_', ' ')}`: от `{min_v}` до `{max_v}`\n"
        
        reply_markup = get_dev_pnl_filter_main_menu_keyboard()
        await query.message.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        return

    # --- Выбор категории фильтра ---
    if command.startswith("dev_pnl_filter_cat_"):
        category_name = command.replace("dev_pnl_filter_cat_", "")
        reply_markup = get_dev_pnl_filter_submenu_keyboard(category_name)
        await query.message.edit_text(f"Выберите метрику из категории '{category_name}':", reply_markup=reply_markup)
        return

    # --- Выбор конкретной колонки для настройки ---
    if command.startswith("dev_pnl_filter_col_"):
        column_name = command.replace("dev_pnl_filter_col_", "")
        context.user_data['dev_pnl_filter_to_set'] = column_name # Запоминаем, какую колонку настраиваем
        context.user_data['state'] = 'awaiting_dev_pnl_filter_value' # Переходим в состояние ожидания текста
        
        await query.message.edit_text(
            f"Введите мин. и макс. значения для `{column_name}` через пробел (например, `50 100`).\n\n"
            "Отправьте одно число для минимума или `0` для сброса."
        )
        return

    # --- Сброс всех PNL-фильтров ---
    if command == "dev_pnl_filter_reset_all":
        if 'dev_pnl_filters' in ud:
            ud.pop('dev_pnl_filters')
        await query.message.edit_text("Все PNL-фильтры для разработчиков сброшены.", reply_markup=get_dev_pnl_filter_main_menu_keyboard())
        
async def dev_parse_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает все кнопки в меню Dev Parse."""
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')
    command = query.data
    ud = context.user_data

    # --- Устанавливаем правильные состояния перед переходом в подменю ---
    if command == "devparse_platforms":
        context.user_data['state'] = 'dev_parse_editing_platforms'
        all_platforms = await supabase_service.fetch_unique_launchpads()
        reply_markup = get_platform_selection_keyboard(lang, all_platforms, ud.get('dev_parse_platforms', []))
        await query.message.edit_text(
            get_text(lang, "dev_parse_platform_prompt"), # <-- ИЗМЕНЕНО
            reply_markup=reply_markup
        )

    elif command == "devparse_category":
        context.user_data['state'] = 'dev_parse_editing_categories'
        reply_markup = get_category_selection_keyboard(lang, ud.get('dev_parse_categories', []), context)
        await query.message.edit_text(get_text(lang, "category_prompt"), reply_markup=reply_markup)

    elif command == "devparse_period":
        context.user_data['state'] = 'dev_parse_editing_period'
        current_period = ud.get('dev_parse_period', '72h')
        reply_markup = get_dev_parse_period_keyboard(lang, current_period)
        await query.message.edit_text(get_text(lang, "time_period_prompt"), reply_markup=reply_markup)
    
    elif command.startswith("devparse_period_select_"):
        selected_period = command.replace("devparse_period_select_", "")
        ud['dev_parse_period'] = selected_period
        await query.message.edit_reply_markup(reply_markup=get_dev_parse_period_keyboard(lang, selected_period))
        
    elif command == "devparse_pnl_filters":
        # Получаем текущие фильтры из памяти
        pnl_filters = ud.get('dev_pnl_filters', {})
        
        # Формируем красивое сообщение с текущими фильтрами
        text = "📊 **Фильтры PNL для разработчиков**\n\nВыберите категорию для настройки.\n\n**Текущие фильтры:**\n"
        if not pnl_filters:
            text += "_Пусто_"
        else:
            for col, val in pnl_filters.items():
                min_v = val.get('min', '-∞')
                max_v = val.get('max', '+∞')
                # Экранируем символы для Markdown
                escaped_col = col.replace('_', '\\_')
                text += f"- `{escaped_col}`: от `{min_v}` до `{max_v}`\n"
        
        reply_markup = get_dev_pnl_filter_main_menu_keyboard()
        await query.message.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        
    elif command == "devparse_period_done":
        context.user_data['state'] = None # Сбрасываем под-состояние
        reply_markup = get_dev_parse_settings_keyboard(lang, context)
        await query.message.edit_text(get_text(lang, "dev_parse_menu_prompt"), reply_markup=reply_markup)

    elif command == "devparse_execute":
        await query.message.edit_text("🔍 Выполняю поиск и фильтрацию...")
        
        hours = int(ud.get('dev_parse_period', '72h').replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        initial_dev_stats = await supabase_service.fetch_dev_stats_by_criteria(
            start_time, ud.get('dev_parse_platforms', []), ud.get('dev_parse_categories', [])
        )

        if not initial_dev_stats:
            await query.message.edit_text("🤷 По вашим критериям токенов разработчики не найдены.")
            return

        pnl_filters = ud.get('dev_pnl_filters', {})
        final_dev_stats = apply_dev_pnl_filters(initial_dev_stats, pnl_filters)
        
        if not final_dev_stats:
            await query.message.edit_text("🤷 Разработчики, соответствующие PNL-фильтрам, не найдены.")
            return

        chat_id = update.effective_chat.id

        # 1. Отправляем первый файл (PNL)
        pnl_output = io.StringIO()
        pd.DataFrame(final_dev_stats).to_csv(pnl_output, index=False)
        pnl_csv_bytes = io.BytesIO(pnl_output.getvalue().encode('utf-8'))
        pnl_csv_bytes.name = "dev_pnl_stats_filtered.csv"
        await context.bot.send_document(
            chat_id=chat_id,
            document=pnl_csv_bytes,
            caption=f"✅ Ваш PNL-отчет по разработчикам готов. Найдено (после всех фильтров): {len(final_dev_stats)} девов."
        )

        # Сообщение о статусе перед отправкой второго файла
        await query.message.edit_text("⚙️ Загружаю список токенов для отфильтрованных разработчиков...")
        
        # 2. Отправляем второй файл (токены)
        developer_addresses = [dev['developer_address'] for dev in final_dev_stats]
        deployed_tokens = await supabase_service.fetch_deployed_tokens_for_devs(developer_addresses)
        
        if deployed_tokens:
            tokens_output = io.StringIO()
            pd.DataFrame(deployed_tokens).to_csv(tokens_output, index=False)
            tokens_csv_bytes = io.BytesIO(tokens_output.getvalue().encode('utf-8'))
            tokens_csv_bytes.name = "dev_deployed_tokens_filtered.csv"
            await context.bot.send_document(
                chat_id=chat_id,
                document=tokens_csv_bytes,
                caption=f"✅ Список из {len(deployed_tokens)} токенов, созданных отфильтрованными разработчиками."
            )
        
        # 3. 🔥 ГЛАВНОЕ ИЗМЕНЕНИЕ: Отправляем новое главное меню вниз
        await send_new_main_menu(context.bot, chat_id, context)
                
async def pnl_filter_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает всю навигацию внутри меню PNL-фильтров.
    """
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'en')
    command = query.data
    template_data = context.user_data.get('template_data')

    if not template_data:
        await query.message.edit_text("Ошибка: данные шаблона потеряны. Пожалуйста, начните заново.")
        return

    # --- Навигация ---
    elif command == "pnl_filter_back_to_template":
        # Возврат в главное меню настроек шаблона
        context.user_data['state'] = 'awaiting_template_settings'
        reply_markup = get_template_settings_keyboard(lang, template_data)
        await query.message.edit_text(f"Настраиваем шаблон '{template_data['template_name']}':", reply_markup=reply_markup)
        return

    elif command == "pnl_filter_back_to_main":
        # Возврат к списку категорий PNL-фильтров
        pnl_filters = template_data.get('pnl_filters', {})
        text = "📊 **PNL-фильтры**\n\nВыберите категорию для настройки.\n\n**Текущие фильтры:**\n"
        if not pnl_filters:
            text += "_Пусто_"
        else:
            for col, val in pnl_filters.items():
                text += f"- `{col}`: от `{val['min']}` до `{val['max']}`\n"
        
        reply_markup = get_pnl_filter_main_menu_keyboard(template_data)
        await query.message.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        return

    # --- Выбор категории фильтра ---
    elif command.startswith("pnl_filter_cat_"):
        category_name = command.replace("pnl_filter_cat_", "")
        reply_markup = get_pnl_filter_submenu_keyboard(category_name)
        await query.message.edit_text(f"Выберите метрику из категории '{category_name}':", reply_markup=reply_markup)
        return

    # --- Выбор конкретной колонки для настройки ---
    elif command.startswith("pnl_filter_col_"):
        column_name = command.replace("pnl_filter_col_", "")
        context.user_data['pnl_filter_to_set'] = column_name # Запоминаем, какую колонку настраиваем
        context.user_data['state'] = 'awaiting_pnl_filter_value' # Переходим в состояние ожидания текста
        
        await query.message.edit_text(
            f"Введите минимальное и максимальное значение для `{column_name}` через пробел (например, `50 100`).\n\n"
            "Отправьте одно число, чтобы задать только минимум (например, `500`).\n"
            "Отправьте `0`, чтобы сбросить фильтр для этой колонки."
        )
        return

    # --- Сброс всех PNL-фильтров ---
    elif command == "pnl_filter_reset_all":
        if 'pnl_filters' in template_data:
            template_data.pop('pnl_filters')
        await query.message.edit_text(get_text(lang, "pnl_filter_reset_all"), reply_markup=get_pnl_filter_main_menu_keyboard(template_data))
        
# New handler for settings submenus
async def settings_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_user_lang(context)
    action = query.data

    if action == "settings_language":
        reply_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🇷🇺 Русский", callback_data="setlang_ru"),
                InlineKeyboardButton("🇬🇧 English", callback_data="setlang_en")
            ],
            [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
        ])
        await query.message.edit_text(get_text(lang, "language_select_prompt"), reply_markup=reply_markup)
        
async def language_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_user_lang(context)
    reply_markup = get_language_keyboard()
    await query.message.edit_text(get_text(lang, "choose_language_prompt"), reply_markup=reply_markup)