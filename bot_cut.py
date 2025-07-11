from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from dotenv import load_dotenv
import os
import logging
import io
import csv
import asyncio
from datetime import datetime, timezone, timedelta
from supabase_client import supabase
import secrets
import string

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_CUT_TOKEN")

# Константы
TIME_PERIODS = {'1h': '1 час', '3h': '3 часа', '6h': '6 часов', '12h': '12 часов', '24h': '24 часа'}
TOKEN_CATEGORIES = ['new_creation', 'completed', 'completing']
MAX_ADDRESS_LIST_SIZE = 40000
sem = asyncio.Semaphore(5)

# Переводы
TRANSLATIONS = {
    "ru": {
        "welcome": "Введите секретный код для доступа к боту:",
        "invalid_code": "❌ Неверный или уже использованный код. Попробуйте снова.",
        "code_accepted": "✅ Код принят! Добро пожаловать!\n\n*Это главное меню. Используйте кнопки для доступа к функциям.*",
        "parse_menu_prompt": "Выберите, что бы вы хотели спарсить:",
        "get_tokens_prompt": "Выберите настройки для парсинга монет:",
        "platforms_menu_prompt": "Выберите одну или несколько платформ. Нажмите еще раз, чтобы убрать выбор. Когда закончите, нажмите 'Назад'.",
        "time_period_prompt": "Выберите период времени:",
        "category_prompt": "Выберите категорию токенов:",
        "get_traders_prompt": "Пожалуйста, отправьте список адресов контрактов:\n- одним сообщением\n- каждый с новой строки\n- без лишних символов\n- как обычный текст или .txt файл.",
        "trader_list_received": "✅ Список получен. Начинаю поиск топ-100 трейдеров для каждого токена...",
        "executing_parse": "🚀 Запускаю парсинг с вашими настройками... Формирую отчет.",
        "no_tokens_found": "🤷 По заданным критериям токены не найдены.",
        "traders_txt_caption": "✅ Ваш отчет по трейдерам готов.\n\nВсего найдено трейдеров: *{}*.\nОбработано токенов: *{}*.\nФайл содержит *{}* уникальных адресов.",
        "csv_caption": "✅ Ваш отчет готов. Найдено {} токенов.",
        "parse_btn": "🔍 Parse",
        "all_in_parse_btn": "All-in parse",
        "get_tokens_btn": "Get tokens",
        "get_top_traders_btn": "Get top traders",
        "get_wallet_stats_btn": "Get Wallet Stats",
        "platforms_btn": "Platforms ({})",
        "category_btn": "Category ({})",
        "time_period_btn": "Time Period ({})",
        "parse_now_btn": "✅ Parse",
        "back_btn": "⬅️ Назад",
        "error_occurred": "Произошла ошибка при обработке запроса. Попробуйте позже.",
        "input_empty_error": "Вы отправили пустое сообщение. Пожалуйста, отправьте список адресов.",
        "input_list_too_long_error": "Вы отправили слишком большой список. Пожалуйста, ограничьте ваш запрос до {} адресов за один раз.",
        "input_address_length_error": "❗️ **Ошибка формата**\n\nНекоторые строки в вашем списке не похожи на адреса кошельков. Адрес должен содержать от 32 до 44 символов.\n\nПроблемные строки:\n{}",
        "token_lookup_failed": "⚠️ Не удалось найти информацию по токену {}. Возможно, адрес некорректен или он еще не появился в базе.",
        "generic_fail_summary": "Не удалось найти трейдеров ни для одного из указанных токенов.",
    }
}

# Генерация секретных кодов
def generate_secret_codes(num_codes: int) -> list:
    chars = string.ascii_letters + string.digits
    return [''.join(secrets.choice(chars) for _ in range(12)) for _ in range(num_codes)]

# Инициализация секретных кодов в Supabase
def initialize_secret_codes():
    try:
        # Проверяем, есть ли уже коды в таблице
        response = supabase.table("secret_codes").select("code").execute()
        existing_codes = response.data
        if existing_codes and len(existing_codes) >= 25:
            logger.info("25 кодов уже существуют в базе.")
            test_codes = [code["code"] for code in supabase.table("secret_codes").select("code").eq("is_test", True).execute().data]
            logger.info("Тестовые коды: %s", test_codes)
            return

        # Генерируем 25 кодов (5 тестовых, 20 обычных)
        codes = generate_secret_codes(25)
        code_entries = [{"code": code, "is_used": False, "is_test": i < 5} for i, code in enumerate(codes)]
        supabase.table("secret_codes").insert(code_entries).execute()
        logger.info("25 секретных кодов успешно сохранены в Supabase (5 тестовых).")
        
        # Выводим тестовые коды для разработчика
        test_codes = [code["code"] for code in code_entries if code["is_test"]]
        logger.info("Тестовые коды: %s", test_codes)
    except Exception as e:
        logger.error(f"Ошибка при создании секретных кодов: {e}")
        raise

# Проверка и активация кода
async def validate_and_activate_code(code: str, user_id: int, chat_id: int) -> bool:
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table("secret_codes")
                            .select("code, is_used")
                            .eq("code", code)
                            .limit(1)
                            .execute()
        )
        if not response.data or response.data[0]["is_used"]:
            return False

        # Отметить код как использованный и сохранить user_id, chat_id
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table("secret_codes")
                            .update({"is_used": True, "user_id": user_id, "chat_id": chat_id})
                            .eq("code", code)
                            .execute()
        )
        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке кода {code}: {e}")
        return False

# Проверка доступа пользователя
async def has_access(user_id: int) -> bool:
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table("secret_codes")
                            .select("user_id")
                            .eq("user_id", user_id)
                            .eq("is_used", True)
                            .limit(1)
                            .execute()
        )
        return bool(response.data)
    except Exception as e:
        logger.error(f"Ошибка при проверке доступа для user_id {user_id}: {e}")
        return False

# Вспомогательные функции
def get_text(lang_code: str, key: str, *args):
    text = TRANSLATIONS.get(lang_code, TRANSLATIONS["ru"]).get(key, f"<{key}_NOT_FOUND>")
    return text.format(*args) if args else text

def get_main_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text(lang, "parse_btn"), callback_data="mainmenu_parse")]
    ])

def get_parse_submenu_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text(lang, "all_in_parse_btn"), callback_data="parse_all_in")],
        [InlineKeyboardButton(get_text(lang, "get_tokens_btn"), callback_data="parse_get_tokens")],
        [InlineKeyboardButton(get_text(lang, "get_top_traders_btn"), callback_data="parse_get_traders")],
        [InlineKeyboardButton(get_text(lang, "get_wallet_stats_btn"), callback_data="parse_get_stats")],
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]
    ])

async def fetch_unique_launchpads() -> list:
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table("tokens").select("launchpad").execute()
        )
        if response.data:
            launchpads = sorted(list(set(item['launchpad'] for item in response.data if item['launchpad'] and item['launchpad'] != 'unknown')))
            return launchpads
        return []
    except Exception as e:
        logger.error(f"Ошибка при получении списка лаунчпадов: {e}")
        return []

def get_token_parse_settings_keyboard(lang: str, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    ud = context.user_data
    selected_platforms_count = len(ud.get('token_parse_platforms', []))
    platforms_text = get_text(lang, "platforms_btn", selected_platforms_count if selected_platforms_count > 0 else 'All')
    selected_categories = ud.get('token_parse_categories', [])
    category_text = get_text(lang, "category_btn", len(selected_categories))
    selected_period = ud.get('token_parse_period', '24h')
    period_text = get_text(lang, "time_period_btn", selected_period)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(platforms_text, callback_data="tokensettings_platforms")],
        [InlineKeyboardButton(category_text, callback_data="tokensettings_category")],
        [InlineKeyboardButton(period_text, callback_data="tokensettings_period")],
        [InlineKeyboardButton(get_text(lang, "parse_now_btn"), callback_data="tokensettings_execute")],
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
    ])

def get_platform_selection_keyboard(lang: str, all_platforms: list, selected_platforms: list) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    for platform in all_platforms:
        text = f"✅ {platform}" if platform in selected_platforms else f"❌ {platform}"
        row.append(InlineKeyboardButton(text, callback_data=f"platform_toggle_{platform}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="platform_done")])
    return InlineKeyboardMarkup(keyboard)

def get_period_selection_keyboard(lang: str, current_period: str) -> InlineKeyboardMarkup:
    keyboard = []
    for period_key, period_text_ru in TIME_PERIODS.items():
        text = f"✅ {period_text_ru}" if period_key == current_period else period_text_ru
        keyboard.append([InlineKeyboardButton(text, callback_data=f"period_select_{period_key}")])
    keyboard.append([InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="period_done")])
    return InlineKeyboardMarkup(keyboard)

def get_category_selection_keyboard(lang: str, selected_categories: list) -> InlineKeyboardMarkup:
    keyboard = []
    for category in TOKEN_CATEGORIES:
        text = f"✅ {category}" if category in selected_categories else f"❌ {category}"
        keyboard.append([InlineKeyboardButton(text, callback_data=f"category_toggle_{category}")])
    keyboard.append([InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="category_done")])
    return InlineKeyboardMarkup(keyboard)

# Обработчики Parse
async def search_by_token_contract(contract_address: str, lang: str, update: Update, return_list=False):
    for attempt in range(3):
        try:
            loop = asyncio.get_event_loop()
            token_response = await loop.run_in_executor(None,
                lambda: supabase.table("tokens")
                                 .select("id, name, traders_last_fetched_at")
                                 .eq("contract_address", contract_address)
                                 .limit(1)
                                 .execute()
            )
            if not token_response.data:
                if not return_list:
                    await update.message.reply_text(
                        f"⏳ Трейдеры для {contract_address} не найдены в базе. Ищу в реальном времени..."
                    )
                insert_resp = await asyncio.to_thread(lambda:
                    supabase.table("tokens")
                            .insert({"contract_address": contract_address})
                            .execute()
                )
                if not insert_resp.data:
                    return get_text(lang, "token_lookup_failed").format(contract_address) if not return_list else []
                
                token_response = await loop.run_in_executor(None,
                    lambda: supabase.table("tokens")
                                     .select("id, name, traders_last_fetched_at")
                                     .eq("contract_address", contract_address)
                                     .limit(1)
                                     .execute()
                )
                if not token_response.data:
                    return get_text(lang, "token_lookup_failed").format(contract_address) if not return_list else []
                token_data = token_response.data[0]
            else:
                token_data = token_response.data[0]
            token_id = token_data['id']
            token_name = token_data.get('name', contract_address)
            traders_fetched_at = token_data.get('traders_last_fetched_at')

            if traders_fetched_at is None or return_list:
                if not return_list:
                    await update.message.reply_text(f"⏳ Трейдеры для {token_name} еще не были собраны. Запускаю поиск в реальном времени...")
                token_info_for_fetcher = {"id": token_id, "contract_address": contract_address}
                # Имитация вызова fetcher-а (замените на вашу функцию)
                traders_response = await loop.run_in_executor(None,
                    lambda: supabase.table("traders").select("trader_address").eq("token_id", token_id).limit(100).execute()
                )
                if not traders_response.data:
                    message = f"Токен {token_name} найден, но активных трейдеров пока не обнаружено.\n"
                    message += "💡 Возможно, токен новый или еще не имеет торговой активности."
                    return message if not return_list else []

            trader_addresses = [item['trader_address'] for item in traders_response.data]
            if return_list:
                return trader_addresses

            response_text = get_text(lang, "top_traders_title").format(len(trader_addresses), token_name)
            for i, addr in enumerate(trader_addresses, 1):
                response_text += f"`{i}. {addr}`\n"
            return response_text
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Retry attempts failed for contract {contract_address}: {e}", exc_info=True)
            return [] if return_list else get_text(lang, "error_occurred")

async def execute_token_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import InputMediaDocument
    lang = context.user_data.get('lang', 'ru')
    ud = context.user_data
    query = update.callback_query
    main_msg_id = context.user_data.get("main_message_id", query.message.message_id)
    await context.bot.edit_message_text(
        chat_id=query.message.chat_id,
        message_id=main_msg_id,
        text=get_text(lang, "executing_parse")
    )
    try:
        selected_platforms = ud.get('token_parse_platforms', [])
        period_key = ud.get('token_parse_period', '24h')
        selected_categories = ud.get('token_parse_categories', [])
        hours = int(period_key.replace('h', ''))
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        sql_query = supabase.table("tokens").select("contract_address, ticker, name, migration_time, launchpad, category")
        if selected_platforms:
            sql_query = sql_query.in_("launchpad", selected_platforms)
        if selected_categories and set(selected_categories) != set(TOKEN_CATEGORIES):
            sql_query = sql_query.in_("category", selected_categories)
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: sql_query.range(0, 100000).execute())
        import pandas as pd
        df = pd.DataFrame(response.data or [])
        if selected_categories and set(selected_categories) != set(TOKEN_CATEGORIES):
            df = df[df['category'].isin(selected_categories)]
        if df.empty:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=get_text(lang, "no_tokens_found"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                ])
            )
            return
        output = io.StringIO()
        fieldnames = ["contract_address", "ticker", "name", "migration_time", "launchpad", "category"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(df.to_dict(orient='records'))
        csv_file = io.BytesIO(output.getvalue().encode('utf-8'))
        csv_file.name = f"tokens_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        await context.bot.edit_message_media(
            chat_id=query.message.chat_id,
            message_id=main_msg_id,
            media=InputMediaDocument(
                media=csv_file,
                caption=get_text(lang, "csv_caption", len(df))
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
            ])
        )
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсинга токенов: {e}", exc_info=True)
        await context.bot.edit_message_text(
            chat_id=query.message.chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "error_occurred"),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
            ])
        )

async def process_trader_list(addresses: list, update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import InputMediaDocument
    lang = context.user_data.get('lang', 'ru')
    main_msg_id = context.user_data.get("main_message_id")
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=main_msg_id,
        text=get_text(lang, "trader_list_received")
    )
    tasks = [search_by_token_contract(addr, lang, update, return_list=True) for addr in addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    lines = []
    processed_count = 0

    for addr, result in zip(addresses, results):
        lines.append(f"Token: {addr}")
        if isinstance(result, list) and result:
            processed_count += 1
            for i, trader_addr in enumerate(result, 1):
                lines.append(f"{i}. {trader_addr}")
        else:
            lines.append("  — нет данных по трейдерам")
        lines.append("")

    errors_only = [
        get_text(lang, "token_lookup_failed", f"`{addr}`")
        for addr, result in zip(addresses, results)
        if not (isinstance(result, list) and result)
    ]
    if errors_only:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=main_msg_id,
            text="\n".join(errors_only),
            parse_mode='Markdown'
        )

    output = io.StringIO()
    output.write("\n".join(lines))
    txt_file = io.BytesIO(output.getvalue().encode('utf-8'))
    txt_file.name = f"top_traders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    total_traders = sum(len(r) for r in results if isinstance(r, list))
    caption = get_text(lang, "traders_txt_caption", total_traders, processed_count, len(lines))
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
    ])
    await context.bot.edit_message_media(
        chat_id=update.effective_chat.id,
        message_id=main_msg_id,
        media=InputMediaDocument(media=txt_file, caption=caption, parse_mode="Markdown"),
        reply_markup=reply_markup
    )
    try:
        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
    except:
        pass

async def process_wallet_stats(addresses: list, update: Update, context: ContextTypes.DEFAULT_TYPE):
    import requests
    from telegram import InputMediaDocument
    lang = context.user_data.get('lang', 'ru')
    main_msg_id = context.user_data.get("main_message_id")
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=main_msg_id,
        text=get_text(lang, "pnl_fetch_started")
    )
    
    pnl_service_url = "http://127.0.0.1:5001/get_pnl"
    try:
        response = await asyncio.to_thread(requests.post, pnl_service_url, json={"wallets": addresses}, timeout=400)
        response.raise_for_status()
        data = response.json()
        csv_path = data.get("file_path")
        
        if csv_path and os.path.exists(csv_path):
            with open(csv_path, 'rb') as csv_file:
                await context.bot.edit_message_media(
                    chat_id=update.effective_chat.id,
                    message_id=main_msg_id,
                    media=InputMediaDocument(media=csv_file, caption=get_text(lang, "pnl_report_caption", len(addresses))),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                    ])
                )
            os.remove(csv_path)
        else:
            error_details = data.get("error", "Неизвестная ошибка от PNL сервиса.")
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=main_msg_id,
                text=f"{get_text(lang, 'error_occurred')}\nДетали: {error_details}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка при обработке PNL: {e}", exc_info=True)
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=main_msg_id,
            text=get_text(lang, "error_occurred"),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
            ])
        )

async def run_all_in_parse_pipeline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import InputMediaDocument
    import subprocess
    lang = context.user_data.get('lang', 'ru')
    chat_id = update.effective_chat.id
    main_msg_id = context.user_data.get("main_message_id")
    try:
        logger.info("ALL-IN-PARSE: Step 1: Fetching tokens...")
        tokens_data_from_api = await fetch_tokens(time_window_hours=24)
        if not tokens_data_from_api:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text=get_text(lang, "all_in_no_tokens"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                ])
            )
            return
        contract_addresses = [t['contract_address'] for t in tokens_data_from_api]
        response_with_ids = await asyncio.get_event_loop().run_in_executor(None,
            lambda: supabase.table('tokens').select('id, contract_address, category').in_('contract_address', contract_addresses).execute()
        )
        tokens_with_ids = response_with_ids.data
        if not tokens_with_ids:
            logger.error("ALL-IN-PARSE: Tokens were fetched from API, but not found in DB right after.")
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text=get_text(lang, "error_occurred"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                ])
            )
            return

        selected_categories = context.user_data.get('token_parse_categories')
        if selected_categories and set(selected_categories) != set(TOKEN_CATEGORIES):
            tokens_with_ids = [t for t in tokens_with_ids if t.get('category') in selected_categories]

        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "all_in_step1_done")
        )
        logger.info(f"ALL-IN-PARSE: Step 2: Fetching traders for {len(tokens_with_ids)} tokens...")
        await process_tokens_for_traders(tokens_with_ids)
        token_ids = [t['id'] for t in tokens_with_ids]
        traders_response = await asyncio.get_event_loop().run_in_executor(None,
            lambda: supabase.table("traders").select("trader_address").in_("token_id", token_ids).execute()
        )
        if not traders_response.data:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text=get_text(lang, "all_in_no_traders"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                ])
            )
            return
        unique_trader_addresses = list(set(item['trader_address'] for item in traders_response.data))
        logger.info(f"ALL-IN-PARSE: Found {len(unique_trader_addresses)} unique traders.")
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "all_in_step2_done")
        )
        logger.info(f"ALL-IN-PARSE: Step 3: Fetching PNL for {len(unique_trader_addresses)} traders...")
        command = ['python', 'get_trader_pnl.py'] + unique_trader_addresses
        result = await asyncio.to_thread(subprocess.run, command, text=True, capture_output=True, timeout=1800)
        if result.returncode == 0:
            output_lines = result.stdout.strip().split('\n')
            csv_path = None
            for line in output_lines:
                if line.startswith("Файл с PNL сохранен здесь:"):
                    csv_path = line.replace("Файл с PNL сохранен здесь: ", "").strip()
                    break
            if csv_path and os.path.exists(csv_path):
                with open(csv_path, 'rb') as csv_file:
                    await context.bot.edit_message_media(
                        chat_id=chat_id,
                        message_id=main_msg_id,
                        media=InputMediaDocument(media=csv_file, caption=get_text(lang, "all_in_final_caption")),
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                        ])
                    )
                logger.info(f"ALL-IN-PARSE: Final report sent to chat {chat_id}")
                os.remove(csv_path)
            else:
                error_msg = f"Не удалось найти путь к PNL файлу.\nВывод скрипта:\n{result.stdout}"
                logger.error(f"ALL-IN-PARSE: {error_msg}")
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=main_msg_id,
                    text=get_text(lang, "error_occurred") + "\n" + error_msg,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                    ])
                )
        else:
            logger.error(f"ALL-IN-PARSE: PNL script failed. Stderr: {result.stderr}")
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=main_msg_id,
                text=f"Ошибка на этапе сбора PNL: \n`{result.stderr}`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
                ])
            )
    except Exception as e:
        logger.error(f"ALL-IN-PARSE: A critical error occurred in the pipeline: {e}", exc_info=True)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=main_msg_id,
            text=get_text(lang, "error_occurred"),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
            ])
        )

# Обработчики команд
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if await has_access(user_id):
        lang = context.user_data.get("lang", "ru")
        await update.message.reply_text(
            text=get_text(lang, "code_accepted"),
            reply_markup=get_main_menu_keyboard(lang),
            parse_mode='Markdown'
        )
    else:
        context.user_data['state'] = 'awaiting_secret_code'
        await update.message.reply_text(get_text("ru", "welcome"))

async def main_menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    context.user_data["main_message_id"] = query.message.message_id
    await query.answer()
    lang = context.user_data.get("lang", "ru")
    action = query.data.replace("mainmenu_", "")

    if action == "parse":
        reply_markup = get_parse_submenu_keyboard(lang)
        await query.edit_message_text(
            get_text(lang, "parse_menu_prompt"),
            reply_markup=reply_markup,
        )

async def parse_submenu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'ru')
    command = query.data
    if command == "parse_all_in":
        await query.edit_message_text(
            text=get_text(lang, "all_in_parse_start"),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]]
            )
        )
        asyncio.create_task(run_all_in_parse_pipeline(update, context))
    elif command == "parse_get_tokens":
        all_platforms = await fetch_unique_launchpads()
        context.user_data['token_parse_platforms'] = all_platforms
        context.user_data['token_parse_period'] = '24h'
        context.user_data['token_parse_categories'] = TOKEN_CATEGORIES.copy()
        reply_markup = get_token_parse_settings_keyboard(lang, context)
        await query.edit_message_text(
            text=get_text(lang, "get_tokens_prompt"),
            reply_markup=reply_markup
        )
    elif command == "parse_get_traders":
        context.user_data['state'] = 'awaiting_trader_list'
        prompt_text = get_text(lang, "get_traders_prompt", MAX_ADDRESS_LIST_SIZE)
        await query.edit_message_text(
            text=prompt_text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]]
            )
        )
    elif command == "parse_get_stats":
        context.user_data['state'] = 'awaiting_wallet_stats'
        await query.edit_message_text(
            text=get_text(lang, "get_traders_prompt"),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]]
            )
        )
    elif command == "parse_back":
        await query.message.delete()
        await query.message.reply_text(
            text=get_text(lang, "code_accepted"),
            reply_markup=get_main_menu_keyboard(lang),
            parse_mode='Markdown'
        )

async def token_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'ru')
    command = query.data
    if command == "tokensettings_platforms":
        all_platforms = await fetch_unique_launchpads()
        selected_platforms = context.user_data.get('token_parse_platforms', [])
        reply_markup = get_platform_selection_keyboard(lang, all_platforms, selected_platforms)
        await query.edit_message_text(text=get_text(lang, "platforms_menu_prompt"), reply_markup=reply_markup)
    elif command == "tokensettings_category":
        selected_categories = context.user_data.get('token_parse_categories', [])
        reply_markup = get_category_selection_keyboard(lang, selected_categories)
        await query.edit_message_text(text=get_text(lang, "category_prompt"), reply_markup=reply_markup)
    elif command == "tokensettings_period":
        current_period = context.user_data.get('token_parse_period', '24h')
        reply_markup = get_period_selection_keyboard(lang, current_period)
        await query.edit_message_text(text=get_text(lang, "time_period_prompt"), reply_markup=reply_markup)
    elif command == "tokensettings_execute":
        await execute_token_parse(update, context)
    elif command == "main_menu":
        await query.message.delete()
        await query.message.reply_text(
            text=get_text(lang, "code_accepted"),
            reply_markup=get_main_menu_keyboard(lang),
            parse_mode='Markdown'
        )

async def platform_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'ru')
    command = query.data
    if command == "platform_done":
        reply_markup = get_token_parse_settings_keyboard(lang, context)
        await query.edit_message_text(text=get_text(lang, "get_tokens_prompt"), reply_markup=reply_markup)
        return
    platform_name = command.replace("platform_toggle_", "")
    selected_platforms = context.user_data.get('token_parse_platforms', [])
    if platform_name in selected_platforms:
        selected_platforms.remove(platform_name)
    else:
        selected_platforms.append(platform_name)
    context.user_data['token_parse_platforms'] = selected_platforms
    all_platforms = await fetch_unique_launchpads()
    reply_markup = get_platform_selection_keyboard(lang, all_platforms, selected_platforms)
    await query.edit_message_reply_markup(reply_markup=reply_markup)

async def period_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'ru')
    command = query.data
    if command == "period_done":
        reply_markup = get_token_parse_settings_keyboard(lang, context)
        await query.edit_message_text(text=get_text(lang, "get_tokens_prompt"), reply_markup=reply_markup)
        return
    selected_period = command.replace("period_select_", "")
    context.user_data['token_parse_period'] = selected_period
    reply_markup = get_period_selection_keyboard(lang, selected_period)
    await query.edit_message_reply_markup(reply_markup=reply_markup)

async def category_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = context.user_data.get('lang', 'ru')
    command = query.data
    if command == "category_done":
        reply_markup = get_token_parse_settings_keyboard(lang, context)
        await query.edit_message_text(text=get_text(lang, "get_tokens_prompt"), reply_markup=reply_markup)
        return
    category_name = command.replace("category_toggle_", "")
    selected_categories = context.user_data.get('token_parse_categories', [])
    if category_name in selected_categories:
        selected_categories.remove(category_name)
    else:
        selected_categories.append(category_name)
    context.user_data['token_parse_categories'] = selected_categories
    reply_markup = get_category_selection_keyboard(lang, selected_categories)
    await query.edit_message_reply_markup(reply_markup=reply_markup)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get('lang', 'ru')
    state = context.user_data.get('state')
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if state == 'awaiting_secret_code':
        code = update.message.text.strip()
        if await validate_and_activate_code(code, user_id, chat_id):
            context.user_data['lang'] = 'ru'
            context.user_data.pop('state', None)
            await update.message.replyDem(
                text=get_text(lang, "code_accepted"),
                reply_markup=get_main_menu_keyboard(lang),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(get_text(lang, "invalid_code"))
        return

    if not await has_access(user_id):
        await update.message.reply_text(get_text(lang, "invalid_code"))
        return

    if state in ['awaiting_trader_list', 'awaiting_wallet_stats']:
        text = update.message.text.strip()
        if not text or not text.strip():
            await update.message.reply_text(get_text(lang, "input_empty_error"))
            return
        addresses = [line.strip() for line in text.splitlines() if line.strip()]
        if not addresses:
            await update.message.reply_text(get_text(lang, "input_empty_error"))
            return
        if len(addresses) > MAX_ADDRESS_LIST_SIZE:
            await update.message.reply_text(get_text(lang, "input_list_too_long_error", MAX_ADDRESS_LIST_SIZE))
            return
        invalid_lines = [addr for addr in addresses if not (32 <= len(addr) <= 44)]
        if invalid_lines:
            error_sample = "\n".join(f"`{line}`" for line in invalid_lines[:5])
            await update.message.reply_text(get_text(lang, "input_address_length_error", error_sample), parse_mode='Markdown')
            return
        context.user_data['main_message_id'] = update.message.message_id
        del context.user_data['state']
        if state == 'awaiting_trader_list':
            await process_trader_list(addresses, update, context)
        elif state == 'awaiting_wallet_stats':
            await process_wallet_stats(addresses, update, context)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get('lang', 'ru')
    user_id = update.effective_user.id
    if not await has_access(user_id):
        await update.message.reply_text(get_text(lang, "invalid_code"))
        return
    current_state = context.user_data.get('state')
    if current_state in ['awaiting_trader_list', 'awaiting_wallet_stats']:
        doc = await context.bot.get_file(update.message.document)
        file_content = (await doc.download_as_bytearray()).decode('utf-8')
        addresses = [line.strip() for line in file_content.splitlines() if line.strip()]
        if not addresses:
            await update.message.reply_text(get_text(lang, "input_empty_error"))
            return
        if len(addresses) > MAX_ADDRESS_LIST_SIZE:
            await update.message.reply_text(get_text(lang, "input_list_too_long_error", MAX_ADDRESS_LIST_SIZE))
            return
        invalid_lines = [addr for addr in addresses if not (32 <= len(addr) <= 44)]
        if invalid_lines:
            error_sample = "\n".join(f"`{line}`" for line in invalid_lines[:5])
            await update.message.reply_text(get_text(lang, "input_address_length_error", error_sample), parse_mode='Markdown')
            return
        context.user_data['main_message_id'] = update.message.message_id
        del context.user_data['state']
        if current_state == 'awaiting_trader_list':
            await process_trader_list(addresses, update, context)
        elif current_state == 'awaiting_wallet_stats':
            await process_wallet_stats(addresses, update, context)

async def log_error(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Ошибка в боте: {context.error}", exc_info=True)

async def post_init(application: Application) -> None:
    await application.bot.set_my_commands([
        BotCommand("start", "🚀 Запустить бота и ввести секретный код")
    ])

def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN не найден в .env файле!")
        return
    initialize_secret_codes()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(main_menu_callback_handler, pattern="^mainmenu_"))
    application.add_handler(CallbackQueryHandler(parse_submenu_callback, pattern="^parse_"))
    application.add_handler(CallbackQueryHandler(token_settings_callback, pattern="^tokensettings_"))
    application.add_handler(CallbackQueryHandler(platform_selection_callback, pattern="^platform_"))
    application.add_handler(CallbackQueryHandler(period_selection_callback, pattern="^period_"))
    application.add_handler(CallbackQueryHandler(category_selection_callback, pattern="^category_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(MessageHandler(filters.Document.TXT, handle_document))
    application.add_error_handler(log_error)

    logger.info("Бот запущен и готов к работе...")
    application.run_polling()

if __name__ == "__main__":
    main()