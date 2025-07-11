from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from .translations import get_text
from config import TIME_PERIODS, TOKEN_CATEGORIES
from telegram.ext import ContextTypes

PNL_FILTER_CATEGORIES = {
    "💰 Balance": ["balance", "wsol_balance"],
    "📈 ROI": ["roi_7d", "roi_30d"],
    "🎯 Winrate": ["winrate_7d", "winrate_30d"],
    "💵 USD Profit": ["usd_profit_7d", "usd_profit_30d", "unrealised_pnl_7d"],
    "⏱️ Holding Time": ["avg_holding_time"],
    "📊 Buys/Sells": [
        "total_buys_7d", "total_sells_7d", "total_buys_30d", "total_sells_30d",
        "pf_buys_7d", "pf_swap_buys_7d", "bonk_buys_7d", "raydium_buys_7d", "boop_buys_7d", "meteora_buys_7d",
        "pf_swap_buys_30d", "bonk_buys_30d", "raydium_buys_30d", "boop_buys_30d", "meteora_buys_30d",
        "avg_buys_per_token_7d", "avg_buys_per_token_30d"
    ],
    "📅 Trade Activity": ["last_trade_time", "traded_tokens"],
    "🕒 Token Age": ["avg_token_age_7d", "avg_token_age_30d"],
    "🏆 Top PNL": ["top_three_pnl"],
    "📉 Quick Trades": ["avg_quick_buy_and_sell_percentage"],
    "📦 Bundled Buys": ["avg_bundled_token_buys_percentage"],
    "📈 Sold vs Bought": ["avg_sold_more_than_bought_percentage"],
    "💹 Market Cap": ["avg_first_buy_mcap_7d", "avg_first_buy_mcap_30d"],
    "💸 Token Costs": ["avg_token_cost_7d", "avg_token_cost_30d", "total_cost_7d", "total_cost_30d"],
    "💸 Forwarder Tips": ["avg_forwarder_tip"]
}

def get_pnl_filter_main_menu_keyboard(template_data: dict) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру первого уровня для меню PNL-фильтров (выбор категории).
    """
    keyboard = []
    # Добавляем кнопки для каждой категории фильтров
    for category_name in PNL_FILTER_CATEGORIES.keys():
        keyboard.append([
            InlineKeyboardButton(category_name, callback_data=f"pnl_filter_cat_{category_name}")
        ])
    
    # Кнопка для сброса всех PNL-фильтров
    keyboard.append([InlineKeyboardButton("🗑️ Сбросить все PNL-фильтры", callback_data="pnl_filter_reset_all")])
    # Кнопка для возврата в основное меню настроек шаблона
    keyboard.append([InlineKeyboardButton("⬅️ Назад к списку шаблонов", callback_data="template_view")])
    
    return InlineKeyboardMarkup(keyboard)


def get_pnl_filter_submenu_keyboard(category_name: str) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру второго уровня (выбор конкретной колонки).
    """
    keyboard = []
    # Получаем список колонок для выбранной категории
    columns = PNL_FILTER_CATEGORIES.get(category_name, [])
    
    for column_name in columns:
        keyboard.append([
            InlineKeyboardButton(column_name, callback_data=f"pnl_filter_col_{column_name}")
        ])
        
    # Кнопка для возврата на первый уровень PNL-фильтров
    keyboard.append([InlineKeyboardButton("⬅️ Назад к категориям", callback_data="pnl_filter_back_to_main")])
    
    return InlineKeyboardMarkup(keyboard)

def get_bundle_tracker_keyboard(lang: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(get_text(lang, "bundle_add_btn"), callback_data="bundle_add")],
        [InlineKeyboardButton(get_text(lang, "bundle_view_btn"), callback_data="bundle_view")],
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="bundle_back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_token_parse_settings_keyboard(lang: str, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    ud = context.user_data
    
    # --- Логика для кнопки "Platforms" ---
    selected_platforms_count = len(ud.get('token_parse_platforms', []))
    platforms_text_template = get_text(lang, "platforms_btn") # Получаем шаблон "Platforms ({})"
    platforms_text = platforms_text_template.format(selected_platforms_count)

    # --- Логика для кнопки "Category" ---
    selected_categories = ud.get('token_parse_categories', [])
    category_text_template = get_text(lang, "category_btn") # Получаем шаблон "Category ({})"
    category_text = category_text_template.format(len(selected_categories))

    # --- Логика для кнопки "Time Period" ---
    selected_period = ud.get('token_parse_period', '24h')
    period_text_template = get_text(lang, "time_period_btn") # Получаем шаблон "Time Period ({})"
    period_text = period_text_template.format(selected_period)

    # --- Собираем клавиатуру ---
    keyboard = [
        [InlineKeyboardButton(platforms_text, callback_data="tokensettings_platforms")],
        [InlineKeyboardButton(category_text, callback_data="tokensettings_category")],
        [InlineKeyboardButton(period_text, callback_data="tokensettings_period")],
        [InlineKeyboardButton(get_text(lang, "parse_now_btn"), callback_data="tokensettings_execute")],
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_parse_submenu_keyboard(lang: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(get_text(lang, "all_in_parse_btn"), callback_data="parse_all_in")],
        [InlineKeyboardButton(get_text(lang, "get_tokens_btn"), callback_data="parse_get_tokens")],
        [InlineKeyboardButton(get_text(lang, "get_top_traders_btn"), callback_data="parse_get_traders")],
        [InlineKeyboardButton(get_text(lang, "get_wallet_stats_btn"), callback_data="parse_get_stats")],
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="parse_back")],
    ]
    return InlineKeyboardMarkup(keyboard)

def get_main_menu_inline_keyboard(lang: str) -> InlineKeyboardMarkup:
    """Inline-клавиатура главного меню с кнопкой обновления цены."""
    keyboard = [
        [InlineKeyboardButton(get_text(lang, "parse_btn"),         callback_data="mainmenu_parse"),
         InlineKeyboardButton(get_text(lang, "dev_parse_btn"),     callback_data="mainmenu_dev_parse")],
        [InlineKeyboardButton(get_text(lang, "program_parse_btn"), callback_data="mainmenu_program_parse"),
         InlineKeyboardButton(get_text(lang, "bundle_tracker_btn"),callback_data="mainmenu_bundle_tracker")],
        [InlineKeyboardButton(get_text(lang, "copytrade_sim_btn"), callback_data="mainmenu_copytrade_sim"),
         InlineKeyboardButton(get_text(lang, "settings_btn"),      callback_data="mainmenu_settings")],
        [InlineKeyboardButton("👨‍💻 Contact developer", callback_data="mainmenu_contact_dev")]

    ]
    return InlineKeyboardMarkup(keyboard)
    
def get_language_keyboard() -> InlineKeyboardMarkup:
    """Инлайн-клавиатура для выбора языка интерфейса."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇷🇺 Русский", callback_data="set_lang_ru"),
            InlineKeyboardButton("🇬🇧 English", callback_data="set_lang_en")
        ]
    ])

def get_platform_selection_keyboard(lang: str, all_platforms: list, selected_platforms: list) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    for platform in all_platforms:
        if platform in selected_platforms:
            text = f"✅ {platform}"
        else:
            text = f"❌ {platform}"
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

def get_category_selection_keyboard(lang: str, selected_categories: list, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """
    ЕДИНАЯ клавиатура для выбора категорий.
    ИСПРАВЛЕНО: Теперь показывает разные категории в зависимости от контекста.
    """
    keyboard = []
    
    current_state = context.user_data.get('state')
    
    # Если мы настраиваем Dev Parse или Шаблон, показываем урезанный список
    if current_state in ['dev_parse_editing_categories', 'template_editing_categories']:
        all_categories = ["completed", "completing"]
    else: # Для обычного парсинга показываем все
        all_categories = TOKEN_CATEGORIES 

    for category in all_categories:
        text = f"✅ {category}" if category in selected_categories else f"❌ {category}"
        keyboard.append([InlineKeyboardButton(text, callback_data=f"category_toggle_{category}")])
    
    keyboard.append([InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="category_done")])
    return InlineKeyboardMarkup(keyboard)

def get_template_management_keyboard(lang: str, user_id: int) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для управления шаблонами."""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить шаблон" if lang == "ru" else "➕ Add Template", callback_data="template_create")],
        [InlineKeyboardButton("📋 Мои шаблоны" if lang == "ru" else "📋 My Templates", callback_data="template_view")],
        [InlineKeyboardButton("⬅️ Назад" if lang == "ru" else "⬅️ Back", callback_data="parse_back")],
    ]
    return InlineKeyboardMarkup(keyboard)

def get_template_view_keyboard(lang: str, templates: list) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для просмотра и управления существующими шаблонами."""
    keyboard = []
    for template in templates:
        template_id = template["id"]
        template_name = template["template_name"]
        keyboard.append([
            InlineKeyboardButton(
                f"{template_name}",
                callback_data=f"template_select_{template_id}"
            ),
            InlineKeyboardButton(
                "✏️ Edit",
                callback_data=f"template_edit_{template_id}"
            ),
            InlineKeyboardButton(
                "🗑️ Delete",
                callback_data=f"template_delete_{template_id}"
            ),
        ])
    keyboard.append([InlineKeyboardButton("⬅️ Назад" if lang == "ru" else "⬅️ Back", callback_data="template_back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

def get_template_edit_keyboard(lang: str, template: dict) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для редактирования шаблона."""
    platforms_count = len(template.get("platforms", []))
    platforms_text = f"Платформы ({platforms_count if platforms_count > 0 else 'Все'})" if lang == "ru" else f"Platforms ({platforms_count if platforms_count > 0 else 'All'})"
    categories = template.get("categories", [])
    category_text = f"Категории ({len(categories)})" if lang == "ru" else f"Category ({len(categories)})"
    period = template.get("time_period", "24h")
    period_text = f"Период времени ({period})" if lang == "ru" else f"Time Period ({period})"
    keyboard = [
        [InlineKeyboardButton(platforms_text, callback_data=f"template_edit_platforms_{template['id']}")],
        [InlineKeyboardButton(category_text, callback_data=f"template_edit_category_{template['id']}")],
        [InlineKeyboardButton(period_text, callback_data=f"template_edit_period_{template['id']}")],
        [InlineKeyboardButton("💾 Сохранить" if lang == "ru" else "💾 Save", callback_data=f"template_save_{template['id']}")],
        [InlineKeyboardButton("⬅️ Назад" if lang == "ru" else "⬅️ Back", callback_data="template_view")],
    ]
    return InlineKeyboardMarkup(keyboard)

def get_template_settings_keyboard(lang: str, template_data: dict) -> InlineKeyboardMarkup:
    """
    Генерирует клавиатуру для настройки параметров шаблона.
    ИСПРАВЛЕНО: Корректное использование get_text().
    """
    platforms_count = len(template_data.get('platforms', []))
    # Сначала получаем шаблон, потом форматируем
    platforms_text_template = get_text(lang, "platforms_btn")
    platforms_text = platforms_text_template.format(platforms_count if platforms_count > 0 else 'All')
    
    categories = template_data.get('categories', [])
    category_text = f"Категории ({len(categories)})"
    
    selected_period = template_data.get('time_period', '24h')
    period_text_template = get_text(lang, "time_period_btn")
    period_text = period_text_template.format(selected_period)

    keyboard = [
        [InlineKeyboardButton(platforms_text, callback_data="template_set_platforms")],
        [InlineKeyboardButton(category_text, callback_data="template_set_category")],
        [InlineKeyboardButton(period_text, callback_data="template_set_period")],
        [InlineKeyboardButton("📊 PNL-фильтры", callback_data="template_set_pnl_filters")],
        [InlineKeyboardButton("✅ Сохранить шаблон", callback_data="template_set_save")],
        [InlineKeyboardButton("❌ Отмена", callback_data="template_cancel")],
    ]
    return InlineKeyboardMarkup(keyboard)

def get_template_category_keyboard(lang: str, selected_categories: list) -> InlineKeyboardMarkup:
    """
    Клавиатура для выбора категорий шаблона (только completed/completing).
    ИСПРАВЛЕНО: callback_data теперь начинается с 'template_set_'.
    """
    keyboard = []
    for category in ["completed", "completing"]:
        text = f"✅ {category}" if category in selected_categories else f"❌ {category}"
        # ИЗМЕНЕНИЕ ЗДЕСЬ:
        keyboard.append([InlineKeyboardButton(text, callback_data=f"template_set_toggle_category_{category}")])
    
    # И ИЗМЕНЕНИЕ ЗДЕСЬ:
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="template_set_category_done")])
    return InlineKeyboardMarkup(keyboard)

def get_dev_parse_settings_keyboard(lang: str, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для настроек Dev Parse."""
    ud = context.user_data
    
    # ИСПРАВЛЕНИЕ: Сначала получаем шаблон, потом форматируем его
    platforms_count = len(ud.get('dev_parse_platforms', []))
    platforms_text_template = get_text(lang, "platforms_btn") # Получаем строку "Platforms ({})"
    platforms_text = platforms_text_template.format(platforms_count if platforms_count > 0 else "All")

    categories_count = len(ud.get('dev_parse_categories', []))
    category_text = f"Категории ({categories_count})"

    period = ud.get('dev_parse_period', '72h')
    period_text_template = get_text(lang, "time_period_btn") # Получаем строку "Time Period ({})"
    period_text = period_text_template.format(period)

    keyboard = [
        [InlineKeyboardButton(platforms_text, callback_data="devparse_platforms")],
        [InlineKeyboardButton(category_text, callback_data="devparse_category")],
        [InlineKeyboardButton(period_text, callback_data="devparse_period")],
        [InlineKeyboardButton("✅ Parse Devs", callback_data="devparse_execute")],
        [InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_dev_parse_period_keyboard(lang: str, current_period: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора периода для Dev Parse с расширенным диапазоном до 72 часов."""
    keyboard = []
    periods = {'1h': '1 час', '3h': '3 часа', '6h': '6 часов', '12h': '12 часов', '24h': '24 часа', '48h': '48 часов', '72h': '72 часа'}
    
    for period_key, period_text_ru in periods.items():
        text = f"✅ {period_text_ru}" if period_key == current_period else period_text_ru
        # Обратите внимание на callback_data, он начинается с devparse_
        keyboard.append([InlineKeyboardButton(text, callback_data=f"devparse_period_select_{period_key}")])
        
    keyboard.append([InlineKeyboardButton(get_text(lang, "back_btn"), callback_data="devparse_period_done")])
    return InlineKeyboardMarkup(keyboard)
