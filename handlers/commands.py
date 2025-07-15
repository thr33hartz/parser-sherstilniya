from telegram import Update, InlineKeyboardMarkup
from telegram.ext import ContextTypes

# Импортируем необходимые UI-компоненты из нового модуля 'ui'
from ui.keyboards import get_main_menu_inline_keyboard, get_language_keyboard
from ui.translations import get_text
from ui.keyboards import get_language_keyboard

from services import price_service, supabase_service, discord_scraper, queue_service
from services import db_access

def get_user_lang(context):
    return context.user_data.get("lang", "en")

# Эту функцию можно будет в будущем вынести в отдельный helper,
# но пока удобно держать ее с хендлерами, которые ее используют.
async def ensure_main_msg(bot, chat_id, context, text="…", **kwargs):
    """
    Гарантирует наличие главного сообщения и возвращает его id.
    Если старое пропало, создаём новое и сохраняем его id.
    """
    mid = context.user_data.get("main_message_id")
    try:
        if mid:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=mid,
                text=text,
                **kwargs, disable_web_page_preview=True
            )
            return mid
    except Exception: # Broad exception to catch message-not-found errors
        # Сообщение удалилось — пришлём новое
        pass

    msg = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    context.user_data["main_message_id"] = msg.message_id
    return msg.message_id


# +++ НАЧАЛО НОВОЙ ФУНКЦИИ +++
async def send_new_main_menu(bot, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """
    Отправляет новое главное меню, удаляя старое.
    Используется после отправки файлов, чтобы меню всегда было внизу.
    """
    lang = get_user_lang(context)

    # Получаем актуальные данные для меню
    sol_price = await price_service.get_sol_price()
    price_str = f"{sol_price:.2f}" if sol_price else "N/A"
    
    text_template = get_text(lang, "main_menu_message") 
    menu_text = text_template.format(price_str)
    
    main_menu_keyboard = get_main_menu_inline_keyboard(lang, context.user_data.get("premium", False))

    # Удаляем старое сообщение с меню, чтобы не было дублей
    old_mid = context.user_data.pop("main_message_id", None)
    if old_mid:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=old_mid)
        except Exception:
            pass # Игнорируем ошибку, если сообщение уже удалено

    # Отправляем новое меню
    new_menu_msg = await bot.send_message(
        chat_id=chat_id,
        text=menu_text,
        reply_markup=main_menu_keyboard,
        parse_mode="Markdown"
    )
    # Обновляем ID главного сообщения, чтобы бот знал, какое сообщение редактировать дальше
    context.user_data["main_message_id"] = new_menu_msg.message_id
# +++ КОНЕЦ НОВОЙ ФУНКЦИИ +++

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик команды /start.
    Проверяет, выбран ли язык, и показывает либо выбор языка, либо главное меню.
    """
    lang = get_user_lang(context)

    # ── NEW: determine premium status each time from DB ──
    premium = db_access.is_premium_user(update.effective_user.id)
    context.user_data["premium"] = premium  # cache for later use
    
    # --- ИСПРАВЛЕНИЕ: Всегда получаем цену перед отправкой меню ---
    if "lang" in context.user_data:
        # 1. Получаем актуальную цену SOL
        sol_price = await price_service.get_sol_price()
        price_str = f"{sol_price:.2f}" if sol_price else "N/A"
        
        # 2. Получаем шаблон текста
        text_template = get_text(lang, "main_menu_message")
        
        # 3. Форматируем текст, вставляя цену
        menu_text = text_template.format(price_str)
        
        # 4. Отправляем готовое сообщение
        await ensure_main_msg(
            context.bot,
            update.effective_chat.id,
            context,
            menu_text,  # <--- Используем отформатированный текст
            reply_markup=get_main_menu_inline_keyboard(lang, premium),
            parse_mode="Markdown",
            disable_web_page_preview=True # <--- Убираем предпросмотр ссылки
        )
    else:
        # Для выбора языка логика остается прежней
        await ensure_main_msg(
            context.bot,
            update.effective_chat.id,
            context,
            get_text("en", "welcome"),
            reply_markup=get_language_keyboard(),
            disable_web_page_preview=True
        )

    # Чистим команду /start из чата для минимальной истории
    try:
        await update.message.delete()
    except Exception:
        pass