from telegram import Update, InlineKeyboardMarkup
from telegram.ext import ContextTypes

# Импортируем необходимые UI-компоненты из нового модуля 'ui'
from ui.keyboards import get_main_menu_inline_keyboard, get_language_keyboard
from ui.translations import get_text

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
                **kwargs
            )
            return mid
    except Exception: # Broad exception to catch message-not-found errors
        # Сообщение удалилось — пришлём новое
        pass

    msg = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    context.user_data["main_message_id"] = msg.message_id
    return msg.message_id


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик команды /start.
    Проверяет, выбран ли язык, и показывает либо выбор языка, либо главное меню.
    """
    # Если язык уже выбран – показываем главное меню, иначе – диалог выбора
    if "lang" in context.user_data:
        lang = context.user_data["lang"]
        await ensure_main_msg(
            context.bot,
            update.effective_chat.id,
            context,
            get_text(lang, "main_menu_message"),
            reply_markup=get_main_menu_inline_keyboard(lang),
            parse_mode="Markdown"
        )
    else:
        await ensure_main_msg(
            context.bot,
            update.effective_chat.id,
            context,
            get_text("ru", "welcome"),  # Одно сообщение для обоих языков
            reply_markup=get_language_keyboard()
        )

    # Чистим команду /start из чата для минимальной истории
    try:
        await update.message.delete()
    except Exception:
        pass


async def language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик команды /language.
    Позволяет пользователю сменить язык в любой момент, показывая клавиатуру выбора.
    """
    context.user_data.pop("lang", None)  # Заставляем выбрать заново
    await ensure_main_msg(
        context.bot,
        update.effective_chat.id,
        context,
        get_text("ru", "welcome"),
        reply_markup=get_language_keyboard(),
        parse_mode="Markdown"
    )
    try:
        await update.message.delete()
    except Exception:
        pass