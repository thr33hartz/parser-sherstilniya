# bot.py
"""
Главный файл для запуска Telegram-бота.
Отвечает за инициализацию, регистрацию обработчиков и запуск бота.
"""

import logging
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from telegram import BotCommand

# --- Основные компоненты приложения ---
import config
from jobs.check_bundle_alerts import check_bundle_alerts # TODO: Раскомментируйте, когда перенесете

# --- Модули с обработчиками ---
from handlers import commands, callbacks, messages
from handlers.conv_activate import conv_activate
from jobs.price_job import update_sol_price_job

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger(__name__)


async def post_init(application: Application):
    """Выполняется после инициализации приложения, перед запуском polling'а."""
    await application.bot.set_my_commands([
        BotCommand("start", "🚀 Запустить / Сменить язык"),
        BotCommand("language", "⚙️ Сменить язык"),
    ])
    
    # TODO: Раскомментируйте этот блок, когда перенесете check_bundle_alerts в jobs/
    application.job_queue.run_repeating(
        check_bundle_alerts,
        interval=30,
        first=15,
        name="bundle_alerts_job"
    )
    application.job_queue.run_repeating(
        update_sol_price_job, 
        interval=3600, 
        first=1, 
        name="update_sol_price"
    )
    logger.info("Команды бота установлены.")


def main():
    """Главная функция для запуска бота."""
    if not config.TELEGRAM_BOT_TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN не найден в .env! Бот не может быть запущен.")
        return

    # 1. Собираем приложение
    application = (
        Application.builder()
        .token(config.TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # 2. Регистрируем обработчики из модулей
    # Каждый хендлер теперь указывает на функцию в соответствующем файле.
    
    # --- Команды ---
    application.add_handler(CommandHandler("start", commands.start))
    application.add_handler(conv_activate)

    # --- Обработка нажатий на инлайн-кнопки ---
    application.add_handler(CallbackQueryHandler(callbacks.set_language_callback, pattern="^set_lang_"))
    application.add_handler(CallbackQueryHandler(callbacks.main_menu_callback_handler, pattern="^mainmenu_"))
    application.add_handler(CallbackQueryHandler(callbacks.show_main_menu, pattern="^main_menu$"))
    application.add_handler(CallbackQueryHandler(callbacks.parse_submenu_callback, pattern="^parse_"))
    application.add_handler(CallbackQueryHandler(callbacks.token_settings_callback, pattern="^tokensettings_"))
    application.add_handler(CallbackQueryHandler(callbacks.platform_selection_callback, pattern="^platform_"))
    application.add_handler(CallbackQueryHandler(callbacks.period_selection_callback, pattern="^period_"))
    application.add_handler(CallbackQueryHandler(callbacks.bundle_tracker_callback, pattern="^bundle_"))
    application.add_handler(CallbackQueryHandler(callbacks.template_settings_callback, pattern="^template_set_"))
    application.add_handler(CallbackQueryHandler(callbacks.template_management_callback, pattern="^template_"))
    application.add_handler(CallbackQueryHandler(callbacks.category_selection_callback, pattern="^category_"))
    application.add_handler(CallbackQueryHandler(callbacks.dev_parse_settings_callback, pattern="^devparse_"))
    application.add_handler(CallbackQueryHandler(callbacks.main_menu_callback_handler, pattern="^mainmenu_"))
    application.add_handler(CallbackQueryHandler(callbacks.pnl_filter_callback_handler, pattern="^pnl_filter_"))
    application.add_handler(CallbackQueryHandler(callbacks.language_settings_callback, pattern="^settings_language$"))
    application.add_handler(CallbackQueryHandler(callbacks.dev_pnl_filter_callback_handler, pattern="^dev_pnl_filter_"))

    
    # --- Обработка сообщений (текст и файлы) ---
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, messages.handle_text))
    application.add_handler(MessageHandler(filters.Document.TXT, messages.handle_document))

    # 3. Запускаем бота
    logger.info("Бот запускается...")
    application.run_polling()


if __name__ == "__main__":
    main()