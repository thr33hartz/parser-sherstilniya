# handlers/conv_activate.py
from telegram.ext import ConversationHandler, MessageHandler, filters, CommandHandler
from services import db_access
from services.db_access import _sb      # ← импортируем клиент Supabase
from services import db_access  

ASK_CODE = 1

async def cmd_activate(update, context):
    await update.message.reply_text("Введите ваш промо-код:")
    return ASK_CODE

async def process_code(update, context):
    code = update.message.text.strip()
    row  = db_access.check_code(code)
    if not row:
        await update.message.reply_text("❌ Код не найден.")
    elif row["used"]:
        await update.message.reply_text("😕 Этот код уже использован.")
    else:
        tg_id = update.effective_user.id
        db_access.mark_code_used(code, tg_id)
        # 1) пишем в БД
        _sb.table("users").upsert(
            {"id": tg_id, "is_premium": True},
            on_conflict="id"
        ).execute()

        # 2) сохраняем в user_data — /start будет читать отсюда
        context.user_data["premium"] = True

        # 3) спасибо‑сообщение
        await update.message.reply_text(
            "✅ Активировано! Перезапустите /start.",
            disable_web_page_preview=True
        )
    return ConversationHandler.END

conv_activate = ConversationHandler(
    entry_points=[CommandHandler("activate", cmd_activate)],
    states={ASK_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_code)]},
    fallbacks=[],
)