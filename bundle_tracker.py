# bundle_tracker.py
from __future__ import annotations
import asyncio, logging
from datetime import datetime
from typing import Dict, Any, List

from telegram import (Update, InlineKeyboardButton, InlineKeyboardMarkup)
from telegram.ext import (ContextTypes, CallbackQueryHandler, MessageHandler, filters)

from supabase_client import supabase     # ← ваш обёрнутый create_client
from translations import get_text        # ← функция, которую уже используете

LOGGER = logging.getLogger(__name__)

MAX_TRACKING_TASKS_PER_USER = 3

# ────────────────── вспомогалки ──────────────────
def tracker_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(get_text(lang, "bundle_add_btn"),
                                  callback_data="bundle_add")],
            [InlineKeyboardButton(get_text(lang, "bundle_view_btn"),
                                  callback_data="bundle_view")],
            [InlineKeyboardButton(get_text(lang, "back_btn"),
                                  callback_data="bundle_back_main")],
        ]
    )

async def user_active_count(user_id: int) -> int:
    rsp = await asyncio.to_thread(
        supabase.table("address_alerts")
        .select("id", count="exact")
        .eq("user_id", user_id)
        .eq("is_active", True)
        .execute
    )
    return rsp.count or 0

# ────────────────── STEP-HANDLERS ──────────────────
async def bundle_start(query, context, lang):
    uid = query.from_user.id
    if await user_active_count(uid) >= MAX_TRACKING_TASKS_PER_USER:
        await query.edit_message_text(
            get_text(lang, "bundle_add_limit_reached",
                     MAX_TRACKING_TASKS_PER_USER),
            parse_mode='Markdown')
        return
    context.user_data["bndl"] = {}
    context.user_data["state"] = "bndl_addr"
    await query.edit_message_text(get_text(lang, "bundle_add_step1_address"),
                                  parse_mode='Markdown')

async def bundle_collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Пошаговая валидация + запись в context.user_data["bndl"]
    после финального шага — upsert в Supabase
    """
    lang = context.user_data.get("lang", "en")
    st   = context.user_data.get("state")
    txt  = update.message.text.strip().replace(',', '.')
    ud   = context.user_data.setdefault("bndl", {})

    try:
        if st == "bndl_addr":
            if not (32 <= len(txt) <= 44):
                raise ValueError("addr")
            ud["address_to_track"] = txt
            context.user_data["state"] = "bndl_minutes"
            await update.message.reply_text(
                get_text(lang, "bundle_add_step2_minutes"), parse_mode='Markdown')

        elif st == "bndl_minutes":
            v = int(txt);  assert v >= 1
            ud["time_gap_min"] = v
            context.user_data["state"] = "bndl_cnt"
            await update.message.reply_text(
                get_text(lang, "bundle_add_step3_count"), parse_mode='Markdown')

        elif st == "bndl_cnt":
            v = int(txt);  assert v >= 1
            ud["min_cnt"] = v
            context.user_data["state"] = "bndl_diff"
            await update.message.reply_text(
                get_text(lang, "bundle_add_step4_diff"), parse_mode='Markdown')

        elif st == "bndl_diff":
            v = float(txt);  assert v > 0
            ud["amount_step"] = v
            context.user_data["state"] = "bndl_minamt"
            await update.message.reply_text(
                get_text(lang, "bundle_add_step5_min_amount"), parse_mode='Markdown')

        elif st == "bndl_minamt":
            v = float(txt);  assert v >= 0
            ud["min_transfer_amount"] = v
            context.user_data["state"] = "bndl_maxamt"
            await update.message.reply_text(
                get_text(lang, "bundle_add_step6_max_amount"), parse_mode='Markdown')

        elif st == "bndl_maxamt":
            v = float(txt);  assert v >= 0
            ud["max_transfer_amount"] = v if v > 0 else None

            payload: Dict[str, Any] = {
                "user_id":       update.effective_user.id,
                "chat_id":       update.effective_chat.id,
                "created_at":    datetime.utcnow().isoformat(),
                "is_active":     True,
                **ud,
            }
            # upsert ЖДЁТ список и tuple колонок
            await asyncio.to_thread(
                supabase.table("address_alerts")
                .upsert([payload], on_conflict=("user_id", "address_to_track"))
                .execute
            )

            max_txt = str(ud["max_transfer_amount"]) if ud["max_transfer_amount"] else "∞"
            await update.message.reply_text(
                get_text(lang, "bundle_add_success",
                         ud["address_to_track"], ud["time_gap_min"], ud["min_cnt"],
                         ud["amount_step"], ud["min_transfer_amount"], max_txt),
                parse_mode='Markdown'
            )
            context.user_data.pop("state", None)
            context.user_data.pop("bndl",  None)

    except (ValueError, AssertionError):
        await update.message.reply_text(get_text(lang, "bundle_add_fail_invalid_number"))
    except Exception as e:
        LOGGER.error("Bundle-tracker save error", exc_info=e)
        await update.message.reply_text(get_text(lang, "error_occurred"))
        context.user_data.pop("state", None)
        context.user_data.pop("bndl",  None)

# ────────────────── PUBLIC: регистратор в боте ──────────────────
def register_handlers(app):
    # кнопки
    app.add_handler(CallbackQueryHandler(
        lambda u,c: bundle_start(u.callback_query, c, c.user_data.get("lang","en")),
        pattern="^bundle_add$"))
    # сбор текстом
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        bundle_collect))
