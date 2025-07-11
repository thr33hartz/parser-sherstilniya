import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dateutil import tz

from telegram.ext import ContextTypes

# --- ИСПРАВЛЕНО: Добавлены все необходимые импорты ---
from supabase_client import supabase

# Инициализируем логгер для этого файла
logger = logging.getLogger(__name__)


async def check_bundle_alerts(context: ContextTypes.DEFAULT_TYPE):
    """
    Проверяет и отправляет алерты по правилам из address_alerts.
    """
    bot = context.bot
    now_utc = datetime.now(timezone.utc)
    loop = asyncio.get_event_loop()

    try:
        # ИСПРАВЛЕНО: Все вызовы к Supabase обернуты для асинхронной работы
        rules_response = await loop.run_in_executor(
            None,
            lambda: supabase.table("address_alerts")
                            .select("*, custom_name")
                            .eq("is_active", True)
                            .execute()
        )
        rules = rules_response.data or []

        for rule in rules:
            addr = rule.get("address_to_track")
            if not addr:
                continue
            
            chat_id = rule.get("chat_id")
            gap_min = rule.get("time_gap_min")
            min_cnt = rule.get("min_cnt")
            amount_eps = Decimal(str(rule.get("amount_step", "0.1")))
            amin = Decimal(str(rule.get("min_transfer_amount", "0")))
            amax_val = rule.get("max_transfer_amount")
            amax = Decimal(str(amax_val)) if amax_val is not None else None

            window_start = (now_utc - timedelta(minutes=gap_min)).isoformat()

            txs_response = await loop.run_in_executor(
                None,
                lambda: supabase.table("tracked_transactions")
                                .select("id,signature,to,amount,decimals,block_time,action")
                                .eq("tracked_address", addr)
                                .eq("sent", False)
                                .eq("action", "TRANSFER")
                                .gte("block_time", window_start)
                                .execute()
            )
            raw_txs = txs_response.data or []

            txs = []
            for t in raw_txs:
                dec = int(t.get("decimals") or 9)
                amount_sol = Decimal(t["amount"]) / (10 ** dec)
                if amount_sol < amin:
                    continue
                if amax is not None and amount_sol > amax:
                    continue
                t["amount_sol"] = amount_sol
                txs.append(t)

            if len(txs) < min_cnt:
                continue

            txs_sorted = sorted(txs, key=lambda t: t["amount_sol"])
            max_group = []
            left = 0
            for right in range(len(txs_sorted)):
                while (txs_sorted[right]["amount_sol"] - txs_sorted[left]["amount_sol"]) > amount_eps:
                    left += 1
                current_group = txs_sorted[left:right + 1]
                if len(current_group) > len(max_group) and len(current_group) >= min_cnt:
                    max_group = current_group

            if len(max_group) < min_cnt:
                continue

            group_txs = max_group
            amounts = [t["amount_sol"] for t in group_txs]
            local_tz = tz.tzlocal()
            ts_human = now_utc.astimezone(local_tz).strftime("%H:%M:%S %d-%m")
            
            lines = [
                f"🚨 *Bundle-alert!* ({ts_human})",
                f"`{addr}`",
                "",
                f"{len(group_txs)} вывода за {gap_min} мин",
                f"Суммы: {', '.join([f'{a:.2f}' for a in amounts])} SOL",
                "",
                "To:",
                *[f"`{t['to']}`" for t in group_txs[:5]]
            ]
            await bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown")

            ids_to_update = [t["id"] for t in group_txs]
            await loop.run_in_executor(
                None,
                lambda: supabase.table("tracked_transactions")
                                .update({"sent": True})
                                .in_("id", ids_to_update)
                                .execute()
            )

    except Exception as e:
        logger.error("[BundleAlertJob] Unhandled exception: %s", e, exc_info=True)