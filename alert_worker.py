#!/usr/bin/env python3
"""
alert_worker.py
Отправляет bundle-алерты в Telegram по правилам из таблицы address_alerts.

Алгоритм по каждому активному правилу:
  1. Берём все выводы (flow='out') за последние time_gap_min минут
     из tracked_transactions по адресу address_to_track.
  2. Оставляем транзакции, попадающие в диапазон
        min_transfer_amount  ≤ amount ≤  max_transfer_amount (если задан).
  3. Если кол-во ≥ min_cnt и (max(amount)-min(amount)) ≤ amount_step – шлём алерт.
"""

import asyncio, logging, os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from telegram import Bot, constants

# ───────────── env / init ─────────────
load_dotenv()

SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
POLL_SEC      = int(os.getenv("ALERT_POLL_SEC", 30))

sb  = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = Bot(BOT_TOKEN)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

LAMPORT = 1_000_000_000  # 1 SOL

# ───────────── helpers ─────────────
def fetch_active_alerts():
    """Все активные правила"""
    res = (
        sb.table("address_alerts")
          .select("*")
          .filter("is_active", "eq", True)
          .execute()
    ).data or []
    return res


def fetch_transactions(addr: str, since: datetime) -> pd.DataFrame:
    """Выборка трансферов из tracked_transactions для адреса после since"""
    since_iso = since.replace(tzinfo=timezone.utc).isoformat()
    res = (
        sb.table("tracked_transactions")
          .select("*")
          .filter("tracked_address", "eq", addr)
          .filter("flow", "eq", "out")
          .filter("block_time", "gte", since_iso)
          .execute()
    ).data or []
    return pd.DataFrame(res)


async def send_alert(task: dict, df: pd.DataFrame):
    """Формируем и отправляем сообщение"""
    addr  = task["address_to_track"]
    cnt   = len(df)

    min_amt  = df["amount"].min() / LAMPORT
    max_amt  = df["amount"].max() / LAMPORT
    avg_amt  = df["amount"].mean() / LAMPORT
    diff_amt = max_amt - min_amt

    tos = ", ".join(sorted(set(df["to"]))[:10])  # не больше 10 адресов

    msg = (
        "🚨 *BUNDLE ALERT!* 🚨\n\n"
        f"*Кошелёк*: `{addr}`\n"
        f"*Выводов*: *{cnt}*  за последние *{task['time_gap_min']} мин*\n"
        f"*Средняя сумма*: `~{avg_amt:.4f} SOL`\n"
        f"*Δ* (max-min): `{diff_amt:.4f} SOL`\n\n"
        "*Получатели*:\n"
        f"`{tos}`\n\n"
        f"_Отправлено c_ `{addr}`"
    )

    await bot.send_message(
        chat_id=task["chat_id"],
        text=msg,
        parse_mode=constants.ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    logging.info("Alert sent to chat %s for %s", task["chat_id"], addr)


def need_alert(task: dict, df: pd.DataFrame) -> bool:
    """Проверяем правила"""
    if df.empty:
        return False
    if len(df) < task["min_cnt"]:
        return False

    # диапазон сумм (лампорты)
    if "min_transfer_amount" in task and task["min_transfer_amount"] is not None:
        df = df[df["amount"] >= int(float(task["min_transfer_amount"]) * LAMPORT)]
    if "max_transfer_amount" in task and task["max_transfer_amount"] is not None:
        df = df[df["amount"] <= int(float(task["max_transfer_amount"]) * LAMPORT)]
    if len(df) < task["min_cnt"]:
        return False

    diff = (df["amount"].max() - df["amount"].min()) / LAMPORT
    return diff <= float(task["amount_step"])


# ───────────── main loop ─────────────
async def handle_task(task: dict):
    ago   = datetime.now(timezone.utc) - timedelta(minutes=task["time_gap_min"])
    df    = fetch_transactions(task["address_to_track"], ago)

    if need_alert(task, df):
        await send_alert(task, df)


async def main_loop():
    while True:
        tasks = fetch_active_alerts()
        if not tasks:
            logging.info("No active alert rules")
        else:
            await asyncio.gather(*(handle_task(t) for t in tasks))
        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("Stopped by user")
