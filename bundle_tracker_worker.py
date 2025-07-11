#!/usr/bin/env python3
"""bundle_tracker_worker.py – MINIMAL (no‑dedup)

➊ Открыть страницу Solscan:
   https://solscan.io/account/{address}?exclude_amount_zero=false&remove_spam=false&flow=out&token_address=So11111111111111111111111111111111111111111#transfers
➋ Нажать «Export CSV»  →  «Download».
➌ Сохранить CSV, прочитать Pandas‑ом и **целиком** вставить в таблицу `tracked_transactions` Supabase **без каких‑либо on‑conflict / dedup**.
➍ Повторять каждые `POLL_INTERVAL` секунд.  Логи в консоль.
"""
from __future__ import annotations

import asyncio
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, List
import uuid

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PwTimeout
from supabase import create_client

# ──────────── ENV ────────────
load_dotenv()
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
HEADLESS      = os.getenv("HEADLESS", "1") == "1"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 30))
DOWNLOAD_DIR  = os.getenv("DOWNLOAD_DIR", "downloads")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
if not os.path.isdir(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ──────────── Browser ctx ────────────
@asynccontextmanager
async def browser_ctx() -> Browser:  # typo fixed earlier
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        try:
            yield browser
        finally:
            await browser.close()

# ──────────── Download helpers ────────────
async def click_export(page: Page) -> None:
    """Нажимаем Export CSV на главном экране"""
    try:
        await page.get_by_role("button", name="Export CSV").click()
    except Exception:
        await page.locator("svg[data-tooltip-id='export']").first.click()

async def click_dialog_download(page: Page) -> str | None:
    """Ждём диалог и скачиваем CSV. Возвращает путь."""
    try:
        async with page.expect_download(timeout=30_000) as info:
            await page.locator("button:has(svg.lucide-cloud-download)").click()
        download = await info.value
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = os.path.join(DOWNLOAD_DIR, f"{ts}_{download.suggested_filename}")
        await download.save_as(path)
        logging.info("CSV saved → %s", path)
        return path
    except PwTimeout:
        logging.error("Timeout waiting for Download dialog")
        return None

async def grab_csv(address: str, browser: Browser) -> pd.DataFrame | None:
    url = (
        f"https://solscan.io/account/{address}"
        "?exclude_amount_zero=false&remove_spam=false&flow=out"
        "&token_address=So11111111111111111111111111111111111111111#transfers"
    )
    ctx = await browser.new_context()
    page = await ctx.new_page()
    await page.goto(url, wait_until="domcontentloaded")
    await click_export(page)
    path = await click_dialog_download(page)
    await ctx.close()
    if not path:
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        if "block_time" in df.columns:
            df["block_time"] = df["block_time"].apply(_to_dt)
        return df
    except Exception as e:
        logging.error("CSV parse error %s: %s", path, e)
        return None

# ──────────── Latest timestamp ────────────
def latest_ts(addr: str) -> datetime | None:
    res = (
        sb.table("tracked_transactions")
          .select("block_time")
          .eq("tracked_address", addr)
          .order("block_time", desc=True)
          .limit(1)
          .execute()
    )
    if res.data:
        return datetime.fromisoformat(res.data[0]["block_time"])
    return None

# ──────────── Filter new transactions ────────────
def filter_new(df: pd.DataFrame, addr: str) -> pd.DataFrame:
    last = latest_ts(addr)
    if last is None:            # адрес ещё не попадался
        return df
    return df[df["block_time"] > last]

# ──────────── Save bundle event ────────────
def save_bundle_event(user_id, chat_id, addr, tx_hash, amount):
    sb.table("bundle_events").insert({
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "chat_id": chat_id,
        "address_to_track": addr,
        "tx_hash": tx_hash,
        "amount_sol": amount,
        "happened_at": datetime.now(timezone.utc),
        "sent": False
    }).execute()
    
# ──────────── Supabase ────────────


def upsert_to_supabase(df: pd.DataFrame, address: str):
    """Вставляем **все** строки как есть (дубликаты допускаются)."""
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    # Sanitize data to avoid JSON serialization errors (NaN / ±inf → None)
    df = (
        df.replace([np.inf, -np.inf], np.nan)   # ±inf → NaN
          .where(pd.notnull(df), None)          # NaN → None (JSON null)
    )

    # block_time в CSV иногда NaN → postgres timestampz допускает NULL
    if "block_time" in df.columns:
        df["block_time"] = df["block_time"].replace({np.nan: None})
        # → ISO‑строки, чтобы json.dumps не падал
        def _bt_to_iso(x):
            if pd.isna(x):
                return None
            # Если число – epoch
            if isinstance(x, (int, float)):
                dt = datetime.utcfromtimestamp(int(x))
            elif isinstance(x, pd.Timestamp):
                dt = x.to_pydatetime()
            elif isinstance(x, datetime):
                dt = x
            else:
                return None
            return dt.replace(tzinfo=timezone.utc).isoformat()

        df["block_time"] = df["block_time"].apply(_bt_to_iso)

    df["tracked_address"] = address
    rows: List[dict[str, Any]] = df.to_dict("records")
    if not rows:
        return
    # ⚠️ без on_conflict – вставляем всё подряд
    sb.table("tracked_transactions").insert(rows).execute()
    logging.info("Inserted %d rows for %s", len(rows), address)
 
# ──────────── Convert to datetime ────────────   
def _to_dt(x):
    if pd.isna(x):
        return pd.NaT
    # epoch (int/float/np.int64) → datetime
    if isinstance(x, (int, float, np.integer)):
        return datetime.utcfromtimestamp(int(x)).replace(tzinfo=timezone.utc)
    # уже Timestamp / datetime
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime().replace(tzinfo=timezone.utc)
    if isinstance(x, datetime):
        return x.replace(tzinfo=timezone.utc)
    return pd.NaT

# ──────────── MAIN LOOP ────────────
async def main():
    async with browser_ctx() as browser:
        while True:
            # Refresh the list of tracked addresses each cycle
            res = sb.table("address_alerts").select("address_to_track").execute().data or []
            addresses = {row["address_to_track"] for row in res}
            if not addresses:
                logging.error("No addresses in Supabase. Populate address_alerts table.")
                await asyncio.sleep(POLL_INTERVAL)
                continue
            for addr in addresses:
                df = await grab_csv(addr, browser)
                if df is not None:
                    df = filter_new(df, addr)
                    if not df.empty:
                        upsert_to_supabase(df, addr)   # или insert, если без upsert
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted")