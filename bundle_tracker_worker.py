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
from typing import Any, List, Coroutine
import uuid
import itertools
import re

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PwTimeout, Route, Playwright
from supabase import create_client

# ──────────── ENV ────────────
load_dotenv()
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
HEADLESS      = os.getenv("HEADLESS", "1") == "1"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60)) # Увеличено для стабильности
DOWNLOAD_DIR  = os.getenv("DOWNLOAD_DIR", "downloads")
MAX_RETRIES   = 3

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
if not os.path.isdir(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ──────────── Proxy rotation ────────────
RAW_PROXIES = [
    # Вставьте сюда ваш список купленных прокси
    "91.124.101.121:51523:HTTP:kdjkprokmp:xfP752jfzb",
    "195.178.135.122:51523:HTTP:kdjkprokmp:xfP752jfzb",
    "95.135.59.114:51523:HTTP:kdjkprokmp:xfP752jfzb",
    "92.118.138.244:51523:HTTP:kdjkprokmp:xfP752jfzb",
    "185.2.212.223:51523:HTTP:kdjkprokmp:xfP752jfzb",
]

def _to_proxy_url(line: str) -> str:
    ip, port, scheme, user, pwd = line.split(":")
    return f"{scheme.lower()}://{user}:{pwd}@{ip}:{port}"

PROXIES = [_to_proxy_url(p) for p in RAW_PROXIES]

# ──────────── User‑Agent rotation ────────────
UA_LIST = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"),
]
_ua_cycle = itertools.cycle(UA_LIST)

# ──────────── Resource Blocker ────────────
BLOCK_RESOURCE_PATTERN = re.compile(r"\.(css|jpg|jpeg|png|gif|svg|woff|woff2)|(google|doubleclick)")

async def block_unnecessary_requests(route: Route):
    if BLOCK_RESOURCE_PATTERN.search(route.request.url):
        await route.abort()
    else:
        await route.continue_()

# ──────────── ИСПРАВЛЕНИЕ: Улучшенная функция для проверки прокси ────────────
async def check_proxy(pw: Playwright, proxy_url: str) -> bool:
    """Проверяет работоспособность прокси, заходя на тестовый сайт и на Solscan."""
    browser = None
    try:
        logging.info(f"Проверка прокси: {proxy_url}...")
        browser = await pw.chromium.launch(headless=HEADLESS, proxy={"server": proxy_url})
        context = await browser.new_context(ignore_https_errors=True, user_agent=next(_ua_cycle))
        page = await context.new_page()

        # Этап 1: Проверка базового подключения
        logging.info(f"[{proxy_url}] Этап 1/2: Проверка доступа в интернет...")
        await page.goto("https://api.ipify.org", timeout=20_000, wait_until="domcontentloaded")
        content = await page.text_content()
        if not re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", content.strip()):
            logging.warning(f"ОШИБКА: Прокси {proxy_url} вернул неожиданный ответ: {content}")
            return False
        logging.info(f"[{proxy_url}] Этап 1/2: Успешно. IP: {content.strip()}")

        # Этап 2: Проверка доступа к Solscan
        logging.info(f"[{proxy_url}] Этап 2/2: Проверка доступа к Solscan...")
        await page.goto("https://solscan.io", timeout=30_000, wait_until="domcontentloaded")
        # Ищем элемент, который точно есть на главной странице Solscan
        await page.wait_for_selector("input[placeholder='Search for Txn, Addr, Block, Token...']", timeout=15_000)
        logging.info(f"УСПЕХ: Прокси {proxy_url} работает и имеет доступ к Solscan.")
        return True

    except Exception as e:
        logging.warning(f"ОШИБКА: Прокси {proxy_url} не прошел проверку: {str(e).splitlines()[0]}")
        return False
    finally:
        if browser:
            await browser.close()

# ──────────── Download helpers ────────────
async def click_export(page: Page) -> None:
    try:
        await page.get_by_role("button", name="Export CSV").click(timeout=15000)
    except PwTimeout:
        logging.warning("Standard 'Export CSV' button not found, trying SVG icon.")
        await page.locator("svg[data-tooltip-id='export']").first.click()

async def click_dialog_download(page: Page) -> str | None:
    try:
        async with page.expect_download(timeout=60_000) as info:
            await page.locator("button:has(svg.lucide-cloud-download)").click()
        download = await info.value
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = os.path.join(DOWNLOAD_DIR, f"{ts}_{download.suggested_filename}")
        await download.save_as(path)
        logging.info("CSV saved → %s", path)
        return path
    except PwTimeout:
        logging.error("Timeout waiting for Download dialog button.")
        return None

async def grab_csv(address: str, browser: Browser) -> pd.DataFrame | None:
    url = (
        f"https://solscan.io/account/{address}"
        "?exclude_amount_zero=false&remove_spam=false&flow=out"
        "&token_address=So11111111111111111111111111111111111111111#transfers"
    )
    page = None
    ctx = None
    try:
        ctx = await browser.new_context(user_agent=next(_ua_cycle), accept_downloads=True)
        page = await ctx.new_page()
        await page.route(BLOCK_RESOURCE_PATTERN, block_unnecessary_requests)
        
        logging.info("Navigating to Solscan for address %s", address)
        await page.goto(url, timeout=120_000, wait_until="domcontentloaded")
        
        await page.wait_for_selector("button:has-text('Export CSV'), div.text-sm.text-gray-500:has-text('Tx Hash')", timeout=120_000)
        logging.info("Page content loaded, ready to export.")

        await click_export(page)
        path = await click_dialog_download(page)
        
        if not path: return None
            
        df = pd.read_csv(path)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        if "block_time" in df.columns:
            df["block_time"] = df["block_time"].apply(_to_dt)
        return df
    except PwTimeout as e:
        logging.error("Playwright Timeout for %s: %s", address, str(e).split('\n')[0])
        return None
    except Exception as e:
        logging.error("General error in grab_csv for %s: %s", address, e)
        return None
    finally:
        if page: await page.close()
        if ctx: await ctx.close()

# ──────────── Helper functions ────────────
def latest_ts(addr: str) -> datetime | None:
    res = sb.table("tracked_transactions").select("block_time").eq("tracked_address", addr).order("block_time", desc=True).limit(1).execute()
    return datetime.fromisoformat(res.data[0]["block_time"]) if res.data else None

def filter_new(df: pd.DataFrame, addr: str) -> pd.DataFrame:
    last = latest_ts(addr)
    return df if last is None else df[df["block_time"] > last]

def upsert_to_supabase(df: pd.DataFrame, address: str):
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

    if "block_time" in df.columns:
        df["block_time"] = df["block_time"].apply(_to_dt).apply(lambda x: x.isoformat() if pd.notna(x) else None)

    df["tracked_address"] = address
    rows: List[dict[str, Any]] = df.to_dict("records")
    if not rows: return
    sb.table("tracked_transactions").insert(rows).execute()
    logging.info("Inserted %d new rows for %s", len(rows), address)
 
def _to_dt(x):
    if pd.isna(x): return pd.NaT
    if isinstance(x, (int, float, np.integer)): return datetime.fromtimestamp(int(x), tz=timezone.utc)
    if isinstance(x, pd.Timestamp): return x.to_pydatetime().replace(tzinfo=timezone.utc) if x.tzinfo is None else x.to_pydatetime()
    if isinstance(x, datetime): return x.replace(tzinfo=timezone.utc) if x.tzinfo is None else x
    try:
        return datetime.fromisoformat(str(x)).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return pd.NaT

# ──────────── MAIN LOOP ────────────
async def main():
    pw = await async_playwright().start()
    
    logging.info("Начинаю предварительную проверку всех прокси из списка...")
    working_proxies = []
    for proxy in PROXIES:
        if await check_proxy(pw, proxy):
            working_proxies.append(proxy)
    
    if not working_proxies:
        logging.error("Не найдено ни одного рабочего прокси. Воркер не может быть запущен. Проверьте список прокси.")
        await pw.stop()
        return

    logging.info(f"Проверка завершена. Найдено рабочих прокси: {len(working_proxies)} из {len(PROXIES)}.")
    _proxy_cycle = itertools.cycle(working_proxies)

    while True:
        res = sb.table("address_alerts").select("address_to_track").execute().data or []
        addresses = {row["address_to_track"] for row in res}
        if not addresses:
            logging.warning("No addresses to track in Supabase. Waiting...")
            await asyncio.sleep(POLL_INTERVAL * 2)
            continue

        for addr in addresses:
            df = None
            for attempt in range(MAX_RETRIES):
                browser = None
                proxy_url = next(_proxy_cycle) # Берем следующий рабочий прокси
                try:
                    logging.info("Processing address: %s (Attempt %d/%d via %s)", addr, attempt + 1, MAX_RETRIES, proxy_url)
                    browser = await pw.chromium.launch(headless=HEADLESS, proxy={"server": proxy_url})
                    df = await grab_csv(addr, browser)
                    if df is not None:
                        logging.info("Successfully fetched data for %s", addr)
                        break 
                except Exception as e:
                    logging.error("Critical error during attempt %d for %s: %s", attempt + 1, addr, e)
                finally:
                    if browser: await browser.close()
                
                if df is None:
                    logging.warning("Attempt %d failed for %s. Retrying with new proxy in 5 seconds...", attempt + 1, addr)
                    await asyncio.sleep(5)

            if df is not None:
                new_df = filter_new(df, addr)
                if not new_df.empty:
                    upsert_to_supabase(new_df, addr)
                else:
                    logging.info("No new transactions found for %s", addr)
            else:
                logging.error("All %d attempts failed for address %s. Skipping for this cycle.", MAX_RETRIES, addr)

        logging.info("Cycle finished. Waiting %d seconds for the next one.", POLL_INTERVAL)
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
