"""get_trader_pnl.py
A robust selenium helper that uploads a list of wallets to the Discord bot, waits for the
reply with a CSV attachment and returns the final, post-processed file path.
The module assumes **driver** is an *already launched* ``selenium.webdriver.Chrome``
instance with a Discord profile that has access to DM with the PNL bot.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from typing import List, Optional

import pandas as pd
import requests
from selenium.common.exceptions import (NoSuchElementException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import config

# ────────────────────────── Constants / selectors ────────────────────────── #
# ────────────────────────── Constants / selectors ────────────────────────── #
TARGET_DM_URL = "https://discord.com/channels/@me/1331338750789419090"
CHROME_PROFILE_PATH = os.path.abspath("chrome_profile")  # kept for reference
FILES_DIR = os.path.abspath("pnl_files")
MAX_ADDRESS_LIST_SIZE = 40000  # <--- ДОБАВЬТЕ ЭТУ СТРОКУ
MESSAGE_LIST_ITEM_SELECTOR = "div[data-list-item-id^='chat-messages___']"

COMMANDS_BUTTON_SELECTOR = "button[class*='entryPointAppCommandButton']"
PNLPLUS_COMMAND_SELECTOR = "button[aria-label='Send pnlplus']"
MESSAGE_TEXTBOX_SELECTOR = "div[role='textbox']"
ATTACHMENT_LINK_SELECTOR = "a[href*='cdn.discordapp.com/attachments'][href*='.csv']"
FILE_INPUT_SELECTOR = "input[type='file']"
RESULT_BUTTON_SELECTOR = ".//button[.//div[text()='Result']]"
VISIT_SITE_BUTTON_SELECTOR = ".//button[.//div[text()='Visit site']]"

# ensure output dir exists
os.makedirs(FILES_DIR, exist_ok=True)

# ───────────────────────────── Logging setup ─────────────────────────────── #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Helper functions ────────────────────────────── #

def _create_trader_list_file(traders: List[str]) -> str:
    """Save list of wallets to a temp txt and return absolute path."""
    filename = f"upload_traders_{uuid.uuid4()}.txt"
    path = os.path.join(FILES_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(traders))
    logger.info("SELENIUM: Created temp file: %s", path)
    return path


def _postprocess_csv(file_path: str) -> None:
    """Rename/ drop / reorder columns to match Wool Parser requirements."""
    logger.info("Applying final formatting to %s", file_path)

    column_mapping = {
        # original → new
        "address": "wallet",
        "sol_balance": "balance",
        "wsol_balance": "wsol_balance",
        "last_trade_timestamp": "last_trade_time",
        "roi_7d": "roi_7d",
        "roi_30d": "roi_30d",
        "winrate_7d": "winrate_7d",
        "winrate_30d": "winrate_30d",
        "unique_tokens_traded": "traded_tokens",
        "average_holding_time_seconds": "avg_holding_time",
        "usd_profit_7d": "usd_profit_7d",
        "usd_profit_30d": "usd_profit_30d",
        "average_swapped_token_age_7d": "avg_token_age_7d",
        "average_swapped_token_age_30d": "avg_token_age_30d",
        "top_three_profit_share_7d": "top_three_pnl",
        "buy_sell_in_10s_percent": "avg_quick_buy_and_sell_percentage",
        "bundled_token_buy_frequency": "avg_bundled_token_buys_percentage",
        "oversold_percentage": "avg_sold_more_than_bought_percentage",
        "average_first_purchase_mcap_7d": "avg_first_buy_mcap_7d",
        "buys_7d": "total_buys_7d",
        "pump_fun_buys_7d": "pf_buys_7d",
        "swap_pump_fun_buys_7d": "pf_swap_buys_7d",
        "bonk_fun_buys_7d": "bonk_buys_7d",
        "launch_lab_buys_7d": "raydium_buys_7d",
        "boop_fun_buys_7d": "boop_buys_7d",
        "meteora_dbc_buys_7d": "meteora_buys_7d",
        "average_first_purchase_mcap_30d": "avg_first_buy_mcap_30d",
        "buys_30d": "total_buys_30d",
        "pump_fun_buys_30d": "pf_buys_30d",
        "swap_pump_fun_buys_30d": "pf_swap_buys_30d",
        "bonk_fun_buys_30d": "bonk_buys_30d",
        "launch_lab_buys_30d": "raydium_buys_30d",
        "boop_fun_buys_30d": "boop_buys_30d",
        "meteora_dbc_buys_30d": "meteora_buys_30d",
        "average_buys_per_token_7d": "avg_buys_per_token_7d",
        "average_buys_per_token_30d": "avg_buys_per_token_30d",
        "sells_7d": "total_sells_7d",
        "sells_30d": "total_sells_30d",
        "average_jito_tip": "avg_forwarder_tip",
        "token_avg_cost_7d": "avg_token_cost_7d",
        "token_avg_cost_30d": "avg_token_cost_30d",
        "unrealised_profit_7d": "unrealised_pnl_7d",
        "unrealised_profit_30d": "unrealised_pnl_30d",
        "total_cost_7d": "total_cost_7d",
        "total_cost_30d": "total_cost_30d",
    }

    final_order = [
        "wallet",
        "balance",
        "wsol_balance",
        "last_trade_time",
        "roi_7d",
        "roi_30d",
        "winrate_7d",
        "winrate_30d",
        "traded_tokens",
        "avg_holding_time",
        "usd_profit_7d",
        "usd_profit_30d",
        "avg_token_age_7d",
        "avg_token_age_30d",
        "top_three_pnl",
        "avg_quick_buy_and_sell_percentage",
        "avg_bundled_token_buys_percentage",
        "avg_sold_more_than_bought_percentage",
        "avg_first_buy_mcap_7d",
        "total_buys_7d",
        "pf_buys_7d",
        "pf_swap_buys_7d",
        "bonk_buys_7d",
        "raydium_buys_7d",
        "boop_buys_7d",
        "meteora_buys_7d",
        "avg_first_buy_mcap_30d",
        "total_buys_30d",
        "pf_buys_30d",
        "pf_swap_buys_30d",
        "bonk_buys_30d",
        "raydium_buys_30d",
        "boop_buys_30d",
        "meteora_buys_30d",
        "avg_buys_per_token_7d",
        "avg_buys_per_token_30d",
        "total_sells_7d",
        "total_sells_30d",
        "avg_forwarder_tip",
        "avg_token_cost_7d",
        "avg_token_cost_30d",
        "unrealised_pnl_7d",
        "unrealised_pnl_30d",
        "total_cost_7d",
        "total_cost_30d",
    ]

    try:
        # Сначала пробуем стандартную и самую быструю
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        # Если не вышло, пробуем другую популярную кодировку
        logger.warning("UTF-8 decoding failed, trying 'utf-16'.")
        df = pd.read_csv(file_path, encoding='utf-16')

    # drop any '*median*' cols silently
    median_cols = [c for c in df.columns if "median" in c.lower()]
    if median_cols:
        df.drop(columns=median_cols, inplace=True)

    # keep/rename only mapped columns that are present
    present_originals = [c for c in column_mapping if c in df.columns]
    df = df[present_originals].rename(columns=column_mapping)

    # reorder & save
    df = df[[c for c in final_order if c in df.columns]]
    df.to_csv(file_path, index=False)
    logger.info("File formatting complete.")

def wait_for_download_and_get_path(timeout: int = 300) -> Optional[str]:
    """Ждет завершения скачивания файла в папке и возвращает его путь."""
    logger.info(f"Waiting for a file to download in '{config.DOWNLOAD_DIR}' for up to {timeout} seconds...")
    end_time = time.time() + timeout
    
    # Сначала очистим папку от старых .csv и .crdownload файлов
    for fname in os.listdir(config.DOWNLOAD_DIR):
        if fname.endswith(('.csv', '.crdownload', '.zip')):
            os.remove(os.path.join(config.DOWNLOAD_DIR, fname))
            logger.info(f"Removed old file: {fname}")

    while time.time() < end_time:
        # Ищем любой файл, который НЕ является временным файлом загрузки Chrome
        downloaded_files = [f for f in os.listdir(config.DOWNLOAD_DIR) if not f.endswith('.crdownload')]
        if downloaded_files:
            # Сортируем по времени изменения и берем самый новый
            full_paths = [os.path.join(config.DOWNLOAD_DIR, f) for f in downloaded_files]
            latest_file = max(full_paths, key=os.path.getctime)
            logger.info(f"Download complete. Found file: {latest_file}")
            return latest_file
        time.sleep(1)
        
    logger.error("File download timed out.")
    return None

# ───────────────────────────── Main routine ─────────────────────────────── #

def perform_pnl_fetch(driver, traders: List[str], timeout: int = 300) -> Optional[str]:
    """
    Запрашивает PNL, используя "умную" навигацию и JS-клики.
    """
    upload_file = None
    try:
        driver.get(TARGET_DM_URL)
        wait = WebDriverWait(driver, 20)
        long_wait = WebDriverWait(driver, timeout)
        
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "main[class*='chatContent']")))
        
        # --- УМНАЯ ЛОГИКА НАВИГАЦИИ ---
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, COMMANDS_BUTTON_SELECTOR))).click()
        time.sleep(1)

        try:
            command_button = driver.find_element(By.CSS_SELECTOR, PNLPLUS_COMMAND_SELECTOR)
            logger.info("SELENIUM(PNL): Command button found directly.")
        except NoSuchElementException:
            logger.info("SELENIUM(PNL): Command not found, clicking bot icon...")
            bot_icon_selector = "div[aria-label*='Wallet Master+']"
            bot_icon_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, bot_icon_selector)))
            
            # ИСПРАВЛЕНО: Используем JavaScript для клика
            driver.execute_script("arguments[0].click();", bot_icon_element)
            
            time.sleep(1)
            command_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, PNLPLUS_COMMAND_SELECTOR)))

        command_button.click()
        logger.info("SELENIUM(PNL): Clicked 'pnlplus' button.")

        # --- Остальная логика без изменений ---
        upload_file = _create_trader_list_file(traders)
        file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, FILE_INPUT_SELECTOR)))
        file_input.send_keys(upload_file)
        msg_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, MESSAGE_TEXTBOX_SELECTOR)))
        time.sleep(3)
        msg_box.send_keys(Keys.ENTER)
        logger.info("SELENIUM(PNL): Command sent. Waiting for bot response...")
        # --- ФИНАЛЬНАЯ ЛОГИКА ОЖИДАНИЯ И СКАЧИВАНИЯ ---
        
        # --- ФИНАЛЬНАЯ ЛОГИКА ОЖИДАНИЯ ---
        def find_new_result_and_process(driver):
            # На каждой итерации ожидания мы будем заново искать все сообщения
            all_messages = driver.find_elements(By.CSS_SELECTOR, MESSAGE_LIST_ITEM_SELECTOR)
            
            # Если нового сообщения нет, выходим и ждем дальше
            if not all_messages or len(all_messages) < initial_msg_cnt + 2:
                return False

            last_message = all_messages[-1]
            try:
                # Пытаемся найти прямую ссылку
                link_element = last_message.find_element(By.CSS_SELECTOR, ATTACHMENT_LINK_SELECTOR)
                download_url = link_element.get_attribute("href")
                
                # Если нашли - сразу скачиваем и возвращаем путь
                resp = requests.get(download_url, timeout=60)
                resp.raise_for_status()
                saved_path = os.path.join(FILES_DIR, f"pnl_{uuid.uuid4()}.csv")
                with open(saved_path, "wb") as f: f.write(resp.content)
                return saved_path

            except NoSuchElementException:
                # Если прямой ссылки нет, ищем кнопку "Result"
                try:
                    result_button = last_message.find_element(By.XPATH, RESULT_BUTTON_SELECTOR)
                    driver.execute_script("arguments[0].click();", result_button)
                    
                    # Ждем и кликаем "Visit site"
                    visit_site_button = wait.until(EC.element_to_be_clickable((By.XPATH, VISIT_SITE_BUTTON_SELECTOR)))
                    driver.execute_script("arguments[0].click();", visit_site_button)

                    # Ждем скачивания файла
                    downloaded_path = wait_for_download_and_get_path()
                    if not downloaded_path:
                        raise TimeoutException("File was not downloaded after clicking 'Visit Site'.")
                    return downloaded_path
                except (NoSuchElementException, TimeoutException):
                    # В последнем сообщении нет ни того, ни другого, ждем дальше
                    return False
        
        # --- Запоминаем количество сообщений и запускаем ожидание ---
        initial_msg_cnt = len(driver.find_elements(By.CSS_SELECTOR, MESSAGE_LIST_ITEM_SELECTOR))
        final_path = long_wait.until(find_new_result_and_process)
        
        if not final_path:
            raise Exception("Failed to download the file using any method.")

        logger.info("SELENIUM(PNL): File obtained and saved to %s", final_path)
        _postprocess_csv(final_path)
        return final_path

    except Exception as exc:
        logger.error("SELENIUM(PNL): A critical error occurred: %s", exc, exc_info=True)
        return None
    finally:
        if upload_file and os.path.exists(upload_file):
            os.remove(upload_file)