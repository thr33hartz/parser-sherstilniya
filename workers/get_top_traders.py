# workers/get_top_traders.py

from __future__ import annotations
import logging
import os
import time
import uuid
import requests
import zipfile
import io
from typing import Optional

from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Константы / Селекторы ---
TARGET_DM_URL = "https://discord.com/channels/@me/1331338750789419090"

# ИСПОЛЬЗУЕМ ТУ ЖЕ ЛОГИКУ, ЧТО И В get_trader_pnl.py
COMMANDS_BUTTON_SELECTOR = "button[class*='entryPointAppCommandButton']"
# ВАШЕ ПРЕДПОЛОЖЕНИЕ - ИСПОЛЬЗУЕМ ARIA-LABEL
TOPTRADERS_COMMAND_SELECTOR = "button[aria-label='Send toptraders']" 
FILE_INPUT_SELECTOR = "input[type='file']"
MESSAGE_TEXTBOX_SELECTOR = "div[role='textbox']"
DOWNLOAD_BUTTON_SELECTOR = 'a[class*="anchor_"][aria-label="Download"]'

TRADERS_DIR = os.path.abspath("top_traders_files")
os.makedirs(TRADERS_DIR, exist_ok=True)
logger = logging.getLogger(__name__)


def perform_toplevel_traders_fetch(driver, addresses_filepath: str, timeout: int = 400) -> Optional[str]:
    """
    Запрашивает топ-трейдеров, используя "умную" навигацию и JS-клики.
    """
    try:
        driver.get(TARGET_DM_URL)
        wait = WebDriverWait(driver, 20)
        long_wait = WebDriverWait(driver, timeout)
        
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "main[class*='chatContent']")))
        logger.info("SELENIUM(Traders): Discord DM loaded.")
        
        # ИСПРАВЛЕНИЕ: Возвращаем эту строку на место
        initial_link_cnt = len(driver.find_elements(By.CSS_SELECTOR, DOWNLOAD_BUTTON_SELECTOR))
        
        # --- УМНАЯ ЛОГИКА НАВИГАЦИИ ---
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, COMMANDS_BUTTON_SELECTOR))).click()
        time.sleep(1)
        
        try:
            command_button = driver.find_element(By.CSS_SELECTOR, TOPTRADERS_COMMAND_SELECTOR)
            logger.info("SELENIUM(Traders): Command button found directly.")
        except NoSuchElementException:
            logger.info("SELENIUM(Traders): Command not found, clicking bot icon via JavaScript...")
            bot_icon_selector = "div[aria-label*='Wallet Master+']"
            bot_icon_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, bot_icon_selector)))
            driver.execute_script("arguments[0].click();", bot_icon_element)
            time.sleep(1)
            command_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, TOPTRADERS_COMMAND_SELECTOR)))

        command_button.click()
        logger.info("SELENIUM(Traders): Clicked 'Send toptraders' button.")
        
        # --- Остальная логика без изменений ---
        file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, FILE_INPUT_SELECTOR)))
        file_input.send_keys(addresses_filepath)
        time.sleep(4)
        msg_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, MESSAGE_TEXTBOX_SELECTOR)))
        msg_box.send_keys(Keys.ENTER)
        logger.info("SELENIUM(Traders): ENTER sent. Waiting for response...")
        
        # Теперь эта строка будет работать, так как initial_link_cnt определена
        long_wait.until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, DOWNLOAD_BUTTON_SELECTOR)) > initial_link_cnt
        )
        
        download_link_el = driver.find_elements(By.CSS_SELECTOR, DOWNLOAD_BUTTON_SELECTOR)[-1]
        download_url = download_link_el.get_attribute("href")
        
        logger.info("SELENIUM(Traders): Bot response detected. URL: %s", download_url)
        resp = requests.get(download_url, timeout=120)
        resp.raise_for_status()

        # ИСПРАВЛЕННЫЙ БЛОК
        if ".zip" in download_url:
            logger.info("SELENIUM(Traders): Detected ZIP file. Merging...")
            all_wallets_text = []
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for filename_in_zip in zf.namelist():
                    if filename_in_zip.endswith('.txt'):
                        content = zf.read(filename_in_zip).decode('utf-8').strip()
                        if content:
                            all_wallets_text.append(content)

            final_text = "\n".join(all_wallets_text)
            final_filename = f"top_traders_merged_{uuid.uuid4()}.txt"
            save_path = os.path.join(TRADERS_DIR, final_filename)
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(final_text)
            logger.info("SELENIUM(Traders): All txt from zip merged into %s", save_path)
            return save_path
        else:
            final_filename = f"top_traders_{uuid.uuid4()}.txt"
            save_path = os.path.join(TRADERS_DIR, final_filename)
            with open(save_path, "wb") as f:
                f.write(resp.content)
            logger.info("SELENIUM(Traders): TXT file saved to %s", save_path)
            return save_path

    except Exception as exc:
        logger.error("SELENIUM(Traders): A critical error occurred: %s", exc, exc_info=True)
        return None