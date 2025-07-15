"""
get_program_swaps.py
Selenium-помощник для команды /programswaps (Program Parse).

Пример использования из Telegram-бота:
    from get_program_swaps import perform_program_swaps
    file_path = perform_program_swaps(driver, program="9Uuu…h1k", interval="6h")
"""

from __future__ import annotations
import pandas as pd
from datetime import datetime
import logging
import os
import time
from typing import Optional
import io

import requests
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ──────────────────────────── константы / селекторы ───────────────────────── #
TARGET_DM_URL               = "https://discord.com/channels/@me/1331338750789419090"

COMMANDS_BUTTON_SELECTOR    = "button[class*='entryPointAppCommandButton']"
PROGRAMSWAPS_COMMAND_SEL    = "//div[@role='button' and .//div[text()='programswaps']]"

MESSAGE_TEXTBOX_SELECTOR    = "div[role='textbox']"
ATTACHMENT_LINK_SELECTOR    = "a[href*='cdn.discordapp.com/attachments'][href*='.csv']"

# папка, куда будем сохранять CSV
SWAPS_DIR = os.path.abspath("swaps_files")
os.makedirs(SWAPS_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


# ──────────────────────────────── основная функция ────────────────────────── #
def perform_program_swaps(driver, program: str, interval: str,
                          timeout: int = 300) -> Optional[str]:
    """
    Запрашивает у Discord-бота WalletMaster swaps по указанной программе.

    Parameters
    ----------
    driver   : selenium.webdriver.Chrome
        Предварительно авторизованный драйвер с Discord-профилем.
    program  : str
        Адрес программы (контракт) Solana.
    interval : {'3h', '6h', '12h', '24h'}
        Тайм-фрейм для выгрузки.
    timeout  : int
        Сколько секунд ждать появления ответа от бота.

    Returns
    -------
    str | None
        Путь к скачанному *.csv* либо *None*, если что-то пошло не так.
    """
    if interval not in {"3h", "6h", "12h", "24h"}:
        logger.error("Unsupported interval: %s", interval)
        return None

    try:
        # ── Шаг 0. Открываем личку с ботом
        driver.get(TARGET_DM_URL)
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,
                                                   "main[class*='chatContent']")))
        time.sleep(2)                                  # маленький буфер
        logger.info("Discord DM loaded.")

        # считаем, сколько csv-линков было до нашего запроса
        initial_links = driver.find_elements(By.CSS_SELECTOR, ATTACHMENT_LINK_SELECTOR)
        initial_cnt   = len(initial_links)

        # ── Шаг 1. Вкладка **Commands** → **programswaps**
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR,
                                               COMMANDS_BUTTON_SELECTOR))).click()
        time.sleep(0.6)
        wait.until(EC.element_to_be_clickable((By.XPATH,
                                            PROGRAMSWAPS_COMMAND_SEL))).click()

        # фокус остался в текстбоксе; заполняем два поля: program → <Tab> → interval
        # ✅ ИСПРАВЛЕННЫЙ И БОЛЕЕ НАДЕЖНЫЙ БЛОК
        # Заполняем поля команды так же, как и раньше
        textbox = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, MESSAGE_TEXTBOX_SELECTOR)))
        textbox.send_keys(program)
        textbox.send_keys(Keys.TAB)
        textbox.send_keys(interval)

        # Даем интерфейсу Discord секунду на то, чтобы обработать ввод
        time.sleep(1)

        # Отправляем ENTER напрямую в активный элемент (поле interval),
        # что гораздо надежнее, чем отправлять его в общий контейнер.
        driver.switch_to.active_element.send_keys(Keys.ENTER)
        driver.switch_to.active_element.send_keys(Keys.ENTER)

        logger.info("Slash-command sent: %s / %s", program, interval)

        # ── Шаг 2. Ждём появления НОВОЙ ссылки на .csv (или кнопки Result)
        def detect_result_message(driver):
            all_messages = driver.find_elements(By.CSS_SELECTOR, "div[data-list-item-id^='chat-messages___']")
            if len(driver.find_elements(By.CSS_SELECTOR, ATTACHMENT_LINK_SELECTOR)) > initial_cnt:
                link_el = driver.find_elements(By.CSS_SELECTOR, ATTACHMENT_LINK_SELECTOR)[-1]
                return {"type": "direct_link", "url": link_el.get_attribute("href")}
            try:
                result_button = all_messages[-1].find_element(By.XPATH, ".//button[.//div[text()='Result']]")
                return {"type": "button", "element": result_button}
            except NoSuchElementException:
                return False

        result_info = WebDriverWait(driver, timeout).until(detect_result_message)

        if not result_info:
            raise TimeoutException("No result (csv or button) received.")

        if result_info["type"] == "direct_link":
            try:
                latest_links = driver.find_elements(By.CSS_SELECTOR, ATTACHMENT_LINK_SELECTOR)
                if not latest_links:
                    raise Exception("CSV link not found after expected time.")
                csv_link_el = latest_links[-1]
                download_url = csv_link_el.get_attribute("href")
                resp = requests.get(download_url, timeout=60)
                resp.raise_for_status()
                df = pd.read_csv(io.BytesIO(resp.content))

                # берём только signer, удаляем пустые и дубликаты
                signers = df["signer"].dropna().drop_duplicates()

                # формируем имя файла
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                filename  = f"program_parse_{interval}_{program[:6]}_{timestamp}.txt"
                save_path = os.path.join(SWAPS_DIR, filename)

                # сохраняем txt
                signers.to_csv(save_path, index=False, header=False)
                logger.info("Program Parse: signers saved to %s", save_path)
                return save_path
            except Exception as e:
                logger.error("Failed to retrieve or save CSV file: %s", e, exc_info=True)
                return None

        elif result_info["type"] == "button":
            result_button = result_info["element"]
            driver.execute_script("arguments[0].click();", result_button)
            visit_site_button = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, ".//button[.//span[text()='Visit site']]"))
            )
            driver.execute_script("arguments[0].click();", visit_site_button)

            # Ждем загрузки
            download_dir = os.path.abspath("downloads")
            end_time = time.time() + timeout
            while time.time() < end_time:
                downloaded_files = [f for f in os.listdir(download_dir) if not f.endswith('.crdownload')]
                if downloaded_files:
                    full_paths = [os.path.join(download_dir, f) for f in downloaded_files]
                    latest_file = max(full_paths, key=os.path.getctime)
                    logger.info("File downloaded: %s", latest_file)
                    return latest_file
                time.sleep(1)
            raise TimeoutException("File was not downloaded after clicking 'Visit site'.")

    except (TimeoutException, NoSuchElementException) as exc:
        logger.error("ProgramSwaps failed: %s", exc, exc_info=True)
        return None

# ─────────────────────────────── CLI helper ──────────────────────────────── #
if __name__ == "__main__":
    """
    Быстрый запуск из терминала:
        python workers/get_program_swaps.py
    Использует авторизованный Chrome‑профиль из config.CHROME_PROFILE_PATH
    и сохраняет txt‑файл со списком signer‑кошельков.
    """
    import config
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    PROGRAM  = "b1oodsU6wfkFxkXKU9hTXPzHisopCbE1NKP5RFQLy7e"
    INTERVAL = "24h"

    opts = Options()
    opts.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    print("[ProgramSwaps] Launching Chrome…")
    drv = webdriver.Chrome(options=opts)
    try:
        result_path = perform_program_swaps(drv, program=PROGRAM, interval=INTERVAL)
        if result_path:
            print(f"[ProgramSwaps] Success! File saved to: {result_path}")
        else:
            print("[ProgramSwaps] Failed to fetch swaps.")
    finally:
        drv.quit()
        print("[ProgramSwaps] Chrome closed.")