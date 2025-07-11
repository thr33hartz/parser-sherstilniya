"""
get_program_swaps.py
Selenium-помощник для команды /programswaps (Program Parse).

Пример использования из Telegram-бота:
    from get_program_swaps import perform_program_swaps
    file_path = perform_program_swaps(driver, program="9Uuu…h1k", interval="6h")
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Optional

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

        # ── Шаг 2. Ждём появления НОВОЙ ссылки на .csv
        def _csv_ready(drv):
            return len(drv.find_elements(By.CSS_SELECTOR,
                                         ATTACHMENT_LINK_SELECTOR)) > initial_cnt

        WebDriverWait(driver, timeout).until(_csv_ready)
        csv_link_el = driver.find_elements(By.CSS_SELECTOR,
                                           ATTACHMENT_LINK_SELECTOR)[-1]
        download_url = csv_link_el.get_attribute("href")
        logger.info("CSV link detected: %s", download_url)

        # ── Шаг 3. Скачиваем файл
        resp = requests.get(download_url, timeout=60)
        resp.raise_for_status()
        filename = f"swaps_{program[:6]}_{interval}_{uuid.uuid4()}.csv"
        save_path = os.path.join(SWAPS_DIR, filename)
        with open(save_path, "wb") as f:
            f.write(resp.content)
        logger.info("CSV saved to %s", save_path)
        return save_path

    except (TimeoutException, NoSuchElementException) as exc:
        logger.error("ProgramSwaps failed: %s", exc, exc_info=True)
        return None