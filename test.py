import sys
import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Импортируем нашу основную функцию и конфиг
import config
from workers.get_trader_pnl import perform_pnl_fetch

# Настройка логгера для теста
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")

def init_test_driver():
    """Создает видимый (не headless) драйвер для теста."""
    opts = Options()
    opts.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    
    # Настройки для автоматического скачивания
    prefs = {"download.default_directory": config.DOWNLOAD_DIR}
    opts.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=opts)
    return driver

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Ошибка: Пожалуйста, укажите путь к .txt файлу с адресами.")
        print("Пример: python test_pnl_fetch.py my_wallets.txt")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Ошибка: Файл не найден по пути {file_path}")
        sys.exit(1)

    with open(file_path, 'r') as f:
        traders = [line.strip() for line in f if line.strip()]

    if not traders:
        print("Файл с адресами пуст.")
        sys.exit(1)

    print(f"Запускаю тест для {len(traders)} адресов из файла {file_path}...")
    driver = init_test_driver()
    try:
        result_path = perform_pnl_fetch(driver, traders)
        if result_path:
            print(f"\n--- УСПЕХ! ---")
            print(f"Финальный файл сохранен по пути: {result_path}")
        else:
            print(f"\n--- НЕУДАЧА! ---")
            print("Скрипт не смог получить файл. Проверьте лог выше и скриншот ошибки (если он был создан).")
    finally:
        print("Закрываю драйвер через 10 секунд...")
        time.sleep(10)
        driver.quit()