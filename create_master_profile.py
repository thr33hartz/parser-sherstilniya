import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import config

# --- НАЧАЛО СКРИПТА ---

print("="*50)
print("Запускаю браузер для входа в Discord...")
print(f"Профиль будет сохранен в папку: {config.CHROME_PROFILE_PATH}")
print("="*50)

# Убедитесь, что папка существует
os.makedirs(config.CHROME_PROFILE_PATH, exist_ok=True)

opts = Options()
# ВАЖНО: Используем основной путь к профилю из конфига
opts.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
# Никакого headless режима, нам нужно видеть браузер
# Убираем аргументы, которые могут помешать отображению
# opts.add_argument("--headless=new") 

driver = None
try:
    driver = webdriver.Chrome(options=opts)
    driver.get("https://discord.com/login")
    
    print("\nПОЖАЛУЙСТА, ВЫПОЛНИТЕ СЛЕДУЮЩИЕ ДЕЙСТВИЯ В ОТКРЫВШЕМСЯ ОКНЕ CHROME:")
    print("1. Войдите в свой аккаунт Discord (используйте email/пароль).")
    print("2. Если появится QR-код, нажмите 'Войти с помощью email'.")
    print("3. Пройдите все проверки (капчи и т.д.).")
    print("4. Обязательно поставьте галочку 'Запомнить меня', если она есть.")
    print("5. После успешного входа, когда увидите интерфейс Discord, вы можете закрыть окно браузера.")
    print("\nСкрипт будет ждать 5 минут, чтобы вы успели войти.")
    
    time.sleep(300) # Даем 5 минут на вход

except Exception as e:
    print(f"\nКРИТИЧЕСКАЯ ОШИБКА: Не удалось запустить браузер. Убедитесь, что Chrome и chromedriver установлены.")
    print(f"Текст ошибки: {e}")
finally:
    if driver:
        driver.quit()
    print("Браузер закрыт. Мастер-профиль должен быть готов.")