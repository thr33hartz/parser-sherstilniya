import requests
import json
import pandas as pd

def get_wallet_data_and_save_csv():
    """
    Отправляет POST-запрос на API, получает данные PnL и сохраняет их в CSV файл.
    """
    # ⚠️ Не забудь обновить URL на актуальный, если потребуется
    url = "https://app.walletmaster.tools/_api/radar/3406/pnl"

    params = {
        'page': '24',
        'limit': '50',
        'order_by': 'usd_profit_30d',
        'order_direction': 'desc',
        'hide_blacklisted': 'false'
    }

    # ⚠️ Cookie может устареть!
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': '_clsk=1jz971q%7C1752549042246%7C17%7C1%7Ci.clarity.ms%2Fcollect; beta-access=2; nuxt-session=Fe26.2**2afaab9c9a482b29af25d8bab7617313e85c19e95a432027f62df32055bbb211*BevwNE2MwT-V13PKTjPNkw*v3-ioMZZjFjl1Ei5lDU9YWCiDPPrSBKcbHvc6QCSbd-4bPwJ5p5MvsNUk29uumLSzsLdQO80uNBqNzr9byNpDUs2dNgg_ZuD0mB73gW1bfoVkTNRHXpp7E7wKA1KilYSZPd3IH1mZSt7if0ml_SzJxOb0aLMXjThCpXPOHzx1Z-fi3Had3PKrw7D7GKctw7kPQiqmlD1kCEp6Z8EAnTyuAKkg6LIyyFwAqtzPzcYJ1CX-lrUi2-v7QcXPM7p6CIjZwUnOdGJNuGf2zAEHSdbN99FQk8qbVNmScatEaz-DFfPHr89HR62tIkTOil_PRiTK5IV00y5GzRkvSdXbAoZxg1ICYPMLB58hU6oOq554MSSg7Q1fnJypcPcqW30ntHdcEV8hiwlzooxgsT3l2hOwpRGTGFeMpVYi4XOwYqtMUx0YCgfqSKGC2aF16oQrnWmqYO-JHBKcV4G6_feiRDglP6o45skuFSgLcF-kt7phVpF3hTzTQZlK-qTtC3incDjS4MNYDG7YDdD2e2FhKtHywgH0OhL6Hum-4p_2tQd4euu2DFZaWvoMGonVhr9Eq-hsbpKlq2vfmgUbtRDlLv_RyY6Q5ujCUEogEeQHc-TjNa0OWB_GVMVqHp_GNeRAM3e-Ejgz9VOgaDe6qvXSRUt-QCJUSs08uXjSU9OY3pfTJupIwkaaTBghM77pdgFd0Ja*1753153391406*338df7920b196f862c251aefc4d0a199da811d8eb2731559952e651dde90d06b*fI9mUKZiBngGYNgJjmtFjBzYk5nly9nCilHmUpMl-ww; _clck=26ydj%7C2%7Cfxm%7C0%7C2022; cookieConsent=accepted; cookieConsentInteracted=true',
        'Origin': 'https://app.walletmaster.tools',
        'Referer': 'https://app.walletmaster.tools/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15'
    }
    
    payload = {}

    try:
        response = requests.post(url, params=params, headers=headers, json=payload)
        
        if response.status_code == 200:
            print("✅ Запрос успешен! Статус: 200 OK")
            response_data = response.json()
            
            # --- НОВЫЙ КОД ДЛЯ СОЗДАНИЯ CSV ---
            # Извлекаем список кошельков из ответа
            pnl_data = response_data.get('data', {}).get('pnlData', [])
            
            if pnl_data:
                # Создаем DataFrame из списка словарей
                df = pd.DataFrame(pnl_data)
                
                # Имя для нашего CSV файла
                csv_filename = 'wallet_pnl_data.csv'
                
                # Сохраняем DataFrame в CSV, без индекса
                df.to_csv(csv_filename, index=False, encoding='utf-8')
                
                print(f"✅ Данные успешно сохранены в файл: {csv_filename}")
            else:
                print("⚠️ В ответе не найдены данные для сохранения (pnlData пуст).")
            # ------------------------------------
            
        else:
            print(f"❌ Ошибка! Статус: {response.status_code}")
            print("Ответ сервера:", response.text)

    except requests.exceptions.RequestException as e:
        print(f"🔥 Произошла ошибка при выполнении запроса: {e}")
    except KeyError:
        print("🔥 Ошибка: Не удалось найти ключ 'pnlData' в JSON-ответе.")
    except Exception as e:
        print(f"🔥 Произошла непредвиденная ошибка: {e}")


# Запускаем функцию
if __name__ == "__main__":
    get_wallet_data_and_save_csv()