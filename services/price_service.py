# services/price_service.py
import httpx
from typing import Optional

async def get_sol_price() -> Optional[float]:
    """
    Получает текущую цену Solana в USD с CoinGecko API.
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "solana",
        "vs_currencies": "usd"
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            response.raise_for_status() # Проверяем на ошибки
            data = response.json()
            price = data.get("solana", {}).get("usd")
            return float(price) if price else None
    except Exception as e:
        print(f"PRICE_SERVICE_ERROR: Could not fetch SOL price: {e}")
        return None