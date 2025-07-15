# services/price_service.py
import httpx
from typing import Optional

_last_sol_price: Optional[float] = None

async def get_sol_price() -> Optional[float]:
    global _last_sol_price
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "solana",
        "vs_currencies": "usd"
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            price = data.get("solana", {}).get("usd")
            if price:
                _last_sol_price = float(price)
                return _last_sol_price
    except Exception as e:
        print(f"PRICE_SERVICE_ERROR: {e}")
        return _last_sol_price  # возвращаем последнюю успешную цену