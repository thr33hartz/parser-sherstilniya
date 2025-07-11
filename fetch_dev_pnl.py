# fetch_dev_pnl.py

import asyncio
import cloudscraper
import random
from datetime import datetime, timezone

# Ваши параметры из логов
API_PARAMS = {
    "device_id": "641b379a-48a1-48d4-8778-f469914782af",
    "client_id": "gmgn_web_20250706-810-d6e015c",
    "from_app": "gmgn",
    "app_ver": "20250706-810-d6e015c"
}
BASE_URL = "https://gmgn.ai/api/v1"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15",
    "Referer": "https://gmgn.ai/"
}

# ... (вспомогательные функции _parse_pnl_stats и _parse_token_stats остаются без изменений) ...
def _parse_pnl_stats(data: dict) -> dict:
    stats = data.get('data', {})
    winrate_ratio = stats.get("winrate")
    return { "pnl_1d_usd": stats.get("realized_profit_1d"), "pnl_7d_usd": stats.get("realized_profit_7d"), "pnl_30d_usd": stats.get("realized_profit_30d"), "winrate": round(winrate_ratio * 100, 2) if winrate_ratio is not None else None }
def _parse_token_stats(data: dict, dev_address: str) -> (dict, list):
    stats_data = data.get("data", {})
    total_launched = stats_data.get("inner_count", 0)
    migrated_count = stats_data.get("open_count", 0)
    try: migration_perc = round(float(stats_data.get("open_ratio", "0")) * 100, 2)
    except (ValueError, TypeError): migration_perc = 0.0
    stats_update = { "total_launched": total_launched, "migrated_count": migrated_count, "non_migrated_count": total_launched - migrated_count, "migration_percentage": migration_perc, }
    deployed_tokens = []
    token_list = stats_data.get("tokens", [])
    if token_list and isinstance(token_list, list):
        if stats_data.get("last_create_timestamp"):
            stats_update["latest_coin_launched_text"] = datetime.fromtimestamp(stats_data["last_create_timestamp"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
        for token in token_list:
            deployed_tokens.append({ "developer_address": dev_address, "token_address": token.get("token_address"), "symbol": token.get("symbol"), "migrated": token.get("is_open"), "market_cap_usd": token.get("marcket_cap"), "liquidity_sol": token.get("pool_liquidity"), "holders": token.get("holders"), "volume_1h_usd": token.get("volume_1h"), "created_at": datetime.fromtimestamp(token["create_timestamp"], tz=timezone.utc).isoformat() if token.get("create_timestamp") else None, })
    return stats_update, deployed_tokens


def fetch_sync_with_scraper(url: str, params: dict):
    """Синхронная функция для выполнения GET-запроса через cloudscraper."""
    scraper = cloudscraper.create_scraper()
    response = scraper.get(url, params=params, headers=HEADERS)
    response.raise_for_status()
    return response.json()

async def fetch_dev_data_from_api(developer_address: str) -> (dict, list):
    """
    Получает всю информацию по одному разработчику, используя cloudscraper.
    """
    if not developer_address:
        return None, None
        
    loop = asyncio.get_event_loop()
    final_stats = {"developer_address": developer_address}
    deployed_tokens_list = []
    
    try:
        # Запрос №1: PNL
        pnl_url = f"{BASE_URL}/wallet_stat/sol/{developer_address}/all"
        pnl_params = {**API_PARAMS, "r": random.randint(100000, 999999)}
        pnl_data = await loop.run_in_executor(None, fetch_sync_with_scraper, pnl_url, pnl_params)
        if pnl_data.get("code") == 0:
            final_stats.update(_parse_pnl_stats(pnl_data))
        else:
            print(f"DEV_API_FETCH: PNL request returned non-zero code for {developer_address}")
            
    except Exception as e:
        print(f"DEV_API_FETCH: PNL request failed for {developer_address}: {e}")

    try:
        # Запрос №2: Токены
        tokens_url = f"{BASE_URL}/dev_created_tokens/sol/{developer_address}"
        tokens_params = {**API_PARAMS, "r": random.randint(100000, 999999)}
        tokens_data = await loop.run_in_executor(None, fetch_sync_with_scraper, tokens_url, tokens_params)
        if tokens_data.get("code") == 0:
            token_stats, deployed_tokens = _parse_token_stats(tokens_data, developer_address)
            final_stats.update(token_stats)
            deployed_tokens_list = deployed_tokens
        else:
            print(f"DEV_API_FETCH: Tokens request returned non-zero code for {developer_address}")

    except Exception as e:
        print(f"DEV_API_FETCH: Tokens request failed for {developer_address}: {e}")

    return final_stats, deployed_tokens_list