"""
Модуль Data Access Layer (DAL) для работы с Supabase.

Этот файл инкапсулирует все прямые запросы к базе данных.
Все функции здесь асинхронны и возвращают данные в виде
простых Python-объектов (списки, словари), не содержат
логики Telegram-бота.
"""

import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

from supabase_client import supabase

# --- Функции для работы с Шаблонами (Templates) ---

async def fetch_user_templates(user_id: int) -> List[Dict[str, Any]]:
    """Получает все шаблоны парсинга для указанного пользователя."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").select("*").eq("user_id", user_id).execute()
        )
        return response.data or []
    except Exception as e:
        print(f"DB_ERROR: fetch_user_templates failed for user {user_id}: {e}")
        return []

async def create_template(template_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Создает новый шаблон в базе данных."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").insert(template_data).execute()
        )
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"DB_ERROR: create_template failed: {e}")
        return None

async def update_template(template_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Обновляет существующий шаблон по его ID."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").update(updates).eq("id", template_id).execute()
        )
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"DB_ERROR: update_template failed for id {template_id}: {e}")
        return None

async def delete_template(template_id: str) -> bool:
    """Удаляет шаблон по его ID."""
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("parse_templates").delete().eq("id", template_id).execute()
        )
        return True
    except Exception as e:
        print(f"DB_ERROR: delete_template failed for id {template_id}: {e}")
        return False

# --- Функции для работы с Токенами ---

async def fetch_unique_launchpads() -> List[str]:
    """Получает список всех уникальных лаунчпадов из таблицы токенов."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("tokens").select("launchpad", count='exact').execute()
        )
        if response.data:
            return sorted(list(set(item['launchpad'] for item in response.data if item['launchpad'] and item['launchpad'] != 'unknown')))
        return []
    except Exception as e:
        print(f"DB_ERROR: fetch_unique_launchpads failed: {e}")
        return []

async def fetch_tokens_by_criteria(start_time: datetime, platforms: List[str], categories: List[str]) -> List[Dict[str, Any]]:
    """Выполняет поиск токенов по заданным критериям времени, платформы и категории."""
    try:
        query = supabase.table("tokens").select("contract_address, ticker, name, migration_time, launchpad, category")
        query = query.gte("migration_time", start_time.isoformat())

        if platforms:
            query = query.in_("launchpad", platforms)
        if categories:
            query = query.in_("category", categories)

        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: query.range(0, 10000).execute()
        )
        return response.data or []
    except Exception as e:
        print(f"DB_ERROR: fetch_tokens_by_criteria failed: {e}")
        return []

# --- Функции для работы со статистикой (Dev, Trader) ---

async def fetch_dev_stats_by_criteria(start_time: datetime, platforms: list, categories: list) -> list:
    """
    Вызывает SQL-функцию в Supabase для получения отфильтрованной статистики по разработчикам.
    """
    try:
        params = {
            'start_time_filter': start_time.isoformat(),
            'platforms_filter': platforms or None,
            'categories_filter': categories or None
        }
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.rpc('get_filtered_dev_stats', params).execute()
        )
        return response.data or []
    except Exception as e:
        print(f"DB_ERROR: fetch_dev_stats_by_criteria failed: {e}")
        return []
    
async def get_developer_stats(address: str) -> Optional[Dict[str, Any]]:
    """Получает статистику разработчика по адресу кошелька."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("developer_stats").select("*").eq("developer_address", address).limit(1).execute()
        )
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"DB_ERROR: get_developer_stats for {address} failed: {e}")
        return None

async def get_trader_stats(address: str) -> Optional[Dict[str, Any]]:
    """Получает статистику трейдера по адресу кошелька."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("trader_stats").select("*").eq("trader_address", address).limit(1).execute()
        )
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"DB_ERROR: get_trader_stats for {address} failed: {e}")
        return None

# --- Функции для Bundle Tracker ---

async def get_user_bundle_alerts(user_id: int) -> List[Dict[str, Any]]:
    """Получает все активные трекеры бандлов для пользователя."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("address_alerts")
                              .select("address_to_track, time_gap_min, min_cnt, amount_step, min_transfer_amount, max_transfer_amount, custom_name")
                              .eq("user_id", user_id).eq("is_active", True).execute()
        )
        return response.data or []
    except Exception as e:
        print(f"DB_ERROR: get_user_bundle_alerts for user {user_id} failed: {e}")
        return []

async def count_user_bundle_alerts(user_id: int) -> int:
    """Считает количество активных трекеров у пользователя."""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("address_alerts").select('id', count='exact').eq('user_id', user_id).eq('is_active', True).execute()
        )
        return response.count
    except Exception as e:
        print(f"DB_ERROR: count_user_bundle_alerts for user {user_id} failed: {e}")
        return 0

async def upsert_bundle_alert(alert_data: Dict[str, Any]) -> bool:
    """Создает или обновляет трекер бандлов."""
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("address_alerts").upsert(alert_data, on_conflict='user_id,address_to_track').execute()
        )
        return True
    except Exception as e:
        print(f"DB_ERROR: upsert_bundle_alert failed: {e}")
        return False

async def delete_bundle_alert(user_id: int, address_to_delete: str) -> bool:
    """Удаляет трекер бандлов для пользователя по адресу."""
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: supabase.table("address_alerts").delete().match({'address_to_track': address_to_delete, 'user_id': user_id}).execute()
        )
        return True
    except Exception as e:
        print(f"DB_ERROR: delete_bundle_alert failed for user {user_id}: {e}")
        return False
    
async def fetch_deployed_tokens_for_devs(developer_addresses: list) -> list:
    """
    Получает все записи из `dev_deployed_tokens` для заданного списка адресов разработчиков.
    """
    if not developer_addresses:
        return []
    try:
        # Разбиваем на пачки, чтобы не превысить лимит длины URL
        all_tokens = []
        batch_size = 300 
        for i in range(0, len(developer_addresses), batch_size):
            batch = developer_addresses[i:i + batch_size]
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda b=batch: supabase.table("dev_deployed_tokens")
                                      .select("*")
                                      .in_("developer_address", b)
                                      .execute()
            )
            if response.data:
                all_tokens.extend(response.data)
        return all_tokens
    except Exception as e:
        print(f"DB_ERROR: fetch_deployed_tokens_for_devs failed: {e}")
        return []