"""
Мост между асинхронным ботом и синхронными Selenium-задачами.

Этот модуль предоставляет асинхронные функции-обертки, которые безопасно
(с использованием блокировки) запускают долгие, блокирующие Selenium-скрипты
в отдельном потоке, не замораживая основной процесс бота.
"""

import asyncio
from typing import Optional, List

# --- Контекст приложения ---
# Импортируем общие, разделяемые объекты: драйвер и блокировку
from app_context import driver, driver_lock

# --- Функции-исполнители из папки workers ---
# Каждый исполнитель отвечает за одну конкретную задачу в Discord.
from workers.get_trader_pnl import perform_pnl_fetch
from workers.get_program_swaps import perform_program_swaps
from workers.get_top_traders import perform_toplevel_traders_fetch


async def fetch_pnl_via_discord(wallets: List[str]) -> Optional[str]:
    """
    Асинхронная обертка для получения PNL-статистики кошельков.

    Запускает `perform_pnl_fetch` в отдельном потоке, обеспечивая
    безопасный доступ к драйверу.
    """
    async with driver_lock:
        loop = asyncio.get_event_loop()
        # logger.info(f"Starting PNL fetch for {len(wallets)} wallets in executor.")
        result_path = await loop.run_in_executor(
            None,  # Используем стандартный ThreadPoolExecutor
            lambda: perform_pnl_fetch(driver, wallets)
        )
        # logger.info(f"PNL fetch finished. Result path: {result_path}")
        return result_path


async def fetch_swaps_via_discord(program: str, interval: str) -> Optional[str]:
    """
    Асинхронная обертка для получения Program Swaps.

    Запускает `perform_program_swaps` в отдельном потоке.
    """
    async with driver_lock:
        loop = asyncio.get_event_loop()
        # logger.info(f"Starting Program Swaps fetch for {program} ({interval}).")
        result_path = await loop.run_in_executor(
            None,
            lambda: perform_program_swaps(driver, program, interval)
        )
        # logger.info(f"Program Swaps fetch finished. Result path: {result_path}")
        return result_path


async def fetch_traders_via_discord(file_path: str) -> Optional[str]:
    """
    Асинхронная обертка для получения Топ-трейдеров по списку токенов.

    Запускает `perform_toplevel_traders_fetch` в отдельном потоке.
    """
    async with driver_lock:
        loop = asyncio.get_event_loop()
        # logger.info(f"Starting Top Traders fetch using file: {file_path}.")
        result_path = await loop.run_in_executor(
            None,
            lambda: perform_toplevel_traders_fetch(driver, file_path)
        )
        # logger.info(f"Top Traders fetch finished. Result path: {result_path}")
        return result_path