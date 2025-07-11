# app_context.py
import asyncio
from selenium import webdriver

# Эти переменные будут импортироваться и использоваться другими модулями.
driver: webdriver.Chrome | None = None
driver_lock = asyncio.Lock()