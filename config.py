# config.py
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")

# Пути
CHROME_PROFILE_PATH = os.path.abspath("chrome_profile")
FILES_DIR = os.path.abspath("pnl_files")
SWAPS_FILES_DIR = os.path.abspath("swaps_files")
TOP_TRADERS_DIR = os.path.abspath("top_traders_files") # Для новой функции
DOWNLOAD_DIR = os.path.abspath("downloads")

# Константы
TARGET_DM_URL = "https://discord.com/channels/@me/1331338750789419090"
MAX_ADDRESS_LIST_SIZE = 40000
MAX_TRACKING_TASKS_PER_USER = 5
TOKEN_CATEGORIES = ['new_creation', 'completed', 'completing']
TIME_PERIODS = {'1h': '1 час', '3h': '3 часа', '6h': '6 часов', '12h': '12 часов', '24h': '24 часа'}