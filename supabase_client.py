# supabase_client.py
from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

# Убедитесь, что вы создали .env файл и добавили в него эти переменные
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and Key must be set in your .env file!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)