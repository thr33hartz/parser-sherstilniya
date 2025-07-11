import os, json, uuid, asyncio
import cloudscraper, requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase_client import supabase

# --- Load config ---
load_dotenv()
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()

API_PARAMS = {
    "device_id": os.getenv("DEVICE_ID"),
    "client_id": os.getenv("CLIENT_ID"),
    "from_app":  os.getenv("FROM_APP"),
    "app_ver":   os.getenv("APP_VER"),
    "tz_name":   os.getenv("TZ_NAME"),
    "tz_offset": os.getenv("TZ_OFFSET"),
    "app_lang":  os.getenv("APP_LANG"),
    "fp_did":    os.getenv("FP_DID"),
    "os":        os.getenv("OS","web"),
}

HEADERS = {
    "Accept":       os.getenv("ACCEPT","application/json, text/plain, */*"),
    "Referer":      os.getenv("REFERER","https://gmgn.ai/"),
    "User-Agent":   os.getenv("USER_AGENT","Mozilla/5.0 ..."),
    "Content-Type": os.getenv("CONTENT_TYPE","application/json"),
}

# limits
LIMIT   = int(os.getenv("TRADER_FETCH_CONCURRENCY_LIMIT","3"))
DELAY   = float(os.getenv("TRADER_REQUEST_DELAY_AFTER_TASK","2.0"))
ATTEMPTS= int(os.getenv("ABSOLUTE_MAX_ATTEMPTS_PER_TOKEN","3"))
RETRY403 = float(os.getenv("INITIAL_403_DELAY_S","10.0"))

async def insert_trader_batch(lst):
    if not lst: return
    await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: supabase.table("traders").insert(lst).execute()
    )
    print(f"Inserted {len(lst)} new traders")

async def get_existing(token_id):
    try:
        resp = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: supabase.table("traders")
                       .select("trader_address")
                       .eq("token_id", token_id)
                       .execute()
        )
        return {r["trader_address"] for r in resp.data}
    except:
        return set()

async def mark_processed(token_id):
    await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: supabase.table("tokens")
                   .update({"traders_last_fetched_at": datetime.now(timezone.utc).isoformat()})
                   .eq("id", token_id)
                   .execute()
    )
    print(f"Token {token_id} marked processed")

def fetch_sync(url, params, headers):
    sc = cloudscraper.create_scraper(browser={'browser':'chrome','platform':'windows','mobile':False})
    r = sc.get(url, params=params, headers=headers)
    r.raise_for_status()
    raw = r.text
    if raw.startswith(")]}',"):
        raw = raw.split('\n',1)[1]
    return json.loads(raw or "{}")

async def fetch_and_store_traders_for_one_token(token):
    tid = token.get("id"); addr = token.get("contract_address")
    if not tid or not addr: 
        return False

    url = f"https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/{addr}"
    params = {**API_PARAMS, "limit":100, "orderby":"realized_profit","direction":"desc"}

    for i in range(1, ATTEMPTS+1):
        tag = f"{addr} (try {i}/{ATTEMPTS})"
        try:
            data = await asyncio.get_event_loop().run_in_executor(None, fetch_sync, url, params, HEADERS)
            tr_list = data.get("data", {}).get("holders", {}).get("holderInfo", [])[:100]
            exist = await get_existing(tid)
            batch = [
                {"id": str(uuid.uuid4()), "token_id": tid, "trader_address": t.get("wallet_address")}
                for t in tr_list if t.get("wallet_address") and t["wallet_address"] not in exist
            ]
            if batch:
                await insert_trader_batch(batch)
            await mark_processed(tid)
            return True
        except (requests.exceptions.HTTPError, cloudscraper.exceptions.CloudflareChallengeError) as e:
            code = e.response.status_code if hasattr(e,'response') else 'CF'
            print(f"{tag} HTTP {code}: {e}")
            if i<ATTEMPTS:
                await asyncio.sleep(RETRY403)
                continue
            return False
        except Exception as e:
            print(f"{tag} unexpected: {e}")
            return False

    print(f"{addr}: exhausted attempts")
    return False

async def process_tokens_for_traders(tokens):
    if not tokens:
        print("Nothing to process")
        return
    sem = asyncio.Semaphore(LIMIT)
    async def worker(t):
        async with sem:
            await fetch_and_store_traders_for_one_token(t)
            await asyncio.sleep(DELAY)
    await asyncio.gather(*(worker(t) for t in tokens))
    print("Done traders batch")

# для отладки
if __name__=="__main__":
    import fetch_tokens
    tokens = asyncio.run(fetch_tokens.fetch_tokens())
    asyncio.run(process_tokens_for_traders(tokens))