from supabase import create_client
import os, datetime

_url  = os.environ["SUPABASE_URL"]
_key  = os.environ["SUPABASE_KEY"]
_sb   = create_client(_url, _key)

def check_code(code: str):
    """Вернёт row либо None."""
    res = _sb.table("access_codes").select("*").eq("code", code).single().execute()
    return res.data if res.data else None

def mark_code_used(code: str, tg_id: int):
    _sb.table("access_codes").update({
        "used": True,
        "used_by": tg_id,
        "used_at": datetime.datetime.utcnow().isoformat()
    }).eq("code", code).execute()


def is_premium_user(tg_id: int) -> bool:
    """
    True, если в access_codes есть строка
    used_by == tg_id  И  used == TRUE
    """
    resp = (
        _sb.table("access_codes")
           .select("code")     # using existing column, table has no "id"
           .eq("used_by", tg_id)
           .eq("used", True)
           .limit(1)
           .execute()
    )
    return bool(resp.data)

def user_is_premium(tg_id: int) -> bool:
    """
    True  → пользователь премиум  
    False → строки нет или флаг = False
    """
    try:
        res = (
            _sb.table("users")
            .select("is_premium")
            .eq("id", tg_id)
            .maybe_single()      # ← не бросает, но может вернуть None
            .execute()
        )
    except Exception as exc:
        # если таблицы users нет, логируем и идём к резервной проверке
        logger.warning("users lookup failed: %s", exc)
        res = None

    if res and res.data:
        return bool(res.data.get("is_premium"))

    # ‼️ резервный путь: считаем премиум, если есть использованный код
    alt = (
        _sb.table("access_codes")
        .select("used")
        .eq("used_by", tg_id)
        .eq("used", True)
        .limit(1)
        .execute()
    )
    return bool(alt.data)