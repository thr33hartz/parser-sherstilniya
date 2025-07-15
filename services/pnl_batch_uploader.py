import uuid, datetime, math
from supabase import create_client
from services import db_access   # чтобы не дублировать .env чтение

_sb = db_access._sb              # ре-используем готовый клиент

COLUMN_MAPPING = {...}           # тот dict, что вы показали
FINAL_ORDER     = [...]          # список «как в таблице»

CHUNK = 500                      # Upsert по 500 строк (ограничение PostgREST)

def _prep_batch_df(df):
    """→ DataFrame готовый к upsert’у (переименован + нужная сортировка)."""
    df = df.rename(columns=COLUMN_MAPPING).reindex(columns=FINAL_ORDER)
    return df

def upload_pnl_batch(df, batch_id=None, created_at=None):
    batch_id = batch_id or str(uuid.uuid4())
    created  = created_at or datetime.datetime.utcnow().isoformat()

    df = _prep_batch_df(df)
    df["batch_id"]         = batch_id
    df["batch_created_at"] = created

    # разбиваем на части, т.к. supabase limit = 1000
    for i in range(0, len(df), CHUNK):
        part = df.iloc[i:i+CHUNK].to_dict(orient="records")
        _sb.table("pnl_batches").upsert(part).execute()

    return batch_id