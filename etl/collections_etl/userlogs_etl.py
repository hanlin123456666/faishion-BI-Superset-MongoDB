import pandas as pd
from datetime import datetime
from sqlalchemy import TEXT, DATETIME, DATE, INTEGER
from tqdm import tqdm
from bson import ObjectId


# =========================
# Helpers
# =========================

def extract_user_id(doc):
    """
    Resolve user id with priority:
    1. user (ObjectId or string)
    2. userId (string)
    """
    user = doc.get("user")
    if isinstance(user, ObjectId):
        return str(user)
    if isinstance(user, str) and user.strip():
        return user

    user_id = doc.get("userId")
    if isinstance(user_id, str) and user_id.strip():
        return user_id

    return None


def safe_text(value, max_len=None):
    """
    Convert value to safe string.
    Optionally truncate to max_len (defensive layer).
    """
    if value is None:
        return None

    text = str(value)
    if max_len:
        return text[:max_len]

    return text


def safe_timestamp(value):
    """
    Ensure timestamp is a datetime object.
    """
    if isinstance(value, datetime):
        return value
    return None


# =========================
# ETL main
# =========================

def run_userlogs_etl(mongo_db, engine, state):
    collection = mongo_db["userlogs"]

    last_ts = datetime.fromisoformat(state["userlogs_last_timestamp"])
    query = {"timestamp": {"$gt": last_ts}}

    total_docs = collection.count_documents(query)
    print(f"Dumping userlogs → auth_userlogs ({total_docs} docs)")

    if total_docs == 0:
        print("No new userlogs")
        return state

    cursor = collection.find(query).sort("timestamp", 1)

    rows = []
    max_ts = last_ts

    for doc in tqdm(cursor, total=total_docs):
        ts = safe_timestamp(doc.get("timestamp"))
        if ts:
            max_ts = max(max_ts, ts)

        rows.append({
            # user
            "user": extract_user_id(doc),

            # action
            "action": safe_text(doc.get("action")),
            "actionType": safe_text(doc.get("actionType")),

            # url & brand
            # TEXT in MySQL, but still protect against insane payloads
            "url": safe_text(doc.get("url"), max_len=8000),
            "brandName": safe_text(doc.get("brandName")),

            # error info
            "error_code": doc.get("error_code"),
            "error_message": safe_text(doc.get("error_message"), max_len=8000),

            # time
            "timestamp": ts,
            "event_date": ts.date() if ts else None
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No valid rows after parsing")
        return state

    df.to_sql(
        name="auth_userlogs",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        dtype={
            "user": TEXT(),
            "action": TEXT(),
            "actionType": TEXT(),
            "url": TEXT(),            # must match MySQL TEXT
            "brandName": TEXT(),
            "error_code": INTEGER(),  # better than FLOAT
            "error_message": TEXT(),
            "timestamp": DATETIME(),
            "event_date": DATE()
        }
    )

    state["userlogs_last_timestamp"] = max_ts.isoformat()
    print(f"Userlogs ETL complete up to {max_ts}")

    return state
