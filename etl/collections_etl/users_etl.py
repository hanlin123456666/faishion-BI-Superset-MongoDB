import pandas as pd
from sqlalchemy import TEXT, DATETIME, BOOLEAN
from tqdm import tqdm
from bson import ObjectId


def extract_user_id(user_doc):
    """
    Convert Mongo ObjectId to hex string
    """
    _id = user_doc.get("_id")
    if isinstance(_id, ObjectId):
        return str(_id)
    return str(_id) if _id else None


def run_users_etl(mongo_db, engine):
    collection = mongo_db["users"]

    total_docs = collection.count_documents({})
    print(f"Dumping users → auth_users ({total_docs} docs)")

    if total_docs == 0:
        print("No users found")
        return

    cursor = collection.find({})

    rows = []

    for doc in tqdm(cursor, total=total_docs):
        rows.append({
            # 🔑 primary user identifier
            "user_id": extract_user_id(doc),

            # 📧 new field
            "email": doc.get("email"),

            # existing fields
            "isEmailVerified": doc.get("isEmailVerified"),
            "createdAt": doc.get("createdAt"),
            "updatedAt": doc.get("updatedAt")
        })

    df = pd.DataFrame(rows)

    df.to_sql(
        "auth_users",
        engine,
        if_exists="replace",   # dimension table: safe to rebuild
        index=False,
        dtype={
            "user_id": TEXT(),
            "email": TEXT(),
            "isEmailVerified": BOOLEAN(),
            "createdAt": DATETIME(),
            "updatedAt": DATETIME()
        }
    )

    print("Users ETL complete")
