import pandas as pd
from sqlalchemy import TEXT
from tqdm import tqdm
from bson import ObjectId


def extract_account_id(doc):
    """
    Convert Mongo ObjectId to hex string
    """
    _id = doc.get("_id")
    if isinstance(_id, ObjectId):
        return str(_id)
    return str(_id) if _id else None


def run_test_acct_etl(mongo_db, engine):
    collection = mongo_db["test_acct"]

    total_docs = collection.count_documents({})
    print(f"Dumping test_acct → auth_test_acct ({total_docs} docs)")

    if total_docs == 0:
        print("No test accounts found")
        return

    cursor = collection.find({})

    rows = []

    for doc in tqdm(cursor, total=total_docs):
        rows.append({
            # ✅ normalized primary key
            "account_id": extract_account_id(doc),

            # required fields
            "email": doc.get("email"),
            "type": doc.get("type")
        })

    df = pd.DataFrame(rows)

    df.to_sql(
        "auth_test_acct",
        engine,
        if_exists="replace",   # dimension table
        index=False,
        dtype={
            "account_id": TEXT(),
            "email": TEXT(),
            "type": TEXT()
        }
    )

    print("Test account ETL complete")
