import sys
import json
from pymongo import MongoClient
from sqlalchemy import create_engine
from tqdm import tqdm

from config import MONGO_URI, MONGO_DB, MYSQL_URI

from collections_etl.userlogs_etl import run_userlogs_etl
from collections_etl.users_etl import run_users_etl
from collections_etl.test_acct import run_test_acct_etl


def load_state():
    with open("state.json", "r") as f:
        return json.load(f)


def save_state(state):
    with open("state.json", "w") as f:
        json.dump(state, f, indent=2)


def main():
    args = sys.argv[1:]  # e.g. ["userlogs"]

    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    engine = create_engine(MYSQL_URI)  # where to dump data 

    state = load_state()

    # Decide which ETL jobs to run
    jobs = []
    if not args or "userlogs" in args:
        jobs.append("userlogs")
    if not args or "users" in args:
        jobs.append("users")
    if not args or "test_acct" in args:
        jobs.append("test_acct")

    print(f"ETL jobs to run: {jobs}")

    # Progress bar over ETL jobs
    for job in tqdm(jobs, desc="Running ETL jobs"):
        if job == "userlogs":
            state = run_userlogs_etl(mongo_db, engine, state)

        elif job == "users":
            run_users_etl(mongo_db, engine)

        elif job == "test_acct":
            run_test_acct_etl(mongo_db, engine)

    save_state(state)
    print("ETL finished successfully")


if __name__ == "__main__":
    main()


# Usage:
# python main.py           -> run all ETL jobs
# python main.py userlogs  -> run only userlogs ETL
# python main.py users     -> run only users ETL
# python main.py test_acct -> run only test_acct ETL
