from sqlalchemy import create_engine, text
from config import MYSQL_URI

engine = create_engine(MYSQL_URI)

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("MySQL connection OK:", result.fetchone())
