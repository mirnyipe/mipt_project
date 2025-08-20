import pandas as pd
from db import get_engine
from sqlalchemy import text
import datetime
import os

EXPECTED_COLS = ["date", "passport"]

def load_passport_blacklist(file_path: str) -> int:
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("truncate table stg.passport_blacklist"))

    df = pd.read_excel(file_path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    if set(df.columns) != set(EXPECTED_COLS):
        raise ValueError(f"Неверные колонки: {list(df.columns)}; ожидались: {EXPECTED_COLS}")

    df = df[EXPECTED_COLS]
    df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.date
    df["passport"] = df["passport"].str.replace(" ", "", regex=False).str.strip()

    df.to_sql("passport_blacklist", engine, schema="stg", if_exists="append", index=False)

    os.makedirs("logs", exist_ok=True)
    log_line = f"{datetime.datetime.now()} | passport_blacklist | {file_path} | {len(df)} rows\n"
    with open("logs/load_log.txt", "a", encoding="utf-8") as f:
        f.write(log_line)
    print(log_line.strip())

    return len(df)
