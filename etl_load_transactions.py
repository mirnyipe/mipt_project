# etl_load_transactions.py
import pandas as pd
from db import get_engine
from sqlalchemy import text
import datetime
import os

EXPECTED_COLS = [
    "transaction_id",
    "transaction_date",
    "amount",
    "card_num",
    "oper_type",
    "oper_result",
    "terminal"
]

def load_transactions(file_path: str) -> int:
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("truncate table stg.transactions"))

    df = pd.read_csv(file_path, sep=";", decimal=",", dtype=str)
    if set(df.columns) != set(EXPECTED_COLS):
        raise ValueError(f"Неверные колонки: {list(df.columns)}; ожидались: {EXPECTED_COLS}")

    df = df[EXPECTED_COLS]

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], format="%Y-%m-%d %H:%M:%S", errors="raise"
    )

    df["card_num"] = df["card_num"].str.replace(" ", "", regex=False)

    df["amount"] = df["amount"].str.replace(",", ".", regex=False)

    df.to_sql("transactions", engine, schema="stg", if_exists="append", index=False)

    os.makedirs("logs", exist_ok=True)
    log_line = f"{datetime.datetime.now()} | transactions | {file_path} | {len(df)} rows\n"
    with open("logs/load_log.txt", "a", encoding="utf-8") as f:
        f.write(log_line)
    print(log_line.strip())

    return len(df)
