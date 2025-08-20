import pandas as pd
from db import get_engine
from sqlalchemy import text

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

        df = pd.read_csv(
            file_path,
            sep=";",
            decimal=",",
            dtype=str
        )

        if set(df.columns) != set(EXPECTED_COLS):
            raise ValueError(f"Неверные колонки: {list(df.columns)}; ожидались: {EXPECTED_COLS}")

        df = df[EXPECTED_COLS]

        # Знаю, что можно было сделать проще, но на просторах интернета говорят, что такой подход правильный (чтобы пандас не путался)
        df["transaction_date"] = pd.to_datetime(
            df["transaction_date"],
            format="%Y-%m-%d %H:%M:%S",
            errors="raise"
        )

        df["card_num"] = df["card_num"].str.replace(" ", "", regex=False)

        df.to_sql(
            "transactions",
            engine,
            schema="stg",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

        conn.execute(
            text(
                "insert into etl_logs(table_name, rows_loaded, load_dttm) values (:t, :r, now())"
            ),
            {"t": "stg.transactions", "r": len(df)}
        )

    return len(df)
