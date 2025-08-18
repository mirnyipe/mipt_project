import pandas as pd
from db import get_connections



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
    engine, _, _ = get_connections

    df = pd.read_csv(
        file_path,
        sep=";",
        decimal=",",
        parse_dates=["transaction_date"]
    )

    if set(df.columns) != set(EXPECTED_COLS):
        raise ValueError(f"Неверные колонки: {list(df.columns)}; ожидались: {EXPECTED_COLS}")
    
    df = df[EXPECTED_COLS]
    df["card_num"] = df["card_num"].str.replace(" ", "", regex=False)
    df.to_sql("transactions", engine, schema="stg", if_exists="append", index=False)

    return len(df)