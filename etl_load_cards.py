import pandas as pd
from db import get_engine
from sqlalchemy import text



def load_cards():
    engine = get_engine()

    src_cards = pd.read_sql("select * from cards", engine)

    insert_sql = text("""--sql
        insert into dwh.dwh_dim_cards (
            card_num, 
            account, 
            effective_from, 
            effective_to, 
            deleted_flg
        )
        values (
            :card_num, 
            :account,
            now(), 
            timestamp '5999-12-31 23:59:59', 
            'N'
        )
    """)

    with engine.begin() as db:
        for _, row in src_cards.iterrows():
            db.execute(insert_sql, {
                "card_num": row['card_num'],
                "account": row['account'],
            })

