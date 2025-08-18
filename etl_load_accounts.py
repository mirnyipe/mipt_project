import pandas as pd
from db import get_connections
from sqlalchemy import text



def load_accounts():
    engine, _, _ = get_connections()

    src_accounts = pd.read_sql("select * from accounts", engine)

    insert_sql = text("""--sql
        insert into dwh.dwh_dim_accounts (
            account, 
            valid_to, 
            client,
            effective_from, 
            effective_to, 
            deleted_flg
        )
        values (
            :account, 
            :valid_to, 
            :client,
            now(), 
            timestamp '5999-12-31 23:59:59', 
            'N'
        )
    """)

    with engine.begin() as db:
        for _, row in src_accounts.iterrows():
            db.execute(insert_sql, {
                "account": row['account'],
                "valid_to": row['valid_to'],
                "client": row['client'],
            })

