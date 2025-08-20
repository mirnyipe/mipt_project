import pandas as pd
from db import get_engine
from sqlalchemy import text



def load_clients():
    engine = get_engine()

    src_clients = pd.read_sql("select * from clients", engine)

    insert_sql = text("""--sql
        insert into dwh.dwh_dim_clients (
            client_id, 
            last_name, 
            first_name, 
            patronymic, 
            date_of_birth,
            passport_num, 
            passport_valid_to, 
            phone,
            effective_from, 
            effective_to, 
            deleted_flg
        )
        values (
            :client_id, 
            :last_name, 
            :first_name, 
            :patronymic, 
            :dob,
            :passport_num, 
            :passport_valid_to, 
            :phone,
            now(), 
            timestamp '5999-12-31 23:59:59', 
            'N'
        )
    """)

    with engine.begin() as db:
        for _, row in src_clients.iterrows():
            db.execute(insert_sql, {
                "client_id": row['client_id'],
                "last_name": row['last_name'],
                "first_name": row['first_name'],
                "patronymic": row['patronymic'],
                "dob": row['date_of_birth'],
                "passport_num": row['passport_num'],
                "passport_valid_to": row['passport_valid_to'],
                "phone": row['phone']
            })

