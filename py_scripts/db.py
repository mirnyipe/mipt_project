from sqlalchemy import text
import pandas as pd
from .config import ENGINE
from .sql_loader import load_sql

def raw_exec_many(sql: str):
    raw = ENGINE.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(sql)
        raw.commit()
        cur.close()
    finally:
        raw.close()

def init_db():
    print(">> INIT: creating schemas/tables/views ...")
    raw_exec_many(load_sql("ddl/schemas.sql"))
    raw_exec_many(load_sql("ddl/tables.sql"))
    raw_exec_many(load_sql("ddl/views.sql"))
    print(">> INIT done")

def df_to_table(df: pd.DataFrame, full_name: str, if_exists="append"):
    schema, table = full_name.split(".")
    df.to_sql(table, ENGINE, schema=schema, if_exists=if_exists,
              index=False, method="multi", chunksize=5000)

def load_dim_scd1_from_public():
    print(">> Load SCD1 from public (upserts)")
    with ENGINE.begin() as cn:

        # Карты
        cn.execute(text("""
            insert into dwh.dwh_dim_cards(card_num, account_num, create_dt)
            select c.card_num, c.account as account_num, current_timestamp
            from public.cards c
            on conflict (card_num) do nothing
        """))
        cn.execute(text("""
            update dwh.dwh_dim_cards d
               set account_num = s.account_num,
                   update_dt   = current_timestamp
            from (select card_num, account as account_num from public.cards) s
            where d.card_num = s.card_num
              and (d.account_num is distinct from s.account_num)
        """))

        # Аккаунты
        cn.execute(text("""
            insert into dwh.dwh_dim_accounts(account_num, valid_to, client, create_dt)
            select a.account, a.valid_to, a.client, current_timestamp
            from public.accounts a
            on conflict (account_num) do nothing
        """))
        cn.execute(text("""
            update dwh.dwh_dim_accounts d
               set valid_to = s.valid_to,
                   client   = s.client,
                   update_dt= current_timestamp
            from (select account as account_num, valid_to, client from public.accounts) s
            where d.account_num = s.account_num
              and (d.valid_to is distinct from s.valid_to
                   or d.client  is distinct from s.client)
        """))

        # Клиенты
        cn.execute(text("""
            insert into dwh.dwh_dim_clients(
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone, create_dt
            )
            select client_id, last_name, first_name, patronymic, date_of_birth,
                   passport_num, passport_valid_to, phone, current_timestamp
            from public.clients
            on conflict (client_id) do nothing
        """))
        cn.execute(text("""
            update dwh.dwh_dim_clients d
               set last_name = s.last_name,
                   first_name = s.first_name,
                   patronymic = s.patronymic,
                   date_of_birth = s.date_of_birth,
                   passport_num = s.passport_num,
                   passport_valid_to = s.passport_valid_to,
                   phone = s.phone,
                   update_dt = current_timestamp
            from (
                select client_id, last_name, first_name, patronymic, date_of_birth,
                       passport_num, passport_valid_to, phone
                from public.clients
            ) s
            where d.client_id = s.client_id
              and (d.last_name is distinct from s.last_name
                   or d.first_name is distinct from s.first_name
                   or d.patronymic is distinct from s.patronymic
                   or d.date_of_birth is distinct from s.date_of_birth
                   or d.passport_num is distinct from s.passport_num
                   or d.passport_valid_to is distinct from s.passport_valid_to
                   or d.phone is distinct from s.phone)
        """))
    print(">> SCD1 upserts done.")

def print_connection_info():
    with ENGINE.begin() as cn:
        info = cn.execute(text(
            "select current_user, current_database(), inet_server_addr(), inet_server_port()"
        )).first()
    print("Connected as:", info)
