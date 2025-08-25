# -*- coding: utf-8 -*-
"""
ETL по ТЗ: STG → DWH (SCD1/SCD2) → REP_FRAUD + архивирование

Шаги перед запуском:
1) cred.json рядом с файлом:
   {
     "user": "postgres",
     "password": "****",
     "host": "localhost",
     "port": 5432,
     "database": "postgres"
   }
2) ОДИН раз выполнить ваш ddl_dml.sql (создаёт public.cards/accounts/clients).
3) Сложить файлы в ./data:
   - transactions_DDMMYYYY.txt
   - terminals_DDMMYYYY.xlsx
   - passport_blacklist_DDMMYYYY.xlsx
4) Запуск:
   python main.py            # полный цикл
   python main.py --init     # только DDL
   python main.py --load-public   # только апсерты SCD1 из public
   python main.py --process  # только обработка новых файлов + отчёты
"""

import os
import re
import json
import shutil
from pathlib import Path
from datetime import datetime, date
import argparse

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ----------------------------
# Пути/регулярки/подключение
# ----------------------------
ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
ARCHIVE_DIR = ROOT / "archive"

RE_TRANS = re.compile(r"^transactions_(\d{2})(\d{2})(\d{4})\.txt$")
RE_PBL   = re.compile(r"^passport_blacklist_(\d{2})(\d{2})(\d{4})\.xlsx$")
RE_TERM  = re.compile(r"^terminals_(\d{2})(\d{2})(\d{4})\.xlsx$")

cred = json.load(open(ROOT / "cred.json", "r", encoding="utf-8"))
ENGINE: Engine = create_engine(
    f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}",
    future=True
)

# ----------------------------
# DDL (везде нижний регистр)
# ----------------------------
SQL_SCHEMAS = """
create schema if not exists stg;
create schema if not exists dwh;
create schema if not exists meta;
create schema if not exists rep;
"""

SQL_TABLES = """
-- STAGING
create table if not exists stg.stg_transactions(
    trans_id    varchar(128),
    trans_date  timestamp,
    card_num    varchar(64),
    oper_type   varchar(64),
    amt         numeric(18,2),
    oper_result varchar(32),
    terminal    varchar(64),
    file_dt     date,
    filename    varchar(256)
);

create table if not exists stg.stg_terminals(
    terminal_id      varchar(64),
    terminal_type    varchar(64),
    terminal_city    varchar(128),
    terminal_address varchar(256),
    file_dt          date,
    filename         varchar(256)
);

create table if not exists stg.stg_passport_blacklist(
    passport_num varchar(32),
    entry_dt     date,
    file_dt      date,
    filename     varchar(256)
);

-- DWH: SCD1
create table if not exists dwh.dwh_dim_cards(
    card_num    varchar(64) primary key,
    account_num varchar(128) not null,
    create_dt   timestamp default current_timestamp,
    update_dt   timestamp
);

create table if not exists dwh.dwh_dim_accounts(
    account_num varchar(128) primary key,
    valid_to    date,
    client      varchar(128),
    create_dt   timestamp default current_timestamp,
    update_dt   timestamp
);

create table if not exists dwh.dwh_dim_clients(
    client_id         varchar(128) primary key,
    last_name         varchar(128),
    first_name        varchar(128),
    patronymic        varchar(128),
    date_of_birth     date,
    passport_num      varchar(32),
    passport_valid_to date,
    phone             varchar(64),
    create_dt         timestamp default current_timestamp,
    update_dt         timestamp
);

-- DWH: SCD2 (терминалы)
create table if not exists dwh.dwh_dim_terminals_hist(
    terminal_id      varchar(64),
    terminal_type    varchar(64),
    terminal_city    varchar(128),
    terminal_address varchar(256),
    effective_from   timestamp not null,
    effective_to     timestamp not null default timestamp '5999-12-31 23:59:59',
    deleted_flg      integer not null default 0,
    primary key (terminal_id, effective_from)
);

-- DWH: FACTS
create table if not exists dwh.dwh_fact_transactions(
    trans_id    varchar(128) primary key,
    trans_date  timestamp,
    card_num    varchar(64),
    oper_type   varchar(64),
    amt         numeric(18,2),
    oper_result varchar(32),
    terminal    varchar(64)
);

create table if not exists dwh.dwh_fact_passport_blacklist(
    passport_num varchar(32),
    entry_dt     date,
    primary key (passport_num, entry_dt)
);

-- META
create table if not exists meta.meta_load_files(
    source_name  varchar(64),
    file_dt      date,
    filename     varchar(256),
    processed_at timestamp default current_timestamp,
    status       varchar(32) default 'DONE',
    primary key (source_name, filename)
);

create table if not exists meta.meta_last_dates(
    source_name  varchar(64) primary key,
    last_dt      date
);

-- REPORT
create table if not exists rep.rep_fraud(
    event_dt   timestamp,
    passport   varchar(32),
    fio        varchar(384),
    phone      varchar(64),
    event_type varchar(256),
    report_dt  timestamp
);
"""

SQL_VIEWS = """
create or replace view dwh.v_term_current as
select t.*
from dwh.dwh_dim_terminals_hist t
where t.deleted_flg = 0
  and current_timestamp between t.effective_from and t.effective_to;
"""

# ----------------------------
# Утилиты
# ----------------------------
def ensure_dirs():
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

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
    raw_exec_many(SQL_SCHEMAS)
    raw_exec_many(SQL_TABLES)
    raw_exec_many(SQL_VIEWS)
    with ENGINE.begin() as cn:
        for sch, tbl in [
            ("dwh","dwh_fact_transactions"),
            ("dwh","dwh_dim_terminals_hist"),
            ("rep","rep_fraud"),
        ]:
            ok = cn.execute(text("""
                select exists(
                    select 1 from information_schema.tables
                    where table_schema=:s and table_name=:t
                )
            """), {"s": sch, "t": tbl}).scalar()
            print(f"   - {sch}.{tbl}: {'OK' if ok else 'MISSING'}")

def df_to_table(df: pd.DataFrame, full_name: str, if_exists="append"):
    schema, table = full_name.split(".")
    schema = schema.lower(); table = table.lower()
    df.to_sql(table, ENGINE, schema=schema, if_exists=if_exists, index=False, method="multi", chunksize=5000)

def parse_dt_from_name(name: str) -> date | None:
    for reg in (RE_TRANS, RE_PBL, RE_TERM):
        m = reg.match(name)
        if m:
            dd, mm, yyyy = m.groups()
            return datetime.strptime(f"{yyyy}-{mm}-{dd}", "%Y-%m-%d").date()
    return None

def already_processed(filename: str) -> bool:
    with ENGINE.begin() as cn:
        row = cn.execute(text("select 1 from meta.meta_load_files where filename=:fn"), {"fn": filename}).first()
    return row is not None

def mark_processed(source_name: str, file_dt: date, filename: str):
    with ENGINE.begin() as cn:
        cn.execute(text("""
            insert into meta.meta_load_files(source_name, file_dt, filename)
            values (:src, :dt, :fn)
            on conflict (source_name, filename) do nothing
        """), {"src": source_name, "dt": file_dt, "fn": filename})
        cn.execute(text("""
            insert into meta.meta_last_dates(source_name, last_dt)
            values (:src, :dt)
            on conflict (source_name) do update set last_dt = excluded.last_dt
        """), {"src": source_name, "dt": file_dt})

def archive_file(path: Path) -> bool:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    src = Path(path)
    dst = ARCHIVE_DIR / (src.name + ".backup")

    if dst.exists():
        print(f">> [ARCHIVE] already exists: {dst.name}")
        if src.exists():
            try:
                src.unlink()
                print(f">> [ARCHIVE] removed duplicate source: {src.name}")
            except Exception as e:
                print(f">> [WARN] could not remove duplicate source {src.name}: {e}")
        return True

    if not src.exists():
        print(f">> [ARCHIVE] source missing, treat as archived: {src.name}")
        return True

    try:
        os.replace(src, dst)
    except Exception:
        try:
            shutil.copy2(str(src), str(dst))
            os.remove(src)
        except Exception as e:
            print(f">> [ERROR] archiving failed for {src.name}: {e}")
            return False

    print(f">> [ARCHIVE] {src.name} -> {dst.name}")
    return True

# ----------------------------
# Загрузка SCD1 из public
# ----------------------------
def load_dim_scd1_from_public():
    print(">> Load SCD1 from public (upserts)")
    with ENGINE.begin() as cn:
        # cards
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

        # accounts
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

        # clients
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

# ----------------------------
# Обработчики файлов
# ----------------------------
def process_terminals(file: Path, file_dt: date):
    print(f">> [TERMINALS] {file.name}")
    df = pd.read_excel(file)
    df.columns = [c.lower().strip() for c in df.columns]

    if "terminal_id" not in df.columns:
        if "id" in df.columns:
            df = df.rename(columns={"id": "terminal_id"})
        else:
            raise ValueError("Не найдена колонка terminal_id / id в файле терминалов")
    if "terminal_type" not in df.columns and "type" in df.columns:
        df = df.rename(columns={"type": "terminal_type"})
    if "terminal_city" not in df.columns and "city" in df.columns:
        df = df.rename(columns={"city": "terminal_city"})
    if "terminal_address" not in df.columns and "address" in df.columns:
        df = df.rename(columns={"address": "terminal_address"})

    df["file_dt"] = pd.to_datetime(file_dt)
    df["filename"] = file.name
    df = df[["terminal_id","terminal_type","terminal_city","terminal_address","file_dt","filename"]]
    df_to_table(df, "stg.stg_terminals")

    # SCD2 (закрываем старые на (dt - 1 сек), открываем новые с dt 00:00:00; удалёнки — тоже с (dt - 1 сек))
    params = {"dt": str(file_dt)}

    upd_close = """
        update dwh.dwh_dim_terminals_hist d
           set effective_to = (%(dt)s::timestamp - interval '1 second')
        from (
            select terminal_id, terminal_type, terminal_city, terminal_address
            from stg.stg_terminals
            where file_dt = %(dt)s::date
        ) s
        where d.terminal_id = s.terminal_id
          and d.effective_to = timestamp '5999-12-31 23:59:59'
          and (
                d.terminal_type    is distinct from s.terminal_type or
                d.terminal_city    is distinct from s.terminal_city or
                d.terminal_address is distinct from s.terminal_address or
                d.deleted_flg <> 0
              );
    """

    ins_open = """
        insert into dwh.dwh_dim_terminals_hist
        (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
        select
            s.terminal_id, s.terminal_type, s.terminal_city, s.terminal_address,
            %(dt)s::timestamp, timestamp '5999-12-31 23:59:59', 0
        from (
            select terminal_id, terminal_type, terminal_city, terminal_address
            from stg.stg_terminals
            where file_dt = %(dt)s::date
        ) s
        left join dwh.dwh_dim_terminals_hist d
          on d.terminal_id = s.terminal_id
         and d.effective_to = timestamp '5999-12-31 23:59:59'
        where d.terminal_id is null
           or d.terminal_type    is distinct from s.terminal_type
           or d.terminal_city    is distinct from s.terminal_city
           or d.terminal_address is distinct from s.terminal_address
           or d.deleted_flg <> 0;
    """

    upd_deleted = """
        update dwh.dwh_dim_terminals_hist d
           set effective_to = (%(dt)s::timestamp - interval '1 second'),
               deleted_flg  = 1
        where d.effective_to = timestamp '5999-12-31 23:59:59'
          and not exists (
              select 1 from stg.stg_terminals s
              where s.file_dt = %(dt)s::date
                and s.terminal_id = d.terminal_id
          );
    """

    with ENGINE.begin() as cn:
        cn.exec_driver_sql(upd_close, params)
        cn.exec_driver_sql(ins_open, params)
        cn.exec_driver_sql(upd_deleted, params)

    archived = archive_file(file)
    if archived:
        mark_processed("terminals", file_dt, file.name)

def process_blacklist(file: Path, file_dt: date):
    print(f">> [BLACKLIST] {file.name}")
    df = pd.read_excel(file)
    df.columns = [c.lower().strip() for c in df.columns]

    if "passport_num" not in df.columns:
        if "passport" in df.columns:
            df = df.rename(columns={"passport": "passport_num"})
        else:
            raise ValueError("Не найдена колонка passport_num / passport в файле blacklist")
    if "entry_dt" not in df.columns:
        if "date" in df.columns:
            df = df.rename(columns={"date": "entry_dt"})
        else:
            raise ValueError("Не найдена колонка entry_dt / date в файле blacklist")

    df["entry_dt"] = pd.to_datetime(df["entry_dt"]).dt.date
    df["file_dt"] = pd.to_datetime(file_dt)
    df["filename"] = file.name
    df = df[["passport_num","entry_dt","file_dt","filename"]]
    df_to_table(df, "stg.stg_passport_blacklist")

    with ENGINE.begin() as cn:
        cn.execute(text("""
            insert into dwh.dwh_fact_passport_blacklist(passport_num, entry_dt)
            select passport_num, entry_dt
            from stg.stg_passport_blacklist
            on conflict do nothing
        """))

    archived = archive_file(file)
    if archived:
        mark_processed("passport_blacklist", file_dt, file.name)

def process_transactions(file: Path, file_dt: date):
    print(f">> [TRANSACTIONS] {file.name}")
    try:
        df = pd.read_csv(file, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(file, sep=";", engine="python")

    df.columns = [c.lower().strip() for c in df.columns]
    rename_map = {}
    if "transaction_id" in df.columns and "trans_id" not in df.columns:
        rename_map["transaction_id"] = "trans_id"
    if "transaction_date" in df.columns and "trans_date" not in df.columns:
        rename_map["transaction_date"] = "trans_date"
    if "amount" in df.columns and "amt" not in df.columns:
        rename_map["amount"] = "amt"
    if "terminal_id" in df.columns and "terminal" not in df.columns:
        rename_map["terminal_id"] = "terminal"
    if rename_map:
        df = df.rename(columns=rename_map)

    required = ["trans_id", "trans_date", "card_num", "oper_type", "amt", "oper_result", "terminal"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В файле транзакций отсутствуют колонки: {missing}")

    df["trans_id"] = df["trans_id"].astype(str).str.strip()
    df["trans_date"] = pd.to_datetime(df["trans_date"])
    df["amt"] = pd.to_numeric(df["amt"].astype(str).str.replace(",", "."), errors="coerce")
    df["oper_result"] = (
        df["oper_result"].astype(str).str.upper().str.strip()
        .replace({"APPROVED":"SUCCESS","ACCEPTED":"SUCCESS","OK":"SUCCESS",
                  "DECLINED":"REJECT","DENIED":"REJECT","FAILED":"REJECT"})
    )

    df["file_dt"] = pd.to_datetime(file_dt)
    df["filename"] = file.name
    df = df[required + ["file_dt","filename"]]
    df_to_table(df, "stg.stg_transactions")

    with ENGINE.begin() as cn:
        cn.execute(text("""
            insert into dwh.dwh_fact_transactions
                (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
            select trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
            from stg.stg_transactions
            on conflict (trans_id) do nothing
        """))

    archived = archive_file(file)
    if archived:
        mark_processed("transactions", file_dt, file.name)

# ----------------------------
# Витрина фрода (через exec_driver_sql + %(name)s)
# ----------------------------
FRAUD_RULE_1 = """
insert into rep.rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select
    t.trans_date as event_dt,
    cl.passport_num as passport,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Просроченный или заблокированный паспорт' as event_type,
    %(report_dt)s::timestamp as report_dt
from dwh.dwh_fact_transactions t
join dwh.dwh_dim_cards c    on c.card_num = t.card_num
join dwh.dwh_dim_accounts a on a.account_num = c.account_num
join dwh.dwh_dim_clients cl on cl.client_id = a.client
left join dwh.dwh_fact_passport_blacklist b
       on b.passport_num = cl.passport_num and b.entry_dt <= t.trans_date::date
where t.trans_date::date = %(day_dt)s::date
  and (
        (cl.passport_valid_to is not null and t.trans_date::date > cl.passport_valid_to)
        or b.passport_num is not null
      );
"""

FRAUD_RULE_2 = """
insert into rep.rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select
    t.trans_date,
    cl.passport_num,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Операция по недействующему договору' as event_type,
    %(report_dt)s::timestamp
from dwh.dwh_fact_transactions t
join dwh.dwh_dim_cards c    on c.card_num = t.card_num
join dwh.dwh_dim_accounts a on a.account_num = c.account_num
join dwh.dwh_dim_clients cl on cl.client_id = a.client
where t.trans_date::date = %(day_dt)s::date
  and a.valid_to is not null
  and t.trans_date::date > a.valid_to;
"""

FRAUD_RULE_3 = """
with tt as (
    select
        t.card_num,
        t.trans_id,
        t.trans_date,
        t.terminal as terminal_id,
        th1.terminal_city as city
    from dwh.dwh_fact_transactions t
    left join dwh.dwh_dim_terminals_hist th1
      on th1.terminal_id = t.terminal
     and t.trans_date between th1.effective_from and th1.effective_to
     and th1.deleted_flg = 0
    where t.trans_date::date = %(day_dt)s::date
),
pairs as (
    select
      t1.card_num,
      t1.trans_date as t1_dt, t1.city as city1,
      t2.trans_date as t2_dt, t2.city as city2,
      t2.trans_id   as trans_id2
    from tt t1
    join tt t2
      on t1.card_num = t2.card_num
     and t2.trans_date > t1.trans_date
     and t2.trans_date <= t1.trans_date + interval '1 hour'
     and coalesce(t1.city,'?') <> coalesce(t2.city,'?')
)
insert into rep.rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select
    t2.trans_date as event_dt,
    cl.passport_num,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Операции в разных городах в течение часа' as event_type,
    %(report_dt)s::timestamp
from dwh.dwh_fact_transactions t2
join pairs p on p.trans_id2 = t2.trans_id
join dwh.dwh_dim_cards c    on c.card_num = t2.card_num
join dwh.dwh_dim_accounts a on a.account_num = c.account_num
join dwh.dwh_dim_clients cl on cl.client_id = a.client;
"""

# >>> ПРАВКА: Rule 4 — расширяем окно на 20 минут назад, чтобы ловить цепочки, начатые накануне
FRAUD_RULE_4 = """
with ord as (
    select t.*,
           lag(amt)         over (partition by card_num order by trans_date) as lag_amt,
           lag(oper_result) over (partition by card_num order by trans_date) as lag_res
    from dwh.dwh_fact_transactions t
    where t.trans_date >= (%(day_dt)s::timestamp - interval '20 minutes')
      and t.trans_date  < (%(day_dt)s::timestamp + interval '1 day')
),
marks as (
    select *,
           case when lag_amt is not null and amt < lag_amt and lag_res = 'REJECT' then 0 else 1 end as reset
    from ord
),
grps as (
    select *,
           sum(reset) over (partition by card_num order by trans_date
                            rows between unbounded preceding and current row) as grp_id
    from marks
),
agg as (
    select
        card_num,
        grp_id,
        min(trans_date) as grp_start_time,
        max(trans_date) as grp_end_time,
        count(*)        as n,
        sum(case when oper_result='REJECT' then 1 else 0 end) as reject_cnt,
        max(case when oper_result='SUCCESS' then 1 else 0 end) as has_success,
        (array_agg(oper_result order by trans_date))[array_length(array_agg(oper_result order by trans_date),1)] as last_res
    from grps
    group by card_num, grp_id
)
insert into rep.rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select
    g.grp_end_time as event_dt,
    cl.passport_num,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Подбор суммы' as event_type,
    %(report_dt)s::timestamp
from agg g
join dwh.dwh_fact_transactions t
  on t.card_num = g.card_num and t.trans_date = g.grp_end_time
join dwh.dwh_dim_cards c    on c.card_num = t.card_num
join dwh.dwh_dim_accounts a on a.account_num = c.account_num
join dwh.dwh_dim_clients cl on cl.client_id = a.client
where g.n >= 4
  and g.reject_cnt = g.n - 1
  and g.has_success = 1
  and g.last_res = 'SUCCESS'
  and g.grp_end_time - g.grp_start_time <= interval '20 minutes'
  and g.grp_end_time::date = %(day_dt)s::date;   -- берём в отчёт только события текущего дня
"""

def build_fraud_report_for_day(day_dt: date):
    print(f">> Build REP_FRAUD for {day_dt}")
    params = {"day_dt": str(day_dt), "report_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    with ENGINE.begin() as cn:
        cn.exec_driver_sql(FRAUD_RULE_1, params)
        cn.exec_driver_sql(FRAUD_RULE_2, params)
        cn.exec_driver_sql(FRAUD_RULE_3, params)
        cn.exec_driver_sql(FRAUD_RULE_4, params)

def build_missing_reports():
    with ENGINE.begin() as cn:
        rows = cn.execute(text("""
            with d as (select distinct trans_date::date as d from dwh.dwh_fact_transactions),
                 r as (select distinct event_dt::date  as d from rep.rep_fraud)
            select d.d from d left join r on r.d = d.d
            where r.d is null
            order by d.d
        """)).fetchall()
    for (d,) in rows:
        build_fraud_report_for_day(d)

def process_inbox_and_build_reports():
    ensure_dirs()
    entries = []
    for p in DATA_DIR.iterdir():
        if not p.is_file():
            continue
        dt = parse_dt_from_name(p.name)
        if not dt:
            continue
        if already_processed(p.name):
            continue
        if RE_TERM.match(p.name):
            typ = "terminals"
        elif RE_PBL.match(p.name):
            typ = "passport_blacklist"
        elif RE_TRANS.match(p.name):
            typ = "transactions"
        else:
            continue
        entries.append((dt, typ, p))

    if not entries:
        print(">> Нет новых файлов в ./data")
        build_missing_reports()
        return

    # 1) измерения дня
    for dt, typ, p in sorted([e for e in entries if e[1] in ("terminals","passport_blacklist")]):
        if typ == "terminals":
            process_terminals(p, dt)
        else:
            process_blacklist(p, dt)

    # 2) факты
    days_with_tx = set()
    for dt, typ, p in sorted([e for e in entries if e[1] == "transactions"]):
        process_transactions(p, dt)
        days_with_tx.add(dt)

    # 3) витрина
    for d in sorted(days_with_tx):
        build_fraud_report_for_day(d)
    build_missing_reports()

# ----------------------------
# CLI
# ----------------------------
def print_connection_info():
    with ENGINE.begin() as cn:
        info = cn.execute(text(
            "select current_user, current_database(), inet_server_addr(), inet_server_port()"
        )).first()
    print("Connected as:", info)

def main():
    parser = argparse.ArgumentParser(description="ETL по ТЗ (STG→DWH→REP_FRAUD)")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--load-public", action="store_true")
    parser.add_argument("--process", action="store_true")
    args = parser.parse_args()

    ensure_dirs()
    print_connection_info()

    if not any([args.init, args.load_public, args.process]):
        init_db()
        load_dim_scd1_from_public()
        process_inbox_and_build_reports()
        print("ETL FINISHED.")
        return

    if args.init:
        init_db()
    if args.load_public:
        load_dim_scd1_from_public()
    if args.process:
        process_inbox_and_build_reports()

if __name__ == "__main__":
    main()
