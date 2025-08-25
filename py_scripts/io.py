import os, shutil
from pathlib import Path
from datetime import datetime, date
import pandas as pd
from sqlalchemy import text
from .config import DATA_DIR, ARCHIVE_DIR, RE_TRANS, RE_PBL, RE_TERM, ENGINE
from .db import df_to_table
from .rules import build_fraud_report_for_day
from .sql_loader import load_sql

def ensure_dirs():
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def parse_dt_from_name(name: str):
    for reg in (RE_TRANS, RE_PBL, RE_TERM):
        m = reg.match(name)
        if m:
            dd, mm, yyyy = m.groups()
            return date(int(yyyy), int(mm), int(dd))
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
            on conflict (source_name, filename) do nothing;
        """), {"src": source_name, "dt": file_dt, "fn": filename})
        cn.execute(text("""
            insert into meta.meta_last_dates(source_name, last_dt)
            values (:src, :dt)
            on conflict (source_name) do update set last_dt = excluded.last_dt;
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

# Процессор
def process_terminals(file: Path, file_dt: date):
    print(f">> [TERMINALS] {file.name}")
    df = pd.read_excel(file)
    df.columns = [c.lower().strip() for c in df.columns]
    if "terminal_id" not in df.columns:
        if "id" in df.columns: df = df.rename(columns={"id": "terminal_id"})
        else: raise ValueError("Нет terminal_id / id")
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
        select s.terminal_id, s.terminal_type, s.terminal_city, s.terminal_address,
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

    if archive_file(file): mark_processed("terminals", file_dt, file.name)

def process_blacklist(file: Path, file_dt: date):
    print(f">> [BLACKLIST] {file.name}")
    df = pd.read_excel(file)
    df.columns = [c.lower().strip() for c in df.columns]
    if "passport_num" not in df.columns:
        if "passport" in df.columns: df = df.rename(columns={"passport": "passport_num"})
        else: raise ValueError("Нет passport_num / passport")
    if "entry_dt" not in df.columns:
        if "date" in df.columns: df = df.rename(columns={"date": "entry_dt"})
        else: raise ValueError("Нет entry_dt / date")

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
            on conflict do nothing;
        """))
    if archive_file(file): mark_processed("passport_blacklist", file_dt, file.name)

def process_transactions(file: Path, file_dt: date):
    print(f">> [TRANSACTIONS] {file.name}")
    try:
        df = pd.read_csv(file, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(file, sep=";", engine="python")
    df.columns = [c.lower().strip() for c in df.columns]
    rename = {}
    if "transaction_id" in df.columns and "trans_id" not in df.columns: rename["transaction_id"]="trans_id"
    if "transaction_date" in df.columns and "trans_date" not in df.columns: rename["transaction_date"]="trans_date"
    if "amount" in df.columns and "amt" not in df.columns: rename["amount"]="amt"
    if "terminal_id" in df.columns and "terminal" not in df.columns: rename["terminal_id"]="terminal"
    if rename: df = df.rename(columns=rename)

    required = ["trans_id","trans_date","card_num","oper_type","amt","oper_result","terminal"]
    miss = [c for c in required if c not in df.columns]
    if miss: raise ValueError(f"В транзакциях нет колонок: {miss}")

    df["trans_id"] = df["trans_id"].astype(str).str.strip()
    df["trans_date"] = pd.to_datetime(df["trans_date"])
    df["amt"] = pd.to_numeric(df["amt"].astype(str).str.replace(",", "."), errors="coerce")
    df["oper_result"] = (df["oper_result"].astype(str).str.upper().str.strip()
                         .replace({"APPROVED":"SUCCESS","ACCEPTED":"SUCCESS","OK":"SUCCESS",
                                   "DECLINED":"REJECT","DENIED":"REJECT","FAILED":"REJECT"}))
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
            on conflict (trans_id) do nothing;
        """))

    if archive_file(file): mark_processed("transactions", file_dt, file.name)

def build_missing_reports():
    with ENGINE.begin() as cn:
        rows = cn.execute(text(load_sql("maintenance/build_missing_reports.sql"))).fetchall()
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
        if RE_TERM.match(p.name): typ = "terminals"
        elif RE_PBL.match(p.name): typ = "passport_blacklist"
        elif RE_TRANS.match(p.name): typ = "transactions"
        else: continue
        entries.append((dt, typ, p))

    if not entries:
        print(">> Нет новых файлов в ./data")
        return

    for dt, typ, p in sorted([e for e in entries if e[1] in ("terminals","passport_blacklist")]):
        if typ == "terminals": process_terminals(p, dt)
        else: process_blacklist(p, dt)

    days = set()
    for dt, typ, p in sorted([e for e in entries if e[1] == "transactions"]):
        process_transactions(p, dt)
        days.add(dt)

    for d in sorted(days):
        build_fraud_report_for_day(d)
