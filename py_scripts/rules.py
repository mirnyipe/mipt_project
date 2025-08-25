from datetime import datetime, date
from sqlalchemy import text
from .config import ENGINE
from .sql_loader import load_sql

def build_fraud_report_for_day(day_dt: date):
    print(f">> Build REP_FRAUD for {day_dt}")
    params = {
        "day_dt": str(day_dt),
        "report_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with ENGINE.begin() as cn:
        # Чистим день (плейсхолдер без ::date справа)
        cn.execute(text("delete from rep.rep_fraud where event_dt::date = :day_dt;"), params)
        # Правила (exec_driver_sql ожидает %(name)s в SQL)
        cn.exec_driver_sql(load_sql("rules/rule_1.sql"), params)
        cn.exec_driver_sql(load_sql("rules/rule_2.sql"), params)
        cn.exec_driver_sql(load_sql("rules/rule_3.sql"), params)
        cn.exec_driver_sql(load_sql("rules/rule_4.sql"), params)
