-- Файл только для очитки всех данных из базы, для удобства тестирования

BEGIN;
-- Порядок важен: отчёты -> факты -> измерения -> стейджинг -> мета
TRUNCATE TABLE rep.rep_fraud;

TRUNCATE TABLE
  dwh.dwh_fact_transactions,
  dwh.dwh_fact_passport_blacklist;

TRUNCATE TABLE
  dwh.dwh_dim_terminals_hist,
  dwh.dwh_dim_cards,
  dwh.dwh_dim_accounts,
  dwh.dwh_dim_clients;

TRUNCATE TABLE
  stg.stg_transactions,
  stg.stg_terminals,
  stg.stg_passport_blacklist;

TRUNCATE TABLE
  meta.meta_load_files,
  meta.meta_last_dates;
COMMIT;
