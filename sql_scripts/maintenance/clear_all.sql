-- Файл только для очитки всех данных из базы (для удобства тестирования)

-- Порядок важен: отчёты -> факты -> измерения -> стейджинг -> мета
truncate table rep.rep_fraud;

truncate table
  dwh.dwh_fact_transactions,
  dwh.dwh_fact_passport_blacklist;

truncate table
  dwh.dwh_dim_terminals_hist,
  dwh.dwh_dim_cards,
  dwh.dwh_dim_accounts,
  dwh.dwh_dim_clients;

truncate table
  stg.stg_transactions,
  stg.stg_terminals,
  stg.stg_passport_blacklist;

truncate table
  meta.meta_load_files,
  meta.meta_last_dates;
