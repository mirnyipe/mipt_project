create or replace view dwh.v_term_current as
select t.*
from dwh.dwh_dim_terminals_hist t
where t.deleted_flg = 0
  and current_timestamp between t.effective_from and t.effective_to;
