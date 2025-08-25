create or replace view dwh.V_TERM_CURRENT as
select t.*
from dwh.DWH_DIM_TERMINALS_HIST t
where t.deleted_flg = 0
  and current_timestamp between t.effective_from and t.effective_to;
