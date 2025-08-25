-- ожидается переменная :file_dt (date)
-- 1) закрыть изменившиеся «открытые» версии
update dwh.DWH_DIM_TERMINALS_HIST d
set effective_to = (:file_dt::timestamp - interval '1 second')
from stg.STG_TERMINALS s
where d.terminal_id = s.terminal_id
  and d.effective_to = timestamp '5999-12-31 23:59:59'
  and (
        d.terminal_type    is distinct from s.terminal_type or
        d.terminal_city    is distinct from s.terminal_city or
        d.terminal_address is distinct from s.terminal_address or
        d.deleted_flg <> 0
      );

-- 2) вставить новые/изменённые «открытые» версии
insert into dwh.DWH_DIM_TERMINALS_HIST
(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
    s.terminal_id, s.terminal_type, s.terminal_city, s.terminal_address,
    :file_dt::timestamp, timestamp '5999-12-31 23:59:59', 0
from stg.STG_TERMINALS s
left join dwh.DWH_DIM_TERMINALS_HIST d
  on d.terminal_id = s.terminal_id
 and d.effective_to = timestamp '5999-12-31 23:59:59'
where d.terminal_id is null
   or d.terminal_type    is distinct from s.terminal_type
   or d.terminal_city    is distinct from s.terminal_city
   or d.terminal_address is distinct from s.terminal_address
   or d.deleted_flg <> 0
;

-- 3) пометить удалённые (есть «открытая» версия, но в снепшоте дня нет)
update dwh.DWH_DIM_TERMINALS_HIST d
set effective_to = (:file_dt::timestamp - interval '1 second'),
    deleted_flg  = 1
where d.effective_to = timestamp '5999-12-31 23:59:59'
  and not exists (
      select 1 from stg.STG_TERMINALS s
      where s.terminal_id = d.terminal_id
  );
