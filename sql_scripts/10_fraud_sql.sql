-- 1) Просроченный или заблокированный паспорт
insert into rep.REP_FRAUD(event_dt, passport, fio, phone, event_type, report_dt)
select
    t.trans_date as event_dt,
    cl.passport_num as passport,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Просроченный или заблокированный паспорт' as event_type,
    %(report_dt)s::timestamp as report_dt
from dwh.DWH_FACT_TRANSACTIONS t
join dwh.DWH_DIM_CARDS c   on c.card_num = t.card_num
join dwh.DWH_DIM_ACCOUNTS a on a.account_num = c.account_num
join dwh.DWH_DIM_CLIENTS cl on cl.client_id = a.client
left join dwh.DWH_FACT_PASSPORT_BLACKLIST b
       on b.passport_num = cl.passport_num and b.entry_dt <= t.trans_date::date
where t.trans_date::date = %(day_dt)s::date
  and (
        (cl.passport_valid_to is not null and t.trans_date::date > cl.passport_valid_to)
        or b.passport_num is not null
      );

-- 2) Недействующий договор (счёт просрочен)
insert into rep.REP_FRAUD(event_dt, passport, fio, phone, event_type, report_dt)
select
    t.trans_date as event_dt,
    cl.passport_num as passport,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Операция по недействующему договору' as event_type,
    %(report_dt)s::timestamp as report_dt
from dwh.DWH_FACT_TRANSACTIONS t
join dwh.DWH_DIM_CARDS c   on c.card_num = t.card_num
join dwh.DWH_DIM_ACCOUNTS a on a.account_num = c.account_num
join dwh.DWH_DIM_CLIENTS cl on cl.client_id = a.client
where t.trans_date::date = %(day_dt)s::date
  and a.valid_to is not null
  and t.trans_date::date > a.valid_to;

-- 3) Разные города в течение 1 часа (второе событие — мошенничество)
insert into rep.REP_FRAUD(event_dt, passport, fio, phone, event_type, report_dt)
with tt as (
    select
        t.card_num,
        t.trans_id,
        t.trans_date,
        t.terminal as terminal_id,
        th1.terminal_city as city
    from dwh.DWH_FACT_TRANSACTIONS t
    left join dwh.DWH_DIM_TERMINALS_HIST th1
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
select
    t2.trans_date as event_dt,
    cl.passport_num,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Операции в разных городах в течение часа' as event_type,
    %(report_dt)s::timestamp as report_dt
from dwh.DWH_FACT_TRANSACTIONS t2
join pairs p on p.trans_id2 = t2.trans_id
join dwh.DWH_DIM_CARDS c   on c.card_num = t2.card_num
join dwh.DWH_DIM_ACCOUNTS a on a.account_num = c.account_num
join dwh.DWH_DIM_CLIENTS cl on cl.client_id = a.client;

-- 4) Подбор суммы: >3 отклонённых убывающих, затем успешная внутри 20 минут
insert into rep.REP_FRAUD(event_dt, passport, fio, phone, event_type, report_dt)
with ord as (
    select t.*,
           lag(amt) over (partition by card_num order by trans_date) as lag_amt,
           lag(oper_result) over (partition by card_num order by trans_date) as lag_res
    from dwh.DWH_FACT_TRANSACTIONS t
    where t.trans_date::date = %(day_dt)s::date
),
marks as (
    -- reset=1, если НЕ выполняется «текущая сумма меньше предыдущей И предыдущая была REJECT»
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
        -- last_value требует FOLLOWS frame
        (array_agg(oper_result order by trans_date)) [array_length(array_agg(oper_result order by trans_date),1)] as last_res
    from grps
    group by card_num, grp_id
)
select
    g.grp_end_time as event_dt,
    cl.passport_num,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Подбор суммы' as event_type,
    %(report_dt)s::timestamp as report_dt
from agg g
join dwh.DWH_FACT_TRANSACTIONS t
  on t.card_num = g.card_num and t.trans_date = g.grp_end_time
join dwh.DWH_DIM_CARDS c   on c.card_num = t.card_num
join dwh.DWH_DIM_ACCOUNTS a on a.account_num = c.account_num
join dwh.DWH_DIM_CLIENTS cl on cl.client_id = a.client
where g.n >= 4                  -- минимум 4 операции в цепочке
  and g.reject_cnt = g.n - 1    -- все кроме последней отклонены
  and g.has_success = 1
  and g.last_res = 'SUCCESS'
  and g.grp_end_time - g.grp_start_time <= interval '20 minutes';
