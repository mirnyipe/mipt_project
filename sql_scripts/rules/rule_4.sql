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
  and g.grp_end_time::date = %(day_dt)s::date
on conflict do nothing;
