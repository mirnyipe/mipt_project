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
join dwh.dwh_dim_clients cl on cl.client_id = a.client
on conflict do nothing;
