insert into rep.rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select
    t.trans_date,
    cl.passport_num,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Операция при недействующем договоре' as event_type,
    %(report_dt)s::timestamp
from dwh.dwh_fact_transactions t
join dwh.dwh_dim_cards c    on c.card_num = t.card_num
join dwh.dwh_dim_accounts a on a.account_num = c.account_num
join dwh.dwh_dim_clients cl on cl.client_id = a.client
where t.trans_date::date = %(day_dt)s::date
  and a.valid_to is not null
  and t.trans_date::date > a.valid_to
on conflict do nothing;
