insert into rep.rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
select
    t.trans_date as event_dt,
    cl.passport_num as passport,
    cl.last_name || ' ' || cl.first_name || ' ' || coalesce(cl.patronymic,'') as fio,
    cl.phone,
    'Просроченный или заблокированный паспорт' as event_type,
    %(report_dt)s::timestamp as report_dt
from dwh.dwh_fact_transactions t
join dwh.dwh_dim_cards c    on c.card_num = t.card_num
join dwh.dwh_dim_accounts a on a.account_num = c.account_num
join dwh.dwh_dim_clients cl on cl.client_id = a.client
left join dwh.dwh_fact_passport_blacklist b
       on b.passport_num = cl.passport_num and b.entry_dt <= t.trans_date::date
where t.trans_date::date = %(day_dt)s::date
  and (
        (cl.passport_valid_to is not null and t.trans_date::date > cl.passport_valid_to)
        or b.passport_num is not null
      )
on conflict do nothing;
