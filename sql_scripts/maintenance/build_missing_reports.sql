with d as (select distinct trans_date::date as d from dwh.dwh_fact_transactions),
     r as (select distinct event_dt::date  as d from rep.rep_fraud)
select d.d
from d left join r on r.d = d.d
where r.d is null
order by d.d;
