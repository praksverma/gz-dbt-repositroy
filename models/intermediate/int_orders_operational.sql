with sales as (
 select * 
 from   {{ref('int_sales_margin')}}
),
ship as (
    select *
    from {{ref('stg_raw__ship')}}
),
joined as (
    select
    s.orders_id,
    s.date_date,
    Round(Sum(p.shipping_fee),2) as shipping_fee,
    Round(sum(p.logcost),2) as logcost,
    Round(Sum(margin+shipping_fee-logcost-shipping_cost),2) as operational_margin
    From sales s
    Left join ship p
    on s.orders_id=p.orders_id
    group by s.orders_id, s.date_date
    order by s.date_date
)
select * 
from joined