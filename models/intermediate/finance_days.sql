with finance as (
    select *
    from {{ref('int_orders_operational')}}
),
revenue_daily as(
    select *
    from {{ref('int_orders_margin')}}
),
renamed as (
    select
    s.date_date,
    count(s.orders_id) as transactions_daily,
    Round(sum(p.revenue),2) as revenue,
    Round(sum(s.operational_margin),2) as operational_margin,
    Round(sum(p.purchase_cost),2) as purchase_cost,
    Round(sum(s.shipping_fee),2) as shipping_fee,
    Round(sum(s.logcost),2) as logcost,
    Sum(quantity)as quantity
    from finance s
    left join revenue_daily p
    on s.orders_id=p.orders_id
    group by s.date_date
)
select *
from renamed