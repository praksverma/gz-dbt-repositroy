with sales as(
    select *
    from {{ref('stg_raw__sales')}}
),
 product as(
    select *
    from {{ref('stg_raw__product')}}
),
joined as (
    select
    s.orders_id,
    p.products_id,
    s.revenue,
    s.date_date,
    s.quantity,
    p.purchase_price,
    Round(quantity*purchase_price,2) AS purchase_cost,
    Round(revenue-(quantity*purchase_price),2) as margin
    From sales s
   left join product p
   on s.products_id=p.products_id
)
select * from joined