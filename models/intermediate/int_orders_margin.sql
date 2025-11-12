select
orders_id,
date_date,
Round(Sum(revenue),2) as revenue,
Sum(quantity) as quantity,
Round(Sum(purchase_cost),2)as purchase_cost,
Round(Sum(margin),2) as margin
from {{ ref("int_sales_margin") }}
group by date_date, orders_id
order by orders_id