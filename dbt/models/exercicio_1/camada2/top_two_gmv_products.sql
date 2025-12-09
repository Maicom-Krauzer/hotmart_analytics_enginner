{{ config (
    materialized='table'
) }}

with
base_join AS  (
	select
		prod_it.product_id
	,	prod_it.purchase_value
	,	prod_it.item_quantity
	from
		{{ref ('purchase_ex1')}}  as purch
	left join
		{{ref ('product_item_ex1')}} as prod_it
		on prod_it.prod_item_id = purch.prod_item_id
		and prod_it.prod_item_partition = purch.prod_item_partition
	where
		purch.release_date is not null
)
select
	product_id
,	sum (purchase_value) as gmv
from 
	base_join
group by
	product_id
order by
	gmv desc -- OBS: Parquet não salva dado ordenado. A função é para responder o exercicio.
limit 2