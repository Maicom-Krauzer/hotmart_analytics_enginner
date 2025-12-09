{{ config (
    materialized='table'
) }}

with
base_join AS  (
	select
		purch.producer_id
	,	prod_it.purchase_value
	,	prod_it.item_quantity
	from
		{{ref ('purchase_ex1')}} as purch
	left join
		{{ref ('product_item_ex1')}} as prod_it
		on prod_it.prod_item_id = purch.prod_item_id
		and prod_it.prod_item_partition = purch.prod_item_partition
	where
		release_date is not null
	and release_date between date'2021-01-01' and date'2021-12-31'
)
select
	producer_id
,	sum (purchase_value) as gmv
from
	base_join
group by
	producer_id
order by  -- OBS: Parquet não salva dado ordenado. A função é para responder o exercicio.
	gmv desc
limit 50