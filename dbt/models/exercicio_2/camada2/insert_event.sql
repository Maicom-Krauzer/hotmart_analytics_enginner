{{ config(
    materialized='incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['purchase_id', 'transaction_date'],
    partition_by= ['transaction_date']
) }}

with
purchase_last_day_ex2 as (
	select
		purchase_id
	,	transaction_date
	,   max_by(order_date, transaction_datetime) as order_date
	,   max_by (prod_item_id,transaction_datetime) as prod_item_id
	,	max_by(buyer_id, transaction_datetime) as buyer_id
	,   max_by(producer_id, transaction_datetime) as producer_id
	,   max_by(release_date, transaction_datetime) as release_date
	from {{ ref ('purchase_ex2')}}
	{% if is_incremental() %}
	where transaction_date = today() -1 -- Necessário considerar idempotência no today() em reexecuções de determinados dias
	{% endif %}
	group by
	    purchase_id
	,   transaction_date
),
product_item_last_day_ex2 as (
	select
		purchase_id
	,	transaction_date
	,	max_by(product_id, transaction_datetime) as product_id
	,	max_by(item_quantity, transaction_datetime) as item_quantity
	,	max_by(purchase_value, transaction_datetime) as purchase_value
	from {{ref ('product_item_ex2') }}
	{% if is_incremental() %}
	where transaction_date = today() -1 -- Necessário considerar idempotência no today() em reexecuções de determinados dias
	{% endif %}
	group by
	    purchase_id
	,   transaction_date
    order by 1,2
),
purchase_extra_info_last_day_ex2 as (
	select
		purchase_id
	,	transaction_date
	,	max_by(subsidiary, transaction_datetime) as subsidiary
	from {{ref ('purchase_extra_info_ex2')}}
	{% if is_incremental() %}
	where transaction_date = today() -1 -- Necessário considerar idempotência no today() em reexecuções de determinados dias
	{% endif %}
	group by
		purchase_id
	,   transaction_date
),
join_purchase_and_product_item as (
	select
		COALESCE(purch.purchase_id, prod_it.purchase_id) as purchase_id
	,	COALESCE (purch.transaction_date, prod_it.transaction_date) as transaction_date
	,	purch.order_date
	,	purch.prod_item_id
	,	purch.buyer_id
	,	purch.producer_id
	,	purch.release_date	
	,	prod_it.product_id
	,	prod_it.item_quantity
	,	prod_it.purchase_value
	from
		purchase_last_day_ex2 as purch
	full outer join
		product_item_last_day_ex2 as prod_it
		on prod_it.purchase_id = purch.purchase_id
		and prod_it.transaction_date = purch.transaction_date
)
select
	COALESCE(ppi.purchase_id, purch_extr.purchase_id) as purchase_id
,	COALESCE (ppi.transaction_date, purch_extr.transaction_date) as transaction_date
,	ppi.order_date
,	ppi.prod_item_id
,	ppi.buyer_id
,	ppi.producer_id
,	ppi.release_date	
,	ppi.product_id
,	ppi.item_quantity
,	ppi.purchase_value
,	purch_extr.subsidiary
from
	join_purchase_and_product_item as ppi
full outer join
	purchase_extra_info_last_day_ex2 as purch_extr
	on purch_extr.purchase_id = ppi.purchase_id
	and purch_extr.transaction_date = ppi.transaction_date