{{ config(
    materialized='incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['purchase_id', 'transaction_date'],
    partition_by= ['transaction_date']
) }}

with
filtro_dia_anterior as (
    select *
    from
        {{ref ('insert_event')}}
    {% if is_incremental() %}
    where transaction_date = today() -1 -- Necessário considerar idempotência no today() em reexecuções de determinados dias
    union
    select *
    from {{ this }} -- Union com os dados da tabela atual para conseguir executar função de janela no script abaixo.
    {% endif %}
)
select
	purchase_id
,	transaction_date
,	LAST_VALUE (order_date ignore nulls) over (partition by purchase_id order by transaction_date) as order_date
,	LAST_VALUE (prod_item_id ignore nulls)  over (partition by purchase_id order by transaction_date)as prod_item_id
,	LAST_VALUE (buyer_id ignore nulls)  over (partition by purchase_id order by transaction_date) as buyer_id
,	LAST_VALUE (producer_id ignore nulls)  over (partition by purchase_id order by transaction_date) as producer_id
,	LAST_VALUE (release_date ignore nulls)  over (partition by purchase_id order by transaction_date) as release_date
,	LAST_VALUE (product_id ignore nulls)  over (partition by purchase_id order by transaction_date) as product_id
,	LAST_VALUE (item_quantity ignore nulls)  over (partition by purchase_id order by transaction_date) as item_quantity
,	LAST_VALUE (purchase_value ignore nulls)  over (partition by purchase_id order by transaction_date) as purchase_value
,	LAST_VALUE (subsidiary ignore nulls)  over (partition by purchase_id order by transaction_date) as subsidiary
from
	filtro_dia_anterior