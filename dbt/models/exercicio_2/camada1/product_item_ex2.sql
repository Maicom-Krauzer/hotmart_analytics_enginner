{{ config (
    materialized='table'
) }}

select * from {{ source ('external_table','product_item_source_ex2') }}