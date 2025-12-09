{{ config (
    materialized='table'
) }}

select * from {{ source ('external_table','purchase_source_ex2') }}