{{ config (
    materialized='table'
) }}

select * from {{ source ('external_table','purchase_extra_info_source_ex2') }}