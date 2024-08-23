{{
    config(
        materialized='table',
        unique_key='fuel_type_id'
    )
}}


select
    *
from {{ ref("fuel_type") }}