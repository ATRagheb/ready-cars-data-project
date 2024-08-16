{{
    config(
        materialized='table', 
        unique_key='fuel_type_id'
    )
    
}}

with fuel_types as (
    select
        distinct fuel_type as fuel_type,
    from {{ source("cars_raw_06", 'cars') }}
)

select
    row_number() over() as fuel_type_id,
    *
from fuel_types

-- {% if is_incremental() %}

-- where fuel_type_id not in (select fuel_type_id from {{ this }})

-- {% endif %}