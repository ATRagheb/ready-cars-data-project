{{
    config(
        materialized='table', 
        unique_key='transmission_type_id'
    )
    
}}

with transmission as (
    select
        distinct transmission as transmission_type,
    from {{ source('cars_raw_06', 'cars') }}
)

select
    row_number() over() as transmission_type_id,
    *
from transmission

-- {% if is_incremental() %}

-- where transmission_type_id not in (select transmission_type_id from {{ this }})

-- {% endif %}