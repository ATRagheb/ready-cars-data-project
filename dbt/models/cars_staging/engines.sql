{{
    config(
        materialized='table', 
        unique_key='engine_id'
    )
    
}}

with engines as (
    select
        distinct engine as engine_name,
        engine_size as engine_size
    from {{ source("cars_raw_06", 'cars') }}
)

select
    row_number() over() as engine_id,
    *
from engines

-- {% if is_incremental() %}

-- where engine_id not in (select engine_id from {{ this }})

-- {% endif %}