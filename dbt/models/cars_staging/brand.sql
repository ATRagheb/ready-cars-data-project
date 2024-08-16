{{
    config(
        materialized='table', 
        unique_key='brand_id'
    )
    
}}

with brand as (
    select
        distinct brand as car_brand,
        model as model,
    from {{ source("cars_raw_06", 'cars') }}
)

select
    row_number() over() as brand_id,
    *
from brand


-- {% if is_incremental() %}

-- where brand_id not in (select brand_id from {{ this }})

-- {% endif %}