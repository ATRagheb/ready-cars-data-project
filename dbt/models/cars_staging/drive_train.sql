{{
    config(
        materialized='table', 
        unique_key='drive_train_id'
    )
    
}}

with drive_trains as (
    select
        distinct drive_train as drive_train,
    from {{ source("cars_raw_06", 'cars') }}
)

select
    row_number() over() as drive_train_id,
    *
from drive_trains

-- {% if is_incremental() %}

-- where drive_train_id not in (select drive_train_id from {{ this }})

-- {% endif %}