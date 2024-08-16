{{
    config(
        materialized='incremental', 
        unique_key='car_id'
    )
    
}}

with cars as (
    select
        cast(SUBSTR(c.year, 1, STRPOS(c.year, '.') - 1) as int64) as manufacture_year,
        e.engine_id,
        transmission.transmission_type_id,
        drive_train.drive_train_id,
        fuel_type.fuel_type_id,
        c.mileage,
        c.price,
        c.min_mpg,
        c.max_mpg
    from {{ source("cars_raw_06", 'cars') }} as c
    left join {{ ref('engines') }} as e on e.engine_name = c.engine and e.engine_size = c.engine_size
    left join {{ ref("transmission") }} as transmission on transmission.transmission_type = c.transmission
    left join {{ ref("drive_train") }} as drive_train on drive_train.drive_train = c.drive_train
    left join {{ ref("fuel_type") }} as fuel_type on fuel_type.fuel_type = c.fuel_type
)


select
    row_number() over() as car_id,
    *
from cars



{% if is_incremental() %}

where event_time >= (select coalesce(max(event_time),'1900-01-01') from {{ this }} )

{% endif %}