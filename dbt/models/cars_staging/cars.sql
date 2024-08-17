{{
    config(
        materialized='table', 
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
        interior_color.interior_color_id,
        exterior_color.exterior_color_id,
        c.mileage,
        c.price,
        c.min_mpg,
        c.max_mpg,
        case
            when cast(c.damaged as float64) = 0.0 then false
            else true
        end as is_damaged,
        case
            when cast(c.first_owner as float64) = 0.0 then false
            else true
        end as is_first_owner,
        case
            when cast(c.personal_using as float64) = 0.0 then false
            else true
        end as is_personal_using,
        case
            when cast(c.turbo as float64) = 0.0 then false
            else true
        end as has_turbo,
        case 
            when cast(c.alloy_wheels as float64) = 0.0 then false
            else true
        end as has_alloy_wheels,
        case
            when cast(c.adaptive_cruise_control as float64) = 0.0 then false
            else true
        end as has_adaptive_cruise_control,
        case
            when cast(c.navigation_system as float64) = 0.0 then false
            else true
        end as has_navigation_system,
        case
            when cast(c.power_liftgate as float64) = 0.0 then false
            else true
        end as has_power_liftgate,
        case
            when cast(c.backup_camera as float64)  = 0.0 then false
            else true
        end as has_backup_camera,
        case
            when cast(c.keyless_start as float64) = 0.0 then false
            else true
        end as has_keyless_start,
        case
            when cast(c.remote_start as float64) = 0.0 then false
            else true
        end as has_remote_start,
        case
            when cast(c.sunroof_moonroof as float64) = 0.0 then false
            else true
        end as has_sunroof_or_moonroof,
        case
            when cast(c.automatic_emergency_braking as float64) = 0.0 then false
            else true
        end as has_automatic_emergency_braking,
        case
            when cast(c.stability_control as float64) = 0.0 then false
            else true
        end as has_stability_control,
        case
            when cast(c.leather_seats as float64) = 0.0 then false
            else true
        end as has_leather_seats,
        case
            when cast(c.memory_seat as float64) = 0.0 then false
            else true
        end as has_memory_seats,
        case
            when cast(c.third_row_seating as float64) = 0.0 then false
            else true
        end as has_third_row_seating,
        case
            when cast(c.apple_car_play_android_auto as float64) = 0.0 then false
            else true
        end as has_apple_car_play_android_auto,
        case
            when cast(c.bluetooth as float64) = 0.0 then false
            else true
        end as has_bluetooth,
        case
            when cast(c.usb_port as float64) = 0.0 then false
            else true
        end as has_usb_port,
        case
            when cast(c.heated_seats as float64) = 0.0 then false
            else true
        end as has_heated_seats,
    from {{ source("cars_raw_06", 'cars') }} as c
    left join {{ ref('engines') }} as e on e.engine_name = c.engine and e.engine_size = c.engine_size
    left join {{ ref("transmission") }} as transmission on transmission.transmission_type = c.transmission
    left join {{ ref("drive_train") }} as drive_train on drive_train.drive_train = c.drive_train
    left join {{ ref("fuel_type") }} as fuel_type on fuel_type.fuel_type = c.fuel_type
    left join {{ ref("interior_color") }} as interior_color on interior_color.interior_color = c.interior_color
    left join {{ ref("exterior_color") }} as exterior_color on exterior_color.exterior_color = c.exterior_color
)


select
    row_number() over() as car_id,
    *
from cars



{% if is_incremental() %}

where event_time >= (select coalesce(max(event_time),'1900-01-01') from {{ this }} )

{% endif %}