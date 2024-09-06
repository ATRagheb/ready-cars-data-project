{{
    config(
        materialized='table',
        unique_key='card_id'
    )
}}



with cars as (
    select
        cast(SUBSTR(c.manufacture_year, 1, STRPOS(c.manufacture_year, '.') - 1) as int64) as manufacture_year,
        c.mileage,
        c.price,
        c.min_mpg,
        c.max_mpg,   
        e.engine_id,
        transmission.transmission_type_id,
        brand.car_brand_id,
        drive_train.drive_train_id,
        fuel_type.fuel_type_id,
        color.color_id,
        options.car_options_id
    from {{ ref("cars") }} as c
    left join {{ ref('dim_engine') }} as e on e.engine_name = c.engine_name and e.engine_size = c.engine_size
    left join {{ ref("dim_transmission_type") }} as transmission on transmission.transmission_type = c.transmission_type
    left join {{ ref("dim_drive_train") }} as drive_train on drive_train.drive_train = c.drive_train
    left join {{ ref("dim_fuel_type") }} as fuel_type on fuel_type.fuel_type = c.fuel_type
    left join {{ ref("dim_brand") }} as brand on brand.car_model = c.model and brand.car_brand = c.brand
    left join {{ ref("dim_color") }} as color on color.interior_color = c.interior_color and color.exterior_color = c.exterior_color
    left join {{ ref("car_options") }} as options on (
            options.is_damaged = c.is_damaged
            and options.is_first_owner = c.is_first_owner
            and options.is_personal_using = c.is_personal_using
            and options.has_turbo = c.has_turbo
            and options.has_alloy_wheels = c.has_alloy_wheels
            and options.has_adaptive_cruise_control = c.has_adaptive_cruise_control
            and options.has_navigation_system = c.has_navigation_system
            and options.has_power_liftgate = c.has_power_liftgate
            and options.has_backup_camera = c.has_backup_camera
            and options.has_keyless_start = c.has_keyless_start
            and options.has_remote_start = c.has_remote_start
            and options.has_sunroof_or_moonroof = c.has_sunroof_or_moonroof
            and options.has_automatic_emergency_braking = c.has_automatic_emergency_braking
            and options.has_stability_control = c.has_stability_control
            and options.has_leather_seats = c.has_leather_seats
            and options.has_memory_seats = c.has_memory_seats
            and options.has_third_row_seating = c.has_third_row_seating
            and options.has_bluetooth = c.has_bluetooth
            and options.has_usb_port = c.has_usb_port
            and options.has_heated_seats = c.has_heated_seats
    ) 
)


select
    row_number() over() as car_id,
    *
from cars