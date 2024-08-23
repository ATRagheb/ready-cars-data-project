{{
    config(
        materialized='table',
        unique_key='car_options_id'
    )
}}


with options as (
    select
        *
        EXCEPT(car_id, manufacture_year, mileage, price, min_mpg, max_mpg, engine_name, transmission_type, engine_size, fuel_type, drive_train, interior_color, exterior_color)
    from {{ ref("cars") }}
)

select
    -- md5(
    --     concat(
    --         is_damaged, '|',
    --         is_first_owner, '|',
    --         is_personal_using, '|',
    --         has_turbo, '|',
    --         has_alloy_wheels, '|',
    --         has_adaptive_cruise_control, '|',
    --         has_navigation_system, '|',
    --         has_power_liftgate, '|',
    --         has_backup_camera, '|',
    --         has_keyless_start, '|',
    --         has_remote_start, '|',
    --         has_sunroof_or_moonroof, '|',
    --         has_automatic_emergency_braking, '|',
    --         has_stability_control, '|',
    --         has_leather_seats, '|',
    --         has_memory_seats, '|',
    --         has_third_row_seating, '|',
    --         has_bluetooth, '|',
    --         has_usb_port, '|',
    --         has_heated_seats, '|'
    --         )  
    --     ) as car_options_id,
    row_number() over() as car_options_id,
    *
from options