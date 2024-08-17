{{
    config(
        materialized='table', 
        unique_key='exterior_color_id'
    )
    
}}


with exterior_colors as (
    select
        distinct exterior_color
    from {{ source("cars_raw_06", "cars") }}
)


select
    row_number() over() as exterior_color_id,
    exterior_color
from exterior_colors