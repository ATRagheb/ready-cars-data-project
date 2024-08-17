{{
    config(
        materialized='table', 
        unique_key='interior_color_id'
    )
    
}}


with interior_colors as (
    select
        distinct interior_color
    from {{ source("cars_raw_06", "cars") }}
)


select
    row_number() over() as interior_color_id,
    interior_color
from interior_colors