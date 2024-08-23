{{
    config(
        materialized='table',
        unique_key='color_id'
    )
}}


with colors as (
    select
        *
    from {{ ref("interior_color") }} ic
    cross join {{ ref("exterior_color") }} ec 
)



select
    row_number() over() as color_id, 
    *
    EXCEPT(interior_color_id, exterior_color_id)
from colors

