{{
    config(
        materialized='table',
        unique_key='transmission_type_id'
    )
}}


select
    *
from {{ ref("transmission") }}
