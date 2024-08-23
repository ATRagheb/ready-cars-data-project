{{
    config(
        materialized='table', 
        unique_key='engine_id'
    )
    
}}


select
    *
from {{ ref("engines") }}