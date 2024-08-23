{{
    config(
        materialized='table', 
        unique_key='drive_train_id'
    )
    
}}


select
    *
from {{ ref("drive_train") }}