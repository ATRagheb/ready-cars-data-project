{{
    config(
        materialized='table', 
        unique_key='car_brand_id'
    )
    
}}



select
    brand_id as car_brand_id,
    model as car_model,
    car_brand
from {{ ref("brand") }}