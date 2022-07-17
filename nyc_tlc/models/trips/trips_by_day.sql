
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select dayname(tpep_pickup_datetime) as day_of_week,
passenger_count, trip_distance, total_amount
from nyc_trip_record_data
