{{ config(materialized='table') }}

select dayname(tpep_pickup_datetime) as day_of_week,
passenger_count, trip_distance, total_amount
from nyc_trip_record_data
