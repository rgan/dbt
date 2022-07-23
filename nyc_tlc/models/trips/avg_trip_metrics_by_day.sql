{{ config(materialized='table') }}

-- Use the `ref` function to select from other models

select avg(total_amount) as average_cost,
avg(passenger_count) as average_passenger_count,
avg(trip_distance) as average_distance
from {{ ref('trips_by_day') }}
group by day_of_week
