use schema public;
create table nyc_trip_record_data (
    vendorid                 integer,
    tpep_pickup_datetime     datetime,
    tpep_dropoff_datetime    datetime,
    passenger_count                 integer,
    trip_distance                   float,
    RatecodeID                      integer,
    store_and_fwd_flag              varchar(1),
    PULocationID                    integer,
    DOLocationID                    integer,
    payment_type                    integer,
    fare_amount                     float,
    extra                           float,
    mta_tax                         float,
    tip_amount                      float,
    tolls_amount                    float,
    improvement_surcharge           float,
    total_amount                    float,
    congestion_surcharge            float,
    airport_fee                     float
)