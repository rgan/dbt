## dbt example

#### Create private/public keypair for Authentication
Install open ssl: On windows, choco install OpenSSL.Light
Generate private key: 
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
Generate public key:
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
Login in to snowsql:
snowsql -a <account> -u user -P

alter user set rsa_public_key="...";
desc user dbt;

#### Load data from local file to snowflake stage:
 snowsql -a shb16499.us-east-1 -u dbt --private-key-path .\rsa_key_snowflake.p8
 put file://C:\work\dbt\yellow_tripdata_202201.parquet @~/stagednyctlc;
 
#### Create database and table
Login as admin user:
    create role dbt_role;
    grant create database on account to role dbt_role;
    grant role dbt_role to user dbt;
Login as dbt user
use dbt_role;
create database nyc_tlc;

create or replace file format my_parquet_format type = parquet;
use WAREHOUSE COMPUTE_WH;
      
create or replace table nyc_trip_record_data (
  vendorid                       integer,
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

COPY INTO nyc_trip_record_data (
vendorid,
tpep_pickup_datetime,
tpep_dropoff_datetime,
passenger_count,
trip_distance,
RatecodeID,
store_and_fwd_flag,
PULocationID,
DOLocationID,
payment_type,
fare_amount,
extra,
mta_tax,
tip_amount,
tolls_amount,
improvement_surcharge,
total_amount,
congestion_surcharge,
airport_fee
) from (select $1:vendorid,
$1:tpep_pickup_datetime,
$1:tpep_dropoff_datetime,
$1:passenger_count,
$1:trip_distance,
$1:RatecodeID,
$1:store_and_fwd_flag,
$1:PULocationID,
$1:DOLocationID,
$1:payment_type,
$1:fare_amount,
$1:extra,
$1:mta_tax,
$1:tip_amount,
$1:tolls_amount,
$1:improvement_surcharge,
$1:total_amount,
$1:congestion_surcharge,
$1:airport_fee
FROM @~/stagednyctlc)
FILE_FORMAT = (format_name = 'my_parquet_format'); 
 
#### Run DBT
inv run-dbt dev
