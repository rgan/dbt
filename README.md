## dbt example with integration tests

#### Create private/public keypair for Authentication
Install open ssl: (On windows, choco install OpenSSL.Light)

Generate private key: 

```openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out private_key_snowflake.rsa -nocrypt```

Generate public key:

```openssl rsa -in private_key_snowflake.rsa -pubout -out rsa_key_snowflake.pub```

Convert key to DER format for use by python connector:
```
openssl rsa -in private_key_snowflake.rsa -out private_key_snowflake.der -outform DER
```

Trouble-shooting: https://community.snowflake.com/s/article/SQL-execution-error-New-public-key-rejected-by-current-policy-Reason-Invalid-public-key.


Install snowsql using directions here: https://docs.snowflake.com/en/user-guide/snowsql-install-config.html#downloading-the-snowsql-installer

Login in to snowsql and setup public key for a user like this:

```snowsql -a account -u user  -P```

```alter user set rsa_public_key=...;```

```desc user <>;```

#### Load data from local file to snowflake stage:
 Download NYC Trip record data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
 ```
 snowsql -a <account_id>.us-east-1 -u dbt --private-key-path .\rsa_key_snowflake.p8
 put file://C:\work\dbt\yellow_tripdata_202201.parquet @~/stagednyctlc;
 ```
#### Create database and table
Login as admin user

    create user dbt;
    alter user set rsa_public_key=...;
    create role dbt_role;
    grant all on account to role dbt_role;  # this is for testing only; need more restricted permissions
    grant role dbt_role to user dbt;

Login as dbt user:

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
 

#### Integration tests
    
The integration tests setup data for the base tables in tests/integration/data in json format. The expected
data after running DBT is also setup in tests/integration/expected in json format. The tests first create an
empty Snowflake database and run the schema migration scripts (see nyc_tlc/base_schema) to setup the base tables.
Then, it loads data from tests/integration/data into the respective base tables. Then, it runs DBT to generate
the resulting tables/views. Finally, we run pytest to query the snowflake database and verify that the expected
data is present in the resulting tables/views.  Note that pytest uses xdist to run each of the tests in parallel
so they should run as quickly as possible within constraints of the snowflake compute warehouse being used. DBT
can run multiple threads in parallel so it can quickly generate data. It is also possible to load the test data
in parallel. This means that these tests will always run in near constant time regardless of how many tables/views
are being tested. It will take as long as it takes to run the  most expensive view/table in DBT. Developers don't
need to write any python code since tests are parameterized. All they need to do is add the input data and 
expected results. To run these tests:

    inv run-integration-tests

Note that there is currently a dependency conflict between dbt-core and schemachange so the latter is
commented out in dev-requirements.txt. You will have to setup schemachange manually after installing requirements.

#### Deployment

Deployment just runs dbt for the given environment e.g. dev/stage/prod. Tests are run before 
deployment (see .github/actions/dev.yml). So if tests don't pass, no deployment occurs.

    inv run-dbt dev
