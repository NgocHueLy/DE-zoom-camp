-- Create external table from GG Cloud Storage
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-412502.ny_taxi.external_green_taxi_data_2022`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dezc_m3_data_warehouse/green_taxi_data_2022']
);
  

-- create a parition, cluster table from materialized table

CREATE OR REPLACE TABLE dtc-de-course-412502.ny_taxi.green_taxi_data_2022_partitioned_clustered
PARTITION BY lpep_pickup_date
CLUSTER BY PULocationID AS
SELECT * FROM dtc-de-course-412502.ny_taxi.green_taxi_data_2022;


-- Question 2: count the distinct number of PULocationIDs from external and materialized green_taxi_data-2022

SELECT count(distinct PULocationID) 
FROM `dtc-de-course-412502.ny_taxi.external_green_taxi_data_2022`;

SELECT count(distinct PULocationID) 
FROM `dtc-de-course-412502.ny_taxi.green_taxi_data_2022`;

-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(1) 
FROM `dtc-de-course-412502.ny_taxi.external_green_taxi_data_2022`
WHERE fare_amount = 0;


--Question 5: retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

SELECT distinct PULocationID
FROM `dtc-de-course-412502.ny_taxi.green_taxi_data_2022`
WHERE lpep_pickup_date >= '2022-06-01' AND lpep_pickup_date <= '2022-06-30';


SELECT distinct PULocationID
FROM `dtc-de-course-412502.ny_taxi.green_taxi_data_2022_partitioned_clustered`
WHERE lpep_pickup_date >= '2022-06-01' AND lpep_pickup_date <= '2022-06-30';