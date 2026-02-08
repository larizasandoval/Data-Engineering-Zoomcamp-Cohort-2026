/*

Queries runned in BigQuery to answer Homework 3
Created by: larizasandoval
*/

/*Create the external table from parquet files in a bucket in GC*/

CREATE OR REPLACE EXTERNAL TABLE `arctic-eye-479622-i5.taxi.external_table`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bucket_kestra/yellow_tripdata_2024-*.parquet']
);

/*Creating native table from external table*/
CREATE OR REPLACE TABLE `arctic-eye-479622-i5.taxi.materialized_table` AS
SELECT * FROM `arctic-eye-479622-i5.taxi.external_table`;

/*******Question 1***********/
SELECT COUNT(*) FROM arctic-eye-479622-i5.taxi.materialized_table
-----Answer Output = 20332093

/*******Question 2***********/

SELECT 
    'Tabla Externa' AS origen,
    COUNT(DISTINCT PULocationID) AS total_distinct_pu
FROM `arctic-eye-479622-i5.taxi.external_table`

SELECT 
    'Tabla Regular' AS origen,
    COUNT(DISTINCT PULocationID) AS total_distinct_pu
FROM `arctic-eye-479622-i5.taxi.materialized_table`;

----Answer: is 0 MB for the External Table and 155.12 MB for the Materialized Table

/*******Question 3***********/

SELECT  PULocationID
FROM `arctic-eye-479622-i5.taxi.materialized_table`;

SELECT  PULocationID,DOLocationID
FROM `arctic-eye-479622-i5.taxi.materialized_table`;

-----Anwer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

/*******Question 4***********/

SELECT COUNT(*)
FROM `arctic-eye-479622-i5.taxi.materialized_table`
WHERE fare_amount=0;

-----Answer = 8333

/*******Question 5***********/
CREATE OR REPLACE TABLE `arctic-eye-479622-i5.taxi.strategy_partition_clustering`
-- 1. Partition by tpep_dropoff_datetime
PARTITION BY DATE(tpep_dropoff_datetime)
-- 2. Clustering by VendorId
CLUSTER BY VendorID
AS
SELECT * FROM `arctic-eye-479622-i5.taxi.materialized_table`;
----Answer =  Partition by tpep_dropoff_datetime and Cluster on VendorID

/*******Question 6***********/

SELECT DISTINCT VendorID
FROM `arctic-eye-479622-i5.taxi.materialized_table`
WHERE tpep_dropoff_datetime between '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `arctic-eye-479622-i5.taxi.strategy_partition_clustering`
WHERE tpep_dropoff_datetime between '2024-03-01' AND '2024-03-15';

----Answer = 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

/*******Question 7***********/

----Answer = GCP Bucket

/*******Question 8***********/
--Answer = False (Its depens)


/*******Question 9***********/
SELECT *
FROM `arctic-eye-479622-i5.taxi.materialized_table`;

--Answer = 2.72GB because we are calling all colums





