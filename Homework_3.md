#### Homework #3
1. *Question 1. Counting records
What is count of records for the 2024 Yellow Taxi Data?*

    Running the following query
```SELECT COUNT(*) FROM arctic-eye-479622-i5.taxi.materialized_table```
    The answer is **20,332,093**

2. _Question 2. Data read estimation
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables._

_What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?_


    With the following queries 
```
SELECT 
    'Tabla Externa' AS origen,
    COUNT(DISTINCT PULocationID) AS total_distinct_pu
FROM `arctic-eye-479622-i5.taxi.external_table`

SELECT 
    'Tabla Regular' AS origen,
    COUNT(DISTINCT PULocationID) AS total_distinct_pu
FROM `arctic-eye-479622-i5.taxi.materialized_table`;

```
The answer is **0 MB for the External Table and 155.12 MB for the Materialized Table**

3. _Question 3. Understanding columnar storage
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table._

_Why are the estimated number of Bytes different?_

    Looking at these queries:
```
SELECT  PULocationID
FROM `arctic-eye-479622-i5.taxi.materialized_table`;

SELECT  PULocationID,DOLocationID
FROM `arctic-eye-479622-i5.taxi.materialized_table`;
```
The answer is because **BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**

4. _Question 4. Counting zero fare trips
How many records have a fare_amount of 0?_

With this query. 
```
SELECT COUNT(*)
FROM `arctic-eye-479622-i5.taxi.materialized_table`
WHERE fare_amount=0;

```
The answer is **8,333**


5. _Question 5. Partitioning and clustering
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)_

With this queries:
```
CREATE OR REPLACE TABLE `arctic-eye-479622-i5.taxi.strategy_partition_clustering`
-- 1. Partition by tpep_dropoff_datetime
PARTITION BY DATE(tpep_dropoff_datetime)
-- 2. Clustering by VendorId
CLUSTER BY VendorID
AS
SELECT * FROM `arctic-eye-479622-i5.taxi.materialized_table`;
```
The answer is **Partition by tpep_dropoff_datetime and Cluster on VendorID**


6. _Question 6. Partition benefits
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)_

_Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?_



With this queries:

```
SELECT DISTINCT VendorID
FROM `arctic-eye-479622-i5.taxi.materialized_table`
WHERE tpep_dropoff_datetime between '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `arctic-eye-479622-i5.taxi.strategy_partition_clustering`
WHERE tpep_dropoff_datetime between '2024-03-01' AND '2024-03-15';
```
The answer is **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

7. _Question 7. External table storage
Where is the data stored in the External Table you created?_

The answer is **GCP Bucket**

8. _Question 8. Clustering best practices
It is best practice in Big Query to always cluster your data_
The answer is **False** it depends on the case. 

9. _Question 9. Understanding table scans
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?_

With this query:

```
SELECT *
FROM `arctic-eye-479622-i5.taxi.materialized_table`;
```

The answer is **2.72GB because we are calling all colums**