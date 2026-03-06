# Module 6 Homework


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

Answer: **the version is 4.1.1**

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

Answer: According to this output
```
part-00000-2fb7b2dd-f317-4e61-a00a-f7b05288c202-c000.snappy.parquet: 24.39 MB
part-00001-2fb7b2dd-f317-4e61-a00a-f7b05288c202-c000.snappy.parquet: 24.42 MB
part-00002-2fb7b2dd-f317-4e61-a00a-f7b05288c202-c000.snappy.parquet: 24.42 MB
part-00003-2fb7b2dd-f317-4e61-a00a-f7b05288c202-c000.snappy.parquet: 24.41 MB
Average file size: 24.41 MB
```
The answer is **25MB**


## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

Answer: with the following query
```
spark.sql("""
SELECT
COUNT(*)
FROM yellow
WHERE YEAR(tpep_pickup_datetime)=2025 
      AND MONTH(tpep_pickup_datetime)=11 
      AND DAY(tpep_pickup_datetime)=15
        """         
          ).show()
```
**The answer is 162,604**



## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

Answer: according to this query
```
spark.sql("""
SELECT *
FROM
(SELECT *, (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS trip_duration_hours
FROM yellow
)A order by trip_duration_hours DESC
        """         
          ).show(5)
```
**The answer is 90.6**


## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

According with this url: http://localhost:4040/jobs/ **the answer is 4040**



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

According to this query 
```
spark.sql("""
    SELECT COUNT(*) as trip_count, z.Zone
    FROM yellow y
    LEFT JOIN zones z ON y.PULocationID = z.LocationID  
    GROUP BY z.Zone   
    ORDER BY trip_count  ASC         
        """).show(5)
```

The answer is **Governor's Island/Ellis Island/Liberty Island**


