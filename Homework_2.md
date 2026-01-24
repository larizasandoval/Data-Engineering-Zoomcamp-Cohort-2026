#### Homework #2 

Elaborated by: larizasandoval

1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

Answer: Looking in the Outputs tab of the execution and then select inside extract task the ouputFile (yellow_tripdata_2020-12.csv) I see the size of the size file is **128.3MiB**

2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

According to the code.The answer is: **green_tripdata_2020-04**



3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

After backfill the 2020 data and with the following query I got this number of rows **24,648,499** 
```
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE LEFT(filename,20)= 'yellow_tripdata_2020'
```

4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?

After backfill the 2020 data and With the following query I got this number of rows **1,734,051** 
```
SELECT COUNT(*)
FROM public.green_tripdata
WHERE  LEFT(filename,19)= 'green_tripdata_2020'
```


5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

After backfill the march data and looking the output tap in the task **yellow_copy_in_to_staging_table** I can see the total rows are **1,925,152**


6. How would you configure the timezone to New York in a Schedule trigger?

The correct answer is: **Add a timezone property set to America/New_York in the Schedule trigger configuration**

```
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
      taxi: green

  - id: yellow_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"
    timezone: America/New_York
    inputs:
      taxi: yellow

```
