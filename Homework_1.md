#### Homework #1

1. Question 1: What's the version of pip in the python:3.13 image? (1 point)
Running the following commands:
**docker run -it python3:13.11**. 
**pip --version**
I got this answer:   pip 25.3

2. Question 2. Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database? (1 point)
Answer is db:5432 

3. Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile? 
After running a pipeline to load green data and with the following query I got the anwser: 8007
```
SELECT count(*) 
FROM public.green_taxi_trips
where trip_distance <=1 and 
		lpep_pickup_datetime >='2025-11-01' and 
		lpep_pickup_datetime <='2025-12-01'
```

4. Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles. (1 point)

With the following query I got the date was **'2025-11-14'**

```

SELECT *
FROM public.green_taxi_trips
where trip_distance <=100 and 
		lpep_pickup_datetime >='2025-11-01' and 
		lpep_pickup_datetime <='2025-12-01'
ORDER BY trip_distance DESC

```
5. Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025? (1 point)

With the following query I got the answer is the zone **"East Harlem North"**

```
SELECT sum(A.total_amount)totalAmount,B."Zone"
FROM public.green_taxi_trips A
LEFT JOIN public.zones B ON A."PULocationID"=B."LocationID"
where A.lpep_pickup_datetime::date = '2025-11-18' 
GROUP BY B."Zone"
ORDER BY totalAmount DESC

```

6. Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? (1 point)

With the following query I got the anwser is **"Yorkville West"**

```
SELECT A.tip_amount, B."Zone" PickupZone , C."Zone" DropZone
FROM public.green_taxi_trips A
LEFT JOIN public.zones B ON A."PULocationID"=B."LocationID"
LEFT JOIN public.zones C ON A."DOLocationID"=C."LocationID"
WHERE B."Zone" = 'East Harlem North'
ORDER BY A.tip_amount DESC
```

7. Question 7. Which of the following sequences describes the Terraform workflow for: 
	1) Downloading plugins and setting up backend, 
	2) Generating and executing changes, 
	3) Removing all resources?
I think according to the steps the answer is **terraform init, terraform apply -auto-approve, terraform destroy**
