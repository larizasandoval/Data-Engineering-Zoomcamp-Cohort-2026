# Homework 7

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

Answer:
```
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```
According to the previos output the answer is **v25.3.9**

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

How long did it take to send the data?

In my case took 58.09 seconds so **60 seconds** so the closest answer.


## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

According to this consumer script: 
```
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'rides'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    pickup_dt = datetime.fromtimestamp(ride.lpep_pickup_datetime / 1000)
    dropoff_dt = datetime.fromtimestamp(ride.lpep_dropoff_datetime / 1000)
    print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
          f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
          f"pickup={pickup_dt}, dropoff={dropoff_dt}")
    if ride.trip_distance > 5.0:
        count += 1
    print(f'... total messages with distance > 5.0 so far: {count}')
   
consumer.close()
```
The answe is: **8506**


## Part 2: PyFlink (Questions 4-6)


## Question 4. Tumbling window - pickup location

Which `PULocationID` had the most trips in a single 5-minute window?


```sql
+---------------------+--------------+-----------+
| window_start        | pulocationid | num_trips |
|---------------------+--------------+-----------|
| 2025-10-22 08:40:00 | 74           | 15        |
| 2025-10-20 16:30:00 | 74           | 14        |
| 2025-10-08 10:35:00 | 74           | 13        |
+---------------------+--------------+-----------+
```
According to this output the answer is: **74**

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

How many trips were in the longest session?

According to this
```
SELECT
     PULocationID,
     num_trips,
     window_start
FROM processed_sessions_aggregated
ORDER BY num_trips DESC
LIMIT 5;
+--------------+-----------+---------------------+
| pulocationid | num_trips | window_start        |
|--------------+-----------+---------------------|
| 74           | 81        | 2025-10-08 06:46:14 |
| 74           | 72        | 2025-10-01 06:52:23 |
| 74           | 71        | 2025-10-22 06:58:31 |
| 74           | 71        | 2025-10-28 08:31:08 |
| 74           | 70        | 2025-10-27 06:56:30 |
+--------------+-----------+---------------------+

```
**The answer is 81**


## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?
According to this output

```
+---------------------+---------------------+-------------------+
| window_start        | window_end          | total_tips        |
|---------------------+---------------------+-------------------|
| 2025-10-16 18:00:00 | 2025-10-16 19:00:00 | 510.8599999999999 |
+---------------------+---------------------+-------------------+
```
The answer is: **2025-10-16 18:00:00**

