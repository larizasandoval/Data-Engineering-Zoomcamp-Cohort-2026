#### Homework #4

1. **Question 1. dbt Lineage and Execution**

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

The answer us
- `int_trips_unioned` only because the --select type command

---

2. **Question 2. dbt Test**

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

the answe is:
- dbt will fail the test, returning a non-zero exit code


---

3. **Question 3. Counting Records in `fct_monthly_zone_revenue`**

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

With the following query
```
	SELECT COUNT(*) FROM prod.fct_monthly_zone_revenue
```
the answer is **12,184**

---

4. **Question 4. Best Performing Zone for Green Taxis (2020)**

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

With the following query 
```
	SELECT  pickup_zone, 
        	sum(revenue_monthly_total_amount) total_amount
	FROM prod.fct_monthly_zone_revenue
	WHERE service_type='Green' AND YEAR(revenue_month)=2020
	GROUP BY pickup_zone
	ORDER BY total_amount DESC
```
The answer is **East Harlem North**


---

5. **Question 5. Green Taxi Trip Counts (October 2019)**

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

With the following query
```
	SELECT  sum(total_monthly_trips) total_amount
	FROM prod.fct_monthly_zone_revenue
	WHERE service_type='Green' 
      AND YEAR(revenue_month)=2019 
      AND MONTH(revenue_month)=10
```
The answer is **384,624**

---

6. **Question 6. Build a Staging Model for FHV Data**

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

With the following query 
```
	SELECT  COUNT(*)
	FROM prod.fhv_tripdata
	where dispatching_base_num is NOT null
```
The answer is **43,244,693**
