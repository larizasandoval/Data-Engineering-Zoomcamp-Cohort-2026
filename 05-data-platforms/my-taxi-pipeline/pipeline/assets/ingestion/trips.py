"""@bruin
name: ingestion.trips
type: python
connection: duckdb-default
image: python:3.11

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: float
  - name: taxi_type
    type: string
  - name: payment_type_id
    type: integer
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime

def materialize():
    """
    Uses Bruin's Python Materialization to generate data and
    automatically appends it to the ingestion.trips table in DuckDB.
    """

    # 1. Load execution window environment variables
    # Uses the start date passed during 'bruin run'.
    start_date = os.getenv("BRUIN_START_DATE", "2021-01-01")

    # 2. Load pipeline variables (taxi_types)
    # Reads the taxi_types variable defined in pipeline.yml.
    bruin_vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    all_data = []

    # 3. Data generation business logic
    for t_type in taxi_types:
        # Generate sample data for each taxi type
        data = pd.DataFrame({
      
            'pickup_datetime': [pd.to_datetime(start_date)] * 2,
            'dropoff_datetime': [pd.to_datetime(start_date) + pd.Timedelta(minutes=15),
                                 pd.to_datetime(start_date) + pd.Timedelta(minutes=30)],
            'pickup_location_id': [101, 102],
            'dropoff_location_id': [201, 202],
            'passenger_count': [1, 2],
            'fare_amount': [15.5, 25.0],
            'payment_type_id': [0, 1],
            'taxi_type': [t_type, t_type]
        })

        # Convert datetime to string to avoid potential pyarrow timezone
        # issues specifically on Windows environments.
        data['pickup_datetime'] = data['pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['dropoff_datetime'] = data['dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        all_data.append(data)

    # Return empty DataFrame if no data is generated
    if not all_data:
        return pd.DataFrame()

    # 4. Combine data and add extraction timestamp for lineage
    final_df = pd.concat(all_data, ignore_index=True)
    final_df['extracted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Returning the DataFrame allows Bruin to load it into the connected DuckDB table.
    return final_df