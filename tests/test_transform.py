from pyspark.sql import SparkSession
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Create Spark session
spark = SparkSession.builder.appName("TestData").getOrCreate()

# Create data as list of rows without specifying schema
data = [
    (1, "2026-01-15 08:30:00", "2026-01-15 08:45:00", 2, 5.5, 1, "N", 100, 200, 1, -10.50, 0.50, 0.50, 2.00, 0.00, 0.30, -7.20, 2.50, 0.00, 0.00),
    (2, "2026-01-15 09:00:00", "2026-01-15 09:30:00", 0, 3.2, 1, "N", 150, 250, 2, 15.50, 1.00, 0.50, 0.00, 0.00, 0.30, 17.30, 2.50, 0.00, 0.00),
    (1, "2026-01-15 10:00:00", "2026-01-15 10:25:00", 3, 7.8, 1, "N", 180, 220, 1, 25.50, 1.50, 0.50, 5.00, 0.00, 0.30, 32.80, 2.50, 0.00, 0.00)
]

# Let Spark infer the schema
columns = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", 
           "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", 
           "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
           "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee", 
           "cbd_congestion_fee"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the data
df.show(truncate=False)

# Show which rows are invalid based on your cleaning criteria
from src.transform import clean

df = clean(df)

# Show data after cleaning
df.show(truncate=False)