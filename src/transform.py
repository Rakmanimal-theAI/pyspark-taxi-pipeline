from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp, hour, count, avg

'''
Step 3 Drop low-quality data as observed in Step 2
'''
def clean(df):
    df_cleaned = df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0) & (col("passenger_count") > 0))
    return(df_cleaned)

'''
Step 4 Add additional features
'''
def add_features(df):
    # Trip duration
    df = df.withColumn("trip_duration",
        (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
    )

    # Filter out invalid durations BEFORE calculating speed
    df = df.filter(col("trip_duration") > 0)

    # Pickup hour
    df = df.withColumn("pickup_hour",
        hour(col("tpep_pickup_datetime"))
    )

    # Average speed
    miles_to_km = 1.60934
    min_to_hours = 1/60
    df = df.withColumn("average_speed",
        (col("trip_distance") * miles_to_km) / (col("trip_duration") * min_to_hours)
    )
    return df

'''
Step 5 Aggregate by zone and hour
'''
def aggregate_by_zone_hour(df):
    df = df.groupBy("PULocationID","pickup_hour").agg(
        count("*").alias("total_trips"),
        avg("fare_amount").alias("Average_fare_amount"),
        avg("trip_distance").alias("Average_trip_distance"),
        avg("trip_duration").alias("Average_trip_duration")
    ).orderBy(col("total_trips").desc())
    return df