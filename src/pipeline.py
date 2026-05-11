from ingest import get_spark,read_parquet
from transform import clean,add_features,aggregate_by_zone_hour
from validate import validate

'''
Step 6 Write the whole pipeline
'''
spark = get_spark()

df = read_parquet(spark, '/Users/nicolasmir/projects/pyspark-taxi-pipeline/data/raw/yellow_tripdata_2026-01.parquet')

df = clean(df)

df = add_features(df)

df = aggregate_by_zone_hour(df)

validate(df)

df.write.partitionBy("pickup_hour").mode("overwrite").parquet("output")