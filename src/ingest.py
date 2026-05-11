from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count

'''
Step 1: Create valid Spark session
'''
def get_spark(shuffle_partitions):
    return SparkSession. \
        builder. \
        appName("Read Parquet"). \
        config("spark.sql.shuffle.partitions", shuffle_partitions). \
        getOrCreate()

def read_parquet(spark, file_path):
    """Read parquet file(s). file_path can be a string or list of strings."""
    if isinstance(file_path, list):
        return spark.read.parquet(*file_path)  # Unpack list as multiple arguments
    return spark.read.parquet(file_path)

if __name__ == "__main__":
    spark = get_spark()

    df = read_parquet(spark, '/Users/nicolasmir/projects/pyspark-taxi-pipeline/data/raw/yellow_tripdata_2026-01.parquet')

    # Print column names
    print("Column names:", df.columns)

    # Print number of rows
    print("Number of rows:", df.count())

    '''
    Step 2: Understand low quality data
    '''
    df.printSchema()
    df.show(5)
    df.describe("trip_distance", "fare_amount", "passenger_count").show()

    # Some trips have a zero distance.
    # Some fares are negative.
    # Some passenger counts are zeros.

    # Count NULLs in each column
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    null_counts.show()

    # Some columns have NULLs but we are not afraid because they are not used for any calculations.