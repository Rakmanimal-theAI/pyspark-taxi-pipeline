# NYC Taxi Batch Pipeline (PySpark)
> Distributed batch processing of ~1GB NYC taxi trip records using PySpark,
> producing a partitioned Parquet summary by pickup zone and hour.

## What it does
- Reads raw Parquet files from the NYC TLC open dataset
- Cleans bad records (nulls, zero-distance trips, impossible speeds)
- Derives new columns: trip duration, avg speed, pickup hour
- Aggregates to a zone × hour summary table
- Validates output quality, then writes partitioned Parquet

## Why it's interesting
PySpark processes data in distributed partitions — the transformations here
are lazy (nothing runs until `.write` or `.count()`). Partitioning the output
by `pickup_hour` means downstream queries only scan the partitions they need.

## Architecture
[paste the architecture diagram as a PNG, or link to it]

## Tech stack
Python · PySpark · Parquet · pytest

## Running it
```bash
pip install -r requirements.txt
python src/pipeline.py
```

## What I learned
- Spark's lazy evaluation model and when actions trigger execution
- Why shuffle partitions matter for aggregation performance
- Partitioned writes and predicate pushdown
- Testing Spark jobs with small in-memory DataFrames

## Dataset
NYC TLC Trip Record Data (Yellow Taxi, Jan 2026) — ~8M rows, ~500MB
