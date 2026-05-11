# NYC Taxi Batch Pipeline (PySpark)

> Distributed batch processing of 12 months of NYC taxi trip records (~6GB)
> using PySpark, producing a partitioned Parquet summary by pickup zone and hour.

## What it does

- Reads raw Parquet files from the NYC TLC open dataset
- Cleans bad records (nulls, zero-distance trips, impossible speeds)
- Derives new columns: trip duration, average speed, pickup hour, year-month
- Aggregates to a zone × hour × month summary table
- Validates output quality, then writes partitioned Parquet
- Benchmarks the pipeline against pandas across multiple data sizes and Spark configurations

## Why it's interesting

PySpark processes data in distributed partitions — the transformations here are
lazy (nothing runs until `.write` or `.count()`). This means Spark builds a full
execution plan before touching any data, which allows it to optimise across steps
rather than executing each one eagerly.

Partitioning the output by `year_month` and `pickup_hour` means downstream queries
only scan the partitions they need — a query filtering on `pickup_hour = 8` skips
every other partition entirely. This is called predicate pushdown, and it's one of
the core reasons columnar formats like Parquet paired with partitioned storage are
standard in production data lakes.

## Benchmarks

The pipeline includes a full benchmark suite (`benchmarks/run_benchmarks.py`) that
compares pandas vs Spark across data sizes and tests the effect of Spark configuration
choices. Key findings:

### Pandas vs Spark

| Tool   | 1 month | 3 months | 6 months | 12 months |
|--------|---------|----------|----------|-----------|
| Pandas | 1.4s    | 6.0s     | 13.7s    | 29.7s     |
| Spark  | 6.2s    | 3.5s     | 3.4s     | 3.7s      |

Pandas wins on a single month — Spark's startup overhead isn't worth it at small scale.
The crossover happens at around 3 months. By 12 months Spark is 8× faster, and
critically its time barely moves as data grows — that's parallelism absorbing the load.
On even larger datasets pandas would eventually run out of memory entirely; Spark
processes data in partitions so it's not constrained by RAM.

### Shuffle partition tuning

All shuffle partition counts (4–128) produced nearly identical times (~3.2–3.4s).
This is expected: shuffle partitions matter most during large joins and groupBy
operations on raw data. Our output DataFrame is small after aggregation, so there's
little data to shuffle and the setting has minimal effect. On a larger intermediate
dataset or a multi-table join this would show a meaningful difference.

### Partition strategy

| Strategy      | Write time | Read time (filter hour=8) |
|---------------|------------|---------------------------|
| None          | 4.0s       | 0.1s                      |
| By hour       | 3.7s       | 0.1s                      |
| By month+hour | 6.7s       | 0.2s                      |

Read times are equal here because the summary DataFrame is already small — predicate
pushdown delivers the biggest gains when filtering large raw datasets, not small
aggregated ones. The `by_month_hour` write is slower because it creates more
directory structure. In a real pipeline the raw-data read would show much larger
differences between strategies.

### Repartition on write

Write time is flat across repartition counts (3.2–3.7s). The meaningful difference
is file count: `repartition(1)` produces one large file while `repartition(32)`
produces 32 small ones. Too many small files is the "small files problem" — a real
production concern because it causes excessive metadata overhead when reading.
A good rule of thumb is to target ~200MB per output file.

## Architecture

```
data/raw/*.parquet
       │
       ▼
   read_parquet()          SparkSession + schema print
       │
       ▼
     clean()               drop nulls, zero-distance, bad fares
       │
       ▼
  add_features()           duration, speed, pickup_hour, year_month
       │
       ▼
aggregate_by_zone_hour()   groupBy zone + hour + month
       │
       ▼
   validate()              row count, null check, hour coverage
       │
       ▼
output/partitioned.parquet partitionBy(year_month, pickup_hour)
```

## Getting the data

Download yellow taxi trip records from:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Download all 12 monthly `.parquet` files for a given year and place them in `data/raw/`
before running the pipeline. The folder is gitignored — data files are not committed.

## Running the pipeline

```bash
pip install -r requirements.txt
python src/pipeline.py
```

## Running the benchmarks

```bash
python benchmarks/run_benchmarks.py
```

Results are written to `benchmarks/results.md`. Expect 20–40 minutes on 12 months
of data depending on your machine.

## Tech stack

- Python 3.11
- PySpark 3.5.1
- PyArrow (Parquet I/O)
- pytest (unit tests)
- NYC TLC Yellow Taxi 2024 — 12 months, ~6GB, ~96M rows

## What I learned

- Spark's lazy evaluation model and when actions actually trigger execution
- Why shuffle partition count matters for groupBy and join performance — and when it doesn't
- Partitioned writes and predicate pushdown, and the conditions under which they help
- The small files problem and how repartition count affects output file size
- How to test Spark jobs with small in-memory DataFrames using `pytest`
- At what data size Spark's parallelism starts to outweigh its startup overhead
