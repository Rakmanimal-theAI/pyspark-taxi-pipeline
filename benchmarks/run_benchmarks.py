"""
benchmarks/run_benchmarks.py

Runs four benchmark suites and writes results to benchmarks/results.md:
  1. Pandas vs Spark         — same aggregation, increasing data size
  2. Shuffle partition tuning — effect of spark.sql.shuffle.partitions
  3. Partition strategy      — read speed with different write strategies
  4. Repartition on write    — file count vs write time tradeoff

Usage:
    python benchmarks/run_benchmarks.py
"""

import os
import sys
import time
import shutil
import glob

import pandas as pd
from pyspark.sql import functions as F

# ── path setup ────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from ingest import get_spark, read_parquet
from transform import clean, add_features, aggregate_by_zone_hour

RAW_DIR    = os.path.join(ROOT, "data", "raw")
OUTPUT_DIR = os.path.join(ROOT, "output", "benchmarks")
RESULTS_MD = os.path.join(ROOT, "benchmarks", "results.md")


# ── helpers ───────────────────────────────────────────────────────────────────

def get_fresh_spark(shuffle_partitions: int = 8):
    """
    Stop any existing SparkSession and start a fresh one.
    """
    from pyspark.sql import SparkSession
    try:
        SparkSession.builder.getOrCreate().stop()
    except Exception:
        pass

    import inspect
    if "shuffle_partitions" in inspect.signature(get_spark).parameters:
        return get_spark(shuffle_partitions=shuffle_partitions)

    # fallback: build directly with the required config
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Benchmark")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def run_spark_pipeline(spark, path: str):
    """
    Full pipeline using your src/ functions:
      read_parquet → clean → add_features → aggregate_by_zone_hour
    Returns (row_count, summary_df). Triggers execution via .count()
    so the timing captures the full job, not just the lazy plan.
    """
    raw      = read_parquet(spark, path)
    cleaned  = clean(raw)
    featured = add_features(cleaned)
    summary  = aggregate_by_zone_hour(featured)
    return summary.count(), summary


def pandas_aggregation(files: list) -> pd.DataFrame:
    """
    Equivalent pipeline in pandas — mirrors your Spark functions exactly:
      read → clean() → add_features() → aggregate_by_zone_hour()

    Assumes aggregate_by_zone_hour() groups by PULocationID, pickup_hour,
    year_month and aggregates trip_count, avg_fare, avg_distance, avg_duration.
    Adjust the groupby/agg below if your Spark version differs.
    """
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True)

    # mirrors clean()
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                            "trip_distance", "fare_amount", "passenger_count"])
    df = df[
        (df["trip_distance"]    > 0) &
        (df["fare_amount"]      > 0) &
        (df["passenger_count"]  > 0)
    ]

    # mirrors add_features()
    df["trip_duration_mins"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60
    df["avg_speed_mph"] = df["trip_distance"] / (df["trip_duration_mins"] / 60)
    df["pickup_hour"]   = df["tpep_pickup_datetime"].dt.hour
    df["year_month"]    = df["tpep_pickup_datetime"].dt.to_period("M").astype(str)

    df = df[
        df["trip_duration_mins"].between(1, 180) &
        df["avg_speed_mph"].between(0.5, 80)
    ]

    # mirrors aggregate_by_zone_hour()
    return (
        df.groupby(["PULocationID", "pickup_hour", "year_month"])
        .agg(
            trip_count   = ("fare_amount",         "count"),
            avg_fare     = ("fare_amount",          "mean"),
            avg_distance = ("trip_distance",        "mean"),
            avg_duration = ("trip_duration_mins",   "mean"),
        )
        .reset_index()
    )


def timed(fn):
    """Run fn(), return (result, elapsed_seconds). Returns -1 on MemoryError."""
    try:
        start = time.time()
        result = fn()
        return result, round(time.time() - start, 1)
    except MemoryError:
        return None, -1


def fmt_time(seconds: float) -> str:
    if seconds < 0:
        return "OOM"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{int(seconds // 60)}m {int(seconds % 60)}s"


def get_files(n: int) -> list:
    """Return the first n parquet files found in data/raw/."""
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.parquet")))
    if len(files) < n:
        raise FileNotFoundError(
            f"Asked for {n} files but only {len(files)} found in {RAW_DIR}"
        )
    return files[:n]


def to_glob(files: list) -> str:
    """Build a Spark-compatible path from a list of files."""
    import os
    # Just return the directory path - Spark will read all parquet files in it
    return os.path.dirname(files[0]) + "/"


def clean_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


# ── benchmark 1 : pandas vs spark ────────────────────────────────────────────

def bench_pandas_vs_spark() -> list:
    print("\n=== Benchmark 1: Pandas vs Spark ===")
    rows = []

    for n in [1, 3, 6, 12]:
        files = get_files(n)

        # pandas — pure python, no Spark
        print(f"  pandas {n:>2} month(s)...", end=" ", flush=True)
        _, t = timed(lambda: pandas_aggregation(files))
        rows.append({"tool": "Pandas", "months": n, "time": t,
                     "notes": "OOM" if t < 0 else ""})
        print(fmt_time(t))

        # spark — uses your src/ pipeline functions
        print(f"  spark  {n:>2} month(s)...", end=" ", flush=True)
        spark = get_fresh_spark(shuffle_partitions=8)
        _, t  = timed(lambda: run_spark_pipeline(spark, to_glob(files)))
        spark.stop()
        rows.append({"tool": "Spark", "months": n, "time": t, "notes": ""})
        print(fmt_time(t))

    return rows


# ── benchmark 2 : shuffle partition tuning ───────────────────────────────────

def bench_shuffle_partitions() -> list:
    print("\n=== Benchmark 2: Shuffle partition tuning (12 months) ===")
    path = to_glob(get_files(12))
    rows = []

    for n in [4, 8, 16, 32, 64, 128]:
        print(f"  shuffle_partitions={n:<3}...", end=" ", flush=True)
        spark = get_fresh_spark(shuffle_partitions=n)
        _, t  = timed(lambda: run_spark_pipeline(spark, path))
        spark.stop()
        rows.append({"shuffle_partitions": n, "time": t})
        print(fmt_time(t))

    return rows


# ── benchmark 3 : partition strategy read speed ──────────────────────────────

def bench_partition_strategy() -> list:
    print("\n=== Benchmark 3: Partition strategy — read speed ===")
    spark = get_fresh_spark(shuffle_partitions=32)

    print("  building summary DataFrame...", flush=True)
    _, summary = run_spark_pipeline(spark, to_glob(get_files(12)))

    strategies = [
        (
            "none",
            lambda df, p: df.write.mode("overwrite").parquet(p),
        ),
        (
            "by_hour",
            lambda df, p: df.write.mode("overwrite")
                            .partitionBy("pickup_hour").parquet(p),
        ),
        (
            "by_month_hour",
            lambda df, p: df.write.mode("overwrite")
                            .partitionBy("year_month", "pickup_hour").parquet(p),
        ),
    ]

    rows = []
    for name, write_fn in strategies:
        out = os.path.join(OUTPUT_DIR, f"strategy_{name}")
        clean_dir(out)

        print(f"  {name:<15} write...", end=" ", flush=True)
        _, write_t = timed(lambda: write_fn(summary, out))
        print(fmt_time(write_t), end=" | read...", flush=True)

        _, read_t = timed(lambda: (
            spark.read.parquet(out)
            .filter(F.col("pickup_hour") == 8)
            .count()
        ))
        print(fmt_time(read_t))

        rows.append({
            "strategy":   name,
            "write_time": write_t,
            "read_time":  read_t,
            "notes":      "filter pickup_hour=8",
        })

    spark.stop()
    return rows


# ── benchmark 4 : repartition count on write ─────────────────────────────────

def bench_repartition() -> list:
    print("\n=== Benchmark 4: Repartition count on write (12 months) ===")
    spark = get_fresh_spark(shuffle_partitions=32)

    print("  building summary DataFrame...", flush=True)
    _, summary = run_spark_pipeline(spark, to_glob(get_files(12)))

    rows = []
    for n in [1, 4, 8, 16, 32]:
        out = os.path.join(OUTPUT_DIR, f"repartition_{n}")
        clean_dir(out)

        print(f"  repartition({n:<2})...", end=" ", flush=True)
        _, t = timed(lambda: (
            summary.repartition(n)
            .write.mode("overwrite")
            .parquet(out)
        ))
        file_count = len(glob.glob(os.path.join(out, "*.parquet")))
        rows.append({"repartition": n, "time": t, "output_files": file_count})
        print(f"{fmt_time(t)} → {file_count} file(s)")

    spark.stop()
    return rows


# ── results writer ────────────────────────────────────────────────────────────

def write_results(b1, b2, b3, b4):
    lines = [
        "# Benchmark results\n",
        "_Generated by `benchmarks/run_benchmarks.py`_\n",

        "\n## 1. Pandas vs Spark\n",
        "| Tool   | Months | Time  | Notes |",
        "|--------|--------|-------|-------|",
    ]
    for r in b1:
        lines.append(
            f"| {r['tool']:<6} | {r['months']:<6} | "
            f"{fmt_time(r['time']):<5} | {r['notes']} |"
        )

    lines += [
        "\n## 2. Shuffle partition tuning (12 months)\n",
        "| shuffle_partitions | Time  |",
        "|--------------------|-------|",
    ]
    for r in b2:
        lines.append(
            f"| {r['shuffle_partitions']:<18} | {fmt_time(r['time']):<5} |"
        )

    lines += [
        "\n## 3. Partition strategy — read speed (filter pickup_hour=8)\n",
        "| Strategy      | Write time | Read time | Notes            |",
        "|---------------|------------|-----------|------------------|",
    ]
    for r in b3:
        lines.append(
            f"| {r['strategy']:<13} | {fmt_time(r['write_time']):<10} | "
            f"{fmt_time(r['read_time']):<9} | {r['notes']} |"
        )

    lines += [
        "\n## 4. Repartition count on write (12 months)\n",
        "| repartition | Write time | Output files |",
        "|-------------|------------|--------------|",
    ]
    for r in b4:
        lines.append(
            f"| {r['repartition']:<11} | {fmt_time(r['time']):<10} | "
            f"{r['output_files']} |"
        )

    os.makedirs(os.path.dirname(RESULTS_MD), exist_ok=True)
    with open(RESULTS_MD, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"\nResults written to {RESULTS_MD}")


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting benchmarks — this will take several minutes on 12 months of data.")
    b1 = bench_pandas_vs_spark()
    b2 = bench_shuffle_partitions()
    b3 = bench_partition_strategy()
    b4 = bench_repartition()
    write_results(b1, b2, b3, b4)
    print("\nAll done.")
