#!/usr/bin/env python3
"""
parse_crr_clearing_silver.py  --year YYYY  [--month YYYY-MM]

Read the *manual* CRR-Clearing CSVs loaded under

    /data/raw/crr_clearing/<YYYY-MM>/<YYYY-MM>.csv            (HDFS)

and write partitioned Parquet to

    /user/sparkuser/silver/crr_clearing/<YYYY>/<YYYY-MM>/…
"""
import argparse, pathlib, shutil, tempfile
from pyspark.sql import SparkSession, functions as F

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year",  required=True, help="e.g. 2025")
    p.add_argument("--month",                help="optional YYYY-MM")
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-crr-clearing-silver")
             .getOrCreate())

    HDFS = "hdfs://hadoop-namenode:8020"

    # ── locate input(s) ──────────────────────────────────────────────────────
    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr_clearing/{args.month}/{args.month}.csv"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr_clearing/{args.year}-*/{args.year}-*.csv"
        out_sub  = args.year

    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr_clearing/{out_sub}"

    # ── read & clean ─────────────────────────────────────────────────────────
    df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(hdfs_in)
                  .withColumn("START_DATE", F.to_timestamp("START_DATE"))
                  .withColumn("END_DATE",   F.to_timestamp("END_DATE"))
                  .withColumn("LOAD_TS",    F.current_timestamp()))

    # ── write partitioned Parquet (partition on MONTH) ──────────────────────
    (df.repartition("TIME_OF_USE")           # cheap-ish shuffle key
       .write.mode("overwrite")
       .parquet(hdfs_out))

    spark.stop()

if __name__ == "__main__":
    main()