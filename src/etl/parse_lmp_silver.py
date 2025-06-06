#!/usr/bin/env python3
"""
parse_lmp_silver.py   --market DAM --year 2024

Reads the raw, daily ZIPs from HDFS and writes a
column-typed, partitioned Parquet table:

    hdfs:///data/silver/lmp_dam/
        OPR_DT=2024-01-01/NODE=TH_SP15_GEN-APND/part-*.snappy.parquet
"""
import argparse, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, unix_timestamp
)

COLS = [          # desired schema in Silver
    "INTERVALSTARTTIME_GMT","INTERVALENDTIME_GMT",
    "OPR_DT","OPR_HR","OPR_INTERVAL",
    "NODE","MARKET_RUN_ID","LMP_TYPE",
    "MW"
]

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--market", default="DAM", choices=["DAM","RTM","RTPD"])
    p.add_argument("--year"  , required=True,  help="YYYY or YYYY-MM")
    return p.parse_args()

def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("parse-lmp-silver")
        # .config("spark.sql.shuffle.partitions","8")   # tweak if wanted
        .getOrCreate()
    )

    # --- 1)  locate every ZIP file we want ------------------------------
    ym_glob = f"{args.year}*" if len(args.year)==4 else args.year
    raw_path = f"hdfs:///data/raw/lmp/{args.market}/{ym_glob}/*.zip"

    # Spark can read inside ZIPs with the “zip:” protocol
    df = (
        spark.read
        .option("header","true")
        .csv(f"zip://{raw_path}")          # 🔑
        .select(COLS)                      # drop unused cols
        .withColumn("MW", col("MW").cast("double"))
        # interval start → UTC timestamp
        .withColumn(
            "ts_utc",
            to_timestamp("INTERVALSTARTTIME_GMT", "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        .dropDuplicates(["ts_utc","NODE","LMP_TYPE","MARKET_RUN_ID"])
    )

    # --- 2) write partitioned Parquet -----------------------------------
    (
        df
        .repartition("OPR_DT","NODE")      # avoids tiny files
        .write
        .mode("overwrite")
        .partitionBy("OPR_DT","NODE")
        .parquet(f"hdfs:///data/silver/lmp_{args.market.lower()}")
    )

    print(f"✓ Silver LMP ({args.market}) written for {args.year}")

if __name__ == "__main__":
    main()