#!/usr/bin/env python3
"""
parse_lmp_silver.py  --market DAM|RTM|RTPD  --year YYYY [--month YYYY-MM]

Unzip CAISO LMP files that sit in the raw HDFS zone, cast columns, de-dup,
and write Parquet to the silver zone:

    /user/sparkuser/silver/lmp/<market>/<year>/…
    /user/sparkuser/silver/lmp/<market>/<year>/<YYYY-MM>/…
"""
import argparse, zipfile, tempfile, pathlib, shutil
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------- #
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--market", choices=["DAM", "RTM", "RTPD"], default="DAM")
    p.add_argument("--year",   required=True, help="YYYY")
    p.add_argument("--month",  help="optional YYYY-MM (ingest just one month)")
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-lmp-silver")
             .getOrCreate())

    HDFS_ROOT = "hdfs://hadoop-namenode:8020"

    # ── pick the raw input pattern and silver output path ───────────────────
    if args.month:
        hdfs_raw   = f"{HDFS_ROOT}/data/raw/lmp/{args.market}/{args.month}/*.zip"
        out_subdir = f"{args.year}/{args.month}"
    else:  # whole year
        hdfs_raw   = f"{HDFS_ROOT}/data/raw/lmp/{args.market}/{args.year}*/*.zip"
        out_subdir = f"{args.year}"

    hdfs_silver = (
        f"{HDFS_ROOT}/user/sparkuser/silver/lmp/{args.market}/{out_subdir}"
    )

    # ── 1  copy ZIPs locally & extract ──────────────────────────────────────
    tmp = pathlib.Path(tempfile.mkdtemp())

    (spark.sparkContext
        .binaryFiles(hdfs_raw)            # RDD[(hdfs_path, bytes)]
        .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name).write_bytes(kv[1])))

    extracted = tmp / "extracted"
    extracted.mkdir()
    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    # ── 2  read CSVs with Spark ─────────────────────────────────────────────
    csv_pattern = f"file://{extracted}/*.csv"

    df = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_pattern))

    # (do your schema tweaks / de-dupes here if needed)

    # ── 3  write Parquet to silver ──────────────────────────────────────────
    (df.repartition("OPR_DT")            # good partition column
       .write
       .mode("overwrite")
       .parquet(hdfs_silver))

    shutil.rmtree(tmp)
    spark.stop()

# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()