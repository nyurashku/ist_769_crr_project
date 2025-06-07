# ─── src/etl/parse_lmp_silver.py ───────────────────────────────────────────────
import argparse, zipfile, tempfile, pathlib, shutil
from pyspark.sql import SparkSession

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--market", choices=["DAM", "RTM", "RTPD"], default="DAM")
    p.add_argument("--year", required=True)
    p.add_argument("--month", help="YYYY-MM (optional, narrows the ingest to one month)")
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-lmp-silver")
             .getOrCreate())
    HDFS_ROOT   = "hdfs://hadoop-namenode:8020"
    # ── pick input pattern ───────────────────────────────────────────
    if args.month:
        # e.g. 2024-02  →  /data/raw/lmp/DAM/2024-02/*.zip
        hdfs_raw = f"{HDFS_ROOT}/data/raw/lmp/{args.market}/{args.month}/*.zip"
        out_sub  = f"{args.year}/{args.month}"
    else:        # whole year
        hdfs_raw = f"{HDFS_ROOT}/data/raw/lmp/{args.market}/{args.year}*/*.zip"
        out_sub  = f"{args.year}"

    hdfs_silver = f"{HDFS_ROOT}/user/sparkuser/silver/lmp/{args.market}/{out_sub}"

# 1️ – copy ZIPs from HDFS to a local temp dir, unzip, and load CSVs
    tmp = pathlib.Path(tempfile.mkdtemp())

    (spark.sparkContext
        .binaryFiles(hdfs_raw)                 # (path, bytes)
        .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name).write_bytes(kv[1])))

    # extract all zip files to tmp/extracted/
    extracted = tmp / "extracted"
    extracted.mkdir()
    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    # 2️ – now Spark can read the plain CSVs
    csv_pattern = f"file://{extracted}/*.csv" 

    df = (spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(csv_pattern))

    # quick schema massage, de-duplication, etc. goes here…

    # 3️ – write to HDFS in Parquet (or whatever you like)
    (df.repartition("OPR_DT")               # good partition key
       .write
       .mode("overwrite")
       .parquet(hdfs_silver))

    shutil.rmtree(tmp)                      # tidy temp dir
    spark.stop()

if __name__ == "__main__":
    main()