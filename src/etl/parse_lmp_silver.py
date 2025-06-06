# ─── src/etl/parse_lmp_silver.py ───────────────────────────────────────────────
import argparse, zipfile, tempfile, pathlib, shutil
from pyspark.sql import SparkSession

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--market", choices=["DAM", "RTM", "RTPD"], default="DAM")
    p.add_argument("--year", required=True)
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-lmp-silver")
             .getOrCreate())
    HDFS_ROOT = "hdfs://hadoop-namenode:8020"
    hdfs_raw    = f"{HDFS_ROOT}/data/raw/lmp/{args.market}/{args.year}*/*.zip"
    hdfs_silver = f"{HDFS_ROOT}/data/silver/lmp/{args.market}/{args.year}"

    # 1️ – copy ZIPs from HDFS to a local temp dir, unzip, and load CSVs
    tmp = pathlib.Path(tempfile.mkdtemp())
    (
        spark.sparkContext
             .binaryFiles(hdfs_raw)      # <path, PortableDataStream>
             .map(lambda kv: (kv[0].split("/")[-1], kv[1].toArray()))
             .foreach(lambda tup: (tmp / tup[0]).write_bytes(tup[1]))
    )

    # extract all zip files to tmp/extracted/
    extracted = tmp / "extracted"
    extracted.mkdir()
    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    # 2️ – now Spark can read the plain CSVs
    df = (spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(str(extracted / "*.csv")))

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