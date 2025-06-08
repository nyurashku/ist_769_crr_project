#!/usr/bin/env python3
"""
parse_crr_silver.py  --year YYYY [--month YYYY-MM]

Unzip CAISO CRR “SEC_ML” CSV archives in HDFS /data/raw/crr and write
Parquet to the silver zone:

    /user/sparkuser/silver/crr/<year>/…
    /user/sparkuser/silver/crr/<year>/<YYYY-MM>/…
"""
import argparse, tempfile, pathlib, zipfile, shutil
from pyspark.sql import SparkSession

def main() -> None:
    # ── CLI ------------------------------------------------------------------
    p = argparse.ArgumentParser()
    p.add_argument("--year",  required=True)
    p.add_argument("--month")                 # optional YYYY-MM
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-crr-silver")
             .getOrCreate())

    HDFS = "hdfs://hadoop-namenode:8020"

    # ── input glob & output path --------------------------------------------
    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.month}/*.zip"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.year}*/*.zip"
        out_sub  = args.year

    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr/{out_sub}"

    # ── 1 – copy & unzip in a temp dir --------------------------------------
    tmp = pathlib.Path(tempfile.mkdtemp())

    (spark.sparkContext
        .binaryFiles(hdfs_in)                               # (path, bytes)
        .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name)
                 .write_bytes(kv[1])))

    extracted = tmp / "extracted"
    extracted.mkdir()
    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    # ── 2 – load the CSV -----------------------------------------------------
    csv_pattern = f"file://{extracted}/*.csv"

    df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(csv_pattern)
                  .selectExpr(
                      "PATHID",
                      "SINK_PNODE_ID",
                      "SOURCE_PNODE_ID",
                      "OPR_DT",
                      "OPR_HR",
                      "double(CRR_MW)    as CRR_MW",
                      "double(CLEAR_PRC) as CLEAR_PRC"))

    # ── 3 – write to silver --------------------------------------------------
    (df.repartition("OPR_DT")
       .write.mode("overwrite")
       .parquet(hdfs_out))

    # ── tidy -----------------------------------------------------------------
    shutil.rmtree(tmp)
    spark.stop()

if __name__ == "__main__":
    main()