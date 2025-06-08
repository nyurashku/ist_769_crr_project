#!/usr/bin/env python3
"""
parse_crr_silver.py  --year YYYY [--month YYYY-MM]

Un-zip CAISO CRR Clearing-Prices CSVs in HDFS /data/raw/crr and write Parquet to
  /user/sparkuser/silver/crr/…
"""
import argparse, tempfile, pathlib, zipfile, shutil
from pyspark.sql import SparkSession

HDFS = "hdfs://hadoop-namenode:8020"

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year",  required=True)
    p.add_argument("--month")          # optional YYYY-MM
    args = p.parse_args()

    spark = SparkSession.builder.appName("parse-crr-silver").getOrCreate()

    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.month}/*.zip"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.year}*/*.zip"
        out_sub  = args.year
    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr/{out_sub}"

    tmp_dir = pathlib.Path(tempfile.mkdtemp())
    # 1) copy files locally
    (spark.sparkContext
         .binaryFiles(hdfs_in)
         .foreach(lambda kv: (tmp_dir / pathlib.Path(kv[0]).name)
                            .write_bytes(kv[1])))
    # 2) unzip
    extracted = tmp_dir / "extracted"; extracted.mkdir()
    for zf_path in tmp_dir.glob("*.zip"):
        with zipfile.ZipFile(zf_path) as zf:
            zf.extractall(extracted)

    # 3) read every CSV we just exploded
    csv_glob = f"file://{extracted}/**/*.csv"
    df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(csv_glob)
                  .selectExpr(
                      "PATHID",
                      "SOURCE_PNODE_ID",
                      "SINK_PNODE_ID",
                      "OPR_DT",
                      "OPR_HR",
                      "double(CRR_MW)    as CRR_MW",
                      "double(PRICE)     as CLEAR_PRC"))   # field name in clearing files

    (df.repartition("OPR_DT")
       .write.mode("overwrite")
       .parquet(hdfs_out))

    shutil.rmtree(tmp_dir)
    spark.stop()

if __name__ == "__main__":
    main()