#!/usr/bin/env python3
"""
parse_crr_silver.py  --year YYYY [--month YYYY-MM]

Un-zip CAISO CRR “SEC_ML” archives in HDFS   /data/raw/crr/…
and write Parquet to                         /user/sparkuser/silver/crr/…
"""
import argparse, tempfile, pathlib, zipfile, shutil
from pyspark.sql import SparkSession

def main() -> None:
    # ── 0 – CLI ────────────────────────────────────────────────────────────────
    p = argparse.ArgumentParser()
    p.add_argument("--year",  required=True)
    p.add_argument("--month")                 # optional YYYY-MM
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-crr-silver")
             .getOrCreate())

    HDFS = "hdfs://hadoop-namenode:8020"

    # ── 1 – locate input ZIP(s) & output folder ───────────────────────────────
    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.month}/*.zip"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.year}*/*.zip"
        out_sub  = args.year

    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr/{out_sub}"

    # ── 2 – copy ZIP(s) locally & extract ─────────────────────────────────────
    tmp = pathlib.Path(tempfile.mkdtemp())

    # copy bytes out of HDFS
    (spark.sparkContext
         .binaryFiles(hdfs_in)                     # (hdfs_path, bytes)
         .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name).write_bytes(kv[1])))

    extracted = tmp / "extracted"
    extracted.mkdir()

    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    # ── 3 – read every CSV in any sub-folder (case-insensitive) ───────────────
    csv_pattern = (
        f"file://{extracted}/**/*.csv,"
        f"file://{extracted}/**/*.CSV"
    )

    df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(csv_pattern)
                  .selectExpr(
                      "PATHID",
                      "SOURCE_PNODE_ID",
                      "SINK_PNODE_ID",
                      "OPR_DT",
                      "OPR_HR",
                      "double(CRR_MW)    as CRR_MW",
                      "double(CLEAR_PRC) as CLEAR_PRC"))

    # ── 4 – write to silver (partition by OPR_DT for pruning) ─────────────────
    (df.repartition("OPR_DT")
       .write.mode("overwrite")
       .parquet(hdfs_out))

    shutil.rmtree(tmp)
    spark.stop()

if __name__ == "__main__":
    main()