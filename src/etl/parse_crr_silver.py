#!/usr/bin/env python3
"""
parse_crr_silver.py  --year YYYY  [--month YYYY-MM]

Un-zip CAISO CRR_SEC_ML archives already in HDFS `/data/raw/crr/…`
and write cleaned Parquet to `/user/sparkuser/silver/crr/…`
"""
import argparse, tempfile, pathlib, zipfile, shutil
from pyspark.sql import SparkSession

def main() -> None:
    # ── CLI args ─────────────────────────────────────────────────────────────
    ap = argparse.ArgumentParser()
    ap.add_argument("--year",  required=True)
    ap.add_argument("--month")                       # optional YYYY-MM
    args = ap.parse_args()

    spark = (SparkSession.builder
                        .appName("parse-crr-silver")
                        .getOrCreate())

    HDFS = "hdfs://hadoop-namenode:8020"

    # ── locate input & output paths ──────────────────────────────────────────
    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.month}/*.zip"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.year}*/*.zip"
        out_sub  = args.year

    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr/{out_sub}"

    # ── copy ZIP(s) to a local temp dir and extract ─────────────────────────
    tmp = pathlib.Path(tempfile.mkdtemp())

    (spark.sparkContext
        .binaryFiles(hdfs_in)                          # (hdfs_path, bytes)
        .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name).write_bytes(kv[1])))

    extracted = tmp / "extracted"
    extracted.mkdir()

    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    # ── load **all** CSVs regardless of sub-folder or case ──────────────────
    csv_pattern = f"file://{extracted}/**/*.csv,file://{extracted}/**/*.CSV"
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

    # ── write to Parquet, partitioned by date for pruning ───────────────────
    (df.repartition("OPR_DT")
       .write.mode("overwrite")
       .parquet(hdfs_out))

    shutil.rmtree(tmp)
    spark.stop()

if __name__ == "__main__":
    main()