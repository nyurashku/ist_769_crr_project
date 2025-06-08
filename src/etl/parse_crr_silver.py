cat >src/etl/parse_crr_silver.py <<'PY'
#!/usr/bin/env python3
"""
parse_crr_silver.py  --year YYYY [--month YYYY-MM]

Unzip CAISO CRR JSON(s) in HDFS /data/raw/crr and write Parquet to
/user/sparkuser/silver/crr/…
"""
import argparse, tempfile, pathlib, zipfile, shutil
from pyspark.sql import SparkSession, functions as F

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year",  required=True)
    p.add_argument("--month")
    args = p.parse_args()

    spark = SparkSession.builder.appName("parse-crr-silver").getOrCreate()
    HDFS = "hdfs://hadoop-namenode:8020"

    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.month}/*.zip"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr/{args.year}*/*.zip"
        out_sub  = args.year

    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr/{out_sub}"

    tmp = pathlib.Path(tempfile.mkdtemp())
    (spark.sparkContext
        .binaryFiles(hdfs_in)
        .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name).write_bytes(kv[1])))

    extracted = tmp / "extracted"; extracted.mkdir()
    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(extracted)

    df = (spark.read.json(f"file://{extracted}/*.json")
                  .selectExpr(
                      "PATHID",
                      "SINK_PNODE_ID",
                      "SOURCE_PNODE_ID",
                      "OPR_DT",
                      "OPR_HR",
                      "double(CRR_MW)   as CRR_MW",
                      "double(CLEAR_PRC) as CLEAR_PRC"))

    (df.repartition("OPR_DT")
       .write.mode("overwrite")
       .parquet(hdfs_out))

    shutil.rmtree(tmp)
    spark.stop()

if __name__ == "__main__":
    main()
PY
chmod +x src/etl/parse_crr_silver.py