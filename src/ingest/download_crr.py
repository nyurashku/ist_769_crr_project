#!/usr/bin/env python3
"""
parse_crr_market_results.py  --year YYYY  [--month YYYY-MM]

Parses CAISO “Public Market Results” XML (auction clearing prices)
found in HDFS /data/raw/crr_market/… and writes columnar Parquet to

   /user/sparkuser/silver/crr_market_results/<year>/[<YYYY-MM>/]…
"""

import argparse, pathlib, tempfile, shutil, xml.etree.ElementTree as ET
from pyspark.sql import SparkSession, Row

NS = {"c": "http://crr.caiso.org/download/xml"}           # namespace helper

def xml_to_rows(path: pathlib.Path):
    "Yield one Python dict for every <Result> inside *path*."
    root = ET.parse(path).getroot()
    for r in root.findall("c:Result", NS):
        yield {
            "crr_id"      : int(r.findtext("c:CRRID",        default="0",     namespaces=NS)),
            "category"    : r.findtext("c:Category",         default="",      namespaces=NS),
            "mp"          : r.findtext("c:MarketParticipant",default="",      namespaces=NS),
            "source"      : r.findtext("c:Source",           default="",      namespaces=NS),
            "sink"        : r.findtext("c:Sink",             default="",      namespaces=NS),
            "start_date"  : r.findtext("c:StartDate",        default="",      namespaces=NS),
            "end_date"    : r.findtext("c:EndDate",          default="",      namespaces=NS),
            "hedge_type"  : r.findtext("c:HedgeType",        default="",      namespaces=NS),
            "crr_type"    : r.findtext("c:CRRType",          default="",      namespaces=NS),
            "buy_sell"    : r.findtext("c:Type",             default="",      namespaces=NS),
            "tou"         : r.findtext("c:TimeOfUse",        default="",      namespaces=NS),
            "mw"          : float(r.findtext("c:MW",             default="0", namespaces=NS)),
            "clearing_prc": float(r.findtext("c:ClearingPrice", default="0", namespaces=NS)),
        }

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year",  required=True)
    p.add_argument("--month")              # optional YYYY-MM
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("parse-crr-market-results")
             .getOrCreate())

    HDFS = "hdfs://hadoop-namenode:8020"

    if args.month:
        hdfs_in  = f"{HDFS}/data/raw/crr_market/{args.month}/*.xml"
        out_sub  = f"{args.year}/{args.month}"
    else:
        hdfs_in  = f"{HDFS}/data/raw/crr_market/{args.year}*/*.xml"
        out_sub  = args.year

    hdfs_out = f"{HDFS}/user/sparkuser/silver/crr_market_results/{out_sub}"

    # ── copy XMLs locally ────────────────────────────────────────────────────
    tmp = pathlib.Path(tempfile.mkdtemp())
    (spark.sparkContext
         .binaryFiles(hdfs_in)                                   # (hdfs_path, bytes)
         .foreach(lambda kv: (tmp / pathlib.Path(kv[0]).name)
                             .write_bytes(kv[1])))

    # ── parse every file → rows → DataFrame ─────────────────────────────────
    rows = []
    for f in tmp.glob("*.xml"):
        rows.extend(xml_to_rows(f))

    df = spark.createDataFrame(Row(**r) for r in rows)

    # ── write out ───────────────────────────────────────────────────────────
    (df.repartition("start_date")     # any date inside the month – good for pruning
       .write.mode("overwrite")
       .parquet(hdfs_out))

    shutil.rmtree(tmp)
    spark.stop()

if __name__ == "__main__":
    main()