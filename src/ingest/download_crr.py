#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Fetch CAISO CRR monthly auction ZIP and push to HDFS raw zone:
    /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip
"""

import os, sys, subprocess, argparse, requests
from urllib.parse import urlencode

# ---------- config -----------------------------------------------------------
HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"

BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "CRR_SEC_ML",   # **monthly** CRR auction results
    "version":      "1",
    "resultformat": "6",
    "outformat":    "zip",
}

# ---------------------------------------------------------------------------
def hdfs_put(local, hdfs):
    subprocess.run([HADOOP, "fs", "-fs", HDFS_URI,
                    "-mkdir", "-p", os.path.dirname(hdfs)], check=True)
    subprocess.run([HADOOP, "fs", "-fs", HDFS_URI,
                    "-put", "-f", local, hdfs], check=True)

# ---------------------------------------------------------------------------
def main(year_month):
    ym_nodash = year_month.replace("-", "")
    qs = COMMON | {
        "startdatetime": f"{ym_nodash}01T00:00-0000",
        "enddatetime":   f"{ym_nodash}02T00:00-0000",
    }
    url = BASE + "?" + urlencode(qs)
    print("Downloading", url)

    r = requests.get(url, timeout=600)
    r.raise_for_status()

    local = f"/tmp/CAISO_CRR_{ym_nodash}.zip"
    open(local, "wb").write(r.content)

    hdfs = f"/data/raw/crr/{year_month}/CAISO_CRR_{ym_nodash}.zip"
    hdfs_put(local, hdfs)
    print("✅  Uploaded →", hdfs)

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must be in YYYY-MM form (e.g. 2024-02)")
    main(args.year_month)