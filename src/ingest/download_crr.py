#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Fetch the monthly CAISO “CRR_SEC_ML” archive and copy it to HDFS
  /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip
"""

import os, sys, subprocess, argparse
from datetime import datetime
from urllib.parse import urlencode
import requests
import zipfile
from io import BytesIO

# Hadoop CLI inside the Spark container
HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"

BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "CRR_SEC_ML",
    "version":      "1",
    "resultformat": "6",      # ZIP container
    "outformat":    "CSV",    # <- *must* be present and upper-case
}

def hdfs_put(local: str, hdfs_dest: str) -> None:
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_dest)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, hdfs_dest], check=True)

def main(ym: str) -> None:
    # ------------ build the OASIS URL -------------
    first = datetime.strptime(ym + "-01", "%Y-%m-%d")
    qs = COMMON | {
        "startdatetime": first.strftime("%Y%m%dT00:00-0000")
        # enddatetime NOT used for this report
    }
    url = BASE + "?" + urlencode(qs)
    print("Downloading", url)

    raw = requests.get(url, timeout=300).content

    # ------------ quick ZIP sanity check -----------
    if not raw.startswith(b"PK"):
        sys.exit("!! server returned non-ZIP payload – aborting")

    with zipfile.ZipFile(BytesIO(raw)) as zf:
        if not any(name.lower().endswith(".csv") for name in zf.namelist()):
            sys.exit("!! ZIP contains no CSV (likely an OASIS error stub) – aborting")

    # ------------ write to tmp, then HDFS ----------
    local = f"/tmp/CAISO_CRR_{ym.replace('-', '')}.zip"
    open(local, "wb").write(raw)

    hdfs_abs = f"{HDFS_URI}/data/raw/crr/{ym}/CAISO_CRR_{ym.replace('-', '')}.zip"
    hdfs_put(local, hdfs_abs)

    size = int(subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs_abs]).split()[0])
    print(f"✅  Uploaded → {hdfs_abs}  ({size/1e6:.2f} MB)")
    os.remove(local)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must look like 2024-02")
    main(args.year_month)