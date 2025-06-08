#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Download a CAISO monthly CRR “SEC_ML” archive and upload it to HDFS

    /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip
"""
import argparse
import os
import subprocess
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests

# ── Hadoop CLI inside the Spark container ────────────────────────────────────
HADOOP   = "/opt/hadoop/bin/hadoop"                 # installed in earlier steps
HDFS_URI = "hdfs://hadoop-namenode:8020"            # canonical NameNode URI

def hdfs_put(local_path: str, hdfs_path: str) -> None:
    """Create parent dir (if needed) and upload *local_path* → *hdfs_path*."""
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local_path, hdfs_path],
                   check=True)

# ── CAISO OASIS query details ────────────────────────────────────────────────
BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "CRR_SEC_ML",
    "version":      "1",
    "resultformat": "6",       # ZIP
}

def month_window(ym: str) -> tuple[str, str]:
    """Return (‘startdatetime’, ‘enddatetime’) covering the whole month."""
    first = datetime.strptime(ym + "-01", "%Y-%m-%d")
    last  = (first.replace(day=28) + timedelta(days=4)).replace(day=1) \
            - timedelta(days=1)
    return (first.strftime("%Y%m%dT00:00-0000"),
            last .strftime("%Y%m%dT23:00-0000"))

# ── main ─────────────────────────────────────────────────────────────────────
def main(ym: str) -> None:
    start, end = month_window(ym)
    qs  = COMMON | {"startdatetime": start, "enddatetime": end}
    url = BASE + "?" + urlencode(qs)

    print("Downloading", url)
    raw = requests.get(url, timeout=300).content

    # quick sanity-check: a real ZIP always starts with “PK\x03\x04”
    if not raw.startswith(b"PK\x03\x04"):
        print("!! OASIS returned non-ZIP payload – aborted")
        return

    local = f"/tmp/CAISO_CRR_{ym.replace('-', '')}.zip"
    with open(local, "wb") as fh:
        fh.write(raw)

    # make final HDFS target
    hdfs_path = f"{HDFS_URI}/data/raw/crr/{ym}/CAISO_CRR_{ym.replace('-', '')}.zip"
    hdfs_put(local, hdfs_path)

    size = int(subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs_path])
               .split()[0])
    print(f"✅  Uploaded → {hdfs_path}  ({size/1e6:.1f} MB)")

    os.remove(local)

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()

    if len(args.year_month) != 7 or args.year_month[4] != "-":
        ap.error("year_month must be in YYYY-MM form, e.g. 2023-11")

    main(args.year_month)