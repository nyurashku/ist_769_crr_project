#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Fetch the monthly CAISO “CRR_SEC_ML” archive for *one* month and copy it to HDFS
  /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip
"""
import os, sys, subprocess, argparse
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests

# ── Hadoop CLI inside the Spark container ────────────────────────────────────
HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"

def hdfs_put(local: str, hdfs_dest: str) -> None:
    """Create parent dir (if needed) and upload *local* → *hdfs_dest*."""
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_dest)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, hdfs_dest], check=True)

# ── CAISO OASIS query parameters ─────────────────────────────────────────────
BASE   = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "CRR_SEC_ML",
    "version":      "1",
    "resultformat": "6",      # ZIP output
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
    url = BASE + "?" + urlencode(COMMON | {"startdatetime": start,
                                           "enddatetime":   end})
    print("Downloading", url)
    raw = requests.get(url, timeout=300).content

    local = f"/tmp/CAISO_CRR_{ym.replace('-', '')}.zip"
    open(local, "wb").write(raw)

    hdfs = f"/data/raw/crr/{ym}/CAISO_CRR_{ym.replace('-', '')}.zip"
    hdfs_put(local, f"{HDFS_URI}{hdfs}")
    size = int(subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs]).split()[0])
    print(f"✅  Uploaded → {hdfs}  ({size/1e6:.1f} MB)")
    os.remove(local)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must look like 2024-02")
    main(args.year_month)