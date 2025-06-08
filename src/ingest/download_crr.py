#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Fetch the monthly CAISO *auction* CRR Clearing-Prices archive and copy it to
  /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip   (in HDFS)
"""
import os, sys, argparse, subprocess, tempfile, zipfile
from datetime import datetime, timedelta
import requests
from urllib.parse import urlencode

HADOOP = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"
BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname": "CRR_CLEARING",
    "version":   "1",
    "resultformat": "6",      # ZIP
}

def month_bounds(ym: str) -> tuple[str, str]:
    """Return (startdatetime, enddatetime) constrained to ≤ 31 days."""
    first = datetime.strptime(ym + "-01", "%Y-%m-%d")

    # first second of next month …
    next_first = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    # … then back one second → 23:59 of the last day this month
    last = next_first - timedelta(seconds=1)

    return (first.strftime("%Y%m%dT00:00-0000"),   # e.g. 20250501T00:00-0000
            last .strftime("%Y%m%dT%H:%M-0000"))  # e.g. 20250531T23:59-0000

def hdfs_put(local: str, hdfs_path: str) -> None:
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, hdfs_path], check=True)

def main(ym: str) -> None:
    start, end = month_bounds(ym)
    url = BASE + "?" + urlencode(COMMON | {"startdatetime": start,
                                           "enddatetime":   end})
    print("Downloading", url)
    raw = requests.get(url, timeout=300).content

    # quick inspection: does the ZIP hold at least one *.csv?
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
        tmp.write(raw)
    with zipfile.ZipFile(tmp.name) as zf:
        csvs = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not csvs:
            print("!! ZIP contains no CSVs – aborting")
            os.remove(tmp.name)
            sys.exit(1)

    hdfs_path = f"{HDFS_URI}/data/raw/crr/{ym}/CAISO_CRR_{ym.replace('-','')}.zip"
    hdfs_put(tmp.name, hdfs_path)
    size = int(subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs_path]).split()[0])
    print(f"✅  Uploaded → {hdfs_path}  ({size/1e6:.1f} MB)")
    os.remove(tmp.name)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must be like 2025-05")
    main(args.year_month)