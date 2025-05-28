#!/usr/bin/env python
"""
download_lmp.py  YYYY-MM  →  stores raw OASIS weekly ZIPs in HDFS

Example:
    python download_lmp.py 2024-01
"""
import os, sys, subprocess, requests, pathlib
from datetime import datetime, timedelta   # 🔄 NEW

YEAR_MONTH = sys.argv[1]         # e.g. 2024-01

# 🔄 CHANGED: OASIS template now has TWO {} slots (start-date, end-date)
OASIS = (
    "http://oasis.caiso.com/oasisapi/SingleZip?"
    "queryname=PRC_LMP&version=1&market_run_id=RTM"
    "&startdatetime={}T00:00-0000&enddatetime={}T23:00-0000"
    "&resultformat=6"
)

def hdfs_put(local_path, hdfs_path):
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)], check=True)
    subprocess.run(["hadoop", "fs", "-put", "-f", local_path, hdfs_path], check=True)

# 🔄 NEW: iterate through the month one week (7-day block) at a time
def weeks_in_month(ym):
    start = datetime.strptime(ym + "-01", "%Y-%m-%d")
    while start.month == int(ym.split("-")[1]):
        end = start + timedelta(days=6)
        # clamp to end-of-month
        if end.month != start.month:
            end = datetime(end.year, end.month, 1) - timedelta(days=1)
        yield start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        start = end + timedelta(days=1)

def main():
    temp = pathlib.Path("/tmp/lmp_{}.zip".format(YEAR_MONTH))
    if temp.exists():
        temp.unlink()           # start fresh if the file is there

    # download & append each weekly ZIP
    for start_date, end_date in weeks_in_month(YEAR_MONTH):
        url = OASIS.format(start_date, end_date)
        print("Downloading {}".format(url))
        r = requests.get(url, timeout=900)
        r.raise_for_status()
        temp.write_bytes(r.content)   # append chunk to temp file

    hdfs_target = "/data/raw/lmp/{}/lmp_{}.zip".format(YEAR_MONTH, YEAR_MONTH)
    hdfs_put(str(temp), hdfs_target)
    print("Uploaded to HDFS: {}".format(hdfs_target))

    # size sanity-check
    size = subprocess.check_output(["hadoop", "fs", "-du", "-s", hdfs_target]).split()[0]
    if int(size) < 10000:
        print("WARNING: ZIP is only {} bytes; CAISO likely returned no data".format(size))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python download_lmp.py YYYY-MM")
    main()