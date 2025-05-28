#!/usr/bin/env python
"""
download_lmp.py  YYYY-MM  →  stores raw OASIS weekly ZIPs in HDFS

Example:
    python download_lmp.py 2024-01
"""
import os, sys, subprocess, requests, pathlib, time, random   # ← added time, random
from datetime import datetime, timedelta

YEAR_MONTH = sys.argv[1]            # e.g. 2024-01

# OASIS URL now expects *start* and *end* dates
OASIS = (
    "http://oasis.caiso.com/oasisapi/SingleZip?"
    "queryname=PRC_LMP&version=1&market_run_id=RTM"
    "&startdatetime={}T00:00-0000&enddatetime={}T23:00-0000"
    "&resultformat=6"
)

MAX_RETRIES = 3
SLEEP_SEC   = 3      # polite gap between calls

# ----------------------------------------------------------------------
def hdfs_put(local_path, hdfs_path):
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)], check=True)
    subprocess.run(["hadoop", "fs", "-put", "-f", local_path, hdfs_path], check=True)

# iterate through month in 7-day blocks
def weeks_in_month(ym):
    start = datetime.strptime(ym + "-01", "%Y-%m-%d")
    while start.month == int(ym.split("-")[1]):
        end = start + timedelta(days=6)
        if end.month != start.month:                      # clamp to month-end
            end = datetime(end.year, end.month, 1) - timedelta(days=1)
        yield start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        start = end + timedelta(days=1)

# polite downloader with 429 retry
def fetch(url):
    for attempt in range(1, MAX_RETRIES + 1):
        r = requests.get(url, timeout=900)
        if r.status_code == 429:
            wait = SLEEP_SEC * attempt + random.uniform(0, 1)
            print("⚠ 429 Too Many Requests – sleeping {:.1f}s (attempt {}/{})"
                  .format(wait, attempt, MAX_RETRIES))
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.content
    raise RuntimeError("OASIS kept returning 429 after {} attempts".format(MAX_RETRIES))

# ----------------------------------------------------------------------
def main():
    temp = pathlib.Path("/tmp/lmp_{}.zip".format(YEAR_MONTH))
    if temp.exists():
        temp.unlink()                   # start fresh

    # download & append each weekly ZIP
    for start_date, end_date in weeks_in_month(YEAR_MONTH):
        url = OASIS.format(start_date, end_date)
        print("Downloading {}".format(url))
        chunk = fetch(url)
        temp.write_bytes(chunk)
        time.sleep(SLEEP_SEC)           # polite gap before next call

    hdfs_target = "/data/raw/lmp/{}/lmp_{}.zip".format(YEAR_MONTH, YEAR_MONTH)
    hdfs_put(str(temp), hdfs_target)
    print("Uploaded to HDFS: {}".format(hdfs_target))

    # size sanity-check
    size = subprocess.check_output(["hadoop", "fs", "-du", "-s", hdfs_target]).split()[0]
    if int(size) < 10000:
        print("WARNING: ZIP is only {} bytes; CAISO likely returned no data".format(size))

# ----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python download_lmp.py YYYY-MM")
    main()