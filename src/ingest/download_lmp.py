#!/usr/bin/env python
"""
download_lmp.py  YYYY-MM
Downloads CAISO RTM LMPs for ALL APnodes (one day at a time) and uploads the
combined ZIP to HDFS at /data/raw/lmp/YYYY-MM/lmp_YYYY-MM.zip
"""
import os, sys, subprocess, requests, pathlib, time, random
from datetime import datetime, timedelta

YEAR_MONTH = sys.argv[1]            # e.g. 2024-01

SINGLEZIP = (
    "http://oasis.caiso.com/oasisapi/SingleZip?"
    "queryname=PRC_LMP&version=1&market_run_id=RTM"
    "&node=ALL_APNODE"          # ← essential!
    "&startdatetime={}T00:00-0000&enddatetime={}T23:00-0000"
    "&resultformat=6"
)

MAX_RETRIES = 3
SLEEP_SEC   = 3

# ----------------------------------------------------------------------
def hdfs_put(src, dst):
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", os.path.dirname(dst)], check=True)
    subprocess.run(["hadoop", "fs", "-put", "-f", src, dst], check=True)

def days_in_month(ym):
    d = datetime.strptime(ym + "-01", "%Y-%m-%d")
    while d.month == int(ym.split("-")[1]):
        yield d.strftime("%Y-%m-%d")   # 2024-01-03
        d += timedelta(days=1)

def fetch(url):
    for attempt in range(1, MAX_RETRIES + 1):
        r = requests.get(url, timeout=900)
        if r.status_code == 429:
            wait = SLEEP_SEC * attempt + random.uniform(0, 1)
            print("429 Too Many Requests; sleeping {:.1f}s (attempt {}/{})"
                  .format(wait, attempt, MAX_RETRIES))
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.content
    raise RuntimeError("OASIS kept returning 429 after {} attempts".format(MAX_RETRIES))

# ----------------------------------------------------------------------
def main():
    tmp = pathlib.Path("/tmp/lmp_{}.zip".format(YEAR_MONTH))
    if tmp.exists():
        tmp.unlink()

    for day in days_in_month(YEAR_MONTH):
        url = SINGLEZIP.format(day, day)
        print("Downloading {}".format(url))
        tmp.write_bytes(fetch(url))
        time.sleep(SLEEP_SEC)

    hdfs_target = "/data/raw/lmp/{}/lmp_{}.zip".format(YEAR_MONTH, YEAR_MONTH)
    hdfs_put(str(tmp), hdfs_target)
    print("Uploaded to HDFS: {}".format(hdfs_target))

    size = int(subprocess.check_output(
        ["hadoop", "fs", "-du", "-s", hdfs_target]).split()[0])
    print("File size in HDFS: {:.1f} MB".format(size / 1e6))

# ----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python download_lmp.py YYYY-MM")
    main()