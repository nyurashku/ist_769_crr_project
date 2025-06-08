#!/usr/bin/env python3
"""
download_crr_public.py YYYY-MM
Fetches daily *Public Market Results* XML from the CRR portal.
"""

import os, sys, subprocess, datetime as dt, requests
from urllib.parse import urlencode

HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"
BASE_URL = "https://crr.caiso.com/crr/resources/download/PublicMarketResults"

def hdfs_put(local, hdfs_path):
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, hdfs_path], check=True)

def fetch_day(day: dt.date, out_dir: str):
    next_day = day + dt.timedelta(days=1)
    url = BASE_URL + "?" + urlencode({
        "startDate": day.isoformat(),
        "endDate":   next_day.isoformat(),
    })
    print("→", day, url)
    xml = requests.get(url, timeout=120, verify=False).content
    local = f"/tmp/{day}.xml"
    open(local, "wb").write(xml)

    hdfs = f"{HDFS_URI}{out_dir}/day={day}.xml"
    hdfs_put(local, hdfs)
    os.remove(local)

def main(year_month: str):
    year, month = map(int, year_month.split("-"))
    first = dt.date(year, month, 1)
    last  = (first.replace(day=28) + dt.timedelta(days=4)).replace(day=1) \
            - dt.timedelta(days=1)

    out_dir = f"/data/raw/crr_public_results/{year_month}"
    d = first
    while d <= last:
        fetch_day(d, out_dir)
        d += dt.timedelta(days=1)

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1][4] != "-":
        sys.exit("Usage: download_crr_public.py YYYY-MM")
    main(sys.argv[1])