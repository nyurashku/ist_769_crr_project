#!/usr/bin/env python
"""
download_lmp.py  YYYY-MM
Fetches CAISO RTM LMPs (all APnodes) one day at a time and uploads the
combined ZIP to HDFS at /data/raw/lmp/YYYY-MM/lmp_YYYY-MM.zip

Example:
    python download_lmp.py 2024-01
"""
import os, sys, subprocess, requests, pathlib, time, random
from datetime import datetime, timedelta

# ----------------------------------------------------------------------
YEAR_MONTH = sys.argv[1]            # e.g. 2024-01  (must be YYYY-MM)

# ArchiveZip query (daily) — 2 placeholders become YYYYMMDD
OASIS = (
    "http://oasis.caiso.com/oasisapi/ArchiveZip?"
    "queryname=PRC_LMP&version=1&market_run_id=RTM"
    "&period=d&node=ALL_APNODE"
    "&startdatetime={}&enddatetime={}"
)

MAX_RETRIES = 3
SLEEP_SEC   = 3     # polite gap between calls

# ----------------------------------------------------------------------
def hdfs_put(local_path: str, hdfs_path: str) -> None:
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
                   check=True)
    subprocess.run(["hadoop", "fs", "-put", "-f", local_path, hdfs_path],
                   check=True)

def days_in_month(ym: str):
    d = datetime.strptime(ym + "-01", "%Y-%m-%d")
    while d.month == int(ym.split("-")[1]):
        yield d.strftime("%Y%m%d")          # 2024-01-03  →  20240103
        d += timedelta(days=1)

def fetch(url: str) -> bytes:
    """GET with retry on HTTP 429."""
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
def main() -> None:
    tmp = pathlib.Path("/tmp/lmp_{}.zip".format(YEAR_MONTH))
    if tmp.exists():
        tmp.unlink()

    # download & append each daily ZIP
    for ymd in days_in_month(YEAR_MONTH):
        url = OASIS.format(ymd, ymd)
        print("Downloading {}".format(url))
        chunk = fetch(url)
        tmp.write_bytes(chunk)
        time.sleep(SLEEP_SEC)              # polite gap

    hdfs_target = "/data/raw/lmp/{}/lmp_{}.zip".format(YEAR_MONTH, YEAR_MONTH)
    hdfs_put(str(tmp), hdfs_target)
    print("Uploaded to HDFS: {}".format(hdfs_target))

    size = int(subprocess.check_output(
        ["hadoop", "fs", "-du", "-s", hdfs_target]).split()[0])
    if size < 10000:
        print("WARNING: ZIP is only {} bytes; CAISO likely returned no data".format(size))
    else:
        print("File size in HDFS: {:.1f} MB".format(size / 1e6))

# ----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python download_lmp.py YYYY-MM")
    main()