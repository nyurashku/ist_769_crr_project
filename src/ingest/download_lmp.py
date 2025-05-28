#!/usr/bin/env python
"""
download_lmp.py  YYYY-MM  →  puts raw OASIS ZIP into HDFS

Example:
    python download_lmp.py 2025-01
"""
import os, sys, subprocess, requests, pathlib

YEAR_MONTH = sys.argv[1]                         # e.g. 2025-01
OASIS = (
    "http://oasis.caiso.com/oasisapi/SingleZip?"
    "queryname=PRC_LMP&version=1&market_run_id=RTM"
    "&startdatetime={}-01T00:00-0000&enddatetime={}-31T23:00-0000"
    "&resultformat=6"
)

def hdfs_put(local_path, hdfs_path):
    # use the hadoop wrapper that ships with Spark
    subprocess.run(
        ["hadoop", "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
        check=True
    )
    subprocess.run(
        ["hadoop", "fs", "-put", "-f", local_path, hdfs_path],
        check=True
    )
def main():
    url = OASIS.format(YEAR_MONTH, YEAR_MONTH)
    print(f"Downloading {url}")
    data = requests.get(url, timeout=900)
    data.raise_for_status()

    temp = pathlib.Path(f"/tmp/lmp_{YEAR_MONTH}.zip")
    temp.write_bytes(data.content)
    hdfs_target = f"/data/raw/lmp/{YEAR_MONTH}/lmp_{YEAR_MONTH}.zip"

    hdfs_put(str(temp), hdfs_target)
    print(f"✓ uploaded to HDFS: {hdfs_target}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python download_lmp.py YYYY-MM")
    main()