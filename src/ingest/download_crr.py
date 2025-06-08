# src/ingest/download_crr.py
#!/usr/bin/env python3
"""
download_crr.py YYYY-MM

Grab CAISO CRR-auction clearing prices for one calendar month and drop the
ZIP into HDFS:
    /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip
"""
import os, sys, argparse, subprocess, tempfile, shutil, requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from dateutil.relativedelta import relativedelta   # pip install python-dateutil

# Hadoop inside the Spark container
HADOOP = "/opt/hadoop/bin/hadoop"
HDFS   = "hdfs://hadoop-namenode:8020"

BASE   = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "CRR_CLEARING",
    "version":      "1",
    "resultformat": "6",        # zip
}

def month_window(ym: str) -> tuple[str, str]:
    """Return (startdatetime, enddatetime) covering the full month."""
    first = datetime.strptime(ym + "-01", "%Y-%m-%d")
    next1 = first + relativedelta(months=+1)
    return (first.strftime("%Y%m%dT00:00-0000"),
            next1.strftime("%Y%m%dT00:00-0000"))

def hdfs_put(local: str, dest: str) -> None:
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(dest)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, dest], check=True)

def main(ym: str) -> None:
    start, end = month_window(ym)
    url = BASE + "?" + urlencode(COMMON | {"startdatetime": start,
                                           "enddatetime":   end})
    print("Downloading", url)
    raw = requests.get(url, timeout=600).content

    # quick sanity check – real archives are at least a few MB
    if len(raw) < 100_000:
        sys.exit("!! payload is too small – looks like an OASIS error stub")

    tmp   = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp.write(raw);  tmp.flush()

    hdfs_path = f"{HDFS}/data/raw/crr/{ym}/CAISO_CRR_{ym.replace('-', '')}.zip"
    hdfs_put(tmp.name, hdfs_path)

    size = subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs_path]).split()[0]
    print(f"✅  Uploaded → {hdfs_path}  ({int(size)//1_000_000:.1f} MB)")
    tmp.close();  os.remove(tmp.name)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM (e.g. 2025-05)")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must be YYYY-MM")
    main(args.year_month)