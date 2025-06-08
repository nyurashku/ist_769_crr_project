#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Download a CAISO monthly CRR “SEC_ML” archive and upload it to HDFS
    /data/raw/crr/<YYYY-MM>/CAISO_CRR_<YYYYMM>.zip
"""
import argparse, os, subprocess
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests

HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"

def hdfs_put(local: str, hdfs_abs: str) -> None:
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_abs)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, hdfs_abs], check=True)

# ── OASIS parameters ────────────────────────────────────────────────────────
BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = { "queryname": "CRR_SEC_ML", "version": "1", "resultformat": "6" }

def month_window(ym: str) -> tuple[str, str]:
    first = datetime.strptime(ym + "-01", "%Y-%m-%d")
    start = first.strftime("%Y%m%dT00:00-0000")
    # end = *first day of next month* @ 00:00
    nxt   = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    end   = nxt.strftime("%Y%m%dT00:00-0000")
    return start, end

def main(ym: str) -> None:
    start, end = month_window(ym)
    url = BASE + "?" + urlencode(COMMON | {"startdatetime": start,
                                           "enddatetime":   end})

    print("Downloading", url)
    raw = requests.get(url, timeout=300).content
    if not raw.startswith(b"PK\x03\x04"):
        print("!! non-ZIP payload – OASIS returned an error stub")
        return

    local = f"/tmp/CAISO_CRR_{ym.replace('-', '')}.zip"
    open(local, "wb").write(raw)

    hdfs_abs = f"{HDFS_URI}/data/raw/crr/{ym}/CAISO_CRR_{ym.replace('-', '')}.zip"
    hdfs_put(local, hdfs_abs)

    size = int(subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs_abs]).split()[0])
    if size < 10_000:                                     # still suspiciously small
        print(f"!! uploaded file is only {size} bytes – probably still a stub")
    else:
        print(f"✅  Uploaded → {hdfs_abs}  ({size/1e6:.1f} MB)")

    os.remove(local)

# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser();  ap.add_argument("year_month")
    ym = ap.parse_args().year_month
    if len(ym) != 7 or ym[4] != "-":
        ap.error("year_month must be in YYYY-MM form, e.g. 2023-11")
    main(ym)