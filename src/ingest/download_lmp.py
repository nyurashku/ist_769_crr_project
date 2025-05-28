#!/usr/bin/env python
"""
download_lmp.py  YYYY-MM
Downloads daily CAISO RTM LMPs for the three zonal hubs
   • SP15  (TH_SP15_GEN-APND)
   • NP15  (TH_NP15_GEN-APND)
   • ZP26  (TH_ZP26_GEN-APND)
Combines all daily ZIPs and uploads to:
   /data/raw/lmp/YYYY-MM/lmp_YYYY-MM.zip   (in HDFS)

Example:
    python download_lmp.py 2024-01
"""
import os, sys, subprocess, requests, pathlib, time, random
from datetime import datetime, timedelta

# ---------------- configuration ------------------------------------------
YEAR_MONTH = sys.argv[1]               # e.g. 2024-01
NODES = "TH_SP15_GEN-APND,TH_NP15_GEN-APND,TH_ZP26_GEN-APND"

URL_TMPL = (
    "http://oasis.caiso.com/oasisapi/SingleZip?"
    "queryname=PRC_LMP&version=1&market_run_id=RTM"
    "&node={nodes}"
    "&startdatetime={day}T00:00-0000&enddatetime={day}T23:00-0000"
    "&resultformat=6"
)

MAX_RETRIES = 3
SLEEP_SEC   = 3
# -------------------------------------------------------------------------

def hdfs_put(src, dst):
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", os.path.dirname(dst)], check=True)
    subprocess.run(["hadoop", "fs", "-put", "-f", src, dst], check=True)

def days_in_month(ym):
    d = datetime.strptime(ym + "-01", "%Y-%m-%d")
    while d.month == int(ym.split("-")[1]):
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)

def fetch(url):
    for i in range(1, MAX_RETRIES + 1):
        r = requests.get(url, timeout=300)
        if r.status_code == 429:
            wait = SLEEP_SEC * i + random.uniform(0, 1)
            print("429 Too Many Requests; sleeping {:.1f}s".format(wait))
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.content
    raise RuntimeError("Still 429 after {} retries".format(MAX_RETRIES))

def main():
    tmp = pathlib.Path("/tmp/lmp_{}.zip".format(YEAR_MONTH))
    if tmp.exists():
        tmp.unlink()

    for day in days_in_month(YEAR_MONTH):
        url = URL_TMPL.format(nodes=NODES, day=day)
        print("Downloading {}".format(url))
        tmp.write_bytes(fetch(url))
        time.sleep(SLEEP_SEC)

    hdfs_target = "/data/raw/lmp/{}/lmp_{}.zip".format(YEAR_MONTH, YEAR_MONTH)
    hdfs_put(str(tmp), hdfs_target)
    size = int(subprocess.check_output(
        ["hadoop", "fs", "-du", "-s", hdfs_target]).split()[0])
    print("Uploaded to HDFS ({:.1f} MB)".format(size / 1e6))

# -------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python download_lmp.py YYYY-MM")
    main()