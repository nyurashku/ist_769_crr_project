#!/usr/bin/env python3
"""
download_lmp.py  YYYY-MM  [--market DAM|RTM]

Fetch CAISO LMP ZIPs one-day-at-a-time for the three hub nodes and
store them in HDFS:

    /data/raw/lmp/<market>/<YYYY-MM>/<NODE>_<YYYYMMDD>.zip
"""
import os, sys, time, random, io, zipfile, subprocess, argparse
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests

NODES = ["TH_SP15_GEN-APND", "TH_NP15_GEN-APND", "TH_ZP26_GEN-APND"]
BASE  = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "PRC_LMP",
    "version":      "12",       # 12 = last “stable” one
    "resultformat": "6",
}

MAX_RETRY  = 3
SLEEP_BASE = 3          # seconds – back-off grows with retry #

# ---------------------------------------------------------------------------
def hdfs_put(local_path, hdfs_path):
    subprocess.run(["hadoop", "fs", "-mkdir", "-p",
                    os.path.dirname(hdfs_path)], check=True)
    subprocess.run(["hadoop", "fs", "-put", "-f",
                    local_path, hdfs_path], check=True)

def one_day_range(ym):
    d = datetime.strptime(ym + "-01", "%Y-%m-%d")
    month = int(ym.split("-")[1])
    while d.month == month:
        yield d
        d += timedelta(days=1)

def fetch(url):
    for attempt in range(1, MAX_RETRY + 1):
        r = requests.get(url, timeout=300)
        if r.status_code == 200:
            return r.content
        if r.status_code == 429:
            wait = SLEEP_BASE * attempt + random.uniform(0, 1)
            print(" 429 – sleeping {:.1f}s (retry {}/{})".format(
                  wait, attempt, MAX_RETRY))
            time.sleep(wait)
            continue
        r.raise_for_status()
    raise RuntimeError("Still hitting 429 after retries")

# ---------------------------------------------------------------------------
def main(year_month, market):
    for day_dt in one_day_range(year_month):
        day_str   = day_dt.strftime("%Y-%m-%d")
        start_str = day_dt.strftime("%Y%m%dT00:00-0000")
        end_str   = (day_dt + timedelta(days=1)).strftime("%Y%m%dT00:00-0000")

        for node in NODES:
            qs = COMMON.copy()
            qs.update({
                "market_run_id": market,
                "node":          node,
                "startdatetime": start_str,
                "enddatetime":   end_str
            })
            url = BASE + "?" + urlencode(qs)
            print("Downloading {}".format(url))
            raw = fetch(url)

            if not raw.startswith(b"PK"):
                print("  !! got non-ZIP payload ({} bytes) – skipping"
                      .format(len(raw)))
                continue

            local = "/tmp/{}_{}.zip".format(node, day_str.replace("-", ""))
            open(local, "wb").write(raw)

            hdfs  = "/data/raw/lmp/{}/{}/{}_{}.zip".format(
                        market, year_month, node, day_str.replace("-", ""))
            hdfs_put(local, hdfs)
            size = int(subprocess.check_output(
                       ["hadoop", "fs", "-du", "-s", hdfs]).split()[0])
            print("✓ uploaded → {} ({:.1f} MB)".format(hdfs, size / 1e6))

            time.sleep(1)   # be polite

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("year_month", help="YYYY-MM")
    parser.add_argument("--market", default="DAM",
                        choices=["DAM", "RTM", "RTPD"],
                        help="market_run_id (default=DAM)")
    args = parser.parse_args()

    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must be in YYYY-MM form (e.g. 2024-01)")

    main(args.year_month, args.market.upper())
