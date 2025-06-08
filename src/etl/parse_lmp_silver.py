#!/usr/bin/env python3
"""
download_lmp.py  YYYY-MM  [--market DAM|RTM]

Download daily CAISO LMP ZIPs for three hub nodes and upload them to HDFS
       /data/raw/lmp/<market>/<YYYY-MM>/<NODE>_<YYYYMMDD>.zip
"""

import os, sys, time, random, subprocess, argparse
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests

# ── NameNode URI used by every hdfs dfs command ───────────────────────────────
HDFS_URI = "hdfs://namenode:8020"

NODES = ["TH_SP15_GEN-APND", "TH_NP15_GEN-APND", "TH_ZP26_GEN-APND"]
BASE  = "https://oasis.caiso.com/oasisapi/SingleZip"
COMMON = {
    "queryname":    "PRC_LMP",
    "version":      "1",
    "resultformat": "6",
}

MAX_RETRY  = 3
SLEEP_BASE = 3  # seconds – back-off grows with retry #

# --------------------------------------------------------------------------- #
def _abs_hdfs(path: str) -> str:
    """Prepend the NameNode URI if *path* is an absolute HDFS path."""
    return HDFS_URI + path if path.startswith("/") else path


def hdfs_put(local_path: str, hdfs_path: str) -> None:
    """Upload *local_path* to *hdfs_path* (overwriting if it exists)."""
    hdfs_path = _abs_hdfs(hdfs_path)
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
        check=True,
    )
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
        check=True,
    )


def one_day_range(year_month: str):
    """Yield datetime objects for every day in YYYY-MM."""
    d = datetime.strptime(year_month + "-01", "%Y-%m-%d")
    target_month = d.month
    while d.month == target_month:
        yield d
        d += timedelta(days=1)


def fetch(url: str) -> bytes:
    """HTTP GET with polite retry/back-off."""
    for attempt in range(1, MAX_RETRY + 1):
        r = requests.get(url, timeout=300)
        if r.status_code == 200:
            return r.content
        if r.status_code == 429:
            wait = SLEEP_BASE * attempt + random.uniform(0, 1)
            print(f" 429 – sleeping {wait:.1f}s (retry {attempt}/{MAX_RETRY})")
            time.sleep(wait)
            continue
        r.raise_for_status()
    raise RuntimeError("Still hitting 429 after retries")


# --------------------------------------------------------------------------- #
def main(year_month: str, market: str) -> None:
    for day_dt in one_day_range(year_month):
        day_str   = day_dt.strftime("%Y-%m-%d")
        start_str = day_dt.strftime("%Y%m%dT00:00-0000")
        end_str   = (day_dt + timedelta(days=1)).strftime("%Y%m%dT00:00-0000")

        for node in NODES:
            # build OASIS query
            qs = COMMON | {
                "market_run_id": market,
                "node":          node,
                "startdatetime": start_str,
                "enddatetime":   end_str,
            }
            url = BASE + "?" + urlencode(qs)
            print("Downloading", url)

            raw = fetch(url)
            if not raw.startswith(b"PK"):          # basic ZIP signature check
                print(f"  !! non-ZIP payload ({len(raw)} bytes) – skipped")
                continue

            local = f"/tmp/{node}_{day_str.replace('-', '')}.zip"
            with open(local, "wb") as fh:
                fh.write(raw)

            hdfs_rel = (
                f"/data/raw/lmp/{market}/{year_month}/"
                f"{node}_{day_str.replace('-', '')}.zip"
            )
            hdfs_put(local, hdfs_rel)

            size = int(
                subprocess.check_output(
                    ["hdfs", "dfs", "-du", "-s", _abs_hdfs(hdfs_rel)]
                ).split()[0]
            )
            print(f"OK → {hdfs_rel} ({size/1e6:.1f} MB)")

            os.remove(local)  # keep container tidy
            time.sleep(0.5)   # be (reasonably) polite


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    ap.add_argument(
        "--market", default="DAM",
        choices=["DAM", "RTM", "RTPD"],
        help="market_run_id (default=DAM)",
    )
    args = ap.parse_args()

    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must be in YYYY-MM form (e.g. 2024-02)")

    main(args.year_month, args.market.upper())