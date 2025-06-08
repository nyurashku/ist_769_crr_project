#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM   (recycled)

Download CAISO *Public Market Results* XML for one month and upload it to HDFS
    /data/raw/crr_market/<YYYY-MM>/PublicMarketResults_<YYYYMM>.xml
Then it’s ready for `parse_crr_market_results.py`.
"""
import os, sys, argparse, subprocess, requests
from datetime import datetime, timedelta
from urllib.parse import urlencode

# ── HDFS helpers ─────────────────────────────────────────────────────────────
HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"

def hdfs_put(local: str, hdfs_path: str) -> None:
    """mkdir -p && put -f."""
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
                   check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", local, hdfs_path], check=True)

# ── month window helper ──────────────────────────────────────────────────────
def month_bounds(ym: str) -> tuple[str, str]:
    first = datetime.strptime(ym + "-01", "%Y-%m-%d")
    next_first = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    return first.strftime("%Y-%m-%d"), next_first.strftime("%Y-%m-%d")

# ── main download routine ────────────────────────────────────────────────────
def main(ym: str) -> None:
    start, end = month_bounds(ym)

    # ▶▶ ***REPLACE ONLY THE `BASE` AND PARAM NAMES IF NEEDED*** ◀◀
    BASE = "https://crr.caiso.com/crr/resources/download/PublicMarketResults"  # working endpoint
    params = {
        "startDate": start,   # exactly the param names the web UI sends
        "endDate"  : end,
        "format"   : "xml"
    }
    url = BASE + "?" + urlencode(params)
    print("Downloading", url)

    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    if b"<CRRDownload:MarketResults" not in resp.content:
        sys.exit("!! response doesn’t look like MarketResults XML – aborting")

    local = f"/tmp/PublicMarketResults_{ym.replace('-','')}.xml"
    open(local, "wb").write(resp.content)

    hdfs_path = (f"{HDFS_URI}/data/raw/crr_market/{ym}/"
                 f"PublicMarketResults_{ym.replace('-','')}.xml")
    hdfs_put(local, hdfs_path)
    size = int(subprocess.check_output(
        [HADOOP, "fs", "-du", "-s", hdfs_path]).split()[0])
    print(f"✅  Uploaded → {hdfs_path}  ({size/1e6:.1f} MB)")
    os.remove(local)

# ── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must look like 2008-01")
    main(args.year_month)