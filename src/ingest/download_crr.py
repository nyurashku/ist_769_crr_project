#!/usr/bin/env python3
"""
download_crr.py  YYYY-MM

Download one month of *public* CRR market-results from
https://crr.caiso.com and push the concatenated XML into HDFS:
  hdfs://hadoop-namenode:8020/data/raw/crr_public/<YYYY-MM>/MarketResults.xml
"""
import os, sys, argparse, subprocess, datetime as dt, ssl, tempfile, shutil, requests
from urllib.parse import urlencode
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# ── force TLS-1.2 + allow legacy ciphers ────────────────────────────────────
class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kw):
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.set_ciphers("ALL:@SECLEVEL=0")          # ← wider & sec-level 0
        ctx.options    |= ssl.OP_LEGACY_SERVER_CONNECT  # ← allow “bad” servers
        kw["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kw)
        
sess = requests.Session()
sess.mount("https://", TLS12Adapter())

# ── constants ───────────────────────────────────────────────────────────────
BASE   = "https://crr.caiso.com/crr/resources/download/PublicMarketResults"
HADOOP = "/opt/hadoop/bin/hadoop"
HDFS   = "hdfs://hadoop-namenode:8020"

def fetch_day(day: dt.date, out_dir: Path):
    nxt = day + dt.timedelta(days=1)
    params = {"startDate": day.isoformat(), "endDate": nxt.isoformat()}
    url    = BASE + "?" + urlencode(params)
    print(f"→ {day} {url}")
    xml    = sess.get(url, timeout=120).content          # TLS settings already applied
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{day}.xml").write_bytes(xml)

def hdfs_put(local_dir: Path, ym: str):
    hdfs_dir = f"{HDFS}/data/raw/crr_public/{ym}"
    subprocess.run([HADOOP, "fs", "-mkdir", "-p", hdfs_dir], check=True)
    subprocess.run([HADOOP, "fs", "-put", "-f", str(local_dir / "*.xml"), hdfs_dir], shell=True, check=True)
    out = subprocess.check_output([HADOOP, "fs", "-du", "-s", hdfs_dir]).split()[0]
    print(f"✅  Uploaded → {hdfs_dir}  ({int(out)/1e6:.1f} MB)")

def main(ym: str):
    first = dt.datetime.strptime(ym + "-01", "%Y-%m-%d").date()
    next_first = (first.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    tmp = Path(tempfile.mkdtemp())
    day = first
    while day < next_first:
        fetch_day(day, tmp)
        day += dt.timedelta(days=1)
    hdfs_put(tmp, ym)
    shutil.rmtree(tmp)

# ── CLI ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("year_month", help="YYYY-MM")
    args = ap.parse_args()
    if len(args.year_month) != 7 or args.year_month[4] != "-":
        sys.exit("year_month must be like 2008-01")
    main(args.year_month)