#!/usr/bin/env python3
"""
upload_crr_clearing.py  <local_crr_data_dir>

Copies each CSV named   YYYY-MM*.csv   into HDFS
  /data/raw/crr_clearing/YYYY-MM/<same-filename>.csv
"""
import sys, os, re, subprocess, pathlib

HADOOP   = "/opt/hadoop/bin/hadoop"
HDFS_URI = "hdfs://hadoop-namenode:8020"

def hdfs_put(local, hdfs_file):
    subprocess.run([HADOOP,"fs","-mkdir","-p",os.path.dirname(hdfs_file)],check=True)
    subprocess.run([HADOOP,"fs","-put","-f",local,hdfs_file],check=True)

def main(folder):
    patt = re.compile(r"^\d{4}-\d{2}")                 # grabs YYYY-MM
    for path in pathlib.Path(folder).iterdir():
        if path.suffix.lower() != ".csv": continue
        m = patt.match(path.name)
        if not m:
            print(f"!! skip {path.name} (no YYYY-MM prefix)")
            continue
        ym   = m.group(0)
        dest = f"{HDFS_URI}/data/raw/crr_clearing/{ym}/{path.name}"
        hdfs_put(str(path), dest)
        print(f"✓ {path.name}  →  {dest}")

if __name__ == "__main__":
    if len(sys.argv)!=2:
        sys.exit("usage: upload_crr_clearing.py  <local_crr_data_dir>")
    main(sys.argv[1])