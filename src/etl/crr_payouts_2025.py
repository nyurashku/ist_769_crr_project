#!/usr/bin/env python3
"""
crr_payouts_2025.py
Join 2025 CRR clearing prices with hourly DAM LMPs → realized congestion
and payout, then write to Cassandra keyspace `caiso`, table `crr_payouts`.
"""

from pyspark.sql import SparkSession, functions as F, types as T

# ─── config ──────────────────────────────────────────────────────────────
CRR_SRC  = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/crr_clearing/2025"
LMP_SRC  = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/lmp/DAM/2025/*"
KEYSPACE = "caiso"
TABLE    = "crr_payouts"
MW_COL   = F.lit(1.0)                       # placeholder until true MW known
# ─────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("crr-payouts-2025")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# ── 1  read silver tables ────────────────────────────────────────────────
crr = spark.read.parquet(CRR_SRC)
lmp = (spark.read.parquet(LMP_SRC)
       .filter("LMP_TYPE = 'LMP'"))

# ── 2  explode every CRR row into the hours that belong to its TOU band ──
@F.udf(T.ArrayType(T.TimestampType()))
def hours_in_tou(st, et, tou):
    import pandas as pd
    rng = pd.date_range(st, et, freq="1H",
                        tz="US/Pacific", inclusive="left")
    if tou == "ON":
        rng = rng[(rng.hour >= 7) & (rng.hour < 23)]
    else:
        rng = rng[(rng.hour  < 7) | (rng.hour >= 23)]
    return list(rng.tz_convert("UTC"))

crr = (
    crr.withColumn("hour_utc",   F.explode(hours_in_tou("START_DATE",
                                                        "END_DATE",
                                                        "TIME_OF_USE")))
       .withColumn("hour_local", F.from_utc_timestamp("hour_utc","US/Pacific"))
       .withColumn("hour_ending", F.hour("hour_local") + 1)        # 1-24 PPT
       .withColumnRenamed("APNODE_ID", "apnode_sink")
       .select("MARKET_NAME","TIME_OF_USE","hour_utc","hour_ending",
               "apnode_sink", MW_COL.alias("cleared_mw"))
)

# ── 3  prepare LMP table & join twice (sink / source) ────────────────────
lmp = (
    lmp.selectExpr(
        "to_timestamp(concat(OPR_DT,' ',OPR_HR-1),'yyyy-MM-dd H') "
        "           as lmp_hour",
        "NODE       as lmp_node",
        "MW         as lmp_val")
)

# 3a  join for sink
with_sink = (
    crr.alias("c")
       .join(lmp.alias("ls"),
             (F.col("c.apnode_sink") == F.col("ls.lmp_node")) &
             (F.col("c.hour_utc")    == F.col("ls.lmp_hour")),
             "left")
       .select("c.*", F.col("ls.lmp_val").alias("lmp_sink"))
)

# 3b  join for source (demo uses SP15 hub as universal source)
with_src = (
    with_sink
      .withColumn("apnode_src", F.lit("TH_SP15_GEN-APND"))
      .join(lmp.alias("lx"),
            (F.col("apnode_src") == F.col("lx.lmp_node")) &
            (F.col("hour_utc")   == F.col("lx.lmp_hour")),
            "left")
      .select("*", F.col("lx.lmp_val").alias("lmp_source"))
)

# ── 4  realised congestion & payout ──────────────────────────────────────
out = (
    with_src
      .withColumn("realized_cong", F.col("lmp_sink") - F.col("lmp_source"))
      .withColumn("payout_usd",    F.col("realized_cong") * F.col("cleared_mw"))
      .withColumn("opr_dt",        F.to_date("hour_utc"))
      .withColumn("load_ts",       F.current_timestamp())
      .select("MARKET_NAME","opr_dt","hour_ending","TIME_OF_USE",
              "apnode_src","apnode_sink",
              "cleared_mw","lmp_source","lmp_sink",
              "realized_cong","payout_usd","load_ts")
)

# ── 5  write to Cassandra ────────────────────────────────────────────────
(out.write
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", KEYSPACE)
    .option("table",    TABLE)
    .mode("append")
    .save())

spark.stop()
