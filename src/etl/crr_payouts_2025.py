#!/usr/bin/env python3
"""
crr_payouts_2025.py – join 2025 CRR clearing with DAM LMPs,
write realised payouts to Cassandra (keyspace caiso.table crr_payouts)
"""
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql import Window

CRR_SRC  = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/crr_clearing/2025"
LMP_SRC  = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/lmp/DAM/2025/*"
KEYSPACE = "caiso";  TABLE = "crr_payouts"

spark = (SparkSession.builder
           .appName("crr-payouts-2025")
           .config("spark.cassandra.connection.host", "cassandra")
           .config("spark.sql.session.timeZone", "UTC")
           .getOrCreate())

MW_COL = F.lit(1.0)                      # placeholder MW

# ── 1  read silver data ──────────────────────────────────────────────
crr = spark.read.parquet(CRR_SRC)
lmp = (spark.read.parquet(LMP_SRC)
           .filter("LMP_TYPE = 'LMP'"))

# ── 2  explode CRR rows into hourly UTC timestamps ───────────────────
crr = (crr
       .withColumn("start_utc", F.to_utc_timestamp("START_DATE",  "US/Pacific"))
       .withColumn("end_utc",   F.to_utc_timestamp("END_DATE",    "US/Pacific"))
       # generate an array of hourly instants [start, end)
       .withColumn("hour_arr",
                   F.expr("sequence(start_utc, end_utc - INTERVAL 1 HOUR, INTERVAL 1 HOUR)"))
       .withColumn("hour_utc",  F.explode("hour_arr"))
       .withColumn("hour_local", F.from_utc_timestamp("hour_utc", "US/Pacific"))
       .filter(  # keep ON / OFF hours
           F.when(F.col("TIME_OF_USE") == "ON",
                  (F.hour("hour_local") >= 7) & (F.hour("hour_local") < 23))
            .otherwise(
                  (F.hour("hour_local") < 7) | (F.hour("hour_local") >= 23)))
       .withColumn("hour_ending", F.hour("hour_local") + 1)
       .withColumnRenamed("APNODE_ID", "apnode_sink")
       .select("MARKET_NAME", "TIME_OF_USE", "hour_utc", "hour_ending",
               "apnode_sink", MW_COL.alias("cleared_mw")))

# ── 3  shape LMP table  (NOTE: rename the timestamp!) ────────────────
lmp_shaped = (lmp.selectExpr(
                 "to_timestamp(concat(OPR_DT,' ',OPR_HR-1),'yyyy-MM-dd H') "
                 "     as lmp_hour",
                 "NODE as lmp_node",
                 "MW   as lmp_val"))

# sink join
with_sink = (crr.alias("c")
             .join(lmp_shaped.alias("ls"),
                   (F.col("c.apnode_sink") == F.col("ls.lmp_node")) &
                   (F.col("c.hour_utc")    == F.col("ls.lmp_hour")),
                   "left")
             .withColumnRenamed("lmp_val", "lmp_sink"))

# source join (demo = SP15 hub)
with_src  = (with_sink
             .withColumn("apnode_src", F.lit("TH_SP15_GEN-APND"))
             .join(lmp_shaped.alias("lx"),
                   (F.col("apnode_src") == F.col("lx.lmp_node")) &
                   (F.col("hour_utc")   == F.col("lx.lmp_hour")),
                   "left")
             .withColumnRenamed("lmp_val", "lmp_source"))
# ── 4  realised congestion & payout ──────────────────────────────────────
out = (
    with_src
      .withColumn("realized_cong", F.col("lmp_sink") - F.col("lmp_source"))
      .withColumn("payout_usd",    F.col("realized_cong") * F.col("cleared_mw"))
      .withColumn("opr_dt",        F.to_date("hour_utc"))
      .withColumn("load_ts",       F.current_timestamp())
      .withColumnRenamed("MARKET_NAME", "market_name")
      .withColumnRenamed("TIME_OF_USE", "time_of_use")      # ▼ rename
      .select(  "market_name", "opr_dt", "hour_ending", "time_of_use",
                "apnode_src",  "apnode_sink",
                "cleared_mw",  "realized_cong", "payout_usd", "load_ts")
)


# ── 5  write to Cassandra ────────────────────────────────────────────
(out.write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")
    .option("keyspace", KEYSPACE)
    .option("table", TABLE)
    .save())

spark.stop()
