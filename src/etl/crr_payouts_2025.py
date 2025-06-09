#!/usr/bin/env python3
"""
crr_payouts_2025.py
Join 2025 CRR clearing prices with hourly DAM LMPs → realized congestion & payout
"""
from pyspark.sql import SparkSession, functions as F, types as T

# ─── config ──────────────────────────────────────────────────────────────
CRR_SRC  = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/crr_clearing/2025"
LMP_SRC  = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/lmp/DAM/2025/*"
KEYSPACE = "caiso"
TABLE    = "crr_payouts"            # will be created if absent
# ─────────────────────────────────────────────────────────────────────────

# Start Spark first
spark = (SparkSession.builder
           .appName("crr-payouts-2025")
           .config("spark.cassandra.connection.host", "cassandra")   # service name / IP
           .getOrCreate())

# now it’s safe to create literals
MW_COL  = F.lit(1.0)                # ⚠️ replace with real cleared_mw later
LOAD_TS = F.current_timestamp()

# 1) read data
crr = spark.read.parquet(CRR_SRC)
lmp = spark.read.parquet(LMP_SRC)

# 2) build hourly list for every CRR row
def make_hour_list(start, end, tou):
    import pandas as pd, pytz
    rng = pd.date_range(start, end, freq="1H", tz="US/Pacific", inclusive="left")
    if tou == "ON":
        rng = rng[(rng.hour >= 7) & (rng.hour < 23)]
    else:
        rng = rng[(rng.hour  < 7) | (rng.hour >= 23)]
    return list(rng.tz_convert("UTC"))

make_hours_udf = F.udf(make_hour_list, T.ArrayType(T.TimestampType()))

crr = (crr
       .withColumn("hour_utc",
                   F.explode(make_hours_udf("START_DATE", "END_DATE", "TIME_OF_USE")))
       .withColumn("hour_local", F.from_utc_timestamp("hour_utc", "US/Pacific"))
       .select("MARKET_NAME",
               "APNODE_ID",
               "TIME_OF_USE",
               "hour_utc",
               "hour_local",
               MW_COL.alias("MW")))

lmp_sel = (lmp
           .filter(F.col("LMP_TYPE") == "LMP")       # keep the actual price rows
           # hour-ending is 1-24 → subtract 1 to get hour-beginning
           .withColumn(
               "hour_utc",
               F.to_timestamp(
                   F.concat_ws(' ', F.col("OPR_DT"),
                               (F.col("OPR_HR") - 1).cast("int")),
                   "yyyy-MM-dd H"
               )
           )
           .select("hour_utc",
                   F.col("NODE").alias("APNODE_ID"),
                   F.col("MW").alias("LMP"))
          )


# sink join
crr_sink = (crr.alias("c")
              .join(lmp_sel.alias("s"), ["APNODE_ID", "hour_utc"])
              .selectExpr("c.*", "s.LMP as LMP_sink"))

# SOURCE JOIN – replace this with real source APNODE_ID when available.
# For the demo we just reuse sink = source so realized congestion = 0.
crr_final = (crr_sink
             .withColumn("LMP_source", F.col("LMP_sink"))   # TODO: proper source join
             .withColumn("realized_cong",  F.col("LMP_sink") - F.col("LMP_source"))
             .withColumn("payout_usd",     F.col("realized_cong") * F.col("MW"))
             .withColumn("load_ts",        LOAD_TS))

# 4) write to Cassandra
(crr_final
 .write
 .format("org.apache.spark.sql.cassandra")
 .mode("append")
 .option("keyspace", KEYSPACE)
 .option("table",    TABLE)
 .save())

spark.stop()
