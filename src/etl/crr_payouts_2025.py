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
MW_COL   = F.lit(1.0)               # ⚠️ put real MW when you have it
# ─────────────────────────────────────────────────────────────────────────

spark = (SparkSession.builder
         .appName("crr-payouts-2025")
         .config("spark.cassandra.connection.host", "cassandra")  # container name / IP
         .getOrCreate())

# 1) read data
crr  = spark.read.parquet(CRR_SRC)
lmp  = spark.read.parquet(LMP_SRC)

# 2) make helper columns
#     a) ON =  HE hour 07-22 PPT  ( = 14-05 UTC )   – 16 intervals
#     b) OFF = the complement   –   8 intervals
def make_hour_list(start, end, tou):
    import pandas as pd, pytz
    rng = pd.date_range(start, end, freq="1H", tz="US/Pacific", inclusive="left")
    if tou == "ON":
        rng = rng[(rng.hour >= 7) & (rng.hour < 23)]
    else:
        rng = rng[(rng.hour  < 7) | (rng.hour >= 23)]
    return list(rng.tz_convert("UTC"))
make_hour_list_udf = F.udf(make_hour_list,
                           T.ArrayType(T.TimestampType()))

crr = (crr
       .withColumn("hour_utc",
                   F.explode(make_hour_list_udf("START_DATE", "END_DATE", "TIME_OF_USE")))
       .withColumn("hour_local", F.from_utc_timestamp("hour_utc", "US/Pacific"))
       .select("MARKET_NAME",
               "APNODE_ID",
               "TIME_OF_USE",
               "hour_utc",
               "hour_local",
               MW_COL.alias("MW")))

# 3) join twice – once as sink, once as source
lmp_renamed = (lmp
               .select(F.col("OPR_DT").alias("hour_utc"),
                       F.col("NODE").alias("APNODE_ID"),
                       F.col("LMP_PRC").alias("LMP")))

crr_sink  = crr.alias("c").join(lmp_renamed.alias("s"),
                                ["APNODE_ID", "hour_utc"]) \
                       .selectExpr("c.*", "s.LMP as LMP_sink")
# here you’d repeat for source node; for demo we just copy sink → source
crr_final = (crr_sink
             .withColumn("LMP_source", F.col("LMP_sink"))   # TODO real source join
             .withColumn("realized_cong",  F.col("LMP_sink") - F.col("LMP_source"))
             .withColumn("realized_payout", F.col("realized_cong") * F.col("MW")))

# 4) write to Cassandra
(crr_final
 .write
 .format("org.apache.spark.sql.cassandra")
 .mode("append")
 .option("keyspace", KEYSPACE)
 .option("table", TABLE)
 .save())

spark.stop()