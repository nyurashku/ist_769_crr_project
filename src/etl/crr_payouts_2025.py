#!/usr/bin/env python3
"""
Join 2025 CRR clearing prices with hourly DAM LMPs
→ realised congestion and payout, write to Cassandra.
"""

from pyspark.sql import SparkSession, functions as F, types as T

# ─── paths ──────────────────────────────────────────────────────────────
CRR_SRC = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/crr_clearing/2025"
LMP_SRC = "hdfs://hadoop-namenode:8020/user/sparkuser/silver/lmp/DAM/2025/*"

KEYSPACE = "caiso"
TABLE    = "crr_payouts"

# dummy MW until you add real cleared-MW from the CRR file


spark = (SparkSession.builder
         .appName("crr-payouts-2025")
         .config("spark.cassandra.connection.host", "cassandra")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

MW_COL = F.lit(1.0)

# ─── 1. load sources ────────────────────────────────────────────────────
crr = spark.read.parquet(CRR_SRC)
lmp = spark.read.parquet(LMP_SRC)

# ─── 2. explode CRR rows into individual UTC hours ──────────────────────
def hours_in_tou(start, end, tou):
    import pandas as pd, pytz
    rng = pd.date_range(start, end, freq="1H", tz="US/Pacific", inclusive="left")
    if tou == "ON":
        rng = rng[(rng.hour >= 7) & (rng.hour < 23)]
    else:                               # OFF
        rng = rng[(rng.hour < 7) | (rng.hour >= 23)]
    return list(rng.tz_convert("UTC"))

hours_udf = F.udf(hours_in_tou, T.ArrayType(T.TimestampType()))

crr_exp = (crr
    .withColumn("hour_utc", F.explode(hours_udf("START_DATE", "END_DATE", "TIME_OF_USE")))
    .withColumn("hour_local", F.from_utc_timestamp("hour_utc", "US/Pacific"))
    .withColumn("hour_ending", (F.hour("hour_local") + 1).cast("int"))
    .withColumnRenamed("APNODE_ID", "apnode_sink")        # sink for first join
    .withColumn("market_name", F.col("MARKET_NAME"))      # rename for PK
    .withColumn("opr_dt", F.to_date("hour_utc"))
    .select("market_name", "opr_dt", "hour_ending",
            "apnode_sink", "TIME_OF_USE", MW_COL.alias("cleared_mw"),
            "hour_utc") )

# ─── 3. prepare LMP price table (only price rows) ───────────────────────
lmp_sel = (lmp
    .filter(F.col("LMP_TYPE") == "LMP")
    .withColumn("hour_utc",
                F.to_timestamp(
                    F.concat_ws(' ', F.col("OPR_DT"),
                                (F.col("OPR_HR")-1).cast("int")),
                    "yyyy-MM-dd H"))
    .select("hour_utc",
            F.col("NODE").alias("apnode_id"),
            F.col("MW").alias("LMP")))

# ── 4a. join for sink price ─────────────────────────────────────────────
sink_join = (crr_exp.alias("c")
    .join(lmp_sel.alias("l"),
          (F.col("c.apnode_sink") == F.col("l.apnode_id")) &
          (F.col("c.hour_utc")    == F.col("l.hour_utc")),
          "left")
    .withColumnRenamed("LMP", "lmp_sink")
    .drop("apnode_id"))

# ── 4b. join again for source price
#       (here we *simulate* source by re-using sink; replace when you have
#       apnode_src column in CRR file)
source_join = (sink_join
    .withColumn("apnode_src", F.col("apnode_sink"))        # TODO real source
    .withColumnRenamed("lmp_sink", "lmp_source"))

# ── 5. metrics & tidy columns ───────────────────────────────────────────
out_df = (source_join
    .withColumn("realized_cong",
                F.col("lmp_sink") - F.col("lmp_source"))
    .withColumn("payout_usd",
                F.col("realized_cong") * F.col("cleared_mw"))
    .withColumn("load_ts", F.current_timestamp())
    .select("market_name", "opr_dt", "hour_ending",
            "time_of_use", "apnode_src", "apnode_sink",
            "cleared_mw", "cleared_price",  # cleared_price not yet present
            "realized_cong", "payout_usd", "load_ts") )

# ─── 6. write to Cassandra ──────────────────────────────────────────────
(out_df
 .write
 .format("org.apache.spark.sql.cassandra")
 .mode("append")
 .option("keyspace", KEYSPACE)
 .option("table", TABLE)
 .save())

spark.stop()
