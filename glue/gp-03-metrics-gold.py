"""
gp-03-metrics-gold
==================

Builds gold/fact_customer_clv_daily: one row per customer per day, from that
customer's first order through the last day in the data.

The grain is the point. A sparse table of order days cannot answer "what was
this customer worth on 14 March" without a gap-filling query at read time; a
dense spine answers it with a partition scan. 10.8M rows, ~0.35 GB.

Expected: spine 10,808,918 rows · final-snapshot 20,059 customers ·
final-snapshot SUM(clv_historic) 1,685,422.79
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

BUCKET = "s3://global-partners-lake-nh"
SILVER, GOLD = f"{BUCKET}/silver", f"{BUCKET}/gold"
EPOCH = "2020-01-01"  # day-number origin for the range windows


def log(m):
    print(f"[gold] {m}")


# ---------- 1. SOURCE ----------
fact = spark.read.parquet(f"{SILVER}/fact_order_line/")
dates = (spark.read.parquet(f"{SILVER}/dim_date/")
              .select(F.col("date_key").alias("snapshot_date")))

MAX_DATE = fact.agg(F.max("order_date")).first()[0]
log(f"silver fact rows: {fact.count()}, latest order_date: {MAX_DATE}")

# ---------- 2. CUSTOMER-DAY GRAIN ----------
# Guest orders carry no user_id, so they contribute to revenue but not to CLV.
cd = (fact.filter(F.col("user_id").isNotNull())
        .groupBy("user_id", "order_date")
        .agg(F.sum("line_revenue").alias("rev_day"),
             F.countDistinct("order_id").alias("orders_day"),
             F.count("*").alias("items_day"),
             F.max(F.col("is_loyalty").cast("int")).alias("loyal_day"))
        .withColumnRenamed("user_id", "cd_user")
        .withColumnRenamed("order_date", "cd_date"))
log(f"customer-day rows: {cd.count()}")

cust = (fact.filter(F.col("user_id").isNotNull())
          .groupBy("user_id").agg(F.min("order_date").alias("first_order_date")))

# ---------- 3. THE DENSE SPINE ----------
# Every customer x every day from their first order to the last day in the data.
# dates is 1,402 rows, so broadcasting it keeps the cross join local to each executor.
spine = (cust.crossJoin(F.broadcast(dates.filter(F.col("snapshot_date") <= F.lit(MAX_DATE))))
             .filter(F.col("snapshot_date") >= F.col("first_order_date")))

snap = (spine.join(
            cd,
            (F.col("user_id") == F.col("cd_user")) & (F.col("snapshot_date") == F.col("cd_date")),
            "left")
        .drop("cd_user", "cd_date")
        .withColumn("day_num", F.datediff("snapshot_date", F.lit(EPOCH))))
log(f"spine rows: {snap.count()}  (expect 10808918)")

# ---------- 4. WINDOWS ----------
# The spine is dense, so a 90-day RANGE window covers exactly 90 calendar days
# whether or not the customer ordered on any of them.
wcum = Window.partitionBy("user_id").orderBy("day_num").rowsBetween(Window.unboundedPreceding, 0)
w90 = Window.partitionBy("user_id").orderBy("day_num").rangeBetween(-89, 0)
w90prev = Window.partitionBy("user_id").orderBy("day_num").rangeBetween(-179, -90)
wsnap = Window.partitionBy("snapshot_date")

z_rev = F.coalesce(F.col("rev_day"), F.lit(0).cast("decimal(12,2)"))
z_int = F.coalesce(F.col("orders_day"), F.lit(0))

snap = (snap
  .withColumn("clv_historic", F.sum(z_rev).over(wcum))
  .withColumn("orders_to_date", F.sum(z_int).over(wcum))
  .withColumn("items_to_date", F.sum(F.coalesce(F.col("items_day"), F.lit(0))).over(wcum))
  .withColumn("is_loyalty", F.max(F.coalesce(F.col("loyal_day"), F.lit(0))).over(wcum) == 1)
  .withColumn("last_order_date",
      F.last(F.when(F.col("rev_day").isNotNull(), F.col("snapshot_date")), True).over(wcum))
  .withColumn("clv_delta_1d", z_rev)
  .withColumn("monetary_90d", F.sum(z_rev).over(w90))
  .withColumn("frequency_90d", F.sum(z_int).over(w90))
  .withColumn("monetary_prev90", F.sum(z_rev).over(w90prev)))

# ---------- 5. DERIVED MEASURES ----------
snap = (snap
  .withColumn("tenure_days", F.datediff("snapshot_date", "first_order_date") + 1)
  .withColumn("recency_days", F.datediff("snapshot_date", "last_order_date"))
  .withColumn("avg_order_value",
      F.when(F.col("orders_to_date") > 0, F.col("clv_historic") / F.col("orders_to_date")))
  .withColumn("avg_days_between_orders",
      F.when(F.col("orders_to_date") > 1, F.col("tenure_days") / (F.col("orders_to_date") - 1)))
  .withColumn("spend_trend_pct",
      F.when(F.col("monetary_prev90") > 0,
             (F.col("monetary_90d") - F.col("monetary_prev90")) / F.col("monetary_prev90")))
  .withColumn("churn_risk_flag", F.col("recency_days") > 45))

# ---------- 6. TIERS AND RFM, RANKED WITHIN EACH SNAPSHOT ----------
# Recomputed per day, so a customer's tier moves as the cohort around them changes.
snap = (snap
  .withColumn("clv_pct_rank", F.percent_rank().over(wsnap.orderBy("clv_historic")))
  .withColumn("clv_tier",
      F.when(F.col("clv_pct_rank") >= 0.80, "High")
       .when(F.col("clv_pct_rank") >= 0.20, "Medium")
       .otherwise("Low"))
  # recency ordered descending so the most recent buyers land in ntile 5
  .withColumn("r_score", F.ntile(5).over(wsnap.orderBy(F.col("recency_days").desc())))
  # F and M are lifetime, not trailing-90d: a 90-day window is almost entirely
  # zeros, which collapses onto recency and makes all three scores identical
  .withColumn("f_score", F.ntile(5).over(wsnap.orderBy("orders_to_date")))
  .withColumn("m_score", F.ntile(5).over(wsnap.orderBy("clv_historic")))
  .withColumn("rfm_segment",
      # the one-order special case is tested first, or VIP/Loyal absorb it
      F.when((F.col("orders_to_date") == 1) & (F.col("r_score") >= 4), "New")
       .when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "VIP")
       .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Loyal")
       # lapsed but historically valuable — the segment worth spending on
       .when((F.col("r_score") <= 2) & (F.col("m_score") >= 4), "At Risk")
       # Dormant: most-lapsed quintile AND never valuable. A day threshold here
       # is unreachable — r_score <= 2 already sits entirely past 180 days.
       .when((F.col("r_score") == 1) & (F.col("m_score") <= 2), "Dormant")
       .otherwise("Churn Risk")))

snap = snap.drop("rev_day", "orders_day", "items_day", "loyal_day", "monetary_prev90", "day_num")

# ---------- 7. WRITE ----------
# repartition by the partition column first: without it each of the 200 shuffle
# partitions writes a file into every date folder, giving ~280,000 tiny objects.
(snap.repartition("snapshot_date")
     .write.mode("overwrite").partitionBy("snapshot_date")
     .parquet(f"{GOLD}/fact_customer_clv_daily/"))

# ---------- 8. VERIFY ----------
final = snap.filter(F.col("snapshot_date") == F.lit(MAX_DATE))
log(f"final-snapshot customers: {final.count()}  (expect 20059)")
log(f"final-snapshot SUM(clv_historic): {final.agg(F.sum('clv_historic')).first()[0]}  (expect 1685422.79)")
final.groupBy("clv_tier").count().show()
final.groupBy("rfm_segment").count().orderBy("count", ascending=False).show()

job.commit()
