"""
gp-02-cleanse-silver
====================

Bronze -> silver: one conformed fact table and four dimensions.

Three things here are not routine and are the reason this job is long:

  * `item_price` is the extended LINE TOTAL, not a unit price, contrary to the
    supplied data dictionary. Revenue is item_price + option_price; multiplying
    by quantity inflates it roughly fivefold. See finding 1 in the design doc.
  * The discount rule the assignment specifies (`option_price < 0`) matches
    nothing in this data. The signal used instead is price variance against each
    item's modal unit price, derived and labelled as such.
  * `dim_date` is generated from rules for 2020-2024, then asserted row-for-row
    against the supplied 2023 CSV. The job fails rather than writes on mismatch,
    so the holiday logic cannot drift silently.

Expected: 202,692 fact rows - SUM(line_revenue) 1,856,502.14 - dim_date parity 365/365
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
BRONZE, SILVER, REJECTS = f"{BUCKET}/bronze", f"{BUCKET}/silver", f"{BUCKET}/rejects"
DEV_APP = "Alltown Fresh - DEVELOPMENT"


def log(m):
    print(f"[silver] {m}")


def blank_to_null(df):
    """SSIS wrote '' where the CSV had nothing. Defensive: a reload would reintroduce it."""
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    return df


def latest_partition(path):
    df = spark.read.parquet(path)
    d = df.selectExpr("max(ingest_date) AS d").first()["d"]
    return df.filter(F.col("ingest_date") == d).drop("ingest_date")


# ---------- 1. READ BRONZE (latest ingest only) ----------
oi_raw = blank_to_null(latest_partition(f"{BRONZE}/order_items/"))
oo_raw = blank_to_null(latest_partition(f"{BRONZE}/order_item_options/"))
dd_raw = blank_to_null(latest_partition(f"{BRONZE}/date_dim/"))
log(f"bronze read: order_items={oi_raw.count()}, options={oo_raw.count()}, date_dim={dd_raw.count()}")

# keys BEFORE the DEVELOPMENT filter, to tell cascade from true orphan
all_keys = oi_raw.filter(F.col("lineitem_id").isNotNull()).select("lineitem_id").distinct()

# ---------- 2. CLEANSE order_items ----------
dev_n = oi_raw.filter(F.col("app_name") == DEV_APP).count()
oi = oi_raw.filter(F.col("app_name") != DEV_APP)
log(f"dropped DEVELOPMENT rows: {dev_n}  (expect 826)")

malformed = oi.filter(F.col("lineitem_id").isNull())
mal_n = malformed.count()
if mal_n:
    (malformed.withColumn("reject_reason", F.lit("null lineitem_id"))
       .write.mode("overwrite").parquet(f"{REJECTS}/order_items_malformed/"))
oi = oi.filter(F.col("lineitem_id").isNotNull())
log(f"quarantined malformed rows: {mal_n}  (expect 1)")

# ---------- 3. TYPE ----------
# Timestamps carry 0-3 fractional digits. regexp_extract returns "" when the fraction
# is absent, which rpad turns into "000" - no array indexing, no ANSI exceptions.
# ".9" means 900ms, so right-padding zeros is semantically correct.
_base = F.regexp_replace(F.col("creation_time_utc"), "Z$", "")
_sec = F.regexp_replace(_base, r"\.\d+$", "")
_frac3 = F.rpad(F.regexp_extract(_base, r"\.(\d+)$", 1), 3, "0")

oi = (oi
  .withColumn("order_ts_utc",
      F.to_timestamp(F.concat(_sec, F.lit("."), _frac3), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
  .withColumn("is_loyalty", F.upper(F.col("is_loyalty")) == "TRUE")
  .withColumn("item_price", F.col("item_price").cast("decimal(12,2)"))
  .withColumn("item_quantity", F.col("item_quantity").cast("int")))

bad_ts = oi.filter(F.col("order_ts_utc").isNull()).count()
log(f"unparseable timestamps: {bad_ts}  (expect 0)")

# ---------- 4. OPTIONS: cascade vs genuine orphan ----------
# Two different problems that look identical in a row count. A cascaded row lost its
# parent because we dropped DEVELOPMENT; a genuine orphan never had one, which is a
# referential integrity defect in the source and belongs in rejects.
live_keys = oi.select("lineitem_id").distinct()
true_orph = oo_raw.join(all_keys, "lineitem_id", "left_anti")  # no parent anywhere
cascaded = oo_raw.join(live_keys, "lineitem_id", "left_anti").join(all_keys, "lineitem_id", "inner")
t_n, c_n = true_orph.count(), cascaded.count()
log(f"option rows dropped: {c_n} cascaded with DEVELOPMENT (expect 266), {t_n} genuine orphans (expect 28)")

if t_n:
    (true_orph.withColumn("reject_reason", F.lit("lineitem_id has no parent in order_items"))
       .write.mode("overwrite").parquet(f"{REJECTS}/options_orphaned/"))

oo = (oo_raw.join(live_keys, "lineitem_id", "inner")
            .withColumn("option_price", F.col("option_price").cast("decimal(12,2)")))
log(f"options retained: {oo.count()}  (expect 192723)")

# ---------- 5. FACT_ORDER_LINE ----------
opt = oo.groupBy("lineitem_id").agg(
        F.sum("option_price").alias("option_revenue"),
        F.count("*").alias("option_count"))

fact = (oi.join(opt, "lineitem_id", "left")
  .withColumn("option_revenue", F.coalesce(F.col("option_revenue"), F.lit(0).cast("decimal(12,2)")))
  .withColumn("option_count", F.coalesce(F.col("option_count"), F.lit(0)))
  # item_price is the LINE TOTAL, not a unit price - see finding 1. Never multiply by quantity.
  .withColumn("line_revenue", F.col("item_price") + F.col("option_revenue"))
  .withColumn("order_date", F.to_date("order_ts_utc"))
  .withColumn("is_guest", F.col("user_id").isNull())
  .withColumn("is_zero_price", F.col("item_price") == 0)
  .withColumn("is_outlier", F.col("line_revenue") > 500))

# ---------- 5b. DISCOUNT SIGNAL ----------
# The assignment specifies detecting discounts via option_price < 0, which finds nothing
# in this data. The derived signal is price variance: a line sold below its item's normal
# unit price. The modal price is taken only from quantity = 1 rows, where item_price is
# unambiguously the unit price (see finding 1).
spec_discounts = oo.filter(F.col("option_price") < 0).count()
log(f"option_price < 0 rows: {spec_discounts}  (expect 0 - the specified rule finds nothing)")

q1 = fact.filter((F.col("item_quantity") == 1) & (F.col("item_price") > 0))
price_counts = q1.groupBy("item_name", "item_price").agg(F.count("*").alias("n"))
w_mode = Window.partitionBy("item_name").orderBy(F.col("n").desc(), F.col("item_price").desc())
modal = (price_counts.withColumn("rk", F.row_number().over(w_mode))
                     .filter(F.col("rk") == 1)
                     .select("item_name", F.col("item_price").alias("modal_unit_price")))
log(f"items with a modal unit price: {modal.count()}  (expect 410)")

# The half-cent tolerance keeps decimal division noise from registering as a discount.
fact = (fact.join(F.broadcast(modal), "item_name", "left")
  .withColumn("unit_price",
      F.when(F.col("item_quantity") > 0, F.col("item_price") / F.col("item_quantity")))
  .withColumn("discount_amount",
      F.when(F.col("modal_unit_price").isNotNull()
             & (F.col("unit_price") < F.col("modal_unit_price") - 0.005),
             ((F.col("modal_unit_price") - F.col("unit_price")) * F.col("item_quantity")).cast("decimal(12,2)"))
       .otherwise(F.lit(0).cast("decimal(12,2)")))
  .withColumn("is_discounted", F.col("discount_amount") > 0))

log(f"discounted lines: {fact.filter(F.col('is_discounted')).count()}  (expect 42031)")
log(f"total discount: {fact.agg(F.sum('discount_amount')).first()[0]}  (expect 46033.81)")
fact.write.mode("overwrite").parquet(f"{SILVER}/fact_order_line/")
log(f"fact_order_line rows: {fact.count()}  (expect 202692)")
log(f"SUM(line_revenue): {fact.agg(F.sum('line_revenue')).first()[0]}  (expect 1856502.14)")

# ---------- 6. DIM_DATE, generated 2020-2024 ----------
# The supplied CSV covers 2023 only, but the fact table spans 2020-2024. Generating from
# rules and proving the generator against the one year we were given is safer than
# extrapolating the CSV, and the proof is section 7.
d = (spark.sql(
        "SELECT explode(sequence(to_date('2020-01-01'), to_date('2024-12-31'), interval 1 day))"
        " AS date_key")
  .withColumn("year", F.year("date_key"))
  .withColumn("month", F.month("date_key"))
  .withColumn("week", F.weekofyear("date_key"))  # ISO-8601, matches source
  .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
  .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
  .withColumn("dom", F.dayofmonth("date_key"))
  .withColumn("nth", F.ceil(F.dayofmonth("date_key") / 7)))

m, dom, nth = F.col("month"), F.col("dom"), F.col("nth")
mon = F.col("day_of_week") == "Monday"
thu = F.col("day_of_week") == "Thursday"

d = d.withColumn("holiday_name",
      F.when((m == 1) & (dom == 1), "New Year's Day")
       .when((m == 1) & mon & (nth == 3), "Martin Luther King Jr.")
       .when((m == 2) & mon & (nth == 3), "Washington's Birthday")
       .when((m == 5) & mon & (dom > 24), "Memorial Day")
       .when((m == 6) & (dom == 19), "Juneteenth")
       .when((m == 7) & (dom == 4), "Indep. Day")
       .when((m == 9) & mon & (nth == 1), "Labor Day")
       .when((m == 10) & mon & (nth == 2), "Columbus Day")
       .when((m == 11) & (dom == 11), "Veterans Day")
       .when((m == 11) & thu & (nth == 4), "Thanksgiving Day")
       .when((m == 12) & (dom == 25), "Christmas"))

# Observed rule as the supplied file demonstrates it: a fixed-date holiday falling on
# Sunday gets a Monday observance. Saturday gets none (Veterans Day 2023 proves this).
w = Window.orderBy("date_key")
d = (d.withColumn("prev_hol", F.lag("holiday_name").over(w))
      .withColumn("prev_dow", F.lag("day_of_week").over(w))
      .withColumn("holiday_name",
          F.when(F.col("holiday_name").isNotNull(), F.col("holiday_name"))
           .when((F.col("prev_dow") == "Sunday") & F.col("prev_hol").isNotNull(),
                 F.concat(F.col("prev_hol"), F.lit(" Observed"))))
      .withColumn("is_holiday", F.col("holiday_name").isNotNull())
      .drop("dom", "nth", "prev_hol", "prev_dow"))

# ---------- 7. PARITY ASSERTION against the supplied 2023 CSV ----------
# exceptAll in both directions, so a row that differs in any column fails rather than
# nets out. The job raises instead of writing: a silent holiday drift would corrupt
# every seasonality metric downstream.
COLS = ["date_key", "year", "month", "week", "day_of_week", "is_weekend", "is_holiday", "holiday_name"]

src = (dd_raw
  .withColumn("date_key", F.to_date("date_key", "dd-MM-yyyy"))
  .withColumn("year", F.col("year").cast("int"))
  .withColumn("month", F.col("month").cast("int"))
  .withColumn("week", F.col("week").cast("int"))
  .withColumn("is_weekend", F.upper(F.col("is_weekend")) == "TRUE")
  .withColumn("is_holiday", F.upper(F.col("is_holiday")) == "TRUE")
  .select(*COLS))

ours = d.filter(F.col("year") == 2023).select(*COLS)

only_ours, only_src = ours.exceptAll(src), src.exceptAll(ours)
n_ours, n_src = only_ours.count(), only_src.count()
if n_ours or n_src:
    log("PARITY FAILED - generated rows absent from source:")
    only_ours.show(20, False)
    log("PARITY FAILED - source rows we did not generate:")
    only_src.show(20, False)
    raise Exception(f"dim_date parity failed: {n_ours} / {n_src} mismatched")
log(f"dim_date parity PASSED: {ours.count()}/365 rows identical to the supplied CSV")

d.coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/dim_date/")
log(f"dim_date rows: {d.count()}  (expect 1827)")

# ---------- 8. DIMENSIONS ----------
# coalesce(1) because each of these is small enough that one file beats 200 of them;
# the read cost of many tiny Parquet objects dominates at this size.
(fact.filter(F.col("user_id").isNotNull()).groupBy("user_id").agg(
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
    F.countDistinct("order_id").alias("orders_lifetime"),
    F.sum("line_revenue").alias("revenue_lifetime"),
    F.max(F.col("is_loyalty").cast("int")).cast("boolean").alias("is_loyalty_ever"))
 .coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/dim_customer/"))

(fact.groupBy("restaurant_id").agg(
    F.countDistinct("order_id").alias("orders_lifetime"),
    F.sum("line_revenue").alias("revenue_lifetime"))
 .coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/dim_restaurant/"))

(fact.filter(F.col("item_name").isNotNull()).groupBy("item_name").agg(
    F.first("item_category").alias("item_category"),
    F.first("modal_unit_price").alias("modal_unit_price"),
    F.count("*").alias("times_sold"))
 .coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/dim_item/"))

log("dimensions written")
job.commit()
