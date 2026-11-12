"""
gp-01-ingest-bronze
===================

Full extract of the three source tables from RDS SQL Server into S3 bronze as
Parquet, partitioned by ingest_date.

Bronze is a faithful copy: no casting, no filtering, no renaming. Anything that
reshapes the data happens in silver, so a bad transform can always be replayed
from bronze without touching the source database again.

Expected row counts: order_items 203,519 · order_item_options 193,017 · date_dim 365
"""

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Dynamic overwrite replaces only the partitions this run writes. Static mode
# would delete every prior ingest_date on each run.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

CONNECTION = "Jdbc connection"
BUCKET = "s3://global-partners-lake-nh"
INGEST_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

TABLES = ["order_items", "order_item_options", "date_dim"]

for t in TABLES:
    # useConnectionProperties pulls host, credentials and VPC config from the
    # named Glue connection, so no secret is ever written into this script.
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="sqlserver",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION,
            "dbtable": f"dbo.{t}",
        },
        transformation_ctx=f"src_{t}",
    )

    df = dyf.toDF().withColumn("ingest_date", lit(INGEST_DATE))
    n = df.count()
    print(f"[ingest] dbo.{t}: {n} rows -> bronze/{t}/ingest_date={INGEST_DATE}")

    (df.write
       .mode("overwrite")
       .partitionBy("ingest_date")
       .parquet(f"{BUCKET}/bronze/{t}/"))

job.commit()
