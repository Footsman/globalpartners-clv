# Global Partners — Business Insights Assessment

## Solution Design & Build Log

Nov 4, 2026 · @Nickolas Herrera

## Assignment at a glance

The deliverable is a daily-refreshed Customer Lifetime Value data product on AWS, fed from SQL Server, transformed only in PySpark, and surfaced in a Streamlit dashboard. CLV is the primary goal; the model must show how each customer's LTV evolves **day by day**, not just a single current total. Everything else (RFM segmentation, churn indicators, sales trends, loyalty impact, location ranking, discount effectiveness) is secondary and must be satisfied by the same model.

### Hard constraints

| Constraint | What it rules out | What it forces |
| --- | --- | --- |
| Source must be SQL Server | Reading the CSVs directly in Glue | CSVs get loaded into SQL Server tables first; the pipeline pulls from the database |
| Entire architecture on AWS | Any third-party platform | Glue, S3, Athena/Redshift, Step Functions or Glue Workflows, KMS, Secrets Manager |
| No new licenses; no Snowflake, DBT or similar | Snowflake, DBT, Fivetran, Airbyte, Databricks | Native AWS services only |
| All logic in PySpark | Pure SQL stored procs, Glue visual-only jobs, pandas | Glue PySpark jobs (Spark SQL inside PySpark is fine) |
| Production-level | A notebook that runs once | Scheduling, encryption, retry and reload-on-failure must be designed in, not bolted on |
| Dashboard in Streamlit | QuickSight | Streamlit app reading the curated layer |

### The seven deliverables

- [ ] Source data loaded into SQL Server and integrity-verified (Step 1)
- [ ] Manual data analysis: schema, relationships, quality issues, join keys (Step 2)
- [ ] Architecture diagram + solution design document + **written SME approval before building** (Step 3)
- [ ] Working pipeline: ingest, transform, store (Step 4)
- [ ] Metrics computed: CLV with High/Medium/Low tiers, RFM, churn flags, sales trends, loyalty comparison, location ranking, discount effect (Step 5)
- [ ] Six Streamlit dashboards, one per business question (Step 6)
- [ ] Submission: code, configs, GitHub CI/CD, short video, and a written justification of *why* each technology was chosen (Step 7)

Step 3 is a gate. The build does not start until the design is approved.

## Source data profile

The three files hold 131,328 orders from 20,174 identifiable customers across 28 restaurants, totalling **$1,864,003** in gross revenue between 21 Apr 2020 and 21 Feb 2024. Row counts match the specification exactly, so no file is truncated. That revenue figure depends on reading `item_price` as the extended line total rather than the unit price the data dictionary describes — see finding 1 in the next section.

| File | Rows | Grain | Key figures |
| --- | --- | --- | --- |
| `order_items` | 203,519 | One row per line item (`lineitem_id` unique, no duplicates) | 131,328 orders · 20,174 users · 28 restaurants · 3 app names · 45 categories · 431 item names · 1.55 items/order |
| `order_item_options` | 193,017 | One row per modifier on a line item | Attaches to 102,697 line items (50.5% of them) · 65,037 priced options · 127,980 free options |
| `date_dim` | 365 | One row per calendar day | Covers 1 Jan – 31 Dec 2023 only |

### Revenue decomposition

| Component | Amount | Share |
| --- | --- | --- |
| Line items, `SUM(item_price)` | $1,778,733.53 | 95.4% |
| Options, `SUM(option_price)` | $85,269.14 | 4.6% |
| **Gross revenue** | **$1,864,002.67** | 100% |
| Attributable to a customer | $1,612,527.59 | 86.5% of gross |

`option_quantity` is 1 on all 193,017 option rows, so options carry no grain ambiguity. They are 4.6% of revenue — small, but the assignment explicitly requires them in the CLV calculation, so they stay in the revenue grain.

Average order value lands at **$14.19**, which is what a café selling $5.99 breakfast sandwiches should produce. The naive `item_price × item_quantity` reading gives $75.64 per order, which is not plausible for this menu and is the strongest independent confirmation that the data dictionary is wrong.

### Distributions worth knowing

| Dimension | Breakdown |
| --- | --- |
| `app_name` | Alltown Fresh 201,423 · Alltown Neighborhood Perks 1,270 · **Alltown Fresh - DEVELOPMENT 826** |
| `is_loyalty` | False 157,435 (77.4%) · True 46,084 (22.6%) |
| Top categories | Breakfast 72,068 · Sandwiches 55,474 · Smoothies 21,152 · Bowls 12,053 · Salads 11,980 |
| `currency` | USD only — no conversion logic needed |
| Item price | median $8.00 · 75th pct $10.00 · max $5,000.00 |
| Item quantity | median 1 · max 500 |

`is_loyalty = True` lines up exactly with the 46,084 rows that carry a `printed_card_number`. The two columns are the same signal, which means the loyalty flag is reliable and no reconciliation is needed between them.

## Data quality findings and the decisions they force

Four findings change the design rather than just the cleansing rules. The data dictionary mislabels `item_price`, the date dimension covers one year out of four, the discount signal the assignment tells us to use does not exist, and 8.8% of line items have no customer attached. Each needs a decision before the architecture is finalised.

| # | Finding | Evidence | Why it matters | Decision needed |
| --- | --- | --- | --- | --- |
| 1 | **`item_price` is the extended line total, not the unit price the dictionary claims** | Of 13,645 rows with quantity > 1, only 47 (0.3%) match the item's modal unit price; 47.8% match unit × quantity exactly. Total deviation from (modal unit × qty) is $172K reading it as a total versus $8.29M reading it as a unit price — a 48× better fit. Average order value is $14.19 as a total, $75.64 as a unit price | Revenue is the base of CLV, the primary metric. `price × quantity` overstates gross revenue by $8.2M on a true base of $1.86M — a 5.6× error that would silently corrupt every downstream number | Revenue = `SUM(item_price)`, not `SUM(item_price × item_quantity)`. Document the contradiction with the supplied dictionary |
| 2 | `date_dim` covers 2023 only, orders span four years | 365 rows, 1 Jan–31 Dec 2023. Orders run 21 Apr 2020 – 21 Feb 2024 across 1,336 distinct dates. **60.4% of line items fall outside the dimension** | An inner join silently deletes 123k rows; a left join leaves `is_holiday` null for most of the history | Regenerate the dimension in PySpark for 2020–2024, or scope the analysis to 2023 |
| 3 | No discounts exist in the data | `option_price < 0` returns **zero rows**. Prices are 127,980 zeros and 65,037 positives. Total discount value $0.00 | Step 5's "Pricing & Discount Effectiveness" has no source signal, and one of the six required dashboards depends on it | Pick a defensible proxy, or build the logic and document the gap |
| 4 | 17,808 line items (8.8%) have no `user_id` | 12,669 orders affected; only 28 carry a loyalty card to fall back on. They hold $251K, or 13.5% of gross revenue | These orders cannot be attributed to a customer, so they belong in revenue totals but not in CLV or RFM | Exclude from customer-grain metrics, keep in order-grain metrics |
| 5 | 826 rows come from a development app | `app_name = 'Alltown Fresh - DEVELOPMENT'` | Test traffic inflating production revenue | Filter at the bronze→silver boundary |
| 6 | Extreme outliers survive even the corrected reading | One `B.L.A.T` line at $2,967 and a `Breakfast Burrito` at $293 against modal prices of $9.99 and $5.89 | A single catering order can dominate a customer's CLV tier | Keep but flag, so the dashboard can toggle them |
| 7 | One malformed row | Null `lineitem_id`, `item_name` and `item_category` on the same record | Breaks the primary key of the fact table | Quarantine to a rejects path |
| 8 | 15 orphan option line items | Option rows whose `lineitem_id` has no parent in `order_items` (0.01%) | Revenue that cannot be allocated to an item | Quarantine and report, do not drop silently |
| 9 | 156 zero-price items, 1 row with quantity ≤ 0 |  | Comped items or voids | Flag, exclude from average-order-value |

### Finding 1, adversarial verification

The first pass used each item's modal price, which is circular: if `item_price` is sometimes extended, the mode is contaminated by those same rows. It also ignored four years of price drift. The claim was therefore re-tested against an oracle built **only from quantity = 1 rows**, where `item_price` is unambiguous because unit and line total are the same number, computed **per item per month** so price changes cannot confound it. 189,717 clean rows built the oracle; 13,645 rows went under test, 94% of them covered.

| Test | Method | Result |
| --- | --- | --- |
| Head-to-head | Same-month oracle from qty=1 rows only: does `price` match the unit price, or unit × quantity? | Extended 8,482 rows, unit price **1 row**. A likelihood ratio of **8,482 : 1** |
| Known price points | Is `price / quantity` a price the item actually sells at? Is raw `price`? | `price / quantity` lands on a known price point **98.8%** of the time; raw `price` only **3.5%** |
| Cents fingerprint | Uses no oracle at all. A $x.99 item priced per unit always ends in .99; extended it must end in (99 × qty) mod 100 | Of 9,568 multi-quantity rows on $x.99 items, **zero** end in .99. Under the dictionary's reading, essentially all would |
| Extreme outliers | Divide the six largest line amounts by quantity, compare to the item's typical price | `Korean Kimchi` $5,000.00 ÷ 500 = **$10.00** against a typical unit price of **$10.00**. Read as a unit price, that one line would be $2.5M |
| Segment uniformity | Does the pattern differ by app, loyalty status or year? A mixed format would need conditional handling | Unit-price matches are **0.0%** in every app, both loyalty values and all five years. The format is uniform, so one global rule is correct |

**The 34% that matched neither reading are also extended**, just against a price point other than that month's median — a different size or variant of the same item. Zero of those 4,344 rows have a `price / (unit × qty)` ratio consistent with a unit price, and their median ratio is 1.17 rather than the 1/quantity a unit price would produce. Test 2 catches them: `price / quantity` is a known price point 98.8% of the time, which covers this bucket almost entirely.

The finding survives every falsification attempt. Revenue is `SUM(item_price)`.

### What is already clean

- `lineitem_id` is unique across all 203,519 rows, so the fact grain is safe.
- No exact duplicate rows.
- `currency` is USD throughout.
- Every timestamp parses; none are null or malformed.
- The options table has no nulls in any column.
- 99.99% of option rows join cleanly to a parent line item.

## Architecture decisions

The stack is RDS SQL Server → Glue PySpark → S3 medallion → Athena → Streamlit, orchestrated by Step Functions on an EventBridge schedule. Every component is a native AWS service, no license is purchased, and all transformation logic is PySpark. The table below is the direct answer to Step 7's requirement to explain *why* behind each choice.

| Layer | Chosen | Why this one | Rejected, and why |
| --- | --- | --- | --- |
| Source database | **RDS for SQL Server Express** | Satisfies "source is SQL Server" with a database Glue can actually reach over VPC. SSMS connects to it from Windows, so the import-wizard workflow from the assignment video still applies. Free-tier eligible | Laptop SQL Server (unreachable from AWS without a tunnel) · EC2 Windows (we'd own patching and backups for no gain) |
| Ingestion | **Glue JDBC connection + PySpark job** | Keeps ingestion inside PySpark as the assignment demands, adds no service to the bill, and Glue job bookmarks give incremental loads for free | DMS (\~$30/mo idle replication instance, and it isn't PySpark). Cited in this doc as the scale-out path for a genuine on-prem source |
| Lake storage | **S3, medallion layout, Parquet + Snappy** | Columnar Parquet cuts Athena scan cost by roughly 10× versus CSV. Bronze/silver/gold separates raw fidelity, cleansing and business logic so a failure is re-runnable from the last good layer | CSV in the lake (scan cost, no schema) · single flat bucket layout (no clean reprocessing boundary) |
| Catalog | **Glue Data Catalog** | Free, and it is the shared metadata layer that Glue jobs, Athena and the crawler all read. Required anyway for Athena to see the tables | Hive metastore on EMR (a cluster to run and pay for) |
| Transform | **Glue 5.0 PySpark jobs** | Serverless Spark with no cluster to size, billed per DPU-second. Spark SQL inside PySpark is still PySpark, so complex window functions stay readable | EMR / EMR Serverless (more control than we need, more to operate) · Glue visual editor (generates code we can't version cleanly) |
| Orchestration | **Step Functions + EventBridge Scheduler** | Per-state `Retry` absorbs transient faults; Redrive from the failed state is the reload mechanism; an EventBridge rule on execution status sends the SNS alert. No `Catch` blocks, because catching an error stops Redrive from working (see the step 9 runbook). Effectively free at this volume, and the execution graph is a good visual for the submission video | Glue Workflows (coarse error handling, no branching) · MWAA (\~$350/mo floor) |
| Serving | **Athena over partitioned gold tables** | Pay-per-scan with zero idle cost. Partitioning gold by `snapshot_date` means a dashboard query touches one day, not four years | Redshift Serverless (8-RPU floor, \~$3/hr while active, easy to leave running) |
| Dashboard | **Streamlit** | Mandated by Step 6 | — |
| Secrets | **AWS Secrets Manager** | SQL Server credentials never appear in job code or in GitHub. Glue connections reference the secret ARN directly | Job parameters or environment variables (credentials end up in CloudWatch logs) |
| Encryption | **SSE-S3 + TLS (as built)** | SSE-S3 default encryption on the lake bucket, RDS storage encrypted with the aws/rds key, credentials in Secrets Manager, TLS on the JDBC connection. A customer-managed KMS key was designed but not adopted: it adds about $1/month plus request charges for audit and revocation control this project does not need. Covers the "encryption" requirement at rest and in transit | SSE-S3 (no key policy, no audit trail of key use) |
| Monitoring | **CloudWatch + SNS** | Job metrics and logs land there by default; SNS turns a failed Step Functions execution into an email | — |
| CI/CD | **GitHub Actions + OIDC to AWS** | Required by Step 7. OIDC means no long-lived AWS keys stored in GitHub secrets. Workflow lints the PySpark, runs unit tests, and syncs scripts to S3 | Static IAM access keys in repo secrets (credential rotation burden and a real leak risk) |

### Cost posture

Run entirely on free tier where possible. The recurring spend is Glue DPU-seconds for the daily run, Athena scans measured in pennies, S3 storage under a gigabyte, and RDS once the 12-month free tier lapses. Nothing in this design idles expensively, which was the deciding factor against DMS, Redshift Serverless and MWAA.

## Target data model

The model is a star schema in silver feeding one wide daily-snapshot fact in gold. `gold.fact_customer_clv_daily` is the primary deliverable: one row per customer per day from their first order to the latest load, 10.8M rows and roughly 0.35 GB in Parquet. Every secondary metric in Step 5 is a column on that table or a small aggregate beside it, so the dashboards never touch silver.

### Decisions locked in

| Decision | Choice |
| --- | --- |
| Snapshot density | Dense daily — a row per customer per day, no carry-forward logic downstream |
| CLV definition | Historic cumulative revenue only, exactly as the how-to steps describe. No predicted column |
| Revenue formula | `SUM(item_price) + SUM(option_price)` — never multiplied by quantity (finding 1) |
| `date_dim` | Regenerated 2020–2024 in PySpark, with a parity assertion against the supplied 2023 CSV |
| Discounts | `option_price < 0` as specified, plus modal-price variance as the derived analytical signal |
| Guest orders | Excluded from customer-grain metrics, retained in order- and location-grain metrics |

### Silver layer

| Table | Grain | Notes |
| --- | --- | --- |
| `fact_order_line` | `lineitem_id` | 203,518 valid rows. Carries `line_revenue` = `item_price` + allocated option revenue, plus quality flags |
| `dim_date` | `date_key` | Generated 2020-01-01 to 2024-12-31, 1,827 rows. ISO week numbering, integer month |
| `dim_customer` | `user_id` | 20,174 rows. First/last order date, loyalty flag, primary restaurant |
| `dim_restaurant` | `restaurant_id` | 28 rows |
| `dim_item` | `item_name` | 431 rows, with modal price for the discount derivation |
| `rejects` | source row | The malformed row, the 15 orphan options, and anything failing a cleansing rule — written, never dropped |

### gold.fact\_customer\_clv\_daily

| Column | Logic |
| --- | --- |
| `snapshot_date` | Partition key. One partition written per daily run |
| `user_id` | Customer |
| `first_order_date`, `last_order_date` | `last_order_date` is the most recent order **on or before** the snapshot |
| `tenure_days` | `snapshot_date - first_order_date + 1` |
| `orders_to_date`, `items_to_date` | Cumulative counts as at the snapshot |
| `clv_historic` | Cumulative `SUM(item_price) + SUM(option_price)` up to the snapshot. **The primary metric** |
| `clv_delta_1d` | Revenue booked on the snapshot date itself — makes the evolution curve chartable directly |
| `avg_order_value` | `clv_historic / orders_to_date` |
| `avg_days_between_orders` | `tenure_days / (orders_to_date - 1)`, null for one-order customers |
| `clv_pct_rank` | `percent_rank()` over that snapshot's cohort |
| `clv_tier` | High ≥ 0.80, Medium 0.20–0.80, Low < 0.20. Recomputed per snapshot, so tiers move over time |
| `recency_days` | `snapshot_date - last_order_date` |
| `frequency_90d`, `monetary_90d` | Trailing 90-day rolling window |
| `r_score`, `f_score`, `m_score` | `ntile(5)` over the snapshot cohort. R from recency\_days, F from orders\_to\_date, M from clv\_historic — lifetime, not the 90-day windows: with most customers inactive those windows are almost entirely zeros, which collapses F and M onto R and makes all three scores identical. |
| `rfm_segment` | Six branches tested in order: New (one order, r>=4); VIP (r>=4, f>=4, m>=4); Loyal (r>=3, f>=3); At Risk (r<=2, m>=4 — lapsed but historically valuable); Dormant (r=1, m<=2); Churn Risk (everything else). Order matters: the one-order case must precede VIP or VIP absorbs it, and Dormant must be defined on the recency quintile rather than a day threshold, since r<=2 already sits entirely past 180 days. |
| `spend_trend_pct` | `monetary_90d` against the prior 90 days |
| `churn_risk_flag` | `recency_days > 45`, per the assignment's threshold |
| `is_loyalty` | Ever held a loyalty card |

### The PySpark shape of the daily snapshot

The dense table is built in four moves, not by looping over dates:

1. Aggregate `fact_order_line` to customer-day — 100,084 rows, one per customer per ordering day.
2. Cross join `dim_customer` to `dim_date` keeping only `date_key >= first_order_date`, which expands to the 10.8M-row spine.
3. Left join the customer-day aggregates onto the spine; non-ordering days arrive null.
4. One window, `partitionBy('user_id').orderBy('snapshot_date').rowsBetween(unboundedPreceding, 0)`, carries `sum()` for cumulative revenue and `last(ignoreNulls=True)` for `last_order_date`. The percentile tiers then use a second window partitioned by `snapshot_date`.

### Why partition gold by `snapshot_date`

A daily run writes exactly one new partition and never rewrites an old one, which is what makes the reload mechanism cheap: a failed or bad day is fixed by overwriting that single partition. The tradeoff is 1,402 partitions averaging roughly 250 KB, which is below the ideal Parquet file size. If scan latency ever matters, a monthly compaction job is the answer, not a coarser partition key.

## Architecture diagram

&#91;embedded content: end-to-end architecture · 3 Glue jobs, 3 lake layers, 1 orchestrator\]

Data moves down the left-hand column of jobs and across into the lake layer each one writes. Step Functions drives all three jobs on a single daily execution, so a failure in cleansing never leaves gold half-written. Only the gold layer is read by Athena, which keeps dashboard queries off the raw data entirely.

## Production concerns

The assignment names three explicitly — scheduling, encryption, and a failure reload mechanism. Each maps to a specific console configuration rather than to code, which keeps the whole thing reviewable.

### Scheduling

EventBridge Scheduler fires the Step Functions state machine daily at 02:00 UTC, late enough that the previous day is closed in every US timezone the restaurants operate in. The state machine runs the three Glue jobs in sequence, each waiting on the previous one's success. A `snapshot_date` parameter is passed in at the top so the same machine can be invoked by hand for any historical date — which is also how the backfill runs.

### Encryption

| Where | Control |
| --- | --- |
| S3, all three layers | SSE-S3 default encryption, versioning enabled. Not applied: a bucket policy denying unencrypted `PutObject` |
| Glue job bookmarks and CloudWatch logs | Glue security configuration referencing the same key |
| SQL Server credentials | Secrets Manager, referenced by ARN from the Glue connection. Never in code, parameters or GitHub |
| In transit | TLS on the JDBC connection, HTTPS on every AWS API call |
| Athena query results | SSE-S3, inherited from the lake bucket (results live under athena-results/) |

One customer-managed key rather than SSE-S3, so key use is auditable in CloudTrail and access can be revoked at the key policy without touching the buckets.

### Failure handling and reload

The layered design is what makes reload cheap. Bronze is immutable and partitioned by `ingest_date`, so it is always replayable; silver and gold are deterministic functions of the layer above them.

| Failure | Response |
| --- | --- |
| Transient Glue or JDBC error | Step Functions `Retry`, 3 attempts with exponential backoff, on every state |
| Job fails after retries | An EventBridge rule on `FAILED`, `TIMED_OUT` or `ABORTED` execution status emails the SNS topic with the execution name and ARN; the run is then resumed with Redrive |
| Bad data for one day | Re-invoke the state machine with that `snapshot_date`; dynamic partition overwrite replaces exactly that partition |
| Logic change needing full rebuild | Re-run silver and gold from bronze; no re-extract from SQL Server, so the source database is never hit twice for the same data |
| Partial write | Glue writes to a staging prefix and the job only commits the partition swap on success, so a killed job cannot leave a half-written partition |
| Source row fails a rule | Written to `silver.rejects` with the rule that rejected it. The job succeeds; the rejects count is a CloudWatch metric with an alarm |

The last row matters for review: a pipeline that silently drops bad rows looks identical to a pipeline that has no bad rows. Quarantining makes the difference visible.

### Cost guardrails

A billing alarm at $10/month, S3 lifecycle rules expiring bronze after 90 days, Athena workgroup with a per-query data scan limit, and the RDS instance on the smallest free-tier class. Nothing in the design idles expensively, which was the deciding factor against DMS, Redshift Serverless and MWAA.

## Build log

Steps 1, 2 and the first half of 4 are complete: the source database is loaded and a Glue PySpark job lands all three tables in bronze. Step 3 is drafted and still awaiting SME sign-off. The AWS footprint is one RDS instance, one S3 bucket, two VPC endpoints and one Glue job.

| Date | Step | What happened |
| --- | --- | --- |
| 10 Nov 2026 | Step 4 — bronze ingest succeeded | `gp-01-ingest-bronze` ran in 83 seconds on 2 G.1X workers, landing all three tables as partitioned Parquet. Seven failed runs preceded it, each exposing a distinct VPC or credential gap |
| 10 Nov 2026 | Step 4 — Secrets Manager endpoint | Added after `Network is unreachable`. Glue workers have no public IP and reach no AWS API without an endpoint. \~$7.30/month |
| 10 Nov 2026 | Step 4 — IAM role and S3 gateway endpoint | `GlueServiceRole-globalpartners` with `AWSGlueServiceRole` plus a scoped inline policy for the bucket and secret |
| 9 Nov 2026 | Step 4 — Glue connection created | JDBC to RDS using Secrets Manager credentials. The console's Test connection never succeeded, failing client-side with `NetworkingError` in two browsers; the working job proved the connection instead |
| 9 Nov 2026 | Step 2 — all three tables loaded | 203,519 / 193,017 / 365 rows, matching the specification. Empty strings normalised to NULL across six columns. Seven distinct wizard traps hit and documented |
| 9 Nov 2026 | Step 3 — S3 bucket | `global-partners-lake-nh` created in us-east-1 with six prefixes and versioning enabled |
| 9 Nov 2026 | Step 2 — RDS provisioned | `globalpartners`, SQL Server Express 2019, db.t3.micro, 20 GiB gp3, us-east-1a. Storage cut from the 200 GiB default, taking the estimate from $39.06 to $18.36/month |
| 5 Nov 2026 | Step 2 — finding 1 verified adversarially | Re-tested against an oracle built only from quantity = 1 rows. Likelihood ratio 8,482 : 1; zero of 9,568 multi-quantity rows on $x.99 items end in .99 |
| 4 Nov 2026 | Step 3 — design | Architecture, data model and production controls drafted. Six design decisions settled: RDS SQL Server, Glue JDBC, Step Functions, Athena, dense daily snapshot, historic-only CLV |
| 4 Nov 2026 | Step 2 — `date_dim` rules reverse-engineered | All 8 columns reproduced from calendar rules: ISO week numbering, integer month, 11 federal holidays. Supplied CSV promoted from source to test oracle |
| 4 Nov 2026 | Step 2 — finding 1 | `item_price` proved to be the extended line total, not the unit price. Gross revenue corrected from $9.93M to $1.86M; average order value $14.19 |
| 4 Nov 2026 | Step 2 — quality analysis | Nine findings logged. Three were blocking design decisions, six were cleansing rules |
| 4 Nov 2026 | Step 1 — integrity check | 203,519 / 193,017 / 365 rows, matching the specification. `lineitem_id` unique, no duplicate rows, all timestamps parse, USD only |

### Sequence from here

Each step below lands in this log as it completes, with the console path used and the check that proves it worked.

1. **SME review and written approval.** Export this document, walk the diagram, get sign-off in writing. Nothing below starts until this is done.
2. **RDS for SQL Server Express.** Console → RDS → Create database → SQL Server Express, db.t3.micro, free tier. Connect SSMS from Windows, create the `globalpartners` database, load all three CSVs with the Import Flat File wizard. *Verify:* `SELECT COUNT(*)` returns 203,519 / 193,017 / 365. Pin `date_key` as `DD-MM-YYYY` during import.
3. **S3.** One bucket with `bronze/`, `silver/`, `gold/`, `rejects/` and `scripts/` prefixes. SSE-S3 default encryption and versioning. *Verify:* an unencrypted `PutObject` is stored encrypted anyway.
4. **Secrets Manager and the Glue connection.** Store the RDS credentials, create a Glue JDBC connection referencing the secret. *Verify:* the connection's built-in test succeeds.
5. **Glue job 1, ingest.** Reads three tables over JDBC, writes bronze Parquet partitioned by `ingest_date`. *Verify:* bronze row counts match SQL Server exactly.
6. **Glue job 2, cleanse.** Types, the corrected revenue formula, quarantine rules, and the `dim_date` generator with its 365-row parity assertion. *Verify:* the assertion passes and `rejects` holds exactly the 16 known bad rows.
7. **Glue job 3, metrics.** Builds `fact_customer_clv_daily` and the aggregates. *Verify:* total `clv_historic` on the final snapshot equals $1,612,527.59, and the tier split is 20/60/20.
8. **Crawler and Athena.** Register gold, run the dashboard queries by hand once. *Verify:* partition pruning shows in the query's scanned-bytes figure.
9. **Step Functions and EventBridge.** Wire the three jobs with Retry and no Catch, add the EventBridge failure-alert rule, and create the 02:00 UTC schedule in a disabled state, since the source data never changes. *Verified 11 Nov:* a deliberate failure sent the alert email, and Redrive resumed at the failed state in 338 ms.
10. **Backfill.** Invoke the state machine across 2020–2024. *Verify:* 1,402 partitions present.
11. **Streamlit, six dashboards.** One per business question in Step 6.
12. **GitHub and CI/CD.** Repo, Actions workflow with OIDC, lint and unit tests, sync scripts to S3 on merge.
13. **Submission.** Code, configs, dashboard, this document, and the short explainer video.

### Runbook: step 2, RDS for SQL Server Express

Everything in this project lives in **us-east-1**. S3, Glue, Athena and Step Functions must all be created in that same region or the Glue connection will not see the database.

**A. Create the instance.** Console → RDS → Databases → Create database.

| Setting | Value | Why |
| --- | --- | --- |
| Creation method | Standard create | Easy create hides the networking we need |
| Engine | Microsoft SQL Server → **SQL Server Express Edition** | Express is the only free-tier-eligible edition and needs no license |
| Template | **Free tier** | Auto-selects Single-AZ and the right instance class. If this template is missing, the account is past its 12-month window — see the cost note |
| DB instance identifier | `globalpartners-sqlserver` |  |
| Master username | `gpadmin` | RDS rejects `admin`, `sa`, `root`, `administrator`, `guest` and `public` |
| Master password | 8–128 chars | SQL Server rejects `/`, `"`, `@` and spaces in the password. Save it now |
| Instance class | `db.t3.micro` |  |
| Storage | 20 GiB gp3** — change it, the console defaults to 200** | 200 GiB costs $23.00/month to hold 60 MB. Express caps a database at 10 GB anyway, and on gp3 the 3,000 IOPS baseline applies from 20 GiB up, so nothing is lost |
| Public access | **Yes** | Lets SSMS connect from your laptop. Glue still reaches it privately — inside the VPC the endpoint resolves to the private IP |
| VPC security group | Create new: `gp-sqlserver-sg` |  |
| Performance Insights | **Off** |  |
| Enhanced monitoring | **Off** |  |
| Encryption | **Enabled**, `aws/rds` key | Satisfies encryption at rest. The dedicated CMK covers S3 |
| Backup retention | 1 day |  |
| Deletion protection | Off | Keeps teardown easy |

There is **no "initial database name" field for SQL Server** — that field only appears for other engines. The database gets created by hand in step D. Provisioning takes 10–15 minutes; wait for status **Available**, then copy the endpoint from the Connectivity & security tab.

**B. Open the firewall.** EC2 → Security Groups → `gp-sqlserver-sg` → Inbound rules → Edit. Add both now to save a second trip:

1. Type `MSSQL`, port 1433, source **My IP** — lets SSMS in.
2. Type **All TCP**, source **this same security group** (paste its own `sg-` id) — Glue requires a self-referencing rule or the connection test fails with a timeout that names nothing useful.

**C. Connect SSMS.** Server name `<endpoint>,1433`, SQL Server Authentication, login `gpadmin`. **Tick "Trust server certificate".** SSMS 20 and 21 default to mandatory encryption and fail with a certificate-chain error without it — this is the single most common first-connect failure.

**D. Create the database and tables in the GUI.** No scripts — every step here has a dialog.

1. Right-click **Databases** → **New Database…** → name it `globalpartners` → OK.
2. Expand `globalpartners`, right-click **Tables** → **New** → **Table…**, type the columns into the designer grid, then Ctrl+S and enter the table name.

Three columns stay as text on purpose: `creation_time_utc`, `date_key` and the TRUE/FALSE flags. Parsing them belongs in PySpark anyway — the assignment requires all logic there — and it removes the only two conversions the import wizard could silently get wrong. Everything the wizard handles safely is typed properly.

`order_items`

| Column Name | Data Type | Allow Nulls |
| --- | --- | --- |
| app\_name | nvarchar(50) | no |
| restaurant\_id | nvarchar(32) | no |
| creation\_time\_utc | nvarchar(32) | no |
| order\_id | nvarchar(32) | no |
| user\_id | nvarchar(32) | **yes** |
| printed\_card\_number | nvarchar(32) | **yes** |
| is\_loyalty | nvarchar(5) | no |
| currency | nvarchar(3) | no |
| lineitem\_id | nvarchar(32) | **yes** |
| item\_category | nvarchar(128) | **yes** |
| item\_name | nvarchar(128) | **yes** |
| item\_price | decimal(12, 2) | no |
| item\_quantity | int | no |

`order_item_options`

| Column Name | Data Type | Allow Nulls |
| --- | --- | --- |
| order\_id | nvarchar(32) | no |
| lineitem\_id | nvarchar(32) | no |
| option\_group\_name | nvarchar(64) | no |
| option\_name | nvarchar(128) | no |
| option\_price | decimal(12, 2) | no |
| option\_quantity | int | no |

`date_dim`

| Column Name | Data Type | Allow Nulls |
| --- | --- | --- |
| date\_key | nvarchar(10) | no |
| year | int | no |
| month | int | no |
| week | int | no |
| day\_of\_week | nvarchar(10) | no |
| is\_weekend | nvarchar(5) | no |
| is\_holiday | nvarchar(5) | no |
| holiday\_name | nvarchar(40) | **yes** |

The four nullable columns in `order_items` matter: 17,808 rows have no `user_id`, 157,435 have no card number, and one malformed row has no `lineitem_id`, `item_category` or `item_name`. Leaving those as NOT NULL makes the import fail partway through.

**Prerequisite on SSMS 22.** The Import and Export Wizard needs SSIS client components, which ship only with the **Business Intelligence** workload in the SSMS installer. Without it the Tasks → Import Data menu item appears but is greyed out, and the only fix is to reopen the Visual Studio Installer and modify the installation. The "Hybrid and Migration" workload offered beside it is for Azure SQL migration assessment and is not needed.

**E. Import the CSVs.** Right-click `globalpartners` → **Tasks** → **Import Data…** (the Import and Export Wizard). Not "Import Flat File" — that one builds its own table and guesses types, which defeats the tables just made by hand.

Run it three times, once per file. Data source **Flat File Source**, destination **Microsoft OLE DB Driver for SQL Server** pointing at `globalpartners`, and on the Select Source Tables and Views page map each file to the matching table. Two settings on the source page decide whether the data arrives correct:

- **Text qualifier `"`** — without it `"Add Meat, Substitute Tofu"` splits on its comma and shifts every later column on that row.
- **Code page 65001 (UTF-8)** — without it the 102 cells containing `Jalapeño` arrive as mojibake.

### Import wizard: the six things that break it

Every one of these was hit on the first file. Worth setting correctly in a single forward pass, because the wizard resets manual choices whenever you navigate **Back**.

| Problem | Symptom | Fix |
| --- | --- | --- |
| Line endings differ per file | `The specified header or data row delimiter "{CR}{LF}" is not found after scanning 65536 bytes` | `order_items` and `order_item_options` are **LF only**; set both Header row delimiter and Row delimiter to `{LF}`. `date_dim` really is CRLF — leave it |
| Source column width defaults to 50 | `Text was truncated... data row 676`, on ITEM\_CATEGORY | On the Advanced page set `OutputColumnWidth` to 128 for **ITEM\_CATEGORY** (max 91) and **OPTION\_NAME** (max 67). No other column across the three files exceeds 50 |
| RDS certificate not trusted | `SSL Provider: The certificate chain was issued by an authority that is not trusted` | In Data Link Properties → **All** tab, set `Trust Server Certificate` = True. Amazon's RDS root CA is not in the Windows trust store |
| Back navigation mangles the destination | `Invalid object name 'dboorder_items'` — the dot is gone | Re-pick `[dbo].[order_items]` from the dropdown rather than typing it |
| Back navigation resets manual mappings | `Cannot insert the value NULL into column 'resturant_id'` | Make the table's column names match the CSV headers exactly, so the wizard auto-maps by name and no manual mapping is needed |
| Text qualifier defaults to `<none>` | No error — 426 rows silently shift a column | Set Text qualifier to `"` |

**Why the column-name typos mattered more than they looked.** `restaurant_id` was misspelled `resturant_id` and `user_id` was `uder_id`, so neither auto-mapped. The restaurant column is NOT NULL, so it failed loudly. `user_id` is nullable by design, so an unmapped USER\_ID would have loaded 203,519 NULL customer IDs with no error at all, and the loss would only have surfaced later as an empty CLV. The NOT NULL constraint on a column that genuinely has no nulls is what turned a silent corruption into a visible failure.

**The seventh trap, and the quietest: SSIS writes empty strings, not NULLs.** The flat file source converts an empty CSV field to `''`, and the wizard exposes no option to change it. The import therefore succeeds, the row count is exactly right, and every guest order carries `user_id = ''` instead of NULL. That single empty string counts as its own customer, so the distinct-customer check reads 20,175 rather than 20,174 — a one-row discrepancy that would have made all 12,669 guest orders collapse into one fake account ranked as the highest-value customer in the business.

Nothing about the import flags it. It was caught only because the acceptance query compared distinct customers and null counts against figures established during profiling. The fix is to normalise after each load:

```sql
UPDATE dbo.order_items SET user_id             = NULL WHERE user_id = '';
UPDATE dbo.order_items SET printed_card_number = NULL WHERE printed_card_number = '';
UPDATE dbo.order_items SET lineitem_id         = NULL WHERE lineitem_id = '';
UPDATE dbo.order_items SET item_category       = NULL WHERE item_category = '';
UPDATE dbo.order_items SET item_name           = NULL WHERE item_name = '';
UPDATE dbo.date_dim    SET holiday_name        = NULL WHERE holiday_name = '';
```

Run on 9 Nov, these affected 17,808 / 157,435 / 1 / 1 / 1 rows respectively — matching the profiled null counts exactly. `order_item_options` has no nullable columns and needs no treatment. The silver PySpark job should apply the same normalisation defensively, since a future reload would reintroduce the empty strings.

### Runbook: step 4, Secrets Manager and the Glue connection

**The secret.** Secrets Manager → Store a new secret → Credentials for Amazon RDS database → pick the `globalpartners` instance → name it `globalpartners/rds/admin`. The user name must be the SQL Server master login, **`admin`** — not the AWS account email the form may prefill.

**The connection.** Glue → Data connections → Create connection → JDBC.

| Field | Value |
| --- | --- |
| JDBC URL | `jdbc:sqlserver://<endpoint>:1433;databaseName=globalpartners` — the full URI, not just the hostname |
| JDBC Driver Class name | empty |
| JDBC Driver S3 Path | empty — that field is for custom drivers; Glue ships a SQL Server driver |
| Credential type | AWS Secrets Manager → `globalpartners/rds/admin` |
| VPC / Subnet | the RDS VPC, a subnet in the same AZ as the instance |
| Security groups | `gp-sqlserver-sg` |

**Three prerequisites the console does not create for you**, each of which fails the connection test in a way that points somewhere misleading:

1. **A self-referencing inbound rule.** `gp-sqlserver-sg` needs Type `All TCP` with the security group's own id as Source. RDS auto-creates only the rule for your own IP, so this one is always missing. Glue's ENIs live in that group and must reach both each other and the database.
2. **An S3 Gateway VPC endpoint.** Glue ENIs get no public IP, so they cannot use the VPC's internet gateway. Without the endpoint the test fails with `Could not find S3 endpoint or NAT gateway for subnetId`. VPC → Endpoints → Create → `com.amazonaws.us-east-1.s3`, **Gateway type**, attached to the subnet's route table. Gateway is free; the Interface variant of the same service costs about $7/month and is not needed.
3. **An IAM role for Glue.** Trusted entity Glue, managed policy `AWSGlueServiceRole`, plus an inline policy granting `s3:GetObject/PutObject/DeleteObject/ListBucket` on the lake bucket and `secretsmanager:GetSecretValue` on the secret. `AWSGlueServiceRole` alone covers only buckets named `aws-glue-*`, and the secret ARN needs a trailing `*` because Secrets Manager appends six random characters.

### Runbook: step 5, the bronze ingest job

First successful run 10 Nov, 83 seconds on 2 G.1X workers, 0.046 DPU-hours. Glue → ETL jobs → **Script editor** (not Visual ETL, which is a drag-and-drop canvas with no place to paste PySpark).

| Setting | Value |
| --- | --- |
| Name | `gp-01-ingest-bronze` |
| IAM role | `GlueServiceRole-globalpartners` |
| Glue version | 6.0 |
| Worker type / count | G.1X, 2 |
| Job bookmark | Disable — bronze is a full reload each run |
| Retries / timeout | 1 / 10 minutes |
| **Advanced properties → Connections** | **`Jdbc connection`** — without this the job runs outside the VPC and cannot reach RDS |

```python
import sys
from datetime import datetime, timezone
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Re-running a date replaces only that partition, not the whole table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

CONNECTION  = "Jdbc connection"
BUCKET      = "s3://global-partners-lake-nh"
INGEST_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

TABLES = ["order_items", "order_item_options", "date_dim"]

for t in TABLES:
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
    n  = df.count()
    print(f"[ingest] dbo.{t}: {n} rows -> bronze/{t}/ingest_date={INGEST_DATE}")

    (df.write
       .mode("overwrite")
       .partitionBy("ingest_date")
       .parquet(f"{BUCKET}/bronze/{t}/"))

job.commit()
```

No credential appears anywhere in the script. `useConnectionProperties` with `connectionName` makes Glue resolve the JDBC URL and fetch the password from Secrets Manager at runtime using the job's IAM role. `partitionOverwriteMode = dynamic` is what makes the reload mechanism cheap — without it, `mode("overwrite")` would wipe the entire table prefix rather than one date.

**Verification:** the three printed row counts must read 203,519 / 193,017 / 365, and `bronze/order_items/ingest_date=<date>/` must hold non-empty Parquet files.

### What VPC-resident Glue actually costs to connect

The single biggest surprise in the build, and worth presenting to the SME because it changes both the diagram and the running cost. **Glue workers placed in a VPC get no public IP address.** They therefore cannot use the VPC's internet gateway, and every AWS API the job calls needs an explicit route. The failures arrived one layer at a time, each one revealing the next:

| Error seen | What it meant | Resolution |
| --- | --- | --- |
| `Could not find S3 endpoint or NAT gateway for subnetId` | Glue validates S3 reachability before it will even save a connection test | S3 **Gateway** endpoint — free |
| `Role ... should be given assume role permissions for Glue Service` | The job was still pointed at an auto-generated role with a broken trust policy | Select the purpose-built role, then **Save** — the dropdown alone does not persist |
| `Unable to execute HTTP request: Network is unreachable` | The workers could not reach Secrets Manager to resolve the database password | Secrets Manager **Interface** endpoint — \~$7.30/month |
| `Login failed for user 'polonick13@gmail.com'` | Everything networked correctly; the secret held the AWS account email rather than the SQL Server login | Edit the secret's `username` to `admin` |

That last error is the useful one to show a reviewer: it is a SQL Server error, which proves the full path — VPC routing, security group, Secrets Manager retrieval and the JDBC handshake — was already working.

**The architectural point.** "Use Secrets Manager" is free to say in a design document and costs an interface endpoint to do from inside a VPC. The alternatives are a NAT gateway at roughly $32/month, which covers every service at once, or dropping the VPC entirely and exposing the database publicly, which is not acceptable. One interface endpoint was the cheapest honest answer here; a production estate calling more AWS services would cross over to the NAT gateway quickly.

Both endpoints bill whether or not a job is running, so they belong on the teardown list beside the RDS instance.

### Silver acceptance numbers

Computed from the source data before building job 2, so the job's printed counts can be checked against them rather than trusted.

| Check | Expected | Job 2 actual |
| --- | --- | --- |
| Bronze `order_items` rows | 203,519 | 203,519 |
| Less DEVELOPMENT app | −826 | 826 |
| Less malformed (null `lineitem_id`) | −1 → **202,692** | 202,692 |
| Unparseable timestamps | 0 | 0 |
| Option rows cascaded with the DEVELOPMENT filter | −266 | 266 |
| Option rows genuinely orphaned | −28 | 28 |
| Option rows retained | **192,723** | 192,723 |
| `SUM(line_revenue)` | **$1,856,502.14** | $1,856,502.14 |
| `dim_date` generated rows | 1,827 | 1,827 |
| `dim_date` 2023 parity against the supplied CSV | 365/365 | **PASSED** |
| `dim_customer` | 20,059 |  |
| `dim_restaurant` | 27 (was 28 — the test app had its own) |  |
| `dim_item` | 420 (was 431) |  |
| Distinct orders | 130,587 |  |
| Guest lines (null `user_id`) | 17,502 |  |

**Two units, easily confused.** Measured against the cleansed fact table, 294 option rows have no parent. Of those, 266 belong to DEVELOPMENT line items and cascade with that filter; only 28 are genuine integrity failures. Those 28 rows sit on just **15 distinct `lineitem_id` values** — the figure the original profiling reported. Quoting the distinct-ID count where a row count was needed caused a false alarm when job 2 first ran, which is a good argument for stating the unit in every acceptance number.

A job that quarantined all 294 would overstate the data-quality problem tenfold, so the cascade has to be handled explicitly rather than lumped in with true orphans.

### Glue 6.0 runs strict ANSI SQL, and it changes how you write PySpark

Job 2 failed twice on the same underlying cause before succeeding. Glue 6.0 ships Spark 4.1, which enforces ANSI semantics by default: operations that returned NULL quietly on older Spark now raise and abort the job.

| Failure | Cause | Fix |
| --- | --- | --- |
| `CANNOT_PARSE_TIMESTAMP` on `'2023-06-14T11:07:55'` | `to_timestamp` with a fixed `.SSS` format. The data carries **variable** fractional precision: 183,348 rows with 3 digits, 18,131 with 2, 1,853 with 1 and 187 with none. One mismatch aborts the whole job rather than yielding a null | Normalise the fraction to exactly three digits before a single parse |
| `INVALID_ARRAY_INDEX` | `split(ts, '.').getItem(1)` on the 187 rows containing no `.` — index 1 of a 1-element array | `regexp_extract`, which returns `""` when there is no match and so never indexes out of bounds |

The general lesson for the SME write-up: under ANSI mode a transformation must be total over its input, not merely correct for the common case. The 187 rows without milliseconds are 0.09% of the data and would have passed unnoticed under permissive parsing — pandas read them without complaint during profiling, which is exactly why the earlier "every timestamp parses" note was true but misleading.

Timing: job 2 runs in 6 minutes 36 seconds on 2 G.1X workers, 0.22 DPU-hours — noticeably longer than ingest because of the `dim_date` window function and the parity comparison.

### Runbook: step 7, the gold CLV snapshot

Built 10 Nov in 6 minutes 8 seconds on 5 G.1X workers, 0.495 DPU-hours, no retries. `gp-03-metrics-gold`, Glue 6.0, timeout 30, retries 1, bookmark Disable, no connection attached.

| Check | Expected | Actual |
| --- | --- | --- |
| Snapshot rows | 10,808,918 |  |
| Distinct customers | 20,059 |  |
| Partitions (`snapshot_date`) | 1,402 |  |
| `SUM(clv_historic)` on the final snapshot | $1,685,422.79 |  |
| Tier split, final snapshot | ≈4,011 / 12,035 / 4,011 |  |

That revenue figure is **attributable** revenue — the $1,856,502.14 in silver less the 17,502 guest lines that carry no `user_id`. Guest orders count toward sales and location metrics but cannot belong to a customer, so they are absent from CLV by design.

**Three techniques carry this job.**

The dense spine is produced by cross-joining 20,059 customers against the 1,402-row date dimension and filtering to days on or after each customer's first order. Broadcasting the date side keeps the expansion local to each executor, so 10.8M rows materialise without a shuffle. No loop over dates is needed.

The rolling windows use `rangeBetween(-89, 0)` over a day-number column rather than `rowsBetween`, so they count calendar days rather than rows. Because the spine is dense — one row per customer per day whether or not they ordered — the two happen to coincide. That equivalence is the main practical payoff of the dense design: on a sparse table, a trailing-90-day measure needs an awkward self-join.

Tiers are ranked **within each snapshot date** via `percent_rank()` partitioned by `snapshot_date`, so a customer's High/Medium/Low standing moves over time as the cohort around them grows and spends. A single lifetime tier would not answer the assignment's question.

**Writing needs `repartition("snapshot_date")` before `partitionBy("snapshot_date")`.** Without it each of the 200 shuffle partitions writes a file into every date folder — roughly 280,000 objects. Hash-repartitioning first puts each date on exactly one task, giving 1,402 files. The same trap had already appeared in silver, where partitioning a 202,692-row table by `order_date` produced about 26,000 tiny files and tripled the job's runtime; there the fix was to drop partitioning altogether, since the table is 17 MB and job 3 reads all of it.

**The query that demonstrates the primary requirement:** select one customer from `fact_customer_clv_daily` filtered to `clv_delta_1d > 0` and ordered by `snapshot_date`. It returns their lifetime as a series of steps — cumulative CLV climbing, recency resetting to zero at each order, tier and RFM segment shifting — which is "how LTV evolves for each customer daily" shown rather than asserted.

### Runbook: step 9, orchestration — and why catching errors broke the reload mechanism

`gp-daily-clv-pipeline` runs the three Glue jobs in sequence via `arn:aws:states:::glue:startJobRun.sync`. The `.sync` suffix is what makes the chain work: plain `startJobRun` fires a job and moves on immediately, so all three would start at once and cleanse would read a half-written bronze layer. With `.sync`, Step Functions polls until each job finishes and fails the state if the job fails.

**The design mistake, found by testing.** The first version put a `Catch` on every state routing to an SNS publish and then a `Fail` state. It was tested deliberately by running before granting `glue:StartCrawler`, and the alerting worked perfectly — the email named the exact IAM action, resource and request ID, which identified the fix with no log-digging.

But **Redrive then did nothing**. Redrive resumes from the state where the execution *terminated*, and catching the error meant termination happened at the `Fail` state rather than at the failed task. A `Fail` state fails every time, so two redrives completed in 40 milliseconds with the execution still failed. The console warns about this: *"Redriving from a Fail state will cause execution to fail again."*

**The general principle:** a `Catch` converts an unhandled failure into a handled one, and Redrive only rescues unhandled failures. Rich in-workflow alerting and working Redrive are mutually exclusive.

**Resolution.** The `Catch` blocks were removed so task failures stay unhandled and Redrive resumes from the failed state in seconds rather than re-running eleven minutes of work. Alerting moved outside the state machine to an EventBridge rule on `Step Functions Execution Status Change`, targeting the same SNS topic with an input transformer for readability. That rule also catches `TIMED_OUT` and `ABORTED`, which the `Catch` blocks never covered — a job that hangs past its timeout is as much an incident as one that errors.

The `Retry` blocks stayed: 2 attempts with exponential backoff on concurrency and internal-service errors, so a transient fault never reaches the alert at all.

**Scheduling.** EventBridge Scheduler fires the state machine daily at 02:00 UTC with scheduler-level retries set to zero — Step Functions handles retries internally, and a scheduler retry would start a second concurrent execution of the same pipeline.

### Known scaling limit of the ingest job

The JDBC read is single-threaded. Without `hashfield` and `hashpartitions` in the connection options, Spark pulls each table through one connection on one executor. At 203,519 rows this is irrelevant — the whole job runs in 83 seconds — but it would be the bottleneck at a hundred times the volume, and the fix is to partition the read on an indexed column. Worth stating as a known limit rather than leaving a reviewer to find it.

A second, smaller inefficiency: the writer produces 20 output files per table regardless of size, so `date_dim` lands as 20 files of 2.7 KB. Harmless at this scale; `coalesce(1)` on the small dimensions would be the tidy fix.

**F. Verify in the GUI, then with four typed queries.** Row counts need no SQL at all: right-click a table → **Properties** → **Storage** shows Row Count. Expect 203,519 / 193,017 / 365.

The rest is worth typing by hand in a new query window, one at a time:

| Type this | Expect | Catches |
| --- | --- | --- |
| `SELECT SUM(item_price) FROM dbo.order_items;` | `1778733.53` | Confirms finding 1 from inside SQL Server, and proves no rows were dropped |
| `SELECT COUNT(*) FROM dbo.order_item_options WHERE option_name LIKE '%Add Meat, Substitute Tofu%';` | greater than 0 | A wrong text qualifier splits that embedded comma and shifts every later column on 426 rows |
| `SELECT COUNT(*) FROM dbo.order_item_options WHERE option_name LIKE N'%Jalapeño%';` | `102` | A wrong code page turns these into mojibake |
| `SELECT TOP 3 date_key, day_of_week FROM dbo.date_dim ORDER BY date_key;` | `01-01-2023` is a Sunday | Confirms the dates landed as written rather than reordered |

All four pass means the load is correct and step 2 is done.

**Cost note.** AWS replaced the 12-month free tier on 15 July 2025 with a credit-based model, which is why the console offers no "Free tier" template — that template belonged to the old programme. The Dev/Test template is a configuration preset only; it grants no free usage. New accounts receive $100 of credit at signup plus up to $100 more for onboarding tasks, and the Free plan runs six months or until the credits are spent.

The console's estimate is therefore real on-demand pricing drawn against that credit balance: **$39.06/month** at the defaults, of which $23.00 was 200 GiB of storage for a 60 MB dataset. At the 20 GiB minimum it is **$18.36/month** ($16.06 instance + $2.30 storage). Stopping the instance between work sessions removes the instance charge and leaves only storage, landing near **$4–5/month**. RDS auto-restarts a stopped instance after 7 days, so a calendar reminder beats good intentions.

**Watch the Free plan expiry date.** When it ends the account closes unless it is upgraded to the Paid plan, and closure takes the database and its data with it after a 90-day grace period. Check Billing and Cost Management → Free tier for the balance and end date. If that date falls inside the project, upgrade to Paid before it — upgrading keeps any unspent credits, so doing it early costs nothing.

### Runbook: step 3, S3 bucket

Independent of RDS, so it can be done while the instance provisions. Create the key first — the bucket's default encryption setting needs it to already exist.

**A. The customer-managed key (designed, not adopted — the bucket was built on SSE-S3).** Console → KMS → Customer managed keys → Create key, in **us-east-1**.

| Setting | Value |
| --- | --- |
| Key type | Symmetric |
| Key usage | Encrypt and decrypt |
| Alias | `globalpartners-lake-key` |
| Key administrators | your own user or role |
| Key users | your own user or role — the Glue service role gets added in step 5, once it exists |

A customer-managed key costs **$1/month** plus request charges, where SSE-S3 is free. The design takes that trade deliberately: a CMK gives an auditable CloudTrail record of every decrypt and lets access be revoked at the key policy without touching the buckets.

**B. The bucket.** Console → S3 → Create bucket, in **us-east-1** — the same region as RDS and everything later, or Glue will not see it.

| Setting | Value |
| --- | --- |
| Name | `globalpartners-lake-<unique suffix>`. Created as global-partners-lake-nh in us-east-1 on 9 Nov, with all six prefixes. Bucket names are globally unique and lowercase, which is why a bare "global-partners" was not available. **Write down what you choose** — every Glue job references it |
| Block all public access | **On**, the default |
| Bucket versioning | **Enable** — lets an overwritten gold partition be recovered, which backs the reload story |
| Default encryption | As built: SSE-S3. Designed as SSE-KMS with `globalpartners-lake-key` |
| S3 Bucket Keys | **Enable** — cuts KMS request charges sharply on a bucket Spark writes in many small pieces |

**C. Create five prefixes** inside the bucket with the Create folder button: `bronze/`, `silver/`, `gold/`, `rejects/` and `athena-results/`. A sixth, `scripts/`, holds the Glue job files that CI/CD will sync in step 12.

*Verify:* the bucket's Properties tab shows Default encryption **SSE-S3** and Bucket Versioning **Enabled** (confirmed 9 Nov).

### 11 Nov — Steps 4–6 complete

All three Glue jobs run clean end to end, and Step Functions `gp-daily-clv-pipeline` succeeds through the gold crawler refresh.

| Layer | Verified |
| --- | --- |
| Silver | 202,692 fact rows · SUM(line\_revenue) $1,856,502.14 · dim\_date parity 365/365 |
| Gold | 10,808,918 rows · 1,402 partitions · final-snapshot SUM(clv\_historic) $1,685,422.79 · 20,059 customers |
| Athena | independently confirms silver: 202,692 / 1,856,502.14 / 20,059 / 130,587 orders |

**Discount signal added to silver.** `option_price < 0` returns zero rows, as the profiling predicted — the rule the assignment specifies finds nothing in this data. The modal-price derivation gives 410 items with a modal unit price, 42,031 discounted lines and $46,033.81 of discount, 2.48% of revenue. Spark's figures supersede the earlier pandas estimates (42,027 / $46,030.51); the ±4 rows and $3.30 are decimal versus float division on items whose price does not divide evenly by quantity.

**RFM rebuilt on lifetime F and M.** The first gold run produced only three segments, split exactly 40/20/40. Cause: `frequency_90d` and `monetary_90d` are zero for the 88% of customers more than 45 days past their last order, so ranking on them reproduces the recency ranking — r, f and m landed on identical quintiles, the VIP branch caught every `r>=4` row, and New, Loyal and Dormant were unreachable. Rebuilt on `orders_to_date` and `clv_historic`, with Dormant defined on the recency quintile rather than a day threshold (a 180-day test is unreachable, since `r<=2` already sits entirely beyond it). The 90-day windows are retained as columns because `spend_trend_pct` needs them. Six segments now populate:

| Segment | Customers | Avg CLV | Avg orders |
| --- | --- | --- | --- |
| Churn Risk | 5,993 | — | — |
| VIP | 4,192 | $195.74 | 13.90 |
| Loyal | 2,904 | $85.73 | 6.60 |
| New | 2,788 | $49.10 | 1.00 |
| At Risk | 2,131 | $204.98 | 13.04 |
| Dormant | 2,051 | $58.80 | 1.12 |

**`clv_tier` lands at 4,007 / 11,829 / 4,223**, not a clean 20/60/20. Many customers share an identical `clv_historic`, and `percent_rank()` assigns tied rows the same rank, so the Low band absorbs the boundary overflow.

**Open observation.** Both single-order segments average roughly $50 per order against $13–16 for every multi-order segment and $14.22 overall. Consistent with bulk or catering purchases by customers who never return — worth checking whether they concentrate in particular categories or locations before CLV is read across both populations as one.

**Dashboard.** Six Streamlit pages over Athena, read through a dedicated IAM user (`streamlit-dashboard-reader`) whose policy allows Athena, Glue catalog read, lake read, and object write only under `athena-results/`. Two choices worth recording: `ctas_approach=False` on every query, because awswrangler's default writes a temporary CTAS table and would need the Glue create/delete rights the dashboard user deliberately lacks; and every gold query pins a single `snapshot_date` as a literal, so Athena prunes to one partition of about 20,059 rows rather than scanning 10.8M.

### 16 Nov — walkthrough recorded

Five-minute video: [go.screenpal.com/watch/cOXD0bn4ux5](https://go.screenpal.com/watch/cOXD0bn4ux5). Linked from the repository README so it reaches anyone who opens the project cold.

Seven sections: why revenue is $1,856,502 and not $10,005,987; the architecture and the constraint behind each choice; a completed Step Functions run with its per-job assertions; lifetime value, the six RFM segments and the At Risk call list; loyalty, discounts and location; keyless CI/CD; and the three limitations worth stating out loud.

The `item_price` finding is given 25 seconds rather than led with. It earns that time because a reviewer working from the data dictionary would expect the $10M figure, and an unexplained gap of 5.4x reads as a pipeline bug rather than a judgement call. The supporting chart is held in reserve rather than shown.

## Open questions

Three for the SME, one still to settle before Step 6.

- [ ] **Finding 1 needs explicit sign-off.** We are overriding the supplied data dictionary: `item_price` is treated as the line total, which restates gross revenue from $9.93M to $1.86M. Verified adversarially on 5 Nov against an oracle built only from unambiguous quantity = 1 rows: the likelihood ratio is 8,482 : 1, and zero of 9,568 multi-quantity rows on $x.99 items end in .99. The conclusion is not in doubt, but a 5.6× change to the headline number should still be acknowledged in writing rather than assumed.
- [ ] **Is 2023 the intended analysis window?** We are regenerating `date_dim` to cover 2020–2024 so all 203,519 rows stay in scope. If the grader intended the supplied single-year dimension to define the window, the answer changes and three quarters of the history drops out.
- [ ] **Is the derived discount signal acceptable?** `option_price < 0` yields nothing, so the discount dashboard runs on modal-price variance instead. This is clearly labelled as derived rather than source, but it is an interpretation and the SME should agree to it.
- [ ] **Where does Streamlit run?** Local is fine for the demo video. If the submission needs a live URL, ECS Fargate or App Runner are the AWS-native options and both carry a small running cost. Decide before Step 6 starts.

### Known risks

| Risk | Mitigation |
| --- | --- |
| RDS free tier lapses or the instance is left running | Billing alarm at $10/month; stop the instance between work sessions |
| The 10.8M-row cross join is the only expensive Spark step | Broadcast `dim_customer` (20,174 rows) and partition the spine by `user_id` hash before the window |
| Backfilling 1,402 partitions in one execution could time out | Run the backfill in yearly chunks; the state machine already takes `snapshot_date` as a parameter |
| Half the `item_price` multi-quantity rows fit neither reading cleanly | 7,082 rows land in the "neither" bucket. They are flagged rather than corrected, and the flag is exposed in the dashboard so the ambiguity stays visible |
