# Global Partners — Customer Lifetime Value Pipeline

A daily-refreshed CLV data product for a café and convenience chain. SQL Server
source, AWS-native pipeline, all transformation logic in PySpark, Streamlit
dashboard, deployed from this repository by GitHub Actions.

Solution design and build log: <https://claude.ai/artifact/5bUSyn8LQPTWGhLU8Vc4G9>

---

## Architecture

```
RDS SQL Server Express
        │  Glue JDBC connection (VPC, Secrets Manager)
        ▼
  S3 bronze/   raw Parquet, partitioned by ingest_date
        │  gp-01-ingest-bronze
        ▼
  S3 silver/   conformed fact + four dimensions
        │  gp-02-cleanse-silver
        ▼
  S3 gold/     fact_customer_clv_daily — dense daily snapshot
        │  gp-03-metrics-gold
        ▼
  Glue Catalog → Athena → Streamlit
```

Step Functions (`gp-daily-clv-pipeline`) runs the three jobs in sequence and
refreshes the gold crawler. EventBridge fires it daily at 02:00 UTC. Failures
raise an SNS alert; a failed run is resumed with Redrive rather than replayed
from the start.

## Why each component

| Component | Choice | Why, and what was rejected |
|---|---|---|
| Source | RDS SQL Server Express | The assignment fixes SQL Server. RDS puts it where Glue can reach it over VPC, and SSMS connects from Windows for the manual load. |
| Transform | Glue 6.0 (Spark 4.1) PySpark | The assignment fixes PySpark. Glue is the AWS-native managed Spark; EMR was rejected as more operational surface than 200k rows justifies. |
| Storage | S3 Parquet, medallion layers | Columnar storage keeps Athena scans small. Partition overwrite on `snapshot_date` makes a reload idempotent. |
| Catalog | Glue Data Catalog + crawlers | Athena needs a catalog; crawlers keep schema in step with the writers without a migration step. |
| Query | Athena | Serverless, pay-per-scan, no cluster to leave running. Redshift was rejected — a 0.35 GB gold layer does not need it. |
| Orchestration | Step Functions + EventBridge | Per-state `Retry` is the failure mechanism the brief asks for; Redrive from the failed state is the reload mechanism. The execution graph is legible without extra tooling. |
| Secrets | Secrets Manager + VPC interface endpoint | Glue workers have no public IP, so the endpoint is required, not optional. |
| Dashboard | Streamlit over Athena | Runs locally for the demo. The read path uses a dedicated IAM user scoped to Athena, catalog read, lake read, and write only under `athena-results/`. |
| CI/CD | GitHub Actions + OIDC | No long-lived AWS keys in the repository. The workflow assumes a role scoped to this repo. |

## Layout

```
glue/                 PySpark job scripts, deployed to S3 by CI
stepfunctions/        state machine definition
iam/                  policy documents, version-controlled alongside the code
dashboard/            Streamlit app
docs/                 exported solution design
.github/workflows/    CI and deploy
```

## Running the dashboard

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

Needs a `[globalpartners]` profile in `~/.aws/credentials` mapping to an IAM
principal with the policy in `iam/gp-dashboard-read.json`.

## Deployment

Pushing to `main` syncs `glue/*.py` to the Glue assets bucket. The next job run
picks up the new script — Glue reads its script from S3 at run time, so there is
no separate job-update call and no window where the job definition and the
script disagree.

CI runs on every push and pull request: byte-compile and lint the PySpark and
dashboard code, and validate the Step Functions and IAM JSON.

No long-lived AWS keys. The deploy role’s trust policy pins the OIDC subject to repo:Footsman@249142260/globalpartners-clv@1455642793:environment:production. This account issues tokens whose subject carries the numeric owner and repository IDs rather than the names — names can be released and re-registered, IDs cannot — so the trust is immune to a rename or a re-registration of the repository name by someone else. The branch is constrained separately by a deployment-branch rule on the environment.

## Verified figures

These are asserted in the jobs themselves and re-checked after every run.

| Layer | Measure | Value |
|---|---|---|
| Bronze | rows ingested | 203,519 / 193,017 / 365 |
| Silver | `fact_order_line` rows | 202,692 |
| Silver | `SUM(line_revenue)` | $1,856,502.14 |
| Silver | `dim_date` parity against supplied CSV | 365 / 365 |
| Silver | discounted lines · discount | 42,031 · $46,033.81 |
| Gold | rows · partitions | 10,808,918 · 1,402 |
| Gold | final-snapshot customers | 20,059 |
| Gold | final-snapshot `SUM(clv_historic)` | $1,685,422.79 |

## Known limitations

**`item_price` is the extended line total**, not the unit price the supplied data
dictionary claims. Revenue is `SUM(item_price) + SUM(option_price)`; never
multiply by quantity. The evidence is in the solution design under Finding 1.

**Guest orders carry no `user_id`** — 17,502 lines, 8.6%. They contribute to
revenue but not to any customer-grain metric, so silver and gold revenue totals
differ by design.

**The discount rule the brief specifies finds nothing.** `option_price < 0`
returns zero rows. The discount signal here is derived from unit price against
each item's modal price, and is labelled as derived wherever it appears.

**Churn at the final snapshot is 88%.** Measured on the last day of a four-year
dataset, every customer who ever lapsed counts. The figure is correct and of
limited operational use on its own.
