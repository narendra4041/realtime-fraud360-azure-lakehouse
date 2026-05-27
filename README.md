# Realtime Fraud360 Azure Lakehouse

Enterprise-grade realtime fraud detection and AI analytics platform built using Azure Event Hubs, Databricks Lakeflow Declarative Pipelines, Unity Catalog, and GitHub Actions CI/CD.

---

# Architecture

```text
Azure Event Hubs
        ↓
Lakeflow Declarative Pipeline
(Bronze + Silver + GOLD Materialized Views)

        ↓
AI Analytics Agent / BI Dashboards
```

---

# Tech Stack

| Layer | Technology |
|---|---|
| Streaming Ingestion | Azure Event Hubs (Kafka API) |
| Stream Processing | Databricks Lakeflow Declarative Pipelines |
| Lakehouse Storage | Delta Lake + Unity Catalog |
| Transformations | PySpark |
| Analytics Layer | Databricks Materialized Views |
| CI/CD | GitHub Actions |
| Authentication | OAuth M2M Service Principal |
| Secrets Management | Databricks Secret Scopes |
| Infrastructure Deployment | Databricks Asset Bundles |

---

# Medallion Architecture

## Bronze Layer

Raw immutable transaction ingestion from Event Hubs.

Table:

```text
fraud360.bronze.transactions_raw_lf
```

Features:
- Raw Kafka ingestion
- Immutable storage
- Replay capability
- Audit timestamps
- Metadata tracking

---

## Silver Layer

Cleaned and validated fraud transaction records.

Table:

```text
fraud360.silver.transactions_clean_lf
```

Features:
- Schema enforcement
- Watermarking
- Deduplication
- Data quality expectations
- Fraud enrichment
- Streaming transformations

---

## Gold Layer

AI-ready materialized views for analytics and fraud intelligence.

Materialized Views:

```text
fraud360.gold.mv_customer_risk_summary
fraud360.gold.mv_merchant_risk_summary
fraud360.gold.mv_daily_fraud_kpis
```

Features:
- Aggregated business KPIs
- AI analytics optimized datasets
- Fast querying
- Incremental refresh
- Unity Catalog governance

---

# Declarative Pipeline Architecture

The project uses Databricks Lakeflow Declarative Pipelines.

Example:

```python
@dp.table(
    name=BRONZE_TABLE_FQN
)
def transactions_raw():
    return kafka_df
```

Databricks manages:
- streaming orchestration
- checkpoints
- table lifecycle
- dependency graph
- retries
- lineage
- refresh execution

---

# Project Structure

```text
realtime-fraud360-azure-lakehouse/
│
├── databricks/
│   └── pipelines/
│       └── fraud360_bronze_silver_pipeline.py
│
├── resources/
│   └── fraud360_pipeline.yml
│
├── .github/
│   └── workflows/
│       └── databricks-bundle-dev.yml
│
├── .databricksignore
├── databricks.yml
└── README.md
```

---

# Databricks Components

## Lakeflow Pipeline

Pipeline:

```text
fraud360_dev_bronze_silver_pipeline
```

Responsibilities:
- Event Hub ingestion
- Bronze streaming table creation
- Silver streaming transformations
- GOLD materialized view creation
- AI-ready analytics datasets

---

# AI Analytics GOLD Materialized Views

## Customer Risk Summary

Materialized View:

```text
fraud360.gold.mv_customer_risk_summary
```

Metrics:
- transaction counts
- suspicious transaction counts
- total spend
- average transaction amount
- merchant diversity
- country diversity

---

## Merchant Risk Summary

Materialized View:

```text
fraud360.gold.mv_merchant_risk_summary
```

Metrics:
- merchant transaction volume
- suspicious transaction volume
- customer diversity
- country diversity
- fraud exposure indicators

---

## Daily Fraud KPIs

Materialized View:

```text
fraud360.gold.mv_daily_fraud_kpis
```

Metrics:
- daily transaction volume
- suspicious transaction rate
- daily fraud KPIs
- customer activity
- merchant activity

---

# Streaming Features

## Watermarking

```python
.withWatermark("event_ts", "10 minutes")
```

Used for:
- late event handling
- state cleanup
- streaming optimization

---

## Deduplication

```python
.dropDuplicates(["event_id"])
```

Used for:
- exactly-once semantics
- replay protection
- duplicate prevention

---

# Data Quality Rules

Silver layer expectations:

```python
@dp.expect_or_drop(
    "valid_amount",
    "amount > 0"
)
```

Implemented rules:
- valid_event_id
- valid_transaction_id
- valid_amount

---

# CI/CD Architecture

GitHub Actions workflow:

```text
.github/workflows/databricks-bundle-dev.yml
```

Responsibilities:
- Validate Databricks bundles
- Deploy pipelines
- Update Databricks workspace resources
- Run deployment automation

---

# Deployment Flow

```text
git push
    ↓
GitHub Actions
    ↓
Databricks Bundle Deploy
    ↓
Databricks Workspace
    ↓
Lakeflow Pipeline Updated
```

---

# GitHub Secrets Required

## Databricks

```text
DATABRICKS_HOST
DATABRICKS_CLIENT_ID
DATABRICKS_CLIENT_SECRET
```

---

# Bundle Variables

Examples:

```text
BUNDLE_VAR_event_hub_bootstrap_servers
BUNDLE_VAR_event_hub_topic
BUNDLE_VAR_bronze_table_fqn
BUNDLE_VAR_silver_table_fqn
BUNDLE_VAR_gold_customer_risk_mv_fqn
```

---

# Local Development

## Install dependencies

```bash
uv sync
```

---

## Databricks Login

```bash
databricks auth login --host <workspace-url>
```

---

## Validate Bundle

```bash
databricks bundle validate -t dev
```

---

## Deploy Bundle

```bash
databricks bundle deploy -t dev
```

---

## Run Pipeline

```bash
databricks bundle run fraud360_bronze_silver_pipeline -t dev
```

---

# Security

## Databricks

- OAuth M2M Service Principal
- Unity Catalog RBAC
- Secret Scopes

---

# Future Improvements

- Production deployment target
- Great Expectations integration
- ML fraud scoring
- Feature store integration
- Terraform infrastructure provisioning
- Databricks Genie integration
- AI semantic layer
- Observability dashboards
- Automated restart policies
- CDC ingestion pipelines

---

# Author

Narendra Reddy