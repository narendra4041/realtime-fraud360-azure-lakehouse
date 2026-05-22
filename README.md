# Realtime Fraud360 Azure Lakehouse

Enterprise-grade realtime fraud detection data platform built using Azure Event Hubs, Databricks Lakeflow Pipelines, Unity Catalog, Snowflake, dbt, and GitHub Actions CI/CD.

---

# Architecture

```text
Azure Event Hubs
        ↓
Lakeflow Declarative Pipeline
(Bronze + Silver on Databricks)

        ↓
Databricks Streaming Job
(Silver → Snowflake)

        ↓
dbt Models on Snowflake
(GOLD Analytics Layer)

        ↓
BI / Fraud Analytics / Dashboards
```

---

# Tech Stack

| Layer | Technology |
|---|---|
| Streaming Ingestion | Azure Event Hubs (Kafka API) |
| Stream Processing | Databricks Lakeflow Declarative Pipelines |
| Lakehouse Storage | Delta Lake + Unity Catalog |
| Transformation | PySpark |
| Warehouse | Snowflake |
| Analytics Modeling | dbt |
| CI/CD | GitHub Actions |
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
- Raw Kafka metadata
- Event replay capability
- Streaming ingestion
- Record hashing
- Audit timestamps

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
- Data quality checks
- Fraud enrichment flags

---

## Gold Layer

Business analytics models in Snowflake using dbt.

Example metrics:
- customer transaction aggregation
- suspicious transaction counts
- merchant analytics
- cross-border behavior
- fraud KPIs

---

# Project Structure

```text
realtime-fraud360-azure-lakehouse/
│
├── configs/
│   └── dev/
│       └── streaming_config.yaml
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── snowflake/
│   └── pipelines/
│
├── dbt_fraud360/
│   ├── models/
│   └── dbt_project.yml
│
├── resources/
│   ├── fraud360_jobs.yml
│   └── fraud360_pipeline.yml
│
├── .github/
│   └── workflows/
│       └── databricks-bundle-dev.yml
│
├── databricks.yml
├── .gitignore
├── pyproject.toml
├── uv.lock
├── requirements.txt
│
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
- Bronze table creation
- Silver transformation
- Continuous streaming processing

---

## Databricks Job

Job:

```text
fraud360_dev_snowflake_stream
```

Responsibilities:
- Stream Silver Delta data
- Export to Snowflake continuously

---

# Snowflake Architecture

Database:

```text
FRAUD360
```

Schemas:

```text
SILVER
GOLD
```

Warehouse:

```text
FRAUD360_WH
```

---

# CI/CD

GitHub Actions workflow:

```text
.github/workflows/databricks-bundle-dev.yml
```

CI/CD responsibilities:
- Validate Databricks bundles
- Deploy Lakeflow pipelines
- Deploy Databricks jobs
- Run dbt models
- Execute dbt tests

---

# Authentication & Security

## Databricks

- OAuth M2M Service Principal Authentication
- Unity Catalog RBAC
- Secret Scopes

## Snowflake

- RSA Key Pair Authentication
- Role-based access control

---

# Streaming Features

## Watermarking

Used for late-arriving event handling.

Example:

```python
.withWatermark("event_ts", "10 minutes")
```

---

## Deduplication

Implemented using:

```python
.dropDuplicates(["event_id"])
```

---

# Data Quality Rules

Silver layer validations:

- valid_event_id
- valid_transaction_id
- valid_amount

Example:

```python
@dp.expect_or_drop("valid_amount", "amount > 0")
```

---

# Local Development

## Install dependencies

```bash
uv sync
```

---

## Databricks CLI

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

# dbt Commands

## Debug

```bash
uv run dbt debug --profiles-dir profiles
```

---

## Run GOLD Models

```bash
uv run dbt run --select gold --profiles-dir profiles
```

---

## Run Tests

```bash
uv run dbt test --select gold --profiles-dir profiles
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

## Snowflake

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PRIVATE_KEY
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
```

---

# Bundle Variables

Provided using:

```text
BUNDLE_VAR_<variable_name>
```

Examples:

```text
BUNDLE_VAR_event_hub_bootstrap_servers
BUNDLE_VAR_bronze_table_fqn
BUNDLE_VAR_silver_table_fqn
```

---

# Future Improvements

- Production environment target
- Automated pipeline restart policies
- Observability dashboards
- Data quality monitoring
- Great Expectations integration
- Terraform infrastructure provisioning
- Alerting integrations
- CDC ingestion support
- Feature store integration

---

# Author

Narendra Reddy

---