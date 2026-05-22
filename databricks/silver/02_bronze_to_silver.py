import os
from pathlib import Path

import yaml

from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_timestamp,
    when,
    expr
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    MapType
)

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--env", default=os.getenv("ENV", "dev"))
args, _ = parser.parse_known_args()

ENV = args.env
# =========================================================
# Load Config
# =========================================================



PROJECT_ROOT = Path.cwd().parent.parent

CONFIG_PATH = (
    PROJECT_ROOT
    / "configs"
    / ENV
    / "streaming_config.yaml"
)

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# =========================================================
# Config Variables
# =========================================================

STORAGE_ACCOUNT = config["storage"]["storage_account"]

CHECKPOINT_CONTAINER = config["storage"]["checkpoint_container"]

CATALOG = config["unity_catalog"]["catalog"]

BRONZE_SCHEMA = config["unity_catalog"]["bronze_schema"]

SILVER_SCHEMA = config["unity_catalog"]["silver_schema"]

SOURCE_TABLE = config["silver"]["source_table"]

TARGET_TABLE = config["silver"]["target_table"]

TRIGGER_INTERVAL = config["silver"]["trigger_interval"]

WATERMARK_DELAY = config["silver"]["watermark_delay"]

CHECKPOINT_SUBPATH = config["silver"]["checkpoint_subpath"]

BRONZE_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.{SOURCE_TABLE}"
)

SILVER_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.{TARGET_TABLE}"
)

CHECKPOINT_LOCATION = (
    f"abfss://{CHECKPOINT_CONTAINER}"
    f"@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"{CHECKPOINT_SUBPATH}"
)

# =========================================================
# Explicit Transaction Schema
# =========================================================

transaction_schema = StructType([
    StructField("event_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("card_id", LongType()),
    StructField("merchant_id", LongType()),
    StructField("merchant_name", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("channel", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_address", StringType()),
    StructField("event_ts", StringType()),
    StructField(
        "risk_signals",
        MapType(StringType(), BooleanType())
    ),
    StructField("risk_score_hint", LongType()),
    StructField("is_suspicious_hint", BooleanType()),
    StructField("producer_app", StringType()),
    StructField("schema_version", StringType()),
])

# =========================================================
# Read Bronze Stream
# =========================================================

bronze_stream_df = (
    spark.readStream
    .table(BRONZE_TABLE)
)

# =========================================================
# Parse and Transform
# =========================================================

parsed_df = (
    bronze_stream_df
    .withColumn(
        "parsed_json",
        from_json(
            col("raw_json"),
            transaction_schema
        )
    )
)

silver_df = (
    parsed_df
    .select(
        "parsed_json.*",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingestion_ts",
        "bronze_record_hash"
    )
    .withColumn(
        "event_ts",
        to_timestamp(col("event_ts"))
    )
    .withColumn(
        "silver_processed_ts",
        current_timestamp()
    )
    .withColumn(
        "is_high_amount_txn",
        when(col("amount") >= 1000, True)
        .otherwise(False)
    )
    .withColumn(
        "is_cross_border",
        when(col("country") != "US", True)
        .otherwise(False)
    )
    .filter(col("event_id").isNotNull())
    .filter(col("transaction_id").isNotNull())
    .filter(col("amount") > 0)
)

# =========================================================
# Watermark + Deduplication
# =========================================================

silver_dedup_df = (
    silver_df
    .withWatermark(
        "event_ts",
        WATERMARK_DELAY
    )
    .dropDuplicates(["event_id"])
)

# =========================================================
# Write Silver Delta Table
# =========================================================

query = (
    silver_dedup_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION
    )
    .trigger(
        processingTime=TRIGGER_INTERVAL
    )
    .toTable(SILVER_TABLE)
)

query.awaitTermination()