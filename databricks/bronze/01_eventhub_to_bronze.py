import yaml

from pyspark.sql.functions import (
    col,
    current_timestamp,
    sha2,
    concat_ws,
    lit
)

# =========================================================
# Load Config
# =========================================================

from pathlib import Path
import os

ENV = os.getenv("ENV", "dev")

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

BOOTSTRAP_SERVERS = config["event_hubs"]["bootstrap_servers"]
TOPIC = config["event_hubs"]["topic"]

SECRET_SCOPE = config["event_hubs"]["secret_scope"]
SECRET_KEY = config["event_hubs"]["connection_secret_key"]

STORAGE_ACCOUNT = config["storage"]["storage_account"]

BRONZE_CONTAINER = config["storage"]["bronze_container"]
CHECKPOINT_CONTAINER = config["storage"]["checkpoint_container"]

CATALOG = config["unity_catalog"]["catalog"]
BRONZE_SCHEMA = config["unity_catalog"]["bronze_schema"]

BRONZE_TABLE_NAME = config["bronze"]["table_name"]

TRIGGER_INTERVAL = config["bronze"]["trigger_interval"]

CHECKPOINT_SUBPATH = config["bronze"]["checkpoint_subpath"]

CONSUMER_GROUP = config["event_hubs"]["consumer_group"]

BRONZE_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE_NAME}"
)

CHECKPOINT_LOCATION = (
    f"abfss://{CHECKPOINT_CONTAINER}"
    f"@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"{CHECKPOINT_SUBPATH}"
)

# =========================================================
# Secret Retrieval
# =========================================================

event_hub_connection_string = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=SECRET_KEY
)

# =========================================================
# Kafka JAAS Config
# =========================================================

kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{event_hub_connection_string}";'
)

# =========================================================
# Read Stream
# =========================================================

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", kafka_jaas_config)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# =========================================================
# Bronze Transformation
# =========================================================

bronze_df = (
    raw_kafka_df
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("timestampType").alias("kafka_timestamp_type"),
    )
    .withColumn(
        "source_system",
        lit("azure_event_hubs_kafka")
    )
    .withColumn(
        "bronze_ingestion_ts",
        current_timestamp()
    )
    .withColumn(
        "bronze_record_hash",
        sha2(
            concat_ws(
                "||",
                col("kafka_key"),
                col("raw_json")
            ),
            256
        )
    )
)

# =========================================================
# Write Bronze Delta
# =========================================================

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION
    )
    .trigger(
        processingTime=TRIGGER_INTERVAL
    )
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()