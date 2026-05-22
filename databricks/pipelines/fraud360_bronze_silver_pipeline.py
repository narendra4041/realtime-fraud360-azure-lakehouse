import os
import yaml
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    sha2,
    concat_ws,
    from_json,
    to_timestamp,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    MapType,
)

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--env", default=os.getenv("ENV", "dev"))
args, _ = parser.parse_known_args()

ENV = args.env

PROJECT_ROOT = Path.cwd().parent.parent
CONFIG_PATH = PROJECT_ROOT / "configs" / ENV / "streaming_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

event_hub_connection_string = dbutils.secrets.get(
    scope=config["event_hubs"]["secret_scope"],
    key=config["event_hubs"]["connection_secret_key"],
)

kafka_jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    "required "
    'username="$ConnectionString" '
    f'password="{event_hub_connection_string}";'
)

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
    StructField("risk_signals", MapType(StringType(), BooleanType())),
    StructField("risk_score_hint", LongType()),
    StructField("is_suspicious_hint", BooleanType()),
    StructField("producer_app", StringType()),
    StructField("schema_version", StringType()),
])


@dp.table(
    name=config["pipeline"]["bronze_table_fqn"],
    comment=config["pipeline"]["bronze_comment"],
)
def transactions_raw():
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["event_hubs"]["bootstrap_servers"])
        .option("subscribe", config["event_hubs"]["topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", kafka_jaas_config)
        .option("kafka.group.id", config["event_hubs"]["consumer_group"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
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
        .withColumn("source_system", lit("azure_event_hubs_kafka"))
        .withColumn("bronze_ingestion_ts", current_timestamp())
        .withColumn(
            "bronze_record_hash",
            sha2(concat_ws("||", col("kafka_key"), col("raw_json")), 256),
        )
    )


@dp.table(
    name=config["pipeline"]["silver_table_fqn"],
    comment=config["pipeline"]["silver_comment"],
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount > 0")
def transactions_clean():
    bronze_df = spark.readStream.table(
        config["pipeline"]["bronze_table_fqn"]
    )

    parsed_df = (
        bronze_df
        .withColumn("parsed_json", from_json(col("raw_json"), transaction_schema))
        .select(
            "parsed_json.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_ingestion_ts",
            "bronze_record_hash",
        )
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn(
            "is_high_amount_txn",
            when(col("amount") >= 1000, True).otherwise(False),
        )
        .withColumn(
            "is_cross_border",
            when(col("country") != "US", True).otherwise(False),
        )
    )

    return (
        parsed_df
        .withWatermark("event_ts", config["silver"]["watermark_delay"])
        .dropDuplicates(["event_id"])
    )