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
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    round as spark_round,
    to_date
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

EVENT_HUB_BOOTSTRAP_SERVERS = spark.conf.get("EVENT_HUB_BOOTSTRAP_SERVERS")
EVENT_HUB_TOPIC = spark.conf.get("EVENT_HUB_TOPIC")
EVENT_HUB_CONSUMER_GROUP = spark.conf.get("EVENT_HUB_CONSUMER_GROUP")
EVENT_HUB_SECRET_SCOPE = spark.conf.get("EVENT_HUB_SECRET_SCOPE")
EVENT_HUB_CONNECTION_SECRET_KEY = spark.conf.get("EVENT_HUB_CONNECTION_SECRET_KEY")

BRONZE_TABLE_FQN = spark.conf.get("BRONZE_TABLE_FQN")
SILVER_TABLE_FQN = spark.conf.get("SILVER_TABLE_FQN")
WATERMARK_DELAY = spark.conf.get("WATERMARK_DELAY")

GOLD_CUSTOMER_RISK_MV_FQN = spark.conf.get("GOLD_CUSTOMER_RISK_MV_FQN")
GOLD_MERCHANT_RISK_MV_FQN = spark.conf.get("GOLD_MERCHANT_RISK_MV_FQN")
GOLD_DAILY_FRAUD_KPIS_MV_FQN = spark.conf.get("GOLD_DAILY_FRAUD_KPIS_MV_FQN")


event_hub_connection_string = dbutils.secrets.get(
    scope=EVENT_HUB_SECRET_SCOPE,
    key=EVENT_HUB_CONNECTION_SECRET_KEY,
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
    name=BRONZE_TABLE_FQN,
    comment="Bronze raw immutable transactions from Azure Event Hubs Kafka endpoint.",
)
def transactions_raw():
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", EVENT_HUB_BOOTSTRAP_SERVERS)
        .option("subscribe", EVENT_HUB_TOPIC)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", kafka_jaas_config)
        .option("kafka.group.id", EVENT_HUB_CONSUMER_GROUP)
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
    name=SILVER_TABLE_FQN,
    comment="Silver cleaned and typed fraud transactions.",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount > 0")
def transactions_clean():
    bronze_df = spark.readStream.table(BRONZE_TABLE_FQN)

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
        .withWatermark("event_ts", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )


@dp.materialized_view(
    name=GOLD_CUSTOMER_RISK_MV_FQN,
    comment="Customer-level fraud risk summary for AI analytics agent.",
)
def mv_customer_risk_summary():
    df = spark.read.table(SILVER_TABLE_FQN)

    return (
        df.groupBy("customer_id")
        .agg(
            count("*").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
            spark_round(spark_max("amount"), 2).alias("max_amount"),
            spark_sum(
                col("is_suspicious_hint").cast("int")
            ).alias("suspicious_transaction_count"),
            count("country").alias("country_observation_count"),
            count("merchant_category").alias("merchant_category_observation_count"),
            spark_max("event_ts").alias("last_transaction_ts"),
        )
    )


@dp.materialized_view(
    name=GOLD_MERCHANT_RISK_MV_FQN,
    comment="Merchant-level fraud risk summary for AI analytics agent.",
)
def mv_merchant_risk_summary():
    df = spark.read.table(SILVER_TABLE_FQN)

    return (
        df.groupBy(
            "merchant_id",
            "merchant_name",
            "merchant_category",
        )
        .agg(
            count("*").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
            spark_sum(
                col("is_suspicious_hint").cast("int")
            ).alias("suspicious_transaction_count"),
            count("customer_id").alias("customer_observation_count"),
            count("country").alias("country_observation_count"),
            spark_max("event_ts").alias("last_transaction_ts"),
        )
    )


@dp.materialized_view(
    name=GOLD_DAILY_FRAUD_KPIS_MV_FQN,
    comment="Daily fraud KPI summary for AI analytics agent.",
)
def mv_daily_fraud_kpis():
    df = spark.read.table(SILVER_TABLE_FQN)

    return (
        df.groupBy(to_date(col("event_ts")).alias("transaction_date"))
        .agg(
            count("*").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(avg("amount"), 2).alias("avg_transaction_amount"),
            spark_sum(
                col("is_suspicious_hint").cast("int")
            ).alias("suspicious_transaction_count"),
            count("customer_id").alias("customer_observation_count"),
            count("merchant_id").alias("merchant_observation_count"),
            spark_max("event_ts").alias("last_transaction_ts"),
        )
        .withColumn(
            "suspicious_rate_pct",
            spark_round(
                100.0
                * col("suspicious_transaction_count")
                / col("transaction_count"),
                2,
            ),
        )
    )