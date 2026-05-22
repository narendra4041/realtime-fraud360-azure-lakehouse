import os
from pathlib import Path

import yaml

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# =========================================================
# Load Config
# =========================================================

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

CATALOG = config["unity_catalog"]["catalog"]

SILVER_SCHEMA = config["unity_catalog"]["silver_schema"]

SOURCE_TABLE = config["silver_export"]["source_table"]

SECRET_SCOPE = config["event_hubs"]["secret_scope"]

SNOWFLAKE_DATABASE = config["snowflake"]["database"]

SNOWFLAKE_SCHEMA = config["snowflake"]["silver_schema"]

SNOWFLAKE_TARGET_TABLE = config["silver_export"]["target_table"]

TRIGGER_INTERVAL = config["silver_export"]["trigger_interval"]

SOURCE_DELTA_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.{SOURCE_TABLE}"
)


# =========================================================
# Checkpoint
# =========================================================

STORAGE_ACCOUNT = config["storage"]["storage_account"]

CHECKPOINT_CONTAINER = config["storage"]["checkpoint_container"]

CHECKPOINT_SUBPATH = config["silver_export"]["checkpoint_subpath"]

CHECKPOINT_LOCATION = (
    f"abfss://{CHECKPOINT_CONTAINER}"
    f"@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"{CHECKPOINT_SUBPATH}"
)


# =========================================================
# Snowflake Secrets
# =========================================================

sf_url = dbutils.secrets.get(
    SECRET_SCOPE,
    config["snowflake"]["url_secret_key"]
)

sf_user = dbutils.secrets.get(
    SECRET_SCOPE,
    config["snowflake"]["user_secret_key"]
)

sf_role = dbutils.secrets.get(
    SECRET_SCOPE,
    config["snowflake"]["role_secret_key"]
)

sf_warehouse = dbutils.secrets.get(
    SECRET_SCOPE,
    config["snowflake"]["warehouse_secret_key"]
)

import re

pem_private_key = dbutils.secrets.get(
    SECRET_SCOPE,
    config["snowflake"]["private_key_secret_key"]
)

pem_passphrase = dbutils.secrets.get(
    SECRET_SCOPE,
    config["snowflake"]["private_key_passphrase_secret_key"]
).strip()

pem_private_key = pem_private_key.strip()

pem_private_key = re.sub(
    r"-----BEGIN ENCRYPTED PRIVATE KEY-----\s*",
    "-----BEGIN ENCRYPTED PRIVATE KEY-----\n",
    pem_private_key,
)

pem_private_key = re.sub(
    r"\s*-----END ENCRYPTED PRIVATE KEY-----",
    "\n-----END ENCRYPTED PRIVATE KEY-----",
    pem_private_key,
)

lines = pem_private_key.splitlines()

if len(lines) == 3:
    body = re.sub(r"\s+", "", lines[1])
    wrapped_body = "\n".join(
        body[i:i + 64]
        for i in range(0, len(body), 64)
    )

    pem_private_key = (
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n"
        f"{wrapped_body}\n"
        "-----END ENCRYPTED PRIVATE KEY-----"
    )

print(f"Key lines: {len(pem_private_key.splitlines())}")


# =========================================================
# Decrypt PEM Private Key
# =========================================================

private_key_obj = serialization.load_pem_private_key(
    pem_private_key.encode("utf-8"),
    password=pem_passphrase.encode("utf-8"),
    backend=default_backend()
)

private_key_der = private_key_obj.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)


# =========================================================
# Snowflake Connector Options
# =========================================================

sf_options = {
    "sfURL": sf_url,
    "sfUser": sf_user,
    "sfDatabase": SNOWFLAKE_DATABASE,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": sf_warehouse,
    "sfRole": sf_role,
    "pem_private_key": private_key_der,
}


# =========================================================
# Read Silver Stream
# =========================================================

silver_stream_df = (
    spark.readStream
    .table(SOURCE_DELTA_TABLE)
)


# =========================================================
# Write Each Micro Batch to Snowflake
# =========================================================

def write_to_snowflake(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Skipping empty batch: {batch_id}")
        return

    (
        batch_df.write
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", SNOWFLAKE_TARGET_TABLE)
        .mode("append")
        .save()
    )

    print(
        f"Successfully exported batch_id={batch_id} "
        f"to Snowflake table "
        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TARGET_TABLE}"
    )


# =========================================================
# Streaming Query
# =========================================================

query = (
    silver_stream_df.writeStream
    .foreachBatch(write_to_snowflake)
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION
    )
    .trigger(
        processingTime=TRIGGER_INTERVAL
    )
    .start()
)

query.awaitTermination()