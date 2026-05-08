from __future__ import annotations
import json
import gzip
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType, IntegerType, LongType, BooleanType,
    TimestampType, DateType, DoubleType, FloatType, ArrayType,
)

from src.storage.s3.partitioning import get_s3_key
from src.utils.config_loader import load_config, load_table_config
from src.utils.logger import get_logger
from src.utils.aws_session import get_s3_client, get_glue_client

logger = get_logger(__name__)
config = load_config()
bucket = config["s3"]["bucket_name"]


# ── Bronze ────────────────────────────────────────────────────────────────────

def upload_to_bronze_s3(
    data: list[dict],
    dataset: str,
    ingestion_time: datetime = None,
) -> str:
    table_config = load_table_config("raw")

    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    filename = f"{dataset}_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json.gz"
    s3_key = get_s3_key(
        layer="raw",
        dataset=dataset,
        filename=filename,
        partition_time=ingestion_time,
        partition_cols=table_config["partition_cols"],
    )

    body = gzip.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    get_s3_client().put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    logger.info(f"bronze upload complete {s3_key}")
    return f"s3://{bucket}/{s3_key}"


# ── Silver / Gold ─────────────────────────────────────────────────────────────

def write_to_S3(df: DataFrame, table_name: str, layer: str, mode: str = "overwrite") -> None:
    table_config = load_table_config(layer)
    partition_cols = table_config["partition_cols"]

    dataset = table_name.split(".")[-1]
    database = table_name.split(".")[0]
    s3a_path = f"s3a://{bucket}/{layer}/{dataset}/" # for Spark write
    s3_path  = f"s3://{bucket}/{layer}/{dataset}/" # for Glue registration

    (df.write
        .mode(mode)
        .partitionBy(*partition_cols)
        .format("parquet")
        .option("compression", "snappy")
        .save(s3a_path)
    )

    _register_glue_table(database, dataset, s3_path, df.schema, partition_cols)
    logger.info(f"{layer} upload complete → {s3a_path} | glue: {database}.{dataset}")


# ── Glue Catalog registration ─────────────────────────────────────────────────

def _spark_type_to_glue(spark_type) -> str:
    type_map = {
        StringType:    "string",
        IntegerType:   "int",
        LongType:      "bigint",
        BooleanType:   "boolean",
        TimestampType: "timestamp",
        DateType:      "date",
        DoubleType:    "double",
        FloatType:     "float",
    }
    for spark_t, glue_t in type_map.items():
        if isinstance(spark_type, spark_t):
            return glue_t
    if isinstance(spark_type, ArrayType):
        return f"array<{_spark_type_to_glue(spark_type.elementType)}>"
    return "string"


def _register_glue_table(
    database: str,
    table: str,
    s3_path: str,
    schema,
    partition_cols: list[str],
) -> None:
    glue = get_glue_client()

    try:
        glue.create_database(DatabaseInput={"Name": database})
        logger.info(f"Glue database created: {database}")
    except glue.exceptions.AlreadyExistsException:
        pass

    partition_col_set = set(partition_cols)
    data_columns = [
        {"Name": f.name, "Type": _spark_type_to_glue(f.dataType)}
        for f in schema.fields
        if f.name not in partition_col_set
    ]
    partition_keys = [
        {"Name": f.name, "Type": _spark_type_to_glue(f.dataType)}
        for f in schema.fields
        if f.name in partition_col_set
    ]

    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": {
            "Columns": data_columns,
            "Location": s3_path,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
        },
        "PartitionKeys": partition_keys,
    }

    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        logger.info(f"Glue table created: {database}.{table}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(DatabaseName=database, TableInput=table_input)
        logger.info(f"Glue table updated: {database}.{table}")
