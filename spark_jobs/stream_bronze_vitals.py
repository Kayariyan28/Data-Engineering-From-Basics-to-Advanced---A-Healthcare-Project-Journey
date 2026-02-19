"""
spark_jobs/stream_bronze_vitals.py
====================================
Spark Structured Streaming job: reads vital signs from Kafka and
lands them in the Bronze layer (MinIO/Delta Lake) as micro-batches.

Run (after docker-compose up):
    spark-submit \
        --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        spark_jobs/stream_bronze_vitals.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, BooleanType,
)
from config.spark_config import build_spark_session, STORAGE_PATHS, KAFKA

spark = build_spark_session("HealthcareVitalsStreaming")

# ── Schema ──────────────────────────────────────────────────────────────────
# Structured Streaming requires an explicit schema — you cannot infer from a
# stream the way you can from a static file.
vitals_schema = StructType([
    StructField("patient_id",    StringType(),  True),
    StructField("timestamp",     StringType(),  True),
    StructField("heart_rate",    IntegerType(), True),
    StructField("systolic_bp",   IntegerType(), True),
    StructField("diastolic_bp",  IntegerType(), True),
    StructField("spo2",          IntegerType(), True),
    StructField("temperature_c", FloatType(),   True),
    StructField("is_anomaly",    BooleanType(), True),
])

# ── Source: Kafka ────────────────────────────────────────────────────────────
# Spark treats the Kafka stream like a table — this is the "unified API"
# that makes Structured Streaming powerful: the same DataFrame operations
# work on both streams and static data.
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA["bootstrap_servers"])
    .option("subscribe", KAFKA["vitals_topic"])
    .option("startingOffsets", "latest")
    .load()
)

# Kafka delivers all data as raw bytes. Cast the value column to string,
# then parse the JSON payload into typed columns.
vitals_stream = (
    kafka_stream
    .select(from_json(col("value").cast("string"), vitals_schema).alias("data"))
    .select("data.*")
    .withColumn("_ingested_at", current_timestamp())
)

# ── Sink: Delta Lake on MinIO ─────────────────────────────────────────────────
# "append" outputMode is correct here — we only ever add new vitals, never
# update or delete them (Bronze is immutable by design).
# checkpointLocation is mandatory for fault tolerance: if the job crashes
# mid-batch, Spark replays from the checkpoint rather than losing data.
query = (
    vitals_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", STORAGE_PATHS["checkpoints_vitals"])
    .trigger(processingTime="30 seconds")
    .start(STORAGE_PATHS["bronze_vitals"])
)

print(f"Streaming vitals → {STORAGE_PATHS['bronze_vitals']}")
print("Query running — press Ctrl+C to stop.")
query.awaitTermination()
