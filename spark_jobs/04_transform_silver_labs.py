"""
SILVER LAYER — Lab Results Transformation
==========================================
Purpose: Clean and enrich Bronze lab results. Key addition here is computing
         whether each result is above/below/within the reference range — a
         derived column with high clinical and analytical value.

Run with:
  spark-submit spark_jobs/04_transform_silver_labs.py
"""

import sys, os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import build_spark_session, STORAGE_PATHS, log_pipeline_run

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, upper, trim, to_timestamp,
    current_timestamp, round as spark_round
)

spark = build_spark_session("Silver_Labs_Transform")
start_time = datetime.now()
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"\n{'='*60}")
print(f"  Silver Transformation: Lab Results  |  Batch: {batch_id}")
print(f"{'='*60}\n")

bronze_df = spark.read.format("delta").load(STORAGE_PATHS["bronze_labs"])
bronze_count = bronze_df.count()
print(f"Bronze lab rows: {bronze_count:,}")

silver_df = bronze_df \
    \
    # Deduplication on the natural key
    .dropDuplicates(["lab_id"]) \
    \
    # Parse string timestamps to proper timestamp type
    .withColumn("collected_at", to_timestamp(col("collected_at"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("resulted_at",  to_timestamp(col("resulted_at"),  "yyyy-MM-dd HH:mm:ss")) \
    \
    # Cast numeric columns
    .withColumn("result_value",   col("result_value").cast("double")) \
    .withColumn("reference_low",  col("reference_low").cast("double")) \
    .withColumn("reference_high", col("reference_high").cast("double")) \
    .withColumn("is_critical",    col("is_critical").cast("boolean")) \
    \
    # Standardize text
    .withColumn("test_name",   upper(trim(col("test_name")))) \
    .withColumn("lab_status",  upper(trim(col("lab_status")))) \
    \
    # Key derived column: result interpretation
    # This saves analysts from writing this CASE WHEN in every single query
    .withColumn("result_interpretation",
                when(col("result_value") < col("reference_low"),  lit("LOW"))
                .when(col("result_value") > col("reference_high"), lit("HIGH"))
                .otherwise(lit("NORMAL"))) \
    \
    # How far outside the reference range? (0 = within range)
    # Useful for severity scoring
    .withColumn("deviation_from_range",
                when(col("result_value") < col("reference_low"),
                     spark_round(col("reference_low") - col("result_value"), 3))
                .when(col("result_value") > col("reference_high"),
                     spark_round(col("result_value") - col("reference_high"), 3))
                .otherwise(lit(0.0))) \
    \
    # Turnaround time in minutes: how long from collection to result?
    # This is a key lab quality metric
    .withColumn("tat_minutes",
                F.round(
                    (col("resulted_at").cast("long") - col("collected_at").cast("long")) / 60, 1
                )) \
    \
    # Data quality
    .withColumn("dq_flag",
                when(col("result_value").isNull(), lit("NULL_RESULT_VALUE"))
                .when(col("collected_at").isNull(), lit("NULL_COLLECTION_TIME"))
                .when(col("tat_minutes") < 0, lit("INVALID_TAT:negative"))
                .otherwise(lit("VALID"))) \
    \
    .withColumn("_silver_processed_at", current_timestamp()) \
    .withColumn("_silver_batch_id", lit(batch_id)) \
    .drop("_source", "_batch_id")

valid_df = silver_df.filter(col("dq_flag") == "VALID")
valid_count = valid_df.count()
print(f"Valid lab results: {valid_count:,} / {bronze_count:,}")

valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("test_name") \
    .save(STORAGE_PATHS["silver_labs"])

print("\nResult interpretation distribution:")
valid_df.groupBy("result_interpretation", "is_critical") \
    .count() \
    .orderBy("result_interpretation", "is_critical") \
    .show()

print(f"\nAverage TAT by test type (minutes):")
valid_df.groupBy("test_name") \
    .agg(F.round(F.avg("tat_minutes"), 1).alias("avg_tat_min"),
         F.count("*").alias("n")) \
    .orderBy("avg_tat_min") \
    .show(truncate=False)

duration = (datetime.now() - start_time).seconds
log_pipeline_run("transform_silver_labs", "silver", "success",
                 records_in=bronze_count, records_out=valid_count,
                 duration_secs=duration, batch_id=batch_id)

print(f"\n✓ Silver labs transformation complete in {duration}s")
spark.stop()
