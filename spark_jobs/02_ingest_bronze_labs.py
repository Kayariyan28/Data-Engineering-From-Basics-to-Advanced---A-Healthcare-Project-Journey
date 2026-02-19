"""
BRONZE LAYER — Lab Results Ingestion
=====================================
Purpose: Ingest lab results CSV into Bronze Delta Lake.
         Same philosophy as admissions: raw, unmodified, with metadata.

Run with:
  spark-submit spark_jobs/02_ingest_bronze_labs.py
"""

import sys, os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import build_spark_session, STORAGE_PATHS, log_pipeline_run
from pyspark.sql.functions import current_timestamp, lit

spark = build_spark_session("Bronze_Labs_Ingestion")
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
start_time = datetime.now()

print(f"\n{'='*60}")
print(f"  Bronze Ingestion: Lab Results  |  Batch: {batch_id}")
print(f"{'='*60}\n")

raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "") \
    .csv("data/lab_results_2024.csv")

raw_count = raw_df.count()
print(f"Loaded {raw_count:,} lab result rows")

bronze_df = raw_df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_source", lit("lab_results_csv_batch"))

# Partition by test_name — analysts almost always filter by specific test types
# e.g., "give me all Troponin results" — partitioning makes this very fast
bronze_path = STORAGE_PATHS["bronze_labs"]
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("test_name") \
    .save(bronze_path)

verify_count = spark.read.format("delta").load(bronze_path).count()
print(f"Bronze labs table: {verify_count:,} total rows")

duration = (datetime.now() - start_time).seconds
log_pipeline_run("ingest_bronze_labs", "bronze", "success",
                 records_in=raw_count, records_out=raw_count,
                 duration_secs=duration, batch_id=batch_id)

print(f"\n✓ Bronze labs ingestion complete in {duration}s")
spark.stop()
