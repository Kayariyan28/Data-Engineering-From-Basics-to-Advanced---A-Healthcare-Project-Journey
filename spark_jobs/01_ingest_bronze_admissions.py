"""
BRONZE LAYER — Admissions Ingestion
====================================
Purpose: Read raw CSV files and land them in the Bronze Delta Lake table with
         zero transformation. Bronze is your "raw vault" — data exactly as received.

Why Delta Lake here?  Plain Parquet files are immutable. Delta adds:
  - ACID transactions (safe concurrent writes)
  - Time travel (query data as it was yesterday)
  - Schema enforcement (catch unexpected column changes)
  - Audit history (_delta_log folder tracks every operation)

Run with:
  spark-submit spark_jobs/01_ingest_bronze_admissions.py
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import build_spark_session, STORAGE_PATHS, log_pipeline_run

# ── 1. Start Spark ──────────────────────────────────────────────────────────
spark = build_spark_session("Bronze_Admissions_Ingestion")
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
start_time = datetime.now()
print(f"\n{'='*60}")
print(f"  Bronze Ingestion: Admissions  |  Batch: {batch_id}")
print(f"{'='*60}\n")

# ── 2. Read Raw CSV ──────────────────────────────────────────────────────────
# We use inferSchema=True here intentionally. In Bronze we want to capture
# exactly what's in the file — even "wrong" types are preserved as strings
# if needed. The Silver layer is where we enforce proper types.
print("Reading raw admissions CSV...")
raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "")       \
    .csv("data/admissions_2024.csv")

raw_count = raw_df.count()
print(f"  Loaded {raw_count:,} rows from CSV")
print(f"  Columns: {raw_df.columns}")

# ── 3. Add Metadata (Audit Columns) ─────────────────────────────────────────
# These metadata columns are the secret power of Bronze:
#   _ingested_at  → when did this record arrive in our platform?
#   _source_file  → which file did it come from? (great for reprocessing)
#   _batch_id     → which pipeline run created this row?
# These are prefixed with _ to distinguish them from business columns.
from pyspark.sql.functions import current_timestamp, lit, input_file_name

bronze_df = raw_df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_source", lit("admissions_csv_batch"))

# ── 4. Write to Bronze Delta Lake ────────────────────────────────────────────
# mode("append") is critical — we never overwrite Bronze.
# If we ever re-run this job (e.g., after a bug fix), old records are still there.
# partitionBy("department") means Spark creates separate folders for each
# department, so queries like "show me only ICU data" read far less data.
print(f"\nWriting {raw_count:,} rows to Bronze Delta table...")
bronze_path = STORAGE_PATHS["bronze_admissions"]

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("department") \
    .save(bronze_path)

# ── 5. Verify the Write ──────────────────────────────────────────────────────
# Always verify. Silent failures are the most dangerous kind.
verify_df = spark.read.format("delta").load(bronze_path)
final_count = verify_df.count()
print(f"  Bronze table now contains {final_count:,} total rows")
print(f"  Partitions (departments): {verify_df.select('department').distinct().count()}")

# ── 6. Show Delta History (the audit trail) ──────────────────────────────────
# This is unique to Delta Lake — you can see every operation ever performed
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, bronze_path)
print("\nDelta Lake History (last 3 operations):")
delta_table.history(3).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# ── 7. Log to Audit Table ────────────────────────────────────────────────────
duration = (datetime.now() - start_time).seconds
log_pipeline_run("ingest_bronze_admissions", "bronze", "success",
                 records_in=raw_count, records_out=raw_count,
                 duration_secs=duration, batch_id=batch_id)

print(f"\n✓ Bronze ingestion complete in {duration}s")
spark.stop()
