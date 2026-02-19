"""
SILVER LAYER — Admissions Transformation
=========================================
Purpose: Clean, validate, standardize, and enrich Bronze admissions data.

The Silver layer is where data engineering skill really shows. Think of it as
the difference between raw ingredients (Bronze) and mise en place (Silver) —
everything is cleaned, measured, and ready to use.

Key operations in this script:
  1. Deduplication        — remove exact duplicate records
  2. Type casting          — string → proper date/numeric types
  3. Standardization       — consistent casing, trimming whitespace
  4. Null handling         — business-rule-based imputation
  5. Derived columns       — computed fields like length_of_stay
  6. Data quality scoring  — flag bad records instead of silently dropping them
  7. MERGE (upsert)        — idempotent writes using Delta's MERGE operation

Run with:
  spark-submit spark_jobs/03_transform_silver_admissions.py
"""

import sys, os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import build_spark_session, STORAGE_PATHS, log_pipeline_run

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, upper, trim, to_date, datediff,
    current_timestamp, coalesce, regexp_replace, length
)
from delta.tables import DeltaTable

spark = build_spark_session("Silver_Admissions_Transform")
start_time = datetime.now()
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"\n{'='*60}")
print(f"  Silver Transformation: Admissions  |  Batch: {batch_id}")
print(f"{'='*60}\n")

# ── 1. Read from Bronze ──────────────────────────────────────────────────────
# We read the full Bronze table. In a real production system you'd use
# incremental processing — only reading rows with _ingested_at > last_run_time.
# We'll keep it simple here so you can see the full transformation logic.
print("Reading from Bronze layer...")
bronze_df = spark.read.format("delta").load(STORAGE_PATHS["bronze_admissions"])
bronze_count = bronze_df.count()
print(f"  Bronze rows: {bronze_count:,}")

# ── 2. Deduplication ─────────────────────────────────────────────────────────
# admission_id should be unique per business rules. dropDuplicates on the
# natural key ensures we only keep one record per admission.
# We keep the most recently ingested record if duplicates exist.
print("\nStep 1: Deduplication...")
deduped_df = bronze_df \
    .orderBy(col("_ingested_at").desc()) \
    .dropDuplicates(["admission_id"])

dup_count = bronze_count - deduped_df.count()
print(f"  Removed {dup_count:,} duplicate records")

# ── 3. Type Casting & Parsing ────────────────────────────────────────────────
# Dates stored as strings are a silent landmine. to_date() returns null
# if the format doesn't match — which surfaces data quality issues immediately
# rather than hiding them in string comparisons.
print("Step 2: Casting types and parsing dates...")
typed_df = deduped_df \
    .withColumn("admission_date",  to_date(col("admission_date"),  "yyyy-MM-dd")) \
    .withColumn("discharge_date",  to_date(col("discharge_date"),  "yyyy-MM-dd")) \
    .withColumn("age",             col("age").cast("integer")) \
    .withColumn("total_cost_usd",  col("total_cost_usd").cast("double")) \
    .withColumn("num_procedures",  col("num_procedures").cast("integer")) \
    .withColumn("readmission",     col("readmission").cast("boolean")) \
    .withColumn("icu_stay",        col("icu_stay").cast("boolean"))

# ── 4. Standardization ───────────────────────────────────────────────────────
# "Cardiology ", "cardiology", "CARDIOLOGY" are all the same department.
# upper(trim()) normalizes all three. This is critical for GROUP BY accuracy —
# without it, your aggregations would be wrong in subtle ways.
print("Step 3: Standardizing text fields...")
standardized_df = typed_df \
    .withColumn("department",       upper(trim(col("department")))) \
    .withColumn("diagnosis",        upper(trim(col("diagnosis")))) \
    .withColumn("gender",           upper(trim(col("gender")))) \
    .withColumn("attending_doctor", upper(trim(col("attending_doctor")))) \
    .withColumn("insurer_name",     upper(trim(coalesce(col("insurer_name"), lit("UNKNOWN")))))

# ── 5. Null Handling ─────────────────────────────────────────────────────────
# Never just drop nulls — in healthcare, missing data often means something.
# "No insurance_id" means self-pay, which is a business-meaningful category.
# Always use domain knowledge to decide the correct null treatment.
print("Step 4: Handling nulls with business rules...")
null_handled_df = standardized_df \
    .withColumn("insurance_id",
                when(col("insurance_id").isNull(), lit("SELF_PAY"))
                .otherwise(col("insurance_id"))) \
    .withColumn("insurer_name",
                when(col("insurer_name") == "UNKNOWN", lit("SELF_PAY"))
                .otherwise(col("insurer_name")))

# ── 6. Derived Columns ───────────────────────────────────────────────────────
# Derived columns are computed from other columns and capture business logic
# that analysts would otherwise re-implement in every query.
print("Step 5: Computing derived columns...")
enriched_df = null_handled_df \
    .withColumn("length_of_stay_days",
                datediff(col("discharge_date"), col("admission_date"))) \
    \
    .withColumn("age_group",
                when(col("age") < 30, "18-29")
                .when(col("age") < 45, "30-44")
                .when(col("age") < 60, "45-59")
                .when(col("age") < 75, "60-74")
                .otherwise("75+")) \
    \
    .withColumn("cost_tier",
                when(col("total_cost_usd") < 5000,  "LOW")
                .when(col("total_cost_usd") < 20000, "MEDIUM")
                .when(col("total_cost_usd") < 50000, "HIGH")
                .otherwise("VERY_HIGH")) \
    \
    .withColumn("admission_year",  F.year(col("admission_date"))) \
    .withColumn("admission_month", F.month(col("admission_date"))) \
    .withColumn("admission_quarter",
                F.concat(lit("Q"), F.ceil(F.month(col("admission_date")) / 3).cast("string")))

# ── 7. Data Quality Flags ────────────────────────────────────────────────────
# Instead of silently dropping bad records, we FLAG them. This lets:
#   a) Analysts understand data completeness
#   b) Engineers investigate root causes
#   c) Downstream jobs filter to only VALID records
# Think of it as a "confidence score" for each row.
print("Step 6: Applying data quality rules...")
quality_df = enriched_df \
    .withColumn("dq_flag",
                when(col("length_of_stay_days") < 0,
                     lit("INVALID_LOS:discharge_before_admission"))
                .when(col("age") < 0,
                     lit("INVALID_AGE:negative"))
                .when(col("age") > 120,
                     lit("INVALID_AGE:unrealistic"))
                .when(col("admission_date").isNull(),
                     lit("NULL_DATE:admission"))
                .when(col("discharge_date").isNull(),
                     lit("NULL_DATE:discharge"))
                .when(col("length_of_stay_days") > 365,
                     lit("SUSPICIOUS_LOS:over_365_days"))
                .when(col("total_cost_usd") <= 0,
                     lit("INVALID_COST:non_positive"))
                .otherwise(lit("VALID"))) \
    .withColumn("_silver_processed_at", current_timestamp()) \
    .withColumn("_silver_batch_id", lit(batch_id)) \
    \
    # Remove Bronze-only metadata columns from Silver
    .drop("_source", "_batch_id")

# ── 8. Separate Valid vs Quarantined Records ─────────────────────────────────
# This is a key data engineering pattern: the quarantine table.
# Bad records go here, NOT into the trash. Someone will investigate them later.
valid_df      = quality_df.filter(col("dq_flag") == "VALID")
quarantine_df = quality_df.filter(col("dq_flag") != "VALID")

valid_count     = valid_df.count()
quarantine_count = quarantine_df.count()

print(f"\nData Quality Summary:")
print(f"  Valid records:      {valid_count:,} ({100*valid_count/bronze_count:.1f}%)")
print(f"  Quarantined records: {quarantine_count:,} ({100*quarantine_count/bronze_count:.1f}%)")

# Show the breakdown of quality issues
print("\nQuarantine breakdown:")
quarantine_df.groupBy("dq_flag").count().orderBy("count", ascending=False).show(truncate=False)

# ── 9. Write Silver Table ────────────────────────────────────────────────────
# We use overwrite here for simplicity. In production you'd use MERGE
# to perform an upsert — update existing records, insert new ones.
# See the SCD2 example in 05_scd2_patients.py for the full MERGE pattern.
print("\nWriting valid records to Silver layer...")
valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("department", "admission_year") \
    .save(STORAGE_PATHS["silver_admissions"])

# Write quarantined records to their own table
quarantine_df.write \
    .format("delta") \
    .mode("append") \
    .save(STORAGE_PATHS["silver_quarantine"])

# ── 10. Verify and Print Statistics ─────────────────────────────────────────
verify_df = spark.read.format("delta").load(STORAGE_PATHS["silver_admissions"])
print(f"\nSilver table written: {verify_df.count():,} rows")

print("\nSample Silver statistics by department:")
verify_df.groupBy("department") \
    .agg(
        F.count("*").alias("admissions"),
        F.round(F.avg("length_of_stay_days"), 2).alias("avg_los"),
        F.round(F.avg("total_cost_usd"), 0).alias("avg_cost_usd"),
        F.round(F.avg("age"), 1).alias("avg_age"),
        F.sum(F.when(col("readmission"), 1).otherwise(0)).alias("readmissions"),
    ) \
    .orderBy("department") \
    .show(truncate=False)

duration = (datetime.now() - start_time).seconds
log_pipeline_run("transform_silver_admissions", "silver", "success",
                 records_in=bronze_count, records_out=valid_count,
                 duration_secs=duration, batch_id=batch_id)

print(f"\n✓ Silver transformation complete in {duration}s")
spark.stop()
