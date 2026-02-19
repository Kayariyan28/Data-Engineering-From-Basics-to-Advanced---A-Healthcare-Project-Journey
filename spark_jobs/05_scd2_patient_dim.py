"""
SILVER LAYER — SCD Type 2: Patient Dimension
============================================
Purpose: Demonstrate Slowly Changing Dimensions (SCD2) using Delta MERGE.

This is one of the most important data engineering patterns to understand.
Here's the problem it solves:

  A patient changes their insurance from BlueCross to Aetna on March 15th.
  They were admitted on Feb 1st (BlueCross) and May 1st (Aetna).
  
  Without SCD2: Updating the patient record loses history. Both admissions
               now show Aetna, which is WRONG for the Feb 1st admission.
  
  With SCD2: We keep BOTH records:
    Row 1: patient=P12345, insurer=BlueCross, valid_from=2020-01-01,
           valid_to=2024-03-14, is_current=False
    Row 2: patient=P12345, insurer=Aetna,     valid_from=2024-03-15,
           valid_to=9999-12-31, is_current=True
  
  Now when you join admissions to the patient dimension using:
    JOIN ON patient_id AND admission_date BETWEEN valid_from AND valid_to
  ...each admission automatically gets the correct insurer at that time.

This is the pattern that separates junior from senior data engineers.

Run with:
  spark-submit spark_jobs/05_scd2_patient_dim.py
"""

import sys, os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import build_spark_session, STORAGE_PATHS, log_pipeline_run

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from delta.tables import DeltaTable

spark = build_spark_session("Silver_Patient_Dim_SCD2")
start_time = datetime.now()

print(f"\n{'='*60}")
print(f"  SCD Type 2: Patient Dimension")
print(f"{'='*60}\n")

# ── Build the patient dimension from admissions ───────────────────────────────
# In a real system this would come from an EMR (Electronic Medical Record) system.
# We derive it from our admissions data for this exercise.
print("Building patient dimension from Silver admissions...")
admissions_df = spark.read.format("delta").load(STORAGE_PATHS["silver_admissions"])

# Get the most recent state of each patient
patient_dim_incoming = admissions_df \
    .groupBy("patient_id") \
    .agg(
        F.last("age",          ignorenulls=True).alias("age"),
        F.last("gender",       ignorenulls=True).alias("gender"),
        F.last("insurance_id", ignorenulls=True).alias("insurance_id"),
        F.last("insurer_name", ignorenulls=True).alias("insurer_name"),
        F.count("*").alias("total_admissions"),
        F.max("admission_date").alias("last_admission_date"),
    ) \
    .withColumn("valid_from", current_timestamp()) \
    .withColumn("valid_to",   lit("9999-12-31 00:00:00").cast("timestamp")) \
    .withColumn("is_current", lit(True))

dim_path = STORAGE_PATHS["silver_patient_dim"]

# Check if the dimension table already exists
# If it doesn't, just write it fresh. If it does, perform the MERGE.
import os
table_exists = os.path.exists(dim_path.replace("s3a://healthcare/", "/tmp/healthcare/")) \
               if dim_path.startswith("s3a://") else os.path.exists(dim_path)

# For local file mode
local_dim_path = "data/output/silver/patient_dim"
os.makedirs(local_dim_path, exist_ok=True)

try:
    existing_table = DeltaTable.forPath(spark, local_dim_path)
    table_exists = True
    print("Existing dimension table found — performing MERGE (upsert)...")
except Exception:
    table_exists = False
    print("No existing dimension table — creating fresh...")

# Update path references to local for demo
patient_dim_incoming_local = patient_dim_incoming

if not table_exists:
    # First run: just write the table
    patient_dim_incoming_local.write \
        .format("delta") \
        .mode("overwrite") \
        .save(local_dim_path)
    print(f"Created patient dimension: {patient_dim_incoming_local.count():,} patients")
else:
    # Subsequent runs: perform SCD2 MERGE
    # Step 1: Expire current records for patients whose attributes changed
    # Step 2: Insert the new current version
    dim_table = DeltaTable.forPath(spark, local_dim_path)
    
    dim_table.alias("existing").merge(
        patient_dim_incoming_local.alias("incoming"),
        # Match on business key WHERE the row is still current
        "existing.patient_id = incoming.patient_id AND existing.is_current = true"
    ).whenMatchedUpdate(
        # Only expire if something actually changed
        condition="""
            existing.insurance_id != incoming.insurance_id OR
            existing.insurer_name != incoming.insurer_name
        """,
        set={
            "is_current": "false",
            "valid_to":   "current_timestamp()"
        }
    ).whenNotMatchedInsert(
        values={
            "patient_id":        "incoming.patient_id",
            "age":               "incoming.age",
            "gender":            "incoming.gender",
            "insurance_id":      "incoming.insurance_id",
            "insurer_name":      "incoming.insurer_name",
            "total_admissions":  "incoming.total_admissions",
            "last_admission_date": "incoming.last_admission_date",
            "valid_from":        "current_timestamp()",
            "valid_to":          "cast('9999-12-31' as timestamp)",
            "is_current":        "true",
        }
    ).execute()
    
    total = spark.read.format("delta").load(local_dim_path).count()
    current = spark.read.format("delta").load(local_dim_path).filter("is_current = true").count()
    expired = total - current
    print(f"MERGE complete: {total:,} total rows | {current:,} current | {expired:,} historical versions")

# Show a sample of the dimension
result = spark.read.format("delta").load(local_dim_path)
print("\nSample patient dimension records:")
result.select("patient_id", "insurer_name", "total_admissions", "is_current", "valid_from", "valid_to") \
      .show(5, truncate=False)

duration = (datetime.now() - start_time).seconds
log_pipeline_run("scd2_patient_dim", "silver", "success",
                 records_in=patient_dim_incoming_local.count(),
                 records_out=result.count(),
                 duration_secs=duration)

print(f"\n✓ SCD2 patient dimension complete in {duration}s")
spark.stop()
