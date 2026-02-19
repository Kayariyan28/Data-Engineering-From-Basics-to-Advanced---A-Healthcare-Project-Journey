"""
GOLD LAYER — Healthcare KPI Analytics
======================================
Purpose: Build business-ready aggregated tables from Silver data.

The Gold layer answers specific business questions. Unlike Silver (which is
normalized and granular), Gold tables are pre-aggregated and optimized for
the exact queries analysts, dashboards, and reports will run.

Think of it this way:
  Bronze = raw ingredients
  Silver = clean, prepared ingredients
  Gold   = finished dishes — ready to serve

This script builds 4 Gold tables:
  1. dept_monthly_kpis    — core operational metrics per department per month
  2. readmission_analysis — drill-down into readmission patterns
  3. financial_summary    — revenue/cost analytics
  4. critical_labs_watch  — patients with critical lab values (safety KPI)

Run with:
  spark-submit spark_jobs/06_build_gold_layer.py
"""

import sys, os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import build_spark_session, STORAGE_PATHS, log_pipeline_run

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, round as spark_round, current_timestamp
from pyspark.sql.window import Window

spark = build_spark_session("Gold_Layer_Build")
start_time = datetime.now()

print(f"\n{'='*60}")
print(f"  Gold Layer Build  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print(f"{'='*60}\n")

# ── Load Silver Tables ───────────────────────────────────────────────────────
print("Loading Silver tables...")
admissions = spark.read.format("delta").load(STORAGE_PATHS["silver_admissions"])
labs        = spark.read.format("delta").load(STORAGE_PATHS["silver_labs"])
print(f"  Admissions: {admissions.count():,} rows")
print(f"  Labs:       {labs.count():,} rows")

# ── Gold Table 1: Department Monthly KPIs ────────────────────────────────────
# This is the bread-and-butter table for hospital operations teams.
# Every metric here is a question a department head asks regularly:
#   - How full are we? (admissions volume)
#   - How long are patients staying? (avg_los vs. target)
#   - Are patients coming back too soon? (readmission rate)
#   - How much does care cost? (avg_cost)
print("\nBuilding Gold Table 1: Department Monthly KPIs...")

# Window spec for rolling averages — this lets us compute "last 3 months average"
# without a separate self-join. The ROWS BETWEEN clause defines the window frame.
dept_window = Window.partitionBy("department") \
                    .orderBy("year_month") \
                    .rowsBetween(-2, 0)  # current row and 2 rows before = 3 months

dept_kpis = admissions \
    .groupBy(
        "department",
        F.date_format("admission_date", "yyyy-MM").alias("year_month"),
        "admission_year",
        "admission_quarter",
    ) \
    .agg(
        F.count("*").alias("total_admissions"),
        F.countDistinct("patient_id").alias("unique_patients"),
        spark_round(F.avg("length_of_stay_days"), 2).alias("avg_los_days"),
        spark_round(F.stddev("length_of_stay_days"), 2).alias("stddev_los_days"),
        F.min("length_of_stay_days").alias("min_los_days"),
        F.max("length_of_stay_days").alias("max_los_days"),
        spark_round(F.sum(when(col("readmission"), 1).otherwise(0)) * 100.0 / F.count("*"), 2).alias("readmission_rate_pct"),
        spark_round(F.avg("total_cost_usd"), 2).alias("avg_cost_usd"),
        spark_round(F.sum("total_cost_usd"), 2).alias("total_revenue_usd"),
        spark_round(F.avg("age"), 1).alias("avg_patient_age"),
        F.sum(when(col("icu_stay"), 1).otherwise(0)).alias("icu_admissions"),
    ) \
    .withColumn("rolling_3mo_avg_los",
                spark_round(F.avg("avg_los_days").over(dept_window), 2)) \
    .withColumn("rolling_3mo_readmission_pct",
                spark_round(F.avg("readmission_rate_pct").over(dept_window), 2)) \
    .withColumn("_built_at", current_timestamp())

gold_path_1 = STORAGE_PATHS["gold_dept_kpis"]
dept_kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path_1)
print(f"  Written: {dept_kpis.count():,} rows to dept_monthly_kpis")

print("\n  Preview — Top departments by average LOS:")
dept_kpis.groupBy("department") \
    .agg(spark_round(F.avg("avg_los_days"), 2).alias("annual_avg_los"),
         F.sum("total_admissions").alias("total_admissions")) \
    .orderBy("annual_avg_los", ascending=False) \
    .show(truncate=False)

# ── Gold Table 2: Readmission Analysis ───────────────────────────────────────
# Readmissions are expensive (hospitals may be penalized) and indicate
# patients didn't receive adequate care or follow-up.
# This table helps care teams identify which diagnoses drive readmissions.
print("Building Gold Table 2: Readmission Analysis...")

readmission_analysis = admissions \
    .groupBy("department", "diagnosis", "age_group") \
    .agg(
        F.count("*").alias("total_admissions"),
        F.sum(when(col("readmission"), 1).otherwise(0)).alias("readmissions"),
        spark_round(
            F.sum(when(col("readmission"), 1).otherwise(0)) * 100.0 / F.count("*"), 2
        ).alias("readmission_rate_pct"),
        spark_round(F.avg("length_of_stay_days"), 2).alias("avg_los"),
        spark_round(F.avg("total_cost_usd"), 2).alias("avg_cost"),
    ) \
    .filter(col("total_admissions") >= 10)  # Only include statistically meaningful groups \
    .withColumn("readmission_risk_tier",
                when(col("readmission_rate_pct") > 25, lit("HIGH_RISK"))
                .when(col("readmission_rate_pct") > 15, lit("MEDIUM_RISK"))
                .otherwise(lit("LOW_RISK"))) \
    .withColumn("_built_at", current_timestamp())

gold_path_2 = STORAGE_PATHS["gold_readmissions"]
readmission_analysis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path_2)
print(f"  Written: {readmission_analysis.count():,} rows to readmission_analysis")

print("\n  High-risk readmission groups:")
readmission_analysis \
    .filter(col("readmission_risk_tier") == "HIGH_RISK") \
    .select("department", "diagnosis", "age_group", "readmission_rate_pct", "total_admissions") \
    .orderBy("readmission_rate_pct", ascending=False) \
    .show(10, truncate=False)

# ── Gold Table 3: Financial Summary ──────────────────────────────────────────
# Finance teams need costs sliced by insurer, department, and time.
print("Building Gold Table 3: Financial Summary...")

financial_summary = admissions \
    .groupBy("department", "insurer_name", "admission_year", "admission_quarter", "cost_tier") \
    .agg(
        F.count("*").alias("admissions"),
        spark_round(F.sum("total_cost_usd"), 2).alias("total_revenue_usd"),
        spark_round(F.avg("total_cost_usd"), 2).alias("avg_revenue_per_admission"),
        spark_round(F.min("total_cost_usd"), 2).alias("min_cost"),
        spark_round(F.max("total_cost_usd"), 2).alias("max_cost"),
        spark_round(F.avg("num_procedures"), 2).alias("avg_procedures"),
    ) \
    .withColumn("_built_at", current_timestamp())

gold_path_3 = STORAGE_PATHS["gold_financial"]
financial_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path_3)
print(f"  Written: {financial_summary.count():,} rows to financial_summary")

print("\n  Revenue by department:")
financial_summary.groupBy("department") \
    .agg(spark_round(F.sum("total_revenue_usd") / 1e6, 2).alias("total_revenue_millions_usd")) \
    .orderBy("total_revenue_millions_usd", ascending=False) \
    .show(truncate=False)

# ── Gold Table 4: Critical Labs Watch ────────────────────────────────────────
# Safety-critical: any patient with abnormal lab values needs rapid follow-up.
# This table is consumed by a clinical alerting system.
print("Building Gold Table 4: Critical Labs Watch...")

critical_labs = labs \
    .filter(col("is_critical") == True) \
    .join(
        admissions.select("admission_id", "patient_id", "department", "attending_doctor", "admission_date"),
        on="admission_id",
        how="left"
    ) \
    .select(
        "lab_id", "patient_id", "admission_id", "department",
        "attending_doctor", "test_name", "marker",
        "result_value", "reference_high", "unit",
        "result_interpretation", "deviation_from_range",
        "collected_at", "resulted_at", "tat_minutes",
        "admission_date",
    ) \
    .withColumn("hours_since_collection",
                spark_round((current_timestamp().cast("long") - col("collected_at").cast("long")) / 3600, 1)) \
    .withColumn("_built_at", current_timestamp())

gold_path_4 = STORAGE_PATHS["gold_critical_labs"]
critical_labs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path_4)
critical_count = critical_labs.count()
print(f"  Written: {critical_count:,} critical lab alerts")

print("\n  Critical labs by department:")
critical_labs.groupBy("department", "test_name") \
    .agg(F.count("*").alias("critical_results"),
         spark_round(F.avg("deviation_from_range"), 2).alias("avg_deviation")) \
    .orderBy("critical_results", ascending=False) \
    .show(10, truncate=False)

# ── Final Summary ────────────────────────────────────────────────────────────
duration = (datetime.now() - start_time).seconds
print(f"\n{'='*60}")
print(f"  Gold Layer Build Complete in {duration}s")
print(f"  Tables built:")
print(f"    dept_monthly_kpis   → {dept_kpis.count():,} rows")
print(f"    readmission_analysis→ {readmission_analysis.count():,} rows")
print(f"    financial_summary   → {financial_summary.count():,} rows")
print(f"    critical_labs_watch → {critical_count:,} rows")
print(f"{'='*60}\n")

log_pipeline_run("build_gold_layer", "gold", "success",
                 records_in=admissions.count() + labs.count(),
                 records_out=dept_kpis.count() + readmission_analysis.count() + critical_count,
                 duration_secs=duration)

spark.stop()
