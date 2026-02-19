#!/usr/bin/env python3
"""
scripts/run_pipeline_local.py
==============================
Run the COMPLETE healthcare pipeline locally using pure Python + Pandas.

This is your "day 1" script that lets you run the full Bronze → Silver → Gold
pipeline WITHOUT needing Spark or Docker. It's pedagogically identical to the
PySpark versions — same logic, same transformations, same output structure —
but uses Pandas so you can run it immediately and see results.

WHEN TO USE THIS:
  - Learning and understanding the pipeline logic
  - Quick iteration and testing on your laptop
  - Demonstrating the concept without infrastructure overhead

WHEN TO USE THE SPARK VERSION (spark_jobs/):
  - Processing data larger than your laptop's RAM
  - Running in production or staging environments
  - Validating performance characteristics

Think of this as the "toy model" and the PySpark scripts as the "production model."
The physics is the same — just the engine is different.

Run with:
  python3 scripts/run_pipeline_local.py
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# ── Output directory setup ─────────────────────────────────────────────────────
# In production this is MinIO (S3). Locally, we use a nested folder structure
# that mirrors the Bronze/Silver/Gold hierarchy exactly.
OUTPUT_BASE = "data/output"
PATHS = {
    "bronze_admissions":  f"{OUTPUT_BASE}/bronze/admissions/",
    "bronze_labs":        f"{OUTPUT_BASE}/bronze/labs/",
    "silver_admissions":  f"{OUTPUT_BASE}/silver/admissions/",
    "silver_labs":        f"{OUTPUT_BASE}/silver/labs/",
    "silver_quarantine":  f"{OUTPUT_BASE}/silver/quarantine/",
    "gold_dept_kpis":     f"{OUTPUT_BASE}/gold/dept_kpis/",
    "gold_readmissions":  f"{OUTPUT_BASE}/gold/readmissions/",
    "gold_financial":     f"{OUTPUT_BASE}/gold/financial/",
    "gold_critical_labs": f"{OUTPUT_BASE}/gold/critical_labs/",
}
for path in PATHS.values():
    os.makedirs(path, exist_ok=True)

# Pipeline metrics (we track these like a real pipeline would)
pipeline_log = []
run_start = datetime.now()

def log_step(step, layer, status, records_in=0, records_out=0, notes=""):
    duration = (datetime.now() - run_start).seconds
    entry = dict(step=step, layer=layer, status=status,
                 records_in=records_in, records_out=records_out,
                 elapsed_sec=duration, notes=notes,
                 timestamp=datetime.now().strftime("%H:%M:%S"))
    pipeline_log.append(entry)
    icon = "✓" if status == "success" else "✗"
    print(f"  {icon} [{layer.upper():6}] {step:<35} "
          f"in={records_in:>6,}  out={records_out:>6,}  ({duration}s)")

print(f"\n{'='*65}")
print(f"  HEALTHCARE DATA PIPELINE — LOCAL RUN")
print(f"  Started: {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}\n")

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 1: BRONZE — Raw ingestion with metadata
# Philosophy: capture exactly what arrived, add only audit columns
# ═══════════════════════════════════════════════════════════════════════════════
print("LAYER 1: BRONZE INGESTION")
print("─" * 45)

# Bronze: Admissions
print("  Ingesting admissions CSV...")
raw_admissions = pd.read_csv("data/admissions_2024.csv")
# The ONLY thing we add in Bronze is metadata — nothing else changes
raw_admissions["_ingested_at"] = datetime.now().isoformat()
raw_admissions["_batch_id"] = run_start.strftime("%Y%m%d_%H%M%S")
raw_admissions["_source"] = "admissions_csv_batch"
raw_admissions.to_csv(f"{PATHS['bronze_admissions']}data.csv", index=False)
log_step("ingest_admissions", "bronze", "success",
         records_in=len(raw_admissions), records_out=len(raw_admissions))

# Bronze: Labs
print("  Ingesting lab results CSV...")
raw_labs = pd.read_csv("data/lab_results_2024.csv")
raw_labs["_ingested_at"] = datetime.now().isoformat()
raw_labs["_batch_id"] = run_start.strftime("%Y%m%d_%H%M%S")
raw_labs["_source"] = "labs_csv_batch"
raw_labs.to_csv(f"{PATHS['bronze_labs']}data.csv", index=False)
log_step("ingest_labs", "bronze", "success",
         records_in=len(raw_labs), records_out=len(raw_labs))

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2: SILVER — Clean, validate, enrich
# Philosophy: fix quality issues, enforce types, add derived columns, quarantine bad rows
# ═══════════════════════════════════════════════════════════════════════════════
print("\nLAYER 2: SILVER TRANSFORMATION")
print("─" * 45)

# ── Silver: Admissions ───────────────────────────────────────────────────────
print("  Transforming admissions...")
bronze_adm = pd.read_csv(f"{PATHS['bronze_admissions']}data.csv")

# Step 1: Deduplication
before_dedup = len(bronze_adm)
silver_adm = bronze_adm.drop_duplicates(subset=["admission_id"])
print(f"    Deduplication: removed {before_dedup - len(silver_adm):,} duplicates")

# Step 2: Parse dates (string → datetime)
silver_adm["admission_date"]  = pd.to_datetime(silver_adm["admission_date"],  errors="coerce")
silver_adm["discharge_date"]  = pd.to_datetime(silver_adm["discharge_date"],  errors="coerce")

# Step 3: Standardize text
silver_adm["department"]       = silver_adm["department"].str.upper().str.strip()
silver_adm["diagnosis"]        = silver_adm["diagnosis"].str.upper().str.strip()
silver_adm["gender"]           = silver_adm["gender"].str.upper().str.strip()
silver_adm["attending_doctor"] = silver_adm["attending_doctor"].str.upper().str.strip()

# Step 4: Null handling with business rules
silver_adm["insurance_id"]  = silver_adm["insurance_id"].fillna("SELF_PAY")
silver_adm["insurer_name"]  = silver_adm["insurer_name"].fillna("SELF_PAY").str.upper()

# Step 5: Derived columns
silver_adm["length_of_stay_days"] = (
    silver_adm["discharge_date"] - silver_adm["admission_date"]
).dt.days

silver_adm["age_group"] = pd.cut(
    silver_adm["age"],
    bins=[0, 29, 44, 59, 74, 200],
    labels=["18-29", "30-44", "45-59", "60-74", "75+"],
    right=True
).astype(str)

silver_adm["cost_tier"] = pd.cut(
    silver_adm["total_cost_usd"],
    bins=[0, 5000, 20000, 50000, float("inf")],
    labels=["LOW", "MEDIUM", "HIGH", "VERY_HIGH"],
    right=True
).astype(str)

silver_adm["admission_year"]    = silver_adm["admission_date"].dt.year
silver_adm["admission_month"]   = silver_adm["admission_date"].dt.month
silver_adm["admission_quarter"] = "Q" + silver_adm["admission_date"].dt.quarter.astype(str)

# Step 6: Data quality flags
def assess_quality(row):
    if pd.isna(row["admission_date"]):  return "NULL_DATE:admission"
    if pd.isna(row["discharge_date"]):  return "NULL_DATE:discharge"
    if row["length_of_stay_days"] < 0:  return "INVALID_LOS:discharge_before_admission"
    if row["age"] < 0:                  return "INVALID_AGE:negative"
    if row["age"] > 120:                return "INVALID_AGE:unrealistic"
    if row["length_of_stay_days"] > 365: return "SUSPICIOUS_LOS:over_365_days"
    return "VALID"

silver_adm["dq_flag"] = silver_adm.apply(assess_quality, axis=1)
silver_adm["_silver_processed_at"] = datetime.now().isoformat()

# Separate valid and quarantine
valid_adm      = silver_adm[silver_adm["dq_flag"] == "VALID"].copy()
quarantine_adm = silver_adm[silver_adm["dq_flag"] != "VALID"].copy()

valid_adm.to_csv(f"{PATHS['silver_admissions']}data.csv", index=False)
quarantine_adm.to_csv(f"{PATHS['silver_quarantine']}admissions.csv", index=False)

log_step("transform_admissions", "silver", "success",
         records_in=len(bronze_adm), records_out=len(valid_adm),
         notes=f"quarantined={len(quarantine_adm)}")

print(f"    Valid: {len(valid_adm):,}  |  Quarantined: {len(quarantine_adm):,}")
if len(quarantine_adm) > 0:
    print("    Quarantine breakdown:")
    for flag, count in quarantine_adm["dq_flag"].value_counts().items():
        print(f"      {flag}: {count:,}")

# ── Silver: Labs ─────────────────────────────────────────────────────────────
print("  Transforming lab results...")
bronze_labs = pd.read_csv(f"{PATHS['bronze_labs']}data.csv")
silver_labs = bronze_labs.drop_duplicates(subset=["lab_id"])

silver_labs["collected_at"] = pd.to_datetime(silver_labs["collected_at"], errors="coerce")
silver_labs["resulted_at"]  = pd.to_datetime(silver_labs["resulted_at"],  errors="coerce")
silver_labs["test_name"]    = silver_labs["test_name"].str.upper().str.strip()
silver_labs["lab_status"]   = silver_labs["lab_status"].str.upper().str.strip()

# Key derived column: result interpretation
def interpret_result(row):
    if pd.isna(row["result_value"]):   return "UNKNOWN"
    if row["result_value"] < row["reference_low"]:  return "LOW"
    if row["result_value"] > row["reference_high"]: return "HIGH"
    return "NORMAL"

silver_labs["result_interpretation"] = silver_labs.apply(interpret_result, axis=1)

# Deviation from reference range
def deviation(row):
    if pd.isna(row["result_value"]): return 0
    if row["result_value"] < row["reference_low"]:
        return round(row["reference_low"] - row["result_value"], 3)
    if row["result_value"] > row["reference_high"]:
        return round(row["result_value"] - row["reference_high"], 3)
    return 0

silver_labs["deviation_from_range"] = silver_labs.apply(deviation, axis=1)

# Turnaround time in minutes
silver_labs["tat_minutes"] = (
    (silver_labs["resulted_at"] - silver_labs["collected_at"])
    .dt.total_seconds() / 60
).round(1)

silver_labs["dq_flag"] = silver_labs.apply(
    lambda r: "NULL_RESULT_VALUE" if pd.isna(r["result_value"])
              else "INVALID_TAT" if (not pd.isna(r["tat_minutes"]) and r["tat_minutes"] < 0)
              else "VALID", axis=1
)

valid_labs = silver_labs[silver_labs["dq_flag"] == "VALID"]
valid_labs.to_csv(f"{PATHS['silver_labs']}data.csv", index=False)
log_step("transform_labs", "silver", "success",
         records_in=len(bronze_labs), records_out=len(valid_labs))

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3: GOLD — Aggregated, business-ready analytics
# Philosophy: answer specific business questions with pre-computed aggregations
# ═══════════════════════════════════════════════════════════════════════════════
print("\nLAYER 3: GOLD AGGREGATIONS")
print("─" * 45)

silver_adm = pd.read_csv(f"{PATHS['silver_admissions']}data.csv",
                         parse_dates=["admission_date", "discharge_date"])
silver_labs = pd.read_csv(f"{PATHS['silver_labs']}data.csv",
                          parse_dates=["collected_at", "resulted_at"])

# ── Gold 1: Department Monthly KPIs ─────────────────────────────────────────
print("  Building dept_monthly_kpis...")
silver_adm["year_month"] = silver_adm["admission_date"].dt.to_period("M").astype(str)

g_kpis = silver_adm.groupby(["department", "year_month", "admission_year", "admission_quarter"]).agg(
    total_admissions=("admission_id", "count"),
    unique_patients=("patient_id", "nunique"),
    avg_los_days=("length_of_stay_days", "mean"),
    stddev_los=("length_of_stay_days", "std"),
    min_los=("length_of_stay_days", "min"),
    max_los=("length_of_stay_days", "max"),
    readmissions=("readmission", "sum"),
    avg_cost_usd=("total_cost_usd", "mean"),
    total_revenue_usd=("total_cost_usd", "sum"),
    avg_patient_age=("age", "mean"),
    icu_admissions=("icu_stay", "sum"),
).reset_index()

g_kpis["readmission_rate_pct"] = (g_kpis["readmissions"] / g_kpis["total_admissions"] * 100).round(2)
g_kpis["avg_los_days"] = g_kpis["avg_los_days"].round(2)
g_kpis["avg_cost_usd"] = g_kpis["avg_cost_usd"].round(2)
g_kpis["total_revenue_usd"] = g_kpis["total_revenue_usd"].round(2)

# Rolling 3-month average — sorted by department+month so the rolling window is correct
g_kpis = g_kpis.sort_values(["department", "year_month"])
g_kpis["rolling_3mo_avg_los"] = (
    g_kpis.groupby("department")["avg_los_days"]
    .transform(lambda x: x.rolling(3, min_periods=1).mean())
    .round(2)
)
g_kpis["rolling_3mo_readmission_pct"] = (
    g_kpis.groupby("department")["readmission_rate_pct"]
    .transform(lambda x: x.rolling(3, min_periods=1).mean())
    .round(2)
)

g_kpis["readmission_status"] = g_kpis["readmission_rate_pct"].apply(
    lambda x: "CRITICAL" if x > 20 else "WARNING" if x > 15 else "OK"
)
g_kpis["_built_at"] = datetime.now().isoformat()
g_kpis.to_csv(f"{PATHS['gold_dept_kpis']}data.csv", index=False)
log_step("build_dept_kpis", "gold", "success",
         records_in=len(silver_adm), records_out=len(g_kpis))

# ── Gold 2: Readmission Analysis ─────────────────────────────────────────────
print("  Building readmission_analysis...")
g_readmit = silver_adm.groupby(["department", "diagnosis", "age_group"]).agg(
    total_admissions=("admission_id", "count"),
    readmissions=("readmission", "sum"),
    avg_los=("length_of_stay_days", "mean"),
    avg_cost=("total_cost_usd", "mean"),
).reset_index()
g_readmit = g_readmit[g_readmit["total_admissions"] >= 5]  # Minimum sample size
g_readmit["readmission_rate_pct"] = (g_readmit["readmissions"] / g_readmit["total_admissions"] * 100).round(2)
g_readmit["readmission_risk_tier"] = g_readmit["readmission_rate_pct"].apply(
    lambda x: "HIGH_RISK" if x > 25 else "MEDIUM_RISK" if x > 15 else "LOW_RISK"
)
g_readmit.to_csv(f"{PATHS['gold_readmissions']}data.csv", index=False)
log_step("build_readmission_analysis", "gold", "success",
         records_in=len(silver_adm), records_out=len(g_readmit))

# ── Gold 3: Financial Summary ─────────────────────────────────────────────────
print("  Building financial_summary...")
g_financial = silver_adm.groupby(
    ["department", "insurer_name", "admission_year", "admission_quarter", "cost_tier"]
).agg(
    admissions=("admission_id", "count"),
    total_revenue_usd=("total_cost_usd", "sum"),
    avg_revenue_per_admission=("total_cost_usd", "mean"),
    min_cost=("total_cost_usd", "min"),
    max_cost=("total_cost_usd", "max"),
    avg_procedures=("num_procedures", "mean"),
).reset_index().round(2)
g_financial.to_csv(f"{PATHS['gold_financial']}data.csv", index=False)
log_step("build_financial_summary", "gold", "success",
         records_in=len(silver_adm), records_out=len(g_financial))

# ── Gold 4: Critical Labs Watch ──────────────────────────────────────────────
print("  Building critical_labs_watch...")
critical_labs = silver_labs[silver_labs["is_critical"] == True].copy()
# Join with admissions to get department and doctor info
adm_lookup = silver_adm[["admission_id", "department", "attending_doctor", "admission_date"]].copy()
g_critical = critical_labs.merge(adm_lookup, on="admission_id", how="left")
g_critical["_built_at"] = datetime.now().isoformat()
g_critical.to_csv(f"{PATHS['gold_critical_labs']}data.csv", index=False)
log_step("build_critical_labs_watch", "gold", "success",
         records_in=len(silver_labs), records_out=len(g_critical))

# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE SUMMARY REPORT
# ═══════════════════════════════════════════════════════════════════════════════
total_duration = (datetime.now() - run_start).seconds
print(f"\n{'='*65}")
print(f"  PIPELINE COMPLETE in {total_duration}s")
print(f"{'='*65}")

print(f"\n  📊 GOLD TABLE PREVIEW — Department KPIs (2024 Annual Summary):")
print("─" * 65)
annual = g_kpis.groupby("department").agg(
    total_admissions=("total_admissions", "sum"),
    avg_los=("avg_los_days", "mean"),
    readmission_rate=("readmission_rate_pct", "mean"),
    total_revenue_M=("total_revenue_usd", "sum"),
).reset_index()
annual["avg_los"] = annual["avg_los"].round(2)
annual["readmission_rate"] = annual["readmission_rate"].round(2)
annual["total_revenue_M"] = (annual["total_revenue_M"] / 1e6).round(2)
print(annual.to_string(index=False))

print(f"\n  ⚠  CRITICAL ALERTS:")
print("─" * 65)
print(f"  Critical lab values needing follow-up: {len(g_critical):,}")
high_risk_depts = g_kpis[g_kpis["readmission_status"] == "CRITICAL"]["department"].unique()
if len(high_risk_depts) > 0:
    print(f"  Departments with critical readmission rate: {', '.join(high_risk_depts)}")
else:
    print(f"  No departments with critical readmission rates this period.")

print(f"\n  📁 OUTPUT FILES WRITTEN:")
print("─" * 65)
for name, path in PATHS.items():
    parquet_path = path + "data.csv"
    if os.path.exists(parquet_path):
        size_kb = os.path.getsize(parquet_path) / 1024
        print(f"  {name:<30} {path}  ({size_kb:.0f} KB)")

# Save the audit log as JSON
with open(f"{OUTPUT_BASE}/pipeline_audit_log.json", "w") as f:
    json.dump(pipeline_log, f, indent=2)
print(f"\n  Audit log: {OUTPUT_BASE}/pipeline_audit_log.json")
print(f"\n  Next step: run  python3 scripts/explore_results.py  to query the Gold tables")
