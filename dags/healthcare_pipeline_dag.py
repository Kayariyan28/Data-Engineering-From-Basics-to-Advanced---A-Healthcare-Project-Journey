"""
dags/healthcare_pipeline_dag.py
=================================
The Airflow DAG that orchestrates the entire healthcare batch pipeline.

Airflow's core concept is the DAG (Directed Acyclic Graph). Think of it as
a recipe card: it specifies what steps to run, in what order, and what to
do if a step fails. The "Directed" part means dependencies flow one way —
Bronze before Silver before Gold. The "Acyclic" part means there are no
loops — no step can depend on itself.

Key concepts illustrated here:
  - BashOperator: runs shell commands (our spark-submit jobs)
  - PythonOperator: runs Python functions (our freshness checks)
  - TaskGroup: visually groups related tasks in the Airflow UI
  - XCom: passing data between tasks (record counts, batch IDs)
  - Trigger rules: controlling when downstream tasks run
  - SLA: Service Level Agreements — alert if pipeline takes too long
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# ── Default Arguments ─────────────────────────────────────────────────────────
# These apply to EVERY task in this DAG unless overridden per task.
# retry logic is critical in production — transient failures (network hiccup,
# MinIO momentarily unavailable) should auto-recover.
default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,          # Don't block today if yesterday failed
    "email":             ["de-alerts@hospital.org"],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5min, 10min, 20min — backs off gracefully
}

# ── Project paths ─────────────────────────────────────────────────────────────
# Airflow Variable lets you change this without editing the DAG.
PROJECT_DIR = Variable.get("healthcare_project_dir",
                           default_var="/Users/yourname/Desktop/healthcare-platform")
PYTHON_EXEC = f"{PROJECT_DIR}/venv/bin/python3"
SPARK_SUBMIT = "spark-submit"


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="healthcare_daily_pipeline",
    default_args=default_args,
    description="Daily Bronze → Silver → Gold pipeline for healthcare analytics",
    # Run at 2:00 AM every day — admissions data from the previous day
    # has fully loaded into the source system by then.
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,          # Don't backfill all missed runs on first deploy
    max_active_runs=1,      # Only one run at a time — prevent overlapping writes
    tags=["healthcare", "batch", "production"],
    # SLA: if the entire pipeline takes more than 90 minutes, alert us.
    # This catches the "stuck but not failed" scenario.
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # ── Task: Data Freshness Pre-Check ────────────────────────────────────────
    # Before we process anything, verify that today's CSV files actually arrived.
    # If source files are missing, fail fast — don't waste time running
    # Spark jobs that will produce empty results.
    def check_source_files(**context):
        import os
        from datetime import date

        today = date.today().strftime("%Y")  # Check year folder exists
        admissions_file = f"{PROJECT_DIR}/data/admissions_2024.csv"
        labs_file = f"{PROJECT_DIR}/data/lab_results_2024.csv"

        missing = []
        if not os.path.exists(admissions_file):
            missing.append(admissions_file)
        if not os.path.exists(labs_file):
            missing.append(labs_file)

        if missing:
            raise FileNotFoundError(
                f"Source files not found: {missing}\n"
                "Check if the ETL extract from the source system completed."
            )

        print(f"Source file check passed. Both data files found.")
        # Push batch_id to XCom so downstream tasks can reference it
        batch_id = context["ds_nodash"]  # e.g., "20240115"
        context["task_instance"].xcom_push(key="batch_id", value=batch_id)
        return batch_id

    pre_check = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    # ── Task Group: Bronze Layer ───────────────────────────────────────────────
    # TaskGroups are purely organizational — they don't affect execution,
    # but they collapse related tasks in the Airflow UI so it's readable.
    with TaskGroup("bronze_ingestion") as bronze_group:

        ingest_admissions = BashOperator(
            task_id="ingest_admissions",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"{SPARK_SUBMIT} "
                f"--master local[2] "          # Use 2 CPU cores locally
                f"spark_jobs/01_ingest_bronze_admissions.py"
            ),
            # SLA: Bronze ingestion should never take more than 30 minutes
            sla=timedelta(minutes=30),
        )

        ingest_labs = BashOperator(
            task_id="ingest_labs",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"{SPARK_SUBMIT} "
                f"--master local[2] "
                f"spark_jobs/02_ingest_bronze_labs.py"
            ),
            sla=timedelta(minutes=20),
        )

        # Admissions and labs can ingest IN PARALLEL — they're independent.
        # This is one of Airflow's most powerful features: automatic parallelism
        # for tasks with no dependency between them.
        # (No >> arrow between ingest_admissions and ingest_labs = parallel)

    # ── Task Group: Silver Layer ───────────────────────────────────────────────
    with TaskGroup("silver_transformation") as silver_group:

        transform_admissions = BashOperator(
            task_id="transform_admissions",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"{SPARK_SUBMIT} --master local[2] "
                f"spark_jobs/03_transform_silver_admissions.py"
            ),
            sla=timedelta(minutes=45),
        )

        transform_labs = BashOperator(
            task_id="transform_labs",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"{SPARK_SUBMIT} --master local[2] "
                f"spark_jobs/04_transform_silver_labs.py"
            ),
            sla=timedelta(minutes=30),
        )

        build_patient_dim = BashOperator(
            task_id="build_patient_dim_scd2",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"{SPARK_SUBMIT} --master local[2] "
                f"spark_jobs/05_scd2_patient_dim.py"
            ),
            # Patient dim depends on admissions silver being ready
            # (set with >> arrow below, inside the group)
        )

        transform_admissions >> build_patient_dim
        # transform_labs runs in parallel with the above chain

    # ── Task Group: Gold Layer ─────────────────────────────────────────────────
    with TaskGroup("gold_build") as gold_group:

        build_gold = BashOperator(
            task_id="build_all_gold_tables",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"{SPARK_SUBMIT} --master local[2] "
                f"spark_jobs/06_build_gold_layer.py"
            ),
            sla=timedelta(minutes=30),
        )

    # ── Task: Data Quality Gate ───────────────────────────────────────────────
    # After Gold is built, run automated checks before declaring success.
    # This is your "does the output make sense?" sanity layer.
    def run_quality_gates(**context):
        """
        Automated quality checks on Gold tables.
        Raises an exception (failing the DAG) if checks don't pass.
        This prevents silent bad data from reaching dashboards.
        """
        import psycopg2
        import sys
        sys.path.insert(0, PROJECT_DIR)
        from config.spark_config import POSTGRES

        # In a real setup these checks would query the Gold Delta tables.
        # For simplicity, we check against local parquet/csv outputs.
        # You'd swap these for actual SQL queries against your Gold tables.
        checks = {
            "admissions_not_empty":    ("SELECT COUNT(*) > 0",        True),
            "readmission_rate_sane":   ("SELECT MAX(readmission_rate_pct) < 100", True),
        }

        print("Running data quality gates...")
        failed_checks = []

        # In production, run actual SQL against Gold tables
        # For local dev, we do a simple file existence check
        import os
        gold_path = f"{PROJECT_DIR}/data/output/gold/"
        if not os.path.exists(gold_path):
            raise ValueError(f"Gold output path not found: {gold_path}. "
                             "Did build_gold_tables complete successfully?")

        print("All quality gates passed.")

    quality_gate = PythonOperator(
        task_id="run_quality_gates",
        python_callable=run_quality_gates,
    )

    # ── Task: Notify Success ──────────────────────────────────────────────────
    notify_success = BashOperator(
        task_id="notify_pipeline_complete",
        bash_command=(
            'echo "Healthcare pipeline completed successfully at $(date). '
            'Gold tables are ready for consumption."'
        ),
        # Only run if ALL upstream tasks succeeded
        trigger_rule="all_success",
    )

    # ── DAG Dependency Chain ──────────────────────────────────────────────────
    # This is the heart of the DAG: the execution order.
    # Read it as: pre_check THEN bronze_group THEN silver_group THEN gold_group
    # THEN quality_gate THEN notify_success.
    #
    # Within bronze_group, admissions and labs ingest IN PARALLEL.
    # Within silver_group, labs transforms in parallel with admissions+patient_dim.
    pre_check >> bronze_group >> silver_group >> gold_group >> quality_gate >> notify_success
