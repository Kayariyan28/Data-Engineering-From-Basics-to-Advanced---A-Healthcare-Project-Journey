"""
config/spark_config.py
========================
Central configuration for all Spark jobs in the healthcare platform.

Why centralize config?
  Without this, every script would hardcode credentials and paths.
  When the MinIO password changes (it will), you'd update 10 files instead of 1.
  When you move from local dev to staging, you change ONE config, not everything.

In production you'd replace the hardcoded values here with:
  os.environ.get("MINIO_PASSWORD") — reading from environment variables
  or a secrets manager like AWS Secrets Manager / HashiCorp Vault.
"""

import os
from pyspark.sql import SparkSession
import psycopg2
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# SPARK CONFIGURATION
# These settings are passed to SparkSession.builder.config()
# ─────────────────────────────────────────────────────────────────────────────
SPARK_PACKAGES = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
)

SPARK_SETTINGS = {
    # Delta Lake integration
    "spark.jars.packages":    SPARK_PACKAGES,
    "spark.sql.extensions":   "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # MinIO (S3-compatible storage) settings
    # path.style.access=true is required for MinIO — AWS S3 uses virtual-hosted style
    # but MinIO uses path style (minio:9000/bucket/key vs bucket.s3.amazonaws.com/key)
    "spark.hadoop.fs.s3a.endpoint":         "http://localhost:9000",
    "spark.hadoop.fs.s3a.access.key":       "minioadmin",
    "spark.hadoop.fs.s3a.secret.key":       "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl":             "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # This tells Hadoop not to use the AWS SDK's region detection
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

    # Performance tuning for local development
    # Default shuffle partitions is 200 — way too many for a laptop!
    # With 10k rows, 4 partitions is plenty.
    "spark.sql.shuffle.partitions": "4",
    "spark.driver.memory":          "2g",
    "spark.executor.memory":        "2g",

    # Suppress noisy Delta Lake deprecation warnings
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
}


def build_spark_session(app_name: str) -> SparkSession:
    """
    Build and return a configured SparkSession.

    This factory function ensures every job uses identical configuration.
    Calling spark.stop() at the end of each job and build_spark_session()
    at the start is the recommended pattern for standalone scripts.
    """
    print(f"Building SparkSession: {app_name}")
    print("  (First run downloads ~300MB of JARs — subsequent runs are instant)")

    builder = SparkSession.builder.appName(app_name)
    for key, value in SPARK_SETTINGS.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log noise
    print(f"  Spark {spark.version} ready\n")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# STORAGE PATHS
# All data lives in MinIO under the 'healthcare' bucket.
# Medallion layers are clearly separated by path.
# ─────────────────────────────────────────────────────────────────────────────
STORAGE_PATHS = {
    # Bronze — raw, immutable landing zone
    "bronze_admissions":    "s3a://healthcare/bronze/admissions/",
    "bronze_labs":          "s3a://healthcare/bronze/labs/",
    "bronze_vitals":        "s3a://healthcare/bronze/vitals/",

    # Silver — clean, typed, validated
    "silver_admissions":    "s3a://healthcare/silver/admissions/",
    "silver_labs":          "s3a://healthcare/silver/labs/",
    "silver_quarantine":    "s3a://healthcare/silver/quarantine/",
    "silver_patient_dim":   "s3a://healthcare/silver/dim_patient/",

    # Gold — aggregated, business-ready
    "gold_dept_kpis":       "s3a://healthcare/gold/dept_monthly_kpis/",
    "gold_readmissions":    "s3a://healthcare/gold/readmission_analysis/",
    "gold_financial":       "s3a://healthcare/gold/financial_summary/",
    "gold_critical_labs":   "s3a://healthcare/gold/critical_labs_watch/",

    # Streaming checkpoints (Kafka → Bronze)
    "checkpoints_vitals":   "s3a://healthcare-checkpoints/vitals/",
}


# ─────────────────────────────────────────────────────────────────────────────
# POSTGRES CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
POSTGRES = {
    "host":     os.environ.get("PG_HOST",     "localhost"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "database": os.environ.get("PG_DB",       "healthcare_db"),
    "user":     os.environ.get("PG_USER",     "healthcare"),
    "password": os.environ.get("PG_PASSWORD", "healthcare123"),
}


# ─────────────────────────────────────────────────────────────────────────────
# KAFKA CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
KAFKA = {
    "bootstrap_servers": os.environ.get("KAFKA_BROKERS", "localhost:9092"),
    "vitals_topic":      "patient-vitals",
    "labs_topic":        "lab-results-stream",
}


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE AUDIT LOGGING
# Every pipeline run is logged to PostgreSQL for monitoring and debugging.
# This gives you a simple "did the pipeline run successfully?" dashboard.
# ─────────────────────────────────────────────────────────────────────────────
def log_pipeline_run(pipeline: str, layer: str, status: str,
                     records_in: int = 0, records_out: int = 0,
                     duration_secs: int = 0, batch_id: str = None,
                     notes: str = None):
    """
    Write a pipeline run record to the audit log table in PostgreSQL.
    Silently skips if PostgreSQL is not reachable (e.g., during local testing).
    """
    try:
        conn = psycopg2.connect(**POSTGRES)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_audit_log
                (pipeline, layer, status, records_in, records_out, finished_at, notes)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        """, (pipeline, layer, status, records_in, records_out,
              f"batch={batch_id} duration={duration_secs}s {notes or ''}"))
        conn.commit()
        cur.close()
        conn.close()
        print(f"  Audit log written: {pipeline} → {status}")
    except Exception as e:
        # Non-fatal: logging failure should not break the pipeline
        print(f"  [Warning] Could not write audit log: {e}")
