-- scripts/init_db.sql
-- =====================
-- Runs automatically when the postgres container starts for the first time.
-- Creates the schemas that dbt and the pipeline audit logger write to.

-- Silver schema: dbt staging models read from here
CREATE SCHEMA IF NOT EXISTS silver;

-- Gold schema: dbt mart/gold models write here
CREATE SCHEMA IF NOT EXISTS gold;

-- Pipeline audit log table (written to by config/spark_config.py)
CREATE TABLE IF NOT EXISTS public.pipeline_audit_log (
    id              SERIAL PRIMARY KEY,
    pipeline        VARCHAR(100) NOT NULL,
    layer           VARCHAR(20)  NOT NULL,  -- bronze / silver / gold
    status          VARCHAR(20)  NOT NULL,  -- success / failure
    records_in      INTEGER      DEFAULT 0,
    records_out     INTEGER      DEFAULT 0,
    finished_at     TIMESTAMP    DEFAULT NOW(),
    notes           TEXT
);

-- Placeholder Silver admissions table (populated by Spark export job)
CREATE TABLE IF NOT EXISTS silver.admissions (
    admission_id            VARCHAR(20),
    patient_id              VARCHAR(20),
    admission_date          DATE,
    discharge_date          DATE,
    department              VARCHAR(50),
    diagnosis               VARCHAR(100),
    age                     INTEGER,
    gender                  CHAR(1),
    attending_doctor        VARCHAR(20),
    insurance_id            VARCHAR(20),
    insurer_name            VARCHAR(50),
    readmission             BOOLEAN,
    icu_stay                BOOLEAN,
    num_procedures          INTEGER,
    total_cost_usd          NUMERIC(10, 2),
    length_of_stay_days     INTEGER,
    age_group               VARCHAR(10),
    cost_tier               VARCHAR(10),
    admission_year          INTEGER,
    admission_month         INTEGER,
    admission_quarter       VARCHAR(5),
    dq_flag                 VARCHAR(50),
    _silver_processed_at    TIMESTAMP
);
