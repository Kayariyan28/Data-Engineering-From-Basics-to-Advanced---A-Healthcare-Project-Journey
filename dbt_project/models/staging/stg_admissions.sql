-- models/staging/stg_admissions.sql
-- ====================================
-- Staging models are a thin layer that selects from source tables and
-- renames/casts columns to a consistent naming convention.
-- 
-- Think of staging as your "translation layer" from source naming to
-- your organization's naming standards. If the source ever changes a
-- column name from "adm_date" to "admission_dt", you only fix it here —
-- not in every downstream model.
--
-- In our architecture, the Silver Delta Lake tables are our "source" for dbt.
-- dbt reads them via the Spark-to-Postgres export or directly from Delta.
-- For simplicity in this project, we point dbt at the PostgreSQL gold schema
-- tables we export Silver data into.

with source as (
    select * from {{ source('silver', 'admissions') }}
),

staged as (
    select
        -- Keys
        admission_id,
        patient_id,
        attending_doctor,

        -- Dates
        admission_date,
        discharge_date,

        -- Patient attributes
        age,
        age_group,
        gender,

        -- Clinical
        department,
        diagnosis,
        length_of_stay_days,
        readmission,
        icu_stay,
        num_procedures,

        -- Financial
        insurance_id,
        insurer_name,
        total_cost_usd,
        cost_tier,

        -- Time dimensions (useful for joining to a date dimension)
        admission_year,
        admission_month,
        admission_quarter,

        -- Quality
        dq_flag

    from source
    where dq_flag = 'VALID'   -- Staging always filters to valid records only
)

select * from staged
