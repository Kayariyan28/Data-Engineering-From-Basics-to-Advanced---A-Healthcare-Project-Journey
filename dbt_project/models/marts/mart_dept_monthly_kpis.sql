-- models/marts/mart_dept_monthly_kpis.sql
-- ==========================================
-- This is the final Gold layer model that analysts and dashboards consume.
-- It answers the question: "How is each department performing each month?"
--
-- The key power of dbt here is that this model:
--   1. Is version-controlled (lives in Git alongside your code)
--   2. Is documented (see schema.yml)
--   3. Is tested (not_null, uniqueness checks run automatically)
--   4. Has lineage (dbt builds a graph from stg_admissions → this model)
--   5. Is reproducible (anyone can run `dbt run` and get identical output)
--
-- This is what separates dbt-managed SQL from ad-hoc SQL scripts scattered
-- in a shared drive that nobody maintains.

with admissions as (
    select * from {{ ref('stg_admissions') }}
),

-- Monthly aggregations per department
monthly_base as (
    select
        department,
        date_trunc('month', admission_date)         as month_start,
        to_char(admission_date, 'YYYY-MM')          as year_month,
        admission_year,
        admission_quarter,

        -- Volume metrics
        count(*)                                    as total_admissions,
        count(distinct patient_id)                  as unique_patients,
        count(distinct attending_doctor)            as active_doctors,

        -- Clinical metrics
        round(avg(length_of_stay_days)::numeric, 2) as avg_los_days,
        round(stddev(length_of_stay_days)::numeric, 2) as stddev_los,
        min(length_of_stay_days)                    as min_los_days,
        max(length_of_stay_days)                    as max_los_days,
        percentile_cont(0.5) within group
            (order by length_of_stay_days)          as median_los_days,

        -- Readmission metrics
        sum(case when readmission then 1 else 0 end) as readmission_count,
        round(
            100.0 * sum(case when readmission then 1 else 0 end) / count(*), 2
        )                                            as readmission_rate_pct,

        -- Acuity metrics
        sum(case when icu_stay then 1 else 0 end)   as icu_admissions,
        round(avg(num_procedures)::numeric, 2)      as avg_procedures,

        -- Financial metrics
        round(avg(total_cost_usd)::numeric, 2)      as avg_cost_usd,
        round(sum(total_cost_usd)::numeric, 2)      as total_revenue_usd,
        round(min(total_cost_usd)::numeric, 2)      as min_cost_usd,
        round(max(total_cost_usd)::numeric, 2)      as max_cost_usd,

        -- Patient demographics
        round(avg(age)::numeric, 1)                 as avg_patient_age

    from admissions
    group by 1, 2, 3, 4, 5
),

-- Add rolling window calculations
-- Window functions in SQL are one of the most powerful analytical tools.
-- The "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" clause means:
-- "for each row, look at the current row and the 2 rows before it
-- (when sorted by month) within the same department."
-- This gives us a 3-month rolling average without any complex self-joins.
with_rolling as (
    select
        *,
        round(avg(avg_los_days) over (
            partition by department
            order by month_start
            rows between 2 preceding and current row
        )::numeric, 2)                              as rolling_3mo_avg_los,

        round(avg(readmission_rate_pct) over (
            partition by department
            order by month_start
            rows between 2 preceding and current row
        )::numeric, 2)                              as rolling_3mo_readmission_pct,

        round(avg(total_revenue_usd) over (
            partition by department
            order by month_start
            rows between 2 preceding and current row
        )::numeric, 2)                              as rolling_3mo_revenue,

        -- Month-over-month change in admissions volume
        lag(total_admissions, 1) over (
            partition by department
            order by month_start
        )                                           as prev_month_admissions,

        round(
            100.0 * (total_admissions - lag(total_admissions, 1) over (
                partition by department order by month_start
            )) / nullif(lag(total_admissions, 1) over (
                partition by department order by month_start
            ), 0), 1
        )                                           as mom_admissions_change_pct

    from monthly_base
)

select
    *,
    -- A business flag: is this department's readmission rate above the 15% target?
    case
        when readmission_rate_pct > 20 then 'CRITICAL'
        when readmission_rate_pct > 15 then 'WARNING'
        else 'OK'
    end                                             as readmission_status,

    current_timestamp                               as _dbt_updated_at

from with_rolling
order by department, month_start
