-- models/gold/avg_los_by_department.sql
-- ======================================
-- Monthly average length-of-stay per department — the #1 operational KPI
-- for hospital bed management and staffing decisions.
--
-- Upstream: silver_admissions (staging model)
-- Consumers: Ops dashboards, Airflow freshness checks, executive reports

with silver_admissions as (
    select * from {{ ref('stg_admissions') }}
    where data_quality_flag = 'VALID'
),

monthly_stats as (
    select
        department,
        date_trunc('month', admission_date::date)   as admission_month,
        count(distinct admission_id)                as total_admissions,
        count(distinct patient_id)                  as unique_patients,
        round(avg(length_of_stay_days)::numeric, 2) as avg_los_days,
        round(stddev(length_of_stay_days)::numeric, 2) as stddev_los_days,
        min(length_of_stay_days)                    as min_los_days,
        max(length_of_stay_days)                    as max_los_days,
        sum(case when readmission then 1 else 0 end) as readmission_count,
        round(
            (100.0 * sum(case when readmission then 1 else 0 end) / count(*))::numeric, 2
        )                                           as readmission_rate_pct,
        round(avg(total_cost_usd)::numeric, 2)      as avg_cost_usd,
        round(sum(total_cost_usd)::numeric, 2)      as total_revenue_usd,
        round(avg(age)::numeric, 1)                 as avg_patient_age,
        sum(case when icu_stay then 1 else 0 end)   as icu_admissions
    from silver_admissions
    group by 1, 2
)

select
    *,
    -- 3-month rolling average LOS for trend smoothing
    round(avg(avg_los_days) over (
        partition by department
        order by admission_month
        rows between 2 preceding and current row
    )::numeric, 2)                                  as rolling_3mo_avg_los,
    -- Readmission status classification for alerting
    case
        when readmission_rate_pct > 20 then 'CRITICAL'
        when readmission_rate_pct > 15 then 'WARNING'
        else 'OK'
    end                                             as readmission_status,
    current_timestamp                               as _dbt_built_at
from monthly_stats
order by department, admission_month
