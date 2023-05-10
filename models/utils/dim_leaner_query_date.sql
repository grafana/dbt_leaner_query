-- Heavily leveraged from the gitlab analytics team's date dimension:
-- https://discourse.getdbt.com/t/building-a-calendar-table-using-dbt/325
-- https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/sources/date/date_details_source.sql

WITH date_spine AS (

    SELECT *
    FROM
        UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2050-01-01', INTERVAL 1 DAY)) AS date_day

),

formatted_dates AS (

    SELECT
        date_day,
        date_day AS date_actual,
        CAST(
            date_day AS timestamp
        ) AS start_of_day_timestamp,
        TIMESTAMP_SUB(CAST(DATE_ADD(date_day, INTERVAL 1 DAY) AS timestamp), INTERVAL 1 SECOND) AS end_of_day_timestamp,
        FORMAT_DATE('%A', date_day) AS day_name,
        FORMAT_DATE('%B', date_day) AS month_name,
        FORMAT_DATE(
            '%b', date_day
        ) AS abbreviated_month_name,
        FORMAT_DATE(
            "%b'%g", date_day
        ) AS formatted_month_year_name,

        EXTRACT(MONTH FROM date_day) AS month_actual,
        EXTRACT(YEAR FROM date_day) AS year_actual,
        EXTRACT(QUARTER FROM date_day) AS quarter_actual,
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,

        DATE_TRUNC(date_day, MONTH) AS first_day_of_month,
        DATE_TRUNC(date_day, WEEK) AS first_day_of_week,
        EXTRACT(WEEK FROM date_day) + 1 AS week_of_year,

        LAST_DAY(date_day, MONTH) AS last_day_of_month,
        LAST_DAY(date_day, YEAR) AS last_day_of_year,
        LAST_DAY(date_day, QUARTER) AS last_day_of_quarter,

        CASE WHEN EXTRACT(MONTH FROM date_day) = 1 THEN 12
            ELSE EXTRACT(MONTH FROM date_day) - 1 END AS month_of_fiscal_year,

        SAFE_CAST(CASE WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN 'New Years Day'
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 25 THEN 'Christmas Day'
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 26 THEN 'Boxing Day'
            ELSE NULL END AS STRING) AS holiday_desc
    FROM date_spine

),

dates_with_fiscal_calcs AS(
    SELECT
        *,
        -- timestamp used for joins to snapshots or anything with valid_to / valid_from
        -- creates a timestamp that is the last second of each day, unless it is the current date
        -- if current date, use current_timestamp
        -- if future date, leave null
        CASE
            WHEN date_day < CURRENT_DATE THEN end_of_day_timestamp
            WHEN date_day = CURRENT_DATE THEN CURRENT_TIMESTAMP
        END AS snapshot_join_timestamp,
        CASE WHEN month_actual < 2
            THEN year_actual
            ELSE (year_actual + 1) END AS fiscal_year,
        CASE WHEN month_actual < 2 THEN '4'
            WHEN month_actual < 5 THEN '1'
            WHEN month_actual < 8 THEN '2'
            WHEN month_actual < 11 THEN '3'
            ELSE '4' END
        AS fiscal_quarter
    FROM formatted_dates
),

dates_with_aggregates AS (
    SELECT
        *,

        fiscal_year || '-' || EXTRACT(
            MONTH FROM date_day
        ) AS fiscal_month_name,
        (
            fiscal_year || '-' || 'Q' || fiscal_quarter
        ) AS fiscal_quarter_name,
        (
            first_day_of_week = date_day
        ) AS is_first_day_of_week,
        (
            last_day_of_month = date_day
        ) AS is_last_day_of_month,
        (day_of_week BETWEEN 2 AND 6) AS is_weekday,


        ROW_NUMBER() OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day) AS day_of_quarter,
        ROW_NUMBER() OVER (PARTITION BY year_actual ORDER BY date_day) AS day_of_year,

        ROW_NUMBER() OVER (
            PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day
        ) AS day_of_fiscal_quarter,
        ROW_NUMBER() OVER (
            PARTITION BY fiscal_year ORDER BY date_day
        ) AS day_of_fiscal_year,

        FIRST_VALUE(
            date_day
        ) OVER (PARTITION BY year_actual ORDER BY date_day) AS first_day_of_year,

        FIRST_VALUE(
            date_day
        ) OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day) AS first_day_of_quarter,

        FIRST_VALUE(date_day) OVER (PARTITION BY fiscal_year, fiscal_quarter
            ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_day_of_fiscal_quarter,
        LAST_VALUE(date_day) OVER (PARTITION BY fiscal_year, fiscal_quarter
            ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_day_of_fiscal_quarter,

        FIRST_VALUE(date_day) OVER (PARTITION BY fiscal_year
            ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_day_of_fiscal_year,
        LAST_VALUE(date_day) OVER (PARTITION BY fiscal_year
            ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_day_of_fiscal_year
    FROM dates_with_fiscal_calcs
),

final AS(
    SELECT
        *,
{{ generate_surrogate_key(['date_actual']) }} AS date_key,
        DATE_DIFF(
            date_actual, first_day_of_fiscal_year, WEEK
        ) + 1 AS week_of_fiscal_year,
        DATE_DIFF(
            date_actual, first_day_of_fiscal_quarter, WEEK
        ) + 1 AS week_of_fiscal_quarter,

        LAST_VALUE(date_day) OVER (PARTITION BY first_day_of_week
            ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_day_of_week,

        (week_of_year - EXTRACT(WEEK FROM first_day_of_month)) AS week_of_month,
        (year_actual || '-Q' || EXTRACT(QUARTER FROM date_day)) AS quarter_name,

        (
            'FY' || SUBSTR(fiscal_quarter_name, 3, 7)
        ) AS fiscal_quarter_name_fy,
        DENSE_RANK() OVER (
            ORDER BY fiscal_quarter_name
        ) AS fiscal_quarter_number_absolute,

        (
            'FY' || SUBSTR(fiscal_month_name, 3, 8)
        ) AS fiscal_month_name_fy,

        SAFE_CAST(CASE WHEN HOLIDAY_DESC IS NULL THEN 0
            ELSE 1 END AS BOOLEAN) AS is_holiday,
        DATE_TRUNC(
            last_day_of_fiscal_quarter, MONTH
        ) AS last_month_of_fiscal_quarter,
        IF(
            DATE_TRUNC(last_day_of_fiscal_quarter, MONTH) = date_actual, TRUE, FALSE
        ) AS is_first_day_of_last_month_of_fiscal_quarter,
        DATE_TRUNC(
            last_day_of_fiscal_year, MONTH
        ) AS last_month_of_fiscal_year,
        IF(
            DATE_TRUNC(last_day_of_fiscal_year, MONTH) = date_actual, TRUE, FALSE
        ) AS is_first_day_of_last_month_of_fiscal_year,
        COUNT(
            date_actual
        ) OVER (PARTITION BY first_day_of_month) AS days_in_month_count,
    FROM dates_with_aggregates

)

SELECT *
FROM final
