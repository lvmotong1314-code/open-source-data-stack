with observed_dates as (

    select trip_start_timestamp::date as full_date
    from {{ ref('stg_taxi_trips') }}
    where trip_start_timestamp is not null

    union all

    select trip_end_timestamp::date as full_date
    from {{ ref('stg_taxi_trips') }}
    where trip_end_timestamp is not null

),

date_bounds as (

    select
        min(full_date) as min_date,
        max(full_date) as max_date
    from observed_dates

),

date_spine as (

    select
        generate_series(
            min_date,
            max_date,
            interval '1 day'
        )::date as full_date
    from date_bounds
    where min_date is not null
      and max_date is not null

),

calendar_dates as (

    select
        to_char(full_date, 'YYYYMMDD')::integer as date_key,
        full_date,

        extract(year from full_date)::integer as year,
        extract(quarter from full_date)::integer as quarter,
        extract(month from full_date)::integer as month,
        to_char(full_date, 'FMMonth') as month_name,

        extract(day from full_date)::integer as day_of_month,
        extract(isodow from full_date)::integer as day_of_week,
        to_char(full_date, 'FMDay') as day_name,

        extract(isodow from full_date) in (6, 7) as is_weekend

    from date_spine

),

unknown_date as (

    select
        0::integer as date_key,
        null::date as full_date,

        null::integer as year,
        null::integer as quarter,
        null::integer as month,
        'Unknown'::text as month_name,

        null::integer as day_of_month,
        null::integer as day_of_week,
        'Unknown'::text as day_name,

        null::boolean as is_weekend

)

select *
from unknown_date

union all

select *
from calendar_dates