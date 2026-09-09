with staging_trips as (

    select trip_id
    from {{ ref('stg_taxi_trips') }}

),

fact_trips as (

    select trip_id
    from {{ ref('fct_taxi_trips') }}

)

select
    coalesce(staging_trips.trip_id, fact_trips.trip_id) as trip_id

from staging_trips

full outer join fact_trips
    on staging_trips.trip_id = fact_trips.trip_id

where staging_trips.trip_id is null
   or fact_trips.trip_id is null